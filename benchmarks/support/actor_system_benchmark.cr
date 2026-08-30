require "../../src/movie"
require "../../src/movie/remote"
require "csv"
require "json"
require "option_parser"
require "process/executable_path"

module Movie
  module Benchmarks
    module ActorSystem
      enum Topology
        Local
        InProcess
        TwoProcess
        All

        def self.from_label(label : String) : self
          case label
          when "local"       then Local
          when "in-process"  then InProcess
          when "two-process" then TwoProcess
          when "all"         then All
          else
            raise ArgumentError.new("unknown topology: #{label}")
          end
        end

        def label : String
          case self
          when Local      then "local"
          when InProcess  then "in-process"
          when TwoProcess then "two-process"
          when All        then "all"
          else                 raise "unreachable topology"
          end
        end
      end

      enum Operation
        Tell
        Ask
        Both

        def self.from_label(label : String) : self
          case label
          when "tell" then Tell
          when "ask"  then Ask
          when "both" then Both
          else
            raise ArgumentError.new("unknown operation: #{label}")
          end
        end

        def label : String
          case self
          when Tell then "tell"
          when Ask  then "ask"
          when Both then "both"
          else           raise "unreachable operation"
          end
        end
      end

      enum OutputFormat
        Human
        Csv
        JsonLines

        def self.from_label(label : String) : self
          case label
          when "human" then Human
          when "csv"   then Csv
          when "jsonl" then JsonLines
          else
            raise ArgumentError.new("unknown output format: #{label}")
          end
        end
      end

      struct Config
        getter topology : Topology
        getter operation : Operation
        getter messages : Int32
        getter payload_bytes : Int32
        getter producers : Int32
        getter actors : Int32
        getter in_flight : Int32
        getter stripes : Int32
        getter warmup_runs : Int32
        getter measurement_runs : Int32
        getter output_format : OutputFormat

        def initialize(
          @topology : Topology = Topology::Local,
          @operation : Operation = Operation::Both,
          @messages : Int32 = 100_000,
          @payload_bytes : Int32 = 16,
          @producers : Int32 = 1,
          @actors : Int32 = 1,
          @in_flight : Int32 = 32,
          @stripes : Int32 = 1,
          @warmup_runs : Int32 = 2,
          @measurement_runs : Int32 = 10,
          @output_format : OutputFormat = OutputFormat::Human,
        )
          validate
        end

        def self.parse(arguments : Array(String)) : self
          topology = Topology::Local
          operation = Operation::Both
          messages = 100_000
          payload_bytes = 16
          producers = 1
          actors = 1
          in_flight = 32
          stripes = 1
          warmup_runs = 2
          measurement_runs = 10
          output_format = OutputFormat::Human

          parser = OptionParser.new do |options|
            options.banner = "Usage: actor-system-benchmark [options]"
            options.on("--topology NAME", "local, in-process, two-process, or all") { |value| topology = Topology.from_label(value) }
            options.on("--operation NAME", "tell, ask, or both") { |value| operation = Operation.from_label(value) }
            options.on("--messages COUNT", "messages per measured batch") { |value| messages = value.to_i }
            options.on("--payload-bytes COUNT", "payload size in bytes") { |value| payload_bytes = value.to_i }
            options.on("--producers COUNT", "concurrent tell producers") { |value| producers = value.to_i }
            options.on("--actors COUNT", "target actor count") { |value| actors = value.to_i }
            options.on("--in-flight COUNT", "concurrent ask requests") { |value| in_flight = value.to_i }
            options.on("--stripes COUNT", "remote connection stripe count") { |value| stripes = value.to_i }
            options.on("--warmup COUNT", "warm-up batches per operation") { |value| warmup_runs = value.to_i }
            options.on("--runs COUNT", "measured batches per operation") { |value| measurement_runs = value.to_i }
            options.on("--format NAME", "human, csv, or jsonl") { |value| output_format = OutputFormat.from_label(value) }
            options.on("-h", "--help", "show this help") do
              puts options
              exit
            end
          end
          parser.parse(arguments.dup)

          new(
            topology: topology,
            operation: operation,
            messages: messages,
            payload_bytes: payload_bytes,
            producers: producers,
            actors: actors,
            in_flight: in_flight,
            stripes: stripes,
            warmup_runs: warmup_runs,
            measurement_runs: measurement_runs,
            output_format: output_format
          )
        rescue ex : ArgumentError
          raise ex
        rescue ex
          raise ArgumentError.new(ex.message || "invalid benchmark arguments")
        end

        private def validate
          validate_positive("messages", @messages)
          validate_non_negative("payload_bytes", @payload_bytes)
          validate_positive("producers", @producers)
          validate_positive("actors", @actors)
          validate_positive("in_flight", @in_flight)
          validate_positive("stripes", @stripes)
          validate_non_negative("warmup", @warmup_runs)
          validate_positive("runs", @measurement_runs)
        end

        private def validate_positive(name : String, value : Int32)
          raise ArgumentError.new("#{name} must be greater than zero") unless value > 0
        end

        private def validate_non_negative(name : String, value : Int32)
          raise ArgumentError.new("#{name} must not be negative") if value < 0
        end
      end

      record Command, kind : String, sequence : Int64, payload : String do
        include JSON::Serializable

        TELL     = "tell"
        ASK      = "ask"
        SNAPSHOT = "snapshot"

        def self.tell(sequence : Int64, payload : String) : self
          new(TELL, sequence, payload)
        end

        def self.ask(sequence : Int64, payload : String) : self
          new(ASK, sequence, payload)
        end

        def self.snapshot : self
          new(SNAPSHOT, 0_i64, "")
        end
      end

      record AskReply, sequence : Int64 do
        include JSON::Serializable
      end

      record Snapshot,
        processed_tells : Int64,
        total_allocated_bytes : UInt64,
        cpu_seconds : Float64 do
        include JSON::Serializable
      end

      def self.register_messages : Nil
        Movie::Remote::MessageRegistry.register(Command)
        Movie::Remote::MessageRegistry.register(AskReply)
        Movie::Remote::MessageRegistry.register(Snapshot)
      end

      class TargetBehavior < Movie::AbstractBehavior(Command)
        @processed_tells = 0_i64

        def receive(message : Command, context : Movie::ActorContext(Command))
          case message.kind
          when Command::TELL
            @processed_tells += 1
          when Command::ASK
            Movie::Ask.reply_if_asked(context.sender, AskReply.new(message.sequence))
          when Command::SNAPSHOT
            Movie::Ask.reply_if_asked(context.sender, Snapshot.new(
              processed_tells: @processed_tells,
              total_allocated_bytes: GC.stats.total_bytes,
              cpu_seconds: Statistics.process_cpu_seconds
            ))
          else
            raise ArgumentError.new("unknown benchmark command: #{message.kind}")
          end
          Movie::Behaviors(Command).same
        end
      end

      struct LocalTarget
        def initialize(@ref : Movie::ActorRef(Command))
        end

        def tell(command : Command) : Nil
          @ref << command
        end

        def ask(command : Command) : AskReply
          @ref.ask(command, AskReply, 30.seconds).await(30.seconds)
        end

        def snapshot : Snapshot
          @ref.ask(Command.snapshot, Snapshot, 30.seconds).await(30.seconds)
        end
      end

      struct RemoteTarget
        def initialize(@ref : Movie::Remote::RemoteActorRef(Command))
        end

        def tell(command : Command) : Nil
          @ref << command
        end

        def ask(command : Command) : AskReply
          @ref.ask(command, AskReply, 30.seconds).await(30.seconds)
        end

        def snapshot : Snapshot
          @ref.ask(Command.snapshot, Snapshot, 30.seconds).await(30.seconds)
        end
      end

      module Statistics
        extend self

        def percentile(samples : Array(Int64), quantile : Float64) : Int64
          raise ArgumentError.new("samples must not be empty") if samples.empty?
          raise ArgumentError.new("quantile must be between zero and one") unless 0.0 <= quantile <= 1.0

          ordered = samples.sort
          rank = (quantile * ordered.size).ceil.to_i - 1
          ordered[rank.clamp(0, ordered.size - 1)]
        end

        def median(samples : Array(Float64)) : Float64
          raise ArgumentError.new("samples must not be empty") if samples.empty?
          ordered = samples.sort
          middle = ordered.size // 2
          return ordered[middle] if ordered.size.odd?
          (ordered[middle - 1] + ordered[middle]) / 2.0
        end

        def process_cpu_seconds : Float64
          times = Process.times
          times.utime + times.stime
        end
      end

      record Measurement,
        timestamp_utc : String,
        git_sha : String,
        crystal_version : String,
        release_build : Bool,
        cpu_count : Int32,
        crystal_workers : String,
        topology : String,
        operation : String,
        messages : Int32,
        payload_bytes : Int32,
        producers : Int32,
        actors : Int32,
        in_flight : Int32,
        stripes : Int32,
        run : Int32,
        elapsed_nanoseconds : Int64,
        processed_messages : Int64,
        messages_per_second : Float64,
        nanoseconds_per_message : Float64,
        client_allocated_bytes : Int64,
        server_allocated_bytes : Int64?,
        bytes_per_message : Float64,
        server_bytes_per_message : Float64?,
        client_cpu_seconds : Float64,
        server_cpu_seconds : Float64?,
        p50_nanoseconds : Int64?,
        p95_nanoseconds : Int64?,
        p99_nanoseconds : Int64?,
        max_nanoseconds : Int64? do
        include JSON::Serializable

        CSV_HEADERS = [
          "timestamp_utc", "git_sha", "crystal_version", "release_build", "cpu_count", "crystal_workers",
          "topology", "operation", "messages", "payload_bytes", "producers", "actors", "in_flight", "stripes", "run",
          "elapsed_nanoseconds", "processed_messages", "messages_per_second", "nanoseconds_per_message",
          "client_allocated_bytes", "server_allocated_bytes", "bytes_per_message", "server_bytes_per_message",
          "client_cpu_seconds", "server_cpu_seconds", "p50_nanoseconds", "p95_nanoseconds", "p99_nanoseconds", "max_nanoseconds",
        ]

        def csv_values : Array(String)
          [
            timestamp_utc, git_sha, crystal_version, release_build.to_s, cpu_count.to_s, crystal_workers,
            topology, operation, messages.to_s, payload_bytes.to_s, producers.to_s, actors.to_s, in_flight.to_s, stripes.to_s, run.to_s,
            elapsed_nanoseconds.to_s, processed_messages.to_s, messages_per_second.to_s, nanoseconds_per_message.to_s,
            client_allocated_bytes.to_s, server_allocated_bytes.try(&.to_s) || "", bytes_per_message.to_s,
            server_bytes_per_message.try(&.to_s) || "", client_cpu_seconds.to_s, server_cpu_seconds.try(&.to_s) || "",
            p50_nanoseconds.try(&.to_s) || "", p95_nanoseconds.try(&.to_s) || "",
            p99_nanoseconds.try(&.to_s) || "", max_nanoseconds.try(&.to_s) || "",
          ]
        end
      end

      private record ProcessSnapshot, allocated_bytes : UInt64, cpu_seconds : Float64
      private record WorkloadResult,
        elapsed_nanoseconds : Int64,
        client_allocated_bytes : Int64,
        client_cpu_seconds : Float64,
        server_allocated_bytes : Int64?,
        server_cpu_seconds : Float64?,
        processed_messages : Int64,
        latencies : Array(Int64)

      class Runner
        def initialize(@config : Config)
          Movie::Benchmarks::ActorSystem.register_messages
          @git_sha = Metadata.git_sha
        end

        def run : Array(Measurement)
          case @config.topology
          when Topology::Local
            run_local
          when Topology::InProcess
            run_in_process
          when Topology::TwoProcess
            run_two_process
          when Topology::All
            run_local + run_in_process + run_two_process
          else
            raise "unreachable topology"
          end
        end

        private def run_local : Array(Measurement)
          system = new_system("actor-bench-local")
          targets = spawn_local_targets(system)
          run_workloads(targets, Topology::Local, capture_server_metrics: false)
        ensure
          system.try &.shutdown
        end

        private def run_in_process : Array(Measurement)
          server = new_system("actor-bench-inproc-server")
          local_refs = spawn_actors(server)
          server.enable_remoting("127.0.0.1", 0, @config.stripes)

          client = new_system("actor-bench-inproc-client")
          client.enable_remoting("127.0.0.1", 0, @config.stripes)
          targets = local_refs.map do |ref|
            remote_ref = client.actor_for(ref.path.not_nil!.to_s, Command).as(Movie::Remote::RemoteActorRef(Command))
            RemoteTarget.new(remote_ref)
          end
          run_workloads(targets, Topology::InProcess, capture_server_metrics: false)
        ensure
          client.try &.shutdown
          server.try &.shutdown
        end

        private def run_two_process : Array(Measurement)
          executable = Process.executable_path || raise "cannot resolve benchmark executable path"
          child = Process.new(
            executable,
            ["server", "--actors", @config.actors.to_s, "--stripes", @config.stripes.to_s],
            input: Process::Redirect::Pipe,
            output: Process::Redirect::Pipe,
            error: Process::Redirect::Inherit
          )

          ready_line = read_server_ready(child)
          ready = ServerReady.from_json(ready_line)

          client = new_system("actor-bench-process-client")
          client.enable_remoting("127.0.0.1", 0, @config.stripes)
          targets = ready.paths.map do |path|
            remote_ref = client.actor_for(path, Command).as(Movie::Remote::RemoteActorRef(Command))
            RemoteTarget.new(remote_ref)
          end
          run_workloads(targets, Topology::TwoProcess, capture_server_metrics: true)
        ensure
          client.try &.shutdown
          if child
            begin
              child.input.close
            rescue IO::Error
            end
            status = child.wait
            raise "benchmark server exited unsuccessfully: #{status}" unless status.success?
          end
        end

        private def run_workloads(targets : Array(T), topology : Topology, capture_server_metrics : Bool) : Array(Measurement) forall T
          results = [] of Measurement
          if @config.operation.in?(Operation::Tell, Operation::Both)
            @config.warmup_runs.times { measure_tell(targets, capture_server_metrics) }
            @config.measurement_runs.times do |index|
              result = measure_tell(targets, capture_server_metrics)
              results << measurement(topology, Operation::Tell, index + 1, result)
            end
          end
          if @config.operation.in?(Operation::Ask, Operation::Both)
            @config.warmup_runs.times { measure_ask(targets, capture_server_metrics) }
            @config.measurement_runs.times do |index|
              result = measure_ask(targets, capture_server_metrics)
              results << measurement(topology, Operation::Ask, index + 1, result)
            end
          end
          results
        end

        private def measure_tell(targets : Array(T), capture_server_metrics : Bool) : WorkloadResult forall T
          before_server = snapshot_process(targets)
          before_client = ProcessSnapshot.new(GC.stats.total_bytes, Statistics.process_cpu_seconds)
          started_at = Time.instant

          send_tells(targets)
          after_server = snapshot_process(targets)

          elapsed_nanoseconds = (Time.instant - started_at).total_nanoseconds.round.to_i64
          after_client = ProcessSnapshot.new(GC.stats.total_bytes, Statistics.process_cpu_seconds)
          processed = after_server[:processed] - before_server[:processed]
          raise "tell barrier observed #{processed} messages, expected #{@config.messages}" unless processed == @config.messages

          build_workload_result(
            elapsed_nanoseconds,
            before_client,
            after_client,
            before_server[:process],
            after_server[:process],
            capture_server_metrics,
            processed,
            [] of Int64
          )
        end

        private def measure_ask(targets : Array(T), capture_server_metrics : Bool) : WorkloadResult forall T
          before_server = snapshot_process(targets)
          before_client = ProcessSnapshot.new(GC.stats.total_bytes, Statistics.process_cpu_seconds)
          started_at = Time.instant

          latencies = send_asks(targets)

          elapsed_nanoseconds = (Time.instant - started_at).total_nanoseconds.round.to_i64
          after_client = ProcessSnapshot.new(GC.stats.total_bytes, Statistics.process_cpu_seconds)
          after_server = snapshot_process(targets)

          build_workload_result(
            elapsed_nanoseconds,
            before_client,
            after_client,
            before_server[:process],
            after_server[:process],
            capture_server_metrics,
            latencies.size.to_i64,
            latencies
          )
        end

        private def send_tells(targets : Array(T)) : Nil forall T
          completions = Channel(Exception?).new(@config.producers)
          payload = "x" * @config.payload_bytes
          base_count = @config.messages // @config.producers
          remainder = @config.messages % @config.producers

          @config.producers.times do |producer_index|
            count = base_count + (producer_index < remainder ? 1 : 0)
            start_sequence = producer_index.to_i64 * base_count + Math.min(producer_index, remainder)
            spawn do
              begin
                count.times do |offset|
                  sequence = start_sequence + offset
                  targets[(sequence % targets.size).to_i].tell(Command.tell(sequence, payload))
                end
                completions.send(nil)
              rescue ex
                completions.send(ex)
              end
            end
          end

          @config.producers.times do
            error = completions.receive
            raise error if error
          end
        end

        private def send_asks(targets : Array(T)) : Array(Int64) forall T
          worker_count = Math.min(@config.in_flight, @config.messages)
          completions = Channel(Tuple(Array(Int64), Exception?)).new(worker_count)
          payload = "x" * @config.payload_bytes
          base_count = @config.messages // worker_count
          remainder = @config.messages % worker_count

          worker_count.times do |worker_index|
            count = base_count + (worker_index < remainder ? 1 : 0)
            start_sequence = worker_index.to_i64 * base_count + Math.min(worker_index, remainder)
            spawn do
              samples = Array(Int64).new(count)
              error = nil.as(Exception?)
              begin
                count.times do |offset|
                  sequence = start_sequence + offset
                  started_at = Time.instant
                  response = targets[(sequence % targets.size).to_i].ask(Command.ask(sequence, payload))
                  samples << (Time.instant - started_at).total_nanoseconds.round.to_i64
                  raise "ask response sequence #{response.sequence} did not match #{sequence}" unless response.sequence == sequence
                end
              rescue ex
                error = ex
              ensure
                completions.send({samples, error})
              end
            end
          end

          latencies = Array(Int64).new(@config.messages)
          worker_count.times do
            samples, error = completions.receive
            raise error if error
            latencies.concat(samples)
          end
          raise "received #{latencies.size} ask responses, expected #{@config.messages}" unless latencies.size == @config.messages
          latencies
        end

        private def snapshot_process(targets : Array(T)) : NamedTuple(processed: Int64, process: ProcessSnapshot) forall T
          completions = Channel(Tuple(Snapshot?, Exception?)).new(targets.size)
          targets.each do |target|
            spawn do
              begin
                completions.send({target.snapshot, nil})
              rescue ex
                completions.send({nil, ex})
              end
            end
          end

          processed = 0_i64
          allocated_bytes = 0_u64
          cpu_seconds = 0.0
          targets.size.times do
            snapshot, error = completions.receive
            raise error if error
            current = snapshot.not_nil!
            processed += current.processed_tells
            allocated_bytes = Math.max(allocated_bytes, current.total_allocated_bytes)
            cpu_seconds = Math.max(cpu_seconds, current.cpu_seconds)
          end
          {processed: processed, process: ProcessSnapshot.new(allocated_bytes, cpu_seconds)}
        end

        private def build_workload_result(
          elapsed_nanoseconds : Int64,
          before_client : ProcessSnapshot,
          after_client : ProcessSnapshot,
          before_server : ProcessSnapshot,
          after_server : ProcessSnapshot,
          capture_server_metrics : Bool,
          processed_messages : Int64,
          latencies : Array(Int64),
        ) : WorkloadResult
          server_allocated = if capture_server_metrics
                               (after_server.allocated_bytes - before_server.allocated_bytes).to_i64
                             end
          server_cpu = if capture_server_metrics
                         after_server.cpu_seconds - before_server.cpu_seconds
                       end

          WorkloadResult.new(
            elapsed_nanoseconds: elapsed_nanoseconds,
            client_allocated_bytes: (after_client.allocated_bytes - before_client.allocated_bytes).to_i64,
            client_cpu_seconds: after_client.cpu_seconds - before_client.cpu_seconds,
            server_allocated_bytes: server_allocated,
            server_cpu_seconds: server_cpu,
            processed_messages: processed_messages,
            latencies: latencies
          )
        end

        private def measurement(topology : Topology, operation : Operation, run : Int32, result : WorkloadResult) : Measurement
          elapsed_seconds = result.elapsed_nanoseconds / 1_000_000_000.0
          count = result.processed_messages
          latencies = result.latencies
          Measurement.new(
            timestamp_utc: Time.utc.to_rfc3339,
            git_sha: @git_sha,
            crystal_version: Crystal::VERSION,
            release_build: {{ flag?(:release) }},
            cpu_count: System.cpu_count,
            crystal_workers: ENV["CRYSTAL_WORKERS"]? || "default",
            topology: topology.label,
            operation: operation.label,
            messages: @config.messages,
            payload_bytes: @config.payload_bytes,
            producers: @config.producers,
            actors: @config.actors,
            in_flight: @config.in_flight,
            stripes: @config.stripes,
            run: run,
            elapsed_nanoseconds: result.elapsed_nanoseconds,
            processed_messages: count,
            messages_per_second: count / elapsed_seconds,
            nanoseconds_per_message: result.elapsed_nanoseconds / count.to_f,
            client_allocated_bytes: result.client_allocated_bytes,
            server_allocated_bytes: result.server_allocated_bytes,
            bytes_per_message: result.client_allocated_bytes / count.to_f,
            server_bytes_per_message: result.server_allocated_bytes.try { |bytes| bytes / count.to_f },
            client_cpu_seconds: result.client_cpu_seconds,
            server_cpu_seconds: result.server_cpu_seconds,
            p50_nanoseconds: latencies.empty? ? nil : Statistics.percentile(latencies, 0.50),
            p95_nanoseconds: latencies.empty? ? nil : Statistics.percentile(latencies, 0.95),
            p99_nanoseconds: latencies.empty? ? nil : Statistics.percentile(latencies, 0.99),
            max_nanoseconds: latencies.max?
          )
        end

        private def new_system(name : String) : Movie::ActorSystem(Nil)
          Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: name)
        end

        private def spawn_actors(system : Movie::ActorSystem(Nil)) : Array(Movie::ActorRef(Command))
          Array(Movie::ActorRef(Command)).new(@config.actors) do |index|
            system.spawn(TargetBehavior.new, name: "target-#{index}")
          end
        end

        private def spawn_local_targets(system : Movie::ActorSystem(Nil)) : Array(LocalTarget)
          spawn_actors(system).map { |ref| LocalTarget.new(ref) }
        end

        private def read_server_ready(child : Process) : String
          lines = Channel(String?).new(1)
          spawn { lines.send(child.output.gets) }
          select
          when line = lines.receive
            line || raise "benchmark server closed stdout before becoming ready"
          when timeout(10.seconds)
            child.terminate
            raise "benchmark server did not become ready within 10 seconds"
          end
        end
      end

      record ServerReady, port : Int32, paths : Array(String) do
        include JSON::Serializable
      end

      module ServerProcess
        extend self

        def run(config : Config) : Nil
          Movie::Benchmarks::ActorSystem.register_messages
          system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "actor-bench-process-server")
          refs = Array(Movie::ActorRef(Command)).new(config.actors) do |index|
            system.spawn(TargetBehavior.new, name: "target-#{index}")
          end
          remote = system.enable_remoting("127.0.0.1", 0, config.stripes)
          ready = ServerReady.new(remote.local_port, refs.map { |ref| ref.path.not_nil!.to_s })
          STDOUT.puts(ready.to_json)
          STDOUT.flush
          STDIN.gets
        ensure
          system.try &.shutdown
        end
      end

      module Metadata
        extend self

        def git_sha : String
          sha = ENV["MOVIE_BENCH_GIT_SHA"]?
          return sha if sha && !sha.empty?
          output = IO::Memory.new
          status = Process.run("git", ["rev-parse", "--short", "HEAD"], output: output, error: Process::Redirect::Close)
          status.success? ? output.to_s.strip : "unknown"
        rescue
          "unknown"
        end
      end

      module Reporter
        extend self

        def emit(results : Array(Measurement), format : OutputFormat, io : IO = STDOUT) : Nil
          case format
          when OutputFormat::Human
            emit_human(results, io)
          when OutputFormat::Csv
            CSV.build(io) do |csv|
              csv.row Measurement::CSV_HEADERS
              results.each { |measurement| csv.row measurement.csv_values }
            end
          when OutputFormat::JsonLines
            results.each { |measurement| io.puts(measurement.to_json) }
          else
            raise "unreachable output format"
          end
        end

        private def emit_human(results : Array(Measurement), io : IO)
          io.puts "ActorSystem end-to-end benchmark"
          results.each do |measurement|
            summary = "#{measurement.topology} #{measurement.operation} run=#{measurement.run}: " \
                      "#{measurement.messages_per_second.round(0)} msg/s, " \
                      "#{measurement.nanoseconds_per_message.round(0)} ns/msg, " \
                      "#{measurement.bytes_per_message.round(1)} client B/msg"
            if p99 = measurement.p99_nanoseconds
              summary += ", p99=#{(p99 / 1_000.0).round(1)} us"
            end
            if server_bytes = measurement.server_bytes_per_message
              summary += ", #{server_bytes.round(1)} server B/msg"
            end
            io.puts summary
          end

          results.group_by { |measurement| {measurement.topology, measurement.operation} }.each do |key, group|
            median = Statistics.median(group.map(&.messages_per_second))
            io.puts "median #{key[0]} #{key[1]}: #{median.round(0)} msg/s across #{group.size} runs"
          end
        end
      end

      module CLI
        extend self

        def run(arguments : Array(String)) : Int32
          Log.setup_from_env(default_level: :warn, backend: Log::IOBackend.new(STDERR))
          args = arguments.dup
          command = args.first? == "server" ? args.shift : nil
          config = Config.parse(args)

          if command == "server"
            ServerProcess.run(config)
          else
            Reporter.emit(Runner.new(config).run, config.output_format)
          end
          0
        rescue ex
          STDERR.puts "actor-system-benchmark: #{ex.message}"
          1
        end
      end
    end
  end
end
