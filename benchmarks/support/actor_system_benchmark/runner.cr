require "./types"
require "process/executable_path"

module Movie
  module Benchmarks
    module ActorSystem
      class TargetBehavior < Movie::AbstractBehavior(Command)
        @processed_tells = 0_i64

        def receive(message : Command, context : Movie::ActorContext(Command))
          case message.kind
          when CommandKind::Tell
            @processed_tells += 1
          when CommandKind::Ask
            Movie::Ask.reply_if_asked(context.sender, AskReply.new(message.sequence))
          when CommandKind::Snapshot
            Movie::Ask.reply_if_asked(context.sender, Snapshot.new(
              processed_tells: @processed_tells,
              total_allocated_bytes: GC.stats.total_bytes,
              cpu_seconds: Statistics.process_cpu_seconds
            ))
          end
          Movie::Behaviors(Command).same
        end
      end

      struct Target(R)
        def initialize(@ref : R)
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

      alias LocalTarget = Target(Movie::ActorRef(Command))
      alias RemoteTarget = Target(Movie::Remote::RemoteActorRef(Command))

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
            cpu_count: System.cpu_count.to_i32,
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
          JsonOutput.write_line(ready, STDOUT)
          STDOUT.flush
          STDIN.gets
        ensure
          system.try &.shutdown
        end
      end
    end
  end
end
