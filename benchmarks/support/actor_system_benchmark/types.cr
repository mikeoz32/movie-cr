require "../../../src/movie"
require "../../../src/movie/remote"
require "json"
require "option_parser"

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

      enum CommandKind
        Tell
        Ask
        Snapshot
      end

      record Command, kind : CommandKind, sequence : Int64, payload : String do
        include JSON::Serializable

        def self.tell(sequence : Int64, payload : String) : self
          new(CommandKind::Tell, sequence, payload)
        end

        def self.ask(sequence : Int64, payload : String) : self
          new(CommandKind::Ask, sequence, payload)
        end

        def self.snapshot : self
          new(CommandKind::Snapshot, 0_i64, "")
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

      record ProcessSnapshot, allocated_bytes : UInt64, cpu_seconds : Float64
      record WorkloadResult,
        elapsed_nanoseconds : Int64,
        client_allocated_bytes : Int64,
        client_cpu_seconds : Float64,
        server_allocated_bytes : Int64?,
        server_cpu_seconds : Float64?,
        processed_messages : Int64,
        latencies : Array(Int64)

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
    end
  end
end
