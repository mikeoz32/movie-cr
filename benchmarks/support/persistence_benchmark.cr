require "../../src/movie"
require "../../src/movie/persistence/postgres"
require "json"
require "option_parser"

module Movie
  module Benchmarks
    module Persistence
      enum BackendKind
        SQLite
        Postgres

        def self.from_label(value : String) : self
          case value
          when "sqlite"   then SQLite
          when "postgres" then Postgres
          else                 raise ArgumentError.new("unknown persistence backend: #{value}")
          end
        end

        def label : String
          self == SQLite ? "sqlite" : "postgres"
        end
      end

      enum OutputFormat
        Human
        JsonLines

        def self.from_label(value : String) : self
          case value
          when "human" then Human
          when "jsonl" then JsonLines
          else              raise ArgumentError.new("unknown output format: #{value}")
          end
        end
      end

      class Config
        getter backend : BackendKind
        getter connection_uri : String
        getter sqlite_path : String
        getter operations : Int32
        getter concurrency : Int32
        getter payload_bytes : Int32
        getter duration : Time::Span
        getter fault_every : Int32
        getter output_format : OutputFormat

        def initialize(
          @backend : BackendKind = BackendKind::SQLite,
          @connection_uri : String = "",
          @sqlite_path : String = "/tmp/movie_persistence_benchmark.sqlite3",
          @operations : Int32 = 10_000,
          @concurrency : Int32 = 1,
          @payload_bytes : Int32 = 64,
          @duration : Time::Span = Time::Span.zero,
          @fault_every : Int32 = 0,
          @output_format : OutputFormat = OutputFormat::Human,
        )
          raise ArgumentError.new("operations must be positive") unless @operations > 0
          raise ArgumentError.new("concurrency must be positive") unless @concurrency > 0
          raise ArgumentError.new("payload bytes cannot be negative") if @payload_bytes < 0
          raise ArgumentError.new("duration cannot be negative") if @duration < Time::Span.zero
          if @fault_every == 1 || @fault_every < 0
            raise ArgumentError.new("fault-every must be zero or at least two")
          end
          if @backend.postgres? && @connection_uri.empty?
            raise ArgumentError.new("connection-uri is required for PostgreSQL")
          end
        end

        def self.parse(arguments : Array(String)) : self
          backend = BackendKind::SQLite
          connection_uri = ENV["MOVIE_POSTGRES_TEST_URL"]? || ""
          sqlite_path = "/tmp/movie_persistence_benchmark.sqlite3"
          operations = 10_000
          concurrency = 1
          payload_bytes = 64
          duration = Time::Span.zero
          fault_every = 0
          output_format = OutputFormat::Human

          parser = OptionParser.new do |options|
            options.banner = "Usage: persistence-benchmark [options]"
            options.on("--backend NAME", "sqlite or postgres") { |value| backend = BackendKind.from_label(value) }
            options.on("--connection-uri URI", "PostgreSQL connection URI") { |value| connection_uri = value }
            options.on("--sqlite-path PATH", "SQLite database path") { |value| sqlite_path = value }
            options.on("--operations COUNT", "attempted writes for load mode") { |value| operations = value.to_i }
            options.on("--concurrency COUNT", "parallel persistence streams") { |value| concurrency = value.to_i }
            options.on("--payload-bytes COUNT", "event payload size") { |value| payload_bytes = value.to_i }
            options.on("--duration-seconds COUNT", "soak duration; zero selects load mode") do |value|
              duration = value.to_i.seconds
            end
            options.on("--fault-every COUNT", "inject one connection loss every N attempts") do |value|
              fault_every = value.to_i
            end
            options.on("--format NAME", "human or jsonl") { |value| output_format = OutputFormat.from_label(value) }
            options.on("-h", "--help", "show this help") do
              puts options
              exit
            end
          end
          parser.parse(arguments.dup)

          new(
            backend: backend,
            connection_uri: connection_uri,
            sqlite_path: sqlite_path,
            operations: operations,
            concurrency: concurrency,
            payload_bytes: payload_bytes,
            duration: duration,
            fault_every: fault_every,
            output_format: output_format
          )
        rescue error : ArgumentError
          raise error
        rescue error
          raise ArgumentError.new(error.message || "invalid persistence benchmark arguments")
        end
      end

      record Measurement,
        backend : String,
        mode : String,
        concurrency : Int32,
        payload_bytes : Int32,
        operations : Int64,
        errors : Int64,
        elapsed_seconds : Float64,
        operations_per_second : Float64,
        p50_nanoseconds : Int64,
        p99_nanoseconds : Int64,
        retries : Int64,
        reconnects : Int64,
        circuit_opens : Int64

      private record WorkerResult, operations : Int64, errors : Int64, latencies : Array(Int64)

      class InjectedConnectionLost < Exception
      end

      class FaultProbe
        def initialize(@every : Int32)
          @attempts = Atomic(Int64).new(0_i64)
        end

        def fail? : Bool
          return false if @every == 0
          attempt = @attempts.add(1_i64) + 1_i64
          attempt % @every == 0_i64
        end
      end

      class FaultSQLiteConnection < Movie::Persistence::SQLiteBackendConnection
        def initialize(connection : DB::Connection, @probe : FaultProbe)
          super(connection)
        end

        def append_events(message : Movie::Persistence::AppendEvents) : Movie::Persistence::WriteResult
          raise InjectedConnectionLost.new("deterministic benchmark fault") if @probe.fail?
          super
        end

        def connection_lost?(error : Exception) : Bool
          error.is_a?(InjectedConnectionLost) || super
        end
      end

      class FaultSQLiteBackend < Movie::Persistence::Backend
        def initialize(@uri : String, @probe : FaultProbe)
        end

        def name : String
          "sqlite"
        end

        def connect : Movie::Persistence::BackendConnection
          connection = DB.connect(@uri)
          connection.exec("PRAGMA busy_timeout = 5000")
          FaultSQLiteConnection.new(connection, @probe)
        end
      end

      class FaultPostgresConnection < Movie::Persistence::PostgresBackendConnection
        def initialize(connection : DB::Connection, @probe : FaultProbe)
          super(connection)
        end

        def append_events(message : Movie::Persistence::AppendEvents) : Movie::Persistence::WriteResult
          raise InjectedConnectionLost.new("deterministic benchmark fault") if @probe.fail?
          super
        end

        def connection_lost?(error : Exception) : Bool
          error.is_a?(InjectedConnectionLost) || super
        end
      end

      class FaultPostgresBackend < Movie::Persistence::Backend
        def initialize(@uri : String, @probe : FaultProbe)
        end

        def name : String
          "postgres"
        end

        def connect : Movie::Persistence::BackendConnection
          FaultPostgresConnection.new(DB.connect(@uri), @probe)
        end
      end

      class Runner
        def initialize(@config : Config)
        end

        def run : Measurement
          telemetry = Movie::Persistence::Telemetry.new
          backend = build_backend
          setup = backend.connect
          setup.ensure_schema
          setup.close
          policy = Movie::Persistence::ResiliencePolicy.new(
            max_retries: 3,
            min_backoff: 1.millisecond,
            max_backoff: 10.milliseconds,
            circuit_failure_threshold: 100,
            circuit_reset_timeout: 1.second
          )
          workers = Array(Movie::Persistence::ConnectionWorker).new(@config.concurrency) do |index|
            Movie::Persistence::ConnectionWorker.new(
              backend,
              "persistence-benchmark-#{index}",
              256,
              telemetry,
              policy
            )
          end
          results = Channel(WorkerResult).new(@config.concurrency)
          prefix = "persistence-benchmark-#{UUID.random}"
          payload = "x" * @config.payload_bytes
          deadline = Time.instant + @config.duration
          started = Time.instant

          workers.each_with_index do |worker, index|
            spawn do
              results.send(run_worker(worker, index, prefix, payload, deadline))
            end
          end

          operations = 0_i64
          errors = 0_i64
          latencies = [] of Int64
          @config.concurrency.times do
            result = results.receive
            operations += result.operations
            errors += result.errors
            latencies.concat(result.latencies)
          end
          elapsed = Time.instant - started
          metrics = telemetry.metrics
          ordered = latencies.sort
          elapsed_seconds = elapsed.total_seconds
          Measurement.new(
            @config.backend.label,
            @config.duration > Time::Span.zero ? "soak" : "load",
            @config.concurrency,
            @config.payload_bytes,
            operations,
            errors,
            elapsed_seconds,
            elapsed_seconds > 0.0 ? operations / elapsed_seconds : 0.0,
            percentile(ordered, 0.50),
            percentile(ordered, 0.99),
            metrics.retries,
            metrics.reconnects,
            metrics.circuit_opens
          )
        ensure
          workers.try &.each(&.close)
        end

        private def run_worker(
          worker : Movie::Persistence::ConnectionWorker,
          index : Int32,
          prefix : String,
          payload : String,
          deadline : Time::Instant,
        ) : WorkerResult
          operations = 0_i64
          errors = 0_i64
          attempts = 0_i64
          revision = 0_i64
          latencies = [] of Int64
          target = operations_for_worker(index)
          while @config.duration > Time::Span.zero ? Time.instant < deadline : attempts < target
            attempts += 1_i64
            started = Time.instant
            begin
              result = worker.execute(retryable: true) do |connection|
                connection.append_events(
                  Movie::Persistence::AppendEvents.new(
                    "#{prefix}-#{index}",
                    revision,
                    Movie::Persistence::OperationId.new("#{index}-#{attempts}"),
                    [Movie::Persistence::SerializedEvent.new("BenchmarkEvent", payload)]
                  )
                )
              end
              revision = result.revision
              operations += 1_i64
              latencies << (Time.instant - started).total_nanoseconds.to_i64
            rescue
              errors += 1_i64
            end
          end
          WorkerResult.new(operations, errors, latencies)
        end

        private def operations_for_worker(index : Int32) : Int64
          quotient = @config.operations // @config.concurrency
          remainder = @config.operations % @config.concurrency
          (quotient + (index < remainder ? 1 : 0)).to_i64
        end

        private def build_backend : Movie::Persistence::Backend
          probe = FaultProbe.new(@config.fault_every)
          case @config.backend
          when BackendKind::SQLite
            FaultSQLiteBackend.new("sqlite3:#{@config.sqlite_path}", probe)
          when BackendKind::Postgres
            FaultPostgresBackend.new(@config.connection_uri, probe)
          else
            raise "unreachable persistence backend"
          end
        end

        private def percentile(ordered : Array(Int64), quantile : Float64) : Int64
          return 0_i64 if ordered.empty?
          rank = (ordered.size * quantile).ceil.to_i - 1
          ordered[rank.clamp(0, ordered.size - 1)]
        end
      end

      module Reporter
        extend self

        def emit(measurement : Measurement, format : OutputFormat, io : IO = STDOUT) : Nil
          case format
          when OutputFormat::Human
            io.puts "Persistence #{measurement.backend} #{measurement.mode}: " +
                    "#{measurement.operations_per_second.round(0)} ops/s, " +
                    "p50=#{(measurement.p50_nanoseconds / 1_000.0).round(1)} us, " +
                    "p99=#{(measurement.p99_nanoseconds / 1_000.0).round(1)} us, " +
                    "errors=#{measurement.errors}, retries=#{measurement.retries}"
          when OutputFormat::JsonLines
            write_json(measurement, io)
            io << '\n'
          else
            raise "unreachable output format"
          end
        end

        private def write_json(measurement : Measurement, io : IO)
          JSON.build(io) do |json|
            json.object do
              json.field "backend", measurement.backend
              json.field "mode", measurement.mode
              json.field "concurrency", measurement.concurrency
              json.field "payload_bytes", measurement.payload_bytes
              json.field "operations", measurement.operations
              json.field "errors", measurement.errors
              json.field "elapsed_seconds", measurement.elapsed_seconds
              json.field "operations_per_second", measurement.operations_per_second
              json.field "p50_nanoseconds", measurement.p50_nanoseconds
              json.field "p99_nanoseconds", measurement.p99_nanoseconds
              json.field "retries", measurement.retries
              json.field "reconnects", measurement.reconnects
              json.field "circuit_opens", measurement.circuit_opens
            end
          end
        end
      end

      module CLI
        extend self

        def run(arguments : Array(String)) : Int32
          config = Config.parse(arguments)
          Reporter.emit(Runner.new(config).run, config.output_format)
          0
        rescue error
          STDERR.puts "persistence-benchmark: #{error.message}"
          1
        end
      end
    end
  end
end
