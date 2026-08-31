require "../spec_helper"
require "../../benchmarks/support/persistence_benchmark"

describe Movie::Benchmarks::Persistence::Config do
  it "parses bounded load, soak, fault, backend, and output options" do
    config = Movie::Benchmarks::Persistence::Config.parse([
      "--backend", "postgres",
      "--connection-uri", "postgres://database/movie",
      "--operations", "250",
      "--concurrency", "4",
      "--payload-bytes", "128",
      "--duration-seconds", "2",
      "--fault-every", "25",
      "--format", "jsonl",
    ])

    config.backend.should eq(Movie::Benchmarks::Persistence::BackendKind::Postgres)
    config.connection_uri.should eq("postgres://database/movie")
    config.operations.should eq(250)
    config.concurrency.should eq(4)
    config.payload_bytes.should eq(128)
    config.duration.should eq(2.seconds)
    config.fault_every.should eq(25)
    config.output_format.should eq(Movie::Benchmarks::Persistence::OutputFormat::JsonLines)
  end

  it "rejects unbounded or invalid inputs" do
    expect_raises(ArgumentError) do
      Movie::Benchmarks::Persistence::Config.parse(["--concurrency", "0"])
    end
    expect_raises(ArgumentError) do
      Movie::Benchmarks::Persistence::Config.parse(["--fault-every", "1"])
    end
  end
end

if ENV["MOVIE_BENCH"]? == "1"
  describe Movie::Benchmarks::Persistence::Runner do
    it "runs a deterministic SQLite load/fault smoke scenario and writes JSON directly" do
      path = "/tmp/movie_persistence_bench_spec_#{UUID.random}.sqlite3"
      config = Movie::Benchmarks::Persistence::Config.new(
        sqlite_path: path,
        operations: 40,
        concurrency: 2,
        payload_bytes: 16,
        fault_every: 5,
        output_format: Movie::Benchmarks::Persistence::OutputFormat::JsonLines
      )

      measurement = Movie::Benchmarks::Persistence::Runner.new(config).run
      measurement.operations.should eq(40_i64)
      measurement.errors.should eq(0_i64)
      measurement.retries.should be > 0_i64
      measurement.reconnects.should eq(measurement.retries)
      measurement.operations_per_second.should be > 0.0
      measurement.p99_nanoseconds.should be >= measurement.p50_nanoseconds

      output = IO::Memory.new
      Movie::Benchmarks::Persistence::Reporter.emit(measurement, config.output_format, output)
      parsed = JSON.parse(output.to_s)
      parsed["backend"].as_s.should eq("sqlite")
      parsed["operations"].as_i64.should eq(40_i64)
      parsed["errors"].as_i64.should eq(0_i64)
      parsed["retries"].as_i64.should eq(measurement.retries)
    ensure
      File.delete(path) if path && File.exists?(path)
    end
  end
end
