require "../spec_helper"
require "../../benchmarks/support/actor_system_benchmark"

alias ActorBench = Movie::Benchmarks::ActorSystem

describe ActorBench::Config do
  it "parses a reproducible benchmark matrix" do
    config = ActorBench::Config.parse([
      "--topology", "in-process",
      "--operation", "both",
      "--messages", "120",
      "--payload-bytes", "64",
      "--producers", "3",
      "--actors", "4",
      "--in-flight", "6",
      "--stripes", "2",
      "--warmup", "1",
      "--runs", "3",
      "--format", "jsonl",
    ])

    config.topology.should eq(ActorBench::Topology::InProcess)
    config.operation.should eq(ActorBench::Operation::Both)
    config.messages.should eq(120)
    config.payload_bytes.should eq(64)
    config.producers.should eq(3)
    config.actors.should eq(4)
    config.in_flight.should eq(6)
    config.stripes.should eq(2)
    config.warmup_runs.should eq(1)
    config.measurement_runs.should eq(3)
    config.output_format.should eq(ActorBench::OutputFormat::JsonLines)
  end

  it "rejects non-positive workload dimensions" do
    expect_raises(ArgumentError, /messages must be greater than zero/) do
      ActorBench::Config.parse(["--messages", "0"])
    end
  end
end

describe ActorBench::Statistics do
  it "calculates nearest-rank latency percentiles" do
    samples = [10_i64, 20_i64, 30_i64, 40_i64, 50_i64]

    ActorBench::Statistics.percentile(samples, 0.50).should eq(30_i64)
    ActorBench::Statistics.percentile(samples, 0.95).should eq(50_i64)
    ActorBench::Statistics.percentile(samples, 0.99).should eq(50_i64)
  end
end

describe ActorBench::Runner do
  it "measures local tell processing through an actor-side barrier" do
    config = ActorBench::Config.new(
      topology: ActorBench::Topology::Local,
      operation: ActorBench::Operation::Tell,
      messages: 100,
      payload_bytes: 8,
      producers: 2,
      actors: 2,
      in_flight: 2,
      stripes: 1,
      warmup_runs: 0,
      measurement_runs: 1,
      output_format: ActorBench::OutputFormat::JsonLines
    )

    results = ActorBench::Runner.new(config).run

    results.size.should eq(1)
    measurement = results.first
    measurement.topology.should eq("local")
    measurement.operation.should eq("tell")
    measurement.processed_messages.should eq(100)
    measurement.messages_per_second.should be > 0
    measurement.bytes_per_message.should be >= 0
  end

  it "measures in-process remote asks end to end" do
    config = ActorBench::Config.new(
      topology: ActorBench::Topology::InProcess,
      operation: ActorBench::Operation::Ask,
      messages: 20,
      payload_bytes: 8,
      producers: 1,
      actors: 2,
      in_flight: 2,
      stripes: 2,
      warmup_runs: 0,
      measurement_runs: 1,
      output_format: ActorBench::OutputFormat::JsonLines
    )

    results = ActorBench::Runner.new(config).run

    results.size.should eq(1)
    measurement = results.first
    measurement.topology.should eq("in-process")
    measurement.operation.should eq("ask")
    measurement.processed_messages.should eq(20)
    measurement.p50_nanoseconds.should_not be_nil
    measurement.p99_nanoseconds.should_not be_nil
  end
end
