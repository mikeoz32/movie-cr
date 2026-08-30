require "../spec_helper"
require "../../src/movie"

alias BlueprintStreams = Movie::Streams::Typed

private BLUEPRINT_STREAM_SYSTEM = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
Spec.after_suite { BLUEPRINT_STREAM_SYSTEM.shutdown }

describe "typed stream blueprints" do
  it "changes element types and combines source and sink materialized values" do
    system = BLUEPRINT_STREAM_SYSTEM
    source = BlueprintStreams::Sources.manual(Int32)
    stringify = BlueprintStreams::Flows.map(Int32, String) { |value| "value=#{value}" }
    length = BlueprintStreams::Flows.map(String, Int32, &.size)
    sink = BlueprintStreams::Sinks.collect(Int32)

    graph = source
      .via(stringify)
      .via(length)
      .to_mat(sink) { |source_control, result| {source_control, result} }

    control, result = graph.run(system)
    control << 7
    control << 42
    control.complete

    result.await(1.second).should eq([7, 8])
  end

  it "re-materializes one immutable blueprint into independent runtimes" do
    system = BLUEPRINT_STREAM_SYSTEM
    graph = BlueprintStreams::Sources.manual(Int32)
      .to_mat(BlueprintStreams::Sinks.fold(Int32, 0) { |sum, value| sum + value }) do |control, result|
        {control, result}
      end

    first_control, first_result = graph.run(system)
    second_control, second_result = graph.run(system)

    first_control << 1
    first_control << 2
    first_control.complete
    second_control << 10
    second_control.complete

    first_result.await(1.second).should eq(3)
    second_result.await(1.second).should eq(10)
  end

  it "composes reusable type-changing flows before attaching a source" do
    system = BLUEPRINT_STREAM_SYSTEM
    stringify = BlueprintStreams::Flows.map(Int32, String) { |value| "n=#{value}" }
    length = BlueprintStreams::Flows.map(String, Int32, &.size)
    composed = stringify.via(length)
    graph = BlueprintStreams::Sources.manual(Int32)
      .via(composed)
      .to_mat(BlueprintStreams::Sinks.collect(Int32)) { |control, result| {control, result} }

    control, result = graph.run(system)
    control << 10
    control.complete

    result.await(1.second).should eq([4])
  end

  it "combines flow materialized values and supports the sink-only to shortcut" do
    system = BLUEPRINT_STREAM_SYSTEM
    flow = BlueprintStreams::Flows.map(Int32, String) { |value| value.to_s }
    source = BlueprintStreams::Sources.manual(Int32)
      .via_mat(flow) { |control, _flow_mat| {control, :mapped} }
    graph = source.to_mat(BlueprintStreams::Sinks.collect(String)) do |source_mat, result|
      control, label = source_mat
      {control, label, result}
    end

    control, label, result = graph.run(system)
    control << 5
    control.complete
    label.should eq(:mapped)
    result.await(1.second).should eq(["5"])

    automatic = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |runtime_system|
      outlet = BlueprintStreams::StageChannel(Int32).new(runtime_system)
      BlueprintStreams::BlueprintRuntime.execute(runtime_system) do
        outlet.push(9)
        outlet.complete
      end
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end

    automatic.to(BlueprintStreams::Sinks.collect(Int32)).run(system).await(1.second).should eq([9])
  end

  it "combines materialized values while composing reusable flows" do
    system = BLUEPRINT_STREAM_SYSTEM
    stringify = BlueprintStreams::Flows.map(Int32, String) { |value| value.to_s }
    length = BlueprintStreams::Flows.map(String, Int32, &.size)
    composed = stringify.via_mat(length) { |_first, _second| :composed }
    source = BlueprintStreams::Sources.manual(Int32)
      .via_mat(composed) { |control, label| {control, label} }
    graph = source.to_mat(BlueprintStreams::Sinks.collect(Int32)) do |source_mat, result|
      control, label = source_mat
      {control, label, result}
    end

    control, label, result = graph.run(system)
    control << 123
    control.complete

    label.should eq(:composed)
    result.await(1.second).should eq([3])
  end

  it "cancels upstream promptly when a transforming stage fails" do
    system = BLUEPRINT_STREAM_SYSTEM
    failing = BlueprintStreams::Flows.map(Int32, String) do |_value|
      raise "transform failed"
    end
    graph = BlueprintStreams::Sources.manual(Int32)
      .via(failing)
      .to_mat(BlueprintStreams::Sinks.collect(String)) { |control, result| {control, result} }

    control, result = graph.run(system)
    control << 1
    expect_raises(Exception, "transform failed") { result.await(1.second) }

    completion = Channel(Exception?).new(1)
    spawn do
      begin
        control.complete
        completion.send(nil)
      rescue ex
        completion.send(ex)
      end
    end

    completion_error = select
    when error = completion.receive
      error
    when timeout(100.milliseconds)
      fail("manual source remained blocked after downstream failure")
    end
    completion_error.should be_nil
  end

  it "propagates a StreamCancelledError raised by user transformation code" do
    system = BLUEPRINT_STREAM_SYSTEM
    failing = BlueprintStreams::Flows.map(Int32, String) do |_value|
      raise BlueprintStreams::StreamCancelledError.new("user transform failed")
    end
    graph = BlueprintStreams::Sources.manual(Int32)
      .via(failing)
      .to_mat(BlueprintStreams::Sinks.collect(String)) { |control, result| {control, result} }

    control, result = graph.run(system)
    control << 1

    expect_raises(BlueprintStreams::StreamCancelledError, "user transform failed") do
      result.await(1.second)
    end
  end

  it "cancels an unfinished graph when its actor system shuts down" do
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
    graph = BlueprintStreams::Sources.manual(Int32)
      .to_mat(BlueprintStreams::Sinks.collect(Int32)) { |control, result| {control, result} }
    control, result = graph.run(system)

    system.shutdown

    expect_raises(Movie::FutureCancelled) { result.await(1.second) }
    expect_raises(BlueprintStreams::StreamClosedError) { control << 1 }
  ensure
    system.try &.shutdown
  end
end
