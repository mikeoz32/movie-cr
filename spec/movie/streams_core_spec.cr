require "../spec_helper"
require "../../src/movie"

alias BlueprintStreams = Movie::Streams::Typed

private BLUEPRINT_STREAM_SYSTEM = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
Spec.after_suite { BLUEPRINT_STREAM_SYSTEM.shutdown }

private def blueprint_eventually(timeout_span : Time::Span = 1.second, &condition : -> Bool)
  deadline = Time.instant + timeout_span
  until condition.call
    fail("condition was not satisfied within #{timeout_span}") if Time.instant >= deadline
    sleep 1.millisecond
  end
end

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

  it "propagates an idle downstream sink failure to the manual source" do
    transformed = Atomic(Int32).new(0)
    flow = BlueprintStreams::Flows.map(Int32, Int32) do |value|
      transformed.add(1)
      value
    end
    sink = BlueprintStreams::Sinks.fold(Int32, 0) do |_sum, _value|
      raise "fold failed"
    end
    graph = BlueprintStreams::Sources.manual(Int32)
      .via(flow)
      .to_mat(sink) { |control, result| {control, result} }
    control, result = graph.run(BLUEPRINT_STREAM_SYSTEM)

    control << 1
    expect_raises(Exception, "fold failed") { result.await(1.second) }

    blueprint_eventually(100.milliseconds) { control.terminal? }
    control.offer(2).status.should eq(BlueprintStreams::QueueOfferStatus::QueueClosed)
    transformed.get.should eq(1)
  end

  it "does not dequeue buffered input after downstream cancellation" do
    inlet = BlueprintStreams::StageChannel(Int32).new(BLUEPRINT_STREAM_SYSTEM, capacity: 1)
    downstream = BlueprintStreams::StageChannel(String).new(BLUEPRINT_STREAM_SYSTEM, capacity: 1)
    inlet.offer(7)
    downstream.cancel

    expect_raises(BlueprintStreams::StreamCancelledError, "downstream cancelled the stream") do
      inlet.receive(until_cancelled_by: downstream)
    end
  ensure
    inlet.try &.cancel
    downstream.try &.cancel
  end

  it "rolls back source and flow edges when a flow materializer raises" do
    edges = [] of BlueprintStreams::StageChannel(Int32)
    created = Channel(BlueprintStreams::StageChannel(Int32)).new(2)
    source = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |system|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    failing_flow = BlueprintStreams::Flow(Int32, Int32, BlueprintStreams::NotUsed).new do |system, _inlet|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      raise "flow materialization failed"
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    graph = source.via(failing_flow).to(BlueprintStreams::Sinks.collect(Int32))

    expect_raises(Exception, "flow materialization failed") { graph.run(BLUEPRINT_STREAM_SYSTEM) }
    edges = [created.receive, created.receive]
    edges.map(&.cancelled?).should eq([true, true])
  ensure
    edges.try { |items| items.each { |edge| edge.cancel } }
  end

  it "rolls back source and sink edges when a sink materializer raises" do
    edges = [] of BlueprintStreams::StageChannel(Int32)
    created = Channel(BlueprintStreams::StageChannel(Int32)).new(2)
    source = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |system|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    failing_sink = BlueprintStreams::Sink(Int32, BlueprintStreams::NotUsed).new do |system, _inlet|
      created.send(BlueprintStreams::StageChannel(Int32).new(system))
      raise "sink materialization failed"
      BlueprintStreams::NotUsed::INSTANCE
    end

    expect_raises(Exception, "sink materialization failed") do
      source.to(failing_sink).run(BLUEPRINT_STREAM_SYSTEM)
    end
    edges = [created.receive, created.receive]
    edges.map(&.cancelled?).should eq([true, true])
  ensure
    edges.try { |items| items.each { |edge| edge.cancel } }
  end

  it "rolls back all composed edges when materialized-value combination raises" do
    edges = [] of BlueprintStreams::StageChannel(Int32)
    created = Channel(BlueprintStreams::StageChannel(Int32)).new(2)
    source = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |system|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    flow = BlueprintStreams::Flow(Int32, Int32, BlueprintStreams::NotUsed).new do |system, _inlet|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    combined = source.via_mat(flow) do |_source_mat, _flow_mat|
      raise "materialized combination failed"
      BlueprintStreams::NotUsed::INSTANCE
    end

    expect_raises(Exception, "materialized combination failed") do
      combined.to(BlueprintStreams::Sinks.collect(Int32)).run(BLUEPRINT_STREAM_SYSTEM)
    end
    edges = [created.receive, created.receive]
    edges.map(&.cancelled?).should eq([true, true])
  ensure
    edges.try { |items| items.each { |edge| edge.cancel } }
  end

  it "rolls back nested flow edges when Flow via_mat combination raises" do
    edges = [] of BlueprintStreams::StageChannel(Int32)
    created = Channel(BlueprintStreams::StageChannel(Int32)).new(3)
    source = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |system|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    first = BlueprintStreams::Flow(Int32, Int32, BlueprintStreams::NotUsed).new do |system, _inlet|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    second = BlueprintStreams::Flow(Int32, Int32, BlueprintStreams::NotUsed).new do |system, _inlet|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    composed = first.via_mat(second) do |_first_mat, _second_mat|
      raise "flow combination failed"
      BlueprintStreams::NotUsed::INSTANCE
    end

    expect_raises(Exception, "flow combination failed") do
      source.via(composed).to(BlueprintStreams::Sinks.collect(Int32)).run(BLUEPRINT_STREAM_SYSTEM)
    end
    edges = [created.receive, created.receive, created.receive]
    edges.map(&.cancelled?).should eq([true, true, true])
  ensure
    edges.try { |items| items.each { |edge| edge.cancel } }
  end

  it "rolls back the source edge when to_mat combination raises" do
    edges = [] of BlueprintStreams::StageChannel(Int32)
    created = Channel(BlueprintStreams::StageChannel(Int32)).new(1)
    source = BlueprintStreams::Source(Int32, BlueprintStreams::NotUsed).new do |system|
      outlet = BlueprintStreams::StageChannel(Int32).new(system)
      created.send(outlet)
      BlueprintStreams::Materialization(Int32, BlueprintStreams::NotUsed).new(
        outlet,
        BlueprintStreams::NotUsed::INSTANCE
      )
    end
    graph = source.to_mat(BlueprintStreams::Sinks.collect(Int32)) do |_source_mat, _sink_mat|
      raise "sink combination failed"
      BlueprintStreams::NotUsed::INSTANCE
    end

    expect_raises(Exception, "sink combination failed") { graph.run(BLUEPRINT_STREAM_SYSTEM) }
    edges = [created.receive]
    edges.map(&.cancelled?).should eq([true])
  ensure
    edges.try { |items| items.each { |edge| edge.cancel } }
  end
end
