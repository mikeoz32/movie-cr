require "../spec_helper"
require "../../src/movie"

alias TestKitStreams = Movie::Streams::Typed

private TESTKIT_STREAM_SYSTEM = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
Spec.after_suite { TESTKIT_STREAM_SYSTEM.shutdown }

describe "typed stream TestKit" do
  it "drives a type-changing bounded pipeline with explicit sink demand" do
    graph = TestKitStreams::TestSources.probe(Int32, buffer_size: 1)
      .via(TestKitStreams::Flows.map(Int32, String, buffer_size: 1) { |value| "value=#{value}" })
      .to_mat(TestKitStreams::TestSinks.probe(String)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(TESTKIT_STREAM_SYSTEM)

    publisher.send_next(7)
    subscriber.expect_no_message(25.milliseconds)
    subscriber.request(1).expect_next("value=7")
    publisher.send_complete
    subscriber.expect_complete
  end

  it "uses one request to expose multiple elements in order" do
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(TESTKIT_STREAM_SYSTEM)

    publisher.send_next(1).send_next(2)
    subscriber.request(2)
      .expect_next(1)
      .expect_next(2)
    publisher.send_complete
    subscriber.expect_complete
  end

  it "re-materializes reusable test blueprints into independent probes" do
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    first_publisher, first_subscriber = graph.run(TESTKIT_STREAM_SYSTEM)
    second_publisher, second_subscriber = graph.run(TESTKIT_STREAM_SYSTEM)

    first_publisher.send_next(1).send_complete
    second_publisher.send_next(2).send_complete
    first_subscriber.request(1).expect_next(1).expect_complete
    second_subscriber.request(1).expect_next(2).expect_complete
  end

  it "asserts the exact upstream failure without requiring demand" do
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(TESTKIT_STREAM_SYSTEM)
    failure = Exception.new("publisher failed")

    publisher.send_error(failure)

    subscriber.expect_error.should be(failure)
  end

  it "reports deterministic timeout diagnostics" do
    subscriber = TestKitStreams::TestSources.probe(Int32)
      .to(TestKitStreams::TestSinks.probe(Int32))
      .run(TESTKIT_STREAM_SYSTEM)
    subscriber.request(1)

    error = expect_raises(TestKitStreams::TestKitAssertionError) do
      subscriber.expect_next(timeout: 20.milliseconds)
    end
    error.message.not_nil!.should contain("timed out after 20 milliseconds")
    error.message.not_nil!.should contain("waiting for next element")
  end

  it "reports unexpected elements while asserting silence" do
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(TESTKIT_STREAM_SYSTEM)
    publisher.send_next(9)
    subscriber.request(1)

    error = expect_raises(TestKitStreams::TestKitAssertionError) do
      subscriber.expect_no_message(100.milliseconds)
    end
    error.message.not_nil!.should contain("expected silence")
    error.message.not_nil!.should contain("element 9")
  end

  it "reports mismatched next elements and terminal signals" do
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(TESTKIT_STREAM_SYSTEM)
    publisher.send_next(1)
    subscriber.request(1)

    mismatch = expect_raises(TestKitStreams::TestKitAssertionError) do
      subscriber.expect_next(2)
    end
    mismatch.message.not_nil!.should contain("expected element 2, received element 1")

    publisher.send_complete
    terminal = expect_raises(TestKitStreams::TestKitAssertionError) do
      subscriber.expect_error
    end
    terminal.message.not_nil!.should contain("expected failure, received completion")
  end

  it "rejects non-positive demand" do
    subscriber = TestKitStreams::TestSources.probe(Int32)
      .to(TestKitStreams::TestSinks.probe(Int32))
      .run(TESTKIT_STREAM_SYSTEM)

    expect_raises(ArgumentError, "demand must be greater than zero") do
      subscriber.request(0)
    end
    expect_raises(ArgumentError, "default_timeout must be greater than zero") do
      TestKitStreams::TestSinks.probe(Int32, default_timeout: Time::Span.zero)
    end
  end

  it "releases a probe waiting on demand when its actor system shuts down" do
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
    graph = TestKitStreams::TestSources.probe(Int32)
      .to_mat(TestKitStreams::TestSinks.probe(Int32)) { |publisher, subscriber| {publisher, subscriber} }
    publisher, subscriber = graph.run(system)
    publisher.send_next(1)

    system.shutdown

    expect_raises(TestKitStreams::TestKitAssertionError, "cannot request from a terminated test subscriber") do
      subscriber.request(1)
    end
    subscriber.expect_error(TestKitStreams::StreamCancelledError)
    expect_raises(TestKitStreams::TestKitAssertionError, "terminal signal was already consumed") do
      subscriber.expect_error
    end
  ensure
    system.try &.shutdown
  end

  it "times out a backpressured send_next and cancels its source" do
    source = TestKitStreams::TestSources.probe(Int32, buffer_size: 1)
      .materialize(TESTKIT_STREAM_SYSTEM)
    publisher = source.value
    publisher.send_next(1)

    error = expect_raises(TestKitStreams::TestKitAssertionError) do
      publisher.send_next(2, timeout: 20.milliseconds)
    end
    error.message.not_nil!.should contain("timed out after 20 milliseconds")
    error.message.not_nil!.should contain("sending element 2")
    publisher.terminal?.should be_true
  ensure
    source.try &.outlet.cancel
  end

  it "diagnoses dropped, failed, and closed publisher sends" do
    dropped_source = TestKitStreams::TestSources.probe(
      Int32,
      buffer_size: 1,
      overflow_strategy: TestKitStreams::OverflowStrategy::DropNew
    ).materialize(TESTKIT_STREAM_SYSTEM)
    dropped = dropped_source.value
    dropped.send_next(1)
    expect_raises(TestKitStreams::TestKitAssertionError, "test publisher element 2 was dropped") do
      dropped.send_next(2)
    end

    failed_source = TestKitStreams::TestSources.probe(
      Int32,
      buffer_size: 1,
      overflow_strategy: TestKitStreams::OverflowStrategy::Fail
    ).materialize(TESTKIT_STREAM_SYSTEM)
    failed = failed_source.value
    failed.send_next(1)
    failure = expect_raises(TestKitStreams::TestKitAssertionError) { failed.send_next(2) }
    failure.message.not_nil!.should contain("BufferOverflowError")

    closed_source = TestKitStreams::TestSources.probe(Int32).materialize(TESTKIT_STREAM_SYSTEM)
    closed = closed_source.value
    closed.send_complete
    expect_raises(TestKitStreams::TestKitAssertionError, "test publisher is already closed") do
      closed.send_next(1)
    end
  ensure
    dropped_source.try &.outlet.cancel
    failed_source.try &.outlet.cancel
    closed_source.try &.outlet.cancel
  end
end
