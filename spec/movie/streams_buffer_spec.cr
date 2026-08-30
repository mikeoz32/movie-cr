require "../spec_helper"
require "../../src/movie"

alias BufferStreams = Movie::Streams::Typed

private BUFFER_STREAM_SYSTEM = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)
Spec.after_suite { BUFFER_STREAM_SYSTEM.shutdown }

private def buffer_receive(channel : Channel(T), timeout_span : Time::Span = 1.second) : T forall T
  select
  when value = channel.receive
    value
  when timeout(timeout_span)
    fail("timed out after #{timeout_span} waiting for buffer test signal")
  end
end

private def buffer_eventually(timeout_span : Time::Span = 1.second, &condition : -> Bool)
  deadline = Time.instant + timeout_span
  until condition.call
    fail("condition was not satisfied within #{timeout_span}") if Time.instant >= deadline
    sleep 1.millisecond
  end
end

private def buffered_values(
  channel : BufferStreams::StageChannel(Int32),
  count : Int32,
) : Array(Int32)
  Array(Int32).new(count) do
    channel.receive.as(BufferStreams::StreamElement(Int32)).value
  end
end

private def gated_buffer_sink(
  gate : Channel(Nil),
  observed : Channel(Int32)? = nil,
) : BufferStreams::Sink(Int32, Movie::Future(Array(Int32)))
  BufferStreams::Sink(Int32, Movie::Future(Array(Int32))).new do |system, inlet|
    promise = Movie::Promise(Array(Int32)).new
    BufferStreams::BlueprintRuntime.execute(system) do
      gate.receive?
      values = [] of Int32
      begin
        loop do
          case event = inlet.receive
          when BufferStreams::StreamElement(Int32)
            values << event.value
            observed.try &.send(event.value)
          when BufferStreams::StreamCompleted(Int32)
            promise.try_success(values)
            break
          when BufferStreams::StreamFailed(Int32)
            promise.try_failure(event.error)
            break
          end
        end
      rescue BufferStreams::StreamCancelledError
        promise.try_cancel
      end
    end
    promise.future
  end
end

describe "typed stream bounded buffers" do
  it "rejects non-positive buffer sizes at blueprint construction" do
    expect_raises(ArgumentError, "buffer_size must be greater than zero") do
      BufferStreams::Sources.manual(Int32, buffer_size: 0)
    end
    expect_raises(ArgumentError, "buffer_size must be greater than zero") do
      BufferStreams::Flows.map(Int32, Int32, buffer_size: -1) { |value| value }
    end
  end

  it "backpressures an offer until bounded capacity becomes available" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::Backpressure
    )
    channel.offer(1).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    second_offer = Channel(BufferStreams::QueueOfferResult).new(1)
    producer_started = Channel(Nil).new(1)
    spawn do
      producer_started.send(nil)
      second_offer.send(channel.offer(2))
    end
    buffer_receive(producer_started)

    select
    when second_offer.receive
      fail("backpressured offer completed while the buffer was full")
    when timeout(50.milliseconds)
    end

    channel.receive.as(BufferStreams::StreamElement(Int32)).value.should eq(1)
    buffer_receive(second_offer).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    channel.complete
    channel.receive.as(BufferStreams::StreamElement(Int32)).value.should eq(2)
    channel.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    channel.try &.cancel
  end

  it "releases a backpressured offer with QueueClosed on cancellation" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::Backpressure
    )
    channel.offer(1)
    blocked_offer = Channel(BufferStreams::QueueOfferResult).new(1)
    producer_started = Channel(Nil).new(1)
    spawn do
      producer_started.send(nil)
      blocked_offer.send(channel.offer(2))
    end
    buffer_receive(producer_started)

    select
    when blocked_offer.receive
      fail("backpressured offer completed while the buffer was full")
    when timeout(50.milliseconds)
    end

    channel.cancel
    buffer_receive(blocked_offer).status.should eq(BufferStreams::QueueOfferStatus::QueueClosed)
  ensure
    channel.try &.cancel
  end

  it "exposes offer outcomes through a materialized manual source control" do
    source = BufferStreams::Sources.manual(
      Int32,
      buffer_size: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::DropNew
    ).materialize(BUFFER_STREAM_SYSTEM)
    control = source.value

    control.offer(1).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    control.offer(2).status.should eq(BufferStreams::QueueOfferStatus::Dropped)
    control.complete

    source.outlet.receive.as(BufferStreams::StreamElement(Int32)).value.should eq(1)
    source.outlet.receive.should be_a(BufferStreams::StreamCompleted(Int32))
    control.offer(3).status.should eq(BufferStreams::QueueOfferStatus::QueueClosed)
  ensure
    source.try &.outlet.cancel
  end

  it "keeps the compatibility insertion operator non-failing for configured drops" do
    source = BufferStreams::Sources.manual(
      Int32,
      buffer_size: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::DropNew
    ).materialize(BUFFER_STREAM_SYSTEM)
    control = source.value

    control << 1
    control << 2
    control.complete

    source.outlet.receive.as(BufferStreams::StreamElement(Int32)).value.should eq(1)
    source.outlet.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    source.try &.outlet.cancel
  end

  it "raises Fail overflow through insertion and forwards the same terminal to the sink" do
    gate = Channel(Nil).new
    observed = Channel(Int32).new(1)
    graph = BufferStreams::Sources.manual(
      Int32,
      buffer_size: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::Fail
    ).to_mat(gated_buffer_sink(gate, observed)) { |control, result| {control, result} }
    control, result = graph.run(BUFFER_STREAM_SYSTEM)

    control << 1
    overflow = expect_raises(BufferStreams::BufferOverflowError) { control << 2 }
    gate.close

    buffer_receive(observed).should eq(1)
    sink_error = expect_raises(BufferStreams::BufferOverflowError) { result.await(1.second) }
    sink_error.should be(overflow)
  ensure
    gate.try &.close
  end

  it "forwards a non-default map buffer failure through a materialized graph" do
    gate = Channel(Nil).new
    observed = Channel(Int32).new(1)
    transformed = Channel(Int32).new(2)
    flow = BufferStreams::Flows.map(
      Int32,
      Int32,
      buffer_size: 1,
      overflow_strategy: BufferStreams::OverflowStrategy::Fail
    ) do |value|
      transformed.send(value)
      value * 10
    end
    graph = BufferStreams::Sources.manual(Int32)
      .via(flow)
      .to_mat(gated_buffer_sink(gate, observed)) { |control, result| {control, result} }
    control, result = graph.run(BUFFER_STREAM_SYSTEM)

    control << 1
    buffer_receive(transformed).should eq(1)
    control << 2
    buffer_receive(transformed).should eq(2)
    buffer_eventually { control.terminal? }
    gate.close

    buffer_receive(observed).should eq(10)
    expect_raises(BufferStreams::BufferOverflowError) { result.await(1.second) }
  ensure
    gate.try &.close
  end

  it "drops the oldest buffered element with DropHead" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 3,
      overflow_strategy: BufferStreams::OverflowStrategy::DropHead
    )
    1.upto(3) { |value| channel.offer(value) }

    channel.offer(4).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    channel.complete

    buffered_values(channel, 3).should eq([2, 3, 4])
    channel.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    channel.try &.cancel
  end

  it "drops the newest buffered element with DropTail" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 3,
      overflow_strategy: BufferStreams::OverflowStrategy::DropTail
    )
    1.upto(3) { |value| channel.offer(value) }

    channel.offer(4).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    channel.complete

    buffered_values(channel, 3).should eq([1, 2, 4])
    channel.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    channel.try &.cancel
  end

  it "reports a dropped incoming element with DropNew" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 3,
      overflow_strategy: BufferStreams::OverflowStrategy::DropNew
    )
    1.upto(3) { |value| channel.offer(value) }

    channel.offer(4).status.should eq(BufferStreams::QueueOfferStatus::Dropped)
    channel.complete

    buffered_values(channel, 3).should eq([1, 2, 3])
    channel.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    channel.try &.cancel
  end

  it "clears buffered elements before accepting one with DropBuffer" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 3,
      overflow_strategy: BufferStreams::OverflowStrategy::DropBuffer
    )
    1.upto(3) { |value| channel.offer(value) }

    channel.offer(4).status.should eq(BufferStreams::QueueOfferStatus::Enqueued)
    channel.complete

    buffered_values(channel, 1).should eq([4])
    channel.receive.should be_a(BufferStreams::StreamCompleted(Int32))
  ensure
    channel.try &.cancel
  end

  it "reports Fail overflow and preserves buffered-before-terminal ordering" do
    channel = BufferStreams::StageChannel(Int32).new(
      BUFFER_STREAM_SYSTEM,
      capacity: 2,
      overflow_strategy: BufferStreams::OverflowStrategy::Fail
    )
    channel.offer(1)
    channel.offer(2)

    overflow = channel.offer(3)
    overflow.status.should eq(BufferStreams::QueueOfferStatus::Failure)
    overflow.error.should be_a(BufferStreams::BufferOverflowError)
    channel.offer(4).status.should eq(BufferStreams::QueueOfferStatus::QueueClosed)

    buffered_values(channel, 2).should eq([1, 2])
    terminal = channel.receive.as(BufferStreams::StreamFailed(Int32))
    terminal.error.should be(overflow.error)
  ensure
    channel.try &.cancel
  end
end
