require "../spec_helper"
require "../../src/movie"

alias StreamsSpec = Movie::Streams::Typed
alias StreamsSpecMessage = StreamsSpec::MessageBase(Int32)

private class StreamsSpecBroadcastHubProbe < StreamsSpec::BroadcastHub(Int32)
  def initialize(@events : Channel(Symbol))
    super()
  end

  def receive(message : StreamsSpecMessage, context : Movie::ActorContext(StreamsSpecMessage))
    behavior = super
    case message
    when StreamsSpec::Subscribe(Int32)
      @events.send(:subscriber)
    when StreamsSpec::OnSubscribe(Int32)
      @events.send(:upstream)
    when StreamsSpec::Request(Int32)
      @events.send(:demand)
    end
    behavior
  end
end

private class StreamsSpecManualSourceProbe < StreamsSpec::ManualSource(Int32)
  def initialize(@requests : Channel(UInt64))
    super()
  end

  def receive(message : StreamsSpecMessage, context : Movie::ActorContext(StreamsSpecMessage))
    behavior = super
    if request = message.as?(StreamsSpec::Request(Int32))
      @requests.send(request.n)
    end
    behavior
  end
end

private def streams_spec_receive(channel : Channel(T), timeout_span : Time::Span = 1.second) : T forall T
  select
  when value = channel.receive
    value
  when timeout(timeout_span)
    fail("timed out after #{timeout_span} waiting for stream test signal")
  end
end

private def stream_spec_system : Movie::ActorSystem(StreamsSpecMessage)
  Movie::ActorSystem(StreamsSpecMessage).new(Movie::Behaviors(StreamsSpecMessage).same)
end

describe Movie::Streams::Typed do
  it "holds produced elements until downstream requests demand" do
    system = stream_spec_system
    pipeline = StreamsSpec.manual(Int32)
      .to_collect(channel_capacity: 2)
      .run(system)
    output = pipeline.out_channel.not_nil!

    pipeline.source << StreamsSpec::Produce(Int32).new(1)
    pipeline.source << StreamsSpec::Produce(Int32).new(2)
    pipeline.source << StreamsSpec::OnComplete(Int32).new

    received_before_demand = false
    select
    when output.receive
      received_before_demand = true
    when timeout(50.milliseconds)
    end
    received_before_demand.should be_false

    pipeline.sink << StreamsSpec::Request(Int32).new(2u64)
    [output.receive, output.receive].should eq([1, 2])
    pipeline.completion.await(1.second).should be_nil
  ensure
    system.try &.shutdown
  end

  it "materializes a typed transform chain with one terminal completion" do
    system = stream_spec_system
    pipeline = StreamsSpec.manual(Int32)
      .via(StreamsSpec::MapFlow(Int32).new { |value| value * 2 })
      .via(StreamsSpec::FilterFlow(Int32).new { |value| value.divisible_by?(3) })
      .via(StreamsSpec::TakeFlow(Int32).new(3u64))
      .to_collect(initial_demand: 3u64, channel_capacity: 3)
      .run(system)
    output = pipeline.out_channel.not_nil!

    1.upto(9) { |value| pipeline.source << StreamsSpec::Produce(Int32).new(value) }
    pipeline.source << StreamsSpec::OnComplete(Int32).new

    [output.receive, output.receive, output.receive].should eq([6, 12, 18])
    pipeline.completion.await(1.second).should be_nil
    pipeline.completion.status.should eq(Movie::FutureStatus::Success)
  ensure
    system.try &.shutdown
  end

  it "cancels promptly with a pending delivery and drops later elements" do
    system = stream_spec_system
    pipeline = StreamsSpec.manual(Int32)
      .to_collect(initial_demand: 1u64)
      .run(system)
    output = pipeline.out_channel.not_nil!

    pipeline.source << StreamsSpec::Produce(Int32).new(1)
    sleep 25.milliseconds
    pipeline.cancel.call
    expect_raises(Movie::FutureCancelled) { pipeline.completion.await(1.second) }

    pipeline.source << StreamsSpec::Produce(Int32).new(2)
    select
    when output.receive
      fail("cancelled stream delivered a pending or later element")
    when timeout(50.milliseconds)
    end
  ensure
    system.try &.shutdown
  end

  it "tracks independent demand for broadcast subscribers" do
    system = stream_spec_system
    hub_events = Channel(Symbol).new(8)
    source_requests = Channel(UInt64).new(4)
    source = system.spawn(StreamsSpecManualSourceProbe.new(source_requests))
    hub = system.spawn(StreamsSpecBroadcastHubProbe.new(hub_events))
    output_a = Channel(Int32).new(2)
    output_b = Channel(Int32).new(2)
    sink_a = system.spawn(StreamsSpec::CollectSink(Int32).new(output_a))
    sink_b = system.spawn(StreamsSpec::CollectSink(Int32).new(output_b))

    hub << StreamsSpec::Subscribe(Int32).new(sink_a)
    hub << StreamsSpec::Subscribe(Int32).new(sink_b)
    source << StreamsSpec::Subscribe(Int32).new(hub)

    wiring_events = Array(Symbol).new(3) { streams_spec_receive(hub_events) }
    wiring_events.count(:subscriber).should eq(2)
    wiring_events.count(:upstream).should eq(1)

    sink_a << StreamsSpec::Request(Int32).new(2u64)
    sink_b << StreamsSpec::Request(Int32).new(1u64)

    demand_events = Array(Symbol).new(2) { streams_spec_receive(hub_events) }
    demand_events.should eq([:demand, :demand])
    total_source_demand = 0u64
    until total_source_demand >= 2
      total_source_demand += streams_spec_receive(source_requests)
    end

    source << StreamsSpec::Produce(Int32).new(10)
    source << StreamsSpec::Produce(Int32).new(20)
    source << StreamsSpec::OnComplete(Int32).new

    [streams_spec_receive(output_a), streams_spec_receive(output_a)].should eq([10, 20])
    streams_spec_receive(output_b).should eq(10)
    select
    when output_b.receive
      fail("broadcast subscriber received more than its demand")
    when timeout(50.milliseconds)
    end
  ensure
    system.try &.shutdown
  end
end
