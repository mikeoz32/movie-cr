require "../src/movie"

alias Streams = Movie::Streams::Typed
alias Elem = Int32
alias Msg = Streams::MessageBase(Elem)

def create_system : Movie::ActorSystem(Msg)
  Movie::ActorSystem(Msg).new(
    Movie::Behaviors(Msg).setup do
      Movie::Behaviors(Msg).same
    end
  )
end

def demo_linear_flow(system : Movie::ActorSystem(Msg))
  puts "\n[1] Linear flow (map/filter/take -> collect)"

  pipeline = Streams.manual(Elem)
    .via(Streams::MapFlow(Elem).new { |v| v * 2 })
    .via(Streams::FilterFlow(Elem).new { |v| (v % 3) == 0 })
    .via(Streams::TakeFlow(Elem).new(3u64))
    .to_collect(initial_demand: 3u64)
    .run(system)

  1.upto(9) do |n|
    pipeline.source << Streams::Produce(Elem).new(n)
  end
  pipeline.source << Streams::OnComplete(Elem).new

  output_ch = pipeline.out_channel.not_nil!
  values = [receive_or_timeout(output_ch), receive_or_timeout(output_ch), receive_or_timeout(output_ch)]
  pipeline.completion.await
  puts "result: #{values.join(", ")}"
end

def demo_fold_flow(system : Movie::ActorSystem(Msg))
  puts "\n[2] Fold flow (take first 5 -> sum)"

  pipeline = Streams.manual(Elem)
    .via(Streams::TakeFlow(Elem).new(5u64))
    .fold(0, ->(acc : Int32, elem : Int32) { acc + elem }, initial_demand: 5u64)
    .run(system)

  1.upto(10) do |n|
    pipeline.source << Streams::Produce(Elem).new(n)
  end
  pipeline.source << Streams::OnComplete(Elem).new

  total = pipeline.completion.await
  puts "sum(first 5): #{total}"
end

def demo_broadcast_flow(system : Movie::ActorSystem(Msg))
  puts "\n[3] Broadcast hub (independent demand)"

  source = system.spawn(Streams::ManualSource(Elem).new)
  hub = system.spawn(Streams::BroadcastHub(Elem).new)

  out_a = Channel(Elem).new(8)
  out_b = Channel(Elem).new(8)

  sink_a = system.spawn(Streams::CollectSink(Elem).new(out_a))
  sink_b = system.spawn(Streams::CollectSink(Elem).new(out_b))

  hub << Streams::Subscribe(Elem).new(sink_a)
  hub << Streams::Subscribe(Elem).new(sink_b)
  source << Streams::Subscribe(Elem).new(hub)

  sink_a << Streams::Request(Elem).new(3u64)
  sink_b << Streams::Request(Elem).new(1u64)
  sleep 100.milliseconds

  source << Streams::Produce(Elem).new(10)
  source << Streams::Produce(Elem).new(20)
  source << Streams::Produce(Elem).new(30)
  source << Streams::OnComplete(Elem).new

  values_a = [receive_or_timeout(out_a), receive_or_timeout(out_a), receive_or_timeout(out_a)]
  value_b = receive_or_timeout(out_b)

  puts "subscriber A: #{values_a.join(", ")}"
  puts "subscriber B: #{value_b}"
end

puts "Movie Streams showcase (typed DSL on external ActorSystem)"
system = create_system

demo_linear_flow(system)
demo_fold_flow(system)
demo_broadcast_flow(system)

puts "\nShowcase complete."

private def receive_or_timeout(ch : Channel(T), wait_for : Time::Span = 2.seconds) : T forall T
  select
  when value = ch.receive
    value
  when timeout(wait_for)
    raise "Timed out waiting for stream output"
  end
end
