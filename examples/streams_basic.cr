require "../src/movie"

alias Streams = Movie::Streams::Typed
alias Elem = Int32

# Basic typed-stream example using an external actor system.
# Run: crystal run examples/streams_basic.cr -Dpreview_mt -Dexecution_context

system = Movie::ActorSystem(Streams::MessageBase(Elem)).new(
  Movie::Behaviors(Streams::MessageBase(Elem)).setup do
    Movie::Behaviors(Streams::MessageBase(Elem)).same
  end
)

pipeline = Streams.manual(Elem)
  .via(Streams::MapFlow(Elem).new { |v| v * 2 })
  .via(Streams::FilterFlow(Elem).new { |v| v.even? })
  .via(Streams::TakeFlow(Elem).new(3u64))
  .to_collect(initial_demand: 3u64)
  .run(system)

source = pipeline.source
out = pipeline.out_channel.not_nil!

source << Streams::Produce(Elem).new(1)
source << Streams::Produce(Elem).new(2)
source << Streams::Produce(Elem).new(3)
source << Streams::Produce(Elem).new(4)
source << Streams::Produce(Elem).new(5)
source << Streams::OnComplete(Elem).new

3.times do
  puts "got #{out.receive}"
end

# Wait for completion to ensure the pipeline stops cleanly.
pipeline.completion.await
