require "../src/movie"

alias Streams = Movie::Streams::Typed

# Reusable, type-changing blueprint graph with bounded runtime edges.
# Run: crystal run examples/streams_blueprints.cr

system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same)

begin
  source = Streams::Sources.manual(Int32, buffer_size: 2)
  stringify = Streams::Flows.map(Int32, String, buffer_size: 2) { |value| "value=#{value}" }
  length = Streams::Flows.map(String, Int32, buffer_size: 2, &.size)
  sink = Streams::Sinks.collect(Int32)

  graph = source
    .via(stringify.via(length))
    .to_mat(sink) { |control, result| {control, result} }

  control, result = graph.run(system)
  control << 7
  control << 42
  control.complete

  puts "lengths: #{result.await.join(", ")}"
ensure
  system.shutdown
end
