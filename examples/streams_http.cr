require "json"
require "http/server"
require "../src/movie"

alias Streams = Movie::Streams::Typed
alias Elem = Int32

# Reuse a single actor system for all stream materializations.
STREAM_SYSTEM = Movie::ActorSystem(Streams::MessageBase(Elem)).new(
  Movie::Behaviors(Streams::MessageBase(Elem)).setup do
    Movie::Behaviors(Streams::MessageBase(Elem)).same
  end
)

# Streams an NDJSON sequence of transformed numbers over HTTP.
# Route: /stream?n=10  (defaults to 5)
# Flow: manual source -> map(*2) -> take(n) -> collect to channel -> flush to HTTP response

def stream_numbers(n : Int32, io : IO)
  count = n < 0 ? 0 : n
  pipeline = Streams.manual(Elem)
    .via(Streams::MapFlow(Elem).new { |v| v * 2 })
    .via(Streams::TakeFlow(Elem).new(count.to_u64))
    .to_collect(initial_demand: count.to_u64, channel_capacity: 16)
    .run(STREAM_SYSTEM)

  source = pipeline.source
  out = pipeline.out_channel.not_nil!

  # Produce on a separate fiber so the HTTP handler only reads/flushes.
  spawn do
    (1..count).each do |i|
      source << Streams::Produce(Elem).new(i)
    end
    source << Streams::OnComplete(Elem).new
  end

  io << "{\"count\":#{count}}\n" if count == 0

  count.times do
    val = out.receive
    io << {value: val}.to_json << "\n"
    io.flush
  end

  # Wait for the pipeline to finish before returning.
  pipeline.completion.await
end

server = HTTP::Server.new do |ctx|
  if ctx.request.path != "/stream"
    ctx.response.status = HTTP::Status::NOT_FOUND
    ctx.response.puts "Not found"
    next
  end

  n = ctx.request.query_params["n"]?.try(&.to_i) || 5
  ctx.response.content_type = "application/x-ndjson"
  ctx.response.headers["Transfer-Encoding"] = "chunked"

  stream_numbers(n, ctx.response)
end

address = server.bind_tcp 9292
puts "HTTP stream server listening on http://#{address}/stream?n=5"
puts "Example: curl -N http://localhost:9292/stream?n=5"
server.listen
