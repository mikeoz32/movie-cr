require "../src/movie"

# Experimental remoting MVP example.
#
# Run with:
#   crystal run examples/remoting_example.cr -Dpreview_mt -Dexecution_context
#
# The example uses port 0 so the operating system selects free ports. It
# demonstrates typed user delivery, remote ask, actor paths, and shutdown.

Log.setup do |c|
  c.bind "*", :warn, Log::IOBackend.new
end

record Ping, sequence : Int32 do
  include JSON::Serializable
end

record EchoRequest, body : String do
  include JSON::Serializable
end

record EchoResponse, body : String do
  include JSON::Serializable
end

Movie::Remote::MessageRegistry.register(Ping)
Movie::Remote::MessageRegistry.register(EchoRequest)
Movie::Remote::MessageRegistry.register(EchoResponse)

class PingActor < Movie::AbstractBehavior(Ping)
  def initialize(@received : Channel(Int32))
  end

  def receive(message : Ping, context : Movie::ActorContext(Ping))
    @received.send(message.sequence)
    puts "[Server] received ping ##{message.sequence}"
    Movie::Behaviors(Ping).same
  end
end

class EchoActor < Movie::AbstractBehavior(EchoRequest)
  def receive(message : EchoRequest, context : Movie::ActorContext(EchoRequest))
    Movie::Ask.reply_if_asked(context.sender, EchoResponse.new("echo: #{message.body}"))
    Movie::Behaviors(EchoRequest).same
  end
end

server = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "server-system"
)
server_remote = server.enable_remoting("127.0.0.1", 0)

received = Channel(Int32).new(3)
ping_actor = server.spawn(PingActor.new(received), name: "ping")
echo_actor = server.spawn(EchoActor.new, name: "echo")

client = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "client-system"
)
client_remote = client.enable_remoting("127.0.0.1", 0)

puts "Server listening on #{server_remote.local_port}"
puts "Client listening on #{client_remote.local_port}"
puts "Ping path: #{ping_actor.path}"
puts "Echo path: #{echo_actor.path}"

sleep 100.milliseconds

ping_path = ping_actor.path.not_nil!.to_s
remote_ping = client.actor_for(ping_path, Ping).as(Movie::Remote::RemoteActorRef(Ping))
3.times do |sequence|
  remote_ping << Ping.new(sequence)
end

3.times do
  puts "[Client] confirmed ping ##{received.receive}"
end

echo_path = echo_actor.path.not_nil!.to_s
remote_echo = client.actor_for(echo_path, EchoRequest).as(
  Movie::Remote::RemoteActorRef(EchoRequest)
)
reply = remote_echo.ask(EchoRequest.new("hello"), EchoResponse, 2.seconds).await(3.seconds)
puts "[Client] remote ask reply: #{reply.body}"

pool = client_remote.pool_for(ping_actor.path.not_nil!.address)
puts "[Client] connection pool: #{pool.stripe_count} stripes"

client.shutdown
server.shutdown

puts "Remoting example completed."
