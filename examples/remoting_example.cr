require "../src/movie"

# Remoting Example
# ================
# Demonstrates actor communication across network boundaries using Movie's
# remoting capabilities. Run this example with:
#
#   crystal run examples/remoting_example.cr -Dpreview_mt -Dexecution_context
#
# This example shows:
# 1. Named actor spawning with automatic path assignment
# 2. Unified actor lookup with actor_for() - works for local and remote
# 3. Transparent location - same code works regardless of actor location
# 4. Connection pools for parallel message delivery

# Quiet down the logs for cleaner output
Log.setup do |c|
  c.bind "*", :warn, Log::IOBackend.new
end

# --- Message Types ---
# All messages must include JSON::Serializable for wire transport

record Ping, sender_path : String, sequence : Int32 do
  include JSON::Serializable
end

record WorkItem, task_id : Int32, data : String do
  include JSON::Serializable
end

record CounterIncrement, amount : Int32 do
  include JSON::Serializable
end

# Register message types with the registry for serialization
Movie::Remote::MessageRegistry.register(Ping)
Movie::Remote::MessageRegistry.register(WorkItem)
Movie::Remote::MessageRegistry.register(CounterIncrement)

# --- Server-side Actors ---

class PingActor < Movie::AbstractBehavior(Ping)
  @ping_count = 0
  @last_sequence = -1

  def receive(message : Ping, context)
    @ping_count += 1
    if message.sequence <= @last_sequence
      puts "[Server] WARNING: Out of order ping! Got #{message.sequence} after #{@last_sequence}"
    end
    @last_sequence = message.sequence
    puts "[Server] PingActor received ping ##{@ping_count} (seq=#{message.sequence})"
    Movie::Behaviors(Ping).same
  end
end

class WorkerActor < Movie::AbstractBehavior(WorkItem)
  @worker_id : String
  @processed = 0

  def initialize(@worker_id : String)
  end

  def receive(message : WorkItem, context)
    @processed += 1
    puts "[Server] Worker #{@worker_id} processed task ##{message.task_id}: '#{message.data}' (total: #{@processed})"
    Movie::Behaviors(WorkItem).same
  end
end

class CounterActor < Movie::AbstractBehavior(CounterIncrement)
  @total = 0

  def receive(message : CounterIncrement, context)
    @total += message.amount
    puts "[Server] Counter total: #{@total} (+#{message.amount})"
    Movie::Behaviors(CounterIncrement).same
  end
end

# --- Main Example ---

puts "=" * 60
puts "Movie Actor Remoting Example (Unified Addressing)"
puts "=" * 60
puts

# Create the server actor system
puts "[Setup] Creating server actor system..."
server_system = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "server-system"
)

# Enable remoting on the server
server_remote = server_system.enable_remoting("127.0.0.1", 9000)
puts "[Setup] Server listening on 127.0.0.1:#{server_remote.local_port}"

# Spawn actors WITH NAMES - paths are automatically assigned!
# No need for manual path registration anymore.
ping_actor = server_system.spawn(PingActor.new, name: "ping")
worker1 = server_system.spawn(WorkerActor.new("worker-1"), name: "worker-1")
worker2 = server_system.spawn(WorkerActor.new("worker-2"), name: "worker-2")
counter = server_system.spawn(CounterActor.new, name: "counter")

# Print the automatically assigned paths
puts "[Setup] Spawned actors with automatic paths:"
puts "  - ping_actor.path: #{ping_actor.path}"
puts "  - worker1.path: #{worker1.path}"
puts "  - worker2.path: #{worker2.path}"
puts "  - counter.path: #{counter.path}"
puts

# Create the client actor system
puts "[Setup] Creating client actor system..."
client_system = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "client-system"
)

# Enable remoting on the client
client_remote = client_system.enable_remoting("127.0.0.1", 9001)
puts "[Setup] Client listening on 127.0.0.1:#{client_remote.local_port}"
puts

# Allow connections to establish
sleep 100.milliseconds

# --- Extension System Demo ---
puts "-" * 60
puts "Extension System"
puts "-" * 60
puts "[Info] Extensions are registered and accessible via system.extension(Type)"
puts "[Info] Registered extensions on server: #{server_system.extensions.all.map(&.class.name)}"
puts "[Info] Access remoting: server_system.extension(Movie::Remote::RemoteExtension)"
remote_via_extension = server_system.extension(Movie::Remote::RemoteExtension)
puts "[Info] Got extension: #{remote_via_extension.class}" if remote_via_extension
puts

# --- Example 1: Unified Actor Lookup with actor_for ---
puts "-" * 60
puts "Example 1: Unified Actor Lookup with actor_for()"
puts "-" * 60

# Use actor_for() to get a reference - works for both local and remote!
# The system automatically determines if it's local or remote.
ping_path = "movie.tcp://server-system@127.0.0.1:9000/user/ping"
remote_ping = client_system.actor_for(ping_path, Ping)

puts "[Client] Got actor ref for: #{ping_path}"
puts "[Client] Ref type: #{remote_ping.class}"

# Send messages - same API regardless of local/remote
5.times do |i|
  remote_ping.as(Movie::Remote::RemoteActorRef(Ping)) << Ping.new(sender_path: "client", sequence: i)
  puts "[Client] Sent ping with sequence #{i}"
end

sleep 100.milliseconds
puts

# --- Example 2: Simplified Local Lookups ---
puts "-" * 60
puts "Example 2: Simplified Local Path Formats"
puts "-" * 60

# Multiple ways to lookup the same local actor:
puts "[Server] Different path formats for local lookup:"

# Full URI
ref1 = server_system.actor_for("movie://server-system/user/ping", Ping)
puts "  Full URI 'movie://server-system/user/ping' -> #{ref1.class}"

# Absolute path (auto-prepends local address)
ref2 = server_system.actor_for("/user/ping", Ping)
puts "  Absolute '/user/ping' -> #{ref2.class}"

# Relative path (auto-prepends local address)
ref3 = server_system.actor_for("user/ping", Ping)
puts "  Relative 'user/ping' -> #{ref3.class}"

# Convenience method
ref4 = server_system.user_actor("ping", Ping)
puts "  user_actor(\"ping\") -> #{ref4.class}"

# All refs point to the same actor
puts "  All refs have same ID: #{ref1.id == ref2.id && ref2.id == ref3.id && ref3.id == ref4.id}"

puts

# --- Example 3: Multiple Remote Actors ---
puts "-" * 60
puts "Example 3: Sending to Multiple Remote Actors"
puts "-" * 60

worker1_ref = client_system.actor_for("movie.tcp://server-system@127.0.0.1:9000/user/worker-1", WorkItem)
worker2_ref = client_system.actor_for("movie.tcp://server-system@127.0.0.1:9000/user/worker-2", WorkItem)

["Process data A", "Analyze report B", "Transform file C", "Validate input D"].each_with_index do |data, i|
  ref = i.even? ? worker1_ref : worker2_ref
  ref.as(Movie::Remote::RemoteActorRef(WorkItem)) << WorkItem.new(task_id: i + 1, data: data)
  puts "[Client] Sent task ##{i + 1} to worker-#{i.even? ? 1 : 2}"
end

sleep 100.milliseconds
puts

# --- Example 4: Connection Pool Info ---
puts "-" * 60
puts "Example 4: Connection Pool for Parallel Sending"
puts "-" * 60

server_addr = Movie::Address.remote("server-system", "127.0.0.1", 9000)
pool = client_remote.pool_for(server_addr)
puts "[Client] Connection pool has #{pool.stripe_count} stripes"

# Show how actors are distributed across stripes
paths = [
  "movie.tcp://server-system@127.0.0.1:9000/user/ping",
  "movie.tcp://server-system@127.0.0.1:9000/user/worker-1",
  "movie.tcp://server-system@127.0.0.1:9000/user/worker-2",
  "movie.tcp://server-system@127.0.0.1:9000/user/counter"
]
puts "[Client] Actors distributed across stripes by path hash:"
paths.each do |path|
  stripe_idx = path.hash.abs % pool.stripe_count
  puts "  #{path.split("/").last} -> stripe #{stripe_idx}"
end

puts

# --- Example 5: High-Throughput Benchmark ---
puts "-" * 60
puts "Example 5: High-Throughput Messaging"
puts "-" * 60

counter_ref = client_system.actor_for("movie.tcp://server-system@127.0.0.1:9000/user/counter", CounterIncrement)

message_count = 1000
puts "[Client] Sending #{message_count} messages..."

elapsed = Time.measure do
  message_count.times do |i|
    counter_ref.as(Movie::Remote::RemoteActorRef(CounterIncrement)) << CounterIncrement.new(amount: 1)
  end
end

sleep 200.milliseconds

throughput = message_count / elapsed.total_seconds
puts "[Client] Sent #{message_count} messages in #{elapsed.total_milliseconds.round(2)}ms"
puts "[Client] Throughput: #{throughput.round(0)} msgs/sec"
puts

# --- Cleanup ---
puts "-" * 60
puts "Cleanup"
puts "-" * 60

client_remote.stop
server_remote.stop

puts "[Done] Remoting example completed!"
puts
puts "Key features with unified addressing:"
puts "  1. spawn(behavior, name: \"foo\") auto-assigns path /user/foo"
puts "  2. actor_for(path) works for BOTH local and remote actors"
puts "  3. No manual path registration needed"
puts "  4. Same code works regardless of actor location"
puts "  5. Simplified local lookups:"
puts "     - actor_for(\"/user/ping\", T)  # absolute path"
puts "     - actor_for(\"user/ping\", T)   # relative path"
puts "     - user_actor(\"ping\", T)       # convenience method"
puts "  6. Path hierarchy: / (root), /system, /user"
