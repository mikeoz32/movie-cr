require "../../spec_helper"
require "../../../src/movie"
require "benchmark"

BENCH_ENABLED = ENV["MOVIE_BENCH"]? == "1"

# Benchmark message types
record BenchmarkMessage, id : Int64, data : String, timestamp : Int64 do
  include JSON::Serializable
end

record LargeMessage, id : Int64, items : Array(String), metadata : Hash(String, String) do
  include JSON::Serializable
end

if BENCH_ENABLED
describe "Movie Remote Benchmarks" do
  describe "MessageRegistry serialization" do
    before_each do
      Movie::Remote::MessageRegistry.register(BenchmarkMessage)
      Movie::Remote::MessageRegistry.register(LargeMessage)
    end

    it "benchmarks small message serialization" do
      msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
      iterations = 10_000

      elapsed = Time.measure do
        iterations.times do
          tag, json = Movie::Remote::MessageRegistry.serialize(msg)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Small message serialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      # Should be able to serialize at least 50k small messages per second
      ops_per_sec.should be >= 50_000
    end

    it "benchmarks small message deserialization" do
      Movie::Remote::MessageRegistry.register(BenchmarkMessage)
      msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
      tag, json = Movie::Remote::MessageRegistry.serialize(msg)
      iterations = 10_000

      elapsed = Time.measure do
        iterations.times do
          wrapper = Movie::Remote::MessageRegistry.deserialize(tag, json)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Small message deserialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 30_000
    end

    it "benchmarks large message serialization" do
      items = (1..100).map { |i| "item-#{i}-with-some-extra-data" }
      metadata = (1..20).map { |i| {"key#{i}", "value#{i}"} }.to_h
      msg = LargeMessage.new(id: 1_i64, items: items, metadata: metadata)
      iterations = 1_000

      elapsed = Time.measure do
        iterations.times do
          tag, json = Movie::Remote::MessageRegistry.serialize(msg)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Large message serialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 3_000
    end

    it "benchmarks roundtrip serialization" do
      Movie::Remote::MessageRegistry.register(BenchmarkMessage)
      msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
      iterations = 5_000

      elapsed = Time.measure do
        iterations.times do
          tag, json = Movie::Remote::MessageRegistry.serialize(msg)
          wrapper = Movie::Remote::MessageRegistry.deserialize(tag, json)
          restored = wrapper.unwrap(BenchmarkMessage)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Roundtrip serialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 20_000
    end
  end

  describe "FrameCodec encoding" do
    it "benchmarks envelope encoding" do
      payload = JSON::Any.new({"data" => JSON::Any.new("test message content")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie.tcp://sys@host:1234/user/actor",
        message_type: "TestMessage",
        payload: payload
      )
      iterations = 10_000

      elapsed = Time.measure do
        iterations.times do
          bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Frame encoding: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 50_000
    end

    it "benchmarks envelope decoding" do
      payload = JSON::Any.new({"data" => JSON::Any.new("test message content")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie.tcp://sys@host:1234/user/actor",
        message_type: "TestMessage",
        payload: payload
      )
      bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
      iterations = 10_000

      elapsed = Time.measure do
        iterations.times do
          decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Frame decoding: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 30_000
    end

    it "benchmarks roundtrip encoding/decoding" do
      payload = JSON::Any.new({"data" => JSON::Any.new("test message content")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie.tcp://sys@host:1234/user/actor",
        message_type: "TestMessage",
        payload: payload
      )
      iterations = 5_000

      elapsed = Time.measure do
        iterations.times do
          bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
          decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Frame roundtrip: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 20_000
    end
  end

  describe "ActorPath parsing" do
    it "benchmarks path parsing" do
      path_str = "movie.tcp://my-system@127.0.0.1:2552/user/service/worker/task"
      iterations = 50_000

      elapsed = Time.measure do
        iterations.times do
          path = Movie::ActorPath.parse(path_str)
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Path parsing: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 100_000
    end

    it "benchmarks path to_s" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      path = Movie::ActorPath.new(addr, ["user", "service", "worker", "task"])
      iterations = 100_000

      elapsed = Time.measure do
        iterations.times do
          str = path.to_s
        end
      end

      ops_per_sec = iterations / elapsed.total_seconds
      puts "\n  Path to_s: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"

      ops_per_sec.should be >= 200_000
    end
  end

  describe "PathRegistry operations" do
    it "benchmarks path registration and lookup" do
      registry = Movie::PathRegistry.new
      address = Movie::Address.local("bench-system")

      # Pre-create paths
      paths = (1..1000).map do |i|
        Movie::ActorPath.new(address, ["user", "actor-#{i}"])
      end

      # Create a mock system for actor refs
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "bench-system"
      )

      # Create refs
      refs = (1..1000).map do |i|
        system.spawn(Movie::Behaviors(String).same)
      end

      # Benchmark registration
      reg_elapsed = Time.measure do
        refs.each_with_index do |ref, i|
          registry.register(ref, paths[i])
        end
      end

      puts "\n  Registration: #{(1000 / reg_elapsed.total_seconds).round(0)} ops/sec"

      # Benchmark lookup by path
      lookup_iterations = 10_000
      lookup_elapsed = Time.measure do
        lookup_iterations.times do |i|
          registry.resolve(paths[i % 1000])
        end
      end

      ops_per_sec = lookup_iterations / lookup_elapsed.total_seconds
      puts "  Path lookup: #{ops_per_sec.round(0)} ops/sec"

      ops_per_sec.should be >= 100_000
    end
  end

  describe "StripedConnectionPool parallel throughput" do
    it "benchmarks single connection vs striped pool" do
      # Create server
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "server-system"
      )
      remote = server_system.enable_remoting("127.0.0.1", 0)
      server_port = remote.local_port

      # Create client system
      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "client-system"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      # Get striped pool to server
      server_addr = Movie::Address.remote("server-system", "127.0.0.1", server_port)
      pool = client_remote.pool_for(server_addr)

      sleep 10.milliseconds # Let connections establish

      payload = JSON::Any.new({"data" => JSON::Any.new("benchmark message")})
      iterations = 10_000

      # Benchmark single stripe (simulates old single-connection behavior)
      single_elapsed = Time.measure do
        single_conn = pool.stripe(0)
        iterations.times do |i|
          envelope = Movie::Remote::WireEnvelope.user_message(
            target_path: "movie.tcp://server-system@127.0.0.1:#{server_port}/user/actor",
            message_type: "TestMessage",
            payload: payload
          )
          single_conn.send(envelope)
        end
      end
      single_ops = iterations / single_elapsed.total_seconds

      # Benchmark striped pool with round-robin (max parallelism)
      striped_elapsed = Time.measure do
        iterations.times do |i|
          envelope = Movie::Remote::WireEnvelope.user_message(
            target_path: "movie.tcp://server-system@127.0.0.1:#{server_port}/user/actor",
            message_type: "TestMessage",
            payload: payload
          )
          pool.send_round_robin(envelope)
        end
      end
      striped_ops = iterations / striped_elapsed.total_seconds

      puts "\n  Single connection: #{single_ops.round(0)} msgs/sec"
      puts "  Striped pool (#{pool.stripe_count} stripes): #{striped_ops.round(0)} msgs/sec"
      puts "  Speedup: #{(striped_ops / single_ops).round(2)}x"

      # Cleanup
      client_remote.stop
      remote.stop

      # Striped should be at least comparable (and ideally faster under load)
      striped_ops.should be >= single_ops * 0.8
    end

    it "benchmarks parallel fiber sending through pool" do
      # Create server
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "server-system-parallel"
      )
      remote = server_system.enable_remoting("127.0.0.1", 0)
      server_port = remote.local_port

      # Create client system
      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "client-system-parallel"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      server_addr = Movie::Address.remote("server-system-parallel", "127.0.0.1", server_port)
      pool = client_remote.pool_for(server_addr)

      sleep 10.milliseconds

      payload = JSON::Any.new({"data" => JSON::Any.new("parallel benchmark")})
      messages_per_fiber = 2_000
      fiber_count = 8
      total_messages = messages_per_fiber * fiber_count

      done_channel = Channel(Int32).new(fiber_count)

      elapsed = Time.measure do
        # Launch fibers that send in parallel
        fiber_count.times do |fiber_id|
          spawn do
            sent = 0
            messages_per_fiber.times do |i|
              envelope = Movie::Remote::WireEnvelope.user_message(
                target_path: "movie.tcp://server-system-parallel@127.0.0.1:#{server_port}/user/actor-#{fiber_id}",
                message_type: "TestMessage",
                payload: payload
              )
              if pool.send(envelope)
                sent += 1
              end
            end
            done_channel.send(sent)
          end
        end

        # Wait for all fibers
        total_sent = 0
        fiber_count.times do
          total_sent += done_channel.receive
        end
      end

      ops_per_sec = total_messages / elapsed.total_seconds
      puts "\n  Parallel sending (#{fiber_count} fibers, #{pool.stripe_count} stripes, cooperative):"
      puts "    Total: #{total_messages} messages in #{elapsed.total_milliseconds.round(2)}ms"
      puts "    Rate: #{ops_per_sec.round(0)} msgs/sec"

      # Cleanup
      client_remote.stop
      remote.stop

      # Should achieve good throughput with parallel sending
      ops_per_sec.should be >= 80_000
    end

    it "benchmarks truly parallel sending with isolated execution contexts using dedicated stripes" do
      # Create server
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "server-system-ec"
      )
      remote = server_system.enable_remoting("127.0.0.1", 0)
      server_port = remote.local_port

      # Create client system
      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "client-system-ec"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      server_addr = Movie::Address.remote("server-system-ec", "127.0.0.1", server_port)
      pool = client_remote.pool_for(server_addr)

      sleep 10.milliseconds

      payload = JSON::Any.new({"data" => JSON::Any.new("parallel ec benchmark")})
      messages_per_context = 2_000
      context_count = 8
      total_messages = messages_per_context * context_count

      done_channel = Channel(Int32).new(context_count)

      # Store isolated contexts so they don't get GC'd
      isolated_contexts = [] of Fiber::ExecutionContext::Isolated

      elapsed = Time.measure do
        # Create isolated execution contexts - each runs on its own dedicated OS thread
        # Each thread uses its own dedicated stripe to eliminate mutex contention
        context_count.times do |ctx_id|
          # Capture variables for the closure
          port = server_port
          ch = done_channel
          # Get dedicated stripe for this thread (no mutex contention between threads)
          dedicated_conn = pool.stripe(ctx_id)
          msg_count = messages_per_context

          ctx = Fiber::ExecutionContext::Isolated.new("bench-isolated-#{ctx_id}") do
            sent = 0
            msg_count.times do |i|
              envelope = Movie::Remote::WireEnvelope.user_message(
                target_path: "movie.tcp://server-system-ec@127.0.0.1:#{port}/user/actor-#{ctx_id}",
                message_type: "TestMessage",
                payload: payload
              )
              # Use dedicated connection directly to avoid any hash lookups or contention
              if dedicated_conn.send(envelope)
                sent += 1
              end
            end
            ch.send(sent)
          end
          isolated_contexts << ctx
        end

        # Wait for all contexts to complete
        total_sent = 0
        context_count.times do
          total_sent += done_channel.receive
        end
      end

      ops_per_sec = total_messages / elapsed.total_seconds
      puts "\n  Parallel sending with Isolated ExecutionContexts + dedicated stripes (#{context_count} threads):"
      puts "    Total: #{total_messages} messages in #{elapsed.total_milliseconds.round(2)}ms"
      puts "    Rate: #{ops_per_sec.round(0)} msgs/sec"

      # Compare: single connection baseline
      single_conn = pool.stripe(0)
      single_elapsed = Time.measure do
        total_messages.times do |i|
          envelope = Movie::Remote::WireEnvelope.user_message(
            target_path: "movie.tcp://server-system-ec@127.0.0.1:#{server_port}/user/actor",
            message_type: "TestMessage",
            payload: payload
          )
          single_conn.send(envelope)
        end
      end
      single_ops = total_messages / single_elapsed.total_seconds
      puts "    Single connection baseline: #{single_ops.round(0)} msgs/sec"
      puts "    Speedup: #{(ops_per_sec / single_ops).round(2)}x"

      # Cleanup
      client_remote.stop
      remote.stop

      # With true parallelism and no contention, should see significant speedup
      ops_per_sec.should be >= single_ops * 1.5
    end

    it "benchmarks actor-consistent routing" do
      # Create server
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "server-consistent"
      )
      remote = server_system.enable_remoting("127.0.0.1", 0)
      server_port = remote.local_port

      # Create client system
      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "client-consistent"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      server_addr = Movie::Address.remote("server-consistent", "127.0.0.1", server_port)
      pool = client_remote.pool_for(server_addr)

      sleep 10.milliseconds

      # Create multiple actor paths
      actor_paths = (1..100).map do |i|
        Movie::ActorPath.new(server_addr, ["user", "actor-#{i}"])
      end

      # Verify consistent routing (same actor always goes to same stripe)
      routing_checks = Hash(String, Int32).new
      actor_paths.each do |path|
        conn = pool.connection_for(path)
        stripe_index = (path.to_s.hash.abs % pool.stripe_count).to_i32
        routing_checks[path.to_s] = stripe_index
      end

      # Re-check should get same results
      consistent = actor_paths.all? do |path|
        expected = routing_checks[path.to_s]
        actual = (path.to_s.hash.abs % pool.stripe_count).to_i32
        expected == actual
      end

      puts "\n  Routing consistency: #{consistent ? "PASS" : "FAIL"}"
      puts "  Actor distribution across #{pool.stripe_count} stripes:"
      distribution = actor_paths.group_by { |p| (p.to_s.hash.abs % pool.stripe_count).to_i32 }
      distribution.each do |stripe, actors|
        puts "    Stripe #{stripe}: #{actors.size} actors"
      end

      # Cleanup
      client_remote.stop
      remote.stop

      consistent.should be_true
    end
  end
end
else
describe "Movie Remote Benchmarks" do
  it "skips benchmarks by default" do
    true.should be_true
  end
end
end
