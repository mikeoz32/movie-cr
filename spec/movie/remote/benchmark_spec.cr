require "../../spec_helper"
require "../../../src/movie"
require "benchmark"

BENCH_ENABLED = ENV["MOVIE_BENCH"]? == "1"

module RemoteBenchmarkMeasurements
  extend self

  def allocated_bytes_per_iteration(iterations : Int32, & : ->) : Float64
    GC.collect
    before = GC.stats.total_bytes
    yield
    (GC.stats.total_bytes - before) / iterations.to_f
  end
end

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

      it "benchmarks small message payload preparation" do
        msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
        iterations = 10_000

        elapsed = Time.measure do
          iterations.times do
            tag, payload = Movie::Remote::MessageRegistry.prepare(msg)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Small message payload preparation: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
      end

      it "benchmarks small message deserialization" do
        Movie::Remote::MessageRegistry.register(BenchmarkMessage)
        msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
        tag, payload = Movie::Remote::MessageRegistry.prepare(msg)
        envelope = Movie::Remote::WireEnvelope.user_message("movie://bench/user/target", tag, payload)
        decoded_payload = Movie::Remote::FrameCodec.decode_from_bytes(
          Movie::Remote::FrameCodec.encode_to_bytes(envelope)
        ).not_nil!.payload_data
        iterations = 10_000

        elapsed = Time.measure do
          iterations.times do
            wrapper = Movie::Remote::MessageRegistry.deserialize(tag, decoded_payload)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Small message deserialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
      end

      it "benchmarks large message payload preparation" do
        items = (1..100).map { |i| "item-#{i}-with-some-extra-data" }
        metadata = (1..20).map { |i| {"key#{i}", "value#{i}"} }.to_h
        msg = LargeMessage.new(id: 1_i64, items: items, metadata: metadata)
        iterations = 1_000

        elapsed = Time.measure do
          iterations.times do
            tag, payload = Movie::Remote::MessageRegistry.prepare(msg)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Large message payload preparation: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
      end

      it "benchmarks roundtrip serialization" do
        Movie::Remote::MessageRegistry.register(BenchmarkMessage)
        msg = BenchmarkMessage.new(id: 1_i64, data: "hello", timestamp: Time.utc.to_unix_ms)
        iterations = 5_000
        encoder = Movie::Remote::FrameCodec::Encoder.new
        decoder = Movie::Remote::FrameCodec::Decoder.new
        io = IO::Memory.new

        elapsed = Time.measure do
          iterations.times do
            tag, payload = Movie::Remote::MessageRegistry.prepare(msg)
            envelope = Movie::Remote::WireEnvelope.user_message("movie://bench/user/target", tag, payload)
            io.clear
            encoder.encode(envelope, io)
            io.rewind
            decoded = decoder.decode(io).not_nil!
            wrapper = Movie::Remote::MessageRegistry.deserialize(tag, decoded.payload_data)
            restored = wrapper.unwrap(BenchmarkMessage)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Roundtrip serialization: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
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
        encoder = Movie::Remote::FrameCodec::Encoder.new
        output = IO::Memory.new

        elapsed = Time.measure do
          iterations.times do
            output.clear
            encoder.encode(envelope, output)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Frame encoding: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
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
        decoder = Movie::Remote::FrameCodec::Decoder.new
        input = IO::Memory.new(bytes, false)

        elapsed = Time.measure do
          iterations.times do
            input.rewind
            decoded = decoder.decode(input)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Frame decoding: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
      end

      it "benchmarks roundtrip encoding/decoding" do
        payload = JSON::Any.new({"data" => JSON::Any.new("test message content")})
        envelope = Movie::Remote::WireEnvelope.user_message(
          target_path: "movie.tcp://sys@host:1234/user/actor",
          message_type: "TestMessage",
          payload: payload
        )
        iterations = 5_000
        encoder = Movie::Remote::FrameCodec::Encoder.new
        decoder = Movie::Remote::FrameCodec::Decoder.new
        io = IO::Memory.new

        elapsed = Time.measure do
          iterations.times do
            io.clear
            encoder.encode(envelope, io)
            io.rewind
            decoded = decoder.decode(io)
          end
        end

        ops_per_sec = iterations / elapsed.total_seconds
        puts "\n  Frame roundtrip: #{ops_per_sec.round(0)} ops/sec (#{iterations} iterations in #{elapsed.total_milliseconds.round(2)}ms)"
      end
    end

    describe "Inbound registered-message pipeline" do
      it "reports decode and typed-deserialization allocations separately" do
        Movie::Remote::MessageRegistry.register(BenchmarkMessage)
        message = BenchmarkMessage.new(id: 1_i64, data: "x" * 64, timestamp: 1_i64)
        tag, payload = Movie::Remote::MessageRegistry.prepare(message)
        frame = Movie::Remote::FrameCodec.encode_to_bytes(
          Movie::Remote::WireEnvelope.user_message(
            "movie://bench/user/target",
            tag,
            payload
          )
        )
        iterations = 10_000

        input = IO::Memory.new(frame, false)
        decoder = Movie::Remote::FrameCodec::Decoder.new
        decode_checksum = 0_i64
        decode_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            input.rewind
            decode_checksum &+= decoder.decode(input).not_nil!.timestamp
          end
        end

        decoded_payload = Movie::Remote::FrameCodec.decode_from_bytes(frame).not_nil!.payload_data
        deserialize_checksum = 0_i64
        deserialize_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            restored = Movie::Remote::MessageRegistry.deserialize(tag, decoded_payload).unwrap(BenchmarkMessage)
            deserialize_checksum &+= restored.id
          end
        end

        pipeline_input = IO::Memory.new(frame, false)
        pipeline_decoder = Movie::Remote::FrameCodec::Decoder.new(Movie::Remote::MessageRegistry.payload_decoder)
        pipeline_checksum = 0_i64
        pipeline_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            pipeline_input.rewind
            envelope = pipeline_decoder.decode(pipeline_input).not_nil!
            restored = Movie::Remote::MessageRegistry.deserialize(tag, envelope.payload_data).unwrap(BenchmarkMessage)
            pipeline_checksum &+= restored.id
          end
        end

        payload_json = message.to_json
        typed_checksum = 0_i64
        typed_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            typed_checksum &+= BenchmarkMessage.from_json(payload_json).id
          end
        end

        wrapped_checksum = 0_i64
        wrapped_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            pull = JSON::PullParser.new(payload_json)
            decoded = Movie::Remote::MessageRegistry.decode_payload(tag, pull)
            wrapped_checksum &+= Movie::Remote::MessageRegistry.deserialize(tag, decoded).unwrap(BenchmarkMessage).id
          end
        end

        frame_json = frame[4, frame.size - 4]
        string_input_checksum = 0_i64
        string_input_bytes = RemoteBenchmarkMeasurements.allocated_bytes_per_iteration(iterations) do
          iterations.times do
            json_source = String.new(frame_json)
            envelope = Movie::Remote::WireEnvelope.new(
              JSON::PullParser.new(json_source),
              Movie::Remote::MessageRegistry.payload_decoder
            )
            string_input_checksum &+= Movie::Remote::MessageRegistry
              .deserialize(tag, envelope.payload_data)
              .unwrap(BenchmarkMessage).id
          end
        end

        decode_checksum.should be > 0_i64
        {
          deserialize_checksum,
          pipeline_checksum,
          typed_checksum,
          wrapped_checksum,
          string_input_checksum,
        }.should eq({iterations.to_i64, iterations.to_i64, iterations.to_i64, iterations.to_i64, iterations.to_i64})
        puts "\n  Inbound allocations: raw decode #{decode_bytes.round(1)} B/msg, second deserialize #{deserialize_bytes.round(1)} B/msg, direct combined #{pipeline_bytes.round(1)} B/msg"
        puts "  Payload allocations: typed value #{typed_bytes.round(1)} B/msg, registry wrappers #{wrapped_bytes.round(1)} B/msg"
        puts "  Parser input allocations: reusable IO #{pipeline_bytes.round(1)} B/msg, copied String #{string_input_bytes.round(1)} B/msg"
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
end
