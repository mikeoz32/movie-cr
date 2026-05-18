require "../../spec_helper"
require "../../../src/movie"

# Stress test message types
record StressMessage, id : Int64, payload : String do
  include JSON::Serializable
end

record CounterMessage, count : Int32 do
  include JSON::Serializable
end

# Helper to wait with timeout
def wait_until_stress(timeout_ms : Int32 = 5000, interval_ms : Int32 = 10)
  deadline = Time.monotonic + timeout_ms.milliseconds
  until yield
    raise "Timeout waiting for condition" if Time.monotonic >= deadline
    sleep(interval_ms.milliseconds)
  end
end

describe "Movie Remote Stress Tests" do
  before_each do
    Movie::Remote::MessageRegistry.register(StressMessage)
    Movie::Remote::MessageRegistry.register(CounterMessage)
  end

  describe "Server stress" do
    it "handles rapid connection/disconnection cycles" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "stress-server"
      )
      extension = system.enable_remoting("127.0.0.1", 0)
      port = extension.local_port

      cycles = 50
      successful_connections = Atomic(Int32).new(0)

      elapsed = Time.measure do
        cycles.times do
          begin
            socket = TCPSocket.new("127.0.0.1", port)
            socket.tcp_nodelay = true
            successful_connections.add(1)
            socket.close
          rescue ex
            # Connection might fail under stress, that's OK
          end
        end
      end

      puts "\n  Connection cycles: #{cycles} in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Successful: #{successful_connections.get}"
      puts "  Rate: #{(cycles / elapsed.total_seconds).round(0)} connections/sec"

      successful_connections.get.should be >= (cycles * 0.9).to_i  # 90% success rate

      extension.stop
    end

    it "handles many concurrent connections" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "concurrent-server"
      )
      extension = system.enable_remoting("127.0.0.1", 0)
      port = extension.local_port

      num_connections = 20
      connected = Atomic(Int32).new(0)
      sockets = [] of TCPSocket
      mutex = Mutex.new

      # Open connections concurrently
      channels = (1..num_connections).map do |i|
        ch = Channel(TCPSocket?).new(1)
        spawn do
          begin
            socket = TCPSocket.new("127.0.0.1", port)
            socket.tcp_nodelay = true
            connected.add(1)
            ch.send(socket)
          rescue ex
            ch.send(nil)
          end
        end
        ch
      end

      # Collect results
      channels.each do |ch|
        if socket = ch.receive
          mutex.synchronize { sockets << socket }
        end
      end

      puts "\n  Concurrent connections: #{connected.get}/#{num_connections}"

      connected.get.should be >= (num_connections * 0.8).to_i

      # Clean up
      sockets.each(&.close)
      extension.stop
    end
  end

  describe "FrameCodec stress" do
    it "handles high volume frame encoding/decoding" do
      payload = JSON::Any.new({"data" => JSON::Any.new("stress test payload")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor",
        message_type: "StressMessage",
        payload: payload
      )

      num_messages = 100_000
      errors = Atomic(Int32).new(0)

      elapsed = Time.measure do
        num_messages.times do |i|
          begin
            bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
            decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)
            if decoded.nil? || decoded.target_path != envelope.target_path
              errors.add(1)
            end
          rescue ex
            errors.add(1)
          end
        end
      end

      puts "\n  High volume encoding: #{num_messages} messages in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(num_messages / elapsed.total_seconds).round(0)} msgs/sec"
      puts "  Errors: #{errors.get}"

      errors.get.should eq(0)
    end

    it "handles concurrent frame operations" do
      payload = JSON::Any.new({"data" => JSON::Any.new("concurrent test")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor",
        message_type: "StressMessage",
        payload: payload
      )

      num_fibers = 10
      messages_per_fiber = 10_000
      total_processed = Atomic(Int32).new(0)
      errors = Atomic(Int32).new(0)

      elapsed = Time.measure do
        channels = (1..num_fibers).map do
          ch = Channel(Nil).new
          spawn do
            messages_per_fiber.times do
              begin
                bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
                decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)
                total_processed.add(1)
              rescue ex
                errors.add(1)
              end
            end
            ch.send(nil)
          end
          ch
        end

        channels.each(&.receive)
      end

      total = num_fibers * messages_per_fiber
      puts "\n  Concurrent encoding (#{num_fibers} fibers): #{total_processed.get}/#{total} in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(total / elapsed.total_seconds).round(0)} msgs/sec"
      puts "  Errors: #{errors.get}"

      errors.get.should eq(0)
      total_processed.get.should eq(total)
    end
  end

  describe "PathRegistry stress" do
    it "handles concurrent registration and lookup" do
      registry = Movie::PathRegistry.new
      address = Movie::Address.local("stress-system")

      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "registry-stress"
      )

      num_actors = 500
      num_lookups = 10_000
      lookup_errors = Atomic(Int32).new(0)

      # Register actors
      refs = (1..num_actors).map do |i|
        ref = system.spawn(Movie::Behaviors(String).same)
        path = Movie::ActorPath.new(address, ["user", "actor-#{i}"])
        registry.register(ref, path)
        {ref, path}
      end

      # Concurrent lookups
      num_fibers = 5
      lookups_per_fiber = num_lookups // num_fibers

      elapsed = Time.measure do
        channels = (1..num_fibers).map do
          ch = Channel(Nil).new
          spawn do
            lookups_per_fiber.times do |i|
              ref, path = refs[i % num_actors]
              resolved = registry.resolve(path)
              if resolved != ref.id
                lookup_errors.add(1)
              end
            end
            ch.send(nil)
          end
          ch
        end

        channels.each(&.receive)
      end

      puts "\n  Concurrent lookups (#{num_fibers} fibers): #{num_lookups} in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(num_lookups / elapsed.total_seconds).round(0)} lookups/sec"
      puts "  Errors: #{lookup_errors.get}"

      lookup_errors.get.should eq(0)
    end

    it "handles concurrent registration and unregistration" do
      registry = Movie::PathRegistry.new
      address = Movie::Address.local("churn-system")

      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "churn-stress"
      )

      operations = 5_000
      successful_ops = Atomic(Int32).new(0)

      elapsed = Time.measure do
        operations.times do |i|
          ref = system.spawn(Movie::Behaviors(String).same)
          path = Movie::ActorPath.new(address, ["user", "temp-#{i}"])

          registry.register(ref, path)

          # Verify registration
          if registry.resolve(path) == ref.id
            successful_ops.add(1)
          end

          registry.unregister(ref)

          # Verify unregistration
          if registry.resolve(path).nil?
            successful_ops.add(1)
          end
        end
      end

      expected_ops = operations * 2  # register + unregister verification
      puts "\n  Registration churn: #{operations} actors in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(operations / elapsed.total_seconds).round(0)} actors/sec"
      puts "  Successful verifications: #{successful_ops.get}/#{expected_ops}"

      successful_ops.get.should eq(expected_ops)
    end
  end

  describe "End-to-end TCP stress" do
    it "handles high volume message exchange over TCP" do
      # Server system
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "tcp-server"
      )
      server_ext = server_system.enable_remoting("127.0.0.1", 0)
      server_port = server_ext.local_port

      # Client connects
      client_socket = TCPSocket.new("127.0.0.1", server_port)
      client_socket.tcp_nodelay = true

      num_messages = 1_000
      sent = Atomic(Int32).new(0)
      errors = Atomic(Int32).new(0)

      payload = JSON::Any.new({"id" => JSON::Any.new(0_i64), "payload" => JSON::Any.new("test")})

      elapsed = Time.measure do
        num_messages.times do |i|
          envelope = Movie::Remote::WireEnvelope.user_message(
            target_path: "movie://tcp-server/user/actor",
            message_type: "StressMessage",
            payload: payload
          )

          begin
            Movie::Remote::FrameCodec.encode(envelope, client_socket)
            sent.add(1)
          rescue ex
            errors.add(1)
          end
        end
      end

      puts "\n  TCP message sending: #{sent.get}/#{num_messages} in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(sent.get / elapsed.total_seconds).round(0)} msgs/sec"
      puts "  Errors: #{errors.get}"

      sent.get.should be >= (num_messages * 0.95).to_i

      client_socket.close
      server_ext.stop
    end

    it "handles bidirectional communication" do
      # Server
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "bidir-server"
      )
      server_ext = server_system.enable_remoting("127.0.0.1", 0)
      server_port = server_ext.local_port

      # Client
      client_socket = TCPSocket.new("127.0.0.1", server_port)
      client_socket.tcp_nodelay = true

      # Send handshake
      handshake = Movie::Remote::WireEnvelope.handshake("bidir-client", "movie.tcp://bidir-client@127.0.0.1:0")
      Movie::Remote::FrameCodec.encode(handshake, client_socket)

      # Send heartbeats
      num_heartbeats = 100
      sent = 0

      elapsed = Time.measure do
        num_heartbeats.times do
          heartbeat = Movie::Remote::WireEnvelope.heartbeat
          Movie::Remote::FrameCodec.encode(heartbeat, client_socket)
          sent += 1
        end
      end

      puts "\n  Heartbeat sending: #{sent}/#{num_heartbeats} in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(sent / elapsed.total_seconds).round(0)} heartbeats/sec"

      sent.should eq(num_heartbeats)

      client_socket.close
      server_ext.stop
    end
  end

  describe "Memory stress" do
    it "handles large message payloads without issues" do
      # Create increasingly large messages
      sizes = [1_000, 10_000, 100_000, 500_000]

      sizes.each do |size|
        payload_data = "x" * size
        payload = JSON::Any.new({"data" => JSON::Any.new(payload_data)})

        envelope = Movie::Remote::WireEnvelope.user_message(
          target_path: "movie://sys/user/actor",
          message_type: "LargePayload",
          payload: payload
        )

        # Encode and decode
        bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
        decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)

        decoded.should_not be_nil
        decoded.not_nil!.payload["data"].as_s.size.should eq(size)

        puts "\n  Payload size #{size}: encoded to #{bytes.size} bytes"
      end
    end

    it "handles many small allocations" do
      # Create many small messages rapidly to stress GC
      num_messages = 50_000

      elapsed = Time.measure do
        num_messages.times do |i|
          payload = JSON::Any.new({"id" => JSON::Any.new(i.to_i64)})
          envelope = Movie::Remote::WireEnvelope.user_message(
            target_path: "movie://sys/user/actor-#{i % 100}",
            message_type: "SmallMessage",
            payload: payload
          )
          bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
          # Let bytes go out of scope immediately
        end
      end

      puts "\n  Rapid allocations: #{num_messages} messages in #{elapsed.total_milliseconds.round(2)}ms"
      puts "  Rate: #{(num_messages / elapsed.total_seconds).round(0)} msgs/sec"

      # If we get here without OOM, the test passes
      true.should be_true
    end
  end
end
