require "../../spec_helper"
require "../../../src/movie"
require "process/executable_path"

STRESS_ENABLED = ENV["MOVIE_STRESS"]? == "1"

# Stress test message types
record StressMessage, id : Int64, payload : String do
  include JSON::Serializable
end

record CounterMessage, count : Int32 do
  include JSON::Serializable
end

private class StressDeliveryProbe < Movie::AbstractBehavior(StressMessage)
  getter received : Atomic(Int32)
  @ids : Array(Int64)
  @ids_mutex : Mutex

  def initialize
    @received = Atomic(Int32).new(0)
    @ids = [] of Int64
    @ids_mutex = Mutex.new
  end

  def receive(message, context)
    @ids_mutex.synchronize { @ids << message.id }
    @received.add(1)
    Movie::Behaviors(StressMessage).same
  end

  def ids : Array(Int64)
    @ids_mutex.synchronize { @ids.dup }
  end
end

private class StressAskProbe < Movie::AbstractBehavior(StressMessage)
  def receive(message : StressMessage, context : Movie::ActorContext(StressMessage))
    unless message.payload == "no-reply"
      Movie::Ask.reply_if_asked(context.sender, CounterMessage.new(message.id.to_i32))
    end
    Movie::Behaviors(StressMessage).same
  end
end

private class StressControlProbe < Movie::AbstractBehavior(String)
  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end

  def on_signal(signal : Movie::SystemMessage)
    if signal.is_a?(Movie::PostStop)
      puts "MOVIE_ASSOCIATION_CONTROL_STOPPED"
      STDOUT.flush
    end
  end
end

private class StressWatcher < Movie::AbstractBehavior(String)
  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end
end

private def run_association_peer : NoReturn
  port = ENV["MOVIE_ASSOCIATION_PEER_PORT"].to_i
  Movie::Remote::MessageRegistry.register(StressMessage)
  Movie::Remote::MessageRegistry.register(CounterMessage)
  system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "process-chaos-server")
  system.spawn(StressAskProbe.new, name: "actor")
  system.spawn(StressControlProbe.new, name: "control")
  system.enable_remoting("127.0.0.1", port, 1)
  puts "MOVIE_ASSOCIATION_PEER_READY"
  STDOUT.flush
  STDIN.gets
  system.shutdown(1.second)
  exit
end

private def complete_ack_loss_handshake(socket : TCPSocket, decoder : Movie::Remote::FrameCodec::Decoder, port : Int32) : Nil
  request = decoder.decode(socket) || raise "ack-loss peer closed before handshake"
  raise "ack-loss peer expected handshake" unless request.kind.handshake?
  client = Movie::Remote::AssociationHandshake.from_json(request.payload_data.json_source)
  challenge = Movie::Remote::AssociationHandshake.create(
    system: "process-ack-server",
    address: "movie.tcp://process-ack-server@127.0.0.1:#{port}",
    node_uid: "stable-ack-loss-node",
    association_id: client.association_id
  )
  Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake_ack(challenge), socket)

  confirmation = decoder.decode(socket) || raise "ack-loss peer closed before confirmation"
  raise "ack-loss peer expected handshake confirmation" unless confirmation.kind.handshake_confirm?
  Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake_ready(client.association_id), socket)
end

private def next_ack_loss_control(socket : TCPSocket, decoder : Movie::Remote::FrameCodec::Decoder) : Movie::Remote::WireEnvelope
  loop do
    envelope = decoder.decode(socket) || raise "ack-loss peer closed before control frame"
    if envelope.kind.heartbeat?
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.heartbeat_ack, socket)
    elsif envelope.kind.system_message?
      return envelope
    end
  end
end

private def run_ack_loss_peer : NoReturn
  port = ENV["MOVIE_ASSOCIATION_PEER_PORT"].to_i
  server = TCPServer.new("127.0.0.1", port)
  puts "MOVIE_ASSOCIATION_ACK_LOSS_READY"
  STDOUT.flush

  first = nil.as(Movie::Remote::WireEnvelope?)
  2.times do |generation|
    socket = server.accept
    socket.tcp_nodelay = true
    decoder = Movie::Remote::FrameCodec::Decoder.new
    complete_ack_loss_handshake(socket, decoder, port)
    control = next_ack_loss_control(socket, decoder)

    if generation == 0
      first = control
      puts "MOVIE_ASSOCIATION_CONTROL_ACK_WITHHELD"
      STDOUT.flush
      socket.close
    else
      original = first.not_nil!
      raise "control stream changed across ACK-loss reconnect" unless control.control_stream == original.control_stream
      raise "control sequence changed across ACK-loss reconnect" unless control.control_sequence == original.control_sequence
      Movie::Remote::FrameCodec.encode(
        Movie::Remote::WireEnvelope.control_ack(control.control_stream.not_nil!, control.control_sequence.not_nil!),
        socket
      )
      puts "MOVIE_ASSOCIATION_CONTROL_RETRANSMITTED"
      STDOUT.flush
      STDIN.gets
      socket.close
    end
  end
  server.close
  exit
end

private def wait_for_peer_line(child : Process, expected : String, timeout_span : Time::Span = 5.seconds) : Nil
  found = Channel(Bool).new(1)
  spawn do
    matched = false
    while line = child.output.gets
      if line == expected
        matched = true
        break
      end
    end
    found.send(matched)
  end
  select
  when matched = found.receive
    raise "association peer exited before #{expected}" unless matched
  when timeout(timeout_span)
    raise "association peer did not emit #{expected}"
  end
end

run_association_peer if ENV["MOVIE_ASSOCIATION_PEER_MODE"]? == "1"
run_ack_loss_peer if ENV["MOVIE_ASSOCIATION_ACK_LOSS_MODE"]? == "1"

private def spawn_association_peer(port : Int32) : Process
  executable = Process.executable_path || raise "cannot resolve stress executable path"
  child = Process.new(
    executable,
    env: {
      "MOVIE_ASSOCIATION_PEER_MODE" => "1",
      "MOVIE_ASSOCIATION_PEER_PORT" => port.to_s,
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  )
  ready = Channel(Bool).new(1)
  spawn do
    found = false
    while line = child.output.gets
      if line == "MOVIE_ASSOCIATION_PEER_READY"
        found = true
        break
      end
    end
    ready.send(found)
  end
  select
  when found = ready.receive
    raise "association peer exited before readiness" unless found
  when timeout(5.seconds)
    child.terminate(graceful: false)
    raise "association peer readiness timed out"
  end
  child
end

private def spawn_ack_loss_peer(port : Int32) : Process
  executable = Process.executable_path || raise "cannot resolve stress executable path"
  child = Process.new(
    executable,
    env: {
      "MOVIE_ASSOCIATION_ACK_LOSS_MODE" => "1",
      "MOVIE_ASSOCIATION_PEER_PORT"     => port.to_s,
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  )
  wait_for_peer_line(child, "MOVIE_ASSOCIATION_ACK_LOSS_READY")
  child
end

private def unused_tcp_port : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  server.close
  port
end

# Helper to wait with timeout
def wait_until_stress(timeout_ms : Int32 = 5000, interval_ms : Int32 = 10, &)
  deadline = Time.instant + timeout_ms.milliseconds
  until yield
    raise "Timeout waiting for condition" if Time.instant >= deadline
    sleep(interval_ms.milliseconds)
  end
end

if STRESS_ENABLED
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

        successful_connections.get.should be >= (cycles * 0.9).to_i # 90% success rate

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
          workers = 5
          per_worker = operations // workers
          channels = (0...workers).map do |worker|
            done = Channel(Nil).new(1)
            spawn do
              per_worker.times do |offset|
                i = worker * per_worker + offset
                ref = system.spawn(Movie::Behaviors(String).same)
                path = Movie::ActorPath.new(address, ["user", "temp-#{i}"])

                registry.register(ref, path)
                successful_ops.add(1) if registry.resolve(path) == ref.id

                registry.unregister(ref)
                successful_ops.add(1) if registry.resolve(path).nil?
              end
              done.send(nil)
            end
            done
          end

          channels.each(&.receive)
        end

        expected_ops = operations * 2 # register + unregister verification
        puts "\n  Registration churn: #{operations} actors in #{elapsed.total_milliseconds.round(2)}ms"
        puts "  Rate: #{(operations / elapsed.total_seconds).round(0)} actors/sec"
        puts "  Successful verifications: #{successful_ops.get}/#{expected_ops}"

        successful_ops.get.should eq(expected_ops)
      end
    end

    describe "End-to-end TCP stress" do
      it "retransmits an unacknowledged control frame across a two-process reconnect" do
        port = unused_tcp_port
        peer = spawn_ack_loss_peer(port)
        settings = Movie::Remote::AssociationSettings.new(
          reconnect_min_backoff: 10.milliseconds,
          reconnect_max_backoff: 100.milliseconds,
          reconnect_jitter: 0.0,
          heartbeat_interval: 20.milliseconds,
          heartbeat_timeout: 200.milliseconds
        )
        client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "process-ack-client")
        client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
        target = Movie::ActorPath.new(
          Movie::Address.remote("process-ack-server", "127.0.0.1", port),
          ["user", "control"]
        )
        remote_ref = client_remote.actor_ref(target, String)
        first_generation = remote_ref.connection.generation

        begin
          remote_ref.send_system(Movie::STOP)
          wait_for_peer_line(peer, "MOVIE_ASSOCIATION_CONTROL_ACK_WITHHELD")
          wait_for_peer_line(peer, "MOVIE_ASSOCIATION_CONTROL_RETRANSMITTED")
          wait_until_stress do
            remote_ref.connection.generation > first_generation &&
              remote_ref.connection.pending_control_count == 0
          end
        ensure
          client.shutdown(1.second)
          peer.input.close rescue nil
          status = peer.wait
          status.success?.should be_true
        end
      end

      it "reconnects one remote ref across an abrupt two-process peer restart" do
        port = unused_tcp_port
        first_peer = spawn_association_peer(port)
        second_peer = nil.as(Process?)
        settings = Movie::Remote::AssociationSettings.new(
          reconnect_min_backoff: 10.milliseconds,
          reconnect_max_backoff: 100.milliseconds,
          reconnect_jitter: 0.0,
          heartbeat_interval: 20.milliseconds,
          heartbeat_timeout: 200.milliseconds
        )
        client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "process-chaos-client")
        client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
        target = Movie::ActorPath.new(
          Movie::Address.remote("process-chaos-server", "127.0.0.1", port),
          ["user", "actor"]
        )
        remote_ref = client_remote.actor_ref(target, StressMessage)
        control_path = Movie::ActorPath.new(
          Movie::Address.remote("process-chaos-server", "127.0.0.1", port),
          ["user", "control"]
        )
        control_ref = client_remote.actor_ref(control_path, String)
        watcher = client.spawn(StressWatcher.new, name: "watcher")

        begin
          control_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
          wait_until_stress { control_ref.connection.pending_control_count == 0 }
          first = remote_ref.ask(StressMessage.new(1_i64, "first"), CounterMessage, 1.second).await(2.seconds)
          first.count.should eq(1)
          first_generation = remote_ref.connection.generation

          interrupted = remote_ref.ask(
            StressMessage.new(99_i64, "no-reply"),
            CounterMessage,
            5.seconds
          )
          first_peer.signal(Signal::STOP)
          wait_until_stress { !remote_ref.connection.active? }
          expect_raises(Movie::Remote::RemoteDeliveryError) { interrupted.await(1.second) }

          first_peer.terminate(graceful: false)
          first_peer.wait
          control_ref.send_system(Movie::STOP)

          second_peer = spawn_association_peer(port)
          wait_until_stress do
            remote_ref.connection.active? && remote_ref.connection.generation > first_generation
          end
          wait_for_peer_line(second_peer, "MOVIE_ASSOCIATION_CONTROL_STOPPED")
          wait_until_stress { control_ref.connection.pending_control_count == 0 }
          second = remote_ref.ask(StressMessage.new(2_i64, "second"), CounterMessage, 1.second).await(2.seconds)
          second.count.should eq(2)
        ensure
          client.shutdown(1.second)
          unless first_peer.terminated?
            first_peer.input.close rescue nil
            first_peer.wait
          end
          if peer = second_peer
            peer.input.close rescue nil
            status = peer.wait
            status.success?.should be_true
          end
        end
      end

      it "survives repeated peer restarts with one long-lived remote ref" do
        restart_count = ENV.fetch("MOVIE_ASSOCIATION_CHAOS_RESTARTS", "10").to_i
        Movie::Remote::MessageRegistry.register(StressMessage)
        settings = Movie::Remote::AssociationSettings.new(
          reconnect_min_backoff: 5.milliseconds,
          reconnect_max_backoff: 50.milliseconds,
          reconnect_jitter: 0.0,
          heartbeat_interval: 20.milliseconds,
          heartbeat_timeout: 200.milliseconds
        )
        server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "chaos-server")
        server_remote = server.enable_remoting("127.0.0.1", 0, 1)
        port = server_remote.local_port
        probe = StressDeliveryProbe.new
        server.spawn(probe, name: "actor")
        client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "chaos-client")
        client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
        target = Movie::ActorPath.new(
          Movie::Address.remote("chaos-server", "127.0.0.1", port),
          ["user", "actor"]
        )
        remote_ref = client_remote.actor_ref(target, StressMessage)
        delivered = 0

        begin
          restart_count.times do |cycle|
            wait_until_stress { remote_ref.connection.active? }
            remote_ref << StressMessage.new(cycle.to_i64, "chaos")
            wait_until_stress { probe.received.get == 1 }
            delivered += 1

            server.shutdown(1.second)
            wait_until_stress { !remote_ref.connection.active? }
            next if cycle == restart_count - 1

            server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "chaos-server")
            server.enable_remoting("127.0.0.1", port, 1)
            probe = StressDeliveryProbe.new
            server.spawn(probe, name: "actor")
          end

          puts "\n  Association chaos: #{delivered}/#{restart_count} restart generations delivered"
          delivered.should eq(restart_count)
          remote_ref.connection.stats.successful_connections.should eq(restart_count)
        ensure
          client.shutdown(1.second)
          server.shutdown(1.second)
        end
      end

      it "handles high volume message exchange over TCP" do
        # Server system
        server_system = Movie::ActorSystem(String).new(
          Movie::Behaviors(String).same,
          name: "tcp-server"
        )
        server_ext = server_system.enable_remoting("127.0.0.1", 0)
        server_port = server_ext.local_port
        probe = StressDeliveryProbe.new
        server_system.spawn(probe, name: "actor")

        # Client connects
        client_socket = TCPSocket.new("127.0.0.1", server_port)
        client_socket.tcp_nodelay = true

        num_messages = 1_000
        sent = Atomic(Int32).new(0)
        errors = Atomic(Int32).new(0)

        Movie::Remote::AssociationNegotiator.connect(
          client_socket,
          Movie::Remote::AssociationHandshake.create(
            system: "tcp-client",
            address: "movie.tcp://tcp-client@127.0.0.1:0",
            node_uid: "tcp-client-node",
            association_id: UUID.random.to_s
          ),
          "tcp-server",
          nil
        )

        elapsed = Time.measure do
          num_messages.times do |i|
            payload = JSON::Any.new({"id" => JSON::Any.new(i.to_i64), "payload" => JSON::Any.new("test")})
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
        wait_until_stress { probe.received.get == sent.get }
        probe.ids.sort.should eq((0...sent.get).map(&.to_i64))

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

        Movie::Remote::AssociationNegotiator.connect(
          client_socket,
          Movie::Remote::AssociationHandshake.create(
            system: "bidir-client",
            address: "movie.tcp://bidir-client@127.0.0.1:0",
            node_uid: "bidir-client-node",
            association_id: UUID.random.to_s
          ),
          "bidir-server",
          nil
        )

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
end
