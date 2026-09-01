require "../../spec_helper"
require "../../../src/movie"

record AssociationProbeMessage, value : String do
  include JSON::Serializable
end

private class AssociationProbe < Movie::AbstractBehavior(AssociationProbeMessage)
  def initialize(@deliveries : Channel(String))
  end

  def receive(message : AssociationProbeMessage, context : Movie::ActorContext(AssociationProbeMessage))
    @deliveries.send(message.value)
    Movie::Behaviors(AssociationProbeMessage).same
  end
end

private class AssociationWatcher < Movie::AbstractBehavior(String)
  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end
end

private class AssociationProbeWithStop < Movie::AbstractBehavior(AssociationProbeMessage)
  def initialize(@deliveries : Channel(String), @stopped : Channel(String))
  end

  def receive(message : AssociationProbeMessage, context : Movie::ActorContext(AssociationProbeMessage))
    @deliveries.send(message.value)
    Movie::Behaviors(AssociationProbeMessage).same
  end

  def on_signal(signal : Movie::SystemMessage)
    @stopped.send("stopped") if signal.is_a?(Movie::PostStop)
  end
end

private def wait_for_association(timeout_span : Time::Span = 2.seconds, &block : -> Bool)
  deadline = Time.instant + timeout_span
  until yield
    fail "association condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

describe "Movie remote associations" do
  it "rejects application traffic before a handshake" do
    Movie::Remote::MessageRegistry.register(AssociationProbeMessage)
    deliveries = Channel(String).new(1)
    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "gated-server")
    remote = system.enable_remoting("127.0.0.1", 0)
    system.spawn(AssociationProbe.new(deliveries), name: "probe")
    socket = TCPSocket.new("127.0.0.1", remote.local_port)
    socket.read_timeout = 500.milliseconds

    begin
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://gated-server/user/probe",
        message_type: "AssociationProbeMessage",
        payload: AssociationProbeMessage.new("must-not-arrive")
      )
      Movie::Remote::FrameCodec.encode(envelope, socket)

      rejection = Movie::Remote::FrameCodec.decode(socket)
      rejection.should_not be_nil
      rejection.not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HANDSHAKE_REJECT)

      select
      when value = deliveries.receive
        fail "received pre-handshake message: #{value}"
      when timeout(100.milliseconds)
      end
    ensure
      socket.close rescue nil
      system.shutdown(1.second)
    end
  end

  it "rejects incompatible protocol versions explicitly" do
    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "version-server")
    remote = system.enable_remoting("127.0.0.1", 0)
    socket = TCPSocket.new("127.0.0.1", remote.local_port)
    socket.read_timeout = 500.milliseconds

    begin
      handshake = Movie::Remote::AssociationHandshake.create(
        system: "version-client",
        address: "movie.tcp://version-client@127.0.0.1:9000",
        node_uid: "client-node",
        association_id: "association-1",
        protocol_version: Movie::Remote::PROTOCOL_VERSION + 1
      )
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake(handshake), socket)

      response = Movie::Remote::FrameCodec.decode(socket)
      response.should_not be_nil
      response.not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HANDSHAKE_REJECT)
      response.not_nil!.payload["reason"].as_s.should contain("protocol version")
    ensure
      socket.close rescue nil
      system.shutdown(1.second)
    end
  end

  it "rejects peers without required association capabilities" do
    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "capability-server")
    remote = system.enable_remoting("127.0.0.1", 0)
    socket = TCPSocket.new("127.0.0.1", remote.local_port)
    socket.read_timeout = 500.milliseconds

    begin
      handshake = Movie::Remote::AssociationHandshake.create(
        system: "capability-client",
        address: "movie.tcp://capability-client@127.0.0.1:9000",
        node_uid: "client-node",
        association_id: "association-1",
        capabilities: [] of String
      )
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake(handshake), socket)

      response = Movie::Remote::FrameCodec.decode(socket).not_nil!
      response.kind.handshake_reject?.should be_true
      response.payload["reason"].as_s.should contain("capabilities")
    ensure
      socket.close rescue nil
      system.shutdown(1.second)
    end
  end

  it "reconnects an existing remote ref after the peer restarts" do
    Movie::Remote::MessageRegistry.register(AssociationProbeMessage)
    first_deliveries = Channel(String).new(1)
    second_deliveries = Channel(String).new(1)
    first_server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "restart-server")
    first_remote = first_server.enable_remoting("127.0.0.1", 0, 1)
    port = first_remote.local_port
    first_server.spawn(AssociationProbe.new(first_deliveries), name: "probe")

    settings = Movie::Remote::AssociationSettings.new(
      reconnect_min_backoff: 10.milliseconds,
      reconnect_max_backoff: 50.milliseconds,
      reconnect_jitter: 0.0,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 200.milliseconds
    )
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "restart-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
    target = Movie::ActorPath.new(
      Movie::Address.remote("restart-server", "127.0.0.1", port),
      ["user", "probe"]
    )
    remote_ref = client_remote.actor_ref(target, AssociationProbeMessage)

    second_server = nil.as(Movie::ActorSystem(String)?)
    begin
      remote_ref << AssociationProbeMessage.new("first")
      first_deliveries.receive.should eq("first")
      initial_generation = remote_ref.connection.generation

      first_server.shutdown(1.second)
      wait_for_association { !remote_ref.connection.active? }

      second_server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "restart-server")
      second_server.enable_remoting("127.0.0.1", port, 1)
      second_server.spawn(AssociationProbe.new(second_deliveries), name: "probe")

      wait_for_association { remote_ref.connection.active? && remote_ref.connection.generation > initial_generation }
      remote_ref << AssociationProbeMessage.new("second")
      second_deliveries.receive.should eq("second")

      stats = remote_ref.connection.stats
      stats.state.should eq(Movie::Remote::Connection::State::Active)
      stats.connect_attempts.should be >= 2
      stats.successful_connections.should eq(2)
      stats.disconnects.should be >= 1
      stats.pending_control.should eq(0)
    ensure
      client.shutdown(1.second)
      second_server.try &.shutdown(1.second)
      first_server.shutdown(1.second)
    end
  end

  it "fails an ask from a lost socket generation before its application timeout" do
    Movie::Remote::MessageRegistry.register(AssociationProbeMessage)
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "ask-loss-server")
    server_remote = server.enable_remoting("127.0.0.1", 0, 1)
    server.spawn(AssociationProbe.new(Channel(String).new(1)), name: "probe")
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "ask-loss-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1)
    target = Movie::ActorPath.new(
      Movie::Address.remote("ask-loss-server", "127.0.0.1", server_remote.local_port),
      ["user", "probe"]
    )
    remote_ref = client_remote.actor_ref(target, AssociationProbeMessage)

    begin
      future = remote_ref.ask(AssociationProbeMessage.new("no reply"), AssociationProbeMessage, 5.seconds)
      server.shutdown(1.second)

      started = Time.instant
      expect_raises(Movie::Remote::RemoteDeliveryError) { future.await(1.second) }
      (Time.instant - started).should be < 1.second
    ensure
      client.shutdown(1.second)
      server.shutdown(1.second)
    end
  end

  it "replays acknowledged control traffic but not user traffic after reconnect" do
    Movie::Remote::MessageRegistry.register(AssociationProbeMessage)
    first_server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "replay-server")
    first_remote = first_server.enable_remoting("127.0.0.1", 0, 1)
    port = first_remote.local_port
    first_server.spawn(AssociationProbe.new(Channel(String).new(1)), name: "probe")
    settings = Movie::Remote::AssociationSettings.new(
      reconnect_min_backoff: 10.milliseconds,
      reconnect_max_backoff: 50.milliseconds,
      reconnect_jitter: 0.0,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 200.milliseconds
    )
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "replay-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
    watcher = client.spawn(AssociationWatcher.new, name: "watcher")
    target = Movie::ActorPath.new(
      Movie::Address.remote("replay-server", "127.0.0.1", port),
      ["user", "probe"]
    )
    remote_ref = client_remote.actor_ref(target, AssociationProbeMessage)
    second_server = nil.as(Movie::ActorSystem(String)?)

    begin
      wait_for_association { remote_ref.connection.active? }
      remote_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
      wait_for_association { remote_ref.connection.pending_control_count == 0 }
      first_server.shutdown(1.second)
      wait_for_association { !remote_ref.connection.active? }

      remote_ref << AssociationProbeMessage.new("do-not-replay")
      remote_ref.send_system(Movie::STOP)
      remote_ref.connection.pending_control_count.should eq(1)

      stopped = Channel(String).new(1)
      deliveries = Channel(String).new(1)
      second_server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "replay-server")
      second_server.enable_remoting("127.0.0.1", port, 1)
      second_server.spawn(AssociationProbeWithStop.new(deliveries, stopped), name: "probe")

      select
      when signal = stopped.receive
        signal.should eq("stopped")
      when timeout(2.seconds)
        fail "replayed control message was not delivered after peer restart"
      end
      wait_for_association { remote_ref.connection.pending_control_count == 0 }
      select
      when value = deliveries.receive
        fail "user traffic was replayed after reconnect: #{value}"
      when timeout(100.milliseconds)
      end
    ensure
      client.shutdown(1.second)
      second_server.try &.shutdown(1.second)
      first_server.shutdown(1.second)
    end
  end

  it "deduplicates sequenced control delivery and rejects gaps" do
    deduplicator = Movie::Remote::ControlDeduplicator.new

    deduplicator.observe("node", "stream", 1_i64).should eq(Movie::Remote::ControlObservation::New)
    deduplicator.observe("node", "stream", 1_i64).should eq(Movie::Remote::ControlObservation::Duplicate)
    deduplicator.observe("node", "stream", 3_i64).should eq(Movie::Remote::ControlObservation::Gap)
    deduplicator.observe("node", "stream", 2_i64).should eq(Movie::Remote::ControlObservation::New)
  end

  it "does not acknowledge a control sequence when delivery raises" do
    deduplicator = Movie::Remote::ControlDeduplicator.new

    expect_raises(Exception, "delivery failed") do
      deduplicator.deliver("node", "stream", 1_i64) { raise "delivery failed" }
    end
    delivered = false
    observation = deduplicator.deliver("node", "stream", 1_i64) { delivered = true }

    observation.should eq(Movie::Remote::ControlObservation::New)
    delivered.should be_true
  end

  it "raises explicitly when the pending control buffer is saturated" do
    settings = Movie::Remote::AssociationSettings.new(
      reconnect_min_backoff: 1.second,
      reconnect_max_backoff: 1.second,
      reconnect_jitter: 0.0,
      control_buffer_capacity: 1
    )
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "saturation-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
    target = Movie::ActorPath.new(
      Movie::Address.remote("missing-server", "127.0.0.1", 1),
      ["user", "target"]
    )
    remote_ref = client_remote.actor_ref(target, String)

    begin
      remote_ref.send_system(Movie::STOP)
      expect_raises(Movie::Remote::RemoteDeliveryError, /control buffer is full/) do
        remote_ref.send_system(Movie::STOP)
      end
    ensure
      client.shutdown(1.second)
    end
  end

  it "acknowledges control messages and purges watches when their inbound generation closes" do
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "control-server")
    server_remote = server.enable_remoting("127.0.0.1", 0, 1)
    target_ref = server.spawn(Movie::Behaviors(String).same, name: "target")
    target_context = server.context(target_ref.id).as(Movie::ActorContext(String))
    baseline = target_context.watcher_count

    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "control-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1)
    watcher = client.spawn(AssociationWatcher.new, name: "watcher")
    target = Movie::ActorPath.new(
      Movie::Address.remote("control-server", "127.0.0.1", server_remote.local_port),
      ["user", "target"]
    )
    remote_ref = client_remote.actor_ref(target, String)

    begin
      remote_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
      wait_for_association { target_context.watcher_count == baseline + 1 }
      wait_for_association { remote_ref.connection.pending_control_count == 0 }

      remote_ref.connection.close
      wait_for_association { target_context.watcher_count == baseline }
    ensure
      client.shutdown(1.second)
      server.shutdown(1.second)
    end
  end

  it "authenticates both sides and invokes transport wrapping hooks" do
    client_wraps = Atomic(Int32).new(0)
    server_wraps = Atomic(Int32).new(0)
    server_settings = Movie::Remote::AssociationSettings.new(
      shared_secret: "association-secret",
      server_transport_wrapper: ->(socket : TCPSocket) {
        server_wraps.add(1)
        socket.as(IO)
      }
    )
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "secure-server")
    server_remote = server.enable_remoting("127.0.0.1", 0, 1, server_settings)

    client_settings = Movie::Remote::AssociationSettings.new(
      shared_secret: "association-secret",
      client_transport_factory: ->(host : String, port : Int32) {
        client_wraps.add(1)
        TCPSocket.new(host, port).as(IO)
      }
    )
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "secure-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1, client_settings)
    target = Movie::ActorPath.new(
      Movie::Address.remote("secure-server", "127.0.0.1", server_remote.local_port),
      ["user", "target"]
    )

    begin
      connection = client_remote.actor_ref(target, String).connection
      connection.active?.should be_true
      client_wraps.get.should eq(1)
      server_wraps.get.should eq(1)
    ensure
      client.shutdown(1.second)
      server.shutdown(1.second)
    end
  end

  it "rejects a peer with the wrong shared secret" do
    server_settings = Movie::Remote::AssociationSettings.new(shared_secret: "correct-secret")
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "auth-server")
    server_remote = server.enable_remoting("127.0.0.1", 0, 1, server_settings)
    client_settings = Movie::Remote::AssociationSettings.new(
      shared_secret: "wrong-secret",
      reconnect_min_backoff: 100.milliseconds,
      reconnect_max_backoff: 100.milliseconds,
      reconnect_jitter: 0.0
    )
    client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "auth-client")
    client_remote = client.enable_remoting("127.0.0.1", 0, 1, client_settings)
    target = Movie::ActorPath.new(
      Movie::Address.remote("auth-server", "127.0.0.1", server_remote.local_port),
      ["user", "target"]
    )

    begin
      connection = client_remote.actor_ref(target, String).connection
      connection.active?.should be_false
      connection.generation.should eq(0)
    ensure
      client.shutdown(1.second)
      server.shutdown(1.second)
    end
  end

  it "rejects a captured handshake confirmation against a fresh server challenge" do
    secret = "replay-secret"
    settings = Movie::Remote::AssociationSettings.new(shared_secret: secret)
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "replay-auth-server")
    remote = server.enable_remoting("127.0.0.1", 0, 1, settings)
    first = TCPSocket.new("127.0.0.1", remote.local_port)
    second = nil.as(TCPSocket?)

    begin
      hello = Movie::Remote::AssociationHandshake.create(
        system: "replay-auth-client",
        address: "movie.tcp://replay-auth-client@127.0.0.1:9000",
        node_uid: "captured-node",
        association_id: "captured-association",
        shared_secret: secret,
        nonce: "captured-client-nonce"
      )
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake(hello), first)
      first_ack = Movie::Remote::FrameCodec.decode(first).not_nil!
      first_challenge = Movie::Remote::AssociationHandshake.from_json(first_ack.payload_data.json_source)
      captured_confirmation = Movie::Remote::AssociationConfirmation.create(hello, first_challenge, secret)
      Movie::Remote::FrameCodec.encode(
        Movie::Remote::WireEnvelope.handshake_confirm(captured_confirmation),
        first
      )
      Movie::Remote::FrameCodec.decode(first).not_nil!.kind.handshake_ready?.should be_true
      first.close

      second = TCPSocket.new("127.0.0.1", remote.local_port)
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.handshake(hello), second)
      second_ack = Movie::Remote::FrameCodec.decode(second).not_nil!
      second_challenge = Movie::Remote::AssociationHandshake.from_json(second_ack.payload_data.json_source)
      second_challenge.nonce.should_not eq(first_challenge.nonce)
      Movie::Remote::FrameCodec.encode(
        Movie::Remote::WireEnvelope.handshake_confirm(captured_confirmation),
        second
      )

      rejection = Movie::Remote::FrameCodec.decode(second).not_nil!
      rejection.kind.handshake_reject?.should be_true
      rejection.payload["reason"].as_s.should contain("confirmation")
    ensure
      first.close rescue nil
      second.try &.close
      server.shutdown(1.second)
    end
  end

  it "closes an inbound association when the peer stops heartbeating" do
    settings = Movie::Remote::AssociationSettings.new(
      heartbeat_interval: 10.milliseconds,
      heartbeat_timeout: 50.milliseconds
    )
    server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "heartbeat-server")
    remote = server.enable_remoting("127.0.0.1", 0, 1, settings)
    socket = TCPSocket.new("127.0.0.1", remote.local_port)
    socket.read_timeout = 250.milliseconds

    begin
      handshake = Movie::Remote::AssociationHandshake.create(
        system: "silent-client",
        address: "movie.tcp://silent-client@127.0.0.1:9000",
        node_uid: "silent-node",
        association_id: "silent-association"
      )
      Movie::Remote::AssociationNegotiator.connect(socket, handshake, "heartbeat-server", nil)

      sleep 100.milliseconds
      Movie::Remote::FrameCodec.decode(socket).should be_nil
    ensure
      socket.close rescue nil
      server.shutdown(1.second)
    end
  end
end
