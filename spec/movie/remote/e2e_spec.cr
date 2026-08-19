require "../../spec_helper"
require "../../../src/movie"

record RemoteDeliveryMessage, body : String do
  include JSON::Serializable
end

record RemoteAskRequest, body : String do
  include JSON::Serializable
end

record RemoteAskResponse, body : String do
  include JSON::Serializable
end

private class RemoteDeliveryProbe < Movie::AbstractBehavior(RemoteDeliveryMessage)
  def initialize(@deliveries : Channel(RemoteDeliveryMessage))
  end

  def receive(message : RemoteDeliveryMessage, context : Movie::ActorContext(RemoteDeliveryMessage))
    @deliveries.send(message)
    Movie::Behaviors(RemoteDeliveryMessage).same
  end
end

private class RemoteSenderProbe < Movie::AbstractBehavior(RemoteDeliveryMessage)
  def initialize(@senders : Channel(Movie::ActorPath?))
  end

  def receive(message : RemoteDeliveryMessage, context : Movie::ActorContext(RemoteDeliveryMessage))
    @senders.send(context.sender.try &.path)
    Movie::Behaviors(RemoteDeliveryMessage).same
  end
end

private class RemoteAskProbe < Movie::AbstractBehavior(RemoteAskRequest)
  def receive(message : RemoteAskRequest, context : Movie::ActorContext(RemoteAskRequest))
    Movie::Ask.success(context.sender, RemoteAskResponse.new("reply: #{message.body}"))
    Movie::Behaviors(RemoteAskRequest).same
  end
end

private class RemoteNoReplyProbe < Movie::AbstractBehavior(RemoteAskRequest)
  def receive(message : RemoteAskRequest, context : Movie::ActorContext(RemoteAskRequest))
    Movie::Behaviors(RemoteAskRequest).same
  end
end

private class RemoteReplyIfAskedProbe < Movie::AbstractBehavior(RemoteAskRequest)
  def receive(message : RemoteAskRequest, context : Movie::ActorContext(RemoteAskRequest))
    Movie::Ask.reply_if_asked(context.sender, RemoteAskResponse.new("helper: #{message.body}"))
    Movie::Behaviors(RemoteAskRequest).same
  end
end

private class RemoteStopProbe < Movie::AbstractBehavior(String)
  def initialize(@stopped : Channel(String))
  end

  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end

  def on_signal(signal : Movie::SystemMessage)
    case signal
    when Movie::PostStop
      @stopped.send("post_stop")
    end
  end
end

private class RemoteTerminationWatcher < Movie::AbstractBehavior(String)
  def initialize(@terminated : Channel(Movie::ActorPath))
  end

  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end

  def on_signal(signal : Movie::SystemMessage)
    case signal
    when Movie::Terminated
      path = signal.actor.path
      @terminated.send(path) if path
    end
  end
end

describe "Movie Remote E2E" do
  it "delivers a user message to a remote actor" do
    Movie::Remote::MessageRegistry.register(RemoteDeliveryMessage)

    deliveries = Channel(RemoteDeliveryMessage).new(1)
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "remote-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteDeliveryProbe.new(deliveries), name: "probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "remote-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "probe"]
      )

      client_remote.actor_ref(target_path, RemoteDeliveryMessage) << RemoteDeliveryMessage.new("hello over tcp")

      received = nil.as(RemoteDeliveryMessage?)
      select
      when message = deliveries.receive
        received = message
      when timeout(1.second)
        fail "expected remote actor to receive the message"
      end

      received.not_nil!.body.should eq("hello over tcp")
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "preserves sender path metadata for remote user messages" do
    Movie::Remote::MessageRegistry.register(RemoteDeliveryMessage)

    senders = Channel(Movie::ActorPath?).new(1)
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "sender-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteSenderProbe.new(senders), name: "probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "sender-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)
      sender_actor = client_system.spawn(Movie::Behaviors(String).same, name: "sender")

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "probe"]
      )

      client_remote.actor_ref(target_path, RemoteDeliveryMessage)
        .tell_from(sender_actor, RemoteDeliveryMessage.new("with sender metadata"))

      sender_path = nil.as(Movie::ActorPath?)
      select
      when path = senders.receive
        sender_path = path
      when timeout(1.second)
        fail "expected remote actor to observe sender metadata"
      end

      sender_path.should_not be_nil
      sender_path.not_nil!.address.system.should eq(client_system.name)
      sender_path.not_nil!.elements.should eq(["user", "sender"])
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "supports remote ask request-response flow" do
    Movie::Remote::MessageRegistry.register(RemoteAskRequest)
    Movie::Remote::MessageRegistry.register(RemoteAskResponse)

    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "ask-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteAskProbe.new, name: "ask-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "ask-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "ask-probe"]
      )

      response = client_remote
        .actor_ref(target_path, RemoteAskRequest)
        .ask(RemoteAskRequest.new("hello"), RemoteAskResponse, 200.milliseconds)
        .await(500.milliseconds)

      response.body.should eq("reply: hello")
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "fails remote ask when the connection closes before a reply arrives" do
    Movie::Remote::MessageRegistry.register(RemoteAskRequest)
    Movie::Remote::MessageRegistry.register(RemoteAskResponse)

    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "disconnect-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteNoReplyProbe.new, name: "silent-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "disconnect-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "silent-probe"]
      )

      future = client_remote
        .actor_ref(target_path, RemoteAskRequest)
        .ask(RemoteAskRequest.new("hello"), RemoteAskResponse, 5.seconds)

      server_remote.stop

      expect_raises(Movie::Remote::RemoteDeliveryError, /Connection closed while waiting for response/) do
        future.await(1.second)
      end
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "supports remote ask helpers that use reply_if_asked" do
    Movie::Remote::MessageRegistry.register(RemoteAskRequest)
    Movie::Remote::MessageRegistry.register(RemoteAskResponse)

    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "reply-if-asked-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteReplyIfAskedProbe.new, name: "helper-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "reply-if-asked-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "helper-probe"]
      )

      response = client_remote
        .actor_ref(target_path, RemoteAskRequest)
        .ask(RemoteAskRequest.new("hello"), RemoteAskResponse, 200.milliseconds)
        .await(500.milliseconds)

      response.body.should eq("helper: hello")
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "delivers stop system messages to a remote actor" do
    stopped = Channel(String).new(1)
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "stop-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteStopProbe.new(stopped), name: "stop-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "stop-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "stop-probe"]
      )

      client_remote.actor_ref(target_path, String).send_system(Movie::STOP)

      stopped_signal = nil.as(String?)
      select
      when signal = stopped.receive
        stopped_signal = signal
      when timeout(1.second)
        fail "expected remote actor to receive stop signal"
      end

      stopped_signal.should eq("post_stop")
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "delivers terminated notifications to a remote watcher" do
    terminated = Channel(Movie::ActorPath).new(1)
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "watch-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteStopProbe.new(Channel(String).new(1)), name: "watched-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "watch-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)
      watcher = client_system.spawn(RemoteTerminationWatcher.new(terminated), name: "watcher")

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "watched-probe"]
      )
      remote_ref = client_remote.actor_ref(target_path, String)

      remote_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
      remote_ref.send_system(Movie::STOP)

      received_path = nil.as(Movie::ActorPath?)
      select
      when path = terminated.receive
        received_path = path
      when timeout(1.second)
        fail "expected remote watcher to receive terminated signal"
      end

      received_path.should eq(target_path)
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "stops terminated notifications after remote unwatch" do
    terminated = Channel(Movie::ActorPath).new(1)
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "unwatch-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(RemoteStopProbe.new(Channel(String).new(1)), name: "watched-probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "unwatch-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)
      watcher = client_system.spawn(RemoteTerminationWatcher.new(terminated), name: "watcher")

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "watched-probe"]
      )
      remote_ref = client_remote.actor_ref(target_path, String)

      remote_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
      remote_ref.send_system(Movie::Unwatch.new(watcher).as(Movie::SystemMessage))
      remote_ref.send_system(Movie::STOP)

      select
      when path = terminated.receive
        fail "expected no terminated signal after unwatch, got #{path}"
      when timeout(300.milliseconds)
      end
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end

  it "raises for unsupported remote lifecycle system messages" do
    server_system = nil.as(Movie::ActorSystem(String)?)
    client_system = nil.as(Movie::ActorSystem(String)?)

    begin
      server_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "unsupported-server"
      )
      server_remote = server_system.enable_remoting("127.0.0.1", 0)
      server_system.spawn(Movie::Behaviors(String).same, name: "probe")

      client_system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "unsupported-client"
      )
      client_remote = client_system.enable_remoting("127.0.0.1", 0)

      target_path = Movie::ActorPath.new(
        Movie::Address.remote(server_system.name, "127.0.0.1", server_remote.local_port),
        ["user", "probe"]
      )

      expect_raises(Movie::Remote::RemoteUnsupportedSystemMessageError, /Movie::PreStart/) do
        client_remote.actor_ref(target_path, String).send_system(Movie::PRE_START)
      end
    ensure
      client_system.try &.shutdown(1.second)
      server_system.try &.shutdown(1.second)
    end
  end
end
