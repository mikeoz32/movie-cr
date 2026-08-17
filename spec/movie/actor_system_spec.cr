require "../spec_helper"
require "../../src/movie"

private def receive_events(events : Channel(String), count : Int32, timeout : Time::Span) : Array(String)
  deadline = Time.instant + timeout
  received = [] of String

  while received.size < count
    remaining = deadline - Time.instant
    break if remaining <= Time::Span.zero

    select
    when event = events.receive
      received << event
    when timeout(remaining)
      break
    end
  end

  received
end

private class EchoRoot < Movie::AbstractBehavior(String)
  def receive(message, context)
    Movie::Ask.success(context.sender, "echo: #{message}")
    Movie::Behaviors(String).same
  end
end

private class SenderProbe < Movie::AbstractBehavior(String)
  def initialize(@channel : Channel(Int32?))
  end

  def receive(message, context)
    @channel.send(context.sender.try &.id)
    Movie::Behaviors(String).same
  end
end

private class ShutdownChildProbe < Movie::AbstractBehavior(Symbol)
  def initialize(@events : Channel(String))
  end

  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    @events.send("child_pre_stop") if signal.is_a?(Movie::PreStop)
  end
end

private class ShutdownRootProbe < Movie::AbstractBehavior(Symbol)
  def initialize(@events : Channel(String))
    @spawned = false
  end

  def receive(message, context)
    case message
    when :spawn_child
      unless @spawned
        context.spawn(ShutdownChildProbe.new(@events), name: "shutdown-child")
        @spawned = true
        @events.send("child_spawned")
      end
    end

    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    @events.send("root_pre_stop") if signal.is_a?(Movie::PreStop)
  end
end

private class ShutdownRecordingExtension < Movie::Extension
  getter stop_count : Atomic(Int32)

  def initialize(@events : Channel(String))
    @stop_count = Atomic(Int32).new(0)
  end

  def stop
    @stop_count.add(1)
    @events.send("extension_stop")
  end
end

describe Movie::ActorSystem do
  it "supports ask on the system root" do
    system = Movie::ActorSystem(String).new(EchoRoot.new)
    result = system.ask("hi", String).await(1.second)
    result.should eq("echo: hi")
  end

  it "uses dead letters as sender for external tell" do
    channel = Channel(Int32?).new(1)

    system = Movie::ActorSystem(String).new(EchoRoot.new)
    probe = system.spawn(SenderProbe.new(channel))

    probe << "ping"

    sender_id = channel.receive
    sender_id.should eq(system.dead_letters.id)
  end

  it "shuts down extensions before stopping actors and is idempotent" do
    events = Channel(String).new(8)
    system = Movie::ActorSystem(Symbol).new(ShutdownRootProbe.new(events))
    extension = ShutdownRecordingExtension.new(events)
    system.register_extension(extension)

    system << :spawn_child
    events.receive.should eq("child_spawned")

    system.shutdown

    received = receive_events(events, 3, 1.second)
    received.first?.should eq("extension_stop")
    received.should contain("child_pre_stop")
    received.should contain("root_pre_stop")
    extension.stop_count.get.should eq(1)

    system.shutdown
    extension.stop_count.get.should eq(1)
  end

  it "cancels pending scheduler callbacks during shutdown" do
    events = Channel(String).new(1)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)

    system.scheduler.schedule_once(200.milliseconds) do
      events.send("timer_fired")
    end

    system.shutdown

    receive_events(events, 1, 300.milliseconds).should be_empty
  end

  it "allows resume supervision without using it as the root restart strategy" do
    config = Movie::ActorSystemConfig.default.with_override(
      Movie::Config.builder.set("supervision.strategy", "resume").build
    )

    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same, config)
    system.shutdown
  end
end
