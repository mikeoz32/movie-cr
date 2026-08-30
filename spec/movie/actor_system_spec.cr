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

private class BlockingStartExtension < Movie::Extension
  @@start_entered : Channel(Nil) = Channel(Nil).new(1)
  @@start_release : Channel(Nil) = Channel(Nil).new(1)

  getter started : Atomic(Bool) = Atomic(Bool).new(false)
  getter stop_count : Atomic(Int32) = Atomic(Int32).new(0)

  def self.reset
    @@start_entered = Channel(Nil).new(1)
    @@start_release = Channel(Nil).new(1)
  end

  def self.wait_until_starting
    @@start_entered.receive
  end

  def self.release_start
    @@start_release.send(nil)
  end

  def start
    @@start_entered.send(nil)
    @@start_release.receive
    @started.set(true)
  end

  def stop
    @stop_count.add(1)
  end
end

private class BlockingStartExtensionId < Movie::ExtensionId(BlockingStartExtension)
  def create(system : Movie::AbstractActorSystem) : BlockingStartExtension
    BlockingStartExtension.new
  end
end

describe Movie::ActorSystem do
  it "does not expose a lazy extension before start completes" do
    BlockingStartExtension.reset
    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same)
    first_result = Channel(BlockingStartExtension).new(1)
    second_result = Channel(BlockingStartExtension).new(1)

    spawn { first_result.send(BlockingStartExtensionId.get(system)) }
    BlockingStartExtension.wait_until_starting
    spawn { second_result.send(BlockingStartExtensionId.get(system)) }

    second_completed_early = false
    select
    when second = second_result.receive
      second_completed_early = !second.started.get
    when timeout(50.milliseconds)
    end

    BlockingStartExtension.release_start
    first = first_result.receive
    second = second_completed_early ? system.extension!(BlockingStartExtension) : second_result.receive

    second_completed_early.should be_false
    second.should be(first)
    second.started.get.should be_true
  ensure
    BlockingStartExtension.release_start rescue nil
    system.try &.shutdown
  end

  it "rejects an extension whose startup finishes after registry shutdown" do
    BlockingStartExtension.reset
    registry = Movie::ExtensionRegistry.new
    extension = BlockingStartExtension.new
    result = Channel(Exception?).new(1)

    spawn do
      begin
        registry.get_or_register(BlockingStartExtension) { extension }
        result.send(nil)
      rescue ex
        result.send(ex)
      end
    end

    BlockingStartExtension.wait_until_starting
    registry.stop_all
    BlockingStartExtension.release_start

    error = result.receive
    error.should be_a(Movie::ActorSystemShuttingDownError)
    registry.registered?(BlockingStartExtension).should be_false
    extension.stop_count.get.should eq(1)
  ensure
    BlockingStartExtension.release_start rescue nil
    registry.try &.stop_all
  end

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

  it "stops actors before extensions and is idempotent" do
    events = Channel(String).new(8)
    system = Movie::ActorSystem(Symbol).new(ShutdownRootProbe.new(events))
    extension = ShutdownRecordingExtension.new(events)
    system.register_extension(extension)

    system << :spawn_child
    events.receive.should eq("child_spawned")

    system.shutdown

    received = receive_events(events, 3, 1.second)
    received.should contain("child_pre_stop")
    received.should contain("root_pre_stop")
    extension_index = received.index("extension_stop").not_nil!
    received.index("child_pre_stop").not_nil!.should be < extension_index
    received.index("root_pre_stop").not_nil!.should be < extension_index
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
end
