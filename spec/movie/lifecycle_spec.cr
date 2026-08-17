require "../spec_helper"
require "../../src/movie"

private def boot_actor_system_stdout : {Process::Status, String}
  stdout = IO::Memory.new
  root = File.expand_path("../..", __DIR__)
  code = <<-'CR'
    require "./src/movie"

    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same)
    sleep 50.milliseconds
  CR

  status = Process.run(
    "crystal",
    ["eval", "-Dpreview_mt", "-Dexecution_context", code],
    chdir: root,
    output: stdout,
    error: Process::Redirect::Close
  )

  {status, stdout.to_s}
end

private def eventually(timeout : Time::Span, interval : Time::Span = 10.milliseconds, &block : -> Bool) : Bool
  deadline = Time.instant + timeout

  loop do
    return true if block.call
    return false if Time.instant >= deadline
    sleep interval
  end
end

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

private class FailingOnStart < Movie::AbstractBehavior(String)
  def receive(message, context)
    Movie::Behaviors(String).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "boom during pre-start" if signal.is_a?(Movie::PreStart)
  end
end

private class StartupFailureObserver < Movie::AbstractBehavior(Symbol)
  def initialize(@events : Channel(String))
    @spawn_count = 0
  end

  def receive(message, context)
    case message
    when :spawn_failing_child
      child_name = "failing-child-#{@spawn_count}"
      @spawn_count += 1
      context.spawn(FailingOnStart.new, name: child_name)
    end

    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    case signal
    when Movie::Failed
      @events.send("failed:#{signal.actor.id}")
    when Movie::Terminated
      @events.send("terminated:#{signal.actor.id}")
    end
  end
end

private class RestartBackoffChild < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    raise "boom during receive" if message == :crash
    Movie::Behaviors(Symbol).same
  end
end

private class RestartBackoffObserver < Movie::AbstractBehavior(Symbol)
  def initialize(@events : Channel(String))
    @child = nil.as(Movie::ActorRef(Symbol)?)
  end

  def receive(message, context)
    case message
    when :spawn_child
      @child = context.spawn(RestartBackoffChild.new, name: "restart-backoff-child")
      @events.send("child_spawned")
    when :crash_child
      @child.not_nil! << :crash
    when :probe
      @events.send("probe_processed")
    end

    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    @events.send("child_failed") if signal.is_a?(Movie::Failed)
  end
end

describe "Movie actor lifecycle" do
  it "does not write guardian lifecycle noise to stdout during boot" do
    status, stdout = boot_actor_system_stdout

    status.success?.should be_true
    stdout.should eq("")
  end

  it "does not keep startup-failed actors registered indefinitely" do
    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same)
    ref = system.spawn(FailingOnStart.new, name: "failing-on-start")

    eventually(500.milliseconds) { system.context(ref.id).nil? }.should be_true
  end

  it "notifies parent watchers when child startup fails" do
    events = Channel(String).new(2)
    system = Movie::ActorSystem(Symbol).new(StartupFailureObserver.new(events))

    system << :spawn_failing_child

    events.receive.should start_with("failed:")
    events.receive.should start_with("terminated:")
  end

  it "delivers failure and termination notifications for each startup-failed child" do
    events = Channel(String).new(40)
    system = Movie::ActorSystem(Symbol).new(StartupFailureObserver.new(events))

    20.times do
      system << :spawn_failing_child
    end

    received = receive_events(events, 40, 2.seconds)

    received.count(&.starts_with?("failed:")).should eq(20)
    received.count(&.starts_with?("terminated:")).should eq(20)
  end

  it "does not block unrelated messages during supervision backoff" do
    events = Channel(String).new(3)
    supervision = Movie::SupervisionConfig.new(
      strategy: Movie::SupervisionStrategy::RESTART,
      scope: Movie::SupervisionScope::ONE_FOR_ONE,
      max_restarts: 3,
      within: 1.second,
      backoff_min: 200.milliseconds,
      backoff_max: 200.milliseconds,
      backoff_factor: 1.0,
      jitter: 0.0
    )
    system = Movie::ActorSystem(Symbol).new(RestartBackoffObserver.new(events), Movie::RestartStrategy::RESTART, supervision)

    system << :spawn_child
    events.receive.should eq("child_spawned")

    system << :crash_child
    events.receive.should eq("child_failed")

    system << :probe

    receive_events(events, 1, 75.milliseconds).should eq(["probe_processed"])
  end
end
