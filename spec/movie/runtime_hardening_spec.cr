require "../spec_helper"
require "../../src/movie"

private def hardening_eventually(timeout : Time::Span, interval : Time::Span = 10.milliseconds, &block : -> Bool) : Bool
  deadline = Time.instant + timeout
  loop do
    return true if block.call
    return false if Time.instant >= deadline
    sleep interval
  end
end

private class RaiseOnPreStopOnce < Movie::AbstractBehavior(Symbol)
  def initialize
    @raised = Atomic(Bool).new(false)
  end

  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    if signal.is_a?(Movie::PreStop)
      _, should_raise = @raised.compare_and_set(false, true)
      raise "pre-stop failure" if should_raise
    end
  end
end

private class RaiseOnPreStart < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "pre-start failure" if signal.is_a?(Movie::PreStart)
  end
end

private class RaiseOnPostStart < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "post-start failure" if signal.is_a?(Movie::PostStart)
  end
end

private class RaiseOnFailed < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "failed-signal failure" if signal.is_a?(Movie::Failed)
  end
end

private class RaiseOnTerminated < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "terminated-signal failure" if signal.is_a?(Movie::Terminated)
  end
end

private class RaiseOnPostStop < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    raise "post-stop failure" if signal.is_a?(Movie::PostStop)
  end
end

private class RestartSignalProbe < Movie::AbstractBehavior(Symbol)
  def initialize(@starts : Channel(Nil))
    @crashed = Atomic(Bool).new(false)
  end

  def receive(message, context)
    if message == :crash
      raise "receive failure"
    end
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    if signal.is_a?(Movie::PostStart)
      @starts.send(nil)
    end
  end
end

private class RestartPreStartRecoveryProbe < Movie::AbstractBehavior(Symbol)
  getter post_start_count : Atomic(Int32)
  getter recovered : Channel(Nil)

  def initialize
    @fail_next_pre_start = Atomic(Bool).new(false)
    @post_start_count = Atomic(Int32).new(0)
    @recovered = Channel(Nil).new(1)
  end

  def receive(message, context)
    case message
    when :crash
      @fail_next_pre_start.set(true)
      raise "receive failure"
    when :probe
      @recovered.send(nil)
    end
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    case signal
    when Movie::PreStart
      if @fail_next_pre_start.get
        @fail_next_pre_start.set(false)
        raise "restart pre-start failure"
      end
    when Movie::PostStart
      @post_start_count.add(1)
    end
  end
end

private class ResumeProbe < Movie::AbstractBehavior(Symbol)
  def initialize(@probed : Channel(Nil))
  end

  def receive(message, context)
    case message
    when :crash
      raise "resume failure"
    when :probe
      @probed.send(nil)
    end
    Movie::Behaviors(Symbol).same
  end
end

private class BlockingStartProbe < Movie::AbstractBehavior(Symbol)
  getter started : Channel(Nil)
  getter release : Channel(Nil)
  getter post_started : Atomic(Bool)

  def initialize
    @started = Channel(Nil).new(1)
    @release = Channel(Nil).new(1)
    @post_started = Atomic(Bool).new(false)
  end

  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    case signal
    when Movie::PreStart
      @started.send(nil)
      @release.receive
    when Movie::PostStart
      @post_started.set(true)
    end
  end
end

private class SilentProbe < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end
end

private class RefAskProbe < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    Movie::Ask.success(context.sender, "reply") if message == :request
    Movie::Behaviors(Symbol).same
  end
end

private class BlockingStopProbe < Movie::AbstractBehavior(Symbol)
  getter started : Channel(Nil)
  getter release : Channel(Nil)

  def initialize
    @started = Channel(Nil).new(1)
    @release = Channel(Nil).new(1)
  end

  def receive(message, context)
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    if signal.is_a?(Movie::PreStop)
      @started.send(nil)
      @release.receive
    end
  end
end

private class QueueStopProbe < Movie::AbstractBehavior(Symbol)
  getter started : Channel(Nil)
  getter release : Channel(Nil)
  getter received : Atomic(Int32)

  def initialize
    @started = Channel(Nil).new(1)
    @release = Channel(Nil).new(1)
    @received = Atomic(Int32).new(0)
  end

  def receive(message, context)
    case message
    when :block
      @started.send(nil)
      @release.receive
    when :work
      @received.add(1)
    end
    Movie::Behaviors(Symbol).same
  end
end

private class CountingExtension < Movie::Extension
  getter stop_count : Atomic(Int32)

  def initialize
    @stop_count = Atomic(Int32).new(0)
  end

  def stop
    @stop_count.add(1)
  end
end

private class SelfShutdownProbe < Movie::AbstractBehavior(Symbol)
  def initialize(@events : Channel(Nil))
    @system = nil.as(Movie::AbstractActorSystem?)
  end

  def system=(system : Movie::AbstractActorSystem)
    @system = system
  end

  def receive(message, context)
    if message == :shutdown
      @system.not_nil!.shutdown(500.milliseconds)
      @events.send(nil)
    end
    Movie::Behaviors(Symbol).same
  end
end

private class EscalatingChild < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    raise "nested failure" if message == :crash
    Movie::Behaviors(Symbol).same
  end
end

private class EscalatingMiddle < Movie::AbstractBehavior(Symbol)
  def initialize(@child : Channel(Movie::ActorRef(Symbol)))
  end

  def receive(message, context)
    if message == :spawn
      @child.send(context.spawn(EscalatingChild.new, name: "nested-child"))
    end
    Movie::Behaviors(Symbol).same
  end
end

private class EscalationRoot < Movie::AbstractBehavior(Symbol)
  def initialize(@middle : Channel(Movie::ActorRef(Symbol)), @failure : Channel(Int32))
  end

  def receive(message, context)
    if message == :spawn
      middle_supervision = Movie::SupervisionConfig.new(
        strategy: Movie::SupervisionStrategy::ESCALATE,
        scope: Movie::SupervisionScope::ONE_FOR_ONE,
        max_restarts: 3,
        within: 1.second
      )
      @middle.send(context.spawn(EscalatingMiddle.new(@middle), Movie::RestartStrategy::RESTART, middle_supervision, "middle"))
    end
    Movie::Behaviors(Symbol).same
  end

  def on_signal(signal : Movie::SystemMessage)
    @failure.send(signal.actor.id) if signal.is_a?(Movie::Failed)
  end
end

describe "Movie runtime hardening" do
  it "recovers the mailbox when PreStart raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnPreStart.new, Movie::RestartStrategy::STOP)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "recovers the mailbox when PostStart raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnPostStart.new, Movie::RestartStrategy::STOP)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "recovers the mailbox when a Failed signal handler raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnFailed.new, Movie::RestartStrategy::STOP)

    actor.send_system(Movie::Failed.new(actor.as(Movie::ActorRefBase), Exception.new("synthetic failure")))

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "recovers the mailbox when a Terminated signal handler raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnTerminated.new, Movie::RestartStrategy::STOP)

    actor.send_system(Movie::Terminated.new(actor.as(Movie::ActorRefBase)))

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "keeps the mailbox recoverable when a lifecycle hook raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnPreStopOnce.new)

    actor.send_system(Movie::STOP)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "deregisters an actor when PostStop raises" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(RaiseOnPostStop.new)

    actor.send_system(Movie::STOP)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    system.shutdown(1.second)
  end

  it "delivers PostStart once across a restart" do
    starts = Channel(Nil).new(4)
    supervision = Movie::SupervisionConfig.new(
      strategy: Movie::SupervisionStrategy::RESTART,
      scope: Movie::SupervisionScope::ONE_FOR_ONE,
      max_restarts: 3,
      within: 1.second,
      backoff_min: Time::Span.zero,
      backoff_max: Time::Span.zero,
      backoff_factor: 1.0,
      jitter: 0.0
    )
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same, Movie::RestartStrategy::RESTART, supervision)
    actor = system.spawn(RestartSignalProbe.new(starts))

    starts.receive
    actor << :crash
    starts.receive

    select
    when starts.receive
      fail "restart delivered PostStart more than once"
    when timeout(100.milliseconds)
    end

    system.shutdown
  end

  it "recovers on a later restart after a restart PreStart failure" do
    probe = RestartPreStartRecoveryProbe.new
    supervision = Movie::SupervisionConfig.new(
      strategy: Movie::SupervisionStrategy::RESTART,
      scope: Movie::SupervisionScope::ONE_FOR_ONE,
      max_restarts: 3,
      within: 1.second,
      backoff_min: 20.milliseconds,
      backoff_max: 20.milliseconds,
      backoff_factor: 1.0,
      jitter: 0.0
    )
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same, Movie::RestartStrategy::RESTART, supervision)
    actor = system.spawn(probe)

    hardening_eventually(1.second) { probe.post_start_count.get == 1 }.should be_true
    actor << :crash
    hardening_eventually(1.second) { probe.post_start_count.get == 2 }.should be_true
    actor << :probe

    select
    when probe.recovered.receive
    when timeout(1.second)
      fail "actor did not recover after the second restart attempt"
    end

    probe.post_start_count.get.should eq(2)
    system.shutdown
  end

  it "resumes a failed actor when supervision strategy is RESUME" do
    probed = Channel(Nil).new(1)
    supervision = Movie::SupervisionConfig.new(
      strategy: Movie::SupervisionStrategy::RESUME,
      scope: Movie::SupervisionScope::ONE_FOR_ONE,
      max_restarts: 3,
      within: 1.second
    )
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same, Movie::RestartStrategy::RESTART, supervision)
    actor = system.spawn(ResumeProbe.new(probed))

    actor << :crash
    actor << :probe

    select
    when probed.receive
    when timeout(1.second)
      fail "RESUME left the actor failed"
    end

    system.shutdown
  end

  it "does not run PostStart after stop wins during PreStart" do
    probe = BlockingStartProbe.new
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(probe)

    probe.started.receive
    actor.send_system(Movie::STOP)
    probe.release.send(nil)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    probe.post_started.get.should be_false
    system.shutdown
  end

  it "prioritizes STOP over user messages queued behind the current message" do
    probe = QueueStopProbe.new
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    actor = system.spawn(probe)

    actor << :block
    probe.started.receive
    1_000.times { actor << :work }
    actor.send_system(Movie::STOP)
    probe.release.send(nil)

    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
    probe.received.get.should be < 1_000
    system.shutdown
  end

  it "stops ask listeners after the target terminates" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    target = system.spawn(SilentProbe.new)
    future = system.ask(target, :request, Nil, 1.second)

    target.send_system(Movie::STOP)
    expect_raises(Movie::Ask::TargetTerminated) { future.await(1.second) }

    system.shutdown(1.second)
  end

  it "supports ask directly on a local ActorRef" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    target = system.spawn(RefAskProbe.new)

    target.ask(:request, String, 1.second).await(1.second).should eq("reply")
    system.shutdown
  end

  it "fails an ask cleanly when the local target is already terminated" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    target = system.spawn(SilentProbe.new)
    target.send_system(Movie::STOP)
    hardening_eventually(1.second) { system.context(target.id).nil? }.should be_true

    future = target.ask(:request, String, 1.second)
    expect_raises(Movie::Ask::TargetTerminated) { future.await(1.second) }
    system.shutdown
  end

  it "does not block when shutdown is called from an actor" do
    events = Channel(Nil).new(1)
    probe = SelfShutdownProbe.new(events)
    system = Movie::ActorSystem(Symbol).new(probe)
    probe.system = system

    system << :shutdown

    select
    when events.receive
    when timeout(1.second)
      fail "actor-side shutdown blocked its mailbox"
    end
  end

  it "rejects new actors after shutdown begins" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    system.shutdown

    expect_raises(Movie::ActorSystemShuttingDownError) do
      system.spawn(Movie::Behaviors(Symbol).same)
    end
  end

  it "keeps extensions alive when actor shutdown times out" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    probe = BlockingStopProbe.new
    actor = system.spawn(probe)
    extension = CountingExtension.new
    system.register_extension(extension)

    actor.send_system(Movie::STOP)
    probe.started.receive
    expect_raises(Exception) { system.shutdown(20.milliseconds) }
    extension.stop_count.get.should eq(0)

    probe.release.send(nil)
    hardening_eventually(1.second) { system.context(actor.id).nil? }.should be_true
  end

  it "escalates a nested child failure to the direct supervisor" do
    middle = Channel(Movie::ActorRef(Symbol)).new(1)
    failure = Channel(Int32).new(1)
    stopping = Movie::SupervisionConfig.new(
      strategy: Movie::SupervisionStrategy::STOP,
      scope: Movie::SupervisionScope::ONE_FOR_ONE,
      max_restarts: 3,
      within: 1.second
    )
    system = Movie::ActorSystem(Symbol).new(EscalationRoot.new(middle, failure), Movie::RestartStrategy::RESTART, stopping)

    system << :spawn
    supervisor = middle.receive
    supervisor << :spawn
    child = middle.receive
    child << :crash

    failure.receive.should eq(supervisor.id)
    hardening_eventually(1.second) { system.context(supervisor.id).nil? }.should be_true
    system.shutdown
  end
end
