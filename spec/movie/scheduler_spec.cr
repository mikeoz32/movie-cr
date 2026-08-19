require "../spec_helper"
require "../../src/movie"

private def receive_scheduler_events(events : Channel(String), count : Int32, timeout : Time::Span) : Array(String)
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

describe Movie::Scheduler do
  it "executes one-shot callbacks once" do
    events = Channel(String).new(2)
    scheduler = Movie::Scheduler.new(Movie::ConcurrentDispatcher.new)

    scheduler.schedule_once(10.milliseconds) do
      events.send("fired")
    end

    receive_scheduler_events(events, 2, 50.milliseconds).should eq(["fired"])
  end

  it "does not execute cancelled callbacks" do
    events = Channel(String).new(1)
    scheduler = Movie::Scheduler.new(Movie::ConcurrentDispatcher.new)
    handle = scheduler.schedule_once(30.milliseconds) do
      events.send("fired")
    end

    handle.cancel
    handle.cancelled?.should be_true

    receive_scheduler_events(events, 1, 75.milliseconds).should be_empty
  end

  it "isolates exceptions between scheduled callbacks" do
    events = Channel(String).new(1)
    scheduler = Movie::Scheduler.new(Movie::ConcurrentDispatcher.new)

    scheduler.schedule_once(10.milliseconds) do
      raise "scheduler boom"
    end

    scheduler.schedule_once(20.milliseconds) do
      events.send("second-fired")
    end

    receive_scheduler_events(events, 1, 75.milliseconds).should eq(["second-fired"])
  end

  it "does not execute callbacks scheduled after stop" do
    events = Channel(String).new(1)
    scheduler = Movie::Scheduler.new(Movie::ConcurrentDispatcher.new)

    scheduler.stop
    handle = scheduler.schedule_once(10.milliseconds) do
      events.send("fired")
    end

    handle.cancelled?.should be_true
    receive_scheduler_events(events, 1, 50.milliseconds).should be_empty
  end
end
