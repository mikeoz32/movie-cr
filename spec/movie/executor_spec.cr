require "../spec_helper"
require "../../src/movie"

private class ExecutorSpecError < Exception
end

private def receive_replies(
  replies : Channel(Movie::ExecutorExtension::TaskReply(Int32)),
  count : Int32,
  timeout : Time::Span,
) : Array(Movie::ExecutorExtension::TaskReply(Int32))
  deadline = Time.instant + timeout
  received = [] of Movie::ExecutorExtension::TaskReply(Int32)

  while received.size < count
    remaining = deadline - Time.instant
    break if remaining <= Time::Span.zero

    select
    when reply = replies.receive
      received << reply
    when timeout(remaining)
      break
    end
  end

  received
end

private class ExecutorReplyProbe < Movie::AbstractBehavior(Movie::ExecutorExtension::TaskReply(Int32))
  def initialize(@replies : Channel(Movie::ExecutorExtension::TaskReply(Int32)))
  end

  def receive(message, context)
    @replies.send(message)
    Movie::Behaviors(Movie::ExecutorExtension::TaskReply(Int32)).same
  end
end

describe Movie::ExecutorExtension do
  it "delivers typed success replies" do
    replies = Channel(Movie::ExecutorExtension::TaskReply(Int32)).new(1)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::Execution.get(system)
    reply_to = system.spawn(ExecutorReplyProbe.new(replies), name: "executor-success-probe")

    executor.execute_with_reply(reply_to, timeout: 100.milliseconds) { 42 }

    reply = replies.receive
    reply.should be_a(Movie::ExecutorExtension::TaskSuccess(Int32))
    reply.as(Movie::ExecutorExtension::TaskSuccess(Int32)).value.should eq(42)
  end

  it "delivers timeout as a typed failure only once" do
    replies = Channel(Movie::ExecutorExtension::TaskReply(Int32)).new(2)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::Execution.get(system)
    reply_to = system.spawn(ExecutorReplyProbe.new(replies), name: "executor-timeout-probe")

    executor.execute_with_reply(reply_to, timeout: 20.milliseconds) do
      sleep 100.milliseconds
      7
    end

    reply = replies.receive
    reply.should be_a(Movie::ExecutorExtension::TaskFailure(Int32))
    reply.as(Movie::ExecutorExtension::TaskFailure(Int32)).error.should be_a(Movie::FutureTimeout)

    receive_replies(replies, 1, 150.milliseconds).should be_empty
  end

  it "blocks submission while the bounded queue is saturated" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::ExecutorExtension.new(system, 1, 1)
    first_started = Channel(Nil).new(1)
    release_first = Channel(Nil).new(1)
    third_submitted = Channel(Movie::Future(Int32)).new(1)

    first = executor.execute do
      first_started.send(nil)
      release_first.receive
      1
    end
    first_started.receive

    second = executor.execute { 2 }

    spawn do
      third = executor.execute { 3 }
      third_submitted.send(third)
    end

    select
    when _future = third_submitted.receive
      fail "expected third submission to block while queue is full"
    when timeout(50.milliseconds)
    end

    release_first.send(nil)

    third = third_submitted.receive
    first.await(100.milliseconds).should eq(1)
    second.await(100.milliseconds).should eq(2)
    third.await(100.milliseconds).should eq(3)
  end

  it "starts the configured worker pool only once under concurrent first submissions" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::ExecutorExtension.new(system, 1, 64)
    ready = Channel(Nil).new(32)
    start = Channel(Nil).new(32)
    submitted = Channel(Movie::Future(Int32)).new(32)
    active = Atomic(Int32).new(0)
    maximum_active = Atomic(Int32).new(0)

    32.times do
      spawn do
        ready.send(nil)
        start.receive
        submitted.send(executor.execute do
          current = active.add(1) + 1
          loop do
            previous = maximum_active.get
            break if previous >= current
            _, updated = maximum_active.compare_and_set(previous, current)
            break if updated
          end
          sleep 10.milliseconds
          active.add(-1)
          1
        end)
      end
    end

    32.times { ready.receive }
    32.times { start.send(nil) }

    futures = Array(Movie::Future(Int32)).new(32)
    32.times { futures << submitted.receive }
    futures.each(&.await(2.seconds))

    maximum_active.get.should eq(1)
  end

  it "reports task exceptions through futures and keeps workers alive" do
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::ExecutorExtension.new(system, 1, 2)

    failed = executor.execute do
      raise ExecutorSpecError.new("boom")
      0
    end

    expect_raises(ExecutorSpecError, "boom") do
      failed.await(100.milliseconds)
    end

    executor.execute { 9 }.await(100.milliseconds).should eq(9)
  end

  it "reports task exceptions through typed replies" do
    replies = Channel(Movie::ExecutorExtension::TaskReply(Int32)).new(2)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::ExecutorExtension.new(system, 1, 2)
    reply_to = system.spawn(ExecutorReplyProbe.new(replies), name: "executor-error-probe")

    executor.execute_with_reply(reply_to) do
      raise ExecutorSpecError.new("reply boom")
      0
    end

    reply = replies.receive
    reply.should be_a(Movie::ExecutorExtension::TaskFailure(Int32))
    reply.as(Movie::ExecutorExtension::TaskFailure(Int32)).error.should be_a(ExecutorSpecError)
    reply.as(Movie::ExecutorExtension::TaskFailure(Int32)).error.message.should eq("reply boom")
  end

  it "fails new work after stop for both future and reply APIs" do
    replies = Channel(Movie::ExecutorExtension::TaskReply(Int32)).new(1)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    executor = Movie::ExecutorExtension.new(system, 1, 1)
    reply_to = system.spawn(ExecutorReplyProbe.new(replies), name: "executor-stop-probe")

    executor.stop

    future = executor.execute { 1 }

    expect_raises(Movie::ExecutorExtension::ExecutorStopped) do
      future.await(100.milliseconds)
    end

    executor.execute_with_reply(reply_to) { 2 }
    reply = replies.receive
    reply.should be_a(Movie::ExecutorExtension::TaskFailure(Int32))
    reply.as(Movie::ExecutorExtension::TaskFailure(Int32)).error.should be_a(Movie::ExecutorExtension::ExecutorStopped)
  end
end
