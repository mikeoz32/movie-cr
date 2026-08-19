require "../spec_helper"
require "../../src/movie"

private class FutureSpecError < Exception
end

describe Movie::Future do
  it "isolates callback exceptions so completion still propagates" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    completed = Channel(Int32).new(1)

    future.on_success do |_value|
      raise "callback boom"
    end

    future.on_complete do |result|
      completed.send(result.value.not_nil!)
    end

    promise.success(42)

    future.await(100.milliseconds).should eq(42)
    completed.receive.should eq(42)
  end

  it "returns successful results and exposes terminal state" do
    promise = Movie::Promise(Int32).new
    future = promise.future

    promise.success(7)

    future.status.should eq(Movie::FutureStatus::Success)
    future.success?.should be_true
    future.result.value.should eq(7)
    future.await(50.milliseconds).should eq(7)
  end

  it "re-raises failures and reports failure state" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    error = FutureSpecError.new("boom")

    promise.failure(error)

    future.failure?.should be_true
    future.result.error.should be(error)

    expect_raises(FutureSpecError, "boom") do
      future.await(50.milliseconds)
    end
  end

  it "notifies cancellation and raises FutureCancelled on await" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    cancelled = Channel(Bool).new(1)

    future.on_cancel { cancelled.send(true) }

    promise.cancel

    future.cancelled?.should be_true
    cancelled.receive.should be_true
    expect_raises(Movie::FutureCancelled) do
      future.await(50.milliseconds)
    end
  end

  it "runs callbacks in registration order" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    events = [] of String

    future.on_complete { |_result| events << "complete-1" }
    future.on_success { |_value| events << "success" }
    future.on_complete { |_result| events << "complete-2" }

    promise.success(1)

    events.should eq(["complete-1", "success", "complete-2"])
  end

  it "runs applicable callbacks immediately when already completed" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    values = [] of Int32

    promise.success(5)

    future.on_success { |value| values << value }
    future.on_complete { |result| values << result.value.not_nil! * 2 }
    future.on_failure { |_error| values << -1 }

    values.should eq([5, 10])
  end

  it "isolates exceptions from callbacks registered after completion" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    completed = Channel(Int32).new(1)

    promise.success(13)

    future.on_success do |_value|
      raise "late callback boom"
    end

    future.on_complete do |result|
      completed.send(result.value.not_nil!)
    end

    completed.receive.should eq(13)
  end

  it "allows callback subscriptions to be cancelled before completion" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    called = Channel(Int32).new(1)

    subscription = future.on_success { |value| called.send(value) }
    subscription.cancel

    promise.success(9)

    select
    when value = called.receive
      fail "expected no callback, got #{value}"
    when timeout(50.milliseconds)
    end
  end

  it "times out without completing the future and can still complete later" do
    promise = Movie::Promise(Int32).new
    future = promise.future

    spawn do
      sleep 50.milliseconds
      promise.try_success(11)
    end

    expect_raises(Movie::FutureTimeout) do
      future.await(10.milliseconds)
    end

    future.pending?.should be_true
    future.await(200.milliseconds).should eq(11)
  end

  it "does not block completion when timeout races with a waiter" do
    200.times do
      promise = Movie::Promise(Int32).new
      completed = Channel(Nil).new(1)

      spawn do
        begin
          promise.future.await(1.nanosecond)
        rescue Movie::FutureTimeout
        end
      end

      spawn do
        promise.try_success(1)
        completed.send(nil)
      end

      select
      when completed.receive
      when timeout(100.milliseconds)
        fail "future completion blocked behind a timed-out waiter"
      end

      promise.future.await(1.second).should eq(1)
    end
  end

  it "wakes awaiters before running blocking completion callbacks" do
    promise = Movie::Promise(Int32).new
    future = promise.future
    callback_entered = Channel(Nil).new(1)
    release_callback = Channel(Nil).new(1)
    waiter = Channel(Int32).new(1)

    future.on_success do |_value|
      callback_entered.send(nil)
      release_callback.receive
    end

    spawn do
      waiter.send(future.await(1.second))
    end

    sleep 10.milliseconds
    spawn { promise.success(42) }
    callback_entered.receive

    select
    when value = waiter.receive
      value.should eq(42)
    when timeout(100.milliseconds)
      fail "future waiter remained blocked behind a completion callback"
    end

    release_callback.send(nil)
  end

  it "rejects a second completion attempt" do
    promise = Movie::Promise(Int32).new

    promise.success(3)

    expect_raises(Movie::FutureAlreadyCompleted) do
      promise.failure(FutureSpecError.new("late failure"))
    end
  end
end
