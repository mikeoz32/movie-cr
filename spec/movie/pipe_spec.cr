require "../spec_helper"
require "../../src/movie"

private class PipeReceiver(T) < Movie::AbstractBehavior(Movie::Pipe::Message(T))
  def initialize(@promise : Movie::Promise(Movie::Pipe::Message(T)))
  end

  def receive(message, ctx)
    @promise.try_success(message)
    ctx.stop
    Movie::Behaviors(Movie::Pipe::Message(T)).same
  end
end

private struct PipeRequest(T)
  getter future : Movie::Future(T)
  getter reply_to : Movie::ActorRef(Movie::Pipe::Message(T))

  def initialize(@future : Movie::Future(T), @reply_to : Movie::ActorRef(Movie::Pipe::Message(T)))
  end
end

private class PipeActor(T) < Movie::AbstractBehavior(PipeRequest(T))
  def receive(message, ctx)
    ctx.pipe(message.future, message.reply_to)
    Movie::Behaviors(PipeRequest(T)).same
  end
end

private class StringReceiver < Movie::AbstractBehavior(String)
  def initialize(@promise : Movie::Promise(String))
  end

  def receive(message, ctx)
    @promise.try_success(message)
    ctx.stop
    Movie::Behaviors(String).same
  end
end

private class MappingPipeActor < Movie::AbstractBehavior(Movie::Future(Int32))
  def initialize(@target : Movie::ActorRef(String))
  end

  def receive(message, ctx)
    ctx.pipe(message, @target, ->(value : Int32) { "value=#{value}" }, ->(ex : Exception) { "error=#{ex.message}" })
    Movie::Behaviors(Movie::Future(Int32)).same
  end
end

describe "Movie::ActorContext#pipe" do
  it "sends Success to target on future success" do
    system = Movie::ActorSystem(Int32).new(Movie::Behaviors(Int32).same)
    promise = Movie::Promise(Movie::Pipe::Message(Int32)).new
    receiver = system.spawn(PipeReceiver(Int32).new(promise))
    actor = system.spawn(PipeActor(Int32).new)

    fut_promise = Movie::Promise(Int32).new
    actor << PipeRequest(Int32).new(fut_promise.future, receiver)
    fut_promise.try_success(42)

    message = promise.future.await(1.second)
    message.is_a?(Movie::Pipe::Success(Int32)).should be_true
    message.as(Movie::Pipe::Success(Int32)).value.should eq(42)
  end

  it "sends Failure to target on future failure" do
    system = Movie::ActorSystem(Int32).new(Movie::Behaviors(Int32).same)
    promise = Movie::Promise(Movie::Pipe::Message(Int32)).new
    receiver = system.spawn(PipeReceiver(Int32).new(promise))
    actor = system.spawn(PipeActor(Int32).new)

    fut_promise = Movie::Promise(Int32).new
    actor << PipeRequest(Int32).new(fut_promise.future, receiver)
    fut_promise.try_failure(Exception.new("boom"))

    message = promise.future.await(1.second)
    message.is_a?(Movie::Pipe::Failure(Int32)).should be_true
    message.as(Movie::Pipe::Failure(Int32)).error.message.should eq("boom")
  end

  it "maps future results to custom messages" do
    system = Movie::ActorSystem(Int32).new(Movie::Behaviors(Int32).same)
    promise = Movie::Promise(String).new

    receiver = system.spawn(StringReceiver.new(promise))
    actor = system.spawn(MappingPipeActor.new(receiver))

    fut_promise = Movie::Promise(Int32).new
    actor << fut_promise.future
    fut_promise.try_success(7)

    value = promise.future.await(1.second)
    value.should eq("value=7")
  end
end
