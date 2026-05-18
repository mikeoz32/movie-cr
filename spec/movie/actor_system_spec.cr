require "../spec_helper"
require "../../src/movie"

private class EchoRoot < Movie::AbstractBehavior(String)
  def receive(message, ctx)
    Movie::Ask.success(ctx.sender, "echo: #{message}")
    Movie::Behaviors(String).same
  end
end

private class SenderProbe < Movie::AbstractBehavior(String)
  def initialize(@channel : Channel(Int32?))
  end

  def receive(message, ctx)
    @channel.send(ctx.sender.try &.id)
    Movie::Behaviors(String).same
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
end
