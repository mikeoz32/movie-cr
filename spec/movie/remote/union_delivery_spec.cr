require "../../spec_helper"
require "../../../src/movie"

record UnionDeliveryOne, value : String do
  include JSON::Serializable
end

record UnionDeliveryTwo, value : Int32 do
  include JSON::Serializable
end

alias UnionDeliveryMessage = UnionDeliveryOne | UnionDeliveryTwo

private class UnionDeliveryBehavior < Movie::AbstractBehavior(UnionDeliveryMessage)
  def initialize(@received : Channel(String))
  end

  def receive(message : UnionDeliveryMessage, context : Movie::ActorContext(UnionDeliveryMessage))
    case message
    when UnionDeliveryOne
      @received.send(message.value)
    when UnionDeliveryTwo
      @received.send(message.value.to_s)
    end

    Movie::Behaviors(UnionDeliveryMessage).same
  end
end

describe "remote message delivery to union actors" do
  it "delivers a concrete wrapper through the union actor context" do
    received = Channel(String).new(1)
    system = Movie::ActorSystem(Symbol).new(Movie::Behaviors(Symbol).same)
    ref = system.spawn(UnionDeliveryBehavior.new(received), name: "union-delivery")
    context = system.context(ref.id).not_nil!
    wrapper = Movie::Remote::TypedMessageWrapper(UnionDeliveryOne).new(UnionDeliveryOne.new("delivered"))

    wrapper.deliver_to(context, system.dead_letters)

    select
    when value = received.receive
      value.should eq("delivered")
    when timeout(500.milliseconds)
      fail "union actor did not receive the concrete remote message"
    end
  end
end
