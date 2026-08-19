module Movie
  abstract class AbstractBehavior(T)
    abstract def receive(message : T, context : ActorContext(T))

    def on_signal(signal : SystemMessage)
    end
  end

  class SameBehavior(T) < AbstractBehavior(T)
    def receive(message : T, context : ActorContext(T)) : AbstractBehavior(T)
      self
    end
  end

  class StoppedBehavior(T) < AbstractBehavior(T)
    def receive(message : T, context : ActorContext(T)) : AbstractBehavior(T)
      self
    end
  end

  class DeferredBehavior(T) < AbstractBehavior(T)
    def initialize(@factory : ActorContext(T) -> AbstractBehavior(T))
    end

    def defer(context : ActorContext(T)) : AbstractBehavior(T) forall T
      @factory.call context
    end

    def receive(message : T, context : ActorContext(T)) : AbstractBehavior(T)
      defer(context)
    end
  end

  class ReceiveMessageBehavior(T) < AbstractBehavior(T)
    def initialize(@handler : Proc(T, ActorContext(T), AbstractBehavior(T)))
    end

    def receive(message : T, context : ActorContext(T)) : AbstractBehavior(T)
      @handler.call message, context
    end
  end

  module Behaviors(T)
    def self.same : SameBehavior(T)
      SameBehavior(T).new
    end

    def self.stopped : StoppedBehavior(T)
      StoppedBehavior(T).new
    end

    def self.setup(&block : Proc(ActorContext(T), AbstractBehavior(T))) : DeferredBehavior(T)
      DeferredBehavior.new block
    end

    def self.receive(&block : Proc(T, ActorContext(T), AbstractBehavior(T))) : ReceiveMessageBehavior(T)
      ReceiveMessageBehavior.new(block)
    end

  end
end
