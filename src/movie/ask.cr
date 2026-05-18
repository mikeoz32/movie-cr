module Movie
  module Ask
    class TargetTerminated < Exception
      def initialize(target : ActorRefBase)
        super("Actor #{target.id} terminated before replying to ask")
      end
    end

    abstract class Response(T)
    end

    class Success(T) < Response(T)
      getter value : T

      def initialize(@value : T)
      end
    end

    class Failure(T) < Response(T)
      getter error : Exception

      def initialize(@error : Exception)
      end
    end

    class Cancelled(T) < Response(T)
    end

    # Shared state between the ask caller and the listener behavior.
    # Holds the promise and an optional timer handle for cancellation.
    class AskState(T)
      getter promise : Promise(T)
      @timer_handle : Atomic(TimerHandle?)

      def initialize(@promise : Promise(T))
        @timer_handle = Atomic(TimerHandle?).new(nil)
      end

      def timer_handle=(handle : TimerHandle)
        @timer_handle.set(handle)
      end

      def cancel_timer
        @timer_handle.get.try &.cancel
      end
    end

    class ListenerBehavior(T) < AbstractBehavior(Response(T))
      def initialize(@state : AskState(T), @target : ActorRefBase)
      end

      def receive(message : Response(T), context : ActorContext(Response(T)))
        @state.cancel_timer
        case message
        when Success(T)
          @state.promise.try_success(message.value)
        when Failure(T)
          @state.promise.try_failure(message.error)
        when Cancelled(T)
          @state.promise.try_cancel
        end
        context.stop
        Behaviors(Response(T)).same
      end

      def on_signal(signal : SystemMessage)
        case signal
        when Terminated
          terminated = signal.as(Terminated)
          if terminated.actor == @target && @state.promise.future.pending?
            @state.cancel_timer
            @state.promise.try_failure(TargetTerminated.new(@target))
          end
        end
      end
    end

    def self.success(sender : ActorRefBase?, value : T) forall T
      reply(sender, Success(T).new(value))
    end

    def self.failure(sender : ActorRefBase?, error : Exception, response_type : T.class) forall T
      reply(sender, Failure(T).new(error))
    end

    # Best-effort reply that only responds when the sender is an ask listener.
    def self.reply_if_asked(sender : ActorRefBase?, value : T) forall T
      return unless sender
      return unless sender.as?(ActorRef(Response(T)))
      reply(sender, Success(T).new(value))
    end

    # Best-effort failure reply that only responds when the sender is an ask listener.
    def self.fail_if_asked(sender : ActorRefBase?, error : Exception, response_type : T.class) forall T
      return unless sender
      return unless sender.as?(ActorRef(Response(T)))
      reply(sender, Failure(T).new(error))
    end

    private def self.reply(sender : ActorRefBase?, response : Response(T)) forall T
      return unless sender
      if ref = sender.as?(ActorRef(Response(T)))
        ref.tell_from(nil, response)
      else
        Log.for("Movie::Ask").warn { "Ask reply dropped: sender #{sender.id} is not ActorRef(Response(#{T}))" }
      end
    end
  end
end
