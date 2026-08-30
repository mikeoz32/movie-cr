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

    # A one-shot, unregistered response endpoint for local asks.
    #
    # Unlike the legacy listener behavior, this ref does not own a mailbox or
    # consume an actor-system ID. Promise completion is the synchronization
    # point: only the first reply, timeout, or termination signal wins.
    class LocalResponseRef(T) < ActorRefBase
      getter future : Future(T)

      @promise : Promise(T)
      @target : ActorRefBase
      @timer_handle : Atomic(TimerHandle?)
      @timer_cancelled : Atomic(Bool)
      @unwatch_requested : Atomic(Bool)

      def initialize(@target : ActorRefBase)
        super(0)
        @promise = Promise(T).new
        @future = @promise.future
        @timer_handle = Atomic(TimerHandle?).new(nil)
        @timer_cancelled = Atomic(Bool).new(false)
        @unwatch_requested = Atomic(Bool).new(false)
      end

      def timer_handle=(handle : TimerHandle)
        @timer_handle.set(handle)
        handle.cancel if @timer_cancelled.get
      end

      def receive(response : Response(T)) : Nil
        completed = case response
                    when Success(T)
                      @promise.try_success(response.value)
                    when Failure(T)
                      @promise.try_failure(response.error)
                    when Cancelled(T)
                      @promise.try_cancel
                    else
                      false
                    end
        finish if completed
      end

      def timeout : Nil
        finish if @promise.try_failure(FutureTimeout.new)
      end

      def target_terminated : Nil
        finish if @promise.try_failure(TargetTerminated.new(@target))
      end

      def send_system(message : SystemMessage)
        if terminated = message.as?(Terminated)
          target_terminated if terminated.actor == @target
        end
      end

      private def finish : Nil
        @timer_cancelled.set(true)
        @timer_handle.get.try &.cancel

        _, should_unwatch = @unwatch_requested.compare_and_set(false, true)
        return unless should_unwatch
        @target.send_system(Unwatch.new(self.as(ActorRefBase)).as(SystemMessage))
      end
    end

    # Creates a local ask without spawning a temporary actor.
    def self.local(
      system : AbstractActorSystem,
      target : ActorRef(M),
      message : M,
      response_type : R.class = Nil,
      timeout : Time::Span? = nil,
    ) : Future(R) forall M, R
      response_ref = LocalResponseRef(R).new(target.as(ActorRefBase))
      target_context = system.context(target.id)

      unless target_context && target_context.register_watcher(response_ref.as(ActorRefBase))
        response_ref.target_terminated
        return response_ref.future
      end

      begin
        target.tell_from(response_ref.as(ActorRefBase), message)
      rescue ex : Exception
        response_ref.target_terminated
        return response_ref.future
      end

      if timeout
        response_ref.timer_handle = system.scheduler.schedule_once(timeout) do
          response_ref.timeout
        end
      end

      response_ref.future
    end

    # Shared state between the ask caller and the listener behavior.
    # Holds the promise and an optional timer handle for cancellation.
    class AskState(T)
      getter promise : Promise(T)
      @timer_handle : Atomic(TimerHandle?)
      @timer_cancelled : Atomic(Bool)
      @listener : Atomic(ActorRefBase?)
      @listener_stop_requested : Atomic(Bool)
      @unwatch_requested : Atomic(Bool)
      @target : ActorRefBase

      def initialize(@promise : Promise(T), @target : ActorRefBase)
        @timer_handle = Atomic(TimerHandle?).new(nil)
        @timer_cancelled = Atomic(Bool).new(false)
        @listener = Atomic(ActorRefBase?).new(nil)
        @listener_stop_requested = Atomic(Bool).new(false)
        @unwatch_requested = Atomic(Bool).new(false)
      end

      def timer_handle=(handle : TimerHandle)
        @timer_handle.set(handle)
        handle.cancel if @timer_cancelled.get
      end

      def cancel_timer
        @timer_cancelled.set(true)
        @timer_handle.get.try &.cancel
      end

      def listener=(listener : ActorRefBase)
        @listener.set(listener)
        if @listener_stop_requested.get
          unwatch_target
          listener.send_system(STOP)
        end
      end

      def stop_listener
        @listener_stop_requested.set(true)
        unwatch_target
        @listener.get.try &.send_system(STOP)
      end

      def unwatch_target : Nil
        listener = @listener.get
        return unless listener
        _, should_unwatch = @unwatch_requested.compare_and_set(false, true)
        return unless should_unwatch
        @target.send_system(Unwatch.new(listener).as(SystemMessage))
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
        @state.stop_listener
        Behaviors(Response(T)).same
      end

      def on_signal(signal : SystemMessage)
        case signal
        when Terminated
          terminated = signal.as(Terminated)
          if terminated.actor == @target && @state.promise.future.pending?
            @state.cancel_timer
            @state.promise.try_failure(TargetTerminated.new(@target))
            @state.stop_listener
          end
        when PostStop
          @state.unwatch_target
        end
      end
    end

    def self.success(sender : ActorRefBase?, value : T) forall T
      reply(sender, Success(T).new(value))
    end

    def self.failure(sender : ActorRefBase?, error : Exception, response_type : T.class) forall T
      reply(sender, Failure(T).new(error))
    end

    def self.cancel(sender : ActorRefBase?, response_type : T.class) forall T
      reply(sender, Cancelled(T).new)
    end

    # Best-effort reply that only responds when the sender is an ask listener.
    def self.reply_if_asked(sender : ActorRefBase?, value : T) forall T
      return unless sender
      return unless sender.as?(ActorRef(Response(T))) || sender.as?(LocalResponseRef(T)) || sender.as?(::Movie::Remote::RemoteAskResponseSenderRef)
      reply(sender, Success(T).new(value))
    end

    # Best-effort failure reply that only responds when the sender is an ask listener.
    def self.fail_if_asked(sender : ActorRefBase?, error : Exception, response_type : T.class) forall T
      return unless sender
      return unless sender.as?(ActorRef(Response(T))) || sender.as?(LocalResponseRef(T)) || sender.as?(::Movie::Remote::RemoteAskResponseSenderRef)
      reply(sender, Failure(T).new(error))
    end

    private def self.reply(sender : ActorRefBase?, response : Response(T)) forall T
      return unless sender
      if ref = sender.as?(ActorRef(Response(T)))
        ref.tell_from(nil, response)
      elsif ref = sender.as?(LocalResponseRef(T))
        ref.receive(response)
      elsif ref = sender.as?(::Movie::Remote::RemoteAskResponseSenderRef)
        case response
        when Success(T)
          if serializable = response.value.as?(JSON::Serializable)
            ref.reply_success(serializable)
          else
            ref.reply_failure(Exception.new("Remote ask response type #{T} is not JSON::Serializable"))
          end
        when Failure(T)
          ref.reply_failure(response.error)
        when Cancelled(T)
          ref.reply_cancelled
        end
      else
        Log.for("Movie::Ask").warn { "Ask reply dropped: sender #{sender.id} is not ActorRef(Response(#{T}))" }
      end
    end
  end
end
