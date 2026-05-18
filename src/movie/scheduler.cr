module Movie
  # Handle for cancelling a scheduled timer.
  # Thread-safe via atomic operations.
  class TimerHandle
    @cancelled = Atomic(Bool).new(false)

    def cancel
      @cancelled.set(true)
    end

    def cancelled?
      @cancelled.get
    end
  end

  # Centralized scheduler for managing timers within the actor system.
  # Provides cancellable one-shot timers that execute callbacks after a delay.
  # Uses the actor system's dispatcher for proper fiber management.
  #
  # Example:
  # ```
  # handle = scheduler.schedule_once(5.seconds) do
  #   puts "Timer fired!"
  # end
  # handle.cancel  # Cancels the timer if not yet fired
  # ```
  class Scheduler
    @log : Log
    @dispatcher : Dispatcher

    def initialize(@dispatcher : Dispatcher)
      @log = Log.for("Movie::Scheduler")
    end

    # Schedules a one-shot timer that executes the block after the given delay.
    # Returns a TimerHandle that can be used to cancel the timer before it fires.
    #
    # If the timer is cancelled before the delay expires, the block will not be executed.
    # If the timer has already fired, cancellation has no effect.
    def schedule_once(delay : Time::Span, &block : -> Nil) : TimerHandle
      handle = TimerHandle.new

      @dispatcher.execute do
        sleep delay
        unless handle.cancelled?
          begin
            block.call
          rescue ex : Exception
            @log.error(exception: ex) { "Error in scheduled callback" }
          end
        end
      end

      handle
    end

    # Schedules a one-shot timer that sends a message to an actor after the given delay.
    # Returns a TimerHandle that can be used to cancel the timer.
    def schedule_message(delay : Time::Span, target : ActorRef(T), message : T) : TimerHandle forall T
      schedule_once(delay) do
        target << message
      end
    end

    # Schedules a one-shot timer that sends a system message to an actor after the given delay.
    # Returns a TimerHandle that can be used to cancel the timer.
    def schedule_system_message(delay : Time::Span, target : ActorRefBase, message : SystemMessage) : TimerHandle
      schedule_once(delay) do
        target.send_system(message)
      end
    end
  end
end
