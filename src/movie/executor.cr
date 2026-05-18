require "./extension_id"
require "./system"
require "./future"

module Movie
  # Generic executor extension — runs arbitrary tasks on a bounded pool of workers.
  class ExecutorExtension < Movie::Extension
    @pool_size : Int32
    @queue_capacity : Int32
    @task_queue : Channel(Proc(Nil))
    @started : Bool

    def initialize(@system : AbstractActorSystem, @pool_size : Int32 = 4, @queue_capacity : Int32 = 128)
      @task_queue = Channel(Proc(Nil)).new(@queue_capacity)
      @started = false
    end

    def start
      # Lazy start — actual worker fibers are spawned on first execute to avoid startup ordering constraints.
    end

    def stop
      # Close the task queue to terminate workers (if running)
      begin
        @task_queue.close
      rescue
      end
    end

    private def ensure_started
      return if @started
      @started = true

      @pool_size.times do
        @system.dispatchers.internal.execute do
          loop do
            task = nil
            begin
              task = @task_queue.receive
            rescue Channel::ClosedError
              break
            end

            begin
              task.call
            rescue ex : Exception
              # log & swallow — keep worker alive
              Log.for("Movie::Executor").error(exception: ex) { "Worker error" }
            end
          end
        end
      end
    end

    # Simple result wrapper sent back to the caller actor.
    record TaskResult(T), value : T

    # Execute a block and return a Future (existing API).
    def execute(timeout : Time::Span? = nil, &block : -> T) : Future(T) forall T
      ensure_started
      promise = Promise(T).new

      task = -> {
        begin
          result = block.call
          promise.try_success(result)
        rescue ex : Exception
          promise.try_failure(ex)
        end
      }

      if timeout
        @system.scheduler.schedule_once(timeout) do
          if promise.future.pending?
            promise.try_failure(FutureTimeout.new)
          end
        end
      end

      @task_queue.send(task)
      promise.future
    end

    # Execute a block and send the result back to a specific actor via a message.
    def execute_with_reply(reply_to : ActorRef(TaskResult(T)), timeout : Time::Span? = nil, &block : -> T) : Nil forall T
      ensure_started
      task = -> {
        begin
          result = block.call
          reply_to << TaskResult(T).new(result)
        rescue ex : Exception
          # In case of error we simply ignore sending a message; the caller can handle timeout if needed.
        end
      }

      if timeout
        @system.scheduler.schedule_once(timeout) do
          reply_to << FutureTimeout.new
        end
      end

      @task_queue.send(task)
    end
  end

  # Akka-style extension id for the executor.
  class Execution < ExtensionId(ExecutorExtension)
    def create(system : AbstractActorSystem) : ExecutorExtension
      cfg = system.config
      pool = cfg.get_int("executor.pool-size", 4)
      cap = cfg.get_int("executor.queue-capacity", 128)
      ExecutorExtension.new(system, pool, cap)
    end
  end

end
