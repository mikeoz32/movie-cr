require "./extension_id"
require "./system"
require "./future"

module Movie
  # Generic executor extension — runs arbitrary tasks on a bounded pool of workers.
  class ExecutorExtension < Movie::Extension
    class ExecutorStopped < Exception
    end

    @pool_size : Int32
    @queue_capacity : Int32
    @task_queue : Channel(Proc(Nil))
    @started : Bool
    @stopped : Atomic(Bool)

    def initialize(@system : AbstractActorSystem, @pool_size : Int32 = 4, @queue_capacity : Int32 = 128)
      @task_queue = Channel(Proc(Nil)).new(@queue_capacity)
      @started = false
      @stopped = Atomic(Bool).new(false)
    end

    def start
      # Lazy start — actual worker fibers are spawned on first execute to avoid startup ordering constraints.
    end

    def stop
      @stopped.set(true)
      # Close the task queue to terminate workers (if running)
      begin
        @task_queue.close
      rescue
      end
    end

    private def ensure_started
      return if @started || @stopped.get
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

    abstract struct TaskReply(T)
    end

    struct TaskSuccess(T) < TaskReply(T)
      getter value : T

      def initialize(@value : T)
      end
    end

    struct TaskFailure(T) < TaskReply(T)
      getter error : Exception

      def initialize(@error : Exception)
      end
    end

    private class ReplyState
      def initialize
        @delivered = Atomic(Bool).new(false)
      end

      def try_mark_delivered : Bool
        _, success = @delivered.compare_and_set(false, true)
        success
      end
    end

    # Execute a block and return a Future (existing API).
    def execute(timeout : Time::Span? = nil, &block : -> T) : Future(T) forall T
      promise = Promise(T).new
      if @stopped.get
        promise.try_failure(ExecutorStopped.new)
        return promise.future
      end

      ensure_started

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

      begin
        @task_queue.send(task)
      rescue Channel::ClosedError
        promise.try_failure(ExecutorStopped.new)
      end
      promise.future
    end

    # Execute a block and send the result back to a specific actor via a message.
    def execute_with_reply(reply_to : ActorRef(TaskReply(T)), timeout : Time::Span? = nil, &block : -> T) : Nil forall T
      state = ReplyState.new
      if @stopped.get
        deliver_reply(reply_to, state, TaskFailure(T).new(ExecutorStopped.new))
        return
      end

      ensure_started

      task = -> {
        begin
          result = block.call
          deliver_reply(reply_to, state, TaskSuccess(T).new(result))
        rescue ex : Exception
          deliver_reply(reply_to, state, TaskFailure(T).new(ex))
        end
      }

      if timeout
        @system.scheduler.schedule_once(timeout) do
          deliver_reply(reply_to, state, TaskFailure(T).new(FutureTimeout.new))
        end
      end

      begin
        @task_queue.send(task)
      rescue Channel::ClosedError
        deliver_reply(reply_to, state, TaskFailure(T).new(ExecutorStopped.new))
      end
    end

    private def deliver_reply(reply_to : ActorRef(TaskReply(T)), state : ReplyState, reply : TaskReply(T)) : Nil forall T
      reply_to << reply if state.try_mark_delivered
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
