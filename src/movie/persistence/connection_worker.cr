module Movie
  module Persistence
    # Owns one database connection on a dedicated OS thread. Jobs are bounded and
    # always execute on the same isolated execution context as the connection.
    class ConnectionWorker
      class Stopped < Exception
      end

      private abstract class Work
        abstract def execute(connection : BackendConnection)
        abstract def fail(error : Exception)
      end

      private class TypedWork(T) < Work
        def initialize(@operation : Proc(BackendConnection, T), @promise : Movie::Promise(T))
        end

        def execute(connection : BackendConnection)
          @promise.try_success(@operation.call(connection))
        end

        def fail(error : Exception)
          @promise.try_failure(error)
        end
      end

      @jobs : Channel(Work)
      @ready : Movie::Promise(Nil)
      @stopped : Atomic(Bool)
      @execution_context : Fiber::ExecutionContext::Isolated

      def initialize(@backend : Backend, name : String, queue_capacity : Int32 = 256)
        @jobs = Channel(Work).new(queue_capacity < 1 ? 1 : queue_capacity)
        @ready = Movie::Promise(Nil).new
        @stopped = Atomic(Bool).new(false)
        @execution_context = Fiber::ExecutionContext::Isolated.new(name) { run }
      end

      def initialize(db_uri : String, name : String, queue_capacity : Int32 = 256)
        initialize(SQLiteBackend.new(db_uri), name, queue_capacity)
      end

      def execute(&operation : BackendConnection -> T) : T forall T
        @ready.future.await
        raise Stopped.new("Database connection worker is stopped") if @stopped.get

        promise = Movie::Promise(T).new
        work = TypedWork(T).new(operation, promise)
        begin
          @jobs.send(work)
        rescue Channel::ClosedError
          raise Stopped.new("Database connection worker is stopped")
        end
        promise.future.await
      end

      def close
        _, changed = @stopped.compare_and_set(false, true)
        if changed
          begin
            @jobs.close
          rescue Channel::ClosedError
          end
        end
        @execution_context.wait
      end

      private def run
        connection = nil.as(BackendConnection?)
        begin
          connection = @backend.connect
          @ready.try_success(nil)

          loop do
            work = @jobs.receive
            begin
              current = connection || @backend.connect
              connection = current
              work.execute(current)
            rescue error
              work.fail(error)
              if current = connection
                if current.connection_lost?(error)
                  begin
                    current.close
                  rescue
                  end
                  connection = nil
                end
              end
            end
          end
        rescue Channel::ClosedError
        rescue error
          @ready.try_failure(error)
          @stopped.set(true)
          begin
            @jobs.close
          rescue Channel::ClosedError
          end
          drain_with_failure(error)
        ensure
          connection.try &.close
        end
      end

      private def drain_with_failure(error : Exception)
        loop do
          work = @jobs.receive?
          break unless work
          work.fail(error)
        end
      rescue Channel::ClosedError
      end
    end
  end
end
