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
        abstract def retryable? : Bool
      end

      private class TypedWork(T) < Work
        def initialize(
          @operation : Proc(BackendConnection, T),
          @promise : Movie::Promise(T),
          @telemetry : Telemetry,
          @retryable : Bool,
        )
        end

        def execute(connection : BackendConnection)
          started = Time.instant
          @telemetry.work_started
          begin
            value = @operation.call(connection)
            @telemetry.work_succeeded(Time.instant - started)
            @promise.try_success(value)
          rescue error
            @telemetry.work_failed(error, Time.instant - started)
            raise error
          end
        end

        def fail(error : Exception)
          @promise.try_failure(error)
        end

        def retryable? : Bool
          @retryable
        end
      end

      @jobs : Channel(Work)
      @ready : Movie::Promise(Nil)
      @stopped : Atomic(Bool)
      @execution_context : Fiber::ExecutionContext::Isolated

      def initialize(
        @backend : Backend,
        name : String,
        queue_capacity : Int32 = 256,
        @telemetry : Telemetry = Telemetry.new,
        @resilience : ResiliencePolicy = ResiliencePolicy.disabled,
      )
        @jobs = Channel(Work).new(queue_capacity < 1 ? 1 : queue_capacity)
        @ready = Movie::Promise(Nil).new
        @stopped = Atomic(Bool).new(false)
        @consecutive_connection_failures = 0
        @circuit_opened_at = nil.as(Time::Instant?)
        @telemetry.worker_registered(queue_capacity < 1 ? 1 : queue_capacity)
        @execution_context = Fiber::ExecutionContext::Isolated.new(name) { run }
      end

      def initialize(
        db_uri : String,
        name : String,
        queue_capacity : Int32 = 256,
        telemetry : Telemetry = Telemetry.new,
        resilience : ResiliencePolicy = ResiliencePolicy.disabled,
      )
        initialize(SQLiteBackend.new(db_uri), name, queue_capacity, telemetry, resilience)
      end

      def execute(retryable : Bool = false, &operation : BackendConnection -> T) : T forall T
        @ready.future.await
        raise Stopped.new("Database connection worker is stopped") if @stopped.get

        promise = Movie::Promise(T).new
        work = TypedWork(T).new(operation, promise, @telemetry, retryable)
        @telemetry.job_enqueued
        begin
          @jobs.send(work)
        rescue Channel::ClosedError
          @telemetry.job_cancelled
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
          @telemetry.worker_stopped
        end
        @execution_context.wait
      end

      private def run
        connection = nil.as(BackendConnection?)
        begin
          @ready.try_success(nil)

          loop do
            work = @jobs.receive
            @telemetry.job_dequeued
            unless circuit_allows_request?
              error = CircuitOpenError.new
              @telemetry.work_rejected(error)
              work.fail(error)
              next
            end

            retries = 0
            loop do
              current = nil.as(BackendConnection?)
              begin
                current = connection || @backend.connect
                connection = current
                work.execute(current)
                connection_succeeded
                break
              rescue error
                connection_failure = connection_failure?(current, error)
                if connection_failure
                  disconnect(current)
                  connection = nil
                  connection_failed
                  if work.retryable? && retries < @resilience.max_retries && !circuit_open?
                    @telemetry.retry
                    backoff = retry_backoff(retries)
                    sleep(backoff) if backoff > Time::Span.zero
                    retries += 1
                    next
                  end
                else
                  connection_succeeded
                end
                work.fail(error)
                break
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

      private def connection_failure?(connection : BackendConnection?, error : Exception) : Bool
        if current = connection
          current.connection_lost?(error)
        else
          error.is_a?(DB::ConnectionLost) || error.is_a?(DB::ConnectionRefused)
        end
      end

      private def disconnect(connection : BackendConnection?)
        @telemetry.reconnect
        begin
          connection.try &.close
        rescue
        end
      end

      private def connection_failed
        @consecutive_connection_failures += 1
        return unless @consecutive_connection_failures >= @resilience.circuit_failure_threshold

        unless circuit_open?
          @telemetry.circuit_opened
        end
        @circuit_opened_at = Time.instant
      end

      private def connection_succeeded
        @consecutive_connection_failures = 0
        if circuit_open?
          @circuit_opened_at = nil
          @telemetry.circuit_closed
        end
      end

      private def circuit_open? : Bool
        !@circuit_opened_at.nil?
      end

      private def circuit_allows_request? : Bool
        return true unless opened_at = @circuit_opened_at
        Time.instant - opened_at >= @resilience.circuit_reset_timeout
      end

      private def retry_backoff(retry_index : Int32) : Time::Span
        minimum_ns = @resilience.min_backoff.total_nanoseconds.to_i64
        maximum_ns = @resilience.max_backoff.total_nanoseconds.to_i64
        return Time::Span.zero if minimum_ns <= 0_i64
        factor = 1_i64 << retry_index.clamp(0, 30)
        return maximum_ns.nanoseconds if minimum_ns > maximum_ns // factor
        (minimum_ns * factor).nanoseconds
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
