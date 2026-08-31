module Movie
  module Persistence
    enum HealthStatus
      Healthy
      Degraded
      Unavailable
    end

    record MetricsSnapshot,
      enqueued : Int64,
      completed : Int64,
      failed : Int64,
      conflicts : Int64,
      reconnects : Int64,
      retries : Int64,
      circuit_opens : Int64,
      queued : Int64,
      queue_high_water : Int64,
      in_flight : Int64,
      total_latency : Time::Span,
      max_latency : Time::Span

    record HealthSnapshot,
      status : HealthStatus,
      workers : Int64,
      available_workers : Int64,
      open_circuits : Int64,
      queue_capacity : Int64,
      queued : Int64,
      last_error : String?

    record ReadinessSnapshot,
      ready : Bool,
      backend : String,
      schema_version : Int64?,
      error : String?

    # Lock-free counters and diagnostics for the hot path. Snapshots never call
    # a persistence backend.
    class Telemetry
      @enqueued = Atomic(Int64).new(0_i64)
      @completed = Atomic(Int64).new(0_i64)
      @failed = Atomic(Int64).new(0_i64)
      @conflicts = Atomic(Int64).new(0_i64)
      @reconnects = Atomic(Int64).new(0_i64)
      @retries = Atomic(Int64).new(0_i64)
      @circuit_opens = Atomic(Int64).new(0_i64)
      @queued = Atomic(Int64).new(0_i64)
      @queue_high_water = Atomic(Int64).new(0_i64)
      @in_flight = Atomic(Int64).new(0_i64)
      @total_latency_ns = Atomic(Int64).new(0_i64)
      @max_latency_ns = Atomic(Int64).new(0_i64)
      @workers = Atomic(Int64).new(0_i64)
      @stopped_workers = Atomic(Int64).new(0_i64)
      @open_circuits = Atomic(Int64).new(0_i64)
      @queue_capacity = Atomic(Int64).new(0_i64)
      @last_error = Atomic(String?).new(nil)

      def worker_registered(queue_capacity : Int32) : Nil
        @workers.add(1_i64)
        @queue_capacity.add(queue_capacity.to_i64)
      end

      def worker_stopped : Nil
        @stopped_workers.add(1_i64)
      end

      def job_enqueued : Nil
        @enqueued.add(1_i64)
        queued = @queued.add(1_i64) + 1_i64
        update_queue_high_water(queued)
      end

      def job_dequeued : Nil
        @queued.sub(1_i64)
      end

      def job_cancelled : Nil
        @queued.sub(1_i64)
      end

      def work_started : Nil
        @in_flight.add(1_i64)
      end

      def work_succeeded(elapsed : Time::Span) : Nil
        record_latency(elapsed)
        @completed.add(1_i64)
        @in_flight.sub(1_i64)
      end

      def work_failed(error : Exception, elapsed : Time::Span) : Nil
        record_latency(elapsed)
        @failed.add(1_i64)
        if error.is_a?(ConcurrentWriteError) || error.is_a?(OperationConflictError)
          @conflicts.add(1_i64)
        end
        @in_flight.sub(1_i64)
        @last_error.set(error.message || error.class.name)
      end

      def work_rejected(error : Exception) : Nil
        @failed.add(1_i64)
        @last_error.set(error.message || error.class.name)
      end

      def reconnect : Nil
        @reconnects.add(1_i64)
      end

      def retry : Nil
        @retries.add(1_i64)
      end

      def circuit_opened : Nil
        @circuit_opens.add(1_i64)
        @open_circuits.add(1_i64)
      end

      def circuit_closed : Nil
        @open_circuits.sub(1_i64)
      end

      def metrics : MetricsSnapshot
        MetricsSnapshot.new(
          @enqueued.get,
          @completed.get,
          @failed.get,
          @conflicts.get,
          @reconnects.get,
          @retries.get,
          @circuit_opens.get,
          @queued.get,
          @queue_high_water.get,
          @in_flight.get,
          @total_latency_ns.get.nanoseconds,
          @max_latency_ns.get.nanoseconds
        )
      end

      def health : HealthSnapshot
        workers = @workers.get
        available = workers - @stopped_workers.get
        open = @open_circuits.get
        queued = @queued.get
        capacity = @queue_capacity.get
        status = if workers == 0_i64 || available <= open
                   HealthStatus::Unavailable
                 elsif open > 0_i64 || (capacity > 0_i64 && queued >= capacity)
                   HealthStatus::Degraded
                 else
                   HealthStatus::Healthy
                 end
        last_error = @last_error.get
        HealthSnapshot.new(status, workers, available, open, capacity, queued, last_error)
      end

      private def record_latency(elapsed : Time::Span)
        nanoseconds = elapsed.total_nanoseconds.to_i64.clamp(1_i64, Int64::MAX)
        @total_latency_ns.add(nanoseconds)
        update_max_latency(nanoseconds)
      end

      private def update_queue_high_water(value : Int64)
        current = @queue_high_water.get
        while value > current
          observed, changed = @queue_high_water.compare_and_set(current, value)
          return if changed
          current = observed
        end
      end

      private def update_max_latency(value : Int64)
        current = @max_latency_ns.get
        while value > current
          observed, changed = @max_latency_ns.compare_and_set(current, value)
          return if changed
          current = observed
        end
      end
    end
  end
end
