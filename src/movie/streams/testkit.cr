module Movie
  module Streams
    module Typed
      class TestKitAssertionError < Exception
      end

      TESTKIT_DEFAULT_TIMEOUT = 1.second

      # Materialized control for a TestSources probe.
      class TestPublisherProbe(T)
        def initialize(
          @control : ManualSourceControl(T),
          @default_timeout : Time::Span = TESTKIT_DEFAULT_TIMEOUT,
        )
          raise ArgumentError.new("send_timeout must be greater than zero") unless @default_timeout > Time::Span.zero
        end

        def offer(value : T) : QueueOfferResult
          @control.offer(value)
        end

        def send_next(value : T, *, timeout : Time::Span = @default_timeout) : self
          raise ArgumentError.new("timeout must be greater than zero") unless timeout > Time::Span.zero
          completion = Channel(QueueOfferResult).new(1)
          spawn { completion.send(offer(value)) }

          result = select
          when offer_result = completion.receive
            offer_result
          when ::timeout(timeout)
            @control.cancel
            raise TestKitAssertionError.new(
              "timed out after #{describe_timeout(timeout)} sending element #{value.inspect}"
            )
          end
          case result.status
          when QueueOfferStatus::Enqueued
          when QueueOfferStatus::Dropped
            raise TestKitAssertionError.new("test publisher element #{value.inspect} was dropped")
          when QueueOfferStatus::QueueClosed
            raise TestKitAssertionError.new("test publisher is already closed")
          when QueueOfferStatus::Failure
            raise TestKitAssertionError.new(
              "test publisher failed while sending #{value.inspect}: #{describe_error(result.error.not_nil!)}"
            )
          end
          self
        end

        def send_complete : self
          @control.complete
          self
        end

        def send_error(error : Exception) : self
          @control.fail(error)
          self
        end

        def terminal? : Bool
          @control.terminal?
        end

        private def describe_error(error : Exception) : String
          message = error.message
          message ? "#{error.class}: #{message}" : error.class.to_s
        end

        private def describe_timeout(timeout_span : Time::Span) : String
          milliseconds = timeout_span.total_milliseconds
          value = milliseconds == milliseconds.to_i64 ? milliseconds.to_i64.to_s : milliseconds.to_s
          "#{value} milliseconds"
        end
      end

      # Materialized control for a TestSinks probe. The probe exposes elements
      # only after explicit demand while terminal events remain observable.
      class TestSubscriberProbe(T)
        @events = Channel(StreamEvent(T)).new(1)
        @pending_demand = 0u64
        @demand_signal = Channel(Nil).new
        @runtime_cancel_signal = Channel(Nil).new
        @runtime_error : Exception? = nil
        @terminal_published = false
        @terminal_consumed = false
        @state_mutex = Mutex.new

        def initialize(@default_timeout : Time::Span = TESTKIT_DEFAULT_TIMEOUT)
          raise ArgumentError.new("default_timeout must be greater than zero") unless @default_timeout > Time::Span.zero
        end

        def request(n : Int32) : self
          raise ArgumentError.new("demand must be greater than zero") unless n > 0
          add_demand(n.to_u64)
          self
        end

        def request(n : UInt64) : self
          raise ArgumentError.new("demand must be greater than zero") if n == 0
          add_demand(n)
          self
        end

        def expect_next(*, timeout : Time::Span = @default_timeout) : T
          event = receive_event("next element", timeout)
          case event
          when StreamElement(T)
            event.value
          else
            raise TestKitAssertionError.new(
              "expected next element, received #{describe_event(event)}"
            )
          end
        end

        def expect_next(expected : T, *, timeout : Time::Span = @default_timeout) : self
          event = receive_event("element #{expected.inspect}", timeout)
          case event
          when StreamElement(T)
            unless event.value == expected
              raise TestKitAssertionError.new(
                "expected element #{expected.inspect}, received element #{event.value.inspect}"
              )
            end
          else
            raise TestKitAssertionError.new(
              "expected element #{expected.inspect}, received #{describe_event(event)}"
            )
          end
          self
        end

        def expect_complete(*, timeout : Time::Span = @default_timeout) : self
          event = receive_event("completion", timeout)
          unless event.is_a?(StreamCompleted(T))
            raise TestKitAssertionError.new("expected completion, received #{describe_event(event)}")
          end
          self
        end

        def expect_error(*, timeout : Time::Span = @default_timeout) : Exception
          event = receive_event("failure", timeout)
          case event
          when StreamFailed(T)
            event.error
          else
            raise TestKitAssertionError.new("expected failure, received #{describe_event(event)}")
          end
        end

        def expect_error(type : E.class, *, timeout : Time::Span = @default_timeout) : E forall E
          error = expect_error(timeout: timeout)
          unless error.is_a?(E)
            raise TestKitAssertionError.new(
              "expected failure #{E}, received #{describe_error(error)}"
            )
          end
          error.as(E)
        end

        def expect_no_message(duration : Time::Span = 100.milliseconds) : self
          raise ArgumentError.new("duration must be greater than zero") unless duration > Time::Span.zero
          ensure_terminal_not_consumed
          select
          when event = @events.receive
            observe_event(event)
            raise TestKitAssertionError.new(
              "expected silence for #{describe_timeout(duration)}, received #{describe_event(event)}"
            )
          when @runtime_cancel_signal.receive?
            event = observe_event(StreamFailed(T).new(current_runtime_error))
            raise TestKitAssertionError.new(
              "expected silence for #{describe_timeout(duration)}, received #{describe_event(event)}"
            )
          when timeout(duration)
            self
          end
        end

        # Internal TestSink hook: blocks until one unit of demand is available.
        def await_demand : Nil
          loop do
            signal = nil.as(Channel(Nil)?)
            runtime_error = nil.as(Exception?)
            @state_mutex.synchronize do
              if error = @runtime_error
                runtime_error = error
              elsif @pending_demand > 0
                @pending_demand -= 1
                return
              else
                signal = @demand_signal
              end
            end
            raise runtime_error.not_nil! if runtime_error
            signal.not_nil!.receive?
          end
        end

        # Internal TestSink hook.
        def publish(event : StreamEvent(T)) : Nil
          unless event.is_a?(StreamElement(T))
            @state_mutex.synchronize do
              @terminal_published = true
              notify_demand_locked
            end
          end
          select
          when @events.send(event)
          when @runtime_cancel_signal.receive?
            raise current_runtime_error
          end
        end

        # Internal runtime hook used to release a probe blocked on demand or
        # an unconsumed assertion event.
        def cancel_from_runtime(error : Exception = StreamCancelledError.new("test subscriber runtime stopped")) : Nil
          @state_mutex.synchronize do
            return if @runtime_error
            @runtime_error = error
            @terminal_published = true
            @runtime_cancel_signal.close
            notify_demand_locked
          end
        end

        private def add_demand(n : UInt64)
          @state_mutex.synchronize do
            raise TestKitAssertionError.new("cannot request from a terminated test subscriber") if @terminal_published || @runtime_error
            available = UInt64::MAX - @pending_demand
            @pending_demand += n > available ? available : n
            notify_demand_locked
          end
        end

        private def current_runtime_error : Exception
          @state_mutex.synchronize do
            @runtime_error || StreamCancelledError.new("test subscriber runtime stopped")
          end
        end

        private def notify_demand_locked
          previous_signal = @demand_signal
          @demand_signal = Channel(Nil).new
          previous_signal.close
        end

        private def receive_event(expectation : String, timeout_span : Time::Span) : StreamEvent(T)
          raise ArgumentError.new("timeout must be greater than zero") unless timeout_span > Time::Span.zero
          deadline = Time.instant + timeout_span
          loop do
            ensure_terminal_not_consumed
            select
            when event = @events.receive
              return observe_event(event)
            else
            end

            runtime_error = @state_mutex.synchronize { @runtime_error }
            return observe_event(StreamFailed(T).new(runtime_error)) if runtime_error

            remaining = deadline - Time.instant
            if remaining <= Time::Span.zero
              raise TestKitAssertionError.new(
                "timed out after #{describe_timeout(timeout_span)} waiting for #{expectation}"
              )
            end

            select
            when event = @events.receive
              return observe_event(event)
            when @runtime_cancel_signal.receive?
              # Re-check the event queue before returning the runtime error.
            when timeout(remaining)
              raise TestKitAssertionError.new(
                "timed out after #{describe_timeout(timeout_span)} waiting for #{expectation}"
              )
            end
          end
        end

        private def ensure_terminal_not_consumed
          consumed = @state_mutex.synchronize { @terminal_consumed }
          raise TestKitAssertionError.new("terminal signal was already consumed") if consumed
        end

        private def observe_event(event : StreamEvent(T)) : StreamEvent(T)
          unless event.is_a?(StreamElement(T))
            @state_mutex.synchronize do
              raise TestKitAssertionError.new("terminal signal was already consumed") if @terminal_consumed
              @terminal_consumed = true
            end
          end
          event
        end

        private def describe_event(event : StreamEvent(T)) : String
          case event
          when StreamElement(T)
            "element #{event.value.inspect}"
          when StreamCompleted(T)
            "completion"
          when StreamFailed(T)
            "failure #{describe_error(event.error)}"
          else
            event.class.to_s
          end
        end

        private def describe_error(error : Exception) : String
          message = error.message
          message ? "#{error.class}: #{message}" : error.class.to_s
        end

        private def describe_timeout(timeout_span : Time::Span) : String
          milliseconds = timeout_span.total_milliseconds
          value = milliseconds == milliseconds.to_i64 ? milliseconds.to_i64.to_s : milliseconds.to_s
          "#{value} milliseconds"
        end
      end

      module TestSources
        def self.probe(
          type : T.class,
          buffer_size : Int32 = DEFAULT_BUFFER_SIZE,
          overflow_strategy : OverflowStrategy = OverflowStrategy::Backpressure,
          send_timeout : Time::Span = TESTKIT_DEFAULT_TIMEOUT,
        ) : Source(T, TestPublisherProbe(T)) forall T
          raise ArgumentError.new("send_timeout must be greater than zero") unless send_timeout > Time::Span.zero
          source = Sources.manual(T, buffer_size, overflow_strategy)
          Source(T, TestPublisherProbe(T)).new do |system|
            materialization = source.materialize(system)
            Materialization(T, TestPublisherProbe(T)).new(
              materialization.outlet,
              TestPublisherProbe(T).new(materialization.value, send_timeout)
            )
          end
        end
      end

      module TestSinks
        def self.probe(
          type : T.class,
          default_timeout : Time::Span = TESTKIT_DEFAULT_TIMEOUT,
        ) : Sink(T, TestSubscriberProbe(T)) forall T
          raise ArgumentError.new("default_timeout must be greater than zero") unless default_timeout > Time::Span.zero
          Sink(T, TestSubscriberProbe(T)).new do |system, inlet|
            probe = TestSubscriberProbe(T).new(default_timeout)
            runtime = system.extensions.get_or_register(StreamRuntimeExtension) do
              StreamRuntimeExtension.new
            end
            registration_id = runtime.register { probe.cancel_from_runtime }
            BlueprintRuntime.execute(system) do
              begin
                loop do
                  case event = inlet.receive
                  when StreamElement(T)
                    probe.await_demand
                    probe.publish(event)
                  when StreamCompleted(T), StreamFailed(T)
                    probe.publish(event)
                    break
                  end
                end
              rescue ex : StreamCancelledError
                probe.cancel_from_runtime(ex)
              rescue ex : Exception
                inlet.cancel
                probe.cancel_from_runtime(ex)
              ensure
                runtime.unregister(registration_id)
              end
            end
            probe
          end
        end
      end
    end
  end
end
