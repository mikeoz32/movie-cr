module Movie
  module Streams
    module Typed
      # Marker materialized value for a stage that exposes no runtime control.
      struct NotUsed
        INSTANCE = new
      end

      abstract class StreamEvent(T)
      end

      class StreamElement(T) < StreamEvent(T)
        getter value : T

        def initialize(@value : T)
        end
      end

      class StreamCompleted(T) < StreamEvent(T)
      end

      class StreamFailed(T) < StreamEvent(T)
        getter error : Exception

        def initialize(@error : Exception)
        end
      end

      class StreamCancelledError < Exception
      end

      class StreamClosedError < Exception
      end

      class BufferOverflowError < Exception
      end

      DEFAULT_BUFFER_SIZE = 16

      enum OverflowStrategy
        Backpressure
        DropHead
        DropTail
        DropNew
        DropBuffer
        Fail
      end

      enum QueueOfferStatus
        Enqueued
        Dropped
        QueueClosed
        Failure
      end

      record QueueOfferResult, status : QueueOfferStatus, error : Exception? = nil do
        ENQUEUED     = new(QueueOfferStatus::Enqueued)
        DROPPED      = new(QueueOfferStatus::Dropped)
        QUEUE_CLOSED = new(QueueOfferStatus::QueueClosed)

        def self.failure(error : Exception) : self
          new(QueueOfferStatus::Failure, error)
        end
      end

      # Owns active blueprint edges so ActorSystem shutdown also terminates
      # stream fibers and their materialized futures.
      class StreamRuntimeExtension < Extension
        @cancellations = {} of UInt64 => Proc(Nil)
        @next_id = 0u64
        @stopped = false
        @mutex = Mutex.new

        def register(&cancel : -> Nil) : UInt64
          registration_id = @mutex.synchronize do
            if @stopped
              nil
            else
              @next_id += 1
              @cancellations[@next_id] = cancel
              @next_id
            end
          end

          unless registration_id
            cancel.call
            raise StreamClosedError.new("actor system stream runtime is stopped")
          end
          registration_id
        end

        def unregister(registration_id : UInt64)
          @mutex.synchronize { @cancellations.delete(registration_id) }
        end

        def stop
          cancellations = @mutex.synchronize do
            @stopped = true
            active = @cancellations.values
            @cancellations.clear
            active
          end
          cancellations.each(&.call)
        end
      end

      # Tracks edges created during one synchronous materialization call. A
      # nested successful scope transfers ownership to its parent; an escaping
      # exception cancels every edge created by that scope in reverse order.
      class MaterializationScope
        @rollbacks = [] of Proc(Nil)

        def track(&rollback : -> Nil)
          @rollbacks << rollback
        end

        def absorb(rollbacks : Array(Proc(Nil)))
          @rollbacks.concat(rollbacks)
        end

        def commit_to(parent : MaterializationScope?)
          parent.try &.absorb(@rollbacks)
          @rollbacks.clear
        end

        def rollback
          @rollbacks.reverse_each(&.call)
          @rollbacks.clear
        end
      end

      # Bounded runtime edge used by the reusable blueprint API.
      class StageChannel(T)
        getter capacity : Int32
        getter overflow_strategy : OverflowStrategy

        @buffer = Deque(T).new
        @terminal : StreamEvent(T)? = nil
        @terminal_delivered = false
        @state_signal = Channel(Nil).new
        @cancel_signal = Channel(Nil).new
        @cancelled = false
        @state_mutex = Mutex.new
        @runtime : StreamRuntimeExtension
        @registration_id : UInt64

        def initialize(
          system : AbstractActorSystem,
          @capacity : Int32 = DEFAULT_BUFFER_SIZE,
          @overflow_strategy : OverflowStrategy = OverflowStrategy::Backpressure,
        )
          BlueprintRuntime.validate_buffer_size(@capacity)
          raise StreamClosedError.new("actor system is shutting down") if system.shutting_down?
          @runtime = system.extensions.get_or_register(StreamRuntimeExtension) do
            StreamRuntimeExtension.new
          end
          @registration_id = @runtime.register { cancel_from_runtime }
          BlueprintRuntime.track_rollback { cancel }
        end

        def push(value : T)
          result = offer(value)
          case result.status
          when QueueOfferStatus::Enqueued, QueueOfferStatus::Dropped
            nil
          when QueueOfferStatus::QueueClosed
            raise StreamCancelledError.new("downstream cancelled the stream edge")
          when QueueOfferStatus::Failure
            raise result.error.not_nil!
          end
        end

        def offer(value : T) : QueueOfferResult
          loop do
            result = nil.as(QueueOfferResult?)
            state_signal = nil.as(Channel(Nil)?)

            @state_mutex.synchronize do
              if @cancelled || @terminal || @terminal_delivered
                result = QueueOfferResult::QUEUE_CLOSED
              elsif @buffer.size < @capacity
                @buffer << value
                notify_state_locked
                result = QueueOfferResult::ENQUEUED
              else
                case @overflow_strategy
                when OverflowStrategy::Backpressure
                  state_signal = @state_signal
                when OverflowStrategy::DropHead
                  @buffer.shift
                  @buffer << value
                  result = QueueOfferResult::ENQUEUED
                when OverflowStrategy::DropTail
                  @buffer.pop
                  @buffer << value
                  result = QueueOfferResult::ENQUEUED
                when OverflowStrategy::DropNew
                  result = QueueOfferResult::DROPPED
                when OverflowStrategy::DropBuffer
                  @buffer.clear
                  @buffer << value
                  result = QueueOfferResult::ENQUEUED
                when OverflowStrategy::Fail
                  error = BufferOverflowError.new("stream buffer capacity #{@capacity} exceeded")
                  @terminal = StreamFailed(T).new(error)
                  notify_state_locked
                  result = QueueOfferResult.failure(error)
                end
              end
            end

            return result.not_nil! if result

            select
            when state_signal.not_nil!.receive?
            when @cancel_signal.receive?
              return QueueOfferResult::QUEUE_CLOSED
            end
          end
        end

        def complete
          set_terminal(StreamCompleted(T).new)
        end

        def fail(error : Exception)
          set_terminal(StreamFailed(T).new(error))
        end

        def receive : StreamEvent(T)
          receive_internal(nil)
        end

        def receive(*, until_cancelled_by downstream : StageChannel(U)) : StreamEvent(T) forall U
          receive_internal(downstream.cancellation_signal)
        end

        protected def cancellation_signal : Channel(Nil)
          @cancel_signal
        end

        private def receive_internal(downstream_cancel_signal : Channel(Nil)?) : StreamEvent(T)
          loop do
            event = nil.as(StreamEvent(T)?)
            state_signal = nil.as(Channel(Nil)?)
            cancelled = false
            downstream_cancelled = false
            terminal = false

            @state_mutex.synchronize do
              if downstream_cancel_signal.try &.closed?
                downstream_cancelled = true
              elsif @cancelled || @terminal_delivered
                cancelled = true
              elsif @buffer.empty?
                if pending_terminal = @terminal
                  event = pending_terminal
                  @terminal = nil
                  @terminal_delivered = true
                  terminal = true
                else
                  state_signal = @state_signal
                end
              else
                event = StreamElement(T).new(@buffer.shift)
                notify_state_locked
              end
            end

            if downstream_cancelled
              raise StreamCancelledError.new("downstream cancelled the stream")
            end
            raise StreamCancelledError.new("stream edge was cancelled") if cancelled
            if received = event
              @runtime.unregister(@registration_id) if terminal
              return received
            end

            if downstream_cancel = downstream_cancel_signal
              select
              when state_signal.not_nil!.receive?
              when @cancel_signal.receive?
                raise StreamCancelledError.new("stream edge was cancelled")
              when downstream_cancel.receive?
                raise StreamCancelledError.new("downstream cancelled the stream")
              end
            else
              select
              when state_signal.not_nil!.receive?
              when @cancel_signal.receive?
                raise StreamCancelledError.new("stream edge was cancelled")
              end
            end
          end
        end

        def cancel
          changed = @state_mutex.synchronize do
            if @cancelled
              false
            else
              @cancelled = true
              @buffer.clear
              @terminal = nil
              @cancel_signal.close
              notify_state_locked
              true
            end
          end
          @runtime.unregister(@registration_id) if changed
        end

        def cancel_from_runtime : Nil
          cancel
        end

        def cancelled? : Bool
          @state_mutex.synchronize { @cancelled || @terminal_delivered }
        end

        private def set_terminal(event : StreamEvent(T))
          cancelled = @state_mutex.synchronize do
            if @cancelled
              true
            elsif @terminal || @terminal_delivered
              false
            else
              @terminal = event
              notify_state_locked
              false
            end
          end
          raise StreamCancelledError.new("downstream cancelled the stream edge") if cancelled
        end

        private def notify_state_locked
          previous_signal = @state_signal
          @state_signal = Channel(Nil).new
          previous_signal.close
        end
      end

      # Materialized control for a manual source blueprint.
      class ManualSourceControl(T)
        @terminal = false
        @state_mutex = Mutex.new
        @send_mutex = Mutex.new

        def initialize(@outlet : StageChannel(T))
        end

        def <<(value : T)
          result = offer(value)
          case result.status
          when QueueOfferStatus::Enqueued, QueueOfferStatus::Dropped
            nil
          when QueueOfferStatus::QueueClosed
            raise StreamClosedError.new("manual source was cancelled downstream")
          when QueueOfferStatus::Failure
            raise result.error.not_nil!
          end
        end

        def offer(value : T) : QueueOfferResult
          @send_mutex.synchronize do
            if terminal?
              QueueOfferResult::QUEUE_CLOSED
            else
              result = @outlet.offer(value)
              if result.status.in?(QueueOfferStatus::QueueClosed, QueueOfferStatus::Failure)
                mark_terminal
              end
              result
            end
          end
        rescue StreamCancelledError
          mark_terminal
          QueueOfferResult::QUEUE_CLOSED
        rescue ex : Exception
          mark_terminal
          QueueOfferResult.failure(ex)
        end

        def complete
          @send_mutex.synchronize do
            return unless mark_terminal
            begin
              @outlet.complete
            rescue StreamCancelledError
              mark_terminal
              # Downstream already selected a terminal outcome.
            end
          end
        end

        def fail(error : Exception)
          @send_mutex.synchronize do
            return unless mark_terminal
            begin
              @outlet.fail(error)
            rescue StreamCancelledError
              # Downstream already selected a terminal outcome.
            end
          end
        end

        def cancel : Nil
          mark_terminal
          @outlet.cancel
        end

        def terminal? : Bool
          @state_mutex.synchronize { @terminal } || @outlet.cancelled?
        end

        private def mark_terminal : Bool
          @state_mutex.synchronize do
            return false if @terminal
            @terminal = true
            true
          end
        end
      end

      record Materialization(Out, Mat), outlet : StageChannel(Out), value : Mat

      module BlueprintRuntime
        @@materialization_scopes = {} of UInt64 => Array(MaterializationScope)
        @@materialization_scope_mutex = Mutex.new

        def self.validate_buffer_size(buffer_size : Int32)
          raise ArgumentError.new("buffer_size must be greater than zero") unless buffer_size > 0
        end

        def self.execute(system : AbstractActorSystem, &block : -> Nil)
          system.dispatchers.default.execute { block.call }
        end

        def self.materialize(&block : -> R) : R forall R
          scope = MaterializationScope.new
          fiber_id = Fiber.current.object_id
          @@materialization_scope_mutex.synchronize do
            stack = @@materialization_scopes[fiber_id] ||= [] of MaterializationScope
            stack << scope
          end

          succeeded = false
          begin
            result = block.call
            succeeded = true
            result
          ensure
            parent = @@materialization_scope_mutex.synchronize do
              stack = @@materialization_scopes[fiber_id]
              popped = stack.pop
              raise "materialization scope stack corrupted" unless popped.same?(scope)
              parent_scope = stack.last?
              @@materialization_scopes.delete(fiber_id) if stack.empty?
              parent_scope
            end

            if succeeded
              scope.commit_to(parent)
            else
              scope.rollback
            end
          end
        end

        def self.track_rollback(&rollback : -> Nil)
          scope = @@materialization_scope_mutex.synchronize do
            @@materialization_scopes[Fiber.current.object_id]?.try &.last?
          end
          scope.try { |current| current.track { rollback.call } }
        end
      end

      # Reusable source blueprint with an output element type and a
      # materialized-value type.
      class Source(Out, Mat)
        @materializer : Proc(AbstractActorSystem, Materialization(Out, Mat))

        def initialize(&block : AbstractActorSystem -> Materialization(Out, Mat))
          @materializer = block
        end

        # Internal materialization hook used by composition.
        def materialize(system : AbstractActorSystem) : Materialization(Out, Mat)
          BlueprintRuntime.materialize { @materializer.call(system) }
        end

        def via(flow : Flow(Out, Next, FlowMat)) : Source(Next, Mat) forall Next, FlowMat
          source = self
          Source(Next, Mat).new do |system|
            source_materialization = source.materialize(system)
            flow_materialization = flow.materialize(system, source_materialization.outlet)
            Materialization(Next, Mat).new(flow_materialization.outlet, source_materialization.value)
          end
        end

        def via_mat(
          flow : Flow(Out, Next, FlowMat),
          &combine : Mat, FlowMat -> Combined
        ) : Source(Next, Combined) forall Next, FlowMat, Combined
          source = self
          Source(Next, Combined).new do |system|
            source_materialization = source.materialize(system)
            flow_materialization = flow.materialize(system, source_materialization.outlet)
            combined = combine.call(source_materialization.value, flow_materialization.value)
            Materialization(Next, Combined).new(flow_materialization.outlet, combined)
          end
        end

        def to(sink : Sink(Out, SinkMat)) : RunnableGraph(SinkMat) forall SinkMat
          source = self
          RunnableGraph(SinkMat).new do |system|
            source_materialization = source.materialize(system)
            sink.materialize(system, source_materialization.outlet)
          end
        end

        def to_mat(
          sink : Sink(Out, SinkMat),
          &combine : Mat, SinkMat -> Combined
        ) : RunnableGraph(Combined) forall SinkMat, Combined
          source = self
          RunnableGraph(Combined).new do |system|
            source_materialization = source.materialize(system)
            sink_mat = sink.materialize(system, source_materialization.outlet)
            combine.call(source_materialization.value, sink_mat)
          end
        end
      end

      # Reusable one-in/one-out flow blueprint. In and Out may differ.
      class Flow(In, Out, Mat)
        @materializer : Proc(AbstractActorSystem, StageChannel(In), Materialization(Out, Mat))

        def initialize(&block : AbstractActorSystem, StageChannel(In) -> Materialization(Out, Mat))
          @materializer = block
        end

        # Internal materialization hook used by composition.
        def materialize(system : AbstractActorSystem, inlet : StageChannel(In)) : Materialization(Out, Mat)
          BlueprintRuntime.materialize { @materializer.call(system, inlet) }
        end

        def via(flow : Flow(Out, Next, NextMat)) : Flow(In, Next, Mat) forall Next, NextMat
          first = self
          Flow(In, Next, Mat).new do |system, inlet|
            first_materialization = first.materialize(system, inlet)
            next_materialization = flow.materialize(system, first_materialization.outlet)
            Materialization(Next, Mat).new(next_materialization.outlet, first_materialization.value)
          end
        end

        def via_mat(
          flow : Flow(Out, Next, NextMat),
          &combine : Mat, NextMat -> Combined
        ) : Flow(In, Next, Combined) forall Next, NextMat, Combined
          first = self
          Flow(In, Next, Combined).new do |system, inlet|
            first_materialization = first.materialize(system, inlet)
            next_materialization = flow.materialize(system, first_materialization.outlet)
            combined = combine.call(first_materialization.value, next_materialization.value)
            Materialization(Next, Combined).new(next_materialization.outlet, combined)
          end
        end
      end

      # Reusable sink blueprint with an input element type and a materialized
      # value, commonly a Future of the sink result.
      class Sink(In, Mat)
        @materializer : Proc(AbstractActorSystem, StageChannel(In), Mat)

        def initialize(&block : AbstractActorSystem, StageChannel(In) -> Mat)
          @materializer = block
        end

        # Internal materialization hook used by composition.
        def materialize(system : AbstractActorSystem, inlet : StageChannel(In)) : Mat
          BlueprintRuntime.materialize { @materializer.call(system, inlet) }
        end
      end

      # Closed reusable graph. Every run allocates an independent runtime and
      # returns a new materialized value.
      class RunnableGraph(Mat)
        @materializer : Proc(AbstractActorSystem, Mat)

        def initialize(&block : AbstractActorSystem -> Mat)
          @materializer = block
        end

        def run(system : AbstractActorSystem) : Mat
          BlueprintRuntime.materialize { @materializer.call(system) }
        end
      end

      module Sources
        def self.manual(
          type : T.class,
          buffer_size : Int32 = DEFAULT_BUFFER_SIZE,
          overflow_strategy : OverflowStrategy = OverflowStrategy::Backpressure,
        ) : Source(T, ManualSourceControl(T)) forall T
          BlueprintRuntime.validate_buffer_size(buffer_size)
          Source(T, ManualSourceControl(T)).new do |system|
            outlet = StageChannel(T).new(system, buffer_size, overflow_strategy)
            Materialization(T, ManualSourceControl(T)).new(outlet, ManualSourceControl(T).new(outlet))
          end
        end
      end

      module Flows
        def self.map(
          input_type : In.class,
          output_type : Out.class,
          buffer_size : Int32 = DEFAULT_BUFFER_SIZE,
          overflow_strategy : OverflowStrategy = OverflowStrategy::Backpressure,
          &transform : In -> Out
        ) : Flow(In, Out, NotUsed) forall In, Out
          BlueprintRuntime.validate_buffer_size(buffer_size)
          Flow(In, Out, NotUsed).new do |system, inlet|
            outlet = StageChannel(Out).new(system, buffer_size, overflow_strategy)
            BlueprintRuntime.execute(system) do
              begin
                loop do
                  case event = inlet.receive(until_cancelled_by: outlet)
                  when StreamElement(In)
                    transformed = begin
                      transform.call(event.value)
                    rescue ex : Exception
                      inlet.cancel
                      begin
                        outlet.fail(ex)
                      rescue StreamCancelledError
                      end
                      break
                    end
                    begin
                      outlet.push(transformed)
                    rescue StreamCancelledError
                      inlet.cancel
                      break
                    rescue BufferOverflowError
                      inlet.cancel
                      break
                    end
                  when StreamCompleted(In)
                    outlet.complete
                    break
                  when StreamFailed(In)
                    outlet.fail(event.error)
                    break
                  end
                end
              rescue StreamCancelledError
                inlet.cancel
                outlet.cancel
              end
            end
            Materialization(Out, NotUsed).new(outlet, NotUsed::INSTANCE)
          end
        end
      end

      module Sinks
        def self.collect(type : T.class) : Sink(T, Future(Array(T))) forall T
          Sink(T, Future(Array(T))).new do |system, inlet|
            promise = Promise(Array(T)).new
            BlueprintRuntime.execute(system) do
              values = [] of T
              begin
                loop do
                  case event = inlet.receive
                  when StreamElement(T)
                    values << event.value
                  when StreamCompleted(T)
                    promise.try_success(values)
                    break
                  when StreamFailed(T)
                    promise.try_failure(event.error)
                    break
                  end
                end
              rescue StreamCancelledError
                promise.try_cancel
              end
            end
            promise.future
          end
        end

        def self.fold(
          type : T.class,
          initial : R,
          &reducer : R, T -> R
        ) : Sink(T, Future(R)) forall T, R
          Sink(T, Future(R)).new do |system, inlet|
            promise = Promise(R).new
            BlueprintRuntime.execute(system) do
              accumulator = initial
              begin
                loop do
                  case event = inlet.receive
                  when StreamElement(T)
                    begin
                      accumulator = reducer.call(accumulator, event.value)
                    rescue ex : Exception
                      inlet.cancel
                      promise.try_failure(ex)
                      break
                    end
                  when StreamCompleted(T)
                    promise.try_success(accumulator)
                    break
                  when StreamFailed(T)
                    promise.try_failure(event.error)
                    break
                  end
                end
              rescue StreamCancelledError
                promise.try_cancel
              end
            end
            promise.future
          end
        end
      end
    end
  end
end
