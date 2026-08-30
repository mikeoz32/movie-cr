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

      # Runtime edge used by the reusable blueprint API. Task 07.2 replaces
      # this rendezvous channel with configurable bounded queues.
      class StageChannel(T)
        @channel = Channel(StreamEvent(T)).new
        @cancel_signal = Channel(Nil).new
        @cancelled = false
        @cancel_mutex = Mutex.new
        @runtime : StreamRuntimeExtension
        @registration_id : UInt64

        def initialize(system : AbstractActorSystem)
          raise StreamClosedError.new("actor system is shutting down") if system.shutting_down?
          @runtime = system.extensions.get_or_register(StreamRuntimeExtension) do
            StreamRuntimeExtension.new
          end
          @registration_id = @runtime.register { cancel_from_runtime }
        end

        def push(value : T)
          send_event(StreamElement(T).new(value))
        end

        def complete
          send_terminal_event(StreamCompleted(T).new)
        end

        def fail(error : Exception)
          send_terminal_event(StreamFailed(T).new(error))
        end

        def receive : StreamEvent(T)
          select
          when event = @channel.receive
            event
          when @cancel_signal.receive?
            raise StreamCancelledError.new("stream edge was cancelled")
          end
        end

        def cancel
          changed = @cancel_mutex.synchronize do
            if @cancelled
              false
            else
              @cancelled = true
              @cancel_signal.close
              true
            end
          end
          @runtime.unregister(@registration_id) if changed
        end

        def cancel_from_runtime : Nil
          cancel
        end

        def cancelled? : Bool
          @cancel_mutex.synchronize { @cancelled }
        end

        private def send_event(event : StreamEvent(T))
          select
          when @channel.send(event)
          when @cancel_signal.receive?
            raise StreamCancelledError.new("downstream cancelled the stream edge")
          end
        end

        private def send_terminal_event(event : StreamEvent(T))
          send_event(event)
        ensure
          @runtime.unregister(@registration_id)
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
          @send_mutex.synchronize do
            ensure_active
            begin
              @outlet.push(value)
            rescue StreamCancelledError
              mark_terminal
              raise StreamClosedError.new("manual source was cancelled downstream")
            end
          end
        end

        def complete
          @send_mutex.synchronize do
            return unless mark_terminal
            begin
              @outlet.complete
            rescue StreamCancelledError
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

        def terminal? : Bool
          @state_mutex.synchronize { @terminal } || @outlet.cancelled?
        end

        private def ensure_active
          @state_mutex.synchronize do
            raise StreamClosedError.new("manual source is already terminal") if @terminal
          end
          raise StreamClosedError.new("manual source was cancelled downstream") if @outlet.cancelled?
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
        def self.execute(system : AbstractActorSystem, &block : -> Nil)
          system.dispatchers.default.execute { block.call }
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
          @materializer.call(system)
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
          @materializer.call(system, inlet)
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
          @materializer.call(system, inlet)
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
          @materializer.call(system)
        end
      end

      module Sources
        def self.manual(type : T.class) : Source(T, ManualSourceControl(T)) forall T
          Source(T, ManualSourceControl(T)).new do |system|
            outlet = StageChannel(T).new(system)
            Materialization(T, ManualSourceControl(T)).new(outlet, ManualSourceControl(T).new(outlet))
          end
        end
      end

      module Flows
        def self.map(
          input_type : In.class,
          output_type : Out.class,
          &transform : In -> Out
        ) : Flow(In, Out, NotUsed) forall In, Out
          Flow(In, Out, NotUsed).new do |system, inlet|
            outlet = StageChannel(Out).new(system)
            BlueprintRuntime.execute(system) do
              begin
                loop do
                  case event = inlet.receive
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
