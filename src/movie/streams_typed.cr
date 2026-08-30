module Movie
  module Streams
    enum StageState
      Active
      Completed
      Failed
      Cancelled
    end

    module Typed
      abstract class MessageBase(T)
      end

      class Subscribe(T) < MessageBase(T)
        getter subscriber : ActorRefBase

        def initialize(@subscriber : ActorRefBase)
        end
      end

      class OnSubscribe(T) < MessageBase(T)
        getter subscription : Subscription(T)

        def initialize(@subscription : Subscription(T))
        end
      end

      class Request(T) < MessageBase(T)
        getter n : UInt64

        def initialize(@n : UInt64)
        end
      end

      class SubscriptionRequest(T) < Request(T)
        getter subscriber : ActorRefBase

        def initialize(n : UInt64, @subscriber : ActorRefBase)
          super(n)
        end
      end

      class Cancel(T) < MessageBase(T)
      end

      class SubscriptionCancel(T) < Cancel(T)
        getter subscriber : ActorRefBase

        def initialize(@subscriber : ActorRefBase)
        end
      end

      class OnNext(T) < MessageBase(T)
        getter elem : T

        def initialize(@elem : T)
        end
      end

      class OnComplete(T) < MessageBase(T)
      end

      class OnError(T) < MessageBase(T)
        getter error : Exception

        def initialize(@error : Exception)
        end
      end

      class Produce(T) < MessageBase(T)
        getter elem : T

        def initialize(@elem : T)
        end
      end

      class Subscription(T)
        @closed = false

        def initialize(@ref : ActorRef(MessageBase(T)), @subscriber : ActorRefBase? = nil)
        end

        def request(n : UInt64)
          return if @closed || n == 0
          if subscriber = @subscriber
            @ref << SubscriptionRequest(T).new(n, subscriber)
          else
            @ref << Request(T).new(n)
          end
        end

        def cancel
          return if @closed
          @closed = true
          if subscriber = @subscriber
            @ref << SubscriptionCancel(T).new(subscriber)
          else
            @ref << Cancel(T).new
          end
        end
      end

      # Shared cancellation signal for a collect sink and its materialized
      # handle. Closing the signal wakes a delivery fiber even when it is
      # waiting on an unbuffered output channel.
      class DeliveryControl
        getter stop_signal = Channel(Nil).new
        @mutex = Mutex.new
        @stopped = false

        def stop
          @mutex.synchronize do
            return if @stopped
            @stopped = true
            @stop_signal.close
          end
        end
      end

      # Asynchronously preserves output order without letting an unbuffered
      # consumer channel block the CollectSink actor's control mailbox.
      private class DeliveryPump(T)
        @queue = [] of T
        @mutex = Mutex.new
        @wakeup = Channel(Nil).new(1)
        @started = false
        @finishing = false

        def initialize(@out : Channel(T), @control : DeliveryControl)
        end

        def enqueue(element : T)
          start unless @started
          @mutex.synchronize { @queue << element }
          wake
        end

        def finish
          return unless @started
          @mutex.synchronize { @finishing = true }
          wake
        end

        def stop
          @control.stop
        end

        private def start
          @started = true
          spawn { deliver }
        end

        private def deliver
          loop do
            has_element = false
            element : T? = nil
            finishing = false

            @mutex.synchronize do
              unless @queue.empty?
                element = @queue.shift
                has_element = true
              end
              finishing = @finishing && @queue.empty? && !has_element
            end

            break if finishing

            if has_element
              select
              when @out.send(element.as(T))
              when @control.stop_signal.receive?
                clear
                break
              end
            else
              select
              when @wakeup.receive
              when @control.stop_signal.receive?
                clear
                break
              end
            end
          end
        rescue Channel::ClosedError
          clear
        end

        private def wake
          select
          when @wakeup.send(nil)
          else
          end
        end

        private def clear
          @mutex.synchronize { @queue.clear }
        end
      end

      class ManualSource(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @buffer : Array(T) = [] of T
        @pending_complete : Bool = false

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when Produce(T)
            handle_produce(message)
          when OnComplete(T)
            handle_complete
          when OnError(T)
            fail(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
          drain_buffer
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @demand = clamp_add(@demand, msg.n)
          drain_buffer
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
        end

        private def handle_produce(msg : Produce(T))
          return if terminal?
          unless @downstream
            @buffer << msg.elem
            return
          end

          if @demand == 0
            @buffer << msg.elem
            return
          end

          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @demand -= 1
          try_emit_complete
        end

        private def handle_complete
          return if terminal?
          @pending_complete = true
          try_emit_complete
        end

        private def fail(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @state = Streams::StageState::Failed
        end

        private def drain_buffer
          return unless @downstream
          while @demand > 0 && (elem = @buffer.shift?)
            @downstream.not_nil! << OnNext(T).new(elem)
            @demand -= 1
            try_emit_complete
          end
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def try_emit_complete
          return unless @pending_complete
          return unless @downstream
          return unless @buffer.empty?
          @downstream.not_nil! << OnComplete(T).new
          @pending_complete = false
          @state = Streams::StageState::Completed
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      class PassThroughFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return unless @downstream
          return if @downstream_demand == 0
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      # Map flow transforms elements while respecting demand.
      class MapFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @fn : T -> T

        def initialize(&block : T -> T)
          @fn = block
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return unless @downstream
          return if @downstream_demand == 0
          transformed = @fn.call(msg.elem)
          @downstream.not_nil! << OnNext(T).new(transformed)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      # Tap flow executes a side-effect and passes elements through unchanged.
      class TapFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @fn : T ->

        def initialize(&block : T ->)
          @fn = block
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return unless @downstream
          return if @downstream_demand == 0
          spawn { @fn.call(msg.elem) }
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      # Filter flow drops elements that do not satisfy predicate without consuming demand.
      class FilterFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @pred : T -> Bool

        def initialize(&block : T -> Bool)
          @pred = block
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return unless @downstream
          unless @pred.call(msg.elem)
            if @downstream_demand > 0
              @upstream.try &.request(1_u64)
            end
            return
          end
          return if @downstream_demand == 0
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      # Take flow completes after emitting N elements.
      class TakeFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @remaining : UInt64

        def initialize(n : UInt64)
          @remaining = n
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return if @remaining == 0
          return unless @downstream
          return if @downstream_demand == 0
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
          @remaining -= 1
          if @remaining == 0
            complete_take
          end
        end

        private def complete_take
          @upstream.try &.cancel
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      # Drop flow discards the first N elements before forwarding.
      class DropFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @pending_drop : UInt64

        def initialize(n : UInt64)
          @pending_drop = n
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          extra = @pending_drop > 0 ? clamp_add(msg.n, @pending_drop) : msg.n
          @upstream.try &.request(extra)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          if @pending_drop > 0
            @pending_drop -= 1
            @upstream.try &.request(1_u64) if @downstream_demand > 0
            return
          end
          return unless @downstream
          return if @downstream_demand == 0
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            extra = @pending_drop > 0 ? clamp_add(@downstream_demand, @pending_drop) : @downstream_demand
            @upstream.try &.request(extra)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      class BroadcastHub(T) < AbstractBehavior(MessageBase(T))
        record SubscriberState(T), ref : ActorRef(MessageBase(T)), demand : UInt64

        @upstream : Subscription(T)?
        @subscribers : Hash(Int32, SubscriberState(T))
        @state : Streams::StageState = Streams::StageState::Active
        @pending_upstream_cancel : Bool = false
        @in_flight : UInt64 = 0u64
        @failure : Exception? = nil

        def initialize
          @subscribers = {} of Int32 => SubscriberState(T)
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel(message)
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            handle_complete
          when OnError(T)
            handle_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          subscriber = msg.subscriber.as(ActorRef(MessageBase(T)))
          case @state
          when Streams::StageState::Active
            unless @subscribers.has_key?(subscriber.id)
              @subscribers[subscriber.id] = SubscriberState(T).new(subscriber, 0u64)
            end
            subscriber << OnSubscribe(T).new(Subscription(T).new(context.ref, subscriber))
          when Streams::StageState::Completed, Streams::StageState::Cancelled
            subscriber << OnComplete(T).new
          when Streams::StageState::Failed
            subscriber << OnError(T).new(@failure || Exception.new("stream failed"))
          end
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          sub_msg = msg.as?(SubscriptionRequest(T))
          return unless sub_msg
          return if msg.n == 0
          state = @subscribers[sub_msg.subscriber.id]?
          return unless state
          @subscribers[sub_msg.subscriber.id] = SubscriberState(T).new(
            state.ref,
            clamp_add(state.demand, msg.n)
          )
          sync_upstream_demand
        end

        private def handle_cancel(msg : Cancel(T))
          return if terminal?
          sub_msg = msg.as?(SubscriptionCancel(T))
          if sub_msg
            @subscribers.delete(sub_msg.subscriber.id)
            if @subscribers.empty?
              cancel_upstream
            else
              sync_upstream_demand
            end
            return
          end

          @subscribers.clear
          cancel_upstream
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          @in_flight -= 1 if @in_flight > 0

          @subscribers.keys.each do |id|
            state = @subscribers[id]?
            next unless state
            next if state.demand == 0
            state.ref << OnNext(T).new(msg.elem)
            @subscribers[id] = SubscriberState(T).new(state.ref, state.demand - 1)
          end

          sync_upstream_demand
        end

        private def handle_complete
          return if terminal?
          @subscribers.each_value { |state| state.ref << OnComplete(T).new }
          @subscribers.clear
          @state = Streams::StageState::Completed
        end

        private def handle_error(error : Exception)
          return if terminal?
          @subscribers.each_value { |state| state.ref << OnError(T).new(error) }
          @subscribers.clear
          @failure = error
          @state = Streams::StageState::Failed
        end

        private def sync_upstream_demand
          return unless @state == Streams::StageState::Active
          desired = 0u64
          @subscribers.each_value do |state|
            desired = state.demand if state.demand > desired
          end
          return unless desired > @in_flight
          to_request = desired - @in_flight
          @upstream.try &.request(to_request)
          @in_flight = clamp_add(@in_flight, to_request)
        end

        private def cancel_upstream
          @state = Streams::StageState::Cancelled
          @pending_upstream_cancel = true unless @upstream
          @upstream.try &.cancel
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_upstream_cancel
            @upstream.try &.cancel
            @pending_upstream_cancel = false
            return
          end
          sync_upstream_demand
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      class CollectSink(T) < AbstractBehavior(MessageBase(T))
        getter state : Streams::StageState = Streams::StageState::Active
        @upstream : Subscription(T)?
        @signals : Channel(Symbol)?
        @pending_demand : UInt64 = 0u64
        @delivery_pump : DeliveryPump(T)

        def initialize(
          output : Channel(T),
          @signals : Channel(Symbol)? = nil,
          @delivery_control : DeliveryControl = DeliveryControl.new,
        )
          @delivery_pump = DeliveryPump(T).new(output, @delivery_control)
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when OnSubscribe(T)
            @upstream = message.subscription
            flush_pending_demand
            cancel_if_needed
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            handle_complete
          when OnError(T)
            handle_error
          end
          Behaviors(MessageBase(T)).same
        end

        def on_signal(signal : SystemMessage)
          @delivery_pump.stop if signal.is_a?(PreStop)
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          @delivery_pump.enqueue(msg.elem)
        end

        private def handle_request(msg : Request(T))
          return if terminal?
          if up = @upstream
            up.request(msg.n)
          else
            @pending_demand = clamp_add(@pending_demand, msg.n)
          end
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @delivery_pump.stop
          if up = @upstream
            up.cancel
          end
          notify(:cancel)
        end

        private def handle_complete
          return if terminal?
          @state = Streams::StageState::Completed
          @delivery_pump.finish
          notify(:complete)
        end

        private def handle_error
          return if terminal?
          @state = Streams::StageState::Failed
          @upstream.try &.cancel
          @delivery_pump.finish
          notify(:error)
        end

        private def flush_pending_demand
          return unless @pending_demand > 0
          if up = @upstream
            up.request(@pending_demand)
            @pending_demand = 0u64
          end
        end

        private def cancel_if_needed
          return unless @state == Streams::StageState::Cancelled
          @upstream.try &.cancel
        end

        private def notify(sym : Symbol)
          @signals.try &.send(sym)
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      struct MaterializedPipeline(T, R)
        getter system : ActorSystem(MessageBase(T))
        getter source : ActorRef(MessageBase(T))
        getter sink : ActorRef(MessageBase(T))
        getter completion : Future(R)
        getter cancel : ->
        getter out_channel : Channel(T)?

        def initialize(
          @system : ActorSystem(MessageBase(T)),
          @source : ActorRef(MessageBase(T)),
          @sink : ActorRef(MessageBase(T)),
          @completion : Future(R),
          @cancel : ->,
          @out_channel : Channel(T)? = nil,
        )
        end
      end

      class CompletionFlow(T) < AbstractBehavior(MessageBase(T))
        @downstream : ActorRef(MessageBase(T))?
        @upstream : Subscription(T)?
        @downstream_demand : UInt64 = 0u64
        @state : Streams::StageState = Streams::StageState::Active
        @pending_cancel : Bool = false
        @promise : Promise(Nil)

        def initialize(@promise : Promise(Nil))
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when Subscribe(T)
            handle_subscribe(message, context)
          when OnSubscribe(T)
            @upstream = message.subscription
            propagate_pending
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            forward_complete
          when OnError(T)
            forward_error(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_subscribe(msg : Subscribe(T), context : ActorContext(MessageBase(T)))
          return if @state != Streams::StageState::Active
          return if @downstream
          @downstream = msg.subscriber.as(ActorRef(MessageBase(T)))
          sub = Subscription(T).new(context.ref)
          @downstream.not_nil! << OnSubscribe(T).new(sub)
        end

        private def handle_request(msg : Request(T))
          return unless @state == Streams::StageState::Active
          @downstream_demand = clamp_add(@downstream_demand, msg.n)
          @upstream.try &.request(msg.n)
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @pending_cancel = true unless @upstream
          @upstream.try &.cancel
          @promise.try_cancel
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          return unless @downstream
          return if @downstream_demand == 0
          @downstream.not_nil! << OnNext(T).new(msg.elem)
          @downstream_demand -= 1
        end

        private def forward_complete
          return if terminal?
          if ds = @downstream
            ds << OnComplete(T).new
          end
          @promise.try_success(nil)
          @state = Streams::StageState::Completed
        end

        private def forward_error(error : Exception)
          return if terminal?
          if ds = @downstream
            ds << OnError(T).new(error)
          end
          @upstream.try &.cancel
          @promise.try_failure(error)
          @state = Streams::StageState::Failed
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def propagate_pending
          if @state == Streams::StageState::Cancelled || @pending_cancel
            @upstream.try &.cancel
            @pending_cancel = false
            return
          end

          if @downstream_demand > 0
            @upstream.try &.request(@downstream_demand)
          end
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      class FoldSink(T, R) < AbstractBehavior(MessageBase(T))
        @upstream : Subscription(T)?
        @state : Streams::StageState = Streams::StageState::Active
        @acc : R
        @reducer : R, T -> R
        @promise : Promise(R)
        @pending_demand : UInt64 = 0u64

        def initialize(@acc : R, @reducer : R, T -> R, @promise : Promise(R))
        end

        def receive(message : MessageBase(T), context : ActorContext(MessageBase(T)))
          case message
          when OnSubscribe(T)
            @upstream = message.subscription
            flush_pending_demand
          when Request(T)
            handle_request(message)
          when Cancel(T)
            handle_cancel
          when OnNext(T)
            handle_on_next(message)
          when OnComplete(T)
            complete_success
          when OnError(T)
            fail(message.error)
          end
          Behaviors(MessageBase(T)).same
        end

        private def handle_on_next(msg : OnNext(T))
          return if terminal?
          @acc = @reducer.call(@acc, msg.elem)
        end

        private def handle_request(msg : Request(T))
          return if terminal?
          if up = @upstream
            up.request(msg.n)
          else
            @pending_demand = clamp_add(@pending_demand, msg.n)
          end
        end

        private def handle_cancel
          return if terminal?
          @state = Streams::StageState::Cancelled
          @upstream.try &.cancel
          @promise.try_cancel
        end

        private def complete_success
          return if terminal?
          @promise.try_success(@acc)
          @state = Streams::StageState::Completed
        end

        private def fail(error : Exception)
          return if terminal?
          @upstream.try &.cancel
          @promise.try_failure(error)
          @state = Streams::StageState::Failed
        end

        private def flush_pending_demand
          return unless @pending_demand > 0
          @upstream.try &.request(@pending_demand)
          @pending_demand = 0u64
        end

        private def terminal?
          @state != Streams::StageState::Active
        end

        private def clamp_add(current : UInt64, delta : UInt64) : UInt64
          max = UInt64::MAX
          if delta > max - current
            max
          else
            current + delta
          end
        end
      end

      struct RunnablePipeline(T)
        def initialize(
          @source_behavior : AbstractBehavior(MessageBase(T)),
          @flows : Array(AbstractBehavior(MessageBase(T))),
          @sink_behavior : AbstractBehavior(MessageBase(T)),
          @initial_demand : UInt64 = 0u64,
          @out_channel : Channel(T)? = nil,
          @pre_cancel : Proc(Nil)? = nil,
        )
        end

        def run(system : ActorSystem(MessageBase(T))) : MaterializedPipeline(T, Nil)
          promise = Promise(Nil).new

          src = system.spawn(@source_behavior)
          flow_refs = @flows.map { |flow| system.spawn(flow) }
          completion = system.spawn(CompletionFlow(T).new(promise))
          sink_actor = system.spawn(@sink_behavior)

          chain = flow_refs + [completion]
          downstream = sink_actor
          chain.reverse_each do |up_actor|
            up_actor << Subscribe(T).new(downstream)
            downstream = up_actor
          end

          src << Subscribe(T).new(downstream)
          sink_actor << Request(T).new(@initial_demand) if @initial_demand > 0

          cancel_proc = -> do
            @pre_cancel.try &.call
            sink_actor << Cancel(T).new
          end
          MaterializedPipeline(T, Nil).new(system, src, sink_actor, promise.future, cancel_proc, @out_channel)
        end
      end

      struct RunnableFoldPipeline(T, R)
        def initialize(
          @source_behavior : AbstractBehavior(MessageBase(T)),
          @flows : Array(AbstractBehavior(MessageBase(T))),
          @initial : R,
          @reducer : R, T -> R,
          @initial_demand : UInt64 = 0u64,
        )
        end

        def run(system : ActorSystem(MessageBase(T))) : MaterializedPipeline(T, R)
          promise = Promise(R).new
          fold_sink = FoldSink(T, R).new(@initial, @reducer, promise)

          src = system.spawn(@source_behavior)
          flow_refs = @flows.map { |flow| system.spawn(flow) }
          sink_actor = system.spawn(fold_sink)

          downstream = sink_actor
          flow_refs.reverse_each do |up_actor|
            up_actor << Subscribe(T).new(downstream)
            downstream = up_actor
          end

          src << Subscribe(T).new(downstream)
          sink_actor << Request(T).new(@initial_demand > 0 ? @initial_demand : UInt64::MAX)

          cancel_proc = -> { sink_actor << Cancel(T).new }
          MaterializedPipeline(T, R).new(system, src, sink_actor, promise.future, cancel_proc)
        end
      end

      class StreamBuilder(T)
        def initialize(@source_behavior : AbstractBehavior(MessageBase(T)))
          @flows = [] of AbstractBehavior(MessageBase(T))
        end

        def via(flow : AbstractBehavior(MessageBase(T))) : StreamBuilder(T)
          @flows << flow
          self
        end

        def to(sink : AbstractBehavior(MessageBase(T)), initial_demand : UInt64 = 0u64) : RunnablePipeline(T)
          RunnablePipeline(T).new(@source_behavior, @flows.dup, sink, initial_demand)
        end

        def to_collect(initial_demand : UInt64 = 0u64, channel_capacity : Int32 = 0) : RunnablePipeline(T)
          out_ch = Channel(T).new(channel_capacity)
          delivery_control = DeliveryControl.new
          sink = CollectSink(T).new(out_ch, delivery_control: delivery_control)
          pre_cancel = -> { delivery_control.stop }
          RunnablePipeline(T).new(@source_behavior, @flows.dup, sink, initial_demand, out_ch, pre_cancel)
        end

        def fold(initial : R, reducer : R, T -> R, initial_demand : UInt64 = 0u64) : RunnableFoldPipeline(T, R) forall R
          RunnableFoldPipeline(T, R).new(@source_behavior, @flows.dup, initial, reducer, initial_demand)
        end
      end

      def self.source(source_behavior : AbstractBehavior(MessageBase(T))) : StreamBuilder(T) forall T
        StreamBuilder(T).new(source_behavior)
      end

      def self.manual(type : T.class) : StreamBuilder(T) forall T
        source(ManualSource(T).new)
      end
    end
  end
end
