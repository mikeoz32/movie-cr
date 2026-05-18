require "mutex"

module Movie
  class FutureCancelled < Exception
  end

  class FutureAlreadyCompleted < Exception
  end

  class FutureTimeout < Exception
  end

  # Handle for cancelling callback subscriptions on Future
  class FutureSubscription
    def initialize(@cancel_proc : Proc(Nil))
    end

    def cancel
      @cancel_proc.call
    end
  end

  enum FutureStatus
    Pending
    Success
    Failure
    Cancelled
  end

  struct FutureResult(T)
    getter status : FutureStatus
    getter value : T?
    getter error : Exception?

    def initialize(@status : FutureStatus, @value : T? = nil, @error : Exception? = nil)
    end

    def success?
      @status == FutureStatus::Success
    end

    def failure?
      @status == FutureStatus::Failure
    end

    def cancelled?
      @status == FutureStatus::Cancelled
    end

    def pending?
      @status == FutureStatus::Pending
    end
  end

  class Future(T)
    def initialize
      @mutex = Mutex.new
      @status = FutureStatus::Pending
      @value = nil.as(T?)
      @error = nil.as(Exception?)
      @callbacks = [] of NamedTuple(id: Int32, kind: Symbol, cb: Proc(FutureResult(T), Nil))
      @waiters = [] of Channel(Nil)
      @next_callback_id = 0
    end

    def status : FutureStatus
      @mutex.synchronize { @status }
    end

    def pending?
      status == FutureStatus::Pending
    end

    def success?
      status == FutureStatus::Success
    end

    def failure?
      status == FutureStatus::Failure
    end

    def cancelled?
      status == FutureStatus::Cancelled
    end

    def result : FutureResult(T)
      @mutex.synchronize { FutureResult(T).new(@status, @value, @error) }
    end

    def await(timeout : Time::Span? = nil) : T
      res = wait_result(timeout)
      case res.status
      when FutureStatus::Success
        res.value.as(T)
      when FutureStatus::Failure
        raise res.error.not_nil!
      when FutureStatus::Cancelled
        raise FutureCancelled.new
      else
        raise "Future not completed"
      end
    end

    def on_complete(&block : FutureResult(T) ->) : FutureSubscription
      register_callback(:complete, block)
    end

    def on_success(&block : T ->) : FutureSubscription
      register_callback(:success, ->(res : FutureResult(T)) { block.call(res.value.as(T)) })
    end

    def on_failure(&block : Exception ->) : FutureSubscription
      register_callback(:failure, ->(res : FutureResult(T)) { block.call(res.error.not_nil!) })
    end

    def on_cancel(&block : ->) : FutureSubscription
      register_callback(:cancel, ->(_res : FutureResult(T)) { block.call })
    end

    protected def try_complete_success(value : T) : Bool
      publish(FutureStatus::Success, value, nil)
    end

    protected def try_complete_failure(error : Exception) : Bool
      publish(FutureStatus::Failure, nil, error)
    end

    protected def try_complete_cancel : Bool
      publish(FutureStatus::Cancelled, nil, nil)
    end

    private def terminal?
      @status != FutureStatus::Pending
    end

    private def wait_result(timeout : Time::Span?) : FutureResult(T)
      wait_ch = nil
      snapshot = nil

      @mutex.synchronize do
        if terminal?
          snapshot = FutureResult(T).new(@status, @value, @error)
        else
          wait_ch = Channel(Nil).new
          @waiters << wait_ch
        end
      end

      return snapshot.not_nil! if snapshot

      timed_out = false

      if timeout
        select
        when wait_ch.not_nil!.receive
        when timeout(timeout)
          timed_out = true
        end
      else
        wait_ch.not_nil!.receive
      end

      if timed_out
        @mutex.synchronize { @waiters.delete(wait_ch.not_nil!) }
        raise FutureTimeout.new unless terminal?
      end

      @mutex.synchronize { FutureResult(T).new(@status, @value, @error) }
    end

    private def publish(status : FutureStatus, value : T?, error : Exception?) : Bool
      callbacks = [] of Proc(FutureResult(T), Nil)
      waiters = [] of Channel(Nil)
      snapshot = nil
      transitioned = false
      @mutex.synchronize do
        return false if terminal?
        @status = status
        @value = value
        @error = error
        snapshot = FutureResult(T).new(@status, @value, @error)
        callbacks = select_callbacks(@status)
        waiters = @waiters
        @waiters = [] of Channel(Nil)
        transitioned = true
      end
      callbacks.each { |cb| cb.call(snapshot.not_nil!) }
      waiters.each do |waiter|
        begin
          waiter.send(nil)
        rescue Channel::ClosedError
        end
      end
      transitioned
    end

    private def register_callback(kind : Symbol, block : Proc(FutureResult(T), Nil)) : FutureSubscription
      snapshot = nil
      id = 0
      @mutex.synchronize do
        if terminal?
          snapshot = FutureResult(T).new(@status, @value, @error)
        else
          @next_callback_id += 1
          id = @next_callback_id
          @callbacks << {id: id, kind: kind, cb: block}
        end
      end

      if snapshot
        if callback_applicable?(kind, snapshot.status)
          block.call(snapshot)
        end
        return FutureSubscription.new(->{} )
      end

      cancel_proc = -> do
        @mutex.synchronize do
          @callbacks.reject! { |entry| entry[:id] == id }
        end
      end
      FutureSubscription.new(cancel_proc)
    end

    private def select_callbacks(status : FutureStatus)
      callbacks = [] of Proc(FutureResult(T), Nil)
      @callbacks.each do |entry|
        callbacks << entry[:cb] if callback_applicable?(entry[:kind], status)
      end
      @callbacks.clear
      callbacks
    end

    private def callback_applicable?(kind : Symbol, status : FutureStatus)
      case kind
      when :complete
        true
      when :success
        status == FutureStatus::Success
      when :failure
        status == FutureStatus::Failure
      when :cancel
        status == FutureStatus::Cancelled
      else
        false
      end
    end
  end

  class Promise(T)
    getter future : Future(T)

    def initialize
      @future = Future(T).new
    end

    def success(value : T) : Nil
      raise FutureAlreadyCompleted.new unless try_success(value)
    end

    def failure(error : Exception) : Nil
      raise FutureAlreadyCompleted.new unless try_failure(error)
    end

    def cancel : Nil
      raise FutureAlreadyCompleted.new unless try_cancel
    end

    def try_success(value : T) : Bool
      @future.try_complete_success(value)
    end

    def try_failure(error : Exception) : Bool
      @future.try_complete_failure(error)
    end

    def try_cancel : Bool
      @future.try_complete_cancel
    end
  end
end
