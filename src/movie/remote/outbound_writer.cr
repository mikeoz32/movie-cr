require "deque"
require "log"
require "sync/condition_variable"
require "sync/mutex"
require "./frame_codec"

module Movie::Remote
  # Connection-owned bounded FIFO that moves frame encoding and socket writes
  # off actor caller fibers. Producers only wait when the queue is saturated.
  class OutboundWriter
    Log = ::Log.for(self)

    DEFAULT_QUEUE_CAPACITY   = 4096
    DEFAULT_MAX_BATCH_FRAMES =  128
    DEFAULT_MAX_BATCH_BYTES  = 64 * 1024

    @queue : Deque(WireEnvelope)
    @drain : Array(WireEnvelope)
    @mutex : Sync::Mutex
    @not_empty : Sync::ConditionVariable
    @not_full : Sync::ConditionVariable
    @encoder : FrameCodec::Encoder
    @batch : IO::Memory
    @started = false
    @accepting = true
    @stopping = false
    @writer_fiber : Fiber?

    def initialize(
      @io : IO,
      @queue_capacity : Int32 = DEFAULT_QUEUE_CAPACITY,
      @max_batch_frames : Int32 = DEFAULT_MAX_BATCH_FRAMES,
      @max_batch_bytes : Int32 = DEFAULT_MAX_BATCH_BYTES,
      @on_error : Proc(Exception, Nil)? = nil,
    )
      raise ArgumentError.new("queue capacity must be positive") unless @queue_capacity > 0
      raise ArgumentError.new("maximum batch frames must be positive") unless @max_batch_frames > 0
      raise ArgumentError.new("maximum batch bytes must be positive") unless @max_batch_bytes > 0

      @queue = Deque(WireEnvelope).new
      @drain = Array(WireEnvelope).new(@max_batch_frames)
      @mutex = Sync::Mutex.new
      @not_empty = Sync::ConditionVariable.new(@mutex)
      @not_full = Sync::ConditionVariable.new(@mutex)
      @encoder = FrameCodec::Encoder.new
      @batch = IO::Memory.new(@max_batch_bytes)
    end

    def initialize(
      io : IO,
      queue_capacity : Int32 = DEFAULT_QUEUE_CAPACITY,
      max_batch_frames : Int32 = DEFAULT_MAX_BATCH_FRAMES,
      max_batch_bytes : Int32 = DEFAULT_MAX_BATCH_BYTES,
      &on_error : Exception -> Nil
    )
      initialize(io, queue_capacity, max_batch_frames, max_batch_bytes, on_error)
    end

    # Starts the single writer fiber. Envelopes may be enqueued before start.
    def start : Bool
      can_start = @mutex.synchronize do
        if @started || @stopping
          false
        else
          @started = true
          true
        end
      end
      return false unless can_start

      @writer_fiber = spawn { writer_loop }
      true
    end

    # Enqueues in FIFO order. When the bounded queue is full, the producer is
    # backpressured until the writer drains capacity or the writer closes.
    def enqueue(envelope : WireEnvelope) : Bool
      @mutex.lock
      begin
        while @queue.size >= @queue_capacity && @accepting
          @not_full.wait
        end
        return false unless @accepting

        @queue << envelope
        @not_empty.signal
        true
      ensure
        @mutex.unlock
      end
    end

    # Stops accepting work, releases blocked producers, and discards envelopes
    # that cannot be delivered after connection shutdown.
    def close : Nil
      @mutex.synchronize { transition_to_stopping_locked }
    end

    private def writer_loop : Nil
      while take_ready
        write_drained
      end
    rescue ex : IO::Error
      stop_after_failure
      report_error(ex)
    ensure
      stop_after_failure
    end

    private def take_ready : Bool
      @mutex.lock
      begin
        while @queue.empty? && !@stopping
          @not_empty.wait
        end
        return false if @stopping

        @drain.clear
        Math.min(@queue.size, @max_batch_frames).times do
          @drain << @queue.shift
        end
        @not_full.broadcast
        true
      ensure
        @mutex.unlock
      end
    end

    private def write_drained : Nil
      @batch.clear
      @drain.each do |envelope|
        begin
          @encoder.with_frame(envelope) do |frame|
            flush_batch if !@batch.empty? && @batch.bytesize + frame.size > @max_batch_bytes
            @batch.write(frame)
          end
        rescue ex : Exception
          report_error(ex)
          next
        end

        flush_batch if @batch.bytesize >= @max_batch_bytes
      end
      flush_batch
    ensure
      @drain.clear
    end

    private def flush_batch : Nil
      return if @batch.empty?

      batch_size = @batch.bytesize
      @io.write(@batch.to_slice)
      @io.flush
      @batch = IO::Memory.new(@max_batch_bytes) if batch_size > FrameCodec::MAX_RETAINED_BUFFER_CAPACITY
      @batch.clear
    end

    private def stop_after_failure : Nil
      @mutex.synchronize { transition_to_stopping_locked }
    end

    private def transition_to_stopping_locked : Nil
      return if @stopping

      @accepting = false
      @stopping = true
      @queue.clear
      @not_empty.broadcast
      @not_full.broadcast
    end

    private def report_error(error : Exception) : Nil
      if handler = @on_error
        handler.call(error)
      else
        Log.error(exception: error) { "Outbound writer failed" }
      end
    rescue callback_error : Exception
      Log.error(exception: callback_error) { "Outbound writer error callback failed" }
    end
  end
end
