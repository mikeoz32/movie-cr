require "uuid"
require "./wire_envelope"

module Movie::Remote
  class ControlDeduplicationCapacityError < Exception
  end

  enum ControlObservation
    New
    Duplicate
    Gap
  end

  private class ControlStreamState
    property sequence = 0_i64
    getter mutex = Mutex.new
  end

  # Tracks one contiguous cursor per node/stream. The global mutex protects
  # only stream lookup; delivery is serialized by the affected stream so a
  # slow actor route does not block unrelated associations.
  class ControlDeduplicator
    @streams = {} of String => ControlStreamState
    @mutex = Mutex.new

    def initialize(@max_streams : Int32 = 8192)
      raise ArgumentError.new("maximum control streams must be positive") unless @max_streams > 0
    end

    def observe(node_uid : String, stream : String, sequence : Int64) : ControlObservation
      state = state_for(node_uid, stream)
      state.mutex.synchronize do
        observation = classify(state.sequence, sequence)
        state.sequence = sequence if observation.new?
        observation
      end
    end

    def deliver(node_uid : String, stream : String, sequence : Int64, &) : ControlObservation
      state = state_for(node_uid, stream)
      state.mutex.synchronize do
        observation = classify(state.sequence, sequence)
        if observation.new?
          yield
          state.sequence = sequence
        end
        observation
      end
    end

    def forget(node_uid : String) : Nil
      prefix = "#{node_uid}\0"
      @mutex.synchronize { @streams.reject! { |key, _| key.starts_with?(prefix) } }
    end

    private def state_for(node_uid : String, stream : String) : ControlStreamState
      key = "#{node_uid}\0#{stream}"
      @mutex.synchronize do
        if state = @streams[key]?
          return state
        end
        if @streams.size >= @max_streams
          raise ControlDeduplicationCapacityError.new(
            "control deduplication capacity #{@max_streams} is exhausted"
          )
        end
        @streams[key] = ControlStreamState.new
      end
    end

    private def classify(previous : Int64, sequence : Int64) : ControlObservation
      return ControlObservation::Duplicate if sequence <= previous
      return ControlObservation::Gap if sequence != previous + 1

      ControlObservation::New
    end
  end

  # Connection-owned bounded buffer for system envelopes awaiting ACK.
  class PendingControlBuffer
    @stream = UUID.random.to_s
    @sequence = 0_i64
    @pending = {} of Int64 => WireEnvelope
    @mutex = Mutex.new

    def initialize(@capacity : Int32)
      raise ArgumentError.new("control buffer capacity must be positive") unless @capacity > 0
    end

    def offer(envelope : WireEnvelope) : WireEnvelope?
      @mutex.synchronize do
        return nil if @pending.size >= @capacity
        @sequence += 1
        envelope.control_stream = @stream
        envelope.control_sequence = @sequence
        @pending[@sequence] = envelope
        envelope
      end
    end

    def acknowledge(stream : String?, sequence : Int64?) : Nil
      return unless stream == @stream && sequence
      @mutex.synchronize { @pending.delete(sequence) }
    end

    def pending : Array(WireEnvelope)
      @mutex.synchronize { @pending.keys.sort.map { |sequence| @pending[sequence] } }
    end

    def rebase : Nil
      @mutex.synchronize do
        envelopes = @pending.keys.sort.map { |sequence| @pending[sequence] }
        @pending.clear
        @stream = UUID.random.to_s
        @sequence = 0_i64
        envelopes.each do |envelope|
          @sequence += 1
          envelope.control_stream = @stream
          envelope.control_sequence = @sequence
          @pending[@sequence] = envelope
        end
      end
    end

    def size : Int32
      @mutex.synchronize { @pending.size }
    end

    def clear : Nil
      @mutex.synchronize { @pending.clear }
    end
  end
end
