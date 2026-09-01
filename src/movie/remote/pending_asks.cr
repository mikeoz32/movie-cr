require "./wire_envelope"

module Movie::Remote
  class PendingAskRegistry
    @channels = {} of String => Channel(WireEnvelope)
    @mutex = Mutex.new

    def register(correlation_id : String) : Channel(WireEnvelope)
      channel = Channel(WireEnvelope).new(1)
      @mutex.synchronize { @channels[correlation_id] = channel }
      channel
    end

    def remove(correlation_id : String) : Channel(WireEnvelope)?
      @mutex.synchronize { @channels.delete(correlation_id) }
    end

    def fail_all : Nil
      @mutex.synchronize do
        @channels.each_value(&.close)
        @channels.clear
      end
    end

    def size : Int32
      @mutex.synchronize { @channels.size }
    end
  end
end
