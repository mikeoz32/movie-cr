require "./frame_codec"

module Movie::Remote
  # Connection-owned read buffer that makes already-received complete frames
  # visible to the inbound batching loop. The wrapped socket must have its own
  # read buffering disabled so each connection retains only one input buffer.
  class InboundFrameReader < IO
    DEFAULT_BUFFER_SIZE = 32 * 1024

    def initialize(@source : IO, buffer_size : Int32 = DEFAULT_BUFFER_SIZE)
      raise ArgumentError.new("buffer_size must be positive") if buffer_size <= 0

      @buffer = Bytes.new(buffer_size)
      @position = 0
      @size = 0
    end

    def read(slice : Bytes) : Int32
      return 0 if slice.empty?
      fill if buffered_bytes == 0
      return 0 if buffered_bytes == 0

      count = {slice.size, buffered_bytes}.min
      slice[0, count].copy_from(@buffer[@position, count])
      @position += count
      count
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("Inbound frame reader is read-only")
    end

    def buffered_bytes : Int32
      @size - @position
    end

    # Returns true only when the next frame can be decoded without another
    # socket read. Invalid length prefixes are considered ready so the decoder
    # can reject them immediately.
    def complete_frame_buffered? : Bool
      return false if buffered_bytes < FrameCodec::FRAME_PREFIX_SIZE

      prefix = @buffer[@position, FrameCodec::FRAME_PREFIX_SIZE]
      length = IO::ByteFormat::BigEndian.decode(UInt32, prefix)
      return true if length == 0 || length > FrameCodec::MAX_FRAME_SIZE

      buffered_bytes.to_u64 >= FrameCodec::FRAME_PREFIX_SIZE.to_u64 + length.to_u64
    end

    private def fill : Nil
      @position = 0
      @size = @source.read(@buffer)
    end
  end
end
