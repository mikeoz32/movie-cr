require "./wire_envelope"
require "./reusable_json_pull_parser"

module Movie::Remote
  # FrameCodec handles length-prefixed framing for wire protocol messages.
  # Frame format: [4-byte length (big-endian)] [JSON payload]
  module FrameCodec
    # Maximum frame size (16 MB)
    MAX_FRAME_SIZE               = 16 * 1024 * 1024
    INITIAL_BUFFER_CAPACITY      = 1024
    MAX_RETAINED_BUFFER_CAPACITY = 1024 * 1024

    # Stateful encoder with a reusable JSON buffer. Callers must serialize
    # access; Connection and InboundConnection already do so with write locks.
    class Encoder
      def initialize(initial_capacity : Int32 = INITIAL_BUFFER_CAPACITY)
        @buffer = IO::Memory.new(initial_capacity)
        @json = JSON::Builder.new(@buffer)
      end

      def encode(envelope : WireEnvelope, io : IO) : Nil
        @buffer.clear
        begin
          @json.document { envelope.to_json(@json) }
        rescue ex
          reset_buffer
          raise ex
        end
        length = @buffer.bytesize

        if length > MAX_FRAME_SIZE
          reset_buffer
          raise FrameTooLargeError.new("Frame size #{length} exceeds maximum #{MAX_FRAME_SIZE}")
        end

        io.write_bytes(length.to_u32, IO::ByteFormat::BigEndian)
        io.write(@buffer.to_slice)
        io.flush
        reset_buffer if length > MAX_RETAINED_BUFFER_CAPACITY
      end

      private def reset_buffer : Nil
        @buffer = IO::Memory.new(INITIAL_BUFFER_CAPACITY)
        @json = JSON::Builder.new(@buffer)
      end
    end

    private class BufferReader < IO
      def initialize
        @buffer = Bytes.empty
        @position = 0
      end

      def reset(@buffer : Bytes) : self
        @position = 0
        self
      end

      def read(slice : Bytes) : Int32
        count = Math.min(slice.size, @buffer.size - @position)
        return 0 if count <= 0

        slice[0, count].copy_from(@buffer[@position, count])
        @position += count
        count
      end

      def write(slice : Bytes) : Nil
        raise IO::Error.new("Frame decode buffer is read-only")
      end
    end

    # Stateful decoder that retains the largest observed frame buffer.
    class Decoder
      def initialize(@payload_decoder : JsonPayloadDecoder? = nil)
        @buffer = Bytes.new(INITIAL_BUFFER_CAPACITY)
        @reader = BufferReader.new
        @json = ReusableJsonPullParser.new(@reader.reset(Bytes.empty))
      end

      def decode(io : IO) : WireEnvelope?
        length = begin
          io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
        rescue IO::EOFError
          return nil
        end

        if length > MAX_FRAME_SIZE
          raise FrameTooLargeError.new("Frame size #{length} exceeds maximum #{MAX_FRAME_SIZE}")
        end

        if length == 0
          raise MalformedFrameError.new("Frame length cannot be zero")
        end

        frame_size = length.to_i
        frame = if frame_size > MAX_RETAINED_BUFFER_CAPACITY
                  Bytes.new(frame_size)
                else
                  if frame_size > @buffer.size
                    capacity = { {@buffer.size * 2, frame_size}.max, MAX_RETAINED_BUFFER_CAPACITY }.min
                    @buffer = Bytes.new(capacity)
                  end
                  @buffer[0, frame_size]
                end
        io.read_fully(frame)
        begin
          @json.reset(@reader.reset(frame))
          WireEnvelope.new(@json, @payload_decoder)
        ensure
          @json.release_excess(frame_size > MAX_RETAINED_BUFFER_CAPACITY)
        end
      rescue IO::EOFError
        nil
      end
    end

    # Encodes a WireEnvelope to the IO with length-prefixed framing.
    def self.encode(envelope : WireEnvelope, io : IO) : Nil
      Encoder.new.encode(envelope, io)
    end

    # Decodes a WireEnvelope from the IO.
    # Returns nil on EOF or if the connection is closed.
    # Raises on malformed frames.
    def self.decode(io : IO) : WireEnvelope?
      Decoder.new.decode(io)
    end

    # Encodes an envelope to bytes (useful for testing).
    def self.encode_to_bytes(envelope : WireEnvelope) : Bytes
      io = IO::Memory.new
      encode(envelope, io)
      io.to_slice
    end

    # Decodes an envelope from bytes (useful for testing).
    def self.decode_from_bytes(bytes : Bytes) : WireEnvelope?
      io = IO::Memory.new(bytes)
      decode(io)
    end
  end

  # Raised when a frame exceeds the maximum allowed size.
  class FrameTooLargeError < Exception
  end

  # Raised when a frame is malformed.
  class MalformedFrameError < Exception
  end
end
