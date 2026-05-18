require "./wire_envelope"

module Movie::Remote
  # FrameCodec handles length-prefixed framing for wire protocol messages.
  # Frame format: [4-byte length (big-endian)] [JSON payload]
  module FrameCodec
    # Maximum frame size (16 MB)
    MAX_FRAME_SIZE = 16 * 1024 * 1024

    # Encodes a WireEnvelope to the IO with length-prefixed framing.
    def self.encode(envelope : WireEnvelope, io : IO) : Nil
      json = envelope.to_json
      length = json.bytesize

      if length > MAX_FRAME_SIZE
        raise FrameTooLargeError.new("Frame size #{length} exceeds maximum #{MAX_FRAME_SIZE}")
      end

      # Write 4-byte length in big-endian
      io.write_bytes(length.to_u32, IO::ByteFormat::BigEndian)
      io << json
      io.flush
    end

    # Decodes a WireEnvelope from the IO.
    # Returns nil on EOF or if the connection is closed.
    # Raises on malformed frames.
    def self.decode(io : IO) : WireEnvelope?
      # Read 4-byte length
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

      # Read payload
      buffer = Bytes.new(length)
      bytes_read = io.read_fully(buffer)

      json_str = String.new(buffer)
      WireEnvelope.from_json(json_str)
    rescue IO::EOFError
      nil
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
