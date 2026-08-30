require "../../spec_helper"
require "../../../src/movie/remote/wire_envelope"
require "../../../src/movie/remote/frame_codec"
require "../../../src/movie/remote/message_registry"

record FrameDirectWriteMessage, value : String do
  include JSON::Serializable

  def to_json : String
    raise "frame payload must use JSON::Builder"
  end
end

record PullParserOnlyFrameMessage, value : String do
  include JSON::Serializable

  def self.from_json(source : String | IO)
    raise "materialized payload parse must not be used"
  end
end

struct ThrowingFramePayload
  def to_json(json : JSON::Builder) : Nil
    json.object do
      json.field("started", true)
      raise "serializer failed"
    end
  end
end

describe Movie::Remote::FrameCodec do
  describe ".encode and .decode" do
    it "encodes and decodes a user message" do
      payload = JSON::Any.new({"data" => JSON::Any.new("hello")})
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor1",
        message_type: "TestMessage",
        payload: payload,
        sender_path: "movie://sys/user/sender"
      )

      bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
      decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)

      decoded.should_not be_nil
      decoded = decoded.not_nil!
      decoded.kind.should eq(envelope.kind)
      decoded.target_path.should eq(envelope.target_path)
      decoded.message_type.should eq(envelope.message_type)
      decoded.sender_path.should eq(envelope.sender_path)
      decoded.payload["data"].as_s.should eq("hello")
    end

    it "encodes and decodes a heartbeat" do
      envelope = Movie::Remote::WireEnvelope.heartbeat

      bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)
      decoded = Movie::Remote::FrameCodec.decode_from_bytes(bytes)

      decoded.should_not be_nil
      decoded.not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    end

    it "handles multiple frames in sequence" do
      io = IO::Memory.new

      env1 = Movie::Remote::WireEnvelope.heartbeat
      env2 = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/a",
        message_type: "M",
        payload: JSON::Any.new({} of String => JSON::Any)
      )

      Movie::Remote::FrameCodec.encode(env1, io)
      Movie::Remote::FrameCodec.encode(env2, io)

      io.rewind

      decoded1 = Movie::Remote::FrameCodec.decode(io)
      decoded2 = Movie::Remote::FrameCodec.decode(io)

      decoded1.should_not be_nil
      decoded1.not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)

      decoded2.should_not be_nil
      decoded2.not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::USER_MESSAGE)
    end

    it "writes a serializable payload directly through the frame encoder" do
      tag, payload = Movie::Remote::MessageRegistry.prepare(FrameDirectWriteMessage.new("direct"))
      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/direct",
        message_type: tag,
        payload: payload
      )

      decoded = Movie::Remote::FrameCodec.decode_from_bytes(
        Movie::Remote::FrameCodec.encode_to_bytes(envelope)
      ).not_nil!

      decoded.payload_data.should be_a(Movie::Remote::RawJsonPayload)
      decoded.payload["value"].as_s.should eq("direct")
    end

    it "reuses stateful frame encoders and decoders across messages" do
      encoder = Movie::Remote::FrameCodec::Encoder.new
      decoder = Movie::Remote::FrameCodec::Decoder.new
      io = IO::Memory.new

      encoder.encode(Movie::Remote::WireEnvelope.heartbeat, io)
      encoder.encode(Movie::Remote::WireEnvelope.heartbeat, io)
      io.rewind

      decoder.decode(io).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
      decoder.decode(io).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    end

    it "recovers a reusable encoder after payload serialization fails" do
      encoder = Movie::Remote::FrameCodec::Encoder.new
      io = IO::Memory.new
      failing = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/failing",
        message_type: "ThrowingFramePayload",
        payload: ThrowingFramePayload.new
      )

      expect_raises(Exception, "serializer failed") { encoder.encode(failing, io) }

      io.clear
      encoder.encode(Movie::Remote::WireEnvelope.heartbeat, io)
      io.rewind
      Movie::Remote::FrameCodec.decode(io).not_nil!.kind.should eq(
        Movie::Remote::WireEnvelope::Kind::HEARTBEAT
      )
    end

    it "decodes registered payloads directly from the envelope pull parser" do
      Movie::Remote::MessageRegistry.clear
      Movie::Remote::MessageRegistry.register(PullParserOnlyFrameMessage)
      tag, payload = Movie::Remote::MessageRegistry.prepare(PullParserOnlyFrameMessage.new("direct"))
      frame = Movie::Remote::FrameCodec.encode_to_bytes(
        Movie::Remote::WireEnvelope.user_message("movie://sys/user/direct", tag, payload)
      )
      decoder = Movie::Remote::FrameCodec::Decoder.new(Movie::Remote::MessageRegistry.payload_decoder)

      envelope = decoder.decode(IO::Memory.new(frame, writable: false)).not_nil!
      restored = Movie::Remote::MessageRegistry.deserialize(tag, envelope.payload_data)

      restored.unwrap(PullParserOnlyFrameMessage).value.should eq("direct")
    ensure
      Movie::Remote::MessageRegistry.clear
    end

    it "keeps unknown payload tags on the raw compatibility path" do
      payload = JSON.parse(%({"value":"raw"}))
      frame = Movie::Remote::FrameCodec.encode_to_bytes(
        Movie::Remote::WireEnvelope.user_message("movie://sys/user/unknown", "UnknownPayload", payload)
      )
      decoder = Movie::Remote::FrameCodec::Decoder.new(Movie::Remote::MessageRegistry.payload_decoder)

      envelope = decoder.decode(IO::Memory.new(frame, writable: false)).not_nil!

      envelope.payload_data.should be_a(Movie::Remote::RawJsonPayload)
      envelope.payload["value"].as_s.should eq("raw")
    end

    it "recovers the direct payload decoder after a malformed registered message" do
      Movie::Remote::MessageRegistry.clear
      Movie::Remote::MessageRegistry.register(PullParserOnlyFrameMessage)
      io = IO::Memory.new
      Movie::Remote::FrameCodec.encode(
        Movie::Remote::WireEnvelope.user_message(
          "movie://sys/user/invalid",
          "PullParserOnlyFrameMessage",
          JSON.parse(%({"unexpected":true}))
        ),
        io
      )
      Movie::Remote::FrameCodec.encode(Movie::Remote::WireEnvelope.heartbeat, io)
      io.rewind
      decoder = Movie::Remote::FrameCodec::Decoder.new(Movie::Remote::MessageRegistry.payload_decoder)

      expect_raises(Movie::Remote::MalformedMessagePayloadError) { decoder.decode(io) }
      decoder.decode(io).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    ensure
      Movie::Remote::MessageRegistry.clear
    end
  end

  describe ".decode" do
    it "returns nil on empty input" do
      io = IO::Memory.new
      decoded = Movie::Remote::FrameCodec.decode(io)
      decoded.should be_nil
    end

    it "returns nil on EOF" do
      io = IO::Memory.new
      io.rewind
      decoded = Movie::Remote::FrameCodec.decode(io)
      decoded.should be_nil
    end
  end

  describe "frame format" do
    it "uses 4-byte big-endian length prefix" do
      payload = JSON::Any.new({} of String => JSON::Any)
      envelope = Movie::Remote::WireEnvelope.heartbeat

      bytes = Movie::Remote::FrameCodec.encode_to_bytes(envelope)

      # First 4 bytes are the length
      length = IO::ByteFormat::BigEndian.decode(UInt32, bytes[0, 4])

      # Rest should be the JSON
      json_bytes = bytes[4..]
      json_bytes.size.should eq(length)

      # JSON should be valid
      parsed = Movie::Remote::WireEnvelope.from_json(String.new(json_bytes))
      parsed.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    end
  end

  describe "error handling" do
    it "raises on zero-length frame" do
      io = IO::Memory.new
      io.write_bytes(0_u32, IO::ByteFormat::BigEndian)
      io.rewind

      expect_raises(Movie::Remote::MalformedFrameError) do
        Movie::Remote::FrameCodec.decode(io)
      end
    end
  end
end
