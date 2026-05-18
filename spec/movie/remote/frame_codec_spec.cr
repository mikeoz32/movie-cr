require "../../spec_helper"
require "../../../src/movie/remote/wire_envelope"
require "../../../src/movie/remote/frame_codec"

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
