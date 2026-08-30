require "../../spec_helper"
require "../../../src/movie/remote/server"

class InboundFrameChunkSource < IO
  getter reads = 0

  def initialize(@bytes : Bytes, @chunk_size : Int32 = Int32::MAX)
    @position = 0
  end

  def read(slice : Bytes) : Int32
    @reads += 1
    count = {@chunk_size, slice.size, @bytes.size - @position}.min
    return 0 if count <= 0

    slice[0, count].copy_from(@bytes[@position, count])
    @position += count
    count
  end

  def write(slice : Bytes) : Nil
    raise IO::Error.new("read-only")
  end
end

describe Movie::Remote::InboundFrameReader do
  it "exposes complete frames already retained by the current read" do
    frames = IO::Memory.new
    encoder = Movie::Remote::FrameCodec::Encoder.new
    3.times { encoder.encode(Movie::Remote::WireEnvelope.heartbeat, frames) }
    source = InboundFrameChunkSource.new(frames.to_slice)
    reader = Movie::Remote::InboundFrameReader.new(source)
    decoder = Movie::Remote::FrameCodec::Decoder.new

    decoder.decode(reader).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    reader.complete_frame_buffered?.should be_true
    decoder.decode(reader).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    reader.complete_frame_buffered?.should be_true
    decoder.decode(reader).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)

    reader.complete_frame_buffered?.should be_false
    source.reads.should eq(1)
  end

  it "does not report a fragmented frame as ready" do
    frame = Movie::Remote::FrameCodec.encode_to_bytes(Movie::Remote::WireEnvelope.heartbeat)
    source = InboundFrameChunkSource.new(frame, chunk_size: 2)
    reader = Movie::Remote::InboundFrameReader.new(source, buffer_size: 8)

    probe = Bytes.new(2)
    reader.read_fully(probe)

    reader.complete_frame_buffered?.should be_false
  end
end

describe Movie::Remote::InboundFrameBatchDecoder do
  it "drains complete ready frames in FIFO order without another source read" do
    frames = IO::Memory.new
    encoder = Movie::Remote::FrameCodec::Encoder.new
    3.times do |index|
      encoder.encode(
        Movie::Remote::WireEnvelope.user_message(
          "movie://sys/user/target-#{index}",
          "Unknown",
          JSON::Any.new({} of String => JSON::Any)
        ),
        frames
      )
    end
    source = InboundFrameChunkSource.new(frames.to_slice)
    decoder = Movie::Remote::InboundFrameBatchDecoder.new(source)

    batch = decoder.next_batch.not_nil!

    batch.map(&.target_path).should eq((0...3).map { |index| "movie://sys/user/target-#{index}" })
    source.reads.should eq(1)
  end

  it "returns one low-traffic frame without reading again to fill the batch" do
    frame = Movie::Remote::FrameCodec.encode_to_bytes(Movie::Remote::WireEnvelope.heartbeat)
    source = InboundFrameChunkSource.new(frame)
    decoder = Movie::Remote::InboundFrameBatchDecoder.new(source)

    decoder.next_batch.not_nil!.size.should eq(1)

    source.reads.should eq(1)
  end

  it "bounds a ready batch and reuses the buffered remainder" do
    frames = IO::Memory.new
    encoder = Movie::Remote::FrameCodec::Encoder.new
    150.times { encoder.encode(Movie::Remote::WireEnvelope.heartbeat, frames) }
    source = InboundFrameChunkSource.new(frames.to_slice)
    decoder = Movie::Remote::InboundFrameBatchDecoder.new(source, batch_size: 128)

    decoder.next_batch.not_nil!.size.should eq(128)
    decoder.next_batch.not_nil!.size.should eq(22)

    source.reads.should eq(1)
  end

  it "returns valid ready frames before surfacing a later protocol error" do
    frames = IO::Memory.new
    Movie::Remote::FrameCodec::Encoder.new.encode(Movie::Remote::WireEnvelope.heartbeat, frames)
    frames.write_bytes(0_u32, IO::ByteFormat::BigEndian)
    source = InboundFrameChunkSource.new(frames.to_slice)
    decoder = Movie::Remote::InboundFrameBatchDecoder.new(source)

    decoder.next_batch.not_nil!.size.should eq(1)
    expect_raises(Movie::Remote::MalformedFrameError) { decoder.next_batch }

    source.reads.should eq(1)
  end
end
