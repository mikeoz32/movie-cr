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
