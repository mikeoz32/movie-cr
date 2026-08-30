require "../../spec_helper"
require "../../../src/movie/remote/outbound_writer"

class Movie::Remote::OutboundWriter
  def retained_envelope_count_for_test : Int32
    @drain.size
  end
end

class BatchedWriteSpy < IO
  @writes = [] of Bytes
  @mutex = Sync::Mutex.new
  @write_signal = Channel(Nil).new(16)

  def read(slice : Bytes) : Int32
    0
  end

  def write(slice : Bytes) : Nil
    @mutex.synchronize { @writes << slice.dup }
    @write_signal.send(nil)
  end

  def wait_for_write : Nil
    select
    when @write_signal.receive
    when timeout(1.second)
      raise "timed out waiting for batched write"
    end
  end

  def wait_for_writes(count : Int32) : Nil
    count.times { wait_for_write }
  end

  def writes : Array(Bytes)
    @mutex.synchronize { @writes.dup }
  end
end

describe Movie::Remote::OutboundWriter do
  it "accepts without synchronous IO and drains ready envelopes as one FIFO batch" do
    io = BatchedWriteSpy.new
    writer = Movie::Remote::OutboundWriter.new(
      io,
      queue_capacity: 4,
      max_batch_frames: 4,
      max_batch_bytes: 64 * 1024
    )

    writer.enqueue(Movie::Remote::WireEnvelope.heartbeat).should be_true
    writer.enqueue(
      Movie::Remote::WireEnvelope.user_message(
        "movie://system/user/target",
        "BatchMessage",
        JSON.parse(%({"value":1}))
      )
    ).should be_true
    io.writes.should be_empty

    writer.start.should be_true
    io.wait_for_write
    writer.close

    writes = io.writes
    writes.size.should eq(1)
    frames = IO::Memory.new(writes.first, false)
    decoder = Movie::Remote::FrameCodec::Decoder.new
    decoder.decode(frames).not_nil!.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
    decoder.decode(frames).not_nil!.message_type.should eq("BatchMessage")
  ensure
    writer.try &.close
  end

  it "backpressures producers when its bounded queue is full" do
    writer = Movie::Remote::OutboundWriter.new(
      BatchedWriteSpy.new,
      queue_capacity: 1,
      max_batch_frames: 1,
      max_batch_bytes: 64 * 1024
    )
    writer.enqueue(Movie::Remote::WireEnvelope.heartbeat).should be_true
    attempted = Channel(Nil).new
    result = Channel(Bool).new

    spawn do
      attempted.send(nil)
      result.send(writer.enqueue(Movie::Remote::WireEnvelope.heartbeat))
    end
    attempted.receive

    select
    when result.receive
      raise "enqueue returned while the bounded queue was full"
    when timeout(10.milliseconds)
    end

    writer.start.should be_true
    select
    when accepted = result.receive
      accepted.should be_true
    when timeout(1.second)
      raise "producer remained blocked after the writer drained queue capacity"
    end
  ensure
    writer.try &.close
  end

  it "releases a backpressured producer with rejection on close" do
    writer = Movie::Remote::OutboundWriter.new(
      BatchedWriteSpy.new,
      queue_capacity: 1,
      max_batch_frames: 1,
      max_batch_bytes: 64 * 1024
    )
    writer.enqueue(Movie::Remote::WireEnvelope.heartbeat).should be_true
    attempted = Channel(Nil).new
    result = Channel(Bool).new
    spawn do
      attempted.send(nil)
      result.send(writer.enqueue(Movie::Remote::WireEnvelope.heartbeat))
    end
    attempted.receive

    writer.close
    select
    when accepted = result.receive
      accepted.should be_false
    when timeout(1.second)
      raise "closing the writer did not release a backpressured producer"
    end
  ensure
    writer.try &.close
  end

  it "flushes before a ready frame would exceed the batch byte target" do
    io = BatchedWriteSpy.new
    payload = JSON.parse(%({"value":"#{"x" * 400}"}))
    envelope = Movie::Remote::WireEnvelope.user_message(
      "movie://system/user/target",
      "SizedBatchMessage",
      payload
    )
    frame_size = Movie::Remote::FrameCodec.encode_to_bytes(envelope).size
    byte_target = frame_size + 1
    writer = Movie::Remote::OutboundWriter.new(
      io,
      queue_capacity: 4,
      max_batch_frames: 4,
      max_batch_bytes: byte_target
    )
    writer.enqueue(envelope).should be_true
    writer.enqueue(envelope).should be_true

    writer.start.should be_true
    io.wait_for_writes(2)

    io.writes.size.should eq(2)
    io.writes.each { |write| write.size.should be <= byte_target }
  ensure
    writer.try &.close
  end

  it "releases envelopes after their batch has been written" do
    io = BatchedWriteSpy.new
    writer = Movie::Remote::OutboundWriter.new(io)
    writer.enqueue(Movie::Remote::WireEnvelope.heartbeat).should be_true

    writer.start.should be_true
    io.wait_for_write
    3.times do
      break if writer.retained_envelope_count_for_test == 0
      Fiber.yield
    end

    writer.retained_envelope_count_for_test.should eq(0)
  ensure
    writer.try &.close
  end
end
