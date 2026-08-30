require "../spec_helper"
require "../../src/movie"

describe Movie::MailboxQueue do
  it "reuses mailbox queue storage without allocating a node and envelope per message" do
    queue = Movie::MailboxQueue(Movie::MailboxEnvelope(Int32)).new
    queue.enqueue(Movie::MailboxEnvelope(Int32).new(0, nil))
    queue.dequeue
    GC.collect
    before = GC.stats.total_bytes
    checksum = 0_i64

    10_000.times do |value|
      queue.enqueue(Movie::MailboxEnvelope(Int32).new(value, nil))
      checksum &+= queue.dequeue.not_nil!.message
    end

    allocated = GC.stats.total_bytes - before
    checksum.should eq(49_995_000_i64)
    allocated.should be <= 4_096_u64
  end
end
