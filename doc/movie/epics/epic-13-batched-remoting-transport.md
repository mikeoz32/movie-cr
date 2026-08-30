# Epic 13: Batched Remoting Transport

**Goal:** Move ordered remote actor delivery from per-message synchronous socket IO to a bounded, batched transport and identify the next receiver-side bottleneck before cluster work begins.

**Depends on:** Epic 12.

**Status:** In progress.

**Done when:**

- the fixed million-message comparator measures actor-processed completion rather than socket enqueue,
- a frame is emitted with one socket write without changing the length-prefixed wire format,
- each connection owns one bounded FIFO outbound queue and writer fiber,
- queued frames are drained into bounded batches without adding an artificial low-traffic delay,
- inbound reads reuse a bounded buffer and drain multiple complete frames per socket read,
- per-actor ordering, ask responses, disconnect failure, and shutdown remain correct,
- repeated before/after benchmarks report throughput and allocations separately,
- the next measured receiver-side or binary-codec task is recorded,
- targeted and full verification plus mandatory review are complete.

## Fixed comparison lanes

All performance work keeps receiver-side completion barriers. The standard comparison lane uses 10,000 messages, a 64-byte payload, 8 producers, 8 actors, 4 stripes, 2 warmups, and 5 measured runs. The saturation comparator uses 1,000,000 zero-payload ordered tells, 1 producer, 1 actor, 1 stripe, no warmup, and 1 measured run.

At the Epic start on commit `22205aa`, the repeated standard two-process tell medians were 40,458-42,164 msg/s. The saturation comparator measured 1,948,648 local msg/s and 53,255 two-process remote msg/s, so remote delivery retained only 2.7% of the equivalent local throughput. The connection-owned frame decoder measured 264,715-285,835 frames/s, leaving a large transport and receiver-delivery gap after the JSON parser.

## Task 13.1: Emit one complete frame per socket write

**Status:** Implementation complete; re-review pending.

**Outcome**

- Reserve the four-byte length prefix in the encoder's reusable buffer.
- Backpatch the encoded payload length and write the complete frame once.
- Preserve the existing `[length][JSON]` protocol and oversized-buffer release.

**Verification**

- Observe a failing IO-spy regression that reports two writes for one frame.
- Verify one write, exact bytes, encoder recovery, and sequential decode.
- Repeat the fixed performance lanes before starting Task 13.2.

### Result

The one-write regression was observed failing with two writes and passed after the encoder reserved and backpatched its four-byte prefix. The standard two-process tell medians were 40,899 and 41,793 msg/s versus the 40,458-42,164 baseline, with unchanged allocations. Repeated saturation results were 49,590 and 50,926 msg/s versus the initial 53,255 msg/s. Removing one socket write per frame did not improve actor-processed throughput, so syscall count alone is not the current dominant boundary.

## Task 13.2: Add a bounded FIFO writer and drain batching

**Status:** Implementation complete; re-review pending.

**Outcome**

- Move connection socket writes off caller fibers onto one connection-owned writer.
- Bound queued bytes/messages and define deterministic saturation behavior.
- Drain ready frames up to a frame-count or byte limit into one socket write.
- Preserve FIFO order within each stripe and close/fail queued work on disconnect.

### Result

Each outbound and inbound TCP connection now owns one writer fiber, a 4,096-envelope FIFO, a 128-frame drain limit, and a strict 64 KiB write target except when one frame is larger by itself. Producers perform no socket IO while capacity is available and block deterministically at saturation rather than dropping accepted tells. Drained envelopes are released after every write cycle, and close wakes blocked producers with rejection. A diagnostic saturation run confirmed that 100,003 frames were emitted in 784 writes (127.6 frames/write, maximum 128), but the first actor-processed comparison fell to 37,305-38,299 msg/s and the first million-message run reached only 46,392 msg/s. Batching removed the syscall fan-out but exposed that it was not the dominant end-to-end boundary.

## Task 13.3: Validate inbound transport buffering

**Status:** Implementation complete; re-review pending.

**Outcome**

- Verify whether the runtime already reads bounded socket chunks into connection-local storage.
- Avoid layering a redundant buffer over existing multi-frame read-ahead.
- Preserve partial-header, partial-payload, maximum-frame, malformed-frame, and EOF behavior.

### Result

Crystal's `TCPSocket` includes `IO::Buffered`: writes are synchronous by default, but reads retain the default bounded 32 KiB read-ahead buffer. A four-byte frame-prefix read fills that buffer, payload and subsequent frame reads consume its remainder, and partial data stays buffered. A second Movie-owned inbound buffer would duplicate the existing mechanism and was therefore not added.

## Task 13.4: Select the next measured hot path

**Status:** Implementation complete; re-review pending.

**Outcome**

- Decompose post-decode path resolution, registry lookup, mailbox enqueue, and actor dispatch.
- Record whether numeric target/type IDs, mailbox batching, or binary protocol v2 is the next dominant task.

### Result

The expanded release stage report separates decoder work, route/registry lookup, local mailbox enqueue and completion, and the combined decoded-delivery enqueue and completion barrier. A final run measured about 217k msg/s for direct typed JSON decode, 203k after route/registry, 3.62M local mailbox enqueues and 3.49M local actor completions, but only 50.1k for both decoded-delivery enqueue and actor completion. The actor keeps up once work is enqueued; the active boundary is the allocation/locking interaction while decoded values are handed off one by one, not a backlog in actor execution.

## Task 13.5: Remove canonical route parsing from delivery

**Status:** Implementation complete; re-review pending.

**Outcome**

- Resolve the exact registered actor path without rebuilding `ActorPath` state.
- Preserve normalized local/remote aliases as a compatibility fallback.
- Keep registration, rebinding, unregister, and clear indexes consistent.

### Result

Canonical string lookup previously rebuilt an `ActorPath` and allocated about 624 B/message. `PathRegistry` now checks an exact registered-path cache before the normalized compatibility fallback, with registration, rebinding, both unregister forms, and clear maintaining all indexes. Repeated canonical benchmarks reach roughly 28-37M lookups/second at 0 B/op while alternate-address aliases retain the parsing fallback. This reduced standard server allocation from roughly 1.37 KiB to 0.69 KiB/message and raised repeated standard medians to 46,809-47,143 msg/s before the mailbox change.

## Task 13.6: Reuse internal mailbox storage

**Status:** Implementation complete; re-review pending.

**Outcome**

- Remove per-message linked-node and reference-envelope allocations from normal mailbox delivery.
- Preserve the public `Envelope`, `Queue`, and `QueueNode` APIs.
- Retain system-message priority and the existing dispatch throughput limit.

### Result

Normal mailbox storage moved from a heap `Envelope` plus linked `QueueNode` per message to internal value envelopes in a reusable deque; the existing public types retain their reference and linked-node semantics. The allocation regression fell from 641,856 bytes for 10,000 steady-state enqueue/dequeue pairs to at most 4 KiB. Final standard medians reached 48,540 and 50,636 msg/s, with 619-624 server B/message; repeated million-message saturation reached 59,835 and 61,473 msg/s with 470-471 server B/message. Local million-message delivery rose from the Epic baseline of 1.95M to 3.00-4.24M msg/s.

The next remoting epic should reduce or batch the decoded receiver-to-mailbox handoff and its allocation/GC interaction. After that gap closes, introduce a negotiated compact protocol with numeric route/type identifiers and macro-generated binary payload codecs; JSON envelope decode then becomes the roughly quarter-million-message ceiling. Cluster work should remain behind both tasks.

## Completion checklist

- [x] Failing test written first.
- [x] Failing test observed red.
- [x] Minimal implementation written.
- [x] Targeted verification green.
- [ ] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated if needed.
- [x] Review requested.
- [x] Review feedback addressed.
