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

**Status:** Completed (2026-08-30).

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

**Status:** In progress.

**Outcome**

- Move connection socket writes off caller fibers onto one connection-owned writer.
- Bound queued bytes/messages and define deterministic saturation behavior.
- Drain ready frames up to a frame-count or byte limit into one socket write.
- Preserve FIFO order within each stripe and close/fail queued work on disconnect.

## Task 13.3: Validate inbound transport buffering

**Status:** Pending.

**Outcome**

- Verify whether the runtime already reads bounded socket chunks into connection-local storage.
- Avoid layering a redundant buffer over existing multi-frame read-ahead.
- Preserve partial-header, partial-payload, maximum-frame, malformed-frame, and EOF behavior.

## Task 13.4: Select the next measured hot path

**Status:** Pending.

**Outcome**

- Decompose post-decode path resolution, registry lookup, mailbox enqueue, and actor dispatch.
- Record whether numeric target/type IDs, mailbox batching, or binary protocol v2 is the next dominant task.

## Task 13.5: Remove canonical route parsing from delivery

**Status:** Pending.

**Outcome**

- Resolve the exact registered actor path without rebuilding `ActorPath` state.
- Preserve normalized local/remote aliases as a compatibility fallback.
- Keep registration, rebinding, unregister, and clear indexes consistent.

## Task 13.6: Reuse internal mailbox storage

**Status:** Pending.

**Outcome**

- Remove per-message linked-node and reference-envelope allocations from normal mailbox delivery.
- Preserve the public `Envelope`, `Queue`, and `QueueNode` APIs.
- Retain system-message priority and the existing dispatch throughput limit.

## Completion checklist

- [ ] Failing test written first.
- [ ] Failing test observed red.
- [ ] Minimal implementation written.
- [ ] Targeted verification green.
- [ ] Broader verification green.
- [ ] Formatting check green.
- [ ] Docs/examples updated if needed.
- [ ] Review requested.
- [ ] Review feedback addressed.
