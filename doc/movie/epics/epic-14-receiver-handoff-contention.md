# Epic 14: Receiver Handoff Contention

**Goal:** Raise remote actor throughput by measuring and removing lock and scheduling amplification between decoded frames and mailbox processing before changing the wire codec.

**Depends on:** Epic 13.

**Status:** In progress.

**Done when:**

- the inbound stage benchmark reports actor-dispatch frequency as well as enqueue and completion throughput,
- registry, path, actor-context, mailbox, and actor-dispatch synchronization are measured separately,
- synchronization is changed only when its measured cost is material to receiver throughput,
- decoded messages can reach a ready mailbox without creating one dispatcher activation per message under sustained input,
- mailbox FIFO order, system-message priority, throughput fairness, shutdown, and remoting delivery remain correct,
- repeated standard and million-message comparisons report throughput and allocations separately,
- targeted and full verification plus mandatory review are complete.

## Fixed comparison lanes

Keep the Epic 13 standard and saturation lanes unchanged. Use receiver-side actor completion barriers, repeat comparable runs on the same host, and report allocation stability separately from throughput.

## Task 14.1: Measure synchronization and scheduling amplification

**Status:** Completed (2026-08-30).

- Count mailbox dispatch activations for local flood delivery and decoded one-by-one delivery.
- Benchmark the global actor-dispatch marker, message registry lookup, exact path lookup, actor-context lookup, mailbox enqueue, and mailbox completion separately.
- Record the dominant lock or scheduling boundary before changing production code.

### Result

The release stage benchmark counted 1,000 mailbox activations for a 100,000-message local flood (100 messages/dispatch), but 60,217-60,767 activations for one-by-one decoded delivery (1.6-1.7 messages/dispatch). The decoded path was therefore creating tens of thousands of short-lived dispatcher fibers while the mailbox repeatedly crossed its scheduling locks.

## Task 14.2: Decide whether actor-dispatch tracking is material

**Status:** Completed (2026-08-30; no production change required).

- Measure actor-dispatch tracking both uncontended and across eight worker threads.
- Replace it only if its cost is material after dispatch amplification is removed.

### Result

The existing actor-dispatch global mutex/hash pair measured roughly 15-28 million enter/leave pairs per second and about 7-16 million pairs per second with eight contending workers. Exact path, actor-context, and message-registry locked lookups measured roughly 28-52 million operations per second in the same diagnostic. These rates are orders of magnitude above receiver throughput, and ready-frame batching reduced dispatch activation frequency itself, so replacing the shutdown marker with a Crystal-internal fiber-local mechanism would add compatibility risk without addressing the measured boundary. The lock implementation remains unchanged.

## Task 14.3: Coalesce decoded receiver handoff

**Status:** Completed (2026-08-30).

- Reuse receiver-owned storage to hand a ready run of decoded messages to mailboxes with one scheduling transition where possible.
- Preserve per-actor FIFO order and bounded memory without adding an artificial low-traffic delay.

### Result

Each inbound connection now owns the only 32 KiB socket-read buffer and a reusable 128-envelope array. The reader blocks for the first frame, then decodes only complete frames already retained in that buffer before delivering the ready batch. It never waits to enlarge a batch. Repeated synthetic decoded-delivery runs rose from about 40-63k to 136-184k msg/s while dispatch activations fell from roughly 35-61k to 1.9-2.0k. Partial frames, malformed payload recovery, FIFO delivery, and single-message latency retain their previous behavior.

## Task 14.4: Re-run end-to-end comparisons

**Status:** Completed (2026-08-30).

- Repeat the standard and saturation lanes at least twice.
- Decide from measured remaining cost whether the next task is compact binary framing, numeric route/type identifiers, or further mailbox work.

### Result

Repeated standard two-process medians rose from Epic 13's 48.5-50.6k to 170.1-172.0k msg/s, while server allocation fell from 619-624 to 436-439 B/message. The million-message single-actor lane rose from 59.8-61.5k to 232.3-234.6k msg/s at 356 B/message; the comparable local run was 3.60M msg/s. Receiver handoff now reaches the existing direct JSON decoder ceiling of roughly 220-230k msg/s, so the next performance epic should target a negotiated compact codec and numeric route/type identifiers rather than more mailbox or registry locking work.

## Completion checklist

- [x] Failing test written first.
- [x] Failing test observed red.
- [x] Minimal implementation written.
- [x] Targeted verification green.
- [x] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated if needed.
- [ ] Review requested.
- [ ] Review feedback addressed.
