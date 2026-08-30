# Epic 11: Inbound Remoting Hot-Path Optimization

**Goal:** Reduce server-side remote message allocations after a frame reaches the socket, while preserving malformed-message isolation, wire compatibility, and public frame-decoder behavior.

**Depends on:** Epic 10.

**Status:** In progress.

**Done when:**

- the inbound tell pipeline reports decode, typed-deserialization, and combined allocations separately,
- registered user messages can be deserialized directly from the envelope pull parser without rebuilding payload JSON,
- unknown message tags and compatible envelopes with `payload` before `message_type` retain the existing raw/lazy payload behavior,
- a malformed registered payload is rejected without poisoning the decoder or consuming the next frame,
- public stateless frame decoding retains its existing raw-payload semantics,
- repeated before/after ActorSystem benchmarks report throughput and client/server allocation deltas separately,
- targeted and full verification plus mandatory review are complete.

## Task 11.1: Decode registered inbound payloads directly into typed messages

**Status:** In progress.

**Files**

- Modify: `src/movie/remote/message_registry.cr`
- Modify: `src/movie/remote/wire_envelope.cr`
- Modify: `src/movie/remote/frame_codec.cr`
- Modify: `src/movie/remote/connection.cr`
- Modify: `src/movie/remote/server.cr`
- Modify: remoting and benchmark specs

**Outcome**

- Inject the registered-message payload decoder only into connection-owned frame decoders.
- Preserve a decoded typed wrapper in the envelope until normal routing consumes it.
- Keep unknown and payload-before-type envelopes raw, and keep malformed typed-payload failures isolated to one already-buffered frame.
- Avoid coupling the public frame codec to the global registry.

**Verification**

- Measure the current frame-decode, typed-deserialization, and combined allocation costs independently.
- Write and observe a failing spec whose type rejects the materialized `from_json(String | IO)` path but accepts `JSON::PullParser` construction.
- Verify decoder recovery by decoding a valid frame after a malformed registered payload, including payload-before-type wire ordering.
- Re-run the unchanged release ActorSystem benchmark matrix from Epic 10.
- Complete Standards and Spec review with no remaining findings.

### Benchmark result

The release microbenchmark isolates the registered-message inbound stages over 10,000 iterations. Before this task, raw envelope decode allocated 2,080.0 B/msg, the second typed-deserialization parse allocated 944.7 B/msg, and their combined cost was 3,023.5-3,023.8 B/msg. With direct pull-parser decoding, the complete registered decode plus typed-message preparation costs 1,504.1-1,535.7 B/msg, approximately 49-50% less.

The unchanged ActorSystem matrix was then repeated twice with 10,000 messages, 64-byte payloads, 8 producers, 8 actors, 64 in-flight asks, 4 remote stripes, 2 warmups, and 5 measured runs per workload.

| Workload allocation | Epic 10 | Epic 11 | Change |
|---|---:|---:|---:|
| In-process remote tell, combined process | 4.999-5.003 KB/msg | 3.446-3.457 KB/msg | approximately -31% |
| In-process remote ask, combined process | 10.272-10.285 KB/msg | 8.706-8.727 KB/msg | approximately -15% |
| Two-process remote tell, client | 1.077 KB/msg | 1.076-1.078 KB/msg | effectively unchanged |
| Two-process remote tell, server | 3.919-3.932 KB/msg | 2.360-2.374 KB/msg | approximately -40% |
| Two-process remote ask, client | 5.506-5.508 KB/msg | 5.506-5.508 KB/msg | effectively unchanged |
| Two-process remote ask, server | 4.741-4.760 KB/msg | 3.187-3.199 KB/msg | approximately -33% |

| Workload median throughput | Epic 10 series 1-2 | Epic 11 series 1-2 |
|---|---:|---:|
| Local tell | 854,231-957,362 msg/s | 821,846-2,185,223 msg/s |
| Local ask | 19,917-21,985 msg/s | 12,962-13,654 msg/s |
| In-process remote tell | 19,744-20,104 msg/s | 20,233-22,097 msg/s |
| In-process remote ask | 13,234-13,652 msg/s | 12,787-14,047 msg/s |
| Two-process remote tell | 27,854-29,528 msg/s | 21,344-28,737 msg/s |
| Two-process remote ask | 21,320-22,277 msg/s | 21,751-23,483 msg/s |

The per-run allocation ranges were stable and changed only on the server/combined side targeted by this task. Throughput and tails remained host-noisy, including large unrelated variation in local workloads, so no throughput conclusion is drawn beyond reporting the repeated medians.

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
