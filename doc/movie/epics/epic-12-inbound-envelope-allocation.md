# Epic 12: Inbound Envelope Allocation Reduction

**Goal:** Reduce the remaining inbound registered-message allocation cost without changing the wire schema or weakening decoder isolation and compatibility.

**Depends on:** Epic 11.

**Status:** In progress.

**Done when:**

- release microbenchmarks separate JSON parser/input, typed-wrapper, and combined envelope costs,
- the dominant avoidable allocation is removed without adding per-message retained state,
- connection-owned decoding remains direct for canonical registered payloads,
- stateless, unknown-tag, and payload-before-type fallbacks retain their existing behavior,
- malformed registered frames remain isolated from the next frame,
- repeated before/after ActorSystem benchmarks report throughput and allocation deltas separately,
- targeted and full verification plus mandatory review are complete.

## Task 12.1: Remove the next measured inbound envelope allocation boundary

**Status:** In progress.

**Files**

- Modify: inbound remoting payload/envelope/codec implementation selected by the allocation decomposition
- Modify: remoting benchmark and regression specs
- Modify: remoting documentation if runtime behavior changes

**Outcome**

- Measure before selecting between parser/input reuse and decoded-wrapper simplification.
- Implement only the smallest change that removes the dominant verified cost.
- Preserve the Epic 11 protocol and error-isolation contracts.

**Verification**

- Record repeated parser/input and wrapper allocation measurements.
- Write and observe a failing behavioral regression before production changes.
- Re-run the unchanged release ActorSystem benchmark matrix from Epic 11.
- Complete Standards and Spec review with no remaining findings.

### Benchmark result

The release allocation decomposition was repeated before selecting an implementation. The decoded typed value itself cost 896.2-896.8 B/msg, while registry wrapping raised that to 975.2-976.1 B/msg, so merging wrapper objects could recover only about 80 B/msg. Copying the frame into a `String` raised the combined parser cost from 1,534.7-1,536.3 B/msg to 1,823.7 B/msg and was rejected. A connection-local reusable pull parser prototype measured 352.1 B/msg twice, identifying parser/lexer state as the dominant avoidable allocation.

After implementation, the same release microbenchmark reported:

| Inbound microbenchmark | Before | After | Change |
|---|---:|---:|---:|
| Canonical registered decode plus typed preparation | 1,534.7-1,536.3 B/msg | 335.6 B/msg | approximately -78% |
| Raw compatibility envelope decode | 2,079.6-2,080.3 B/msg | 880.1 B/msg | approximately -58% |
| Frame decode throughput | 210,766-213,327 ops/s | 269,626-293,264 ops/s | approximately +26% to +39% |

The unchanged ActorSystem matrix was repeated twice with 10,000 messages, 64-byte payloads, 8 producers, 8 actors, 64 in-flight asks, 4 remote stripes, 2 warmups, and 5 measured runs per workload.

| Workload allocation | Epic 11 | Epic 12 | Change |
|---|---:|---:|---:|
| In-process remote tell, combined process | 3.446-3.457 KB/msg | 2.438-2.455 KB/msg | approximately -29% |
| In-process remote ask, combined process | 8.706-8.727 KB/msg | 6.201-6.213 KB/msg | approximately -29% |
| Two-process remote tell, client | 1.076-1.078 KB/msg | 1.076-1.077 KB/msg | effectively unchanged |
| Two-process remote tell, server | 2.360-2.374 KB/msg | 1.362-1.381 KB/msg | approximately -42% |
| Two-process remote ask, client | 5.506-5.508 KB/msg | 4.322-4.324 KB/msg | approximately -21% |
| Two-process remote ask, server | 3.187-3.199 KB/msg | 1.865-1.872 KB/msg | approximately -41% |

| Workload median throughput | Epic 11 series 1-2 | Epic 12 series 1-2 |
|---|---:|---:|
| Local tell | 821,846-2,185,223 msg/s | 836,192-851,933 msg/s |
| Local ask | 12,962-13,654 msg/s | 16,930-18,754 msg/s |
| In-process remote tell | 20,233-22,097 msg/s | 21,488-23,639 msg/s |
| In-process remote ask | 12,787-14,047 msg/s | 18,732-19,916 msg/s |
| Two-process remote tell | 21,344-28,737 msg/s | 35,009-37,718 msg/s |
| Two-process remote ask | 21,751-23,483 msg/s | 25,702-27,314 msg/s |

Remote throughput improved in both repeated series, while local results remained unrelated and noisy. Allocation ranges were stable: tell-client allocations stayed unchanged, while every process that decodes response or request frames improved.

The parser retains at most 256 pooled JSON keys and discards lexer storage after frames larger than 1 MiB. The implementation passes the full suite on the repository's Crystal 1.19.1 CI version as well as the local Crystal 1.21 toolchain. The stale `shard.yml` minimum was corrected from 1.18.2, which cannot compile the existing `Time::Instant` runtime code, to the verified 1.19.1 baseline.

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
