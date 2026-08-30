# Epic 10: IO-Native Serialization

**Goal:** Minimize serialization allocations by writing JSON and other wire representations directly to reusable `IO` buffers wherever the destination contract does not require a materialized `String`.

**Depends on:** Epic 08 and Epic 09.

**Status:** In progress.

**Done when:**

- outbound remoting does not materialize message JSON strings or parse them back into `JSON::Any`,
- frame encoding writes envelope JSON directly into a reusable per-connection buffer before the length-prefixed socket write,
- frame decoding reuses its byte buffer and avoids a full `Bytes` -> `String` copy,
- incoming user payloads remain raw until the registered typed deserializer consumes them,
- system-message and remote-ask payloads use the same direct writer path,
- machine-readable benchmark output writes directly to its destination `IO`,
- remaining no-argument `to_json` calls in production are documented boundaries that require a materialized value,
- repeated before/after ActorSystem benchmarks report throughput and client/server allocation deltas separately,
- targeted and full verification plus mandatory review are complete.

## Task 10.1: Replace intermediate remoting JSON values with direct IO writers

**Status:** In progress.

**Files**

- Add: `src/movie/remote/json_payload.cr`
- Modify: `src/movie/remote/message_registry.cr`
- Modify: `src/movie/remote/wire_envelope.cr`
- Modify: `src/movie/remote/frame_codec.cr`
- Modify: `src/movie/remote/connection.cr`
- Modify: `src/movie/remote/server.cr`
- Modify: `src/movie/remote/remote_actor_ref.cr`
- Modify: `src/movie/remote/extension.cr`
- Modify: `benchmarks/support/actor_system_benchmark/runner.cr`
- Modify: `benchmarks/support/actor_system_benchmark/reporter.cr`
- Modify: remoting and benchmark contract specs

**Outcome**

- Represent outbound payloads as objects that implement `to_json(JSON::Builder)` instead of an eagerly built JSON DOM.
- Keep decoded payload JSON raw until typed deserialization; materialize `JSON::Any` only for the dynamic payload APIs that require it.
- Reuse encoder and decoder storage per connection under the existing read/write ownership boundaries.
- Preserve the wire schema and public envelope behavior.

**Verification**

- Write and observe a failing spec whose message rejects no-argument `to_json` but succeeds through direct builder serialization.
- Verify wire compatibility, multi-frame decoding, typed message round trips, remote ask/system protocols, and two-process delivery.
- Audit production no-argument serialization calls and record intentional materialization boundaries.
- Re-run the unchanged release ActorSystem benchmark matrix from Epic 09.
- Complete Standards and Spec review with no remaining findings.

### Benchmark result

The release comparison used the unchanged Epic 09 matrix twice: 10,000 messages, 64-byte payloads, 8 producers, 8 actors, 64 in-flight asks, 4 remote stripes, 2 warmups, and 5 measured runs per workload. Allocation ranges were stable across both matrices; throughput remained noisier.

| Workload allocation | Before | After | Change |
|---|---:|---:|---:|
| In-process remote tell, combined client/server process | 8.82-8.84 KB/msg | 5.00 KB/msg | approximately -43% |
| In-process remote ask, combined client/server process | 16.40-16.41 KB/msg | 10.27-10.28 KB/msg | approximately -37% |
| Two-process remote tell, client | 4.29-4.30 KB/msg | 1.077 KB/msg | approximately -75% |
| Two-process remote tell, server | 4.53-4.54 KB/msg | 3.92-3.93 KB/msg | approximately -13% |
| Two-process remote ask, client | 9.01 KB/msg | 5.506-5.508 KB/msg | approximately -39% |
| Two-process remote ask, server | 7.40-7.41 KB/msg | 4.74-4.76 KB/msg | approximately -36% |

| Workload median throughput | Epic 09 baseline | Epic 10 series 1-2 | Change range |
|---|---:|---:|---:|
| Local tell | 753,216 msg/s | 854,231-957,362 msg/s | +13% to +27% |
| Local ask | 18,436 msg/s | 19,917-21,985 msg/s | +8% to +19% |
| In-process remote tell | 17,906 msg/s | 19,744-20,104 msg/s | +10% to +12% |
| In-process remote ask | 12,682 msg/s | 13,234-13,652 msg/s | +4% to +8% |
| Two-process remote tell | 34,027 msg/s | 27,854-29,528 msg/s | -18% to -13% |
| Two-process remote ask | 18,018 msg/s | 21,320-22,277 msg/s | +18% to +24% |

Local workloads are included for completeness even though this change does not target them and their allocation profile stayed effectively unchanged. Five of six throughput workloads improved; two-process tell measured lower. Host-level throughput and tail latency remain too noisy to treat that single workload as a regression without profiler evidence. Allocation reductions were consistent in every measured remote run.

The production serialization audit leaves two intentional no-argument `to_json` calls in persistence. Event journal and durable-state store interfaces require a materialized `String` for SQLite text binding, so direct streaming cannot remove that final value without changing the storage contract.

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
