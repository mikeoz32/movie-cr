# Epic 09: Local Ask Hot-Path Optimization

**Goal:** Remove per-request actor lifecycle overhead from local ask while preserving reply, timeout, termination, and watcher-cleanup semantics.

**Depends on:** Epic 08.

**Status:** Completed (2026-08-30).

**Done when:**

- local `ActorRef#ask`, `ActorContext#ask`, and `ActorSystem#ask` share one lightweight response path,
- a local ask no longer spawns or registers a temporary listener actor,
- successful, failed, cancelled, timed-out, and target-terminated asks remain race-safe,
- target watcher registrations are removed after every terminal outcome,
- repeated before/after ActorSystem benchmarks report throughput, latency, and allocation deltas,
- targeted and full verification plus mandatory review are complete.

## Task 09.1: Replace local ask listener actors with one-shot response refs

**Status:** Completed (2026-08-30).

**Files**

- Modify: `src/movie/ask.cr`
- Modify: `src/movie.cr`
- Modify: `src/movie/context.cr`
- Modify: `spec/movie/runtime_hardening_spec.cr`
- Modify: `doc/movie/actor_lifecycle.md`

**Outcome**

- Introduce an unregistered, one-shot local response ref backed directly by a promise.
- Centralize local ask creation, watcher registration, delivery, timeout scheduling, and cleanup.
- Keep the existing public ask APIs and response helpers compatible.
- Preserve remote ask behavior.

**Verification**

- Write and observe a failing spec proving that local ask does not consume a temporary actor registration/ID.
- Re-run local ask success, timeout, termination-race, context, and system-root specs.
- Compare the same release ActorSystem benchmark matrix used by Epic 08.
- Complete Standards and Spec review with no remaining findings.

### Benchmark result

Release comparison used the unchanged Epic 08 matrix: 10,000 messages, 64-byte payloads, 8 producers, 8 actors, 64 in-flight asks, 4 remote stripes, 2 warmups, and 5 measured runs per workload.

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Local ask median throughput | 6,352 msg/s | 18,436 msg/s | +190% |
| Local ask median latency | approximately 157 us/msg | 54.2 us/msg | approximately -66% |
| Local ask client allocation | 4.43-4.46 KB/msg | 1.28-1.29 KB/msg | approximately -71% |

Local tell and remote workloads remain noisy but retain their previous allocation profiles. In particular, in-process remote ask remains approximately 16.41 KB/msg and two-process remote ask remains approximately 9.01 KB/msg client plus 7.40 KB/msg server. Those remoting allocations are a separate optimization target.

## Completion checklist

- [x] Failing test written first.
- [x] Failing test observed red.
- [x] Minimal implementation written.
- [x] Targeted verification green.
- [x] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated if needed.
- [x] Review requested.
- [x] Review feedback addressed.

Final verification: the registration regression was observed red with the next actor receiving ID 10 instead of 8, then passed with the one-shot response ref. The focused actor-system, benchmark-contract, and runtime-hardening set passes with 37 examples; the full CI-flagged suite passes with 241 examples; the opt-in release remoting benchmark passes with 14 examples; formatting is clean across `src`, `spec`, `examples`, and `benchmarks`; and the standalone release runner completes the unchanged five-run comparison matrix. Initial Standards review findings about the unused legacy path, repeated sender dispatch, and stale listener terminology were fixed. Final Standards and Spec reviews have no remaining findings.
