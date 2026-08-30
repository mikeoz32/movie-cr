# Epic 08: Actor System Performance Benchmarks

**Goal:** Add repeatable, end-to-end performance diagnostics for the local actor runtime and remoting with comparable workloads and receiver-side completion barriers.

**Depends on:** Epic 06.

**Status:** Done.

**Done when:**

- the same tell and ask workloads can run locally, through in-process loopback remoting, and through two-process loopback remoting,
- throughput is measured through actor-side completion rather than socket enqueue completion,
- latency, allocation, topology, payload, concurrency, and environment metadata are emitted in machine-readable form,
- warm-up and repeated measurement runs are configurable,
- benchmark commands build and execute in release mode,
- targeted and full verification plus mandatory review are complete.

## Task 08.1: Add an end-to-end ActorSystem benchmark runner

**Status:** Done.

**Files**

- Add: `benchmarks/actor_system.cr`
- Add: `benchmarks/support/actor_system_benchmark.cr`
- Add: `benchmarks/support/actor_system_benchmark/types.cr`
- Add: `benchmarks/support/actor_system_benchmark/runner.cr`
- Add: `benchmarks/support/actor_system_benchmark/reporter.cr`
- Add: `spec/movie/actor_system_benchmark_spec.cr`
- Modify: `README.md`
- Modify: `doc/movie/development_workflow.md`

**Outcome**

- Benchmark local, in-process remote, and two-process remote actor delivery with the same serializable command protocol.
- Cover tell throughput and ask round-trip latency with configurable message count, payload size, producer concurrency, actor count, stripe count, warm-up runs, and measured runs.
- Use actor-side acknowledgements to include mailbox dispatch and message processing in every end-to-end measurement.
- Emit human-readable summaries plus CSV or JSON Lines records containing reproducibility metadata.
- Keep host-dependent results measurement-only; do not add unstable performance thresholds to correctness tests.

**Verification**

- Write benchmark configuration, statistics, and small end-to-end contract specs first and observe them fail.
- Build the standalone runner with `--release -Dpreview_mt -Dexecution_context`.
- Run small local, in-process remote, and two-process smoke benchmarks.
- Run the default correctness suite and mandatory review.

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

Final verification: the benchmark contract suite passes with 5 examples; the full CI-flagged suite passes with 239 examples; formatting is clean across `src`, `spec`, `examples`, and `benchmarks`; the standalone release runner builds and completes local, in-process, and two-process tell/ask workloads in human, CSV, and JSON Lines formats; the existing opt-in remote benchmark passes with 14 examples. Final Standards and Spec reviews have no remaining findings.

A measurement-only diagnostic run on the 16-CPU development host used 10,000 messages per batch, 64-byte payloads, 8 producers, 8 actors, 64 asks in flight, 4 stripes, 2 warm-ups, and 5 measured runs. Median throughput was 896,372 local tell, 16,780 in-process remote tell, 41,479 two-process remote tell, 6,352 local ask, 10,672 in-process remote ask, and 15,920 two-process remote ask messages per second. Allocation rates were comparatively stable while throughput and tail latency were noisy. These values are evidence for follow-up profiling, not portable performance targets.
