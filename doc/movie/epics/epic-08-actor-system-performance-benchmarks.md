# Epic 08: Actor System Performance Benchmarks

**Goal:** Add repeatable, end-to-end performance diagnostics for the local actor runtime and remoting with comparable workloads and receiver-side completion barriers.

**Depends on:** Epic 06.

**Status:** In progress.

**Done when:**

- the same tell and ask workloads can run locally, through in-process loopback remoting, and through two-process loopback remoting,
- throughput is measured through actor-side completion rather than socket enqueue completion,
- latency, allocation, topology, payload, concurrency, and environment metadata are emitted in machine-readable form,
- warm-up and repeated measurement runs are configurable,
- benchmark commands build and execute in release mode,
- targeted and full verification plus mandatory review are complete.

## Task 08.1: Add an end-to-end ActorSystem benchmark runner

**Status:** In progress.

**Files**

- Add: `benchmarks/actor_system.cr`
- Add: `benchmarks/support/actor_system_benchmark.cr`
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

- [ ] Failing test written first.
- [ ] Failing test observed red.
- [ ] Minimal implementation written.
- [ ] Targeted verification green.
- [ ] Broader verification green.
- [ ] Formatting check green.
- [ ] Docs/examples updated if needed.
- [ ] Review requested.
- [ ] Review feedback addressed.
