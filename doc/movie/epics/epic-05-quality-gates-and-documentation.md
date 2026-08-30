# Epic 05: Quality Gates and Documentation Accuracy

**Goal:** Make the repository safer to change by default and easier to understand for the next implementation wave.

**Why this epic exists:** The project currently relies on manual discipline, the README is too thin for the actual feature surface, and correctness checks are mixed with benchmark-style specs that are noisy in normal runs.

**Depends on:** Epic 01 through Epic 04 ideally complete, but some tasks can start earlier.

**Status:** Completed on 2026-08-30.

**Done when:**

- the repository has explicit correctness gates,
- documentation reflects feature maturity honestly,
- default verification is quieter and more intentional,
- epic and workflow docs are easy to discover.

## Task 05.1: Separate correctness specs from benchmark and stress runs

**Implementation:** Complete. Benchmarks require `MOVIE_BENCH=1`; stress scenarios require `MOVIE_STRESS=1`.

**Files**

- Modify: `spec/movie/remote/benchmark_spec.cr`
- Modify: `spec/movie/remote/stress_spec.cr`
- Modify: `README.md`

**Outcome**

- Keep correctness specs in the default suite.
- Move benchmarks and stress-style checks behind an explicit flag or separate command path.

**Verification**

- Run the default spec command and confirm the default output stays focused on correctness.
- Run the opt-in benchmark/stress command separately.

## Task 05.2: Add CI for spec and example build verification

**Implementation:** Complete. CI checks formatting, runs the default correctness suite, and builds every example.

**Files**

- Add: `.github/workflows/ci.yml`
- Modify: `README.md`

**Outcome**

- Run the default spec command in CI.
- Build examples in CI so public snippets do not silently rot.

**Verification**

- Validate the workflow locally as far as practical.
- Confirm YAML and referenced commands are correct.

## Task 05.3: Expand README into a real project entry point

**Implementation:** Complete. README documents maturity, core actors, async APIs, optional persistence, streams, remoting, configuration, and verification.

**Files**

- Modify: `README.md`

**Outcome**

- Document feature maturity, installation, core usage, persistence, remoting status, streams, and verification commands.
- Link to deeper docs in `doc/movie`.

**Verification**

- Build any examples referenced by new README snippets.

## Task 05.4: Add a docs index and cross-links

**Implementation:** Complete. The docs index links architecture, configuration, protocol, workflow, backlog, and epic documents.

**Files**

- Add: `doc/movie/README.md`
- Modify: `doc/movie/actor_lifecycle.md`
- Modify: `doc/movie/streams.md`

**Outcome**

- Make `doc/movie` browsable from a single page.
- Link the backlog, workflow, architecture docs, and epics together.

**Verification**

- Read through the docs from the new index and confirm every link resolves.

## Task 05.5: Enforce workflow rules in contributor-facing docs

**Implementation:** Complete. README and the development workflow both require TDD, fresh verification, documentation updates, and review.

**Files**

- Modify: `README.md`
- Modify: `doc/movie/development_workflow.md`

**Outcome**

- Make TDD, verification, and mandatory review part of normal contributor guidance rather than tribal knowledge.

**Verification**

- Confirm the workflow doc and README do not contradict each other.

## Task 05.6: Serialize lazy extension startup

**Implementation:** Complete. Concurrent callers share one fully started extension; failed startup is not published.

**Files**

- Modify: `src/movie.cr`
- Modify: `src/movie/extension_id.cr`
- Modify: `src/movie/remote/extension_id.cr`
- Modify: `spec/movie/actor_system_spec.cr`

**Outcome**

- Make extension creation and startup one atomic registry operation.
- Do not expose an extension until `start` succeeds.
- Publish the actual remoting address only after a successful bind.

**Verification**

- Add a failing concurrent-start regression spec first.
- Run actor-system and remoting integration specs.

## Task 05.7: Add direct typed-stream correctness gates

**Implementation:** Complete. Direct specs cover demand, transformations, terminal cancellation, and broadcast demand.

**Files**

- Modify: `src/movie/streams_typed.cr`
- Add: `spec/movie/streams_typed_spec.cr`
- Modify: `doc/movie/streams.md`

**Outcome**

- Cover backpressure and terminal behavior without relying on examples.
- Keep collect-sink cancellation responsive even while delivery waits on an unbuffered output channel.
- Drop pending and later elements after cancellation.

**Verification**

- Observe the pending-delivery cancellation spec time out before the fix.
- Run direct stream specs and the full correctness suite.

## Task 05.8: Validate onboarding examples and persistence defaults

**Implementation:** Complete. The supervision example uses a real parent/child tree and persistence creates a configured path's parent directories.

**Files**

- Modify: `examples/supervision_example.cr`
- Modify: `src/movie/persistence.cr`
- Modify: `spec/movie/persistence_store_spec.cr`
- Modify: `README.md`

**Outcome**

- Demonstrate one-for-one and all-for-one supervision with the actual hierarchy those scopes act on.
- Make the documented default and nested SQLite paths usable on a clean checkout.
- State the optional persistence entrypoint explicitly.

**Verification**

- Add a failing nested-path persistence spec first.
- Build and run the supervision example and run persistence specs.

## Completion evidence

- [x] Failing test written first.
- [x] Failing test observed red.
- [x] Minimal implementation written.
- [x] Targeted verification green.
- [x] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated if needed.
- [x] Review requested.
- [x] Review feedback addressed.

Additional completion gates: the opt-in benchmark and stress suites, dependency check, workflow YAML parse, all example builds, runtime smokes, and local documentation link validation are green.

### Review notes

- The spec review has no remaining missing, scope, or implementation findings after the env-conversion and pending-delivery cancellation fixes.
- The standards review has no hard violations. Its new-stream responsibility finding was addressed by extracting `DeliveryPump(T)` from `CollectSink(T)`.
- A low-severity duplication between the pre-existing event-sourcing and durable-state entity-resolution helpers is intentionally deferred to a persistence-internals refactor; changing that stable behavior is outside this quality-gate epic.
