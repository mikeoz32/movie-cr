# Epic 07: Stream Blueprints, Boundedness, and TestKit

**Goal:** Move Movie Streams from a homogeneous linear MVP toward a reusable, type-safe, bounded stream core with first-class protocol testing.

**Depends on:** Epic 05.

**Status:** In progress.

**Done when:**

- a pipeline can change element types while preserving compile-time port compatibility,
- `Source(Out, Mat)`, `Flow(In, Out, Mat)`, `Sink(In, Mat)`, and `RunnableGraph(Mat)` are reusable blueprints,
- new stream-runtime queues are bounded and expose explicit overflow behavior,
- test probes can control downstream demand and assert elements and terminal signals,
- existing typed-stream MVP APIs remain compatible during migration,
- targeted and full verification plus mandatory review are complete.

## Task 07.1: Introduce typed reusable stream blueprints

**Status:** Done.

**Files**

- Add: `src/movie/streams/core.cr`
- Add: `spec/movie/streams_core_spec.cr`
- Modify: `src/movie.cr`
- Modify: `doc/movie/streams.md`

**Outcome**

- Add generic `Source(Out, Mat)`, `Flow(In, Out, Mat)`, `Sink(In, Mat)`, and `RunnableGraph(Mat)` blueprints.
- Support type-changing `via` composition.
- Support explicit materialized-value composition and independent re-materialization.
- Keep the existing homogeneous actor-stage DSL available as the legacy MVP surface.

**Verification**

- Observe a type-changing pipeline spec fail before implementation.
- Run the new core specs and existing direct stream specs.
- Complete spec and standards review with no remaining findings.

## Task 07.2: Enforce bounded runtime queues and overflow policies

**Status:** Planned.

**Files**

- Modify: `src/movie/streams/core.cr`
- Add: `spec/movie/streams_buffer_spec.cr`
- Modify: `doc/movie/streams.md`

**Outcome**

- Bound every queue in the new blueprint runtime.
- Add `Backpressure`, `DropHead`, `DropTail`, `DropNew`, `DropBuffer`, and `Fail` policies.
- Preserve terminal ordering and make offer outcomes observable.
- Reject invalid buffer sizes.

**Verification**

- Write overflow-policy and blocked-producer specs first.
- Run buffer, core, and existing stream specs.

## Task 07.3: Add a stream protocol TestKit

**Status:** Planned.

**Files**

- Add: `src/movie/streams/testkit.cr`
- Add: `spec/movie/streams_testkit_spec.cr`
- Modify: `src/movie.cr`
- Modify: `doc/movie/streams.md`

**Outcome**

- Add reusable test sources and sinks.
- Let a sink probe request elements explicitly and assert next, completion, failure, and silence.
- Provide deterministic timeout failures with useful diagnostics.

**Verification**

- Write probe behavior specs first.
- Use the TestKit to verify a type-changing bounded pipeline.

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
