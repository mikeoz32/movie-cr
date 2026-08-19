# Epic 02: Async Primitives and Executor Hardening

**Goal:** Make futures, scheduler, and executor APIs correct, typed, and covered by dedicated tests.

**Why this epic exists:** Timeout and callback behavior are central to ask/remoting, but the executor has an invalid timeout path and the async primitives have thinner coverage than their importance warrants.

**Depends on:** Epic 01 for stable runtime semantics.

**Done when:**

- executor reply contracts are type-safe,
- scheduler cancellation semantics are tested,
- future and promise lifecycle behavior is covered directly,
- async primitives are reliable enough to support remoting work.

## Task 02.1: Fix executor reply timeout contract

**Files**

- Modify: `src/movie/executor.cr`
- Add: `spec/movie/executor_spec.cr`

**Outcome**

- Replace the invalid `TaskResult(T)`/`FutureTimeout` mismatch with a single typed protocol.
- Decide whether timeout is represented as failure message, explicit union, or future-only API.

**Verification**

- Add a failing spec that instantiates the timeout path.
- Run targeted executor specs and full suite.

## Task 02.2: Add direct future and promise specs

**Files**

- Add: `spec/movie/future_spec.cr`
- Modify: `src/movie/future.cr` only if behavior defects are found

**Outcome**

- Cover success, failure, cancellation, callback ordering, immediate callback execution on completed futures, and timeout behavior.

**Verification**

- Run `crystal spec spec/movie/future_spec.cr -Dpreview_mt -Dexecution_context`.
- Run full suite.

## Task 02.3: Add scheduler correctness specs

**Files**

- Add: `spec/movie/scheduler_spec.cr`
- Modify: `src/movie/scheduler.cr` only if tests expose defects

**Outcome**

- Cover one-shot execution, cancellation before fire, no execution after cancel, and exception isolation in scheduled callbacks.

**Verification**

- Run targeted scheduler specs and full suite.

## Task 02.4: Add executor queueing and error-path coverage

**Files**

- Modify: `spec/movie/executor_spec.cr`
- Modify: `src/movie/executor.cr` if tests expose defects

**Outcome**

- Cover worker startup, queue saturation behavior, task exception handling, and shutdown behavior.

**Verification**

- Run targeted executor specs and full suite.

## Task 02.5: Document async API expectations

**Files**

- Modify: `README.md`
- Add or modify: `doc/movie/actor_lifecycle.md`

**Outcome**

- Document how futures, scheduler, and executor should be used.
- Clarify which APIs are stable and which are internal building blocks.

**Verification**

- Build all examples if any example code changes.

