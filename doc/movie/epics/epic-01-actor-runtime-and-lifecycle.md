# Epic 01: Actor Runtime and Lifecycle Hardening

**Goal:** Make the local actor runtime trustworthy under startup failure, supervision, stop, and restart scenarios.

**Why this epic exists:** The core runtime currently has incomplete failure semantics, mismatched behavior method signatures, blocking supervision behavior, and no clear actor-system shutdown story.

**Depends on:** None.

**Status:** Completed on 2026-05-24.

**Done when:**

- actor startup failures have explicit and tested semantics,
- restart and stop paths are deterministic,
- guardian actors follow the same behavior contract as user actors,
- the actor system exposes a documented shutdown path.

## Task 01.1: Make the behavior contract explicit

**Status:** Done.

**Files**

- Modify: `src/movie/behavior.cr`
- Modify: `src/movie.cr`
- Add or modify: `spec/movie/actor_system_spec.cr`
- Add: `spec/movie/lifecycle_spec.cr`

**Outcome**

- Remove accidental no-op behavior overrides.
- Align guardian `receive` and `on_signal` methods with the real abstract contract.
- Eliminate default stdout noise from root/system/user guardians.

**Verification**

- Run targeted lifecycle specs.
- Run `crystal spec spec/movie -Dpreview_mt -Dexecution_context`.

## Task 01.2: Define startup failure semantics

**Status:** Done.

**Files**

- Modify: `src/movie/context.cr`
- Modify: `src/movie/system.cr`
- Modify or add: `spec/movie/lifecycle_spec.cr`

**Outcome**

- Decide whether `PreStart` failure means immediate stop, restart, or terminal failure.
- Ensure failed actors are not left registered indefinitely unless that is an intentional state.
- Notify watchers consistently.

**Verification**

- Add a failing spec for `PreStart` exceptions.
- Run targeted lifecycle specs and full actor specs.

## Task 01.3: Add actor-system shutdown API

**Status:** Done.

**Files**

- Modify: `src/movie.cr`
- Modify: `src/movie/context.cr`
- Modify: `src/movie/persistence.cr`
- Modify: `src/movie/remote/extension.cr`
- Add or modify: `spec/movie/actor_system_spec.cr`

**Outcome**

- Add a public `shutdown` or equivalent API on `ActorSystem`.
- Stop guardians, child actors, scheduler-owned work, and registered extensions in a defined order.
- Make shutdown idempotent.

**Verification**

- Add specs for repeated shutdown calls and extension stop ordering.
- Run full movie spec suite.

## Task 01.4: Remove blocking supervision behavior

**Status:** Done.

**Files**

- Modify: `src/movie/context.cr`
- Modify: `src/movie/scheduler.cr`
- Modify or add: `spec/movie/lifecycle_spec.cr`

**Outcome**

- Replace blocking `sleep` in supervision backoff with scheduler-based delayed restart.
- Keep the mailbox responsive while restart delay is pending.

**Verification**

- Add a failing spec that proves unrelated messages are not blocked by restart backoff.
- Run targeted lifecycle specs and full suite.

## Task 01.5: Remove deprecated runtime APIs

**Status:** Done.

**Files**

- Modify: `src/movie/context.cr`
- Modify any related specs if timing helpers change

**Outcome**

- Replace deprecated `Time.monotonic` and `Random::DEFAULT` usage.
- Keep behavior unchanged other than API modernization.

**Verification**

- Run full spec suite and confirm deprecation warnings are gone for touched code paths.
