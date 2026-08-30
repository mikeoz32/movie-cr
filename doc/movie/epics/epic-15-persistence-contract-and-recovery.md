# Epic 15: Persistence Contract and Recovery

**Goal:** Turn SQLite persistence from a happy-path helper into a coherent pre-1.0 API with explicit effects, atomic journal semantics, restart-safe recovery, entity lifecycle management, schema evolution, snapshots, and isolated blocking I/O.

**Depends on:** Epic 14.

**Status:** Completed (2026-08-30).

**Done when:**

- event-sourced and durable-state command handlers return typed effects whose side effects run only after successful storage,
- journal writes use per-entity sequence numbers and atomically persist all events produced by one command,
- storage failures preserve their original cause and ambiguous timeout outcomes cannot be silently retried as new writes,
- persistent behaviors discard in-memory state and recover again after restart,
- stopped entity references are evicted and can be resolved to a new recovering actor,
- snapshots and event manifests/upcasting provide a bounded, evolvable recovery path,
- SQLite calls cannot block the actor execution pool,
- public persistence contracts and maturity are documented,
- targeted and full verification plus mandatory two-axis review are complete.

## Integrated delivery task

The user requested the complete persistence contract as one cohesive delivery. The sections below are acceptance milestones of that single task, not independently shippable tasks: effects depend on revisions, timeout safety depends on operation ids and recovery, and snapshots depend on the same journal schema.

### Milestone 15.1: Establish persistence failure regressions

**Status:** Complete.

- Cover original database error propagation instead of timeout-only failure.
- Cover entity eviction and re-resolution after termination.
- Cover restart recovery for event-sourced and durable-state behaviors.

### Milestone 15.2: Introduce typed effects

**Status:** Complete.

- Replace `Array(Event)` and nullable-state command results with typed event and durable effects.
- Support `persist`, `persist_all`, `delete`, `none`, `stop`, and post-persist callbacks.
- Require a stable operation id for persisted commands and deduplicate retries atomically.
- Prevent callbacks and in-memory mutation before a successful write.

### Milestone 15.3: Add atomic per-entity journal revisions

**Status:** Complete.

- Store `(persistence_id, sequence_nr)` as the journal identity.
- Append all events from one command in a single transaction.
- Reject stale expected revisions with a typed concurrency error.

### Milestone 15.4: Make recovery restart-safe

**Status:** Complete.

- Reset persistent state and recovery flags on restart.
- Define recovery and persistence failure hooks.
- Ensure failed commands do not partially update state or run post-persist callbacks.

### Milestone 15.5: Repair entity lifecycle

**Status:** Complete.

- Watch spawned entities, remove terminated references, and respawn on the next lookup.
- Keep event-sourced and durable registries type-safe without duplicated factory logic.

### Milestone 15.6: Add snapshots and schema evolution

**Status:** Complete.

- Add event manifests and a typed upcast hook.
- Add snapshot save/load/delete contracts and recovery from snapshot plus later events.
- Keep snapshot policy explicit in the behavior API.

### Milestone 15.7: Isolate SQLite blocking I/O

**Status:** Complete.

- Execute each SQLite connection on a dedicated bounded blocking worker.
- Preserve connection affinity, orderly shutdown, and original exception propagation.
- Keep actor dispatcher threads free from native database calls.

### Milestone 15.8: Document and verify the public contract

**Status:** Complete.

- Add a dedicated persistence guide and complete public example.
- Update feature maturity and configuration documentation.
- Pass focused persistence, full-suite, minimum-version, formatting, example, and review gates.

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

## Final verification

- Persistence specs: 67 examples, 0 failures on the minimum supported Crystal 1.19.1.
- Full Movie specs: 277 examples, 0 failures on Crystal 1.21 with execution contexts enabled.
- Every example builds; the persistence example also recovers the expected counter value of 5.
- Dependency, formatting, and whitespace checks pass.
- The final Spec review has no findings, and the final Standards review has no hard findings.

## Deferred maintainability notes

Two non-blocking review observations are intentionally left for a later refactor: split `src/movie/persistence.cr` into smaller modules with narrower ownership, and consider executable storage requests so a new operation does not need matching dispatch edits across several layers. Neither changes the completed persistence contract or its correctness guarantees.
