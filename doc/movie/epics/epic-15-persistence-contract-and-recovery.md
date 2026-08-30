# Epic 15: Persistence Contract and Recovery

**Goal:** Turn SQLite persistence from a happy-path helper into a coherent pre-1.0 API with explicit effects, atomic journal semantics, restart-safe recovery, entity lifecycle management, schema evolution, snapshots, and isolated blocking I/O.

**Depends on:** Epic 14.

**Status:** In progress.

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

## Task 15.1: Establish persistence failure regressions

**Status:** In progress.

- Cover original database error propagation instead of timeout-only failure.
- Cover entity eviction and re-resolution after termination.
- Cover restart recovery for event-sourced and durable-state behaviors.

## Task 15.2: Introduce typed effects

**Status:** Pending.

- Replace `Array(Event)` and nullable-state command results with typed event and durable effects.
- Support `persist`, `persist_all`, `delete`, `none`, `stop`, and post-persist callbacks.
- Prevent callbacks and in-memory mutation before a successful write.

## Task 15.3: Add atomic per-entity journal revisions

**Status:** Pending.

- Store `(persistence_id, sequence_nr)` as the journal identity.
- Append all events from one command in a single transaction.
- Reject stale expected revisions with a typed concurrency error.

## Task 15.4: Make recovery restart-safe

**Status:** Pending.

- Reset persistent state and recovery flags on restart.
- Define recovery and persistence failure hooks.
- Ensure failed commands do not partially update state or run post-persist callbacks.

## Task 15.5: Repair entity lifecycle

**Status:** Pending.

- Watch spawned entities, remove terminated references, and respawn on the next lookup.
- Keep event-sourced and durable registries type-safe without duplicated factory logic.

## Task 15.6: Add snapshots and schema evolution

**Status:** Pending.

- Add event manifests and a typed upcast hook.
- Add snapshot save/load/delete contracts and recovery from snapshot plus later events.
- Keep snapshot policy explicit in the behavior API.

## Task 15.7: Isolate SQLite blocking I/O

**Status:** Pending.

- Execute each SQLite connection on a dedicated bounded blocking worker.
- Preserve connection affinity, orderly shutdown, and original exception propagation.
- Keep actor dispatcher threads free from native database calls.

## Task 15.8: Document and verify the public contract

**Status:** Pending.

- Add a dedicated persistence guide and complete public example.
- Update feature maturity and configuration documentation.
- Pass focused persistence, full-suite, minimum-version, formatting, example, and review gates.

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
