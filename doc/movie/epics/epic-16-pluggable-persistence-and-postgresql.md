# Epic 16: Pluggable Persistence and PostgreSQL

**Goal:** Preserve the Epic 15 persistence semantics while separating them from SQLite and adding a shared PostgreSQL journal suitable for multiple actor-system nodes.

**Depends on:** Epic 15.

**Fixed point:** `9e1c114`.

**Status:** Completed (2026-08-31).

**Done when:**

- persistence behaviors depend on a backend-neutral storage contract rather than SQLite SQL,
- SQLite remains the default, backward-compatible local backend,
- applications can opt into PostgreSQL through an explicit entrypoint and configuration,
- journal batches, operation-id deduplication, optimistic revisions, durable tombstones, and snapshots have the same contract on both backends,
- a lost PostgreSQL connection fails the ambiguous in-flight operation and reconnects for later retries,
- independent actor systems can contend for and recover the same persistence id safely,
- CI exercises the shared persistence contract against SQLite and PostgreSQL,
- public configuration, operational limits, and examples are documented,
- targeted, full, minimum-version, formatting, example, and two-axis review gates pass.

## Task 16.1: Extract the backend-neutral storage contract

**Status:** Complete.

- Add a failing fake-backend regression before changing production storage code.
- Introduce backend and connection contracts plus executable typed storage requests.
- Move SQLite-specific connection and schema behavior behind the SQLite backend.
- Preserve existing public persistence behavior and SQLite migration support.

## Task 16.2: Add the PostgreSQL backend

**Status:** Complete.

- Add the explicit `movie/persistence/postgres` entrypoint and backend registration.
- Implement PostgreSQL schema creation and all journal, state, and snapshot operations.
- Preserve atomic operation-id and optimistic-revision guarantees across connections.

## Task 16.3: Verify shared and multi-node semantics

**Status:** Complete.

- Run one backend contract suite against SQLite and PostgreSQL.
- Cover competing writers from independent actor systems and recovery on another system.
- Cover connection loss, a failed ambiguous operation, reconnect, and idempotent retry.

## Task 16.4: Complete production integration

**Status:** Complete.

- Add backend and connection URI configuration with environment mappings.
- Run PostgreSQL integration specs in CI.
- Update persistence/configuration documentation and provide a PostgreSQL example.
- Complete mandatory Spec and Standards review and address material findings.

## Follow-up Task 16.5: Decompose the persistence module

**Status:** Complete.

- Preserve `require "movie/persistence"` and every public persistence type and behavior.
- Split protocol/SPI, connection workers, shared SQL, SQLite, storage actors, and persistent behaviors by responsibility.
- Treat the existing persistence and full suites as characterization tests because this refactor intentionally adds no behavior.
- Verify PostgreSQL's explicit entrypoint still composes with the split default persistence facade.

**Verification:** Extracted implementation bodies match their original source ranges exactly; SQLite characterization specs pass 20/20, PostgreSQL integration specs pass 6/6, and the PostgreSQL-enabled full suite passes 291/291 on Crystal 1.21 and the minimum supported Crystal 1.19.1.

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

- Full Movie specs with PostgreSQL enabled: 291 examples, 0 failures on Crystal 1.21.
- CI-equivalent minimum-version specs with PostgreSQL enabled: 291 examples, 0 failures on Crystal 1.19.1.
- Dedicated PostgreSQL integration specs: 6 examples, 0 failures, including physical connection termination and a committed-write/ack-loss retry.
- All 8 examples build; the PostgreSQL example persists and recovers the expected counter value of 1.
- Dependency, formatting, and whitespace checks pass.
- The final Spec review has no findings, and the final Standards review has no hard findings.

## Resolved maintainability note

The Standards review originally identified `src/movie/persistence.cr` as a non-blocking Divergent Change. Follow-up Task 16.5 resolved it by retaining that path as the stable facade while moving the backend SPI, connection worker, shared SQL engine, SQLite adapter, storage actors, and persistent behaviors into responsibility-focused modules without changing the public contract.
