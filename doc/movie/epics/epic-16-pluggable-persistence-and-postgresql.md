# Epic 16: Pluggable Persistence and PostgreSQL

**Goal:** Preserve the Epic 15 persistence semantics while separating them from SQLite and adding a shared PostgreSQL journal suitable for multiple actor-system nodes.

**Depends on:** Epic 15.

**Fixed point:** `9e1c114`.

**Status:** In progress.

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

**Status:** In progress.

- Add the explicit `movie/persistence/postgres` entrypoint and backend registration.
- Implement PostgreSQL schema creation and all journal, state, and snapshot operations.
- Preserve atomic operation-id and optimistic-revision guarantees across connections.

## Task 16.3: Verify shared and multi-node semantics

**Status:** Pending.

- Run one backend contract suite against SQLite and PostgreSQL.
- Cover competing writers from independent actor systems and recovery on another system.
- Cover connection loss, a failed ambiguous operation, reconnect, and idempotent retry.

## Task 16.4: Complete production integration

**Status:** Pending.

- Add backend and connection URI configuration with environment mappings.
- Run PostgreSQL integration specs in CI.
- Update persistence/configuration documentation and provide a PostgreSQL example.
- Complete mandatory Spec and Standards review and address material findings.

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
