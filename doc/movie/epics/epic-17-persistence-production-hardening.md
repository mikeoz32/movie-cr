# Epic 17: Persistence Production Hardening

**Goal:** Turn the shared persistence beta into an operable production subsystem with explicit schema evolution, telemetry, bounded retention, query/projection support, transactional outbox delivery, configurable database resilience, and reproducible load/fault evidence.

**Depends on:** Epic 16.

**Fixed point:** `238e850`.

**Status:** Completed (2026-08-31).

**Done when:**

- every backend applies ordered, transactional, versioned migrations and rejects unsupported future schemas,
- applications can inspect persistence health, saturation, latency, failures, reconnects, retries, open circuits, and concurrency conflicts,
- journal retention is snapshot-safe and exposes explicit maintenance results,
- a globally ordered event query and durable projection checkpoints support restartable read-side processing,
- outbox records are committed atomically with journal or durable-state writes and acknowledged idempotently,
- bounded retry/backoff and circuit-breaker policies distinguish retryable connection loss from application conflicts,
- opt-in load, soak, and fault harnesses report comparable persistence measurements without host-dependent thresholds,
- SQLite and PostgreSQL preserve the same public contracts, and full/minimum-version verification plus two-axis review pass.

## Task 17.1: Versioned migrations

**Status:** Completed.

- Add schema history with ordered versions and checksums.
- Serialize migration execution per backend and apply each version transactionally.
- Adopt existing Epic 16 tables without rewriting valid data.
- Reject a database whose recorded version is newer than this Movie build.

## Task 17.2: Telemetry and health

**Status:** Completed.

- Expose immutable metrics and health snapshots from `DatabaseExtension`.
- Track queue depth/high-water mark, active work, completions, failures, latency, conflicts, reconnects, retries, and circuit state.
- Keep telemetry reads non-blocking and independent of backend availability.

## Task 17.3: Safe retention and maintenance

**Status:** Completed.

- Delete journal events only through a sequence covered by the current snapshot.
- Preserve stream revisions and operation-id deduplication after compaction.
- Expose explicit retention results and backend maintenance hooks.

## Task 17.4: Persistence query and projections

**Status:** Completed.

- Assign a globally ordered offset to every committed event.
- Query events after an offset in bounded pages.
- Store projection checkpoints monotonically and reject offset regression.

## Task 17.5: Transactional outbox

**Status:** Completed.

- Attach stable outbox records to event and durable-state effects.
- Commit outbox records in the same transaction as the state change.
- Load pending records in bounded pages and acknowledge delivery idempotently.

## Task 17.6: Retry, backoff, and circuit breaking

**Status:** Completed.

- Add bounded exponential retry policy for typed, idempotent persistence requests.
- Open a worker circuit after configurable consecutive connection failures and probe again after reset timeout.
- Never retry operation conflicts or optimistic-concurrency failures.

## Task 17.7: Load, soak, and fault harnesses

**Status:** Completed.

- Add opt-in SQLite and PostgreSQL throughput/latency scenarios.
- Add configurable soak duration and bounded concurrency.
- Add deterministic connection-loss and retry reporting.
- Emit machine-readable output without pass/fail throughput thresholds.

## Completion checklist

- [x] Failing tests written first.
- [x] Failing tests observed red.
- [x] Minimal implementations written.
- [x] Targeted verification green.
- [x] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated.
- [x] Review requested.
- [x] Review feedback addressed.

## Completion notes

- Version 3 schema history is ordered and checksummed. SQLite and PostgreSQL adopt populated Epic 16 journals; PostgreSQL migration and event-commit ordering have dedicated integration regressions.
- Passive metrics/health use atomics only. Active readiness verifies connectivity and returns the backend plus current schema version.
- Retention remains snapshot- and projection-safe; projections and outbox dispatchers are restartable at-least-once APIs.
- Typed idempotent operations retry bounded connection loss, while conflicts and ambiguous non-idempotent maintenance/acknowledgement operations are not retried.
- Focused production/PostgreSQL verification passes 25 examples. The full suite passes 312 examples on both the local Crystal toolchain and minimum supported Crystal 1.19.1 with PostgreSQL enabled. The opt-in benchmark smoke passes 3 examples, formatting/dependencies are clean, and all 9 examples build.
- Final release measurements on this host, 128-byte payload and concurrency 4: SQLite load 1,603 ops/s for 5,000 writes; PostgreSQL load 839 ops/s for 5,000 writes; PostgreSQL 2-second deterministic-fault soak 1,030 ops/s with 2,064 successful writes, 20 retries, 20 reconnects, and no errors. These are observations, not pass/fail thresholds.
- PostgreSQL global event offsets use a short transaction-tail advisory sequencer. It trades some parallel event-write throughput for the invariant that a projection cannot checkpoint past a lower offset that commits later; durable-state writes are not serialized by this lock.
