# Persistence

[Documentation index](README.md) · [Configuration](configuration.md) · [Cluster singleton](singleton.md) · [Recovery backlog](backlog.md)

Movie persistence provides backend-neutral event-sourced and durable-state actors with SQLite and PostgreSQL storage implementations. It provides typed command effects, atomic optimistic revisions, restart recovery, entity re-resolution, event manifests, snapshots, versioned migrations, safe retention, global event queries, projection checkpoints, a transactional outbox, telemetry, and isolated resilient database I/O.

SQLite is local to one process. PostgreSQL is a shared journal that lets another actor-system node recover the same persistence id and arbitrates concurrent revisions across nodes. The separate [cluster sharding](sharding.md) and [cluster singleton](singleton.md) extensions add logical ownership, relocation, leases, and transactionally validated fencing epochs for PostgreSQL-backed event-sourced and durable-state entities. Direct, unsharded persistent refs still require the application to maintain one logical owner per persistence id; if two owners write the same revision, exactly one succeeds and the other receives `Movie::Persistence::ConcurrentWriteError`.

## Event-sourced behavior

Persistent events and state must implement `JSON::Serializable`. Command handlers return an `EventEffect(Event, State)`:

```crystal
require "movie"
require "movie/persistence"

struct Added
  include JSON::Serializable
  getter amount : Int32

  def initialize(@amount : Int32)
  end
end

struct CounterState
  include JSON::Serializable
  getter value : Int32

  def initialize(@value : Int32 = 0)
  end
end

struct Add
  getter amount : Int32
  getter operation_id : Movie::Persistence::OperationId

  def initialize(
    @amount : Int32,
    @operation_id : Movie::Persistence::OperationId = Movie::Persistence::OperationId.random,
  )
  end
end

struct GetCounter
  getter reply_to : Movie::ActorRef(Int32)

  def initialize(@reply_to : Movie::ActorRef(Int32))
  end
end

struct StopCounter
end

alias CounterCommand = Add | GetCounter | StopCounter

class Counter < Movie::EventSourcedBehavior(CounterCommand, Added, CounterState)
  protected def empty_state : CounterState
    CounterState.new
  end

  protected def apply_event(state : CounterState, event : Added) : CounterState
    CounterState.new(state.value + event.amount)
  end

  protected def handle_command(
    state : CounterState,
    command : CounterCommand,
    context : Movie::ActorContext(CounterCommand),
  ) : Movie::EventEffect(Added, CounterState)
    case command
    when Add
      persist(Added.new(command.amount), command.operation_id)
    when GetCounter
      none.then_run { |current| command.reply_to << current.value }
    when StopCounter
      stop
    else
      none
    end
  end

  protected def snapshot_every : Int32?
    1_000
  end
end

config = Movie::Config.builder
  .set("persistence.db-path", "data/movie.sqlite3")
  .set("persistence.pool-size", 2)
  .build
system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
extension = Movie::EventSourcing.get(system)
counter_type = extension.register_entity(Counter, CounterCommand) do |id, store|
  Counter.new(id.persistence_id, store)
end
counter = extension.get_entity_ref(counter_type.id("counter-1"))
```

A runnable version is available at [`examples/persistence_example.cr`](../../examples/persistence_example.cr).

`persist(event, operation_id)` appends one event. `persist_all(events, operation_id)` appends every event from one command in one transaction. Every persisted command must carry a stable `OperationId`; retain and reuse it when retrying after a timeout. Reusing an id with different content raises `OperationConflictError`. `none` performs no storage write. `stop` requests graceful actor termination. `then_run` callbacks run only after a successful journal write and state application; use them for acknowledgements and other externally visible command side effects.

`apply_event` must be deterministic. Movie gives command and event handlers a defensive JSON copy, evaluates the candidate state before writing, persists the complete event batch atomically, then publishes the new in-memory state. A mutable handler therefore cannot alter the live state when storage fails.

## Durable-state behavior

A `DurableStateBehavior(Command, State)` command handler returns `DurableEffect(State)`:

```crystal
protected def handle_command(state, command, context) : Movie::DurableEffect(Profile)
  case command
  when ReplaceProfile
    persist(command.profile, command.operation_id)
  when DeleteProfile
    delete(command.operation_id)
  when GetProfile
    none.then_run { |current| command.reply_to << current }
  when StopProfile
    stop
  else
    none
  end
end
```

Every `persist` and `delete` advances an optimistic revision. Delete writes a tombstone instead of removing revision history, so a stale writer cannot silently recreate an older state.

## Recovery, schema evolution, and snapshots

Persistent behaviors recover before handling their first user message. A restart clears their in-memory state and revision and performs recovery again. Event recovery loads the latest snapshot, then replays only later events. Stopped entities are evicted from the extension registry; resolving the same persistence id creates a fresh actor that recovers its state.

The default event, state, and snapshot manifests are their Crystal type names. Override these hooks to keep stored schemas stable and upcast older payloads:

- `event_manifest(event)` and `deserialize_event(manifest, payload)`;
- `state_manifest(state)` and `deserialize_state(manifest, payload)`;
- `snapshot_manifest(state)` and `deserialize_snapshot(manifest, payload)`.

Override `snapshot_every` with a positive event count to enable periodic snapshots. The default is `nil`, which disables snapshots. One latest snapshot is upserted per persistence id, so snapshots do not grow without bound.

Schema changes are ordered in `SCHEMA_MIGRATIONS` and recorded in `movie_schema_migration` with a version and checksum. `DatabaseExtension#readiness` applies missing migrations, validates recorded checksums, and rejects a schema newer than the running Movie build. Normal store use also ensures the schema lazily. SQLite serializes migration work inside the process and relies on its database locking across processes; PostgreSQL uses an advisory lock. Each migration and its history row commit in one transaction. Existing Epic 16 tables are adopted in place, and existing journal rows are backfilled into the global event feed without rewriting their stored payloads.

The application role needs DDL permission while a migration is pending. If production policy separates schema and runtime roles, run `database.readiness` during a deployment step with the migration-capable role before starting runtime nodes. Do not edit an applied migration or its checksum; add a new migration.

Recovery and write hooks are available for metrics and logging: `on_recovery_completed`, `on_recovery_failure`, and `on_persist_failure`. A timed-out backend write is not cancelled. The actor restarts and recovers; retrying with the same mandatory operation id does not append or apply the effect again. A duplicate response triggers another recovery, which also covers the race where the late commit became visible after restart recovery. Optimistic revisions still reject a genuinely different stale operation.

## Health and telemetry

`DatabaseExtension#metrics` and `#health` are local, non-blocking snapshots. They never contact the database. Metrics include submitted/completed/failed work, conflicts, retries, reconnects, circuit openings, current and high-water queue depth, in-flight work, and total/maximum observed latency. A failure count is an attempt count, so a request that succeeds after one retry contributes one failed attempt, one retry, and one completion.

`health.status` is `Healthy`, `Degraded`, or `Unavailable` from worker lifecycle, queue saturation, and circuit state. `last_error` is the most recently observed diagnostic and can remain populated after recovery. Use `database.readiness` when an endpoint or deployment gate must actively verify connectivity, migrations, and the current schema version:

```crystal
readiness = Movie::Database.get(system).readiness
abort readiness.error || "persistence unavailable" unless readiness.ready
puts "#{readiness.backend} schema v#{readiness.schema_version}"
```

## Snapshot-safe retention and maintenance

`database.delete_events_to(persistence_id, sequence_nr)` removes journal and query-feed rows only when the latest snapshot covers the requested sequence. It preserves the stream revision and operation-id history, so later appends and ambiguous-write deduplication remain correct. Retention is also rejected while any registered projection checkpoint is behind the highest event offset being removed.

Event-sourced actors can opt into automatic compaction after a successful snapshot:

```crystal
protected def snapshot_every : Int32?
  10_000
end

protected def delete_events_on_snapshot? : Bool
  true
end
```

An automatic retention failure is reported to `on_retention_failure` and does not turn the already committed command into a failure. Remove an intentionally retired projection fence with `database.delete_projection_offset(name)`; deleting an active projection checkpoint makes replay from its former offset impossible after retained events are gone.

`database.run_maintenance` runs `PRAGMA optimize` plus `VACUUM` on SQLite and `VACUUM (ANALYZE)` on PostgreSQL persistence tables. Schedule it outside latency-sensitive paths. Time/size retention policy, archival export, and PostgreSQL table partitioning remain deployment concerns; the runtime exposes the safe deletion primitive but does not guess those policies.

## Queries and projections

Every committed journal event receives a monotonically increasing global offset in the same transaction as the event batch. Offsets preserve commit order and may contain gaps after rolled-back transactions. SQLite's write transaction provides this ordering; PostgreSQL event appends take a transaction-scoped advisory sequencer lock before allocating offsets. This deliberately serializes PostgreSQL event commits across streams so a projection can never checkpoint past a lower offset that commits later. Durable-state writes remain independent. Query bounded pages after an offset, optionally for one persistence id:

```crystal
page = database.query_events(after_offset: 0_i64, limit: 100, persistence_id: "Order:42")
page.events.each { |event| process(event) }
```

Limits must be between 1 and 1,000. Projection checkpoints are monotonic: saving the same or a newer offset is accepted, while regression raises `ProjectionOffsetRegressionError`. `ProjectionRunner#run_once` reads one bounded page, invokes the handler, and checkpoints each event only after the handler succeeds:

```crystal
runner = Movie::Persistence::ProjectionRunner.new(database, "order-totals", page_size: 250)
while (step = runner.run_once { |event| update_read_model(event) }).has_more
end
```

This is restartable at-least-once processing. A crash between the handler and checkpoint can repeat an event, so projection handlers and read-model writes must be idempotent or transactional with their own checkpoint.

## Transactional outbox

Attach an `OutboxEntry` to an event or durable-state effect with `then_publish`, or pass entries to the typed storage request. The journal/state change, operation-id record, and outbox rows commit in the same database transaction:

```crystal
persist(OrderPlaced.new(command.id), command.operation_id)
  .then_publish(command.message_id, "billing", InvoiceRequested.new(command.id))
```

`message_id` must be stable across retries. Reusing an operation id with changed outbox content raises `OperationConflictError`; reusing an outbox message id for another committed operation fails the transaction.

`OutboxDispatcher#run_once` claims a bounded lease, invokes the publisher, then acknowledges success or releases a failure with its diagnostic. Delivery is at least once: lease expiry or an ambiguous acknowledgement can redeliver, so downstream consumers must deduplicate by `message_id`. Use a distinct stable owner per live dispatcher instance; do not run concurrent dispatch loops with the same owner.

```crystal
dispatcher = Movie::Persistence::OutboxDispatcher.new(database, "mail-node-1")
dispatcher.run_once { |message| publisher.publish(message.destination, message.payload) }
```

## I/O and concurrency

Each backend connection is owned by one bounded `ConnectionWorker` on a dedicated `Fiber::ExecutionContext::Isolated` OS thread. Connect, statements, transactions, and close all execute there, so storage work does not block actor dispatcher threads. Store actors forward independent operations asynchronously across the configured connection pool.

Typed idempotent persistence requests automatically reconnect and retry connection loss with bounded exponential backoff. Journal and state mutations carry their original stable operation id through every attempt, so an ambiguous commit returns the stored duplicate result instead of applying twice. Queries, schema probes, projection checkpoint writes, and lease claims are also retry-safe. Raw SQL, maintenance, outbox acknowledge, and outbox release are not retried automatically because their semantics cannot be inferred safely. Optimistic concurrency and operation-content conflicts never count as connection failures and are never retried.

Each worker opens its circuit after the configured consecutive connection failures. While open it rejects work immediately; after the reset timeout it admits a probe and closes on success. Retry, reconnect, and circuit activity is visible through telemetry.

JSON is written directly into the one `String` required for SQL text binding; no second intermediate JSON string is built. Persistent command processing also performs one defensive state round trip to enforce commit-before-mutation semantics; this is a deliberate correctness allocation boundary.

## Load, soak, and fault harness

Build the opt-in benchmark in release mode:

```bash
crystal build benchmarks/persistence.cr --release -Dpreview_mt -Dexecution_context -o /tmp/movie-persistence-benchmark
```

Run a bounded SQLite load, a timed PostgreSQL soak, or deterministic connection faults:

```bash
/tmp/movie-persistence-benchmark --backend sqlite --operations 100000 --concurrency 4
/tmp/movie-persistence-benchmark --backend postgres --connection-uri "$DATABASE_URL" --duration-seconds 60 --concurrency 8 --format jsonl
/tmp/movie-persistence-benchmark --backend postgres --connection-uri "$DATABASE_URL" --operations 10000 --fault-every 100
```

The report includes successful operations, errors, throughput, p50/p99 operation latency, retries, reconnects, and circuit openings. It intentionally has no machine-dependent pass/fail throughput threshold. Use the same release binary, database topology, payload, concurrency, and host conditions when comparing runs. Fault injection is deterministic at the Movie connection boundary; it validates recovery policy but is not a substitute for database failover testing.

## Backends

SQLite remains the zero-configuration default:

```crystal
require "movie/persistence"
```

PostgreSQL is an explicit entrypoint so applications opt into its driver:

```crystal
require "movie/persistence/postgres"

config = Movie::Config.builder
  .set("persistence.backend", "postgres")
  .set("persistence.connection-uri", ENV["DATABASE_URL"])
  .set("persistence.pool-size", 4)
  .build
```

PostgreSQL schema creation is automatic and serialized with an advisory lock, so the configured role needs DDL permissions on first startup. Later nodes use the same tables. Credentials and TLS options belong in the connection URI and should come from deployment secrets, not source control.

Custom implementations register an immutable `Persistence::Backend` factory and return one `Persistence::BackendConnection` per worker. The connection implements the `JournalBackend`, `SnapshotBackend`, and `DurableStateBackend` contracts. Run the shared backend contract specs before relying on a custom implementation.

## Configuration

| Path | Default | Meaning |
|---|---:|---|
| `persistence.backend` | `sqlite` | Registered backend name: built-ins are `sqlite` and, after requiring its entrypoint, `postgres`. |
| `persistence.connection-uri` | empty | Required PostgreSQL connection URI; ignored by the default SQLite backend. |
| `persistence.db-path` | `data/movie_persistence.sqlite3` | SQLite database file. |
| `persistence.pool-size` | `1` | Parallel connection workers. Start with `1`; raise only for measured concurrent workloads. |
| `persistence.io-queue-capacity` | `256` | Bounded jobs waiting per connection worker. |
| `persistence.operation-timeout` | `5s` | Ask timeout for journal and durable-state operations. |
| `persistence.retry.max-retries` | `2` | Automatic connection-loss retries for typed idempotent requests. |
| `persistence.retry.min-backoff` | `10ms` | First retry delay. |
| `persistence.retry.max-backoff` | `250ms` | Maximum exponential retry delay. |
| `persistence.circuit-breaker.failure-threshold` | `5` | Consecutive connection failures before a worker circuit opens. |
| `persistence.circuit-breaker.reset-timeout` | `5s` | Delay before an open circuit admits a probe. |

PostgreSQL supplies shared durable storage and node-to-node recovery. The optional [cluster sharding extension](sharding.md) supplies actor placement, relocation, and lease-epoch write fencing for registered persistent entity types. It does not turn PostgreSQL into active-active storage or decide which side of a network partition may continue; persistent sharding stops on ambiguity and waits for explicit membership resolution plus lease expiry. PostgreSQL replication and failover remain deployment responsibilities; Movie reconnects after a lost established connection but does not provision or promote database replicas.
