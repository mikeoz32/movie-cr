# Persistence

[Documentation index](README.md) · [Configuration](configuration.md) · [Recovery backlog](backlog.md)

Movie persistence provides backend-neutral event-sourced and durable-state actors with SQLite and PostgreSQL storage implementations. It provides typed command effects, atomic optimistic revisions, restart recovery, entity re-resolution, event manifests, snapshots, and isolated database I/O.

SQLite is local to one process. PostgreSQL is a shared journal that lets another actor-system node recover the same persistence id and arbitrates concurrent revisions across nodes. Movie still does not provide cluster membership, sharding, automatic entity relocation, or split-brain ownership. Use one logical owner per persistence id; if two owners write the same revision, exactly one succeeds and the other receives `Movie::Persistence::ConcurrentWriteError`.

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

Override `snapshot_every` with a positive event count to enable periodic snapshots. The default is `nil`, which disables snapshots. The pre-revision Movie tables are migrated in place; their existing payloads receive the `legacy` manifest.

Recovery and write hooks are available for metrics and logging: `on_recovery_completed`, `on_recovery_failure`, and `on_persist_failure`. A timed-out backend write is not cancelled. The actor restarts and recovers; retrying with the same mandatory operation id does not append or apply the effect again. A duplicate response triggers another recovery, which also covers the race where the late commit became visible after restart recovery. Optimistic revisions still reject a genuinely different stale operation.

## I/O and concurrency

Each backend connection is owned by one bounded `ConnectionWorker` on a dedicated `Fiber::ExecutionContext::Isolated` OS thread. Connect, statements, transactions, and close all execute there, so storage work does not block actor dispatcher threads. Store actors forward independent operations asynchronously across the configured connection pool. A lost established connection fails the in-flight operation because its commit status may be ambiguous; the worker reconnects for the next retry. Reuse the same operation id for that retry.

JSON is written directly into the one `String` required for SQL text binding; no second intermediate JSON string is built. Persistent command processing also performs one defensive state round trip to enforce commit-before-mutation semantics; this is a deliberate correctness allocation boundary.

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

PostgreSQL supplies shared durable storage and node-to-node recovery, but not actor ownership. A complete distributed cluster still needs membership, failure detection, sharding/placement, and split-brain protection above this persistence layer. PostgreSQL replication and failover are deployment responsibilities; Movie reconnects after a lost established connection but does not provision or promote database replicas.
