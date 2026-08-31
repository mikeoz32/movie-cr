# Movie

Movie is a lightweight typed actor framework for Crystal. It provides actor lifecycle and supervision, ask/futures, scheduling, bounded execution, pluggable SQLite/PostgreSQL persistence, typed streams, and an experimental TCP remoting MVP.

## Feature maturity

| Area | Status | Notes |
|---|---|---|
| Typed actors and lifecycle | Stable core | Hierarchical actors, mailbox isolation, watching, restart/stop/resume supervision, and orderly shutdown. |
| Futures, ask, scheduler | Stable public API | Thread-safe terminal futures, lightweight local ask response refs, and cancellable one-shot timers. |
| Executor | Advanced API | Bounded worker pool; task timeout does not cancel the task body. |
| Persistence | Beta | Typed effects, atomic revisions, restart recovery, snapshots, schema upcasting, local SQLite, and a shared PostgreSQL backend; cluster sharding is not included. |
| Typed streams | MVP | Manual sources, transform stages, fold/collect sinks, cancellation, backpressure, and broadcast fan-out. |
| Remoting | Experimental MVP | Typed TCP delivery and remote ask, without production transport guarantees. |

## Requirements and installation

- Crystal 1.19.1 through 1.21.x.
- SQLite development headers when using persistence or running the full test suite.

Add Movie to `shard.yml`:

```yaml
dependencies:
  movie:
    github: mikeoz32/movie
```

Then install dependencies:

```bash
shards install
```

## Typed actors

The main entrypoint includes the actor runtime, async primitives, streams, and remoting:

```crystal
require "movie"

class Printer < Movie::AbstractBehavior(String)
  def initialize(@received : Channel(String))
  end

  def receive(message : String, context : Movie::ActorContext(String))
    puts message
    @received.send(message)
    Movie::Behaviors(String).same
  end
end

received = Channel(String).new(1)
system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "example")
printer = system.spawn(Printer.new(received), name: "printer")
printer << "hello from Movie"
received.receive
system.shutdown
```

An actor returns its next behavior from `receive`. `Behaviors(T).same` keeps the active behavior, `Behaviors(T).stopped` requests a graceful stop, and `Behaviors(T).setup` builds a behavior with access to its `ActorContext`.

A parent's `SupervisionConfig` controls failures of that parent's children. See [the lifecycle architecture](doc/movie/actor_lifecycle.md) and the corrected [supervision example](examples/supervision_example.cr).

## Ask, futures, scheduler, and executor

`ActorRef#ask`, `ActorContext#ask`, and `ActorSystem#ask` are the local request/response APIs. They return `Future(T)`, which completes once with a value, exception, or cancellation. `Future#await` raises `Movie::FutureTimeout` for a waiting timeout, `Movie::FutureCancelled` for cancellation, and re-raises the original failure.

Actors reply with `Movie::Ask.reply_if_asked(context.sender, value)` or the explicit success/failure helpers.

`Movie::Scheduler` provides `schedule_once`, `schedule_message`, and `schedule_system_message`. Cancelling a `TimerHandle` prevents a callback only if it has not fired; it does not interrupt running work.

`Movie::Execution.get(system)` exposes the bounded executor. `execute` returns a future; `execute_with_reply` sends `TaskSuccess(T)` or `TaskFailure(T)`. A timeout completes the result path with `FutureTimeout` but does not cancel the underlying task body.

## Persistence

Persistence is intentionally optional and has a separate entrypoint:

```crystal
require "movie"
require "movie/persistence"

config = Movie::Config.builder
  .set("persistence.db-path", "data/movie.sqlite3")
  .set("persistence.pool-size", 1)
  .set("persistence.io-queue-capacity", 256)
  .set_duration("persistence.operation-timeout", 5.seconds)
  .build

system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
event_sourcing = Movie::EventSourcing.get(system)
durable_state = Movie::DurableState.get(system)
```

`EventSourcedBehavior` and `DurableStateBehavior` use typed effects, optimistic revisions, restart-safe recovery, and post-persist callbacks. Event batches are atomic; deletes retain revision tombstones; optional snapshots bound replay. Backend connections run on dedicated isolated connection threads rather than actor dispatchers. See the [persistence guide](doc/movie/persistence.md) for the complete API and cluster limits.

Run the complete event-sourcing example with `crystal run examples/persistence_example.cr -Dpreview_mt -Dexecution_context`.

For a shared PostgreSQL journal, require `movie/persistence/postgres`, set `persistence.backend = postgres` and `persistence.connection-uri`, or run `MOVIE_POSTGRES_URL=postgres://... crystal run examples/postgres_persistence_example.cr -Dpreview_mt -Dexecution_context`.

## Typed streams

Typed streams run on an existing actor system and do not create a hidden runtime:

```crystal
alias Streams = Movie::Streams::Typed
alias Message = Streams::MessageBase(Int32)

system = Movie::ActorSystem(Message).new(Movie::Behaviors(Message).same)
pipeline = Streams.manual(Int32)
  .via(Streams::MapFlow(Int32).new { |value| value * 2 })
  .to_collect(initial_demand: 2u64, channel_capacity: 2)
  .run(system)

pipeline.source << Streams::Produce(Int32).new(1)
pipeline.source << Streams::Produce(Int32).new(2)
pipeline.source << Streams::OnComplete(Int32).new
pipeline.completion.await
system.shutdown
```

See [the streams protocol](doc/movie/streams.md), [typed blueprint example](examples/streams_blueprints.cr), [legacy basic example](examples/streams_basic.cr), and [showcase](examples/streams_showcase.cr).

## Remoting

Remoting is an experimental MVP for validating typed delivery between Movie systems. Wire messages must include `JSON::Serializable` and be registered with `Movie::Remote::MessageRegistry` on both systems.

It supports typed fire-and-forget delivery, remote ask, sender paths, and remote `Stop`, `Watch`, `Unwatch`, `Terminated`, and `Failed`. It does not provide authentication, encryption, automatic reconnect, acknowledgements, version negotiation, or durable delivery.

`ActorSystem#actor_for` returns `ActorRefBase`; narrow a remote result before using its typed API:

```crystal
remote = system.actor_for(remote_path, Ping).as(Movie::Remote::RemoteActorRef(Ping))
remote << Ping.new(1)
reply = remote.ask(Request.new("hello"), Response).await(2.seconds)
```

See [the remoting contract](doc/movie/remoting.md) and [complete example](examples/remoting_example.cr).

## Configuration

Configuration supports YAML, JSON, builders, fallbacks, and environment overrides. Public keys use dotted sections and hyphenated compound names, for example `supervision.max-restarts` and `remoting.stripe-count`.

The complete schema, null semantics, error behavior, defaults, and environment-variable mapping are documented in [configuration.md](doc/movie/configuration.md).

## API stability

Stable application-facing APIs:

- typed actors, actor references, lifecycle, supervision, and shutdown;
- local ask APIs and `Future(T)` read-side operations;
- scheduler one-shot timers and `TimerHandle`.

Advanced APIs that may change more aggressively:

- `Promise(T)` callback bridging;
- executor protocol types and direct executor integrations;
- persistence entity/store internals;
- streams and remoting while they remain MVP features.

## Development and verification

Every implementation task follows the repository workflow: start from an explicit epic task, write and observe a failing test before production code, run fresh targeted and broad verification, update public documentation, and complete a review pass. See [development_workflow.md](doc/movie/development_workflow.md).

Default correctness gates:

```bash
crystal tool format --check src spec examples
crystal spec spec/movie -Dpreview_mt -Dexecution_context
for file in examples/*.cr; do crystal build "$file" -Dpreview_mt -Dexecution_context -o "/tmp/movie-$(basename "$file" .cr)"; done
```

Benchmarks and stress scenarios are intentionally opt-in:

```bash
MOVIE_BENCH=1 crystal spec --release spec/movie/remote/benchmark_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_STRESS=1 crystal spec spec/movie/remote/stress_spec.cr -Dpreview_mt -Dexecution_context
```

Benchmark output is measurement-only because absolute throughput and relative speedup depend on the host, Crystal version, and scheduler. Correctness remains enforced by the default and stress suites.

### ActorSystem end-to-end benchmark

The standalone ActorSystem runner compares the same serializable tell and ask workloads across local delivery, two actor systems connected in-process, and two separate processes over TCP loopback. Tell batches stop timing only after actor-side snapshot barriers confirm that every message was processed; remote results therefore include serialization, framing, TCP, routing, mailbox dispatch, and behavior execution rather than only socket writes.

Build the runner in release mode:

```bash
crystal build benchmarks/actor_system.cr --release -Dpreview_mt -Dexecution_context \
  -o /tmp/movie-actor-system-benchmark
```

Run a comparable topology matrix:

```bash
/tmp/movie-actor-system-benchmark \
  --topology all \
  --operation both \
  --messages 100000 \
  --payload-bytes 64 \
  --producers 8 \
  --actors 8 \
  --in-flight 64 \
  --stripes 4 \
  --warmup 2 \
  --runs 10
```

Use `--format csv` or `--format jsonl` for machine-readable output. Every row includes the Git revision, Crystal version, release flag, CPU count, workload dimensions, end-to-end throughput, client allocation and CPU deltas, ask latency percentiles, and separate server allocation/CPU deltas for two-process remoting. The two-process topology starts and gracefully stops a child server using the same benchmark executable.

The documentation index and recovery history live under [doc/movie](doc/movie/README.md).
