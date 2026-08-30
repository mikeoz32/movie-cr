# Movie

Movie is a lightweight typed actor framework for Crystal. It provides actor lifecycle and supervision, ask/futures, scheduling, bounded execution, optional SQLite persistence, typed streams, and an experimental TCP remoting MVP.

## Feature maturity

| Area | Status | Notes |
|---|---|---|
| Typed actors and lifecycle | Stable core | Hierarchical actors, mailbox isolation, watching, restart/stop/resume supervision, and orderly shutdown. |
| Futures, ask, scheduler | Stable public API | Thread-safe terminal futures, local ask listeners, and cancellable one-shot timers. |
| Executor | Advanced API | Bounded worker pool; task timeout does not cancel the task body. |
| Persistence | Optional, usable | SQLite event journal and durable state helpers; require the persistence entrypoint explicitly. |
| Typed streams | MVP | Manual sources, transform stages, fold/collect sinks, cancellation, backpressure, and broadcast fan-out. |
| Remoting | Experimental MVP | Typed TCP delivery and remote ask, without production transport guarantees. |

## Requirements and installation

- Crystal 1.18.2 or newer.
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
  .build

system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
event_sourcing = Movie::EventSourcing.get(system)
durable_state = Movie::DurableState.get(system)
```

`EventSourcedBehavior` replays JSON events and appends new events. `DurableStateBehavior` loads and replaces a JSON state snapshot. Entity factories must be registered with the corresponding extension before resolving entity references.

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

See [the streams protocol](doc/movie/streams.md), [basic example](examples/streams_basic.cr), and [showcase](examples/streams_showcase.cr).

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
MOVIE_BENCH=1 crystal spec spec/movie/remote/benchmark_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_STRESS=1 crystal spec spec/movie/remote/stress_spec.cr -Dpreview_mt -Dexecution_context
```

Benchmark output is measurement-only because absolute throughput and relative speedup depend on the host, Crystal version, and scheduler. Correctness remains enforced by the default and stress suites.

The documentation index and recovery history live under [doc/movie](doc/movie/README.md).
