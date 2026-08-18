# Movie

A lightweight actor framework for Crystal with typed actors, supervision, remoting, persistence, futures, ask-pattern support, and typed streams.

## Features

- Typed actor system and actor references
- Supervision and lifecycle hooks
- Ask pattern and futures
- Persistence helpers and SQLite-backed stores
- Experimental remoting MVP
- Typed streams

## Installation

Add this to your `shard.yml`:

```yaml
dependencies:
  movie:
    github: mikeoz32/movie
```

## Usage

```crystal
require "movie"
```

## Async primitives

`ActorRef#ask` and `ActorContext#ask` are the primary request/response APIs. They return `Future(T)`, which completes once with either a value, an exception, or cancellation. `Future#await` raises `Movie::FutureTimeout` when the waiting side times out, `Movie::FutureCancelled` when the future is cancelled, and re-raises the original exception on failure.

`Movie::Scheduler` is the supported one-shot timer API. `schedule_once`, `schedule_message`, and `schedule_system_message` all return `TimerHandle`. Cancelling the handle prevents execution only if the callback has not fired yet; it does not interrupt a callback that is already running.

`Movie::Execution.get(system)` exposes a bounded executor for offloading blocking or expensive work away from actor message handling. `execute` returns a `Future(T)`. `execute_with_reply` sends either `TaskSuccess(T)` or `TaskFailure(T)` to the supplied actor. Timeout on executor work completes the future or reply path with `Movie::FutureTimeout`, but it does not cancel the underlying task body.

## Remoting

Remoting is currently an experimental MVP. It supports typed user-message delivery, remote ask request/response, and a limited system-message protocol over TCP. Messages sent over the wire must include `JSON::Serializable` and be registered with `Movie::Remote::MessageRegistry` on both systems.

`ActorSystem#actor_for` returns `ActorRefBase`, so a remote reference must be narrowed to `Movie::Remote::RemoteActorRef(T)` before sending or asking:

```crystal
remote = system.actor_for(remote_path, Ping).as(Movie::Remote::RemoteActorRef(Ping))
remote << Ping.new(1)
reply = remote.ask(Request.new("hello"), Response).await(2.seconds)
```

Remote ask failures, connection loss, and timeouts complete the returned future with an exception. The supported remote system messages are `Stop`, `Watch`, `Unwatch`, `Terminated`, and `Failed`. Lifecycle messages such as `PreStart`, `PostStart`, `PreStop`, `PostStop`, and `Restart` are intentionally unsupported and raise `RemoteUnsupportedSystemMessageError` when sent through the remote protocol.

The high-level `ActorRef#ask`, `ActorContext#watch`, and `ActorContext#ask` APIs currently accept local typed `ActorRef` values only. Use `RemoteActorRef#ask` for remote request/response, and use the low-level `send_system` API for remote watch operations when the watcher has a registered actor path. Binding to port `0` is supported; use `RemoteExtension#local_port` and the actor's rebound path after startup.

See [doc/movie/remoting.md](doc/movie/remoting.md) and [examples/remoting_example.cr](examples/remoting_example.cr) for the complete supported workflow.

## API stability

Stable application-facing APIs:

- `ActorRef#ask` and `ActorContext#ask`
- `Future(T)` read-side APIs such as `await`, `status`, `result`, and callback registration
- `Scheduler` one-shot timer APIs and `TimerHandle`

Advanced or internal building blocks that may change more aggressively:

- `Promise(T)`, which is intended for bridging callback-style code into `Future(T)`
- `ExecutorExtension` / `Execution`, especially direct `execute_with_reply` integrations
- `ExecutorExtension::TaskReply`, `TaskSuccess`, and `TaskFailure`, which are executor protocol messages rather than general actor reply contracts

More detail is in [doc/movie/actor_lifecycle.md](doc/movie/actor_lifecycle.md).

## Development

Run specs:

```bash
crystal spec spec/movie -Dpreview_mt -Dexecution_context
```

Build all examples:

```bash
for f in examples/*.cr; do crystal build "$f" -Dpreview_mt -Dexecution_context -o "/tmp/movie-$(basename "$f" .cr)"; done
```
