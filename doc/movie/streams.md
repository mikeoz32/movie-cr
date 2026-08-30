# Streams Protocol (Source/Flow/Sink MVP)

[Documentation index](README.md) · [Development workflow](development_workflow.md) · [Direct stream specs](../../spec/movie/streams_typed_spec.cr)

## Goals
- Backpressure-first: downstream explicitly requests demand; upstream never overruns demand.
- Clear terminals: completion, error, cancel are terminal; no signals after terminal.
- Keep single-subscription semantics for linear Source/Flow/Sink stages.
- Support multi-subscriber fan-out via `BroadcastHub`.

## Message types
Control/Data messages exchanged between adjacent stages (upstream -> downstream unless noted):
- `Subscribe(subscriber)` (downstream -> upstream): ask to start; upstream replies with an initial `OnSubscribe` or rejects.
- `OnSubscribe(subscription)` (upstream -> downstream): carries a handle to send `Request(n)` / `Cancel` upstream.
- `Request(n : UInt64)` (downstream -> upstream): additive demand; `n > 0`. Zero is ignored.
- `Cancel` (downstream -> upstream): terminal from downstream; upstream must stop sending and may clean up.
- `SubscriptionRequest(n, subscriber)` (downstream -> upstream): subscriber-scoped demand used by `BroadcastHub`.
- `SubscriptionCancel(subscriber)` (downstream -> upstream): subscriber-scoped cancel used by `BroadcastHub`.
- `OnNext(elem)` (upstream -> downstream): data element; allowed only when outstanding demand > 0.
- `OnComplete` (upstream -> downstream): terminal successful completion.
- `OnError(error)` (upstream -> downstream): terminal failure; error is opaque payload.

## Invariants
- Demand gate: `OnNext` only when outstanding demand > 0; each `OnNext` decrements demand by 1.
- Additive demand: `Request(n)` adds to outstanding demand (clamp at UInt64::MAX to avoid overflow).
- Non-positive requests: `Request(0)` is ignored; negative not allowed by type.
- Single terminal: after any of `OnComplete` / `OnError` / `Cancel`, no further signals (including `OnNext`, `Request`, or another terminal) are processed or emitted.
- Single subscription: linear Source/Flow/Sink stages accept at most one `Subscribe`.
- Multi-subscriber fan-out: `BroadcastHub` accepts many subscribers with independent demand/cancel state.
- Late signals: signals arriving after terminal are dropped.
- Ordering: signals are delivered in send order per link.
- Backpressure hop-by-hop: if downstream is slow, upstream must pause until it receives more `Request`.

## Stage responsibilities
- Source: owns production; sends `OnSubscribe` then waits for `Request`; emits up to demand; sends `OnComplete` when done; on error sends `OnError`; on `Cancel` stops promptly.
- Flow: on `OnSubscribe`, returns a downstream subscription; forwards `Request` upstream respecting its own buffering (MVP: no extra buffering beyond demand). Transforms/filters elements; honours demand and terminals.
- BroadcastHub: one upstream, many downstream subscribers. Tracks per-subscriber demand and propagates an upstream demand equal to max downstream outstanding demand.
- Sink: initiates `Subscribe`; manages demand policy (e.g., request batch-by-batch); handles `OnNext`/`OnComplete`/`OnError`; may `Cancel` proactively.

## Error and cancellation
- Upstream failure => `OnError` to downstream; downstream should stop and may propagate `Cancel` further upstream if needed (MVP: assume single upstream link so `OnError` is terminal).
- Downstream `Cancel` => upstream stops emitting and drops further signals; upstream may propagate `Cancel` further upstream (for Flow) and complete internal cleanup.

## Buffering (MVP)
- Default: zero/strict buffering in Flow — only emit when demand present; may hold at most one in-flight transform step.
- If implementation adds small buffer, it must still respect outstanding demand and not overrun requested total.

## Element types
- Streams are typed via `Movie::Streams::Typed`; every stage/message is parameterized by `T`.
- For mixed payloads, define your own union (for example `Nil | Int32 | Int64 | Float64 | String | Bool | Symbol | JSON::Any`).

## Rejection / violations (to decide in impl)
- If `Request` arrives before `OnSubscribe`, either queue until subscribed or drop with warning.
- If `Subscribe` arrives when already subscribed, respond with `OnError` or ignore; choose consistent behaviour across stages.

## Future work
- Add source types beyond the current manual source.
- Add operators beyond the current map/tap/filter/take/drop MVP.
- Expand multi-subscriber, failure-race, and performance coverage.

## Builder surface & materialization (OZW-65)
- Single-subscription builders in MVP.
- Sources: `Streams::Typed.manual(T)` is the currently implemented source builder. Array, single, and tick sources are future work.
- Flows (initial set): `Flow.map`, `Flow.filter`, `Flow.take(n)`, `Flow.drop(n)`; more to follow in operator tasks. MVP implementations exist as actors: `MapFlow`, `FilterFlow`, `TakeFlow`, `DropFlow`.
- Sinks (initial set): `Sink.foreach(&block)`, `Sink.fold(seed, &block)`, `Sink.first`.
- Composition DSL: `Streams::Typed.manual(T).via(flow).to(sink).run(system)` returns a materialized handle.
- Materialized handle: `{completion: Future(T), cancel: -> Void}` where `T` is the sink’s materialized value (e.g., `Nil` for foreach, accumulator for fold). Cancel is idempotent and propagates `Cancel` upstream.
- Completion semantics: completion future succeeds on `OnComplete`, fails on `OnError`, cancels on `Cancel`.
- Re-materialization: calling `.to` again builds a new graph; prior refs are independent.

## Current DSL (MVP)
- `Movie::Streams::Typed.manual(T)` creates a manual source builder.
- `.via(flow)` appends a flow stage.
- `.to(sink, initial_demand = 0)` creates a runnable pipeline.
- `.to_collect(initial_demand = 0, channel_capacity = 0)` creates a runnable pipeline with `CollectSink`.
- `.fold(initial, reducer, initial_demand = 0)` creates a runnable fold pipeline.
- `.run(system)` materializes on an existing `ActorSystem` (streams never create their own system).
- Returns `MaterializedPipeline` with:
	- `source`/`sink` refs for pushing `Produce`/`Request`/terminals.
	- `completion : Future(Nil)` completed by upstream `OnComplete`, failed by `OnError`, cancelled by downstream `Cancel`.
	- `cancel : ->` that sends `Cancel` to the sink (propagates upstream via flows).
	- `system : ActorSystem(MessageBase)` used for materialization.
	- Single-subscription only (mirrors MVP invariant).

## Runnable example
- File: [examples/streams_basic.cr](../../examples/streams_basic.cr)
- Run: `crystal run examples/streams_basic.cr -Dpreview_mt -Dexecution_context`
- Flow: external system + manual source produces 1..5, then flows map `*2`, filter evens, take 3, collect to a channel, print results, await completion.

## Showcase example
- File: [examples/streams_showcase.cr](../../examples/streams_showcase.cr)
- Run: `crystal run examples/streams_showcase.cr -Dpreview_mt -Dexecution_context`
- Demonstrates: linear transform chain, fold materialization, and broadcast hub fan-out on one external actor system.

## HTTP streaming example
- File: [examples/streams_http.cr](../../examples/streams_http.cr)
- Run: `crystal run examples/streams_http.cr -Dpreview_mt -Dexecution_context`
- Usage: `curl -N http://localhost:9292/stream?n=5`
- Flow: per request builds pipeline on shared external system (manual source -> map `*2` -> take(n) -> collect) and streams NDJSON over chunked HTTP.

## Future and Promise integration
- `FutureStatus` has `Pending`, `Success`, `Failure`, and `Cancelled` terminal states.
- `Future#await`, callbacks, status predicates, and `result` are thread-safe; a single terminal transition wins.
- `Promise#success`, `failure`, `cancel`, and their `try_*` variants complete the read-only future.
- A materialized stream completes its future on `OnComplete`, fails it on `OnError`, and cancels it when cancellation propagates through the sink.
