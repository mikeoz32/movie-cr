# Streams Protocol and Typed Blueprints

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
- Streams are typed via `Movie::Streams::Typed`.
- The legacy actor-stage DSL uses one `MessageBase(T)` across a linear pipeline. Use a union only when that legacy surface must carry mixed payloads.
- The reusable blueprint API models `Source(Out, Mat)`, `Flow(In, Out, Mat)`, and `Sink(In, Mat)` separately, so a flow may change its element type without a union.

## Rejection / violations (to decide in impl)
- If `Request` arrives before `OnSubscribe`, either queue until subscribed or drop with warning.
- If `Subscribe` arrives when already subscribed, respond with `OnError` or ignore; choose consistent behaviour across stages.

## Future work
- Add source types beyond the current manual source.
- Add operators beyond the current map/tap/filter/take/drop MVP.
- Expand multi-subscriber, failure-race, and performance coverage.

## Reusable typed blueprints (Epic 07)

The blueprint API is the forward-looking stream surface. A blueprint is immutable and reusable; every `RunnableGraph#run` creates independent runtime channels, controls, and materialized values.

```crystal
alias Streams = Movie::Streams::Typed

source = Streams::Sources.manual(Int32)
stringify = Streams::Flows.map(Int32, String) { |value| "value=#{value}" }
length = Streams::Flows.map(String, Int32, &.size)
sink = Streams::Sinks.collect(Int32)

graph = source
  .via(stringify.via(length))
  .to_mat(sink) { |control, result| {control, result} }

control, result = graph.run(system)
control << 7
control.complete
result.await # => [7]
```

Blueprint types:

- `Source(Out, Mat)` has one typed output and materializes a source-specific control value.
- `Flow(In, Out, Mat)` has distinct input/output types and composes through `via` or `via_mat`.
- `Sink(In, Mat)` has one typed input and materializes its result or control value.
- `RunnableGraph(Mat)` is closed and returns a fresh `Mat` on every run.
- `NotUsed` marks stages without a runtime control value.

`via` preserves the materialized value on its left. `via_mat` combines the two values explicitly. `to` keeps the sink materialized value, while `to_mat` combines source and sink values. Current factories are `Sources.manual`, `Flows.map`, `Sinks.collect`, and `Sinks.fold`.

Failure is terminal in both directions: a failing flow reports the error downstream and cancels its inlet so a manual producer cannot remain blocked on an abandoned edge. Blueprint runtime edges are owned by the supplied `ActorSystem`; shutting that system down cancels unfinished materialized futures and releases blocked producers.

### Bounded buffers and overflow

Every blueprint edge has a positive, fixed capacity. `Sources.manual` and `Flows.map` accept `buffer_size` and `overflow_strategy`; the defaults are `16` and `OverflowStrategy::Backpressure`. Non-positive sizes raise `ArgumentError` when the blueprint is created.

| Strategy | Behavior when full | Offered element result |
| --- | --- | --- |
| `Backpressure` | Wait for downstream space | `Enqueued` after space is available |
| `DropHead` | Remove the oldest buffered element | `Enqueued` |
| `DropTail` | Remove the newest buffered element | `Enqueued` |
| `DropNew` | Discard the offered element | `Dropped` |
| `DropBuffer` | Clear the buffer, then enqueue the offered element | `Enqueued` |
| `Fail` | Close the queue with `BufferOverflowError` | `Failure` with the same error |

`ManualSourceControl#offer` returns a `QueueOfferResult` with `Enqueued`, `Dropped`, `QueueClosed`, or `Failure` status. The compatibility `<<` method uses the same bounded queue, waits under `Backpressure`, tolerates configured drops, and raises for closed or failed queues. Completion and failure never overtake elements already accepted into the buffer.

### Stream TestKit

`TestSources.probe(T)` and `TestSinks.probe(T)` are reusable blueprints for protocol-level specs. Materializing them returns independent `TestPublisherProbe(T)` and `TestSubscriberProbe(T)` controls:

```crystal
graph = Streams::TestSources.probe(Int32, buffer_size: 1)
  .via(Streams::Flows.map(Int32, String) { |value| "value=#{value}" })
  .to_mat(Streams::TestSinks.probe(String)) { |publisher, subscriber| {publisher, subscriber} }

publisher, subscriber = graph.run(system)
publisher.send_next(7)
subscriber.expect_no_message(25.milliseconds)
subscriber.request(1).expect_next("value=7")
publisher.send_complete
subscriber.expect_complete
```

The publisher supports `offer`, `send_next`, `send_complete`, and `send_error`. `offer` is the low-level blocking queue primitive; assertion-style `send_next` has a one-second default timeout (overridable per source or call) and cancels the source if backpressure exceeds it. The subscriber supports explicit `request(n)` plus `expect_next`, `expect_complete`, `expect_error`, and `expect_no_message`. Every assertion has a bounded timeout and reports what it awaited and what it received. Terminal signals are consumed once, reject later demand, and actor-system shutdown releases probes blocked on demand or an unconsumed assertion event.

## Legacy builder surface & materialization (OZW-65)
- Single-subscription builders in MVP.
- Sources: `Streams::Typed.manual(T)` is the currently implemented source builder. Array, single, and tick sources are future work.
- Flows (initial set): `Flow.map`, `Flow.filter`, `Flow.take(n)`, `Flow.drop(n)`; more to follow in operator tasks. MVP implementations exist as actors: `MapFlow`, `FilterFlow`, `TakeFlow`, `DropFlow`.
- Sinks: custom actor sinks through `.to`, collect through `.to_collect`, and fold through `.fold`. Named `foreach` and `first` sink factories are future work.
- Composition DSL: `Streams::Typed.manual(T).via(flow).to(sink).run(system)` returns a materialized handle.
- Materialized handle: `{completion: Future(T), cancel: -> Void}` where `T` is the sink’s materialized value (e.g., `Nil` for foreach, accumulator for fold). Cancel is idempotent and propagates `Cancel` upstream.
- Completion semantics: completion future succeeds on `OnComplete`, fails on `OnError`, cancels on `Cancel`.
- Re-materialization: calling `.to` again builds a new graph; prior refs are independent.

## Current legacy DSL (MVP)
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
