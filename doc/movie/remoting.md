# Remoting

Remoting is a production-beta association transport. It provides restart-tolerant point-to-point delivery between Movie actor systems, but it is not cluster membership, failure voting, sharding, or a durable messaging system.

## Supported Workflows

The current protocol supports:

- typed fire-and-forget user messages;
- typed remote ask request/response;
- sender path metadata for inbound user messages when the sender has a registered path;
- remote `Stop`, `Watch`, `Unwatch`, `Terminated`, and `Failed` system messages;
- TCP connections managed through striped connection pools;
- versioned handshake negotiation with node UID, socket generation, and capabilities;
- bounded exponential reconnect with jitter while existing remote refs stay valid;
- heartbeat failure detection in both directions;
- acknowledged, sequenced, and deduplicated outbound system/control delivery;
- replay-resistant HMAC challenge-response authentication and application-owned transport wrapping hooks;
- binding to port `0` for test or dynamically allocated local ports.

User messages deliberately remain ordered-per-stripe, at-most-once traffic. A message accepted by the current socket writer is not a delivery acknowledgement and is never replayed across a socket generation. Remote asks fail when their carrying generation is lost. Use the persistence transactional outbox when a business message must survive process or network failure.

Remote watches are owned by the inbound socket generation. Disconnect cleanup sends compensating `Unwatch` messages locally, so dead remote watcher refs do not accumulate. An application that wants to continue watching after reconnect must issue a new `Watch`.

## Message Registration

Wire messages must implement `JSON::Serializable` and be registered before sending. Register the same message type on both actor systems:

```crystal
record Ping, sequence : Int32 do
  include JSON::Serializable
end

Movie::Remote::MessageRegistry.register(Ping)
```

Outgoing registered messages retain their `JSON::Serializable` value until frame encoding. The connection writes envelope and payload JSON through `to_json(JSON::Builder)` into a reusable per-connection `IO::Memory`; it does not build a message JSON `String` or an intermediate `JSON::Any`. Incoming connections reuse their frame byte buffer and decode registered payloads directly from the envelope `JSON::PullParser` into typed messages, avoiding a raw payload rebuild and second parse. Unknown tags, or compatible envelopes whose `payload` field precedes `message_type`, retain raw payload JSON for the existing lazy typed/dynamic path. Dynamic `WireEnvelope#payload` access remains available, but materializes `JSON::Any` lazily and should not be used on delivery hot paths.

Encoder/decoder buffers retain at most 1 MiB per connection. Connection-owned decoders also reuse their JSON pull parser, lexer token, object stack, string buffer, and a pool of at most 256 JSON keys. Larger frames up to the 16 MiB protocol limit use temporary storage and release oversized lexer storage so a single large message does not permanently multiply memory across all stripes.

Each TCP connection owns one writer fiber and a bounded FIFO of at most 4,096 envelopes. A successful remote `tell` means the envelope was accepted into the active generation's queue; it is not a delivery acknowledgement. Producers return without performing socket IO while capacity is available and are backpressured when the queue is full. The writer drains up to 128 ready frames and emits chunks near 64 KiB without adding a timer delay, preserving the existing sequence of `[length][JSON]` frames and per-stripe FIFO order. Generation loss rejects new user work, releases blocked producers, and discards queued user envelopes that can no longer be delivered.

Every stripe is a long-lived logical `Connection` with `Disconnected`, `Connecting`, `Handshaking`, `Active`, `Backoff`, and `Stopped` states. The socket and writer are generation-owned and replaceable. Reconnect delay grows from `50ms` to `2s` by default, with `20%` jitter; the jittered result never exceeds the configured maximum. Existing `RemoteActorRef` values retain the logical connection and resume sending when it becomes active.

Inbound connections disable the Crystal socket's built-in read buffering and replace it with one connection-owned 32 KiB buffer whose ready-byte count is observable by the transport. After the first blocking frame decode, the reader decodes up to 128 complete frames already present in that buffer before handing the batch to delivery; it never waits for another frame to fill a batch. Partial headers and payloads continue through normal blocking reads into the decoder's existing bounded frame storage. This preserves one transport input buffer per connection while avoiding a new mailbox dispatch for nearly every decoded message. Canonical registered actor paths use an exact lookup cache on delivery; alternate local/remote address forms retain the normalized parsing fallback.

Parser reuse resets private state from Crystal's standard JSON lexer and pull parser. Movie currently bounds support to Crystal 1.19.1 through 1.21.x, whose layouts are verified by the minimum-version CI lane and the current development toolchain. Supporting a newer Crystal minor requires reviewing this reset contract before widening the version constraint.

## Starting Two Systems

Use the actual bound port in paths when binding to port `0`. Actor paths are rebound to the remote address after remoting starts:

```crystal
server = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "server-system"
)
server_remote = server.enable_remoting("127.0.0.1", 0)

actor = server.spawn(PingActor.new, name: "ping")
path = actor.path.not_nil!.to_s

client = Movie::ActorSystem(String).new(
  Movie::Behaviors(String).same,
  name: "client-system"
)
client.enable_remoting("127.0.0.1", 0)
```

`server_remote.local_port` exposes the selected port, and `actor.path` contains the corresponding `movie.tcp://` address. This avoids hard-coded ports and prevents stale local actor paths after enabling remoting.

## User Messages

`ActorSystem#actor_for` returns the common `ActorRefBase` type. Narrow remote references explicitly before using their typed API:

```crystal
remote = client.actor_for(path, Ping).as(Movie::Remote::RemoteActorRef(Ping))
remote << Ping.new(1)
```

`RemoteActorRef#tell_from` can include sender path metadata when the sender is registered in the local `PathRegistry`. The receiving actor reads it through `context.sender`.

User delivery has no acknowledgement or retry contract. In particular, an envelope accepted by the outbound queue may still be lost if later serialization or socket IO fails; those failures are logged and IO errors replace the socket generation.

## Remote Ask

Remote ask is available directly on `RemoteActorRef`. The target actor replies through the normal ask helpers:

```crystal
record Request, body : String do
  include JSON::Serializable
end

record Response, body : String do
  include JSON::Serializable
end

remote_request = client.actor_for(request_path, Request).as(
  Movie::Remote::RemoteActorRef(Request)
)

future = remote_request.ask(Request.new("hello"), Response, 2.seconds)
response = future.await(3.seconds)
```

The target can reply with `Movie::Ask.reply_if_asked(context.sender, response)`. A remote ask completes with `RemoteAskError` for a target-side failure, `RemoteDeliveryError` for connection loss, or `FutureTimeout` when the timeout expires.

The high-level `ActorContext#ask` and `ActorSystem#ask` overloads currently accept local typed `ActorRef` values. They do not accept `RemoteActorRef` directly.

## System Messages

The supported remote subset is deliberately explicit. System messages initiated through `RemoteActorRef#send_system` receive a monotonic per-stripe control sequence, remain in a bounded pending buffer, and are replayed after reconnect until the receiver acknowledges them. The receiving node deduplicates the stable node/stream/sequence tuple before local routing. Deduplication state defaults to 8,192 streams and fails closed for a new stream at capacity rather than evicting a tracked stream and turning its next frame into a permanent sequence gap. When a peer process restart changes its node UID, remaining control work is moved to a fresh stream and resequenced from one rather than being mistaken for a gap in the new process.

Reverse `Terminated` and `Failed` notifications use a prewarmed outbound control association to the watcher's published address, so they receive the same sequence/ACK/dedup contract instead of being written at-most-once through the inbound socket. The watch itself remains owned by the inbound generation and is removed if that generation closes.

| Message | Meaning |
|---|---|
| `Stop` | Gracefully stop the remote target. |
| `Watch` | Register a remote watcher. The watcher must have a registered actor path. |
| `Unwatch` | Remove a remote watcher. |
| `Terminated` | Notify a remote watcher that an actor stopped. |
| `Failed` | Forward a failure notification with remote class and message metadata. |

`PreStart`, `PostStart`, `PreStop`, `PostStop`, `PreRestart`, `PostRestart`, `Restart`, and `Terminate` are not wire protocol messages. Sending an unsupported message through `RemoteActorRef#send_system` raises `RemoteUnsupportedSystemMessageError`; inbound unsupported messages are rejected and logged. The default pending-control limit is 1,024 messages per stripe; saturation rejects new control work instead of growing memory without bound.

The high-level `ActorContext#watch(remote_ref)` API is not available because `ActorContext#watch` currently accepts local typed `ActorRef` values. Remote watch is exposed through the lower-level system-message API and should be used only when the watcher path is registered.

## Association configuration

Programmatic settings can be supplied as the fourth `enable_remoting` argument:

```crystal
settings = Movie::Remote::AssociationSettings.new(
  reconnect_min_backoff: 50.milliseconds,
  reconnect_max_backoff: 2.seconds,
  heartbeat_interval: 1.second,
  heartbeat_timeout: 5.seconds,
  control_buffer_capacity: 1024,
  control_deduplication_capacity: 8192,
  shared_secret: ENV["MOVIE_REMOTE_SECRET"]?
)

system.enable_remoting("0.0.0.0", 2552, 8, settings)
```

The same settings are available under `remoting.*` configuration keys. Both peers must use the same non-empty shared secret when authentication is enabled. The client first proves its versioned identity, the server returns a signed fresh challenge, and the client confirms both nonces before either side accepts actor traffic. A captured confirmation is invalid on the next socket generation, and the secret itself is never sent over the wire.

HMAC authenticates the handshake but does not encrypt actor traffic. Deployments that need TLS supply `client_transport_factory` and `server_transport_wrapper` callbacks returning an `IO`, typically an application-configured `OpenSSL::SSL::Socket::Client` and `Server`. Certificate loading, rotation, trust roots, and server-name policy intentionally remain application responsibilities.

`RemoteExtension#control_deduplication_stats` reports tracked streams, configured capacity, rejected admissions, and the node UIDs currently retaining cursors. Alert before the receiver approaches capacity. Once an operator has independently confirmed that a specific process incarnation is permanently stopped, `RemoteExtension#retire_control_node(node_uid)` releases all of its cursors and returns the number removed. Never retire a live or reconnectable node: its next control sequence will no longer have a valid deduplication history. This lifecycle is deliberately explicit because an elapsed-time heuristic cannot prove that a distributed peer is permanently dead. Authenticate associations in exposed deployments so an unknown peer cannot consume receiver-side stream capacity anonymously.

## Failure detection and observability

An active outbound generation sends heartbeats at the configured interval and reconnects after the peer has been silent for the configured timeout. Inbound generations apply the same silence timeout, so a half-open or stalled client cannot retain server-side watch state indefinitely. Both directions measure elapsed silence with the monotonic clock, so wall-clock corrections cannot create or postpone a timeout.

`RemoteActorRef#connection.stats` returns a point-in-time `ConnectionStats` snapshot with state, socket generation, connect attempts, successful connections, disconnects, heartbeat timeouts, protocol failures, last-received timestamp, pending asks, and pending control messages. `RemoteExtension#pool_stats` retains the per-address stripe summary.

## Shutdown

Stop both actor systems after a remoting workflow:

```crystal
client.shutdown
server.shutdown
```

`ActorSystem#shutdown` stops the actor tree first, then registered extensions and remoting connections, and finally scheduler infrastructure. This ordering lets actor `PreStop`/`PostStop` cleanup use registered extensions and the scheduler. Shutdown is idempotent.

## Verification

Run the focused remoting specs:

```bash
crystal spec spec/movie/remote/integration_spec.cr spec/movie/remote/e2e_spec.cr \
  -Dpreview_mt -Dexecution_context
crystal spec spec/movie/remote/association_spec.cr -Dpreview_mt -Dexecution_context
```

Opt-in resilience and measurement runs:

```bash
MOVIE_STRESS=1 crystal spec spec/movie/remote/stress_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_BENCH=1 crystal spec --release spec/movie/remote/association_benchmark_spec.cr \
  -Dpreview_mt -Dexecution_context
```

The stress executable re-execs itself as a real peer process and covers a stopped/silent process, interruption of an in-flight ask, replay of queued control work after a new peer UID, reuse of the same remote ref after restart, and retransmission of the same control sequence after a real peer deliberately closes before sending its first ACK.

Build the example:

```bash
crystal build examples/remoting_example.cr -Dpreview_mt -Dexecution_context
```
