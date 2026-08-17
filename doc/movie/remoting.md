# Remoting

Remoting is an experimental MVP. The current implementation is suitable for validating typed message delivery between two Movie actor systems, but it is not yet a production cluster transport.

## Supported Workflows

The current protocol supports:

- typed fire-and-forget user messages;
- typed remote ask request/response;
- sender path metadata for inbound user messages when the sender has a registered path;
- remote `Stop`, `Watch`, `Unwatch`, `Terminated`, and `Failed` system messages;
- TCP connections managed through striped connection pools;
- binding to port `0` for test or dynamically allocated local ports.

The protocol does not currently provide authentication, encryption, reconnection, delivery acknowledgements, protocol version negotiation, or durable delivery guarantees.

## Message Registration

Wire messages must implement `JSON::Serializable` and be registered before sending. Register the same message type on both actor systems:

```crystal
record Ping, sequence : Int32 do
  include JSON::Serializable
end

Movie::Remote::MessageRegistry.register(Ping)
```

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

The supported remote subset is deliberately explicit:

| Message | Meaning |
|---|---|
| `Stop` | Gracefully stop the remote target. |
| `Watch` | Register a remote watcher. The watcher must have a registered actor path. |
| `Unwatch` | Remove a remote watcher. |
| `Terminated` | Notify a remote watcher that an actor stopped. |
| `Failed` | Forward a failure notification with remote class and message metadata. |

`PreStart`, `PostStart`, `PreStop`, `PostStop`, `PreRestart`, `PostRestart`, `Restart`, and `Terminate` are not wire protocol messages. Sending an unsupported message through `RemoteActorRef#send_system` raises `RemoteUnsupportedSystemMessageError`; inbound unsupported messages are rejected and logged.

The high-level `ActorContext#watch(remote_ref)` API is not available because `ActorContext#watch` currently accepts local typed `ActorRef` values. Remote watch is exposed through the lower-level system-message API and should be used only when the watcher path is registered.

## Shutdown

Stop both actor systems after a remoting workflow:

```crystal
client.shutdown
server.shutdown
```

`ActorSystem#shutdown` stops registered extensions, closes remoting connections, stops actors, and is idempotent.

## Verification

Run the focused remoting specs:

```bash
crystal spec spec/movie/remote/integration_spec.cr spec/movie/remote/e2e_spec.cr \
  -Dpreview_mt -Dexecution_context
```

Build the example:

```bash
crystal build examples/remoting_example.cr -Dpreview_mt -Dexecution_context
```
