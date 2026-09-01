# Cluster Membership and Reachability

Movie cluster membership is a production-alpha extension layered on the normal actor and remoting runtime. It supplies static-seed discovery, process-incarnation identity, convergent membership, deterministic leadership, reachability observations, graceful leave, manual downing, typed events, and telemetry. It does not supply sharding, actor placement, distributed persistence ownership, automatic split-brain resolution, or durable application delivery.

## Start a cluster

Every node enables remoting first. Seed addresses include the actor-system name because association negotiation verifies it:

```crystal
seed_remote = seed_system.enable_remoting("127.0.0.1", 2551)
seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
  cluster_name: "orders",
  roles: ["seed", "backend"]
))

worker_system.enable_remoting("127.0.0.1", 2552)
worker = worker_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
  cluster_name: "orders",
  seed_nodes: [seed_remote.address],
  roles: ["worker"]
))
worker.await_up(10.seconds)
```

An empty seed list forms a one-node cluster immediately. A non-seed starts as `Joining` and retries every `join_retry_interval` until a configured seed returns membership containing that exact address and node UID as `Up`. Repeated `enable_cluster`, join, welcome, and gossip messages are idempotent.

## Identity and membership

`UniqueAddress` combines the advertised remoting `Address` with the random process node UID negotiated by associations. Every inbound cluster message must match the daemon path, address, and process UID authenticated by that remoting association. Reusing the same system/host/port after a restart creates a distinct incarnation. Membership records move monotonically through:

```text
Joining -> Up -> Leaving -> Exiting -> Removed
                  \-> Down -> Removed
```

`Removed` records remain as bounded tombstones, so delayed gossip cannot resurrect an old UID. `cluster.max-members` bounds live records plus tombstones and fails closed before a merge can partially overflow the state.

Each gossip round sends a canonical full-state snapshot to at most `gossip_fanout` peers. Member status precedence, revision, and change-origin ordering make merge deterministic. ACKs carry the receiver's deterministic digest; `cluster.converged?` becomes true when every locally reachable `Up` member has acknowledged the current digest. The lowest reachable `UniqueAddress` in `Up` or `Leaving` state is the local leader.

## Reachability is not downing

Cluster heartbeats run through the internal `/system/cluster` daemon over existing remoting connections. Elapsed silence uses a monotonic clock. A timeout adds the exact UID to the observer's local `snapshot.unreachable` set and can change its local leader view, but never changes the member's globally gossiped status.

Movie deliberately has no automatic downing policy. During a partition, both sides can be alive, and a timeout cannot identify the side allowed to continue. Resolve that ambiguity externally with deployment/quorum/lease knowledge, then invoke `down(unique_address)` on the current leader. Self-down is rejected because it would stop the leader before it could disseminate the terminal record. A down request sent to another leader requires `remoting.shared-secret`; without HMAC, invoke it locally on the leader. Downing the wrong side can create split-brain application behavior because this epic does not provide write fencing.

Use `remoting.shared-secret` on every non-isolated deployment. Association identity is always checked for consistency, but without the shared secret a network peer is not cryptographically authenticated.

## Graceful leave

Use a two-step shutdown so peers receive the tombstone before transport disappears:

```crystal
cluster.leave
cluster.await_removed(10.seconds)
system.shutdown
```

Calling `ActorSystem#shutdown` directly is an abrupt loss from the cluster's perspective. Peers will mark the UID unreachable and retain it until an operator downs it.

## Snapshots and events

`cluster.snapshot` returns defensive member and unreachable arrays plus the current leader. `Member#roles` is defensive as well. Subscribe a local typed actor to receive one initial state followed by effective transitions:

```crystal
events = system.spawn(MyClusterEventBehavior.new, name: "cluster-events")
cluster.subscribe(events)
# CurrentState, MemberJoined, MemberUp, MemberLeaving, MemberExiting,
# MemberDown, MemberRemoved, UnreachableMember, ReachableMember, LeaderChanged
cluster.unsubscribe(events)
```

Repeated gossip that does not change effective state emits no duplicate membership events.

`cluster.stats` is a local, non-blocking snapshot of join attempts, gossip rounds/messages/ACKs, membership merges, heartbeat traffic/timeouts/restorations, rejected protocol/capacity operations, and subscriber count.

## Configuration

Automatic startup requires both extensions:

```crystal
config = Movie::Config.builder
  .set("remoting.enabled", true)
  .set("remoting.host", "0.0.0.0")
  .set("remoting.port", 2551)
  .set("remoting.shared-secret", ENV["MOVIE_REMOTE_SECRET"])
  .set("cluster.enabled", true)
  .set("cluster.name", "orders")
  .set("cluster.seed-nodes", ["movie.tcp://seed@10.0.0.10:2551"])
  .set("cluster.roles", ["backend"])
  .build

system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, config)
cluster = system.cluster.not_nil!
```

See [configuration.md](configuration.md) for every default and environment mapping. Static seed addresses are the only discovery provider in this release.

## Verification and measurements

```bash
crystal spec spec/movie/cluster -Dpreview_mt -Dexecution_context
MOVIE_CLUSTER_STRESS=1 crystal spec spec/movie/cluster/stress_spec.cr \
  -Dpreview_mt -Dexecution_context
MOVIE_CLUSTER_BENCH=1 crystal spec --release spec/movie/cluster/benchmark_spec.cr \
  -Dpreview_mt -Dexecution_context
```

The stress executable re-execs itself into real seed/joiner processes and covers startup ordering, three-node convergence, idempotent join, `SIGSTOP` reachability and recovery, graceful leave, abrupt death, explicit downing, and same-address/new-UID restart. Benchmarks report measurements without host-dependent thresholds.
