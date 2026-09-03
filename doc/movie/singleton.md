# Cluster Singleton

[Documentation index](README.md) · [Cluster membership](cluster.md) · [Cluster sharding](sharding.md) · [Persistence](persistence.md)

`Movie::ClusterSingleton` is a separate Akka-style actor-system extension built above cluster sharding. It keeps one logical actor eagerly active on one eligible `Up` member and returns a stable typed `ClusterSingletonRef(T)` from every participating node.

Use a singleton for cluster-wide coordination such as a leader-only poller, control-plane worker, migration coordinator, or scheduler. Use [cluster sharding](sharding.md) when there are many independently addressable entity ids. A singleton is not replicated state and should not carry ordinary horizontally scalable traffic.

## Ordinary singleton

Register every concrete wire message and initialize the same logical name, message type, roles, and timing settings on every node:

```crystal
Movie::Remote::MessageRegistry.register(RunJob)
Movie::Remote::MessageRegistry.register(JobReply)

system.enable_remoting("0.0.0.0", 2551)
system.enable_cluster(cluster_settings)

singletons = Movie::ClusterSingleton.get(system)
jobs = singletons.init(
  "jobs",
  RunJob,
  roles: ["singleton"],
  activation_interval: 250.milliseconds,
  activation_timeout: 2.seconds
) do
  JobCoordinator.new
end

jobs << RunJob.new("daily")
reply = jobs.ask(RunJob.new("status"), JobReply, 3.seconds).await(3.seconds)
```

The fixed singleton entity is allocated with sticky `NoRebalance` placement. The lowest eligible `UniqueAddress` owns the initial instance; adding a lower-sorted member does not move a healthy singleton. Roles constrain ownership only—the proxy remains usable from nodes without those roles. If no eligible member exists, no actor runs and new asks fail with `NoShardOwnerError`.

`init` is idempotent on one actor system when all settings match and rejects incompatible reuse with `ClusterSingletonConfigurationError`. The internal singleton partitioner embeds the backing mode, persistence entity type when present, wire message type, entity id, timing settings, and lease settings in one canonical distributed compatibility key. Nodes that disagree therefore fail closed before messages can reach a wrongly typed actor.

## Lifecycle and delivery

The current sharding coordinator sends bounded activation checks, so the first actor starts without an application message and a deliberately stopped instance is recreated behind the same proxy. `ref.send_system(Movie::STOP)` requests passivation and immediate reactivation; it does not permanently disable the singleton.

On graceful leave, cluster departure waits for the sharding handoff barrier. Already accepted mailbox work drains on the previous owner, later accepted traffic is buffered in FIFO order, and the replacement is prepared before the new plan is published. If the final role-eligible member leaves, the actor drains and stops, the allocation becomes empty, and departure still completes.

User delivery remains at-most-once. A disconnected tell is not replayed, and an ask timeout is ambiguous if the command may already have reached the actor. Reachability alone never relocates a singleton. After an external controller chooses the surviving partition, explicitly down the exact stale `UniqueAddress`; Movie intentionally has no automatic split-brain resolution.

For an ordinary singleton, safety after manual downing depends on choosing the correct surviving side because an isolated old process cannot be fenced by memory alone. Use a persistent singleton when stale writes must be rejected independently by shared storage.

## Persistent singleton

Persistent helpers live in the optional persistence entrypoint and require PostgreSQL:

```crystal
require "movie/persistence/postgres"

events = Movie::EventSourcing.get(system)
counter_type = events.register_entity(Counter, CounterCommand) do |id, store|
  Counter.new(id.persistence_id, store)
end

counter = Movie::ClusterSingleton.get(system).init_event_sourced(
  "cluster-counter",
  counter_type,
  entity_id: "cluster-counter",
  roles: ["persistence"],
  lease_duration: 10.seconds,
  lease_renew_interval: 3.seconds
)
```

`init_event_sourced` and `init_durable_state` expose the same proxy and placement contract. Eager activation creates the persistent actor and acquires its shard-zero PostgreSQL lease. The existing persistence behavior loads state when its first command is processed. SQLite is rejected for clustered ownership.

Every event, snapshot, retention, and durable-state mutation carries the acquired fencing epoch and validates it in the same PostgreSQL transaction as the write. After relocation, a paused actor with an older epoch receives `Persistence::StaleShardOwnerError` and cannot change state. Operation ids and optimistic revisions remain independent safeguards; retry an ambiguous command with the same `OperationId`.

## Inspection and telemetry

The proxy exposes `owner` and `locally_owned?`. The extension exposes `owner(name)`, `handoff_in_progress?(name)`, `stats`, and `last_activation_error(name)`.

`SingletonStats` reports process-local registrations, activation attempts, first actor creations, repeated actor creations observed by the same extension instance, activation failures, proxy tells/asks/stops, known synchronous or asynchronous routing rejections, and observed owner changes. The `shared_sharding_*` handoff, rejected-envelope, and PostgreSQL lease fields deliberately expose the actor system's underlying sharding totals; their names make clear that unrelated sharded entities registered in the same actor system contribute to them. `handoff_in_progress?(name)` remains the per-singleton live check.

Watch repeated activation failures and `last_activation_error` together. A persistent replacement may report `ShardLeaseUnavailableError` until the previous lease expires; no actor is admitted under an unowned epoch.

## Verification

```bash
crystal spec spec/movie/cluster/singleton_spec.cr \
  -Dpreview_mt -Dexecution_context

MOVIE_POSTGRES_TEST_URL=postgres://... crystal spec \
  spec/movie/postgres_persistence_spec.cr \
  -Dpreview_mt -Dexecution_context --example "singleton"

MOVIE_SINGLETON_STRESS=1 crystal spec \
  spec/movie/cluster/singleton_stress_spec.cr \
  -Dpreview_mt -Dexecution_context
```

The opt-in multi-process stress scenario proves eager startup, remote proxy routing, graceful handoff, same-system-name restart with a new UID, abrupt owner loss, explicit downing, and recovery on the surviving process. See [`examples/cluster_singleton_example.cr`](../../examples/cluster_singleton_example.cr) for a runnable ordinary singleton.
