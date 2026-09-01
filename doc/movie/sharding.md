# Cluster Sharding

[Documentation index](README.md) · [Cluster membership](cluster.md) · [Persistence](persistence.md)

Movie cluster sharding is a separate extension above remoting and cluster membership. It gives an entity a stable logical address while its physical actor is activated on demand on the node that currently owns the entity's shard:

```text
EntityType + EntityId -> EntityPartitioner -> ShardId
ShardId -> ShardAllocationStrategy -> UniqueAddress
UniqueAddress -> local activation or remote sharding daemon
```

Partitioning and placement are intentionally independent. Changing node placement does not change an entity's shard, and a logical `ShardedEntityRef(T)` remains usable after passivation or relocation.

## Ordinary entities

Enable remoting and cluster membership on every node, register concrete wire types on every node, then initialize the same entity type with identical settings:

```crystal
Movie::Remote::MessageRegistry.register(DeliverOrder)
Movie::Remote::MessageRegistry.register(OrderReply)

system.enable_remoting("0.0.0.0", 2551)
system.enable_cluster(cluster_settings)
sharding = Movie::ClusterSharding.get(system)

orders = sharding.init(
  "Order",
  OrderCommand,
  shard_count: 256,
  allocation: Movie::Cluster::RoleAwareAllocation.new(
    Movie::Cluster::RendezvousAllocation.new,
    ["backend"]
  ),
  rebalance: Movie::Cluster::RateLimitedRebalance.new(
    threshold: 1,
    max_concurrent: 4
  ),
  idle_timeout: 2.minutes
) do |entity_id|
  OrderBehavior.new(entity_id)
end

order = sharding.entity_ref_for(orders, "order-42")
order << DeliverOrder.new("parcel-7")
reply = order.ask(GetOrder.new, OrderReply, 3.seconds).await(3.seconds)
```

`init` is idempotent only when the entity name, message type, shard count, policies, lease settings, and idle timeout match. Every eligible owner must register the entity type before it can receive traffic. Command and reply structs used remotely must include `JSON::Serializable`; concrete command variants must be registered with `Movie::Remote::MessageRegistry` on every node.

Sharding envelopes use the remoting direct JSON writer and registered pull-parser decode path. Movie does not build a `JSON::Any` tree or an intermediate payload string on the normal registered-message path.

## Placement strategies

Movie ships several interchangeable policies:

| Policy | Intended use | Movement and balance |
|---|---|---|
| `StableHashPartitioner` | Default entity-to-shard mapping | Stable FNV-1a mapping across processes and restarts. Never uses randomized runtime `String#hash`. |
| `LeastLoadedAllocation` | Even shard counts on similar nodes | Deterministically fills the least-loaded eligible node. |
| `RendezvousAllocation` | Membership that changes often | Highest-random-weight placement; adding a node moves only shards won by that node. |
| `WeightedLeastLoadedAllocation` | Nodes with different capacity | Balances shard counts relative to a positive application-supplied weight. Give the strategy a stable `strategy_id` on every node. |
| `RoleAwareAllocation` | Dedicated entity hosts | Decorates another allocation strategy and requires all configured cluster roles. |
| `NoRebalance` | Stable placement until an owner leaves | Suppresses voluntary moves; shards still move off ineligible owners. |
| `RateLimitedRebalance` | Controlled convergence | Moves only `max_concurrent` shards per reconciliation step until the load threshold is met. |

Prefer rendezvous allocation when minimizing movement matters, least-loaded allocation for simple homogeneous pools, and weighted least-loaded allocation only when the capacity weights are stable and identically computed on every node. Wrap any of them in `RoleAwareAllocation` to keep entity actors off frontend or seed-only nodes.

The shard count is a compatibility boundary. Choose it above the expected node count so the allocator has enough units to balance, and do not change it in place for a live entity type without an explicit migration plan.

## Activation and passivation

An entity actor is created only when its first message reaches the owning node. `ref.send_system(Movie::STOP)` asks the current owner to passivate that entity; the next message activates a new actor through the same logical reference. `idle_timeout` enables automatic passivation. Graceful cluster leave removes the leaving node from allocation, passivates its actors, and routes later traffic to the new owner.

After membership converges, the lowest `UniqueAddress` among `Up` members acts as the sharding coordinator. It owns the authoritative allocation plan and synchronizes a coordinator term plus monotonically increasing plan generation to the other nodes. A newly elected coordinator first collects versioned snapshots and then publishes a higher generation; delayed updates cannot overwrite a newer plan. Callers route first through that coordinator, which either activates locally or forwards one hop to the selected owner. Routing fails closed while there is no converged coordinator; it does not guess an owner from a node-local view.

Ownership changes use an explicit handoff. The coordinator serializes routing with the ownership barrier, buffers up to 1,024 accepted deliveries per moving shard, asks the previous owner to drain already accepted mailbox work and passivate, prepares the next owner, forwards the handoff buffer in FIFO order, and only then publishes the completed plan generation. Forwarded delivery and control traffic must come from the coordinator identity authenticated by remoting, and the receiving node must already own or have prepared that shard. An `ask` forwarded by the coordinator remains a real remote ask, so success, failure, cancellation, and timeout propagate to the original caller. A three-node forwarding hop also preserves the original actor sender path. User delivery remains at-most-once because the underlying remoting contract does not replay disconnected traffic.

Transient lease or handoff admission failures are retried through one bounded FIFO barrier per shard on the authorized owner. Later accepted tells and asks join that queue instead of starting independent timer fibers, and ownership handoff waits for the queue to drain. This preserves entity ordering while a PostgreSQL lease expires or becomes available; exhausting the configured lease window rejects the remaining operation rather than replaying disconnected traffic.

## Persistent entities

Require the persistence entrypoint and register the persistent entity normally. Then initialize its sharded facade:

```crystal
require "movie/persistence/postgres"

event_sourcing = Movie::EventSourcing.get(system)
counter_type = event_sourcing.register_entity(Counter, CounterCommand) do |id, store|
  Counter.new(id.persistence_id, store)
end

sharded_counters = Movie::ClusterSharding.get(system).init_event_sourced(
  counter_type,
  shard_count: 256,
  allocation: Movie::Cluster::RoleAwareAllocation.new(
    Movie::Cluster::RendezvousAllocation.new,
    ["persistence"]
  ),
  lease_duration: 10.seconds,
  lease_renew_interval: 3.seconds,
  idle_timeout: 5.minutes
)
```

`init_event_sourced` and `init_durable_state` require the PostgreSQL backend. SQLite is deliberately rejected because its file-local ownership cannot fence writers on different nodes.

The low-level SQLite backend implements the lease-table contract so backend conformance and fencing behavior can be tested without an external service. That primitive is not a supported clustered-ownership mode and does not make a SQLite file safe to share across nodes.

Each locally owned persistent shard holds a PostgreSQL lease identified by cluster name, entity type, and shard id. Ownership transfer increments a monotonic epoch. Event appends, snapshot saves/deletes, event-retention deletes, and durable-state saves/deletes validate that epoch in the same transaction as the mutation. An actor holding an older epoch receives `StaleShardOwnerError` and cannot commit, even if it was paused and resumes after another node has recovered the entity.

Optimistic revisions and operation-id deduplication remain active in addition to fencing: revisions detect concurrent history, operation ids make ambiguous retries idempotent, and the shard epoch decides which cluster owner is authorized to write.

## Failure and partition semantics

Reachability is only an observation and never authorizes relocation. While membership is temporarily non-converged, new routes fail closed because there is no authoritative coordinator. When a persistent sharding provider sees an unreachable member, its local leases are no longer used, local persistent entities stop, and the database leases are allowed to expire rather than being released to a possibly active partition.

After an operator or deployment controller determines the surviving side, explicitly down the other `UniqueAddress` through the cluster leader. Once membership converges and the old database lease expires, the selected owner acquires a higher epoch and queued delivery retries can recover the entity there. Movie does not include automatic split-brain resolution, quorum voting, replicated event sourcing, CRDT merging, or active-active persistent writers.

A short lease reduces failover time but increases sensitivity to database pauses and scheduler stalls. Keep `lease_renew_interval` comfortably below `lease_duration`, monitor the database, and choose an ask timeout that includes the maximum expected lease-expiry handoff. A timed-out persistent command remains ambiguous; retry it with the same `OperationId`.

## Inspection and verification

`sharding.allocations(entity_type)` returns a defensive current shard-to-node plan, `sharding.local_entity_count` reports active local entities, and `sharding.handoffs_in_progress` plus `handoff_in_progress?` expose current transitions. `sharding.stats` exposes routes, local delivery, remote routes, forwarding, activation, passivation requests and outcomes, idle passivation, handoff/rebalance moves, lease acquisition/renewal/loss/retry, and rejected envelopes.

Run the focused correctness suites with:

```bash
crystal spec spec/movie/cluster/sharding_strategy_spec.cr \
  spec/movie/cluster/sharding_extension_spec.cr \
  -Dpreview_mt -Dexecution_context

MOVIE_POSTGRES_TEST_URL=postgres://... crystal spec \
  spec/movie/persistence_fencing_spec.cr \
  spec/movie/postgres_persistence_spec.cr \
  -Dpreview_mt -Dexecution_context

MOVIE_SHARDING_STRESS=1 MOVIE_POSTGRES_TEST_URL=postgres://... crystal spec \
  spec/movie/cluster/sharding_stress_spec.cr \
  -Dpreview_mt -Dexecution_context
```

Use the existing remoting and cluster benchmark/stress commands in the root README to measure the transport and membership layers below sharding. The complete ordinary-entity example is [`examples/cluster_sharding_example.cr`](../../examples/cluster_sharding_example.cr).
