# Epic 20 - Cluster Sharding and Persistent Entity Ownership

**Goal:** Add logical entity addressing and safe, pluggable shard placement above
cluster membership so one active owner handles each entity id across the cluster.

## Contract

- Partitioning and placement are separate policies:
  `EntityId -> ShardId -> UniqueAddress`.
- Entity partitioning is deterministic across processes and never uses the
  runtime `String#hash` implementation.
- Allocation supports least-loaded, rendezvous, weighted, and role-constrained
  strategies behind a public strategy interface.
- Rebalancing is independently configurable and rate limited.
- The lowest converged `Up` `UniqueAddress` coordinates one authenticated,
  generation-ordered authoritative plan; routing fails closed when no such
  coordinator exists.
- A logical entity reference routes through the current shard owner without
  exposing its physical actor path.
- Ordinary, event-sourced, and durable-state entities share the same sharding
  surface. Persistent entities use PostgreSQL when more than one cluster node
  can own them.
- One active owner exists for a `(entity type, shard id)` at a time. Persistent
  writes carry a database-validated fencing epoch, so a stale owner cannot
  commit after relocation.
- Reachability alone never reallocates a shard. Graceful leave or explicit
  downing changes eligibility; this preserves the cluster membership contract
  while automatic split-brain resolution remains deferred.

## Task 20.1 - Strategy contracts

- Add stable hash partitioning with a configurable positive shard count.
- Add least-loaded, rendezvous, weighted, and role-aware allocation.
- Add no-rebalance and bounded threshold/rate-limited rebalance policies.
- Prove determinism, balance, minimal movement, eligibility constraints, and
  move bounds with focused specs.

## Task 20.2 - Logical entity routing

- Add an Akka-style `ClusterSharding` extension id.
- Register entity types on every eligible node.
- Route `tell` and `ask` through a logical typed entity reference.
- Activate entities on demand and preserve per-entity ordering.
- Reject incompatible duplicate entity-type registrations.

## Task 20.3 - Ownership lifecycle

- Track active local entities by entity type and shard.
- Passivate idle or explicitly stopped entities without losing their logical
  address.
- Reconcile allocations after membership convergence.
- Drain the previous owner, prepare the next owner, and release coordinator-
  buffered deliveries in FIFO order before publishing completed ownership.
- Hand shards off on graceful leave and relocate only after a member is removed
  or explicitly downed.

## Task 20.4 - Persistent fencing

- Add a versioned shard-lease schema migration.
- Acquire and renew one lease per locally owned persistent shard.
- Carry the monotonically increasing fencing epoch on event, snapshot,
  retention, and durable-state mutations and validate it in the same
  transaction as the mutation.
- Stop local entities and reject routing after lease loss.
- Keep optimistic revisions and operation-id deduplication as independent
  safety layers.

## Task 20.5 - Operational evidence

- Add local and real-process tests for activation, remote routing, rebalance,
  graceful leave, abrupt loss, explicit downing, stale-owner fencing, and
  recovery on a new node.
- Expose allocation, activation, passivation, handoff, lease, and rejection
  telemetry.
- Document configuration, supported persistence backends, failure semantics,
  and benchmark commands.

## Deferred

- Automatic split-brain resolution and quorum downing.
- Replicated Event Sourcing, CRDT merge semantics, and active-active writers.
- Cross-data-center sharding.
- Minor internal cleanup: ordinary and persistent providers still duplicate
  entity lifecycle bookkeeping. A shared lifecycle registry is deferred until
  it can be extracted without mixing a broad refactor into the sharding
  correctness work.

## Completion checklist

- [x] Failing test written first
- [x] Failing test observed red
- [x] Minimal implementation written
- [x] Targeted verification green
- [x] Broader verification green
- [x] Formatting check green
- [x] Docs/examples updated if needed
- [x] Review requested
- [x] Review feedback addressed
