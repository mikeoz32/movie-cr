# Epic 21 - Cluster Singleton

**Goal:** Add an Akka-style cluster singleton extension that keeps one eagerly
active logical actor on one eligible cluster node and exposes a stable typed
proxy from every node.

## Contract

- `ClusterSingleton` is a separate actor-system extension composed above
  cluster sharding instead of duplicating membership, routing, or handoff code.
- A singleton registration has one logical name, one message type, one fixed
  entity id, and compatible settings on every participating node. Message type
  identity is part of the distributed sharding compatibility key.
- The current owner remains stable while it is eligible. Initial placement and
  replacement select the lowest eligible `Up` `UniqueAddress` deterministically.
- Optional role constraints limit eligible owners without limiting proxy use.
- The singleton is activated eagerly, remains addressable through a typed
  proxy, and is reactivated after termination or ownership relocation.
- Graceful leave drains accepted mailbox work before the new owner activates.
- Reachability alone never moves an ordinary singleton. Explicit downing or
  completed removal changes eligibility, matching the cluster contract.
- Event-sourced and durable-state singleton variants use PostgreSQL shard-lease
  fencing so a stale owner cannot commit after handoff.
- Proxy routing remains at-most-once across disconnected transport, exactly as
  documented for remoting and sharding.

## Task 21.1 - Singleton API and placement

- Add an Akka-style `ClusterSingleton` extension id and typed
  `ClusterSingletonRef(T)`.
- Add deterministic sticky single-owner allocation with optional role filters.
- Reject empty names and incompatible duplicate registrations.
- Expose current owner and local-owner inspection.

## Task 21.2 - Eager lifecycle and proxy routing

- Eagerly activate the singleton without requiring an application message.
- Route `tell`, `ask`, and explicit stop through the stable proxy.
- Reactivate after an intentional stop while ownership remains local.
- Preserve sender identity and per-singleton ordering through remote routing.

## Task 21.3 - Handoff and failure behavior

- Reuse the sharding ownership barrier for graceful leave and explicit downing.
- Start the replacement only after the previous owner drains and stops.
- Fail closed while membership has no converged coordinator.
- Add real-process evidence for eager startup, remote proxy routing, graceful
  handoff, abrupt loss, explicit downing, and same-name recovery.

## Task 21.4 - Persistent singleton fencing

- Add event-sourced and durable-state singleton registration helpers.
- Reuse PostgreSQL shard leases and transactionally validated fencing epochs.
- Prove that a paused stale singleton cannot commit after ownership transfer.
- Keep SQLite rejected for clustered persistent singleton ownership.

## Task 21.5 - Operations and documentation

- Expose registration, activation, reactivation, routing, handoff, lease, and
  rejection telemetry.
- Document role placement, failure semantics, PostgreSQL requirements, and the
  relationship between singleton and sharding.
- Add a complete ordinary singleton example and opt-in stress command.

## Deferred

- Automatic split-brain resolution and quorum/lease downing policy.
- Multi-data-center singleton replication.
- Exactly-once or durable proxy delivery.
- Cron/scheduling policy above the singleton actor itself.

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
