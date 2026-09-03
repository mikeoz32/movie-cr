# Epic 22 - Cluster Receptionist and Service Discovery

**Goal:** Add an Akka-style typed cluster receptionist that discovers actor
services across member incarnations without introducing remote deployment or
unsafe ownership decisions.

## Contract

- `ClusterReceptionist` is a separate actor-system extension above remoting and
  cluster membership.
- `ServiceKey(T)` binds one stable logical key to one wire message type.
- Applications may register and deregister local actor refs, synchronously find
  the current reachable listing, and subscribe local typed actors to listing
  changes.
- Registrations are replicated as full per-node, monotonically versioned state.
  Only the authenticated node incarnation may author its state.
- Duplicate registration is idempotent. Reusing a key with a different message
  type fails closed locally and incompatible remote state is rejected.
- Actor termination automatically removes every local registration for that
  actor.
- Listings contain only `Up`, locally reachable member incarnations. A service
  disappears during observed unreachability, may return after reachability is
  restored, and is purged after `Down` or `Removed`.
- Reusing the same host, port, and actor-system name with a new node UID never
  resurrects registrations owned by the old incarnation.
- Discovery does not guarantee delivery, replay messages, down members, elect
  owners, or replace cluster sharding.

## Task 22.1 - Typed API and local lifecycle

- Add `ServiceKey(T)`, `ServiceRef(T)`, and `ReceptionistListing(T)`.
- Add idempotent `register`, `deregister`, `find`, `subscribe`, and `unsubscribe`.
- Watch registered local actors and remove their services on termination.
- Return defensive, deterministically ordered listings.

## Task 22.2 - Authenticated replicated registry

- Add a dedicated receptionist daemon and protocol tag.
- Replicate complete per-node state with monotonic revisions and periodic
  anti-entropy.
- Validate cluster name, daemon path, transport address, process UID, entry
  ownership, capacity, and service-key compatibility before merging.
- Keep remote state replacement atomic and reject conflicting equal revisions.

## Task 22.3 - Membership and failure behavior

- Filter unreachable and non-`Up` owners from listings without mutating cluster
  membership.
- Restore listings when reachability returns.
- Purge terminal member incarnations and reject delayed state from them.
- Cover graceful leave, abrupt loss plus explicit downing, and same-address/new-
  UID restart.

## Task 22.4 - Operations and evidence

- Expose registration, deregistration, replication, listing, subscriber, purge,
  and rejection telemetry.
- Add deterministic in-process specs and an opt-in real-process stress scenario.
- Add a complete example and document when to choose receptionist, sharding, or
  singleton.

## Deferred

- Distributed pub/sub topics and delivery semantics.
- Cluster-aware group and pool routers.
- Automatic split-brain resolution or downing.
- Durable service registrations, remote actor deployment, and external service
  discovery providers.

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
