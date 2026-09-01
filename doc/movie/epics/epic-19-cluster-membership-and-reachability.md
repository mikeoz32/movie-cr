# Epic 19 - Cluster Membership and Reachability

**Goal:** Build an operable cluster-membership layer above reliable remoting associations without prematurely coupling membership to sharding, actor placement, or persistence ownership.

**Status:** Completed (2026-09-01).

**Fixed review point:** `bef87a6` (`docs(remoting): complete reliable associations epic`).

## Delivery contract

- Keep cluster protocol messages typed and carried by ordinary remoting actor delivery; do not add a second socket stack.
- Identify every member by advertised remoting address plus process node UID so a restarted process is a new incarnation.
- Use static seed nodes for discovery in this epic. DNS/service discovery providers remain follow-up work.
- Converge membership through versioned gossip and deterministic status precedence without resurrecting removed incarnations.
- Elect the lowest reachable `UniqueAddress` as leader using a total ordering shared by every node.
- Detect reachability with cluster heartbeats and monotonic elapsed time. Reachability alone must never remove a member.
- Default to explicit/manual downing. Movie must not claim split-brain safety until a quorum or lease-based policy exists.
- Preserve remoting's at-most-once user-message contract. Cluster membership does not make application delivery durable.
- Keep membership snapshots and events immutable from the caller's perspective and expose bounded operational counters.
- Prove join, convergence, graceful leave, abrupt loss, restart incarnation, and manual downing with real OS processes.

## Task 19.1 - Membership model and convergent merge

**Status:** Completed.

- Add `UniqueAddress`, member roles, ordered member statuses, versioned records, and immutable snapshots.
- Merge newer member records deterministically and retain `Removed` tombstones so stale gossip cannot resurrect a node incarnation.
- Derive leader and local reachability without mixing observer-local failure evidence into globally merged membership records.

## Task 19.2 - Cluster extension, daemon, and static-seed join

**Status:** Completed.

- Add a typed internal cluster daemon actor at one well-known path.
- Add idempotent `enable_cluster`, `cluster`, `join`, `leave`, and snapshot APIs.
- Join through any reachable static seed, return the current state, and promote joining members through the leader.
- Start and stop cluster after/before remoting respectively.

## Task 19.3 - Gossip convergence and deterministic leadership

**Status:** Completed.

- Periodically gossip bounded full-state snapshots to peers with per-node monotonic versions.
- Track convergence acknowledgements and expose rounds/messages/merge counters.
- Emit membership and leader-change events only when the effective snapshot changes.

## Task 19.4 - Reachability, graceful leave, and explicit downing

**Status:** Completed.

- Exchange heartbeat/ack traffic outside user actor mailboxes where practical, while retaining one remoting transport.
- Mark members unreachable/reachable from monotonic local observations without globally removing them.
- Complete graceful `Leaving -> Exiting -> Removed` transitions through the leader.
- Expose authenticated operator-driven downing; keep automatic downing disabled and document the split-brain boundary.

## Task 19.5 - Configuration, subscriptions, and observability

**Status:** Completed.

- Add canonical `cluster.*` settings and environment mappings.
- Add typed subscriptions for member, reachability, and leader events.
- Expose immutable state plus join/gossip/heartbeat/reachability counters without network IO in reads.

## Task 19.6 - Multi-process resilience evidence and documentation

**Status:** Completed.

- Cover seed startup order, idempotent join, three-node convergence, graceful leave, abrupt loss, manual downing, and same-address/new-UID restart in deterministic child-process tests.
- Add opt-in membership convergence and heartbeat measurement harnesses without host-dependent thresholds.
- Update README, remoting/configuration guides, examples, maturity boundaries, and backlog status.

## Explicitly deferred

- Cluster sharding, singleton actors, routers, distributed pub/sub, receptionist, and actor placement.
- Persistence-id ownership, entity relocation, passivation, and split-brain write fencing.
- Automatic downing, quorum/lease split-brain resolution, multi-data-center semantics, and rolling protocol upgrades beyond association capability negotiation.
- DNS, Kubernetes, cloud, or application-specific discovery providers.

## Completion checklist

- [x] Failing tests written first.
- [x] Failing tests observed red.
- [x] Minimal implementations written.
- [x] Targeted verification green.
- [x] Broader verification green.
- [x] Formatting check green.
- [x] Docs/examples updated.
- [x] Review requested.
- [x] Review feedback addressed.

## Completion evidence

- Cluster specs pass 19 examples; the cluster/configuration set passes 70 examples.
- The complete suite passes 342 examples on Crystal 1.21.0 and the minimum supported Crystal 1.19.1.
- Formatter and dependency checks pass, and all 10 examples build on both supported verification toolchains.
- The opt-in real-process stress scenario passes seed startup ordering, three-node convergence, reachability recovery, graceful leave, abrupt loss, explicit downing, and same-address/new-UID restart.
- The final release measurement converged 5 nodes in 85.33 ms, sent/received 94/93 gossip messages, and observed 472 heartbeat messages in 500.04 ms (944 msg/s aggregate). These are host observations, not thresholds.
- Review feedback added association-bound cluster identity, leader-authorized lifecycle gossip, acknowledged terminal leave delivery, typed `UniqueAddress` state keys, authenticated leader redirect, and complete multi-seed discovery. Final Standards and Spec reviews report zero remaining actionable findings against `bef87a6`.
