# Movie Recovery Backlog

This backlog converts the current code review findings into an execution order that is practical for incremental delivery.

Mandatory workflow for every task lives in [development_workflow.md](development_workflow.md).

## Execution Order

| Priority | Epic | Why first | Status |
|---|---|---|---|
| P0 | [Epic 01 - Actor Runtime and Lifecycle Hardening](epics/epic-01-actor-runtime-and-lifecycle.md) | Fixes core runtime correctness and failure semantics. | Completed (2026-05-24) |
| P0 | [Epic 02 - Async Primitives and Executor Hardening](epics/epic-02-async-primitives-and-executor.md) | Stabilizes timeouts, futures, scheduler, and executor before protocol work. | Completed (2026-08-17) |
| P0 | [Epic 03 - Remoting Delivery and Protocol Completion](epics/epic-03-remoting-delivery-and-protocol.md) | Delivers the documented experimental remoting MVP and its supported protocol subset. | Completed (2026-08-17) |
| P0 | [Epic 06 - Runtime Review Hardening](epics/epic-06-runtime-review-hardening.md) | Closes the remaining lifecycle, mailbox, ask, registry, and remoting correctness blockers found during pre-merge review. | Completed (2026-08-19) |
| P1 | [Epic 04 - Configuration API Consistency](epics/epic-04-configuration-api-consistency.md) | Removes ambiguous config behavior and normalizes public settings. | Completed (2026-08-30) |
| P1 | [Epic 05 - Quality Gates and Documentation Accuracy](epics/epic-05-quality-gates-and-documentation.md) | Makes regressions harder to reintroduce and aligns docs with reality. | Completed (2026-08-30) |
| P0 | [Epic 07 - Stream Blueprints, Boundedness, and TestKit](epics/epic-07-stream-blueprints-boundedness-and-testkit.md) | Establishes the typed, bounded stream core required before graph junctions and a Graph DSL. | Done |
| P1 | [Epic 08 - Actor System Performance Benchmarks](epics/epic-08-actor-system-performance-benchmarks.md) | Establishes comparable local and remote end-to-end measurements before runtime performance tuning. | Completed (2026-08-30) |
| P1 | [Epic 09 - Local Ask Hot-Path Optimization](epics/epic-09-local-ask-hot-path.md) | Removes the highest-impact actor lifecycle bottleneck identified by the end-to-end benchmark. | Completed (2026-08-30) |
| P1 | [Epic 10 - IO-Native Serialization](epics/epic-10-io-native-serialization.md) | Writes remoting and machine-readable output directly to reusable IO buffers to minimize intermediate JSON allocations. | Completed (2026-08-30) |
| P1 | [Epic 11 - Inbound Remoting Hot-Path Optimization](epics/epic-11-inbound-remoting-hot-path.md) | Removes the remaining payload rebuild and second JSON parse from registered inbound messages. | Completed (2026-08-30) |
| P1 | [Epic 12 - Inbound Envelope Allocation Reduction](epics/epic-12-inbound-envelope-allocation.md) | Measures and removes the next dominant parser/input or decoded-wrapper allocation boundary. | Completed (2026-08-30) |
| P0 | [Epic 13 - Batched Remoting Transport](epics/epic-13-batched-remoting-transport.md) | Removes synchronous per-message socket IO and measures the remaining receiver path before cluster work. | Completed (2026-08-30) |
| P0 | [Epic 14 - Receiver Handoff Contention](epics/epic-14-receiver-handoff-contention.md) | Measures and removes receiver-side lock and scheduling amplification before changing the wire codec. | Completed (2026-08-30) |

## Planning Rules

- One epic equals one feature area.
- Every task must be small enough to complete with a single TDD cycle.
- No task is complete until:
  - the targeted test was written first and observed failing,
  - the relevant verification commands were run fresh,
  - a review pass was requested and actionable feedback addressed.

## Current Strengths To Preserve

- Typed actor model and public API shape are coherent.
- Persistence primitives are usable and already have meaningful tests.
- Streams documentation and examples are ahead of the rest of the project.
- Remote/path/frame abstractions are good enough to build on instead of rewriting.

## Deferred Work

These items are intentionally not in the first recovery wave:

- New stream operators beyond current MVP.
- New persistence features beyond stabilization and documentation.
- Performance tuning before correctness gaps are closed.
