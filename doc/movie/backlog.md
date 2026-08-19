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
| P1 | [Epic 04 - Configuration API Consistency](epics/epic-04-configuration-api-consistency.md) | Removes ambiguous config behavior and normalizes public settings. | Ready |
| P1 | [Epic 05 - Quality Gates and Documentation Accuracy](epics/epic-05-quality-gates-and-documentation.md) | Makes regressions harder to reintroduce and aligns docs with reality. | Ready |

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
