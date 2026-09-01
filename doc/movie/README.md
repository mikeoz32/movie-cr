# Movie Docs

## Core Docs

- [Actor lifecycle architecture](actor_lifecycle.md)
- [Configuration schema and semantics](configuration.md)
- [Persistence API and recovery](persistence.md)
- [Streams protocol and DSL notes](streams.md)
- [Remoting MVP and protocol limits](remoting.md)
- [Development workflow](development_workflow.md)
- [Recovery backlog](backlog.md)

## Epics

- [Epic 01 - Actor runtime and lifecycle hardening](epics/epic-01-actor-runtime-and-lifecycle.md)
- [Epic 02 - Async primitives and executor hardening](epics/epic-02-async-primitives-and-executor.md)
- [Epic 03 - Remoting delivery and protocol completion](epics/epic-03-remoting-delivery-and-protocol.md)
- [Epic 04 - Configuration API consistency](epics/epic-04-configuration-api-consistency.md)
- [Epic 05 - Quality gates and documentation accuracy](epics/epic-05-quality-gates-and-documentation.md)
- [Epic 06 - Runtime review hardening](epics/epic-06-runtime-review-hardening.md)
- [Epic 07 - Stream blueprints, boundedness, and TestKit](epics/epic-07-stream-blueprints-boundedness-and-testkit.md)
- [Epic 08 - Actor system performance benchmarks](epics/epic-08-actor-system-performance-benchmarks.md)
- [Epic 09 - Local ask hot-path optimization](epics/epic-09-local-ask-hot-path.md)
- [Epic 10 - IO-native serialization](epics/epic-10-io-native-serialization.md)
- [Epic 11 - Inbound remoting hot-path optimization](epics/epic-11-inbound-remoting-hot-path.md)
- [Epic 12 - Inbound envelope allocation reduction](epics/epic-12-inbound-envelope-allocation.md)
- [Epic 13 - Batched remoting transport](epics/epic-13-batched-remoting-transport.md)
- [Epic 14 - Receiver handoff contention](epics/epic-14-receiver-handoff-contention.md)
- [Epic 15 - Persistence contract and recovery](epics/epic-15-persistence-contract-and-recovery.md)
- [Epic 16 - Pluggable persistence and PostgreSQL](epics/epic-16-pluggable-persistence-and-postgresql.md)
- [Epic 17 - Persistence production hardening](epics/epic-17-persistence-production-hardening.md)
- [Epic 18 - Reliable remoting associations](epics/epic-18-reliable-remoting-associations.md)

## How To Use This Folder

1. Start with [backlog.md](backlog.md) for execution order.
2. Follow [development_workflow.md](development_workflow.md) for TDD, verification, and review gates.
3. Pick the active epic and execute one task at a time.
