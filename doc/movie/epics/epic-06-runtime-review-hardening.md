# Epic 06: Runtime Review Hardening

**Status:** In progress
**Priority:** P0
**Depends on:** Epics 01-03
**Feature:** Make actor lifecycle, mailbox scheduling, ask cleanup, registries, and remoting failure paths deterministic under concurrency.

## Definition Of Done

- Every behavioral change has a regression spec written first and observed failing.
- Lifecycle transitions are explicit, invalid system-message orderings are ignored safely, and terminal actors are deregistered.
- Mailbox processing cannot wedge after a user or lifecycle callback raises; system messages cannot wait behind an unbounded user queue.
- Shutdown rejects new actors, does not deadlock when initiated from an actor, and stops the actor tree before extensions.
- Ask listeners and timers are cleaned up on reply, timeout, and target termination.
- Remoting closes failed connections, handles malformed protocol input inside the reader boundary, and never reports a failed bind as enabled.
- Duplicate paths and message tags fail atomically without leaving registry entries behind.
- A fresh full spec run, example build, diff check, and mandatory review pass are completed before merge.

## Tasks

### 06.1 Lifecycle and mailbox contract

- [x] Add lifecycle transition guards and mailbox exception-safe dispatch.
- [x] Prioritize system messages and discard queued user messages after stop.
- [x] Cover stop-during-start, restart ordering, lifecycle-hook exceptions, and actor-side shutdown.

### 06.2 Supervision semantics

- [x] Implement `RESUME` and correct `ESCALATE` propagation to the direct supervisor.
- [x] Clear restart counters when children terminate.
- [x] Add multi-level supervision specs for nested escalation propagation.

### 06.3 Ask and scheduler lifecycle

- [x] Stop ask listeners after `TargetTerminated`.
- [x] Make timer installation race-safe with early replies and termination.
- [x] Serialize lazy scheduler and dispatcher initialization.

### 06.4 Remoting failure paths

- [x] Close sockets and pending asks on disconnect, including malformed inbound protocol input.
- [x] Make pool state reflect disconnected stripes and close partial/racing candidates.
- [x] Reject failed extension startup and protect local delivery from invalid paths.
- [x] Preserve remote actor paths and avoid consuming local actor IDs.

### 06.5 Registry and public contract consistency

- [x] Reject duplicate normalized actor paths and custom message tags atomically.
- [x] Roll back actor registration when path registration fails.
- [x] Align lifecycle docs and stable API documentation with implementation.
- [x] Replace over-claimed remote stress specs with real delivery assertions.

## Workflow Gate

The project-wide TDD and mandatory post-development review rules in [development_workflow.md](../development_workflow.md) apply to every task in this epic. No task is complete based only on a passing happy-path spec; concurrency and failure-path behavior must be reviewed explicitly.
