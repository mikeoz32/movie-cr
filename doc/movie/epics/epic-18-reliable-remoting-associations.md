# Epic 18: Reliable Remoting Associations

**Goal:** Turn the remoting transport into a restart-tolerant, observable association layer with negotiated protocol identity, bounded reconnect behavior, reliable control delivery, failure detection, and deployable security hooks without slowing the user-message hot path with per-message acknowledgements.

**Depends on:** Epic 14.

**Fixed point:** `49768eb`.

**Status:** Completed (2026-09-01).

**Delivery contract:**

- user `tell` remains ordered per stripe and at-most-once; messages are never replayed across a socket generation,
- remote `ask` fails promptly when the socket generation carrying it is lost,
- system/control messages are acknowledged, deduplicated, and retried within a bounded association buffer,
- a `RemoteActorRef` remains usable while its association reconnects,
- persistence outbox remains the supported durable business-delivery mechanism.

## Task 18.1: Versioned association handshake

**Status:** Completed.

- Give every actor-system process a stable runtime node UID and every socket a generation ID.
- Negotiate protocol version and capabilities before accepting application frames.
- Reject malformed, incompatible, wrong-system, and unauthenticated peers explicitly.

## Task 18.2: Reconnect lifecycle

**Status:** Completed.

- Model disconnected, connecting, handshaking, active, backoff, and stopped states.
- Reconnect with bounded exponential backoff and jitter while preserving existing remote refs.
- Bound disconnected behavior and expose association state/statistics.

## Task 18.3: Reliable control delivery

**Status:** Completed.

- Sequence and acknowledge system messages without acknowledging user traffic.
- Deduplicate retransmitted control frames and bound pending control state.
- Preserve per-stripe FIFO and fail explicitly when the control buffer is saturated.

## Task 18.4: Association cleanup

**Status:** Completed.

- Fail pending asks from a lost socket generation.
- Purge remote watch registrations owned by a closed inbound generation.
- Prevent stale replies and stale association generations from mutating current state.

## Task 18.5: Heartbeat and failure detection

**Status:** Completed.

- Exchange lightweight heartbeats outside the actor mailbox path.
- Close and reconnect associations whose peer is silent beyond the configured timeout.
- Expose last activity, reconnect, timeout, and protocol-failure counters.

## Task 18.6: Authentication and transport hooks

**Status:** Completed.

- Support a fail-closed shared-secret handshake authenticator without sending the secret on the wire.
- Expose client/server socket wrapping hooks so deployments can install TLS or another secured transport.
- Keep insecure plaintext operation explicit and backward-compatible for local development.

## Task 18.7: Chaos and performance evidence

**Status:** Completed.

- Cover peer restart, half-open/silent peer, ask interruption, and control retransmission with deterministic two-process tests.
- Add opt-in reconnect/control benchmarks and a configurable soak scenario.
- Report measurements without host-dependent pass/fail thresholds.

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

## Completion notes

- The three-flight handshake negotiates protocol/capabilities and uses a fresh server challenge for replay-resistant optional HMAC authentication. Client/server `IO` hooks keep TLS certificate policy outside Movie.
- Logical connections survive socket loss with bounded exponential backoff, hard-capped jitter, monotonic heartbeat timeout detection, generation-safe ask failure, and observable counters. A stopped connection remains terminal under reconnect races.
- User messages remain at-most-once. Outbound and reverse lifecycle control traffic uses bounded sequence/ACK/dedup state; a changed peer node UID rebases pending control onto a new stream, while configurable dedup capacity fails closed without eviction and exposes metrics plus explicit retired-node reclamation.
- Inbound watch ownership is generation-scoped and disconnect cleanup sends local compensating `Unwatch` messages.
- The opt-in stress harness uses separate OS processes to cover silent-process detection, interrupted asks, a deliberately lost control ACK with same-sequence retransmission, control rebase after peer restart, and reuse of one remote ref. The release benchmark reports acknowledged control throughput and peer-restart reconnect latency without thresholds.
- The first implementation commit grouped tasks 18.1-18.6 more broadly than the repository's preferred one-task commit shape. Review-driven protocol, concurrency, and decomposition corrections are isolated in follow-up commits; future distributed epics should retain task-sized commits from the start.
- Final verification passes 321 examples on both Crystal 1.21 and the minimum supported Crystal 1.19.1, 162 focused remoting/config examples, 13 opt-in stress examples, formatting/dependency checks, and all 9 examples. Three final host observations at 1,000 acknowledged controls ranged from 25,713 to 35,772 control messages/s (median 30,216); the last three-restart run measured reconnect p50 10.34ms and p95 10.39ms. These are measurements, not thresholds.
- The final two-axis review against the complete epic diff reports zero Standards findings and zero Spec findings.
