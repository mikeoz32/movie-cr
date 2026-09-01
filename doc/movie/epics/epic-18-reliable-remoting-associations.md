# Epic 18: Reliable Remoting Associations

**Goal:** Turn the remoting transport into a restart-tolerant, observable association layer with negotiated protocol identity, bounded reconnect behavior, reliable control delivery, failure detection, and deployable security hooks without slowing the user-message hot path with per-message acknowledgements.

**Depends on:** Epic 14.

**Fixed point:** `49768eb`.

**Status:** In progress.

**Delivery contract:**

- user `tell` remains ordered per stripe and at-most-once; messages are never replayed across a socket generation,
- remote `ask` fails promptly when the socket generation carrying it is lost,
- system/control messages are acknowledged, deduplicated, and retried within a bounded association buffer,
- a `RemoteActorRef` remains usable while its association reconnects,
- persistence outbox remains the supported durable business-delivery mechanism.

## Task 18.1: Versioned association handshake

**Status:** In progress.

- Give every actor-system process a stable runtime node UID and every socket a generation ID.
- Negotiate protocol version and capabilities before accepting application frames.
- Reject malformed, incompatible, wrong-system, and unauthenticated peers explicitly.

## Task 18.2: Reconnect lifecycle

**Status:** Planned.

- Model disconnected, connecting, handshaking, active, backoff, and stopped states.
- Reconnect with bounded exponential backoff and jitter while preserving existing remote refs.
- Bound disconnected behavior and expose association state/statistics.

## Task 18.3: Reliable control delivery

**Status:** Planned.

- Sequence and acknowledge system messages without acknowledging user traffic.
- Deduplicate retransmitted control frames and bound pending control state.
- Preserve per-stripe FIFO and fail explicitly when the control buffer is saturated.

## Task 18.4: Association cleanup

**Status:** Planned.

- Fail pending asks from a lost socket generation.
- Purge remote watch registrations owned by a closed inbound generation.
- Prevent stale replies and stale association generations from mutating current state.

## Task 18.5: Heartbeat and failure detection

**Status:** Planned.

- Exchange lightweight heartbeats outside the actor mailbox path.
- Close and reconnect associations whose peer is silent beyond the configured timeout.
- Expose last activity, reconnect, timeout, and protocol-failure counters.

## Task 18.6: Authentication and transport hooks

**Status:** Planned.

- Support a fail-closed shared-secret handshake authenticator without sending the secret on the wire.
- Expose client/server socket wrapping hooks so deployments can install TLS or another secured transport.
- Keep insecure plaintext operation explicit and backward-compatible for local development.

## Task 18.7: Chaos and performance evidence

**Status:** Planned.

- Cover peer restart, half-open/silent peer, ask interruption, and control retransmission with deterministic two-process tests.
- Add opt-in reconnect/control benchmarks and a configurable soak scenario.
- Report measurements without host-dependent pass/fail thresholds.

## Completion checklist

- [ ] Failing tests written first.
- [ ] Failing tests observed red.
- [ ] Minimal implementations written.
- [ ] Targeted verification green.
- [ ] Broader verification green.
- [ ] Formatting check green.
- [ ] Docs/examples updated.
- [ ] Review requested.
- [ ] Review feedback addressed.
