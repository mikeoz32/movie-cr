# Epic 03: Remoting Delivery and Protocol Completion

**Goal:** Turn remoting from partially implemented infrastructure into a feature that either works end-to-end or is explicitly downgraded in public docs until it does.

**Why this epic exists:** Remote refs can be created, but inbound user delivery, ask response wiring, and system-message delivery are incomplete. Current docs and examples overstate maturity.

**Depends on:** Epic 01 and Epic 02.

**Status:** Completed on 2026-08-17 as an experimental MVP.

**Done when:**

- a remote actor can receive user messages end-to-end,
- remote ask works with real request/response wiring,
- supported remote system messages are delivered correctly or explicitly unsupported,
- remoting examples and README match actual behavior.

## Task 03.1: Add true end-to-end remoting spec

**Files**

- Modify: `spec/movie/remote/integration_spec.cr`
- Add: `spec/movie/remote/e2e_spec.cr`

**Outcome**

- Create a two-system test that proves a remote actor actually receives a message.
- Add a second test for remote ask once the protocol exists.

**Verification**

- Run targeted remote integration specs.
- Run full movie spec suite.

## Task 03.2: Implement typed inbound user-message delivery

**Files**

- Modify: `src/movie/context.cr`
- Modify: `src/movie/remote/extension.cr`
- Modify: `src/movie/remote/message_registry.cr` if needed

**Outcome**

- Add a safe type-erased bridge from deserialized remote payloads into local typed actor mailboxes.
- Preserve sender-path metadata when available.

**Verification**

- Start with the failing E2E spec from Task 03.1.
- Run targeted remote specs and full suite.

## Task 03.3: Implement remote ask request/response flow

**Files**

- Modify: `src/movie/remote/extension.cr`
- Modify: `src/movie/remote/remote_actor_ref.cr`
- Modify: `src/movie/ask.cr` if protocol glue requires it
- Modify: `spec/movie/remote/e2e_spec.cr`

**Outcome**

- Correlate remote ask requests with remote ask responses.
- Ensure timeouts and connection-closed paths complete the caller future correctly.

**Verification**

- Add failing remote ask tests first.
- Run targeted remote specs and full suite.

## Task 03.4: Implement or explicitly constrain remote system messages

**Files**

- Modify: `src/movie/remote/extension.cr`
- Modify: `src/movie/remote/remote_actor_ref.cr`
- Modify: `src/movie/system.cr` if protocol support changes
- Modify: `spec/movie/remote/e2e_spec.cr`

**Outcome**

- Support the remote subset that is actually needed, at minimum stop and any watch-related messages that remoting advertises.
- If some system messages remain unsupported, fail loudly and document the limit.

**Verification**

- Add failing tests for the supported subset.
- Run targeted remote specs and full suite.

## Task 03.5: Reconcile public docs with implementation

**Status:** Done.

**Files**

- Modify: `README.md`
- Modify: `examples/remoting_example.cr`
- Add or modify: `doc/movie/remoting.md`

**Outcome**

- If remoting is complete, document supported workflows and caveats.
- If not complete, clearly label it as experimental and trim the example accordingly.

**Verification**

- Build `examples/remoting_example.cr`.
- Run the specific remote E2E smoke command used during implementation.
