# burn_p2p Runtime Events And State Transitions

## Purpose

This document provides a human-readable taxonomy of the most important runtime transitions.

## Native runtime

High-level native flow:

1. bootstrap discovery and admission
2. directory and release-train verification
3. head sync
4. role-specific work:
   - serve/archive
   - train
   - validate
   - reduce/promote
5. receipt publication and persistence

Key event families:

- peer connection/disconnection
- pubsub/control announcements
- artifact/chunk requests and responses
- lag/catch-up transitions
- reducer assignment announcements
- head announcements
- receipt submission/acceptance

## Browser runtime

High-level browser flow:

1. portal capability discovery
2. login/enrollment
3. directory sync
4. head sync
5. transport connect
6. observer/verifier/trainer activation
7. receipt flush and acknowledgement

Browser state transitions:

- `PortalOnly` -> `Joining`
- `Joining` -> `Observer` / `Verifier` / `Trainer`
- active role -> `BackgroundSuspended`
- `BackgroundSuspended` -> `Catchup`
- `Catchup` -> active role or `Blocked`

## Failure-mode expectations

### Lag

- within safe lag bounds: catch up and continue
- beyond training-safe lag bounds: block lease renewal
- beyond rebase bounds: full rebase required

### Admission

- stale revocation epoch: reject
- wrong release train: reject
- wrong target artifact: reject
- scope mismatch: reject or portal-only downgrade

### Browser capability mismatch

- no WebGPU: downgrade trainer to verifier/observer
- insufficient memory or persistence: downgrade or block
- browser join disabled: portal-only

## Suggested future diagrams

If the repository adds generated diagrams later, they should cover:

- native training window
- validator merge and promotion
- browser join
- reenrollment after revocation epoch bump
