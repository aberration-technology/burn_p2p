# burn_p2p Browser Runtime Guide

## Purpose

This guide explains the browser runtime states, storage model, transport expectations, and receipt
flow for `burn_p2p_browser`.

## Browser roles

The browser runtime can be configured for:

- `PortalViewer`
- `Observer`
- `Verifier`
- `TrainerWgpu`
- `Fallback`

Only browser-enabled revisions should ever promote into peer roles.

## Runtime states

The current high-level states are:

- `PortalOnly`
- `Joining`
- `Observer`
- `Verifier`
- `Trainer`
- `BackgroundSuspended`
- `Catchup`
- `Blocked`

### Joining stages

The runtime currently progresses through:

1. directory sync
2. head sync
3. transport connect
4. active peer role

Promotion should not skip head sync or transport readiness.

## Storage

The browser storage snapshot persists:

- peer identity and certificate material
- session state
- release-train snapshot
- directory snapshot
- selected experiment/revision
- cached heads and chunks
- cached shards
- receipt outbox and acknowledgements

## Receipt flow

1. verify/train work produces a `ContributionReceipt`
2. the worker queues it in the local outbox
3. the portal client flushes to `/receipts/browser`
4. accepted receipts are acknowledged back into browser storage

The browser must never treat local receipt generation as final acceptance.

## Suspension and resume

Expected flow:

1. active browser role enters `BackgroundSuspended`
2. resume promotes to `Catchup`
3. control-plane/head sync revalidates state
4. transport reconnect occurs if required
5. the runtime returns to `Observer`, `Verifier`, or `Trainer`

Suspension must not silently preserve a stale trainer state.

## Trainer downgrade policy

The downgrade order should remain:

1. trainer
2. verifier
3. observer
4. portal-only

Triggers for downgrade:

- WebGPU unavailable or unstable
- memory/persistence class below workload requirements
- repeated suspension or catch-up failure
- head lag beyond training-safe bounds
- browser join policy disabled for the revision

## Device classes

The browser runtime currently relies on explicit capability probes plus revision policy.

Important fields:

- WebGPU availability
- memory class
- persistence availability
- transport support
- estimated window budget
- shard and checkpoint budget

## Operational guidance

Use browser trainer mode only when:

- the revision is browser-enabled
- WebGPU is available
- the window budget is short enough to finish reliably
- the shard budget fits persistence and memory constraints

Otherwise prefer verifier or observer.

## What to verify in tests

- directory sync before role promotion
- head sync before transport-backed activation
- receipt outbox flush/ack path
- suspend/resume into catch-up
- blocked or portal-only downgrade when scopes or capability are insufficient
