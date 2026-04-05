# Architecture

This document is the durable architecture baseline for the current
`burn_p2p` workspace. The code remains the source of truth; this file names the
crate boundaries and the intended layering.

## Layer model

The workspace is organized in five layers.

### 1. Canonical schema layer

- `burn_p2p_core`

This layer owns:

- IDs
- canonical manifests
- runtime wire types
- metrics schemas
- publication schemas
- edge/browser/service capability schemas

Nothing below this layer should depend upward.

### 2. Domain boundary layer

- `burn_p2p_experiment`
- `burn_p2p_checkpoint`
- `burn_p2p_dataloader`
- `burn_p2p_limits`
- `burn_p2p_security`
- `burn_p2p_swarm`
- `burn_p2p_engine`

These crates are the main domain seams:

- experiment specs, directory policy, reducer topology
- checkpoint CAS, lineage, sync, and merge planning
- dataset registration, shard planning, lease planning, caching
- capability calibration, role recommendation, budget sizing
- identity, release policy, admission, reputation, validator evidence
- transport shells, overlay topics, control-plane snapshots, live events
- Burn-specific module inspection, merge, and artifact materialization

### 3. Public runtime facade

- `burn_p2p`

This is the downstream entrypoint. It composes the lower boundary crates into
the user-facing runtime path:

- `P2pWorkload`
- `P2pProjectFamily`
- `NodeBuilder`
- `RunningNode`

### 4. Optional service and product surfaces

- `burn_p2p_metrics`
- `burn_p2p_publish`
- `burn_p2p_social`
- `burn_p2p_app`
- `burn_p2p_browser`
- auth connector crates:
  - `burn_p2p_auth_external`
  - `burn_p2p_auth_github`
  - `burn_p2p_auth_oidc`
  - `burn_p2p_auth_oauth`
- `burn_p2p_views`

These are optional read-model, operator, browser, or product-facing
companions. They should stay off the training-critical path.

### 5. Deployment and verification layer

- `burn_p2p_bootstrap`
- `burn_p2p_testkit`

`burn_p2p_bootstrap` is the main composition root for deployment-facing
services. `burn_p2p_testkit` spans the workspace to provide simulation,
browser-path, deployment-profile, and multiprocess verification.

## Crate inventory

| Crate | Publish | Purpose | Main internal deps |
| --- | --- | --- | --- |
| `burn_p2p_core` | yes | canonical IDs, schema, encoding | - |
| `burn_p2p_experiment` | yes | study/experiment/revision/control/topology | `burn_p2p_core` |
| `burn_p2p_checkpoint` | yes | artifact CAS, checkpoint lineage, sync/merge | `burn_p2p_core`, `burn_p2p_experiment` |
| `burn_p2p_dataloader` | yes | microshards, leases, cache, fetch | `burn_p2p_core`, `burn_p2p_experiment` |
| `burn_p2p_limits` | yes | capability probes and runtime budgeting | `burn_p2p_core` |
| `burn_p2p_security` | yes | auth, admission, release policy, reputation | `burn_p2p_core`, `burn_p2p_experiment` |
| `burn_p2p_swarm` | yes | transport shells and control-plane overlays | `burn_p2p_core`, `burn_p2p_experiment` |
| `burn_p2p_engine` | yes | Burn-specific merge and artifact helpers | `burn_p2p_checkpoint`, `burn_p2p_core`, `burn_p2p_experiment` |
| `burn_p2p` | yes | stable public facade | domain boundary crates above |
| `burn_p2p_metrics` | no | metrics indexer and read model | `burn_p2p_core` |
| `burn_p2p_publish` | no | artifact aliasing/export/download | `burn_p2p_checkpoint`, `burn_p2p_core` |
| `burn_p2p_social` | no | leaderboard/profile/badge services | `burn_p2p_core` |
| `burn_p2p_app` | no | reference HTML rendering surface | - |
| `burn_p2p_browser` | no | browser client, worker, and portal bindings | `burn_p2p`, `burn_p2p_core`, `burn_p2p_swarm`, optional service crates |
| `burn_p2p_views` | no | typed presentation/view contracts | `burn_p2p_core` plus some higher-level deps today |
| `burn_p2p_bootstrap` | no | deployment composition root | runtime + optional services |
| `burn_p2p_testkit` | no | simulation and regression harness | nearly whole workspace |

## Primary end-to-end flows

### Runtime bring-up

1. Downstream code implements `P2pWorkload`.
2. One or more workloads are grouped in a `P2pProjectFamily`.
3. `NodeBuilder` binds the selected family/workload to storage, identity,
   release, bootstrap peers, and auth.
4. `RunningNode` executes training and validation windows over the swarm
   control plane.

### Training

1. Runtime restores state and resolves lag.
2. Capability probe and budget derive window sizing.
3. Dataloader plans microshards and leases.
4. Workload executes one lease-scoped training window.
5. Checkpoint artifacts and contribution receipts are materialized.
6. Metrics and announcements are emitted.

### Validation and promotion

1. Reducer/validator windows are resolved from control-plane state.
2. Candidate artifacts are synchronized from peers.
3. Candidate models are evaluated and merged.
4. Merge, reduction, and contribution certificates are emitted.
5. Canonical heads advance through validator-backed promotion.

### Metrics

1. Runtime emits `PeerWindowMetrics`, `ReducerCohortMetrics`,
   `HeadEvalReport`, and evaluation protocol manifests.
2. `burn_p2p_metrics` ingests and persists the raw metric envelopes.
3. Derived rollups, snapshots, and live events feed bootstrap and browser flows.

### Artifact publication

1. Canonical heads and eval reports generate latest and best-val alias families.
2. `burn_p2p_publish` materializes export jobs and publication records.
3. Bootstrap exposes artifact query, export, and download endpoints.

### Browser edge

1. Bootstrap exposes directory, portal, transport, trust, metrics, and artifact
   surfaces suitable for browser peers.
2. `burn_p2p_browser` authenticates, enrolls, syncs, and drives worker state.

## Intentional invariants

The current architecture assumes:

- one network equals one trust domain
- one network equals one pinned project family and release train
- checkpoint CAS is canonical
- validator promotion is the path to canonical heads
- training is async and windowed rather than globally synchronous
- optional metrics/publication/social/browser surfaces remain non-consensus
  services

## Current structural debt

These are known architectural cleanup targets, not bugs:

1. Dense service crates
   - `burn_p2p_bootstrap` has started this split with dedicated admin, browser-edge, daemon, deploy, history, metrics, portal, publication, and state modules, and the bootstrap binary now has dedicated auth-state, browser-surface, key-material, synthetic-dataset, capability-manifest, artifact-route, metrics-route, get-route, post-route, portal-state, and trust-state modules; the remaining cleanup is smaller feature/service glue rather than a single binary hotspot
   - `burn_p2p_metrics` now has dedicated derive, query, retention, and store modules; the remaining root is mostly public types and reexports
   - `burn_p2p_publish` now has dedicated aliases, backend, download, jobs, registry, and view/filter modules; the remaining root is mostly backfill/prune orchestration plus shared store/error wiring
2. Public facade breadth
   - `burn_p2p` is smaller and clearer after collapsing `ProjectBackend`, but it
     still re-exports a large amount of lower-level surface
3. Bootstrap breadth
   - `burn_p2p_bootstrap` is correctly the composition root, but it still wants
     some deeper internal cleanup across remaining feature/service glue

The earlier `burn_p2p_browser -> burn_p2p_bootstrap` and
`burn_p2p_views -> burn_p2p_bootstrap` layering smells have been repaired by
moving shared browser-edge/trust contracts into `burn_p2p_core` and keeping
presentation-owned diagnostics contracts inside `burn_p2p_views`.

## Verification anchor

The current architecture baseline is checked continuously through the workspace
tests, examples, deployment-profile checks, and release gate in
`scripts/release_prep.sh`.
