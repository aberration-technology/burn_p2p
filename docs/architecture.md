# burn_p2p Architecture

## Purpose

This document summarizes the crate boundaries and the main data flow through the repository.

The core design constraints remain:

- one network is one trust domain
- one network is pinned to one project family
- one network is pinned to one compatible release train
- admission is certificate-based and strict
- checkpoints move by manifest and chunk, not by global blob broadcast
- the Burn execution seam stays internal-facing

## Layering

### Layer A: core runtime

Always-on crates:

- `burn_p2p`
- `burn_p2p_core`
- `burn_p2p_experiment`
- `burn_p2p_checkpoint`
- `burn_p2p_engine`
- `burn_p2p_dataloader`
- `burn_p2p_limits`
- `burn_p2p_swarm`
- core portions of `burn_p2p_security`

These crates define the wire model, compatibility model, checkpoint flow, runtime state machine,
lease assignment, merge/promotion flow, and admission rules. They must stay lean enough to build
without portal, social, browser, or heavyweight auth-provider stacks.

### Layer B: optional edge services

Optional deployment-facing crates:

- `burn_p2p_bootstrap`
- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_browser`
- `burn_p2p_portal`

These crates expose HTTP routes, enrollment/auth flows, portal rendering, browser ingress, and
browser-specific runtime adapters.

### Layer C: optional product surfaces

Optional user-facing crates:

- `burn_p2p_social`
- `burn_p2p_ui`
- `burn_p2p_portal`

These crates consume accepted receipts and identity mappings. They must never decide runtime
correctness, merge validity, or peer admission.

## Main runtime flow

### 1. Build-time compatibility

`burn_p2p_core` defines the canonical IDs and manifests:

- `ReleaseTrainManifest`
- `TargetArtifactManifest`
- `NetworkManifest`
- `ExperimentDirectoryEntry`
- `RevisionManifest`
- `NodeCertificate`

`burn_p2p` re-exports these so downstream applications can configure one public facade crate.

### 2. Downstream workload binding

The intended downstream path is:

1. implement `P2pWorkload`
2. wrap one or more workloads in `P2pProjectFamily`
3. construct a node with `NodeBuilder`
4. bind identity, storage, bootstrap peers, and roles

Legacy `P2pProject` and `RuntimeProject` remain under `burn_p2p::compat`.

### 3. Admission and enrollment

`burn_p2p_security` owns:

- certificate issuance and verification
- release policy
- admission decisions
- revocation epoch checks
- auth/session traits

Optional auth crates implement provider-specific flows and hand principal sessions back to the
security core for certificate issuance.

### 4. Experiment and workload selection

`burn_p2p_experiment` owns:

- experiment directory modeling
- revision policy
- browser eligibility flags
- lag and merge-window policy

The selected directory entry feeds both native nodes and browser runtimes.

### 5. Data and checkpoint movement

`burn_p2p_dataloader` plans microshards and lease-aware fetch work.

`burn_p2p_checkpoint` owns:

- manifest/chunk descriptors
- artifact stores
- merge planning
- sync and recovery paths

Large blobs never go out over global broadcast. Only manifests, chunks, and targeted artifact
fetches move across the runtime.

### 6. Runtime communication

`burn_p2p_swarm` owns:

- overlay topics
- control-plane shells
- pubsub payloads
- browser edge control-plane sync
- transport selection and event waiting

### 7. Runtime execution

`burn_p2p` owns the higher-level native runtime loops:

- sync and lag assessment
- one-window training
- validation and merge selection
- reducer assignment and promotion
- persistence and recovery

`burn_p2p_engine` keeps the Burn-facing execution seam isolated from the wire and control-plane
types.

### 8. Edge and product surfaces

`burn_p2p_bootstrap` is the deployment composition root. It exposes:

- capability manifests
- portal snapshots
- signed directory and leaderboard views
- admin routes
- auth entry points
- browser receipt submission

`burn_p2p_portal` turns those snapshots into HTML.
`burn_p2p_social` turns accepted receipts into rollups, badges, and leaderboards.

## Major data products

### Trust data

- node certificates
- trust bundles
- revocation epochs
- release manifests

### Runtime data

- experiment directory entries
- revision manifests
- head descriptors
- update announcements
- merge certificates
- reducer assignments

### Product data

- social profiles
- leaderboard snapshots
- badge awards
- edge service manifests

## Current complexity hotspots

The crate map is now directionally correct, but the following roots still deserve more extraction:

- `crates/burn_p2p/src/lib.rs`
- `crates/burn_p2p_bootstrap/src/bin/burn-p2p-bootstrap.rs`
- `crates/burn_p2p_swarm/src/lib.rs`
- `crates/burn_p2p_core/src/schema.rs`

The main goal of the next cleanup slices should be to keep the existing semantics while shrinking
review scope and blast radius.
