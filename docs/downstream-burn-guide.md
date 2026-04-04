# Downstream Burn Integration Guide

This guide shows the recommended shape for embedding `burn_p2p` into a Burn
project today.

## Recommended Mental Model

Treat `burn_p2p` as a runtime that asks your project for three things:

1. workload metadata
2. training and evaluation logic
3. dataset registration and shard-loading hooks

The forward-facing model is:

- `P2pWorkload`
- `P2pProjectFamily`
- `NodeBuilder`

## Minimal Integration Flow

### 1. Define a backend-facing project type

Your project type owns runtime-local configuration such as dataset paths,
evaluation behavior, or hyperparameters.

Example reference:

- [burn_ndarray_runtime.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/burn_ndarray_runtime.rs)

### 2. Implement the runtime hooks

Today, a complete integration implements:

- `P2pWorkload`

Practical guidance:

- put Burn-specific model/batch logic on `P2pWorkload`
- put `type Device`, dataset, and checkpoint/materialization hooks on `P2pWorkload`
- put workload metadata and role support on `P2pWorkload`

## 3. Wrap the workload in a project family

If you have one workload, use `SingleWorkloadProjectFamily`.

```rust
let family = SingleWorkloadProjectFamily::new(
    release_manifest,
    workload,
);
```

If you have multiple workloads, implement `P2pProjectFamily` directly and use
the family to select the workload before spawning the node.

## 4. Build the node

The intended downstream builder flow is:

1. `NodeBuilder::new(family)`
2. `with_network(...)`
3. `for_workload(...)`
4. `with_storage(...)`
5. `with_identity(...)` or enrollment/auth configuration
6. `spawn()`

The exact builder methods available depend on the enabled feature set and the
runtime profile being used.

## 5. Pick a role and capability profile deliberately

Use runtime capability estimates to choose realistic roles:

- CPU / NdArray: good for synthetic flows, low-end validation, and some small native training
- native WGPU: good for mid-tier native GPU participation where supported
- browser WGPU: best treated as explicitly browser-enabled trainer work with short windows
- CUDA: preferred high-throughput native trainer path

If you are unsure, start with:

- validator or verifier on CPU/native
- observer or verifier in browser
- trainer only once capability estimates and window budgets are calibrated

## 6. Wire dataset registration and shard loading carefully

Your runtime integration should provide:

- stable dataset registration metadata
- a deterministic dataset view
- shard fetch/materialization behavior that matches your storage layout

The examples in this repository use synthetic and local-file flows. Production
integrations should document:

- where manifests live
- how dataset views are versioned
- how shard bytes are fetched and cached

## Current Caveats

- The facade crate is still large, so browsing docs through the source can feel
  heavier than it should.
- Browser and social surfaces are optional deployment services. They are not
  required for native Burn integration.

## Suggested Starting Points

- Minimal family/workload example:
  [family_workload_minimal.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/family_workload_minimal.rs)
- Minimal native example:
  [synthetic_trainer.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/synthetic_trainer.rs)
- Burn example:
  [burn_ndarray_runtime.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/burn_ndarray_runtime.rs)
- Embedded daemon example:
  [embedded_runtime_daemon.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p_bootstrap/examples/embedded_runtime_daemon.rs)

## Recommended Next Improvements For This Guide

This guide is the canonical downstream path for now, but it should evolve
toward:

- a thinner overall facade surface
- a dedicated multi-workload guide
- example compilation in CI as a hard gate
