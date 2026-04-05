# Downstream Burn Integration Guide

This guide shows the recommended shape for embedding `burn_p2p` into a Burn
project today.

## Recommended Mental Model

Treat `burn_p2p` as a runtime that wraps your existing training code. You do
not hand it a model and then switch to a new training API. Instead, you expose
your current project through:

1. `P2pWorkload`
2. `SingleWorkloadProjectFamily` or `P2pProjectFamily`
3. `NodeBuilder`

If you already have a model, loss fn, optimizer, and dataset loader, the
translation is:

- your model type becomes `P2pWorkload::Model`
- your batch type becomes `P2pWorkload::Batch`
- your loss and optimizer step live in `train_window(...)`
- your evaluation metrics live in `evaluate(...)`
- your dataset metadata and shard loading live in `dataset_registration(...)`,
  `microshard_plan(...)`, and `load_batches(...)`
- your checkpoint serialization lives in `load_model_artifact(...)` and
  `materialize_model_artifact(...)`

## Minimal Integration Flow

### 1. Define a backend-facing project type

Your project type owns runtime-local configuration such as dataset paths,
evaluation behavior, or hyperparameters.

Example reference:

- [burn_ndarray_runtime.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/burn_ndarray_runtime.rs)

### 2. Implement the runtime hooks

Today, the concrete downstream integration point is `P2pWorkload`.

Practical guidance:

- put your forward pass, loss fn, and optimizer step in `train_window`
- put your validation metrics in `evaluate`
- put Burn-specific model/device/batch types on the associated types
- put dataset registration and shard loading on the dataset hooks
- put checkpoint load/save behavior on the artifact hooks
- put workload metadata and compatibility identifiers on `supported_workload`
  and `model_schema_hash`

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
2. `for_workload(...)` if the family has more than one workload
3. `with_network(...)`
4. `with_roles(...)`
5. `with_storage(...)`
6. `with_identity(...)` or enrollment/auth configuration
7. `spawn()`

The exact builder methods available depend on the enabled feature set and the
runtime profile being used.

For a trainer, the minimal practical shape is:

```rust
let mut trainer = NodeBuilder::new(family)
    .with_network(network_manifest)?
    .with_roles(RoleSet::default_trainer())
    .with_storage(StorageConfig::new("./burn-p2p-node"))
    .with_bootstrap_peer(validator_addr)
    .spawn()?;

let experiment = trainer.experiment(
    StudyId::new("study"),
    ExperimentId::new("experiment"),
    RevisionId::new("rev-1"),
);

let outcome = trainer.train_window_once(&experiment)?;
```

That call to `train_window_once` is the actual “make this node a trainer” step.
It pulls the current base head and assigned shards, runs one local training
window, and publishes the candidate update.

One important point: a trainer is not enough by itself. A validator / authority
path must already exist in the network to initialize the revision head and
validate/promote candidate updates.

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
