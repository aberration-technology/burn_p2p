# Downstream Burn Integration Guide

This guide shows the recommended shape for embedding `burn_p2p` into a Burn
project today.

## Recommended Mental Model

Treat `burn_p2p` as a runtime that wraps your existing burn learner. You do not
switch to a separate training stack. Start from:

1. keep your burn `TrainStep` / `InferenceStep` model
2. build `burn::train::Learner::new(model, optimizer, scheduler)`
3. keep using burn train / eval dataloaders
4. wrap it with `burn_p2p::burn::from_loaders(...)`
5. call `.trainer(...)` or `.validator(...)`

translation is:

- your train model stays your burn `TrainStep` model
- your learner stays `Learner::new(model, optimizer, scheduler)`
- your batch type stays the burn train input
- your loss and optimizer step stay inside burn `TrainStep`
- your canonical loader path lives in burn `TrainLoader` / `ValidLoader`, exposed here as train/eval loaders
- your evaluation metrics can live in `.with_evaluate(...)`
- your shard-backed train dataset can usually live in `BurnShardedDataset` plus `.with_sharded_dataset(...)`
- low-level shard hooks still exist, but now live behind `burn_p2p::burn::advanced`
- the adapter handles the window loop, checkpoint load/save, schema hashing, merge defaults,
  local dataset plumbing, neutral fallback validation metrics, and optional root ema

runtime path is:

1. clone learner
2. load current p2p head into the learner model
3. run `lr_step()`
4. run `train_step(batch)`
5. run `optimizer_step(grads)`
6. publish the updated model artifact

## Minimal Integration Flow

### 1. Define a backend-facing project type

Your project type owns runtime-local configuration such as dataset paths,
evaluation behavior, or hyperparameters.

Example reference:

- [burn_ndarray_runtime.rs](../crates/burn_p2p/examples/burn_ndarray_runtime.rs)

### 2. Add the runtime hooks

For Burn-native projects, the recommended integration point is now
`burn_p2p::burn::from_loaders(...)`.

Practical guidance:

- keep your forward pass, loss fn, and optimizer step in burn `TrainStep`
- start from `Learner::new(model, optimizer, scheduler)`
- pass burn `TrainLoader` / `ValidLoader` directly when they already exist
- add `.with_sharded_dataset(...)` when the train dataset should be fetched lease-by-lease by native or browser/wasm peers
- use `.with_train_loader(...)` when only the training loader is available
- use `.with_eval_loader(...)` when only the evaluation loader is available
- add `.with_evaluate(...)` when validator or trainer should emit real validation metrics
- use `.with_step_metrics(...)` / `.with_window_metrics(...)` for extra metric rows
- use `burn_p2p::burn::advanced::BurnLearnerProjectBuilderAdvancedExt` only when you need local batch closures or fully custom shard hooks

The adapter fills in:

- learner restore from the current head
- per-window learner loop
- model schema hashing
- model artifact load/save
- weighted-mean merge
- auto-generated local dataset registration + one-shard planning for the simple batch path
- neutral validation metrics when you omit `.with_evaluate(...)`
- optional root ema

Use `BurnLearnerWorkload` when you want the same hooks on a reusable project
trait.

Use `BurnWorkload` only when you need a fully custom window loop.

Use `P2pWorkload` directly only for non-Burn or fully custom runtimes.

## 3. Wrap the workload in a project family

The Burn-native happy path is:

```rust
let trainer = burn_p2p::burn::from_loaders(
    Learner::new(model, optimizer, scheduler),
    device,
    train_loader,
    eval_loader,
)
.trainer(release_manifest, supported_workload)?;
```

If the train dataset should be p2p-distributed, keep the same loader entry and
add the shard helper:

```rust
let train_dataset = burn_p2p::burn::BurnShardedDataset::write_local(
    "./target/train-shards",
    &train_records,
    burn_p2p::burn::BurnShardedDatasetConfig::new("train").with_microshards(8),
)?;

let trainer = burn_p2p::burn::from_loaders(
    Learner::new(model, optimizer, scheduler),
    device,
    train_loader,
    eval_loader,
)
.with_sharded_dataset(train_dataset, train_batcher, 32)
.trainer(release_manifest, supported_workload)?;
```

`.trainer(...)` returns the normal `NodeBuilder`, already wrapped around a
single-workload Burn family.

Validator-only path is simpler:

```rust
let validator = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_eval_loader(eval_loader)
.with_evaluate(|model, split| evaluate(model, split))
.validator(release_manifest, supported_workload)?;
```

`.validator(...)` does not need a train loader unless the same local node also
trains.

Use `.connect(BurnTarget::Custom(...), ...)` when you want a custom role set.

Non-training custom roles can stay minimal:

```rust
let viewer = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.connect(
    burn_p2p::burn::BurnTarget::Custom(
        burn_p2p::PeerRoleSet::new([
            burn_p2p::PeerRole::Viewer,
            burn_p2p::PeerRole::BrowserObserver,
        ]),
    ),
    release_manifest,
    supported_workload,
)?;
```

Use `.trainer_with_config(...)`, `.validator_with_config(...)`, or
`.connect_with_config(...)` when you want to override artifact format, chunking,
or merge behavior.

If browser or wasm peers should fetch the same assigned shards over http, point
the prepared dataset at an http origin:

```rust
let browser_train_dataset = train_dataset
    .clone()
    .with_http_upstream("https://edge.example/datasets/train");
```

If you already have a real dataset registration and custom shard path, drop to
the advanced hooks:

```rust
use burn_p2p::burn::advanced::BurnLearnerProjectBuilderAdvancedExt;

.with_evaluate(|model, split| evaluate(model, split))
.with_dataset(dataset_registration, microshard_plan)
.with_load_batches(|lease, cached_microshards, device| {
    load_batches(lease, cached_microshards, device)
})
```

If your project has no burn dataloader yet, the low-friction fallback is:

```rust
use burn_p2p::burn::advanced::BurnLearnerProjectBuilderAdvancedExt;

let trainer = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_batches(|device| build_training_batches(device))
.trainer(release_manifest, supported_workload)?;
```

shared ui/auth contract now looks like:

- browser runtime builds `burn_p2p_views::NodeAppClientView` through `BrowserAppController`
- native runtime builds the same contract through `RunningNode::app_view(...)`
- headless/native auth uses `burn_p2p::EdgeAuthClient`
- `burn_p2p_app` now renders the same dioxus component tree on web and native hosts

current host situation:

- shared typed app state is common
- browser host is the wasm `web-client` entry
- native host is `burn_p2p_app::launch_node_app(...)` behind `desktop-client`
- linux desktop builds need gtk/webkit pkg-config libs

If you want full manual control, you can still use `SingleWorkloadProjectFamily`.

```rust
let workload = my_workload.into_p2p_workload(config)?;

let family = SingleWorkloadProjectFamily::new(
    release_manifest,
    workload,
)?;
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
  [family_workload_minimal.rs](../crates/burn_p2p/examples/family_workload_minimal.rs)
- Minimal native example:
  [synthetic_trainer.rs](../crates/burn_p2p/examples/synthetic_trainer.rs)
- Burn example:
  [burn_ndarray_runtime.rs](../crates/burn_p2p/examples/burn_ndarray_runtime.rs)
- Embedded daemon example:
  [embedded_runtime_daemon.rs](../crates/burn_p2p_bootstrap/examples/embedded_runtime_daemon.rs)

## Recommended Next Improvements For This Guide

This guide is the canonical downstream path for now, but it should evolve
toward:

- a thinner overall facade surface
- a dedicated multi-workload guide
- example compilation in CI as a hard gate
