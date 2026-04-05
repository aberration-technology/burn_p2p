# Downstream Burn Integration Guide

This guide shows the recommended shape for embedding `burn_p2p` into a Burn
project today.

## Recommended Mental Model

Treat `burn_p2p` as a runtime that wraps your existing burn learner. You do not
switch to a separate training stack. Start from:

1. keep your burn `TrainStep` / `InferenceStep` model
2. build `burn::train::Learner::new(model, optimizer, scheduler)`
3. wrap it with `burn_p2p::burn::from_learner(...)`
4. add a batch source
5. call `.trainer(...)` or `.validator(...)`

translation is:

- your train model stays your burn `TrainStep` model
- your learner stays `Learner::new(model, optimizer, scheduler)`
- your batch type stays the burn train input
- your loss and optimizer step stay inside burn `TrainStep`
- your simplest training input path lives in `.with_batches(...)`
- your evaluation metrics can live in `.with_evaluate(...)`
- your shard-backed dataset path can live in `.with_dataset(...)` and `.with_load_batches(...)`
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

- [burn_ndarray_runtime.rs](/home/mosure/repos/burn_p2p/crates/burn_p2p/examples/burn_ndarray_runtime.rs)

### 2. Add the runtime hooks

For Burn-native projects, the recommended integration point is now
`burn_p2p::burn::from_learner(...)`.

Practical guidance:

- keep your forward pass, loss fn, and optimizer step in burn `TrainStep`
- start from `Learner::new(model, optimizer, scheduler)`
- use `.with_batches(...)` when the learner can use a local dataset path as-is
- use `.with_local_dataset(...)` when that simple path needs a custom dataset label or sizing
- use `.with_assignment_batches(...)` when batch selection depends on the p2p assignment lease
- add `.with_evaluate(...)` when you want real validation metrics
- add `.with_dataset(...)` and `.with_load_batches(...)` when you already have shard-backed data
- use `.with_step_metrics(...)` / `.with_window_metrics(...)` for extra metric rows

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
let trainer = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_batches(|device| {
    build_training_batches(device)
})
.trainer(release_manifest, supported_workload)?;
```

`.trainer(...)` returns the normal `NodeBuilder`, already wrapped around a
single-workload Burn family.

Use `.validator(...)` for the authority / validator / archive side.

Use `.connect(BurnTarget::Custom(...), ...)` when you want a custom role set.

Use `.trainer_with_config(...)`, `.validator_with_config(...)`, or
`.connect_with_config(...)` when you want to override artifact format, chunking,
or merge behavior.

If you already have a real dataset registration and shard path, swap the simple
hook for:

```rust
.with_evaluate(|model, split| evaluate(model, split))
.with_dataset(dataset_registration, microshard_plan)
.with_load_batches(|lease, cached_microshards, device| {
    load_batches(lease, cached_microshards, device)
})
```

shared ui/auth contract now looks like:

- browser runtime builds `burn_p2p_ui::NodeAppClientView` through `BrowserAppController`
- native runtime builds the same contract through `RunningNode::app_view(...)`
- headless/native auth uses `burn_p2p::PortalAuthClient`
- `burn_p2p_portal` now renders the same dioxus component tree on web and native hosts

current host situation:

- shared typed app state is common
- browser host is the wasm `web-client` entry
- native host is `burn_p2p_portal::launch_node_app(...)` behind `desktop-client`
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
