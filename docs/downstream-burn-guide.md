# downstream burn integration guide

this guide shows the recommended shape for embedding `burn_p2p` into a burn
project today.

## recommended mental model

treat `burn_p2p` as a runtime around your existing burn learner. the basic path
is:

1. keep your burn `TrainStep` / `InferenceStep` model
2. build `burn::train::Learner::new(model, optimizer, scheduler)`
3. keep using burn train / eval dataloaders
4. wrap it with `burn_p2p::burn::from_loaders(...)`
5. call `.trainer(...)` or `.validator(...)`

mental model:

- your train model stays your burn `TrainStep` model
- your learner stays `Learner::new(model, optimizer, scheduler)`
- your batch type stays the burn train input
- your loss and optimizer step stay in burn `TrainStep`
- your train/validation loader path stays in burn `TrainLoader` / `ValidLoader`
- lease-scoped micro-epochs are the unit that drives checkpoint publication and canonical reconcile
- use `BurnShardedDataset` + `.with_sharded_dataset(...)` for shard files
- use `LeaseDataPipeline` for indexed, generated, or custom data flows
- use `.with_evaluate(...)` if you want real validation metrics

runtime path:

1. clone learner
2. load current p2p head into the learner model
3. run `lr_step()`
4. run `train_step(batch)`
5. run `optimizer_step(grads)`
6. publish the updated model artifact

## reusable downstream-facing surfaces

the repo now exposes a small set of product/runtime seams intended for real
downstream apps:

- `burn_p2p_admin`
  - typed native+wasm client for `GET /state`, `GET /directory`,
    `GET /directory/signed`, and `POST /admin`
  - use this instead of custom downstream admin http glue
- `burn_p2p_workload::DirectoryMetadataAttachment`
  - versioned json-in-directory-metadata attachment and resolution helpers
  - use this instead of custom directory metadata boilerplate
- `burn_p2p_views`
  - typed operator/browser/runtime view models for downstream ui composition
- `burn_p2p_app`
  - reusable dioxus widgets for auth/session, runtime, training, and
    operator-directory/rollout panels
- `burn_p2p_browser::BrowserSiteBootstrapConfig`
  - static browser-shell config helper emitted by the upstream site builder
- `burn_p2p_e2e`
  - public browser capture/export facade and generic validation assertions
  - use this instead of depending directly on `burn_p2p_testkit`

these surfaces are intentionally mechanism-oriented. keep product semantics,
model types, dataset semantics, and custom experiment profiles downstream.

## reference infra templates

the repo now also ships copy-edit reference assets for downstream repos under
[`docs/reference-assets/`](reference-assets/README.md).

use those when you need:

- a github pages workflow for a wasm browser shell
- a dataset publication workflow pattern
- a staged validation workflow
- a bootstrap/cloud deployment starting point

important boundary:

- crate apis such as `burn_p2p_admin`, `burn_p2p_e2e`, `burn_p2p_views`, and
  `burn_p2p_app` are the reusable upstream interfaces
- the files in `docs/reference-assets/` and `deploy/` are templates and
  examples, not semver-stable compatibility promises

## minimal integration flow

### 1. define a backend-facing project type

your project type owns runtime-local configuration such as dataset paths,
evaluation behavior, or hyperparameters.

reference:

- [burn_ndarray_runtime.rs](../crates/burn_p2p/examples/burn_ndarray_runtime.rs)

### 2. add the runtime hooks

for burn-native projects, the recommended integration point is
`burn_p2p::burn::from_loaders(...)`.

practical guidance:

- keep your forward pass, loss fn, and optimizer step in burn `TrainStep`
- start from `Learner::new(model, optimizer, scheduler)`
- pass burn `TrainLoader` / `ValidLoader` directly when they already exist
- add `.with_sharded_dataset(...)` for prepared shard files
- use `LeaseDataPipeline` plus `.with_data_pipeline(...)` for indexed, generated, or custom micro-epochs
- use `.with_validation_loader(...)` when a validator/viewer should use the default validation loop
- add `.with_evaluate(...)` when validator or trainer should emit real validation metrics
- use `.with_step_metrics(...)` / `.with_window_metrics(...)` for extra metric rows

the adapter fills in:

- learner restore from the current head
- per-window learner loop
- model schema hashing
- model artifact load/save
- weighted-mean merge
- auto-generated local dataset registration + one-shard planning for the simple batch path
- neutral validation metrics when you omit `.with_evaluate(...)`
- optional root ema

use `BurnLearnerWorkload` when you want the same hooks on a reusable project
trait.

use `BurnWorkload` only when you need a fully custom window loop.

use `P2pWorkload` directly only for non-burn or fully custom runtimes.

## 3. wrap the workload in a project family

the burn-native happy path is:

```rust
let trainer = burn_p2p::burn::from_loaders(
    Learner::new(model, optimizer, scheduler),
    device,
    train_loader,
    validation_loader,
)
.trainer(release_manifest, supported_workload)?;
```

if the train dataset should be p2p-distributed, keep the same loader entry and
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
    validation_loader,
)
.with_sharded_dataset(train_dataset, train_batcher, 32)
.trainer(release_manifest, supported_workload)?;
```

`.trainer(...)` returns the normal `NodeBuilder`, already wrapped around a
single-workload Burn family.

validator-only path is simpler:

```rust
let validator = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_validation_loader(validation_loader)
.with_evaluate(|model, split| evaluate(model, split))
.validator(release_manifest, supported_workload)?;
```

`.validator(...)` does not need a train loader unless the same local node also
trains.

use `.connect(BurnTarget::Custom(...), ...)` when you want a custom role set.

non-training custom roles can stay minimal:

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

use `.trainer_with_config(...)`, `.validator_with_config(...)`, or
`.connect_with_config(...)` when you want to override artifact format, chunking,
or merge behavior.

if browser or wasm peers should fetch the same assigned shards over http, point
the prepared dataset at an http origin:

```rust
let browser_train_dataset = train_dataset
    .clone()
    .with_http_upstream("https://edge.example/datasets/train");
```

if micro-epochs should be derived from a sampler or generator instead of shard
files, make that explicit:

```rust
use burn_p2p::{
    LeaseDataPipeline,
    LeaseDataPipelineDescriptor,
    LeaseDataPipelineKind,
};

let micro_epoch_pipeline = LeaseDataPipeline::new(
    LeaseDataPipelineDescriptor::new(
        "nca-generated-micro-epochs",
        LeaseDataPipelineKind::GeneratedDataset,
    ),
    move || Ok(dataset_registration.clone()),
    move |_registration| Ok(microshard_plan.clone()),
    move |lease, _cached_microshards, device| {
        build_batches_from_seed_bank(lease, device)
    },
);

let trainer = burn_p2p::burn::from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_data_pipeline(micro_epoch_pipeline)
.trainer(release_manifest, supported_workload)?;
```

the same concept is available on the python/torch side through
`burn_p2p_python::PythonTorchProject::new_with_data_pipeline(...)`.
the built-in helpers are:

- `sharded_data_pipeline(...)` for prepared shard files
- `indexed_dataset_pipeline(...)` for existing `Dataset` / `Sampler` / `DataLoader` style code
- `generated_dataset_pipeline(...)` for deterministic synthetic or recipe-driven flows

those helpers keep the network cadence aligned with one leased micro-epoch per
checkpoint publication, while leaving the inner dataloader or generator logic
inside the backend runtime.

for parity with the burn adapter, python/torch projects now expose the same
pipeline introspection methods as `BurnLearnerProject`:
`data_pipeline_descriptor()`, `data_pipeline_kind()`, and `local_upstream_root()`.
treat `local_upstream_root()` as optional by design; indexed/generated
micro-epoch pipelines may not have any meaningful filesystem root.

if you want full manual control, you can still use
`SingleWorkloadProjectFamily`.

```rust
let workload = my_workload.into_p2p_workload(config)?;

let family = SingleWorkloadProjectFamily::new(
    release_manifest,
    workload,
)?;
```

if you have multiple workloads, implement `P2pProjectFamily` directly and use
the family to select the workload before spawning the node.

## 4. build the node

the intended downstream builder flow is:

1. `NodeBuilder::new(family)`
2. `for_workload(...)` if the family has more than one workload
3. `with_network(...)`
4. `with_roles(...)`
5. `with_storage(...)`
6. `with_identity(...)` or enrollment/auth configuration
7. `spawn()`

the exact builder methods available depend on the enabled feature set and the
runtime profile being used.

for a trainer, the minimal practical shape is:

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

let mut trainer_session = trainer.continuous_trainer(&experiment)?;
let outcome = trainer_session.train_next_window()?;
```

`continuous_trainer()` is the opinionated optimistic trainer path. it keeps a
warm in-memory model, immediately begins the next local window after publishing,
and reconciles newer canonical heads between windows using the default trainer
policy.

`train_window_once()` is still available as the low-level one-shot primitive.
use it when you explicitly want one published window and no retained trainer
session state, or when you want external orchestration to wait for every
canonical promotion before continuing.

one important point: a trainer is not enough by itself. a validator / authority
path must already exist in the network to initialize the revision head and
validate/promote candidate updates.

## 5. choose roles deliberately

use runtime capability estimates to choose realistic roles:

- CPU / NdArray: good for synthetic flows, low-end validation, and some small native training
- native WGPU: good for mid-tier native GPU participation where supported
- browser WGPU: best treated as explicitly browser-enabled trainer work with short windows
- CUDA: preferred high-throughput native trainer path

if you are unsure, start with:

- validator or verifier on CPU/native
- observer or verifier in browser
- trainer only once capability estimates and window budgets are calibrated

## 6. wire data carefully

your runtime integration should provide:

- stable dataset registration metadata
- a deterministic dataset view
- shard fetch/materialization behavior that matches your storage layout

the examples in this repository use synthetic and local-file flows. production
integrations should document:

- where manifests live
- how dataset views are versioned
- how shard bytes are fetched and cached

## starting points

- minimal family/workload example:
  [family_workload_minimal.rs](../crates/burn_p2p/examples/family_workload_minimal.rs)
- minimal native example:
  [synthetic_trainer.rs](../crates/burn_p2p/examples/synthetic_trainer.rs)
- burn example:
  [burn_ndarray_runtime.rs](../crates/burn_p2p/examples/burn_ndarray_runtime.rs)
- embedded daemon example:
  [embedded_runtime_daemon.rs](../crates/burn_p2p_bootstrap/examples/embedded_runtime_daemon.rs)
