# burn_p2p 🔥🤝

`burn_p2p` is the public entrypoint for peer-to-peer Burn training.

if you already have a Burn model, loss function, optimizer, and dataset, the
integration path is:

1. wrap your existing training code in `P2pWorkload`
2. wrap workload in `SingleWorkloadProjectFamily`
3. spawn a trainer node with `NodeBuilder`
4. point it at an existing experiment revision and call `train_window_once`

## install

```toml
[dependencies]
burn_p2p = "=0.21.0-pre.4"
```

for the burn-backed runtime path and examples:

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.4", features = ["burn"] }
```

## usage

### if you already have a model, loss fn, and dataset

`burn_p2p` does not replace your training step. you keep your model, loss, and
batch logic; you expose them through `P2pWorkload`.

helper names inside the snippet are your own project code.

```rust
use std::{collections::BTreeMap, path::PathBuf};

use burn_p2p::{
    AssignmentLease, CachedMicroShard, CapabilityEstimate, ContentId, DatasetRegistration,
    DatasetSizing, EvalSplit, FsArtifactStore, MetricReport, MetricValue, P2pWorkload,
    PatchOutcome, PatchSupport, TrainError, WindowCtx, WindowReport,
};

#[derive(Clone, Debug)]
struct MyWorkload {
    dataset_root: PathBuf,
}

impl P2pWorkload for MyWorkload {
    type Device = MyDevice;
    type Model = MyModel;
    type Batch = MyBatch;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &Self::Device) -> Self::Model {
        MyModel::new(device)
    }

    fn benchmark(&self, model: &Self::Model, device: &Self::Device) -> CapabilityEstimate {
        estimate_local_capacity(model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let loss = run_one_window(&mut ctx.model, &ctx.batches)?;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
            completed_at: chrono::Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        evaluate_model(model, split)
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        register_dataset(&self.dataset_root)
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
        plan_microshards(registration, DatasetSizing {
            total_examples: 1_000_000,
            total_tokens: 128_000_000,
            total_bytes: 4_000_000_000,
        })
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        load_batches_for_lease(lease, cached_microshards)
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &burn_p2p::ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        load_checkpoint(model, descriptor, store, device)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: burn_p2p::ArtifactKind,
        head_id: burn_p2p::HeadId,
        base_head_id: Option<burn_p2p::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<burn_p2p::ArtifactDescriptor> {
        save_checkpoint(model, artifact_kind, head_id, base_head_id, store)
    }

    fn supported_workload(&self) -> burn_p2p::SupportedWorkload {
        supported_workload_manifest()
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("my-model-schema")
    }

    fn runtime_device(&self) -> Self::Device {
        default_device()
    }

    fn apply_patch(&mut self, _patch: &burn_p2p::RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("no runtime patches".into())
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }
}
```

mapping:

- your model type becomes `type Model`
- your batch type becomes `type Batch`
- your loss function lives inside `train_window`
- your evaluation metrics live inside `evaluate`
- your dataset and shard loading live in `dataset_registration`, `microshard_plan`, and `load_batches`
- your checkpoint format lives in `load_model_artifact` and `materialize_model_artifact`

### turning workload into a p2p trainer

trainer flow:

```rust
use burn_p2p::{
    ExperimentId, NodeBuilder, RevisionId, RoleSet, SingleWorkloadProjectFamily, StorageConfig,
    StudyId,
};

let family = SingleWorkloadProjectFamily::new(release_manifest, workload)?;

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
println!("published head {}", outcome.head.head_id.as_str());
```

the trainer joins an existing experiment revision,
downloads the current base head and assigned shards, runs one training window,
and publishes a candidate head.

you still need a validator / authority path somewhere in the network to:

- initialize the first head for the revision
- validate candidate heads
- promote accepted updates

for a complete local trainer + validator walkthrough, start with:

- [`crates/burn_p2p/examples/synthetic_trainer.rs`](crates/burn_p2p/examples/synthetic_trainer.rs)
- [`crates/burn_p2p/examples/burn_ndarray_runtime.rs`](crates/burn_p2p/examples/burn_ndarray_runtime.rs)

if you only need the trait shape, start with:

- [`crates/burn_p2p/examples/family_workload_minimal.rs`](crates/burn_p2p/examples/family_workload_minimal.rs)

if your family exposes more than one workload, select one before spawn:

```rust
let trainer = NodeBuilder::new(family)
    .for_workload(burn_p2p::WorkloadId::new("my-workload"))?
    .with_network(network_manifest)?
    .with_roles(RoleSet::default_trainer())
    .with_storage(StorageConfig::new("./burn-p2p-node"))
    .spawn()?;
```

## examples

```bash
# minimal family/workload integration
cargo run -p burn_p2p --example family_workload_minimal

# synthetic runtime walkthrough
cargo run -p burn_p2p --example synthetic_trainer

# burn-backed facade example
cargo run -p burn_p2p --example burn_ndarray_runtime --features burn
```

## other crates

there are also companion crates for:

- bootstrap and operator surfaces
- browser workers and edge clients
- artifact publication and download
- metrics indexing and portal rendering
- auth and social integrations

start from `burn_p2p`. reach for the others only when you are building
deployment, browser, or operator flows.

## docs

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/downstream-burn-guide.md`](docs/downstream-burn-guide.md)
- [`docs/operator-runbook.md`](docs/operator-runbook.md)
- [`docs/testing/local-dev.md`](docs/testing/local-dev.md)
- [`docs/testing/browser-local.md`](docs/testing/browser-local.md)
- [`docs/testing/ci-profiles.md`](docs/testing/ci-profiles.md)
- [`docs/testing/adversarial.md`](docs/testing/adversarial.md)

## local verification

the local-first test surface is `cargo xtask`:

```bash
cargo xtask doctor
cargo xtask check
cargo xtask e2e smoke
cargo xtask e2e services
cargo xtask browser smoke
cargo xtask adversarial smoke
```

`cargo xtask e2e smoke` is the default local system check. It now covers the
native smoke cluster together with a minimal metrics and publication/download
slice. Use `cargo xtask e2e services` when you want the focused services-only
rerun path.

to rerun the exact CI lanes locally:

```bash
cargo xtask ci pr-fast --keep-artifacts
cargo xtask ci browser --keep-artifacts
cargo xtask ci integration --keep-artifacts
cargo xtask ci services --keep-artifacts
cargo xtask ci nightly --keep-artifacts
cargo xtask ci publish --keep-artifacts
```
