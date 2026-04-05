# burn_p2p 🔥🤝

`burn_p2p` is the public entrypoint for peer-to-peer Burn training.

integration path:

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

### burn happy path

`burn_p2p` does not replace your training step. you keep your model, loss, and
batch logic.

if you are already using burn, start with `burn_p2p::burn::BurnWorkload`.

you still implement:

- `init_model()`
- `benchmark()`
- `train_window()`
- `evaluate()`
- `runtime_device()`
- `dataset_registration()`
- `microshard_plan()`
- `load_batches()`
- `contribution_metrics()`
- optional `apply_patch()` and `supported_patch_classes()`

the adapter fills in:

- model schema hashing
- model artifact load/save
- weighted-mean merge
- optional root ema

minimal burn wrapper:

```rust
use burn_p2p::{
    ChunkingScheme, StorageConfig,
};
use burn_p2p::burn::{
    native, BurnArtifactConfig, BurnNodeTarget, BurnWorkload, BurnWorkloadConfig,
};

let mut trainer_node = native(
    BurnNodeTarget::Trainer,
    release_manifest,
    MyBurnWorkload { dataset_root, ema_decay: 0.995 },
    BurnWorkloadConfig::new(
        supported_workload,
        BurnArtifactConfig::burnpack(ChunkingScheme::new(64)?),
    )
    .with_root_ema(0.995),
)?
    .with_network(network_manifest)?
    .with_storage(StorageConfig::new("./burn-p2p-node"))
    .with_bootstrap_peer(validator_addr)
    .spawn()?;
```

matching validator / authority path:

```rust
let mut validator_node = native(
    BurnNodeTarget::Validator,
    release_manifest,
    MyBurnWorkload { dataset_root, ema_decay: 0.995 },
    BurnWorkloadConfig::new(
        supported_workload,
        BurnArtifactConfig::burnpack(ChunkingScheme::new(64)?),
    )
    .with_root_ema(0.995),
)?
    .with_network(network_manifest)?
    .with_storage(StorageConfig::new("./burn-p2p-validator"))
    .spawn()?;
```

thin wrappers still exist:

- `burn_p2p::burn::trainer(...)`
- `burn_p2p::burn::validator(...)`

browser mirror:

```rust
use burn_p2p_browser::{
    BrowserAppConnectConfig, BrowserAppController, BrowserAppTarget,
};

let controller = BrowserAppController::connect_with(
    BrowserAppConnectConfig::new(
        "https://edge.example",
        capability_report,
        BrowserAppTarget::Train,
    )
    .with_selection("exp-browser", Some("rev-browser")),
).await?;
```

copy real values from:

- [`crates/burn_p2p/examples/burn_ndarray_runtime.rs`](crates/burn_p2p/examples/burn_ndarray_runtime.rs)

full burn example:

- [`crates/burn_p2p/examples/burn_ndarray_runtime.rs`](crates/burn_p2p/examples/burn_ndarray_runtime.rs)

manual non-burn path:

- implement `P2pWorkload` directly
- wire `SingleWorkloadProjectFamily::new(...)` + `NodeBuilder::new(...)` yourself
- start from [`crates/burn_p2p/examples/family_workload_minimal.rs`](crates/burn_p2p/examples/family_workload_minimal.rs)

### trainer flow

```rust
use burn_p2p::{ExperimentId, RevisionId, StudyId};

let experiment = trainer_node.experiment(
    StudyId::new("study"),
    ExperimentId::new("experiment"),
    RevisionId::new("rev-1"),
);

let outcome = trainer_node.train_window_once(&experiment)?;
println!("published head {}", outcome.head.head_id.as_str());
```

the trainer joins an existing experiment revision, downloads the current base
head and assigned shards, runs one training window, and publishes a candidate
head.

you still need a validator / authority path somewhere in the network to:

- initialize the first head for the revision
- validate candidate heads
- promote accepted updates

for a complete local trainer + validator walkthrough, start with:

- [`crates/burn_p2p/examples/synthetic_trainer.rs`](crates/burn_p2p/examples/synthetic_trainer.rs)
- [`crates/burn_p2p/examples/burn_ndarray_runtime.rs`](crates/burn_p2p/examples/burn_ndarray_runtime.rs)

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
