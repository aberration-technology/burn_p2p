# burn_p2p 🔥🤝

`burn_p2p` is the public entrypoint for peer-to-peer Burn training.

integration path:

1. burn users: start from `burn::train::Learner::new(...)`
2. custom runtimes: start from `P2pWorkload`
3. build a node and join an experiment revision
4. call `train_window_once`

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

`burn_p2p` should wrap your existing burn learner seam, not replace it.

start from a real burn `Learner`.

you keep:

- burn `TrainStep` / `InferenceStep`
- model, optimizer, scheduler
- training batches

adapter fills in:

- one-window learner loop
- learner restore from the current p2p head
- `lr_step()`, `train_step(...)`, and `optimizer_step(...)` inside each window
- model schema hashing
- model artifact load/save
- weighted merge
- auto-generated local dataset metadata when the learner keeps dataset loading local
- neutral validation metrics when you do not provide eval hooks
- optional root ema
- optional eval, dataset, shard-loader, step-metric, and window-metric hooks

minimal learner-backed path:

```rust
use burn::train::Learner;
use burn_p2p::StorageConfig;
use burn_p2p::burn::from_learner;

let mut trainer_node = from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_batches(|device| {
    build_training_batches(device)
})
.trainer(release_manifest, supported_workload)?
.with_network(network_manifest)?
.with_storage(StorageConfig::new("./burn-p2p-node"))
.with_bootstrap_peer(validator_addr)
.spawn()?;
```

`with_batches(...)` ignores p2p lease/shard assignment and just asks the local
learner path for batches.

matching validator / authority path:

```rust
let mut validator_node = from_learner(
    Learner::new(model, optimizer, scheduler),
    device,
)
.with_batches(|device| {
    build_training_batches(device)
})
.validator(release_manifest, supported_workload)?
.with_network(network_manifest)?
.with_storage(StorageConfig::new("./burn-p2p-validator"))
.spawn()?;
```

optional hooks:

- `.trainer_with_config(...)` / `.validator_with_config(...)` when you want to pass a custom `BurnWorkloadConfig`
- `.with_evaluate(...)` when you want real validation metrics instead of the neutral default
- `.with_local_dataset(...)` when you want to override the auto-generated local dataset name or sizing used by `.with_batches(...)`
- `.with_assignment_batches(...)` when local batch selection depends on the p2p assignment lease
- `.with_dataset(...)` + `.with_load_batches(...)` when you already have a shard-backed dataset path
- `.with_step_metrics(...)` / `.with_window_metrics(...)` for extra training telemetry

low-level escape hatch:

- `burn_p2p::burn::BurnLearnerWorkload`
- `burn_p2p::burn::BurnWorkload`
- use them only when the learner builder hooks are not enough

browser mirror:

```rust
use burn_p2p_browser::BrowserAppController;

let controller = BrowserAppController::train("https://edge.example", capability_report).await?;
```

shared ui/auth seams:

- shared app contract: `burn_p2p_ui::NodeAppClientView` / `NodeAppSurface`
- browser path: `BrowserAppController` builds that contract from browser runtime state
- native path: `RunningNode::app_view(...)` builds that contract from native node telemetry
- headless/native auth path: `burn_p2p::PortalAuthClient`
- same dioxus component tree now backs both browser and native portal hosts
- native desktop host is behind `burn_p2p_portal` feature `desktop-client`

native ui-facing view snapshot:

```rust
let view = trainer_node.app_view("https://edge.example", None);
```

native desktop host:

```rust,no_run
# #[cfg(feature = "desktop-client")] {
use burn_p2p_portal::{launch_node_app, NodeAppHostConfig, NodeAppHostSource};

# let trainer_node: burn_p2p::RunningNode<()> = todo!();
let source = NodeAppHostSource::from_running_node(
    &trainer_node,
    NodeAppHostConfig::new("https://edge.example"),
);

launch_node_app(source);
# }
```

headless/device auth path:

```rust
use burn_p2p::{ContentId, PortalAuthClient, PortalEnrollmentConfig};
use std::collections::BTreeSet;

let auth = PortalAuthClient::new(
    "https://edge.example",
    PortalEnrollmentConfig {
        network_id: network_id.clone(),
        project_family_id: project_family_id.clone(),
        release_train_hash: release_train_hash.clone(),
        target_artifact_id: "native-peer".into(),
        target_artifact_hash: ContentId::new("artifact"),
        login_path: "/login/oidc".into(),
        device_path: Some("/device/oidc".into()),
        callback_path: "/callback/oidc".into(),
        enroll_path: "/enroll".into(),
        trust_bundle_path: "/trust".into(),
        requested_scopes: BTreeSet::new(),
        session_ttl_secs: 3600,
    },
);

let login = auth.begin_device_login(Some("alice".into())).await?;
```

current state:

- shared typed ui state is native/browser compatible
- shared auth/session/certificate flow is native/browser compatible
- browser and native portal hosts now share one dioxus component tree
- linux desktop builds need gtk/webkit pkg-config libs when `desktop-client` is enabled

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
