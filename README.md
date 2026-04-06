# burn_p2p 🔥🤝

[![GitHub License](https://img.shields.io/github/license/aberration-technology/burn_p2p)](https://raw.githubusercontent.com/aberration-technology/burn_p2p/main/LICENSE-MIT)
[![crates.io](https://img.shields.io/crates/v/burn_p2p.svg)](https://crates.io/crates/burn_p2p)

`burn_p2p` turns a burn learner into a decentralized training network.

core shape:

- trainers download the current head and assigned data shard, run one train window, then publish a candidate head
- validators evaluate candidates and promote accepted updates
- the same network can include native peers, browser peers, viewers, and validators

## install

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.5", features = ["burn"] }
```

## happy path

```rust
use burn::train::Learner;
use burn_p2p::{ExperimentId, RevisionId, StorageConfig, StudyId};
use burn_p2p::burn::from_loaders;

let mut trainer = from_loaders(
    Learner::new(model, optimizer, scheduler),
    device,
    train_loader,
    eval_loader,
)
.trainer(release_manifest, supported_workload)?
.with_network(network_manifest)?
.with_storage(StorageConfig::new("./node"))
.with_bootstrap_peer(validator_addr)
.spawn()?;

let experiment = trainer.experiment(
    StudyId::new("study"),
    ExperimentId::new("mnist"),
    RevisionId::new("rev-1"),
);

let outcome = trainer.train_window_once(&experiment)?;
println!("published {}", outcome.head.head_id.as_str());
```

keep your existing burn model, optimizer, scheduler, and loaders.

`burn_p2p` handles:

- head sync
- window-by-window training publication
- checkpoint/artifact movement
- validator promotion flow
- assigned shard fetch for distributed runs

one validator/authority node must exist somewhere on the network to initialize
and promote heads.

## data

trainers fetch only their assigned shard:
`with_sharded_dataset(...)`.

> browser peers fetch assigned shards from an http dataset origin, not over the
p2p overlay.

## what the repo includes

- `burn_p2p`: core runtime
- `burn_p2p_browser`: browser runtime
- `burn_p2p_bootstrap`: bootstrap/browser-edge http surface
- `burn_p2p_app`: reference dioxus ui

same experiment layout works across native and browser peers. browser-facing
runtime and ui live in the companion crates above.

## see it working

single-machine mixed-fleet mnist sanity run:

```bash
cargo xtask e2e mnist --profile smoke --keep-artifacts
```

best follow-up docs:

- [docs/examples/mnist.md](docs/examples/mnist.md)
- [docs/downstream-burn-guide.md](docs/downstream-burn-guide.md)
- [docs/features.md](docs/features.md)

non-burn runtime? implement `P2pWorkload` directly.
