# burn_p2p 🔥🤝

[![GitHub License](https://img.shields.io/github/license/aberration-technology/burn_p2p)](https://raw.githubusercontent.com/aberration-technology/burn_p2p/main/LICENSE-MIT)
[![crates.io](https://img.shields.io/crates/v/burn_p2p.svg)](https://crates.io/crates/burn_p2p)

`burn_p2p` turns a burn learner into a decentralized training network.

core shape:

- trainers lease shard slices, sync the current head, run one train window, then publish update artifacts
- reducers combine eligible updates into aggregate proposals
- validators attest accepted proposals and promote merged heads
- cheap bootstrap/coherence seeds handle ingress, discovery, relay fallback, and browser-edge http state
- the same network can include native peers, browser peers, viewers, reducers, validators, and trainer pools on different hardware classes

## install

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.7", features = ["burn"] }
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
    validation_loader,
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

let mut session = trainer.continuous_trainer(&experiment)?;
let outcome = session.train_next_window()?;
println!("published {}", outcome.head.head_id.as_str());
```

keep your existing burn model, optimizer, scheduler, and loaders.

use `train_window_once(...)` instead when you want a single strictly
orchestrated training window with no retained session state.

`burn_p2p` handles:

- head sync
- lease-scoped shard assignment
- window-by-window training publication
- checkpoint/artifact movement
- reducer proposal flow
- validator attestation and promotion
- peer discovery, relay fallback, and control-plane sync

most deployments should separate:

- cheap bootstrap/coherence seeds
- reducer nodes
- validator / authority nodes
- trainer pools

for trainer nodes, `from_loaders(...)` is still the main public entrypoint. use
`from_learner(...)` for reducer, validator, viewer, and helper-style runtime
roles.

## data

one lease is one micro-epoch. that is the unit that drives publish cadence and
canonical reconcile.

use `with_sharded_dataset(...)` when data already lives as prepared shard
files.

use `LeaseDataPipeline<Device, Batch>` when batches should be rebuilt from
indices, samplers, seeds, recipes, or custom lease metadata.

pipeline kinds stay simple:
- `ShardedStatic`: shard files
- `IndexedDataset`: dataset + sampler scope
- `GeneratedDataset`: deterministic generation
- `Custom`: anything else

burn uses `.with_data_pipeline(...)`. python/torch uses
`PythonTorchProject::new_with_data_pipeline(...)`.

both adapters expose the same inspection surface:
- `data_pipeline_descriptor()`
- `data_pipeline_kind()`
- `local_upstream_root()`

`local_upstream_root()` only returns `Some(...)` for local shard-backed
pipelines.

native peers exchange control-plane state, heads, checkpoints, and artifacts
over the peer network.

browser peers fetch only the active lease-scoped shard data through the browser
edge. today that path is peer-backed (`p2p-artifact-via-edge`): native peers
sync the prepared shard bundle over the overlay, and the edge serves only the
leased slice to the browser.

## what the repo includes

- `burn_p2p`: core runtime, burn-facing facade, training, validation, and promotion flow
- `burn_p2p_swarm`: native transport, discovery, relay/rendezvous integration, and control-plane event model
- `burn_p2p_bootstrap`: coherence-seed, reducer/validator deployment surface, and browser-edge http/admin surface
- `burn_p2p_browser`: browser runtime bridge and wasm-facing transport glue
- `burn_p2p_app`: reference dioxus app and browser-edge product surface
- `examples/mnist_p2p_demo`: real downstream-style mixed-fleet demo used by `cargo xtask e2e mnist`
- `examples/torch_mnist_p2p_demo`: python/torch subprocess-backed mnist demo using the same p2p runtime

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
- [docs/learning-dynamics.md](docs/learning-dynamics.md)
- [docs/features.md](docs/features.md)

non-burn runtime? implement `P2pWorkload` directly.
