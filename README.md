# burn_p2p 🔥🤝

[![GitHub License](https://img.shields.io/github/license/aberration-technology/burn_p2p)](https://raw.githubusercontent.com/aberration-technology/burn_p2p/main/LICENSE-MIT)
[![crates.io](https://img.shields.io/crates/v/burn_p2p.svg)](https://crates.io/crates/burn_p2p)

`burn_p2p` turns a burn learner into a decentralized training network.

core shape:

- trainers lease shard slices, sync the latest visible canonical head, run one train window, then publish candidate updates
- reducers are non-authoritative proposal builders; they can reduce early, but validators independently rescreen the cohort and can locally re-reduce if reducer output is missing or wrong
- only validator quorum can attest a reduction, issue merge/quorum certificates, and advance the canonical head
- quarantined or revoked peers are filtered out of future trainer, reducer, and validator pools
- cheap bootstrap/coherence seeds handle ingress, discovery, relay fallback, and browser-edge http state
- the same network can include native peers, browser peers, viewers, reducers, validators, and trainer pools on different hardware classes

## install

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.43", features = ["burn"] }
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

for revisions using `TrainingProtocol::DiLoCo`, use `train_protocol_once(...)`
or `diloco_round_once(...)`. `train_window_once(...)` intentionally stays an
artifact-window primitive so artifact publication and DiLoCo checkpoint rounds
do not share an ambiguous return contract.

`burn_p2p` handles:

- head sync
- lease-scoped shard assignment
- window-by-window training publication
- checkpoint/artifact movement
- reducer proposal flow
- validator attestation and promotion
- peer discovery, relay fallback, and control-plane sync

## safety boundary

the important trust split is:

- trainer updates are candidate inputs, not canonical state
- reducer output is a proposal, not canonical state
- validator quorum is the authority boundary for canonical promotion

that means:

- a bad trainer can be rejected, downweighted, quarantined, or left out of the merge
- a bad reducer can waste time or bandwidth, but validators still recompute the expected accepted cohort locally before promotion
- if a dedicated reducer is silent or serves a mismatched aggregate, validators fall back to local reduction instead of letting the reducer stall the window
- a canonical head only moves after validator attestation and merge promotion

the main operating guidance for adversarial or semi-trusted deployments is:

- keep reducers and validators as separate roles
- run more than one validator; `validator_quorum = 1` is a lab mode, not a safe default
- use admission/auth for untrusted membership
- treat reducers as optional acceleration, not as authorities

this is a decentralized training protocol, not a full bft ledger. if validator
quorum is compromised, canonical safety is compromised too.

most deployments should separate:

- cheap bootstrap/coherence seeds
- reducer nodes
- validator / authority nodes
- trainer pools

the reference deploy shape follows that split directly:

- bootstrap seeds are public ingress/discovery nodes
- validators and reducers stay private by default
- browser edge is an explicit opt-in profile, not the baseline split-fleet role

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

## workspace map

crates:

- `burn_p2p`: public runtime facade, trainer/reducer/validator orchestration, validation, and promotion flow
- `burn_p2p_app`: reference dioxus app and browser-edge product surface
- `burn_p2p_auth_external`: trusted-ingress and provider-mapped external auth connectors
- `burn_p2p_auth_github`: github-focused auth connector wrapper with github api defaults
- `burn_p2p_auth_oauth`: generic oauth connector wrapper
- `burn_p2p_auth_oidc`: oidc connector wrapper
- `burn_p2p_bootstrap`: coherence seed, browser-edge http surface, auth issuance, and deployment-facing daemon
- `burn_p2p_browser`: browser runtime bridge and wasm transport glue
- `burn_p2p_checkpoint`: checkpoint encoding, replay, and import/export surfaces
- `burn_p2p_core`: shared ids, schema, manifests, signatures, and protocol wire types
- `burn_p2p_dataloader`: prepared-shard and lease-data-loader support
- `burn_p2p_engine`: execution/runtime engine glue used by higher-level roles
- `burn_p2p_experiment`: experiment/revision/head metadata and experiment-local helpers
- `burn_p2p_limits`: capability probing, budgets, and placement heuristics
- `burn_p2p_metrics`: runtime metrics and reporting surface
- `burn_p2p_publish`: artifact export, publication targets, and download tickets
- `burn_p2p_python`: subprocess-backed python/torch workload adapter
- `burn_p2p_security`: auth connectors, admission policy, certificate issuance, and peer auth verification
- `burn_p2p_social`: social/profile/feed-facing data surface
- `burn_p2p_swarm`: native libp2p transport, discovery, relay/rendezvous, and control-plane sync
- `burn_p2p_testkit`: deterministic harnesses, multiprocess soak tools, and integration support
- `burn_p2p_views`: query/view models used by app/bootstrap/social surfaces
- `burn_p2p_workload`: backend-neutral workload and lease-data-pipeline seam

top-level support:

- `examples/mnist_p2p_demo`: mixed-fleet burn demo used by `cargo xtask e2e mnist`
- `examples/torch_mnist_p2p_demo`: python/torch subprocess-backed mnist demo on the same runtime
- `deploy/`: reference configs, compose stacks, and cloud deployment assets
- `docs/`: protocol, operator, deployment, and roadmap docs
- `xtask/`: repo automation for ci, demos, deploy flows, and release/publish checks

same experiment layout works across native and browser peers. browser-facing
runtime and ui live in the companion crates above.

## see it working

single-machine mixed-fleet mnist sanity run:

```bash
cargo xtask e2e mnist --profile smoke --keep-artifacts
```

python/torch adapter sanity run:

```bash
cargo run --manifest-path examples/torch_mnist_p2p_demo/Cargo.toml -- \
  --root ./target/torch-mnist-demo \
  --python python3
```

best follow-up docs:

- [docs/examples/mnist.md](docs/examples/mnist.md)
- [docs/downstream-burn-guide.md](docs/downstream-burn-guide.md)
- [docs/learning-dynamics.md](docs/learning-dynamics.md)
- [docs/protocol-shape.md](docs/protocol-shape.md)
- [docs/formal-verification-plan.md](docs/formal-verification-plan.md)
- [docs/production-roadmap.md](docs/production-roadmap.md)
- [deploy/README.md](deploy/README.md)
- [docs/operator-runbook.md](docs/operator-runbook.md)
- [docs/features.md](docs/features.md)

non-burn runtime? implement `P2pWorkload` directly.
