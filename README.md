# burn_p2p 🔥🤝

`burn_p2p` is the public entrypoint for peer-to-peer Burn training.

start with `P2pProjectFamily`, `P2pWorkload`, and `NodeBuilder`.

## install

```toml
[dependencies]
burn_p2p = "=0.21.0-pre.4"
```

for the Burn-backed runtime path and examples:

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.4", features = ["burn"] }
```

## usage

the model is family/workload first:

```rust
use burn_p2p::{
    GenesisSpec, NodeBuilder, SingleWorkloadProjectFamily, StorageConfig,
};

let family = SingleWorkloadProjectFamily::new(release_manifest, workload)?;

let node = NodeBuilder::new(family)
    .with_mainnet(GenesisSpec {
        network_id,
        protocol_version,
        display_name,
        created_at,
        metadata,
    })
    .with_storage(StorageConfig::new("./burn-p2p-node"))
    .spawn()?;
```

full example:
[`crates/burn_p2p/examples/family_workload_minimal.rs`](crates/burn_p2p/examples/family_workload_minimal.rs).

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
