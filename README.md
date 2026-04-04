# burn_p2p 🔥🤝

`burn_p2p` is the stable public facade for peer-to-peer Burn training
orchestration.

The forward path is:

- `P2pProjectFamily`
- `P2pWorkload`
- `NodeBuilder`

The repository also contains browser, bootstrap, portal, metrics, publish, auth,
and other operator-facing workspace crates, but new downstream integrations
should start from `burn_p2p`.

## install

```toml
[dependencies]
burn_p2p = "=0.21.0-pre.2"
```

If you want the Burn-backed runtime facade and examples, enable the `burn`
feature:

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.2", features = ["burn"] }
```

## usage

The public model is family/workload oriented. A minimal integration looks like:

```rust
use burn_p2p::{
    GenesisSpec, NodeBuilder, ProjectBackend, SingleWorkloadProjectFamily,
    StorageConfig,
};

#[derive(Clone, Debug)]
struct Backend;

impl ProjectBackend for Backend {
    type Device = String;
}

let family = SingleWorkloadProjectFamily::<Backend, _>::new(
    release_manifest,
    workload,
)?;

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

For a complete working example, use
[`crates/burn_p2p/examples/family_workload_minimal.rs`](crates/burn_p2p/examples/family_workload_minimal.rs).

Legacy compatibility layers still exist under `burn_p2p::compat`, but they are
compatibility adapters, not the recommended entry point for new code.

## examples

```bash
# minimal family/workload integration
cargo run -p burn_p2p --example family_workload_minimal

# synthetic runtime walkthrough
cargo run -p burn_p2p --example synthetic_trainer

# burn-backed facade example
cargo run -p burn_p2p --example burn_ndarray_runtime --features burn
```

## workspace shape

This repository includes more than the crates.io facade crate. The workspace also
contains internal or operator-oriented companions for:

- bootstrap and operator surfaces
- browser workers and edge clients
- artifact publication and download
- metrics indexing and portal rendering
- auth and social integrations

Those crates are useful inside the workspace, but they are not the primary
starting point for downstream consumers.

## docs

Durable docs live under [`docs/README.md`](docs/README.md).

Useful starting points:

- [`docs/downstream-burn-guide.md`](docs/downstream-burn-guide.md)
- [`docs/multi-workload-guide.md`](docs/multi-workload-guide.md)
- [`docs/browser-runtime-guide.md`](docs/browser-runtime-guide.md)
- [`docs/operator-runbook.md`](docs/operator-runbook.md)

For fast-moving implementation details, prefer the code and tests over prose.
