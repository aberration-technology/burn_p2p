# burn_p2p 🔥🤝

`burn_p2p` is the stable public facade for peer-to-peer Burn training
orchestration.

The intended path is:

- `P2pProjectFamily`
- `P2pWorkload`
- `NodeBuilder`

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

For a complete working example, use
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

## companion crates

Operator and platform-specific surfaces also exist for:

- bootstrap and operator surfaces
- browser workers and edge clients
- artifact publication and download
- metrics indexing and portal rendering
- auth and social integrations

Start integrations from `burn_p2p`; reach for the companion crates only when
you are explicitly building deployment, browser, or operator flows.

## docs

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/downstream-burn-guide.md`](docs/downstream-burn-guide.md)
- [`docs/operator-runbook.md`](docs/operator-runbook.md)
