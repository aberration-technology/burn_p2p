# Publish Order

Target prerelease line: `0.21.0-pre.2`

Topological publish order for the published library set:

1. `burn_p2p_core`
2. `burn_p2p_experiment`
3. `burn_p2p_checkpoint`
4. `burn_p2p_limits`
5. `burn_p2p_dataloader`
6. `burn_p2p_security`
7. `burn_p2p_swarm`
8. `burn_p2p_engine`
9. `burn_p2p`

Clean-tree dry-run commands:

```bash
cargo publish --dry-run -p burn_p2p_core
cargo publish --dry-run -p burn_p2p_experiment
cargo publish --dry-run -p burn_p2p_checkpoint
cargo publish --dry-run -p burn_p2p_limits
cargo publish --dry-run -p burn_p2p_dataloader
cargo publish --dry-run -p burn_p2p_security
cargo publish --dry-run -p burn_p2p_swarm
cargo publish --dry-run -p burn_p2p_engine
cargo publish --dry-run -p burn_p2p
```

Local prerelease verification before the first upstream publish should use the
release-prep script, which overlays earlier published workspace crates through a
temporary `[patch.crates-io]` cargo-home config while preserving the same
topological order.
