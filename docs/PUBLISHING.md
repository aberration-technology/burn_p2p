# Publishing

This document records the publish-readiness gate for the `0.21.0-pre.2`
release line.

## Publish Set

Published crates:

- `burn_p2p_core`
- `burn_p2p_experiment`
- `burn_p2p_checkpoint`
- `burn_p2p_limits`
- `burn_p2p_dataloader`
- `burn_p2p_security`
- `burn_p2p_swarm`
- `burn_p2p_engine`
- `burn_p2p`

Unpublished workspace crates:

- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oauth`
- `burn_p2p_auth_oidc`
- `burn_p2p_bootstrap`
- `burn_p2p_browser`
- `burn_p2p_metrics`
- `burn_p2p_portal`
- `burn_p2p_publish`
- `burn_p2p_social`
- `burn_p2p_testkit`
- `burn_p2p_ui`

See [PUBLISH_MATRIX.md](PUBLISH_MATRIX.md) for the rationale and
[PUBLISH_ORDER.md](PUBLISH_ORDER.md) for the topological dry-run order.

## Release Prep Command

Clean tree:

```bash
./scripts/release_prep.sh
```

Local verification in a dirty prerelease worktree:

```bash
ALLOW_DIRTY=1 ./scripts/release_prep.sh
```

When `ALLOW_DIRTY=1` is set, the script also enables local prerelease
`cargo publish --dry-run` verification through a temporary `[patch.crates-io]`
`CARGO_HOME` overlay so unpublished earlier crates in the same release line are
resolved from the workspace instead of crates.io.

## Verification Matrix

The release-prep script runs, in order:

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo clippy -p burn_p2p --all-targets --features burn -- -D warnings`
4. `cargo clippy -p burn_p2p_swarm --all-targets -- -D warnings`
5. `cargo clippy -p burn_p2p_security --all-targets -- -D warnings`
6. `cargo clippy -p burn_p2p_publish --all-targets --features fs,s3 -- -D warnings`
7. `cargo clippy -p burn_p2p_bootstrap --all-targets -- -D warnings`
8. `cargo clippy -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static --all-targets -- -D warnings`
9. `cargo clippy -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,portal,browser-edge,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social --all-targets -- -D warnings`
10. `cargo test --workspace`
11. `cargo test -p burn_p2p --features burn`
12. `cargo test -p burn_p2p_publish --features fs,s3`
13. `cargo test -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static`
14. `cargo test -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,portal,browser-edge,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social`
15. `RUSTDOCFLAGS="-D warnings" cargo test --workspace --doc`
16. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
17. `cargo package --list -p <crate>` for each published crate in publish order
18. `cargo publish --dry-run -p <crate>` for each published crate in publish order

## Recorded Results

Recorded on 2026-04-04 from `/home/mosure/repos/burn_p2p`:

- `ALLOW_DIRTY=1 ./scripts/release_prep.sh`: passed end to end
- `cargo fmt --all --check`: passed
- `cargo clippy --workspace --all-targets -- -D warnings`: passed
- targeted clippy matrix from the script: passed
- `cargo test --workspace`: passed
- targeted test matrix from the script: passed
- `RUSTDOCFLAGS="-D warnings" cargo test --workspace --doc`: passed
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`: passed
- `cargo package --list --allow-dirty -p <crate>` for every published crate:
  passed
- `cargo publish --dry-run --allow-dirty -p burn_p2p_core`: passed
- `cargo publish --dry-run --allow-dirty -p burn_p2p_experiment`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_checkpoint`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_limits`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_dataloader`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_security`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_swarm`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p_engine`: passed via
  temporary prerelease patch overlay
- `cargo publish --dry-run --allow-dirty -p burn_p2p`: passed via temporary
  prerelease patch overlay

During the local prerelease dry-run phase, Cargo may print non-fatal warnings
about unused temporary `[patch.crates-io]` entries for crates that are earlier
in the publish order but not required by the current crate's dependency graph.
Those warnings are expected under the local overlay strategy and did not affect
any successful dry-run result above.

## Packaging Expectations

- All published crates use `version = "0.21.0-pre.2"`.
- All intra-workspace published dependencies use exact prerelease matching:
  `version = "=0.21.0-pre.2"`.
- Published crates carry repository, readme, rust-version, license, keywords,
  and categories metadata.
- Published crates do not depend on unpublished workspace crates in normal
  dependencies.
- Internal companion/service crates remain explicitly `publish = false`.

## Release Checklist

1. Run `./scripts/release_prep.sh` on a clean tree.
2. Confirm `docs/PUBLISH_MATRIX.md`, `docs/PUBLISH_ORDER.md`, and this document
   still match the current dependency graph.
3. Confirm the README installation snippet still points to
   `burn_p2p = "=0.21.0-pre.2"`.
4. Publish crates in the order from [PUBLISH_ORDER.md](PUBLISH_ORDER.md).
5. After publication, tag the release and verify the docs.rs builds for the
   public facade and supporting library crates.
