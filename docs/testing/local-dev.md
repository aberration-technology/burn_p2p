# Local Dev Testing

The local command surface is `cargo xtask`. CI calls the same commands with CI
profiles.

## Start Here

```bash
cargo xtask doctor
cargo xtask check
cargo xtask e2e smoke
```

## Common Commands

Fast local checks:

```bash
cargo xtask check
```

Native smoke E2E:

```bash
cargo xtask e2e smoke
```

This includes the default native smoke cluster plus one metrics catchup check
and one publication/download smoke.

Mixed native/browser smoke:

```bash
cargo xtask e2e mixed
```

Downstream MNIST sanity:

```bash
cargo xtask e2e mnist
```

This runs the standalone `examples/mnist_p2p_demo` crate on one machine,
builds browser portal captures, runs Playwright, and writes a summary proving
real shard assignment, checkpoint sync, custom metrics, and baseline-vs-low-lr
experiment behavior. it also asserts lease-scoped browser shard fetch over the
configured signed-bundle/http path, live browser burn/webgpu wasm execution,
latency-shaped browser dataset fetch,
browser runtime role drills, adversarial annex checks, and trainer restart
recovery.

Publication and metrics smoke:

```bash
cargo xtask e2e services
```

Use this when you want the focused services-only rerun path without the broader
native smoke cluster.

Browser smoke:

```bash
cargo xtask setup browser
cargo xtask browser smoke
```

Browser trainer smoke:

```bash
cargo xtask browser trainer
```

Real browser probe:

```bash
cargo xtask browser real --profile real-browser
```

Multiprocess stress:

```bash
cargo xtask stress multiprocess --peers 8 --duration 90s
```

This churn-oriented soak treats validator-side merge progress as the success
criterion. Trainer-side canonical-head observation is still recorded when it
lands in time, but it is best-effort so one lagging trainer does not fail the
entire fleet stress lane.

Seeded chaos replay:

```bash
cargo xtask stress chaos --seed 12345 --events 8 --peers 6
```

Core benches:

```bash
cargo xtask bench core
```

Network dynamics bench:

```bash
cargo xtask bench network
```

This runs a metrics-backed native synthetic soak plus a larger-fleet discovery
simulation and writes deployment-shaped performance artifacts such as
train/validation throughput, idle ratios, startup/sync/convergence timing, and
peer-visibility continuity into `metrics/network-bench-summary.json`. The
native soak now keeps trainers alive across multiple windows so the bench
captures steady-state lease reuse and next-window prefetch behavior instead of
mostly measuring cold restarts.

Native accelerator bench:

```bash
cargo xtask bench accelerator
```

This runs a real native Burn autodiff probe against `cuda` or `wgpu` and writes
`metrics/native-accelerator-summary.json`. The default backend request is
`auto`, which tries `cuda` first and then `wgpu`. Override it with:

```bash
export BURN_P2P_NATIVE_ACCELERATOR_BACKEND=wgpu   # or cuda / auto
export BURN_P2P_NATIVE_ACCELERATOR_MATRIX_SIZE=1024
export BURN_P2P_NATIVE_ACCELERATOR_WARMUP=3
export BURN_P2P_NATIVE_ACCELERATOR_ITERATIONS=10
```

On machines without a supported native accelerator, the lane records `skipped`
instead of failing the whole bench run.

Adversarial smoke and matrix:

```bash
cargo xtask adversarial smoke
cargo xtask adversarial matrix
```

Seeded adversarial replay:

```bash
cargo xtask adversarial chaos --seed 12345 --events 8 --peers 12
```

Robustness benchmarks:

```bash
cargo xtask bench robust
```

Release readiness:

```bash
cargo install cargo-deny --locked
cargo xtask check publish
```

Dry-run the full publish sequence in dependency order:

```bash
cargo xtask publish --dry-run
```

Publish the full workspace release after re-running publish-readiness checks:

```bash
cargo xtask publish
```

Resume a partial publish from one crate:

```bash
cargo xtask publish --from burn_p2p_browser
```

Live OIDC validation against a real IdP:

```bash
export BURN_P2P_REAL_OIDC_ISSUER=...
export BURN_P2P_REAL_OIDC_CLIENT_ID=...
export BURN_P2P_REAL_OIDC_ID_TOKEN=...
export BURN_P2P_REAL_OIDC_EXPECTED_SUBJECT=...
cargo xtask check auth-oidc-live --profile nightly
```

Optional env vars such as `BURN_P2P_REAL_OIDC_ACCESS_TOKEN`,
`BURN_P2P_REAL_OIDC_USERINFO_URL`, `BURN_P2P_REAL_OIDC_JWKS_URL`,
`BURN_P2P_REAL_OIDC_EXPECTED_EMAIL`, and
`BURN_P2P_REAL_OIDC_EXPECTED_DISPLAY_NAME` let the same lane validate live
userinfo hydration and stricter claim mapping against a corporate IdP.

Redis-backed browser-edge auth HA and failure-mode validation:

```bash
cargo xtask check auth-redis-live --profile nightly
```

This lane requires `redis-server` on `PATH`. It exercises shared pending-login
state across edges plus restart, lock-contention, and transient-loss behavior.

`check publish` allows dirty local worktrees by default so package and dry-run
checks remain usable while iterating. `xtask publish` is stricter: it requires a
clean worktree unless you pass `--allow-dirty`, because it is the real crates.io
publisher rather than a packaging check. The dry-run phase verifies the current
packaged workspace crates together, so sibling crate checks do not depend on
stale crates.io copies at the same version.

Optional thin `just` wrappers:

```bash
just check
just e2e
just e2e-mnist
just e2e-mixed
just e2e-services
just browser-smoke
just browser-real
just stress 8
just chaos 12345
just bench
just adversarial-smoke
just adversarial-matrix
just adversarial-chaos 12345
just bench-robust
just release-check
just ci-pr
just ci-browser
just ci-integration
just ci-services
just ci-nightly
just ci-publish
```

Exact CI lanes are also runnable locally through the canonical runner:

```bash
cargo xtask ci local-contract --keep-artifacts
cargo xtask ci pr-fast --keep-artifacts
cargo xtask ci browser --keep-artifacts
cargo xtask ci integration --keep-artifacts
cargo xtask ci services --keep-artifacts
cargo xtask ci nightly --keep-artifacts
cargo xtask ci publish --keep-artifacts
```

Use `ci local-contract` before deploy-facing changes. It is heavier than
`pr-fast`, but still local-first: mixed browser/native E2E, services
publication checks, adversarial smoke, bounded multiprocess stress, and chaos
run under one artifact summary.

## Artifacts

Every E2E, browser, stress, and bench run writes a predictable artifact root:

`target/test-artifacts/<suite>/<run-id>/`

Each run includes:

- `summary.json`
- `summary.md`
- `stdout/`
- `stderr/`
- `configs/`
- `screenshots/`
- `playwright-traces/`
- `metrics/`
- `publication/`
- `topology/`
- `ports.json`
- `seed.txt`

The command prints the artifact path at the end of the run.
