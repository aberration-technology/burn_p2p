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

Seeded chaos replay:

```bash
cargo xtask stress chaos --seed 12345 --events 8 --peers 6
```

Core benches:

```bash
cargo xtask bench core
```

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
cargo xtask check publish
```

`check publish` allows dirty local worktrees by default so package and dry-run
checks remain usable while iterating. CI still runs the same path on a clean
checkout. The dry-run phase verifies the current packaged workspace crates
together, so sibling crate checks do not depend on stale crates.io copies at the
same version.

Optional thin `just` wrappers:

```bash
just check
just e2e
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
cargo xtask ci pr-fast --keep-artifacts
cargo xtask ci browser --keep-artifacts
cargo xtask ci integration --keep-artifacts
cargo xtask ci services --keep-artifacts
cargo xtask ci nightly --keep-artifacts
cargo xtask ci publish --keep-artifacts
```

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
