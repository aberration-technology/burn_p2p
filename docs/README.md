# Docs

This folder intentionally keeps only durable docs.

Code, tests, examples, and release scripts are the source of truth for runtime
behavior. Transient review snapshots, dated status reports, and one-off
verification artifacts are not kept here.

## Integration

- [downstream-burn-guide.md](downstream-burn-guide.md): embedding `burn_p2p`
  into a Burn project
- [multi-workload-guide.md](multi-workload-guide.md): project-family and
  multi-workload integration
- [browser-runtime-guide.md](browser-runtime-guide.md): browser worker/runtime
  model and storage flow
- [dataloader-adapters.md](dataloader-adapters.md): adapter shapes and dataset
  registration
- [dataloader-tuning.md](dataloader-tuning.md): planner/cache tuning guidance

## Operations

- [architecture.md](architecture.md): crate boundaries and main data flow
- [operator-runbook.md](operator-runbook.md): bootstrap/operator deployment
  guidance
- [auth-setup-guide.md](auth-setup-guide.md): auth provider setup and trust
  model
- [runtime-events.md](runtime-events.md): runtime event/state taxonomy
- [leaderboard-scoring.md](leaderboard-scoring.md): leaderboard scoring rules

## Release

- [PUBLISHING.md](PUBLISHING.md): release-prep workflow and verification matrix
- [PUBLISH_MATRIX.md](PUBLISH_MATRIX.md): publishable crate set and rationale
- [PUBLISH_ORDER.md](PUBLISH_ORDER.md): topological publish order

## Code First

For fast-moving or implementation-heavy areas, prefer the code directly:

- metrics: `crates/burn_p2p_core/src/schema/metrics.rs`,
  `crates/burn_p2p/src/metrics_runtime.rs`,
  `crates/burn_p2p_metrics/src/lib.rs`
- artifact publication: `crates/burn_p2p_core/src/schema/publication.rs`,
  `crates/burn_p2p_publish/src/lib.rs`
- browser/device capability logic: `crates/burn_p2p_limits/src/lib.rs`,
  `crates/burn_p2p_testkit/tests/browser_real_device.rs`
- deployment examples: `crates/burn_p2p_bootstrap/examples/` and
  `deploy/compose/`
