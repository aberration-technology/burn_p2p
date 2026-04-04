# Docs

This folder keeps only docs that help someone integrate, run, or operate the
code.

Code, tests, examples, deployment configs, and scripts are the source of
truth. Historical notes, audit snapshots, release paperwork, dependency
snapshots, and design diaries are intentionally not kept here.

## Start Here

- [ARCHITECTURE.md](ARCHITECTURE.md): stable crate boundaries, layering, and
  major end-to-end flows
- [downstream-burn-guide.md](downstream-burn-guide.md): integrating
  `burn_p2p` into a Burn project
- [operator-runbook.md](operator-runbook.md): deployment and operator guidance

## Code First

For implementation-heavy areas, prefer the code directly:

- metrics: `crates/burn_p2p_core/src/schema/metrics.rs`,
  `crates/burn_p2p/src/metrics_runtime.rs`,
  `crates/burn_p2p_metrics/src/`
- artifact publication: `crates/burn_p2p_core/src/schema/publication.rs`,
  `crates/burn_p2p_publish/src/`
- browser/device capability logic: `crates/burn_p2p_limits/src/lib.rs`,
  `crates/burn_p2p_browser/src/`,
  `crates/burn_p2p_testkit/tests/browser_real_device.rs`
- auth and admission: `crates/burn_p2p_security/src/`,
  `crates/burn_p2p_auth_*/src/`
- dataloader planning and cache/fetch flow: `crates/burn_p2p_dataloader/src/`
- deployment examples: `crates/burn_p2p_bootstrap/examples/` and
  `deploy/compose/`
