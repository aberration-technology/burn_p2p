# docs

this folder keeps only docs that help someone integrate, run, or operate the
code.

code, tests, examples, deployment configs, and scripts are the source of
truth. docs here should stay short and code-adjacent.

## primary docs

- [features.md](features.md): feature flags, crate composition, and common
  dependency shapes
- [downstream-burn-guide.md](downstream-burn-guide.md): integrating
  `burn_p2p` into a Burn project
- [learning-dynamics.md](learning-dynamics.md): how windowed p2p averaging
  learns, how contributions are weighted, and where it differs from sync ddp
- [protocol-shape.md](protocol-shape.md): reducer vs validator authority,
  canonical promotion, and the intended trust boundary
- [browser-transport-backend.md](browser-transport-backend.md): browser
  WebRTC/WebTransport backend choice, runtime state contract, and deployment
  gates
- [examples/mnist.md](examples/mnist.md): real single-machine mnist p2p demo,
  multi-node topology, browser captures, and artifact outputs
- [operator-runbook.md](operator-runbook.md): deployment and operator guidance
- [reference-assets/README.md](reference-assets/README.md): copy-edit workflow
  and deployment templates for downstream repos
- [testing/local-dev.md](testing/local-dev.md): local-first smoke, browser,
  stress, and publish-readiness commands
- [testing/ci-profiles.md](testing/ci-profiles.md): how xtask profiles map to
  CI workflows
- [testing/browser-local.md](testing/browser-local.md): Playwright, headless,
  headed, and real-browser local flows
- [testing/artifacts.md](testing/artifacts.md): artifact layout used both
  locally and in CI
- [testing/adversarial.md](testing/adversarial.md): robustness smoke, matrix,
  seeded replay, and benchmark commands

## planning records

these are status records, not configuration source of truth. keep them aligned
with code when they are useful, and delete or shorten them when implementation
has moved into tests, deploy settings, or crate APIs.

- [burn-dragon-productization-roadmap.md](burn-dragon-productization-roadmap.md):
  upstream/downstream boundary notes for `burn_dragon_p2p`
- [production-roadmap.md](production-roadmap.md): production hardening status
  and remaining operator gaps
- [network-administration-roadmap.md](network-administration-roadmap.md):
  lifecycle, scheduler, and multi-experiment administration status
- [memory-retention.md](memory-retention.md): memory-growth audit and remaining
  long-run retention boundaries
- [formal-verification-plan.md](formal-verification-plan.md): veil proof scope,
  command surface, and current verified slice

## code first

for implementation-heavy areas, prefer the code directly:

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
- backend-neutral workload seam and python adapter:
  `crates/burn_p2p_workload/src/`,
  `crates/burn_p2p_python/src/`
- dataloader planning and cache/fetch flow: `crates/burn_p2p_dataloader/src/`
- deployment examples: `crates/burn_p2p_bootstrap/examples/` and
  `deploy/`
- crate boundaries and package metadata: workspace `Cargo.toml` files under
  `crates/`
