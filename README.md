# burn_p2p

Distributed Burn training over a policy-gated peer-to-peer network.

This workspace is intentionally split into one stable facade crate, a small
set of companion crates, and a set of internal implementation crates that are
not semver promises yet. The companion/operator crates
`burn_p2p_auth_external`, `burn_p2p_auth_github`, `burn_p2p_auth_oidc`,
`burn_p2p_auth_oauth`, `burn_p2p_bootstrap`, `burn_p2p_browser`,
`burn_p2p_portal`, `burn_p2p_social`, `burn_p2p_ui`, and
`burn_p2p_testkit` are consumed directly instead of being re-exported from the
facade.

Current status:

- Verified baseline: see [docs/stabilization-baseline.md](docs/stabilization-baseline.md)
  for the exact commands and results recorded on 2026-04-03.
- Canonical public model: `burn_p2p` is now centered on
  `P2pProjectFamily` + `P2pWorkload` with workload selection through
  `NodeBuilder::with_network(...)` and `NodeBuilder::for_workload(...)`.
  Legacy `P2pProject` / `RuntimeProject` layers still exist as compatibility
  adapters, but they are no longer the forward path for docs or new examples.
- Compatibility model: networks are pinned to one project family and one
  release train, with a bounded set of approved target artifacts inside that
  train. Native and browser builds can coexist only when they share the same
  release train and exact experiment/revision/schema gates.
- Native runtime baseline: `spawn()` starts a real runtime; native peers can
  discover, admit, sync snapshots and artifacts, train leased windows, validate
  candidate heads, advance certified heads, and recover persisted state after
  restart. TCP and QUIC native control-plane paths are exercised in tests.
- Checkpoint/data baseline: the workspace has a real filesystem CAS, streamed
  chunking/materialization, pinning/GC, resumable artifact sync state, manifest-
  backed local/http shard fetch, lease-scoped shard caching, and per-experiment
  rebudgeting from observed throughput.
- Security/admission baseline: admission is certificate-based and strict.
  Static principal sessions, node certificates bound to peer IDs, trusted issuer
  validation, revocation epochs, trust-bundle rollout, and exact release-train /
  target-artifact checks are implemented and tested.
- Control-plane and topology baseline: the live runtime publishes and consumes
  directory, head, lease, reducer, merge-window, aggregate, and reduction
  announcements. The replicated-rendezvous DAG merge policy, deterministic
  reducer assignment, aggregate evidence persistence, and validator promotion
  flow are all live and covered by simulation tests.
- Browser baseline: the bootstrap daemon now serves browser-edge snapshot
  surfaces (`/portal/snapshot`, `/directory/signed`, `/leaderboard`,
  `/leaderboard/signed`, `/trust`, `/reenrollment`), and the new
  `burn_p2p_browser` crate provides typed browser portal/enrollment/storage and
  worker-state client logic. The browser worker can select authorized
  assignments, run local verify/train flows, queue and submit receipts to the
  browser edge, and the wasm compile matrix is now exercised explicitly through
  testkit. Live browser swarm join and transport-backed browser execution are
  still in progress.
- Bootstrap/operator baseline: `burn-p2p-bootstrap` exposes `/status`,
  `/metrics`, `/events`, `/diagnostics/bundle`, `/heads`, `/receipts`,
  `/reducers/load`, `/admin`, and the browser-edge routes above. It supports
  embedded runtime mode, trust-bundle export, authority rotation, provider-
  specific auth routes, RBAC-backed operator sessions, a signed edge
  capability manifest at `/capabilities` and
  `/.well-known/burn-p2p-capabilities`, optional portal/social/browser-edge
  route gating, and startup validation when config requests services that were
  not compiled in. The `x-admin-token` path is now an explicit dev-only
  fallback behind `allow_dev_admin_token`.
- Social modularization baseline: leaderboard aggregation and badge rollups now
  live in the optional `burn_p2p_social` crate. `burn_p2p_bootstrap` consumes
  it through its `social` feature and falls back to an empty snapshot when that
  feature is not compiled in.
- Auth modularization baseline: provider-specific auth connectors now live in
  dedicated optional crates. `burn_p2p_security` retains certificate/admission
  core, static auth, connector traits, and revocation/release policy logic,
  while `burn_p2p_auth_github`, `burn_p2p_auth_oidc`,
  `burn_p2p_auth_oauth`, and `burn_p2p_auth_external` provide the optional
  edge-facing identity connectors.
- Portal modularization baseline: the reference portal HTML/rendering surface
  now lives in the optional `burn_p2p_portal` crate. `burn_p2p_bootstrap`
  consumes it through its `portal` feature and minimal builds can exclude the
  portal implementation entirely.
- UI/testkit baseline: `burn_p2p_ui` remains a framework-neutral typed contract
  crate with portal, picker, trust, and participant views. `burn_p2p_testkit`
  provides topology simulation, mixed-fleet browser/native fixtures,
  deployment-profile build checks, multiprocess smoke tests, and soak/stress
  entrypoints.

Known gaps:

- GitHub/OIDC/OAuth connectors now support provider-specific bootstrap/browser
  login routes plus exchange-backed callback resolution, userinfo/profile
  hydration, and provider-side refresh/revoke hooks. The remaining auth gap is
  full upstream provider integration beyond this generic edge contract, such as
  real token exchange discovery/metadata and provider-native revocation policy.
- Browser support now covers portal, picker, worker state, local verify/train,
  receipt outbox, receipt submission, mixed native+browser worker coverage,
  live browser-client-to-edge HTTP coverage, and wasm compile coverage. Live
  browser swarm join and transport-backed browser execution are still not
  complete.
- The reference browser portal is now split into the dedicated
  `burn_p2p_portal` crate, but it is still intentionally lightweight HTML plus
  typed contracts rather than a fuller product UI.
- Benchmark baselines are now recorded, and the latest security benchmark rerun
  shows broad improvement across release-policy, auth-admission,
  validator-policy, and reputation paths.

Quick start surfaces:

- Library example: `cargo run -p burn_p2p --example synthetic_trainer`
- Burn facade example: `cargo run -p burn_p2p --example burn_ndarray_runtime --features burn`
- Bootstrap daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/local-bootstrap.json`
- Auth-enabled bootstrap daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/authenticated-bootstrap.json`
- Trusted-minimal example: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/trusted-minimal.json`
- Enterprise SSO example: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/enterprise-sso.json`
- Trusted-browser example: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/trusted-browser.json`
- Community-web example: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/community-web.json`
- Embedded validator daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/embedded-validator.json`
- Downstream-style embedded daemon example: `cargo run -p burn_p2p_bootstrap --example embedded_runtime_daemon`
- Edge capability manifest: `curl http://127.0.0.1:9000/capabilities`
- Browser client crate tests: `cargo test --manifest-path Cargo.toml -p burn_p2p_browser`
- Social service crate tests: `cargo test --manifest-path Cargo.toml -p burn_p2p_social`
- Portal crate tests: `cargo test --manifest-path Cargo.toml -p burn_p2p_portal`
- Auth connector crate tests:
  `cargo test --manifest-path Cargo.toml -p burn_p2p_auth_external -p burn_p2p_auth_github -p burn_p2p_auth_oidc -p burn_p2p_auth_oauth`
- Browser client-to-edge HTTP e2e: `cargo test --manifest-path Cargo.toml -p burn_p2p_bootstrap --bin burn-p2p-bootstrap browser_portal_client_round_trips_against_live_http_router`
- Browser provider-exchange auth e2e: `cargo test --manifest-path Cargo.toml -p burn_p2p_bootstrap --bin burn-p2p-bootstrap browser_portal_client_completes_github_login_via_exchange_callback`
- Browser wasm matrix: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test browser_matrix -- --ignored`
- Deployment profile matrix: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test deployment_profiles -- --ignored`
- Mixed native+browser worker test: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test mixed_browser_worker`
- Trusted-minimal bootstrap compile: `cargo check --manifest-path Cargo.toml -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static`
- Trusted-minimal bootstrap tree hygiene:
  `cargo tree --manifest-path Cargo.toml -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static -e normal`
- Security-core dependency hygiene:
  `cargo tree --manifest-path Cargo.toml -p burn_p2p_security -e normal`
- Workspace tests: `cargo test --manifest-path Cargo.toml --workspace --features burn`
- Multi-process smoke test: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test multiprocess`
- Soak runner: `cargo run -p burn_p2p_testkit --bin burn-p2p-testkit-soak -- crates/burn_p2p_testkit/examples/native-soak.json`
- Merge-topology simulator tests: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit merge_topology`
- Checkpoint artifact pipeline benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_checkpoint --bench artifact_pipeline`
- Burn module merge benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_engine --bench module_merge`
- Merge-topology simulation benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_testkit --bench merge_topology`
- Swarm control-plane benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_swarm --bench control_plane`
- Dataloader planning benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_dataloader --bench planning`
- Limits calibration benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_limits --bench capability`
- Security auth/admission benchmarks:
  `cargo bench --manifest-path Cargo.toml -p burn_p2p_security --bench security`
- 32-trainer stress entrypoint:
  `cargo run -p burn_p2p_testkit --bin burn-p2p-testkit-soak -- crates/burn_p2p_testkit/examples/native-stress-32.json`
- Multi-process fan-in stress test: `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test multiprocess validator_can_fan_in_many_process_trainers`
- Multi-process soak test entrypoints:
  `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test multiprocess native_process_soak_runner_reports_multi_window_progress`
  and
  `cargo test --manifest-path Cargo.toml -p burn_p2p_testkit --test multiprocess native_process_soak_runner_reports_concurrent_round_progress`
