# Stabilization Baseline

Date: 2026-04-03  
Workspace root: `/home/mosure/repos/burn_p2p`

This document records the current stabilization baseline for the native runtime
plus the browser-edge/client work already landed. It is based on commands that
were actually run against the checked-out tree, not source inspection alone.

## Scope

This baseline freezes the current workspace shape for the next phase:

- `burn_p2p`
- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_bootstrap`
- `burn_p2p_browser`
- `burn_p2p_engine`
- `burn_p2p_checkpoint`
- `burn_p2p_core`
- `burn_p2p_dataloader`
- `burn_p2p_experiment`
- `burn_p2p_limits`
- `burn_p2p_portal`
- `burn_p2p_security`
- `burn_p2p_social`
- `burn_p2p_swarm`
- `burn_p2p_testkit`
- `burn_p2p_ui`

The current implementation baseline is:

- strict certificate-based admission
- project-family/workload public model
- release-train plus target-artifact compatibility gating
- filesystem CAS and manifest/chunk artifact movement
- validator promotion and replicated-rendezvous merge topology
- bootstrap/admin daemon with browser-edge routes and reference portal HTML
- provider-specific auth extracted into optional auth connector crates
- reference portal rendering extracted into optional `burn_p2p_portal`
- bootstrap composition-root capability manifest plus optional-service startup
  validation and route gating
- optional social aggregation extracted into `burn_p2p_social`
- browser client crate for portal/enrollment/storage/worker state
- named deployment profiles codified as explicit testkit feature bundles

## Commands Run

The following commands were run successfully against this tree:

```bash
cargo fmt --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml --all
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_auth_external -p burn_p2p_auth_github -p burn_p2p_auth_oidc -p burn_p2p_auth_oauth -p burn_p2p_security -p burn_p2p_bootstrap
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_bootstrap
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_bootstrap --bin burn-p2p-bootstrap browser_portal_client_completes_github_login_via_exchange_callback
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_bootstrap --bin burn-p2p-bootstrap browser_portal_client_round_trips_against_live_http_router
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_core schema::tests::edge_service_manifest_round_trips
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_browser
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_portal
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_social
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml --workspace --features burn
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_testkit --test multiprocess
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_testkit --test mixed_browser_worker
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_testkit --test browser_matrix -- --ignored
cargo test --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_testkit --test deployment_profiles -- --ignored
cargo check --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_security
cargo tree --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_security -e normal
cargo check --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static
cargo tree --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static -e normal
cargo clippy --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml --workspace --features burn --all-targets -- -D warnings
cargo bench --manifest-path /home/mosure/repos/burn_p2p/Cargo.toml -p burn_p2p_security --bench security -- --sample-size 10
```

## Recorded Results

### Workspace tests

`cargo test --workspace --features burn` passed.

Observed per-target counts from the run:

- `burn_p2p`: 39 passed
- `burn_p2p_auth_external`: 1 passed
- `burn_p2p_auth_github`: 1 passed
- `burn_p2p_auth_oidc`: 1 passed
- `burn_p2p_auth_oauth`: 1 passed
- `burn_p2p_bootstrap` lib: 14 passed
- `burn_p2p_bootstrap` bin: 16 passed
- `burn_p2p_browser`: 17 passed
- `burn_p2p_checkpoint`: 13 passed
- `burn_p2p_core`: 12 passed
- `burn_p2p_dataloader`: 7 passed
- `burn_p2p_engine`: 8 passed
- `burn_p2p_experiment`: 14 passed
- `burn_p2p_limits`: 6 passed
- `burn_p2p_portal`: 2 passed
- `burn_p2p_security`: 8 passed
- `burn_p2p_social`: 3 passed
- `burn_p2p_swarm`: 18 passed
- `burn_p2p_testkit`: 14 passed
- `burn_p2p_testkit` mixed browser worker: 1 passed
- `burn_p2p_testkit` multiprocess: 5 passed
- `burn_p2p_ui`: 9 passed

Total non-doc tests observed in the full workspace run: 210 passed.  
Current listed non-doc tests in the workspace inventory: 216.

### Multiprocess smoke suite

`cargo test -p burn_p2p_testkit --test multiprocess` passed with 5/5 tests:

- `smoke_cluster_runs_across_processes`
- `native_process_soak_runner_reports_concurrent_round_progress`
- `native_process_soak_runner_reports_multi_window_progress`
- `validator_can_fan_in_many_process_trainers`
- `validator_restart_restores_head_across_processes`

### Browser matrix

`cargo test -p burn_p2p_testkit --test browser_matrix -- --ignored` passed
with 2/2 checks:

- `browser_crate_compiles_for_wasm_target`
- `browser_supporting_crates_compile_for_wasm_target`

The browser matrix validates the wasm32 compile path for:

- `burn_p2p_browser`
- `burn_p2p_swarm`
- `burn_p2p_dataloader`

### Deployment profile matrix

`cargo test -p burn_p2p_testkit --test deployment_profiles -- --ignored`
passed with 4/4 checks:

- `trusted_minimal_profile_builds_without_optional_stacks`
- `enterprise_sso_profile_builds_with_oidc_and_portal_only`
- `community_web_profile_builds_with_browser_and_social`
- `mixed_native_browser_profile_builds_without_social`

This matrix now codifies the supported modularized deployment bundles instead
of leaving them as documentation only.

### Browser client-to-edge HTTP coverage

`cargo test -p burn_p2p_bootstrap --bin burn-p2p-bootstrap
browser_portal_client_round_trips_against_live_http_router` passed.

This test exercises a real `burn_p2p_browser::BrowserPortalClient` against the
bootstrap HTTP router over localhost, including:

- portal snapshot fetch
- login and callback completion
- trust-bundle fetch
- browser enrollment
- scoped directory fetch
- signed directory and leaderboard fetch

### Provider-exchange callback coverage

`cargo test -p burn_p2p_bootstrap --bin burn-p2p-bootstrap
browser_portal_client_completes_github_login_via_exchange_callback` passed.

This test exercises the bootstrap edge with a mock provider exchange endpoint
and proves that:

- a GitHub login can start through the browser client
- callback completion can resolve principal identity through provider code
- the bootstrap edge issues a provider-specific session after exchange-backed
  principal resolution plus provider-driven profile hydration
- receipt submission
- session refresh and logout

### Capability manifest and service-gating coverage

`cargo test -p burn_p2p_bootstrap` now covers the bootstrap composition-root
modularization surface, including:

- signed `EdgeServiceManifest` export at `/capabilities`
- signed `EdgeServiceManifest` export at
  `/.well-known/burn-p2p-capabilities`
- typed compiled/configured/active service-set projection
- disabled portal/social/browser-edge routes returning `404`
- startup validation rejecting config that requests services not compiled into
  the current binary

### Mixed native+browser worker coverage

`cargo test -p burn_p2p_testkit --test mixed_browser_worker` passed.

This test exercises a real `burn_p2p_browser::BrowserWorkerRuntime` against a
`burn_p2p_testkit::SimulationRunner` outcome, including:

- mixed-fleet browser fixture selection from the simulation harness
- browser-authorized directory projection
- trainer assignment and local training receipt generation
- verifier receipt generation
- receipt outbox flush and acknowledgment flow

### Social modularization coverage

`cargo test -p burn_p2p_social` passed with 3/3 tests.

This new optional crate now owns:

- receipt-driven leaderboard aggregation
- badge rollup logic
- no-op social services for trusted/minimal deployments
- profile-service interfaces for future optional profile backends

`burn_p2p_bootstrap` now depends on `burn_p2p_social` only through its
`social` feature.

### Auth modularization coverage

`cargo test -p burn_p2p_auth_external -p burn_p2p_auth_github -p
burn_p2p_auth_oidc -p burn_p2p_auth_oauth -p burn_p2p_security` passed.

Provider-specific auth now lives in:

- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`

`burn_p2p_security` retains only certificate/admission/release/revocation
core plus connector traits and the static connector.

### Portal modularization coverage

`cargo test -p burn_p2p_portal -p burn_p2p_bootstrap` passed.

The reference portal HTML/rendering surface now lives in `burn_p2p_portal`,
and `burn_p2p_bootstrap` consumes it through its optional `portal` feature.

### Trusted-minimal dependency-hygiene coverage

`cargo check -p burn_p2p_bootstrap --no-default-features --features
admin-http,metrics,auth-static` passed.

The corresponding normal dependency tree from `cargo tree` did not include:

- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_portal`
- `burn_p2p_social`
- `burn_p2p_browser`
- `burn_p2p_ui`

This is the current proof that a trusted-minimal bootstrap build can exclude
the optional social/browser product stack at the normal dependency layer.

### Security-core dependency-hygiene coverage

`cargo check -p burn_p2p_security` passed.

The corresponding normal dependency tree from `cargo tree -p burn_p2p_security
-e normal` did not include:

- `reqwest`
- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_browser`
- `burn_p2p_portal`
- `burn_p2p_social`
- `burn_p2p_ui`

This is the current proof that the security core can build without optional
auth, portal, browser, or social stacks.

### Linting

`cargo clippy --workspace --features burn --all-targets -- -D warnings` passed.

### Benchmarks

`cargo bench -p burn_p2p_security --bench security -- --sample-size 10`
completed successfully.

Selected observed timing ranges from the run:

- `security_release_policy/evaluate_client/4`: 21.958 ns to 22.138 ns
- `security_release_policy/evaluate_client/32`: 47.742 ns to 48.393 ns
- `security_release_policy/evaluate_client/128`: 184.38 ns to 188.50 ns
- `security_auth_admission/verify_peer_auth/1`: 51.805 us to 52.022 us
- `security_auth_admission/issue_certificate/1`: 16.606 us to 16.766 us
- `security_auth_admission/static_connector_roundtrip`: 1.2189 us to 1.2360 us
- `security_validator_policy/audit_data_receipt/128`: 5.7651 us to 5.8329 us
- `security_reputation/apply_observation/4096`: 46.902 ns to 47.532 ns

The latest rerun improved the release-policy, auth-admission,
validator-policy, and reputation paths against the stored baseline.

## Current Product Baseline

### Native runtime

- Real `spawn()` path and restart-safe persisted runtime state
- Strict certificate admission with trusted issuers and revocation epochs
- Native TCP and QUIC control-plane shells
- Live train/validate/head-sync flows
- Exact experiment/revision/schema compatibility checks

### Browser-facing baseline

- Browser HTML portal route at `/portal`
- Browser-edge portal snapshot route
- Signed directory snapshot route
- Signed leaderboard snapshot route
- Trust-bundle and reenrollment routes
- Browser client crate with:
  - provider-aware login/callback flow
  - exchange-backed provider callback resolution on the bootstrap edge
  - session refresh/logout edge flow
  - enrollment request building
  - trust-bundle fetch
  - signed directory/leaderboard fetch
  - persisted browser session/snapshot storage
  - experiment picker and runtime-state projection
  - local verify/train receipt generation
  - browser receipt outbox and edge submission
  - live browser client-to-edge HTTP roundtrip coverage
  - mixed native+browser worker runtime coverage through testkit
  - wasm compile coverage through testkit browser matrix

### Operator baseline

- Bootstrap daemon status, metrics, events, diagnostics bundle, heads, receipts,
  reducer-load export, trust export, admin surface, and signed capability
  manifest endpoints
- Authority rotation and trust-bundle rollout
- RBAC-backed operator sessions on `/admin`
- `x-admin-token` available only behind explicit `allow_dev_admin_token`
- Optional-service startup validation for portal/social/browser-edge/auth
  support against the compiled feature set
- Optional portal/social/browser-edge routes that can be disabled without
  affecting the native core runtime
- Embedded validator/trainer daemon mode

### Social/product baseline

- Optional `burn_p2p_social` crate for leaderboard aggregation and badges
- Optional `burn_p2p_portal` crate for the reference portal/product HTML
- Optional auth connector crates for GitHub, OIDC, OAuth, and trusted external
  proxy enrollment
- Bootstrap social surface composed through the `social` feature instead of
  hardwiring leaderboard rollup code into the bootstrap crate itself
- Empty leaderboard snapshot fallback when bootstrap is built without social

## Known Gaps

These are still incomplete after this stabilization pass:

- Real upstream GitHub App, OIDC, and generic OAuth protocol integrations
  remain incomplete. The current connector set now supports provider-specific
  login routes, exchange-backed callback resolution, userinfo/profile
  hydration, and provider-side refresh/revoke hooks, but it still stops short
  of full production provider discovery and native upstream token semantics.
- Browser worker/runtime logic is present, but live browser swarm join and
  transport-backed verifier/trainer execution are still incomplete.
- Mixed-fleet browser worker coverage and browser client-to-edge HTTP coverage
  now exist, but live browser transport CI and full browser swarm e2e are not
  complete yet.
- Portal asset/UI productization beyond the lightweight `burn_p2p_portal`
  reference HTML and typed view contracts is still pending.
- The working tree has not yet been converted into a clean commit in this pass.

## Risk Notes

### Upgrade risk

- The public facade still carries legacy `P2pProject` / `RuntimeProject`
  compatibility layers. They remain supported for now, but the next API cleanup
  pass should make `P2pProjectFamily` / `P2pWorkload` the only forward path.
- Provider connectors are no longer re-exported from the facade; downstream
  code that wants concrete GitHub/OIDC/OAuth/external connectors now consumes
  the dedicated optional auth crates directly.

### Browser risk

- Browser support is real at the portal/enrollment/worker/receipt layer and the
  wasm compile path is now verified, but browser peers still cannot complete
  the full live swarm transport path from wasm in this baseline.

### Auth risk

- Provider-specific connectors, exchange-backed callback resolution,
  userinfo/profile hydration, provider-side refresh/revoke hooks, and operator
  RBAC are now present in modularized crates. The remaining auth risk is the
  gap to fully native upstream GitHub/OIDC/OAuth integrations.

### Performance risk

- Benchmark coverage now exists and the latest security rerun improved the
  measured release-policy, auth-admission, validator-policy, and reputation
  paths. The remaining performance risk is general regression drift as more
  browser/auth work lands, not a currently measured hotspot in this slice.

### Ops risk

- The bootstrap daemon is strong enough for local and controlled deployments,
  but packaging, richer portal assets, and production deployment guidance still
  need a dedicated hardening pass.
