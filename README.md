# burn_p2p

Distributed Burn training over a policy-gated peer-to-peer network.

This workspace is intentionally split into one stable facade crate, a small
set of companion crates, and a set of internal implementation crates that are
not semver promises yet. The companion operator crates
`burn_p2p_bootstrap`, `burn_p2p_ui`, and `burn_p2p_testkit` are consumed
directly instead of being re-exported from the facade.

Current status:

- The workspace scaffold and shared schemas are in place.
- `burn_p2p` now has a real `spawn()` path that starts a live background runtime
  plus `prepare()` for the old handle-only behavior.
- `burn_p2p` defaults to a native TCP listen address, persists identity when
  configured, and auto-syncs the remote control snapshot on connection.
- `burn_p2p` now exposes a stable `RuntimeProject` trait plus one-step native
  runtime methods for initializing a canonical head, training a leased window,
  validating candidate heads, syncing canonical heads, and restoring persisted
  canonical state after restart.
- `burn_p2p` now also exposes the first phase of the new project-family facade:
  `P2pProjectFamily`, `P2pWorkload`, and `SingleWorkloadProjectFamily`, plus
  family-selected runtime binding through `NodeBuilder::with_network(...)` for
  single-workload releases and `NodeBuilder::for_workload(...)` for
  multi-workload releases, so downstream applications can bind one approved
  client release to a compiled workload registry without importing internal
  engine crates.
- `burn_p2p` now also exposes a feature-gated `burn` runtime helper surface
  for Burn-native checkpoint bytes and artifact materialization, plus a public
  `burn_ndarray_runtime` example that exercises the facade without importing
  internal adapter crates. That path now includes parameter-wise weighted
  model merging and single-root EMA helpers for real Burn modules.
- `burn_p2p_swarm` now has both memory-transport and native TCP+QUIC
  request-response control-plane shells plus identify, mDNS, and gossipsub
  control propagation for live connectivity, discovery, snapshot exchange,
  lease announcements, merge announcements, and artifact transfer.
- `burn_p2p` now persists discovered peer listen addresses in node state and
  reloads them on restart, so native peers can reconnect from remembered peers
  instead of relying only on the original bootstrap list.
- `burn_p2p_checkpoint` now supports streamed artifact descriptor building and
  an on-disk filesystem CAS with chunk storage, manifest persistence, pinning,
  materialization, and garbage collection.
- `burn_p2p_dataloader` now has real manifest-backed `local` and `http` shard
  fetch plus disk caching, and it only fetches shards that are actually leased.
- `burn_p2p_limits` now supports both initial capability calibration and
  observed-throughput rebudgeting, and the live `burn_p2p` runtime now persists
  and reapplies the effective limit profile per experiment so window budgets can
  correct toward measured runtime throughput instead of staying locked to the
  initial benchmark.
- `burn_p2p_security` now has a first-class auth/admission layer with
  principal sessions, a static identity connector, network-signed node
  certificates bound to peer IDs, peer auth envelopes, trusted-issuer
  verification, revocation-epoch checks, and experiment-scoped admission.
- `burn_p2p_core`, `burn_p2p_experiment`, `burn_p2p_testkit`, and
  `burn_p2p_ui` now include a first-class merge-topology subsystem:
  replicated-rendezvous DAG policy/schema types, deterministic reducer
  assignment, merge-window state, aggregate envelopes, reduction
  certificates, reducer-load reporting, topology dashboard views, and a
  policy-comparison simulator for broadcast, central reducer, tree, gossip,
  and hierarchical cohort strategies. The live native runtime now publishes
  merge-window, reducer-assignment, update, aggregate, reduction-certificate,
  and reducer-load announcements through the control plane during
  train/validate flows, and validator promotion now stores a real aggregate
  evidence artifact in the filesystem CAS instead of pointing aggregate
  envelopes at the promoted trainer head artifact. The root validator merge
  path now also supports exact weighted candidate combination over model
  parameters through the internal engine seam instead of only promoting a
  winning candidate head.
- `burn_p2p_experiment` now exposes a typed experiment directory with
  scope-aware visibility filtering, and the live control-plane snapshot now
  carries authenticated-peer and experiment-directory announcements.
- `burn_p2p_bootstrap` now ships a first headless binary surface,
  `burn-p2p-bootstrap`, with JSON config loading, `/status`, `/metrics`,
  `/events`, `/receipts`, `/admin`, a static operator dashboard, and an
  embedded validator/trainer loop path built on the public `burn_p2p` runtime.
  When `auth` is configured it also exposes a minimal static admission portal:
  `POST /login/static`, `POST /device`, `POST /callback/static`,
  `POST /enroll`, `GET /revocations`, and `GET /directory`.
  It also now includes a library-level embedded daemon example showing how a
  downstream project packages its own `RuntimeProject` instead of relying on
  the bundled synthetic bootstrap binary path.
- `burn_p2p_ui` remains a framework-neutral typed contract crate; it now also
  includes typed auth portal, trust badge, contribution identity, and
  experiment picker views. The reference bootstrap dashboard is intentionally
  minimal and static-asset based.
- `burn_p2p_testkit` includes semantic simulation coverage and higher-peer mixed
  update tests, a synthetic multi-process native harness, smoke/restart
  coverage across separate processes, runnable coordinated soak and heavy-stress
  binary/config paths, stable non-ignored multi-window, concurrent soak, and
  process fan-in stress tests. `burn_p2p` itself also has native two-trainer,
  restart, and many-trainer runtime e2e coverage.
- The workspace now also ships real Criterion benchmark targets for streamed
  checkpoint packaging, Burn-side weighted module merge/root EMA, and
  merge-topology simulation throughput, plus control-plane pubsub/snapshot/
  manifest serialization throughput for the native swarm, plus dataloader
  microshard planning, lease planning, and receipt generation throughput, plus
  limits calibration and observed-throughput rebudgeting throughput.

The previously open native-MVP runtime blockers are closed. What remains is
follow-on hardening and scale work rather than missing runtime closure.

Quick start surfaces:

- Library example: `cargo run -p burn_p2p --example synthetic_trainer`
- Burn facade example: `cargo run -p burn_p2p --example burn_ndarray_runtime --features burn`
- Bootstrap daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/local-bootstrap.json`
- Auth-enabled bootstrap daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/authenticated-bootstrap.json`
- Embedded validator daemon: `cargo run -p burn_p2p_bootstrap --bin burn-p2p-bootstrap -- crates/burn_p2p_bootstrap/examples/embedded-validator.json`
- Downstream-style embedded daemon example: `cargo run -p burn_p2p_bootstrap --example embedded_runtime_daemon`
- Workspace tests: `cargo test --manifest-path Cargo.toml`
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
