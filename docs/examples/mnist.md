# MNIST Demo

`examples/mnist_p2p_demo` is the real downstream-style demo for `burn_p2p`.

It is not a synthetic toy workload. It uses:

- real mnist data from Burn
- real shard files plus a fetch manifest
- real trainer lease assignment
- real validator promotion
- real reconnect / restart handling in the full mnist path
- real metrics indexing
- real browser runtime role exercises
- real browser portal snapshots
- real browser burn/webgpu wasm execution in the correctness harness
- real browser dataset latency / bandwidth shaping in the correctness harness
- real Playwright captures through `cargo xtask e2e mnist`

## Why It Exists

Purpose:

- prove the public Burn-facing API on a familiar workload
- prove native node roles on one machine
- prove browser-facing runtime and portal surfaces against a real run
- prove live browser burn/webgpu execution against the same prepared mnist shards
- prove artifact sync, checkpoint sync, and shard assignment
- prove browser shard access stays lease-scoped and transport-aware
- prove browser dataset latency and bandwidth shaping change end-to-end timing
- prove restart recovery in the full mnist path
- prove bounded-adversary behavior in the correctness harness
- give downstream users one complete example crate to copy

## Topology

The demo runs one net with:

- one validator / authority node
- one helper node acting as the net's cheap coherence seed and relay helper
- one viewer node with portal-viewer and browser-observer roles
- three initial native trainers
- one late-joining native trainer
- one restarted native trainer reusing its storage root

Browser-facing runtime drills also exercise:

- one browser viewer role
- one browser verifier role
- one browser wgpu trainer role

It also runs two experiments on the same net:

- `mnist-baseline`
- `mnist-low-lr`

The baseline experiment uses a higher learning rate. The low-lr experiment
shares the same net and data shape but should move more slowly. The summary
checks `baseline_outperformed_low_lr`.

The core example crate stays native-focused.

`cargo xtask e2e mnist` layers correctness tooling on top of the core run:

- portal bundle export and Playwright capture
- live browser burn/webgpu probe against leased mnist shards
- latency / bandwidth-shaped browser dataset fetch profiles
- adversarial annex checks from `burn_p2p_testkit`
- summary assertions over both the native run and the browser probe

The helper node is deliberately not a validator. It exists to exercise the intended deployment
split:

- cheap coherence/bootstrap seed
- separate validator / authority
- separate reducer
- separate trainers

## Files

Main files:

- `examples/mnist_p2p_demo/src/main.rs`: CLI entrypoint and export writing
- `examples/mnist_p2p_demo/src/core.rs`: core happy-path burn_p2p setup,
  node topology, experiment setup, training windows, and checkpoint flow
- `examples/mnist_p2p_demo/src/scenario.rs`: thin wrapper from core run to
  correctness export
- `examples/mnist_p2p_demo/src/data/`: mnist subset preparation, shard files,
  fetch manifest, and lease-to-batch loading
- `examples/mnist_p2p_demo/src/correctness/browser.rs`: browser runtime drills,
  browser dataset access probe, browser portal export contracts
- `examples/mnist_p2p_demo/src/correctness/export.rs`: summary and browser
  export contracts
- `examples/mnist_p2p_demo/src/correctness/report.rs`: correctness summaries
  derived from the core run
- `examples/mnist_p2p_demo/src/model/`: model, train/eval steps, custom
  metrics
- `examples/torch_mnist_p2p_demo/src/main.rs`: native Rust harness for the
  Python/Torch `P2pWorkload`
- `examples/torch_mnist_p2p_demo/python/torch_mnist_p2p_demo/runtime.py`:
  subprocess-owned Torch model, dataset preparation, training, and validation

## Burn Integration

The demo uses the public Burn facade, not internal-only seams.

Core path:

- start from `burn::train::Learner::new(...)`
- use `burn_p2p::burn::from_loaders(...)` for trainer nodes
- use `burn_p2p::burn::from_learner(...)` for validator, helper, and viewer nodes
- prepare the train dataset with `burn_p2p::burn::BurnShardedDataset::write_local(...)`
- keep task-specific hooks in:
  - `.with_sharded_dataset(...)`
  - `.with_evaluate(...)`
  - `.with_step_metrics(...)`
  - `.with_window_metrics(...)`
- spawn each node from the smallest builder that matches its role

Why `BurnShardedDataset` matters:

- the same public burn api still starts from `from_loaders(...)`
- train data is fetched lease-by-lease through the runtime cache path
- the same prepared dataset can be flipped to http upstream for browser or wasm
  trainers with `.with_http_upstream(...)`
- the demo proves shard fetch / assignment behavior through the normal
  `from_loaders(...)` + `.with_sharded_dataset(...)` path

Browser data transport in the demo is intentionally not raw shard gossip:

- browser-style shard access reads `fetch-manifest.json` from the dataset origin
- browser-style shard access fetches only the assigned shard locators for the
  active lease
- mnist shard bytes remain lease-scoped; the live browser probe now expects the
  `p2p-signed-peer-bundle` path when that transport is advertised
- shard payloads are not broadcast over the peer overlay
- native dataset preparation uses Burn's mnist dataset helpers on the host side
- the browser wasm probe does not depend on `burn/vision`; it reads the
  prepared shard records directly and trains with `burn["webgpu", "autodiff"]`

## Python/Torch Adapter

`examples/torch_mnist_p2p_demo` proves the same runtime boundary without using
Burn for model execution.

It uses:

- `burn_p2p_python` as the Rust adapter
- a Python subprocess for Torch model state, train steps, validation, and shard
  preparation
- the same `NodeBuilder`, trainer session, validator promotion, canonical-head
  sync, dataset planning, and fetch-manifest path as native workloads

This demo is intentionally small. It is the quickest way to verify that a
non-Burn runtime can plug into `P2pWorkload` while leaving p2p orchestration,
leases, validation, storage, and head sync in Rust.

## Portal And Testkit

`portal` here means the optional reference dioxus product surface.

- `burn_p2p_app` owns the reference browser/native ui tree
- bootstrap serves the live portal snapshot from `/portal/snapshot` when the
  `browser-edge` feature is enabled
- static portal assets can be built separately and hosted from a cdn
- live data still comes from the bootstrap/browser-edge http surface

`burn_p2p_testkit` is not part of the downstream mnist app.

- the core `mnist_p2p_demo` crate no longer depends on `burn_p2p_testkit`
- `burn_p2p_testkit` is the repo's qa/simulation harness
- `cargo xtask e2e mnist` uses it for portal capture bundles, live browser wasm
  probing, adversarial annex checks, and related correctness tooling

## Custom Metrics

The demo exports more than just loss:

- `accuracy`
- `loss`
- `digit_zero_accuracy`
- `digit_zero_examples`
- `parameter_count`

Those metrics show up in:

- trainer lease summaries
- validation metric previews
- browser portal export snapshots
- xtask summary artifacts

## Commands

Run the example crate directly:

```bash
cargo run --manifest-path examples/mnist_p2p_demo/Cargo.toml -- --output ./target/mnist-demo
```

Run the Python/Torch adapter demo:

```bash
cargo run --manifest-path examples/torch_mnist_p2p_demo/Cargo.toml -- \
  --root ./target/torch-mnist-demo \
  --python python3
```

Run the full local sanity lane:

```bash
cargo xtask e2e mnist --profile smoke --keep-artifacts
```

Thin wrapper:

```bash
just e2e-mnist
```

## What `cargo xtask e2e mnist` Proves

The xtask lane runs the example, keeps the native mnist fleet alive, then:

- reads `browser-live.json`
- runs the browser wasm probe against the same live mnist run and browser edge
- builds a browser portal bundle
- runs Playwright against viewer / validate / train / network scenarios
- builds a wasm browser probe from the same downstream example crate
- runs live browser burn/webgpu training and eval against the same leased mnist shard slice
- runs fast and slow browser fetch profiles with injected latency/bandwidth
- stores screenshots and trace zips
- writes a compact core `summary.json`
- writes a separate `correctness.json`
- writes `browser-export.json` for the portal/capture surface
- folds the live browser runtime and wasm training result directly into `correctness.json`

Checks and exported signals covered by `summary.json`, `correctness.json`, and
the top-level xtask artifact summary:

- hosted `ci-integration` on GitHub Actions runs the bounded core path
- hosted `ci-integration` proves reducer/validator promotion and treats
  trainer/viewer promoted-head fanout as best-effort on hosted runners
- local runs and heavier runners keep the full resilience drills
- `baseline_outperformed_low_lr`
- `shard_assignments_are_distinct`
- `browser_dataset_access.fetch_manifest_requested`
- `browser_dataset_access.fetched_only_leased_shards`
- `browser_dataset_access.shards_distributed_over_p2p == true` when the live
  browser run advertises the `p2p-signed-peer-bundle` path
- `browser_wasm_probe.browser_execution.live_browser_training`
- `browser_wasm_probe.browser_execution.browser_latency_emulated`
- `browser_wasm_probe.browser_execution.slower_profile_increased_total_time`
- `correctness.browser_execution.trainer_runtime_and_wasm_training_coherent`
- `correctness.browser_execution.same_network_context`
- `correctness.browser_execution.same_experiment_context`
- `correctness.browser_execution.same_revision_context`
- `correctness.browser_execution.same_head_context`
- `correctness.browser_execution.same_lease_context`
- `correctness.browser_execution.same_leased_microshards`
- `resilience.executed`
- `late_joiner_synced_checkpoint` when `resilience.executed == true`
- `resilience.trainer_restart_reconnected` when `resilience.executed == true`
- `resilience.trainer_restart_resumed_training` when `resilience.executed == true`
- `correctness.adversarial.reports[*].malicious_update_acceptance_rate == 0.0`
- `assessment.browser_runtime_roles_exercised`
- per-experiment initial and final accuracy
- per-node role summaries
- per-lease shard assignments and sample counts
- train-window timing summaries
- adversarial annex summaries for replay / free-rider / nan-inf / late-flood

## Artifacts

Direct example output:

- `summary.json`
- `correctness.json`
- `browser-live.json`
- `browser-export.json`
- `browser-probe-result.json`
- `dataset/`
- `nodes/`

Xtask artifact root:

- `target/test-artifacts/e2e-mnist/<run-id>/summary.json`
- `target/test-artifacts/e2e-mnist/<run-id>/metrics/mnist-summary.json`
- `target/test-artifacts/e2e-mnist/<run-id>/metrics/mnist-correctness.json`
- `target/test-artifacts/e2e-mnist/<run-id>/mnist-demo/`
- `target/test-artifacts/e2e-mnist/<run-id>/playwright/`
- `target/test-artifacts/e2e-mnist/<run-id>/browser-wasm-probe/`
- `target/test-artifacts/e2e-mnist/<run-id>/screenshots/`
- `target/test-artifacts/e2e-mnist/<run-id>/playwright-traces/`

## Browser Coverage

The exported browser portal scenarios cover:

- viewer
- validate
- train
- network

Separate browser runtime drills cover:

- browser viewer state
- browser verifier receipt flow
- browser wgpu trainer receipt flow
- transport stall and recovery
- lease-scoped http shard fetching
- live browser burn/webgpu training and eval through the wasm probe
- latency-shaped dataset fetch over the prepared http shard origin

Current split:

- the core `mnist_p2p_demo` binary proves the native p2p workload path
- the xtask correctness layer proves browser burn/webgpu execution and
  latency-shaped shard fetching against the same prepared dataset
- the xtask correctness layer also owns the adversarial annex
- browser shards still come from the dataset http origin, not the peer overlay

## When To Copy This

Use the mnist demo when you want:

- a complete downstream example crate
- a real shard-backed Burn workload
- a multi-role single-machine sanity environment
- a reference for custom metrics and portal surfacing

Use `burn_ndarray_runtime.rs` when you want:

- the smallest Burn-facing example
- a simpler local integration path
- loader-oriented happy-path usage without the full shard-backed scenario
