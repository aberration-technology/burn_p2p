# CI Profiles

The canonical runner is `cargo xtask`. Workflows choose profiles; they do not
encode test logic directly. Release checks validate packaged workspace crates
against the same local package set, rather than assuming sibling crates are
already available from crates.io.

## Profile Map

- `dev`: fastest local coding loop
- `smoke`: default local smoke profile
- `ci-pr`: lean pull-request profile
- `ci-integration`: broader bounded integration profile
- `nightly`: heavier browser, stress, and bench profile
- `real-browser`: headed or local real-browser validation profile

## Workflow Mapping

- `pr-fast.yml`
  - `cargo xtask ci pr-fast`
- `browser.yml`
  - `cargo xtask ci browser`
- `integration.yml`
  - `cargo xtask ci integration`
- `services.yml`
  - `cargo xtask ci services`
- `nightly.yml`
  - `cargo xtask ci nightly`
- `publish-readiness.yml`
  - `cargo xtask ci publish`

## PR vs Nightly

PR CI stays lean:

- fast checks
- native smoke E2E
- adversarial smoke covering replay and browser-style free-rider attacks
- browser smoke only in the dedicated browser workflow
- publication and metrics smoke only in the dedicated services workflow when
  those paths change

Integration CI adds broader product-shape coverage:

- mixed native/browser E2E
- downstream mnist sanity with browser runtime drills, browser captures,
  Playwright, live browser burn/webgpu execution, latency-shaped shard fetch,
  reconnect recovery, lease-scoped http shard checks, and adversarial annex
  checks
- bounded multiprocess stress

Nightly absorbs heavier work:

- adversarial matrix by policy and attack family
- seeded adversarial chaos replay
- robustness benchmarks
- seeded chaos
- larger multiprocess runs
- browser smoke with nightly profile
- benches
