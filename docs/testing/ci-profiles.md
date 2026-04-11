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
- downstream mnist core integration with browser runtime drills, browser
  captures, Playwright, live browser burn/webgpu execution, lease-scoped shard
  checks, and adversarial annex checks
- the hosted `ci-integration` mnist lane is intentionally bounded: it keeps the
  baseline/low-lr promotion path on the reducer/validator tier, but skips the
  restart and late-joiner resilience drills and does not require full
  trainer/viewer promoted-head fanout on `ubuntu-latest`
- bounded multiprocess stress

Nightly absorbs heavier work:

- adversarial matrix by policy and attack family
- seeded adversarial chaos replay
- network dynamics bench with metrics-backed native soak and discovery summaries
- native accelerator bench for real cuda/wgpu Burn backend probes
- robustness benchmarks
- seeded chaos
- larger multiprocess runs
- browser smoke with nightly profile
- redis-backed browser-edge auth HA and failure-mode validation through
  `cargo xtask check auth-redis-live`
- env-gated live oidc validation through `cargo xtask check auth-oidc-live`
- benches

MNIST note:

- `cargo xtask e2e mnist --profile ci-integration` on hosted GitHub Actions is
  the bounded core path
- full MNIST restart and late-joiner resilience drills remain available via the
  example and xtask outside that bounded hosted path
