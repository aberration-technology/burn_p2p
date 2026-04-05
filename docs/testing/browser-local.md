# Browser Testing Local

Browser verification is local-first and routed through `cargo xtask`.

## Setup

Install browser prerequisites:

```bash
cargo xtask doctor
cargo xtask setup browser
```

`setup browser` does three things:

- ensures `wasm32-unknown-unknown` is installed
- warms the Playwright package
- installs Playwright-managed Chromium and Firefox

## Modes

Headless browser smoke:

```bash
cargo xtask browser smoke
```

Headed browser smoke:

```bash
cargo xtask browser smoke --headed
```

Browser trainer path:

```bash
cargo xtask browser trainer
```

Real browser probe:

```bash
cargo xtask browser real --profile real-browser
```

Exact browser CI lane locally:

```bash
cargo xtask ci browser --keep-artifacts
```

## Environment Overrides

Optional browser executable overrides:

- `BURN_P2P_PLAYWRIGHT_CHROME`
- `BURN_P2P_CHROME_BIN`
- `BURN_P2P_FIREFOX_BIN`

These are only needed when local browsers live outside the default paths and
you do not want to use Playwright-managed browsers.

## Captures

Browser smoke and browser trainer runs save:

- screenshots
- Playwright traces
- browser console logs
- rendered bundle state

The real browser probe also saves screenshots, traces, and console logs under
its artifact directory when Playwright launches successfully.

## Existing Real-Device Path

The old ignored local-only probe remains the underlying harness:

- `crates/burn_p2p_testkit/tests/browser_real_device.rs`
- `crates/burn_p2p_testkit/scripts/browser_real_device_probe.mjs`

Use `cargo xtask browser real` instead of invoking the ignored test directly.
It works with Playwright-managed browsers from `cargo xtask setup browser`, and
can also use local system browser binaries through the override env vars above.
