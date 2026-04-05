# Test Artifacts

All local-first E2E, browser, stress, and bench commands write into:

`target/test-artifacts/<suite>/<run-id>/`

## Layout

- `summary.json`: machine-readable run summary
- `summary.md`: quick human-readable summary
- `stdout/`: captured command stdout logs
- `stderr/`: captured command stderr logs
- `configs/`: generated run configs and profile metadata
- `screenshots/`: collected PNG screenshots
- `playwright-traces/`: collected Playwright trace archives
- `metrics/`: metrics and bench outputs
- `publication/`: publication-related outputs when present
- `topology/`: simulation, multiprocess, and chaos outputs
- `ports.json`: reserved or discovered port information
- `seed.txt`: replay seed for seeded chaos runs

## Local Use

Each command prints the artifact directory on completion.

Examples:

```bash
cargo xtask browser smoke
cargo xtask stress chaos --seed 12345
```

## CI Use

GitHub Actions uploads `target/test-artifacts/` directly. There is no separate
CI-only artifact layout.

For `cargo xtask ci ...` lane runs, the top-level `summary.json` also includes
`extra.nested_artifacts`, which points to the underlying per-command artifact
directories for faster reruns and debugging.
