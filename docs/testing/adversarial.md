# Adversarial Robustness

The robustness subsystem is resistant, not immune. It is designed to block
malformed, replayed, stale, extreme, and bounded-minority poisoning attempts
under configured assumptions. It does not claim safety under majority capture.

## Local Commands

Default smoke:

```bash
cargo xtask adversarial smoke
```

Broader matrix:

```bash
cargo xtask adversarial matrix
```

Seeded replayable run:

```bash
cargo xtask adversarial chaos --seed 12345 --events 8 --peers 12
```

Synthetic robustness benchmarks:

```bash
cargo xtask bench robust
```

## CI Mapping

- PR fast lane: `cargo xtask ci pr-fast`
  - includes adversarial smoke
  - smoke explicitly runs the mixed browser/native replay and free-rider regression
  - smoke also covers replay and browser-style free-rider behavior in the synthetic scenario harness
- Nightly lane: `cargo xtask ci nightly`
  - includes adversarial matrix
  - includes adversarial seeded chaos
  - includes robustness benchmarks

## Artifacts

Each run writes to:

`target/test-artifacts/<suite>/<run-id>/`

Useful files:

- `metrics/scenario-reports.json`
- `metrics/summary.json`
- `topology/attack-sequence.json` for seeded chaos
- `seed.txt` for replay

## Current Defense Shape

The current default defense layers are:

1. hard reject malformed, replayed, stale, mismatched, or quarantined updates
2. clip and bound effective contribution weight
3. score updates with compact feature sketches and similarity checks
4. aggregate the screened cohort with a bounded rule
5. block promotion on canary regression
6. feed decisions into trust and quarantine state

## Limits

- Robust aggregation is not treated as a complete Byzantine solution.
- Browser peers are rate-limited by the same screening and trust path, but they
  are not assumed to be inherently trustworthy.
- Majority compromise remains an operator containment and recovery problem, not
  a convergence guarantee.
