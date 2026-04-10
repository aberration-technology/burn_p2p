# burn_p2p formal

this directory is the standalone formal verification project for `burn_p2p`.

it is intentionally:

- outside the cargo workspace
- outside `crates/`
- optional for normal rust contributors

the goal is to prove protocol safety for the decentralized training control
plane without leaking lean or veil dependencies into the runtime crates.

## what lives here

- `BurnP2P/Protocol/`
  - abstract state, actions, invariants, and governance model
- `BurnP2P/Proofs/`
  - theorem statements and proofs
- `BurnP2P/ModelCheck/`
  - small-state smoke models
- `BurnP2P/Trace/`
  - the lean-side view of exported rust protocol traces

## commands

from this directory:

```bash
lake build
lake build BurnP2P.ModelCheck.Smoke
```

from the repo root:

```bash
cargo xtask formal check
cargo xtask formal modelcheck
cargo xtask formal export-trace --scenario mnist-smoke
cargo xtask formal export-trace --scenario reducer-adversarial --format cbor
cargo xtask formal verify-trace --scenario mnist-smoke
cargo xtask formal verify-trace --scenario reducer-adversarial
```

## source of truth

- rust runtime/schema source of truth:
  - `crates/burn_p2p_core/src/`
  - `crates/burn_p2p/src/validation.rs`
  - `crates/burn_p2p/src/validation/robustness.rs`
- formal protocol safety source of truth:
  - this project

the formal model should track stable protocol concepts, not incidental rust
implementation details.

## current scope

the current scope is still intentionally narrow, but it is no longer only a
scaffold. it now includes:

- a dedicated veil-ready lean project
- abstract protocol semantics
- safety lemmas for quorum-gated promotion and reducer non-authority
- policy-level canary and robustness gating lemmas
- rust trace lowering into the lean action model
- generated lean fixtures that replay exported rust traces under `lake`

it still does not claim:

- end-to-end ml-quality proofs
- universal correctness of poisoning detectors
- a fully general refinement theorem for every possible rust trace
