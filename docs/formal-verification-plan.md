# formal verification plan

this note describes how to use [`veil`](https://github.com/verse-lab/veil) to
prove protocol safety for `burn_p2p` without polluting the runtime crates or
pretending formal protocol proofs are the same thing as ml-quality proofs.

the short version:

- use veil to prove control-plane and authority safety
- keep the formal model modular and out of the rust workspace hot path
- prove the protocol first, then connect runtime traces back to that model
- do not use veil to claim convergence, poisoning completeness, or model quality

## what veil is good for

veil is a lean-based framework for specifying and proving safety properties of
transition systems, with a focus on distributed protocols.

for `burn_p2p`, that makes it a good fit for:

- canonical promotion safety
- validator quorum safety
- reducer non-authority
- receipt replay exclusion
- revocation and authority-epoch enforcement
- canonical lineage and uniqueness
- provenance preservation across aggregate construction

it is not the right tool for proving:

- sgd convergence
- generalization quality
- full resistance to all model poisoning strategies
- correctness of the rust implementation by itself

those need empirical evaluation, runtime testing, and implementation-level
conformance checks.

## the proof target

the proof target should be the decentralized training protocol described by the
current code and docs:

- [docs/protocol-shape.md](protocol-shape.md)
- [docs/learning-dynamics.md](learning-dynamics.md)
- [crates/burn_p2p/src/validation.rs](../crates/burn_p2p/src/validation.rs)
- [crates/burn_p2p/src/validation/robustness.rs](../crates/burn_p2p/src/validation/robustness.rs)
- [crates/burn_p2p_core/src/schema/manifests.rs](../crates/burn_p2p_core/src/schema/manifests.rs)

the important trust split is:

- trainers publish candidate updates
- reducers may publish aggregate proposals
- validators decide accepted inputs and canonical promotion

that means the first proof goal is not "training works". it is:

- no invalid head becomes canonical without validator quorum

## what to model

the veil model should stay at the protocol layer, not the tensor/math layer.

### state

model these as abstract state:

- peers, roles, and peer status
- validator set, quorum threshold, authority epoch
- revocation epoch and trust/admission status
- experiments, revisions, windows, and base heads
- candidate updates and their provenance
- reducer proposals
- validator attestations
- quorum certificates and merge certificates
- canonical head pointer

### actions

the minimal action set should look like:

- `publish_update`
- `publish_reducer_proposal`
- `reject_candidate`
- `accept_candidate`
- `attest_reduction`
- `emit_validation_quorum`
- `promote_head`
- `revoke_peer`
- `rotate_validator_set`
- `advance_authority_epoch`

### assumptions

be explicit about what is assumed rather than proved:

- certificates and signatures are unforgeable
- validator-local screening is deterministic
- the merge function is deterministic over accepted inputs
- the honest validator quorum threshold holds
- artifact bytes match their declared identity

these assumptions should be written down in the spec, not left implicit.

## first theorem set

phase 1 should prove these invariants:

- `quorum_required_for_promotion`
  - a head cannot become canonical without enough validator attestations
- `reducer_non_authority`
  - a reducer proposal alone cannot advance canonical state
- `accepted_inputs_only`
  - a promoted head is derived only from validator-accepted contributions
- `receipt_replay_excluded`
  - the same contribution cannot be counted twice in one accepted cohort
- `canonical_uniqueness_per_window`
  - under honest quorum assumptions, there are not two conflicting canonical
    heads for the same `(network, study, experiment, revision, window)`
- `revoked_peers_excluded`
  - once revocation takes effect for an epoch, the affected peer cannot
    contribute to future accepted cohorts
- `provenance_preserved`
  - accepted aggregate records retain input attribution to peer/artifact pairs

this is the minimum useful proof package.

## second theorem set

phase 2 should cover governance and stronger authority behavior:

- `validator_set_manifest_well_formed`
- `authority_epoch_monotonic`
- `validator_rotation_requires_valid_transition`
- `evidence_records_do_not_change_canonical_state_without_authority_action`
- `ban_or_quarantine_effect_is_monotonic_within_epoch`

these theorems belong once validator-set governance becomes a real control-plane
flow rather than only schema/config validation.

## what the proofs still cannot say

even with the theorems above, the following are still outside the proof claim:

- that the canary detector catches every attack
- that accepted updates are always semantically useful
- that the merge rule converges optimally
- that colluding peers are always individually attributable

the right claim is:

- protocol safety under stated assumptions

not:

- perfect learning robustness

## repo organization

the formal work should be modular and should not leak lean/veil dependencies
into the rust workspace.

### directory layout

recommended layout:

```text
formal/
  veil/
    README.md
    lakefile.toml
    lean-toolchain
    BurnP2P/
      Protocol/
        State.lean
        Actions.lean
        Invariants.lean
        Governance.lean
      Proofs/
        Safety.lean
        Governance.lean
      ModelCheck/
        Smoke.lean
      Trace/
        EventSchema.lean
```

keep this outside `crates/` and outside the cargo workspace.

### rust integration points

add rust-side support only through narrow seams:

- `burn_p2p_core`
  - stable protocol identifiers and schema types
- `burn_p2p_testkit`
  - deterministic trace fixtures and adversarial scenarios
- `xtask`
  - optional `formal` commands for running/exporting verification assets

do not:

- add lean or veil dependencies to rust crates
- invoke veil from normal cargo build/test paths
- generate rust runtime code from lean proofs

the formal system should validate the design and selected traces, not become a
runtime dependency.

## source-of-truth rules

to keep the formal model from drifting:

- rust schema types in `burn_p2p_core` remain the source of truth for wire and
  persistence identifiers
- the veil model remains the source of truth for abstract safety invariants
- trace adapters are the bridge between the two

that means:

- protocol names and enum shapes should be mirrored deliberately
- theorem statements should refer to stable protocol concepts, not rust-internal
  incidental details
- if a protocol change affects promotion, governance, or revocation semantics,
  the matching veil model must change in the same pull request

## execution model

the clean execution path is:

1. write the abstract protocol in veil
2. model-check small-state versions for counterexamples
3. prove the main safety invariants
4. export runtime traces from rust tests and demos
5. check that traces refine the abstract protocol

this gives three layers of assurance:

- abstract protocol model checking
- theorem-level safety proofs
- implementation-to-model conformance over real traces

## current verified slice

the repo now has an initial but real formal layer:

- `lake build` succeeds for the standalone `formal/veil` project
- lean proves quorum-gated promotion and reducer non-authority safety lemmas
- lean models canary/robustness policy gates at the protocol-policy layer
- rust exports versioned protocol traces and checks them for protocol
  conformance before lowering
- `cargo xtask formal verify-trace --scenario ...` generates a lean fixture from
  a rust trace and asks `lake` to replay that trace through the abstract model

the important claim is:

- selected rust traces are now replay-checked against the lean protocol model

the important non-claim is still:

- this does not prove the ml detector semantics are universally correct

## trace integration

the next clean rust addition is a small trace export seam.

recommended files:

- `crates/burn_p2p_testkit/src/formal_trace.rs`
- `crates/burn_p2p_testkit/tests/formal_trace_*.rs`
- `xtask/src/formal.rs`

the trace format should be:

- append-only
- json or cbor
- deterministic and versioned
- limited to protocol events, not full model weights

recommended event types:

- update published
- reducer proposal observed
- candidate accepted/rejected
- validator attestation emitted
- quorum certificate emitted
- merge certificate emitted
- canonical head promoted
- peer revoked or quarantined
- validator set changed

this should be enough to replay control-plane safety transitions without mixing
in large artifact bytes.

## acceptance criteria

the formal track is in good shape once all of these exist:

- a standalone `formal/veil` project builds independently
- phase-1 invariants are proved
- at least one adversarial reducer case and one replay case are model-checked
- rust tests can emit formal traces for at least the mnist demo and one
  dedicated adversarial test
- ci has an optional formal lane that runs veil without blocking normal rust
  contributor flows

## ci and local commands

keep the formal flow explicit and optional.

recommended commands:

- `cargo xtask formal export-trace --scenario mnist-smoke`
- `cargo xtask formal export-trace --scenario reducer-adversarial`
- `cargo xtask formal check`
- `cargo xtask formal modelcheck`

the normal release and smoke flows should not require lean.

## implementation sequence

the right order is:

1. create `formal/veil` with the phase-1 protocol model
2. prove quorum, replay, and reducer non-authority invariants
3. add rust trace export from `burn_p2p_testkit`
4. add trace conformance checks
5. expand to authority-epoch and validator-set governance

that keeps the first formal milestone small and useful.

## current recommendation

use veil to prove the protocol claim the repo already makes:

- validator quorum is the authority boundary
- reducers are helpers, not authorities
- canonical promotion preserves accepted-input provenance

do not use it to overclaim:

- byzantine ml robustness
- optimizer convergence
- implementation correctness without refinement checks

that is the clean, modular, and credible way to add formal verification to the
existing codebase.
