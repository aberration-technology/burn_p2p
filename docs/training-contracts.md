# signed training contracts

`burn_p2p` separates semantic training identity from local execution
capability. A revision says what must be computed and validated. A peer decides
whether it can execute that contract on CPU, CUDA, ROCm, WGPU, or WebGPU.

This separation lets a native trainer, a browser trainer, and a read-only
verifier join the same revision without pretending their hardware is
identical.

## contract layers

The authority-controlled contract is a `RevisionContractBundle`:

1. `RevisionManifest`
   - activation, placement, browser, protocol, and topology policy
2. `TrainingContractManifest`
   - model program and tensor schema
   - checkpoint format and initialization
   - dataset view, tokenizer, preprocessing, and objective
   - optimizer, scheduler, recurrent-state, aggregation, and validation policy
   - update codec
3. `ModelGenesisManifest`
   - exactly one full, base-less model artifact
   - tensor digest and initialization identity
4. two domain-separated Ed25519 signatures
   - one over the genesis payload
   - one over the complete revision/training/genesis binding

Backend name, device model, memory capacity, and peer-local batch calibration
are deliberately excluded from the semantic contract. Changing one of those
must not create a new mathematical revision. Changing an objective, tokenizer,
parameter schema, update rule, or validation rule must.

## fail-closed startup

Network deployments should configure nodes with both:

```rust
let builder = NodeBuilder::new(project)
    .with_revision_contract(contract)?
    .require_signed_revision_contracts(true);
```

The runtime then refuses to initialize or execute a revision when:

- the authority signer is not trusted
- either signature is missing, invalid, or uses the wrong domain
- a content identifier does not match canonical CBOR content
- the revision, training contract, and genesis identities disagree
- genesis is not a complete full-head artifact
- the local workload cannot load the exact artifact and tensor schema
- the canonical digest of the decoded tensor layout/values differs from the
  signed genesis digest

`require_signed_revision_contracts(false)` remains available for isolated tests
and single-process development. It is not a public-network setting.

Bootstrap services can load contract bundles from
`BURN_P2P_REVISION_CONTRACT_FILES`. Every file is verified against the active
trust bundle before it enters the browser-edge snapshot. Conflicting contracts
for one revision fail registration. The authenticated admin API can replace a
complete verified contract set atomically, including an explicitly authorized
signature rotation; no reader sees a partially updated set.

## update codecs

`UpdateCodec` is architecture-neutral:

| codec | wire payload | validator work |
| --- | --- | --- |
| `FullModel` | complete checkpoint | schema and candidate evaluation |
| `DenseDelta` | one value per parameter | reconstruct and candidate evaluation |
| `QuantizedBlock` | block scales and quantized values | decode, reconstruct, evaluate |
| `SeededLowRank` | deterministic low-rank factors | regenerate, reconstruct, evaluate |
| `SubspaceLatent` | coefficients in a shared seeded subspace | regenerate, reconstruct, evaluate |
| `PowerSgd` | low-rank compressor factors | reconstruct with declared error policy |
| `SeededFitness` | seeds plus scalar fitness observations | replay optimizer and independently verify fitness |

`CompactUpdatePayload` is bounded, versioned, content-addressed, bound to one
training contract and model schema, and carried as a `DeltaPack` artifact.
Peer-provided norm statistics and feature sketches are telemetry only.
Validators compute their own evidence from reconstructed tensors.

`SubspaceLatent` is the direct low-dimensional affine-update seam. It follows
the same systems principle as FLITE-style communication-efficient fine-tuning:
agree on a deterministic low-dimensional parameter subspace and transmit only
coordinates. The codec does not prescribe a model architecture or optimizer.

`SeededFitness` is suitable for forward-only zeroth-order optimizers. A payload
contains contiguous generations, deterministic perturbation identity, exact
batch digests, and compact fitness vectors. Reconstructing the resulting model
is necessary but not sufficient for adversarial validation. A validator must
also recover the assigned deterministic batches and independently recompute a
sampled or complete set of transmitted fitness values.

## workload boundary

Downstream integrations implement `P2pWorkload`. Signed startup requires:

```rust
fn model_tensor_digest(
    &self,
    model: &Self::Model,
) -> anyhow::Result<ContentId>;
```

The Burn adapter implements this as the canonical
`FlattenedTensorPack` checksum, streamed one parameter tensor at a time to avoid
a second model-sized buffer. A workload that does not implement canonical
tensor identity fails closed when a signed genesis is used.

Full checkpoints use the normal model artifact path. Typed compact updates use:

```rust
fn validate_and_apply_workload_update(
    &self,
    base_model: Self::Model,
    descriptor: &ArtifactDescriptor,
    update: &WorkloadUpdateEnvelope,
    contract: &TrainingContractManifest,
    store: &FsArtifactStore,
    device: &Self::Device,
    replay: WorkloadUpdateReplayContext<'_>,
) -> anyhow::Result<ValidatedWorkloadUpdate<Self::Model>>;
```

The runtime invokes this only after checking the envelope, artifact, base head,
revision, signed contract, authenticated assignment lease, and cached
microshard identities. The workload remains responsible for
architecture-specific reconstruction and independent numerical replay.
Replay-required codecs cannot pass admission with `replay_verified=false`.

## recurrent and optimizer state

The contract makes state ownership explicit:

- optimizer: reset per window, peer-local until reconcile, canonical artifact,
  stateless forward-only, or a named custom policy
- scheduler: canonical global step, canonical accepted work, reset per window,
  or custom
- recurrent state: ephemeral, lease-scoped, canonical artifact, or custom

For TBPTT or recurrent models, lease-scoped state must be keyed by revision,
head, lease, and logical stream. It must be invalidated on incompatible
canonical reconciliation. Process-global recurrent state is not a valid
multi-run implementation.

`ArtifactWindows` and `DiLoCo` are distinct signed protocol choices.
Artifact-window endpoint averaging does not imply centralized optimizer
equivalence, particularly when each peer runs an adaptive optimizer over
non-IID local data. A convergence claim therefore requires a matched reference
with the same examples and optimizer-update count; exact artifact replay proves
protocol correctness, not learning parity.

`train_protocol_once` dispatches the active signed revision to the appropriate
runtime path. DiLoCo persists an outer optimizer separately from peer-local
inner optimization. Its default outer implementation supports SGD, momentum,
Nesterov acceleration, and coupled weight decay. Momentum state uses a compact
binary representation bound to the model schema, tensor layout, exact
parameter checksum, and parameter count; its content identifier is revalidated
before use. Adaptive outer optimizers remain workload-defined.

## lease partitioning

One microshard is the smallest exclusive assignment unit. A grouped exporter
must keep every group intact, then pack groups largest-first into the
least-loaded microshard. This avoids deterministic key-order imbalance without
breaking stream, sequence, episode, or document continuity.

The runtime never falls back from a missing deterministic shard assignment to
the full dataset. If the number of concurrently eligible trainers exceeds the
number of independently leasable partitions, excess peers receive no valid
work. Downstream schedulers should surface that as idle/no-work and provision a
shard count appropriate for the intended cohort.

For finite static datasets, window execution must rotate bounded micro-epochs
through the assigned shard rather than selecting the same prefix every round.
The rotation key is the signed window/lease identity; changing selection
semantics changes the dataset view or training contract.

## observation and ecs integration

Native execution exposes a non-blocking `TrainingWindowObserver`. Every
one-shot and continuous artifact window emits:

- `TrainingWindowStartedEvent` after planning and before data fetch
- `TrainingWindowCompletedEvent` after local artifact publication

Multiple observers can subscribe. Observers run inline and must enqueue or
record without blocking the training thread.

With the `ecs` feature, `P2pTrainingEcsObserver` sends those events through a
bounded `P2pTrainingIngressPlugin` channel. The plugin projects them onto one
`burn_ecs` run entity, including capability transitions, window metadata,
canonical reconciliation, and telemetry. Model tensors and optimizer state
remain owned by the workload.

Active native roles can be changed without rebuilding the transport:

```rust
control.update_roles(
    PeerRoleSet::new([PeerRole::Viewer]),
    Duration::from_secs(2),
)?;
```

The control plane permits read-only downgrade roles and compute roles from the
startup capability set, republishes the latest role set, and rejects new
training windows when the active role is no longer a trainer. Peer-directory
coalescing treats the newest role set as authoritative rather than unioning it
with stale trainer advertisements.

## production trust boundary

The current trust boundary remains validator quorum:

- signed contracts protect revision and genesis identity
- typed payload validation protects wire/schema/base-head identity
- deterministic reconstruction protects update interpretation
- independent replay protects claimed learning signal
- validator quorum protects canonical promotion

Do not equate signature verification with proof that a training update is
useful. Do not equate deterministic reconstruction with verification of
peer-reported fitness. Public or semi-trusted deployments need all of the
layers above, plus admission, rate limiting, anti-replay, and a validator
quorum greater than one.
