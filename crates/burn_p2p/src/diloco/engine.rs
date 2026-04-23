use std::collections::BTreeMap;

use anyhow::ensure;
use burn_p2p_core::{
    BaseCheckpointId, DiLoCoPolicy, DiLoCoStateSnapshot, ExperimentId, FlattenedTensorPack,
    GroupId, HeadId, PeerId, RoundCursor, RoundPhase, StateBlob, TrainingProtocol,
};
use burn_p2p_workload::DiLoCoWorkload;
use chrono::Utc;

use crate::diloco::codec::{
    EncodedPseudoGradient, aggregate_pseudo_gradients, decode_pseudo_gradient,
    encode_pseudo_gradient,
};

#[derive(Clone, Debug)]
/// Stores one reference peer participating in the DiLoCo round engine.
pub struct DiLoCoReferencePeer<P>
where
    P: DiLoCoWorkload,
{
    /// Stable peer identifier.
    pub peer_id: PeerId,
    /// Runtime model representation.
    pub model: P::Model,
    /// Serialized outer optimizer state.
    pub outer_optimizer_state: StateBlob,
    /// Serialized inner optimizer state retained across rounds, when present.
    pub inner_optimizer_state: Option<StateBlob>,
    /// Current round cursor.
    pub round_cursor: RoundCursor,
    /// Last published checkpoint head, when present.
    pub checkpoint_head_id: Option<HeadId>,
}

impl<P> DiLoCoReferencePeer<P>
where
    P: DiLoCoWorkload,
{
    /// Bootstraps one reference peer from the workload's initial model.
    pub fn bootstrap(
        workload: &P,
        peer_id: PeerId,
        policy: &DiLoCoPolicy,
        base_checkpoint_id: BaseCheckpointId,
    ) -> anyhow::Result<Self> {
        let device = workload.runtime_device();
        let model = workload.init_model(&device);
        let outer_optimizer_state =
            workload.initialize_outer_optimizer_state(&model, &policy.outer_optimizer_policy)?;
        Ok(Self {
            peer_id,
            model,
            outer_optimizer_state,
            inner_optimizer_state: None,
            round_cursor: RoundCursor::new(base_checkpoint_id, policy.num_inner_steps),
            checkpoint_head_id: None,
        })
    }

    /// Restores one reference peer from a published checkpoint snapshot.
    pub fn from_checkpoint(
        workload: &P,
        peer_id: PeerId,
        policy: &DiLoCoPolicy,
        checkpoint: &DiLoCoReferenceCheckpoint,
    ) -> anyhow::Result<Self> {
        let device = workload.runtime_device();
        let model = workload.import_parameter_pack(&device, &checkpoint.parameters)?;
        let outer_optimizer_state = match checkpoint.snapshot.outer_optimizer_state.as_ref() {
            Some(state) => workload.load_outer_optimizer_state(state)?,
            None => {
                workload.initialize_outer_optimizer_state(&model, &policy.outer_optimizer_policy)?
            }
        };
        Ok(Self {
            peer_id,
            model,
            outer_optimizer_state,
            inner_optimizer_state: None,
            round_cursor: checkpoint.snapshot.round_cursor.clone(),
            checkpoint_head_id: checkpoint.snapshot.checkpoint_head_id.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Holds one encoded peer contribution captured during a reference DiLoCo round.
pub struct DiLoCoPeerContribution {
    /// Contributing peer.
    pub peer_id: PeerId,
    /// Encoded transport payload.
    pub encoded: EncodedPseudoGradient,
    /// Decoded pseudo-gradient after transport validation.
    pub decoded_gradient: FlattenedTensorPack,
}

#[derive(Clone, Debug, PartialEq)]
/// Stores one cold-path checkpoint published by the reference DiLoCo engine.
pub struct DiLoCoReferenceCheckpoint {
    /// Flattened parameters for the published checkpoint.
    pub parameters: FlattenedTensorPack,
    /// Durable state snapshot anchored at the checkpoint.
    pub snapshot: DiLoCoStateSnapshot,
}

#[derive(Clone, Debug, PartialEq)]
/// Summarizes one completed reference DiLoCo round.
pub struct DiLoCoReferenceRoundOutcome {
    /// Participants selected for the round.
    pub participant_peer_ids: Vec<PeerId>,
    /// Deterministic aggregation group identifier.
    pub group_id: GroupId,
    /// Cursor that was completed.
    pub completed_round: RoundCursor,
    /// Next cursor assigned to successful participants.
    pub next_round_cursor: RoundCursor,
    /// Aggregated pseudo-gradient applied by the outer loop.
    pub aggregate: FlattenedTensorPack,
    /// Individual peer contributions.
    pub contributions: Vec<DiLoCoPeerContribution>,
    /// Published checkpoint, when the round crossed the configured interval.
    pub published_checkpoint: Option<DiLoCoReferenceCheckpoint>,
}

#[derive(Clone, Debug, PartialEq)]
/// Deterministic in-memory/native reference coordinator for the DiLoCo round engine.
pub struct DiLoCoReferenceCoordinator {
    /// Experiment scope for the simulated rounds.
    pub experiment_id: ExperimentId,
    /// Revision scope for the simulated rounds.
    pub revision_id: burn_p2p_core::RevisionId,
    /// DiLoCo policy used for the rounds.
    pub policy: DiLoCoPolicy,
    /// Encoded chunk size used by the simulated transport.
    pub chunk_size_bytes: usize,
}

impl DiLoCoReferenceCoordinator {
    /// Creates a new coordinator.
    pub fn new(
        experiment_id: ExperimentId,
        revision_id: burn_p2p_core::RevisionId,
        policy: DiLoCoPolicy,
    ) -> Self {
        Self {
            experiment_id,
            revision_id,
            policy,
            chunk_size_bytes: 1024,
        }
    }

    /// Overrides the encoded chunk size used during transport simulation.
    pub fn with_chunk_size_bytes(mut self, chunk_size_bytes: usize) -> Self {
        self.chunk_size_bytes = chunk_size_bytes.max(1);
        self
    }

    /// Runs one deterministic DiLoCo round over the selected peers.
    pub fn run_round<P>(
        &self,
        workload: &P,
        peers: &mut [DiLoCoReferencePeer<P>],
        peer_batches: &BTreeMap<PeerId, Vec<P::Batch>>,
    ) -> anyhow::Result<DiLoCoReferenceRoundOutcome>
    where
        P: DiLoCoWorkload,
        P::Batch: Clone,
    {
        ensure!(!peers.is_empty(), "cannot run a DiLoCo round without peers");

        let mut indices = (0..peers.len()).collect::<Vec<_>>();
        indices.sort_by(|left, right| {
            peers[*left]
                .peer_id
                .as_str()
                .cmp(peers[*right].peer_id.as_str())
        });
        let participant_count = indices.len().min(self.policy.target_group_size as usize);
        ensure!(
            participant_count >= self.policy.minimum_group_size as usize,
            "need at least {} peers for a DiLoCo round, found {}",
            self.policy.minimum_group_size,
            participant_count
        );
        let participant_indices = indices
            .into_iter()
            .take(participant_count)
            .collect::<Vec<_>>();
        let participant_peer_ids = participant_indices
            .iter()
            .map(|index| peers[*index].peer_id.clone())
            .collect::<Vec<_>>();

        let baseline_cursor = peers[participant_indices[0]].round_cursor.clone();
        let baseline_pack = workload.export_parameter_pack(&peers[participant_indices[0]].model)?;
        let baseline_checksum = baseline_pack.checksum()?;
        for index in &participant_indices[1..] {
            let peer = &peers[*index];
            ensure!(
                peer.round_cursor.round_id == baseline_cursor.round_id,
                "stale DiLoCo round: peer {} is on round {}, expected {}",
                peer.peer_id.as_str(),
                peer.round_cursor.round_id,
                baseline_cursor.round_id
            );
            ensure!(
                peer.round_cursor.base_checkpoint_id == baseline_cursor.base_checkpoint_id,
                "peer {} has base checkpoint {}, expected {}",
                peer.peer_id.as_str(),
                peer.round_cursor.base_checkpoint_id.as_str(),
                baseline_cursor.base_checkpoint_id.as_str()
            );
            let pack = workload.export_parameter_pack(&peer.model)?;
            ensure!(
                pack.checksum()? == baseline_checksum,
                "peer {} model checksum drifted away from shared DiLoCo base {}",
                peer.peer_id.as_str(),
                baseline_checksum.as_str()
            );
        }

        let group_id = GroupId::derive(&(
            baseline_cursor.round_id.as_u64(),
            baseline_cursor.base_checkpoint_id.as_str(),
            participant_peer_ids
                .iter()
                .map(|peer_id| peer_id.as_str())
                .collect::<Vec<_>>(),
        ))?;

        let mut completed_round = baseline_cursor.clone();
        completed_round.group_id = Some(group_id.clone());
        completed_round.phase = RoundPhase::BuildPseudoGradient;

        let mut contributions = Vec::with_capacity(participant_indices.len());
        for index in &participant_indices {
            let peer = &mut peers[*index];
            peer.round_cursor.group_id = Some(group_id.clone());
            peer.round_cursor.phase = RoundPhase::InnerTrain;

            let batches = peer_batches.get(&peer.peer_id).cloned().unwrap_or_default();
            let inner_report = workload.run_inner_steps(
                &peer.model,
                &batches,
                self.policy.num_inner_steps,
                peer.inner_optimizer_state.as_ref(),
            )?;
            let pseudo_gradient =
                workload.build_pseudo_gradient(&baseline_pack, &inner_report.local_parameters)?;
            let encoded = encode_pseudo_gradient(
                self.experiment_id.clone(),
                self.revision_id.clone(),
                peer.peer_id.clone(),
                completed_round.clone(),
                self.policy.codec.clone(),
                &pseudo_gradient,
                self.chunk_size_bytes,
            )?;
            let decoded_gradient = decode_pseudo_gradient(&encoded.manifest, &encoded.chunks)?;
            peer.inner_optimizer_state = inner_report.inner_optimizer_state.clone();
            contributions.push(DiLoCoPeerContribution {
                peer_id: peer.peer_id.clone(),
                encoded,
                decoded_gradient,
            });
        }

        completed_round.phase = RoundPhase::Aggregate;
        let aggregate = aggregate_pseudo_gradients(
            &self.policy.aggregation_policy,
            &contributions
                .iter()
                .map(|contribution| contribution.decoded_gradient.clone())
                .collect::<Vec<_>>(),
        )?;

        let mut updated_pack = None;
        let mut checkpoint_head_id = None;
        for index in &participant_indices {
            let peer = &mut peers[*index];
            peer.round_cursor.phase = RoundPhase::OuterApply;
            let (next_pack, next_outer_optimizer_state) = workload.apply_aggregated_outer_update(
                &baseline_pack,
                &aggregate,
                &peer.outer_optimizer_state,
                &self.policy.outer_optimizer_policy,
            )?;
            let device = workload.runtime_device();
            peer.model = workload.import_parameter_pack(&device, &next_pack)?;
            peer.outer_optimizer_state =
                workload.save_outer_optimizer_state(&next_outer_optimizer_state)?;
            updated_pack.get_or_insert(next_pack);
        }
        let updated_pack = updated_pack.expect("participant set is non-empty");
        let next_base_checkpoint_id = BaseCheckpointId::new(updated_pack.checksum()?.into_inner());
        let checkpoint_due = (baseline_cursor.round_id.as_u64() + 1)
            .is_multiple_of(u64::from(self.policy.checkpoint_interval_rounds.max(1)));
        let published_checkpoint = if checkpoint_due {
            let head_id = HeadId::new(format!("diloco-{}", next_base_checkpoint_id.as_str()));
            checkpoint_head_id = Some(head_id.clone());
            Some(DiLoCoReferenceCheckpoint {
                parameters: updated_pack.clone(),
                snapshot: DiLoCoStateSnapshot {
                    experiment_id: self.experiment_id.clone(),
                    revision_id: self.revision_id.clone(),
                    training_protocol: TrainingProtocol::DiLoCo(self.policy.clone()),
                    round_cursor: baseline_cursor.advance(next_base_checkpoint_id.clone()),
                    checkpoint_head_id: Some(head_id),
                    latest_gradient_manifest_id: contributions
                        .first()
                        .map(|contribution| contribution.encoded.manifest.manifest_id.clone()),
                    current_parameter_checksum: Some(updated_pack.checksum()?),
                    outer_optimizer_state: Some(
                        peers[participant_indices[0]].outer_optimizer_state.clone(),
                    ),
                    signature_bundle: Vec::new(),
                    updated_at: Utc::now(),
                },
            })
        } else {
            None
        };

        let next_round_cursor = baseline_cursor.advance(next_base_checkpoint_id.clone());
        for index in &participant_indices {
            let peer = &mut peers[*index];
            peer.round_cursor = next_round_cursor.clone();
            if let Some(head_id) = checkpoint_head_id.as_ref() {
                peer.checkpoint_head_id = Some(head_id.clone());
            }
        }

        Ok(DiLoCoReferenceRoundOutcome {
            participant_peer_ids,
            group_id,
            completed_round,
            next_round_cursor,
            aggregate,
            contributions,
            published_checkpoint,
        })
    }
}
