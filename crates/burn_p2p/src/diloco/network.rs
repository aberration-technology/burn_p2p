use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use anyhow::{Context, anyhow, ensure};
use chrono::Utc;

use super::{
    DiLoCoPeerContribution, EncodedPseudoGradient, decode_pseudo_gradient, encode_pseudo_gradient,
    engine::aggregate_peer_contributions,
};
use crate::Duration;
use crate::runtime_support::trace_to_stderr;
use crate::runtime_support::{
    connected_peer_ids, load_head_state, load_json, persist_head_state, persist_json,
    runtime_training_protocol, verify_diloco_gradient_manifest_signature,
    verify_diloco_state_snapshot_signature,
};
use crate::training::load_model_for_head;
use crate::{
    ArtifactKind, AssignmentLease, BaseCheckpointId, ContentId, ControlPlaneSnapshot,
    DiLoCoAggregateReady, DiLoCoInnerLoopReport, DiLoCoPolicy, DiLoCoRoundFinalize,
    DiLoCoRoundOffer, DiLoCoStateSnapshot, DiLoCoTopologyMode, DiLoCoWorkload, EvalSplit,
    ExperimentHandle, FlattenedTensorPack, FsArtifactStore, GroupId, HeadDescriptor, HeadId,
    Instant, NodeRuntimeState, PeerId, PseudoGradientManifest, RoundCursor, RoundId, RoundPhase,
    RunningNode, SlotAssignmentState, SlotRuntimeState, StateBlob, StorageConfig, TrainingProtocol,
};

const DILOCO_TRACE_ENV: &str = "BURN_P2P_DILOCO_TRACE";
const DILOCO_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DILOCO_GRADIENT_MAX_BACKOFF: Duration = Duration::from_secs(4);
// Logical polling slice for one DiLoCo phase. The runtime keeps the underlying
// transport request alive for its longer lifetime and coalesces retries.
const DILOCO_REQUEST_TIMEOUT_CAP: Duration =
    burn_p2p_swarm::CONTROL_REQUEST_RESPONSE_TIMEOUT.saturating_add(Duration::from_secs(1));
// State synchronization runs during connection churn and must share the full
// request-response lifetime. Logical retries remain coalesced by the runtime.
const DILOCO_STATE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
const DILOCO_STATE_TRANSFER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
const DILOCO_FINALIZE_BEST_EFFORT_BUDGET: Duration = Duration::from_millis(500);
const DILOCO_FINALIZE_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);
// Stay comfortably below the control codec's 10 MiB response limit while
// avoiding one request/response stream per MiB of model parameters.
const DILOCO_CHUNK_SIZE_BYTES: usize = 256 * 1024;
const DILOCO_CHUNK_FETCH_WINDOW: usize = 4;
const DILOCO_CHUNK_WINDOW_TIMEOUT: Duration = Duration::from_secs(2);
const DILOCO_MAX_GRADIENT_CHUNKS: u32 = 65_536;

fn diloco_trace(args: std::fmt::Arguments<'_>) {
    trace_to_stderr(DILOCO_TRACE_ENV, "burn_p2p diloco", args);
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct PersistedDiLoCoRuntimeState {
    snapshot: DiLoCoStateSnapshot,
    current_parameters: FlattenedTensorPack,
    outer_optimizer_state: StateBlob,
    inner_optimizer_state: Option<StateBlob>,
}

#[derive(Clone, Debug, PartialEq)]
/// Wall-clock phase timings captured by one local DiLoCo participant.
pub struct DiLoCoRoundTiming {
    /// Time spent synchronizing/bootstraping durable round state.
    pub state_sync_ms: u64,
    /// Time spent forming the compatible participant cohort.
    pub matchmaking_ms: u64,
    /// Time spent in workload-local inner optimization.
    pub local_inner_loop_ms: u64,
    /// Time spent encoding, publishing, collecting, and aggregating gradients.
    pub gradient_exchange_ms: u64,
    /// Time spent encoding and atomically publishing the local round gradient.
    pub gradient_publish_ms: u64,
    /// Time spent collecting and decoding remote round gradients.
    pub gradient_collection_ms: u64,
    /// Time spent applying and persisting the outer optimizer update.
    pub outer_apply_ms: u64,
    /// Time spent evaluating/materializing a due cold-path checkpoint.
    pub checkpoint_publish_ms: u64,
    /// Total time inside the protocol round after batches were materialized.
    pub total_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiLoCoRoundOutcome {
    /// Automatically planned data lease used for the local inner loop.
    ///
    /// This is `None` when callers provide already-materialized batches through
    /// [`RunningNode::diloco_round_once_with_batches`].
    pub training_lease: Option<AssignmentLease>,
    pub participant_peer_ids: Vec<PeerId>,
    pub group_id: GroupId,
    pub completed_round: RoundCursor,
    pub next_round_cursor: RoundCursor,
    pub aggregate: FlattenedTensorPack,
    /// Contributions materialized by this peer.
    ///
    /// The round reducer retains the complete cohort; followers retain only
    /// their local contribution after fetching the reduced aggregate.
    pub contributions: Vec<DiLoCoPeerContribution>,
    /// Deterministic rotating peer that reduced this round's local gradients.
    pub reducer_peer_id: PeerId,
    /// Manifest for the reduced pseudo-gradient applied by every participant.
    pub aggregate_manifest: PseudoGradientManifest,
    /// Local gradient manifests committed by the reducer in canonical peer order.
    pub contribution_manifest_ids: Vec<ContentId>,
    /// Workload metrics and step count emitted by this peer's local inner loop.
    pub local_inner_report: DiLoCoInnerLoopReport,
    pub local_gradient_manifest: PseudoGradientManifest,
    pub current_parameters: FlattenedTensorPack,
    pub published_checkpoint: Option<HeadDescriptor>,
    /// Local phase timings for utilization and protocol-overhead analysis.
    pub timing: DiLoCoRoundTiming,
}

#[derive(Clone, Debug)]
struct RemoteDiLoCoState {
    snapshot: DiLoCoStateSnapshot,
    current_parameters: FlattenedTensorPack,
    outer_optimizer_state: StateBlob,
}

#[derive(Clone, Debug)]
struct RemoteDiLoCoSnapshot {
    peer_id: PeerId,
    snapshot: DiLoCoStateSnapshot,
}

struct DiLoCoFinalizeBroadcast<'a> {
    experiment: &'a ExperimentHandle,
    participants: &'a [PeerId],
    local_peer_id: &'a PeerId,
    round_cursor: &'a RoundCursor,
    participant_count: u16,
    aggregate_checksum: Option<ContentId>,
}

fn request_timeout(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        None
    } else {
        Some(remaining.min(DILOCO_REQUEST_TIMEOUT_CAP))
    }
}

fn diloco_reducer_peer<'a>(
    participants: &'a [PeerId],
    round_cursor: &RoundCursor,
) -> Option<&'a PeerId> {
    if participants.is_empty() {
        return None;
    }
    let round = usize::try_from(round_cursor.round_id.as_u64()).ok()?;
    participants.get(round % participants.len())
}

fn checkpoint_due(policy: &DiLoCoPolicy, round: &RoundCursor) -> bool {
    (round.round_id.as_u64() + 1)
        .is_multiple_of(u64::from(policy.checkpoint_interval_rounds.max(1)))
}

fn diloco_state_signature_acceptable(
    policy: &DiLoCoPolicy,
    snapshot: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    state: &DiLoCoStateSnapshot,
) -> bool {
    if state.signature_bundle.is_empty() {
        return !policy.require_signed_peer_payloads;
    }
    if diloco_peer_has_auth(&snapshot.control_plane, peer_id) {
        verify_diloco_state_snapshot_signature(&snapshot.control_plane, peer_id, state)
    } else {
        !policy.require_signed_peer_payloads
    }
}

fn diloco_state_compatibility_error(
    policy: &DiLoCoPolicy,
    telemetry: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    state: &DiLoCoStateSnapshot,
    experiment: &ExperimentHandle,
    round_cursor: &RoundCursor,
) -> Option<String> {
    if state.experiment_id != experiment.experiment_id {
        return Some("experiment mismatch".into());
    }
    if state.revision_id != experiment.revision_id {
        return Some("revision mismatch".into());
    }
    if state.round_cursor.round_id != round_cursor.round_id {
        return Some(format!(
            "round mismatch: remote={} local={}",
            state.round_cursor.round_id, round_cursor.round_id
        ));
    }
    if state.round_cursor.base_checkpoint_id != round_cursor.base_checkpoint_id {
        return Some(format!(
            "base mismatch: remote={} local={}",
            state.round_cursor.base_checkpoint_id, round_cursor.base_checkpoint_id
        ));
    }
    if state.training_protocol != TrainingProtocol::DiLoCo(policy.clone()) {
        return Some("training protocol mismatch".into());
    }
    if !diloco_state_signature_acceptable(policy, telemetry, peer_id, state) {
        return Some("state signature rejected".into());
    }
    None
}

fn diloco_matchmaking_compatibility_error(
    policy: &DiLoCoPolicy,
    telemetry: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    state: &DiLoCoStateSnapshot,
    experiment: &ExperimentHandle,
    round_cursor: &RoundCursor,
) -> Option<String> {
    if let Some(reason) = diloco_state_compatibility_error(
        policy,
        telemetry,
        peer_id,
        state,
        experiment,
        round_cursor,
    ) {
        return Some(reason);
    }
    if !matches!(
        state.round_cursor.phase,
        RoundPhase::SyncBase | RoundPhase::Matchmake
    ) {
        return Some(format!(
            "peer is not accepting cohort membership in phase {:?}",
            state.round_cursor.phase
        ));
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiLoCoGroupBarrierStage {
    CohortAnnounced,
    InnerTrainReady,
}

#[derive(Clone, Copy, Debug)]
struct DiLoCoGroupBarrier<'a> {
    round_cursor: &'a RoundCursor,
    group_id: &'a GroupId,
    stage: DiLoCoGroupBarrierStage,
}

fn diloco_group_barrier_error(
    policy: &DiLoCoPolicy,
    telemetry: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    state: &DiLoCoStateSnapshot,
    experiment: &ExperimentHandle,
    barrier: DiLoCoGroupBarrier<'_>,
) -> Option<String> {
    if let Some(reason) = diloco_state_compatibility_error(
        policy,
        telemetry,
        peer_id,
        state,
        experiment,
        barrier.round_cursor,
    ) {
        return Some(reason);
    }
    if state.round_cursor.group_id.as_ref() != Some(barrier.group_id) {
        return Some(format!(
            "group mismatch: remote={:?} local={}",
            state.round_cursor.group_id.as_ref().map(GroupId::as_str),
            barrier.group_id.as_str()
        ));
    }
    if state.round_cursor.phase == RoundPhase::SyncBase {
        return Some("peer has not entered the matched cohort".into());
    }
    if barrier.stage == DiLoCoGroupBarrierStage::InnerTrainReady
        && state.round_cursor.phase == RoundPhase::Matchmake
    {
        return Some("peer has not acknowledged inner-train readiness".into());
    }
    None
}

fn diloco_manifest_signature_acceptable(
    policy: &DiLoCoPolicy,
    snapshot: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    manifest: &PseudoGradientManifest,
) -> bool {
    if manifest.signature_bundle.is_empty() {
        return !policy.require_signed_peer_payloads;
    }
    if diloco_peer_has_auth(&snapshot.control_plane, peer_id) {
        verify_diloco_gradient_manifest_signature(&snapshot.control_plane, peer_id, manifest)
    } else {
        !policy.require_signed_peer_payloads
    }
}

fn diloco_peer_has_auth(control_plane: &ControlPlaneSnapshot, peer_id: &PeerId) -> bool {
    control_plane
        .auth_announcements
        .iter()
        .any(|announcement| &announcement.peer_id == peer_id)
}

fn transport_decoded_contribution(
    peer_id: PeerId,
    encoded: EncodedPseudoGradient,
) -> anyhow::Result<DiLoCoPeerContribution> {
    ensure!(
        encoded.manifest.peer_id == peer_id,
        "DiLoCo contribution peer {} does not match manifest peer {}",
        peer_id.as_str(),
        encoded.manifest.peer_id.as_str()
    );
    let decoded_gradient = decode_pseudo_gradient(&encoded.manifest, &encoded.chunks)?;
    Ok(DiLoCoPeerContribution {
        peer_id,
        encoded,
        decoded_gradient,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiLoCoCandidateRoute {
    Direct,
    Unknown,
    RelayOnly,
}

impl DiLoCoCandidateRoute {
    fn preference_rank(self) -> u8 {
        match self {
            Self::Direct => 0,
            Self::Unknown => 1,
            Self::RelayOnly => 2,
        }
    }
}

fn diloco_candidate_route(
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
) -> DiLoCoCandidateRoute {
    let mut saw_relay = false;
    for address in control_plane
        .peer_directory_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .flat_map(|announcement| announcement.addresses.iter())
    {
        if address.is_relay_circuit() {
            saw_relay = true;
        } else {
            return DiLoCoCandidateRoute::Direct;
        }
    }
    if saw_relay {
        DiLoCoCandidateRoute::RelayOnly
    } else {
        DiLoCoCandidateRoute::Unknown
    }
}

fn compare_diloco_candidate_transport(
    policy: &DiLoCoPolicy,
    control_plane: &ControlPlaneSnapshot,
    left: &PeerId,
    right: &PeerId,
) -> Ordering {
    if policy.topology_policy.prefer_low_latency {
        diloco_candidate_route(control_plane, left)
            .preference_rank()
            .cmp(&diloco_candidate_route(control_plane, right).preference_rank())
    } else {
        Ordering::Equal
    }
}

fn relay_allowed_for_diloco_candidate(
    policy: &DiLoCoPolicy,
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
) -> bool {
    policy.topology_policy.allow_relay
        || diloco_candidate_route(control_plane, peer_id) != DiLoCoCandidateRoute::RelayOnly
}

fn role_allows_diloco_training(control_plane: &ControlPlaneSnapshot, peer_id: &PeerId) -> bool {
    control_plane
        .peer_directory_announcements
        .iter()
        .rev()
        .find(|announcement| &announcement.peer_id == peer_id)
        .and_then(|announcement| announcement.advertised_roles.as_ref())
        .is_none_or(|roles| {
            roles.contains(&crate::PeerRole::TrainerGpu)
                || roles.contains(&crate::PeerRole::TrainerCpu)
                || roles.contains(&crate::PeerRole::BrowserTrainerWgpu)
                || roles.contains(&crate::PeerRole::BrowserTrainer)
        })
}

fn order_diloco_candidates(
    policy: &DiLoCoPolicy,
    control_plane: &ControlPlaneSnapshot,
    local_peer_id: &PeerId,
    round_cursor: &RoundCursor,
    candidates: &mut Vec<PeerId>,
) -> anyhow::Result<()> {
    candidates.retain(|peer_id| {
        role_allows_diloco_training(control_plane, peer_id)
            && relay_allowed_for_diloco_candidate(policy, control_plane, peer_id)
    });
    match policy.topology_policy.mode {
        DiLoCoTopologyMode::DeterministicRendezvous | DiLoCoTopologyMode::RelayAssisted => {
            candidates.sort_by(|left, right| {
                compare_diloco_candidate_transport(policy, control_plane, left, right)
                    .then_with(|| left.as_str().cmp(right.as_str()))
            });
        }
        DiLoCoTopologyMode::GossipNeighborhood => {
            let round_id = round_cursor.round_id.as_u64();
            let mut ordered = candidates
                .iter()
                .map(|peer_id| {
                    let key = ContentId::derive(&(
                        "diloco-gossip-neighborhood",
                        local_peer_id.as_str(),
                        round_id,
                        peer_id.as_str(),
                    ))?;
                    Ok((key, peer_id.clone()))
                })
                .collect::<Result<Vec<_>, burn_p2p_core::SchemaError>>()?;
            ordered.sort_by(|left, right| {
                compare_diloco_candidate_transport(policy, control_plane, &left.1, &right.1)
                    .then_with(|| {
                        left.0
                            .as_str()
                            .cmp(right.0.as_str())
                            .then_with(|| left.1.as_str().cmp(right.1.as_str()))
                    })
            });
            for (slot, (_, peer_id)) in candidates.iter_mut().zip(ordered) {
                *slot = peer_id;
            }
        }
    }
    Ok(())
}

fn derive_group_id(round: &RoundCursor, participants: &[PeerId]) -> anyhow::Result<GroupId> {
    let mut participant_keys = participants
        .iter()
        .map(|peer_id| peer_id.as_str().to_owned())
        .collect::<Vec<_>>();
    participant_keys.sort();
    GroupId::derive(&(
        round.round_id.as_u64(),
        round.base_checkpoint_id.as_str(),
        participant_keys,
    ))
    .map_err(anyhow::Error::from)
}

fn assigned_diloco_cohort(
    assigned_peer_ids: &BTreeSet<PeerId>,
    local_peer_id: &PeerId,
    target_group_size: usize,
) -> Option<Vec<PeerId>> {
    let assigned_peer_ids = assigned_peer_ids.iter().cloned().collect::<Vec<_>>();
    let local_index = assigned_peer_ids
        .iter()
        .position(|peer_id| peer_id == local_peer_id)?;
    let target_group_size = target_group_size.max(1);
    let group_start = (local_index / target_group_size) * target_group_size;
    let group_end = (group_start + target_group_size).min(assigned_peer_ids.len());
    Some(assigned_peer_ids[group_start..group_end].to_vec())
}

impl PersistedDiLoCoRuntimeState {
    fn publishable_snapshot(&self) -> anyhow::Result<DiLoCoStateSnapshot> {
        let mut snapshot = self.snapshot.clone();
        // Optimizer state has a dedicated control-plane endpoint. Keeping it out
        // of the frequently polled snapshot avoids retransmitting model-sized
        // state during matchmaking, barriers, and gradient collection.
        snapshot.outer_optimizer_state = None;
        snapshot.current_parameter_checksum = Some(self.current_parameters.checksum()?);
        Ok(snapshot)
    }
}

impl<P> RunningNode<P>
where
    P: DiLoCoWorkload,
    P::Batch: Clone,
{
    pub fn diloco_round_once_with_batches(
        &mut self,
        experiment: &ExperimentHandle,
        batches: &[P::Batch],
    ) -> anyhow::Result<DiLoCoRoundOutcome> {
        self.diloco_round_once_with_batches_and_lease(experiment, batches, None)
    }

    pub(crate) fn diloco_round_once_with_batches_and_lease(
        &mut self,
        experiment: &ExperimentHandle,
        batches: &[P::Batch],
        training_lease: Option<AssignmentLease>,
    ) -> anyhow::Result<DiLoCoRoundOutcome> {
        let round_started = Instant::now();
        let storage = self
            .config()
            .storage
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("DiLoCo rounds require configured storage"))?;
        let telemetry_snapshot = self.telemetry().snapshot();
        let policy = match runtime_training_protocol(self.config(), &telemetry_snapshot, experiment)
        {
            TrainingProtocol::DiLoCo(policy) => policy,
            TrainingProtocol::ArtifactWindows => {
                anyhow::bail!(
                    "revision {} is configured for TrainingProtocol::ArtifactWindows; use train_window_once instead",
                    experiment.revision_id.as_str()
                )
            }
        };
        let local_peer_id = telemetry_snapshot
            .local_peer_id
            .clone()
            .ok_or_else(|| anyhow!("runtime does not have a local peer id yet"))?;
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)
            .context("persist DiLoCo slot assignment")?;
        self.ensure_experiment_topics(experiment)
            .context("subscribe DiLoCo experiment topics")?;

        let store = FsArtifactStore::new(storage.root.clone());
        store
            .ensure_layout()
            .context("ensure DiLoCo artifact-store layout")?;
        let base_head = self
            .ensure_diloco_base_head(experiment)
            .context("resolve DiLoCo base head")?;
        let mut state = self
            .load_or_bootstrap_diloco_state(experiment, &policy, base_head.as_ref(), &store)
            .context("load or bootstrap DiLoCo runtime state")?;
        state.snapshot.training_protocol = TrainingProtocol::DiLoCo(policy.clone());
        state.snapshot.round_cursor.num_inner_steps = policy.num_inner_steps;
        state.snapshot.round_cursor.group_id = None;
        state.snapshot.round_cursor.phase = RoundPhase::SyncBase;
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish completed DiLoCo round state")?;
        let state_sync_ms = round_started.elapsed().as_millis() as u64;

        let matchmaking_started = Instant::now();
        let participant_peer_ids = self
            .matchmake_diloco_participants(
                experiment,
                &policy,
                &local_peer_id,
                &state.snapshot.round_cursor,
                training_lease.as_ref(),
            )
            .context("matchmake DiLoCo participants")?;
        let group_id = derive_group_id(&state.snapshot.round_cursor, &participant_peer_ids)?;
        let mut completed_round = state.snapshot.round_cursor.clone();
        completed_round.group_id = Some(group_id.clone());
        completed_round.phase = RoundPhase::Matchmake;
        state.snapshot.round_cursor = completed_round.clone();
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo matchmaking state")?;
        self.wait_for_diloco_group_barrier(
            experiment,
            &policy,
            &local_peer_id,
            &participant_peer_ids,
            DiLoCoGroupBarrier {
                round_cursor: &completed_round,
                group_id: &group_id,
                stage: DiLoCoGroupBarrierStage::CohortAnnounced,
            },
        )
        .context("wait for DiLoCo cohort-announcement barrier")?;
        completed_round.phase = RoundPhase::InnerTrain;
        state.snapshot.round_cursor = completed_round.clone();
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo inner-train readiness")?;
        self.wait_for_diloco_group_barrier(
            experiment,
            &policy,
            &local_peer_id,
            &participant_peer_ids,
            DiLoCoGroupBarrier {
                round_cursor: &completed_round,
                group_id: &group_id,
                stage: DiLoCoGroupBarrierStage::InnerTrainReady,
            },
        )
        .context("wait for DiLoCo inner-train readiness barrier")?;
        let matchmaking_ms = matchmaking_started.elapsed().as_millis() as u64;
        self.update_runtime_state(
            NodeRuntimeState::TrainingWindow,
            Some(SlotRuntimeState::Training(assignment.clone())),
        );
        let local_inner_started = Instant::now();
        let (local_gradient, saved_inner_optimizer_state, local_inner_report) =
            (|| -> anyhow::Result<_> {
                let project = &mut self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node")
                    .project;
                let device = project.runtime_device();
                let base_model =
                    project.import_parameter_pack(&device, &state.current_parameters)?;
                let loaded_inner_optimizer_state = state
                    .inner_optimizer_state
                    .as_ref()
                    .map(|blob| project.load_inner_optimizer_state(blob))
                    .transpose()?;
                let inner_report = project.run_inner_steps(
                    &base_model,
                    batches,
                    policy.num_inner_steps,
                    loaded_inner_optimizer_state.as_ref(),
                )?;
                let saved_inner_optimizer_state = inner_report
                    .inner_optimizer_state
                    .as_ref()
                    .map(|blob| project.save_inner_optimizer_state(blob))
                    .transpose()?;
                let pseudo_gradient = project.build_pseudo_gradient(
                    &state.current_parameters,
                    &inner_report.local_parameters,
                )?;
                Ok((pseudo_gradient, saved_inner_optimizer_state, inner_report))
            })()
            .context("run local DiLoCo inner loop")?;
        let local_inner_loop_ms = local_inner_started.elapsed().as_millis() as u64;
        state.inner_optimizer_state = saved_inner_optimizer_state;

        let gradient_exchange_started = Instant::now();
        completed_round.phase = RoundPhase::BuildPseudoGradient;
        state.snapshot.round_cursor = completed_round.clone();
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo pseudo-gradient state")?;
        let encoded = encode_pseudo_gradient(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            local_peer_id.clone(),
            completed_round.clone(),
            policy.codec.clone(),
            &local_gradient,
            DILOCO_CHUNK_SIZE_BYTES,
        )?;
        let local_gradient_manifest = encoded.manifest.clone();
        self.control
            .publish_diloco_gradient(encoded.manifest.clone(), encoded.chunks.clone())
            .context("publish local DiLoCo pseudo-gradient")?;
        state.snapshot.latest_gradient_manifest_id =
            Some(local_gradient_manifest.manifest_id.clone());
        state.snapshot.round_cursor.phase = RoundPhase::Aggregate;
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo aggregate-wait state")?;
        let gradient_publish_ms = gradient_exchange_started.elapsed().as_millis() as u64;

        let gradient_collection_started = Instant::now();
        let local_contribution =
            transport_decoded_contribution(local_peer_id.clone(), encoded.clone())
                .context("decode local DiLoCo pseudo-gradient transport representation")?;
        let reducer_peer_id = diloco_reducer_peer(&participant_peer_ids, &completed_round)
            .cloned()
            .ok_or_else(|| anyhow!("cannot select a DiLoCo reducer from an empty cohort"))?;
        let (aggregate, contributions, aggregate_manifest, contribution_manifest_ids) =
            if reducer_peer_id == local_peer_id {
                let mut contributions = vec![local_contribution];
                let (remote_contributions, remote_observations) = self
                    .collect_remote_diloco_contributions(
                        experiment,
                        &participant_peer_ids,
                        &local_peer_id,
                        &local_gradient_manifest,
                        &completed_round,
                        &policy,
                    )
                    .context("collect remote DiLoCo contributions as round reducer")?;
                contributions.extend(remote_contributions);
                let contributions_by_peer = contributions
                    .iter()
                    .map(|contribution| (contribution.peer_id.clone(), contribution))
                    .collect::<BTreeMap<_, _>>();
                let missing_peer_ids = participant_peer_ids
                    .iter()
                    .filter(|peer_id| !contributions_by_peer.contains_key(*peer_id))
                    .map(PeerId::as_str)
                    .collect::<Vec<_>>();
                ensure!(
                    contributions.len() == participant_peer_ids.len(),
                    "DiLoCo reducer collected {} of {} declared participant(s); missing={missing_peer_ids:?}; last_observations={remote_observations:?}",
                    contributions.len(),
                    participant_peer_ids.len()
                );
                let contribution_manifest_ids = participant_peer_ids
                    .iter()
                    .map(|peer_id| {
                        contributions_by_peer
                            .get(peer_id)
                            .map(|contribution| contribution.encoded.manifest.manifest_id.clone())
                            .ok_or_else(|| {
                                anyhow!(
                                    "missing manifest commitment for DiLoCo participant {}",
                                    peer_id.as_str()
                                )
                            })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                ensure!(
                    contribution_manifest_ids
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .len()
                        == contribution_manifest_ids.len(),
                    "DiLoCo contribution manifest identifiers must be unique"
                );

                let reduced =
                    aggregate_peer_contributions(&policy.aggregation_policy, &contributions)?;
                let encoded_aggregate = encode_pseudo_gradient(
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    reducer_peer_id.clone(),
                    completed_round.clone(),
                    policy.codec.clone(),
                    &reduced,
                    DILOCO_CHUNK_SIZE_BYTES,
                )
                .context("encode reduced DiLoCo aggregate")?;
                // Every participant, including the reducer, applies the exact
                // transport-decoded aggregate. This preserves parity for lossy
                // codecs instead of giving the reducer a higher precision path.
                let aggregate =
                    decode_pseudo_gradient(&encoded_aggregate.manifest, &encoded_aggregate.chunks)
                        .context("decode local reduced DiLoCo aggregate")?;
                self.control
                    .publish_diloco_aggregate(
                        encoded_aggregate.manifest.clone(),
                        encoded_aggregate.chunks,
                        participant_peer_ids.clone(),
                        contribution_manifest_ids.clone(),
                    )
                    .context("publish reduced DiLoCo aggregate")?;
                self.broadcast_diloco_aggregate_ready(
                    experiment,
                    &participant_peer_ids,
                    &local_peer_id,
                    &completed_round,
                    &encoded_aggregate.manifest,
                    &contribution_manifest_ids,
                    &policy,
                )
                .context("release followers to fetch reduced DiLoCo aggregate")?;
                (
                    aggregate,
                    contributions,
                    encoded_aggregate.manifest,
                    contribution_manifest_ids,
                )
            } else {
                let aggregate_ready = self
                    .wait_for_local_diloco_aggregate_ready(
                        experiment,
                        &reducer_peer_id,
                        &completed_round,
                        &policy,
                    )
                    .context("wait for reducer aggregate-ready release")?;
                let (encoded_aggregate, contribution_manifest_ids, aggregate) = self
                    .collect_remote_diloco_aggregate(
                        experiment,
                        &reducer_peer_id,
                        &participant_peer_ids,
                        &local_peer_id,
                        &local_gradient_manifest,
                        &aggregate_ready,
                        &completed_round,
                        &policy,
                    )
                    .context("collect reduced DiLoCo aggregate")?;
                (
                    aggregate,
                    vec![local_contribution],
                    encoded_aggregate.manifest,
                    contribution_manifest_ids,
                )
            };
        let gradient_collection_ms = gradient_collection_started.elapsed().as_millis() as u64;
        let gradient_exchange_ms = gradient_exchange_started.elapsed().as_millis() as u64;
        state.snapshot.round_cursor.phase = RoundPhase::OuterApply;
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo outer-apply state")?;

        let outer_apply_started = Instant::now();
        let (next_parameters, next_outer_optimizer_state) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            project.apply_aggregated_outer_update(
                &state.current_parameters,
                &aggregate,
                &state.outer_optimizer_state,
                &policy.outer_optimizer_policy,
            )
        }
        .context("apply DiLoCo outer update")?;
        state.current_parameters = next_parameters;
        state.outer_optimizer_state = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            project.save_outer_optimizer_state(&next_outer_optimizer_state)
        }
        .context("persist DiLoCo outer optimizer state")?;
        let outer_apply_ms = outer_apply_started.elapsed().as_millis() as u64;

        let next_base_checkpoint_id =
            BaseCheckpointId::new(state.current_parameters.checksum()?.into_inner());
        let next_round_cursor = completed_round.advance(next_base_checkpoint_id);
        let checkpoint_publish_started = Instant::now();
        let published_checkpoint = if checkpoint_due(&policy, &completed_round) {
            Some(
                self.publish_diloco_checkpoint_head(
                    experiment,
                    &storage,
                    &store,
                    &mut state,
                    base_head.as_ref(),
                    &completed_round,
                )
                .context("publish DiLoCo checkpoint head")?,
            )
        } else {
            None
        };
        let checkpoint_publish_ms = checkpoint_publish_started.elapsed().as_millis() as u64;
        state.snapshot.round_cursor = next_round_cursor.clone();
        state.snapshot.round_cursor.phase = RoundPhase::Completed;
        state.snapshot.round_cursor.group_id = None;
        state.snapshot.updated_at = Utc::now();
        if let Some(head) = published_checkpoint.as_ref() {
            state.snapshot.checkpoint_head_id = Some(head.head_id.clone());
        }
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)?;
        self.broadcast_diloco_finalize(DiLoCoFinalizeBroadcast {
            experiment,
            participants: &participant_peer_ids,
            local_peer_id: &local_peer_id,
            round_cursor: &completed_round,
            participant_count: participant_peer_ids.len() as u16,
            aggregate_checksum: Some(aggregate.checksum()?),
        });
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);

        Ok(DiLoCoRoundOutcome {
            training_lease,
            participant_peer_ids,
            group_id,
            completed_round,
            next_round_cursor,
            aggregate,
            contributions,
            reducer_peer_id,
            aggregate_manifest,
            contribution_manifest_ids,
            local_inner_report,
            local_gradient_manifest,
            current_parameters: state.current_parameters,
            published_checkpoint,
            timing: DiLoCoRoundTiming {
                state_sync_ms,
                matchmaking_ms,
                local_inner_loop_ms,
                gradient_exchange_ms,
                gradient_publish_ms,
                gradient_collection_ms,
                outer_apply_ms,
                checkpoint_publish_ms,
                total_ms: round_started.elapsed().as_millis() as u64,
            },
        })
    }

    fn ensure_diloco_base_head(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>> {
        if let Some(storage) = self.config().storage.as_ref()
            && let Some(head) = load_head_state(storage, experiment)?
        {
            let has_local_artifact = self
                .artifact_store()
                .map(|store| store.has_complete_artifact(&head.artifact_id))
                .transpose()?
                .unwrap_or(false);
            if has_local_artifact {
                return Ok(Some(head));
            }
        }
        if let Some(head) = self.sync_experiment_head(experiment)? {
            return Ok(Some(head));
        }
        Ok(Some(self.initialize_local_head(experiment)?))
    }

    fn load_or_bootstrap_diloco_state(
        &mut self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
        base_head: Option<&HeadDescriptor>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<PersistedDiLoCoRuntimeState> {
        let storage = self
            .config()
            .storage
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("DiLoCo rounds require configured storage"))?;
        let local_path = storage.scoped_diloco_state_path(experiment);
        if let Some(mut state) = load_json::<PersistedDiLoCoRuntimeState>(local_path.clone())? {
            self.validate_loaded_diloco_state(policy, &state)?;
            if let Some(remote) = self.best_remote_diloco_snapshot(experiment, policy)? {
                let local_round = state.snapshot.round_cursor.round_id.as_u64();
                let remote_round = remote.snapshot.round_cursor.round_id.as_u64();
                let lag = remote_round.saturating_sub(local_round);
                if lag > 0
                    && (lag <= u64::from(policy.rejoin_policy.max_fast_forward_round_lag)
                        || remote.snapshot.checkpoint_head_id.is_some())
                    && let Some(remote) =
                        self.fetch_remote_diloco_state(experiment, policy, remote)?
                {
                    state = PersistedDiLoCoRuntimeState {
                        snapshot: remote.snapshot,
                        current_parameters: remote.current_parameters,
                        outer_optimizer_state: remote.outer_optimizer_state,
                        inner_optimizer_state: None,
                    };
                    persist_json(local_path, &state)?;
                }
            }
            return Ok(state);
        }

        let mut state = self.bootstrap_local_diloco_state(experiment, policy, base_head, store)?;
        state.snapshot.updated_at = Utc::now();
        // Make a canonical local base available before probing the cohort. This
        // prevents a fresh cohort from waiting on mutually unavailable state.
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)?;
        if let Some(remote) = self.best_remote_diloco_snapshot(experiment, policy)? {
            let local_round = state.snapshot.round_cursor.round_id.as_u64();
            let remote_round = remote.snapshot.round_cursor.round_id.as_u64();
            let lag = remote_round.saturating_sub(local_round);
            if lag > 0
                && (lag <= u64::from(policy.rejoin_policy.max_fast_forward_round_lag)
                    || remote.snapshot.checkpoint_head_id.is_some())
                && let Some(remote) = self.fetch_remote_diloco_state(experiment, policy, remote)?
            {
                state = PersistedDiLoCoRuntimeState {
                    snapshot: remote.snapshot,
                    current_parameters: remote.current_parameters,
                    outer_optimizer_state: remote.outer_optimizer_state,
                    inner_optimizer_state: None,
                };
                persist_json(local_path, &state)?;
            }
        }
        Ok(state)
    }

    fn validate_loaded_diloco_state(
        &self,
        policy: &DiLoCoPolicy,
        state: &PersistedDiLoCoRuntimeState,
    ) -> anyhow::Result<()> {
        ensure!(
            state.snapshot.training_protocol == TrainingProtocol::DiLoCo(policy.clone()),
            "persisted DiLoCo state does not match the active training protocol"
        );
        ensure!(
            state.snapshot.current_parameter_checksum.as_ref()
                == Some(&state.current_parameters.checksum()?),
            "persisted DiLoCo parameters do not match the stored checksum"
        );
        Ok(())
    }

    fn bootstrap_local_diloco_state(
        &mut self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
        base_head: Option<&HeadDescriptor>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<PersistedDiLoCoRuntimeState> {
        let revision_contract = self
            .node
            .as_ref()
            .and_then(|node| node.revision_contracts.get(&experiment.revision_id))
            .cloned();
        let (model, checkpoint_head_id) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let model = if let Some(head) = base_head {
                load_model_for_head(project, head, revision_contract.as_ref(), store, &device)?
            } else {
                project.init_model(&device)
            };
            (model, base_head.map(|head| head.head_id.clone()))
        };
        let (current_parameters, outer_optimizer_state) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let current_parameters = project.export_parameter_pack(&model)?;
            let outer_optimizer_state =
                project.initialize_outer_optimizer_state(&model, &policy.outer_optimizer_policy)?;
            (
                current_parameters,
                project.save_outer_optimizer_state(&outer_optimizer_state)?,
            )
        };
        let base_checkpoint_id = checkpoint_head_id
            .clone()
            .map(BaseCheckpointId::from)
            .unwrap_or_else(|| {
                BaseCheckpointId::new(
                    current_parameters
                        .checksum()
                        .expect("checksum")
                        .into_inner(),
                )
            });
        let mut round_cursor = RoundCursor::new(base_checkpoint_id, policy.num_inner_steps);
        if let Some(head) = base_head {
            round_cursor.round_id = RoundId::new(head.global_step);
        }
        Ok(PersistedDiLoCoRuntimeState {
            snapshot: DiLoCoStateSnapshot {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
                round_cursor,
                checkpoint_head_id,
                latest_gradient_manifest_id: None,
                current_parameter_checksum: Some(current_parameters.checksum()?),
                outer_optimizer_state: Some(outer_optimizer_state.clone()),
                signature_bundle: Vec::new(),
                updated_at: Utc::now(),
            },
            current_parameters,
            outer_optimizer_state,
            inner_optimizer_state: None,
        })
    }

    fn best_remote_diloco_snapshot(
        &self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<Option<RemoteDiLoCoSnapshot>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let local_peer_id = telemetry_snapshot.local_peer_id.as_ref();
        let mut remote_states = Vec::new();
        let peer_ids = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .filter(|peer_id| local_peer_id.is_none_or(|local| local != peer_id))
            .collect::<Vec<_>>();
        let responses = self.control.fetch_diloco_state_snapshots_concurrently(
            &peer_ids,
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            DILOCO_STATE_SNAPSHOT_TIMEOUT,
        );
        let peer_count = responses.len();
        let mut completed_probes = 0_usize;
        let mut probe_errors = Vec::new();
        for (peer_id, response) in responses {
            let snapshot = match response {
                Ok(snapshot) => {
                    completed_probes += 1;
                    let Some(snapshot) = snapshot else {
                        continue;
                    };
                    snapshot
                }
                Err(error) => {
                    probe_errors.push(format!("{}: {error:#}", peer_id.as_str()));
                    continue;
                }
            };
            if !diloco_state_signature_acceptable(policy, &telemetry_snapshot, &peer_id, &snapshot)
            {
                continue;
            }
            if snapshot.training_protocol != TrainingProtocol::DiLoCo(policy.clone()) {
                continue;
            }
            remote_states.push(RemoteDiLoCoSnapshot { peer_id, snapshot });
        }
        ensure!(
            peer_count == 0 || completed_probes > 0,
            "all {peer_count} connected DiLoCo state probes failed: {probe_errors:?}"
        );
        remote_states.sort_by(|left, right| {
            right
                .snapshot
                .round_cursor
                .round_id
                .cmp(&left.snapshot.round_cursor.round_id)
                .then_with(|| right.snapshot.updated_at.cmp(&left.snapshot.updated_at))
                .then_with(|| left.peer_id.as_str().cmp(right.peer_id.as_str()))
        });
        Ok(remote_states.into_iter().next())
    }

    fn fetch_remote_diloco_state(
        &self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
        remote: RemoteDiLoCoSnapshot,
    ) -> anyhow::Result<Option<RemoteDiLoCoState>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut candidate_peer_ids = vec![remote.peer_id.clone()];
        for peer_id in connected_peer_ids(&telemetry_snapshot) {
            if !candidate_peer_ids.contains(&peer_id) {
                candidate_peer_ids.push(peer_id);
            }
        }
        let mut failures = Vec::new();
        for peer_id in candidate_peer_ids {
            let bundle = match self.control.fetch_diloco_state_bundle(
                peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                DILOCO_STATE_TRANSFER_ATTEMPT_TIMEOUT,
            ) {
                Ok(Some(bundle)) => bundle,
                Ok(None) => {
                    failures.push(format!("{}: state bundle unavailable", peer_id.as_str()));
                    continue;
                }
                Err(error) => {
                    failures.push(format!("{}: {error:#}", peer_id.as_str()));
                    continue;
                }
            };
            let signature_acceptable = diloco_state_signature_acceptable(
                policy,
                &telemetry_snapshot,
                &peer_id,
                &bundle.snapshot,
            );
            let bundle_round = bundle.snapshot.round_cursor.round_id;
            let remote_round = remote.snapshot.round_cursor.round_id;
            let cursor_is_current = bundle_round > remote_round
                || (bundle_round == remote_round
                    && bundle.snapshot.round_cursor.base_checkpoint_id
                        == remote.snapshot.round_cursor.base_checkpoint_id);
            let snapshot_is_compatible = bundle.snapshot.training_protocol
                == TrainingProtocol::DiLoCo(policy.clone())
                && cursor_is_current
                && signature_acceptable;
            let checksum_matches = bundle.snapshot.current_parameter_checksum.as_ref()
                == Some(&bundle.current_parameters.checksum()?);
            if !snapshot_is_compatible || !checksum_matches {
                failures.push(format!(
                    "{}: incompatible state bundle (round={}, base={}, signature_acceptable={}, checksum_matches={checksum_matches})",
                    peer_id.as_str(),
                    bundle.snapshot.round_cursor.round_id,
                    bundle.snapshot.round_cursor.base_checkpoint_id,
                    signature_acceptable,
                ));
                continue;
            }
            return Ok(Some(RemoteDiLoCoState {
                snapshot: bundle.snapshot,
                current_parameters: bundle.current_parameters,
                outer_optimizer_state: bundle.outer_optimizer_state,
            }));
        }
        anyhow::bail!("no coherent remote DiLoCo state bundle available: {failures:?}")
    }

    fn persist_and_publish_diloco_state(
        &self,
        storage: &StorageConfig,
        experiment: &ExperimentHandle,
        state: &mut PersistedDiLoCoRuntimeState,
    ) -> anyhow::Result<()> {
        let publishable_snapshot = state.publishable_snapshot()?;
        state.snapshot = publishable_snapshot.clone();
        persist_json(storage.scoped_diloco_state_path(experiment), state)?;
        self.control.publish_diloco_state(
            publishable_snapshot,
            Some(state.outer_optimizer_state.clone()),
            Some(state.current_parameters.clone()),
        )?;
        Ok(())
    }

    fn matchmake_diloco_participants(
        &self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
        local_peer_id: &PeerId,
        round_cursor: &RoundCursor,
        training_lease: Option<&AssignmentLease>,
    ) -> anyhow::Result<Vec<PeerId>> {
        if let Some(training_lease) = training_lease {
            return self.matchmake_assigned_diloco_participants(
                policy,
                local_peer_id,
                training_lease,
            );
        }

        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.matchmaking_timeout_ms.max(1)));
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut candidates = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .filter(|peer_id| peer_id != local_peer_id)
            .collect::<Vec<_>>();
        order_diloco_candidates(
            policy,
            &telemetry_snapshot.control_plane,
            local_peer_id,
            round_cursor,
            &mut candidates,
        )?;
        let fanout = usize::from(policy.topology_policy.fanout)
            .max(usize::from(policy.target_group_size))
            .max(1);
        candidates.truncate(fanout);
        let mut compatible = BTreeSet::from([local_peer_id.clone()]);
        let mut incompatibility_reasons = BTreeMap::<PeerId, String>::new();

        while Instant::now() < deadline && compatible.len() < policy.target_group_size as usize {
            let mut progressed = false;
            for peer_id in &candidates {
                if compatible.contains(peer_id) {
                    continue;
                }
                let Some(timeout) = request_timeout(deadline) else {
                    break;
                };
                let snapshot = match self.control.fetch_diloco_state_snapshot(
                    peer_id.as_str(),
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    timeout,
                ) {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => {
                        incompatibility_reasons
                            .insert(peer_id.clone(), "state snapshot unavailable".into());
                        continue;
                    }
                    Err(error) => {
                        incompatibility_reasons.insert(
                            peer_id.clone(),
                            format!("state snapshot request failed: {error}"),
                        );
                        continue;
                    }
                };
                if let Some(reason) = diloco_matchmaking_compatibility_error(
                    policy,
                    &telemetry_snapshot,
                    peer_id,
                    &snapshot,
                    experiment,
                    round_cursor,
                ) {
                    incompatibility_reasons.insert(peer_id.clone(), reason);
                } else {
                    compatible.insert(peer_id.clone());
                    incompatibility_reasons.remove(peer_id);
                    progressed = true;
                }
            }
            if compatible.len() >= policy.target_group_size as usize {
                break;
            }
            if !progressed {
                std::thread::sleep(DILOCO_POLL_INTERVAL);
            }
        }

        let mut participants = compatible.into_iter().collect::<Vec<_>>();
        participants.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        participants.truncate(policy.target_group_size as usize);
        ensure!(
            participants.len() >= policy.minimum_group_size as usize,
            "need at least {} DiLoCo participant(s), found {}; candidates={:?}; incompatibilities={:?}",
            policy.minimum_group_size,
            participants.len(),
            candidates.iter().map(PeerId::as_str).collect::<Vec<_>>(),
            incompatibility_reasons
                .iter()
                .map(|(peer_id, reason)| (peer_id.as_str(), reason.as_str()))
                .collect::<Vec<_>>(),
        );

        for peer_id in &participants {
            if peer_id == local_peer_id {
                continue;
            }
            let Some(timeout) = request_timeout(deadline) else {
                break;
            };
            let offer = DiLoCoRoundOffer {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                peer_id: local_peer_id.clone(),
                round_cursor: round_cursor.clone(),
                target_group_size: policy.target_group_size,
                issued_at: Utc::now(),
            };
            let _ = self
                .control
                .send_diloco_round_offer(peer_id.as_str(), offer, timeout);
        }
        Ok(participants)
    }

    fn matchmake_assigned_diloco_participants(
        &self,
        policy: &DiLoCoPolicy,
        local_peer_id: &PeerId,
        training_lease: &AssignmentLease,
    ) -> anyhow::Result<Vec<PeerId>> {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.matchmaking_timeout_ms.max(1)));
        let mut assigned_peer_ids = BTreeSet::new();
        let mut ready_partial_cohort = None;
        let mut last_observations = BTreeMap::<PeerId, String>::new();

        while Instant::now() < deadline {
            let telemetry_snapshot = self.telemetry().snapshot();
            assigned_peer_ids = telemetry_snapshot
                .control_plane
                .lease_announcements
                .iter()
                .filter(|announcement| {
                    let lease = &announcement.lease;
                    lease.network_id == training_lease.network_id
                        && lease.study_id == training_lease.study_id
                        && lease.experiment_id == training_lease.experiment_id
                        && lease.revision_id == training_lease.revision_id
                        && lease.dataset_view_id == training_lease.dataset_view_id
                        && lease.window_id == training_lease.window_id
                })
                .map(|announcement| announcement.lease.peer_id.clone())
                .chain(std::iter::once(training_lease.peer_id.clone()))
                .collect();
            let Some(participants) = assigned_diloco_cohort(
                &assigned_peer_ids,
                local_peer_id,
                usize::from(policy.target_group_size),
            ) else {
                anyhow::bail!(
                    "local peer {} has no assignment lease in window {}",
                    local_peer_id.as_str(),
                    training_lease.window_id.0
                );
            };
            if participants.len() < usize::from(policy.minimum_group_size) {
                std::thread::sleep(DILOCO_POLL_INTERVAL);
                continue;
            }

            let connected = connected_peer_ids(&telemetry_snapshot);
            last_observations.clear();
            for peer_id in participants
                .iter()
                .filter(|peer_id| *peer_id != local_peer_id)
            {
                if !connected.contains(peer_id) {
                    last_observations.insert(peer_id.clone(), "not connected".into());
                }
            }
            if last_observations.is_empty() {
                if participants.len() >= usize::from(policy.target_group_size) {
                    return Ok(participants);
                }
                ready_partial_cohort = Some(participants);
            } else {
                ready_partial_cohort = None;
            }
            std::thread::sleep(DILOCO_POLL_INTERVAL);
        }

        let participants = ready_partial_cohort.ok_or_else(|| {
            anyhow!(
                "assigned DiLoCo cohort for window {} did not become ready; assigned={:?}; observations={:?}",
                training_lease.window_id.0,
                assigned_peer_ids
                    .iter()
                    .map(PeerId::as_str)
                    .collect::<Vec<_>>(),
                last_observations
                    .iter()
                    .map(|(peer_id, reason)| (peer_id.as_str(), reason.as_str()))
                    .collect::<Vec<_>>(),
            )
        })?;
        ensure!(
            participants.len() >= usize::from(policy.minimum_group_size),
            "assigned DiLoCo cohort for window {} has {} participant(s), below minimum {}; assigned={:?}",
            training_lease.window_id.0,
            participants.len(),
            policy.minimum_group_size,
            assigned_peer_ids
                .iter()
                .map(PeerId::as_str)
                .collect::<Vec<_>>(),
        );
        Ok(participants)
    }

    fn wait_for_diloco_group_barrier(
        &self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
        local_peer_id: &PeerId,
        participants: &[PeerId],
        barrier: DiLoCoGroupBarrier<'_>,
    ) -> anyhow::Result<()> {
        const BARRIER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.matchmaking_timeout_ms.max(1)));
        let mut pending = participants
            .iter()
            .filter(|peer_id| *peer_id != local_peer_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut last_observations = BTreeMap::<PeerId, String>::new();

        while !pending.is_empty() && Instant::now() < deadline {
            let telemetry = self.telemetry().snapshot();
            let connected = connected_peer_ids(&telemetry);
            let probe_peers = pending
                .iter()
                .filter(|peer_id| {
                    if connected.contains(*peer_id) {
                        true
                    } else {
                        last_observations.insert((*peer_id).clone(), "not connected".into());
                        false
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            let Some(probe_timeout) =
                request_timeout(deadline).map(|timeout| timeout.min(BARRIER_PROBE_TIMEOUT))
            else {
                break;
            };
            let responses = self.control.fetch_diloco_state_snapshots_concurrently(
                &probe_peers,
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                probe_timeout,
            );
            let mut progressed = false;
            for (peer_id, response) in responses {
                match response {
                    Ok(Some(state)) => {
                        if let Some(reason) = diloco_group_barrier_error(
                            policy, &telemetry, &peer_id, &state, experiment, barrier,
                        ) {
                            last_observations.insert(peer_id, reason);
                        } else {
                            pending.remove(&peer_id);
                            last_observations.remove(&peer_id);
                            progressed = true;
                        }
                    }
                    Ok(None) => {
                        last_observations.insert(peer_id, "state unavailable".into());
                    }
                    Err(error) => {
                        last_observations.insert(peer_id, format!("state probe failed: {error:#}"));
                    }
                }
            }
            if !pending.is_empty() && !progressed {
                std::thread::sleep(DILOCO_POLL_INTERVAL);
            }
        }

        ensure!(
            pending.is_empty(),
            "DiLoCo {:?} barrier for group {} timed out; pending={:?}; observations={:?}",
            barrier.stage,
            barrier.group_id.as_str(),
            pending.iter().map(PeerId::as_str).collect::<Vec<_>>(),
            last_observations
                .iter()
                .map(|(peer_id, reason)| (peer_id.as_str(), reason.as_str()))
                .collect::<Vec<_>>(),
        );
        Ok(())
    }

    fn broadcast_diloco_finalize(&self, broadcast: DiLoCoFinalizeBroadcast<'_>) {
        let deadline = Instant::now() + DILOCO_FINALIZE_BEST_EFFORT_BUDGET;
        for peer_id in broadcast.participants {
            if peer_id == broadcast.local_peer_id {
                continue;
            }
            let Some(timeout) = request_timeout(deadline)
                .map(|timeout| timeout.min(DILOCO_FINALIZE_ATTEMPT_TIMEOUT))
            else {
                break;
            };
            let finalize = DiLoCoRoundFinalize {
                experiment_id: broadcast.experiment.experiment_id.clone(),
                revision_id: broadcast.experiment.revision_id.clone(),
                peer_id: broadcast.local_peer_id.clone(),
                round_cursor: broadcast.round_cursor.clone(),
                participant_count: broadcast.participant_count,
                aggregate_checksum: broadcast.aggregate_checksum.clone(),
                finalized_at: Utc::now(),
            };
            let _ = self
                .control
                .send_diloco_round_finalize(peer_id.as_str(), finalize, timeout);
        }
    }

    fn collect_remote_diloco_contributions(
        &self,
        experiment: &ExperimentHandle,
        participants: &[PeerId],
        local_peer_id: &PeerId,
        local_manifest: &PseudoGradientManifest,
        round_cursor: &RoundCursor,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<(Vec<DiLoCoPeerContribution>, BTreeMap<PeerId, String>)> {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.aggregation_timeout_ms.max(1)));
        let mut pending = participants
            .iter()
            .filter(|peer_id| *peer_id != local_peer_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut contributions = BTreeMap::<PeerId, DiLoCoPeerContribution>::new();
        let mut last_observations = BTreeMap::<PeerId, String>::new();
        let mut retry_backoff = DILOCO_POLL_INTERVAL;

        while !pending.is_empty() {
            let mut progressed = false;
            // Bulk response fan-in is deliberately serialized. Concurrent
            // model-sized responses can exhaust mux receive windows even when
            // each individual stream is healthy; control messages remain
            // concurrent elsewhere in the protocol.
            for peer_id in pending.iter().cloned().collect::<Vec<_>>() {
                let peer_collection_started = Instant::now();
                let Some(timeout) = request_timeout(deadline) else {
                    break;
                };
                let first_slice_started = Instant::now();
                let first_slice_result = self.control.fetch_diloco_gradient_slice(
                    peer_id.as_str(),
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    round_cursor.clone(),
                    0,
                    timeout,
                );
                diloco_trace(format_args!(
                    "gradient-slice peer={} round={} chunk=0 elapsed_ms={} result={}",
                    peer_id.as_str(),
                    round_cursor.round_id,
                    first_slice_started.elapsed().as_millis(),
                    if first_slice_result.is_ok() {
                        "ok"
                    } else {
                        "error"
                    }
                ));
                let first_slice = match first_slice_result {
                    Ok(Some(slice)) => slice,
                    Ok(None) => {
                        last_observations
                            .insert(peer_id.clone(), "gradient slice unavailable".into());
                        continue;
                    }
                    Err(error) => {
                        last_observations
                            .insert(peer_id.clone(), format!("slice request error: {error}"));
                        continue;
                    }
                };
                let telemetry_snapshot = self.telemetry().snapshot();
                let manifest = first_slice.manifest;
                if !diloco_manifest_signature_acceptable(
                    policy,
                    &telemetry_snapshot,
                    &peer_id,
                    &manifest,
                ) {
                    last_observations.insert(peer_id.clone(), "gradient signature rejected".into());
                    continue;
                }
                let invalid_reason = if manifest.peer_id != peer_id {
                    Some("manifest peer mismatch")
                } else if manifest.experiment_id != experiment.experiment_id {
                    Some("manifest experiment mismatch")
                } else if manifest.revision_id != experiment.revision_id {
                    Some("manifest revision mismatch")
                } else if manifest.round_cursor != *round_cursor {
                    Some("manifest round cursor mismatch")
                } else if manifest.codec != policy.codec {
                    Some("manifest codec mismatch")
                } else if manifest.model_schema_hash != local_manifest.model_schema_hash {
                    Some("manifest model schema mismatch")
                } else if manifest.layout_hash != local_manifest.layout_hash {
                    Some("manifest tensor layout mismatch")
                } else if manifest.parameter_count != local_manifest.parameter_count {
                    Some("manifest parameter count mismatch")
                } else if manifest.chunk_count == 0 {
                    Some("manifest has no chunks")
                } else if manifest.chunk_count > DILOCO_MAX_GRADIENT_CHUNKS {
                    Some("manifest chunk count exceeds safety cap")
                } else if first_slice.chunk.manifest_id != manifest.manifest_id {
                    Some("first chunk manifest mismatch")
                } else if first_slice.chunk.chunk_index != 0 {
                    Some("first chunk index mismatch")
                } else {
                    None
                };
                if let Some(reason) = invalid_reason {
                    last_observations.insert(peer_id.clone(), reason.into());
                    continue;
                }
                let mut chunks_by_index = BTreeMap::from([(0_u32, first_slice.chunk)]);
                let mut pending_chunk_indices = (1..manifest.chunk_count).collect::<BTreeSet<_>>();
                let mut chunk_retry_backoff = DILOCO_POLL_INTERVAL;
                while !pending_chunk_indices.is_empty() {
                    let Some(timeout) = request_timeout(deadline)
                        .map(|timeout| timeout.min(DILOCO_CHUNK_WINDOW_TIMEOUT))
                    else {
                        break;
                    };
                    let window_indices = pending_chunk_indices
                        .iter()
                        .copied()
                        .take(DILOCO_CHUNK_FETCH_WINDOW)
                        .collect::<Vec<_>>();
                    let requests = window_indices
                        .iter()
                        .copied()
                        .map(|chunk_index| {
                            (
                                peer_id.clone(),
                                crate::DiLoCoRequest::GradientSlice {
                                    experiment_id: experiment.experiment_id.clone(),
                                    revision_id: experiment.revision_id.clone(),
                                    round_cursor: round_cursor.clone(),
                                    chunk_index,
                                },
                            )
                        })
                        .collect::<Vec<_>>();
                    let window_started = Instant::now();
                    let mut window_progress = false;
                    let mut window_error = None;
                    for (_, response) in self.control.fetch_diloco_concurrently(requests, timeout) {
                        match response {
                            Ok(crate::DiLoCoResponse::GradientSlice(Some(slice)))
                                if slice.manifest == manifest
                                    && slice.chunk.manifest_id == manifest.manifest_id
                                    && pending_chunk_indices.contains(&slice.chunk.chunk_index) =>
                            {
                                let chunk_index = slice.chunk.chunk_index;
                                chunks_by_index.insert(chunk_index, slice.chunk);
                                pending_chunk_indices.remove(&chunk_index);
                                window_progress = true;
                            }
                            Ok(response) => {
                                window_error = Some(format!(
                                    "unexpected {} response",
                                    diloco_response_kind(&response)
                                ));
                            }
                            Err(error) => {
                                window_error = Some(error.to_string());
                            }
                        }
                    }
                    diloco_trace(format_args!(
                        "gradient-window peer={} round={} requested={:?} remaining={} elapsed_ms={} progressed={}",
                        peer_id.as_str(),
                        round_cursor.round_id,
                        window_indices,
                        pending_chunk_indices.len(),
                        window_started.elapsed().as_millis(),
                        window_progress
                    ));
                    if let Some(error) = window_error {
                        last_observations.insert(
                            peer_id.clone(),
                            format!("gradient window {:?} incomplete: {error}", window_indices),
                        );
                    }
                    if window_progress {
                        chunk_retry_backoff = DILOCO_POLL_INTERVAL;
                    } else if Instant::now() < deadline {
                        std::thread::sleep(chunk_retry_backoff);
                        chunk_retry_backoff = chunk_retry_backoff
                            .saturating_mul(2)
                            .min(DILOCO_GRADIENT_MAX_BACKOFF);
                    }
                }
                if !pending_chunk_indices.is_empty() {
                    continue;
                }
                let chunks = (0..manifest.chunk_count)
                    .map(|chunk_index| {
                        chunks_by_index
                            .remove(&chunk_index)
                            .ok_or_else(|| anyhow!("missing accepted gradient chunk {chunk_index}"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let chunk_count = manifest.chunk_count;
                let encoded = EncodedPseudoGradient { manifest, chunks };
                contributions.insert(
                    peer_id.clone(),
                    transport_decoded_contribution(peer_id.clone(), encoded)
                        .context("decode remote DiLoCo pseudo-gradient transport representation")?,
                );
                last_observations.insert(peer_id.clone(), "gradient accepted".into());
                pending.remove(&peer_id);
                progressed = true;
                diloco_trace(format_args!(
                    "gradient-complete peer={} round={} chunks={} elapsed_ms={}",
                    peer_id.as_str(),
                    round_cursor.round_id,
                    chunk_count,
                    peer_collection_started.elapsed().as_millis()
                ));
            }
            if pending.is_empty() || Instant::now() >= deadline {
                break;
            }
            if progressed {
                retry_backoff = DILOCO_POLL_INTERVAL;
            } else {
                std::thread::sleep(retry_backoff);
                retry_backoff = retry_backoff
                    .saturating_mul(2)
                    .min(DILOCO_GRADIENT_MAX_BACKOFF);
            }
        }

        Ok((contributions.into_values().collect(), last_observations))
    }

    #[allow(clippy::too_many_arguments)]
    fn broadcast_diloco_aggregate_ready(
        &self,
        experiment: &ExperimentHandle,
        participants: &[PeerId],
        reducer_peer_id: &PeerId,
        round_cursor: &RoundCursor,
        aggregate_manifest: &PseudoGradientManifest,
        contribution_manifest_ids: &[ContentId],
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<()> {
        let ready = DiLoCoAggregateReady {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            reducer_peer_id: reducer_peer_id.clone(),
            round_cursor: round_cursor.clone(),
            aggregate_manifest_id: aggregate_manifest.manifest_id.clone(),
            participant_peer_ids: participants.to_vec(),
            contribution_manifest_ids: contribution_manifest_ids.to_vec(),
            emitted_at: Utc::now(),
        };
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.aggregation_timeout_ms.max(1)));
        let requests = participants
            .iter()
            .filter(|peer_id| *peer_id != reducer_peer_id)
            .cloned()
            .map(|peer_id| {
                (
                    peer_id,
                    crate::DiLoCoRequest::AggregateReady(Box::new(ready.clone())),
                )
            })
            .collect::<Vec<_>>();
        let Some(timeout) = request_timeout(deadline) else {
            anyhow::bail!("DiLoCo aggregate-ready deadline expired before broadcast");
        };
        let mut failures = Vec::new();
        for (peer_id, response) in self.control.fetch_diloco_concurrently(requests, timeout) {
            match response {
                Ok(crate::DiLoCoResponse::Ack {
                    accepted: true,
                    cursor: Some(cursor),
                    ..
                }) if cursor == *round_cursor => {}
                Ok(response) => failures.push(format!(
                    "{}: unexpected {} response",
                    peer_id.as_str(),
                    diloco_response_kind(&response)
                )),
                Err(error) => failures.push(format!("{}: {error}", peer_id.as_str())),
            }
        }
        ensure!(
            failures.is_empty(),
            "failed to release DiLoCo aggregate to every follower: {failures:?}"
        );
        Ok(())
    }

    fn wait_for_local_diloco_aggregate_ready(
        &self,
        experiment: &ExperimentHandle,
        reducer_peer_id: &PeerId,
        round_cursor: &RoundCursor,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<DiLoCoAggregateReady> {
        self.control.wait_for_diloco_aggregate_ready(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            reducer_peer_id.clone(),
            round_cursor.clone(),
            Duration::from_millis(u64::from(policy.aggregation_timeout_ms.max(1))),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_remote_diloco_aggregate(
        &self,
        experiment: &ExperimentHandle,
        reducer_peer_id: &PeerId,
        participants: &[PeerId],
        local_peer_id: &PeerId,
        local_manifest: &PseudoGradientManifest,
        aggregate_ready: &DiLoCoAggregateReady,
        round_cursor: &RoundCursor,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<(EncodedPseudoGradient, Vec<ContentId>, FlattenedTensorPack)> {
        ensure!(
            aggregate_ready.experiment_id == experiment.experiment_id
                && aggregate_ready.revision_id == experiment.revision_id
                && aggregate_ready.reducer_peer_id == *reducer_peer_id
                && aggregate_ready.round_cursor == *round_cursor,
            "aggregate-ready release scope does not match the active DiLoCo round"
        );
        ensure!(
            aggregate_ready.participant_peer_ids == participants,
            "aggregate-ready participant cohort mismatch"
        );
        ensure!(
            aggregate_ready.contribution_manifest_ids.len() == participants.len(),
            "aggregate-ready contribution commitment count mismatch"
        );
        let local_participant_index = participants
            .iter()
            .position(|peer_id| peer_id == local_peer_id)
            .ok_or_else(|| anyhow!("local peer is absent from aggregate-ready cohort"))?;
        ensure!(
            aggregate_ready
                .contribution_manifest_ids
                .get(local_participant_index)
                == Some(&local_manifest.manifest_id),
            "aggregate-ready release does not commit to the local gradient manifest"
        );
        let timeout_ms = u64::from(policy.aggregation_timeout_ms.max(1)).saturating_mul(2);
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let mut retry_backoff = DILOCO_POLL_INTERVAL;
        let mut last_observation = "aggregate not requested".to_string();

        while let Some(timeout) = request_timeout(deadline) {
            let first_slice = match self.control.fetch_diloco_aggregate_slice(
                reducer_peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                round_cursor.clone(),
                0,
                timeout,
            ) {
                Ok(Some(slice)) => slice,
                Ok(None) => {
                    last_observation = "aggregate slice unavailable".into();
                    if Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(retry_backoff);
                    retry_backoff = retry_backoff
                        .saturating_mul(2)
                        .min(DILOCO_GRADIENT_MAX_BACKOFF);
                    continue;
                }
                Err(error) => {
                    last_observation = format!("aggregate slice request error: {error}");
                    if Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(retry_backoff);
                    retry_backoff = retry_backoff
                        .saturating_mul(2)
                        .min(DILOCO_GRADIENT_MAX_BACKOFF);
                    continue;
                }
            };

            let telemetry_snapshot = self.telemetry().snapshot();
            let manifest = first_slice.manifest;
            let participant_peer_ids = first_slice.participant_peer_ids;
            let contribution_manifest_ids = first_slice.contribution_manifest_ids;
            let signature_acceptable = diloco_manifest_signature_acceptable(
                policy,
                &telemetry_snapshot,
                reducer_peer_id,
                &manifest,
            );
            let invalid_reason = if !signature_acceptable {
                Some("aggregate signature rejected")
            } else if manifest.manifest_id != aggregate_ready.aggregate_manifest_id {
                Some("aggregate manifest does not match ready release")
            } else if manifest.peer_id != *reducer_peer_id {
                Some("aggregate manifest reducer mismatch")
            } else if manifest.experiment_id != experiment.experiment_id {
                Some("aggregate manifest experiment mismatch")
            } else if manifest.revision_id != experiment.revision_id {
                Some("aggregate manifest revision mismatch")
            } else if manifest.round_cursor != *round_cursor {
                Some("aggregate manifest round cursor mismatch")
            } else if manifest.codec != policy.codec {
                Some("aggregate manifest codec mismatch")
            } else if manifest.model_schema_hash != local_manifest.model_schema_hash {
                Some("aggregate manifest model schema mismatch")
            } else if manifest.layout_hash != local_manifest.layout_hash {
                Some("aggregate manifest tensor layout mismatch")
            } else if manifest.parameter_count != local_manifest.parameter_count {
                Some("aggregate manifest parameter count mismatch")
            } else if manifest.chunk_count == 0 {
                Some("aggregate manifest has no chunks")
            } else if manifest.chunk_count > DILOCO_MAX_GRADIENT_CHUNKS {
                Some("aggregate manifest chunk count exceeds safety cap")
            } else if first_slice.chunk.manifest_id != manifest.manifest_id {
                Some("aggregate first chunk manifest mismatch")
            } else if first_slice.chunk.chunk_index != 0 {
                Some("aggregate first chunk index mismatch")
            } else if participant_peer_ids != participants {
                Some("aggregate participant cohort mismatch")
            } else if participant_peer_ids != aggregate_ready.participant_peer_ids {
                Some("aggregate participant cohort differs from ready release")
            } else if contribution_manifest_ids.len() != participants.len() {
                Some("aggregate contribution commitment count mismatch")
            } else if contribution_manifest_ids != aggregate_ready.contribution_manifest_ids {
                Some("aggregate commitments differ from ready release")
            } else if contribution_manifest_ids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != contribution_manifest_ids.len()
            {
                Some("aggregate contribution commitments are not unique")
            } else if contribution_manifest_ids.get(local_participant_index)
                != Some(&local_manifest.manifest_id)
            {
                Some("aggregate does not commit to the local gradient manifest")
            } else {
                None
            };
            if let Some(reason) = invalid_reason {
                last_observation = reason.into();
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(retry_backoff);
                retry_backoff = retry_backoff
                    .saturating_mul(2)
                    .min(DILOCO_GRADIENT_MAX_BACKOFF);
                continue;
            }

            let mut chunks_by_index = BTreeMap::from([(0_u32, first_slice.chunk)]);
            let mut pending_chunk_indices = (1..manifest.chunk_count).collect::<BTreeSet<_>>();
            let mut chunk_retry_backoff = DILOCO_POLL_INTERVAL;
            while !pending_chunk_indices.is_empty() {
                let Some(timeout) = request_timeout(deadline)
                    .map(|timeout| timeout.min(DILOCO_CHUNK_WINDOW_TIMEOUT))
                else {
                    break;
                };
                let window_indices = pending_chunk_indices
                    .iter()
                    .copied()
                    .take(DILOCO_CHUNK_FETCH_WINDOW)
                    .collect::<Vec<_>>();
                let requests = window_indices
                    .iter()
                    .copied()
                    .map(|chunk_index| {
                        (
                            reducer_peer_id.clone(),
                            crate::DiLoCoRequest::AggregateSlice {
                                experiment_id: experiment.experiment_id.clone(),
                                revision_id: experiment.revision_id.clone(),
                                round_cursor: round_cursor.clone(),
                                chunk_index,
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                let window_started = Instant::now();
                let mut window_progress = false;
                let mut window_error = None;
                for (_, response) in self.control.fetch_diloco_concurrently(requests, timeout) {
                    match response {
                        Ok(crate::DiLoCoResponse::AggregateSlice(Some(slice)))
                            if slice.manifest == manifest
                                && slice.participant_peer_ids == participants
                                && slice.contribution_manifest_ids == contribution_manifest_ids
                                && slice.chunk.manifest_id == manifest.manifest_id
                                && pending_chunk_indices.contains(&slice.chunk.chunk_index) =>
                        {
                            let chunk_index = slice.chunk.chunk_index;
                            chunks_by_index.insert(chunk_index, slice.chunk);
                            pending_chunk_indices.remove(&chunk_index);
                            window_progress = true;
                        }
                        Ok(response) => {
                            window_error = Some(format!(
                                "unexpected {} response",
                                diloco_response_kind(&response)
                            ));
                        }
                        Err(error) => {
                            window_error = Some(error.to_string());
                        }
                    }
                }
                diloco_trace(format_args!(
                    "aggregate-window reducer={} round={} requested={:?} remaining={} elapsed_ms={} progressed={}",
                    reducer_peer_id.as_str(),
                    round_cursor.round_id,
                    window_indices,
                    pending_chunk_indices.len(),
                    window_started.elapsed().as_millis(),
                    window_progress
                ));
                if let Some(error) = window_error {
                    last_observation =
                        format!("aggregate window {:?} incomplete: {error}", window_indices);
                }
                if window_progress {
                    chunk_retry_backoff = DILOCO_POLL_INTERVAL;
                } else if Instant::now() < deadline {
                    std::thread::sleep(chunk_retry_backoff);
                    chunk_retry_backoff = chunk_retry_backoff
                        .saturating_mul(2)
                        .min(DILOCO_GRADIENT_MAX_BACKOFF);
                }
            }
            if !pending_chunk_indices.is_empty() {
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(retry_backoff);
                retry_backoff = retry_backoff
                    .saturating_mul(2)
                    .min(DILOCO_GRADIENT_MAX_BACKOFF);
                continue;
            }
            let chunks = (0..manifest.chunk_count)
                .map(|chunk_index| {
                    chunks_by_index
                        .remove(&chunk_index)
                        .ok_or_else(|| anyhow!("missing accepted aggregate chunk {chunk_index}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            let decoded = decode_pseudo_gradient(&manifest, &chunks)
                .context("decode reducer-published DiLoCo aggregate")?;
            return Ok((
                EncodedPseudoGradient { manifest, chunks },
                contribution_manifest_ids,
                decoded,
            ));
        }

        anyhow::bail!(
            "timed out waiting for DiLoCo aggregate from reducer {} for round {}: {last_observation}",
            reducer_peer_id.as_str(),
            round_cursor.round_id
        )
    }

    fn publish_diloco_checkpoint_head(
        &mut self,
        experiment: &ExperimentHandle,
        storage: &StorageConfig,
        store: &FsArtifactStore,
        state: &mut PersistedDiLoCoRuntimeState,
        base_head: Option<&HeadDescriptor>,
        completed_round: &RoundCursor,
    ) -> anyhow::Result<HeadDescriptor> {
        let parent_head_id = state
            .snapshot
            .checkpoint_head_id
            .clone()
            .or_else(|| base_head.map(|head| head.head_id.clone()));
        let head_id = HeadId::new(format!(
            "diloco-{}",
            state.current_parameters.checksum()?.as_str()
        ));
        let (artifact, metrics) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let model = project.import_parameter_pack(&device, &state.current_parameters)?;
            let artifact = project.materialize_model_artifact(
                &model,
                ArtifactKind::FullHead,
                head_id.clone(),
                parent_head_id.clone(),
                store,
            )?;
            let metrics = project.evaluate(&model, EvalSplit::Validation).metrics;
            (artifact, metrics)
        };
        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id,
            global_step: completed_round.round_id.as_u64() + 1,
            created_at: Utc::now(),
            metrics,
        };
        persist_head_state(storage, experiment, &head)?;
        persist_json(storage.scoped_head_path(&head.head_id), &head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&head.artifact_id)?;
        self.publish_head_provider(experiment, &head)?;
        Ok(head)
    }
}

fn diloco_response_kind(response: &crate::DiLoCoResponse) -> &'static str {
    match response {
        crate::DiLoCoResponse::Ack { .. } => "ack",
        crate::DiLoCoResponse::StateSnapshot(_) => "state-snapshot",
        crate::DiLoCoResponse::StateBundle(_) => "state-bundle",
        crate::DiLoCoResponse::OuterOptimizerState(_) => "outer-optimizer-state",
        crate::DiLoCoResponse::GradientManifest(_) => "gradient-manifest",
        crate::DiLoCoResponse::CurrentParameters(_) => "current-parameters",
        crate::DiLoCoResponse::GradientChunk(_) => "gradient-chunk",
        crate::DiLoCoResponse::GradientSlice(_) => "gradient-slice",
        crate::DiLoCoResponse::AggregateSlice(_) => "aggregate-slice",
        crate::DiLoCoResponse::Unavailable { .. } => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::ErrorKind,
        net::TcpListener,
        sync::{Mutex, OnceLock},
        thread,
    };

    use chrono::Utc;
    use semver::Version;
    use tempfile::tempdir;

    use super::*;
    use crate::diloco::test_support::ScalarDiLoCoTestWorkload;
    use crate::{
        AuthConfig, ContentId, DatasetViewId, ExperimentDirectoryEntry, ExperimentId,
        ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
        ExperimentVisibility, GenesisSpec, MainnetHandle, NetworkId, NodeBuilder, NodeConfig,
        PeerDirectoryAnnouncement, PeerRole, PeerRoleSet, RevisionId, SwarmAddress, WorkloadId,
    };

    fn native_swarm_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn local_transport_contribution_uses_codec_decoded_values() {
        let peer_id = PeerId::new("peer-local");
        let original = FlattenedTensorPack::new(
            ContentId::new("schema"),
            ContentId::new("layout"),
            vec![0.123_456_7, -0.987_654_3, 123.456_7],
        );
        let encoded = encode_pseudo_gradient(
            ExperimentId::new("exp-transport"),
            RevisionId::new("rev-transport"),
            peer_id.clone(),
            RoundCursor::new(BaseCheckpointId::new("base-transport"), 1),
            crate::GradientCodec::Fp16,
            &original,
            64,
        )
        .expect("encode local FP16 contribution");
        let expected = decode_pseudo_gradient(&encoded.manifest, &encoded.chunks)
            .expect("decode local FP16 contribution");
        assert_ne!(expected.values, original.values);

        let contribution = transport_decoded_contribution(peer_id.clone(), encoded)
            .expect("construct transport-decoded contribution");
        assert_eq!(contribution.peer_id, peer_id);
        assert_eq!(contribution.decoded_gradient, expected);
    }

    #[test]
    fn assignment_leases_partition_into_deterministic_diloco_cohorts() {
        let assigned = (0..6)
            .map(|index| PeerId::new(format!("peer-{index}")))
            .collect::<BTreeSet<_>>();

        assert_eq!(
            assigned_diloco_cohort(&assigned, &PeerId::new("peer-1"), 3),
            Some(vec![
                PeerId::new("peer-0"),
                PeerId::new("peer-1"),
                PeerId::new("peer-2"),
            ])
        );
        assert_eq!(
            assigned_diloco_cohort(&assigned, &PeerId::new("peer-4"), 3),
            Some(vec![
                PeerId::new("peer-3"),
                PeerId::new("peer-4"),
                PeerId::new("peer-5"),
            ])
        );
        assert_eq!(
            assigned_diloco_cohort(&assigned, &PeerId::new("unassigned"), 3),
            None
        );
    }

    #[test]
    fn diloco_state_readiness_requires_the_same_experiment_round_base_and_protocol() {
        let experiment = experiment();
        let policy = DiLoCoPolicy::default();
        let peer_id = PeerId::new("peer-ready");
        let round_cursor = RoundCursor::new(BaseCheckpointId::new("base-ready"), 4);
        let telemetry = crate::NodeTelemetrySnapshot::starting(&mainnet(), &NodeConfig::default());
        let state = DiLoCoStateSnapshot {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
            round_cursor: round_cursor.clone(),
            checkpoint_head_id: None,
            latest_gradient_manifest_id: None,
            current_parameter_checksum: None,
            outer_optimizer_state: None,
            signature_bundle: Vec::new(),
            updated_at: Utc::now(),
        };

        assert_eq!(
            diloco_state_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &state,
                &experiment,
                &round_cursor,
            ),
            None
        );
        assert_eq!(
            diloco_matchmaking_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &state,
                &experiment,
                &round_cursor,
            ),
            None
        );

        let mut completed = state.clone();
        completed.round_cursor.phase = RoundPhase::Completed;
        assert!(
            diloco_matchmaking_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &completed,
                &experiment,
                &round_cursor,
            )
            .is_some_and(|reason| reason.contains("not accepting cohort membership"))
        );
        assert_eq!(
            diloco_state_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &completed,
                &experiment,
                &round_cursor,
            ),
            None,
            "completed peers remain valid bootstrap and rejoin sources"
        );

        let mut wrong_round = state.clone();
        wrong_round.round_cursor.round_id = RoundId::new(1);
        assert!(
            diloco_state_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &wrong_round,
                &experiment,
                &round_cursor,
            )
            .is_some_and(|reason| reason.contains("round mismatch"))
        );

        let mut wrong_base = state.clone();
        wrong_base.round_cursor.base_checkpoint_id = BaseCheckpointId::new("other-base");
        assert!(
            diloco_state_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &wrong_base,
                &experiment,
                &round_cursor,
            )
            .is_some_and(|reason| reason.contains("base mismatch"))
        );

        let mut wrong_protocol = state;
        wrong_protocol.training_protocol = TrainingProtocol::ArtifactWindows;
        assert_eq!(
            diloco_state_compatibility_error(
                &policy,
                &telemetry,
                &peer_id,
                &wrong_protocol,
                &experiment,
                &round_cursor,
            )
            .as_deref(),
            Some("training protocol mismatch")
        );

        let group_id = GroupId::new("group-ready");
        let mut barrier_state = wrong_protocol;
        barrier_state.training_protocol = TrainingProtocol::DiLoCo(policy.clone());
        barrier_state.round_cursor.group_id = Some(group_id.clone());
        barrier_state.round_cursor.phase = RoundPhase::Matchmake;
        assert_eq!(
            diloco_group_barrier_error(
                &policy,
                &telemetry,
                &peer_id,
                &barrier_state,
                &experiment,
                DiLoCoGroupBarrier {
                    round_cursor: &round_cursor,
                    group_id: &group_id,
                    stage: DiLoCoGroupBarrierStage::CohortAnnounced,
                },
            ),
            None
        );

        assert_eq!(
            diloco_group_barrier_error(
                &policy,
                &telemetry,
                &peer_id,
                &barrier_state,
                &experiment,
                DiLoCoGroupBarrier {
                    round_cursor: &round_cursor,
                    group_id: &group_id,
                    stage: DiLoCoGroupBarrierStage::InnerTrainReady,
                },
            )
            .as_deref(),
            Some("peer has not acknowledged inner-train readiness")
        );

        barrier_state.round_cursor.phase = RoundPhase::InnerTrain;
        assert_eq!(
            diloco_group_barrier_error(
                &policy,
                &telemetry,
                &peer_id,
                &barrier_state,
                &experiment,
                DiLoCoGroupBarrier {
                    round_cursor: &round_cursor,
                    group_id: &group_id,
                    stage: DiLoCoGroupBarrierStage::InnerTrainReady,
                },
            ),
            None
        );

        barrier_state.round_cursor.phase = RoundPhase::SyncBase;
        assert_eq!(
            diloco_group_barrier_error(
                &policy,
                &telemetry,
                &peer_id,
                &barrier_state,
                &experiment,
                DiLoCoGroupBarrier {
                    round_cursor: &round_cursor,
                    group_id: &group_id,
                    stage: DiLoCoGroupBarrierStage::CohortAnnounced,
                },
            )
            .as_deref(),
            Some("peer has not entered the matched cohort")
        );
    }

    #[test]
    fn diloco_reducer_rotates_in_canonical_peer_order() {
        let participants = vec![
            PeerId::new("peer-a"),
            PeerId::new("peer-b"),
            PeerId::new("peer-c"),
        ];
        let mut cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);

        for (round, expected) in ["peer-a", "peer-b", "peer-c", "peer-a"]
            .into_iter()
            .enumerate()
        {
            cursor.round_id = RoundId::new(round as u64);
            assert_eq!(
                diloco_reducer_peer(&participants, &cursor).map(PeerId::as_str),
                Some(expected)
            );
        }
        assert!(diloco_reducer_peer(&[], &cursor).is_none());
    }

    #[test]
    fn publishable_diloco_snapshot_omits_bulk_optimizer_state() {
        let experiment = experiment();
        let policy = DiLoCoPolicy::default();
        let parameters = FlattenedTensorPack::new(
            ContentId::new("schema"),
            ContentId::new("layout"),
            vec![1.0, 2.0, 3.0],
        );
        let optimizer =
            StateBlob::try_new("test/opaque", vec![7_u8; 4 * 1024 * 1024]).expect("optimizer blob");
        let state = PersistedDiLoCoRuntimeState {
            snapshot: DiLoCoStateSnapshot {
                experiment_id: experiment.experiment_id,
                revision_id: experiment.revision_id,
                training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
                round_cursor: RoundCursor::new(
                    BaseCheckpointId::new("base"),
                    policy.num_inner_steps,
                ),
                checkpoint_head_id: None,
                latest_gradient_manifest_id: None,
                current_parameter_checksum: None,
                outer_optimizer_state: Some(optimizer),
                signature_bundle: Vec::new(),
                updated_at: Utc::now(),
            },
            current_parameters: parameters.clone(),
            outer_optimizer_state: StateBlob::try_new("test/opaque", vec![7_u8; 4 * 1024 * 1024])
                .expect("optimizer blob"),
            inner_optimizer_state: None,
        };

        let snapshot = state.publishable_snapshot().expect("publishable snapshot");

        assert!(snapshot.outer_optimizer_state.is_none());
        assert_eq!(
            snapshot.current_parameter_checksum,
            Some(parameters.checksum().expect("parameter checksum"))
        );
        assert!(
            serde_json::to_vec(&snapshot)
                .expect("serialize metadata snapshot")
                .len()
                < 16 * 1024,
            "live snapshot should remain metadata-sized"
        );
    }

    #[test]
    fn diloco_gossip_topology_orders_candidates_per_local_peer_and_round() {
        let base = vec![
            PeerId::new("peer-a"),
            PeerId::new("peer-b"),
            PeerId::new("peer-c"),
        ];
        let round_zero = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        let round_one = round_zero.advance(BaseCheckpointId::new("base-next"));
        let control_plane = ControlPlaneSnapshot::default();
        let policy = DiLoCoPolicy {
            topology_policy: crate::DiLoCoTopologyPolicy {
                mode: DiLoCoTopologyMode::GossipNeighborhood,
                fanout: 2,
                prefer_low_latency: true,
                allow_relay: true,
            },
            ..DiLoCoPolicy::default()
        };

        let mut local_a = base.clone();
        order_diloco_candidates(
            &policy,
            &control_plane,
            &PeerId::new("local-a"),
            &round_zero,
            &mut local_a,
        )
        .expect("order local a");
        let mut local_b = base.clone();
        order_diloco_candidates(
            &policy,
            &control_plane,
            &PeerId::new("local-b"),
            &round_zero,
            &mut local_b,
        )
        .expect("order local b");
        let mut next_round = base.clone();
        order_diloco_candidates(
            &policy,
            &control_plane,
            &PeerId::new("local-a"),
            &round_one,
            &mut next_round,
        )
        .expect("order next round");

        assert_eq!(local_a.len(), base.len());
        assert_ne!(local_a, local_b);
        assert_ne!(local_a, next_round);
    }

    #[test]
    fn diloco_topology_prefers_direct_candidates_and_filters_relay_only_paths() {
        let direct = PeerId::new("peer-direct");
        let relay_only = PeerId::new("peer-relay");
        let unknown = PeerId::new("peer-unknown");
        let mut control_plane = ControlPlaneSnapshot::default();
        control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("net-diloco"),
                peer_id: relay_only.clone(),
                addresses: vec![
                    SwarmAddress::new("/ip4/127.0.0.1/tcp/4001/p2p-circuit").expect("relay addr"),
                ],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("net-diloco"),
                peer_id: direct.clone(),
                addresses: vec![SwarmAddress::new("/ip4/127.0.0.1/tcp/4002").expect("direct addr")],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        let round = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        let mut candidates = vec![relay_only.clone(), unknown.clone(), direct.clone()];
        let policy = DiLoCoPolicy {
            topology_policy: crate::DiLoCoTopologyPolicy {
                mode: DiLoCoTopologyMode::RelayAssisted,
                fanout: 3,
                prefer_low_latency: true,
                allow_relay: true,
            },
            ..DiLoCoPolicy::default()
        };

        order_diloco_candidates(
            &policy,
            &control_plane,
            &PeerId::new("local"),
            &round,
            &mut candidates,
        )
        .expect("order relay-assisted candidates");

        assert_eq!(
            candidates,
            vec![direct.clone(), unknown, relay_only.clone()]
        );

        let mut direct_only_candidates = vec![relay_only.clone(), direct.clone()];
        let direct_only_policy = DiLoCoPolicy {
            topology_policy: crate::DiLoCoTopologyPolicy {
                allow_relay: false,
                ..policy.topology_policy
            },
            ..DiLoCoPolicy::default()
        };
        order_diloco_candidates(
            &direct_only_policy,
            &control_plane,
            &PeerId::new("local"),
            &round,
            &mut direct_only_candidates,
        )
        .expect("filter relay candidates");

        assert_eq!(direct_only_candidates, vec![direct]);
    }

    #[test]
    fn diloco_topology_excludes_known_non_training_service_peers() {
        let trainer = PeerId::new("peer-trainer");
        let bootstrap = PeerId::new("peer-bootstrap");
        let unknown = PeerId::new("peer-unknown");
        let mut control_plane = ControlPlaneSnapshot::default();
        for (peer_id, roles) in [
            (trainer.clone(), PeerRoleSet::default_trainer()),
            (
                bootstrap.clone(),
                PeerRoleSet::new([PeerRole::Bootstrap, PeerRole::RelayHelper]),
            ),
        ] {
            control_plane
                .peer_directory_announcements
                .push(PeerDirectoryAnnouncement {
                    network_id: NetworkId::new("net-diloco"),
                    peer_id,
                    addresses: vec![
                        SwarmAddress::new("/ip4/127.0.0.1/tcp/4001").expect("direct addr"),
                    ],
                    advertised_roles: Some(roles),
                    announced_at: Utc::now(),
                });
        }
        let mut candidates = vec![bootstrap, unknown.clone(), trainer.clone()];

        order_diloco_candidates(
            &DiLoCoPolicy::default(),
            &control_plane,
            &PeerId::new("local"),
            &RoundCursor::new(BaseCheckpointId::new("base"), 4),
            &mut candidates,
        )
        .expect("filter service peers");

        assert_eq!(candidates, vec![trainer, unknown]);
    }

    fn loopback_listen_address() -> Option<SwarmAddress> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {
                eprintln!("skipping networked DiLoCo test: loopback bind denied by environment");
                return None;
            }
            Err(error) => panic!("bind test listener: {error}"),
        };
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        Some(SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/{port}")).expect("listen"))
    }

    fn mainnet() -> MainnetHandle {
        MainnetHandle {
            genesis: GenesisSpec {
                network_id: NetworkId::new("net-diloco"),
                protocol_version: Version::new(0, 1, 0),
                display_name: "diloco-testnet".into(),
                created_at: Utc::now(),
                metadata: BTreeMap::new(),
            },
            roles: PeerRoleSet::default_trainer(),
        }
    }

    fn experiment() -> ExperimentHandle {
        mainnet().experiment(
            crate::StudyId::new("study-diloco"),
            crate::ExperimentId::new("exp-diloco"),
            crate::RevisionId::new("rev-diloco"),
        )
    }

    fn diloco_directory_entry(
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
    ) -> ExperimentDirectoryEntry {
        ExperimentDirectoryEntry {
            network_id: experiment.network_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            workload_id: WorkloadId::new("scalar-diloco-network"),
            display_name: "Scalar DiLoCo".into(),
            model_schema_hash: ContentId::new("scalar-schema"),
            dataset_view_id: DatasetViewId::new("scalar-view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 1024,
                estimated_window_seconds: 5,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: experiment.revision_id.clone(),
            current_head_id: None,
            allowed_roles: PeerRoleSet::default_trainer(),
            allowed_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: experiment.experiment_id.clone(),
            }]),
            training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
            metadata: BTreeMap::new(),
        }
    }

    fn wait_for(timeout: Duration, mut predicate: impl FnMut() -> bool, message: &str) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("{message}");
    }

    #[test]
    fn native_runtime_reliably_serves_sequential_diloco_state_requests() {
        let _guard = native_swarm_test_guard();
        let experiment = experiment();
        let policy = DiLoCoPolicy::default();
        let seed_storage = tempdir().expect("seed storage");
        let peer_storage = tempdir().expect("peer storage");
        let Some(seed_listen) = loopback_listen_address() else {
            return;
        };
        let seed = NodeBuilder::new(ScalarDiLoCoTestWorkload::network(0.5))
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(seed_storage.path()))
            .with_listen_address(seed_listen)
            .spawn()
            .expect("seed spawn");
        let seed_telemetry = seed.telemetry();
        wait_for(
            Duration::from_secs(5),
            || {
                let snapshot = seed_telemetry.snapshot();
                snapshot.local_peer_id.is_some() && !snapshot.listen_addresses.is_empty()
            },
            "seed runtime did not start",
        );
        let seed_snapshot = seed_telemetry.snapshot();
        let seed_peer_id = seed_snapshot.local_peer_id.expect("seed peer id");
        let seed_address = seed_snapshot.listen_addresses[0].clone();
        let peer = NodeBuilder::new(ScalarDiLoCoTestWorkload::network(0.25))
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(peer_storage.path()))
            .with_bootstrap_peer(seed_address)
            .spawn()
            .expect("peer spawn");
        let peer_telemetry = peer.telemetry();
        wait_for(
            Duration::from_secs(5),
            || peer_telemetry.snapshot().connected_peers >= 1,
            "peer did not connect",
        );

        let parameters = FlattenedTensorPack::new(
            ContentId::new("scalar-schema"),
            ContentId::new("scalar-layout"),
            vec![0.5],
        );
        let optimizer =
            StateBlob::try_new("application/json", b"{}".to_vec()).expect("optimizer state");
        seed.control_handle()
            .publish_diloco_state(
                DiLoCoStateSnapshot {
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
                    round_cursor: RoundCursor::new(
                        BaseCheckpointId::new("base"),
                        policy.num_inner_steps,
                    ),
                    checkpoint_head_id: None,
                    latest_gradient_manifest_id: None,
                    current_parameter_checksum: Some(
                        parameters.checksum().expect("parameter checksum"),
                    ),
                    outer_optimizer_state: None,
                    signature_bundle: Vec::new(),
                    updated_at: Utc::now(),
                },
                Some(optimizer.clone()),
                Some(parameters.clone()),
            )
            .expect("publish state");

        let control = peer.control_handle();
        wait_for(
            Duration::from_secs(5),
            || {
                control
                    .fetch_diloco_state_snapshot(
                        seed_peer_id.as_str(),
                        experiment.experiment_id.clone(),
                        experiment.revision_id.clone(),
                        Duration::from_secs(1),
                    )
                    .ok()
                    .flatten()
                    .is_some()
            },
            "published DiLoCo state did not become visible",
        );
        for request_index in 0..32 {
            assert!(
                control
                    .fetch_diloco_state_snapshot(
                        seed_peer_id.as_str(),
                        experiment.experiment_id.clone(),
                        experiment.revision_id.clone(),
                        Duration::from_secs(3),
                    )
                    .unwrap_or_else(|error| {
                        panic!("state request {request_index} failed: {error:#}")
                    })
                    .is_some()
            );
            assert_eq!(
                control
                    .fetch_diloco_current_parameters(
                        seed_peer_id.as_str(),
                        experiment.experiment_id.clone(),
                        experiment.revision_id.clone(),
                        Duration::from_secs(3),
                    )
                    .unwrap_or_else(|error| {
                        panic!("parameter request {request_index} failed: {error:#}")
                    }),
                Some(parameters.clone())
            );
            assert_eq!(
                control
                    .fetch_diloco_outer_optimizer_state(
                        seed_peer_id.as_str(),
                        experiment.experiment_id.clone(),
                        experiment.revision_id.clone(),
                        Duration::from_secs(3),
                    )
                    .unwrap_or_else(|error| {
                        panic!("optimizer request {request_index} failed: {error:#}")
                    }),
                Some(optimizer.clone())
            );
            let bundle = control
                .fetch_diloco_state_bundle(
                    seed_peer_id.as_str(),
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    Duration::from_secs(3),
                )
                .unwrap_or_else(|error| {
                    panic!("state-bundle request {request_index} failed: {error:#}")
                })
                .expect("published state bundle");
            assert_eq!(bundle.current_parameters, parameters);
            assert_eq!(bundle.outer_optimizer_state, optimizer);
            assert!(!bundle.snapshot.signature_bundle.is_empty());
        }

        peer.shutdown().expect("peer shutdown");
        let _ = peer.await_termination().expect("peer termination");
        seed.shutdown().expect("seed shutdown");
        let _ = seed.await_termination().expect("seed termination");
    }

    #[test]
    fn networked_diloco_round_exchanges_gradients_and_bootstraps_rejoin_state() {
        let _guard = native_swarm_test_guard();
        let experiment = experiment();
        let policy = DiLoCoPolicy {
            num_inner_steps: 2,
            target_group_size: 2,
            minimum_group_size: 1,
            matchmaking_timeout_ms: 1_000,
            aggregation_timeout_ms: 2_000,
            checkpoint_interval_rounds: 1,
            outer_optimizer_policy: crate::OuterOptimizerPolicy::Sgd {
                learning_rate_micros: 1_000_000,
                momentum_micros: Some(250_000),
                nesterov: false,
                weight_decay_micros: None,
                max_pseudo_gradient_rms_ratio_micros: None,
            },
            ..DiLoCoPolicy::default()
        };
        let directory_entry = diloco_directory_entry(&experiment, &policy);
        let auth = AuthConfig::new().with_experiment_directory(vec![directory_entry.clone()]);

        let seed_storage = tempdir().expect("seed storage");
        let peer_storage = tempdir().expect("peer storage");
        let late_storage = tempdir().expect("late storage");
        let Some(seed_listen) = loopback_listen_address() else {
            return;
        };

        let mut seed = NodeBuilder::new(ScalarDiLoCoTestWorkload::network(0.5))
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(seed_storage.path()))
            .with_auth(auth.clone())
            .with_listen_address(seed_listen.clone())
            .spawn()
            .expect("seed spawn");
        let seed_telemetry = seed.telemetry();
        wait_for(
            Duration::from_secs(5),
            || {
                let snapshot = seed_telemetry.snapshot();
                snapshot.status == crate::RuntimeStatus::Running
                    && snapshot.local_peer_id.is_some()
                    && !snapshot.listen_addresses.is_empty()
            },
            "seed runtime did not start",
        );
        seed.initialize_local_head(&experiment)
            .expect("seed genesis head");
        let seed_addr = seed_telemetry.snapshot().listen_addresses[0].clone();

        let mut peer = NodeBuilder::new(ScalarDiLoCoTestWorkload::network(0.25))
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(peer_storage.path()))
            .with_auth(auth.clone())
            .with_bootstrap_peer(seed_addr.clone())
            .spawn()
            .expect("peer spawn");
        let peer_telemetry = peer.telemetry();
        wait_for(
            Duration::from_secs(5),
            || seed_telemetry.snapshot().connected_peers >= 1,
            "seed did not connect to peer",
        );
        wait_for(
            Duration::from_secs(5),
            || peer_telemetry.snapshot().connected_peers >= 1,
            "peer did not connect to seed",
        );
        wait_for(
            Duration::from_secs(5),
            || {
                peer.sync_experiment_head(&experiment)
                    .expect("peer head sync")
                    .is_some()
            },
            "peer did not sync genesis head",
        );

        let experiment_for_peer = experiment.clone();
        let peer_thread = thread::spawn(move || {
            peer.diloco_round_once_with_batches(&experiment_for_peer, &[0.5, 0.5])
                .map(|outcome| (peer, outcome))
                .map_err(|error| format!("{error:#}"))
        });
        let seed_outcome = seed
            .diloco_round_once_with_batches(&experiment, &[1.0, 1.0])
            .expect("seed diloco round");
        let (peer, peer_outcome) = peer_thread
            .join()
            .expect("peer round thread should not panic")
            .unwrap_or_else(|error| panic!("peer diloco round failed: {error}"));
        let peer = peer;

        assert_eq!(seed_outcome.completed_round.round_id.as_u64(), 0);
        assert_eq!(peer_outcome.completed_round.round_id.as_u64(), 0);
        assert!(seed_outcome.training_lease.is_none());
        assert!(peer_outcome.training_lease.is_none());
        assert_eq!(seed_outcome.local_inner_report.steps_completed, 2);
        assert_eq!(peer_outcome.local_inner_report.steps_completed, 2);
        assert_eq!(seed_outcome.next_round_cursor.round_id.as_u64(), 1);
        assert_eq!(peer_outcome.next_round_cursor.round_id.as_u64(), 1);
        assert_eq!(seed_outcome.group_id, peer_outcome.group_id);
        assert_eq!(seed_outcome.reducer_peer_id, peer_outcome.reducer_peer_id);
        assert_eq!(
            seed_outcome.contribution_manifest_ids,
            peer_outcome.contribution_manifest_ids
        );
        assert_eq!(seed_outcome.contribution_manifest_ids.len(), 2);
        assert_eq!(
            seed_outcome.aggregate_manifest.manifest_id,
            peer_outcome.aggregate_manifest.manifest_id
        );
        assert_eq!(
            seed_outcome.contributions.len() + peer_outcome.contributions.len(),
            3,
            "the reducer retains both contributions and the follower retains its local one"
        );
        assert_eq!(
            seed_outcome.current_parameters,
            peer_outcome.current_parameters
        );
        assert_eq!(
            seed_outcome
                .published_checkpoint
                .as_ref()
                .expect("seed checkpoint")
                .head_id,
            peer_outcome
                .published_checkpoint
                .as_ref()
                .expect("peer checkpoint")
                .head_id
        );
        let seed_state: PersistedDiLoCoRuntimeState = load_json(
            StorageConfig::new(seed_storage.path()).scoped_diloco_state_path(&experiment),
        )
        .expect("load seed state")
        .expect("seed state exists");
        let peer_state: PersistedDiLoCoRuntimeState = load_json(
            StorageConfig::new(peer_storage.path()).scoped_diloco_state_path(&experiment),
        )
        .expect("load peer state")
        .expect("peer state exists");
        assert_eq!(seed_state.current_parameters, peer_state.current_parameters);
        assert_eq!(seed_state.snapshot.round_cursor.round_id.as_u64(), 1);
        assert_eq!(peer_state.snapshot.round_cursor.round_id.as_u64(), 1);

        let mut late_joiner = NodeBuilder::new(ScalarDiLoCoTestWorkload::network(0.125))
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(late_storage.path()))
            .with_auth(auth)
            .with_bootstrap_peer(seed_addr)
            .spawn()
            .expect("late joiner spawn");
        let late_telemetry = late_joiner.telemetry();
        wait_for(
            Duration::from_secs(5),
            || late_telemetry.snapshot().connected_peers >= 1,
            "late joiner did not connect",
        );
        let late_outcome = late_joiner
            .diloco_round_once_with_batches(&experiment, &[0.25, 0.25])
            .unwrap_or_else(|error| {
                let snapshot = late_telemetry.snapshot();
                panic!(
                    "late joiner diloco round: {error:#}; connected={:?}; failures={:?}; recent_events={:?}",
                    snapshot
                        .connected_peer_ids
                        .iter()
                        .map(PeerId::as_str)
                        .collect::<Vec<_>>(),
                    snapshot.request_failures,
                    snapshot.recent_events.iter().rev().take(24).collect::<Vec<_>>(),
                )
            });
        assert_eq!(
            late_outcome.completed_round.round_id.as_u64(),
            1,
            "late joiner replayed a completed round; timing={:?}; connected={:?}; failures={:?}; recent_events={:?}",
            late_outcome.timing,
            late_telemetry
                .snapshot()
                .connected_peer_ids
                .iter()
                .map(PeerId::as_str)
                .collect::<Vec<_>>(),
            late_telemetry.snapshot().request_failures,
            late_telemetry
                .snapshot()
                .recent_events
                .iter()
                .rev()
                .take(24)
                .collect::<Vec<_>>(),
        );
        assert_eq!(late_outcome.next_round_cursor.round_id.as_u64(), 2);
        assert!(late_outcome.published_checkpoint.is_some());

        late_joiner.shutdown().expect("late joiner shutdown");
        let _ = late_joiner
            .await_termination()
            .expect("late joiner termination");
        peer.shutdown().expect("peer shutdown");
        let _ = peer.await_termination().expect("peer termination");
        seed.shutdown().expect("seed shutdown");
        let _ = seed.await_termination().expect("seed termination");
    }

    fn run_strict_three_peer_diloco_round(parameter_count: usize, timeout_ms: u32) {
        let _guard = native_swarm_test_guard();
        let experiment = experiment();
        let policy = DiLoCoPolicy {
            num_inner_steps: 2,
            target_group_size: 3,
            minimum_group_size: 3,
            matchmaking_timeout_ms: timeout_ms,
            aggregation_timeout_ms: timeout_ms,
            checkpoint_interval_rounds: 1,
            codec: crate::GradientCodec::Fp32,
            ..DiLoCoPolicy::default()
        };
        let auth = AuthConfig::new()
            .with_experiment_directory(vec![diloco_directory_entry(&experiment, &policy)]);
        let storages = [
            tempdir().expect("seed storage"),
            tempdir().expect("peer b storage"),
            tempdir().expect("peer c storage"),
        ];
        let listen_addresses = [
            loopback_listen_address().expect("seed listen"),
            loopback_listen_address().expect("peer b listen"),
            loopback_listen_address().expect("peer c listen"),
        ];

        let mut seed =
            NodeBuilder::new(ScalarDiLoCoTestWorkload::network_wide(0.5, parameter_count))
                .with_mainnet(mainnet().genesis.clone())
                .with_storage(StorageConfig::new(storages[0].path()))
                .with_auth(auth.clone())
                .with_listen_address(listen_addresses[0].clone())
                .spawn()
                .expect("seed spawn");
        let seed_telemetry = seed.telemetry();
        wait_for(
            Duration::from_secs(5),
            || seed_telemetry.snapshot().local_peer_id.is_some(),
            "seed did not start",
        );
        seed.initialize_local_head(&experiment)
            .expect("seed genesis head");

        let mut peer_b = NodeBuilder::new(ScalarDiLoCoTestWorkload::network_wide(
            0.25,
            parameter_count,
        ))
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storages[1].path()))
        .with_auth(auth.clone())
        .with_listen_address(listen_addresses[1].clone())
        .with_bootstrap_peer(listen_addresses[0].clone())
        .spawn()
        .expect("peer b spawn");
        let peer_b_telemetry = peer_b.telemetry();
        wait_for(
            Duration::from_secs(5),
            || peer_b_telemetry.snapshot().connected_peers >= 1,
            "peer b did not connect",
        );
        let mut peer_c = NodeBuilder::new(ScalarDiLoCoTestWorkload::network_wide(
            0.125,
            parameter_count,
        ))
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storages[2].path()))
        .with_auth(auth)
        .with_listen_address(listen_addresses[2].clone())
        .with_bootstrap_peers([listen_addresses[0].clone(), listen_addresses[1].clone()])
        .spawn()
        .expect("peer c spawn");
        let peer_c_telemetry = peer_c.telemetry();
        for (label, telemetry) in [
            ("seed", &seed_telemetry),
            ("peer b", &peer_b_telemetry),
            ("peer c", &peer_c_telemetry),
        ] {
            wait_for(
                Duration::from_secs(10),
                || telemetry.snapshot().connected_peers >= 2,
                &format!("{label} did not establish the trainer mesh"),
            );
        }
        for peer in [&peer_b, &peer_c] {
            wait_for(
                Duration::from_secs(5),
                || {
                    peer.sync_experiment_head(&experiment)
                        .expect("sync genesis")
                        .is_some()
                },
                "peer did not sync genesis",
            );
        }

        let experiment_b = experiment.clone();
        let experiment_c = experiment.clone();
        let peer_b_ref = &mut peer_b;
        let peer_c_ref = &mut peer_c;
        let (seed_result, peer_b_result, peer_c_result) = thread::scope(|scope| {
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
            let barrier_b = std::sync::Arc::clone(&barrier);
            let b = scope.spawn(move || {
                barrier_b.wait();
                peer_b_ref.diloco_round_once_with_batches(&experiment_b, &[0.5, 0.5])
            });
            let barrier_c = std::sync::Arc::clone(&barrier);
            let c = scope.spawn(move || {
                barrier_c.wait();
                peer_c_ref.diloco_round_once_with_batches(&experiment_c, &[0.25, 0.25])
            });
            barrier.wait();
            let seed = seed.diloco_round_once_with_batches(&experiment, &[1.0, 1.0]);
            (
                seed,
                b.join().expect("peer b thread"),
                c.join().expect("peer c thread"),
            )
        });
        let outcomes = [
            seed_result.expect("seed round"),
            peer_b_result.expect("peer b round"),
            peer_c_result.expect("peer c round"),
        ];
        if parameter_count.saturating_mul(std::mem::size_of::<f32>()) > DILOCO_CHUNK_SIZE_BYTES {
            assert!(
                outcomes
                    .iter()
                    .all(|outcome| outcome.local_gradient_manifest.chunk_count > 1),
                "model-sized DiLoCo payloads must remain split into bounded transport chunks"
            );
            assert!(
                outcomes
                    .iter()
                    .all(|outcome| outcome.aggregate_manifest.chunk_count > 1),
                "model-sized reduced aggregates must remain split into bounded transport chunks"
            );
        }
        let reducer_peer_id = outcomes[0].reducer_peer_id.clone();
        assert!(outcomes.iter().all(|outcome| {
            outcome.reducer_peer_id == reducer_peer_id
                && outcome.contribution_manifest_ids == outcomes[0].contribution_manifest_ids
                && outcome.contribution_manifest_ids.len() == 3
                && outcome.aggregate_manifest.manifest_id
                    == outcomes[0].aggregate_manifest.manifest_id
        }));
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome.contributions.len() == 3)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome.contributions.len() == 1)
                .count(),
            2
        );
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.group_id == outcomes[0].group_id)
        );
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.current_parameters == outcomes[0].current_parameters)
        );
        let seed_peer_id = seed_telemetry
            .snapshot()
            .local_peer_id
            .expect("seed peer id");
        let state_bundle = peer_c
            .control_handle()
            .fetch_diloco_state_bundle(
                seed_peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                Duration::from_secs(30),
            )
            .expect("fetch post-round state bundle")
            .expect("post-round state bundle");
        assert_eq!(
            state_bundle.current_parameters,
            outcomes[0].current_parameters
        );
        assert_eq!(
            state_bundle.snapshot.current_parameter_checksum,
            Some(
                state_bundle
                    .current_parameters
                    .checksum()
                    .expect("bundle parameter checksum")
            )
        );
        assert_eq!(state_bundle.snapshot.round_cursor.round_id.as_u64(), 1);

        peer_c.shutdown().expect("peer c shutdown");
        let _ = peer_c.await_termination().expect("peer c termination");
        peer_b.shutdown().expect("peer b shutdown");
        let _ = peer_b.await_termination().expect("peer b termination");
        seed.shutdown().expect("seed shutdown");
        let _ = seed.await_termination().expect("seed termination");
    }

    #[test]
    fn strict_three_peer_diloco_round_converges_on_one_shared_update() {
        run_strict_three_peer_diloco_round(1, 10_000);
    }

    #[test]
    fn strict_three_peer_diloco_bulk_round_exchanges_one_million_parameters() {
        run_strict_three_peer_diloco_round(1_000_000, 60_000);
    }
}
