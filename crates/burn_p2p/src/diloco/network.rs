use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, anyhow, ensure};
use chrono::Utc;

use super::{
    DiLoCoPeerContribution, EncodedPseudoGradient, aggregate_pseudo_gradients,
    decode_pseudo_gradient, encode_pseudo_gradient,
};
use crate::Duration;
use crate::runtime_support::{
    connected_peer_ids, load_head_state, load_json, persist_head_state, persist_json,
    runtime_training_protocol, verify_diloco_gradient_manifest_signature,
    verify_diloco_state_snapshot_signature,
};
use crate::training::load_model_for_head;
use crate::{
    ArtifactKind, BaseCheckpointId, ContentId, ControlPlaneSnapshot, DiLoCoPolicy,
    DiLoCoRoundFinalize, DiLoCoRoundHeartbeat, DiLoCoRoundOffer, DiLoCoStateSnapshot,
    DiLoCoTopologyMode, DiLoCoWorkload, EvalSplit, ExperimentHandle, FlattenedTensorPack,
    FsArtifactStore, GroupId, HeadDescriptor, HeadId, Instant, NodeRuntimeState, PeerId,
    PseudoGradientManifest, RoundCursor, RoundPhase, RunningNode, SlotAssignmentState,
    SlotRuntimeState, StateBlob, StorageConfig, TrainingProtocol,
};

const DILOCO_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DILOCO_REQUEST_TIMEOUT_CAP: Duration = Duration::from_millis(750);
const DILOCO_CHUNK_SIZE_BYTES: usize = 1024;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct PersistedDiLoCoRuntimeState {
    snapshot: DiLoCoStateSnapshot,
    current_parameters: FlattenedTensorPack,
    outer_optimizer_state: StateBlob,
    inner_optimizer_state: Option<StateBlob>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiLoCoRoundOutcome {
    pub participant_peer_ids: Vec<PeerId>,
    pub group_id: GroupId,
    pub completed_round: RoundCursor,
    pub next_round_cursor: RoundCursor,
    pub aggregate: FlattenedTensorPack,
    pub contributions: Vec<DiLoCoPeerContribution>,
    pub local_gradient_manifest: PseudoGradientManifest,
    pub current_parameters: FlattenedTensorPack,
    pub published_checkpoint: Option<HeadDescriptor>,
}

#[derive(Clone, Debug)]
struct RemoteDiLoCoState {
    peer_id: PeerId,
    snapshot: DiLoCoStateSnapshot,
    current_parameters: FlattenedTensorPack,
    outer_optimizer_state: StateBlob,
}

fn request_timeout(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        None
    } else {
        Some(remaining.min(DILOCO_REQUEST_TIMEOUT_CAP))
    }
}

fn checkpoint_due(policy: &DiLoCoPolicy, round: &RoundCursor) -> bool {
    ((round.round_id.as_u64() + 1) % u64::from(policy.checkpoint_interval_rounds.max(1))) == 0
}

fn diloco_state_signature_acceptable(
    snapshot: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    state: &DiLoCoStateSnapshot,
) -> bool {
    if state.signature_bundle.is_empty() {
        return true;
    }
    diloco_peer_has_auth(&snapshot.control_plane, peer_id)
        .then(|| verify_diloco_state_snapshot_signature(&snapshot.control_plane, peer_id, state))
        .unwrap_or(true)
}

fn diloco_manifest_signature_acceptable(
    snapshot: &crate::NodeTelemetrySnapshot,
    peer_id: &PeerId,
    manifest: &PseudoGradientManifest,
) -> bool {
    if manifest.signature_bundle.is_empty() {
        return true;
    }
    diloco_peer_has_auth(&snapshot.control_plane, peer_id)
        .then(|| {
            verify_diloco_gradient_manifest_signature(&snapshot.control_plane, peer_id, manifest)
        })
        .unwrap_or(true)
}

fn diloco_peer_has_auth(control_plane: &ControlPlaneSnapshot, peer_id: &PeerId) -> bool {
    control_plane
        .auth_announcements
        .iter()
        .any(|announcement| &announcement.peer_id == peer_id)
}

fn order_diloco_candidates(
    policy: &DiLoCoPolicy,
    local_peer_id: &PeerId,
    round_cursor: &RoundCursor,
    candidates: &mut [PeerId],
) -> anyhow::Result<()> {
    match policy.topology_policy.mode {
        DiLoCoTopologyMode::DeterministicRendezvous | DiLoCoTopologyMode::RelayAssisted => {
            candidates.sort_by(|left, right| left.as_str().cmp(right.as_str()));
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
                left.0
                    .as_str()
                    .cmp(right.0.as_str())
                    .then_with(|| left.1.as_str().cmp(right.1.as_str()))
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

impl PersistedDiLoCoRuntimeState {
    fn publishable_snapshot(&self) -> anyhow::Result<DiLoCoStateSnapshot> {
        let mut snapshot = self.snapshot.clone();
        snapshot.outer_optimizer_state = Some(self.outer_optimizer_state.clone());
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

        let participant_peer_ids = self
            .matchmake_diloco_participants(
                experiment,
                &policy,
                &local_peer_id,
                &state.snapshot.round_cursor,
            )
            .context("matchmake DiLoCo participants")?;
        let group_id = derive_group_id(&state.snapshot.round_cursor, &participant_peer_ids)?;
        let mut completed_round = state.snapshot.round_cursor.clone();
        completed_round.group_id = Some(group_id.clone());
        completed_round.phase = RoundPhase::BuildPseudoGradient;
        state.snapshot.round_cursor = completed_round.clone();
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo matchmaking state")?;
        self.broadcast_diloco_heartbeat(
            experiment,
            &participant_peer_ids,
            &local_peer_id,
            &completed_round,
            participant_peer_ids.len() as u16,
            &policy,
        );

        self.update_runtime_state(
            NodeRuntimeState::TrainingWindow,
            Some(SlotRuntimeState::Training(assignment.clone())),
        );
        let (local_gradient, saved_inner_optimizer_state) = (|| -> anyhow::Result<_> {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let base_model = project.import_parameter_pack(&device, &state.current_parameters)?;
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
            let pseudo_gradient = project
                .build_pseudo_gradient(&state.current_parameters, &inner_report.local_parameters)?;
            Ok((pseudo_gradient, saved_inner_optimizer_state))
        })()
        .context("run local DiLoCo inner loop")?;
        state.inner_optimizer_state = saved_inner_optimizer_state;

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

        let mut contributions = vec![DiLoCoPeerContribution {
            peer_id: local_peer_id.clone(),
            encoded: encoded.clone(),
            decoded_gradient: local_gradient,
        }];
        contributions.extend(
            self.collect_remote_diloco_contributions(
                experiment,
                &participant_peer_ids,
                &local_peer_id,
                &completed_round,
                &policy,
            )
            .context("collect remote DiLoCo contributions")?,
        );
        ensure!(
            contributions.len() >= policy.minimum_group_size as usize,
            "DiLoCo aggregation only collected {} participant(s), below the minimum {}",
            contributions.len(),
            policy.minimum_group_size
        );

        let aggregate = aggregate_pseudo_gradients(
            &policy.aggregation_policy,
            &contributions
                .iter()
                .map(|contribution| contribution.decoded_gradient.clone())
                .collect::<Vec<_>>(),
        )?;
        state.snapshot.round_cursor.phase = RoundPhase::OuterApply;
        state.snapshot.updated_at = Utc::now();
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)
            .context("publish DiLoCo outer-apply state")?;

        let (next_parameters, next_outer_optimizer_state) = (|| -> anyhow::Result<_> {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            Ok(project.apply_aggregated_outer_update(
                &state.current_parameters,
                &aggregate,
                &state.outer_optimizer_state,
                &policy.outer_optimizer_policy,
            )?)
        })()
        .context("apply DiLoCo outer update")?;
        state.current_parameters = next_parameters;
        state.outer_optimizer_state = (|| -> anyhow::Result<_> {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            Ok(project.save_outer_optimizer_state(&next_outer_optimizer_state)?)
        })()
        .context("persist DiLoCo outer optimizer state")?;

        let next_base_checkpoint_id =
            BaseCheckpointId::new(state.current_parameters.checksum()?.into_inner());
        let next_round_cursor = completed_round.advance(next_base_checkpoint_id);
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
        state.snapshot.round_cursor = next_round_cursor.clone();
        state.snapshot.round_cursor.phase = RoundPhase::Completed;
        state.snapshot.round_cursor.group_id = None;
        state.snapshot.updated_at = Utc::now();
        if let Some(head) = published_checkpoint.as_ref() {
            state.snapshot.checkpoint_head_id = Some(head.head_id.clone());
        }
        self.persist_and_publish_diloco_state(&storage, experiment, &mut state)?;
        self.broadcast_diloco_finalize(
            experiment,
            &participant_peer_ids,
            &local_peer_id,
            &completed_round,
            contributions.len() as u16,
            Some(aggregate.checksum()?),
            &policy,
        );
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);

        Ok(DiLoCoRoundOutcome {
            participant_peer_ids,
            group_id,
            completed_round,
            next_round_cursor,
            aggregate,
            contributions,
            local_gradient_manifest,
            current_parameters: state.current_parameters,
            published_checkpoint,
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
            if head.global_step == 0 || has_local_artifact {
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
            if let Some(remote) = self.best_remote_diloco_state(experiment, policy)? {
                let local_round = state.snapshot.round_cursor.round_id.as_u64();
                let remote_round = remote.snapshot.round_cursor.round_id.as_u64();
                let lag = remote_round.saturating_sub(local_round);
                if lag > 0
                    && (lag <= u64::from(policy.rejoin_policy.max_fast_forward_round_lag)
                        || remote.snapshot.checkpoint_head_id.is_some())
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

        if let Some(remote) = self.best_remote_diloco_state(experiment, policy)? {
            let state = PersistedDiLoCoRuntimeState {
                snapshot: remote.snapshot,
                current_parameters: remote.current_parameters,
                outer_optimizer_state: remote.outer_optimizer_state,
                inner_optimizer_state: None,
            };
            persist_json(local_path, &state)?;
            return Ok(state);
        }

        let mut state = self.bootstrap_local_diloco_state(experiment, policy, base_head, store)?;
        state.snapshot.updated_at = Utc::now();
        persist_json(local_path, &state)?;
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
        let (model, checkpoint_head_id) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let model = if let Some(head) = base_head {
                load_model_for_head(project, head, store, &device)?
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
        Ok(PersistedDiLoCoRuntimeState {
            snapshot: DiLoCoStateSnapshot {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
                round_cursor: RoundCursor::new(base_checkpoint_id, policy.num_inner_steps),
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

    fn best_remote_diloco_state(
        &self,
        experiment: &ExperimentHandle,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<Option<RemoteDiLoCoState>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let local_peer_id = telemetry_snapshot.local_peer_id.as_ref();
        let mut remote_states = Vec::new();
        for peer_id in connected_peer_ids(&telemetry_snapshot) {
            if local_peer_id.is_some_and(|local| local == &peer_id) {
                continue;
            }
            let Some(timeout) = request_timeout(Instant::now() + Duration::from_millis(500)) else {
                continue;
            };
            let Ok(Some(snapshot)) = self.control.fetch_diloco_state_snapshot(
                peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                timeout,
            ) else {
                continue;
            };
            if !diloco_state_signature_acceptable(&telemetry_snapshot, &peer_id, &snapshot) {
                continue;
            }
            if snapshot.training_protocol != TrainingProtocol::DiLoCo(policy.clone()) {
                continue;
            }
            let Ok(Some(current_parameters)) = self.control.fetch_diloco_current_parameters(
                peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                timeout,
            ) else {
                continue;
            };
            let Ok(Some(outer_optimizer_state)) = self.control.fetch_diloco_outer_optimizer_state(
                peer_id.as_str(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                timeout,
            ) else {
                continue;
            };
            if snapshot.current_parameter_checksum.as_ref() != Some(&current_parameters.checksum()?)
            {
                continue;
            }
            remote_states.push(RemoteDiLoCoState {
                peer_id,
                snapshot,
                current_parameters,
                outer_optimizer_state,
            });
        }
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
    ) -> anyhow::Result<Vec<PeerId>> {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.matchmaking_timeout_ms.max(1)));
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut candidates = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .filter(|peer_id| peer_id != local_peer_id)
            .collect::<Vec<_>>();
        order_diloco_candidates(policy, local_peer_id, round_cursor, &mut candidates)?;
        let fanout = usize::from(policy.topology_policy.fanout)
            .max(usize::from(policy.target_group_size))
            .max(1);
        candidates.truncate(fanout);
        let mut compatible = BTreeSet::from([local_peer_id.clone()]);

        while Instant::now() < deadline && compatible.len() < policy.target_group_size as usize {
            let mut progressed = false;
            for peer_id in &candidates {
                if compatible.contains(peer_id) {
                    continue;
                }
                let Some(timeout) = request_timeout(deadline) else {
                    break;
                };
                let Ok(Some(snapshot)) = self.control.fetch_diloco_state_snapshot(
                    peer_id.as_str(),
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    timeout,
                ) else {
                    continue;
                };
                if snapshot.round_cursor.round_id == round_cursor.round_id
                    && snapshot.round_cursor.base_checkpoint_id == round_cursor.base_checkpoint_id
                {
                    compatible.insert(peer_id.clone());
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
            "need at least {} DiLoCo participant(s), found {}",
            policy.minimum_group_size,
            participants.len()
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

    fn broadcast_diloco_heartbeat(
        &self,
        experiment: &ExperimentHandle,
        participants: &[PeerId],
        local_peer_id: &PeerId,
        round_cursor: &RoundCursor,
        observed_participants: u16,
        policy: &DiLoCoPolicy,
    ) {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.matchmaking_timeout_ms.max(1)));
        for peer_id in participants {
            if peer_id == local_peer_id {
                continue;
            }
            let Some(timeout) = request_timeout(deadline) else {
                break;
            };
            let heartbeat = DiLoCoRoundHeartbeat {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                peer_id: local_peer_id.clone(),
                round_cursor: round_cursor.clone(),
                observed_participants,
                emitted_at: Utc::now(),
            };
            let _ = self
                .control
                .send_diloco_round_heartbeat(peer_id.as_str(), heartbeat, timeout);
        }
    }

    fn broadcast_diloco_finalize(
        &self,
        experiment: &ExperimentHandle,
        participants: &[PeerId],
        local_peer_id: &PeerId,
        round_cursor: &RoundCursor,
        participant_count: u16,
        aggregate_checksum: Option<ContentId>,
        policy: &DiLoCoPolicy,
    ) {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.aggregation_timeout_ms.max(1)));
        for peer_id in participants {
            if peer_id == local_peer_id {
                continue;
            }
            let Some(timeout) = request_timeout(deadline) else {
                break;
            };
            let finalize = DiLoCoRoundFinalize {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                peer_id: local_peer_id.clone(),
                round_cursor: round_cursor.clone(),
                participant_count,
                aggregate_checksum: aggregate_checksum.clone(),
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
        round_cursor: &RoundCursor,
        policy: &DiLoCoPolicy,
    ) -> anyhow::Result<Vec<DiLoCoPeerContribution>> {
        let deadline =
            Instant::now() + Duration::from_millis(u64::from(policy.aggregation_timeout_ms.max(1)));
        let mut pending = participants
            .iter()
            .filter(|peer_id| *peer_id != local_peer_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut contributions = BTreeMap::<PeerId, DiLoCoPeerContribution>::new();

        while !pending.is_empty() {
            let mut progressed = false;
            let peers = pending.iter().cloned().collect::<Vec<_>>();
            for peer_id in peers {
                let Some(timeout) = request_timeout(deadline) else {
                    break;
                };
                let Ok(Some(snapshot)) = self.control.fetch_diloco_state_snapshot(
                    peer_id.as_str(),
                    experiment.experiment_id.clone(),
                    experiment.revision_id.clone(),
                    timeout,
                ) else {
                    continue;
                };
                let telemetry_snapshot = self.telemetry().snapshot();
                if !diloco_state_signature_acceptable(&telemetry_snapshot, &peer_id, &snapshot) {
                    continue;
                }
                let Some(manifest_id) = snapshot.latest_gradient_manifest_id.clone() else {
                    continue;
                };
                let Ok(Some(manifest)) = self.control.fetch_diloco_gradient_manifest(
                    peer_id.as_str(),
                    manifest_id,
                    timeout,
                ) else {
                    continue;
                };
                if !diloco_manifest_signature_acceptable(&telemetry_snapshot, &peer_id, &manifest) {
                    continue;
                }
                if manifest.round_cursor.round_id != round_cursor.round_id
                    || manifest.round_cursor.base_checkpoint_id != round_cursor.base_checkpoint_id
                    || manifest.round_cursor.group_id != round_cursor.group_id
                {
                    continue;
                }
                let mut chunks = Vec::with_capacity(manifest.chunk_count as usize);
                let mut complete = true;
                for chunk_index in 0..manifest.chunk_count {
                    match self.control.fetch_diloco_gradient_chunk(
                        peer_id.as_str(),
                        manifest.manifest_id.clone(),
                        chunk_index,
                        timeout,
                    ) {
                        Ok(Some(chunk)) => chunks.push(chunk),
                        Ok(None) | Err(_) => {
                            complete = false;
                            break;
                        }
                    }
                }
                if !complete {
                    continue;
                }
                let decoded_gradient = decode_pseudo_gradient(&manifest, &chunks)?;
                contributions.insert(
                    peer_id.clone(),
                    DiLoCoPeerContribution {
                        peer_id: peer_id.clone(),
                        encoded: EncodedPseudoGradient { manifest, chunks },
                        decoded_gradient,
                    },
                );
                pending.remove(&peer_id);
                progressed = true;
            }
            if pending.is_empty() || Instant::now() >= deadline {
                break;
            }
            if !progressed {
                std::thread::sleep(DILOCO_POLL_INTERVAL);
            }
        }

        Ok(contributions.into_values().collect())
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::{Cursor, ErrorKind},
        net::TcpListener,
        sync::{Mutex, OnceLock},
        thread,
    };

    use burn_p2p_workload::P2pWorkload;
    use chrono::Utc;
    use semver::Version;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ArtifactBuildSpec, AuthConfig, CapabilityEstimate, ChunkingScheme, ContentId, DatasetId,
        DatasetManifest, DatasetRegistration, DatasetView, DatasetViewId, EvalSplit,
        ExperimentDirectoryEntry, ExperimentOptInPolicy, ExperimentResourceRequirements,
        ExperimentScope, ExperimentVisibility, GenesisSpec, MainnetHandle, MetricReport,
        MetricValue, NetworkId, NodeBuilder, PatchOutcome, PatchSupport, PeerRole, PeerRoleSet,
        Precision, SupportedWorkload, SwarmAddress, TrainError, UpstreamAdapter, WindowCtx,
        WindowReport, WorkloadId,
    };

    #[derive(Clone, Debug)]
    struct NetworkScalarWorkload {
        inner_lr: f32,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct MomentumState {
        velocity: f32,
    }

    impl P2pWorkload for NetworkScalarWorkload {
        type Device = ();
        type Model = f32;
        type Batch = f32;
        type WindowStats = BTreeMap<String, MetricValue>;

        fn init_model(&self, _device: &Self::Device) -> Self::Model {
            0.0
        }

        fn benchmark(&self, _model: &Self::Model, _device: &Self::Device) -> CapabilityEstimate {
            CapabilityEstimate {
                preferred_backends: vec!["ndarray".into()],
                work_units_per_second: 32.0,
                target_window_seconds: 1,
            }
        }

        fn train_window(
            &self,
            ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
        ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
            let delta = ctx.batches.iter().copied().sum::<f32>() * self.inner_lr;
            ctx.model += delta;
            Ok(WindowReport {
                contribution: None,
                stats: BTreeMap::from([("delta".into(), MetricValue::Float(delta as f64))]),
                completed_at: Utc::now(),
            })
        }

        fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
            MetricReport {
                metrics: BTreeMap::from([("model".into(), MetricValue::Float(*model as f64))]),
                captured_at: Utc::now(),
            }
        }

        fn apply_patch(&mut self, _patch: &crate::RuntimePatch) -> PatchOutcome {
            PatchOutcome::Rejected("unsupported".into())
        }

        fn supported_patch_classes(&self) -> PatchSupport {
            PatchSupport::default()
        }

        fn runtime_device(&self) -> Self::Device {}

        fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
            Ok(DatasetRegistration {
                manifest: DatasetManifest {
                    dataset_id: DatasetId::new("scalar-dataset"),
                    source_uri: "memory://scalar-dataset".into(),
                    format: "synthetic".into(),
                    manifest_hash: ContentId::new("scalar-manifest"),
                    metadata: BTreeMap::new(),
                },
                view: DatasetView {
                    dataset_view_id: DatasetViewId::new("scalar-view"),
                    dataset_id: DatasetId::new("scalar-dataset"),
                    preprocessing_hash: ContentId::new("scalar-preprocess"),
                    tokenizer_hash: None,
                    manifest_hash: ContentId::new("scalar-manifest"),
                    metadata: BTreeMap::new(),
                },
                upstream: UpstreamAdapter::Local {
                    root: "/tmp".into(),
                },
            })
        }

        fn microshard_plan(
            &self,
            _registration: &DatasetRegistration,
        ) -> anyhow::Result<crate::MicroShardPlan> {
            unimplemented!("network DiLoCo tests inject batches directly")
        }

        fn load_batches(
            &self,
            _lease: &crate::AssignmentLease,
            _cached_microshards: &[crate::CachedMicroShard],
        ) -> anyhow::Result<Vec<Self::Batch>> {
            unimplemented!("network DiLoCo tests inject batches directly")
        }

        fn load_model_artifact(
            &self,
            _model: Self::Model,
            descriptor: &crate::ArtifactDescriptor,
            store: &FsArtifactStore,
            _device: &Self::Device,
        ) -> anyhow::Result<Self::Model> {
            Ok(serde_json::from_slice(
                &store.materialize_artifact_bytes(descriptor)?,
            )?)
        }

        fn materialize_model_artifact(
            &self,
            model: &Self::Model,
            artifact_kind: crate::ArtifactKind,
            head_id: HeadId,
            base_head_id: Option<HeadId>,
            store: &FsArtifactStore,
        ) -> anyhow::Result<crate::ArtifactDescriptor> {
            let bytes = serde_json::to_vec(model)?;
            let mut spec = ArtifactBuildSpec::new(
                artifact_kind,
                Precision::Fp32,
                ContentId::new("scalar-schema"),
                "scalar-json",
            )
            .with_head(head_id);
            if let Some(base_head_id) = base_head_id {
                spec = spec.with_base_head(base_head_id);
            }
            Ok(store.store_artifact_reader(&spec, Cursor::new(bytes), ChunkingScheme::new(8)?)?)
        }

        fn contribution_metrics(
            &self,
            report: &WindowReport<Self::WindowStats>,
        ) -> BTreeMap<String, MetricValue> {
            report.stats.clone()
        }

        fn supported_workload(&self) -> SupportedWorkload {
            SupportedWorkload {
                workload_id: WorkloadId::new("scalar-diloco-network"),
                workload_name: "Scalar DiLoCo Network".into(),
                model_program_hash: ContentId::new("scalar-program"),
                checkpoint_format_hash: ContentId::new("scalar-format"),
                supported_revision_family: ContentId::new("scalar-family"),
                resource_class: "cpu".into(),
            }
        }

        fn model_schema_hash(&self) -> ContentId {
            ContentId::new("scalar-schema")
        }
    }

    impl DiLoCoWorkload for NetworkScalarWorkload {
        fn export_parameter_pack(
            &self,
            model: &Self::Model,
        ) -> anyhow::Result<FlattenedTensorPack> {
            Ok(FlattenedTensorPack::new(
                self.model_schema_hash(),
                ContentId::new("scalar-layout"),
                vec![*model],
            ))
        }

        fn import_parameter_pack(
            &self,
            _device: &Self::Device,
            pack: &FlattenedTensorPack,
        ) -> anyhow::Result<Self::Model> {
            Ok(*pack.values.first().unwrap_or(&0.0))
        }

        fn run_inner_steps(
            &self,
            model: &Self::Model,
            batches: &[Self::Batch],
            num_inner_steps: u32,
            inner_optimizer_state: Option<&StateBlob>,
        ) -> Result<crate::DiLoCoInnerLoopReport, TrainError> {
            let mut local = *model;
            for batch in batches
                .iter()
                .copied()
                .cycle()
                .take(num_inner_steps as usize)
            {
                local += batch * self.inner_lr;
            }
            Ok(crate::DiLoCoInnerLoopReport {
                local_parameters: self
                    .export_parameter_pack(&local)
                    .map_err(|error| TrainError::new(error.to_string()))?,
                inner_optimizer_state: inner_optimizer_state.cloned(),
                steps_completed: num_inner_steps,
                metrics: BTreeMap::from([("local_model".into(), MetricValue::Float(local as f64))]),
            })
        }

        fn initialize_outer_optimizer_state(
            &self,
            _model: &Self::Model,
            _policy: &crate::OuterOptimizerPolicy,
        ) -> anyhow::Result<StateBlob> {
            StateBlob::try_new(
                "application/json",
                serde_json::to_vec(&MomentumState { velocity: 0.0 })?,
            )
            .map_err(anyhow::Error::from)
        }

        fn apply_aggregated_outer_update(
            &self,
            base: &FlattenedTensorPack,
            aggregate: &FlattenedTensorPack,
            outer_optimizer_state: &StateBlob,
            policy: &crate::OuterOptimizerPolicy,
        ) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
            let mut state: MomentumState = serde_json::from_slice(&outer_optimizer_state.bytes)?;
            let momentum = policy.momentum().unwrap_or(0.0) as f32;
            let grad = aggregate.values[0];
            state.velocity = momentum * state.velocity + grad;
            let step = match policy {
                crate::OuterOptimizerPolicy::Sgd { nesterov: true, .. } => {
                    momentum * state.velocity + grad
                }
                _ => state.velocity,
            };
            let next = base.values[0] - ((policy.learning_rate() as f32) * step);
            Ok((
                FlattenedTensorPack::new(
                    base.model_schema_hash.clone(),
                    base.layout_hash.clone(),
                    vec![next],
                ),
                StateBlob::try_new("application/json", serde_json::to_vec(&state)?)?,
            ))
        }
    }

    fn native_swarm_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        order_diloco_candidates(&policy, &PeerId::new("local-a"), &round_zero, &mut local_a)
            .expect("order local a");
        let mut local_b = base.clone();
        order_diloco_candidates(&policy, &PeerId::new("local-b"), &round_zero, &mut local_b)
            .expect("order local b");
        let mut next_round = base.clone();
        order_diloco_candidates(
            &policy,
            &PeerId::new("local-a"),
            &round_one,
            &mut next_round,
        )
        .expect("order next round");

        assert_eq!(local_a.len(), base.len());
        assert_ne!(local_a, local_b);
        assert_ne!(local_a, next_round);
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
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "burn_p2p.revision.training_protocol.policy_json".into(),
            serde_json::to_string(&TrainingProtocol::DiLoCo(policy.clone()))
                .expect("training protocol json"),
        );
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
            metadata,
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

        let mut seed = NodeBuilder::new(NetworkScalarWorkload { inner_lr: 0.5 })
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

        let mut peer = NodeBuilder::new(NetworkScalarWorkload { inner_lr: 0.25 })
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
        assert_eq!(seed_outcome.next_round_cursor.round_id.as_u64(), 1);
        assert_eq!(peer_outcome.next_round_cursor.round_id.as_u64(), 1);
        assert_eq!(seed_outcome.group_id, peer_outcome.group_id);
        assert_eq!(seed_outcome.contributions.len(), 2);
        assert_eq!(peer_outcome.contributions.len(), 2);
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

        let mut late_joiner = NodeBuilder::new(NetworkScalarWorkload { inner_lr: 0.125 })
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
            .expect("late joiner diloco round");
        assert_eq!(late_outcome.completed_round.round_id.as_u64(), 1);
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
}
