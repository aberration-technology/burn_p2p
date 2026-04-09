use super::*;
use crate::metrics_runtime::{
    ValidationMetricBuildArgs, build_head_eval_report, build_metrics_announcement,
    build_reducer_cohort_metrics, build_validation_peer_window_metrics,
    persist_eval_protocol_manifest, persist_head_eval_report, persist_peer_window_metrics,
    persist_reducer_cohort_metrics,
};
use crate::runtime_support::LagAssessment;
use crate::runtime_support::{
    active_experiment_directory_entry, load_json, runtime_training_peers,
    snapshots_with_local_control_plane,
};
use burn_p2p_core::MetricsLiveEventKind;
use burn_p2p_core::QuarantinePolicy;
use burn_p2p_core::RobustnessAlertSeverity;

mod candidate;
mod output;
mod robustness;

use candidate::{
    ValidationCandidate, ValidationCandidateHead, ValidationCandidateLoadArgs,
    ValidationCandidateView, collect_validation_candidate_heads, fallback_best_candidate_index,
    load_validation_base_model, load_validation_candidate_model, select_validation_head,
};
use output::{
    LocalAggregateMaterialization, ValidationExecution, aggregate_stats_from_updates,
    build_aggregate_record, build_reduction_certificate, build_validation_aggregate,
    build_validation_contribution, build_validation_merge_certificate,
    build_validation_quorum_certificate, build_validation_reducer_load,
};
use robustness::{
    CandidateRobustnessOutcome, PersistedRobustnessState, ValidationRobustnessExecution,
    append_canary_escalation_alert, append_quarantine_escalation_alerts,
    append_replica_disagreement_alert, build_validation_canary_report,
    evaluate_candidate_robustness, observed_replica_agreement, persist_validation_robustness_state,
};
#[cfg(test)]
use robustness::{PeerRobustnessState, project_robustness_state};

const VALIDATION_QUORUM_WAIT: Duration = Duration::from_secs(5);
const REDUCER_PROPOSAL_WAIT: Duration = Duration::from_secs(2);
const VALIDATION_PREPARE_REMOTE_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(750);
const VALIDATION_ARTIFACT_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const VALIDATION_PROMOTION_GRACE: Duration = Duration::from_millis(300);
const VALIDATION_COORDINATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

struct ValidationPreparedState {
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    visible_snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    base_head_id: HeadId,
    dataset_view_id: DatasetViewId,
    merge_window: MergeWindowState,
    updates: Vec<UpdateAnnounce>,
    expected_training_peer_count: usize,
    first_update_announced_at: Option<DateTime<Utc>>,
    metrics_retention: MetricsRetentionBudget,
    robustness_policy: RobustnessPolicy,
    robustness_state: PersistedRobustnessState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidationCandidateCacheKey {
    study_id: StudyId,
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    window_id: WindowId,
    base_head_id: HeadId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidationExecutionDraftKey {
    accepted_candidates: Vec<(PeerId, ArtifactId, u64, u64)>,
}

#[derive(Clone)]
struct ValidationExecutionDraft {
    key: ValidationExecutionDraftKey,
    source_peer_id: PeerId,
    merged_head: HeadDescriptor,
    evaluation: MetricReport,
    aggregate_record: AggregateArtifactRecord,
    local_aggregate_materialization: LocalAggregateMaterialization,
}

pub(crate) struct ValidationCandidateCache<M> {
    key: ValidationCandidateCacheKey,
    base_model: Option<M>,
    candidates: Vec<ValidationCandidate<M>>,
    draft: Option<ValidationExecutionDraft>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateResolutionMode {
    RequireLocalReduction,
    PreferRemoteReducerProposal,
}

struct ResolvedAggregateProposal {
    aggregate: AggregateEnvelope,
    local_aggregate_materialization: Option<LocalAggregateMaterialization>,
}

struct ValidationSnapshotObservation {
    canonical_snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    observed_merge_window: Option<MergeWindowState>,
    updates: Vec<UpdateAnnounce>,
    candidate_head_count: usize,
    base_head_id: HeadId,
}

fn effective_validator_quorum(merge_window: &MergeWindowState) -> usize {
    usize::from(merge_window.policy.promotion_policy.validator_quorum.max(1))
        .min(merge_window.validators.len().max(1))
}

fn validation_candidate_cache_key(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
) -> ValidationCandidateCacheKey {
    ValidationCandidateCacheKey {
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
    }
}

fn effective_canary_minimum_evaluator_quorum(prepared: &ValidationPreparedState) -> u16 {
    prepared
        .robustness_policy
        .validator_canary_policy
        .minimum_evaluator_quorum
        .min(effective_validator_quorum(&prepared.merge_window) as u16)
}

fn validation_execution_draft_key<M>(
    candidate_models: &[ValidationCandidateView<'_, M>],
) -> ValidationExecutionDraftKey {
    ValidationExecutionDraftKey {
        accepted_candidates: candidate_models
            .iter()
            .map(|candidate| {
                (
                    candidate.peer_id.clone(),
                    candidate.head.artifact_id.clone(),
                    candidate.sample_weight.to_bits(),
                    candidate.quality_weight.to_bits(),
                )
            })
            .collect(),
    }
}

fn canary_blocks_promotion(
    prepared: &ValidationPreparedState,
    canary_report: &CanaryEvalReport,
) -> bool {
    let policy = &prepared.robustness_policy.validator_canary_policy;
    !canary_report.accepted
        || canary_report.detected_backdoor_trigger
        || canary_report.regression_margin > policy.maximum_regression_delta
        || canary_report.evaluator_quorum < effective_canary_minimum_evaluator_quorum(prepared)
}

fn should_wait_for_candidate_settle(
    prepared: &ValidationPreparedState,
    candidate_head_count: usize,
    now: DateTime<Utc>,
    resolution_mode: AggregateResolutionMode,
) -> bool {
    if !matches!(
        resolution_mode,
        AggregateResolutionMode::RequireLocalReduction
    ) {
        return false;
    }
    if candidate_head_count == 0 {
        return false;
    }

    let expected_peer_count = prepared.expected_training_peer_count.max(1);
    let target_cohort = usize::from(prepared.merge_window.policy.target_leaf_cohort.max(1));
    let expected_candidate_count = expected_peer_count.min(target_cohort);
    if expected_candidate_count <= 1 {
        return false;
    }
    if candidate_head_count >= expected_candidate_count {
        return false;
    }
    if candidate_head_count != 1 {
        return false;
    }
    if prepared.updates.len() <= candidate_head_count {
        return false;
    }

    let settle_ms = i64::from(
        prepared
            .merge_window
            .policy
            .publish_jitter_ms
            .div_ceil(3)
            .clamp(25, 250),
    );
    let settle_anchor = prepared
        .first_update_announced_at
        .unwrap_or(prepared.merge_window.opened_at);
    now < settle_anchor + chrono::Duration::milliseconds(settle_ms)
}

fn runtime_window_validators(
    roles: &PeerRoleSet,
    local_peer_id: &PeerId,
    telemetry_snapshot: &NodeTelemetrySnapshot,
    updates: &[UpdateAnnounce],
    requested_quorum: u16,
) -> Vec<PeerId> {
    let update_peers = updates
        .iter()
        .map(|update| update.peer_id.clone())
        .collect::<BTreeSet<_>>();
    let mut validator_peers = runtime_validator_peers(telemetry_snapshot, roles, local_peer_id);
    validator_peers.retain(|peer_id| peer_id == local_peer_id || !update_peers.contains(peer_id));
    if validator_peers.is_empty() {
        validator_peers.push(local_peer_id.clone());
    }
    runtime_validators(roles, local_peer_id, &validator_peers, requested_quorum)
}

fn node_can_reduce(roles: &PeerRoleSet) -> bool {
    roles.contains(&PeerRole::Reducer)
}

fn node_can_validate(roles: &PeerRoleSet) -> bool {
    roles.contains(&PeerRole::Validator) || roles.contains(&PeerRole::Authority)
}

fn validation_idle_state_for_roles(roles: &PeerRoleSet) -> NodeRuntimeState {
    if node_can_validate(roles) {
        NodeRuntimeState::PassiveValidator
    } else {
        config::default_node_runtime_state(roles)
    }
}

fn dedicated_reducer_peers(prepared: &ValidationPreparedState) -> BTreeSet<PeerId> {
    let update_peers = prepared
        .updates
        .iter()
        .map(|update| update.peer_id.clone())
        .collect::<BTreeSet<_>>();
    prepared
        .merge_window
        .reducers
        .iter()
        .filter(|peer_id| {
            !prepared.merge_window.validators.contains(*peer_id) && !update_peers.contains(*peer_id)
        })
        .cloned()
        .collect()
}

fn has_observed_remote_proposal(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate_id: &ContentId,
) -> bool {
    snapshot
        .control_plane
        .aggregate_proposal_announcements
        .iter()
        .any(|announcement| {
            announcement.proposal.study_id == experiment.study_id
                && announcement.proposal.experiment_id == experiment.experiment_id
                && announcement.proposal.revision_id == experiment.revision_id
                && announcement.proposal.window_id == prepared.merge_window.window_id
                && announcement.proposal.base_head_id == prepared.base_head_id
                && announcement.proposal.aggregate_id == *aggregate_id
                && announcement.proposal.reducer_peer_id != prepared.local_peer_id
                && !prepared
                    .updates
                    .iter()
                    .any(|update| update.peer_id == announcement.proposal.reducer_peer_id)
        })
}

fn load_aggregate_artifact_record(
    store: &FsArtifactStore,
    artifact_id: &ArtifactId,
) -> anyhow::Result<AggregateArtifactRecord> {
    let descriptor = store.load_manifest(artifact_id)?;
    let mut bytes = Vec::with_capacity(descriptor.bytes_len as usize);
    for chunk in &descriptor.chunks {
        bytes.extend(store.load_chunk_bytes(chunk)?);
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode aggregate artifact record: {error}"))
}

fn aggregate_records_match(
    expected: &AggregateArtifactRecord,
    observed: &AggregateArtifactRecord,
) -> bool {
    let mut normalized_observed = observed.clone();
    normalized_observed.created_at = expected.created_at;
    &normalized_observed == expected
}

enum ValidationAttempt {
    Blocked(Box<ValidationRobustnessExecution>),
    Promoted(Box<ValidationExecution>),
}

fn observe_validation_snapshots(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    telemetry_snapshot: &NodeTelemetrySnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_peer_id: &PeerId,
) -> anyhow::Result<ValidationSnapshotObservation> {
    let canonical_snapshots = snapshots_with_local_control_plane(
        snapshots,
        Some(local_peer_id),
        &telemetry_snapshot.control_plane,
    );
    let current_head =
        resolve_canonical_head(storage, experiment, &canonical_snapshots)?.or_else(|| {
            latest_head_from_snapshot(telemetry_snapshot.control_plane.clone(), experiment)
        });
    let base_head_id = current_head
        .as_ref()
        .map(|(_, head)| head.head_id.clone())
        .unwrap_or_else(|| HeadId::new("genesis"));
    let observed_merge_window = latest_merge_window_from_connected_snapshots(
        &telemetry_snapshot.control_plane,
        snapshots,
        experiment,
        Some(&base_head_id),
    );
    let updates = update_announces_from_connected_snapshots(
        &telemetry_snapshot.control_plane,
        snapshots,
        experiment,
        observed_merge_window
            .as_ref()
            .map(|merge_window| merge_window.window_id)
            .unwrap_or(inferred_next_window_id(
                storage,
                experiment,
                current_head.as_ref().map(|(_, head)| head),
            )?),
        &base_head_id,
    );
    let candidate_head_count = collect_validation_candidate_heads(
        experiment,
        &canonical_snapshots,
        local_peer_id,
        current_head.as_ref().map(|(_, head)| &head.head_id),
        current_head
            .as_ref()
            .map(|(_, head)| head.global_step.saturating_add(1))
            .unwrap_or(0),
        &updates,
    )
    .len();

    Ok(ValidationSnapshotObservation {
        canonical_snapshots,
        current_head,
        observed_merge_window,
        updates,
        candidate_head_count,
        base_head_id,
    })
}

impl<P> RunningNode<P> {
    /// Validates the candidates once.
    pub fn validate_candidates_once(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<ValidationOutcome>>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        let prepared = self.prepare_validation_state(experiment)?;
        let Some(attempt) = self.execute_validation_candidates(
            experiment,
            &prepared,
            AggregateResolutionMode::PreferRemoteReducerProposal,
        )?
        else {
            return Ok(None);
        };
        match attempt {
            ValidationAttempt::Blocked(robustness) => self
                .handle_blocked_validation_attempt(experiment, &prepared, &robustness)
                .map(|_| None),
            ValidationAttempt::Promoted(execution) => {
                self.persist_validation_robustness(experiment, &prepared, &execution.robustness)?;
                self.publish_validation_execution(experiment, &prepared, &execution)
            }
        }
    }

    /// Performs the reducer aggregate-proposal operation once.
    pub fn reduce_candidates_once(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<ReducerOutcome>>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        if !node_can_reduce(&self.mainnet().roles) {
            anyhow::bail!("reducer execution requires the local Reducer role");
        }

        let prepared = self.prepare_validation_state(experiment)?;
        let Some(attempt) = self.execute_validation_candidates(
            experiment,
            &prepared,
            AggregateResolutionMode::RequireLocalReduction,
        )?
        else {
            return Ok(None);
        };

        match attempt {
            ValidationAttempt::Blocked(robustness) => self
                .handle_blocked_validation_attempt(experiment, &prepared, &robustness)
                .map(|_| None),
            ValidationAttempt::Promoted(execution) => {
                self.persist_validation_robustness(experiment, &prepared, &execution.robustness)?;
                let materialization = execution
                    .local_aggregate_materialization
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("local reduction did not produce an aggregate proposal")
                    })?;
                self.update_runtime_state(
                    NodeRuntimeState::PublishingUpdate,
                    Some(SlotRuntimeState::Publishing(prepared.assignment.clone())),
                );
                prepared.store.store_prebuilt_artifact_bytes(
                    &materialization.aggregate_artifact.descriptor,
                    &materialization.aggregate_artifact.bytes,
                )?;
                prepared
                    .store
                    .pin_artifact(&materialization.aggregate_artifact.descriptor.artifact_id)?;
                self.publish_local_aggregate_materialization(
                    experiment,
                    prepared.merge_window.clone(),
                    materialization,
                )?;
                self.set_experiment_idle_state(
                    experiment,
                    validation_idle_state_for_roles(&self.mainnet().roles),
                );
                Ok(Some(ReducerOutcome {
                    source_peer_id: execution.source_peer_id.clone(),
                    merged_head: execution.merged_head.clone(),
                    aggregate: execution.aggregate.clone(),
                    reducer_load_report: materialization.reducer_load_report.clone(),
                    evaluation: execution.evaluation.clone(),
                }))
            }
        }
    }

    fn prepare_validation_state(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<ValidationPreparedState> {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::WaitingMerge,
            Some(SlotRuntimeState::Assigned(assignment.clone())),
        );
        self.ensure_experiment_topics(experiment)?;

        let storage = self
            .config()
            .storage
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("validation requires configured storage"))?;
        let store = FsArtifactStore::new(storage.root.clone());
        store.ensure_layout()?;

        let telemetry_snapshot = self.telemetry().snapshot();
        let local_peer_id = telemetry_snapshot
            .local_peer_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let roles = self.mainnet().roles.clone();
        let metrics_retention = self.config().metrics_retention.resolve_for_roles(&roles);
        let mut snapshots = cached_connected_snapshots(&telemetry_snapshot);
        let mut lag_assessment = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let mut observation = observe_validation_snapshots(
            &storage,
            experiment,
            &telemetry_snapshot,
            &snapshots,
            &local_peer_id,
        )?;
        let should_refresh_remote = telemetry_snapshot.connected_peers > 0
            && (observation.current_head.is_none()
                || observation.candidate_head_count < observation.updates.len()
                || (observation.observed_merge_window.is_none() && observation.updates.is_empty()));
        if should_refresh_remote {
            snapshots = self.fetch_experiment_snapshots(
                experiment,
                VALIDATION_PREPARE_REMOTE_SNAPSHOT_TIMEOUT,
            )?;
            lag_assessment = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
            observation = observe_validation_snapshots(
                &storage,
                experiment,
                &telemetry_snapshot,
                &snapshots,
                &local_peer_id,
            )?;
        }
        if matches!(
            lag_assessment.state,
            LagState::LeaseBlocked | LagState::RebaseRequired
        ) {
            let reason = validation_blocked_reason(&lag_assessment);
            self.update_runtime_state(
                NodeRuntimeState::HeadSync,
                Some(SlotRuntimeState::Blocked {
                    assignment: Some(assignment.clone()),
                    reason: reason.clone(),
                }),
            );
            return Err(anyhow::anyhow!(reason));
        }

        if let Some((source_peer_id, source_head)) = observation.current_head.as_ref()
            && !store.has_manifest(&source_head.artifact_id)
            && source_head.global_step > 0
        {
            self.sync_artifact_from_peer_bounded(
                source_peer_id,
                source_head.artifact_id.clone(),
                VALIDATION_ARTIFACT_SYNC_TIMEOUT,
            )?;
        }

        let current_head = observation.current_head;
        let base_head_id = observation.base_head_id;
        let topology_policy =
            runtime_merge_topology_policy(&telemetry_snapshot, experiment, Some(&base_head_id));
        let topology_peers = runtime_topology_peers(&telemetry_snapshot, &roles, &local_peer_id);
        let updates = observation.updates;
        let merge_window = match observation.observed_merge_window {
            Some(mut merge_window) => {
                merge_window.validators = runtime_window_validators(
                    &roles,
                    &local_peer_id,
                    &telemetry_snapshot,
                    &updates,
                    merge_window.policy.promotion_policy.validator_quorum,
                );
                merge_window
            }
            None => {
                let validators = runtime_window_validators(
                    &roles,
                    &local_peer_id,
                    &telemetry_snapshot,
                    &updates,
                    topology_policy.promotion_policy.validator_quorum,
                );
                open_runtime_merge_window(
                    experiment,
                    inferred_next_window_id(
                        &storage,
                        experiment,
                        current_head.as_ref().map(|(_, head)| head),
                    )?,
                    base_head_id.clone(),
                    topology_policy,
                    topology_peers,
                    validators,
                )?
            }
        };
        let robustness_policy =
            runtime_robustness_policy(self.config(), &telemetry_snapshot, experiment);
        let dataset_view_id =
            active_experiment_directory_entry(self.config(), &telemetry_snapshot, experiment)
                .map(|entry| entry.dataset_view_id)
                .unwrap_or_else(|| DatasetViewId::new("runtime-default"));
        let expected_training_peer_count =
            runtime_training_peers(&telemetry_snapshot, &roles, &local_peer_id)
                .len()
                .max(1);
        let first_update_announced_at = updates.iter().map(|update| update.announced_at).min();
        let robustness_state = load_json::<PersistedRobustnessState>(
            storage.scoped_robustness_state_path(experiment),
        )?
        .unwrap_or_default();

        Ok(ValidationPreparedState {
            assignment,
            storage,
            store,
            local_peer_id,
            snapshots,
            visible_snapshots: observation.canonical_snapshots,
            current_head,
            base_head_id,
            dataset_view_id,
            merge_window,
            updates,
            expected_training_peer_count,
            first_update_announced_at,
            metrics_retention,
            robustness_policy,
            robustness_state,
        })
    }

    fn prime_validation_candidate_cache(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        candidate_heads: &[ValidationCandidateHead],
    ) -> anyhow::Result<()>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        let cache_key = validation_candidate_cache_key(experiment, prepared);
        let reset_cache = self
            .validation_cache
            .as_ref()
            .and_then(|cache| cache.downcast_ref::<ValidationCandidateCache<P::Model>>())
            .is_none_or(|cache| cache.key != cache_key);
        if reset_cache {
            self.validation_cache = Some(Box::new(ValidationCandidateCache::<P::Model> {
                key: cache_key.clone(),
                base_model: None,
                candidates: Vec::new(),
                draft: None,
            }));
        }
        {
            let node = self
                .node
                .as_mut()
                .expect("running node should retain prepared node");
            let project = &mut node.project;
            let device = project.runtime_device();
            let cache = self
                .validation_cache
                .as_mut()
                .and_then(|cache| cache.downcast_mut::<ValidationCandidateCache<P::Model>>())
                .expect("validation candidate cache should be initialized");
            if cache.base_model.is_none() {
                cache.base_model = Some(load_validation_base_model(
                    project,
                    &prepared.current_head,
                    &prepared.store,
                    &device,
                )?);
            }
        }
        for candidate in candidate_heads {
            let already_cached = self
                .validation_cache
                .as_ref()
                .and_then(|cache| cache.downcast_ref::<ValidationCandidateCache<P::Model>>())
                .expect("validation candidate cache should be initialized")
                .candidates
                .iter()
                .any(|cached| {
                    cached.peer_id == candidate.origin_peer_id
                        && cached.head.artifact_id == candidate.head.artifact_id
                });
            if already_cached {
                continue;
            }
            self.wait_for_artifact_from_peers(
                &candidate.provider_peer_ids,
                &candidate.head.artifact_id,
                VALIDATION_ARTIFACT_SYNC_TIMEOUT,
            )?;
            let loaded = {
                let node = self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node");
                let project = &mut node.project;
                let device = project.runtime_device();
                load_validation_candidate_model(
                    project,
                    ValidationCandidateLoadArgs {
                        experiment,
                        store: &prepared.store,
                        device: &device,
                        current_head: &prepared.current_head,
                        canary_threshold: prepared
                            .robustness_policy
                            .validator_canary_policy
                            .maximum_regression_delta,
                    },
                    ValidationCandidateHead {
                        origin_peer_id: candidate.origin_peer_id.clone(),
                        provider_peer_ids: candidate.provider_peer_ids.clone(),
                        head: candidate.head.clone(),
                        update: candidate.update.clone(),
                    },
                )?
            };
            self.validation_cache
                .as_mut()
                .and_then(|cache| cache.downcast_mut::<ValidationCandidateCache<P::Model>>())
                .expect("validation candidate cache should be initialized")
                .candidates
                .push(loaded);
        }

        Ok(())
    }

    fn execute_validation_candidates(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        resolution_mode: AggregateResolutionMode,
    ) -> anyhow::Result<Option<ValidationAttempt>>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        let started_at = Utc::now();
        let candidate_heads = collect_validation_candidate_heads(
            experiment,
            &prepared.visible_snapshots,
            &prepared.local_peer_id,
            prepared
                .current_head
                .as_ref()
                .map(|(_, head)| &head.head_id),
            prepared
                .current_head
                .as_ref()
                .map(|(_, head)| head.global_step.saturating_add(1))
                .unwrap_or(0),
            &prepared.updates,
        );
        self.prime_validation_candidate_cache(experiment, prepared, &candidate_heads)?;
        if should_wait_for_candidate_settle(
            prepared,
            candidate_heads.len(),
            started_at,
            resolution_mode,
        ) {
            return Ok(None);
        }
        let cache_key = validation_candidate_cache_key(experiment, prepared);
        let (node_opt, validation_cache_opt) = (&mut self.node, &self.validation_cache);
        let cache = validation_cache_opt
            .as_ref()
            .and_then(|cache| cache.downcast_ref::<ValidationCandidateCache<P::Model>>())
            .filter(|cache| cache.key == cache_key)
            .ok_or_else(|| anyhow::anyhow!("validation candidate cache was not initialized"))?;
        let all_candidate_models = candidate_heads
            .iter()
            .map(|candidate| {
                cache
                    .candidates
                    .iter()
                    .find(|cached| {
                        cached.peer_id == candidate.origin_peer_id
                            && cached.head.artifact_id == candidate.head.artifact_id
                    })
                    .map(ValidationCandidateView::from)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "validation candidate cache missing artifact {} for peer {}",
                            candidate.head.artifact_id.as_str(),
                            candidate.origin_peer_id.as_str()
                        )
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let base_model = cache
            .base_model
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("validation base model cache was not initialized"))?;
        let node = node_opt
            .as_mut()
            .expect("running node should retain prepared node");
        let project = &mut node.project;
        let merge_policy = MergePolicy::QualityWeightedEma;
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let CandidateRobustnessOutcome {
            decisions,
            trust_scores,
            mut filtered_updates,
            accepted_weights,
        } = evaluate_candidate_robustness(&engine, prepared, &all_candidate_models, started_at);
        let mut candidate_models = all_candidate_models
            .iter()
            .filter_map(|candidate| {
                accepted_weights
                    .get(&(
                        candidate.peer_id.clone(),
                        candidate.head.artifact_id.clone(),
                    ))
                    .copied()
                    .map(|effective_weight| ValidationCandidateView {
                        peer_id: candidate.peer_id,
                        head: candidate.head,
                        update: candidate.update,
                        evaluation: candidate.evaluation,
                        canary_report: candidate.canary_report,
                        sample_weight: candidate.sample_weight * effective_weight.max(0.01),
                        quality_weight: candidate.quality_weight,
                        model: candidate.model,
                    })
            })
            .collect::<Vec<_>>();
        candidate_models.sort_by(|left, right| {
            left.peer_id
                .cmp(right.peer_id)
                .then(left.head.head_id.cmp(&right.head.head_id))
                .then(left.head.artifact_id.cmp(&right.head.artifact_id))
        });
        filtered_updates.sort_by(|left, right| {
            left.peer_id
                .cmp(&right.peer_id)
                .then(left.delta_artifact_id.cmp(&right.delta_artifact_id))
                .then_with(|| left.receipt_ids.cmp(&right.receipt_ids))
        });

        let cohort_report = engine.summarize_cohort(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            prepared.merge_window.window_id,
            &decisions,
            Utc::now(),
        );
        let mut robustness = ValidationRobustnessExecution {
            decisions,
            trust_scores,
            cohort_report,
            canary_report: None,
            replica_agreement: observed_replica_agreement(
                &prepared.visible_snapshots,
                experiment,
                &prepared.merge_window,
                &prepared.base_head_id,
            ),
        };
        append_quarantine_escalation_alerts(experiment, prepared, &mut robustness, started_at);
        append_replica_disagreement_alert(experiment, prepared, &mut robustness, started_at);
        if prepared
            .robustness_policy
            .screening_policy
            .require_replica_agreement
            && prepared
                .robustness_policy
                .escalation_policy
                .pause_on_replica_disagreement
            && matches!(robustness.replica_agreement, Some(score) if score < 0.999)
        {
            return Ok(Some(ValidationAttempt::Blocked(Box::new(robustness))));
        }

        if candidate_models.is_empty() {
            return Ok(Some(ValidationAttempt::Blocked(Box::new(robustness))));
        };
        let Some(fallback_best_index) = fallback_best_candidate_index(&candidate_models) else {
            return Ok(Some(ValidationAttempt::Blocked(Box::new(robustness))));
        };
        let draft_key = validation_execution_draft_key(&candidate_models);
        let cached_draft = self
            .validation_cache
            .as_ref()
            .and_then(|cache| cache.downcast_ref::<ValidationCandidateCache<P::Model>>())
            .and_then(|cache| cache.draft.as_ref())
            .filter(|draft| draft.key == draft_key)
            .cloned();

        let (
            source_peer_id,
            merged_head,
            evaluation,
            aggregate_record,
            local_aggregate_materialization,
            cache_draft,
        ) = if let Some(draft) = cached_draft {
            (
                draft.source_peer_id,
                draft.merged_head,
                draft.evaluation,
                draft.aggregate_record,
                draft.local_aggregate_materialization,
                None,
            )
        } else {
            let (source_peer_id, merged_head, evaluation) = select_validation_head(
                project,
                experiment,
                &prepared.store,
                &prepared.current_head,
                &prepared.base_head_id,
                prepared.merge_window.window_id,
                base_model,
                &candidate_models,
                fallback_best_index,
                merge_policy.clone(),
                &prepared.local_peer_id,
            )?;
            let canary_report = build_validation_canary_report(
                experiment,
                &prepared.current_head,
                &merged_head,
                &evaluation,
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                effective_validator_quorum(&prepared.merge_window) as u16,
            )?;
            robustness.canary_report = Some(canary_report.clone());
            append_canary_escalation_alert(
                experiment,
                prepared,
                &mut robustness,
                &canary_report,
                started_at,
            );
            if canary_blocks_promotion(prepared, &canary_report) {
                return Ok(Some(ValidationAttempt::Blocked(Box::new(robustness))));
            }

            let aggregate_id = ContentId::derive(&(
                experiment.study_id.as_str(),
                experiment.experiment_id.as_str(),
                experiment.revision_id.as_str(),
                prepared.merge_window.window_id.0,
                prepared.base_head_id.as_str(),
                merged_head.head_id.as_str(),
            ))?;
            let aggregate_stats = aggregate_stats_from_updates(&filtered_updates);
            let aggregate_record = build_aggregate_record(
                experiment,
                &prepared.base_head_id,
                &aggregate_id,
                merge_policy.clone(),
                &evaluation,
                &filtered_updates,
                &aggregate_stats,
                prepared.merge_window.opened_at,
            );
            let aggregate_artifact =
                materialize_aggregate_artifact_bytes(&aggregate_record, ChunkingScheme::default())?;
            let aggregate = build_validation_aggregate(
                experiment,
                prepared,
                &aggregate_id,
                &aggregate_artifact,
                &aggregate_stats,
                &filtered_updates,
            );
            let local_aggregate_materialization = LocalAggregateMaterialization {
                reducer_load_report: build_validation_reducer_load(
                    &prepared.local_peer_id,
                    &aggregate,
                    &filtered_updates,
                ),
                aggregate,
                aggregate_artifact,
            };
            let draft = ValidationExecutionDraft {
                key: draft_key,
                source_peer_id: source_peer_id.clone(),
                merged_head: merged_head.clone(),
                evaluation: evaluation.clone(),
                aggregate_record: aggregate_record.clone(),
                local_aggregate_materialization: local_aggregate_materialization.clone(),
            };
            (
                source_peer_id,
                merged_head,
                evaluation,
                aggregate_record,
                local_aggregate_materialization,
                Some(draft),
            )
        };
        if let Some(draft) = cache_draft
            && let Some(cache) = self
                .validation_cache
                .as_mut()
                .and_then(|cache| cache.downcast_mut::<ValidationCandidateCache<P::Model>>())
        {
            cache.draft = Some(draft);
        }
        if robustness.canary_report.is_none() {
            let canary_report = build_validation_canary_report(
                experiment,
                &prepared.current_head,
                &merged_head,
                &evaluation,
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                effective_validator_quorum(&prepared.merge_window) as u16,
            )?;
            robustness.canary_report = Some(canary_report.clone());
            append_canary_escalation_alert(
                experiment,
                prepared,
                &mut robustness,
                &canary_report,
                started_at,
            );
            if canary_blocks_promotion(prepared, &canary_report) {
                return Ok(Some(ValidationAttempt::Blocked(Box::new(robustness))));
            }
        }
        let resolved_aggregate = self.resolve_aggregate_proposal(
            experiment,
            prepared,
            &aggregate_record,
            local_aggregate_materialization,
            resolution_mode,
        )?;
        let reduction_certificate = build_reduction_certificate(
            experiment,
            prepared,
            &resolved_aggregate.aggregate,
            &prepared.merge_window,
        )?;
        let contribution =
            build_validation_contribution(experiment, &source_peer_id, &merged_head, &evaluation);
        let merge_certificate = build_validation_merge_certificate(
            experiment,
            &prepared.local_peer_id,
            &prepared.base_head_id,
            &merged_head,
            merge_policy,
            &contribution,
            &filtered_updates,
        );
        let finished_at = Utc::now();

        Ok(Some(ValidationAttempt::Promoted(Box::new(
            ValidationExecution {
                source_peer_id,
                merged_head,
                accepted_updates: filtered_updates,
                merge_certificate,
                contribution,
                evaluation,
                aggregate: resolved_aggregate.aggregate,
                local_aggregate_materialization: resolved_aggregate.local_aggregate_materialization,
                reduction_certificate,
                robustness,
                started_at,
                finished_at,
            },
        ))))
    }

    fn resolve_aggregate_proposal(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        expected_record: &AggregateArtifactRecord,
        local_aggregate_materialization: LocalAggregateMaterialization,
        resolution_mode: AggregateResolutionMode,
    ) -> anyhow::Result<ResolvedAggregateProposal> {
        if matches!(
            resolution_mode,
            AggregateResolutionMode::RequireLocalReduction
        ) || node_can_reduce(&self.mainnet().roles)
        {
            return Ok(ResolvedAggregateProposal {
                aggregate: local_aggregate_materialization.aggregate.clone(),
                local_aggregate_materialization: Some(local_aggregate_materialization),
            });
        }

        let dedicated_reducers = dedicated_reducer_peers(prepared);
        let observed_remote_proposal = has_observed_remote_proposal(
            &self.telemetry().snapshot(),
            experiment,
            prepared,
            &local_aggregate_materialization.aggregate.aggregate_id,
        );
        if dedicated_reducers.is_empty() && !observed_remote_proposal {
            return Ok(ResolvedAggregateProposal {
                aggregate: local_aggregate_materialization.aggregate.clone(),
                local_aggregate_materialization: Some(local_aggregate_materialization),
            });
        }

        if let Some(aggregate) = self.wait_for_reducer_aggregate_proposal(
            experiment,
            prepared,
            &dedicated_reducers,
            expected_record,
            &local_aggregate_materialization.aggregate,
        )? {
            if aggregate.aggregate_artifact_id
                == local_aggregate_materialization
                    .aggregate_artifact
                    .descriptor
                    .artifact_id
            {
                prepared.store.store_prebuilt_artifact_bytes(
                    &local_aggregate_materialization
                        .aggregate_artifact
                        .descriptor,
                    &local_aggregate_materialization.aggregate_artifact.bytes,
                )?;
            }
            return Ok(ResolvedAggregateProposal {
                aggregate,
                local_aggregate_materialization: None,
            });
        }

        if !dedicated_reducers.is_empty() || observed_remote_proposal {
            let still_observed_remote_proposal = has_observed_remote_proposal(
                &self.telemetry().snapshot(),
                experiment,
                prepared,
                &local_aggregate_materialization.aggregate.aggregate_id,
            );
            if !still_observed_remote_proposal {
                return Ok(ResolvedAggregateProposal {
                    aggregate: local_aggregate_materialization.aggregate.clone(),
                    local_aggregate_materialization: Some(local_aggregate_materialization),
                });
            }
            anyhow::bail!(
                "dedicated reducer proposal for aggregate {} was observed but could not be materialized locally",
                local_aggregate_materialization
                    .aggregate
                    .aggregate_id
                    .as_str()
            );
        }

        Ok(ResolvedAggregateProposal {
            aggregate: local_aggregate_materialization.aggregate.clone(),
            local_aggregate_materialization: Some(local_aggregate_materialization),
        })
    }

    fn wait_for_reducer_aggregate_proposal(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        dedicated_reducers: &BTreeSet<PeerId>,
        expected_record: &AggregateArtifactRecord,
        expected_aggregate: &AggregateEnvelope,
    ) -> anyhow::Result<Option<AggregateEnvelope>> {
        let overlay = experiment.overlay_set()?.heads;
        let deadline = Instant::now() + REDUCER_PROPOSAL_WAIT;
        let update_peers = prepared
            .updates
            .iter()
            .map(|update| update.peer_id.clone())
            .collect::<BTreeSet<_>>();
        let mut last_failure = None::<String>;
        let mut observed_any_matching_proposal = false;

        loop {
            let local_snapshot = self.telemetry().snapshot().control_plane;
            let remote_snapshots =
                self.fetch_experiment_snapshots(experiment, Duration::from_millis(250))?;
            let mut proposals = local_snapshot
                .aggregate_proposal_announcements
                .iter()
                .chain(
                    remote_snapshots
                        .iter()
                        .flat_map(|(_, snapshot)| snapshot.aggregate_proposal_announcements.iter()),
                )
                .filter(|announcement| {
                    announcement.overlay == overlay
                        && announcement.proposal.study_id == experiment.study_id
                        && announcement.proposal.experiment_id == experiment.experiment_id
                        && announcement.proposal.revision_id == experiment.revision_id
                        && announcement.proposal.window_id == prepared.merge_window.window_id
                        && announcement.proposal.base_head_id == prepared.base_head_id
                        && announcement.proposal.aggregate_id == expected_aggregate.aggregate_id
                        && announcement.proposal.reducer_peer_id != prepared.local_peer_id
                        && !update_peers.contains(&announcement.proposal.reducer_peer_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            proposals.sort_by(|left, right| {
                dedicated_reducers
                    .contains(&right.proposal.reducer_peer_id)
                    .cmp(&dedicated_reducers.contains(&left.proposal.reducer_peer_id))
                    .then_with(|| right.announced_at.cmp(&left.announced_at))
            });

            for announcement in proposals {
                let proposal = announcement.proposal;
                observed_any_matching_proposal = true;
                if proposal.aggregate_artifact_id == expected_aggregate.aggregate_artifact_id {
                    return Ok(Some(proposal));
                }
                let provider_candidates = dedupe_peer_ids(
                    std::iter::once(proposal.reducer_peer_id.clone())
                        .chain(proposal.providers.iter().cloned()),
                );
                for provider in provider_candidates {
                    let sync_result = self.sync_artifact_from_peer_bounded(
                        &provider,
                        proposal.aggregate_artifact_id.clone(),
                        VALIDATION_ARTIFACT_SYNC_TIMEOUT,
                    );
                    if let Err(error) = sync_result {
                        last_failure = Some(format!(
                            "sync from provider {} failed for aggregate artifact {}: {error}",
                            provider.as_str(),
                            proposal.aggregate_artifact_id.as_str(),
                        ));
                        continue;
                    }
                    if proposal.aggregate_artifact_id == expected_aggregate.aggregate_artifact_id {
                        prepared
                            .store
                            .pin_artifact(&proposal.aggregate_artifact_id)?;
                        return Ok(Some(proposal));
                    }
                    let observed_record = load_aggregate_artifact_record(
                        &prepared.store,
                        &proposal.aggregate_artifact_id,
                    )?;
                    if aggregate_records_match(expected_record, &observed_record) {
                        prepared
                            .store
                            .pin_artifact(&proposal.aggregate_artifact_id)?;
                        return Ok(Some(proposal));
                    }
                    last_failure = Some(format!(
                        "provider {} returned a semantically different aggregate artifact {}; expected_artifact_id={}; expected_inputs={:?}; observed_inputs={:?}; expected_weight={}; observed_weight={}; expected_metrics={:?}; observed_metrics={:?}",
                        provider.as_str(),
                        proposal.aggregate_artifact_id.as_str(),
                        expected_aggregate.aggregate_artifact_id.as_str(),
                        expected_record
                            .inputs
                            .iter()
                            .map(|input| {
                                (
                                    input.peer_id.as_str().to_owned(),
                                    input.artifact_id.as_str().to_owned(),
                                    input.sample_weight,
                                    input.quality_weight,
                                )
                            })
                            .collect::<Vec<_>>(),
                        observed_record
                            .inputs
                            .iter()
                            .map(|input| {
                                (
                                    input.peer_id.as_str().to_owned(),
                                    input.artifact_id.as_str().to_owned(),
                                    input.sample_weight,
                                    input.quality_weight,
                                )
                            })
                            .collect::<Vec<_>>(),
                        expected_record.merge_plan.total_weight,
                        observed_record.merge_plan.total_weight,
                        expected_record.merge_plan.aggregated_numeric_metrics,
                        observed_record.merge_plan.aggregated_numeric_metrics,
                    ));
                }
            }

            if Instant::now() >= deadline {
                if let Some(last_failure) = last_failure {
                    return Err(anyhow::anyhow!(last_failure));
                }
                if observed_any_matching_proposal {
                    return Err(anyhow::anyhow!(
                        "observed reducer proposal {} but no provider yielded a usable artifact before deadline",
                        expected_aggregate.aggregate_id.as_str()
                    ));
                }
                return Ok(None);
            }
            std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
        }
    }

    fn publish_local_aggregate_materialization(
        &mut self,
        experiment: &ExperimentHandle,
        merge_window: MergeWindowState,
        materialization: &LocalAggregateMaterialization,
    ) -> anyhow::Result<()> {
        let overlays = experiment.overlay_set()?;
        let artifact = &materialization.aggregate_artifact;
        let descriptor = artifact.descriptor.clone();
        self.control.publish_merge_window(MergeWindowAnnouncement {
            overlay: overlays.heads.clone(),
            merge_window,
            announced_at: Utc::now(),
        })?;
        self.control.publish_artifact(
            descriptor.clone(),
            descriptor
                .chunks
                .iter()
                .map(|chunk| {
                    let start = chunk.offset_bytes as usize;
                    let end = start + chunk.length_bytes as usize;
                    ArtifactChunkPayload {
                        artifact_id: descriptor.artifact_id.clone(),
                        chunk: chunk.clone(),
                        bytes: artifact.bytes[start..end].to_vec(),
                        generated_at: Utc::now(),
                    }
                })
                .collect(),
        )?;
        self.control
            .publish_aggregate_proposal(AggregateProposalAnnouncement {
                overlay: overlays.heads.clone(),
                proposal: materialization.aggregate.clone(),
                announced_at: Utc::now(),
            })?;
        self.control.publish_reducer_load(ReducerLoadAnnouncement {
            overlay: overlays.telemetry,
            report: materialization.reducer_load_report.clone(),
        })?;
        Ok(())
    }

    fn publish_validation_execution(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<Option<ValidationOutcome>> {
        persist_json(
            prepared
                .storage
                .scoped_receipt_path(&execution.contribution.receipt_id),
            &execution.contribution,
        )?;

        self.update_runtime_state(
            NodeRuntimeState::PublishingUpdate,
            Some(SlotRuntimeState::Publishing(prepared.assignment.clone())),
        );

        let overlays = experiment.overlay_set()?;
        if let Some(materialization) = execution.local_aggregate_materialization.as_ref() {
            prepared.store.store_prebuilt_artifact_bytes(
                &materialization.aggregate_artifact.descriptor,
                &materialization.aggregate_artifact.bytes,
            )?;
            prepared
                .store
                .pin_artifact(&materialization.aggregate_artifact.descriptor.artifact_id)?;
            self.publish_local_aggregate_materialization(
                experiment,
                prepared.merge_window.clone(),
                materialization,
            )?;
        } else {
            prepared
                .store
                .pin_artifact(&execution.aggregate.aggregate_artifact_id)?;
            self.publish_artifact_from_store(&execution.aggregate.aggregate_artifact_id)?;
        }
        self.control
            .publish_reduction_certificate(ReductionCertificateAnnouncement {
                overlay: overlays.heads.clone(),
                certificate: execution.reduction_certificate.clone(),
                announced_at: Utc::now(),
            })?;
        let coordination =
            self.wait_for_validation_coordination(experiment, prepared, execution)?;
        let attesters = coordination.attesters;
        let reduction_ids = coordination.reduction_ids;
        let quorum = effective_validator_quorum(&prepared.merge_window);
        if coordination.merge_announced
            || coordination.quorum_announced
            || attesters.len() >= quorum
        {
            persist_window_id(
                &prepared.storage,
                experiment,
                prepared.merge_window.window_id,
            )?;
            persist_head_state(&prepared.storage, experiment, &execution.merged_head)?;
            persist_json(
                prepared
                    .storage
                    .scoped_head_path(&execution.merged_head.head_id),
                &execution.merged_head,
            )?;
            prepared.store.pin_head(&execution.merged_head.head_id)?;
            prepared
                .store
                .pin_artifact(&execution.merged_head.artifact_id)?;
        }
        let mut publish_latency_ms = 0;
        let mut promoted = false;
        if !coordination.merge_announced && attesters.len() >= quorum {
            let local_rank = attesters
                .iter()
                .position(|peer_id| peer_id == &prepared.local_peer_id);
            if let Some(local_rank) = local_rank {
                let grace_ms = VALIDATION_PROMOTION_GRACE.as_millis() as u64 * local_rank as u64;
                let promotion_deadline = Instant::now() + Duration::from_millis(grace_ms);
                while Instant::now() < promotion_deadline {
                    std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
                    let observed =
                        self.observe_validation_coordination(experiment, prepared, execution)?;
                    if observed.merge_announced {
                        break;
                    }
                }

                let observed =
                    self.observe_validation_coordination(experiment, prepared, execution)?;
                if !observed.merge_announced {
                    let publish_started_at = Utc::now();
                    if !observed.quorum_announced {
                        self.control
                            .publish_validation_quorum(ValidationQuorumAnnouncement {
                                overlay: overlays.heads.clone(),
                                certificate: build_validation_quorum_certificate(
                                    experiment,
                                    prepared,
                                    &execution.aggregate,
                                    &execution.merged_head,
                                    &attesters,
                                    &reduction_ids,
                                )?,
                                announced_at: Utc::now(),
                            })?;
                    }
                    persist_json(
                        prepared
                            .storage
                            .scoped_merge_cert_path(&execution.merge_certificate.merge_cert_id),
                        &execution.merge_certificate,
                    )?;

                    if !observed.aggregate_proposal_announced {
                        self.control
                            .publish_aggregate_proposal(AggregateProposalAnnouncement {
                                overlay: overlays.heads.clone(),
                                proposal: execution.aggregate.clone(),
                                announced_at: Utc::now(),
                            })?;
                        self.publish_artifact_from_store(
                            &execution.aggregate.aggregate_artifact_id,
                        )?;
                    }
                    self.publish_artifact_from_store(&execution.merged_head.artifact_id)?;
                    self.control.publish_merge(MergeAnnouncement {
                        overlay: overlays.heads.clone(),
                        certificate: execution.merge_certificate.clone(),
                        announced_at: Utc::now(),
                    })?;
                    self.control.publish_head(HeadAnnouncement {
                        overlay: overlays.heads.clone(),
                        provider_peer_id: Some(prepared.local_peer_id.clone()),
                        head: execution.merged_head.clone(),
                        announced_at: Utc::now(),
                    })?;
                    let publish_finished_at = Utc::now();
                    publish_latency_ms = (publish_finished_at - publish_started_at)
                        .num_milliseconds()
                        .max(0) as u64;
                    promoted = true;
                }
            }
        }
        let head_lag_steps = self.telemetry().snapshot().head_lag_steps;
        let peer_window_metrics = build_validation_peer_window_metrics(ValidationMetricBuildArgs {
            config: self.config(),
            experiment,
            local_peer_id: &prepared.local_peer_id,
            merge_window: &prepared.merge_window,
            base_head_id: &prepared.base_head_id,
            updates: &execution.accepted_updates,
            evaluation: &execution.evaluation,
            started_at: execution.started_at,
            finished_at: execution.finished_at,
            publish_latency_ms,
            head_lag_at_start: head_lag_steps,
            head_lag_at_finish: head_lag_steps,
        });
        persist_peer_window_metrics(
            &prepared.storage,
            experiment,
            &peer_window_metrics,
            prepared.metrics_retention,
        )?;
        if let Some(materialization) = execution.local_aggregate_materialization.as_ref() {
            let reducer_cohort_metrics = build_reducer_cohort_metrics(
                self.config(),
                experiment,
                &prepared.merge_window,
                &execution.merged_head,
                &execution.aggregate,
                &materialization.reducer_load_report,
                &execution.accepted_updates,
            )?;
            let mut reducer_cohort_metrics = reducer_cohort_metrics;
            reducer_cohort_metrics.replica_agreement = execution.robustness.replica_agreement;
            if matches!(
                execution.robustness.replica_agreement,
                Some(score) if score < 0.999
            ) {
                reducer_cohort_metrics.status = ReducerCohortStatus::Inconsistent;
            }
            persist_reducer_cohort_metrics(
                &prepared.storage,
                experiment,
                &reducer_cohort_metrics,
                prepared.metrics_retention,
            )?;
        }
        let (head_eval_report, eval_protocol_manifest) = build_head_eval_report(
            self.config(),
            experiment,
            &execution.merged_head,
            &execution.evaluation,
            execution.started_at,
            execution.finished_at,
            &prepared.local_peer_id,
        )?;
        persist_head_eval_report(
            &prepared.storage,
            experiment,
            &head_eval_report,
            prepared.metrics_retention,
        )?;
        persist_eval_protocol_manifest(
            &prepared.storage,
            experiment,
            &eval_protocol_manifest,
            prepared.metrics_retention,
        )?;
        if promoted {
            self.control.publish_metrics(build_metrics_announcement(
                experiment,
                overlays.metrics,
                MetricsLiveEventKind::CatchupRefresh,
                Some(execution.merged_head.head_id.clone()),
                Some(prepared.merge_window.merge_window_id.clone()),
            ))?;
        }
        self.set_experiment_idle_state(
            experiment,
            validation_idle_state_for_roles(&self.mainnet().roles),
        );

        Ok(promoted.then(|| ValidationOutcome {
            source_peer_id: execution.source_peer_id.clone(),
            merged_head: execution.merged_head.clone(),
            merge_certificate: execution.merge_certificate.clone(),
            contribution: execution.contribution.clone(),
            evaluation: execution.evaluation.clone(),
        }))
    }

    fn handle_blocked_validation_attempt(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        robustness: &ValidationRobustnessExecution,
    ) -> anyhow::Result<()> {
        self.persist_validation_robustness(experiment, prepared, robustness)?;
        let reason = robustness
            .canary_report
            .as_ref()
            .map(|report| {
                format!(
                    "validation blocked by robustness canary: regression margin {:.4}",
                    report.regression_margin
                )
            })
            .unwrap_or_else(|| {
                "validation blocked by robustness screening; no acceptable candidates".into()
            });
        {
            let mut snapshot = self
                .telemetry
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            snapshot.last_error = Some(reason.clone());
        }
        self.update_runtime_state(
            validation_idle_state_for_roles(&self.mainnet().roles),
            Some(SlotRuntimeState::Blocked {
                assignment: Some(prepared.assignment.clone()),
                reason,
            }),
        );
        Ok(())
    }

    fn persist_validation_robustness(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        robustness: &ValidationRobustnessExecution,
    ) -> anyhow::Result<()> {
        let persisted_trust_scores = persist_validation_robustness_state(
            &prepared.storage,
            experiment,
            &prepared.robustness_state,
            &robustness.decisions,
            &prepared.robustness_policy.quarantine_policy,
            prepared.merge_window.window_id,
        )?;
        let trust_scores = if persisted_trust_scores.is_empty() {
            robustness.trust_scores.clone()
        } else {
            persisted_trust_scores
        };

        persist_json(
            prepared
                .storage
                .scoped_cohort_robustness_report_path(experiment, &prepared.merge_window.window_id),
            &robustness.cohort_report,
        )?;
        persist_json(
            prepared.storage.scoped_trust_scores_path(experiment),
            &trust_scores,
        )?;
        if let Some(canary_report) = robustness.canary_report.as_ref() {
            persist_json(
                prepared
                    .storage
                    .scoped_canary_eval_report_path(experiment, &canary_report.candidate_head_id),
                canary_report,
            )?;
        }

        {
            let telemetry = self.telemetry();
            let mut snapshot = telemetry
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            snapshot.set_robustness_state(
                prepared.robustness_policy.clone(),
                robustness.cohort_report.clone(),
                trust_scores.clone(),
                robustness.canary_report.clone(),
            );
            if let Some(storage) = self.config().storage.as_ref()
                && let Err(error) =
                    crate::runtime_support::persist_runtime_security_state(storage, &snapshot)
            {
                snapshot.last_error = Some(format!(
                    "failed to persist robustness telemetry state: {error}"
                ));
            }
        }

        Ok(())
    }
}

fn validation_blocked_reason(lag_assessment: &LagAssessment) -> String {
    match lag_assessment.state {
        LagState::LeaseBlocked => format!(
            "validation blocked: local node is {} head steps behind canonical head",
            lag_assessment.head_lag_steps
        ),
        LagState::RebaseRequired => format!(
            "validation blocked: full rebase required after falling {} head steps behind",
            lag_assessment.head_lag_steps
        ),
        _ => unreachable!("non-blocking lag state matched blocking branch"),
    }
}

fn reduction_attestations_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
) -> BTreeMap<PeerId, ContentId> {
    snapshot
        .reduction_certificate_announcements
        .iter()
        .filter(|announcement| {
            announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.aggregate_id == *aggregate_id
        })
        .map(|announcement| {
            (
                announcement.certificate.validator.clone(),
                announcement.certificate.reduction_id.clone(),
            )
        })
        .collect()
}

fn aggregate_proposal_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
) -> bool {
    snapshot
        .aggregate_proposal_announcements
        .iter()
        .any(|announcement| {
            announcement.overlay == *overlay
                && announcement.proposal.study_id == experiment.study_id
                && announcement.proposal.experiment_id == experiment.experiment_id
                && announcement.proposal.revision_id == experiment.revision_id
                && announcement.proposal.aggregate_id == *aggregate_id
        })
}

fn merge_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    merged_head_id: &HeadId,
) -> bool {
    snapshot.merge_announcements.iter().any(|announcement| {
        announcement.overlay == *overlay
            && announcement.certificate.study_id == experiment.study_id
            && announcement.certificate.experiment_id == experiment.experiment_id
            && announcement.certificate.revision_id == experiment.revision_id
            && announcement.certificate.merged_head_id == *merged_head_id
    })
}

fn validation_quorum_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &HeadId,
) -> bool {
    snapshot
        .validation_quorum_announcements
        .iter()
        .any(|announcement| {
            announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.aggregate_id == *aggregate_id
                && announcement.certificate.merged_head_id == *merged_head_id
        })
}

fn merge_certificate_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    merged_head_id: &HeadId,
) -> Option<MergeCertificate> {
    snapshot
        .merge_announcements
        .iter()
        .find_map(|announcement| {
            (announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.merged_head_id == *merged_head_id)
                .then(|| announcement.certificate.clone())
        })
}

fn collect_validation_coordination_from_snapshots<'a>(
    snapshots: impl IntoIterator<Item = &'a ControlPlaneSnapshot>,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &HeadId,
    local_attestation: Option<(PeerId, ContentId)>,
) -> ValidationCoordinationState {
    let mut attestations = BTreeMap::new();
    if let Some((peer_id, reduction_id)) = local_attestation {
        attestations.insert(peer_id, reduction_id);
    }
    let mut aggregate_proposal_announced = false;
    let mut quorum_announced = false;
    let mut merge_announced = false;
    let mut merge_certificate = None;
    for snapshot in snapshots {
        attestations.extend(reduction_attestations_from_snapshot(
            snapshot,
            overlay,
            experiment,
            aggregate_id,
        ));
        aggregate_proposal_announced |=
            aggregate_proposal_announced_in_snapshot(snapshot, overlay, experiment, aggregate_id);
        quorum_announced |= validation_quorum_announced_in_snapshot(
            snapshot,
            overlay,
            experiment,
            aggregate_id,
            merged_head_id,
        );
        merge_announced |=
            merge_announced_in_snapshot(snapshot, overlay, experiment, merged_head_id);
        if merge_certificate.is_none() {
            merge_certificate =
                merge_certificate_from_snapshot(snapshot, overlay, experiment, merged_head_id);
        }
    }
    ValidationCoordinationState {
        attesters: attestations.keys().cloned().collect(),
        reduction_ids: attestations.into_values().collect(),
        aggregate_proposal_announced,
        quorum_announced,
        merge_announced,
        merge_certificate,
    }
}

fn merge_promoted_validation_outcome(
    promoted: Option<ValidationOutcome>,
    outcome: Option<ValidationOutcome>,
) -> anyhow::Result<Option<ValidationOutcome>> {
    let Some(outcome) = outcome else {
        return Ok(promoted);
    };
    if let Some(existing) = &promoted {
        anyhow::ensure!(
            existing.merged_head.head_id == outcome.merged_head.head_id,
            "validators promoted different merged heads for the same aggregate proposal",
        );
        Ok(promoted)
    } else {
        Ok(Some(outcome))
    }
}

impl<P> RunningNode<P> {
    /// Observes validation coordination for one aggregate and merged head across the local and
    /// currently reachable experiment peers.
    pub fn observe_validation_coordination_for_head(
        &self,
        experiment: &ExperimentHandle,
        aggregate_id: &ContentId,
        merged_head_id: &HeadId,
    ) -> anyhow::Result<ValidationCoordinationState>
    where
        P: P2pWorkload,
    {
        let overlay = experiment.overlay_set()?.heads;
        let local_snapshot = self.telemetry().snapshot().control_plane;
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot),
            &overlay,
            experiment,
            aggregate_id,
            merged_head_id,
            None,
        ))
    }

    /// Drives validator execution until the local node has made visible progress or the
    /// aggregate settles.
    pub fn drive_validation_until_local_progress(
        &mut self,
        experiment: &ExperimentHandle,
        aggregate_id: &ContentId,
        merged_head_id: &HeadId,
        timeout: Duration,
    ) -> anyhow::Result<ValidationDriveOutcome>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("validation driver missing local peer id"))?;
        let deadline = Instant::now() + timeout;
        let mut attempts = 0usize;
        let mut promoted = None;
        let mut coordination = self.observe_validation_coordination_for_head(
            experiment,
            aggregate_id,
            merged_head_id,
        )?;
        while Instant::now() < deadline {
            if promoted.is_some()
                || coordination.quorum_announced
                || coordination.merge_announced
                || coordination.attesters.contains(&local_peer_id)
            {
                break;
            }
            attempts += 1;
            let next = self.validate_candidates_once(experiment)?;
            promoted = merge_promoted_validation_outcome(promoted, next)?;
            coordination = self.observe_validation_coordination_for_head(
                experiment,
                aggregate_id,
                merged_head_id,
            )?;
            if promoted.is_none()
                && !coordination.quorum_announced
                && !coordination.merge_announced
                && !coordination.attesters.contains(&local_peer_id)
            {
                std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
            }
        }
        Ok(ValidationDriveOutcome {
            attempts,
            promoted,
            coordination,
        })
    }

    fn observe_validation_coordination(
        &self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<ValidationCoordinationState> {
        let overlay = experiment.overlay_set()?.heads;
        let local_snapshot = self.telemetry().snapshot().control_plane;
        let remote_snapshots =
            self.fetch_experiment_snapshots(experiment, Duration::from_millis(250))?;
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot)
                .chain(remote_snapshots.iter().map(|(_, snapshot)| snapshot)),
            &overlay,
            experiment,
            &execution.aggregate.aggregate_id,
            &execution.merged_head.head_id,
            Some((
                prepared.local_peer_id.clone(),
                execution.reduction_certificate.reduction_id.clone(),
            )),
        ))
    }

    fn wait_for_validation_coordination(
        &self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<ValidationCoordinationState> {
        let quorum = effective_validator_quorum(&prepared.merge_window);
        let deadline = Instant::now() + VALIDATION_QUORUM_WAIT;
        loop {
            let observed = self.observe_validation_coordination(experiment, prepared, execution)?;
            if observed.settled(quorum) || Instant::now() >= deadline {
                return Ok(observed);
            }
            std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric_report(loss: f64) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([(String::from("loss"), MetricValue::Float(loss))]),
            captured_at: Utc::now(),
        }
    }

    fn prepared_state() -> ValidationPreparedState {
        let root = std::env::temp_dir().join(format!(
            "burn-p2p-validation-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&root).expect("create temp root");
        let storage = StorageConfig::new(root);
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        ValidationPreparedState {
            assignment: SlotAssignmentState::new(
                experiment.study_id.clone(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
            ),
            storage: storage.clone(),
            store: FsArtifactStore::new(storage.root.clone()),
            local_peer_id: PeerId::new("validator-a"),
            snapshots: Vec::new(),
            visible_snapshots: Vec::new(),
            current_head: Some((
                PeerId::new("source-a"),
                HeadDescriptor {
                    head_id: HeadId::new("head-base"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-base"),
                    parent_head_id: None,
                    global_step: 3,
                    created_at: Utc::now(),
                    metrics: metric_report(0.4).metrics,
                },
            )),
            base_head_id: HeadId::new("head-base"),
            dataset_view_id: DatasetViewId::new("view-a"),
            merge_window: open_runtime_merge_window(
                &experiment,
                WindowId(4),
                HeadId::new("head-base"),
                MergeTopologyPolicy::default(),
                vec![PeerId::new("reducer-a")],
                vec![PeerId::new("validator-a")],
            )
            .expect("merge window"),
            updates: vec![
                UpdateAnnounce {
                    peer_id: PeerId::new("peer-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(4),
                    base_head_id: HeadId::new("head-base"),
                    lease_id: Some(LeaseId::new("lease-good")),
                    delta_artifact_id: ArtifactId::new("artifact-good"),
                    sample_weight: 16.0,
                    quality_weight: 1.0,
                    norm_stats: UpdateNormStats {
                        l2_norm: 1.0,
                        max_abs: 1.0,
                        clipped: false,
                        non_finite_tensors: 0,
                    },
                    feature_sketch: None,
                    receipt_root: ContentId::new("receipt-root-good"),
                    receipt_ids: vec![ContributionReceiptId::new("receipt-good")],
                    providers: vec![PeerId::new("peer-good")],
                    announced_at: Utc::now(),
                },
                UpdateAnnounce {
                    peer_id: PeerId::new("peer-replay"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(4),
                    base_head_id: HeadId::new("head-base"),
                    lease_id: Some(LeaseId::new("lease-replay")),
                    delta_artifact_id: ArtifactId::new("artifact-replay"),
                    sample_weight: 16.0,
                    quality_weight: 1.0,
                    norm_stats: UpdateNormStats {
                        l2_norm: 1.0,
                        max_abs: 1.0,
                        clipped: false,
                        non_finite_tensors: 0,
                    },
                    feature_sketch: None,
                    receipt_root: ContentId::new("receipt-root-good"),
                    receipt_ids: vec![ContributionReceiptId::new("receipt-good")],
                    providers: vec![PeerId::new("peer-replay")],
                    announced_at: Utc::now(),
                },
            ],
            expected_training_peer_count: 2,
            first_update_announced_at: Some(Utc::now()),
            metrics_retention: MetricsRetentionBudget::default(),
            robustness_policy: RobustnessPolicy::strict(),
            robustness_state: PersistedRobustnessState::default(),
        }
    }

    #[test]
    fn candidate_settle_waits_for_expected_training_peers_within_grace() {
        let mut prepared = prepared_state();
        prepared.expected_training_peer_count = 4;
        prepared.merge_window.policy.publish_jitter_ms = 750;
        let now = Utc::now();
        prepared.first_update_announced_at = Some(now);

        assert!(should_wait_for_candidate_settle(
            &prepared,
            1,
            now,
            AggregateResolutionMode::RequireLocalReduction,
        ));
        assert!(!should_wait_for_candidate_settle(
            &prepared,
            2,
            now + chrono::Duration::milliseconds(50),
            AggregateResolutionMode::RequireLocalReduction,
        ));
        assert!(!should_wait_for_candidate_settle(
            &prepared,
            1,
            now + chrono::Duration::milliseconds(300),
            AggregateResolutionMode::RequireLocalReduction,
        ));
    }

    #[test]
    fn candidate_settle_stops_waiting_after_publish_jitter_grace() {
        let mut prepared = prepared_state();
        prepared.expected_training_peer_count = 5;
        prepared.merge_window.policy.publish_jitter_ms = 500;
        let now = Utc::now();
        prepared.first_update_announced_at = Some(now);

        assert!(!should_wait_for_candidate_settle(
            &prepared,
            1,
            now + chrono::Duration::milliseconds(600),
            AggregateResolutionMode::RequireLocalReduction
        ));
    }

    #[test]
    fn candidate_settle_only_applies_to_reducer_reduction_pass() {
        let mut prepared = prepared_state();
        prepared.expected_training_peer_count = 4;
        prepared.merge_window.policy.publish_jitter_ms = 750;
        let now = Utc::now();
        prepared.first_update_announced_at = Some(now);

        assert!(!should_wait_for_candidate_settle(
            &prepared,
            1,
            now,
            AggregateResolutionMode::PreferRemoteReducerProposal,
        ));
    }

    #[test]
    fn candidate_robustness_rejects_replay_and_keeps_clean_update() {
        let prepared = prepared_state();
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let candidates = [
            ValidationCandidate {
                peer_id: PeerId::new("peer-good"),
                head: HeadDescriptor {
                    head_id: HeadId::new("head-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-good"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.35).metrics.clone(),
                },
                update: prepared.updates[0].clone(),
                evaluation: metric_report(0.35),
                canary_report: build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new("head-good"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new("artifact-good"),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(0.35).metrics.clone(),
                    },
                    &metric_report(0.35),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                model: (),
            },
            ValidationCandidate {
                peer_id: PeerId::new("peer-replay"),
                head: HeadDescriptor {
                    head_id: HeadId::new("head-replay"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-replay"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.36).metrics.clone(),
                },
                update: prepared.updates[1].clone(),
                evaluation: metric_report(0.36),
                canary_report: build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new("head-replay"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new("artifact-replay"),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(0.36).metrics.clone(),
                    },
                    &metric_report(0.36),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                model: (),
            },
        ];

        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let candidate_views = candidates
            .iter()
            .map(ValidationCandidateView::from)
            .collect::<Vec<_>>();
        let CandidateRobustnessOutcome {
            decisions,
            trust_scores: _,
            filtered_updates,
            accepted_weights,
        } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

        assert_eq!(filtered_updates.len(), 1);
        assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-good"));
        assert_eq!(
            decisions
                .iter()
                .find(|decision| decision.peer_id == PeerId::new("peer-replay"))
                .and_then(|decision| decision.rejection_reason.clone()),
            Some(RejectionReason::Replay)
        );
        assert!(
            accepted_weights
                .contains_key(&(PeerId::new("peer-good"), ArtifactId::new("artifact-good")))
        );
    }

    #[test]
    fn candidate_robustness_allows_peer_after_inactive_quarantine_expires() {
        let mut prepared = prepared_state();
        prepared.merge_window.window_id = WindowId(7);
        prepared
            .robustness_policy
            .quarantine_policy
            .quarantine_duration_windows = 2;
        prepared.robustness_state = PersistedRobustnessState {
            peers: BTreeMap::from([(
                PeerId::new("peer-good"),
                PeerRobustnessState {
                    trust_score: -3.0,
                    consecutive_rejections: 2,
                    quarantined: true,
                    last_rejection_reason: Some(RejectionReason::Replay),
                    updated_at: Some(Utc::now()),
                    quarantine_started_window: Some(WindowId(4)),
                    last_quarantine_window: Some(WindowId(4)),
                    ban_recommended: false,
                },
            )]),
        };
        prepared.updates[0].window_id = WindowId(7);
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let candidate = ValidationCandidate {
            peer_id: PeerId::new("peer-good"),
            head: HeadDescriptor {
                head_id: HeadId::new("head-good"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-good"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 4,
                created_at: Utc::now(),
                metrics: metric_report(0.35).metrics.clone(),
            },
            update: prepared.updates[0].clone(),
            evaluation: metric_report(0.35),
            canary_report: build_validation_canary_report(
                &experiment,
                &prepared.current_head,
                &HeadDescriptor {
                    head_id: HeadId::new("head-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-good"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.35).metrics.clone(),
                },
                &metric_report(0.35),
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                2,
            )
            .expect("canary"),
            sample_weight: 16.0,
            quality_weight: 1.0,
            model: (),
        };

        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let candidate_view = ValidationCandidateView::from(&candidate);
        let CandidateRobustnessOutcome {
            decisions,
            filtered_updates,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &[candidate_view], Utc::now());

        assert_eq!(filtered_updates.len(), 1);
        assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-good"));
        assert!(
            decisions
                .iter()
                .find(|decision| decision.peer_id == PeerId::new("peer-good"))
                .is_some_and(|decision| decision.accepted)
        );
    }

    #[test]
    fn candidate_robustness_caps_surviving_updates_to_maximum_cohort_size() {
        let mut prepared = prepared_state();
        prepared
            .robustness_policy
            .aggregation_policy
            .maximum_cohort_size = 1;
        prepared.updates = vec![
            UpdateAnnounce {
                peer_id: PeerId::new("peer-a"),
                study_id: StudyId::new("study-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                window_id: WindowId(4),
                base_head_id: HeadId::new("head-base"),
                lease_id: Some(LeaseId::new("lease-a")),
                delta_artifact_id: ArtifactId::new("artifact-a"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                norm_stats: UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new("receipt-root-a"),
                receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
                providers: vec![PeerId::new("peer-a")],
                announced_at: Utc::now(),
            },
            UpdateAnnounce {
                peer_id: PeerId::new("peer-b"),
                study_id: StudyId::new("study-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                window_id: WindowId(4),
                base_head_id: HeadId::new("head-base"),
                lease_id: Some(LeaseId::new("lease-b")),
                delta_artifact_id: ArtifactId::new("artifact-b"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                norm_stats: UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new("receipt-root-b"),
                receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
                providers: vec![PeerId::new("peer-b")],
                announced_at: Utc::now(),
            },
            UpdateAnnounce {
                peer_id: PeerId::new("peer-c"),
                study_id: StudyId::new("study-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                window_id: WindowId(4),
                base_head_id: HeadId::new("head-base"),
                lease_id: Some(LeaseId::new("lease-c")),
                delta_artifact_id: ArtifactId::new("artifact-c"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                norm_stats: UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new("receipt-root-c"),
                receipt_ids: vec![ContributionReceiptId::new("receipt-c")],
                providers: vec![PeerId::new("peer-c")],
                announced_at: Utc::now(),
            },
        ];
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let candidate =
            |peer_id: &str, artifact_id: &str, head_id: &str, loss: f64| ValidationCandidate {
                peer_id: PeerId::new(peer_id),
                head: HeadDescriptor {
                    head_id: HeadId::new(head_id),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new(artifact_id),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(loss).metrics.clone(),
                },
                update: prepared
                    .updates
                    .iter()
                    .find(|update| update.peer_id == PeerId::new(peer_id))
                    .cloned()
                    .expect("candidate update"),
                evaluation: metric_report(loss),
                canary_report: build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new(head_id),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new(artifact_id),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(loss).metrics.clone(),
                    },
                    &metric_report(loss),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                model: (),
            };
        let candidates = [
            candidate("peer-a", "artifact-a", "head-a", 0.35),
            candidate("peer-b", "artifact-b", "head-b", 0.36),
            candidate("peer-c", "artifact-c", "head-c", 0.37),
        ];

        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let candidate_views = candidates
            .iter()
            .map(ValidationCandidateView::from)
            .collect::<Vec<_>>();
        let CandidateRobustnessOutcome {
            decisions,
            filtered_updates,
            accepted_weights,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

        assert_eq!(filtered_updates.len(), 1);
        assert_eq!(accepted_weights.len(), 1);
        assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-a"));
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| decision.accepted)
                .count(),
            1
        );
    }

    #[test]
    fn candidate_robustness_caps_browser_contribution_weight() {
        let mut prepared = prepared_state();
        prepared
            .robustness_policy
            .aggregation_policy
            .browser_contribution_cap = 0.25;
        prepared.snapshots = vec![(
            PeerId::new("peer-good"),
            ControlPlaneSnapshot {
                auth_announcements: vec![PeerAuthAnnouncement {
                    peer_id: PeerId::new("peer-good"),
                    envelope: PeerAuthEnvelope {
                        peer_id: PeerId::new("peer-good"),
                        certificate: NodeCertificate::new(
                            semver::Version::new(0, 1, 0),
                            NodeCertificateClaims {
                                network_id: NetworkId::new("net-a"),
                                project_family_id: ProjectFamilyId::new("family-a"),
                                release_train_hash: ContentId::new("train-a"),
                                target_artifact_hash: ContentId::new("artifact-browser"),
                                peer_id: PeerId::new("peer-good"),
                                peer_public_key_hex: "001122".into(),
                                principal_id: PrincipalId::new("principal-browser"),
                                provider: AuthProvider::Static {
                                    authority: "lab-browser".into(),
                                },
                                granted_roles: PeerRoleSet::new([PeerRole::BrowserTrainerWgpu]),
                                experiment_scopes: BTreeSet::from([
                                    ExperimentScope::Connect,
                                    ExperimentScope::Train {
                                        experiment_id: ExperimentId::new("exp-a"),
                                    },
                                ]),
                                client_policy_hash: None,
                                not_before: Utc::now(),
                                not_after: Utc::now() + chrono::Duration::minutes(10),
                                serial: 1,
                                revocation_epoch: RevocationEpoch(1),
                            },
                            burn_p2p_core::SignatureMetadata {
                                signer: PeerId::new("authority-browser"),
                                key_id: "authority-key".into(),
                                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                                signed_at: Utc::now(),
                                signature_hex: "deadbeef".into(),
                            },
                        )
                        .expect("node certificate"),
                        client_manifest_id: Some(ContentId::new("manifest-browser")),
                        requested_scopes: BTreeSet::from([ExperimentScope::Train {
                            experiment_id: ExperimentId::new("exp-a"),
                        }]),
                        nonce_hash: ContentId::new("nonce-browser"),
                        challenge_signature_hex: "feedbead".into(),
                        presented_at: Utc::now(),
                    },
                    announced_at: Utc::now(),
                }],
                ..ControlPlaneSnapshot::default()
            },
        )];
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let candidates = [ValidationCandidate {
            peer_id: PeerId::new("peer-good"),
            head: HeadDescriptor {
                head_id: HeadId::new("head-good"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-good"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 4,
                created_at: Utc::now(),
                metrics: metric_report(0.35).metrics.clone(),
            },
            update: prepared.updates[0].clone(),
            evaluation: metric_report(0.35),
            canary_report: build_validation_canary_report(
                &experiment,
                &prepared.current_head,
                &HeadDescriptor {
                    head_id: HeadId::new("head-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-good"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.35).metrics.clone(),
                },
                &metric_report(0.35),
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                2,
            )
            .expect("canary"),
            sample_weight: 16.0,
            quality_weight: 1.0,
            model: (),
        }];

        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let candidate_views = candidates
            .iter()
            .map(ValidationCandidateView::from)
            .collect::<Vec<_>>();
        let CandidateRobustnessOutcome {
            accepted_weights,
            filtered_updates,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

        let weight = accepted_weights
            .get(&(PeerId::new("peer-good"), ArtifactId::new("artifact-good")))
            .copied()
            .expect("accepted browser weight");
        assert_eq!(weight, 0.25);
        assert_eq!(filtered_updates[0].sample_weight, 16.0 * 0.25);
    }

    #[test]
    fn quarantine_escalation_emits_peer_and_fraction_alerts() {
        let prepared = prepared_state();
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let mut robustness = ValidationRobustnessExecution {
            decisions: vec![RobustnessDecision {
                peer_id: PeerId::new("peer-suspicious"),
                accepted: false,
                hard_rejected: true,
                downweighted: false,
                quarantined: true,
                rejection_reason: Some(RejectionReason::Replay),
                screen_score: 5.0,
                effective_weight: 0.0,
                effective_norm: 0.0,
                trust_score: Some(TrustScore {
                    peer_id: PeerId::new("peer-suspicious"),
                    score: -3.0,
                    reducer_eligible: false,
                    validator_eligible: false,
                    quarantined: true,
                    ban_recommended: false,
                    updated_at: Utc::now(),
                }),
            }],
            trust_scores: vec![TrustScore {
                peer_id: PeerId::new("peer-suspicious"),
                score: -3.0,
                reducer_eligible: false,
                validator_eligible: false,
                quarantined: true,
                ban_recommended: false,
                updated_at: Utc::now(),
            }],
            cohort_report: engine.summarize_cohort(
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                prepared.merge_window.window_id,
                &[],
                Utc::now(),
            ),
            canary_report: None,
            replica_agreement: None,
        };

        append_quarantine_escalation_alerts(&experiment, &prepared, &mut robustness, Utc::now());

        assert!(robustness.cohort_report.alerts.iter().any(|alert| {
            alert.peer_id == Some(PeerId::new("peer-suspicious"))
                && alert.reason == RejectionReason::QuarantinedPrincipal
                && alert.severity == RobustnessAlertSeverity::Warn
        }));
        assert!(robustness.cohort_report.alerts.iter().any(|alert| {
            alert.peer_id.is_none()
                && alert.reason == RejectionReason::QuarantinedPrincipal
                && alert.severity == RobustnessAlertSeverity::Critical
        }));
    }

    #[test]
    fn projected_robustness_state_expires_inactive_quarantine() {
        let previous = PersistedRobustnessState {
            peers: BTreeMap::from([(
                PeerId::new("peer-a"),
                PeerRobustnessState {
                    trust_score: -3.0,
                    consecutive_rejections: 3,
                    quarantined: true,
                    last_rejection_reason: Some(RejectionReason::Replay),
                    updated_at: Some(Utc::now()),
                    quarantine_started_window: Some(WindowId(1)),
                    last_quarantine_window: Some(WindowId(1)),
                    ban_recommended: false,
                },
            )]),
        };

        let (next, trust_scores) =
            project_robustness_state(&previous, &[], &QuarantinePolicy::default(), WindowId(5));
        let peer = next.peers.get(&PeerId::new("peer-a")).expect("peer state");

        assert!(trust_scores.is_empty());
        assert!(!peer.quarantined);
        assert_eq!(peer.consecutive_rejections, 0);
        assert_eq!(peer.last_rejection_reason, None);
        assert!(peer.quarantine_started_window.is_none());
        assert!(peer.last_quarantine_window.is_none());
        assert!(!peer.ban_recommended);
    }

    #[test]
    fn quarantine_escalation_emits_ban_recommendation_alert_once_due() {
        let mut prepared = prepared_state();
        prepared.merge_window.window_id = WindowId(9);
        prepared
            .robustness_policy
            .quarantine_policy
            .ban_after_quarantine_windows = 4;
        prepared.robustness_state = PersistedRobustnessState {
            peers: BTreeMap::from([(
                PeerId::new("peer-repeat"),
                PeerRobustnessState {
                    trust_score: -4.5,
                    consecutive_rejections: 4,
                    quarantined: true,
                    last_rejection_reason: Some(RejectionReason::Replay),
                    updated_at: Some(Utc::now()),
                    quarantine_started_window: Some(WindowId(1)),
                    last_quarantine_window: Some(WindowId(9)),
                    ban_recommended: false,
                },
            )]),
        };
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let mut robustness = ValidationRobustnessExecution {
            decisions: Vec::new(),
            trust_scores: Vec::new(),
            cohort_report: engine.summarize_cohort(
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                prepared.merge_window.window_id,
                &[],
                Utc::now(),
            ),
            canary_report: None,
            replica_agreement: None,
        };

        append_quarantine_escalation_alerts(&experiment, &prepared, &mut robustness, Utc::now());

        assert!(robustness.cohort_report.alerts.iter().any(|alert| {
            alert.peer_id == Some(PeerId::new("peer-repeat"))
                && alert.reason == RejectionReason::QuarantinedPrincipal
                && alert.severity == RobustnessAlertSeverity::Critical
                && alert.message.contains("recommend operator ban")
        }));
    }

    #[test]
    fn canary_escalation_emits_critical_alert_when_promotion_pauses() {
        let prepared = prepared_state();
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let canary_report = build_validation_canary_report(
            &experiment,
            &prepared.current_head,
            &HeadDescriptor {
                head_id: HeadId::new("head-bad"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-bad"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 5,
                created_at: Utc::now(),
                metrics: metric_report(0.9).metrics.clone(),
            },
            &metric_report(0.9),
            prepared
                .robustness_policy
                .validator_canary_policy
                .maximum_regression_delta,
            1,
        )
        .expect("canary");
        let mut robustness = ValidationRobustnessExecution {
            decisions: Vec::new(),
            trust_scores: Vec::new(),
            cohort_report: engine.summarize_cohort(
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                prepared.merge_window.window_id,
                &[],
                Utc::now(),
            ),
            canary_report: Some(canary_report.clone()),
            replica_agreement: None,
        };

        append_canary_escalation_alert(
            &experiment,
            &prepared,
            &mut robustness,
            &canary_report,
            Utc::now(),
        );

        assert!(robustness.cohort_report.alerts.iter().any(|alert| {
            alert.reason == RejectionReason::CanaryRegression
                && alert.severity == RobustnessAlertSeverity::Critical
        }));
    }

    #[test]
    fn replica_disagreement_from_reducer_snapshots_emits_alert() {
        let mut prepared = prepared_state();
        prepared.merge_window.reducers = vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")];
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let aggregate = |peer_id: &str, artifact_id: &str| AggregateEnvelope {
            aggregate_id: ContentId::new(format!("aggregate-{artifact_id}")),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: prepared.merge_window.window_id,
            base_head_id: prepared.base_head_id.clone(),
            aggregate_artifact_id: ArtifactId::new(artifact_id),
            tier: AggregateTier::RootCandidate,
            reducer_peer_id: PeerId::new(peer_id),
            contributor_peers: vec![PeerId::new("peer-good")],
            child_aggregate_ids: Vec::new(),
            stats: AggregateStats {
                accepted_updates: 1,
                duplicate_updates: 0,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 1.0,
                sum_quality_weight: 1.0,
                sum_weighted_delta_norm: 1.0,
                max_update_norm: 1.0,
                accepted_sample_coverage: 1.0,
            },
            providers: vec![PeerId::new(peer_id)],
            published_at: Utc::now(),
        };
        prepared.snapshots = vec![
            (
                PeerId::new("reducer-a"),
                ControlPlaneSnapshot {
                    aggregate_proposal_announcements: vec![AggregateProposalAnnouncement {
                        overlay: burn_p2p_swarm::OverlayTopic::control(
                            experiment.network_id.clone(),
                        ),
                        proposal: aggregate("reducer-a", "artifact-a"),
                        announced_at: Utc::now(),
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            ),
            (
                PeerId::new("reducer-b"),
                ControlPlaneSnapshot {
                    aggregate_proposal_announcements: vec![AggregateProposalAnnouncement {
                        overlay: burn_p2p_swarm::OverlayTopic::control(
                            experiment.network_id.clone(),
                        ),
                        proposal: aggregate("reducer-b", "artifact-b"),
                        announced_at: Utc::now(),
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            ),
        ];
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let mut robustness = ValidationRobustnessExecution {
            decisions: Vec::new(),
            trust_scores: Vec::new(),
            cohort_report: engine.summarize_cohort(
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                prepared.merge_window.window_id,
                &[],
                Utc::now(),
            ),
            canary_report: None,
            replica_agreement: observed_replica_agreement(
                &prepared.snapshots,
                &experiment,
                &prepared.merge_window,
                &prepared.base_head_id,
            ),
        };

        append_replica_disagreement_alert(&experiment, &prepared, &mut robustness, Utc::now());

        assert_eq!(robustness.replica_agreement, Some(0.5));
        assert!(robustness.cohort_report.alerts.iter().any(|alert| {
            alert.reason == RejectionReason::ReplicaDisagreement
                && alert.severity == RobustnessAlertSeverity::Critical
        }));
    }

    #[test]
    fn canary_report_flags_regression_above_threshold() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let current_head = Some((
            PeerId::new("peer-base"),
            HeadDescriptor {
                head_id: HeadId::new("head-base"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-base"),
                parent_head_id: None,
                global_step: 1,
                created_at: Utc::now(),
                metrics: metric_report(0.2).metrics,
            },
        ));
        let report = build_validation_canary_report(
            &experiment,
            &current_head,
            &HeadDescriptor {
                head_id: HeadId::new("head-candidate"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-candidate"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 2,
                created_at: Utc::now(),
                metrics: metric_report(0.5).metrics.clone(),
            },
            &metric_report(0.5),
            0.05,
            2,
        )
        .expect("report");

        assert!(!report.accepted);
        assert!(report.regression_margin > 0.05);
    }

    #[test]
    fn merge_certificate_ids_are_validator_scoped() {
        let prepared = prepared_state();
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let merged_head = HeadDescriptor {
            head_id: HeadId::new("head-merged"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-merged"),
            parent_head_id: Some(prepared.base_head_id.clone()),
            global_step: 9,
            created_at: Utc::now(),
            metrics: metric_report(0.1).metrics,
        };
        let contribution = build_validation_contribution(
            &experiment,
            &PeerId::new("trainer-a"),
            &merged_head,
            &metric_report(0.1),
        );

        let first = build_validation_merge_certificate(
            &experiment,
            &PeerId::new("validator-a"),
            &prepared.base_head_id,
            &merged_head,
            MergePolicy::QualityWeightedEma,
            &contribution,
            &[],
        );
        let second = build_validation_merge_certificate(
            &experiment,
            &PeerId::new("validator-b"),
            &prepared.base_head_id,
            &merged_head,
            MergePolicy::QualityWeightedEma,
            &contribution,
            &[],
        );

        assert_ne!(first.merge_cert_id, second.merge_cert_id);
        assert_eq!(
            first.contribution_receipts,
            vec![contribution.receipt_id.clone()]
        );
    }
}
