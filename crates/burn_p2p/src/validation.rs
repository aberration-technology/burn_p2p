use super::*;
use crate::metrics_runtime::{
    ValidationMetricBuildArgs, build_head_eval_report, build_metrics_announcement,
    build_reducer_cohort_metrics, build_validation_peer_window_metrics,
    persist_eval_protocol_manifest, persist_head_eval_report, persist_peer_window_metrics,
    persist_reducer_cohort_metrics,
};
use crate::runtime_support::LagAssessment;
use crate::runtime_support::{active_experiment_directory_entry, load_json};
use burn_p2p_core::MetricsLiveEventKind;
use burn_p2p_core::QuarantinePolicy;
use burn_p2p_core::RobustnessAlertSeverity;

mod candidate;
mod output;
mod robustness;

use candidate::{
    ValidationCandidate, ValidationCandidateLoadArgs, collect_validation_candidate_heads,
    fallback_best_candidate_index, load_validation_base_model, load_validation_candidate_models,
    select_validation_head,
};
use output::{
    ValidationExecution, aggregate_stats_from_updates, build_aggregate_record,
    build_reduction_certificate, build_validation_aggregate, build_validation_contribution,
    build_validation_merge_certificate, build_validation_reducer_load,
};
use robustness::{
    CandidateRobustnessOutcome, PersistedRobustnessState, ValidationRobustnessExecution,
    append_canary_escalation_alert, append_quarantine_escalation_alerts,
    append_replica_disagreement_alert, build_validation_canary_report,
    evaluate_candidate_robustness, observed_replica_agreement, persist_validation_robustness_state,
};
#[cfg(test)]
use robustness::{PeerRobustnessState, project_robustness_state};

struct ValidationPreparedState {
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    base_head_id: HeadId,
    dataset_view_id: DatasetViewId,
    merge_window: MergeWindowState,
    updates: Vec<UpdateAnnounce>,
    metrics_retention: MetricsRetentionBudget,
    robustness_policy: RobustnessPolicy,
    robustness_state: PersistedRobustnessState,
}

enum ValidationAttempt {
    Blocked(Box<ValidationRobustnessExecution>),
    Promoted(Box<ValidationExecution>),
}

impl<P> RunningNode<P> {
    /// Validates the candidates once.
    pub fn validate_candidates_once(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<ValidationOutcome>>
    where
        P: P2pWorkload,
    {
        let prepared = self.prepare_validation_state(experiment)?;
        let Some(attempt) = self.execute_validation_candidates(experiment, &prepared)? else {
            return Ok(None);
        };
        match attempt {
            ValidationAttempt::Blocked(robustness) => {
                self.persist_validation_robustness(experiment, &prepared, &robustness)?;
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
                        "validation blocked by robustness screening; no acceptable candidates"
                            .into()
                    });
                {
                    let mut snapshot = self
                        .telemetry
                        .state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.last_error = Some(reason.clone());
                }
                self.update_runtime_state(
                    NodeRuntimeState::PassiveValidator,
                    Some(SlotRuntimeState::Blocked {
                        assignment: Some(prepared.assignment.clone()),
                        reason,
                    }),
                );
                Ok(None)
            }
            ValidationAttempt::Promoted(execution) => {
                self.persist_validation_robustness(experiment, &prepared, &execution.robustness)?;
                self.publish_validation_execution(experiment, &prepared, &execution)?;

                Ok(Some(ValidationOutcome {
                    source_peer_id: execution.source_peer_id,
                    merged_head: execution.merged_head,
                    merge_certificate: execution.merge_certificate,
                    contribution: execution.contribution,
                    evaluation: execution.evaluation,
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

        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let metrics_retention = self
            .config()
            .metrics_retention
            .resolve_for_roles(&self.mainnet().roles);
        let snapshots = self.fetch_connected_snapshots(Duration::from_secs(3))?;
        let telemetry_snapshot = self.telemetry().snapshot();
        let lag_assessment = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let current_head =
            resolve_canonical_head(&storage, experiment, &snapshots)?.or_else(|| {
                latest_head_from_snapshot(telemetry_snapshot.control_plane.clone(), experiment)
            });
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

        if let Some((source_peer_id, source_head)) = current_head.as_ref()
            && !store.has_manifest(&source_head.artifact_id)
            && source_head.global_step > 0
        {
            self.sync_artifact_from_peer(source_peer_id, source_head.artifact_id.clone())?;
        }

        let base_head_id = current_head
            .as_ref()
            .map(|(_, head)| head.head_id.clone())
            .unwrap_or_else(|| HeadId::new("genesis"));
        let topology_policy =
            runtime_merge_topology_policy(&telemetry_snapshot, experiment, Some(&base_head_id));
        let topology_peers = runtime_topology_peers(&telemetry_snapshot, &local_peer_id);
        let validator_peers = runtime_validator_peers(&telemetry_snapshot, &local_peer_id);
        let validators = runtime_validators(
            &self.mainnet().roles,
            &local_peer_id,
            &validator_peers,
            topology_policy.promotion_policy.validator_quorum,
        );
        let merge_window = latest_merge_window_from_connected_snapshots(
            &telemetry_snapshot.control_plane,
            &snapshots,
            experiment,
            Some(&base_head_id),
        )
        .unwrap_or(open_runtime_merge_window(
            experiment,
            next_window_id(&storage, experiment)?,
            base_head_id.clone(),
            topology_policy,
            topology_peers,
            validators,
        )?);
        let updates = update_announces_from_connected_snapshots(
            &telemetry_snapshot.control_plane,
            &snapshots,
            experiment,
            merge_window.window_id,
            &base_head_id,
        );
        let robustness_policy =
            runtime_robustness_policy(self.config(), &telemetry_snapshot, experiment);
        let dataset_view_id =
            active_experiment_directory_entry(self.config(), &telemetry_snapshot, experiment)
                .map(|entry| entry.dataset_view_id)
                .unwrap_or_else(|| DatasetViewId::new("runtime-default"));
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
            current_head,
            base_head_id,
            dataset_view_id,
            merge_window,
            updates,
            metrics_retention,
            robustness_policy,
            robustness_state,
        })
    }

    fn execute_validation_candidates(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
    ) -> anyhow::Result<Option<ValidationAttempt>>
    where
        P: P2pWorkload,
    {
        let started_at = Utc::now();
        let candidate_heads = collect_validation_candidate_heads(
            experiment,
            &prepared.snapshots,
            &prepared.local_peer_id,
            prepared
                .current_head
                .as_ref()
                .map(|(_, head)| &head.head_id),
        );
        for (peer_id, head) in &candidate_heads {
            self.sync_artifact_from_peer(peer_id, head.artifact_id.clone())?;
        }
        let project = &mut self
            .node
            .as_mut()
            .expect("running node should retain prepared node")
            .project;
        let device = project.runtime_device();
        let merge_policy = MergePolicy::QualityWeightedEma;
        let base_model =
            load_validation_base_model(project, &prepared.current_head, &prepared.store, &device)?;
        let mut all_candidate_models = load_validation_candidate_models(
            project,
            ValidationCandidateLoadArgs {
                experiment,
                store: &prepared.store,
                device: &device,
                updates: &prepared.updates,
                current_head: &prepared.current_head,
                canary_threshold: prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
            },
            candidate_heads,
        )?;
        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let CandidateRobustnessOutcome {
            decisions,
            trust_scores,
            filtered_updates,
            accepted_weights,
        } = evaluate_candidate_robustness(&engine, prepared, &all_candidate_models, started_at);
        let mut candidate_models = all_candidate_models
            .drain(..)
            .filter_map(|mut candidate| {
                accepted_weights
                    .get(&(
                        candidate.peer_id.clone(),
                        candidate.head.artifact_id.clone(),
                    ))
                    .copied()
                    .map(|effective_weight| {
                        candidate.sample_weight *= effective_weight.max(0.01);
                        candidate
                    })
            })
            .collect::<Vec<_>>();

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
                &prepared.snapshots,
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
            prepared
                .merge_window
                .policy
                .promotion_policy
                .validator_quorum
                .max(1),
        )?;
        robustness.canary_report = Some(canary_report.clone());
        append_canary_escalation_alert(
            experiment,
            prepared,
            &mut robustness,
            &canary_report,
            started_at,
        );
        if engine.candidate_blocked_by_canary(&canary_report) {
            all_candidate_models.clear();
            candidate_models.clear();
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
        let reduction_certificate =
            build_reduction_certificate(experiment, prepared, &aggregate, &prepared.merge_window)?;
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
        let reducer_load_report =
            build_validation_reducer_load(&prepared.local_peer_id, &aggregate, &filtered_updates);
        let finished_at = Utc::now();

        all_candidate_models.clear();
        candidate_models.clear();

        Ok(Some(ValidationAttempt::Promoted(Box::new(
            ValidationExecution {
                source_peer_id,
                merged_head,
                accepted_updates: filtered_updates,
                merge_certificate,
                contribution,
                evaluation,
                aggregate,
                aggregate_artifact,
                reduction_certificate,
                reducer_load_report,
                robustness,
                started_at,
                finished_at,
            },
        ))))
    }

    fn publish_validation_execution(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<()> {
        let publish_started_at = Utc::now();
        persist_json(
            prepared
                .storage
                .scoped_receipt_path(&execution.contribution.receipt_id),
            &execution.contribution,
        )?;
        persist_json(
            prepared
                .storage
                .scoped_merge_cert_path(&execution.merge_certificate.merge_cert_id),
            &execution.merge_certificate,
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
        prepared
            .store
            .pin_artifact(&execution.aggregate_artifact.descriptor.artifact_id)?;
        prepared.store.store_prebuilt_artifact_bytes(
            &execution.aggregate_artifact.descriptor,
            &execution.aggregate_artifact.bytes,
        )?;

        self.update_runtime_state(
            NodeRuntimeState::PublishingUpdate,
            Some(SlotRuntimeState::Publishing(prepared.assignment.clone())),
        );

        let overlays = experiment.overlay_set()?;
        self.control.publish_merge_window(MergeWindowAnnouncement {
            overlay: overlays.heads.clone(),
            merge_window: prepared.merge_window.clone(),
            announced_at: Utc::now(),
        })?;
        self.control.publish_aggregate(AggregateAnnouncement {
            overlay: overlays.heads.clone(),
            aggregate: execution.aggregate.clone(),
            announced_at: Utc::now(),
        })?;
        self.control
            .publish_reduction_certificate(ReductionCertificateAnnouncement {
                overlay: overlays.heads.clone(),
                certificate: execution.reduction_certificate.clone(),
                announced_at: Utc::now(),
            })?;
        self.control.publish_reducer_load(ReducerLoadAnnouncement {
            overlay: overlays.telemetry.clone(),
            report: execution.reducer_load_report.clone(),
        })?;
        self.control.publish_artifact(
            execution.aggregate_artifact.descriptor.clone(),
            execution
                .aggregate_artifact
                .descriptor
                .chunks
                .iter()
                .map(|chunk| {
                    let start = chunk.offset_bytes as usize;
                    let end = start + chunk.length_bytes as usize;
                    ArtifactChunkPayload {
                        artifact_id: execution.aggregate_artifact.descriptor.artifact_id.clone(),
                        chunk: chunk.clone(),
                        bytes: execution.aggregate_artifact.bytes[start..end].to_vec(),
                        generated_at: Utc::now(),
                    }
                })
                .collect(),
        )?;
        self.publish_artifact_from_store(&execution.merged_head.artifact_id)?;
        self.control.publish_merge(MergeAnnouncement {
            overlay: overlays.heads.clone(),
            certificate: execution.merge_certificate.clone(),
            announced_at: Utc::now(),
        })?;
        self.control.publish_head(HeadAnnouncement {
            overlay: overlays.heads,
            provider_peer_id: Some(prepared.local_peer_id.clone()),
            head: execution.merged_head.clone(),
            announced_at: Utc::now(),
        })?;
        let publish_finished_at = Utc::now();
        let publish_latency_ms = (publish_finished_at - publish_started_at)
            .num_milliseconds()
            .max(0) as u64;
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
        let reducer_cohort_metrics = build_reducer_cohort_metrics(
            self.config(),
            experiment,
            &prepared.merge_window,
            &execution.merged_head,
            &execution.aggregate,
            &execution.reducer_load_report,
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
        self.control.publish_metrics(build_metrics_announcement(
            experiment,
            overlays.metrics,
            MetricsLiveEventKind::CatchupRefresh,
            Some(execution.merged_head.head_id.clone()),
            Some(prepared.merge_window.merge_window_id.clone()),
        ))?;
        self.set_experiment_idle_state(experiment, NodeRuntimeState::PassiveValidator);

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
                .expect("telemetry state lock should not be poisoned");
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
            metrics_retention: MetricsRetentionBudget::default(),
            robustness_policy: RobustnessPolicy::strict(),
            robustness_state: PersistedRobustnessState::default(),
        }
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
        let candidates = vec![
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
        let CandidateRobustnessOutcome {
            decisions,
            trust_scores: _,
            filtered_updates,
            accepted_weights,
        } = evaluate_candidate_robustness(&engine, &prepared, &candidates, Utc::now());

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
        let CandidateRobustnessOutcome {
            decisions,
            filtered_updates,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &[candidate], Utc::now());

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
        let candidates = vec![
            candidate("peer-a", "artifact-a", "head-a", 0.35),
            candidate("peer-b", "artifact-b", "head-b", 0.36),
            candidate("peer-c", "artifact-c", "head-c", 0.37),
        ];

        let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
        let CandidateRobustnessOutcome {
            decisions,
            filtered_updates,
            accepted_weights,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &candidates, Utc::now());

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
        let candidates = vec![ValidationCandidate {
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
        let CandidateRobustnessOutcome {
            accepted_weights,
            filtered_updates,
            ..
        } = evaluate_candidate_robustness(&engine, &prepared, &candidates, Utc::now());

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
                    aggregate_announcements: vec![AggregateAnnouncement {
                        overlay: burn_p2p_swarm::OverlayTopic::control(
                            experiment.network_id.clone(),
                        ),
                        aggregate: aggregate("reducer-a", "artifact-a"),
                        announced_at: Utc::now(),
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            ),
            (
                PeerId::new("reducer-b"),
                ControlPlaneSnapshot {
                    aggregate_announcements: vec![AggregateAnnouncement {
                        overlay: burn_p2p_swarm::OverlayTopic::control(
                            experiment.network_id.clone(),
                        ),
                        aggregate: aggregate("reducer-b", "artifact-b"),
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
}
