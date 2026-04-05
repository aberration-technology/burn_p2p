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

struct ValidationCandidate<M> {
    peer_id: PeerId,
    head: HeadDescriptor,
    evaluation: MetricReport,
    canary_report: CanaryEvalReport,
    sample_weight: f64,
    quality_weight: f64,
    model: M,
}

struct ValidationCandidateLoadArgs<'a, D> {
    experiment: &'a ExperimentHandle,
    store: &'a FsArtifactStore,
    device: &'a D,
    updates: &'a [UpdateAnnounce],
    current_head: &'a Option<(PeerId, HeadDescriptor)>,
    canary_threshold: f64,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct PeerRobustnessState {
    trust_score: f64,
    consecutive_rejections: u16,
    quarantined: bool,
    last_rejection_reason: Option<RejectionReason>,
    updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    quarantine_started_window: Option<WindowId>,
    #[serde(default)]
    last_quarantine_window: Option<WindowId>,
    #[serde(default)]
    ban_recommended: bool,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct PersistedRobustnessState {
    peers: BTreeMap<PeerId, PeerRobustnessState>,
}

#[derive(Clone, Debug)]
struct ValidationRobustnessExecution {
    decisions: Vec<RobustnessDecision>,
    trust_scores: Vec<TrustScore>,
    cohort_report: CohortRobustnessReport,
    canary_report: Option<CanaryEvalReport>,
    replica_agreement: Option<f64>,
}

struct ValidationExecution {
    source_peer_id: PeerId,
    merged_head: HeadDescriptor,
    accepted_updates: Vec<UpdateAnnounce>,
    merge_certificate: MergeCertificate,
    contribution: ContributionReceipt,
    evaluation: MetricReport,
    aggregate: AggregateEnvelope,
    aggregate_artifact: AggregateArtifactBytes,
    reduction_certificate: ReductionCertificate,
    reducer_load_report: ReducerLoadReport,
    robustness: ValidationRobustnessExecution,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

enum ValidationAttempt {
    Blocked(Box<ValidationRobustnessExecution>),
    Promoted(Box<ValidationExecution>),
}

type AcceptedCandidateWeights = BTreeMap<(PeerId, ArtifactId), f64>;

struct CandidateRobustnessOutcome {
    decisions: Vec<RobustnessDecision>,
    trust_scores: Vec<TrustScore>,
    filtered_updates: Vec<UpdateAnnounce>,
    accepted_weights: AcceptedCandidateWeights,
}

struct AcceptedCohortCandidate {
    update_index: usize,
    sketch: UpdateFeatureSketch,
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

fn collect_validation_candidate_heads(
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_peer_id: &PeerId,
    expected_parent_head_id: Option<&HeadId>,
) -> Vec<(PeerId, HeadDescriptor)> {
    let mut candidate_heads = Vec::new();
    for (peer_id, snapshot) in snapshots {
        for announcement in &snapshot.head_announcements {
            if !matches_experiment_head(&announcement.head, experiment) {
                continue;
            }
            if peer_id == local_peer_id {
                continue;
            }
            if expected_parent_head_id != announcement.head.parent_head_id.as_ref() {
                continue;
            }
            candidate_heads.push((peer_id.clone(), announcement.head.clone()));
        }
    }
    candidate_heads
}

fn load_validation_base_model<P>(
    project: &mut P,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    store: &FsArtifactStore,
    device: &P::Device,
) -> anyhow::Result<P::Model>
where
    P: P2pWorkload,
{
    Ok(if let Some((_, base_head)) = current_head.as_ref() {
        match store.load_manifest(&base_head.artifact_id) {
            Ok(descriptor) => project.load_model_artifact(
                project.init_model(device),
                &descriptor,
                store,
                device,
            )?,
            Err(_) if base_head.global_step == 0 => project.init_model(device),
            Err(error) => return Err(error.into()),
        }
    } else {
        project.init_model(device)
    })
}

fn load_validation_candidate_models<P>(
    project: &mut P,
    args: ValidationCandidateLoadArgs<'_, P::Device>,
    candidate_heads: Vec<(PeerId, HeadDescriptor)>,
) -> anyhow::Result<Vec<ValidationCandidate<P::Model>>>
where
    P: P2pWorkload,
{
    let mut candidate_models = Vec::new();
    for (peer_id, head) in candidate_heads {
        let descriptor = args.store.load_manifest(&head.artifact_id)?;
        let model = project.load_model_artifact(
            project.init_model(args.device),
            &descriptor,
            args.store,
            args.device,
        )?;
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let quality = args
            .updates
            .iter()
            .find(|update| {
                update.peer_id == peer_id && update.delta_artifact_id == head.artifact_id
            })
            .map(|update| update.quality_weight)
            .unwrap_or_else(|| (1.0 / (1.0 + metric_quality(&evaluation.metrics).abs())).max(0.01));
        let sample_weight = args
            .updates
            .iter()
            .find(|update| {
                update.peer_id == peer_id && update.delta_artifact_id == head.artifact_id
            })
            .map(|update| update.sample_weight)
            .unwrap_or(1.0);
        let canary_report = build_validation_canary_report(
            args.experiment,
            args.current_head,
            &head,
            &evaluation,
            args.canary_threshold,
            2,
        )?;
        candidate_models.push(ValidationCandidate {
            peer_id,
            head,
            evaluation,
            canary_report,
            sample_weight,
            quality_weight: quality,
            model,
        });
    }
    Ok(candidate_models)
}

fn fallback_best_candidate_index<M>(candidate_models: &[ValidationCandidate<M>]) -> Option<usize> {
    candidate_models
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| {
            metric_quality(&left.evaluation.metrics)
                .partial_cmp(&metric_quality(&right.evaluation.metrics))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(index, _)| index)
}

#[allow(clippy::too_many_arguments)]
fn select_validation_head<P>(
    project: &mut P,
    experiment: &ExperimentHandle,
    store: &FsArtifactStore,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    base_head_id: &HeadId,
    window_id: WindowId,
    base_model: P::Model,
    candidate_models: &[ValidationCandidate<P::Model>],
    fallback_best_index: usize,
    merge_policy: MergePolicy,
    local_peer_id: &PeerId,
) -> anyhow::Result<(PeerId, HeadDescriptor, MetricReport)>
where
    P: P2pWorkload,
{
    let merged_model = {
        let merge_candidates = candidate_models
            .iter()
            .map(|candidate| MergeModelCandidate {
                peer_id: &candidate.peer_id,
                head_id: &candidate.head.head_id,
                artifact_id: &candidate.head.artifact_id,
                model: &candidate.model,
                sample_weight: candidate.sample_weight,
                quality_weight: candidate.quality_weight,
            })
            .collect::<Vec<_>>();
        match project.merge_candidate_models(
            &base_model,
            &merge_candidates,
            merge_policy.clone(),
        )? {
            Some(merged_model) => Some(project.apply_single_root_ema(
                &base_model,
                merged_model,
                merge_policy.clone(),
            )?),
            None => None,
        }
    };

    if let Some(merged_model) = merged_model {
        let evaluation = project.evaluate(&merged_model, EvalSplit::Validation);
        let merged_head_id = HeadId::new(format!(
            "{}-merged-window-{}",
            experiment.experiment_id.as_str(),
            window_id.0
        ));
        let artifact = project.materialize_model_artifact(
            &merged_model,
            ArtifactKind::FullHead,
            merged_head_id.clone(),
            Some(base_head_id.clone()),
            store,
        )?;
        let merged_head = HeadDescriptor {
            head_id: merged_head_id,
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: Some(base_head_id.clone()),
            global_step: current_head
                .as_ref()
                .map(|(_, head)| head.global_step + 1)
                .unwrap_or(0),
            created_at: Utc::now(),
            metrics: evaluation.metrics.clone(),
        };
        let source_peer_id = candidate_models
            .iter()
            .max_by(|left, right| {
                (left.sample_weight * left.quality_weight)
                    .partial_cmp(&(right.sample_weight * right.quality_weight))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|candidate| candidate.peer_id.clone())
            .unwrap_or_else(|| local_peer_id.clone());
        return Ok((source_peer_id, merged_head, evaluation));
    }

    let candidate = &candidate_models[fallback_best_index];
    let evaluation = project.evaluate(&candidate.model, EvalSplit::Validation);
    Ok((
        candidate.peer_id.clone(),
        candidate.head.clone(),
        evaluation,
    ))
}

fn evaluate_candidate_robustness<M>(
    engine: &burn_p2p_security::RobustnessEngine,
    prepared: &ValidationPreparedState,
    candidate_models: &[ValidationCandidate<M>],
    evaluated_at: DateTime<Utc>,
) -> CandidateRobustnessOutcome {
    let mut seen_receipt_roots = BTreeSet::new();
    let mut seen_receipt_ids = BTreeSet::new();
    let mut seen_artifacts = BTreeSet::new();
    let browser_peers = browser_peer_ids_from_snapshots(&prepared.snapshots);
    let mut decisions = Vec::new();
    let mut accepted_candidates = Vec::new();
    let mut candidate_states = Vec::new();

    for (update_index, update) in prepared.updates.iter().enumerate() {
        let candidate = candidate_models.iter().find(|candidate| {
            candidate.peer_id == update.peer_id
                && candidate.head.artifact_id == update.delta_artifact_id
        });
        let peer_state = prepared
            .robustness_state
            .peers
            .get(&update.peer_id)
            .map(|state| {
                resolved_peer_robustness_state(
                    &prepared.robustness_policy.quarantine_policy,
                    prepared.merge_window.window_id,
                    state,
                )
            })
            .unwrap_or_default();
        let duplicate = !seen_receipt_roots.insert(update.receipt_root.clone())
            || update
                .receipt_ids
                .iter()
                .any(|receipt_id| !seen_receipt_ids.insert(receipt_id.clone()))
            || !seen_artifacts.insert((update.peer_id.clone(), update.delta_artifact_id.clone()));
        let observation = burn_p2p_security::RobustUpdateObservation {
            peer_id: update.peer_id.clone(),
            base_head_id: update.base_head_id.clone(),
            expected_base_head_id: prepared.base_head_id.clone(),
            revision_id: update.revision_id.clone(),
            expected_revision_id: prepared.merge_window.revision_id.clone(),
            dataset_view_id: prepared.dataset_view_id.clone(),
            expected_dataset_view_id: prepared.dataset_view_id.clone(),
            lease_id: update.lease_id.clone(),
            expected_lease_id: update.lease_id.clone(),
            schema_matches: candidate.is_some(),
            duplicate,
            quarantined: peer_state.quarantined,
            consecutive_rejections: peer_state.consecutive_rejections,
            current_trust_score: peer_state.trust_score,
            canary_report: candidate.map(|candidate| candidate.canary_report.clone()),
            feature_sketch: update
                .feature_sketch
                .clone()
                .unwrap_or_else(|| fallback_update_feature_sketch(update)),
        };
        let decision = engine.evaluate_update(&observation, evaluated_at);
        if decision.accepted && candidate.is_some() {
            let sketch = update
                .feature_sketch
                .clone()
                .unwrap_or_else(|| fallback_update_feature_sketch(update));
            accepted_candidates.push(AcceptedCohortCandidate {
                update_index,
                sketch,
            });
        }
        candidate_states.push((peer_state, update.clone()));
        decisions.push(decision);
    }

    let accepted_sketches = accepted_candidates
        .iter()
        .map(|candidate| candidate.sketch.clone())
        .collect::<Vec<_>>();
    let accepted_base_weights = accepted_candidates
        .iter()
        .map(|candidate| decisions[candidate.update_index].effective_weight)
        .collect::<Vec<_>>();
    let cohort_decisions = burn_p2p_security::filter_update_sketches_with_policy(
        &prepared.robustness_policy,
        &accepted_sketches,
        &accepted_base_weights,
    );
    for (accepted_candidate, cohort_decision) in accepted_candidates.iter().zip(cohort_decisions) {
        let decision = &mut decisions[accepted_candidate.update_index];
        if !cohort_decision.accepted {
            decision.accepted = false;
            decision.downweighted = false;
            decision.rejection_reason = Some(RejectionReason::SimilarityOutlier);
            decision.effective_weight = 0.0;
        } else {
            decision.effective_weight *= cohort_decision.weight_scale.max(0.01);
            if cohort_decision.weight_scale < 0.999 {
                decision.downweighted = true;
            }
        }
    }
    let maximum_cohort_size = usize::from(
        prepared
            .robustness_policy
            .aggregation_policy
            .maximum_cohort_size
            .max(1),
    );
    let mut ranked_accepted = decisions
        .iter()
        .enumerate()
        .filter(|(_, decision)| decision.accepted)
        .map(|(index, decision)| (index, decision.effective_weight, decision.screen_score))
        .collect::<Vec<_>>();
    if ranked_accepted.len() > maximum_cohort_size {
        ranked_accepted.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.2.total_cmp(&right.2))
                .then_with(|| left.0.cmp(&right.0))
        });
        let keep = ranked_accepted
            .iter()
            .take(maximum_cohort_size)
            .map(|(index, _, _)| *index)
            .collect::<BTreeSet<_>>();
        for (index, decision) in decisions.iter_mut().enumerate() {
            if decision.accepted && !keep.contains(&index) {
                decision.accepted = false;
                decision.downweighted = false;
                decision.rejection_reason = Some(RejectionReason::SimilarityOutlier);
                decision.effective_weight = 0.0;
            }
        }
    }

    let mut trust_scores = Vec::new();
    let mut filtered_updates = Vec::new();
    let mut accepted_weights = BTreeMap::new();
    for (index, decision) in decisions.iter_mut().enumerate() {
        let (peer_state, update) = &candidate_states[index];
        decision.quarantined = burn_p2p_security::should_quarantine(
            &prepared.robustness_policy.quarantine_policy,
            peer_state.consecutive_rejections,
            decision,
        );
        let trust_score =
            burn_p2p_security::updated_trust_score(burn_p2p_security::TrustUpdateInput {
                policy: &prepared.robustness_policy.reputation_policy,
                peer_id: decision.peer_id.clone(),
                current_score: peer_state.trust_score,
                accepted: decision.accepted,
                downweighted: decision.downweighted,
                screen_score: decision.screen_score,
                rejection_reason: decision.rejection_reason.as_ref(),
                quarantined: decision.quarantined,
                updated_at: evaluated_at,
            });
        decision.trust_score = Some(trust_score.clone());
        trust_scores.push(trust_score);

        if decision.accepted {
            let capped_weight = if browser_peers.contains(&update.peer_id) {
                decision
                    .effective_weight
                    .min(
                        prepared
                            .robustness_policy
                            .aggregation_policy
                            .browser_contribution_cap,
                    )
                    .max(0.01)
            } else {
                decision.effective_weight.max(0.01)
            };
            decision.effective_weight = capped_weight;
            accepted_weights.insert(
                (update.peer_id.clone(), update.delta_artifact_id.clone()),
                capped_weight,
            );
            let mut filtered = update.clone();
            filtered.sample_weight *= capped_weight;
            filtered_updates.push(filtered);
        }
    }

    CandidateRobustnessOutcome {
        decisions,
        trust_scores,
        filtered_updates,
        accepted_weights,
    }
}

fn browser_peer_ids_from_snapshots(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> BTreeSet<PeerId> {
    snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            snapshot
                .auth_announcements
                .iter()
                .filter(|announcement| &announcement.peer_id == peer_id)
                .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
                .and_then(|announcement| {
                    announcement
                        .envelope
                        .certificate
                        .claims()
                        .granted_roles
                        .roles
                        .iter()
                        .any(|role| {
                            matches!(
                                role,
                                PeerRole::PortalViewer
                                    | PeerRole::BrowserObserver
                                    | PeerRole::BrowserVerifier
                                    | PeerRole::BrowserTrainerWgpu
                                    | PeerRole::BrowserFallback
                                    | PeerRole::BrowserTrainer
                            )
                        })
                        .then(|| peer_id.clone())
                })
        })
        .collect()
}

fn fallback_update_feature_sketch(update: &UpdateAnnounce) -> UpdateFeatureSketch {
    UpdateFeatureSketch {
        artifact_size_bytes: 0,
        global_norm: update.norm_stats.l2_norm,
        per_layer_norms: BTreeMap::from([(String::from("update"), update.norm_stats.l2_norm)]),
        random_projection: vec![update.norm_stats.l2_norm, update.norm_stats.max_abs],
        sign_projection: vec![
            if update.norm_stats.l2_norm > 0.0 {
                1
            } else {
                0
            },
            if update.norm_stats.max_abs > 0.0 {
                1
            } else {
                0
            },
        ],
        top_k_indices: Vec::new(),
        cosine_to_reference: None,
        sign_agreement_fraction: None,
        canary_loss_delta: None,
        historical_deviation_score: None,
        neighbor_distance: None,
        staleness_windows: 0,
        receive_delay_ms: 0,
        non_finite_tensor_count: update.norm_stats.non_finite_tensors,
    }
}

fn build_validation_canary_report(
    experiment: &ExperimentHandle,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    candidate_head: &HeadDescriptor,
    evaluation: &MetricReport,
    maximum_regression_delta: f64,
    evaluator_quorum: u16,
) -> anyhow::Result<CanaryEvalReport> {
    let base_quality = current_head
        .as_ref()
        .map(|(_, head)| metric_quality(&head.metrics))
        .unwrap_or_else(|| metric_quality(&evaluation.metrics));
    let candidate_quality = metric_quality(&evaluation.metrics);
    let regression_margin = (candidate_quality - base_quality).max(0.0);
    let metric_deltas = evaluation
        .metrics
        .iter()
        .filter_map(|(key, value)| match value {
            MetricValue::Integer(value) => Some((key.clone(), *value as f64)),
            MetricValue::Float(value) => Some((key.clone(), *value)),
            MetricValue::Bool(_) | MetricValue::Text(_) => None,
        })
        .collect::<BTreeMap<_, _>>();

    Ok(CanaryEvalReport {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        eval_protocol_id: ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            "runtime-canary",
        ))?,
        candidate_head_id: candidate_head.head_id.clone(),
        base_head_id: current_head.as_ref().map(|(_, head)| head.head_id.clone()),
        accepted: regression_margin <= maximum_regression_delta,
        metric_deltas,
        regression_margin,
        detected_backdoor_trigger: false,
        evaluator_quorum,
        evaluated_at: Utc::now(),
    })
}

fn append_quarantine_escalation_alerts(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    emitted_at: DateTime<Utc>,
) {
    let policy = &prepared.robustness_policy.escalation_policy;
    let resolved_previous = prepared
        .robustness_state
        .peers
        .iter()
        .map(|(peer_id, state)| {
            (
                peer_id.clone(),
                resolved_peer_robustness_state(
                    &prepared.robustness_policy.quarantine_policy,
                    prepared.merge_window.window_id,
                    state,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let (next_state, _) = project_robustness_state(
        &prepared.robustness_state,
        &robustness.decisions,
        &prepared.robustness_policy.quarantine_policy,
        prepared.merge_window.window_id,
    );

    if policy.alert_on_quarantine {
        for (peer_id, _) in next_state
            .peers
            .iter()
            .filter(|(_, state)| state.quarantined)
        {
            let was_quarantined = resolved_previous
                .get(peer_id)
                .map(|state| state.quarantined)
                .unwrap_or(false);
            if was_quarantined {
                continue;
            }
            robustness.cohort_report.alerts.push(RobustnessAlert {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: Some(prepared.merge_window.window_id),
                peer_id: Some(peer_id.clone()),
                reason: RejectionReason::QuarantinedPrincipal,
                severity: RobustnessAlertSeverity::Warn,
                message: "peer entered automatic quarantine after repeated suspicious updates"
                    .into(),
                emitted_at,
            });
        }
    }

    for (peer_id, _) in next_state
        .peers
        .iter()
        .filter(|(_, state)| state.quarantined && state.ban_recommended)
    {
        let previously_recommended = prepared
            .robustness_state
            .peers
            .get(peer_id)
            .map(|state| state.ban_recommended)
            .unwrap_or(false);
        if previously_recommended {
            continue;
        }
        robustness.cohort_report.alerts.push(RobustnessAlert {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: Some(prepared.merge_window.window_id),
            peer_id: Some(peer_id.clone()),
            reason: RejectionReason::QuarantinedPrincipal,
            severity: RobustnessAlertSeverity::Critical,
            message:
                "peer remained under automatic quarantine long enough to recommend operator ban"
                    .into(),
            emitted_at,
        });
    }

    let effective_quarantine = next_state
        .peers
        .iter()
        .map(|(peer_id, state)| (peer_id.clone(), state.quarantined))
        .collect::<BTreeMap<_, _>>();
    let total_peers = effective_quarantine.len();
    if total_peers == 0 {
        return;
    }
    let quarantined_peers = effective_quarantine
        .values()
        .filter(|quarantined| **quarantined)
        .count();
    let quarantined_fraction = quarantined_peers as f64 / total_peers as f64;
    if quarantined_fraction > policy.maximum_quarantined_fraction {
        robustness.cohort_report.alerts.push(RobustnessAlert {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: Some(prepared.merge_window.window_id),
            peer_id: None,
            reason: RejectionReason::QuarantinedPrincipal,
            severity: RobustnessAlertSeverity::Critical,
            message: format!(
                "quarantined peer fraction {:.2} exceeded configured maximum {:.2}",
                quarantined_fraction, policy.maximum_quarantined_fraction
            ),
            emitted_at,
        });
    }
}

fn append_canary_escalation_alert(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    canary_report: &CanaryEvalReport,
    emitted_at: DateTime<Utc>,
) {
    if !prepared
        .robustness_policy
        .escalation_policy
        .pause_on_canary_regression
    {
        return;
    }
    let policy = &prepared.robustness_policy.validator_canary_policy;
    let blocked = !canary_report.accepted
        || canary_report.detected_backdoor_trigger
        || canary_report.regression_margin > policy.maximum_regression_delta
        || canary_report.evaluator_quorum < policy.minimum_evaluator_quorum;
    if !blocked {
        return;
    }

    robustness.cohort_report.alerts.push(RobustnessAlert {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: Some(prepared.merge_window.window_id),
        peer_id: None,
        reason: RejectionReason::CanaryRegression,
        severity: RobustnessAlertSeverity::Critical,
        message: format!(
            "candidate promotion paused by canary regression {:.4}",
            canary_report.regression_margin
        ),
        emitted_at,
    });
}

fn append_replica_disagreement_alert(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    emitted_at: DateTime<Utc>,
) {
    let Some(replica_agreement) = robustness.replica_agreement else {
        return;
    };
    if replica_agreement >= 0.999 {
        return;
    }

    robustness.cohort_report.alerts.push(RobustnessAlert {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: Some(prepared.merge_window.window_id),
        peer_id: None,
        reason: RejectionReason::ReplicaDisagreement,
        severity: if prepared
            .robustness_policy
            .escalation_policy
            .pause_on_replica_disagreement
        {
            RobustnessAlertSeverity::Critical
        } else {
            RobustnessAlertSeverity::Warn
        },
        message: format!(
            "reducer replica agreement {:.2} fell below unanimity for the active cohort",
            replica_agreement
        ),
        emitted_at,
    });
}

fn observed_replica_agreement(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    base_head_id: &HeadId,
) -> Option<f64> {
    let reducer_set = merge_window
        .reducers
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut latest = BTreeMap::<PeerId, AggregateEnvelope>::new();
    for (peer_id, snapshot) in snapshots {
        if !reducer_set.contains(peer_id) {
            continue;
        }
        let candidate = snapshot
            .aggregate_announcements
            .iter()
            .filter(|announcement| {
                let aggregate = &announcement.aggregate;
                aggregate.study_id == experiment.study_id
                    && aggregate.experiment_id == experiment.experiment_id
                    && aggregate.revision_id == experiment.revision_id
                    && aggregate.window_id == merge_window.window_id
                    && &aggregate.base_head_id == base_head_id
                    && &aggregate.reducer_peer_id == peer_id
            })
            .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
            .map(|announcement| announcement.aggregate.clone());
        if let Some(candidate) = candidate {
            latest.insert(peer_id.clone(), candidate);
        }
    }
    if latest.len() < 2 {
        return None;
    }

    let mut fingerprints = BTreeMap::<String, usize>::new();
    for aggregate in latest.values() {
        let mut contributors = aggregate
            .contributor_peers
            .iter()
            .map(|peer| peer.as_str().to_owned())
            .collect::<Vec<_>>();
        contributors.sort();
        let mut providers = aggregate
            .providers
            .iter()
            .map(|peer| peer.as_str().to_owned())
            .collect::<Vec<_>>();
        providers.sort();
        let fingerprint = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            aggregate.aggregate_artifact_id.as_str(),
            aggregate.stats.accepted_updates,
            aggregate.stats.duplicate_updates,
            aggregate.stats.dropped_updates,
            aggregate.stats.late_updates,
            aggregate.stats.sum_sample_weight.to_bits(),
            contributors.join(","),
            providers.join(","),
        );
        *fingerprints.entry(fingerprint).or_default() += 1;
    }

    let agreeing = fingerprints.values().copied().max().unwrap_or(0);
    Some(agreeing as f64 / latest.len() as f64)
}

fn persist_validation_robustness_state(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    previous: &PersistedRobustnessState,
    decisions: &[RobustnessDecision],
    policy: &QuarantinePolicy,
    current_window: WindowId,
) -> anyhow::Result<Vec<TrustScore>> {
    let (next, trust_scores) =
        project_robustness_state(previous, decisions, policy, current_window);
    persist_json(storage.scoped_robustness_state_path(experiment), &next)?;
    Ok(trust_scores)
}

fn project_robustness_state(
    previous: &PersistedRobustnessState,
    decisions: &[RobustnessDecision],
    policy: &QuarantinePolicy,
    current_window: WindowId,
) -> (PersistedRobustnessState, Vec<TrustScore>) {
    let mut next = previous.clone();
    let mut trust_scores = Vec::new();
    for peer_state in next.peers.values_mut() {
        *peer_state = resolved_peer_robustness_state(policy, current_window, peer_state);
    }

    for decision in decisions {
        let peer_state = next.peers.entry(decision.peer_id.clone()).or_default();
        let was_quarantined = peer_state.quarantined;
        if decision.accepted {
            peer_state.consecutive_rejections = 0;
        } else {
            peer_state.consecutive_rejections = peer_state.consecutive_rejections.saturating_add(1);
        }
        if let Some(trust) = decision.trust_score.clone() {
            peer_state.trust_score = trust.score;
            peer_state.quarantined = trust.quarantined;
            peer_state.updated_at = Some(trust.updated_at);
            let mut trust = trust;
            if trust.quarantined {
                if !was_quarantined || peer_state.quarantine_started_window.is_none() {
                    peer_state.quarantine_started_window = Some(current_window);
                }
                peer_state.last_quarantine_window = Some(current_window);
                let quarantine_started_window = peer_state
                    .quarantine_started_window
                    .unwrap_or(current_window);
                peer_state.ban_recommended =
                    quarantine_ban_due(policy, current_window, quarantine_started_window);
            } else {
                peer_state.quarantine_started_window = None;
                peer_state.last_quarantine_window = None;
                peer_state.ban_recommended = false;
            }
            trust.ban_recommended = peer_state.ban_recommended;
            trust_scores.push(trust);
        }
        peer_state.last_rejection_reason = decision.rejection_reason.clone();
    }

    (next, trust_scores)
}

fn resolved_peer_robustness_state(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    state: &PeerRobustnessState,
) -> PeerRobustnessState {
    let mut resolved = state.clone();
    if !policy.automatic_quarantine {
        resolved.quarantined = false;
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }
    if !resolved.quarantined {
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }

    let quarantine_started_window = resolved.quarantine_started_window.unwrap_or(current_window);
    let last_quarantine_window = resolved
        .last_quarantine_window
        .or(resolved.quarantine_started_window)
        .unwrap_or(current_window);
    if quarantine_expired(policy, current_window, last_quarantine_window) {
        resolved.consecutive_rejections = 0;
        resolved.quarantined = false;
        resolved.last_rejection_reason = None;
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }

    resolved.quarantine_started_window = Some(quarantine_started_window);
    resolved.last_quarantine_window = Some(last_quarantine_window);
    resolved.ban_recommended =
        quarantine_ban_due(policy, current_window, quarantine_started_window);
    resolved
}

fn quarantine_expired(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    last_quarantine_window: WindowId,
) -> bool {
    current_window.0
        >= last_quarantine_window.0 + u64::from(policy.quarantine_duration_windows.max(1))
}

fn quarantine_ban_due(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    quarantine_started_window: WindowId,
) -> bool {
    current_window.0
        >= quarantine_started_window.0 + u64::from(policy.ban_after_quarantine_windows.max(1))
}

fn aggregate_stats_from_updates(updates: &[UpdateAnnounce]) -> AggregateStats {
    AggregateStats {
        accepted_updates: updates.len() as u32,
        duplicate_updates: 0,
        dropped_updates: 0,
        late_updates: 0,
        sum_sample_weight: updates.iter().map(|update| update.sample_weight).sum(),
        sum_quality_weight: updates.iter().map(|update| update.quality_weight).sum(),
        sum_weighted_delta_norm: updates
            .iter()
            .map(|update| update.sample_weight * update.quality_weight * update.norm_stats.l2_norm)
            .sum(),
        max_update_norm: updates
            .iter()
            .map(|update| update.norm_stats.max_abs)
            .fold(0.0_f64, f64::max),
        accepted_sample_coverage: if updates.is_empty() { 0.0 } else { 1.0 },
    }
}

fn build_aggregate_record(
    experiment: &ExperimentHandle,
    base_head_id: &HeadId,
    aggregate_id: &ContentId,
    merge_policy: MergePolicy,
    evaluation: &MetricReport,
    updates: &[UpdateAnnounce],
    aggregate_stats: &AggregateStats,
) -> AggregateArtifactRecord {
    AggregateArtifactRecord {
        aggregate_id: aggregate_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: base_head_id.clone(),
        policy: merge_policy,
        aggregate_stats: aggregate_stats.clone(),
        merge_plan: MergePlan {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            base_head_id: base_head_id.clone(),
            policy: MergePolicy::QualityWeightedEma,
            contribution_receipt_ids: updates
                .iter()
                .flat_map(|update| update.receipt_ids.clone())
                .collect(),
            artifact_ids: updates
                .iter()
                .map(|update| update.delta_artifact_id.clone())
                .collect(),
            total_weight: updates
                .iter()
                .map(|update| update.sample_weight * update.quality_weight)
                .sum(),
            aggregated_numeric_metrics: evaluation
                .metrics
                .iter()
                .filter_map(|(name, value)| match value {
                    MetricValue::Integer(value) => Some((name.clone(), *value as f64)),
                    MetricValue::Float(value) => Some((name.clone(), *value)),
                    MetricValue::Bool(_) | MetricValue::Text(_) => None,
                })
                .collect(),
        },
        inputs: updates
            .iter()
            .map(|update| AggregateArtifactInput {
                peer_id: update.peer_id.clone(),
                artifact_id: update.delta_artifact_id.clone(),
                sample_weight: update.sample_weight,
                quality_weight: update.quality_weight,
                receipt_ids: update.receipt_ids.clone(),
            })
            .collect(),
        created_at: Utc::now(),
    }
}

fn build_validation_aggregate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate_id: &ContentId,
    aggregate_artifact: &AggregateArtifactBytes,
    aggregate_stats: &AggregateStats,
    updates: &[UpdateAnnounce],
) -> AggregateEnvelope {
    AggregateEnvelope {
        aggregate_id: aggregate_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_artifact_id: aggregate_artifact.descriptor.artifact_id.clone(),
        tier: AggregateTier::RootCandidate,
        reducer_peer_id: prepared.local_peer_id.clone(),
        contributor_peers: updates
            .iter()
            .map(|update| update.peer_id.clone())
            .collect(),
        child_aggregate_ids: Vec::new(),
        stats: aggregate_stats.clone(),
        providers: std::iter::once(prepared.local_peer_id.clone())
            .chain(updates.iter().flat_map(|update| update.providers.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        published_at: Utc::now(),
    }
}

fn build_reduction_certificate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate: &AggregateEnvelope,
    merge_window: &MergeWindowState,
) -> anyhow::Result<ReductionCertificate> {
    Ok(ReductionCertificate {
        reduction_id: ContentId::derive(&(
            aggregate.aggregate_id.as_str(),
            prepared.local_peer_id.as_str(),
            merge_window.window_id.0,
        ))?,
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_id: aggregate.aggregate_id.clone(),
        validator: prepared.local_peer_id.clone(),
        validator_quorum: merge_window.policy.promotion_policy.validator_quorum.max(1),
        cross_checked_reducers: merge_window
            .reducers
            .iter()
            .take(usize::from(merge_window.policy.reducer_replication.max(1)))
            .cloned()
            .collect(),
        issued_at: Utc::now(),
    })
}

fn build_validation_contribution(
    experiment: &ExperimentHandle,
    source_peer_id: &PeerId,
    merged_head: &HeadDescriptor,
    evaluation: &MetricReport,
) -> ContributionReceipt {
    ContributionReceipt {
        receipt_id: ContributionReceiptId::new(format!(
            "{}-validated-{}-{}",
            experiment.experiment_id.as_str(),
            source_peer_id.as_str(),
            merged_head.global_step
        )),
        peer_id: source_peer_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: merged_head
            .parent_head_id
            .clone()
            .unwrap_or_else(|| HeadId::new("genesis")),
        artifact_id: merged_head.artifact_id.clone(),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: evaluation.metrics.clone(),
        merge_cert_id: None,
    }
}

fn build_validation_merge_certificate(
    experiment: &ExperimentHandle,
    local_peer_id: &PeerId,
    base_head_id: &HeadId,
    merged_head: &HeadDescriptor,
    merge_policy: MergePolicy,
    contribution: &ContributionReceipt,
    updates: &[UpdateAnnounce],
) -> MergeCertificate {
    MergeCertificate {
        merge_cert_id: MergeCertId::new(format!(
            "{}-merge-{}",
            experiment.experiment_id.as_str(),
            merged_head.global_step
        )),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: base_head_id.clone(),
        merged_head_id: merged_head.head_id.clone(),
        merged_artifact_id: merged_head.artifact_id.clone(),
        policy: merge_policy,
        issued_at: Utc::now(),
        validator: local_peer_id.clone(),
        contribution_receipts: if updates.is_empty() {
            vec![contribution.receipt_id.clone()]
        } else {
            updates
                .iter()
                .flat_map(|update| update.receipt_ids.clone())
                .collect()
        },
    }
}

fn build_validation_reducer_load(
    local_peer_id: &PeerId,
    aggregate: &AggregateEnvelope,
    updates: &[UpdateAnnounce],
) -> ReducerLoadReport {
    ReducerLoadReport {
        peer_id: local_peer_id.clone(),
        window_id: aggregate.window_id,
        assigned_leaf_updates: aggregate.stats.accepted_updates,
        assigned_aggregate_inputs: aggregate.stats.accepted_updates,
        ingress_bytes: updates.len() as u128 * 1024,
        egress_bytes: aggregate.providers.len() as u128 * 512,
        duplicate_transfer_ratio: 0.0,
        overload_ratio: 0.0,
        reported_at: Utc::now(),
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
