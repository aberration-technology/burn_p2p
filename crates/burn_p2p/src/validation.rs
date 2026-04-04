use super::*;
use crate::metrics_runtime::{
    ValidationMetricBuildArgs, build_head_eval_report, build_metrics_announcement,
    build_reducer_cohort_metrics, build_validation_peer_window_metrics,
    persist_eval_protocol_manifest, persist_head_eval_report, persist_peer_window_metrics,
    persist_reducer_cohort_metrics,
};
use crate::runtime_support::LagAssessment;
use burn_p2p_core::MetricsLiveEventKind;

struct ValidationPreparedState {
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    updates: Vec<UpdateAnnounce>,
    metrics_retention: MetricsRetentionBudget,
}

struct ValidationCandidate<M> {
    peer_id: PeerId,
    head: HeadDescriptor,
    evaluation: MetricReport,
    sample_weight: f64,
    quality_weight: f64,
    model: M,
}

struct ValidationExecution {
    source_peer_id: PeerId,
    merged_head: HeadDescriptor,
    merge_certificate: MergeCertificate,
    contribution: ContributionReceipt,
    evaluation: MetricReport,
    aggregate: AggregateEnvelope,
    aggregate_artifact: AggregateArtifactBytes,
    reduction_certificate: ReductionCertificate,
    reducer_load_report: ReducerLoadReport,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

impl<P> RunningNode<P> {
    /// Validates the candidates once.
    pub fn validate_candidates_once<B>(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<ValidationOutcome>>
    where
        B: ProjectBackend,
        P: RuntimeProject<B>,
    {
        let prepared = self.prepare_validation_state(experiment)?;
        let Some(execution) = self.execute_validation_candidates::<B>(experiment, &prepared)?
        else {
            return Ok(None);
        };
        self.publish_validation_execution(experiment, &prepared, &execution)?;

        Ok(Some(ValidationOutcome {
            source_peer_id: execution.source_peer_id,
            merged_head: execution.merged_head,
            merge_certificate: execution.merge_certificate,
            contribution: execution.contribution,
            evaluation: execution.evaluation,
        }))
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
        let validators = runtime_validators(
            &self.mainnet().roles,
            &local_peer_id,
            &topology_peers,
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

        Ok(ValidationPreparedState {
            assignment,
            storage,
            store,
            local_peer_id,
            snapshots,
            current_head,
            base_head_id,
            merge_window,
            updates,
            metrics_retention,
        })
    }

    fn execute_validation_candidates<B>(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
    ) -> anyhow::Result<Option<ValidationExecution>>
    where
        B: ProjectBackend,
        P: RuntimeProject<B>,
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
        if candidate_heads.is_empty() {
            return Ok(None);
        }
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
        let base_model = load_validation_base_model::<B, P>(
            project,
            &prepared.current_head,
            &prepared.store,
            &device,
        )?;
        let mut candidate_models = load_validation_candidate_models(
            project,
            &prepared.store,
            &device,
            &prepared.updates,
            candidate_heads,
        )?;
        let Some(fallback_best_index) = fallback_best_candidate_index(&candidate_models) else {
            return Ok(None);
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

        let aggregate_id = ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            prepared.merge_window.window_id.0,
            prepared.base_head_id.as_str(),
            merged_head.head_id.as_str(),
        ))?;
        let aggregate_stats = aggregate_stats_from_updates(&prepared.updates);
        let aggregate_record = build_aggregate_record(
            experiment,
            &prepared.base_head_id,
            &aggregate_id,
            merge_policy.clone(),
            &evaluation,
            &prepared.updates,
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
            &prepared.updates,
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
            &prepared.updates,
        );
        let reducer_load_report =
            build_validation_reducer_load(&prepared.local_peer_id, &aggregate, &prepared.updates);
        let finished_at = Utc::now();

        candidate_models.clear();

        Ok(Some(ValidationExecution {
            source_peer_id,
            merged_head,
            merge_certificate,
            contribution,
            evaluation,
            aggregate,
            aggregate_artifact,
            reduction_certificate,
            reducer_load_report,
            started_at,
            finished_at,
        }))
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
            updates: &prepared.updates,
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
            &prepared.updates,
        )?;
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

fn load_validation_base_model<B, P>(
    project: &mut P,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    store: &FsArtifactStore,
    device: &B::Device,
) -> anyhow::Result<P::Model>
where
    B: ProjectBackend,
    P: RuntimeProject<B>,
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

fn load_validation_candidate_models<B, P>(
    project: &mut P,
    store: &FsArtifactStore,
    device: &B::Device,
    updates: &[UpdateAnnounce],
    candidate_heads: Vec<(PeerId, HeadDescriptor)>,
) -> anyhow::Result<Vec<ValidationCandidate<P::Model>>>
where
    B: ProjectBackend,
    P: RuntimeProject<B>,
{
    let mut candidate_models = Vec::new();
    for (peer_id, head) in candidate_heads {
        let descriptor = store.load_manifest(&head.artifact_id)?;
        let model =
            project.load_model_artifact(project.init_model(device), &descriptor, store, device)?;
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let quality = updates
            .iter()
            .find(|update| {
                update.peer_id == peer_id && update.delta_artifact_id == head.artifact_id
            })
            .map(|update| update.quality_weight)
            .unwrap_or_else(|| (1.0 / (1.0 + metric_quality(&evaluation.metrics).abs())).max(0.01));
        let sample_weight = updates
            .iter()
            .find(|update| {
                update.peer_id == peer_id && update.delta_artifact_id == head.artifact_id
            })
            .map(|update| update.sample_weight)
            .unwrap_or(1.0);
        candidate_models.push(ValidationCandidate {
            peer_id,
            head,
            evaluation,
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
fn select_validation_head<B, P>(
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
    B: ProjectBackend,
    P: RuntimeProject<B>,
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
