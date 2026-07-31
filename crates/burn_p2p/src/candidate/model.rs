use super::*;

fn empty_metric_report() -> MetricReport {
    MetricReport {
        metrics: BTreeMap::new(),
        captured_at: Utc::now(),
    }
}

pub(super) fn authenticated_update_lease(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    origin_peer_id: &PeerId,
    update: &WorkloadUpdateEnvelope,
    dataset_view_id: &DatasetViewId,
) -> anyhow::Result<AssignmentLease> {
    let mut matching = snapshots
        .iter()
        .flat_map(|(_, snapshot)| snapshot.lease_announcements.iter())
        .map(|announcement| &announcement.lease)
        .filter(|lease| lease.lease_id == update.lease_id)
        .cloned()
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !matching.is_empty(),
        "typed workload update {} is missing its authenticated lease announcement",
        update.artifact.artifact_id.as_str(),
    );
    matching.sort_by(|left, right| left.assignment_hash.cmp(&right.assignment_hash));
    matching.dedup();
    anyhow::ensure!(
        matching.len() == 1,
        "typed workload update {} has conflicting lease announcements",
        update.artifact.artifact_id.as_str(),
    );
    let lease = matching.pop().expect("non-empty checked above");
    anyhow::ensure!(
        lease.peer_id == *origin_peer_id
            && lease.study_id == experiment.study_id
            && lease.experiment_id == experiment.experiment_id
            && lease.revision_id == update.revision_id
            && lease.window_id == update.window_id
            && lease.dataset_view_id == *dataset_view_id,
        "typed workload update lease identity does not match its candidate and contract"
    );
    Ok(lease)
}

pub(crate) fn load_validation_base_model<P>(
    project: &mut P,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    revision_contract: Option<&RevisionContractBundle>,
    store: &FsArtifactStore,
    device: &P::Device,
) -> anyhow::Result<P::Model>
where
    P: P2pWorkload,
{
    let (_, base_head) = current_head
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("validation requires a materialized canonical base head"))?;
    crate::training::load_model_for_head(project, base_head, revision_contract, store, device)
}

pub(crate) fn load_validation_candidate_model<P>(
    project: &mut P,
    args: ValidationCandidateLoadArgs<'_, P::Device>,
    candidate_head: ValidationCandidateHead,
) -> anyhow::Result<ValidationCandidate<P::Model>>
where
    P: P2pWorkload,
{
    let peer_id = candidate_head.origin_peer_id;
    let head = candidate_head.head;
    let update = candidate_head.update;
    let workload_update = candidate_head.workload_update;
    let descriptor = args.store.load_manifest(&head.artifact_id)?;
    let (model, update_evidence) = match workload_update {
        Some(workload_update) => {
            anyhow::ensure!(
                descriptor.kind == ArtifactKind::DeltaPack,
                "typed workload update artifact {} must be a delta pack",
                descriptor.artifact_id.as_str(),
            );
            let revision_contract = args.revision_contract.ok_or_else(|| {
                anyhow::anyhow!(
                    "typed workload update {} requires an authority-signed revision contract",
                    descriptor.artifact_id.as_str(),
                )
            })?;
            revision_contract.validate()?;
            workload_update.validate_against(
                &revision_contract.training_contract_id,
                &revision_contract.training,
            )?;
            let (_, current_head) = args.current_head.as_ref().ok_or_else(|| {
                anyhow::anyhow!("typed workload update requires a canonical base head")
            })?;
            anyhow::ensure!(
                workload_update.revision_id == args.experiment.revision_id
                    && workload_update.base_head_id == current_head.head_id
                    && workload_update.base_head_id == update.base_head_id
                    && workload_update.window_id == update.window_id
                    && update.lease_id.as_ref() == Some(&workload_update.lease_id)
                    && workload_update.artifact == descriptor
                    && update.delta_artifact_id == descriptor.artifact_id,
                "typed workload update identities do not match its candidate announcement"
            );
            let base_model = load_validation_base_model(
                project,
                args.current_head,
                args.revision_contract,
                args.store,
                args.device,
            )?;
            let lease = authenticated_update_lease(
                args.replay_snapshots,
                args.experiment,
                &peer_id,
                &workload_update,
                &revision_contract.training.dataset_view_id,
            )?;
            let cached_microshards = if revision_contract
                .training
                .update_codec
                .requires_independent_replay()
            {
                let registration = project.dataset_registration()?;
                anyhow::ensure!(
                    registration.view.dataset_view_id == lease.dataset_view_id,
                    "validator dataset view does not match the authenticated update lease"
                );
                let microshard_plan = project.microshard_plan(&registration)?;
                ShardCache::new(args.dataset_cache_dir.clone()).fetch_lease_microshards(
                    &registration,
                    &microshard_plan,
                    &lease,
                )?
            } else {
                Vec::new()
            };
            let validated = project.validate_and_apply_workload_update(
                base_model,
                WorkloadUpdateValidationContext {
                    descriptor: &descriptor,
                    update: &workload_update,
                    contract: &revision_contract.training,
                    store: args.store,
                    device: args.device,
                    replay: WorkloadUpdateReplayContext {
                        lease: &lease,
                        cached_microshards: &cached_microshards,
                        validator_peer_id: args.validator_peer_id,
                    },
                },
            )?;
            anyhow::ensure!(
                validated.evidence.update_envelope_id == ContentId::derive(&workload_update)?
                    && validated.evidence.validator_peer_id == *args.validator_peer_id
                    && validated.evidence.reconstruction_verified,
                "workload update validator returned inconsistent reconstruction evidence"
            );
            anyhow::ensure!(
                !revision_contract
                    .training
                    .update_codec
                    .requires_independent_replay()
                    || validated.evidence.replay_verified,
                "workload update failed required independent replay"
            );
            (validated.model, Some(validated.evidence))
        }
        None => {
            anyhow::ensure!(
                descriptor.kind != ArtifactKind::DeltaPack,
                "delta-pack candidate {} is missing a typed workload update envelope",
                descriptor.artifact_id.as_str(),
            );
            (
                project.load_model_artifact(
                    project.init_model(args.device),
                    &descriptor,
                    args.store,
                    args.device,
                )?,
                None,
            )
        }
    };
    let (mut evaluation, canary_report) = if args.evaluate_candidates {
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let canary_report = Some(match args.baseline_metrics {
            Some(baseline_metrics) => build_validation_canary_report_against_baseline(
                args.experiment,
                args.current_head,
                baseline_metrics,
                &head,
                &evaluation,
                args.canary_threshold,
                2,
            )?,
            None => build_validation_canary_report(
                args.experiment,
                args.current_head,
                &head,
                &evaluation,
                args.canary_threshold,
                2,
            )?,
        });
        (evaluation, canary_report)
    } else {
        (empty_metric_report(), None)
    };
    if let Some(evidence) = update_evidence.as_ref() {
        evaluation.metrics.insert(
            "update_reconstruction_verified".into(),
            MetricValue::Bool(evidence.reconstruction_verified),
        );
        evaluation.metrics.insert(
            "update_replay_verified".into(),
            MetricValue::Bool(evidence.replay_verified),
        );
        if let Some(replay) = evidence.replay_stats.as_ref() {
            evaluation.metrics.insert(
                "update_replay_generations_checked".into(),
                MetricValue::Integer(i64::from(replay.generations_checked)),
            );
            evaluation.metrics.insert(
                "update_replay_pairs_checked".into(),
                MetricValue::Integer(i64::from(replay.pairs_checked)),
            );
            evaluation.metrics.insert(
                "update_replay_max_absolute_error".into(),
                MetricValue::Float(replay.max_absolute_error),
            );
            evaluation.metrics.insert(
                "update_replay_max_relative_error".into(),
                MetricValue::Float(replay.max_relative_error),
            );
        }
    }
    let quality = if update.quality_weight.is_finite() {
        update.quality_weight
    } else {
        1.0
    };
    let sample_weight = update.sample_weight.max(0.0);
    Ok(ValidationCandidate {
        peer_id,
        head,
        update,
        evaluation,
        canary_report,
        sample_weight,
        quality_weight: quality,
        model,
        update_evidence,
    })
}

pub(crate) fn fallback_best_candidate_index<M>(
    candidate_models: &[ValidationCandidateView<'_, M>],
) -> Option<usize> {
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
pub(crate) fn select_validation_head<P>(
    project: &mut P,
    experiment: &ExperimentHandle,
    store: &FsArtifactStore,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    base_head_id: &HeadId,
    window_id: WindowId,
    base_model: &P::Model,
    candidate_models: &[ValidationCandidateView<'_, P::Model>],
    fallback_best_index: usize,
    merge_policy: MergePolicy,
    local_peer_id: &PeerId,
    allow_single_candidate_direct_promotion: bool,
) -> anyhow::Result<(PeerId, HeadDescriptor, MetricReport)>
where
    P: P2pWorkload,
{
    let expected_global_step = current_head
        .as_ref()
        .map(|(_, head)| head.global_step + 1)
        .unwrap_or(0);
    if allow_single_candidate_direct_promotion
        && let [candidate] = candidate_models
        && candidate.head.parent_head_id.as_ref() == Some(base_head_id)
        && candidate.head.global_step == expected_global_step
    {
        let mut promoted_head = candidate.head.clone();
        promoted_head.metrics = candidate.evaluation.metrics.clone();
        return Ok((
            candidate.peer_id.clone(),
            promoted_head,
            candidate.evaluation.clone(),
        ));
    }

    let merged_model = {
        let merge_candidates = candidate_models
            .iter()
            .map(|candidate| MergeModelCandidate {
                peer_id: candidate.peer_id,
                head_id: &candidate.head.head_id,
                artifact_id: &candidate.head.artifact_id,
                model: candidate.model,
                sample_weight: candidate.sample_weight,
                quality_weight: candidate.quality_weight,
            })
            .collect::<Vec<_>>();
        match project.merge_candidate_models(base_model, &merge_candidates, merge_policy.clone())? {
            Some(merged_model) => Some(project.apply_single_root_ema(
                base_model,
                merged_model,
                merge_policy.clone(),
            )?),
            None => None,
        }
    };

    if let Some(merged_model) = merged_model {
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
        let device = project.runtime_device();
        let materialized_model =
            project.load_model_artifact(merged_model, &artifact, store, &device)?;
        let evaluation = project.evaluate(&materialized_model, EvalSplit::Validation);
        let merged_head = HeadDescriptor {
            head_id: merged_head_id,
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: Some(base_head_id.clone()),
            global_step: expected_global_step,
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
    let evaluation = project.evaluate(candidate.model, EvalSplit::Validation);
    if candidate.head.parent_head_id.as_ref() == Some(base_head_id)
        && candidate.head.global_step == expected_global_step
    {
        return Ok((
            candidate.peer_id.clone(),
            candidate.head.clone(),
            evaluation,
        ));
    }
    let rebased_head_id = HeadId::new(format!(
        "{}-{}-canonical-window-{}",
        experiment.experiment_id.as_str(),
        candidate.peer_id.as_str(),
        window_id.0
    ));
    let artifact = project.materialize_model_artifact(
        candidate.model,
        ArtifactKind::FullHead,
        rebased_head_id.clone(),
        Some(base_head_id.clone()),
        store,
    )?;
    let device = project.runtime_device();
    let materialized_model =
        project.load_model_artifact(project.init_model(&device), &artifact, store, &device)?;
    let evaluation = project.evaluate(&materialized_model, EvalSplit::Validation);
    Ok((
        candidate.peer_id.clone(),
        HeadDescriptor {
            head_id: rebased_head_id,
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: Some(base_head_id.clone()),
            global_step: expected_global_step,
            created_at: Utc::now(),
            metrics: evaluation.metrics.clone(),
        },
        evaluation,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn select_reducer_authority_head<P>(
    project: &mut P,
    experiment: &ExperimentHandle,
    store: &FsArtifactStore,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    base_head_id: &HeadId,
    window_id: WindowId,
    base_model: &P::Model,
    candidate_models: &[ValidationCandidateView<'_, P::Model>],
    merge_policy: MergePolicy,
    local_peer_id: &PeerId,
) -> anyhow::Result<(PeerId, HeadDescriptor, MetricReport)>
where
    P: P2pWorkload,
{
    let source_peer_id = candidate_models
        .iter()
        .max_by(|left, right| {
            (left.sample_weight * left.quality_weight)
                .partial_cmp(&(right.sample_weight * right.quality_weight))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|candidate| candidate.peer_id.clone())
        .unwrap_or_else(|| local_peer_id.clone());
    let expected_global_step = current_head
        .as_ref()
        .map(|(_, head)| head.global_step + 1)
        .unwrap_or(0);
    let merged_model = {
        let merge_candidates = candidate_models
            .iter()
            .map(|candidate| MergeModelCandidate {
                peer_id: candidate.peer_id,
                head_id: &candidate.head.head_id,
                artifact_id: &candidate.head.artifact_id,
                model: candidate.model,
                sample_weight: candidate.sample_weight,
                quality_weight: candidate.quality_weight,
            })
            .collect::<Vec<_>>();
        project
            .merge_candidate_models(base_model, &merge_candidates, merge_policy.clone())?
            .map(|merged_model| {
                project.apply_single_root_ema(base_model, merged_model, merge_policy.clone())
            })
            .transpose()?
    };
    let best_index = candidate_models
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| {
            (left.sample_weight * left.quality_weight)
                .partial_cmp(&(right.sample_weight * right.quality_weight))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(index, _)| index)
        .ok_or_else(|| anyhow::anyhow!("reducer-authority promotion requires candidates"))?;
    let head_suffix = if merged_model.is_some() {
        format!("reducer-canonical-window-{}", window_id.0)
    } else {
        format!(
            "{}-reducer-canonical-window-{}",
            candidate_models[best_index].peer_id.as_str(),
            window_id.0,
        )
    };
    let promoted_head_id = HeadId::new(format!(
        "{}-{}",
        experiment.experiment_id.as_str(),
        head_suffix,
    ));
    let artifact = match merged_model.as_ref() {
        Some(merged_model) => project.materialize_model_artifact(
            merged_model,
            ArtifactKind::FullHead,
            promoted_head_id.clone(),
            Some(base_head_id.clone()),
            store,
        )?,
        None => project.materialize_model_artifact(
            candidate_models[best_index].model,
            ArtifactKind::FullHead,
            promoted_head_id.clone(),
            Some(base_head_id.clone()),
            store,
        )?,
    };
    let device = project.runtime_device();
    let materialized_model = project.load_model_artifact(
        merged_model.unwrap_or_else(|| project.init_model(&device)),
        &artifact,
        store,
        &device,
    )?;
    let evaluation = project.evaluate(&materialized_model, EvalSplit::Validation);
    Ok((
        source_peer_id,
        HeadDescriptor {
            head_id: promoted_head_id,
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: Some(base_head_id.clone()),
            global_step: expected_global_step,
            created_at: Utc::now(),
            metrics: evaluation.metrics.clone(),
        },
        evaluation,
    ))
}
