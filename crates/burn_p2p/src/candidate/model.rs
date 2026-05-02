use super::*;

fn empty_metric_report() -> MetricReport {
    MetricReport {
        metrics: BTreeMap::new(),
        captured_at: Utc::now(),
    }
}

pub(crate) fn load_validation_base_model<P>(
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
    let descriptor = args.store.load_manifest(&head.artifact_id)?;
    let model = project.load_model_artifact(
        project.init_model(args.device),
        &descriptor,
        args.store,
        args.device,
    )?;
    let (evaluation, canary_report) = if args.evaluate_candidates {
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
        return Ok((
            candidate.peer_id.clone(),
            candidate.head.clone(),
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
            metrics: BTreeMap::new(),
        },
        empty_metric_report(),
    ))
}
