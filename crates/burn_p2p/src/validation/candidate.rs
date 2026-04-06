use super::*;

pub(super) struct ValidationCandidate<M> {
    pub peer_id: PeerId,
    pub head: HeadDescriptor,
    pub evaluation: MetricReport,
    pub canary_report: CanaryEvalReport,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: M,
}

pub(super) struct ValidationCandidateLoadArgs<'a, D> {
    pub experiment: &'a ExperimentHandle,
    pub store: &'a FsArtifactStore,
    pub device: &'a D,
    pub updates: &'a [UpdateAnnounce],
    pub current_head: &'a Option<(PeerId, HeadDescriptor)>,
    pub canary_threshold: f64,
}

pub(super) fn collect_validation_candidate_heads(
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
            let provider_peer_id = announcement
                .provider_peer_id
                .clone()
                .unwrap_or_else(|| peer_id.clone());
            if provider_peer_id == *local_peer_id {
                continue;
            }
            if expected_parent_head_id != announcement.head.parent_head_id.as_ref() {
                continue;
            }
            candidate_heads.push((provider_peer_id, announcement.head.clone()));
        }
    }
    candidate_heads
}

pub(super) fn load_validation_base_model<P>(
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

pub(super) fn load_validation_candidate_models<P>(
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

pub(super) fn fallback_best_candidate_index<M>(
    candidate_models: &[ValidationCandidate<M>],
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
pub(super) fn select_validation_head<P>(
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
