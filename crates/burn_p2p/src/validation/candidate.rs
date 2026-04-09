use super::*;

pub(super) struct ValidationCandidate<M> {
    pub peer_id: PeerId,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
    pub evaluation: MetricReport,
    pub canary_report: CanaryEvalReport,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: M,
}

#[derive(Clone, Copy)]
pub(super) struct ValidationCandidateView<'a, M> {
    pub peer_id: &'a PeerId,
    pub head: &'a HeadDescriptor,
    pub update: &'a UpdateAnnounce,
    pub evaluation: &'a MetricReport,
    pub canary_report: &'a CanaryEvalReport,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: &'a M,
}

impl<'a, M> From<&'a ValidationCandidate<M>> for ValidationCandidateView<'a, M> {
    fn from(candidate: &'a ValidationCandidate<M>) -> Self {
        Self {
            peer_id: &candidate.peer_id,
            head: &candidate.head,
            update: &candidate.update,
            evaluation: &candidate.evaluation,
            canary_report: &candidate.canary_report,
            sample_weight: candidate.sample_weight,
            quality_weight: candidate.quality_weight,
            model: &candidate.model,
        }
    }
}

pub(super) struct ValidationCandidateLoadArgs<'a, D> {
    pub experiment: &'a ExperimentHandle,
    pub store: &'a FsArtifactStore,
    pub device: &'a D,
    pub current_head: &'a Option<(PeerId, HeadDescriptor)>,
    pub canary_threshold: f64,
}

pub(super) struct ValidationCandidateHead {
    pub origin_peer_id: PeerId,
    pub provider_peer_ids: Vec<PeerId>,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
}

fn synthesize_candidate_head(
    experiment: &ExperimentHandle,
    expected_parent_head_id: Option<&HeadId>,
    update: &UpdateAnnounce,
    local_peer_id: &PeerId,
    fallback_global_step: u64,
) -> Option<ValidationCandidateHead> {
    let provider_peer_ids = dedupe_peer_ids(
        std::iter::once(update.peer_id.clone())
            .chain(update.providers.iter().cloned())
            .filter(|provider_peer_id| provider_peer_id != local_peer_id),
    );
    if provider_peer_ids.is_empty() {
        return None;
    }

    Some(ValidationCandidateHead {
        origin_peer_id: update.peer_id.clone(),
        provider_peer_ids,
        update: update.clone(),
        head: HeadDescriptor {
            head_id: HeadId::new(format!(
                "{}-{}-window-{}",
                experiment.experiment_id.as_str(),
                update.peer_id.as_str(),
                update.window_id.0
            )),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: update.delta_artifact_id.clone(),
            parent_head_id: expected_parent_head_id.cloned(),
            global_step: fallback_global_step,
            created_at: update.announced_at,
            metrics: BTreeMap::new(),
        },
    })
}

fn experiment_updates_from_snapshots(
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> Vec<UpdateAnnounce> {
    let mut updates = BTreeMap::<(PeerId, WindowId, HeadId, ArtifactId), UpdateAnnounce>::new();
    for (_, snapshot) in snapshots {
        for announcement in &snapshot.update_announcements {
            let update = &announcement.update;
            if update.study_id != experiment.study_id
                || update.experiment_id != experiment.experiment_id
                || update.revision_id != experiment.revision_id
            {
                continue;
            }
            let key = (
                update.peer_id.clone(),
                update.window_id,
                update.base_head_id.clone(),
                update.delta_artifact_id.clone(),
            );
            match updates.get(&key) {
                Some(existing) if existing.announced_at >= update.announced_at => {}
                _ => {
                    updates.insert(key, update.clone());
                }
            }
        }
    }
    updates.into_values().collect()
}

fn matching_candidate_head_for_update(
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_peer_id: &PeerId,
    expected_parent_head_id: Option<&HeadId>,
    expected_global_step: u64,
    update: &UpdateAnnounce,
) -> Option<ValidationCandidateHead> {
    let mut matches = snapshots
        .iter()
        .flat_map(|(peer_id, snapshot)| {
            snapshot
                .head_announcements
                .iter()
                .filter_map(|announcement| {
                    if !matches_experiment_head(&announcement.head, experiment) {
                        return None;
                    }
                    if expected_parent_head_id != announcement.head.parent_head_id.as_ref() {
                        return None;
                    }
                    if announcement.head.artifact_id != update.delta_artifact_id {
                        return None;
                    }
                    let provider_peer_id = announcement
                        .provider_peer_id
                        .clone()
                        .unwrap_or_else(|| peer_id.clone());
                    if provider_peer_id == *local_peer_id {
                        return None;
                    }
                    let provider_rank = if provider_peer_id == update.peer_id {
                        0_u8
                    } else if update.providers.contains(&provider_peer_id) {
                        1_u8
                    } else {
                        2_u8
                    };
                    Some((
                        provider_rank,
                        std::cmp::Reverse(announcement.announced_at),
                        provider_peer_id,
                        announcement.head.clone(),
                    ))
                })
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.1.cmp(&right.1))
            .then(left.2.cmp(&right.2))
            .then(left.3.head_id.cmp(&right.3.head_id))
    });
    if let Some((_, _, _, head)) = matches.first().cloned() {
        let provider_peer_ids = dedupe_peer_ids(
            std::iter::once(update.peer_id.clone())
                .chain(update.providers.iter().cloned())
                .chain(matches.into_iter().filter_map(
                    |(_, _, provider_peer_id, candidate_head)| {
                        (candidate_head.head_id == head.head_id
                            && candidate_head.artifact_id == head.artifact_id)
                            .then_some(provider_peer_id)
                    },
                ))
                .filter(|provider_peer_id| provider_peer_id != local_peer_id),
        );
        Some(ValidationCandidateHead {
            origin_peer_id: update.peer_id.clone(),
            provider_peer_ids,
            head,
            update: update.clone(),
        })
    } else {
        synthesize_candidate_head(
            experiment,
            expected_parent_head_id,
            update,
            local_peer_id,
            expected_global_step,
        )
    }
}

fn collapse_speculative_candidate_update(
    root_update: &UpdateAnnounce,
    chained_updates: &[UpdateAnnounce],
    latest_provider_peer_ids: &[PeerId],
) -> UpdateAnnounce {
    let latest = chained_updates
        .last()
        .expect("candidate chain should contain at least one update");
    if chained_updates.len() == 1 {
        let mut update = root_update.clone();
        update.providers = latest_provider_peer_ids.to_vec();
        return update;
    }

    let sample_weight = chained_updates
        .iter()
        .map(|update| update.sample_weight)
        .sum::<f64>();
    let weighted_quality_sum = chained_updates
        .iter()
        .map(|update| update.sample_weight.max(0.0) * update.quality_weight)
        .sum::<f64>();
    let quality_weight = if sample_weight > 0.0 {
        weighted_quality_sum / sample_weight
    } else {
        chained_updates
            .iter()
            .map(|update| update.quality_weight)
            .sum::<f64>()
            / chained_updates.len() as f64
    };
    let mut receipt_ids = chained_updates
        .iter()
        .flat_map(|update| update.receipt_ids.clone())
        .collect::<Vec<_>>();
    receipt_ids.sort();
    receipt_ids.dedup();

    UpdateAnnounce {
        peer_id: root_update.peer_id.clone(),
        study_id: root_update.study_id.clone(),
        experiment_id: root_update.experiment_id.clone(),
        revision_id: root_update.revision_id.clone(),
        window_id: root_update.window_id,
        base_head_id: root_update.base_head_id.clone(),
        lease_id: chained_updates
            .iter()
            .all(|update| update.lease_id == latest.lease_id)
            .then(|| latest.lease_id.clone())
            .flatten(),
        delta_artifact_id: latest.delta_artifact_id.clone(),
        sample_weight,
        quality_weight,
        norm_stats: UpdateNormStats {
            l2_norm: chained_updates
                .iter()
                .map(|update| update.norm_stats.l2_norm)
                .sum(),
            max_abs: chained_updates
                .iter()
                .map(|update| update.norm_stats.max_abs)
                .fold(0.0_f64, f64::max),
            clipped: chained_updates
                .iter()
                .any(|update| update.norm_stats.clipped),
            non_finite_tensors: chained_updates
                .iter()
                .map(|update| update.norm_stats.non_finite_tensors)
                .sum(),
        },
        feature_sketch: None,
        receipt_root: latest.receipt_root.clone(),
        receipt_ids,
        providers: latest_provider_peer_ids.to_vec(),
        announced_at: latest.announced_at,
    }
}

pub(super) fn collect_validation_candidate_heads(
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_peer_id: &PeerId,
    expected_parent_head_id: Option<&HeadId>,
    expected_global_step: u64,
    updates: &[UpdateAnnounce],
) -> Vec<ValidationCandidateHead> {
    let all_updates = experiment_updates_from_snapshots(experiment, snapshots);
    let mut candidate_heads = Vec::new();
    for update in updates {
        let Some(mut candidate) = matching_candidate_head_for_update(
            experiment,
            snapshots,
            local_peer_id,
            expected_parent_head_id,
            expected_global_step,
            update,
        ) else {
            continue;
        };
        let mut chained_updates = vec![candidate.update.clone()];
        let mut latest_window_id = candidate.update.window_id;

        loop {
            let descendants = all_updates
                .iter()
                .filter(|next| {
                    next.peer_id == candidate.origin_peer_id
                        && next.base_head_id == candidate.head.head_id
                        && next.window_id > latest_window_id
                })
                .cloned()
                .collect::<Vec<_>>();
            if descendants.len() != 1 {
                break;
            }
            let next_update = descendants[0].clone();
            let Some(next_candidate) = matching_candidate_head_for_update(
                experiment,
                snapshots,
                local_peer_id,
                Some(&candidate.head.head_id),
                candidate.head.global_step.saturating_add(1),
                &next_update,
            ) else {
                break;
            };
            if next_candidate.head.parent_head_id.as_ref() != Some(&candidate.head.head_id) {
                break;
            }
            latest_window_id = next_update.window_id;
            chained_updates.push(next_update);
            candidate = next_candidate;
        }

        candidate.update = collapse_speculative_candidate_update(
            update,
            &chained_updates,
            &candidate.provider_peer_ids,
        );
        candidate_heads.push(candidate);
    }
    candidate_heads.sort_by(|left, right| {
        left.origin_peer_id
            .cmp(&right.origin_peer_id)
            .then(
                left.provider_peer_ids
                    .first()
                    .cmp(&right.provider_peer_ids.first()),
            )
            .then(left.head.head_id.cmp(&right.head.head_id))
            .then(left.head.artifact_id.cmp(&right.head.artifact_id))
    });
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

pub(super) fn load_validation_candidate_model<P>(
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
    let evaluation = project.evaluate(&model, EvalSplit::Validation);
    let quality = if update.quality_weight.is_finite() {
        update.quality_weight
    } else {
        1.0
    };
    let sample_weight = update.sample_weight.max(0.0);
    let canary_report = build_validation_canary_report(
        args.experiment,
        args.current_head,
        &head,
        &evaluation,
        args.canary_threshold,
        2,
    )?;
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

pub(super) fn fallback_best_candidate_index<M>(
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
pub(super) fn select_validation_head<P>(
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
) -> anyhow::Result<(PeerId, HeadDescriptor, MetricReport)>
where
    P: P2pWorkload,
{
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
    let evaluation = project.evaluate(candidate.model, EvalSplit::Validation);
    let expected_global_step = current_head
        .as_ref()
        .map(|(_, head)| head.global_step + 1)
        .unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_heads_keep_all_known_provider_peers_for_selected_head() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let overlay = experiment.overlay_set().expect("overlay").heads;
        let announced_at = Utc::now();
        let head = HeadDescriptor {
            head_id: HeadId::new("head-a"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-a"),
            parent_head_id: Some(HeadId::new("head-base")),
            global_step: 2,
            created_at: announced_at,
            metrics: BTreeMap::new(),
        };
        let update = UpdateAnnounce {
            peer_id: PeerId::new("trainer-a"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(2),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-a")),
            delta_artifact_id: head.artifact_id.clone(),
            sample_weight: 8.0,
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
            providers: vec![PeerId::new("provider-b"), PeerId::new("provider-c")],
            announced_at,
        };
        let snapshots = vec![
            (
                PeerId::new("observer-a"),
                ControlPlaneSnapshot {
                    head_announcements: vec![HeadAnnouncement {
                        overlay: overlay.clone(),
                        head: head.clone(),
                        provider_peer_id: Some(PeerId::new("provider-b")),
                        announced_at,
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            ),
            (
                PeerId::new("observer-b"),
                ControlPlaneSnapshot {
                    head_announcements: vec![HeadAnnouncement {
                        overlay,
                        head: head.clone(),
                        provider_peer_id: Some(PeerId::new("provider-d")),
                        announced_at,
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            ),
        ];

        let candidates = collect_validation_candidate_heads(
            &experiment,
            &snapshots,
            &PeerId::new("validator-a"),
            Some(&HeadId::new("head-base")),
            2,
            &[update],
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0]
                .provider_peer_ids
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["trainer-a", "provider-b", "provider-c", "provider-d"]
        );
    }

    #[test]
    fn candidate_heads_can_be_synthesized_from_updates_without_head_announcements() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let update = UpdateAnnounce {
            peer_id: PeerId::new("trainer-a"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(4),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-a")),
            delta_artifact_id: ArtifactId::new("artifact-a"),
            sample_weight: 8.0,
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
            providers: vec![PeerId::new("provider-b")],
            announced_at: Utc::now(),
        };

        let candidates = collect_validation_candidate_heads(
            &experiment,
            &[],
            &PeerId::new("validator-a"),
            Some(&HeadId::new("head-base")),
            7,
            &[update],
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].head.head_id,
            HeadId::new("exp-a-trainer-a-window-4")
        );
        assert_eq!(
            candidates[0].head.parent_head_id,
            Some(HeadId::new("head-base"))
        );
        assert_eq!(candidates[0].head.global_step, 7);
        assert_eq!(
            candidates[0]
                .provider_peer_ids
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["trainer-a", "provider-b"]
        );
    }

    #[test]
    fn candidate_heads_collapse_linear_same_peer_descendants_into_rooted_candidate() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
        };
        let overlay = experiment.overlay_set().expect("overlay").heads;
        let announced_at = Utc::now();
        let root_update = UpdateAnnounce {
            peer_id: PeerId::new("trainer-a"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(4),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-a")),
            delta_artifact_id: ArtifactId::new("artifact-a"),
            sample_weight: 8.0,
            quality_weight: 0.8,
            norm_stats: UpdateNormStats {
                l2_norm: 1.0,
                max_abs: 1.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: Some(UpdateFeatureSketch {
                artifact_size_bytes: 1,
                global_norm: 1.0,
                per_layer_norms: BTreeMap::new(),
                random_projection: vec![1.0],
                sign_projection: vec![1],
                top_k_indices: Vec::new(),
                cosine_to_reference: None,
                sign_agreement_fraction: None,
                canary_loss_delta: None,
                historical_deviation_score: None,
                neighbor_distance: None,
                staleness_windows: 0,
                receive_delay_ms: 0,
                non_finite_tensor_count: 0,
            }),
            receipt_root: ContentId::new("receipt-root-a"),
            receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
            providers: vec![PeerId::new("provider-a")],
            announced_at,
        };
        let descendant_update = UpdateAnnounce {
            peer_id: PeerId::new("trainer-a"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(5),
            base_head_id: HeadId::new("head-a"),
            lease_id: Some(LeaseId::new("lease-b")),
            delta_artifact_id: ArtifactId::new("artifact-b"),
            sample_weight: 6.0,
            quality_weight: 0.4,
            norm_stats: UpdateNormStats {
                l2_norm: 2.0,
                max_abs: 3.0,
                clipped: true,
                non_finite_tensors: 1,
            },
            feature_sketch: Some(UpdateFeatureSketch {
                artifact_size_bytes: 1,
                global_norm: 2.0,
                per_layer_norms: BTreeMap::new(),
                random_projection: vec![2.0],
                sign_projection: vec![1],
                top_k_indices: Vec::new(),
                cosine_to_reference: None,
                sign_agreement_fraction: None,
                canary_loss_delta: None,
                historical_deviation_score: None,
                neighbor_distance: None,
                staleness_windows: 0,
                receive_delay_ms: 0,
                non_finite_tensor_count: 1,
            }),
            receipt_root: ContentId::new("receipt-root-b"),
            receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
            providers: vec![PeerId::new("provider-b"), PeerId::new("provider-c")],
            announced_at: announced_at + chrono::Duration::milliseconds(50),
        };
        let snapshots = vec![(
            PeerId::new("observer-a"),
            ControlPlaneSnapshot {
                head_announcements: vec![
                    HeadAnnouncement {
                        overlay: overlay.clone(),
                        head: HeadDescriptor {
                            head_id: HeadId::new("head-a"),
                            study_id: experiment.study_id.clone(),
                            experiment_id: experiment.experiment_id.clone(),
                            revision_id: experiment.revision_id.clone(),
                            artifact_id: root_update.delta_artifact_id.clone(),
                            parent_head_id: Some(HeadId::new("head-base")),
                            global_step: 7,
                            created_at: announced_at,
                            metrics: BTreeMap::new(),
                        },
                        provider_peer_id: Some(PeerId::new("provider-a")),
                        announced_at,
                    },
                    HeadAnnouncement {
                        overlay,
                        head: HeadDescriptor {
                            head_id: HeadId::new("head-b"),
                            study_id: experiment.study_id.clone(),
                            experiment_id: experiment.experiment_id.clone(),
                            revision_id: experiment.revision_id.clone(),
                            artifact_id: descendant_update.delta_artifact_id.clone(),
                            parent_head_id: Some(HeadId::new("head-a")),
                            global_step: 8,
                            created_at: descendant_update.announced_at,
                            metrics: BTreeMap::new(),
                        },
                        provider_peer_id: Some(PeerId::new("provider-c")),
                        announced_at: descendant_update.announced_at,
                    },
                ],
                update_announcements: vec![
                    UpdateEnvelopeAnnouncement {
                        overlay: experiment.overlay_set().expect("overlay").heads.clone(),
                        update: root_update.clone(),
                    },
                    UpdateEnvelopeAnnouncement {
                        overlay: experiment.overlay_set().expect("overlay").heads,
                        update: descendant_update.clone(),
                    },
                ],
                ..ControlPlaneSnapshot::default()
            },
        )];

        let candidates = collect_validation_candidate_heads(
            &experiment,
            &snapshots,
            &PeerId::new("validator-a"),
            Some(&HeadId::new("head-base")),
            7,
            &[root_update],
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].head.head_id, HeadId::new("head-b"));
        assert_eq!(candidates[0].update.base_head_id, HeadId::new("head-base"));
        assert_eq!(candidates[0].update.window_id, WindowId(4));
        assert_eq!(
            candidates[0].update.delta_artifact_id,
            ArtifactId::new("artifact-b")
        );
        assert_eq!(candidates[0].update.sample_weight, 14.0);
        assert!(
            (candidates[0].update.quality_weight - (8.8 / 14.0)).abs() < 1e-9,
            "unexpected quality weight: {}",
            candidates[0].update.quality_weight
        );
        assert_eq!(candidates[0].update.norm_stats.l2_norm, 3.0);
        assert_eq!(candidates[0].update.norm_stats.max_abs, 3.0);
        assert!(candidates[0].update.feature_sketch.is_none());
        assert_eq!(
            candidates[0].update.receipt_ids,
            vec![
                ContributionReceiptId::new("receipt-a"),
                ContributionReceiptId::new("receipt-b")
            ]
        );
        assert_eq!(
            candidates[0]
                .provider_peer_ids
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["trainer-a", "provider-b", "provider-c"]
        );
    }
}
