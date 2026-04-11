use super::*;

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

pub(in crate::validation) fn collect_validation_candidate_heads(
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
