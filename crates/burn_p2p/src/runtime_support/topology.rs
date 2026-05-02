use super::*;

pub(crate) fn connected_peer_ids(snapshot: &NodeTelemetrySnapshot) -> BTreeSet<PeerId> {
    snapshot.connected_peer_ids.clone()
}

pub(crate) fn experiment_snapshot_peer_ids(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> BTreeSet<PeerId> {
    let mut peer_ids = BTreeSet::new();
    peer_ids.extend(
        snapshot
            .control_plane
            .head_announcements
            .iter()
            .filter(|announcement| matches_experiment_head(&announcement.head, experiment))
            .filter_map(|announcement| announcement.provider_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .lease_announcements
            .iter()
            .filter(|announcement| {
                announcement.lease.study_id == experiment.study_id
                    && announcement.lease.experiment_id == experiment.experiment_id
                    && announcement.lease.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.lease.peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .update_announcements
            .iter()
            .filter(|announcement| {
                announcement.update.study_id == experiment.study_id
                    && announcement.update.experiment_id == experiment.experiment_id
                    && announcement.update.revision_id == experiment.revision_id
            })
            .flat_map(|announcement| {
                std::iter::once(announcement.update.peer_id.clone())
                    .chain(announcement.update.providers.iter().cloned())
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .merge_window_announcements
            .iter()
            .filter(|announcement| {
                announcement.merge_window.study_id == experiment.study_id
                    && announcement.merge_window.experiment_id == experiment.experiment_id
                    && announcement.merge_window.revision_id == experiment.revision_id
            })
            .flat_map(|announcement| {
                announcement
                    .merge_window
                    .validators
                    .iter()
                    .cloned()
                    .chain(announcement.merge_window.reducers.iter().cloned())
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .aggregate_proposal_announcements
            .iter()
            .filter(|announcement| {
                announcement.proposal.study_id == experiment.study_id
                    && announcement.proposal.experiment_id == experiment.experiment_id
                    && announcement.proposal.revision_id == experiment.revision_id
            })
            .flat_map(|announcement| {
                std::iter::once(announcement.proposal.reducer_peer_id.clone())
                    .chain(announcement.proposal.providers.iter().cloned())
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .reduction_certificate_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.certificate.promoter_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .validation_quorum_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .flat_map(|announcement| {
                std::iter::once(announcement.certificate.coordinator.clone()).chain(
                    announcement
                        .certificate
                        .attesting_validators
                        .iter()
                        .cloned(),
                )
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .trainer_promotion_attestation_announcements
            .iter()
            .filter(|announcement| {
                announcement.attestation.study_id == experiment.study_id
                    && announcement.attestation.experiment_id == experiment.experiment_id
                    && announcement.attestation.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.attestation.attester_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .diffusion_promotion_certificate_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .flat_map(|announcement| {
                std::iter::once(announcement.certificate.promoter_peer_id.clone())
                    .chain(announcement.certificate.attesting_trainers.iter().cloned())
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .merge_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.certificate.promoter_peer_id.clone()),
    );

    if peer_ids.is_empty() {
        connected_peer_ids(snapshot)
    } else {
        peer_ids
    }
}

pub(crate) fn prioritized_experiment_snapshot_peer_ids(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> Vec<PeerId> {
    let mut prioritized = Vec::new();

    if let Some(announcement) = snapshot
        .control_plane
        .merge_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
        })
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
    {
        prioritized.push(announcement.certificate.promoter_peer_id.clone());
    }

    if let Some(announcement) = snapshot
        .control_plane
        .validation_quorum_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
        })
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
    {
        prioritized.push(announcement.certificate.coordinator.clone());
        prioritized.extend(
            announcement
                .certificate
                .attesting_validators
                .iter()
                .cloned(),
        );
    }

    let mut diffusion_certificates = snapshot
        .control_plane
        .diffusion_promotion_certificate_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
        })
        .collect::<Vec<_>>();
    diffusion_certificates.sort_by(|left, right| {
        right
            .certificate
            .window_id
            .cmp(&left.certificate.window_id)
            .then(
                right
                    .certificate
                    .settled_at
                    .cmp(&left.certificate.settled_at),
            )
            .then(right.announced_at.cmp(&left.announced_at))
    });
    for announcement in diffusion_certificates {
        prioritized.push(announcement.certificate.promoter_peer_id.clone());
        prioritized.extend(announcement.certificate.attesting_trainers.iter().cloned());
    }

    let mut proposals = snapshot
        .control_plane
        .aggregate_proposal_announcements
        .iter()
        .filter(|announcement| {
            announcement.proposal.study_id == experiment.study_id
                && announcement.proposal.experiment_id == experiment.experiment_id
                && announcement.proposal.revision_id == experiment.revision_id
        })
        .collect::<Vec<_>>();
    proposals.sort_by(|left, right| {
        right
            .proposal
            .window_id
            .cmp(&left.proposal.window_id)
            .then(right.announced_at.cmp(&left.announced_at))
    });
    for announcement in proposals {
        prioritized.push(announcement.proposal.reducer_peer_id.clone());
        prioritized.extend(announcement.proposal.providers.iter().cloned());
    }

    if let Some(merge_window) =
        latest_merge_window_from_snapshot(&snapshot.control_plane, experiment, None)
    {
        prioritized.extend(merge_window.validators);
        prioritized.extend(merge_window.reducers);
    }

    let mut heads = snapshot
        .control_plane
        .head_announcements
        .iter()
        .filter(|announcement| matches_experiment_head(&announcement.head, experiment))
        .collect::<Vec<_>>();
    heads.sort_by(|left, right| {
        right
            .head
            .global_step
            .cmp(&left.head.global_step)
            .then(right.head.created_at.cmp(&left.head.created_at))
            .then(right.announced_at.cmp(&left.announced_at))
    });
    prioritized.extend(
        heads
            .into_iter()
            .filter_map(|announcement| announcement.provider_peer_id.clone()),
    );

    let mut updates = snapshot
        .control_plane
        .update_announcements
        .iter()
        .filter(|announcement| {
            announcement.update.study_id == experiment.study_id
                && announcement.update.experiment_id == experiment.experiment_id
                && announcement.update.revision_id == experiment.revision_id
        })
        .collect::<Vec<_>>();
    updates.sort_by(|left, right| {
        right
            .update
            .window_id
            .cmp(&left.update.window_id)
            .then(right.update.announced_at.cmp(&left.update.announced_at))
    });
    for announcement in updates {
        prioritized.push(announcement.update.peer_id.clone());
        prioritized.extend(announcement.update.providers.iter().cloned());
    }

    let mut leases = snapshot
        .control_plane
        .lease_announcements
        .iter()
        .filter(|announcement| {
            announcement.lease.study_id == experiment.study_id
                && announcement.lease.experiment_id == experiment.experiment_id
                && announcement.lease.revision_id == experiment.revision_id
        })
        .collect::<Vec<_>>();
    leases.sort_by(|left, right| {
        right
            .lease
            .window_id
            .cmp(&left.lease.window_id)
            .then(right.announced_at.cmp(&left.announced_at))
    });
    prioritized.extend(
        leases
            .into_iter()
            .map(|announcement| announcement.lease.peer_id.clone()),
    );

    prioritized.extend(experiment_snapshot_peer_ids(snapshot, experiment));
    prioritized.extend(connected_peer_ids(snapshot));
    dedupe_peer_ids(prioritized)
}

fn cached_snapshot_peer_ids(snapshot: &NodeTelemetrySnapshot) -> BTreeSet<PeerId> {
    let mut peer_ids = connected_peer_ids(snapshot);
    peer_ids.extend(
        snapshot
            .control_plane
            .auth_announcements
            .iter()
            .map(|announcement| announcement.peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .map(|announcement| announcement.peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .head_announcements
            .iter()
            .filter_map(|announcement| announcement.provider_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .lease_announcements
            .iter()
            .map(|announcement| announcement.lease.peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .update_announcements
            .iter()
            .map(|announcement| announcement.update.peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .aggregate_proposal_announcements
            .iter()
            .map(|announcement| announcement.proposal.reducer_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .reduction_certificate_announcements
            .iter()
            .map(|announcement| announcement.certificate.promoter_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .validation_quorum_announcements
            .iter()
            .map(|announcement| announcement.certificate.coordinator.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .trainer_promotion_attestation_announcements
            .iter()
            .map(|announcement| announcement.attestation.attester_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .diffusion_promotion_certificate_announcements
            .iter()
            .flat_map(|announcement| {
                std::iter::once(announcement.certificate.promoter_peer_id.clone())
                    .chain(announcement.certificate.attesting_trainers.iter().cloned())
            }),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .merge_announcements
            .iter()
            .map(|announcement| announcement.certificate.promoter_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .reducer_assignment_announcements
            .iter()
            .map(|announcement| announcement.assignment.source_peer_id.clone()),
    );
    peer_ids.extend(
        snapshot
            .control_plane
            .reducer_load_announcements
            .iter()
            .map(|announcement| announcement.report.peer_id.clone()),
    );
    peer_ids
}

pub(crate) fn cached_connected_snapshots(
    snapshot: &NodeTelemetrySnapshot,
) -> Vec<(PeerId, ControlPlaneSnapshot)> {
    let aggregate = &snapshot.control_plane;
    cached_snapshot_peer_ids(snapshot)
        .into_iter()
        .map(|peer_id| {
            (
                peer_id.clone(),
                ControlPlaneSnapshot {
                    control_announcements: aggregate.control_announcements.clone(),
                    lifecycle_announcements: aggregate.lifecycle_announcements.clone(),
                    schedule_announcements: aggregate.schedule_announcements.clone(),
                    head_announcements: aggregate
                        .head_announcements
                        .iter()
                        .filter(|announcement| {
                            announcement.provider_peer_id.as_ref() == Some(&peer_id)
                        })
                        .cloned()
                        .collect(),
                    lease_announcements: aggregate.lease_announcements.clone(),
                    merge_announcements: aggregate.merge_announcements.clone(),
                    merge_window_announcements: aggregate.merge_window_announcements.clone(),
                    reducer_assignment_announcements: aggregate
                        .reducer_assignment_announcements
                        .clone(),
                    update_announcements: aggregate
                        .update_announcements
                        .iter()
                        .filter(|announcement| announcement.update.peer_id == peer_id)
                        .cloned()
                        .collect(),
                    trainer_promotion_attestation_announcements: aggregate
                        .trainer_promotion_attestation_announcements
                        .iter()
                        .filter(|announcement| announcement.attestation.attester_peer_id == peer_id)
                        .cloned()
                        .collect(),
                    diffusion_promotion_certificate_announcements: aggregate
                        .diffusion_promotion_certificate_announcements
                        .clone(),
                    aggregate_proposal_announcements: aggregate
                        .aggregate_proposal_announcements
                        .clone(),
                    reduction_certificate_announcements: aggregate
                        .reduction_certificate_announcements
                        .clone(),
                    validation_quorum_announcements: aggregate
                        .validation_quorum_announcements
                        .clone(),
                    reducer_load_announcements: aggregate.reducer_load_announcements.clone(),
                    auth_announcements: aggregate
                        .auth_announcements
                        .iter()
                        .filter(|announcement| announcement.peer_id == peer_id)
                        .cloned()
                        .collect(),
                    directory_announcements: aggregate.directory_announcements.clone(),
                    peer_directory_announcements: aggregate.peer_directory_announcements.clone(),
                    metrics_announcements: aggregate.metrics_announcements.clone(),
                },
            )
        })
        .collect()
}

pub(crate) fn merge_control_plane_snapshot(
    target: &mut ControlPlaneSnapshot,
    remote: &ControlPlaneSnapshot,
) {
    target.merge_from_semantic(remote);
}

pub(crate) fn matches_experiment_head(
    head: &HeadDescriptor,
    experiment: &ExperimentHandle,
) -> bool {
    head.study_id == experiment.study_id
        && head.experiment_id == experiment.experiment_id
        && head.revision_id == experiment.revision_id
}

pub(crate) fn latest_head_from_snapshot(
    snapshot: ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    snapshot
        .head_announcements
        .into_iter()
        .filter(|announcement| matches_experiment_head(&announcement.head, experiment))
        .max_by(|left, right| {
            left.head
                .global_step
                .cmp(&right.head.global_step)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| {
            (
                announcement
                    .provider_peer_id
                    .unwrap_or_else(|| PeerId::new("unknown-provider")),
                announcement.head,
            )
        })
}

pub(crate) fn head_provider_peers(
    primary_provider: Option<&PeerId>,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_control_plane: &ControlPlaneSnapshot,
    local_peer_id: Option<&PeerId>,
    experiment: &ExperimentHandle,
    head: &HeadDescriptor,
) -> Vec<PeerId> {
    fn provider_is_usable(peer_id: &PeerId) -> bool {
        !matches!(peer_id.as_str(), "local" | "unknown-provider")
    }

    fn announcement_matches_head(
        announcement: &HeadAnnouncement,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> bool {
        matches_experiment_head(&announcement.head, experiment)
            && announcement.head.head_id == head.head_id
            && announcement.head.artifact_id == head.artifact_id
    }

    dedupe_peer_ids(
        primary_provider
            .into_iter()
            .filter(|peer_id| provider_is_usable(peer_id))
            .cloned()
            .chain(
                local_control_plane
                    .head_announcements
                    .iter()
                    .filter(|announcement| {
                        announcement_matches_head(announcement, experiment, head)
                    })
                    .filter_map(|announcement| {
                        announcement
                            .provider_peer_id
                            .as_ref()
                            .or(local_peer_id)
                            .filter(|peer_id| provider_is_usable(peer_id))
                            .cloned()
                    }),
            )
            .chain(snapshots.iter().flat_map(|(snapshot_peer_id, snapshot)| {
                snapshot
                    .head_announcements
                    .iter()
                    .filter(move |announcement| {
                        announcement_matches_head(announcement, experiment, head)
                    })
                    .filter_map(move |announcement| {
                        let peer_id = announcement
                            .provider_peer_id
                            .as_ref()
                            .unwrap_or(snapshot_peer_id)
                            .clone();
                        provider_is_usable(&peer_id).then_some(peer_id)
                    })
            })),
    )
}

fn latest_remote_head(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    strongest_remote_promoted_head(snapshots, experiment).or_else(|| {
        snapshots
            .iter()
            .filter_map(|(_, snapshot)| latest_head_from_snapshot(snapshot.clone(), experiment))
            .max_by(|left, right| {
                left.1
                    .global_step
                    .cmp(&right.1.global_step)
                    .then(left.1.created_at.cmp(&right.1.created_at))
            })
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LagAssessment {
    pub state: LagState,
    pub head_lag_steps: u64,
}

pub(crate) fn assess_head_lag(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    lag_policy: &LagPolicy,
) -> anyhow::Result<LagAssessment> {
    let local_global_step = load_head_state(storage, experiment)?
        .map(|head| head.global_step)
        .unwrap_or(0);
    let remote_global_step = latest_remote_head(snapshots, experiment)
        .map(|(_, head)| head.global_step)
        .unwrap_or(local_global_step);
    let head_lag_steps = remote_global_step.saturating_sub(local_global_step);
    let state = if head_lag_steps == 0 {
        LagState::Current
    } else if head_lag_steps <= lag_policy.max_head_lag_before_catchup {
        LagState::SlightlyBehind
    } else if head_lag_steps <= lag_policy.max_head_lag_before_block {
        LagState::CatchupRequired
    } else if head_lag_steps <= lag_policy.max_head_lag_before_full_rebase {
        LagState::LeaseBlocked
    } else {
        LagState::RebaseRequired
    };

    Ok(LagAssessment {
        state,
        head_lag_steps,
    })
}

fn latest_merge_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
) -> Option<MergeCertificate> {
    snapshot
        .merge_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
        })
        .max_by(|left, right| {
            left.certificate
                .issued_at
                .cmp(&right.certificate.issued_at)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| announcement.certificate.clone())
}

fn head_for_diffusion_certificate(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    certificate: &DiffusionPromotionCertificate,
) -> Option<(PeerId, HeadDescriptor)> {
    snapshots
        .iter()
        .flat_map(|(peer_id, snapshot)| {
            snapshot
                .head_announcements
                .iter()
                .filter(move |announcement| {
                    announcement.head.head_id == certificate.merged_head_id
                        && announcement.head.artifact_id == certificate.merged_artifact_id
                })
                .map(move |announcement| {
                    (
                        announcement
                            .provider_peer_id
                            .clone()
                            .unwrap_or_else(|| peer_id.clone()),
                        announcement.head.clone(),
                    )
                })
        })
        .max_by(|left, right| {
            (left.0 == certificate.promoter_peer_id)
                .cmp(&(right.0 == certificate.promoter_peer_id))
                .then(left.1.global_step.cmp(&right.1.global_step))
                .then(left.1.created_at.cmp(&right.1.created_at))
        })
}

fn strongest_diffusion_certificate_from_snapshots(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
) -> Vec<DiffusionPromotionCertificate> {
    let mut winners = BTreeMap::<(WindowId, HeadId), DiffusionPromotionCertificate>::new();
    for certificate in snapshots.iter().flat_map(|(_, snapshot)| {
        snapshot
            .diffusion_promotion_certificate_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.certificate.clone())
            .collect::<Vec<_>>()
    }) {
        let key = (certificate.window_id, certificate.base_head_id.clone());
        match winners.get(&key) {
            Some(existing)
                if compare_diffusion_certificate_strength(existing, &certificate).is_ge() => {}
            _ => {
                winners.insert(key, certificate);
            }
        }
    }
    winners.into_values().collect()
}

fn compare_diffusion_certificate_strength(
    left: &DiffusionPromotionCertificate,
    right: &DiffusionPromotionCertificate,
) -> std::cmp::Ordering {
    left.attester_count
        .cmp(&right.attester_count)
        .then_with(|| {
            left.cumulative_sample_weight
                .total_cmp(&right.cumulative_sample_weight)
        })
        .then(left.settled_at.cmp(&right.settled_at))
        .then_with(|| right.merged_head_id.cmp(&left.merged_head_id))
}

fn head_for_merge_certificate(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    merge: &MergeCertificate,
) -> Option<(PeerId, HeadDescriptor)> {
    snapshots
        .iter()
        .flat_map(|(peer_id, snapshot)| {
            snapshot
                .head_announcements
                .iter()
                .filter(move |announcement| {
                    announcement.head.head_id == merge.merged_head_id
                        && announcement.head.artifact_id == merge.merged_artifact_id
                })
                .map(move |announcement| {
                    (
                        announcement
                            .provider_peer_id
                            .clone()
                            .unwrap_or_else(|| peer_id.clone()),
                        announcement.head.clone(),
                    )
                })
        })
        .max_by(|left, right| {
            (left.0 == merge.promoter_peer_id)
                .cmp(&(right.0 == merge.promoter_peer_id))
                .then(left.1.global_step.cmp(&right.1.global_step))
                .then(left.1.created_at.cmp(&right.1.created_at))
        })
}

fn latest_merge_with_head_from_snapshots<'a>(
    snapshots: &'a [(PeerId, ControlPlaneSnapshot)],
    experiment: &'a ExperimentHandle,
) -> impl Iterator<Item = (MergeCertificate, (PeerId, HeadDescriptor))> + 'a {
    snapshots.iter().filter_map(move |(_, snapshot)| {
        let merge = latest_merge_from_snapshot(snapshot, experiment)?;
        let head = head_for_merge_certificate(snapshots, &merge)?;
        Some((merge, head))
    })
}

fn strongest_remote_promoted_head(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    let remote_diffusion = strongest_diffusion_certificate_from_snapshots(snapshots, experiment)
        .into_iter()
        .filter_map(|certificate| {
            head_for_diffusion_certificate(snapshots, &certificate).map(|head| (certificate, head))
        })
        .max_by(|left, right| {
            left.1
                .1
                .global_step
                .cmp(&right.1.1.global_step)
                .then_with(|| compare_diffusion_certificate_strength(&left.0, &right.0))
                .then(left.1.1.created_at.cmp(&right.1.1.created_at))
                .then_with(|| right.1.1.head_id.cmp(&left.1.1.head_id))
        })
        .map(|(_, head)| head);
    let remote_merged =
        latest_merge_with_head_from_snapshots(snapshots, experiment).max_by(|left, right| {
            left.1
                .1
                .global_step
                .cmp(&right.1.1.global_step)
                .then(left.1.1.created_at.cmp(&right.1.1.created_at))
        });

    match (remote_diffusion, remote_merged) {
        (Some(diffusion), Some((merge, merged_head)))
            if !matches!(
                merge.promotion_mode,
                HeadPromotionMode::DiffusionSteadyState
            ) && merged_head.1.global_step > diffusion.1.global_step =>
        {
            Some(merged_head)
        }
        (Some(diffusion), _) => Some(diffusion),
        (None, Some((_, merged_head))) => Some(merged_head),
        (None, None) => None,
    }
}

pub fn latest_promoted_head_from_control_plane(
    control_plane: &ControlPlaneSnapshot,
    local_peer_id: Option<&PeerId>,
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    let snapshot_peer_id = local_peer_id
        .cloned()
        .unwrap_or_else(|| PeerId::new("local"));
    strongest_remote_promoted_head(&[(snapshot_peer_id, control_plane.clone())], experiment)
}

pub(crate) fn snapshots_with_local_control_plane(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_peer_id: Option<&PeerId>,
    local_snapshot: &ControlPlaneSnapshot,
) -> Vec<(PeerId, ControlPlaneSnapshot)> {
    let mut combined = snapshots.to_vec();
    if let Some(local_peer_id) = local_peer_id {
        combined.push((local_peer_id.clone(), local_snapshot.clone()));
    }
    combined
}

pub(crate) fn resolve_canonical_head(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> anyhow::Result<Option<(PeerId, HeadDescriptor)>> {
    let mut best = load_head_state(storage, experiment)?.map(|head| (PeerId::new("local"), head));
    if let Some(remote_merged) = strongest_remote_promoted_head(snapshots, experiment) {
        let replace = best
            .as_ref()
            .map(|(_, head)| remote_merged.1.global_step >= head.global_step)
            .unwrap_or(true);
        if replace {
            best = Some(remote_merged);
        }
    }

    if best.is_none() {
        best = snapshots
            .iter()
            .filter_map(|(_, snapshot)| latest_head_from_snapshot(snapshot.clone(), experiment))
            .filter(|(_, head)| head.global_step == 0 || head.parent_head_id.is_none())
            .max_by(|left, right| {
                left.1
                    .global_step
                    .cmp(&right.1.global_step)
                    .then(left.1.created_at.cmp(&right.1.created_at))
            });
    }

    Ok(best)
}

pub(crate) fn metric_quality(metrics: &BTreeMap<String, MetricValue>) -> f64 {
    if let Some(MetricValue::Float(loss)) = metrics.get("loss") {
        return *loss;
    }
    if let Some(MetricValue::Integer(loss)) = metrics.get("loss") {
        return *loss as f64;
    }
    if let Some(MetricValue::Float(loss)) = metrics.get("train_loss") {
        return *loss;
    }
    if let Some(MetricValue::Integer(loss)) = metrics.get("train_loss") {
        return *loss as f64;
    }
    if let Some(MetricValue::Float(score)) = metrics.get("score") {
        return -*score;
    }
    if let Some(MetricValue::Float(accuracy)) = metrics.get("accuracy") {
        return -*accuracy;
    }

    0.0
}

fn numeric_metric_values(metrics: &BTreeMap<String, MetricValue>) -> Vec<f64> {
    metrics
        .values()
        .filter_map(|value| match value {
            MetricValue::Integer(value) => Some(*value as f64),
            MetricValue::Float(value) => Some(*value),
            MetricValue::Bool(_) | MetricValue::Text(_) => None,
        })
        .collect()
}

pub(crate) fn update_feature_sketch_from_metrics(
    metrics: &BTreeMap<String, MetricValue>,
    reference_metrics: Option<&BTreeMap<String, MetricValue>>,
    sketch_dimensionality: usize,
    staleness_windows: u16,
    receive_delay_ms: u64,
    canary_loss_delta: Option<f64>,
) -> UpdateFeatureSketch {
    let values = numeric_metric_values(metrics);
    let reference = reference_metrics.map(numeric_metric_values);
    burn_p2p_security::extract_feature_sketch(
        &values,
        reference.as_deref(),
        &[burn_p2p_security::FeatureLayer::new(
            "metrics",
            0,
            values.len(),
        )],
        sketch_dimensionality.max(1),
        staleness_windows,
        receive_delay_ms,
        canary_loss_delta,
    )
}

fn lifecycle_announcements_for_experiment<'a>(
    snapshot: &'a ControlPlaneSnapshot,
    network_id: &'a NetworkId,
    study_id: &'a StudyId,
    experiment_id: &'a ExperimentId,
) -> impl Iterator<Item = &'a ExperimentLifecycleAnnouncement> + 'a {
    snapshot
        .lifecycle_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.network_id == *network_id
                && announcement.certificate.body.payload.payload.plan.study_id == *study_id
                && announcement
                    .certificate
                    .body
                    .payload
                    .payload
                    .plan
                    .experiment_id
                    == *experiment_id
        })
}

fn schedule_announcements_for_network<'a>(
    snapshot: &'a ControlPlaneSnapshot,
    network_id: &'a NetworkId,
) -> impl Iterator<Item = &'a FleetScheduleAnnouncement> + 'a {
    snapshot
        .schedule_announcements
        .iter()
        .filter(|announcement| announcement.certificate.network_id == *network_id)
}

fn latest_matching_lifecycle_target_entry(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
) -> Option<ExperimentDirectoryEntry> {
    lifecycle_announcements_for_experiment(
        snapshot,
        &experiment.network_id,
        &experiment.study_id,
        &experiment.experiment_id,
    )
    .filter(|announcement| {
        let plan = &announcement.certificate.body.payload.payload.plan;
        plan.phase.authorizes_switch()
            && plan.target_entry.current_revision_id == experiment.revision_id
    })
    .max_by(|left, right| {
        left.certificate
            .activation
            .activation_window
            .cmp(&right.certificate.activation.activation_window)
            .then(
                left.certificate
                    .body
                    .payload
                    .payload
                    .plan
                    .plan_epoch
                    .cmp(&right.certificate.body.payload.payload.plan.plan_epoch),
            )
            .then(left.announced_at.cmp(&right.announced_at))
    })
    .map(|announcement| {
        announcement
            .certificate
            .body
            .payload
            .payload
            .plan
            .target_entry
            .clone()
    })
}

pub(crate) fn experiment_has_lifecycle_plan(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
    study_id: &StudyId,
    experiment_id: &ExperimentId,
) -> bool {
    lifecycle_announcements_for_experiment(snapshot, network_id, study_id, experiment_id)
        .next()
        .is_some()
}

pub(crate) fn effective_experiment_lifecycle_plan(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
    study_id: &StudyId,
    experiment_id: &ExperimentId,
    activation_window: WindowId,
) -> Option<ExperimentLifecyclePlan> {
    lifecycle_announcements_for_experiment(snapshot, network_id, study_id, experiment_id)
        .filter(|announcement| {
            announcement
                .certificate
                .body
                .payload
                .payload
                .plan
                .is_effective_for_window(activation_window)
        })
        .max_by(|left, right| {
            left.certificate
                .activation
                .activation_window
                .cmp(&right.certificate.activation.activation_window)
                .then(
                    left.certificate
                        .body
                        .payload
                        .payload
                        .plan
                        .plan_epoch
                        .cmp(&right.certificate.body.payload.payload.plan.plan_epoch),
                )
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| announcement.certificate.body.payload.payload.plan.clone())
}

pub(crate) fn effective_fleet_schedule_epoch(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
    activation_window: WindowId,
) -> Option<FleetScheduleEpoch> {
    schedule_announcements_for_network(snapshot, network_id)
        .filter(|announcement| {
            announcement
                .certificate
                .body
                .payload
                .payload
                .epoch
                .is_effective_for_window(activation_window)
        })
        .max_by(|left, right| {
            left.certificate
                .activation
                .activation_window
                .cmp(&right.certificate.activation.activation_window)
                .then(
                    left.certificate
                        .body
                        .payload
                        .payload
                        .epoch
                        .plan_epoch
                        .cmp(&right.certificate.body.payload.payload.epoch.plan_epoch),
                )
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| announcement.certificate.body.payload.payload.epoch.clone())
}

pub(crate) fn active_experiment_directory_entry(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> Option<ExperimentDirectoryEntry> {
    config
        .auth
        .as_ref()
        .and_then(|auth| {
            auth.experiment_directory.iter().find(|entry| {
                entry.network_id == experiment.network_id
                    && entry.study_id == experiment.study_id
                    && entry.experiment_id == experiment.experiment_id
                    && entry.current_revision_id == experiment.revision_id
            })
        })
        .cloned()
        .or_else(|| latest_matching_lifecycle_target_entry(&snapshot.control_plane, experiment))
        .or_else(|| {
            snapshot
                .control_plane
                .directory_announcements
                .iter()
                .filter(|announcement| announcement.network_id == experiment.network_id)
                .flat_map(|announcement| announcement.entries.iter())
                .find(|entry| {
                    entry.network_id == experiment.network_id
                        && entry.study_id == experiment.study_id
                        && entry.experiment_id == experiment.experiment_id
                        && entry.current_revision_id == experiment.revision_id
                })
                .cloned()
        })
}

pub(crate) fn runtime_robustness_policy(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> RobustnessPolicy {
    active_experiment_directory_entry(config, snapshot, experiment)
        .and_then(|entry| entry.robustness_policy())
        .unwrap_or_default()
}

pub(crate) fn update_norm_stats(metrics: &BTreeMap<String, MetricValue>) -> UpdateNormStats {
    let values = numeric_metric_values(metrics);
    let l2_norm = values.iter().map(|value| value * value).sum::<f64>().sqrt();
    let max_abs = values
        .iter()
        .map(|value| value.abs())
        .fold(0.0_f64, f64::max);
    let non_finite_tensors = values.iter().filter(|value| !value.is_finite()).count() as u32;

    UpdateNormStats {
        l2_norm,
        max_abs,
        clipped: false,
        non_finite_tensors,
    }
}

pub(crate) fn latest_merge_window_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> Option<MergeWindowState> {
    snapshot
        .merge_window_announcements
        .iter()
        .filter(|announcement| {
            announcement.merge_window.study_id == experiment.study_id
                && announcement.merge_window.experiment_id == experiment.experiment_id
                && announcement.merge_window.revision_id == experiment.revision_id
                && base_head_id
                    .map(|head_id| &announcement.merge_window.base_head_id == head_id)
                    .unwrap_or(true)
        })
        .max_by(|left, right| {
            left.merge_window
                .window_id
                .cmp(&right.merge_window.window_id)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| normalize_merge_window_state(announcement.merge_window.clone()))
}

pub(crate) fn latest_merge_window_from_connected_snapshots(
    local_snapshot: &ControlPlaneSnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> Option<MergeWindowState> {
    let mut best = latest_merge_window_from_snapshot(local_snapshot, experiment, base_head_id);
    for (_, snapshot) in snapshots {
        let Some(candidate) = latest_merge_window_from_snapshot(snapshot, experiment, base_head_id)
        else {
            continue;
        };
        let replace = best
            .as_ref()
            .map(|current| {
                candidate.window_id > current.window_id
                    || (candidate.window_id == current.window_id
                        && candidate.opened_at > current.opened_at)
            })
            .unwrap_or(true);
        if replace {
            best = Some(candidate);
        }
    }
    best
}

pub(crate) fn latest_reducer_assignment_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    window_id: WindowId,
    source_peer_id: &PeerId,
) -> Option<ReducerAssignment> {
    snapshot
        .reducer_assignment_announcements
        .iter()
        .filter(|announcement| {
            announcement.assignment.window_id == window_id
                && &announcement.assignment.source_peer_id == source_peer_id
        })
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| announcement.assignment.clone())
}

fn update_announces_for_window(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: &HeadId,
) -> Vec<UpdateAnnounce> {
    let mut updates = BTreeMap::<PeerId, UpdateAnnounce>::new();
    for announcement in &snapshot.update_announcements {
        if announcement.update.study_id != experiment.study_id
            || announcement.update.experiment_id != experiment.experiment_id
            || announcement.update.revision_id != experiment.revision_id
            || announcement.update.window_id != window_id
            || &announcement.update.base_head_id != base_head_id
        {
            continue;
        }
        updates.insert(
            announcement.update.peer_id.clone(),
            announcement.update.clone(),
        );
    }

    updates.into_values().collect()
}

pub(crate) fn update_announces_from_connected_snapshots(
    local_snapshot: &ControlPlaneSnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: &HeadId,
) -> Vec<UpdateAnnounce> {
    let mut updates = BTreeMap::<PeerId, UpdateAnnounce>::new();
    for update in update_announces_for_window(local_snapshot, experiment, window_id, base_head_id) {
        updates.insert(update.peer_id.clone(), update);
    }
    for (_, snapshot) in snapshots {
        for update in update_announces_for_window(snapshot, experiment, window_id, base_head_id) {
            updates.insert(update.peer_id.clone(), update);
        }
    }
    updates.into_values().collect()
}

pub(crate) fn runtime_training_protocol(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> TrainingProtocol {
    active_experiment_directory_entry(config, snapshot, experiment)
        .map(|entry| entry.training_protocol())
        .unwrap_or_default()
}

pub(crate) fn runtime_merge_topology_policy(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> MergeTopologyPolicy {
    latest_merge_window_from_snapshot(&snapshot.control_plane, experiment, base_head_id)
        .map(|merge_window| merge_window.policy)
        .or_else(|| {
            active_experiment_directory_entry(config, snapshot, experiment)
                .and_then(|entry| entry.merge_topology_policy())
        })
        .map(normalize_merge_topology_policy)
        .unwrap_or_default()
}

pub(crate) fn runtime_topology_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_reducer_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if local_roles.contains(&PeerRole::Reducer) && peer_is_reducer_eligible(snapshot, local_peer_id)
    {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() && local_roles.contains(&PeerRole::Reducer) {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_training_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut candidates = connected_peer_ids(snapshot);
    candidates.extend(
        snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .map(|announcement| announcement.peer_id.clone()),
    );
    candidates.extend(
        snapshot
            .control_plane
            .lease_announcements
            .iter()
            .map(|announcement| announcement.lease.peer_id.clone()),
    );
    let mut peers = candidates
        .into_iter()
        .filter(|peer_id| peer_is_trainer_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if local_roles_allow_training(local_roles) && peer_is_trainer_eligible(snapshot, local_peer_id)
    {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() && local_roles_allow_training(local_roles) {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_validator_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_validator_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if (local_roles.contains(&PeerRole::Validator) || local_roles.contains(&PeerRole::Authority))
        && peer_is_validator_eligible(snapshot, local_peer_id)
    {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty()
        && (local_roles.contains(&PeerRole::Validator)
            || local_roles.contains(&PeerRole::Authority))
    {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_validators(
    roles: &PeerRoleSet,
    local_peer_id: &PeerId,
    peers: &[PeerId],
    quorum: u16,
) -> Vec<PeerId> {
    let mut validators = Vec::new();
    let local_in_pool = peers.iter().any(|peer_id| peer_id == local_peer_id);
    if local_in_pool
        && (roles.contains(&PeerRole::Validator) || roles.contains(&PeerRole::Authority))
    {
        validators.push(local_peer_id.clone());
    }
    for peer_id in peers {
        if peer_id == local_peer_id
            && !(roles.contains(&PeerRole::Validator) || roles.contains(&PeerRole::Authority))
        {
            continue;
        }
        if !validators.contains(peer_id) {
            validators.push(peer_id.clone());
        }
        if validators.len() >= usize::from(quorum.max(1)) {
            break;
        }
    }
    if validators.is_empty() {
        validators.push(
            peers
                .first()
                .cloned()
                .unwrap_or_else(|| local_peer_id.clone()),
        );
    }
    validators
}

fn local_roles_allow_training(roles: &PeerRoleSet) -> bool {
    roles.contains(&PeerRole::TrainerGpu)
        || roles.contains(&PeerRole::TrainerCpu)
        || roles.contains(&PeerRole::BrowserTrainerWgpu)
        || roles.contains(&PeerRole::BrowserTrainer)
}

pub(crate) fn open_runtime_merge_window(
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: HeadId,
    policy: MergeTopologyPolicy,
    reducers: Vec<PeerId>,
    validators: Vec<PeerId>,
) -> anyhow::Result<MergeWindowState> {
    let policy = normalize_merge_topology_policy(policy);
    ensure_runtime_merge_strategy_supported(&policy)?;
    let reducers = runtime_window_reducers(&base_head_id, window_id, &policy, &reducers);
    let validators = match policy.promotion_policy.mode {
        HeadPromotionMode::ValidatorQuorum => validators,
        HeadPromotionMode::ReducerAuthority | HeadPromotionMode::DiffusionSteadyState => Vec::new(),
    };
    let opened_at = Utc::now();
    let closes_at = opened_at + chrono::Duration::seconds(i64::from(policy.window_duration_secs));
    Ok(MergeWindowState {
        merge_window_id: ContentId::derive(&(
            experiment.network_id.as_str(),
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            window_id.0,
            base_head_id.as_str(),
        ))?,
        network_id: experiment.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id,
        base_head_id,
        policy,
        reducers,
        validators,
        opened_at,
        closes_at,
    })
}

fn normalize_merge_topology_policy(mut policy: MergeTopologyPolicy) -> MergeTopologyPolicy {
    match policy.promotion_policy.mode {
        HeadPromotionMode::ValidatorQuorum => {}
        HeadPromotionMode::ReducerAuthority => {
            policy.reducer_replication = 1;
            policy.promotion_policy.validator_quorum = 1;
            policy.promotion_policy.diffusion = None;
        }
        HeadPromotionMode::DiffusionSteadyState => {
            policy.reducer_replication = 0;
            policy.upper_fanin = 0;
            policy.promotion_policy.validator_quorum = 0;
            policy.promotion_policy.diffusion = Some(
                policy
                    .promotion_policy
                    .diffusion
                    .clone()
                    .unwrap_or_default(),
            );
        }
    }
    policy
}

fn ensure_runtime_merge_strategy_supported(policy: &MergeTopologyPolicy) -> anyhow::Result<()> {
    if !matches!(
        policy.promotion_policy.mode,
        HeadPromotionMode::DiffusionSteadyState
    ) {
        return Ok(());
    }

    match policy.strategy {
        MergeStrategy::GlobalBroadcastBaseline
        | MergeStrategy::RandomPeerGossip
        | MergeStrategy::KRegularGossip
        | MergeStrategy::LocalGossipPlusPeriodicGlobal => Ok(()),
        MergeStrategy::CentralReducerBaseline
        | MergeStrategy::FixedTreeReduce
        | MergeStrategy::RotatingRendezvousTree
        | MergeStrategy::ReplicatedRendezvousDag
        | MergeStrategy::MicrocohortReducePlusValidatorPromotion => Err(anyhow::anyhow!(
            "diffusion steady-state promotion requires a trainer-only gossip or broadcast topology"
        )),
    }
}

fn reducer_authority_rank(base_head_id: &HeadId, window_id: WindowId, peer_id: &PeerId) -> String {
    ContentId::derive(&(
        "reducer-authority",
        base_head_id.as_str(),
        window_id.0,
        peer_id.as_str(),
    ))
    .map(|content_id| content_id.as_str().to_owned())
    .unwrap_or_else(|_| peer_id.as_str().to_owned())
}

pub(crate) fn runtime_window_reducers(
    base_head_id: &HeadId,
    window_id: WindowId,
    policy: &MergeTopologyPolicy,
    reducers: &[PeerId],
) -> Vec<PeerId> {
    if matches!(
        policy.promotion_policy.mode,
        HeadPromotionMode::DiffusionSteadyState
    ) {
        return Vec::new();
    }
    if !matches!(
        policy.promotion_policy.mode,
        HeadPromotionMode::ReducerAuthority
    ) {
        return reducers.to_vec();
    }

    reducers
        .iter()
        .min_by(|left, right| {
            reducer_authority_rank(base_head_id, window_id, left)
                .cmp(&reducer_authority_rank(base_head_id, window_id, right))
                .then(left.cmp(right))
        })
        .cloned()
        .into_iter()
        .collect()
}

fn normalize_merge_window_state(mut merge_window: MergeWindowState) -> MergeWindowState {
    merge_window.policy = normalize_merge_topology_policy(merge_window.policy);
    merge_window.reducers = runtime_window_reducers(
        &merge_window.base_head_id,
        merge_window.window_id,
        &merge_window.policy,
        &merge_window.reducers,
    );
    if matches!(
        merge_window.policy.promotion_policy.mode,
        HeadPromotionMode::ReducerAuthority | HeadPromotionMode::DiffusionSteadyState
    ) {
        merge_window.validators.clear();
    }
    if matches!(
        merge_window.policy.promotion_policy.mode,
        HeadPromotionMode::DiffusionSteadyState
    ) {
        merge_window.reducers.clear();
    }
    merge_window
}

pub(crate) fn runtime_assign_reducers(
    merge_window: &MergeWindowState,
    source_peer_id: &PeerId,
    reducer_pool: &[PeerId],
) -> anyhow::Result<ReducerAssignment> {
    match burn_p2p_experiment::assign_reducers(merge_window, source_peer_id, reducer_pool) {
        Ok(assignment) => Ok(assignment),
        Err(_) => {
            let mut assigned_reducers = reducer_pool
                .iter()
                .take(usize::from(merge_window.policy.reducer_replication.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if assigned_reducers.is_empty() {
                assigned_reducers.push(source_peer_id.clone());
            }

            let mut repair_reducers = reducer_pool
                .iter()
                .skip(assigned_reducers.len())
                .take(usize::from(merge_window.policy.reducer_replication.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if repair_reducers.is_empty() {
                repair_reducers = assigned_reducers.clone();
            }

            let mut upper_tier_reducers = reducer_pool
                .iter()
                .filter(|peer_id| !assigned_reducers.contains(*peer_id))
                .take(usize::from(merge_window.policy.upper_fanin.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if upper_tier_reducers.is_empty() {
                upper_tier_reducers = assigned_reducers.clone();
            }

            Ok(ReducerAssignment {
                assignment_id: ContentId::derive(&(
                    merge_window.merge_window_id.as_str(),
                    source_peer_id.as_str(),
                    &assigned_reducers,
                    &repair_reducers,
                    &upper_tier_reducers,
                ))?,
                window_id: merge_window.window_id,
                source_peer_id: source_peer_id.clone(),
                assigned_reducers,
                repair_reducers,
                upper_tier_reducers,
                assigned_at: Utc::now(),
            })
        }
    }
}
