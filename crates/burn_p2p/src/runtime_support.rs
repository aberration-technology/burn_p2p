use super::*;
use crate::config::{
    ClientReenrollmentStatus, RuntimeCommand, TrustBundleState, default_node_runtime_state,
};
use crate::handles::dedupe_peer_ids;
use burn_p2p_core::{
    FleetPlacementPeer, FleetPlacementSnapshot, SignatureAlgorithm, SignatureMetadata,
};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use sysinfo::{Disks, System};
mod control_plane;
mod persistence;
mod trust;

pub(crate) use control_plane::run_control_plane;
pub(crate) use persistence::{
    inferred_next_window_id, load_head_state, load_json, load_known_peers,
    load_latest_merge_certificate, load_limit_profile, load_primary_slot_assignment,
    persist_auth_state, persist_control_plane_state, persist_head_state,
    persist_in_flight_transfer_states, persist_json, persist_known_peers, persist_limit_profile,
    persist_primary_slot_assignment, persist_runtime_binding_state, persist_runtime_security_state,
    persist_window_id, remove_artifact_transfer_state, restore_auth_config,
    restore_control_plane_state, restore_in_flight_transfer_states, restore_runtime_binding_config,
    restore_runtime_security_state,
};
#[cfg(test)]
pub(crate) use persistence::{
    load_artifact_transfer_state, load_scoped_lease_announcement, persist_artifact_transfer_state,
};
use persistence::{
    persist_local_peer_auth, seed_shell_control_plane_state, sync_control_plane_snapshot,
};
pub(crate) use trust::verify_snapshot_admission;
use trust::{
    admission_rejection_reason, note_admitted_peer, note_rejected_peer, peer_is_reducer_eligible,
    peer_is_trainer_eligible, peer_is_validator_eligible, prune_tracked_peer_security_state,
    reconcile_live_revocation_policy, reconcile_remote_trust_bundle,
};

pub(crate) fn runtime_limit_policy(estimate: &CapabilityEstimate) -> LimitPolicy {
    LimitPolicy {
        target_window_seconds: estimate.target_window_seconds.max(1),
        browser_target_window_seconds: estimate.target_window_seconds.max(1),
        ..LimitPolicy::default()
    }
}

fn storage_probe_root(config: &NodeConfig) -> Option<PathBuf> {
    config
        .storage
        .as_ref()
        .map(|storage| storage.root.clone())
        .or_else(|| std::env::current_dir().ok())
}

fn available_disk_bytes_for_path(path: &Path) -> Option<u64> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let disks = Disks::new_with_refreshed_list();
    disks
        .iter()
        .filter(|disk| canonical.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .map(|disk| disk.available_space())
}

fn runtime_resource_probe(config: &NodeConfig) -> CapabilityResourceProbe {
    let mut system = System::new();
    system.refresh_memory();

    CapabilityResourceProbe {
        device_memory_bytes: None,
        system_memory_bytes: Some(system.total_memory()),
        disk_bytes: storage_probe_root(config)
            .as_deref()
            .and_then(available_disk_bytes_for_path),
        upload_mbps: None,
        download_mbps: None,
    }
}

fn runtime_limit_profile_from_benchmark(
    peer_id: &PeerId,
    config: &NodeConfig,
    estimate: &CapabilityEstimate,
    reported_at: DateTime<Utc>,
) -> anyhow::Result<LimitProfile> {
    let persistence = if config.storage.is_some() {
        burn_p2p_core::PersistenceClass::Durable
    } else {
        burn_p2p_core::PersistenceClass::Session
    };
    let probe = native_probe_from_estimate(
        peer_id.clone(),
        estimate,
        runtime_resource_probe(config),
        persistence,
        burn_p2p_core::AttestationLevel::None,
        None,
    );

    CapabilityCalibrator::new(runtime_limit_policy(estimate))?
        .calibrate(probe, reported_at)
        .map_err(anyhow::Error::from)
}

pub(crate) fn effective_limit_profile(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    peer_id: &PeerId,
    config: &NodeConfig,
    estimate: &CapabilityEstimate,
    reported_at: DateTime<Utc>,
) -> anyhow::Result<LimitProfile> {
    let calibrator = CapabilityCalibrator::new(runtime_limit_policy(estimate))?;
    let profile = match load_limit_profile(storage, experiment)? {
        Some(profile)
            if profile.card.peer_id == *peer_id
                && profile.card.platform == burn_p2p_core::ClientPlatform::Native
                && profile.card.preferred_backends == estimate.preferred_backends =>
        {
            calibrator.recalibrate_profile(&profile, reported_at)?
        }
        _ => runtime_limit_profile_from_benchmark(peer_id, config, estimate, reported_at)?,
    };
    persist_limit_profile(storage, experiment, &profile)?;
    Ok(profile)
}

pub(crate) fn set_effective_limit_profile(
    telemetry: &TelemetryHandle,
    profile: Option<LimitProfile>,
) {
    let mut snapshot = lock_telemetry_state(&telemetry.state);
    snapshot.effective_limit_profile = profile;
    snapshot.updated_at = Utc::now();
}

pub(crate) fn lock_telemetry_state(
    state: &Arc<Mutex<NodeTelemetrySnapshot>>,
) -> std::sync::MutexGuard<'_, NodeTelemetrySnapshot> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn connected_peer_ids(snapshot: &NodeTelemetrySnapshot) -> BTreeSet<PeerId> {
    snapshot.observed_peer_ids.clone()
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
            .map(|announcement| announcement.certificate.validator.clone()),
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
            .merge_announcements
            .iter()
            .filter(|announcement| {
                announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
            })
            .map(|announcement| announcement.certificate.validator.clone()),
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
        prioritized.push(announcement.certificate.validator.clone());
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
            .map(|announcement| announcement.certificate.validator.clone()),
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
            .merge_announcements
            .iter()
            .map(|announcement| announcement.certificate.validator.clone()),
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

fn latest_remote_head(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    let remote_merged =
        latest_merged_head_from_snapshots(snapshots, experiment).max_by(|left, right| {
            left.1
                .global_step
                .cmp(&right.1.global_step)
                .then(left.1.created_at.cmp(&right.1.created_at))
        });

    remote_merged.or_else(|| {
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
            (left.0 == merge.validator)
                .cmp(&(right.0 == merge.validator))
                .then(left.1.global_step.cmp(&right.1.global_step))
                .then(left.1.created_at.cmp(&right.1.created_at))
        })
}

fn latest_merged_head_from_snapshots<'a>(
    snapshots: &'a [(PeerId, ControlPlaneSnapshot)],
    experiment: &'a ExperimentHandle,
) -> impl Iterator<Item = (PeerId, HeadDescriptor)> + 'a {
    snapshots.iter().filter_map(move |(_, snapshot)| {
        let merge = latest_merge_from_snapshot(snapshot, experiment)?;
        head_for_merge_certificate(snapshots, &merge)
    })
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

    let remote_merged =
        latest_merged_head_from_snapshots(snapshots, experiment).max_by(|left, right| {
            left.1
                .global_step
                .cmp(&right.1.global_step)
                .then(left.1.created_at.cmp(&right.1.created_at))
        });

    if let Some(remote_merged) = remote_merged {
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
        .map(|announcement| announcement.merge_window.clone())
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

pub(crate) fn runtime_merge_topology_policy(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> MergeTopologyPolicy {
    latest_merge_window_from_snapshot(&snapshot.control_plane, experiment, base_head_id)
        .map(|merge_window| merge_window.policy)
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

const TRAINING_PLACEMENT_HINT_FRESHNESS_MINUTES: i64 = 30;
const TRAINING_PLACEMENT_HISTORY_WINDOWS: usize = 8;
const TRAINING_PLACEMENT_HISTORY_DECAY_FACTOR: f64 = 0.65;
const TRAINING_PLACEMENT_FAILURE_STREAK_PRUNE: usize = 2;
const TRAINING_PLACEMENT_MIN_WINDOWS_FOR_HEALTH_PRUNE: usize = 3;
const TRAINING_PLACEMENT_MIN_WEIGHTED_HEALTH: f64 = 0.2;
const FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS: u32 = 15 * 60;
const FLEET_PLACEMENT_HISTORY_HORIZON_SECS: i64 = 6 * 60 * 60;
const FLEET_PLACEMENT_HISTORY_MAX_SNAPSHOTS: usize = 24;
const FLEET_PLACEMENT_HISTORY_DECAY_FACTOR: f64 = 0.82;
const FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE: usize = 4;
const FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS: usize = 2;
const FLEET_PLACEMENT_MIN_SUPPORT_RATIO_FOR_ASSIGNMENT: f64 = 0.2;
const FLEET_PLACEMENT_MIN_CONSENSUS_RATIO_FOR_ASSIGNMENT: f64 = 0.35;
const FLEET_PLACEMENT_MIN_BUDGET_SCALE_FOR_ASSIGNMENT: f64 = 0.55;
const FLEET_PLACEMENT_MIN_MICROSHARD_SCALE_FOR_ASSIGNMENT: f64 = 0.55;
const FLEET_PLACEMENT_PLANNER_VERSION: &str = "burn_p2p.runtime.v1";

#[derive(Clone, Debug)]
struct TrainingPlacementHistory {
    recent_window_count: usize,
    completed_window_count: usize,
    recent_failure_streak: usize,
    latest_status: PeerWindowStatus,
    latest_finished_at: DateTime<Utc>,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
}

#[derive(Clone, Debug)]
struct FleetPlacementPlannerHistory {
    snapshot_count: usize,
    distinct_planner_count: usize,
    selected_planner_count: usize,
    visible_weight: f64,
    selected_weight: f64,
    weighted_rank: f64,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
    weighted_trust: f64,
    weighted_budget_scale: f64,
    weighted_microshard_scale: f64,
    oldest_generated_at: DateTime<Utc>,
    latest_generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
struct FleetPlacementPlannerPeerContribution {
    snapshot_count: usize,
    visible_weight: f64,
    selected_weight: f64,
    weighted_rank: f64,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
    weighted_trust: f64,
    weighted_budget_scale: f64,
    weighted_microshard_scale: f64,
    oldest_generated_at: Option<DateTime<Utc>>,
    latest_generated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TrainingScheduleHint {
    pub budget_scale: f64,
    pub microshard_scale: f64,
}

impl Default for TrainingScheduleHint {
    fn default() -> Self {
        Self {
            budget_scale: 1.0,
            microshard_scale: 1.0,
        }
    }
}

fn training_status_health_score(status: &PeerWindowStatus) -> f64 {
    match status {
        PeerWindowStatus::Completed => 1.0,
        PeerWindowStatus::Aborted => 0.4,
        PeerWindowStatus::Rejected => 0.2,
        PeerWindowStatus::Failed => 0.0,
    }
}

fn recent_training_peer_window_histories(
    snapshot: &NodeTelemetrySnapshot,
    latest_hints: &[PeerWindowPlacementHint],
) -> BTreeMap<PeerId, Vec<PeerWindowPlacementHint>> {
    let freshness_cutoff =
        Utc::now() - chrono::Duration::minutes(TRAINING_PLACEMENT_HINT_FRESHNESS_MINUTES);
    let mut hints = BTreeMap::<PeerId, Vec<PeerWindowPlacementHint>>::new();
    for announcement in &snapshot.control_plane.metrics_announcements {
        for hint in &announcement.peer_window_hints {
            if hint.window_finished_at < freshness_cutoff {
                continue;
            }
            if !matches!(
                hint.role,
                PeerRole::TrainerGpu
                    | PeerRole::TrainerCpu
                    | PeerRole::BrowserTrainerWgpu
                    | PeerRole::BrowserTrainer
            ) {
                continue;
            }
            hints
                .entry(hint.peer_id.clone())
                .or_default()
                .push(hint.clone());
        }
    }
    for hint in latest_hints {
        if hint.window_finished_at < freshness_cutoff {
            continue;
        }
        if !matches!(
            hint.role,
            PeerRole::TrainerGpu
                | PeerRole::TrainerCpu
                | PeerRole::BrowserTrainerWgpu
                | PeerRole::BrowserTrainer
        ) {
            continue;
        }
        hints
            .entry(hint.peer_id.clone())
            .or_default()
            .push(hint.clone());
    }
    for values in hints.values_mut() {
        values.sort_by_key(|hint| std::cmp::Reverse(hint.window_finished_at));
        values.truncate(TRAINING_PLACEMENT_HISTORY_WINDOWS);
    }
    hints
}

fn summarize_training_peer_window_history(
    hints: &[PeerWindowPlacementHint],
) -> Option<TrainingPlacementHistory> {
    let latest = hints.first()?;
    let mut total_weight = 0.0;
    let mut weighted_health = 0.0;
    let mut weighted_head_lag = 0.0;
    let mut weighted_throughput = 0.0;
    let mut completed_window_count = 0usize;

    for (index, hint) in hints.iter().enumerate() {
        let weight = TRAINING_PLACEMENT_HISTORY_DECAY_FACTOR.powi(index as i32);
        total_weight += weight;
        weighted_health += weight * training_status_health_score(&hint.status);
        weighted_head_lag += weight * hint.head_lag_at_finish as f64;
        weighted_throughput += weight
            * ((hint.accepted_tokens_or_samples.max(1) as f64 * 1000.0)
                / hint.window_elapsed_ms.max(1) as f64);
        if hint.status == PeerWindowStatus::Completed {
            completed_window_count += 1;
        }
    }

    let recent_failure_streak = hints
        .iter()
        .take_while(|hint| hint.status != PeerWindowStatus::Completed)
        .count();

    Some(TrainingPlacementHistory {
        recent_window_count: hints.len(),
        completed_window_count,
        recent_failure_streak,
        latest_status: latest.status.clone(),
        latest_finished_at: latest.window_finished_at,
        weighted_health: weighted_health / total_weight.max(f64::EPSILON),
        weighted_head_lag: weighted_head_lag / total_weight.max(f64::EPSILON),
        weighted_throughput: weighted_throughput / total_weight.max(f64::EPSILON),
    })
}

fn peer_trust_score_value(snapshot: &NodeTelemetrySnapshot, peer_id: &PeerId) -> f64 {
    snapshot
        .trust_scores
        .iter()
        .find(|score| &score.peer_id == peer_id)
        .map(|score| score.score)
        .unwrap_or(0.0)
}

fn training_history_priority_rank(history: Option<&TrainingPlacementHistory>) -> u8 {
    match history {
        Some(history)
            if history.latest_status == PeerWindowStatus::Completed
                && history.recent_failure_streak == 0 =>
        {
            3
        }
        Some(history) if history.completed_window_count > 0 => 2,
        None => 1,
        Some(_) => 0,
    }
}

fn compare_training_assignment_peer(
    snapshot: &NodeTelemetrySnapshot,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    planner_histories: &BTreeMap<PeerId, FleetPlacementPlannerHistory>,
    left: &PeerId,
    right: &PeerId,
) -> Ordering {
    let left_history = histories.get(left);
    let right_history = histories.get(right);
    let left_rank = training_history_priority_rank(left_history);
    let right_rank = training_history_priority_rank(right_history);
    if left_rank != right_rank {
        return right_rank.cmp(&left_rank);
    }

    if let (Some(left_history), Some(right_history)) = (left_history, right_history) {
        let left_planner = planner_histories.get(left);
        let right_planner = planner_histories.get(right);
        let consensus_cmp = planner_consensus_ratio(right_planner)
            .total_cmp(&planner_consensus_ratio(left_planner));
        if consensus_cmp != Ordering::Equal {
            return consensus_cmp;
        }
        let distinct_cmp =
            planner_distinct_count(right_planner).cmp(&planner_distinct_count(left_planner));
        if distinct_cmp != Ordering::Equal {
            return distinct_cmp;
        }
        let support_cmp =
            planner_support_ratio(right_planner).total_cmp(&planner_support_ratio(left_planner));
        if support_cmp != Ordering::Equal {
            return support_cmp;
        }
        let rank_cmp =
            planner_average_rank(left_planner).total_cmp(&planner_average_rank(right_planner));
        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }
        let planner_lag_cmp = planner_average_head_lag(left_planner)
            .total_cmp(&planner_average_head_lag(right_planner));
        if planner_lag_cmp != Ordering::Equal {
            return planner_lag_cmp;
        }
        let planner_health_cmp =
            planner_average_health(right_planner).total_cmp(&planner_average_health(left_planner));
        if planner_health_cmp != Ordering::Equal {
            return planner_health_cmp;
        }
        let planner_throughput_cmp = planner_average_throughput(right_planner)
            .total_cmp(&planner_average_throughput(left_planner));
        if planner_throughput_cmp != Ordering::Equal {
            return planner_throughput_cmp;
        }
        let lag_cmp = left_history
            .weighted_head_lag
            .total_cmp(&right_history.weighted_head_lag);
        if lag_cmp != Ordering::Equal {
            return lag_cmp;
        }
        let health_cmp = right_history
            .weighted_health
            .total_cmp(&left_history.weighted_health);
        if health_cmp != Ordering::Equal {
            return health_cmp;
        }
        let throughput_cmp = right_history
            .weighted_throughput
            .total_cmp(&left_history.weighted_throughput);
        if throughput_cmp != Ordering::Equal {
            return throughput_cmp;
        }
        let recency_cmp = left_history
            .latest_finished_at
            .cmp(&right_history.latest_finished_at);
        if recency_cmp != Ordering::Equal {
            return recency_cmp.reverse();
        }
    }

    let left_planner = planner_histories.get(left);
    let right_planner = planner_histories.get(right);
    let consensus_cmp =
        planner_consensus_ratio(right_planner).total_cmp(&planner_consensus_ratio(left_planner));
    if consensus_cmp != Ordering::Equal {
        return consensus_cmp;
    }
    let distinct_cmp =
        planner_distinct_count(right_planner).cmp(&planner_distinct_count(left_planner));
    if distinct_cmp != Ordering::Equal {
        return distinct_cmp;
    }
    let support_cmp =
        planner_support_ratio(right_planner).total_cmp(&planner_support_ratio(left_planner));
    if support_cmp != Ordering::Equal {
        return support_cmp;
    }
    let rank_cmp =
        planner_average_rank(left_planner).total_cmp(&planner_average_rank(right_planner));
    if rank_cmp != Ordering::Equal {
        return rank_cmp;
    }
    let planner_lag_cmp =
        planner_average_head_lag(left_planner).total_cmp(&planner_average_head_lag(right_planner));
    if planner_lag_cmp != Ordering::Equal {
        return planner_lag_cmp;
    }
    let planner_health_cmp =
        planner_average_health(right_planner).total_cmp(&planner_average_health(left_planner));
    if planner_health_cmp != Ordering::Equal {
        return planner_health_cmp;
    }
    let planner_throughput_cmp = planner_average_throughput(right_planner)
        .total_cmp(&planner_average_throughput(left_planner));
    if planner_throughput_cmp != Ordering::Equal {
        return planner_throughput_cmp;
    }
    let planner_trust_cmp =
        planner_average_trust(right_planner).total_cmp(&planner_average_trust(left_planner));
    if planner_trust_cmp != Ordering::Equal {
        return planner_trust_cmp;
    }

    let left_trust = peer_trust_score_value(snapshot, left);
    let right_trust = peer_trust_score_value(snapshot, right);
    let trust_cmp = right_trust.total_cmp(&left_trust);
    if trust_cmp != Ordering::Equal {
        return trust_cmp;
    }

    left.as_str().cmp(right.as_str())
}

fn training_assignment_histories(
    snapshot: &NodeTelemetrySnapshot,
    latest_hints: &[PeerWindowPlacementHint],
) -> BTreeMap<PeerId, TrainingPlacementHistory> {
    recent_training_peer_window_histories(snapshot, latest_hints)
        .into_iter()
        .filter_map(|(peer_id, hints)| {
            summarize_training_peer_window_history(&hints).map(|history| (peer_id, history))
        })
        .collect()
}

pub(crate) fn local_training_adaptation_factor(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> f64 {
    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let lag_penalty = (1.0 / (1.0 + snapshot.head_lag_steps as f64 * 0.2)).clamp(0.2, 1.0);
    let local_history_penalty = histories
        .get(local_peer_id)
        .map(|history| {
            let failure_penalty =
                (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.75)).clamp(0.35, 1.0);
            let completion_penalty = if history.latest_status == PeerWindowStatus::Completed {
                1.0
            } else {
                0.75
            };
            failure_penalty * history.weighted_health.clamp(0.35, 1.0) * completion_penalty
        })
        .unwrap_or(1.0);
    let planner_penalty = planner_histories
        .get(local_peer_id)
        .and_then(|history| {
            (history.snapshot_count >= 3
                && history.distinct_planner_count
                    >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS
                && history.visible_weight > f64::EPSILON)
                .then_some(
                    (planner_support_ratio(Some(history)) * 0.65
                        + planner_consensus_ratio(Some(history)) * 0.35)
                        .clamp(0.35, 1.0),
                )
        })
        .unwrap_or(1.0);
    let planner_backpressure_penalty = (1.0
        - planner_global_backpressure(planner_histories.get(local_peer_id)) * 0.7)
        .clamp(0.25, 1.0);

    (lag_penalty * local_history_penalty * planner_penalty * planner_backpressure_penalty)
        .clamp(0.2, 1.0)
}

fn planner_average_budget_scale(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_budget_scale / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(1.0)
}

fn planner_average_microshard_scale(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_microshard_scale / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(1.0)
}

fn planner_global_backpressure(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    let Some(history) = history else {
        return 0.0;
    };
    if history.snapshot_count < FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE {
        return 0.0;
    }
    let support_gap = (1.0 - planner_support_ratio(Some(history))).clamp(0.0, 1.0);
    let consensus_gap = (1.0 - planner_consensus_ratio(Some(history))).clamp(0.0, 1.0);
    let budget_gap = (1.0 - planner_average_budget_scale(Some(history))).clamp(0.0, 1.0);
    let microshard_gap = (1.0 - planner_average_microshard_scale(Some(history))).clamp(0.0, 1.0);
    (support_gap * 0.3 + consensus_gap * 0.25 + budget_gap * 0.3 + microshard_gap * 0.15)
        .clamp(0.0, 1.0)
}

fn planner_prunes_training_assignment(history: Option<&FleetPlacementPlannerHistory>) -> bool {
    let Some(history) = history else {
        return false;
    };
    history.snapshot_count >= FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE
        && history.distinct_planner_count >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS
        && planner_support_ratio(Some(history)) < FLEET_PLACEMENT_MIN_SUPPORT_RATIO_FOR_ASSIGNMENT
        && planner_consensus_ratio(Some(history))
            < FLEET_PLACEMENT_MIN_CONSENSUS_RATIO_FOR_ASSIGNMENT
        && planner_average_budget_scale(Some(history))
            < FLEET_PLACEMENT_MIN_BUDGET_SCALE_FOR_ASSIGNMENT
        && planner_average_microshard_scale(Some(history))
            < FLEET_PLACEMENT_MIN_MICROSHARD_SCALE_FOR_ASSIGNMENT
}

pub(crate) fn local_training_schedule_hint(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> TrainingScheduleHint {
    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let planner_history = planner_histories.get(local_peer_id);
    let local_history = histories.get(local_peer_id);

    let planner_support = planner_support_ratio(planner_history);
    let planner_consensus = planner_consensus_ratio(planner_history);
    let planner_backpressure = planner_global_backpressure(planner_history);
    let lag_penalty = (1.0 / (1.0 + snapshot.head_lag_steps as f64 * 0.15)).clamp(0.45, 1.0);
    let failure_penalty = local_history
        .map(|history| (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.5)).clamp(0.5, 1.0))
        .unwrap_or(1.0);
    let health_factor = local_history
        .map(|history| history.weighted_health.clamp(0.45, 1.2))
        .unwrap_or(1.0);
    let confidence = planner_scaling_confidence(planner_history);
    let planner_pressure_scale = (1.0 - planner_backpressure * 0.6).clamp(0.4, 1.0);
    let budget_scale = if confidence <= f64::EPSILON {
        1.0
    } else {
        let scaled = 1.0
            + (planner_average_budget_scale(planner_history) - 1.0)
                * confidence
                * (0.65 + planner_consensus * 0.35);
        (scaled
            * lag_penalty
            * failure_penalty
            * health_factor.clamp(0.8, 1.1)
            * planner_pressure_scale)
            .clamp(0.25, 1.6)
    };
    let microshard_scale = if confidence <= f64::EPSILON {
        1.0
    } else {
        let scaled = 1.0
            + (planner_average_microshard_scale(planner_history) - 1.0)
                * confidence
                * (0.65 + planner_support * 0.2 + planner_consensus * 0.15);
        (scaled
            * lag_penalty.sqrt()
            * failure_penalty
            * health_factor.clamp(0.85, 1.05)
            * planner_pressure_scale)
            .clamp(0.25, 1.4)
    };

    TrainingScheduleHint {
        budget_scale,
        microshard_scale,
    }
}

fn filter_training_assignment_peers(
    peers: &[PeerId],
    snapshot: &NodeTelemetrySnapshot,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    planner_histories: &BTreeMap<PeerId, FleetPlacementPlannerHistory>,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    peers
        .iter()
        .filter(|peer_id| {
            if *peer_id == local_peer_id {
                return true;
            }
            if planner_prunes_training_assignment(planner_histories.get(*peer_id)) {
                return false;
            }
            histories
                .get(*peer_id)
                .map(|history| {
                    history.weighted_head_lag
                        <= snapshot.lag_policy.max_head_lag_before_block as f64
                        && !(history.latest_status != PeerWindowStatus::Completed
                            && history.completed_window_count == 0)
                        && history.recent_failure_streak < TRAINING_PLACEMENT_FAILURE_STREAK_PRUNE
                        && !(history.recent_window_count
                            >= TRAINING_PLACEMENT_MIN_WINDOWS_FOR_HEALTH_PRUNE
                            && history.weighted_health < TRAINING_PLACEMENT_MIN_WEIGHTED_HEALTH)
                })
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn recent_verified_fleet_placement_snapshots(
    snapshot: &NodeTelemetrySnapshot,
) -> Vec<&FleetPlacementSnapshot> {
    let now = Utc::now();
    let freshness_cutoff = now - chrono::Duration::seconds(FLEET_PLACEMENT_HISTORY_HORIZON_SECS);
    let mut placements = snapshot
        .control_plane
        .metrics_announcements
        .iter()
        .filter_map(|announcement| announcement.placement_snapshot.as_ref())
        .filter(|placement| {
            !placement.selected_peer_ids.is_empty()
                && !placement.signature_bundle.is_empty()
                && placement.generated_at >= freshness_cutoff
                && fleet_placement_snapshot_is_verified(snapshot, placement)
        })
        .collect::<Vec<_>>();
    placements.sort_by_key(|placement| std::cmp::Reverse(placement.generated_at));
    placements.truncate(FLEET_PLACEMENT_HISTORY_MAX_SNAPSHOTS);
    placements
}

fn latest_planner_public_key_hex<'a>(
    snapshot: &'a NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> Option<&'a str> {
    snapshot
        .control_plane
        .auth_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| {
            announcement
                .envelope
                .certificate
                .claims()
                .peer_public_key_hex
                .as_str()
        })
}

fn fleet_placement_snapshot_is_verified(
    snapshot: &NodeTelemetrySnapshot,
    placement: &FleetPlacementSnapshot,
) -> bool {
    let Some(public_key_hex) = latest_planner_public_key_hex(snapshot, &placement.planner_peer_id)
    else {
        return false;
    };
    let Ok(public_key_bytes) = hex::decode(public_key_hex) else {
        return false;
    };
    let Ok(public_key) = libp2p_identity::PublicKey::try_decode_protobuf(&public_key_bytes) else {
        return false;
    };
    if PeerId::new(libp2p_identity::PeerId::from_public_key(&public_key).to_string())
        != placement.planner_peer_id
    {
        return false;
    }
    let mut unsigned = placement.clone();
    unsigned.signature_bundle.clear();
    let Ok(message) = burn_p2p_core::deterministic_cbor(&unsigned) else {
        return false;
    };
    placement.signature_bundle.iter().any(|signature| {
        signature.signer == placement.planner_peer_id
            && signature.key_id == "runtime-fleet-placement"
            && matches!(signature.algorithm, SignatureAlgorithm::Ed25519)
            && hex::decode(&signature.signature_hex)
                .map(|raw| public_key.verify(&message, &raw))
                .unwrap_or(false)
    })
}

fn placement_snapshot_peer_rank(
    placement: &FleetPlacementSnapshot,
    peer_id: &PeerId,
) -> Option<usize> {
    placement
        .selected_peer_ids
        .iter()
        .position(|candidate| candidate == peer_id)
}

fn planner_history_visibility_rank(
    placement: &FleetPlacementSnapshot,
    peer_id: &PeerId,
) -> Option<usize> {
    if let Some(rank) = placement_snapshot_peer_rank(placement, peer_id) {
        return Some(rank);
    }
    placement
        .ranked_candidates
        .iter()
        .position(|candidate| &candidate.peer_id == peer_id)
        .map(|rank| placement.selected_peer_ids.len() + rank)
}

fn fleet_placement_planner_histories(
    snapshot: &NodeTelemetrySnapshot,
) -> BTreeMap<PeerId, FleetPlacementPlannerHistory> {
    let placements = recent_verified_fleet_placement_snapshots(snapshot);
    let mut planner_contributions =
        BTreeMap::<PeerId, BTreeMap<PeerId, FleetPlacementPlannerPeerContribution>>::new();

    for (index, placement) in placements.into_iter().enumerate() {
        let recency_weight = FLEET_PLACEMENT_HISTORY_DECAY_FACTOR.powi(index as i32);
        let planner_weight = recency_weight
            * (1.0 + peer_trust_score_value(snapshot, &placement.planner_peer_id).clamp(0.0, 1.0));

        for (rank, peer_id) in placement.selected_peer_ids.iter().enumerate() {
            let entry = planner_contributions
                .entry(peer_id.clone())
                .or_default()
                .entry(placement.planner_peer_id.clone())
                .or_default();
            entry.snapshot_count += 1;
            entry.visible_weight += planner_weight;
            entry.selected_weight += planner_weight;
            entry.weighted_rank += planner_weight * rank as f64;
            entry.oldest_generated_at = Some(
                entry
                    .oldest_generated_at
                    .map(|current| current.min(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
            entry.latest_generated_at = Some(
                entry
                    .latest_generated_at
                    .map(|current| current.max(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
        }

        for candidate in &placement.ranked_candidates {
            let rank = planner_history_visibility_rank(placement, &candidate.peer_id)
                .unwrap_or(placement.selected_peer_ids.len() + placement.ranked_candidates.len());
            let entry = planner_contributions
                .entry(candidate.peer_id.clone())
                .or_default()
                .entry(placement.planner_peer_id.clone())
                .or_default();
            if !placement
                .selected_peer_ids
                .iter()
                .any(|peer_id| peer_id == &candidate.peer_id)
            {
                entry.snapshot_count += 1;
                entry.visible_weight += planner_weight;
                entry.weighted_rank += planner_weight * rank as f64;
            }
            entry.weighted_health += planner_weight * candidate.weighted_health;
            entry.weighted_head_lag += planner_weight * candidate.weighted_head_lag;
            entry.weighted_throughput += planner_weight * candidate.weighted_throughput;
            entry.weighted_trust += planner_weight * candidate.trust_score;
            entry.weighted_budget_scale += planner_weight * candidate.recommended_budget_scale;
            entry.weighted_microshard_scale +=
                planner_weight * candidate.recommended_microshard_scale;
            entry.oldest_generated_at = Some(
                entry
                    .oldest_generated_at
                    .map(|current| current.min(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
            entry.latest_generated_at = Some(
                entry
                    .latest_generated_at
                    .map(|current| current.max(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
        }
    }

    planner_contributions
        .into_iter()
        .map(|(peer_id, planners)| {
            let snapshot_count = planners
                .values()
                .map(|contribution| contribution.snapshot_count)
                .sum();
            let distinct_planner_count = planners.len();
            let selected_planner_count = planners
                .values()
                .filter(|contribution| contribution.selected_weight > f64::EPSILON)
                .count();
            let visible_weight = planners
                .values()
                .map(|contribution| contribution.visible_weight)
                .sum();
            let selected_weight = planners
                .values()
                .map(|contribution| contribution.selected_weight)
                .sum();
            let weighted_rank = planners
                .values()
                .map(|contribution| contribution.weighted_rank)
                .sum();
            let weighted_health = planners
                .values()
                .map(|contribution| contribution.weighted_health)
                .sum();
            let weighted_head_lag = planners
                .values()
                .map(|contribution| contribution.weighted_head_lag)
                .sum();
            let weighted_throughput = planners
                .values()
                .map(|contribution| contribution.weighted_throughput)
                .sum();
            let weighted_trust = planners
                .values()
                .map(|contribution| contribution.weighted_trust)
                .sum();
            let weighted_budget_scale = planners
                .values()
                .map(|contribution| contribution.weighted_budget_scale)
                .sum();
            let weighted_microshard_scale = planners
                .values()
                .map(|contribution| contribution.weighted_microshard_scale)
                .sum();
            let oldest_generated_at = planners
                .values()
                .filter_map(|contribution| contribution.oldest_generated_at)
                .min()
                .unwrap_or_else(Utc::now);
            let latest_generated_at = planners
                .values()
                .filter_map(|contribution| contribution.latest_generated_at)
                .max()
                .unwrap_or_else(Utc::now);
            (
                peer_id,
                FleetPlacementPlannerHistory {
                    snapshot_count,
                    distinct_planner_count,
                    selected_planner_count,
                    visible_weight,
                    selected_weight,
                    weighted_rank,
                    weighted_health,
                    weighted_head_lag,
                    weighted_throughput,
                    weighted_trust,
                    weighted_budget_scale,
                    weighted_microshard_scale,
                    oldest_generated_at,
                    latest_generated_at,
                },
            )
        })
        .collect()
}

fn planner_support_ratio(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| {
            if history.visible_weight <= f64::EPSILON {
                0.0
            } else {
                history.selected_weight / history.visible_weight
            }
        })
        .unwrap_or(0.0)
}

fn planner_consensus_ratio(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| {
            if history.distinct_planner_count == 0 {
                0.0
            } else {
                history.selected_planner_count as f64 / history.distinct_planner_count as f64
            }
        })
        .unwrap_or(0.0)
}

fn planner_history_horizon_secs(history: Option<&FleetPlacementPlannerHistory>) -> i64 {
    history
        .map(|history| {
            history
                .latest_generated_at
                .signed_duration_since(history.oldest_generated_at)
                .num_seconds()
                .max(0)
        })
        .unwrap_or(0)
}

fn planner_scaling_confidence(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    let Some(history) = history else {
        return 0.0;
    };
    let support = planner_support_ratio(Some(history));
    let consensus = planner_consensus_ratio(Some(history));
    let horizon_factor = (planner_history_horizon_secs(Some(history)) as f64
        / FLEET_PLACEMENT_HISTORY_HORIZON_SECS as f64)
        .clamp(0.25, 1.0);

    if history.distinct_planner_count >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS {
        (0.35 + support * 0.3 + consensus * 0.35)
            .mul_add(horizon_factor, 0.0)
            .clamp(0.45, 1.0)
    } else {
        (0.15 + support * 0.2)
            .mul_add(horizon_factor, 0.0)
            .clamp(0.1, 0.35)
    }
}

fn planner_distinct_count(history: Option<&FleetPlacementPlannerHistory>) -> usize {
    history
        .map(|history| history.distinct_planner_count)
        .unwrap_or(0)
}

fn planner_average_rank(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_rank / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(f64::MAX)
}

fn planner_average_health(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_health / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn planner_average_head_lag(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_head_lag / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(f64::MAX)
}

fn planner_average_throughput(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_throughput / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn planner_average_trust(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_trust / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn fleet_placement_peer_summary(
    peer_id: &PeerId,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    hints: &BTreeMap<PeerId, Vec<PeerWindowPlacementHint>>,
    trust_score: f64,
    max_weighted_throughput: f64,
) -> Option<FleetPlacementPeer> {
    let history = histories.get(peer_id)?;
    let latest_hint = hints.get(peer_id).and_then(|values| values.first())?;
    let normalized_throughput = if max_weighted_throughput > f64::EPSILON {
        (history.weighted_throughput / max_weighted_throughput).clamp(0.0, 1.5)
    } else {
        1.0
    };
    let lag_penalty = (1.0 / (1.0 + history.weighted_head_lag * 0.25)).clamp(0.4, 1.0);
    let failure_penalty =
        (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.5)).clamp(0.45, 1.0);
    let completion_bias = if history.latest_status == PeerWindowStatus::Completed {
        1.0
    } else {
        0.75
    };
    let recommended_budget_scale = ((0.45
        + history.weighted_health.clamp(0.0, 1.1) * 0.55
        + normalized_throughput * 0.55
        + trust_score.clamp(0.0, 1.0) * 0.15)
        * lag_penalty
        * failure_penalty
        * completion_bias)
        .clamp(0.35, 1.75);
    let recommended_microshard_scale = ((0.45
        + history.weighted_health.clamp(0.0, 1.1) * 0.6
        + normalized_throughput * 0.35
        + trust_score.clamp(0.0, 1.0) * 0.1)
        * lag_penalty.sqrt()
        * failure_penalty
        * completion_bias)
        .clamp(0.35, 1.5);
    Some(FleetPlacementPeer {
        peer_id: peer_id.clone(),
        role: latest_hint.role.clone(),
        backend_class: latest_hint.backend_class.clone(),
        recent_window_count: history.recent_window_count,
        completed_window_count: history.completed_window_count,
        recent_failure_streak: history.recent_failure_streak,
        latest_status: history.latest_status.clone(),
        latest_finished_at: history.latest_finished_at,
        weighted_health: history.weighted_health,
        weighted_head_lag: history.weighted_head_lag,
        weighted_throughput: history.weighted_throughput,
        trust_score,
        recommended_budget_scale,
        recommended_microshard_scale,
    })
}

pub(crate) fn build_fleet_placement_snapshot(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
    latest_hints: &[PeerWindowPlacementHint],
) -> Option<FleetPlacementSnapshot> {
    let network_id = snapshot.network_id.clone()?;
    let peers = runtime_training_peers(snapshot, local_roles, local_peer_id);
    if peers.is_empty() {
        return None;
    }

    let hint_history = recent_training_peer_window_histories(snapshot, latest_hints);
    let histories = hint_history
        .iter()
        .filter_map(|(peer_id, hints)| {
            summarize_training_peer_window_history(hints).map(|history| (peer_id.clone(), history))
        })
        .collect::<BTreeMap<_, _>>();
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let max_weighted_throughput = histories
        .values()
        .map(|history| history.weighted_throughput)
        .fold(0.0, f64::max);

    let mut selected = filter_training_assignment_peers(
        &peers,
        snapshot,
        &histories,
        &planner_histories,
        local_peer_id,
    );
    if selected.is_empty() {
        selected = peers.clone();
    }
    selected.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });

    let mut ranked_peer_ids = peers;
    ranked_peer_ids.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });
    let ranked_candidates = ranked_peer_ids
        .into_iter()
        .filter_map(|peer_id| {
            fleet_placement_peer_summary(
                &peer_id,
                &histories,
                &hint_history,
                peer_trust_score_value(snapshot, &peer_id),
                max_weighted_throughput,
            )
        })
        .collect::<Vec<_>>();

    Some(FleetPlacementSnapshot {
        network_id,
        planner_peer_id: local_peer_id.clone(),
        generated_at: Utc::now(),
        freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
        retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
        selected_peer_ids: selected,
        ranked_candidates,
        planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
        signature_bundle: Vec::new(),
    })
}

pub(crate) fn sign_fleet_placement_snapshot(
    keypair: &Keypair,
    snapshot: &FleetPlacementSnapshot,
) -> anyhow::Result<SignatureMetadata> {
    let mut unsigned = snapshot.clone();
    unsigned.signature_bundle.clear();
    let message = burn_p2p_core::deterministic_cbor(&unsigned)?;
    let signature = keypair
        .sign(&message)
        .map_err(|error| anyhow::anyhow!("failed to sign placement snapshot: {error}"))?;
    Ok(SignatureMetadata {
        signer: PeerId::new(
            libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string(),
        ),
        key_id: "runtime-fleet-placement".into(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: hex::encode(signature),
    })
}

pub(crate) fn runtime_training_assignment_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let peers = runtime_training_peers(snapshot, local_roles, local_peer_id);
    if peers.len() <= 1 {
        return peers;
    }

    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let mut selected = filter_training_assignment_peers(
        &peers,
        snapshot,
        &histories,
        &planner_histories,
        local_peer_id,
    );

    if selected.is_empty() {
        selected = peers;
    }

    selected.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });
    selected
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

pub(crate) fn resolve_identity(
    identity: &IdentityConfig,
    storage: Option<&StorageConfig>,
) -> anyhow::Result<Keypair> {
    match identity {
        IdentityConfig::Ephemeral => Ok(Keypair::generate_ed25519()),
        IdentityConfig::Persistent => {
            let storage = storage.ok_or_else(|| {
                anyhow::anyhow!("persistent identity requires a configured storage root")
            })?;
            let identity_path = storage.identity_path();
            if identity_path.exists() {
                let bytes = fs::read(&identity_path).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to read persisted identity {}: {error}",
                        identity_path.display()
                    )
                })?;
                return Keypair::from_protobuf_encoding(&bytes).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to decode persisted identity {}: {error}",
                        identity_path.display()
                    )
                });
            }

            let keypair = Keypair::generate_ed25519();
            let bytes = keypair
                .to_protobuf_encoding()
                .map_err(|error| anyhow::anyhow!("failed to encode persisted identity: {error}"))?;
            fs::write(&identity_path, bytes).map_err(|error| {
                anyhow::anyhow!(
                    "failed to persist identity {}: {error}",
                    identity_path.display()
                )
            })?;
            Ok(keypair)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p_core::{
        AuthProvider, BackendClass, ContentId, FleetPlacementPeer, MetricsLiveEvent,
        MetricsLiveEventKind, NodeCertificate, NodeCertificateClaims, PeerAuthEnvelope,
        PrincipalId, ProjectFamilyId, RevocationEpoch,
    };

    fn test_snapshot(roles: impl IntoIterator<Item = PeerRole>) -> NodeTelemetrySnapshot {
        let mut snapshot = NodeTelemetrySnapshot::starting(
            &MainnetHandle {
                genesis: GenesisSpec {
                    network_id: NetworkId::new("net-test"),
                    protocol_version: Version::new(1, 0, 0),
                    display_name: String::from("test"),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                roles: PeerRoleSet::new(roles),
            },
            &NodeConfig::default(),
        );
        snapshot.local_peer_id = Some(PeerId::new("local"));
        snapshot
    }

    fn trust_score(
        peer_id: &str,
        reducer_eligible: bool,
        validator_eligible: bool,
        quarantined: bool,
    ) -> TrustScore {
        TrustScore {
            peer_id: PeerId::new(peer_id),
            score: if quarantined { -4.0 } else { 0.5 },
            reducer_eligible,
            validator_eligible,
            quarantined,
            ban_recommended: false,
            updated_at: Utc::now(),
        }
    }

    fn advertise_peer(snapshot: &mut NodeTelemetrySnapshot, peer_id: &str, roles: &[PeerRole]) {
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("net-test"),
                peer_id: PeerId::new(peer_id),
                addresses: vec![
                    SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/4{:03}", peer_id.len()))
                        .expect("peer directory address"),
                ],
                advertised_roles: Some(PeerRoleSet::new(roles.iter().cloned())),
                announced_at: Utc::now(),
            });
    }

    fn advertise_auth_peer(
        snapshot: &mut NodeTelemetrySnapshot,
        peer_id: &str,
        keypair: &Keypair,
        announced_at: DateTime<Utc>,
    ) {
        let peer_id = PeerId::new(peer_id);
        let certificate = NodeCertificate::new(
            Version::new(1, 0, 0),
            NodeCertificateClaims {
                network_id: NetworkId::new("net-test"),
                project_family_id: ProjectFamilyId::new("family-test"),
                release_train_hash: ContentId::new("train-test"),
                target_artifact_hash: ContentId::new("artifact-test"),
                peer_id: peer_id.clone(),
                peer_public_key_hex: hex::encode(keypair.public().encode_protobuf()),
                principal_id: PrincipalId::new(format!("principal-{}", peer_id.as_str())),
                provider: AuthProvider::Static {
                    authority: "test".into(),
                },
                granted_roles: PeerRoleSet::new([PeerRole::TrainerCpu]),
                experiment_scopes: BTreeSet::new(),
                client_policy_hash: None,
                auth_policy_snapshot: None,
                not_before: announced_at,
                not_after: announced_at + chrono::Duration::hours(1),
                serial: 1,
                revocation_epoch: RevocationEpoch(0),
            },
            SignatureMetadata {
                signer: peer_id.clone(),
                key_id: "test".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: announced_at,
                signature_hex: "00".into(),
            },
        )
        .expect("node certificate");
        snapshot
            .control_plane
            .auth_announcements
            .push(PeerAuthAnnouncement {
                peer_id: peer_id.clone(),
                envelope: PeerAuthEnvelope {
                    peer_id,
                    certificate,
                    client_manifest_id: None,
                    requested_scopes: BTreeSet::new(),
                    nonce_hash: ContentId::new("nonce-test"),
                    challenge_signature_hex: "00".into(),
                    presented_at: announced_at,
                },
                announced_at,
            });
    }

    #[test]
    fn runtime_topology_peers_exclude_quarantined_and_reducer_demoted_peers() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-good"),
            PeerId::new("peer-quarantined"),
            PeerId::new("peer-validator-only"),
        ]);
        snapshot.trust_scores = vec![
            trust_score("peer-quarantined", false, false, true),
            trust_score("peer-validator-only", false, true, false),
        ];
        advertise_peer(&mut snapshot, "peer-good", &[PeerRole::Reducer]);
        advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::Reducer]);
        advertise_peer(&mut snapshot, "peer-validator-only", &[PeerRole::Validator]);

        let peers = runtime_topology_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::Reducer]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("local")));
        assert!(peers.contains(&PeerId::new("peer-good")));
        assert!(!peers.contains(&PeerId::new("peer-quarantined")));
        assert!(!peers.contains(&PeerId::new("peer-validator-only")));
    }

    #[test]
    fn runtime_training_peers_exclude_non_trainers_and_quarantined_peers() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-trainer"),
            PeerId::new("peer-browser-trainer"),
            PeerId::new("peer-validator"),
            PeerId::new("peer-quarantined"),
        ]);
        snapshot.trust_scores = vec![trust_score("peer-quarantined", false, false, true)];
        advertise_peer(&mut snapshot, "peer-trainer", &[PeerRole::TrainerGpu]);
        advertise_peer(
            &mut snapshot,
            "peer-browser-trainer",
            &[PeerRole::BrowserTrainerWgpu],
        );
        advertise_peer(&mut snapshot, "peer-validator", &[PeerRole::Validator]);
        advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::TrainerCpu]);

        let peers = runtime_training_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("local")));
        assert!(peers.contains(&PeerId::new("peer-trainer")));
        assert!(peers.contains(&PeerId::new("peer-browser-trainer")));
        assert!(!peers.contains(&PeerId::new("peer-validator")));
        assert!(!peers.contains(&PeerId::new("peer-quarantined")));
    }

    fn publish_training_hint(
        snapshot: &mut NodeTelemetrySnapshot,
        peer_id: &str,
        status: PeerWindowStatus,
        accepted_tokens_or_samples: u64,
        window_elapsed_ms: u64,
        head_lag_at_finish: u64,
        window_finished_at: DateTime<Utc>,
    ) {
        snapshot
            .control_plane
            .metrics_announcements
            .push(MetricsAnnouncement {
                overlay: OverlayTopic::experiment(
                    NetworkId::new("net-test"),
                    StudyId::new("study-test"),
                    ExperimentId::new("exp-test"),
                    OverlayChannel::Metrics,
                )
                .expect("metrics overlay"),
                event: MetricsLiveEvent {
                    network_id: NetworkId::new("net-test"),
                    kind: MetricsLiveEventKind::LedgerAppend,
                    cursors: Vec::new(),
                    generated_at: window_finished_at,
                },
                peer_window_hints: vec![PeerWindowPlacementHint {
                    peer_id: PeerId::new(peer_id),
                    role: PeerRole::TrainerCpu,
                    backend_class: BackendClass::Cpu,
                    status,
                    accepted_tokens_or_samples,
                    window_elapsed_ms,
                    head_lag_at_finish,
                    window_finished_at,
                }],
                placement_snapshot: None,
            });
    }

    fn publish_signed_placement_snapshot(
        snapshot: &mut NodeTelemetrySnapshot,
        planner_peer_id: &str,
        planner_keypair: &Keypair,
        selected_peer_ids: &[&str],
        generated_at: DateTime<Utc>,
    ) {
        let mut placement = FleetPlacementSnapshot {
            network_id: NetworkId::new("net-test"),
            planner_peer_id: PeerId::new(planner_peer_id),
            generated_at,
            freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
            retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
            selected_peer_ids: selected_peer_ids
                .iter()
                .map(|peer_id| PeerId::new(*peer_id))
                .collect(),
            ranked_candidates: Vec::new(),
            planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
            signature_bundle: Vec::new(),
        };
        placement
            .signature_bundle
            .push(sign_fleet_placement_snapshot(planner_keypair, &placement).expect("signature"));
        snapshot
            .control_plane
            .metrics_announcements
            .push(MetricsAnnouncement {
                overlay: OverlayTopic::experiment(
                    NetworkId::new("net-test"),
                    StudyId::new("study-test"),
                    ExperimentId::new("exp-test"),
                    OverlayChannel::Metrics,
                )
                .expect("metrics overlay"),
                event: MetricsLiveEvent {
                    network_id: NetworkId::new("net-test"),
                    kind: MetricsLiveEventKind::LedgerAppend,
                    cursors: Vec::new(),
                    generated_at,
                },
                peer_window_hints: Vec::new(),
                placement_snapshot: Some(placement),
            });
    }

    fn publish_ranked_signed_placement_snapshot(
        snapshot: &mut NodeTelemetrySnapshot,
        planner_peer_id: &str,
        planner_keypair: &Keypair,
        selected_peer_ids: &[&str],
        ranked_candidates: &[(&str, f64, f64, f64)],
        generated_at: DateTime<Utc>,
    ) {
        publish_scaled_signed_placement_snapshot(
            snapshot,
            planner_peer_id,
            planner_keypair,
            selected_peer_ids,
            &ranked_candidates
                .iter()
                .map(|(peer_id, health, lag, throughput)| {
                    (*peer_id, *health, *lag, *throughput, 1.0, 1.0)
                })
                .collect::<Vec<_>>(),
            generated_at,
        );
    }

    fn publish_scaled_signed_placement_snapshot(
        snapshot: &mut NodeTelemetrySnapshot,
        planner_peer_id: &str,
        planner_keypair: &Keypair,
        selected_peer_ids: &[&str],
        ranked_candidates: &[(&str, f64, f64, f64, f64, f64)],
        generated_at: DateTime<Utc>,
    ) {
        let mut placement = FleetPlacementSnapshot {
            network_id: NetworkId::new("net-test"),
            planner_peer_id: PeerId::new(planner_peer_id),
            generated_at,
            freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
            retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
            selected_peer_ids: selected_peer_ids
                .iter()
                .map(|peer_id| PeerId::new(*peer_id))
                .collect(),
            ranked_candidates: ranked_candidates
                .iter()
                .map(
                    |(peer_id, health, lag, throughput, budget_scale, microshard_scale)| {
                        FleetPlacementPeer {
                            peer_id: PeerId::new(*peer_id),
                            role: PeerRole::TrainerCpu,
                            backend_class: BackendClass::Cpu,
                            recent_window_count: 4,
                            completed_window_count: 4,
                            recent_failure_streak: 0,
                            latest_status: PeerWindowStatus::Completed,
                            latest_finished_at: generated_at,
                            weighted_health: *health,
                            weighted_head_lag: *lag,
                            weighted_throughput: *throughput,
                            trust_score: peer_trust_score_value(snapshot, &PeerId::new(*peer_id)),
                            recommended_budget_scale: *budget_scale,
                            recommended_microshard_scale: *microshard_scale,
                        }
                    },
                )
                .collect(),
            planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
            signature_bundle: Vec::new(),
        };
        placement
            .signature_bundle
            .push(sign_fleet_placement_snapshot(planner_keypair, &placement).expect("signature"));
        snapshot
            .control_plane
            .metrics_announcements
            .push(MetricsAnnouncement {
                overlay: OverlayTopic::experiment(
                    NetworkId::new("net-test"),
                    StudyId::new("study-test"),
                    ExperimentId::new("exp-test"),
                    OverlayChannel::Metrics,
                )
                .expect("metrics overlay"),
                event: MetricsLiveEvent {
                    network_id: NetworkId::new("net-test"),
                    kind: MetricsLiveEventKind::LedgerAppend,
                    cursors: Vec::new(),
                    generated_at,
                },
                peer_window_hints: Vec::new(),
                placement_snapshot: Some(placement),
            });
    }

    #[test]
    fn runtime_training_assignment_peers_prune_recent_failed_trainers() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-fast"),
            PeerId::new("peer-failed"),
            PeerId::new("peer-unknown"),
        ]);
        advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-failed", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-unknown", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-fast",
            PeerWindowStatus::Completed,
            120,
            1_000,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-failed",
            PeerWindowStatus::Failed,
            0,
            1_000,
            0,
            now,
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("local")));
        assert!(peers.contains(&PeerId::new("peer-fast")));
        assert!(peers.contains(&PeerId::new("peer-unknown")));
        assert!(!peers.contains(&PeerId::new("peer-failed")));
    }

    #[test]
    fn runtime_training_assignment_peers_ignore_stale_failed_hints() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-stale-failed")]);
        advertise_peer(&mut snapshot, "peer-stale-failed", &[PeerRole::TrainerCpu]);
        publish_training_hint(
            &mut snapshot,
            "peer-stale-failed",
            PeerWindowStatus::Failed,
            0,
            1_000,
            0,
            Utc::now() - chrono::Duration::minutes(60),
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("peer-stale-failed")));
    }

    #[test]
    fn runtime_training_assignment_peers_keep_single_recent_failure_with_healthy_history() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-recovering")]);
        advertise_peer(&mut snapshot, "peer-recovering", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-recovering",
            PeerWindowStatus::Failed,
            0,
            1_000,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-recovering",
            PeerWindowStatus::Completed,
            128,
            900,
            0,
            now - chrono::Duration::seconds(30),
        );
        publish_training_hint(
            &mut snapshot,
            "peer-recovering",
            PeerWindowStatus::Completed,
            128,
            950,
            0,
            now - chrono::Duration::minutes(1),
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("peer-recovering")));
    }

    #[test]
    fn runtime_training_assignment_peers_prefer_consistent_completed_history() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids =
            BTreeSet::from([PeerId::new("peer-consistent"), PeerId::new("peer-bursty")]);
        advertise_peer(&mut snapshot, "peer-consistent", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-bursty", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-consistent",
            PeerWindowStatus::Completed,
            128,
            1_000,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-consistent",
            PeerWindowStatus::Completed,
            120,
            950,
            0,
            now - chrono::Duration::seconds(30),
        );
        publish_training_hint(
            &mut snapshot,
            "peer-bursty",
            PeerWindowStatus::Completed,
            180,
            900,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-bursty",
            PeerWindowStatus::Failed,
            0,
            900,
            0,
            now - chrono::Duration::seconds(30),
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        let consistent_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-consistent"))
            .expect("peer-consistent present");
        let bursty_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-bursty"))
            .expect("peer-bursty present");

        assert!(consistent_index < bursty_index);
    }

    #[test]
    fn runtime_training_assignment_peers_accept_verified_signed_snapshot_ranking() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
        advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
        let planner_keypair = Keypair::generate_ed25519();
        let planner_peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&planner_keypair.public()).to_string(),
        );
        let now = Utc::now();
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner_keypair,
            now,
        );
        publish_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner_keypair,
            &["peer-b", "peer-a"],
            now,
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );
        let peer_a_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-a"))
            .expect("peer-a present");
        let peer_b_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-b"))
            .expect("peer-b present");

        assert!(peer_b_index < peer_a_index);
    }

    #[test]
    fn runtime_training_assignment_peers_ignore_unverified_signed_snapshot_ranking() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
        advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
        let trusted_planner = Keypair::generate_ed25519();
        let untrusted_planner = Keypair::generate_ed25519();
        let planner_peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&trusted_planner.public()).to_string(),
        );
        let now = Utc::now();
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &trusted_planner,
            now,
        );
        publish_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &untrusted_planner,
            &["peer-b", "peer-a"],
            now,
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );
        let peer_a_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-a"))
            .expect("peer-a present");
        let peer_b_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-b"))
            .expect("peer-b present");

        assert!(peer_a_index < peer_b_index);
    }

    #[test]
    fn runtime_training_assignment_peers_use_decayed_multi_planner_history() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
        advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
        let planner_a = Keypair::generate_ed25519();
        let planner_b = Keypair::generate_ed25519();
        let planner_c = Keypair::generate_ed25519();
        let planner_a_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_a.public()).to_string());
        let planner_b_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_b.public()).to_string());
        let planner_c_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_c.public()).to_string());
        let now = Utc::now();
        advertise_auth_peer(&mut snapshot, planner_a_peer.as_str(), &planner_a, now);
        advertise_auth_peer(
            &mut snapshot,
            planner_b_peer.as_str(),
            &planner_b,
            now - chrono::Duration::seconds(30),
        );
        advertise_auth_peer(
            &mut snapshot,
            planner_c_peer.as_str(),
            &planner_c,
            now - chrono::Duration::seconds(60),
        );
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_a_peer.as_str(),
            &planner_a,
            &["peer-b"],
            &[("peer-b", 0.95, 0.0, 160.0), ("peer-a", 0.8, 0.0, 140.0)],
            now,
        );
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_b_peer.as_str(),
            &planner_b,
            &["peer-b"],
            &[("peer-b", 0.94, 0.0, 150.0), ("peer-a", 0.8, 0.0, 130.0)],
            now - chrono::Duration::seconds(30),
        );
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_c_peer.as_str(),
            &planner_c,
            &["peer-a"],
            &[("peer-a", 0.9, 0.0, 155.0), ("peer-b", 0.85, 0.0, 145.0)],
            now - chrono::Duration::seconds(60),
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );
        let peer_a_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-a"))
            .expect("peer-a present");
        let peer_b_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-b"))
            .expect("peer-b present");

        assert!(peer_b_index < peer_a_index);
    }

    #[test]
    fn runtime_training_assignment_peers_prefer_distinct_planner_consensus_over_single_planner_volume()
     {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
        advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-a",
            PeerWindowStatus::Completed,
            128,
            950,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-b",
            PeerWindowStatus::Completed,
            126,
            955,
            0,
            now,
        );

        let planner_a = Keypair::generate_ed25519();
        let planner_b = Keypair::generate_ed25519();
        let planner_c = Keypair::generate_ed25519();
        let planner_a_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_a.public()).to_string());
        let planner_b_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_b.public()).to_string());
        let planner_c_peer =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_c.public()).to_string());
        advertise_auth_peer(&mut snapshot, planner_a_peer.as_str(), &planner_a, now);
        advertise_auth_peer(
            &mut snapshot,
            planner_b_peer.as_str(),
            &planner_b,
            now - chrono::Duration::seconds(10),
        );
        advertise_auth_peer(
            &mut snapshot,
            planner_c_peer.as_str(),
            &planner_c,
            now - chrono::Duration::seconds(20),
        );

        for offset in [0, 15, 30, 45] {
            publish_ranked_signed_placement_snapshot(
                &mut snapshot,
                planner_a_peer.as_str(),
                &planner_a,
                &["peer-a"],
                &[("peer-a", 0.96, 0.0, 175.0), ("peer-b", 0.85, 0.0, 170.0)],
                now - chrono::Duration::seconds(offset),
            );
        }
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_b_peer.as_str(),
            &planner_b,
            &["peer-b"],
            &[("peer-b", 0.97, 0.0, 178.0), ("peer-a", 0.85, 0.0, 168.0)],
            now - chrono::Duration::seconds(5),
        );
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_c_peer.as_str(),
            &planner_c,
            &["peer-b"],
            &[("peer-b", 0.95, 0.0, 176.0), ("peer-a", 0.84, 0.0, 167.0)],
            now - chrono::Duration::seconds(12),
        );

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );
        let peer_a_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-a"))
            .expect("peer-a present");
        let peer_b_index = peers
            .iter()
            .position(|peer_id| peer_id == &PeerId::new("peer-b"))
            .expect("peer-b present");

        assert!(peer_b_index < peer_a_index);
    }

    #[test]
    fn runtime_training_assignment_peers_do_not_prune_from_single_planner_backpressure() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids =
            BTreeSet::from([PeerId::new("peer-backed-off"), PeerId::new("peer-healthy")]);
        advertise_peer(&mut snapshot, "peer-backed-off", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-healthy", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-backed-off",
            PeerWindowStatus::Completed,
            96,
            1_000,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-healthy",
            PeerWindowStatus::Completed,
            128,
            950,
            0,
            now,
        );
        let planner = Keypair::generate_ed25519();
        let planner_peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
        advertise_auth_peer(&mut snapshot, planner_peer_id.as_str(), &planner, now);
        for offset in [0, 20, 40, 60] {
            publish_scaled_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                &["peer-healthy"],
                &[
                    ("peer-healthy", 0.96, 0.0, 180.0, 1.2, 1.1),
                    ("peer-backed-off", 0.7, 0.0, 90.0, 0.4, 0.4),
                ],
                now - chrono::Duration::seconds(offset),
            );
        }

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("peer-healthy")));
        assert!(peers.contains(&PeerId::new("peer-backed-off")));
    }

    #[test]
    fn runtime_training_assignment_peers_prune_sustained_global_backpressure() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids =
            BTreeSet::from([PeerId::new("peer-backed-off"), PeerId::new("peer-healthy")]);
        advertise_peer(&mut snapshot, "peer-backed-off", &[PeerRole::TrainerCpu]);
        advertise_peer(&mut snapshot, "peer-healthy", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "peer-backed-off",
            PeerWindowStatus::Completed,
            96,
            1_000,
            0,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "peer-healthy",
            PeerWindowStatus::Completed,
            128,
            950,
            0,
            now,
        );
        for offset in [0, 20, 40, 60] {
            let planner = Keypair::generate_ed25519();
            let planner_peer_id = PeerId::new(
                libp2p_identity::PeerId::from_public_key(&planner.public()).to_string(),
            );
            let generated_at = now - chrono::Duration::seconds(offset);
            advertise_auth_peer(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                generated_at,
            );
            publish_scaled_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                &["peer-healthy"],
                &[
                    ("peer-healthy", 0.96, 0.0, 180.0, 1.2, 1.1),
                    ("peer-backed-off", 0.7, 0.0, 90.0, 0.45, 0.45),
                ],
                generated_at,
            );
        }

        let peers = runtime_training_assignment_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::TrainerCpu]),
            &PeerId::new("local"),
        );

        assert!(peers.contains(&PeerId::new("peer-healthy")));
        assert!(!peers.contains(&PeerId::new("peer-backed-off")));
    }

    #[test]
    fn local_training_adaptation_factor_uses_lag_history_and_planner_pressure() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.head_lag_steps = 4;
        snapshot.observed_peer_ids =
            BTreeSet::from([PeerId::new("peer-fast"), PeerId::new("local")]);
        advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "local",
            PeerWindowStatus::Failed,
            0,
            1_200,
            3,
            now,
        );
        publish_training_hint(
            &mut snapshot,
            "local",
            PeerWindowStatus::Failed,
            0,
            1_150,
            3,
            now - chrono::Duration::seconds(20),
        );
        for offset in [0, 15, 30] {
            let planner = Keypair::generate_ed25519();
            let planner_peer_id = PeerId::new(
                libp2p_identity::PeerId::from_public_key(&planner.public()).to_string(),
            );
            let generated_at = now - chrono::Duration::seconds(offset);
            advertise_auth_peer(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                generated_at,
            );
            publish_ranked_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                &["peer-fast"],
                &[("peer-fast", 0.98, 0.0, 180.0), ("local", 0.4, 3.0, 60.0)],
                generated_at,
            );
        }

        let factor = local_training_adaptation_factor(&snapshot, &PeerId::new("local"));

        assert!(factor < 0.35, "unexpected adaptation factor: {factor}");
        assert!(
            factor >= 0.2,
            "factor should remain clamped to the safety floor"
        );
    }

    #[test]
    fn local_training_schedule_hint_uses_verified_planner_scales() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids =
            BTreeSet::from([PeerId::new("peer-fast"), PeerId::new("local")]);
        advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "local",
            PeerWindowStatus::Completed,
            100,
            900,
            0,
            now,
        );
        for offset in [0, 20, 40] {
            let planner = Keypair::generate_ed25519();
            let planner_peer_id = PeerId::new(
                libp2p_identity::PeerId::from_public_key(&planner.public()).to_string(),
            );
            let generated_at = now - chrono::Duration::seconds(offset);
            advertise_auth_peer(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                generated_at,
            );
            publish_scaled_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                &["local"],
                &[
                    ("local", 0.95, 0.0, 180.0, 1.35, 1.25),
                    ("peer-fast", 0.8, 0.0, 140.0, 0.85, 0.9),
                ],
                generated_at,
            );
        }

        let hint = local_training_schedule_hint(&snapshot, &PeerId::new("local"));

        assert!(
            hint.budget_scale > 1.15,
            "unexpected budget scale: {}",
            hint.budget_scale
        );
        assert!(
            hint.microshard_scale > 1.05,
            "unexpected microshard scale: {}",
            hint.microshard_scale
        );
    }

    #[test]
    fn local_training_schedule_hint_ignores_unverified_planner_scales() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "local",
            PeerWindowStatus::Completed,
            100,
            900,
            0,
            now,
        );
        for offset in [0, 20, 40] {
            let trusted_planner = Keypair::generate_ed25519();
            let signing_planner = Keypair::generate_ed25519();
            let planner_peer_id = PeerId::new(
                libp2p_identity::PeerId::from_public_key(&trusted_planner.public()).to_string(),
            );
            let generated_at = now - chrono::Duration::seconds(offset);
            advertise_auth_peer(
                &mut snapshot,
                planner_peer_id.as_str(),
                &trusted_planner,
                generated_at,
            );
            publish_scaled_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &signing_planner,
                &["local"],
                &[("local", 0.95, 0.0, 180.0, 1.45, 1.35)],
                generated_at,
            );
        }

        let hint = local_training_schedule_hint(&snapshot, &PeerId::new("local"));

        assert_eq!(hint.budget_scale, 1.0);
        assert_eq!(hint.microshard_scale, 1.0);
    }

    #[test]
    fn local_training_schedule_hint_downscales_under_sustained_planner_backpressure() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        let now = Utc::now();
        publish_training_hint(
            &mut snapshot,
            "local",
            PeerWindowStatus::Completed,
            96,
            1_000,
            0,
            now,
        );
        for offset in [0, 20, 40, 60] {
            let planner = Keypair::generate_ed25519();
            let planner_peer_id = PeerId::new(
                libp2p_identity::PeerId::from_public_key(&planner.public()).to_string(),
            );
            let generated_at = now - chrono::Duration::seconds(offset);
            advertise_auth_peer(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                generated_at,
            );
            publish_scaled_signed_placement_snapshot(
                &mut snapshot,
                planner_peer_id.as_str(),
                &planner,
                &["peer-fast"],
                &[
                    ("local", 0.75, 0.0, 90.0, 0.5, 0.5),
                    ("peer-fast", 0.98, 0.0, 180.0, 1.2, 1.1),
                ],
                generated_at,
            );
        }

        let hint = local_training_schedule_hint(&snapshot, &PeerId::new("local"));

        assert!(
            hint.budget_scale < 0.8,
            "unexpected budget scale: {}",
            hint.budget_scale
        );
        assert!(
            hint.microshard_scale < 0.8,
            "unexpected microshard scale: {}",
            hint.microshard_scale
        );
    }

    #[test]
    fn runtime_validator_peers_and_quorum_respect_validator_eligibility() {
        let mut snapshot = test_snapshot([PeerRole::Validator]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-good"),
            PeerId::new("peer-reducer-only"),
            PeerId::new("peer-quarantined"),
        ]);
        snapshot.trust_scores = vec![
            trust_score("peer-reducer-only", true, false, false),
            trust_score("peer-quarantined", false, false, true),
        ];
        advertise_peer(&mut snapshot, "peer-good", &[PeerRole::Validator]);
        advertise_peer(&mut snapshot, "peer-reducer-only", &[PeerRole::Reducer]);
        advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::Validator]);

        let validator_peers = runtime_validator_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::Validator]),
            &PeerId::new("local"),
        );
        let validators = runtime_validators(
            &PeerRoleSet::new([PeerRole::Validator]),
            &PeerId::new("local"),
            &validator_peers,
            2,
        );

        assert!(validator_peers.contains(&PeerId::new("local")));
        assert!(validator_peers.contains(&PeerId::new("peer-good")));
        assert!(!validator_peers.contains(&PeerId::new("peer-reducer-only")));
        assert!(!validator_peers.contains(&PeerId::new("peer-quarantined")));
        assert_eq!(
            validators,
            vec![PeerId::new("local"), PeerId::new("peer-good")]
        );
    }

    #[test]
    fn runtime_validators_cap_selection_to_the_requested_quorum() {
        let peers = std::iter::once(PeerId::new("local"))
            .chain((0..25).map(|index| PeerId::new(format!("validator-{index:02}"))))
            .collect::<Vec<_>>();

        let validators = runtime_validators(
            &PeerRoleSet::new([PeerRole::Validator]),
            &PeerId::new("local"),
            &peers,
            2,
        );

        assert_eq!(validators.len(), 2);
        assert_eq!(validators[0], PeerId::new("local"));
        assert_eq!(validators[1], PeerId::new("validator-00"));
    }

    #[test]
    fn runtime_validator_peers_ignore_transport_only_observed_peers() {
        let mut snapshot = test_snapshot([PeerRole::Validator]);
        snapshot.local_peer_id = Some(PeerId::new("local"));
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-validator"),
            PeerId::new("peer-transport-only"),
        ]);
        advertise_peer(&mut snapshot, "peer-validator", &[PeerRole::Validator]);

        let validator_peers = runtime_validator_peers(
            &snapshot,
            &PeerRoleSet::new([PeerRole::Validator]),
            &PeerId::new("local"),
        );

        assert!(validator_peers.contains(&PeerId::new("local")));
        assert!(validator_peers.contains(&PeerId::new("peer-validator")));
        assert!(!validator_peers.contains(&PeerId::new("peer-transport-only")));
    }
}
