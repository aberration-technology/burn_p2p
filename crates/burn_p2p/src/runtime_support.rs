use super::*;
use crate::config::{
    ClientReenrollmentStatus, RuntimeCommand, TrustBundleState, default_node_runtime_state,
    peer_id_from_event,
};
mod control_plane;
mod persistence;
mod trust;

pub(crate) use control_plane::run_control_plane;
#[cfg(test)]
pub(crate) use persistence::{
    load_artifact_transfer_state, load_scoped_lease_announcement, persist_artifact_transfer_state,
};
pub(crate) use persistence::{
    load_head_state, load_json, load_known_peers, load_latest_merge_certificate,
    load_limit_profile, load_primary_slot_assignment, next_window_id, persist_auth_state,
    persist_control_plane_state, persist_head_state, persist_in_flight_transfer_states,
    persist_json, persist_known_peers, persist_limit_profile, persist_primary_slot_assignment,
    persist_runtime_binding_state, persist_runtime_security_state, persist_window_id,
    remove_artifact_transfer_state, restore_auth_config, restore_control_plane_state,
    restore_in_flight_transfer_states, restore_runtime_binding_config,
    restore_runtime_security_state,
};
use persistence::{
    persist_local_peer_auth, seed_shell_control_plane_state, sync_control_plane_snapshot,
};
pub(crate) use trust::verify_snapshot_admission;
use trust::{
    admission_rejection_reason, note_admitted_peer, note_rejected_peer, peer_is_reducer_eligible,
    peer_is_validator_eligible, reconcile_live_revocation_policy, reconcile_remote_trust_bundle,
};

pub(crate) fn runtime_limit_policy(estimate: &CapabilityEstimate) -> LimitPolicy {
    LimitPolicy {
        target_window_seconds: estimate.target_window_seconds.max(1),
        browser_target_window_seconds: estimate.target_window_seconds.max(1),
        ..LimitPolicy::default()
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
    let available_backends = if estimate.preferred_backends.is_empty() {
        vec![LocalBackend::Cpu]
    } else {
        estimate
            .preferred_backends
            .iter()
            .map(|backend| match backend.as_str() {
                "cuda" => LocalBackend::Cuda,
                "wgpu" => LocalBackend::Wgpu,
                "ndarray" => LocalBackend::Ndarray,
                "cpu" => LocalBackend::Cpu,
                other => LocalBackend::Custom(other.to_owned()),
            })
            .collect()
    };
    let probe = CapabilityProbe {
        peer_id: peer_id.clone(),
        platform: burn_p2p_core::ClientPlatform::Native,
        available_backends,
        device_memory_bytes: None,
        system_memory_bytes: 0,
        disk_bytes: 0,
        upload_mbps: 0.0,
        download_mbps: 0.0,
        persistence,
        attestation_level: burn_p2p_core::AttestationLevel::None,
        work_units_per_second: estimate.work_units_per_second,
        benchmark_hash: None,
    };

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
    let mut snapshot = telemetry
        .state
        .lock()
        .expect("telemetry state lock should not be poisoned");
    snapshot.effective_limit_profile = profile;
    snapshot.updated_at = Utc::now();
}

pub(crate) fn connected_peer_ids(snapshot: &NodeTelemetrySnapshot) -> BTreeSet<PeerId> {
    snapshot
        .observed_peer_ids
        .iter()
        .cloned()
        .chain(snapshot.recent_events.iter().filter_map(peer_id_from_event))
        .collect()
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
    let remote_merged = snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            if let Some(merge) = latest_merge_from_snapshot(snapshot, experiment) {
                return snapshot
                    .head_announcements
                    .iter()
                    .find(|announcement| announcement.head.head_id == merge.merged_head_id)
                    .map(|announcement| {
                        (
                            announcement
                                .provider_peer_id
                                .clone()
                                .unwrap_or_else(|| peer_id.clone()),
                            announcement.head.clone(),
                        )
                    });
            }
            None
        })
        .max_by(|left, right| {
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

pub(crate) fn resolve_canonical_head(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> anyhow::Result<Option<(PeerId, HeadDescriptor)>> {
    let mut best = load_head_state(storage, experiment)?.map(|head| (PeerId::new("local"), head));

    let remote_merged = snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            if let Some(merge) = latest_merge_from_snapshot(snapshot, experiment) {
                return snapshot
                    .head_announcements
                    .iter()
                    .find(|announcement| announcement.head.head_id == merge.merged_head_id)
                    .map(|announcement| {
                        (
                            announcement
                                .provider_peer_id
                                .clone()
                                .unwrap_or_else(|| peer_id.clone()),
                            announcement.head.clone(),
                        )
                    });
            }
            None
        })
        .max_by(|left, right| {
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
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_reducer_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if peer_is_reducer_eligible(snapshot, local_peer_id) {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_validator_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_validator_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if peer_is_validator_eligible(snapshot, local_peer_id) {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() {
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

    fn test_snapshot(roles: impl IntoIterator<Item = PeerRole>) -> NodeTelemetrySnapshot {
        NodeTelemetrySnapshot::starting(
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
        )
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

        let peers = runtime_topology_peers(&snapshot, &PeerId::new("local"));

        assert!(peers.contains(&PeerId::new("local")));
        assert!(peers.contains(&PeerId::new("peer-good")));
        assert!(!peers.contains(&PeerId::new("peer-quarantined")));
        assert!(!peers.contains(&PeerId::new("peer-validator-only")));
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

        let validator_peers = runtime_validator_peers(&snapshot, &PeerId::new("local"));
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
}
