use super::*;
use crate::config::{
    ClientReenrollmentStatus, RuntimeCommand, TrustBundleState, default_node_runtime_state,
};
use crate::handles::dedupe_peer_ids;
use std::path::{Path, PathBuf};
use sysinfo::{Disks, System};

mod control_plane;
mod persistence;
mod placement;
mod signing;
#[cfg(test)]
mod tests;
mod topology;
mod trust;

pub(crate) use control_plane::run_control_plane;
pub(crate) use persistence::{
    inferred_next_window_id, load_head_state, load_json, load_known_peers,
    load_latest_merge_certificate, load_limit_profile, load_primary_slot_assignment,
    load_slot_assignments, persist_auth_state, persist_control_plane_state, persist_head_state,
    persist_in_flight_transfer_states, persist_json, persist_known_peers, persist_limit_profile,
    persist_primary_slot_assignment, persist_runtime_binding_state, persist_runtime_security_state,
    persist_slot_assignments, persist_window_id, remove_artifact_transfer_state,
    restore_auth_config, restore_control_plane_state, restore_in_flight_transfer_states,
    restore_runtime_binding_config, restore_runtime_security_state,
};
#[cfg(test)]
pub(crate) use persistence::{
    load_artifact_transfer_state, load_scoped_lease_announcement, persist_artifact_transfer_state,
};
use persistence::{
    persist_local_peer_auth, seed_shell_control_plane_state, sync_control_plane_snapshot,
};
pub(crate) use placement::{
    build_fleet_placement_snapshot, local_training_adaptation_factor, local_training_schedule_hint,
    runtime_training_assignment_peers, sign_fleet_placement_snapshot,
};
pub(crate) use signing::{
    sign_diloco_gradient_manifest, sign_diloco_state_snapshot,
    verify_diloco_gradient_manifest_signature, verify_diloco_state_snapshot_signature,
};
#[cfg(test)]
pub(crate) use topology::experiment_snapshot_peer_ids;
pub use topology::latest_promoted_head_from_control_plane;
pub(crate) use topology::{
    LagAssessment, active_experiment_directory_entry, assess_head_lag, cached_connected_snapshots,
    connected_peer_ids, effective_experiment_lifecycle_plan, effective_fleet_schedule_epoch,
    experiment_has_lifecycle_plan, head_provider_peers, latest_head_from_snapshot,
    latest_merge_window_from_connected_snapshots, latest_merge_window_from_snapshot,
    latest_reducer_assignment_from_snapshot, matches_experiment_head, merge_control_plane_snapshot,
    metric_quality, open_runtime_merge_window, prioritized_experiment_snapshot_peer_ids,
    resolve_canonical_head, runtime_assign_reducers, runtime_merge_topology_policy,
    runtime_robustness_policy, runtime_topology_peers, runtime_training_peers,
    runtime_training_protocol, runtime_validator_peers, runtime_validators,
    runtime_window_reducers, snapshots_with_local_control_plane,
    update_announces_from_connected_snapshots, update_feature_sketch_from_metrics,
    update_norm_stats,
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

pub(crate) fn env_trace_enabled(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| {
        let value = value.to_string_lossy();
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        )
    })
}

pub(crate) fn trace_to_stderr(name: &str, prefix: &str, args: std::fmt::Arguments<'_>) {
    if env_trace_enabled(name) {
        eprintln!("[{prefix}] {args}");
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
