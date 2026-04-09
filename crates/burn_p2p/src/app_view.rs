use burn_p2p_views::{
    BrowserAppClientView, BrowserAppExperimentSummary, BrowserAppMetricPreview,
    BrowserAppNetworkView, BrowserAppSurface, BrowserAppTrainingView, BrowserAppValidationView,
    BrowserAppViewerView,
};

use crate::{
    ExperimentDirectoryEntry, ExperimentId, NodeRuntimeState, NodeTelemetrySnapshot, PeerRole,
    RevisionId, RunningNode, SlotAssignmentState, SlotRuntimeState,
};

#[derive(Clone, Debug, PartialEq, Eq)]
/// Optional experiment focus for one native node app view.
pub struct NodeAppSelection {
    /// Selected experiment identifier.
    pub experiment_id: ExperimentId,
    /// Selected revision identifier.
    pub revision_id: RevisionId,
}

impl NodeAppSelection {
    /// Creates a new explicit selection.
    pub fn new(experiment_id: ExperimentId, revision_id: RevisionId) -> Self {
        Self {
            experiment_id,
            revision_id,
        }
    }
}

/// Builds a shared app view from native node telemetry and one experiment
/// directory.
pub fn build_node_app_view(
    snapshot: &NodeTelemetrySnapshot,
    edge_base_url: impl Into<String>,
    directory: &[ExperimentDirectoryEntry],
    selection: Option<&NodeAppSelection>,
) -> BrowserAppClientView {
    let edge_base_url = edge_base_url.into();
    let selected = resolve_selected_experiment(snapshot, directory, selection);
    let validate_available = snapshot.configured_roles.contains(&PeerRole::Validator);
    let train_available = has_training_role(&snapshot.configured_roles);
    let can_validate = validate_available && !node_is_blocked(snapshot.node_state.clone());
    let can_train = train_available && !node_is_blocked(snapshot.node_state.clone());
    let max_window_secs = selected
        .as_ref()
        .map(|entry| entry.resource_requirements.estimated_window_seconds);
    let latest_head_id = selected
        .as_ref()
        .and_then(|entry| entry.current_head_id.as_ref())
        .map(|head_id| head_id.as_str().to_owned());
    let active_assignment = selected.as_ref().map(|entry| {
        format!(
            "{}/{}",
            entry.experiment_id.as_str(),
            entry.current_revision_id.as_str()
        )
    });
    let validation_metric_preview = selected
        .as_ref()
        .map(|entry| selected_head_metric_preview(snapshot, entry))
        .unwrap_or_default();

    BrowserAppClientView {
        network_id: snapshot
            .network_id
            .as_ref()
            .map(|network_id| network_id.as_str().to_owned())
            .unwrap_or_else(|| "unknown".into()),
        default_surface: default_surface(snapshot, can_validate, can_train),
        runtime_label: runtime_label(snapshot),
        runtime_detail: runtime_detail(snapshot),
        capability_summary: capability_summary(snapshot),
        session_label: session_label(snapshot),
        selected_experiment: selected.as_ref().map(entry_summary),
        viewer: BrowserAppViewerView {
            visible_experiments: directory.len(),
            visible_heads: directory
                .iter()
                .filter(|entry| entry.current_head_id.is_some())
                .count(),
            leaderboard_entries: 0,
            signed_directory_ready: !directory.is_empty(),
            signed_leaderboard_ready: false,
            experiments_preview: directory.iter().map(entry_summary).collect(),
            leaderboard_preview: Vec::new(),
        },
        validation: BrowserAppValidationView {
            validate_available,
            can_validate,
            current_head_id: latest_head_id.clone(),
            metrics_sync_at: Some(snapshot.updated_at.to_rfc3339()),
            pending_receipts: snapshot.control_plane.update_announcements.len(),
            validation_status: Some(validation_status(snapshot, can_validate)),
            checked_chunks: Some(snapshot.in_flight_transfers.len()),
            emitted_receipt_id: None,
            evaluation_summary: Some(validation_summary(snapshot)),
            metric_preview: validation_metric_preview,
        },
        training: BrowserAppTrainingView {
            train_available,
            can_train,
            active_assignment,
            slice_status: training_slice_status(snapshot),
            latest_head_id,
            cached_chunk_artifacts: snapshot.in_flight_transfers.len(),
            cached_microshards: 0,
            pending_receipts: snapshot.control_plane.update_announcements.len(),
            max_window_secs,
            last_window_secs: max_window_secs,
            optimizer_steps: None,
            accepted_samples: None,
            slice_target_samples: None,
            slice_remaining_samples: None,
            last_loss: None,
            publish_latency_ms: None,
            throughput_summary: Some(training_summary(snapshot)),
            last_artifact_id: snapshot
                .in_flight_transfers
                .keys()
                .next_back()
                .map(|artifact_id| artifact_id.as_str().to_owned()),
            last_receipt_id: None,
        },
        network: BrowserAppNetworkView {
            edge_base_url,
            transport: "native".into(),
            node_state: runtime_label(snapshot),
            direct_peers: snapshot.connected_peers,
            observed_peers: snapshot.observed_peer_ids.len(),
            estimated_network_size: snapshot
                .connected_peers
                .max(snapshot.observed_peer_ids.len())
                .max(snapshot.admitted_peers.len()),
            accepted_receipts: snapshot.control_plane.update_announcements.len(),
            certified_merges: snapshot.control_plane.merge_announcements.len(),
            in_flight_transfers: snapshot.in_flight_transfers.len(),
            network_note: network_note(snapshot),
            metrics_live_ready: !snapshot.control_plane.metrics_announcements.is_empty(),
            last_directory_sync_at: Some(snapshot.updated_at.to_rfc3339()),
            last_error: snapshot.last_error.clone(),
            performance: None,
        },
    }
}

impl<P> RunningNode<P> {
    /// Builds the shared app view used by the browser dioxus surface from the
    /// current native node telemetry snapshot.
    ///
    /// `edge_base_url` should point at the bootstrap edge or local control
    /// surface the node is paired with.
    pub fn app_view(
        &self,
        edge_base_url: impl Into<String>,
        selection: Option<NodeAppSelection>,
    ) -> BrowserAppClientView {
        build_node_app_view(
            &self.telemetry().snapshot(),
            edge_base_url,
            &self
                .config()
                .auth
                .as_ref()
                .map_or_else(Vec::new, |auth| auth.experiment_directory.clone()),
            selection.as_ref(),
        )
    }
}

fn resolve_selected_experiment(
    snapshot: &NodeTelemetrySnapshot,
    directory: &[ExperimentDirectoryEntry],
    selection: Option<&NodeAppSelection>,
) -> Option<ExperimentDirectoryEntry> {
    if let Some(selection) = selection {
        return directory
            .iter()
            .find(|entry| {
                entry.experiment_id == selection.experiment_id
                    && entry.current_revision_id == selection.revision_id
            })
            .cloned();
    }

    let assigned = primary_slot_assignment(snapshot);
    if let Some(assigned) = assigned
        && let Some(entry) = directory.iter().find(|entry| {
            entry.experiment_id == assigned.experiment_id
                && entry.current_revision_id == assigned.revision_id
        })
    {
        return Some(entry.clone());
    }

    directory.first().cloned()
}

fn primary_slot_assignment(snapshot: &NodeTelemetrySnapshot) -> Option<&SlotAssignmentState> {
    match snapshot.slot_states.first() {
        Some(SlotRuntimeState::Assigned(assignment))
        | Some(SlotRuntimeState::MaterializingBase(assignment))
        | Some(SlotRuntimeState::FetchingShards(assignment))
        | Some(SlotRuntimeState::Training(assignment))
        | Some(SlotRuntimeState::Publishing(assignment))
        | Some(SlotRuntimeState::CoolingDown(assignment))
        | Some(SlotRuntimeState::Migrating(assignment)) => Some(assignment),
        Some(SlotRuntimeState::Blocked {
            assignment: Some(assignment),
            ..
        }) => Some(assignment),
        _ => None,
    }
}

fn entry_summary(entry: &ExperimentDirectoryEntry) -> BrowserAppExperimentSummary {
    BrowserAppExperimentSummary {
        display_name: entry.display_name.clone(),
        experiment_id: entry.experiment_id.as_str().to_owned(),
        revision_id: entry.current_revision_id.as_str().to_owned(),
        workload_id: entry.workload_id.as_str().to_owned(),
        current_head_id: entry
            .current_head_id
            .as_ref()
            .map(|head| head.as_str().to_owned()),
        validate_available: entry.allowed_roles.contains(&PeerRole::Validator),
        train_available: has_training_role(&entry.allowed_roles),
        availability: if entry.allowed_roles.contains(&PeerRole::Validator)
            && has_training_role(&entry.allowed_roles)
        {
            "train + validate".into()
        } else if has_training_role(&entry.allowed_roles) {
            "train".into()
        } else if entry.allowed_roles.contains(&PeerRole::Validator) {
            "validate".into()
        } else {
            "view".into()
        },
    }
}

fn selected_head_metric_preview(
    snapshot: &NodeTelemetrySnapshot,
    entry: &ExperimentDirectoryEntry,
) -> Vec<BrowserAppMetricPreview> {
    snapshot
        .control_plane
        .head_announcements
        .iter()
        .rev()
        .find(|announcement| {
            announcement.head.experiment_id == entry.experiment_id
                && announcement.head.revision_id == entry.current_revision_id
                && entry
                    .current_head_id
                    .as_ref()
                    .is_none_or(|head_id| &announcement.head.head_id == head_id)
        })
        .map(|announcement| metric_preview(&announcement.head.metrics))
        .unwrap_or_default()
}

fn metric_preview(
    metrics: &std::collections::BTreeMap<String, crate::MetricValue>,
) -> Vec<BrowserAppMetricPreview> {
    let mut preferred = ["accuracy", "loss", "train_loss_mean", "digit_zero_accuracy"]
        .into_iter()
        .filter_map(|key| {
            metrics.get(key).map(|value| BrowserAppMetricPreview {
                label: key.to_owned(),
                value: metric_value_label(value),
            })
        })
        .collect::<Vec<_>>();
    let preferred_labels = preferred
        .iter()
        .map(|preview| preview.label.clone())
        .collect::<std::collections::BTreeSet<_>>();

    let remaining = metrics
        .iter()
        .filter(|(key, _)| !preferred_labels.contains(key.as_str()))
        .take(4usize.saturating_sub(preferred.len()))
        .map(|(key, value)| BrowserAppMetricPreview {
            label: key.clone(),
            value: metric_value_label(value),
        });
    preferred.extend(remaining);
    preferred
}

fn metric_value_label(value: &crate::MetricValue) -> String {
    match value {
        crate::MetricValue::Integer(value) => value.to_string(),
        crate::MetricValue::Float(value) => format!("{value:.4}"),
        crate::MetricValue::Bool(value) => {
            if *value {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        crate::MetricValue::Text(value) => value.clone(),
    }
}

fn default_surface(
    snapshot: &NodeTelemetrySnapshot,
    can_validate: bool,
    can_train: bool,
) -> BrowserAppSurface {
    match snapshot.node_state {
        NodeRuntimeState::TrainingWindow | NodeRuntimeState::PublishingUpdate if can_train => {
            BrowserAppSurface::Train
        }
        NodeRuntimeState::PassiveValidator if can_validate => BrowserAppSurface::Validate,
        NodeRuntimeState::Connecting
        | NodeRuntimeState::DirectorySync
        | NodeRuntimeState::HeadSync => BrowserAppSurface::Network,
        _ => BrowserAppSurface::Viewer,
    }
}

fn runtime_label(snapshot: &NodeTelemetrySnapshot) -> String {
    match snapshot.node_state {
        NodeRuntimeState::Starting => "starting",
        NodeRuntimeState::Connecting => "connecting",
        NodeRuntimeState::Admitting => "admitting",
        NodeRuntimeState::DirectorySync => "directory sync",
        NodeRuntimeState::HeadSync => "head sync",
        NodeRuntimeState::IdleReady => "ready",
        NodeRuntimeState::LeasePending => "waiting",
        NodeRuntimeState::TrainingWindow => "train",
        NodeRuntimeState::PublishingUpdate => "publishing",
        NodeRuntimeState::WaitingMerge => "waiting merge",
        NodeRuntimeState::PassiveValidator => "validate",
        NodeRuntimeState::PassiveArchive => "observe",
        NodeRuntimeState::Quarantined => "quarantined",
        NodeRuntimeState::Revoked => "revoked",
        NodeRuntimeState::ShuttingDown => "shutting down",
    }
    .into()
}

fn runtime_detail(snapshot: &NodeTelemetrySnapshot) -> String {
    if let Some(error) = snapshot.last_error.as_ref() {
        return error.clone();
    }
    match snapshot.slot_states.first() {
        Some(SlotRuntimeState::Assigned(assignment))
        | Some(SlotRuntimeState::MaterializingBase(assignment))
        | Some(SlotRuntimeState::FetchingShards(assignment))
        | Some(SlotRuntimeState::Training(assignment))
        | Some(SlotRuntimeState::Publishing(assignment))
        | Some(SlotRuntimeState::CoolingDown(assignment))
        | Some(SlotRuntimeState::Migrating(assignment)) => format!(
            "{}/{}/{}",
            assignment.study_id.as_str(),
            assignment.experiment_id.as_str(),
            assignment.revision_id.as_str()
        ),
        Some(SlotRuntimeState::Blocked { reason, .. }) => reason.clone(),
        _ => format!(
            "{} direct peers · lag {}",
            snapshot.connected_peers, snapshot.head_lag_steps
        ),
    }
}

fn capability_summary(snapshot: &NodeTelemetrySnapshot) -> String {
    match snapshot.runtime_boundary.as_ref() {
        Some(boundary) => format!("{:?} native", boundary.environment),
        None => "native runtime".into(),
    }
}

fn session_label(snapshot: &NodeTelemetrySnapshot) -> String {
    snapshot
        .local_peer_id
        .as_ref()
        .map(|peer_id| format!("peer {}", peer_id.as_str()))
        .unwrap_or_else(|| "local node".into())
}

fn validation_status(snapshot: &NodeTelemetrySnapshot, can_validate: bool) -> String {
    if !can_validate {
        return "unavailable".into();
    }
    match snapshot.node_state {
        NodeRuntimeState::PassiveValidator => "watching".into(),
        NodeRuntimeState::Quarantined => "blocked".into(),
        NodeRuntimeState::Revoked => "revoked".into(),
        _ => "ready".into(),
    }
}

fn validation_summary(snapshot: &NodeTelemetrySnapshot) -> String {
    if let Some(report) = snapshot.canary_reports.first() {
        return format!(
            "canary margin {:.4} · backdoor {}",
            report.regression_margin, report.detected_backdoor_trigger
        );
    }
    "native validator view".into()
}

fn training_summary(snapshot: &NodeTelemetrySnapshot) -> String {
    match snapshot.node_state {
        NodeRuntimeState::TrainingWindow => "training live".into(),
        NodeRuntimeState::PublishingUpdate => "publishing local update".into(),
        NodeRuntimeState::LeasePending => "waiting for lease".into(),
        _ => "native training runtime".into(),
    }
}

fn training_slice_status(snapshot: &NodeTelemetrySnapshot) -> String {
    match snapshot.slot_states.first() {
        Some(SlotRuntimeState::Assigned(_)) => "assignment ready".into(),
        Some(SlotRuntimeState::MaterializingBase(_)) => "materializing base checkpoint".into(),
        Some(SlotRuntimeState::FetchingShards(_)) => "downloading assigned slice".into(),
        Some(SlotRuntimeState::Training(_)) => "training assigned slice".into(),
        Some(SlotRuntimeState::Publishing(_)) => "publishing slice update".into(),
        Some(SlotRuntimeState::CoolingDown(_)) => "cooling down after slice".into(),
        Some(SlotRuntimeState::Migrating(_)) => "migrating to the next slice".into(),
        Some(SlotRuntimeState::Blocked { reason, .. }) => reason.clone(),
        Some(SlotRuntimeState::Unassigned) | None => "waiting for work".into(),
    }
}

fn network_note(snapshot: &NodeTelemetrySnapshot) -> String {
    format!(
        "{} connected · {} observed · {} admitted",
        snapshot.connected_peers,
        snapshot.observed_peer_ids.len(),
        snapshot.admitted_peers.len()
    )
}

fn node_is_blocked(node_state: NodeRuntimeState) -> bool {
    matches!(
        node_state,
        NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked | NodeRuntimeState::ShuttingDown
    )
}

fn has_training_role(roles: &crate::PeerRoleSet) -> bool {
    roles.contains(&PeerRole::TrainerCpu) || roles.contains(&PeerRole::TrainerGpu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ContentId, ControlPlaneSnapshot, ExperimentOptInPolicy, ExperimentResourceRequirements,
        ExperimentVisibility, LagPolicy, LagState, NetworkId, PeerRoleSet, RevocationEpoch,
        RuntimeStatus, WorkloadId,
    };
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn native_app_view_prefers_selected_entry() {
        let snapshot = NodeTelemetrySnapshot {
            status: RuntimeStatus::Running,
            network_id: Some(NetworkId::new("demo")),
            slot_states: vec![SlotRuntimeState::Unassigned],
            lag_state: LagState::Current,
            head_lag_steps: 0,
            lag_policy: LagPolicy::default(),
            local_peer_id: None,
            configured_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Validator]),
            node_state: NodeRuntimeState::TrainingWindow,
            connected_peers: 3,
            observed_peer_ids: BTreeSet::new(),
            known_peer_addresses: BTreeSet::new(),
            runtime_boundary: None,
            listen_addresses: Vec::new(),
            control_plane: ControlPlaneSnapshot::default(),
            recent_events: Vec::new(),
            last_snapshot_peer_id: None,
            last_snapshot: None,
            admitted_peers: BTreeMap::new(),
            rejected_peers: BTreeMap::new(),
            peer_reputation: BTreeMap::new(),
            minimum_revocation_epoch: Some(RevocationEpoch(0)),
            trust_bundle: None,
            in_flight_transfers: BTreeMap::new(),
            robustness_policy: None,
            latest_cohort_robustness: None,
            trust_scores: Vec::new(),
            canary_reports: Vec::new(),
            effective_limit_profile: None,
            last_error: None,
            started_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let entry = ExperimentDirectoryEntry {
            network_id: NetworkId::new("demo"),
            study_id: crate::StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            workload_id: WorkloadId::new("wl"),
            display_name: "demo".into(),
            model_schema_hash: ContentId::new("schema"),
            dataset_view_id: crate::DatasetViewId::new("dataset"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 10,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: RevisionId::new("rev"),
            current_head_id: Some(crate::HeadId::new("head")),
            allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Validator]),
            allowed_scopes: BTreeSet::new(),
            metadata: BTreeMap::new(),
        };

        let view = build_node_app_view(
            &snapshot,
            "http://127.0.0.1:9000",
            &[entry],
            Some(&NodeAppSelection::new(
                ExperimentId::new("exp"),
                RevisionId::new("rev"),
            )),
        );

        assert_eq!(view.default_surface, BrowserAppSurface::Train);
        assert_eq!(
            view.selected_experiment
                .as_ref()
                .expect("selected experiment")
                .experiment_id,
            "exp"
        );
        assert_eq!(view.network.transport, "native");
    }
}
