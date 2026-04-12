use burn_p2p_views::{
    BrowserAppClientView, BrowserAppNetworkView, ExperimentPickerView,
    LifecycleAssignmentStatusView, ParticipantAppView, TrainingResultSummaryView,
};

/// Asserts that one browser app snapshot exposes the selected experiment.
pub fn assert_browser_client_selected_experiment(
    view: &BrowserAppClientView,
    experiment_id: &str,
    revision_id: &str,
) -> Result<(), String> {
    let selected = view
        .selected_experiment
        .as_ref()
        .ok_or_else(|| "browser client view does not have a selected experiment".to_owned())?;
    if selected.experiment_id != experiment_id {
        return Err(format!(
            "selected experiment mismatch: expected {experiment_id}, got {}",
            selected.experiment_id
        ));
    }
    if selected.revision_id != revision_id {
        return Err(format!(
            "selected revision mismatch: expected {revision_id}, got {}",
            selected.revision_id
        ));
    }
    Ok(())
}

/// Asserts that one experiment picker contains one allowed revision.
pub fn assert_experiment_picker_contains_allowed_revision(
    picker: &ExperimentPickerView,
    experiment_id: &str,
    revision_id: &str,
) -> Result<(), String> {
    let entry = picker
        .entries
        .iter()
        .find(|entry| {
            entry.experiment_id.as_str() == experiment_id
                && entry.current_revision_id.as_str() == revision_id
        })
        .ok_or_else(|| {
            format!("experiment picker does not contain {experiment_id}/{revision_id}")
        })?;
    if !entry.allowed {
        return Err(format!(
            "experiment picker entry {experiment_id}/{revision_id} is not allowed"
        ));
    }
    Ok(())
}

/// Asserts that one training result snapshot looks complete enough for downstream smoke tests.
pub fn assert_training_result_complete(result: &TrainingResultSummaryView) -> Result<(), String> {
    if result.artifact_id.is_empty() {
        return Err("training result is missing artifact_id".into());
    }
    if result.window_secs == 0 {
        return Err("training result has zero window_secs".into());
    }
    Ok(())
}

/// Asserts that one browser network snapshot is connected enough for UI smoke tests.
pub fn assert_transport_health_ready(network: &BrowserAppNetworkView) -> Result<(), String> {
    if network.edge_base_url.is_empty() {
        return Err("network snapshot is missing edge_base_url".into());
    }
    if network.transport.is_empty() {
        return Err("network snapshot is missing transport label".into());
    }
    if network.last_error.is_some() {
        return Err(format!(
            "network snapshot surfaced last_error={:?}",
            network.last_error
        ));
    }
    Ok(())
}

/// Asserts that one participant snapshot exposes at least one accepted receipt.
pub fn assert_participant_has_receipts(
    participant: &ParticipantAppView,
    minimum_receipts: usize,
) -> Result<(), String> {
    if participant.accepted_receipts.len() < minimum_receipts {
        return Err(format!(
            "participant receipt count {} is below required minimum {}",
            participant.accepted_receipts.len(),
            minimum_receipts
        ));
    }
    Ok(())
}

/// Asserts that one lifecycle status snapshot matches the expected experiment binding.
pub fn assert_lifecycle_assignment_matches(
    status: &LifecycleAssignmentStatusView,
    experiment_label: &str,
    revision_label: &str,
) -> Result<(), String> {
    if status.experiment_label != experiment_label {
        return Err(format!(
            "lifecycle experiment mismatch: expected {experiment_label}, got {}",
            status.experiment_label
        ));
    }
    if status.revision_label != revision_label {
        return Err(format!(
            "lifecycle revision mismatch: expected {revision_label}, got {}",
            status.revision_label
        ));
    }
    Ok(())
}
