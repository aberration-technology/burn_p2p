use serde::{Deserialize, Serialize};

use burn_p2p::{
    ArtifactTargetKind, BrowserJoinPolicy, BrowserRole, ExperimentDirectoryAccess,
    ExperimentDirectoryEntry, ExperimentId, ExperimentScope, RevisionId,
    browser_join_policy_for_entry,
};
use burn_p2p_core::{BrowserDirectorySnapshot, BrowserPortalSnapshot};
use burn_p2p_ui::{
    AuthPortalView, BrowserExperimentPickerCard, BrowserExperimentPickerState,
    BrowserExperimentPickerView, ExperimentPickerView, ParticipantPortalView,
};

use crate::{
    BrowserCapabilityReport, BrowserRuntimeRole, BrowserRuntimeState, BrowserSessionState,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser edge endpoints.
pub struct BrowserEdgeEndpoints {
    /// The portal snapshot path.
    pub portal_snapshot_path: String,
    /// The directory path.
    pub directory_path: String,
    /// The heads path.
    pub heads_path: String,
    /// The signed directory path.
    pub signed_directory_path: String,
    /// The leaderboard path.
    pub leaderboard_path: String,
    /// The signed leaderboard path.
    pub signed_leaderboard_path: String,
    /// The receipt submit path.
    pub receipt_submit_path: String,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The events path.
    pub events_path: String,
    /// The metrics path.
    pub metrics_path: String,
    /// The metrics snapshot path.
    pub metrics_snapshot_path: String,
    /// The metrics ledger path prefix.
    pub metrics_ledger_path: String,
    /// The live metrics stream path.
    pub metrics_live_path: String,
    /// The latest live metrics frame path.
    pub metrics_live_latest_path: String,
    /// The metrics catchup path prefix.
    pub metrics_catchup_path: String,
    /// The candidate-focused metrics path prefix.
    pub metrics_candidates_path: String,
    /// The disagreement-focused metrics path prefix.
    pub metrics_disagreements_path: String,
    /// The peer-window distribution metrics path prefix.
    pub metrics_peer_windows_path: String,
    /// The head metrics path prefix.
    pub metrics_heads_path: String,
    /// The experiment metrics path prefix.
    pub metrics_experiments_path: String,
    /// The artifact alias path prefix.
    pub artifacts_aliases_path: String,
    /// The live artifact event stream path.
    pub artifacts_live_path: String,
    /// The latest artifact live-event path.
    pub artifacts_live_latest_path: String,
    /// The run-scoped artifact history path prefix.
    pub artifacts_runs_path: String,
    /// The head-scoped artifact path prefix.
    pub artifacts_heads_path: String,
    /// The artifact export path.
    pub artifacts_export_path: String,
    /// The artifact job path prefix.
    pub artifacts_export_jobs_path: String,
    /// The artifact download-ticket path.
    pub artifacts_download_ticket_path: String,
    /// The artifact download path prefix.
    pub artifacts_download_path: String,
    /// The trust path.
    pub trust_path: String,
    /// The reenrollment path.
    pub reenrollment_path: String,
}

impl Default for BrowserEdgeEndpoints {
    fn default() -> Self {
        Self {
            portal_snapshot_path: "/portal/snapshot".into(),
            directory_path: "/directory".into(),
            heads_path: "/heads".into(),
            signed_directory_path: "/directory/signed".into(),
            leaderboard_path: "/leaderboard".into(),
            signed_leaderboard_path: "/leaderboard/signed".into(),
            receipt_submit_path: "/receipts/browser".into(),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            events_path: "/events".into(),
            metrics_path: "/metrics".into(),
            metrics_snapshot_path: "/metrics/snapshot".into(),
            metrics_ledger_path: "/metrics/ledger".into(),
            metrics_live_path: "/metrics/live".into(),
            metrics_live_latest_path: "/metrics/live/latest".into(),
            metrics_catchup_path: "/metrics/catchup".into(),
            metrics_candidates_path: "/metrics/candidates".into(),
            metrics_disagreements_path: "/metrics/disagreements".into(),
            metrics_peer_windows_path: "/metrics/peer-windows".into(),
            metrics_heads_path: "/metrics/heads".into(),
            metrics_experiments_path: "/metrics/experiments".into(),
            artifacts_aliases_path: "/artifacts/aliases".into(),
            artifacts_live_path: "/artifacts/live".into(),
            artifacts_live_latest_path: "/artifacts/live/latest".into(),
            artifacts_runs_path: "/artifacts/runs".into(),
            artifacts_heads_path: "/artifacts/heads".into(),
            artifacts_export_path: "/artifacts/export".into(),
            artifacts_export_jobs_path: "/artifacts/export".into(),
            artifacts_download_ticket_path: "/artifacts/download-ticket".into(),
            artifacts_download_path: "/artifacts/download".into(),
            trust_path: "/trust".into(),
            reenrollment_path: "/reenrollment".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser UI bindings.
pub struct BrowserUiBindings {
    /// The edge base URL.
    pub edge_base_url: String,
    /// The paths.
    pub paths: BrowserEdgeEndpoints,
}

impl BrowserUiBindings {
    /// Creates a new value.
    pub fn new(edge_base_url: impl Into<String>) -> Self {
        Self {
            edge_base_url: edge_base_url.into().trim_end_matches('/').to_owned(),
            paths: BrowserEdgeEndpoints::default(),
        }
    }

    /// Creates a value from the portal snapshot.
    pub fn from_portal_snapshot(
        edge_base_url: impl Into<String>,
        snapshot: &BrowserPortalSnapshot,
    ) -> Self {
        let primary_provider = snapshot.login_providers.first();
        Self {
            edge_base_url: edge_base_url.into().trim_end_matches('/').to_owned(),
            paths: BrowserEdgeEndpoints {
                portal_snapshot_path: snapshot.paths.portal_snapshot_path.clone(),
                directory_path: snapshot.paths.directory_path.clone(),
                heads_path: snapshot.paths.heads_path.clone(),
                signed_directory_path: snapshot.paths.signed_directory_path.clone(),
                leaderboard_path: snapshot.paths.leaderboard_path.clone(),
                signed_leaderboard_path: snapshot.paths.signed_leaderboard_path.clone(),
                receipt_submit_path: snapshot.paths.receipt_submit_path.clone(),
                login_path: primary_provider
                    .map(|provider| provider.login_path.clone())
                    .unwrap_or_else(|| snapshot.paths.login_path.clone()),
                callback_path: primary_provider
                    .and_then(|provider| provider.callback_path.clone())
                    .unwrap_or_else(|| snapshot.paths.callback_path.clone()),
                enroll_path: snapshot.paths.enroll_path.clone(),
                events_path: snapshot.paths.event_stream_path.clone(),
                metrics_path: snapshot.paths.metrics_path.clone(),
                metrics_snapshot_path: snapshot.paths.metrics_snapshot_path.clone(),
                metrics_ledger_path: snapshot.paths.metrics_ledger_path.clone(),
                metrics_live_path: snapshot.paths.metrics_live_path.clone(),
                metrics_live_latest_path: snapshot.paths.metrics_live_latest_path.clone(),
                metrics_catchup_path: snapshot.paths.metrics_catchup_path.clone(),
                metrics_candidates_path: snapshot.paths.metrics_candidates_path.clone(),
                metrics_disagreements_path: snapshot.paths.metrics_disagreements_path.clone(),
                metrics_peer_windows_path: snapshot.paths.metrics_peer_windows_path.clone(),
                metrics_heads_path: snapshot.paths.metrics_heads_path.clone(),
                metrics_experiments_path: snapshot.paths.metrics_experiments_path.clone(),
                artifacts_aliases_path: snapshot.paths.artifacts_aliases_path.clone(),
                artifacts_live_path: snapshot.paths.artifacts_live_path.clone(),
                artifacts_live_latest_path: snapshot.paths.artifacts_live_latest_path.clone(),
                artifacts_runs_path: snapshot.paths.artifacts_runs_path.clone(),
                artifacts_heads_path: snapshot.paths.artifacts_heads_path.clone(),
                artifacts_export_path: snapshot.paths.artifacts_export_path.clone(),
                artifacts_export_jobs_path: snapshot.paths.artifacts_export_jobs_path.clone(),
                artifacts_download_ticket_path: snapshot
                    .paths
                    .artifacts_download_ticket_path
                    .clone(),
                artifacts_download_path: snapshot.paths.artifacts_download_path.clone(),
                trust_path: snapshot.paths.trust_bundle_path.clone(),
                reenrollment_path: snapshot.paths.reenrollment_path.clone(),
            },
        }
    }

    /// Performs the endpoint URL operation.
    pub fn endpoint_url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            return path.to_owned();
        }

        if let Ok(url) = reqwest::Url::parse(&self.edge_base_url)
            && path.starts_with('/')
        {
            let base_path = url.path().trim_end_matches('/');
            if !base_path.is_empty() && base_path != "/" && path.starts_with(base_path) {
                let mut origin =
                    format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
                if let Some(port) = url.port() {
                    origin.push(':');
                    origin.push_str(&port.to_string());
                }
                return format!("{origin}{path}");
            }
        }

        format!("{}{}", self.edge_base_url, path)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures browser portal UI state.
pub struct BrowserPortalUiState {
    /// The auth.
    pub auth: Option<AuthPortalView>,
    /// The experiment picker.
    pub experiment_picker: Option<ExperimentPickerView>,
    /// The browser experiment picker.
    pub browser_experiment_picker: Option<BrowserExperimentPickerView>,
    /// The participant.
    pub participant: Option<ParticipantPortalView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser experiment candidate.
pub struct BrowserExperimentCandidate {
    /// The policy.
    pub policy: BrowserJoinPolicy,
    /// The recommended role.
    pub recommended_role: Option<BrowserRole>,
    /// The fallback from preferred.
    pub fallback_from_preferred: bool,
    /// The recommended state.
    pub recommended_state: BrowserRuntimeState,
}

fn allowed_by_scopes(entry: &ExperimentDirectoryEntry, scopes: &[ExperimentScope]) -> bool {
    if scopes.is_empty() {
        return entry.is_visible_to(&std::collections::BTreeSet::new());
    }
    let scope_set = scopes
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    entry
        .allowed_scopes
        .iter()
        .any(|scope| scope_set.contains(scope))
}

fn scoped_browser_join_policy(
    entry: &ExperimentDirectoryEntry,
    mut policy: BrowserJoinPolicy,
    scopes: &[ExperimentScope],
) -> BrowserJoinPolicy {
    if allowed_by_scopes(entry, scopes) {
        return policy;
    }

    policy
        .blocked_reasons
        .push("session scopes do not allow this experiment".into());
    policy
        .eligible_roles
        .retain(|role| matches!(role, BrowserRole::PortalViewer));
    policy
}

fn browser_picker_state_from_runtime(state: &BrowserRuntimeState) -> BrowserExperimentPickerState {
    match state {
        BrowserRuntimeState::PortalOnly => BrowserExperimentPickerState::PortalOnly,
        BrowserRuntimeState::Joining { role, .. } => match role {
            BrowserRuntimeRole::PortalViewer => BrowserExperimentPickerState::PortalOnly,
            BrowserRuntimeRole::BrowserObserver | BrowserRuntimeRole::BrowserFallback => {
                BrowserExperimentPickerState::Observer
            }
            BrowserRuntimeRole::BrowserVerifier => BrowserExperimentPickerState::Verifier,
            BrowserRuntimeRole::BrowserTrainerWgpu => BrowserExperimentPickerState::Trainer,
        },
        BrowserRuntimeState::Observer => BrowserExperimentPickerState::Observer,
        BrowserRuntimeState::Verifier => BrowserExperimentPickerState::Verifier,
        BrowserRuntimeState::Trainer => BrowserExperimentPickerState::Trainer,
        BrowserRuntimeState::BackgroundSuspended { .. } => {
            BrowserExperimentPickerState::BackgroundSuspended
        }
        BrowserRuntimeState::Catchup { .. } => BrowserExperimentPickerState::Catchup,
        BrowserRuntimeState::Blocked { .. } => BrowserExperimentPickerState::Blocked,
    }
}

fn session_scopes(session: Option<&BrowserSessionState>) -> Vec<ExperimentScope> {
    session
        .and_then(|session| session.session.as_ref())
        .map(|session| session.claims.granted_scopes.iter().cloned().collect())
        .unwrap_or_default()
}

/// Performs the browser join policies from directory operation.
pub fn browser_join_policies_from_directory(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
) -> Vec<BrowserJoinPolicy> {
    let target_artifact = ArtifactTargetKind::parse(target_artifact_id);
    let capabilities = capability_report.capabilities();
    directory
        .entries
        .iter()
        .map(|entry| browser_join_policy_for_entry(entry, &target_artifact, &capabilities))
        .collect()
}

fn browser_experiment_candidates_with_scopes(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: Option<&[ExperimentScope]>,
) -> Vec<BrowserExperimentCandidate> {
    let target_artifact = ArtifactTargetKind::parse(target_artifact_id);
    let capabilities = capability_report.capabilities();
    directory
        .entries
        .iter()
        .map(|entry| {
            let policy = browser_join_policy_for_entry(entry, &target_artifact, &capabilities);
            let policy = match scopes {
                Some(scopes) => scoped_browser_join_policy(entry, policy, scopes),
                None => policy,
            };
            BrowserExperimentCandidate {
                recommended_role: policy.recommended_role(preferred_role.as_browser_role()),
                fallback_from_preferred: policy
                    .recommended_role(preferred_role.as_browser_role())
                    .is_some_and(|role| role != preferred_role.as_browser_role()),
                recommended_state: BrowserRuntimeState::from_join_policy(
                    &policy,
                    preferred_role.clone(),
                ),
                policy,
            }
        })
        .collect()
}

/// Performs the browser experiment candidates from directory operation.
pub fn browser_experiment_candidates_from_directory(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
) -> Vec<BrowserExperimentCandidate> {
    browser_experiment_candidates_with_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        None,
    )
}

/// Performs the browser experiment candidates for scopes operation.
pub fn browser_experiment_candidates_for_scopes(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: &[ExperimentScope],
) -> Vec<BrowserExperimentCandidate> {
    browser_experiment_candidates_with_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        Some(scopes),
    )
}

/// Performs the browser experiment candidate for selection operation.
pub fn browser_experiment_candidate_for_selection(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    experiment_id: &ExperimentId,
    revision_id: Option<&RevisionId>,
    scopes: &[ExperimentScope],
) -> Option<BrowserExperimentCandidate> {
    browser_experiment_candidates_for_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        scopes,
    )
    .into_iter()
    .find(|candidate| {
        candidate.policy.experiment_id == *experiment_id
            && revision_id
                .map(|revision_id| candidate.policy.revision_id == *revision_id)
                .unwrap_or(true)
    })
}

/// Performs the recommended browser candidate for scopes operation.
pub fn recommended_browser_candidate_for_scopes(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: &[ExperimentScope],
) -> Option<BrowserExperimentCandidate> {
    let candidates = browser_experiment_candidates_for_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role.clone(),
        scopes,
    );
    let preferred_role = preferred_role.as_browser_role();
    candidates
        .iter()
        .find(|candidate| candidate.policy.eligible_roles.contains(&preferred_role))
        .cloned()
        .or_else(|| {
            candidates
                .iter()
                .find(|candidate| candidate.policy.allows_peer_join())
                .cloned()
        })
        .or_else(|| {
            candidates
                .iter()
                .find(|candidate| candidate.policy.allows_portal_only())
                .cloned()
        })
        .or_else(|| {
            candidates
                .iter()
                .find(|candidate| candidate.policy.allows_join())
                .cloned()
        })
}

/// Performs the browser experiment picker view from directory operation.
pub fn browser_experiment_picker_view_from_directory(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: &[ExperimentScope],
) -> BrowserExperimentPickerView {
    let candidates = browser_experiment_candidates_for_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        scopes,
    );
    let candidate_map = candidates
        .into_iter()
        .map(|candidate| {
            (
                (
                    candidate.policy.experiment_id.clone(),
                    candidate.policy.revision_id.clone(),
                ),
                candidate,
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let entries = directory
        .entries
        .iter()
        .filter_map(|entry| {
            let key = (
                entry.experiment_id.clone(),
                entry.current_revision_id.clone(),
            );
            let candidate = candidate_map.get(&key)?;
            Some(BrowserExperimentPickerCard {
                experiment_id: entry.experiment_id.clone(),
                study_id: entry.study_id.clone(),
                display_name: entry.display_name.clone(),
                current_revision_id: entry.current_revision_id.clone(),
                current_head_id: entry.current_head_id.clone(),
                estimated_download_bytes: entry.resource_requirements.estimated_download_bytes,
                estimated_window_seconds: entry.resource_requirements.estimated_window_seconds,
                allowed: allowed_by_scopes(entry, scopes),
                recommended_state: browser_picker_state_from_runtime(&candidate.recommended_state),
                recommended_role: candidate.recommended_role,
                fallback_from_preferred: candidate.fallback_from_preferred,
                eligible_roles: candidate.policy.eligible_roles.clone(),
                blocked_reasons: candidate.policy.blocked_reasons.clone(),
            })
        })
        .collect();

    BrowserExperimentPickerView::new(directory.network_id.clone(), entries)
}

/// Performs the recommended browser join policy operation.
pub fn recommended_browser_join_policy(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
) -> Option<BrowserJoinPolicy> {
    let policies =
        browser_join_policies_from_directory(directory, target_artifact_id, capability_report);
    let preferred_role = preferred_role.as_browser_role();
    policies
        .iter()
        .find(|policy| policy.eligible_roles.contains(&preferred_role))
        .cloned()
        .into_iter()
        .chain(
            policies
                .iter()
                .filter(|policy| policy.allows_peer_join())
                .cloned(),
        )
        .chain(
            policies
                .iter()
                .filter(|policy| policy.allows_portal_only())
                .cloned(),
        )
        .chain(
            policies
                .iter()
                .filter(|policy| policy.allows_join())
                .cloned(),
        )
        .next()
}

/// Performs the recommended browser join policy for scopes operation.
pub fn recommended_browser_join_policy_for_scopes(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: &[ExperimentScope],
) -> Option<BrowserJoinPolicy> {
    recommended_browser_candidate_for_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        scopes,
    )
    .map(|candidate| candidate.policy)
}

/// Performs the recommended browser runtime state operation.
pub fn recommended_browser_runtime_state(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
) -> Option<BrowserRuntimeState> {
    let fallback_state = browser_experiment_candidates_from_directory(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role.clone(),
    )
    .into_iter()
    .map(|candidate| candidate.recommended_state)
    .next();

    recommended_browser_join_policy(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role.clone(),
    )
    .map(|policy| BrowserRuntimeState::from_join_policy(&policy, preferred_role))
    .or(fallback_state)
}

/// Performs the recommended browser runtime state for scopes operation.
pub fn recommended_browser_runtime_state_for_scopes(
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
    scopes: &[ExperimentScope],
) -> Option<BrowserRuntimeState> {
    recommended_browser_candidate_for_scopes(
        directory,
        target_artifact_id,
        capability_report,
        preferred_role,
        scopes,
    )
    .map(|candidate| candidate.recommended_state)
}

/// Performs the browser portal UI state from directory operation.
pub fn browser_portal_ui_state_from_directory(
    auth: Option<AuthPortalView>,
    participant: Option<ParticipantPortalView>,
    session: Option<&BrowserSessionState>,
    directory: &BrowserDirectorySnapshot,
    target_artifact_id: &str,
    capability_report: &BrowserCapabilityReport,
    preferred_role: BrowserRuntimeRole,
) -> BrowserPortalUiState {
    let scopes = session_scopes(session);
    BrowserPortalUiState {
        auth,
        experiment_picker: Some(ExperimentPickerView::from_directory(
            directory.network_id.clone(),
            directory.entries.clone(),
            &scopes,
        )),
        browser_experiment_picker: Some(browser_experiment_picker_view_from_directory(
            directory,
            target_artifact_id,
            capability_report,
            preferred_role,
            &scopes,
        )),
        participant,
    }
}
