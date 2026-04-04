use burn_p2p_core::{
    ArtifactId, AssignmentLease, AuthProvider, BrowserRole, ContributionReceipt,
    ExperimentDirectoryEntry, ExperimentId, ExperimentScope, HeadId, NetworkId, PeerId, RevisionId,
    StudyId, TelemetrySummary,
};
use burn_p2p_security::PeerTrustLevel;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one login provider in the auth portal.
pub struct LoginProviderView {
    /// Auth provider identifier.
    pub provider: AuthProvider,
    /// Human-readable provider label.
    pub label: String,
    /// Path that begins the login flow.
    pub login_path: String,
    /// Optional browser callback path.
    pub callback_path: Option<String>,
    /// Optional device-flow path.
    pub device_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one trust badge shown beside an identity.
pub struct TrustBadgeView {
    /// Underlying trust level.
    pub level: PeerTrustLevel,
    /// Human-readable trust label.
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Summarizes the currently authenticated participant identity.
pub struct ContributionIdentityPanel {
    /// Principal identifier.
    pub principal_id: String,
    /// Provider display label.
    pub provider_label: String,
    /// Trust badges shown for the session.
    pub trust_badges: Vec<TrustBadgeView>,
    /// Experiments the session is scoped to.
    pub scoped_experiments: Vec<ExperimentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// View model for the auth portal surface.
pub struct AuthPortalView {
    /// Current network identifier.
    pub network_id: NetworkId,
    /// Login providers currently available.
    pub providers: Vec<LoginProviderView>,
    /// Active session, when present.
    pub active_session: Option<ContributionIdentityPanel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One experiment row in the generic experiment picker.
pub struct ExperimentPickerCard {
    /// Experiment identifier.
    pub experiment_id: ExperimentId,
    /// Study identifier.
    pub study_id: StudyId,
    /// Human-readable display name.
    pub display_name: String,
    /// Current revision identifier.
    pub current_revision_id: RevisionId,
    /// Current certified head identifier, if one exists.
    pub current_head_id: Option<HeadId>,
    /// Estimated download bytes required to participate.
    pub estimated_download_bytes: u64,
    /// Estimated window duration in seconds.
    pub estimated_window_seconds: u64,
    /// Whether the current session is allowed to access the experiment.
    pub allowed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Network-scoped experiment picker view.
pub struct ExperimentPickerView {
    /// Current network identifier.
    pub network_id: NetworkId,
    /// Sorted experiment cards.
    pub entries: Vec<ExperimentPickerCard>,
}

impl ExperimentPickerView {
    /// Builds a picker view from signed directory entries and visible scopes.
    pub fn from_directory(
        network_id: NetworkId,
        entries: Vec<ExperimentDirectoryEntry>,
        scopes: &[ExperimentScope],
    ) -> Self {
        let scope_set = scopes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut cards = entries
            .into_iter()
            .map(|entry| ExperimentPickerCard {
                experiment_id: entry.experiment_id.clone(),
                study_id: entry.study_id,
                display_name: entry.display_name,
                current_revision_id: entry.current_revision_id,
                current_head_id: entry.current_head_id,
                estimated_download_bytes: entry.resource_requirements.estimated_download_bytes,
                estimated_window_seconds: entry.resource_requirements.estimated_window_seconds,
                allowed: entry
                    .allowed_scopes
                    .iter()
                    .any(|scope| scope_set.contains(scope)),
            })
            .collect::<Vec<_>>();
        cards.sort_by(|left, right| left.display_name.cmp(&right.display_name));

        Self {
            network_id,
            entries: cards,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the browser-specific experiment picker states.
pub enum BrowserExperimentPickerState {
    /// Portal-only visibility with no peer participation.
    PortalOnly,
    /// Observer participation is recommended.
    Observer,
    /// Verifier participation is recommended.
    Verifier,
    /// Browser trainer participation is recommended.
    Trainer,
    /// The browser is suspended in the background.
    BackgroundSuspended,
    /// The browser must catch up before rejoining.
    Catchup,
    /// The browser is blocked from participating.
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One browser-specific experiment picker row with role guidance.
pub struct BrowserExperimentPickerCard {
    /// Experiment identifier.
    pub experiment_id: ExperimentId,
    /// Study identifier.
    pub study_id: StudyId,
    /// Human-readable display name.
    pub display_name: String,
    /// Current revision identifier.
    pub current_revision_id: RevisionId,
    /// Current certified head identifier, if one exists.
    pub current_head_id: Option<HeadId>,
    /// Estimated download bytes required to participate.
    pub estimated_download_bytes: u64,
    /// Estimated window duration in seconds.
    pub estimated_window_seconds: u64,
    /// Whether the session is allowed to access the experiment.
    pub allowed: bool,
    /// Recommended browser participation state.
    pub recommended_state: BrowserExperimentPickerState,
    /// Recommended browser role, when one is allowed.
    pub recommended_role: Option<BrowserRole>,
    /// Whether the selected role is a fallback from the preferred request.
    pub fallback_from_preferred: bool,
    /// Browser roles that remain eligible.
    pub eligible_roles: Vec<BrowserRole>,
    /// Human-readable block reasons for the browser.
    pub blocked_reasons: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Browser-scoped experiment picker view.
pub struct BrowserExperimentPickerView {
    /// Current network identifier.
    pub network_id: NetworkId,
    /// Sorted browser picker cards.
    pub entries: Vec<BrowserExperimentPickerCard>,
}

impl BrowserExperimentPickerView {
    /// Creates a sorted browser picker view.
    pub fn new(network_id: NetworkId, mut entries: Vec<BrowserExperimentPickerCard>) -> Self {
        entries.sort_by(|left, right| left.display_name.cmp(&right.display_name));
        Self {
            network_id,
            entries,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Link to a connected GitHub profile.
pub struct GitHubProfileLink {
    /// GitHub login.
    pub login: String,
    /// Canonical profile URL.
    pub profile_url: String,
    /// Timestamp when the profile was linked.
    pub linked_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Participant-facing profile summary.
pub struct ParticipantProfile {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Optional display name.
    pub display_name: Option<String>,
    /// Optional GitHub link metadata.
    pub github: Option<GitHubProfileLink>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One downloadable checkpoint artifact offered to a participant.
pub struct CheckpointDownload {
    /// Head identifier represented by the artifact.
    pub head_id: HeadId,
    /// Artifact identifier to download.
    pub artifact_id: ArtifactId,
    /// Human-readable label.
    pub label: String,
    /// Download path surfaced by the edge.
    pub download_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Participant-facing portal view with receipts, telemetry, and downloads.
pub struct ParticipantPortalView {
    /// Linked profile summary.
    pub profile: ParticipantProfile,
    /// Current shard assignment, when present.
    pub current_assignment: Option<AssignmentLease>,
    /// Local telemetry summary, when available.
    pub local_telemetry: Option<TelemetrySummary>,
    /// Accepted receipts sorted by newest first.
    pub accepted_receipts: Vec<ContributionReceipt>,
    /// Most recent accepted receipt, when present.
    pub latest_accepted_receipt: Option<ContributionReceipt>,
    /// Most recent published artifact identifier, when present.
    pub latest_published_artifact_id: Option<ArtifactId>,
    /// Available checkpoint downloads.
    pub checkpoint_downloads: Vec<CheckpointDownload>,
}

impl ParticipantPortalView {
    /// Creates a participant portal view and normalizes receipt ordering.
    pub fn new(
        profile: ParticipantProfile,
        current_assignment: Option<AssignmentLease>,
        local_telemetry: Option<TelemetrySummary>,
        mut accepted_receipts: Vec<ContributionReceipt>,
        latest_published_artifact_id: Option<ArtifactId>,
        checkpoint_downloads: Vec<CheckpointDownload>,
    ) -> Self {
        accepted_receipts.sort_by_key(|receipt| std::cmp::Reverse(receipt.accepted_at));
        let latest_accepted_receipt = accepted_receipts.first().cloned();

        Self {
            profile,
            current_assignment,
            local_telemetry,
            accepted_receipts,
            latest_accepted_receipt,
            latest_published_artifact_id,
            checkpoint_downloads,
        }
    }
}
