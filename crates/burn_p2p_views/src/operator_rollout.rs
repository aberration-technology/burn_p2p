use burn_p2p_core::ExperimentDirectoryEntry;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One admin/session summary row shown by downstream operator shells.
pub struct AdminSessionSummaryView {
    /// Human-readable session label.
    pub session_label: String,
    /// Principal label or identifier, when present.
    #[serde(default)]
    pub principal_label: Option<String>,
    /// Provider label, when present.
    #[serde(default)]
    pub provider_label: Option<String>,
    /// Session identifier, when present.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Whether the session is currently authorized to submit rollouts.
    pub rollout_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One key/value metadata row rendered for a directory entry.
pub struct DirectoryEntryMetadataView {
    /// Metadata key.
    pub key: String,
    /// Metadata value.
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One typed directory entry row for operator list/detail rendering.
pub struct ExperimentDirectoryEntryView {
    /// Human-readable experiment name.
    pub display_name: String,
    /// Study identifier.
    pub study_id: String,
    /// Experiment identifier.
    pub experiment_id: String,
    /// Current revision identifier.
    pub revision_id: String,
    /// Workload identifier.
    pub workload_id: String,
    /// Human-readable visibility label.
    pub visibility: String,
    /// Human-readable role summary.
    pub allowed_roles: Vec<String>,
    /// Human-readable scope summary.
    pub allowed_scopes: Vec<String>,
    /// Attached metadata rows.
    pub metadata: Vec<DirectoryEntryMetadataView>,
}

impl From<&ExperimentDirectoryEntry> for ExperimentDirectoryEntryView {
    fn from(entry: &ExperimentDirectoryEntry) -> Self {
        Self {
            display_name: entry.display_name.clone(),
            study_id: entry.study_id.as_str().to_owned(),
            experiment_id: entry.experiment_id.as_str().to_owned(),
            revision_id: entry.current_revision_id.as_str().to_owned(),
            workload_id: entry.workload_id.as_str().to_owned(),
            visibility: format!("{:?}", entry.visibility),
            allowed_roles: entry
                .allowed_roles
                .roles
                .iter()
                .map(|role| format!("{role:?}"))
                .collect(),
            allowed_scopes: entry
                .allowed_scopes
                .iter()
                .map(|scope| format!("{scope:?}"))
                .collect(),
            metadata: entry
                .metadata
                .iter()
                .map(|(key, value)| DirectoryEntryMetadataView {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Directory list/detail state used by downstream operator panels.
pub struct ExperimentDirectoryListView {
    /// JSON path used to fetch the unsigned directory.
    pub directory_path: String,
    /// JSON path used to fetch the signed directory.
    pub signed_directory_path: String,
    /// Selected experiment identifier, when one is focused.
    #[serde(default)]
    pub selected_experiment_id: Option<String>,
    /// Selected revision identifier, when one is focused.
    #[serde(default)]
    pub selected_revision_id: Option<String>,
    /// Visible directory rows.
    pub entries: Vec<ExperimentDirectoryEntryView>,
}

impl ExperimentDirectoryListView {
    /// Builds one list view from raw directory entries.
    pub fn from_entries(
        directory_path: impl Into<String>,
        signed_directory_path: impl Into<String>,
        selected_experiment_id: Option<String>,
        selected_revision_id: Option<String>,
        entries: &[ExperimentDirectoryEntry],
    ) -> Self {
        Self {
            directory_path: directory_path.into(),
            signed_directory_path: signed_directory_path.into(),
            selected_experiment_id,
            selected_revision_id,
            entries: entries.iter().map(Self::entry_view).collect(),
        }
    }

    fn entry_view(entry: &ExperimentDirectoryEntry) -> ExperimentDirectoryEntryView {
        ExperimentDirectoryEntryView::from(entry)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Editable directory-entry draft state for downstream operator forms.
pub struct DirectoryEntryDraftView {
    /// Human-readable experiment name.
    pub display_name: String,
    /// Study identifier.
    pub study_id: String,
    /// Experiment identifier.
    pub experiment_id: String,
    /// Current revision identifier.
    pub revision_id: String,
    /// Workload identifier.
    pub workload_id: String,
    /// Human-readable visibility label.
    pub visibility: String,
    /// Human-readable role list.
    pub allowed_roles: Vec<String>,
    /// Human-readable scope list.
    pub allowed_scopes: Vec<String>,
    /// Pretty-printed metadata JSON used by editors.
    pub metadata_json: String,
}

impl DirectoryEntryDraftView {
    /// Builds one editable draft from a typed directory entry.
    pub fn from_entry(entry: &ExperimentDirectoryEntry) -> Self {
        Self {
            display_name: entry.display_name.clone(),
            study_id: entry.study_id.as_str().to_owned(),
            experiment_id: entry.experiment_id.as_str().to_owned(),
            revision_id: entry.current_revision_id.as_str().to_owned(),
            workload_id: entry.workload_id.as_str().to_owned(),
            visibility: format!("{:?}", entry.visibility),
            allowed_roles: entry
                .allowed_roles
                .roles
                .iter()
                .map(|role| format!("{role:?}"))
                .collect(),
            allowed_scopes: entry
                .allowed_scopes
                .iter()
                .map(|scope| format!("{scope:?}"))
                .collect(),
            metadata_json: serde_json::to_string_pretty(&entry.metadata)
                .unwrap_or_else(|_| "{}".into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed rollout preview state for downstream operator flows.
pub struct RolloutPreviewView {
    /// Human-readable preview label.
    pub summary_label: String,
    /// Target path used for the submission.
    pub submit_path: String,
    /// Whether an authenticated rollout session is required.
    pub requires_session: bool,
    /// Entries that will be applied by the rollout.
    pub entries: Vec<ExperimentDirectoryEntryView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed mutation result summary for one directory rollout.
pub struct DirectoryMutationResultView {
    /// Human-readable status label.
    pub status_label: String,
    /// Number of directory entries surfaced after the mutation.
    pub directory_entries: usize,
    /// Number of trusted issuers retained after the mutation.
    pub trusted_issuers: usize,
    /// Whether the mutation requires re-enrollment.
    pub reenrollment_required: bool,
}
