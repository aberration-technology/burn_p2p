use burn_p2p_workload::{
    WorkloadExecutionStage, WorkloadTrainingBudget, WorkloadTrainingProgress,
    WorkloadTrainingResult, WorkloadValidationProgress, WorkloadValidationResult,
};
use serde::{Deserialize, Serialize};

fn stage_label(stage: WorkloadExecutionStage) -> String {
    match stage {
        WorkloadExecutionStage::Preparing => "preparing",
        WorkloadExecutionStage::Executing => "executing",
        WorkloadExecutionStage::Publishing => "publishing",
        WorkloadExecutionStage::Completed => "completed",
    }
    .into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed view summary for one workload training budget.
pub struct TrainingBudgetSummaryView {
    /// Human-readable execution mode label.
    pub execution_mode: String,
    /// Maximum window duration in seconds.
    pub max_window_secs: u64,
    /// Maximum checkpoint bytes.
    pub max_checkpoint_bytes: u64,
    /// Maximum shard bytes.
    pub max_shard_bytes: u64,
    /// Maximum batch size, when one is suggested.
    pub max_batch_size: Option<u32>,
    /// Preferred precision label, when one is suggested.
    pub precision: Option<String>,
}

impl From<&WorkloadTrainingBudget> for TrainingBudgetSummaryView {
    fn from(value: &WorkloadTrainingBudget) -> Self {
        Self {
            execution_mode: if value.requires_webgpu {
                "webgpu-required".into()
            } else {
                "host-neutral".into()
            },
            max_window_secs: value.max_window_secs,
            max_checkpoint_bytes: value.max_checkpoint_bytes,
            max_shard_bytes: value.max_shard_bytes,
            max_batch_size: value.max_batch_size,
            precision: value
                .precision
                .as_ref()
                .map(|precision| format!("{precision:?}").to_lowercase()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed view summary for one training progress event.
pub struct TrainingProgressSummaryView {
    /// Current stage label.
    pub stage: String,
    /// Completed work units.
    pub completed_units: u64,
    /// Total work units, when known.
    pub total_units: Option<u64>,
    /// Optional supporting detail.
    pub detail: Option<String>,
}

impl From<&WorkloadTrainingProgress> for TrainingProgressSummaryView {
    fn from(value: &WorkloadTrainingProgress) -> Self {
        Self {
            stage: stage_label(value.stage),
            completed_units: value.completed_units,
            total_units: value.total_units,
            detail: value.detail.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed view summary for one validation progress event.
pub struct ValidationProgressSummaryView {
    /// Current stage label.
    pub stage: String,
    /// Completed work units.
    pub completed_units: u64,
    /// Total work units, when known.
    pub total_units: Option<u64>,
    /// Optional supporting detail.
    pub detail: Option<String>,
}

impl From<&WorkloadValidationProgress> for ValidationProgressSummaryView {
    fn from(value: &WorkloadValidationProgress) -> Self {
        Self {
            stage: stage_label(value.stage),
            completed_units: value.completed_units,
            total_units: value.total_units,
            detail: value.detail.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed view summary for one training result.
pub struct TrainingResultSummaryView {
    /// Published artifact identifier.
    pub artifact_id: String,
    /// Emitted receipt identifier, when present.
    pub receipt_id: Option<String>,
    /// Window duration in seconds.
    pub window_secs: u64,
}

impl From<&WorkloadTrainingResult> for TrainingResultSummaryView {
    fn from(value: &WorkloadTrainingResult) -> Self {
        Self {
            artifact_id: value.artifact_id.as_str().into(),
            receipt_id: value.receipt_id.as_ref().map(|value| value.as_str().into()),
            window_secs: value.window_secs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed view summary for one validation result.
pub struct ValidationResultSummaryView {
    /// Head identifier under review.
    pub head_id: String,
    /// Whether validation accepted the checked material.
    pub accepted: bool,
    /// Checked chunk count.
    pub checked_chunks: usize,
    /// Emitted receipt identifier, when present.
    pub emitted_receipt_id: Option<String>,
}

impl From<&WorkloadValidationResult> for ValidationResultSummaryView {
    fn from(value: &WorkloadValidationResult) -> Self {
        Self {
            head_id: value.head_id.as_str().into(),
            accepted: value.accepted,
            checked_chunks: value.checked_chunks,
            emitted_receipt_id: value
                .emitted_receipt_id
                .as_ref()
                .map(|value| value.as_str().into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Compact runtime capability summary for reusable product panels.
pub struct RuntimeCapabilitySummaryView {
    /// Human-readable preferred role label.
    pub preferred_role: String,
    /// Human-readable backend availability note.
    pub backend_summary: String,
    /// Whether training is available on this runtime.
    pub can_train: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Compact lifecycle and assignment status summary for reusable product panels.
pub struct LifecycleAssignmentStatusView {
    /// Human-readable selected experiment label.
    pub experiment_label: String,
    /// Human-readable revision label.
    pub revision_label: String,
    /// Human-readable lifecycle phase label.
    pub lifecycle_phase: String,
    /// Human-readable assignment status.
    pub assignment_status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Compact trust and participation summary for reusable product panels.
pub struct ParticipationTrustSummaryView {
    /// Human-readable session or identity label.
    pub principal_label: String,
    /// Human-readable trust summary.
    pub trust_summary: String,
    /// Human-readable runtime participation status.
    pub participation_status: String,
}
