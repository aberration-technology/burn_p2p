use burn_p2p_core::{
    ArtifactId, ContributionReceiptId, ExperimentId, HeadId, Precision, RevisionId, StudyId,
    WorkloadId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Declares the host-neutral execution stage for one workload task.
pub enum WorkloadExecutionStage {
    /// The workload is validating the plan or preparing local state.
    Preparing,
    /// The workload is actively executing compute.
    Executing,
    /// The workload is materializing or publishing output.
    Publishing,
    /// The workload has finished the requested operation.
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral training budget.
pub struct WorkloadTrainingBudget {
    /// Maximum allowed wall-clock execution time for one training window.
    pub max_window_secs: u64,
    /// Maximum checkpoint bytes the host may materialize or accept.
    pub max_checkpoint_bytes: u64,
    /// Maximum assigned shard bytes the host should consume.
    pub max_shard_bytes: u64,
    /// Whether execution requires a WebGPU-capable browser host.
    pub requires_webgpu: bool,
    /// Optional maximum batch size for the host.
    pub max_batch_size: Option<u32>,
    /// Optional preferred numeric precision for the host.
    pub precision: Option<Precision>,
}

impl Default for WorkloadTrainingBudget {
    fn default() -> Self {
        Self {
            max_window_secs: 30,
            max_checkpoint_bytes: 16 * 1024 * 1024,
            max_shard_bytes: 8 * 1024 * 1024,
            requires_webgpu: true,
            max_batch_size: Some(4),
            precision: Some(Precision::Fp16),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral training plan.
pub struct WorkloadTrainingPlan {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The execution budget.
    pub budget: WorkloadTrainingBudget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral training progress event.
pub struct WorkloadTrainingProgress {
    /// The current execution stage.
    pub stage: WorkloadExecutionStage,
    /// Completed units of work, when measurable.
    pub completed_units: u64,
    /// Total units of work expected, when known.
    pub total_units: Option<u64>,
    /// Optional supporting detail for UI or logging surfaces.
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral training result.
pub struct WorkloadTrainingResult {
    /// The emitted artifact ID.
    pub artifact_id: ArtifactId,
    /// The emitted contribution receipt, when one was produced.
    pub receipt_id: Option<ContributionReceiptId>,
    /// Total training-window duration in seconds.
    pub window_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral validation plan.
pub struct WorkloadValidationPlan {
    /// The head under review.
    pub head_id: HeadId,
    /// Maximum checkpoint bytes the host may materialize or accept.
    pub max_checkpoint_bytes: u64,
    /// Sample or chunk budget used by the validation pass.
    pub sample_budget: u32,
    /// Whether validation should emit a receipt.
    pub emit_receipt: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral validation progress event.
pub struct WorkloadValidationProgress {
    /// The current execution stage.
    pub stage: WorkloadExecutionStage,
    /// Completed units of work, when measurable.
    pub completed_units: u64,
    /// Total units of work expected, when known.
    pub total_units: Option<u64>,
    /// Optional supporting detail for UI or logging surfaces.
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one host-neutral validation result.
pub struct WorkloadValidationResult {
    /// The head under review.
    pub head_id: HeadId,
    /// Whether validation accepted the material it checked.
    pub accepted: bool,
    /// Number of chunks or samples checked locally.
    pub checked_chunks: usize,
    /// The emitted receipt, when one was produced.
    pub emitted_receipt_id: Option<ContributionReceiptId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_training_budget_defaults_to_browser_safe_limits() {
        let budget = WorkloadTrainingBudget::default();
        assert_eq!(budget.max_window_secs, 30);
        assert!(budget.requires_webgpu);
        assert_eq!(budget.max_batch_size, Some(4));
        assert_eq!(budget.precision, Some(Precision::Fp16));
    }

    #[test]
    fn workload_execution_payloads_round_trip_through_json() {
        let training = WorkloadTrainingPlan {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("workload"),
            budget: WorkloadTrainingBudget::default(),
        };
        let validation = WorkloadValidationPlan {
            head_id: HeadId::new("head"),
            max_checkpoint_bytes: 1024,
            sample_budget: 12,
            emit_receipt: true,
        };

        let training_json = serde_json::to_string(&training).expect("serialize training");
        let validation_json = serde_json::to_string(&validation).expect("serialize validation");

        let decoded_training: WorkloadTrainingPlan =
            serde_json::from_str(&training_json).expect("deserialize training");
        let decoded_validation: WorkloadValidationPlan =
            serde_json::from_str(&validation_json).expect("deserialize validation");

        assert_eq!(decoded_training, training);
        assert_eq!(decoded_validation, validation);
    }
}
