use burn_p2p::{
    ArtifactId, ContributionReceiptId, ExperimentId, Precision, RevisionId, StudyId, WorkloadId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser training budget.
pub struct BrowserTrainingBudget {
    /// The max window secs.
    pub max_window_secs: u64,
    /// The max checkpoint bytes.
    pub max_checkpoint_bytes: u64,
    /// The max shard bytes.
    pub max_shard_bytes: u64,
    /// The requires WebGPU.
    pub requires_webgpu: bool,
    /// The max batch size.
    pub max_batch_size: Option<u32>,
    /// The precision.
    pub precision: Option<Precision>,
}

impl Default for BrowserTrainingBudget {
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
/// Represents a browser training plan.
pub struct BrowserTrainingPlan {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The budget.
    pub budget: BrowserTrainingBudget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser training result.
pub struct BrowserTrainingResult {
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The receipt ID.
    pub receipt_id: Option<ContributionReceiptId>,
    /// The window secs.
    pub window_secs: u64,
}
