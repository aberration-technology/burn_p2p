use burn_p2p::{
    ArtifactId, ContributionReceiptId, ExperimentId, Precision, RevisionId, StudyId, WorkloadId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTrainingBudget {
    pub max_window_secs: u64,
    pub max_checkpoint_bytes: u64,
    pub max_shard_bytes: u64,
    pub requires_webgpu: bool,
    pub max_batch_size: Option<u32>,
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
pub struct BrowserTrainingPlan {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub workload_id: WorkloadId,
    pub budget: BrowserTrainingBudget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTrainingResult {
    pub artifact_id: ArtifactId,
    pub receipt_id: Option<ContributionReceiptId>,
    pub window_secs: u64,
}
