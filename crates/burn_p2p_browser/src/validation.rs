use burn_p2p::{ContributionReceiptId, HeadId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserValidationPlan {
    pub head_id: HeadId,
    pub max_checkpoint_bytes: u64,
    pub sample_budget: u32,
    pub emit_receipt: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserValidationResult {
    pub head_id: HeadId,
    pub accepted: bool,
    pub checked_chunks: usize,
    pub emitted_receipt_id: Option<ContributionReceiptId>,
}
