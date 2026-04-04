use burn_p2p::{ContributionReceiptId, HeadId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser validation plan.
pub struct BrowserValidationPlan {
    /// The head ID.
    pub head_id: HeadId,
    /// The max checkpoint bytes.
    pub max_checkpoint_bytes: u64,
    /// The sample budget.
    pub sample_budget: u32,
    /// The emit receipt.
    pub emit_receipt: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser validation result.
pub struct BrowserValidationResult {
    /// The head ID.
    pub head_id: HeadId,
    /// The accepted.
    pub accepted: bool,
    /// The checked chunks.
    pub checked_chunks: usize,
    /// The emitted receipt ID.
    pub emitted_receipt_id: Option<ContributionReceiptId>,
}
