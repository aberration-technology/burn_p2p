use burn_p2p_core::{AssignmentLease, ContentId, DataReceipt, MicroShardId, WorkDisposition};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::DataloaderError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a data receipt builder.
pub struct DataReceiptBuilder {
    /// The disposition.
    pub disposition: WorkDisposition,
    /// The examples processed.
    pub examples_processed: u64,
    /// The tokens processed.
    pub tokens_processed: u64,
}

impl DataReceiptBuilder {
    /// Performs the accepted operation.
    pub fn accepted(examples_processed: u64, tokens_processed: u64) -> Self {
        Self {
            disposition: WorkDisposition::Accepted,
            examples_processed,
            tokens_processed,
        }
    }

    /// Performs the build operation.
    pub fn build(
        &self,
        lease: &AssignmentLease,
        completed_at: DateTime<Utc>,
    ) -> Result<DataReceipt, DataloaderError> {
        let receipt_id = ContentId::derive(&(
            lease.lease_id.as_str(),
            lease.peer_id.as_str(),
            completed_at,
            self.examples_processed,
            self.tokens_processed,
            &self.disposition,
            lease
                .microshards
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        Ok(DataReceipt {
            receipt_id,
            lease_id: lease.lease_id.clone(),
            peer_id: lease.peer_id.clone(),
            completed_at,
            microshards: lease.microshards.clone(),
            examples_processed: self.examples_processed,
            tokens_processed: self.tokens_processed,
            disposition: self.disposition.clone(),
        })
    }
}
