use burn_p2p_core::{ContentId, DatasetView, MicroShard, MicroShardId};
use serde::{Deserialize, Serialize};

use crate::DataloaderError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a shard fetch entry.
pub struct ShardFetchEntry {
    /// The microshard ID.
    pub microshard_id: MicroShardId,
    /// The ordinal.
    pub ordinal: u32,
    /// The locator.
    pub locator: String,
    /// The content hash.
    pub content_hash: ContentId,
    /// The bytes len.
    pub bytes_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the shard fetch.
pub struct ShardFetchManifest {
    /// The dataset view ID.
    pub dataset_view_id: burn_p2p_core::DatasetViewId,
    /// The entries.
    pub entries: Vec<ShardFetchEntry>,
}

impl ShardFetchManifest {
    /// Performs the entry for microshard operation.
    pub fn entry_for_microshard(&self, microshard_id: &MicroShardId) -> Option<&ShardFetchEntry> {
        self.entries
            .iter()
            .find(|entry| &entry.microshard_id == microshard_id)
    }

    /// Creates a value from the microshards.
    pub fn from_microshards(
        dataset_view: &DatasetView,
        microshards: &[MicroShard],
        bytes_for_ordinal: impl Fn(u32) -> Vec<u8>,
    ) -> Self {
        Self {
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            entries: microshards
                .iter()
                .map(|microshard| {
                    let bytes = bytes_for_ordinal(microshard.ordinal);
                    ShardFetchEntry {
                        microshard_id: microshard.microshard_id.clone(),
                        ordinal: microshard.ordinal,
                        locator: format!("{:05}.bin", microshard.ordinal),
                        content_hash: ContentId::from_multihash(
                            burn_p2p_core::codec::multihash_sha256(&bytes),
                        ),
                        bytes_len: bytes.len() as u64,
                    }
                })
                .collect(),
        }
    }
}

pub(crate) fn verify_bytes(
    microshard_id: &MicroShardId,
    entry: &ShardFetchEntry,
    bytes: &[u8],
) -> Result<(), DataloaderError> {
    if entry.bytes_len != bytes.len() as u64 {
        return Err(DataloaderError::HashMismatch {
            microshard_id: microshard_id.clone(),
        });
    }

    let content_hash = ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(bytes));
    if content_hash != entry.content_hash {
        return Err(DataloaderError::HashMismatch {
            microshard_id: microshard_id.clone(),
        });
    }

    Ok(())
}
