use burn_p2p_core::{DatasetView, MicroShard, MicroShardId};
use serde::{Deserialize, Serialize};

use crate::{DataloaderError, DatasetSizing};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures micro shard planner.
pub struct MicroShardPlannerConfig {
    /// The target microshard bytes.
    pub target_microshard_bytes: u64,
    /// The min microshards.
    pub min_microshards: u32,
    /// The max microshards.
    pub max_microshards: u32,
}

impl Default for MicroShardPlannerConfig {
    fn default() -> Self {
        Self {
            target_microshard_bytes: 64 * 1024 * 1024,
            min_microshards: 1,
            max_microshards: 16_384,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard plan.
pub struct MicroShardPlan {
    /// The dataset view.
    pub dataset_view: DatasetView,
    /// The sizing.
    pub sizing: DatasetSizing,
    /// The microshards.
    pub microshards: Vec<MicroShard>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard planner.
pub struct MicroShardPlanner {
    /// The config.
    pub config: MicroShardPlannerConfig,
}

impl MicroShardPlanner {
    /// Creates a new value.
    pub fn new(config: MicroShardPlannerConfig) -> Result<Self, DataloaderError> {
        if config.target_microshard_bytes == 0 {
            return Err(DataloaderError::InvalidTargetMicroshardBytes);
        }

        Ok(Self { config })
    }

    /// Performs the plan operation.
    pub fn plan(
        &self,
        dataset_view: &DatasetView,
        sizing: DatasetSizing,
    ) -> Result<MicroShardPlan, DataloaderError> {
        let shard_count = shard_count_for(&self.config, &sizing);
        let shard_count_u64 = u64::from(shard_count);
        let mut microshards = Vec::with_capacity(shard_count as usize);

        for ordinal in 0..shard_count {
            let ordinal_u64 = u64::from(ordinal);
            let examples =
                evenly_distributed_share(sizing.total_examples, shard_count_u64, ordinal_u64);
            let tokens =
                evenly_distributed_share(sizing.total_tokens, shard_count_u64, ordinal_u64);
            let bytes = evenly_distributed_share(sizing.total_bytes, shard_count_u64, ordinal_u64);

            let microshard_id = MicroShardId::derive(&(
                dataset_view.dataset_view_id.as_str(),
                ordinal,
                examples,
                tokens,
                bytes,
            ))?;

            microshards.push(MicroShard {
                microshard_id,
                dataset_view_id: dataset_view.dataset_view_id.clone(),
                ordinal,
                estimated_examples: examples,
                estimated_tokens: tokens,
                estimated_bytes: bytes,
            });
        }

        Ok(MicroShardPlan {
            dataset_view: dataset_view.clone(),
            sizing,
            microshards,
        })
    }
}

fn shard_count_for(config: &MicroShardPlannerConfig, sizing: &DatasetSizing) -> u32 {
    let sizing_bytes = if sizing.total_bytes > 0 {
        sizing.total_bytes
    } else if sizing.total_tokens > 0 {
        sizing.total_tokens
    } else {
        sizing.total_examples
    };
    let unclamped = ceil_div(sizing_bytes.max(1), config.target_microshard_bytes.max(1));
    unclamped.clamp(
        u64::from(config.min_microshards.max(1)),
        u64::from(config.max_microshards.max(config.min_microshards.max(1))),
    ) as u32
}

fn evenly_distributed_share(total: u64, count: u64, index: u64) -> u64 {
    let base = total / count.max(1);
    let remainder = total % count.max(1);
    base + u64::from(index < remainder)
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if divisor <= 1 {
        value
    } else {
        value.div_ceil(divisor)
    }
}
