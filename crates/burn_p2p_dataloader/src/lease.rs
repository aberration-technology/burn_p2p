use std::collections::BTreeMap;

use burn_p2p_core::{
    AssignmentLease, ContentId, ExperimentId, LeaseId, MicroShard, MicroShardId, NetworkId, PeerId,
    RevisionId, StudyId, WindowId,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{DataloaderError, DatasetView};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a shard cost model.
pub struct ShardCostModel {
    /// The tokens per work unit.
    pub tokens_per_work_unit: u64,
    /// The examples per work unit.
    pub examples_per_work_unit: u64,
    /// The bytes per work unit.
    pub bytes_per_work_unit: u64,
    /// The minimum work units.
    pub minimum_work_units: u64,
}

impl Default for ShardCostModel {
    fn default() -> Self {
        Self {
            tokens_per_work_unit: 1_024,
            examples_per_work_unit: 16,
            bytes_per_work_unit: 256 * 1_024,
            minimum_work_units: 1,
        }
    }
}

impl ShardCostModel {
    /// Performs the estimate work units operation.
    pub fn estimate_work_units(&self, microshard: &MicroShard) -> u64 {
        let token_units = ceil_div(
            microshard.estimated_tokens.max(1),
            self.tokens_per_work_unit.max(1),
        );
        let example_units = ceil_div(
            microshard.estimated_examples.max(1),
            self.examples_per_work_unit.max(1),
        );
        let byte_units = ceil_div(
            microshard.estimated_bytes.max(1),
            self.bytes_per_work_unit.max(1),
        );

        token_units
            .max(example_units)
            .max(byte_units)
            .max(self.minimum_work_units.max(1))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures lease planner.
pub struct LeasePlannerConfig {
    /// The lease duration seconds.
    pub lease_duration_seconds: i64,
    /// The max microshards per lease.
    pub max_microshards_per_lease: usize,
    /// The cost model.
    pub cost_model: ShardCostModel,
}

impl Default for LeasePlannerConfig {
    fn default() -> Self {
        Self {
            lease_duration_seconds: 300,
            max_microshards_per_lease: 128,
            cost_model: ShardCostModel::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a lease selection.
pub struct LeaseSelection {
    /// The microshards.
    pub microshards: Vec<MicroShard>,
    /// The budget work units.
    pub budget_work_units: u64,
    /// The estimated work units.
    pub estimated_work_units: u64,
    /// The estimated examples.
    pub estimated_examples: u64,
    /// The estimated tokens.
    pub estimated_tokens: u64,
    /// The estimated bytes.
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a planned lease.
pub struct PlannedLease {
    /// The lease.
    pub lease: AssignmentLease,
    /// The selection.
    pub selection: LeaseSelection,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a lease planner.
pub struct LeasePlanner {
    /// The config.
    pub config: LeasePlannerConfig,
}

impl LeasePlanner {
    /// Creates a new value.
    pub fn new(config: LeasePlannerConfig) -> Result<Self, DataloaderError> {
        if config.lease_duration_seconds <= 0 {
            return Err(DataloaderError::InvalidLeaseDuration);
        }

        Ok(Self { config })
    }

    /// Performs the select microshards operation.
    pub fn select_microshards(
        &self,
        peer_id: &PeerId,
        budget_work_units: u64,
        microshards: &[MicroShard],
    ) -> Result<LeaseSelection, DataloaderError> {
        if budget_work_units == 0 {
            return Err(DataloaderError::InvalidBudget);
        }
        if microshards.is_empty() {
            return Err(DataloaderError::NoMicroshards);
        }

        let mut ranked = microshards
            .iter()
            .cloned()
            .map(|microshard| {
                let score =
                    ContentId::derive(&(peer_id.as_str(), microshard.microshard_id.as_str()))?;
                Ok::<_, DataloaderError>((score, microshard))
            })
            .collect::<Result<Vec<_>, _>>()?;

        ranked.sort_by(|left, right| {
            right
                .0
                .as_str()
                .cmp(left.0.as_str())
                .then(left.1.ordinal.cmp(&right.1.ordinal))
        });

        let mut selected = Vec::new();
        let mut estimated_work_units = 0_u64;
        let mut estimated_examples = 0_u64;
        let mut estimated_tokens = 0_u64;
        let mut estimated_bytes = 0_u64;

        for (_, microshard) in ranked {
            if selected.len() >= self.config.max_microshards_per_lease {
                break;
            }

            let shard_cost = self.config.cost_model.estimate_work_units(&microshard);
            let would_fit = estimated_work_units + shard_cost <= budget_work_units;

            if would_fit || selected.is_empty() {
                estimated_work_units += shard_cost;
                estimated_examples += microshard.estimated_examples;
                estimated_tokens += microshard.estimated_tokens;
                estimated_bytes += microshard.estimated_bytes;
                selected.push(microshard);
            }
        }

        Ok(LeaseSelection {
            microshards: selected,
            budget_work_units,
            estimated_work_units,
            estimated_examples,
            estimated_tokens,
            estimated_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Performs the plan lease operation.
    pub fn plan_lease(
        &self,
        network_id: NetworkId,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        dataset_view: &DatasetView,
        peer_id: PeerId,
        window_id: WindowId,
        granted_at: DateTime<Utc>,
        budget_work_units: u64,
        microshards: &[MicroShard],
    ) -> Result<PlannedLease, DataloaderError> {
        let selection = self.select_microshards(&peer_id, budget_work_units, microshards)?;
        let microshard_ids = selection
            .microshards
            .iter()
            .map(|microshard| microshard.microshard_id.clone())
            .collect::<Vec<_>>();

        let expires_at = granted_at + Duration::seconds(self.config.lease_duration_seconds);
        let lease_id = LeaseId::derive(&(
            network_id.as_str(),
            study_id.as_str(),
            experiment_id.as_str(),
            revision_id.as_str(),
            peer_id.as_str(),
            dataset_view.dataset_view_id.as_str(),
            window_id.0,
            microshard_ids
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        let assignment_hash = ContentId::derive(&(
            lease_id.as_str(),
            network_id.as_str(),
            study_id.as_str(),
            experiment_id.as_str(),
            revision_id.as_str(),
            peer_id.as_str(),
            dataset_view.dataset_view_id.as_str(),
            window_id.0,
            granted_at,
            expires_at,
            selection.budget_work_units,
            microshard_ids
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        let lease = AssignmentLease {
            lease_id,
            network_id,
            study_id,
            experiment_id,
            revision_id,
            peer_id,
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            window_id,
            granted_at,
            expires_at,
            budget_work_units: selection.budget_work_units,
            microshards: microshard_ids,
            assignment_hash,
        };

        Ok(PlannedLease { lease, selection })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a lease cache.
pub struct LeaseCache {
    leases_by_window: BTreeMap<WindowId, BTreeMap<PeerId, AssignmentLease>>,
}

impl LeaseCache {
    /// Performs the insert operation.
    pub fn insert(&mut self, lease: AssignmentLease) -> Option<AssignmentLease> {
        self.leases_by_window
            .entry(lease.window_id)
            .or_default()
            .insert(lease.peer_id.clone(), lease)
    }

    /// Performs the get operation.
    pub fn get(&self, window_id: WindowId, peer_id: &PeerId) -> Option<&AssignmentLease> {
        self.leases_by_window.get(&window_id)?.get(peer_id)
    }

    /// Performs the leases for window operation.
    pub fn leases_for_window(
        &self,
        window_id: WindowId,
    ) -> Option<&BTreeMap<PeerId, AssignmentLease>> {
        self.leases_by_window.get(&window_id)
    }

    /// Performs the evict before operation.
    pub fn evict_before(&mut self, earliest_window: WindowId) -> usize {
        let original_len = self.leases_by_window.len();
        self.leases_by_window
            .retain(|window_id, _| *window_id >= earliest_window);
        original_len - self.leases_by_window.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a shard aware sampler.
pub struct ShardAwareSampler {
    ordered_microshards: Vec<MicroShardId>,
    next_index: usize,
}

impl ShardAwareSampler {
    /// Creates a value from the lease.
    pub fn from_lease(lease: &AssignmentLease) -> Result<Self, DataloaderError> {
        let mut ordered_scored = lease
            .microshards
            .iter()
            .cloned()
            .map(|microshard_id| {
                let score = ContentId::derive(&(lease.window_id.0, microshard_id.as_str()))?;
                Ok::<_, DataloaderError>((score, microshard_id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        ordered_scored.sort_by(|left, right| {
            right
                .0
                .as_str()
                .cmp(left.0.as_str())
                .then(left.1.as_str().cmp(right.1.as_str()))
        });
        let ordered_microshards = ordered_scored
            .into_iter()
            .map(|(_, microshard_id)| microshard_id)
            .collect();

        Ok(Self {
            ordered_microshards,
            next_index: 0,
        })
    }

    /// Performs the ordered microshards operation.
    pub fn ordered_microshards(&self) -> &[MicroShardId] {
        &self.ordered_microshards
    }

    /// Performs the next microshard operation.
    pub fn next_microshard(&mut self) -> Option<&MicroShardId> {
        let microshard = self.ordered_microshards.get(self.next_index)?;
        self.next_index += 1;
        Some(microshard)
    }
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if divisor <= 1 {
        value
    } else {
        value.div_ceil(divisor)
    }
}
