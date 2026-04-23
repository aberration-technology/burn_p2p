use std::collections::BTreeMap;

use burn_p2p_core::{FlattenedTensorPack, MetricValue, OuterOptimizerPolicy, StateBlob};

use crate::{P2pWorkload, TrainError};

#[derive(Clone, Debug, PartialEq)]
/// Captures the deterministic local result of one DiLoCo inner loop.
pub struct DiLoCoInnerLoopReport {
    /// Flattened local parameters after the inner loop completes.
    pub local_parameters: FlattenedTensorPack,
    /// Serialized inner optimizer state retained across rounds, when present.
    pub inner_optimizer_state: Option<StateBlob>,
    /// Number of inner-loop steps completed.
    pub steps_completed: u32,
    /// Workload-defined metrics emitted by the inner loop.
    pub metrics: BTreeMap<String, MetricValue>,
}

/// Extends [`P2pWorkload`] with deterministic parameter-pack and optimizer hooks required by
/// the DiLoCo inner/outer synchronization protocol.
pub trait DiLoCoWorkload: P2pWorkload {
    /// Exports one deterministic flattened parameter pack from the runtime model.
    fn export_parameter_pack(&self, model: &Self::Model) -> anyhow::Result<FlattenedTensorPack>;

    /// Imports one deterministic flattened parameter pack into the runtime model representation.
    fn import_parameter_pack(
        &self,
        device: &Self::Device,
        pack: &FlattenedTensorPack,
    ) -> anyhow::Result<Self::Model>;

    /// Runs a deterministic inner loop for the requested number of steps.
    fn run_inner_steps(
        &self,
        model: &Self::Model,
        batches: &[Self::Batch],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<DiLoCoInnerLoopReport, TrainError>;

    /// Builds the pseudo-gradient `base - local` used by the DiLoCo outer loop.
    fn build_pseudo_gradient(
        &self,
        base: &FlattenedTensorPack,
        local: &FlattenedTensorPack,
    ) -> anyhow::Result<FlattenedTensorPack> {
        if !base.is_compatible_with(local) {
            anyhow::bail!(
                "DiLoCo pseudo-gradient packs are incompatible: base={} layout={} count={}, local={} layout={} count={}",
                base.model_schema_hash.as_str(),
                base.layout_hash.as_str(),
                base.parameter_count(),
                local.model_schema_hash.as_str(),
                local.layout_hash.as_str(),
                local.parameter_count(),
            );
        }

        Ok(FlattenedTensorPack::new(
            base.model_schema_hash.clone(),
            base.layout_hash.clone(),
            base.values
                .iter()
                .zip(&local.values)
                .map(|(base_value, local_value)| base_value - local_value)
                .collect(),
        ))
    }

    /// Initializes the serialized outer optimizer state anchored at the current model.
    fn initialize_outer_optimizer_state(
        &self,
        _model: &Self::Model,
        _policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<StateBlob> {
        StateBlob::try_new("application/json", b"{}".to_vec()).map_err(anyhow::Error::from)
    }

    /// Applies the aggregated pseudo-gradient to the base parameters and returns the updated pack.
    ///
    /// The default implementation supports plain SGD without momentum or weight decay. Workloads
    /// that want momentum, adaptive optimizers, or custom tensor semantics should override this.
    fn apply_aggregated_outer_update(
        &self,
        base: &FlattenedTensorPack,
        aggregate: &FlattenedTensorPack,
        outer_optimizer_state: &StateBlob,
        policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
        if !base.is_compatible_with(aggregate) {
            anyhow::bail!(
                "DiLoCo outer update packs are incompatible: base layout={} count={}, aggregate layout={} count={}",
                base.layout_hash.as_str(),
                base.parameter_count(),
                aggregate.layout_hash.as_str(),
                aggregate.parameter_count(),
            );
        }

        match policy {
            OuterOptimizerPolicy::Sgd {
                momentum_micros: None,
                nesterov: false,
                weight_decay_micros: None,
                ..
            } => Ok((
                FlattenedTensorPack::new(
                    base.model_schema_hash.clone(),
                    base.layout_hash.clone(),
                    base.values
                        .iter()
                        .zip(&aggregate.values)
                        .map(|(value, grad)| *value - ((policy.learning_rate() as f32) * *grad))
                        .collect(),
                ),
                outer_optimizer_state.clone(),
            )),
            _ => anyhow::bail!(
                "DiLoCo outer optimizer policy {:?} requires a workload-specific implementation",
                policy
            ),
        }
    }

    /// Optionally reserializes the outer optimizer state before persistence.
    fn save_outer_optimizer_state(&self, state: &StateBlob) -> anyhow::Result<StateBlob> {
        Ok(state.clone())
    }

    /// Optionally loads and validates the outer optimizer state before reuse.
    fn load_outer_optimizer_state(&self, state: &StateBlob) -> anyhow::Result<StateBlob> {
        Ok(state.clone())
    }

    /// Optionally reserializes one inner optimizer state blob before persistence.
    fn save_inner_optimizer_state(&self, state: &StateBlob) -> anyhow::Result<StateBlob> {
        Ok(state.clone())
    }

    /// Optionally loads and validates one inner optimizer state blob before reuse.
    fn load_inner_optimizer_state(&self, state: &StateBlob) -> anyhow::Result<StateBlob> {
        Ok(state.clone())
    }
}
