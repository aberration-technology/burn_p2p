use std::collections::BTreeMap;

use burn_p2p_core::{FlattenedTensorPack, MetricValue, OuterOptimizerPolicy, StateBlob};

use crate::{P2pWorkload, TrainError};

const OUTER_SGD_STATE_ENCODING: &str = "application/vnd.burn-p2p.diloco-sgd-state-v1";
const OUTER_SGD_STATE_MAGIC: &[u8; 8] = b"DLSGD001";

#[derive(Clone, Debug, PartialEq)]
struct OuterSgdState {
    step: u64,
    velocity: Vec<f32>,
}

fn push_len_prefixed(bytes: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| anyhow::anyhow!("DiLoCo outer SGD state identifier is too long"))?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> anyhow::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("DiLoCo outer SGD state length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow::anyhow!("DiLoCo outer SGD state is truncated"))?;
    *cursor = end;
    Ok(value)
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    let value: [u8; 4] = take_bytes(bytes, cursor, 4)?
        .try_into()
        .expect("exact u32 byte width");
    Ok(u32::from_le_bytes(value))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> anyhow::Result<u64> {
    let value: [u8; 8] = take_bytes(bytes, cursor, 8)?
        .try_into()
        .expect("exact u64 byte width");
    Ok(u64::from_le_bytes(value))
}

fn take_len_prefixed<'a>(bytes: &'a [u8], cursor: &mut usize) -> anyhow::Result<&'a str> {
    let len = usize::try_from(take_u32(bytes, cursor)?)
        .map_err(|_| anyhow::anyhow!("DiLoCo outer SGD identifier length exceeds usize"))?;
    std::str::from_utf8(take_bytes(bytes, cursor, len)?)
        .map_err(|error| anyhow::anyhow!("DiLoCo outer SGD identifier is not UTF-8: {error}"))
}

fn encode_outer_sgd_state(
    pack: &FlattenedTensorPack,
    state: &OuterSgdState,
) -> anyhow::Result<StateBlob> {
    anyhow::ensure!(
        state.velocity.len() == pack.values.len(),
        "DiLoCo outer SGD velocity has {} values, expected {}",
        state.velocity.len(),
        pack.values.len()
    );
    anyhow::ensure!(
        state.velocity.iter().all(|value| value.is_finite()),
        "DiLoCo outer SGD velocity contains non-finite values"
    );

    let parameter_count = u64::try_from(pack.values.len())
        .map_err(|_| anyhow::anyhow!("DiLoCo parameter count exceeds u64::MAX"))?;
    let parameter_checksum = pack.checksum()?;
    let mut bytes = Vec::with_capacity(
        OUTER_SGD_STATE_MAGIC.len()
            + 8
            + 4
            + pack.model_schema_hash.as_str().len()
            + 4
            + pack.layout_hash.as_str().len()
            + 4
            + parameter_checksum.as_str().len()
            + 8
            + state.velocity.len().saturating_mul(4),
    );
    bytes.extend_from_slice(OUTER_SGD_STATE_MAGIC);
    bytes.extend_from_slice(&state.step.to_le_bytes());
    push_len_prefixed(&mut bytes, pack.model_schema_hash.as_str())?;
    push_len_prefixed(&mut bytes, pack.layout_hash.as_str())?;
    push_len_prefixed(&mut bytes, parameter_checksum.as_str())?;
    bytes.extend_from_slice(&parameter_count.to_le_bytes());
    for value in &state.velocity {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    StateBlob::try_new(OUTER_SGD_STATE_ENCODING, bytes).map_err(anyhow::Error::from)
}

fn decode_outer_sgd_state(
    blob: &StateBlob,
    pack: &FlattenedTensorPack,
) -> anyhow::Result<OuterSgdState> {
    anyhow::ensure!(
        blob.encoding == OUTER_SGD_STATE_ENCODING,
        "DiLoCo outer SGD state encoding is {}, expected {}",
        blob.encoding,
        OUTER_SGD_STATE_ENCODING
    );
    let canonical = StateBlob::try_new(blob.encoding.clone(), blob.bytes.clone())?;
    anyhow::ensure!(
        canonical.content_id == blob.content_id,
        "DiLoCo outer SGD state content identifier does not match its bytes"
    );

    let mut cursor = 0;
    anyhow::ensure!(
        take_bytes(&blob.bytes, &mut cursor, OUTER_SGD_STATE_MAGIC.len())? == OUTER_SGD_STATE_MAGIC,
        "DiLoCo outer SGD state magic/version is invalid"
    );
    let step = take_u64(&blob.bytes, &mut cursor)?;
    let model_schema_hash = take_len_prefixed(&blob.bytes, &mut cursor)?;
    let layout_hash = take_len_prefixed(&blob.bytes, &mut cursor)?;
    let parameter_checksum = take_len_prefixed(&blob.bytes, &mut cursor)?;
    anyhow::ensure!(
        model_schema_hash == pack.model_schema_hash.as_str(),
        "DiLoCo outer SGD state model schema does not match the active parameter pack"
    );
    anyhow::ensure!(
        layout_hash == pack.layout_hash.as_str(),
        "DiLoCo outer SGD state layout does not match the active parameter pack"
    );
    anyhow::ensure!(
        parameter_checksum == pack.checksum()?.as_str(),
        "DiLoCo outer SGD state is anchored to a different parameter pack"
    );
    let parameter_count = usize::try_from(take_u64(&blob.bytes, &mut cursor)?)
        .map_err(|_| anyhow::anyhow!("DiLoCo outer SGD parameter count exceeds usize"))?;
    anyhow::ensure!(
        parameter_count == pack.values.len(),
        "DiLoCo outer SGD state has {parameter_count} values, expected {}",
        pack.values.len()
    );

    let mut velocity = Vec::with_capacity(parameter_count);
    for _ in 0..parameter_count {
        let value: [u8; 4] = take_bytes(&blob.bytes, &mut cursor, 4)?
            .try_into()
            .expect("exact f32 byte width");
        velocity.push(f32::from_le_bytes(value));
    }
    anyhow::ensure!(
        cursor == blob.bytes.len(),
        "DiLoCo outer SGD state contains trailing bytes"
    );
    anyhow::ensure!(
        velocity.iter().all(|value| value.is_finite()),
        "DiLoCo outer SGD velocity contains non-finite values"
    );
    Ok(OuterSgdState { step, velocity })
}

fn initialize_outer_sgd_state(
    pack: &FlattenedTensorPack,
    policy: &OuterOptimizerPolicy,
) -> anyhow::Result<StateBlob> {
    if policy.momentum().unwrap_or_default() > 0.0 {
        return encode_outer_sgd_state(
            pack,
            &OuterSgdState {
                step: 0,
                velocity: vec![0.0; pack.values.len()],
            },
        );
    }
    StateBlob::try_new("application/json", b"{}".to_vec()).map_err(anyhow::Error::from)
}

fn apply_outer_sgd_update(
    base: &FlattenedTensorPack,
    aggregate: &FlattenedTensorPack,
    outer_optimizer_state: &StateBlob,
    policy: &OuterOptimizerPolicy,
) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
    anyhow::ensure!(
        base.is_compatible_with(aggregate),
        "DiLoCo outer update packs are incompatible: base layout={} count={}, aggregate layout={} count={}",
        base.layout_hash.as_str(),
        base.parameter_count(),
        aggregate.layout_hash.as_str(),
        aggregate.parameter_count(),
    );
    anyhow::ensure!(
        base.values.iter().all(|value| value.is_finite())
            && aggregate.values.iter().all(|value| value.is_finite()),
        "DiLoCo outer update contains non-finite parameters or pseudo-gradients"
    );

    let (momentum, nesterov) = match policy {
        OuterOptimizerPolicy::Sgd {
            momentum_micros,
            nesterov,
            ..
        } => (
            momentum_micros
                .map(|value| value as f32 / 1_000_000.0)
                .unwrap_or_default(),
            *nesterov,
        ),
    };
    let learning_rate = policy.learning_rate() as f32;
    let weight_decay = policy.weight_decay().unwrap_or_default() as f32;
    let max_pseudo_gradient_rms_ratio = policy.max_pseudo_gradient_rms_ratio();
    anyhow::ensure!(
        learning_rate.is_finite() && learning_rate > 0.0,
        "DiLoCo outer SGD learning rate must be finite and positive"
    );
    anyhow::ensure!(
        momentum.is_finite() && (0.0..=1.0).contains(&momentum),
        "DiLoCo outer SGD momentum must be in [0, 1]"
    );
    anyhow::ensure!(
        weight_decay.is_finite() && (0.0..=1.0).contains(&weight_decay),
        "DiLoCo outer SGD weight decay must be in [0, 1]"
    );
    anyhow::ensure!(
        max_pseudo_gradient_rms_ratio.is_none_or(|ratio| ratio.is_finite() && ratio > 0.0),
        "DiLoCo outer SGD pseudo-gradient RMS ratio must be finite and positive"
    );
    anyhow::ensure!(
        !nesterov || momentum > 0.0,
        "DiLoCo outer SGD Nesterov acceleration requires positive momentum"
    );
    let mut state = if momentum > 0.0 {
        decode_outer_sgd_state(outer_optimizer_state, base)?
    } else {
        OuterSgdState {
            step: 0,
            velocity: Vec::new(),
        }
    };
    let pseudo_gradient_scale = max_pseudo_gradient_rms_ratio
        .map(|maximum_ratio| {
            let count = base.values.len().max(1) as f64;
            let parameter_rms = (base
                .values
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                / count)
                .sqrt();
            let pseudo_gradient_rms = (aggregate
                .values
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                / count)
                .sqrt();
            if pseudo_gradient_rms <= f64::EPSILON {
                1.0
            } else {
                (maximum_ratio * parameter_rms.max(f64::EPSILON) / pseudo_gradient_rms).min(1.0)
                    as f32
            }
        })
        .unwrap_or(1.0);

    let mut values = Vec::with_capacity(base.values.len());
    for (index, (parameter, pseudo_gradient)) in
        base.values.iter().zip(&aggregate.values).enumerate()
    {
        let gradient = *pseudo_gradient * pseudo_gradient_scale + weight_decay * *parameter;
        let update = if momentum > 0.0 {
            let velocity = momentum * state.velocity[index] + gradient;
            state.velocity[index] = velocity;
            if nesterov {
                gradient + momentum * velocity
            } else {
                velocity
            }
        } else {
            gradient
        };
        let value = *parameter - learning_rate * update;
        anyhow::ensure!(
            value.is_finite(),
            "DiLoCo outer SGD produced a non-finite parameter at index {index}"
        );
        values.push(value);
    }

    let updated = FlattenedTensorPack::new(
        base.model_schema_hash.clone(),
        base.layout_hash.clone(),
        values,
    );
    let state = if momentum > 0.0 {
        state.step = state.step.saturating_add(1);
        encode_outer_sgd_state(&updated, &state)?
    } else {
        outer_optimizer_state.clone()
    };
    Ok((updated, state))
}

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
        model: &Self::Model,
        policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<StateBlob> {
        if policy.momentum().unwrap_or_default() > 0.0 {
            initialize_outer_sgd_state(&self.export_parameter_pack(model)?, policy)
        } else {
            StateBlob::try_new("application/json", b"{}".to_vec()).map_err(anyhow::Error::from)
        }
    }

    /// Applies the aggregated pseudo-gradient to the base parameters and returns the updated pack.
    ///
    /// The default implementation supports SGD, momentum, Nesterov acceleration, and coupled
    /// weight decay. Workloads that want adaptive optimizers or custom tensor semantics should
    /// override this.
    fn apply_aggregated_outer_update(
        &self,
        base: &FlattenedTensorPack,
        aggregate: &FlattenedTensorPack,
        outer_optimizer_state: &StateBlob,
        policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
        apply_outer_sgd_update(base, aggregate, outer_optimizer_state, policy)
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

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p_core::ContentId;

    fn pack(layout: &str, values: Vec<f32>) -> FlattenedTensorPack {
        FlattenedTensorPack::new(
            ContentId::new("model-schema"),
            ContentId::new(layout),
            values,
        )
    }

    fn policy(
        learning_rate_micros: u64,
        momentum_micros: Option<u64>,
        nesterov: bool,
        weight_decay_micros: Option<u64>,
    ) -> OuterOptimizerPolicy {
        OuterOptimizerPolicy::Sgd {
            learning_rate_micros,
            momentum_micros,
            nesterov,
            weight_decay_micros,
            max_pseudo_gradient_rms_ratio_micros: None,
        }
    }

    #[test]
    fn outer_sgd_applies_learning_rate_and_weight_decay() {
        let base = pack("layout", vec![1.0, -2.0]);
        let aggregate = pack("layout", vec![0.5, -0.25]);
        let policy = policy(100_000, None, false, Some(10_000));
        let state = initialize_outer_sgd_state(&base, &policy).expect("initialize state");

        let (updated, next_state) =
            apply_outer_sgd_update(&base, &aggregate, &state, &policy).expect("apply update");

        assert!((updated.values[0] - 0.949).abs() <= 1.0e-6);
        assert!((updated.values[1] - -1.973).abs() <= 1.0e-6);
        assert_eq!(next_state, state);
    }

    #[test]
    fn outer_sgd_clips_pseudo_gradient_by_relative_rms() {
        let base = pack("layout", vec![3.0, 4.0]);
        let aggregate = pack("layout", vec![3.0, 4.0]);
        let policy = OuterOptimizerPolicy::Sgd {
            learning_rate_micros: 1_000_000,
            momentum_micros: None,
            nesterov: false,
            weight_decay_micros: None,
            max_pseudo_gradient_rms_ratio_micros: Some(100_000),
        };
        let state = initialize_outer_sgd_state(&base, &policy).expect("initialize state");

        let (updated, _) =
            apply_outer_sgd_update(&base, &aggregate, &state, &policy).expect("apply update");

        assert!((updated.values[0] - 2.7).abs() <= 1.0e-6);
        assert!((updated.values[1] - 3.6).abs() <= 1.0e-6);
    }

    #[test]
    fn outer_sgd_persists_momentum_across_rounds() {
        let base = pack("layout", vec![1.0]);
        let policy = policy(1_000_000, Some(500_000), false, None);
        let state = initialize_outer_sgd_state(&base, &policy).expect("initialize state");

        let (round_one, state) =
            apply_outer_sgd_update(&base, &pack("layout", vec![0.2]), &state, &policy)
                .expect("round one");
        let (round_two, state) =
            apply_outer_sgd_update(&round_one, &pack("layout", vec![0.1]), &state, &policy)
                .expect("round two");
        let decoded = decode_outer_sgd_state(&state, &round_two).expect("decode state");

        assert!((round_one.values[0] - 0.8).abs() <= 1.0e-6);
        assert!((round_two.values[0] - 0.6).abs() <= 1.0e-6);
        assert_eq!(decoded.step, 2);
        assert!((decoded.velocity[0] - 0.2).abs() <= 1.0e-6);
    }

    #[test]
    fn outer_sgd_nesterov_uses_lookahead_update() {
        let base = pack("layout", vec![1.0]);
        let policy = policy(1_000_000, Some(500_000), true, None);
        let state = initialize_outer_sgd_state(&base, &policy).expect("initialize state");

        let (updated, _) =
            apply_outer_sgd_update(&base, &pack("layout", vec![0.2]), &state, &policy)
                .expect("apply update");

        assert!((updated.values[0] - 0.7).abs() <= 1.0e-6);
    }

    #[test]
    fn outer_sgd_state_rejects_wrong_layout_and_tampering() {
        let base = pack("layout-a", vec![1.0]);
        let policy = policy(1_000_000, Some(900_000), false, None);
        let state = initialize_outer_sgd_state(&base, &policy).expect("initialize state");

        let wrong_layout = pack("layout-b", vec![1.0]);
        assert!(decode_outer_sgd_state(&state, &wrong_layout).is_err());

        let wrong_parameters = pack("layout-a", vec![2.0]);
        assert!(decode_outer_sgd_state(&state, &wrong_parameters).is_err());

        let mut tampered = state;
        *tampered.bytes.last_mut().expect("state payload") ^= 1;
        assert!(decode_outer_sgd_state(&tampered, &base).is_err());
    }
}
