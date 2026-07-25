use anyhow::{Context, ensure};
use burn_p2p_core::{
    CanonicalSchema, CompactScalarEncoding, CompactScalarVector, CompactUpdateBody,
    CompactUpdatePayload, ContentId, FlattenedTensorPack, TrainingContractManifest,
    UpdateNormStats, deterministic_cbor, from_cbor_slice,
};

/// Hard upper bound accepted by the generic compact-update decoder.
pub const MAX_COMPACT_UPDATE_BYTES: usize = 64 * 1024 * 1024;

/// A compact payload that has been structurally checked against its contract.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedCompactUpdate {
    /// Canonical payload identifier.
    pub payload_id: ContentId,
    /// Decoded compact payload.
    pub payload: CompactUpdatePayload,
}

/// Encodes a compact update after validating it against the exact contract.
pub fn encode_compact_update(
    payload: &CompactUpdatePayload,
    contract_id: &ContentId,
    contract: &TrainingContractManifest,
) -> anyhow::Result<Vec<u8>> {
    validate_payload_contract(payload, contract_id, contract)?;
    let bytes = deterministic_cbor(payload).context("encode compact update payload")?;
    ensure!(
        bytes.len() <= MAX_COMPACT_UPDATE_BYTES,
        "compact update payload is {} bytes, limit is {}",
        bytes.len(),
        MAX_COMPACT_UPDATE_BYTES
    );
    Ok(bytes)
}

/// Decodes and validates an untrusted compact update.
pub fn decode_compact_update(
    bytes: &[u8],
    contract_id: &ContentId,
    contract: &TrainingContractManifest,
) -> anyhow::Result<ValidatedCompactUpdate> {
    ensure!(
        bytes.len() <= MAX_COMPACT_UPDATE_BYTES,
        "compact update payload is {} bytes, limit is {}",
        bytes.len(),
        MAX_COMPACT_UPDATE_BYTES
    );
    let payload: CompactUpdatePayload =
        from_cbor_slice(bytes).context("decode compact update payload")?;
    validate_payload_contract(&payload, contract_id, contract)?;
    let payload_id = payload
        .content_id()
        .context("derive compact update payload id")?;
    Ok(ValidatedCompactUpdate {
        payload_id,
        payload,
    })
}

fn validate_payload_contract(
    payload: &CompactUpdatePayload,
    contract_id: &ContentId,
    contract: &TrainingContractManifest,
) -> anyhow::Result<()> {
    contract.validate().context("validate training contract")?;
    ensure!(
        &payload.training_contract_id == contract_id,
        "compact update training contract id mismatch"
    );
    ensure!(
        payload.model_schema_hash == contract.model_schema_hash,
        "compact update model schema mismatch"
    );
    payload
        .validate_against_codec(&contract.update_codec)
        .context("validate compact update codec")
}

/// Reconstructs canonical update tensors from one validated compact payload.
pub trait CompactUpdateReconstructor {
    /// Reconstructs the canonical flattened update pack.
    fn reconstruct(&self, update: &ValidatedCompactUpdate) -> anyhow::Result<FlattenedTensorPack>;
}

/// Deterministic CountSketch-style decoder for `SubspaceLatent` updates.
///
/// Every flattened parameter coordinate maps to one seeded coefficient and one
/// sign. The map is linear, so averaging coefficients before reconstruction is
/// exactly equivalent to averaging reconstructed model deltas.
#[derive(Clone, Copy, Debug, Default)]
pub struct SeededSubspaceReconstructor;

impl CompactUpdateReconstructor for SeededSubspaceReconstructor {
    fn reconstruct(&self, update: &ValidatedCompactUpdate) -> anyhow::Result<FlattenedTensorPack> {
        let CompactUpdateBody::SubspaceLatent {
            dimensions,
            seed,
            coefficients,
        } = &update.payload.body
        else {
            anyhow::bail!("seeded subspace reconstructor requires a subspace-latent payload");
        };
        let coefficients = coefficients
            .decode()
            .context("decode subspace coefficients")?;
        ensure!(
            coefficients.len() == *dimensions as usize,
            "subspace coefficient count mismatch"
        );
        let parameter_count = usize::try_from(update.payload.parameter_count)
            .context("compact update parameter count exceeds local usize")?;
        let values = (0..parameter_count)
            .map(|parameter_index| {
                let mixed = splitmix64(seed.wrapping_add(parameter_index as u64));
                let coefficient_index = (mixed % u64::from(*dimensions)) as usize;
                let sign = if mixed & (1 << 63) == 0 { 1.0 } else { -1.0 };
                coefficients[coefficient_index] * sign
            })
            .collect();
        Ok(FlattenedTensorPack::new(
            update.payload.model_schema_hash.clone(),
            update.payload.parameter_catalog_hash.clone(),
            values,
        ))
    }
}

/// Computes validator-owned norm telemetry for a reconstructed update.
pub fn reconstructed_update_norm_stats(pack: &FlattenedTensorPack) -> UpdateNormStats {
    UpdateNormStats {
        l2_norm: pack.l2_norm(),
        max_abs: pack.max_abs() as f64,
        clipped: false,
        non_finite_tensors: u32::from(pack.values.iter().any(|value| !value.is_finite())),
    }
}

/// Averages compatible subspace updates in coefficient space.
pub fn average_subspace_updates(
    updates: &[(&CompactUpdatePayload, f64)],
    encoding: CompactScalarEncoding,
) -> anyhow::Result<CompactUpdatePayload> {
    ensure!(!updates.is_empty(), "cannot average zero compact updates");
    let (first, _) = updates[0];
    let CompactUpdateBody::SubspaceLatent {
        dimensions,
        seed,
        coefficients: first_coefficients,
    } = &first.body
    else {
        anyhow::bail!("coefficient-space aggregation requires subspace-latent updates");
    };
    let mut aggregate = vec![0.0_f64; *dimensions as usize];
    let mut total_weight = 0.0_f64;

    for (payload, weight) in updates {
        ensure!(
            weight.is_finite() && *weight > 0.0,
            "compact update aggregation weights must be finite and positive"
        );
        ensure!(
            payload.version == first.version
                && payload.training_contract_id == first.training_contract_id
                && payload.model_schema_hash == first.model_schema_hash
                && payload.parameter_catalog_hash == first.parameter_catalog_hash
                && payload.parameter_count == first.parameter_count,
            "compact update metadata mismatch"
        );
        let CompactUpdateBody::SubspaceLatent {
            dimensions: candidate_dimensions,
            seed: candidate_seed,
            coefficients,
        } = &payload.body
        else {
            anyhow::bail!("cannot mix compact update body types");
        };
        ensure!(
            candidate_dimensions == dimensions && candidate_seed == seed,
            "compact subspace definition mismatch"
        );
        let coefficients = coefficients
            .decode()
            .context("decode compact update coefficients")?;
        for (aggregate, coefficient) in aggregate.iter_mut().zip(coefficients) {
            *aggregate += f64::from(coefficient) * *weight;
        }
        total_weight += *weight;
    }
    ensure!(
        total_weight.is_finite() && total_weight > 0.0,
        "compact update aggregate weight is invalid"
    );
    let coefficients = aggregate
        .into_iter()
        .map(|value| (value / total_weight) as f32)
        .collect::<Vec<_>>();
    let coefficients = CompactScalarVector::encode(&coefficients, encoding)
        .context("encode aggregated subspace coefficients")?;
    let _ = first_coefficients;

    Ok(CompactUpdatePayload {
        version: first.version,
        training_contract_id: first.training_contract_id.clone(),
        model_schema_hash: first.model_schema_hash.clone(),
        parameter_catalog_hash: first.parameter_catalog_hash.clone(),
        parameter_count: first.parameter_count,
        body: CompactUpdateBody::SubspaceLatent {
            dimensions: *dimensions,
            seed: *seed,
            coefficients,
        },
    })
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use burn_p2p_core::{
        DatasetViewId, LocalOptimizerStatePolicy, RecurrentStatePolicy, SchedulerStatePolicy,
        TRAINING_CONTRACT_VERSION, UpdateCodec, WorkloadId,
    };

    use super::*;

    fn content(value: &str) -> ContentId {
        ContentId::new(value)
    }

    fn contract() -> (ContentId, TrainingContractManifest) {
        let contract = TrainingContractManifest {
            version: TRAINING_CONTRACT_VERSION,
            workload_id: WorkloadId::new("test"),
            model_program_hash: content("program"),
            model_schema_hash: content("schema"),
            checkpoint_format_hash: content("checkpoint"),
            dataset_view_id: DatasetViewId::new("dataset"),
            tokenizer_hash: content("tokenizer"),
            preprocessing_hash: content("preprocessing"),
            objective_hash: content("objective"),
            optimizer_hash: content("optimizer"),
            scheduler_hash: content("scheduler"),
            optimizer_state_policy: LocalOptimizerStatePolicy::StatelessForwardOnly,
            scheduler_state_policy: SchedulerStatePolicy::CanonicalAcceptedWork,
            recurrent_state_policy: RecurrentStatePolicy::Ephemeral,
            update_codec: UpdateCodec::SubspaceLatent {
                dimensions: 3,
                seed: 19,
            },
            aggregation_hash: content("aggregation"),
            validation_hash: content("validation"),
            initialization_hash: content("initialization"),
            extensions: BTreeMap::new(),
        };
        let id = contract.contract_id().expect("contract id");
        (id, contract)
    }

    fn payload(id: &ContentId, coefficients: &[f32]) -> CompactUpdatePayload {
        CompactUpdatePayload {
            version: burn_p2p_core::COMPACT_UPDATE_PAYLOAD_VERSION,
            training_contract_id: id.clone(),
            model_schema_hash: content("schema"),
            parameter_catalog_hash: content("layout"),
            parameter_count: 17,
            body: CompactUpdateBody::SubspaceLatent {
                dimensions: 3,
                seed: 19,
                coefficients: CompactScalarVector::encode(
                    coefficients,
                    CompactScalarEncoding::Fp32,
                )
                .expect("coefficients"),
            },
        }
    }

    #[test]
    fn compact_update_round_trip_is_contract_bound() {
        let (id, contract) = contract();
        let payload = payload(&id, &[1.0, -2.0, 0.5]);
        let bytes = encode_compact_update(&payload, &id, &contract).expect("encode");
        let update = decode_compact_update(&bytes, &id, &contract).expect("decode");
        assert_eq!(update.payload, payload);

        let other_id = content("other-contract");
        assert!(decode_compact_update(&bytes, &other_id, &contract).is_err());
    }

    #[test]
    fn coefficient_aggregation_equals_reconstructed_delta_aggregation() {
        let (id, contract) = contract();
        let first = payload(&id, &[1.0, -2.0, 0.5]);
        let second = payload(&id, &[-0.5, 4.0, 2.0]);
        let averaged = average_subspace_updates(
            &[(&first, 1.0), (&second, 3.0)],
            CompactScalarEncoding::Fp32,
        )
        .expect("average");
        let reconstructor = SeededSubspaceReconstructor;
        let reconstruct = |payload: &CompactUpdatePayload| {
            let bytes = encode_compact_update(payload, &id, &contract).expect("encode");
            let validated = decode_compact_update(&bytes, &id, &contract).expect("decode");
            reconstructor.reconstruct(&validated).expect("reconstruct")
        };
        let first_pack = reconstruct(&first);
        let second_pack = reconstruct(&second);
        let averaged_pack = reconstruct(&averaged);
        for ((first, second), averaged) in first_pack
            .values
            .iter()
            .zip(second_pack.values.iter())
            .zip(averaged_pack.values.iter())
        {
            let expected = (*first + *second * 3.0) / 4.0;
            assert!((averaged - expected).abs() <= f32::EPSILON);
        }
    }

    #[test]
    fn malformed_compact_update_fails_before_reconstruction() {
        let (id, contract) = contract();
        let mut payload = payload(&id, &[1.0, 2.0, 3.0]);
        let CompactUpdateBody::SubspaceLatent { coefficients, .. } = &mut payload.body else {
            unreachable!()
        };
        coefficients.bytes.pop();
        let raw = deterministic_cbor(&payload).expect("raw payload");
        assert!(decode_compact_update(&raw, &id, &contract).is_err());
    }
}
