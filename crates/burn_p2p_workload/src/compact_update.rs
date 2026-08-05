use anyhow::{Context, ensure};
use burn_p2p_core::{
    CanonicalSchema, CompactScalarEncoding, CompactScalarVector, CompactUpdateBody,
    CompactUpdatePayload, ContentId, FlattenedTensorPack, TrainingContractManifest,
    UpdateNormStats, WorkloadUpdateEnvelope, deterministic_cbor, from_cbor_slice,
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

/// Decodes a context-sparse payload and proves that its routed identity is the one signed by the
/// workload envelope. Consumers must use this boundary rather than trusting either copy alone.
pub fn decode_context_sparse_update(
    bytes: &[u8],
    envelope: &WorkloadUpdateEnvelope,
    contract_id: &ContentId,
    contract: &TrainingContractManifest,
) -> anyhow::Result<ValidatedCompactUpdate> {
    envelope
        .validate_against(contract_id, contract)
        .context("validate context-sparse workload envelope")?;
    let expected = envelope
        .routing_context
        .as_ref()
        .context("context-sparse workload envelope is missing routed identity")?;
    let update = decode_compact_update(bytes, contract_id, contract)?;
    let CompactUpdateBody::ContextSparseDelta { context, .. } = &update.payload.body else {
        anyhow::bail!("context-sparse envelope references a different compact payload body");
    };
    ensure!(
        context == expected,
        "context-sparse payload identity does not match signed workload envelope"
    );
    Ok(update)
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

/// Direct decoder for a canonical mutable-parameter subset.
#[derive(Clone, Copy, Debug, Default)]
pub struct MutableSubsetParameterReconstructor;

impl CompactUpdateReconstructor for MutableSubsetParameterReconstructor {
    fn reconstruct(&self, update: &ValidatedCompactUpdate) -> anyhow::Result<FlattenedTensorPack> {
        let CompactUpdateBody::MutableSubsetParameters { values } = &update.payload.body else {
            anyhow::bail!("mutable subset reconstructor requires a mutable-subset payload");
        };
        let values = values.decode().context("decode mutable-subset values")?;
        ensure!(
            values.len() as u64 == update.payload.parameter_count,
            "mutable-subset parameter count mismatch"
        );
        Ok(FlattenedTensorPack::new(
            update.payload.model_schema_hash.clone(),
            update.payload.parameter_catalog_hash.clone(),
            values,
        ))
    }
}

/// Direct decoder for a dynamically catalogued routed-context delta.
#[derive(Clone, Copy, Debug, Default)]
pub struct ContextSparseDeltaReconstructor;

impl CompactUpdateReconstructor for ContextSparseDeltaReconstructor {
    fn reconstruct(&self, update: &ValidatedCompactUpdate) -> anyhow::Result<FlattenedTensorPack> {
        let CompactUpdateBody::ContextSparseDelta { context, deltas } = &update.payload.body else {
            anyhow::bail!("context sparse reconstructor requires a context-sparse payload");
        };
        ensure!(
            context.parameter_catalog_hash == update.payload.parameter_catalog_hash,
            "context parameter catalog mismatch"
        );
        let values = deltas.decode().context("decode context-sparse deltas")?;
        ensure!(
            values.len() as u64 == update.payload.parameter_count,
            "context-sparse parameter count mismatch"
        );
        Ok(FlattenedTensorPack::new(
            update.payload.model_schema_hash.clone(),
            update.payload.parameter_catalog_hash.clone(),
            values,
        ))
    }
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

/// Averages compatible mutable-parameter subsets in canonical parameter order.
pub fn average_mutable_subset_parameters(
    updates: &[(&CompactUpdatePayload, f64)],
    encoding: CompactScalarEncoding,
) -> anyhow::Result<CompactUpdatePayload> {
    ensure!(!updates.is_empty(), "cannot average zero compact updates");
    let (first, _) = updates[0];
    let CompactUpdateBody::MutableSubsetParameters {
        values: first_values,
    } = &first.body
    else {
        anyhow::bail!("mutable-subset aggregation requires mutable-subset updates");
    };
    let parameter_count = usize::try_from(first.parameter_count)
        .context("mutable-subset parameter count exceeds local usize")?;
    let mut aggregate = vec![0.0_f64; parameter_count];
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
        let CompactUpdateBody::MutableSubsetParameters { values } = &payload.body else {
            anyhow::bail!("cannot mix compact update body types");
        };
        let values = values.decode().context("decode mutable-subset values")?;
        ensure!(
            values.len() == parameter_count,
            "mutable-subset scalar count mismatch"
        );
        for (aggregate, value) in aggregate.iter_mut().zip(values) {
            *aggregate += f64::from(value) * *weight;
        }
        total_weight += *weight;
    }
    ensure!(
        total_weight.is_finite() && total_weight > 0.0,
        "compact update aggregate weight is invalid"
    );
    let values = aggregate
        .into_iter()
        .map(|value| (value / total_weight) as f32)
        .collect::<Vec<_>>();
    let values = CompactScalarVector::encode(&values, encoding)
        .context("encode aggregated mutable-subset values")?;
    let _ = first_values;

    Ok(CompactUpdatePayload {
        version: first.version,
        training_contract_id: first.training_contract_id.clone(),
        model_schema_hash: first.model_schema_hash.clone(),
        parameter_catalog_hash: first.parameter_catalog_hash.clone(),
        parameter_count: first.parameter_count,
        body: CompactUpdateBody::MutableSubsetParameters { values },
    })
}

/// Averages deltas only when every update targets the exact same context
/// family, slot generation, and dynamic parameter catalog.
pub fn average_context_sparse_deltas(
    updates: &[(&CompactUpdatePayload, f64)],
    encoding: CompactScalarEncoding,
) -> anyhow::Result<CompactUpdatePayload> {
    ensure!(!updates.is_empty(), "cannot average zero compact updates");
    let (first, _) = updates[0];
    let CompactUpdateBody::ContextSparseDelta {
        context,
        deltas: first_deltas,
    } = &first.body
    else {
        anyhow::bail!("context aggregation requires context-sparse updates");
    };
    let parameter_count = usize::try_from(first.parameter_count)
        .context("context-sparse parameter count exceeds local usize")?;
    let mut aggregate = vec![0.0_f64; parameter_count];
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
        let CompactUpdateBody::ContextSparseDelta {
            context: candidate_context,
            deltas,
        } = &payload.body
        else {
            anyhow::bail!("cannot mix compact update body types");
        };
        ensure!(
            candidate_context == context,
            "context-sparse updates target different context generations"
        );
        let values = deltas.decode().context("decode context-sparse deltas")?;
        ensure!(
            values.len() == parameter_count,
            "context-sparse scalar count mismatch"
        );
        for (aggregate, value) in aggregate.iter_mut().zip(values) {
            *aggregate += f64::from(value) * *weight;
        }
        total_weight += *weight;
    }
    ensure!(
        total_weight.is_finite() && total_weight > 0.0,
        "compact update aggregate weight is invalid"
    );
    let values = aggregate
        .into_iter()
        .map(|value| (value / total_weight) as f32)
        .collect::<Vec<_>>();
    let deltas = CompactScalarVector::encode(&values, encoding)
        .context("encode aggregated context-sparse deltas")?;
    let _ = first_deltas;
    Ok(CompactUpdatePayload {
        version: first.version,
        training_contract_id: first.training_contract_id.clone(),
        model_schema_hash: first.model_schema_hash.clone(),
        parameter_catalog_hash: first.parameter_catalog_hash.clone(),
        parameter_count: first.parameter_count,
        body: CompactUpdateBody::ContextSparseDelta {
            context: context.clone(),
            deltas,
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
        ArtifactDescriptor, ArtifactId, ArtifactKind, DatasetViewId, HeadId, LeaseId,
        LocalOptimizerStatePolicy, Precision, RecurrentStatePolicy, RevisionId,
        SchedulerStatePolicy, TRAINING_CONTRACT_VERSION, UpdateCodec, UpdateRoutingContext,
        WindowId, WorkloadId, WorkloadUpdateEnvelope,
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

    fn context_contract() -> (ContentId, TrainingContractManifest) {
        let (_, mut contract) = contract();
        contract.update_codec = UpdateCodec::ContextSparseDelta {
            context_family_hash: content("context-family"),
            max_parameter_count: 8,
            encoding: CompactScalarEncoding::SymmetricInt16,
        };
        let id = contract.contract_id().expect("context contract id");
        (id, contract)
    }

    fn context_payload(id: &ContentId, generation: u64, values: &[f32]) -> CompactUpdatePayload {
        let context = UpdateRoutingContext {
            context_family_hash: content("context-family"),
            slot: 2,
            generation,
            parameter_catalog_hash: content("context-layout"),
        };
        CompactUpdatePayload {
            version: burn_p2p_core::COMPACT_UPDATE_PAYLOAD_VERSION,
            training_contract_id: id.clone(),
            model_schema_hash: content("schema"),
            parameter_catalog_hash: context.parameter_catalog_hash.clone(),
            parameter_count: values.len() as u64,
            body: CompactUpdateBody::ContextSparseDelta {
                context,
                deltas: CompactScalarVector::encode(values, CompactScalarEncoding::SymmetricInt16)
                    .expect("context deltas"),
            },
        }
    }

    fn context_envelope(
        id: &ContentId,
        contract: &TrainingContractManifest,
        context: UpdateRoutingContext,
    ) -> WorkloadUpdateEnvelope {
        WorkloadUpdateEnvelope {
            training_contract_id: id.clone(),
            revision_id: RevisionId::new("revision"),
            base_head_id: HeadId::new("base"),
            window_id: WindowId(3),
            lease_id: LeaseId::new("lease"),
            codec: contract.update_codec.clone(),
            routing_context: Some(context),
            artifact: ArtifactDescriptor {
                artifact_id: ArtifactId::new("context-update"),
                kind: ArtifactKind::DeltaPack,
                head_id: None,
                base_head_id: Some(HeadId::new("base")),
                precision: Precision::Custom("context-int16".into()),
                model_schema_hash: content("schema"),
                record_format: "test-context-update".into(),
                bytes_len: 1,
                chunks: Vec::new(),
                root_hash: content("context-update-root"),
            },
            decoded_tensor_digest: None,
            claimed_norm_stats: None,
            claimed_feature_sketch: None,
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
    fn context_sparse_roundtrip_reconstructs_and_rejects_stale_generation_aggregation() {
        let (id, contract) = context_contract();
        let first = context_payload(&id, 4, &[1.0, -2.0, 0.5]);
        let second = context_payload(&id, 4, &[3.0, 2.0, -0.5]);
        let bytes = encode_compact_update(&first, &id, &contract).expect("encode context update");
        let validated =
            decode_compact_update(&bytes, &id, &contract).expect("decode context update");
        let reconstructed = ContextSparseDeltaReconstructor
            .reconstruct(&validated)
            .expect("reconstruct context update");
        assert_eq!(reconstructed.values.len(), 3);

        let averaged = average_context_sparse_deltas(
            &[(&first, 1.0), (&second, 1.0)],
            CompactScalarEncoding::SymmetricInt16,
        )
        .expect("average same context generation");
        let CompactUpdateBody::ContextSparseDelta { deltas, .. } = averaged.body else {
            panic!("context body");
        };
        let values = deltas.decode().expect("averaged deltas");
        for (actual, expected) in values.into_iter().zip([2.0_f32, 0.0, 0.0]) {
            assert!((actual - expected).abs() < 1.0e-3);
        }

        let stale = context_payload(&id, 3, &[3.0, 2.0, -0.5]);
        assert!(
            average_context_sparse_deltas(
                &[(&first, 1.0), (&stale, 1.0)],
                CompactScalarEncoding::SymmetricInt16,
            )
            .is_err()
        );

        let context = match &first.body {
            CompactUpdateBody::ContextSparseDelta { context, .. } => context.clone(),
            _ => unreachable!(),
        };
        let envelope = context_envelope(&id, &contract, context);
        decode_context_sparse_update(&bytes, &envelope, &id, &contract)
            .expect("signed envelope and context payload agree");
        let mut mismatched = envelope;
        mismatched
            .routing_context
            .as_mut()
            .expect("context identity")
            .generation += 1;
        assert!(decode_context_sparse_update(&bytes, &mismatched, &id, &contract).is_err());
    }

    #[test]
    fn context_sparse_int16_payload_preserves_the_expected_bandwidth_reduction() {
        const ACTIVE_VALUES: usize = 4_096;
        const DENSE_VALUES: usize = ACTIVE_VALUES * 4;
        let (_, mut contract) = context_contract();
        contract.update_codec = UpdateCodec::ContextSparseDelta {
            context_family_hash: content("context-family"),
            max_parameter_count: ACTIVE_VALUES as u64,
            encoding: CompactScalarEncoding::SymmetricInt16,
        };
        let id = contract.contract_id().expect("context contract id");
        let values = (0..ACTIVE_VALUES)
            .map(|index| (index as f32 / ACTIVE_VALUES as f32).mul_add(2.0, -1.0))
            .collect::<Vec<_>>();
        let payload = context_payload(&id, 7, &values);
        let bytes = encode_compact_update(&payload, &id, &contract).expect("context wire payload");
        let dense_fp32_bytes = DENSE_VALUES * std::mem::size_of::<f32>();

        // A 25%-active int16 update has one eighth of the dense FP32 scalar
        // body. Canonical CBOR and identity metadata must keep total wire cost
        // below one sixth of dense FP32, leaving margin for schema evolution.
        assert!(
            bytes.len() * 6 < dense_fp32_bytes,
            "context payload {} bytes did not preserve the bandwidth bound against {dense_fp32_bytes} dense bytes",
            bytes.len()
        );
        let decoded =
            decode_compact_update(&bytes, &id, &contract).expect("decode context payload");
        assert_eq!(decoded.payload.parameter_count, ACTIVE_VALUES as u64);
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
