use anyhow::{bail, ensure};
use burn_p2p_core::{
    DiLoCoAggregationPolicy, ExperimentId, FlattenedTensorPack, GradientCodec, PeerId,
    PseudoGradientChunk, PseudoGradientManifest, PseudoGradientManifestInput, RevisionId,
    RoundCursor, SignMajorityTieBreak,
};
use chrono::Utc;
use half::f16;

#[derive(Clone, Debug, PartialEq)]
/// Holds one encoded pseudo-gradient together with its manifest and chunk payloads.
pub struct EncodedPseudoGradient {
    /// Manifest describing the encoded payload.
    pub manifest: PseudoGradientManifest,
    /// Encoded chunks.
    pub chunks: Vec<PseudoGradientChunk>,
}

impl EncodedPseudoGradient {
    /// Returns the total encoded bytes across all chunks.
    pub fn total_encoded_bytes(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }
}

/// Encodes one pseudo-gradient pack using the configured transport codec and chunk size.
pub fn encode_pseudo_gradient(
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    peer_id: PeerId,
    round_cursor: RoundCursor,
    codec: GradientCodec,
    pack: &FlattenedTensorPack,
    chunk_size_bytes: usize,
) -> anyhow::Result<EncodedPseudoGradient> {
    ensure!(chunk_size_bytes > 0, "chunk size must be positive");

    let encoded = encode_values(&codec, &pack.values)?;
    let transport_values = decode_values(&codec, &encoded, pack.parameter_count())?;
    let transport_pack = FlattenedTensorPack::new(
        pack.model_schema_hash.clone(),
        pack.layout_hash.clone(),
        transport_values,
    );
    let created_at = Utc::now();
    let chunks = encoded
        .chunks(chunk_size_bytes)
        .enumerate()
        .map(|(chunk_index, bytes)| PseudoGradientChunk {
            manifest_id: burn_p2p_core::ContentId::new("pending-manifest"),
            chunk_index: chunk_index as u32,
            bytes: bytes.to_vec(),
            generated_at: created_at,
        })
        .collect::<Vec<_>>();

    let manifest = PseudoGradientManifest::try_new(PseudoGradientManifestInput {
        experiment_id,
        revision_id,
        peer_id,
        round_cursor,
        codec,
        pack: &transport_pack,
        chunk_count: chunks.len() as u32,
        total_encoded_bytes: encoded.len() as u64,
        created_at,
    })?;

    let chunks = chunks
        .into_iter()
        .map(|chunk| PseudoGradientChunk {
            manifest_id: manifest.manifest_id.clone(),
            ..chunk
        })
        .collect();

    Ok(EncodedPseudoGradient { manifest, chunks })
}

/// Decodes one pseudo-gradient manifest plus its chunks back into a flattened tensor pack.
pub fn decode_pseudo_gradient(
    manifest: &PseudoGradientManifest,
    chunks: &[PseudoGradientChunk],
) -> anyhow::Result<FlattenedTensorPack> {
    ensure!(
        chunks.len() == manifest.chunk_count as usize,
        "manifest {} expected {} chunks, received {}",
        manifest.manifest_id.as_str(),
        manifest.chunk_count,
        chunks.len()
    );

    let mut ordered = chunks.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|chunk| chunk.chunk_index);
    for (expected, chunk) in ordered.iter().enumerate() {
        ensure!(
            chunk.manifest_id == manifest.manifest_id,
            "pseudo-gradient chunk {} belongs to manifest {}, expected {}",
            chunk.chunk_index,
            chunk.manifest_id.as_str(),
            manifest.manifest_id.as_str()
        );
        ensure!(
            chunk.chunk_index == expected as u32,
            "pseudo-gradient chunk ordering is not contiguous: expected {}, got {}",
            expected,
            chunk.chunk_index
        );
    }

    let bytes = ordered
        .into_iter()
        .flat_map(|chunk| chunk.bytes.iter().copied())
        .collect::<Vec<_>>();
    ensure!(
        bytes.len() == manifest.total_encoded_bytes as usize,
        "manifest {} expected {} encoded bytes, received {}",
        manifest.manifest_id.as_str(),
        manifest.total_encoded_bytes,
        bytes.len()
    );

    let values = decode_values(&manifest.codec, &bytes, manifest.parameter_count as usize)?;
    let pack = FlattenedTensorPack::new(
        manifest.model_schema_hash.clone(),
        manifest.layout_hash.clone(),
        values,
    );
    let checksum = pack.checksum()?;
    ensure!(
        checksum == manifest.checksum,
        "manifest {} checksum mismatch: decoded {}, expected {}",
        manifest.manifest_id.as_str(),
        checksum.as_str(),
        manifest.checksum.as_str()
    );
    Ok(pack)
}

/// Aggregates compatible pseudo-gradients according to the configured DiLoCo policy.
pub fn aggregate_pseudo_gradients(
    policy: &DiLoCoAggregationPolicy,
    gradients: &[FlattenedTensorPack],
) -> anyhow::Result<FlattenedTensorPack> {
    match policy {
        DiLoCoAggregationPolicy::Mean => average_pseudo_gradients(gradients),
        DiLoCoAggregationPolicy::CoordinateMedian => coordinate_median_pseudo_gradients(gradients),
        DiLoCoAggregationPolicy::SignMajority { tie_break, .. } => {
            sign_majority_pseudo_gradients(gradients, policy.minimum_agreement(), tie_break)
        }
    }
}

/// Averages one set of compatible pseudo-gradients with equal weight.
pub fn average_pseudo_gradients(
    gradients: &[FlattenedTensorPack],
) -> anyhow::Result<FlattenedTensorPack> {
    ensure!(
        !gradients.is_empty(),
        "cannot average zero pseudo-gradients"
    );

    let first = &gradients[0];
    let mut values = vec![0.0_f32; first.values.len()];
    for gradient in gradients {
        ensure!(
            first.is_compatible_with(gradient),
            "pseudo-gradient packs are incompatible for aggregation"
        );
        for (aggregate, value) in values.iter_mut().zip(&gradient.values) {
            *aggregate += *value;
        }
    }
    let scale = 1.0_f32 / gradients.len() as f32;
    for value in &mut values {
        *value *= scale;
    }

    Ok(FlattenedTensorPack::new(
        first.model_schema_hash.clone(),
        first.layout_hash.clone(),
        values,
    ))
}

fn coordinate_median_pseudo_gradients(
    gradients: &[FlattenedTensorPack],
) -> anyhow::Result<FlattenedTensorPack> {
    ensure!(
        !gradients.is_empty(),
        "cannot aggregate zero pseudo-gradients"
    );

    let first = &gradients[0];
    for gradient in gradients {
        ensure!(
            first.is_compatible_with(gradient),
            "pseudo-gradient packs are incompatible for aggregation"
        );
    }

    let mut values = Vec::with_capacity(first.values.len());
    let midpoint = gradients.len() / 2;
    for value_index in 0..first.values.len() {
        let mut coordinate_values = gradients
            .iter()
            .map(|gradient| gradient.values[value_index])
            .collect::<Vec<_>>();
        coordinate_values.sort_by(f32::total_cmp);
        let median = if coordinate_values.len() % 2 == 0 {
            (coordinate_values[midpoint - 1] + coordinate_values[midpoint]) * 0.5
        } else {
            coordinate_values[midpoint]
        };
        values.push(median);
    }

    Ok(FlattenedTensorPack::new(
        first.model_schema_hash.clone(),
        first.layout_hash.clone(),
        values,
    ))
}

fn sign_majority_pseudo_gradients(
    gradients: &[FlattenedTensorPack],
    minimum_agreement: f32,
    tie_break: &SignMajorityTieBreak,
) -> anyhow::Result<FlattenedTensorPack> {
    ensure!(
        !gradients.is_empty(),
        "cannot aggregate zero pseudo-gradients"
    );

    let first = &gradients[0];
    let mut values = Vec::with_capacity(first.values.len());
    for gradient in gradients {
        ensure!(
            first.is_compatible_with(gradient),
            "pseudo-gradient packs are incompatible for aggregation"
        );
    }

    for value_index in 0..first.values.len() {
        let mut positive = 0usize;
        let mut negative = 0usize;
        let mut zero = 0usize;
        for gradient in gradients {
            let value = gradient.values[value_index];
            if value > 0.0 {
                positive += 1;
            } else if value < 0.0 {
                negative += 1;
            } else {
                zero += 1;
            }
        }

        let max_votes = positive.max(negative).max(zero);
        let positive_tied = positive == max_votes && positive > 0;
        let negative_tied = negative == max_votes && negative > 0;
        let zero_tied = zero == max_votes && zero > 0;
        let (candidate, agreement_votes) = if zero_tied {
            (0.0_f32, zero)
        } else if positive_tied && negative_tied {
            match tie_break {
                SignMajorityTieBreak::Zero => (0.0_f32, zero),
                SignMajorityTieBreak::Positive => (1.0_f32, positive),
                SignMajorityTieBreak::Negative => (-1.0_f32, negative),
            }
        } else if positive_tied {
            (1.0_f32, positive)
        } else if negative_tied {
            (-1.0_f32, negative)
        } else {
            (0.0_f32, zero)
        };

        let agreement = agreement_votes as f32 / gradients.len() as f32;
        if candidate != 0.0 && agreement < minimum_agreement {
            values.push(0.0);
        } else {
            values.push(candidate);
        }
    }

    Ok(FlattenedTensorPack::new(
        first.model_schema_hash.clone(),
        first.layout_hash.clone(),
        values,
    ))
}

fn encode_values(codec: &GradientCodec, values: &[f32]) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match codec {
        GradientCodec::Fp32 => {
            bytes.reserve(std::mem::size_of_val(values));
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        GradientCodec::Fp16 => {
            bytes.reserve(values.len() * std::mem::size_of::<u16>());
            for value in values {
                bytes.extend_from_slice(&f16::from_f32(*value).to_le_bytes());
            }
        }
        GradientCodec::BlockwiseInt8 { block_size } => {
            let block_size = usize::try_from(*block_size).unwrap_or(0);
            ensure!(block_size > 0, "blockwise int8 block size must be positive");
            for block in values.chunks(block_size) {
                let max_abs = block
                    .iter()
                    .fold(0.0_f32, |acc, value| acc.max(value.abs()));
                let scale = if max_abs <= f32::EPSILON {
                    0.0_f32
                } else {
                    max_abs / 127.0_f32
                };
                bytes.extend_from_slice(&(block.len() as u16).to_le_bytes());
                bytes.extend_from_slice(&scale.to_le_bytes());
                for value in block {
                    let quantized = if scale == 0.0 {
                        0_i8
                    } else {
                        (value / scale).round().clamp(-127.0, 127.0) as i8
                    };
                    bytes.push(quantized as u8);
                }
            }
        }
        GradientCodec::SignSgd { .. } => {
            bytes = encode_sign_sgd_values(values);
        }
        GradientCodec::Qsgd { bits, stochastic } => {
            bytes = encode_qsgd_values(values, *bits, *stochastic)?;
        }
        GradientCodec::LowRank { .. } => bail!(
            "low-rank DiLoCo transport requires tensor-shape layout metadata; flattened packs cannot preserve rank semantics"
        ),
    }
    Ok(bytes)
}

fn decode_values(
    codec: &GradientCodec,
    bytes: &[u8],
    expected_values: usize,
) -> anyhow::Result<Vec<f32>> {
    match codec {
        GradientCodec::Fp32 => {
            ensure!(
                bytes.len() == expected_values * std::mem::size_of::<f32>(),
                "fp32 payload expected {} bytes, received {}",
                expected_values * std::mem::size_of::<f32>(),
                bytes.len()
            );
            Ok(bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("fp32 chunk")))
                .collect())
        }
        GradientCodec::Fp16 => {
            ensure!(
                bytes.len() == expected_values * std::mem::size_of::<u16>(),
                "fp16 payload expected {} bytes, received {}",
                expected_values * std::mem::size_of::<u16>(),
                bytes.len()
            );
            Ok(bytes
                .chunks_exact(2)
                .map(|chunk| f16::from_le_bytes(chunk.try_into().expect("fp16 chunk")).to_f32())
                .collect())
        }
        GradientCodec::BlockwiseInt8 { .. } => {
            let mut offset = 0usize;
            let mut values = Vec::with_capacity(expected_values);
            while values.len() < expected_values {
                ensure!(offset + 6 <= bytes.len(), "truncated blockwise int8 header");
                let block_len = u16::from_le_bytes(bytes[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                let scale = f32::from_le_bytes(bytes[offset..offset + 4].try_into()?);
                offset += 4;
                ensure!(
                    offset + block_len <= bytes.len(),
                    "truncated blockwise int8 block"
                );
                for quantized in &bytes[offset..offset + block_len] {
                    let signed = *quantized as i8;
                    values.push(f32::from(signed) * scale);
                }
                offset += block_len;
            }
            ensure!(
                values.len() == expected_values,
                "decoded {} values, expected {}",
                values.len(),
                expected_values
            );
            ensure!(
                offset == bytes.len(),
                "blockwise int8 payload has trailing bytes"
            );
            Ok(values)
        }
        GradientCodec::SignSgd { .. } => decode_sign_sgd_values(bytes, expected_values),
        GradientCodec::Qsgd { bits, .. } => decode_qsgd_values(bytes, expected_values, *bits),
        GradientCodec::LowRank { .. } => bail!(
            "low-rank DiLoCo transport requires tensor-shape layout metadata; flattened packs cannot preserve rank semantics"
        ),
    }
}

fn encode_sign_sgd_values(values: &[f32]) -> Vec<u8> {
    let mut bytes = vec![0u8; values.len().saturating_add(3) / 4];
    for (index, value) in values.iter().enumerate() {
        let code = if *value > 0.0 {
            1u8
        } else if *value < 0.0 {
            2u8
        } else {
            0u8
        };
        bytes[index / 4] |= code << ((index % 4) * 2);
    }
    bytes
}

fn decode_sign_sgd_values(bytes: &[u8], expected_values: usize) -> anyhow::Result<Vec<f32>> {
    let expected_bytes = expected_values.saturating_add(3) / 4;
    ensure!(
        bytes.len() == expected_bytes,
        "signSGD payload expected {} packed bytes for {} values, received {}",
        expected_bytes,
        expected_values,
        bytes.len()
    );

    let mut values = Vec::with_capacity(expected_values);
    for index in 0..expected_values {
        values.push(match packed_sign_code(bytes, index) {
            0 => 0.0,
            1 => 1.0,
            2 => -1.0,
            _ => bail!("signSGD payload contains reserved sign code"),
        });
    }
    for index in expected_values..(expected_bytes * 4) {
        ensure!(
            packed_sign_code(bytes, index) == 0,
            "signSGD payload contains non-zero padding bits"
        );
    }

    Ok(values)
}

fn packed_sign_code(bytes: &[u8], index: usize) -> u8 {
    (bytes[index / 4] >> ((index % 4) * 2)) & 0b11
}

fn encode_qsgd_values(values: &[f32], bits: u8, stochastic: bool) -> anyhow::Result<Vec<u8>> {
    let levels = qsgd_levels(bits)?;
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    ensure!(norm.is_finite(), "QSGD payload norm must be finite");

    let mut bytes = Vec::with_capacity(4 + values.len() * 3);
    bytes.extend_from_slice(&norm.to_le_bytes());
    for (index, value) in values.iter().enumerate() {
        let sign = if *value > 0.0 {
            1i8
        } else if *value < 0.0 {
            -1i8
        } else {
            0i8
        };
        let level = if sign == 0 || norm <= f32::EPSILON {
            0u16
        } else {
            let scaled = (value.abs() / norm) * levels as f32;
            let quantized = if stochastic {
                let floor = scaled.floor();
                let fraction = scaled - floor;
                floor
                    + if qsgd_deterministic_unit(index, *value, bits) < fraction {
                        1.0
                    } else {
                        0.0
                    }
            } else {
                scaled.round()
            };
            quantized.clamp(0.0, levels as f32) as u16
        };
        bytes.extend_from_slice(&level.to_le_bytes());
        bytes.push(sign as u8);
    }
    Ok(bytes)
}

fn decode_qsgd_values(bytes: &[u8], expected_values: usize, bits: u8) -> anyhow::Result<Vec<f32>> {
    let levels = qsgd_levels(bits)?;
    let expected_bytes = 4 + expected_values * 3;
    ensure!(
        bytes.len() == expected_bytes,
        "QSGD payload expected {} bytes, received {}",
        expected_bytes,
        bytes.len()
    );
    let norm = f32::from_le_bytes(bytes[0..4].try_into()?);
    ensure!(
        norm.is_finite() && norm >= 0.0,
        "QSGD payload norm must be finite and non-negative"
    );

    let mut offset = 4usize;
    let mut values = Vec::with_capacity(expected_values);
    for _ in 0..expected_values {
        let level = u16::from_le_bytes(bytes[offset..offset + 2].try_into()?);
        offset += 2;
        ensure!(
            u32::from(level) <= levels,
            "QSGD level {} exceeds configured maximum {}",
            level,
            levels
        );
        let sign = bytes[offset] as i8;
        offset += 1;
        ensure!(
            matches!(sign, -1..=1),
            "QSGD payload contains invalid sign {}",
            sign
        );
        ensure!(
            sign != 0 || level == 0,
            "QSGD payload contains non-zero level for zero sign"
        );
        let value = if sign == 0 || level == 0 || norm <= f32::EPSILON {
            0.0
        } else {
            f32::from(sign) * norm * (f32::from(level) / levels as f32)
        };
        values.push(value);
    }
    Ok(values)
}

fn qsgd_levels(bits: u8) -> anyhow::Result<u32> {
    ensure!(
        (1..=15).contains(&bits),
        "QSGD bits must be in 1..=15, got {}",
        bits
    );
    Ok((1u32 << bits) - 1)
}

fn qsgd_deterministic_unit(index: usize, value: f32, bits: u8) -> f32 {
    let seed = index as u64
        ^ (u64::from(value.to_bits()).rotate_left(17))
        ^ (u64::from(bits).rotate_left(48));
    let mixed = splitmix64(seed);
    ((mixed >> 40) as u32) as f32 / ((1u32 << 24) as f32)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}
