use std::collections::BTreeMap;

use burn_p2p_core::UpdateFeatureSketch;

#[derive(Clone, Debug, PartialEq, Eq)]
/// One named layer span used to derive per-layer norms.
pub struct FeatureLayer {
    /// Human-readable layer label.
    pub name: String,
    /// Start index within the flattened vector.
    pub start: usize,
    /// Exclusive end index within the flattened vector.
    pub end: usize,
}

impl FeatureLayer {
    /// Creates a feature layer span.
    pub fn new(name: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            name: name.into(),
            start,
            end,
        }
    }
}

/// Computes cosine similarity between two vectors when both are non-zero.
pub fn cosine_similarity(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }

    let dot = left.iter().zip(right).map(|(a, b)| a * b).sum::<f64>();
    let left_norm = l2_norm(left);
    let right_norm = l2_norm(right);
    if left_norm == 0.0 || right_norm == 0.0 {
        None
    } else {
        Some((dot / (left_norm * right_norm)).clamp(-1.0, 1.0))
    }
}

/// Extracts a compact update feature sketch from one dense flattened update vector.
pub fn extract_feature_sketch(
    update: &[f64],
    reference: Option<&[f64]>,
    layers: &[FeatureLayer],
    sketch_dimensionality: usize,
    staleness_windows: u16,
    receive_delay_ms: u64,
    canary_loss_delta: Option<f64>,
) -> UpdateFeatureSketch {
    let artifact_size_bytes = std::mem::size_of_val(update) as u64;
    let global_norm = l2_norm(update);
    let non_finite_tensor_count = update.iter().filter(|value| !value.is_finite()).count() as u32;

    let per_layer_norms = if layers.is_empty() {
        BTreeMap::from([(String::from("update"), global_norm)])
    } else {
        layers
            .iter()
            .map(|layer| {
                let end = layer.end.min(update.len());
                let start = layer.start.min(end);
                let norm = l2_norm(&update[start..end]);
                (layer.name.clone(), norm)
            })
            .collect::<BTreeMap<_, _>>()
    };

    let random_projection = projection_sketch(update, sketch_dimensionality.max(1));
    let sign_projection = random_projection
        .iter()
        .map(|value| {
            if *value > 0.0 {
                1
            } else if *value < 0.0 {
                -1
            } else {
                0
            }
        })
        .collect::<Vec<_>>();
    let top_k_indices = top_k_indices(update, 8);
    let cosine_to_reference = reference.and_then(|candidate| cosine_similarity(update, candidate));
    let sign_agreement_fraction = reference.map(|candidate| {
        let agreement = update
            .iter()
            .zip(candidate.iter())
            .filter(|(left, right)| left.signum() == right.signum())
            .count();
        agreement as f64 / update.len().max(1) as f64
    });
    let historical_deviation_score = cosine_to_reference.map(|value| (1.0 - value).max(0.0));
    let neighbor_distance = cosine_to_reference.map(|value| (1.0 - value).abs() * global_norm);

    UpdateFeatureSketch {
        artifact_size_bytes,
        global_norm,
        per_layer_norms,
        random_projection,
        sign_projection,
        top_k_indices,
        cosine_to_reference,
        sign_agreement_fraction,
        canary_loss_delta,
        historical_deviation_score,
        neighbor_distance,
        staleness_windows,
        receive_delay_ms,
        non_finite_tensor_count,
    }
}

fn l2_norm(vector: &[f64]) -> f64 {
    vector.iter().map(|value| value * value).sum::<f64>().sqrt()
}

fn projection_sketch(update: &[f64], dimensions: usize) -> Vec<f64> {
    if update.is_empty() {
        return vec![0.0; dimensions];
    }
    let chunk = update.len().div_ceil(dimensions).max(1);
    (0..dimensions)
        .map(|index| {
            let start = index * chunk;
            let end = (start + chunk).min(update.len());
            if start >= end {
                0.0
            } else {
                update[start..end].iter().sum::<f64>() / (end - start) as f64
            }
        })
        .collect()
}

fn top_k_indices(update: &[f64], limit: usize) -> Vec<u32> {
    let mut ranked = update
        .iter()
        .enumerate()
        .map(|(index, value)| (index as u32, value.abs()))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .take(limit)
        .map(|(index, _)| index)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{FeatureLayer, cosine_similarity, extract_feature_sketch};

    #[test]
    fn feature_sketch_captures_norms_similarity_and_top_k() {
        let update = vec![1.0, 2.0, -3.0, 0.5, -0.25, 0.25];
        let reference = vec![0.75, 1.5, -2.5, 0.5, -0.5, 0.125];
        let sketch = extract_feature_sketch(
            &update,
            Some(&reference),
            &[
                FeatureLayer::new("encoder", 0, 4),
                FeatureLayer::new("head", 4, 6),
            ],
            4,
            1,
            250,
            Some(0.01),
        );

        assert!(sketch.global_norm > 0.0);
        assert_eq!(sketch.per_layer_norms.len(), 2);
        assert_eq!(sketch.top_k_indices.first().copied(), Some(2));
        assert!(sketch.cosine_to_reference.expect("cosine") > 0.9);
        assert_eq!(sketch.receive_delay_ms, 250);
    }

    #[test]
    fn cosine_similarity_requires_matching_lengths_and_non_zero_norms() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0]), None);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), Some(1.0));
    }
}
