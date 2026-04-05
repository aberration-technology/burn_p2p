use burn_p2p_core::{
    AggregationStrategy, CohortFilterStrategy, RobustnessPolicy, UpdateFeatureSketch,
};

#[derive(Clone, Debug, PartialEq)]
/// Per-update cohort filter outcome derived from feature sketches.
pub struct CohortWeightDecision {
    /// Whether the update remains in the screened cohort.
    pub accepted: bool,
    /// Relative weight multiplier applied after the filter stage.
    pub weight_scale: f64,
}

/// Aggregates dense update vectors according to the configured robustness strategy.
pub fn aggregate_updates_with_policy(
    policy: &RobustnessPolicy,
    vectors: &[Vec<f64>],
    weights: &[f64],
) -> Vec<f64> {
    match policy.aggregation_policy.strategy {
        AggregationStrategy::WeightedMean | AggregationStrategy::TrustedReferenceWeightedMean => {
            clipped_weighted_mean(vectors, weights, f64::INFINITY)
        }
        AggregationStrategy::ClippedWeightedMean => {
            clipped_weighted_mean(vectors, weights, policy.clipping_policy.global_norm_cap)
        }
        AggregationStrategy::TrimmedMean => {
            trimmed_mean(vectors, policy.cohort_filter_policy.trim_fraction)
        }
        AggregationStrategy::Median => coordinate_median(vectors),
    }
}

/// Applies the configured cohort filter policy to update sketches and returns one
/// acceptance/weight decision per sketch in order.
pub fn filter_update_sketches_with_policy(
    policy: &RobustnessPolicy,
    sketches: &[UpdateFeatureSketch],
    weights: &[f64],
) -> Vec<CohortWeightDecision> {
    let minimum_survivors = usize::from(policy.cohort_filter_policy.minimum_survivor_count.max(1));
    if sketches.is_empty() {
        return Vec::new();
    }
    if sketches.len() <= minimum_survivors
        || matches!(
            policy.cohort_filter_policy.strategy,
            CohortFilterStrategy::None
        )
    {
        return vec![
            CohortWeightDecision {
                accepted: true,
                weight_scale: 1.0,
            };
            sketches.len()
        ];
    }

    let vectors = sketches
        .iter()
        .map(sketch_projection_vector)
        .collect::<Vec<_>>();
    let base_weights = normalized_weights(weights, vectors.len());
    match policy.cohort_filter_policy.strategy {
        CohortFilterStrategy::None => vec![
            CohortWeightDecision {
                accepted: true,
                weight_scale: 1.0,
            };
            vectors.len()
        ],
        CohortFilterStrategy::SimilarityAware
        | CohortFilterStrategy::TrustedReferenceWeightedMean
        | CohortFilterStrategy::ClippedMean
        | CohortFilterStrategy::TrimmedMean
        | CohortFilterStrategy::Median => {
            centered_distance_filter(policy, sketches, &vectors, &base_weights)
        }
        CohortFilterStrategy::Krum
        | CohortFilterStrategy::MultiKrum
        | CohortFilterStrategy::Bulyan
        | CohortFilterStrategy::MultiBulyan => krum_family_filter(policy, sketches, &vectors),
    }
}

/// Computes a clipped weighted mean over dense vectors.
pub fn clipped_weighted_mean(vectors: &[Vec<f64>], weights: &[f64], clip: f64) -> Vec<f64> {
    if vectors.is_empty() {
        return Vec::new();
    }
    let width = vectors[0].len();
    let mut output = vec![0.0; width];
    let mut weight_sum = 0.0_f64;

    for (vector, weight) in vectors.iter().zip(weights.iter().copied()) {
        if vector.len() != width || weight <= 0.0 {
            continue;
        }
        let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
        let scale = if clip.is_finite() && norm > 0.0 && norm > clip {
            clip / norm
        } else {
            1.0
        };
        for (slot, value) in output.iter_mut().zip(vector) {
            *slot += value * scale * weight;
        }
        weight_sum += weight;
    }

    if weight_sum > 0.0 {
        for slot in &mut output {
            *slot /= weight_sum;
        }
    }

    output
}

/// Computes a coordinate-wise trimmed mean.
pub fn trimmed_mean(vectors: &[Vec<f64>], trim_fraction: f64) -> Vec<f64> {
    if vectors.is_empty() {
        return Vec::new();
    }
    let width = vectors[0].len();
    let trim_each_side = ((vectors.len() as f64) * trim_fraction).floor() as usize;
    let mut output = vec![0.0; width];

    for dimension in 0..width {
        let mut values = vectors
            .iter()
            .filter(|vector| vector.len() == width)
            .map(|vector| vector[dimension])
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.total_cmp(right));
        let start = trim_each_side.min(values.len());
        let end = values.len().saturating_sub(trim_each_side);
        let kept = if start < end {
            &values[start..end]
        } else {
            &values[..]
        };
        output[dimension] = kept.iter().sum::<f64>() / kept.len().max(1) as f64;
    }

    output
}

/// Computes a coordinate-wise median.
pub fn coordinate_median(vectors: &[Vec<f64>]) -> Vec<f64> {
    if vectors.is_empty() {
        return Vec::new();
    }
    let width = vectors[0].len();
    let mut output = vec![0.0; width];

    for dimension in 0..width {
        let mut values = vectors
            .iter()
            .filter(|vector| vector.len() == width)
            .map(|vector| vector[dimension])
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.total_cmp(right));
        let mid = values.len() / 2;
        output[dimension] = if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        };
    }

    output
}

/// Returns the indices selected by Multi-Krum style scoring.
pub fn multi_krum_indices(
    vectors: &[Vec<f64>],
    assumed_byzantine_count: usize,
    selected_count: usize,
) -> Vec<usize> {
    if vectors.is_empty() {
        return Vec::new();
    }
    let neighbor_count = vectors
        .len()
        .saturating_sub(assumed_byzantine_count + 2)
        .max(1);
    let mut scores = vectors
        .iter()
        .enumerate()
        .map(|(index, vector)| {
            let mut distances = vectors
                .iter()
                .enumerate()
                .filter(|(candidate, other)| *candidate != index && other.len() == vector.len())
                .map(|(_, other)| squared_distance(vector, other))
                .collect::<Vec<_>>();
            distances.sort_by(|left, right| left.total_cmp(right));
            let score = distances.into_iter().take(neighbor_count).sum::<f64>();
            (index, score)
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scores
        .into_iter()
        .take(selected_count.min(vectors.len()))
        .map(|(index, _)| index)
        .collect()
}

fn squared_distance(left: &[f64], right: &[f64]) -> f64 {
    left.iter()
        .zip(right.iter())
        .map(|(a, b)| {
            let delta = a - b;
            delta * delta
        })
        .sum::<f64>()
}

fn normalized_weights(weights: &[f64], len: usize) -> Vec<f64> {
    (0..len)
        .map(|index| weights.get(index).copied().unwrap_or(1.0).max(0.01))
        .collect()
}

fn sketch_projection_vector(sketch: &UpdateFeatureSketch) -> Vec<f64> {
    if !sketch.random_projection.is_empty() {
        return sketch.random_projection.clone();
    }
    let mut values = vec![
        sketch.global_norm,
        sketch.cosine_to_reference.unwrap_or(0.0),
        sketch.sign_agreement_fraction.unwrap_or(0.0),
        sketch.canary_loss_delta.unwrap_or(0.0),
        sketch.historical_deviation_score.unwrap_or(0.0),
        sketch.neighbor_distance.unwrap_or(0.0),
        sketch.staleness_windows as f64,
        (sketch.receive_delay_ms as f64) / 1_000.0,
    ];
    values.extend(sketch.sign_projection.iter().map(|value| *value as f64));
    values
}

fn centered_distance_filter(
    policy: &RobustnessPolicy,
    sketches: &[UpdateFeatureSketch],
    vectors: &[Vec<f64>],
    weights: &[f64],
) -> Vec<CohortWeightDecision> {
    let minimum_survivors =
        usize::from(policy.cohort_filter_policy.minimum_survivor_count.max(1)).min(vectors.len());
    let center = match policy.cohort_filter_policy.strategy {
        CohortFilterStrategy::TrimmedMean => {
            trimmed_mean(vectors, policy.cohort_filter_policy.trim_fraction)
        }
        CohortFilterStrategy::Median => coordinate_median(vectors),
        CohortFilterStrategy::SimilarityAware
        | CohortFilterStrategy::TrustedReferenceWeightedMean
        | CohortFilterStrategy::ClippedMean
        | CohortFilterStrategy::None
        | CohortFilterStrategy::Krum
        | CohortFilterStrategy::MultiKrum
        | CohortFilterStrategy::Bulyan
        | CohortFilterStrategy::MultiBulyan => {
            clipped_weighted_mean(vectors, weights, policy.clipping_policy.global_norm_cap)
        }
    };

    let mut ranked = vectors
        .iter()
        .enumerate()
        .map(|(index, vector)| {
            let distance = squared_distance(vector, &center).sqrt()
                + (sketches[index].neighbor_distance.unwrap_or_default() * 0.25);
            let reference_penalty = match policy.cohort_filter_policy.strategy {
                CohortFilterStrategy::TrustedReferenceWeightedMean => {
                    1.0 - sketches[index]
                        .cosine_to_reference
                        .unwrap_or(0.0)
                        .clamp(-1.0, 1.0)
                }
                _ => 0.0,
            };
            (index, distance + reference_penalty)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    let max_score = ranked.last().map(|(_, score)| *score).unwrap_or_default();
    let min_score = ranked.first().map(|(_, score)| *score).unwrap_or_default();
    if matches!(
        policy.cohort_filter_policy.strategy,
        CohortFilterStrategy::SimilarityAware | CohortFilterStrategy::ClippedMean
    ) && (max_score <= policy.cohort_filter_policy.maximum_pairwise_distance * 2.0
        || (max_score - min_score) <= policy.cohort_filter_policy.maximum_pairwise_distance)
    {
        return vec![
            CohortWeightDecision {
                accepted: true,
                weight_scale: 1.0,
            };
            vectors.len()
        ];
    }

    let mut keep = vec![false; vectors.len()];
    for (index, score) in &ranked {
        if *score <= policy.cohort_filter_policy.maximum_pairwise_distance {
            keep[*index] = true;
        }
    }
    if keep.iter().filter(|accepted| **accepted).count() < minimum_survivors {
        for (index, _) in ranked.iter().take(minimum_survivors) {
            keep[*index] = true;
        }
    }

    keep.into_iter()
        .enumerate()
        .map(|(index, accepted)| {
            if !accepted {
                return CohortWeightDecision {
                    accepted: false,
                    weight_scale: 0.0,
                };
            }
            let distance = ranked
                .iter()
                .find(|(candidate, _)| *candidate == index)
                .map(|(_, score)| *score)
                .unwrap_or_default();
            let mut weight_scale =
                if distance <= policy.cohort_filter_policy.maximum_pairwise_distance * 0.5 {
                    1.0
                } else {
                    (1.0 / (1.0 + distance)).clamp(0.25, 1.0)
                };
            if matches!(
                policy.cohort_filter_policy.strategy,
                CohortFilterStrategy::TrustedReferenceWeightedMean
            ) {
                let reference_scale = sketches[index]
                    .cosine_to_reference
                    .unwrap_or(0.5)
                    .clamp(0.1, 1.0);
                if reference_scale < 0.95 {
                    weight_scale *= reference_scale;
                }
            }
            CohortWeightDecision {
                accepted: true,
                weight_scale,
            }
        })
        .collect()
}

fn krum_family_filter(
    policy: &RobustnessPolicy,
    sketches: &[UpdateFeatureSketch],
    vectors: &[Vec<f64>],
) -> Vec<CohortWeightDecision> {
    let assumed = ((vectors.len() as f64) * policy.assumed_byzantine_fraction)
        .ceil()
        .max(0.0) as usize;
    let assumed = assumed.min(vectors.len().saturating_sub(2));
    let minimum_survivors =
        usize::from(policy.cohort_filter_policy.minimum_survivor_count.max(1)).min(vectors.len());
    let selected_count = match policy.cohort_filter_policy.strategy {
        CohortFilterStrategy::Krum => 1,
        CohortFilterStrategy::MultiKrum => vectors
            .len()
            .saturating_sub(assumed + 2)
            .max(minimum_survivors),
        CohortFilterStrategy::Bulyan | CohortFilterStrategy::MultiBulyan => vectors
            .len()
            .saturating_sub(assumed.saturating_mul(2).max(assumed + 2))
            .max(minimum_survivors),
        _ => minimum_survivors,
    };
    let mut selected = multi_krum_indices(vectors, assumed, selected_count.max(minimum_survivors));
    if matches!(
        policy.cohort_filter_policy.strategy,
        CohortFilterStrategy::Bulyan | CohortFilterStrategy::MultiBulyan
    ) && !selected.is_empty()
    {
        let selected_vectors = selected
            .iter()
            .map(|index| vectors[*index].clone())
            .collect::<Vec<_>>();
        let median = coordinate_median(&selected_vectors);
        selected.sort_by(|left, right| {
            let left_score = squared_distance(&vectors[*left], &median)
                + sketches[*left].neighbor_distance.unwrap_or_default();
            let right_score = squared_distance(&vectors[*right], &median)
                + sketches[*right].neighbor_distance.unwrap_or_default();
            left_score
                .total_cmp(&right_score)
                .then_with(|| left.cmp(right))
        });
        selected.truncate(minimum_survivors.max(selected.len() / 2));
    }
    let selected_set = selected
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    (0..vectors.len())
        .map(|index| CohortWeightDecision {
            accepted: selected_set.contains(&index),
            weight_scale: if selected_set.contains(&index) {
                1.0
            } else {
                0.0
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{
        AggregationStrategy, CohortFilterStrategy, RobustnessPolicy, UpdateFeatureSketch,
    };

    use super::{
        aggregate_updates_with_policy, clipped_weighted_mean, coordinate_median,
        filter_update_sketches_with_policy, multi_krum_indices, trimmed_mean,
    };

    #[test]
    fn trimmed_mean_and_median_bound_one_large_outlier() {
        let vectors = vec![
            vec![1.0, 1.0],
            vec![1.1, 0.9],
            vec![0.95, 1.05],
            vec![50.0, 50.0],
        ];

        let trimmed = trimmed_mean(&vectors, 0.25);
        let median = coordinate_median(&vectors);

        assert!(trimmed[0] < 2.0);
        assert!(median[0] < 2.0);
    }

    #[test]
    fn multi_krum_discards_a_clear_outlier() {
        let vectors = vec![
            vec![1.0, 1.0],
            vec![1.1, 0.95],
            vec![0.9, 1.05],
            vec![25.0, 25.0],
        ];

        let indices = multi_krum_indices(&vectors, 1, 2);
        assert!(!indices.contains(&3));
    }

    #[test]
    fn policy_aggregation_uses_clipped_weighted_mean_by_default() {
        let policy = RobustnessPolicy::balanced();
        let vectors = vec![vec![1.0, 1.0], vec![1.1, 0.9], vec![20.0, 20.0]];
        let weights = vec![1.0, 1.0, 1.0];

        let aggregate = aggregate_updates_with_policy(&policy, &vectors, &weights);
        assert!(aggregate[0] < 10.0);

        let mut median_policy = policy.clone();
        median_policy.aggregation_policy.strategy = AggregationStrategy::Median;
        let median = aggregate_updates_with_policy(&median_policy, &vectors, &weights);
        assert!(median[0] < 2.0);
    }

    #[test]
    fn sketch_filter_rejects_clear_outlier_under_median_and_krum_modes() {
        let sketches = vec![
            UpdateFeatureSketch {
                random_projection: vec![1.0, 1.0],
                ..UpdateFeatureSketch::default()
            },
            UpdateFeatureSketch {
                random_projection: vec![1.1, 0.9],
                ..UpdateFeatureSketch::default()
            },
            UpdateFeatureSketch {
                random_projection: vec![25.0, 25.0],
                neighbor_distance: Some(8.0),
                ..UpdateFeatureSketch::default()
            },
        ];
        let weights = vec![1.0, 1.0, 1.0];

        let mut median_policy = RobustnessPolicy::balanced();
        median_policy.cohort_filter_policy.strategy = CohortFilterStrategy::Median;
        let median = filter_update_sketches_with_policy(&median_policy, &sketches, &weights);
        assert!(median[0].accepted);
        assert!(median[1].accepted);
        assert!(!median[2].accepted);

        let mut krum_policy = RobustnessPolicy::strict();
        krum_policy.cohort_filter_policy.strategy = CohortFilterStrategy::MultiKrum;
        let krum = filter_update_sketches_with_policy(&krum_policy, &sketches, &weights);
        assert!(krum[0].accepted || krum[1].accepted);
        assert!(!krum[2].accepted);
    }

    #[test]
    fn clipped_weighted_mean_respects_zero_or_invalid_weights() {
        let aggregate = clipped_weighted_mean(&[vec![1.0], vec![9.0]], &[1.0, 0.0], 100.0);
        assert_eq!(aggregate, vec![1.0]);
    }
}
