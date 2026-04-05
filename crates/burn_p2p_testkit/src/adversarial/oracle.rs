/// Returns cosine similarity between the aggregate and the trusted reference.
pub fn aggregate_cosine_to_reference(reference: &[f64], aggregate: &[f64]) -> f64 {
    let dot = reference
        .iter()
        .zip(aggregate.iter())
        .map(|(left, right)| left * right)
        .sum::<f64>();
    let reference_norm = reference
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    let aggregate_norm = aggregate
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if reference_norm == 0.0 || aggregate_norm == 0.0 {
        0.0
    } else {
        (dot / (reference_norm * aggregate_norm)).clamp(-1.0, 1.0)
    }
}

/// Returns whether the attack still succeeded after defense.
pub fn attack_success(reference: &[f64], aggregate: &[f64], minimum_cosine: f64) -> bool {
    aggregate_cosine_to_reference(reference, aggregate) < minimum_cosine
}

/// Returns quarantine precision and recall when oracle labels are available.
pub fn quarantine_precision_recall(quarantined: &[bool], malicious: &[bool]) -> (f64, f64) {
    let mut true_positive = 0_u32;
    let mut false_positive = 0_u32;
    let mut false_negative = 0_u32;

    for (is_quarantined, is_malicious) in quarantined.iter().zip(malicious.iter()) {
        match (*is_quarantined, *is_malicious) {
            (true, true) => true_positive += 1,
            (true, false) => false_positive += 1,
            (false, true) => false_negative += 1,
            (false, false) => {}
        }
    }

    let precision = if true_positive + false_positive == 0 {
        1.0
    } else {
        true_positive as f64 / (true_positive + false_positive) as f64
    };
    let recall = if true_positive + false_negative == 0 {
        1.0
    } else {
        true_positive as f64 / (true_positive + false_negative) as f64
    };

    (precision, recall)
}
