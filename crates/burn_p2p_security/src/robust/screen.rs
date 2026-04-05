use burn_p2p_core::{ClippingPolicy, RejectionReason, RobustnessPolicy};

use crate::robust::policy::RobustUpdateObservation;

/// Returns a deterministic hard rejection reason when one applies.
pub fn hard_rejection_reason(
    policy: &RobustnessPolicy,
    observation: &RobustUpdateObservation,
) -> Option<RejectionReason> {
    if observation.quarantined {
        return Some(RejectionReason::QuarantinedPrincipal);
    }
    if !observation.schema_matches {
        return Some(if policy.hard_reject_policy.strict_schema_compatibility {
            RejectionReason::SchemaMismatch
        } else if policy.hard_reject_policy.reject_malformed_updates {
            RejectionReason::Malformed
        } else {
            RejectionReason::SchemaMismatch
        });
    }
    if observation.base_head_id != observation.expected_base_head_id {
        return Some(RejectionReason::BaseHeadMismatch);
    }
    if observation.revision_id != observation.expected_revision_id {
        return Some(RejectionReason::RevisionMismatch);
    }
    if observation.dataset_view_id != observation.expected_dataset_view_id {
        return Some(RejectionReason::DatasetViewMismatch);
    }
    if policy.hard_reject_policy.reject_replays && observation.duplicate {
        return Some(RejectionReason::Replay);
    }
    if policy.hard_reject_policy.require_lease_binding
        && (observation.lease_id.is_none() || observation.lease_id != observation.expected_lease_id)
    {
        return Some(RejectionReason::LeaseMismatch);
    }
    if observation.feature_sketch.staleness_windows > policy.hard_reject_policy.max_stale_windows {
        return Some(RejectionReason::TooStale);
    }
    if observation.feature_sketch.non_finite_tensor_count > 0
        || !observation.feature_sketch.global_norm.is_finite()
    {
        return Some(RejectionReason::NanInf);
    }
    if let Some(max_update_bytes) = policy.hard_reject_policy.max_update_bytes
        && observation.feature_sketch.artifact_size_bytes > max_update_bytes
    {
        return Some(RejectionReason::NormTooLarge);
    }
    if let Some(canary_report) = observation.canary_report.as_ref()
        && policy.validator_canary_policy.block_on_regression
        && (!canary_report.accepted || canary_report.detected_backdoor_trigger)
    {
        return Some(RejectionReason::CanaryRegression);
    }
    None
}

/// Applies configured norm clipping and returns the effective norm.
pub fn effective_norm(policy: &ClippingPolicy, observation: &RobustUpdateObservation) -> f64 {
    let mut effective = observation.feature_sketch.global_norm;
    if effective > policy.global_norm_cap {
        effective = policy.global_norm_cap;
    }
    if let Some(per_layer_cap) = policy.per_layer_norm_cap {
        let layer_peak = observation
            .feature_sketch
            .per_layer_norms
            .values()
            .copied()
            .fold(0.0_f64, f64::max);
        if layer_peak > per_layer_cap && layer_peak > 0.0 {
            effective *= per_layer_cap / layer_peak;
        }
    }
    if policy.adaptive_norm_bands
        && !observation.feature_sketch.per_layer_norms.is_empty()
        && effective > 0.0
    {
        let mut norms = observation
            .feature_sketch
            .per_layer_norms
            .values()
            .copied()
            .filter(|value| value.is_finite() && *value > 0.0)
            .collect::<Vec<_>>();
        norms.sort_by(|left, right| left.total_cmp(right));
        if let Some(median) = norms.get(norms.len() / 2).copied() {
            let adaptive_cap = median * (norms.len() as f64).sqrt() * 2.0;
            if adaptive_cap.is_finite() && adaptive_cap > 0.0 {
                effective = effective.min(adaptive_cap);
            }
        }
    }
    effective
}

/// Computes a cheap screening score from similarity and trust features.
pub fn screening_score(policy: &RobustnessPolicy, observation: &RobustUpdateObservation) -> f64 {
    let features = &observation.feature_sketch;
    let mut score = 0.0_f64;

    if let Some(cosine) = features.cosine_to_reference
        && cosine < policy.screening_policy.minimum_cosine_similarity
    {
        score += (policy.screening_policy.minimum_cosine_similarity - cosine) * 8.0;
    }
    if let Some(sign_fraction) = features.sign_agreement_fraction
        && sign_fraction < policy.screening_policy.minimum_sign_agreement_fraction
    {
        score += (policy.screening_policy.minimum_sign_agreement_fraction - sign_fraction) * 6.0;
    }
    if let Some(distance) = features.neighbor_distance
        && distance > policy.screening_policy.maximum_neighbor_distance
    {
        score += distance - policy.screening_policy.maximum_neighbor_distance;
    }
    if let Some(delta) = features.canary_loss_delta
        && delta > policy.screening_policy.maximum_canary_loss_delta
    {
        score += (delta - policy.screening_policy.maximum_canary_loss_delta) * 10.0;
    }
    if let Some(historical) = features.historical_deviation_score
        && policy.screening_policy.use_historical_centroid
    {
        score += historical;
    }

    score
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::RobustnessPolicy;

    use crate::robust::{
        FeatureLayer, extract_feature_sketch,
        policy::RobustUpdateObservation,
        screen::{effective_norm, hard_rejection_reason, screening_score},
    };

    fn observation() -> RobustUpdateObservation {
        RobustUpdateObservation {
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            base_head_id: burn_p2p_core::HeadId::new("head-1"),
            expected_base_head_id: burn_p2p_core::HeadId::new("head-1"),
            revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            expected_revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            expected_dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            lease_id: Some(burn_p2p_core::LeaseId::new("lease-1")),
            expected_lease_id: Some(burn_p2p_core::LeaseId::new("lease-1")),
            schema_matches: true,
            duplicate: false,
            quarantined: false,
            consecutive_rejections: 0,
            current_trust_score: 0.0,
            canary_report: None,
            feature_sketch: extract_feature_sketch(
                &[1.0, 2.0, -1.0, 0.5],
                Some(&[1.0, 1.8, -0.9, 0.4]),
                &[FeatureLayer::new("layer", 0, 4)],
                4,
                0,
                10,
                Some(0.0),
            ),
        }
    }

    #[test]
    fn hard_rejection_detects_replay_and_quarantine() {
        let policy = RobustnessPolicy::balanced();
        let mut replay = observation();
        replay.duplicate = true;
        assert_eq!(
            hard_rejection_reason(&policy, &replay),
            Some(burn_p2p_core::RejectionReason::Replay)
        );

        let mut quarantined = observation();
        quarantined.quarantined = true;
        assert_eq!(
            hard_rejection_reason(&policy, &quarantined),
            Some(burn_p2p_core::RejectionReason::QuarantinedPrincipal)
        );

        let mut schema_mismatch_policy = policy.clone();
        schema_mismatch_policy
            .hard_reject_policy
            .strict_schema_compatibility = true;
        let mut schema_mismatch = observation();
        schema_mismatch.schema_matches = false;
        assert_eq!(
            hard_rejection_reason(&schema_mismatch_policy, &schema_mismatch),
            Some(burn_p2p_core::RejectionReason::SchemaMismatch)
        );
    }

    #[test]
    fn screening_score_and_effective_norm_reflect_similarity_and_clipping() {
        let policy = RobustnessPolicy::strict();
        let mut suspicious = observation();
        suspicious.feature_sketch.cosine_to_reference = Some(-0.2);
        suspicious.feature_sketch.sign_agreement_fraction = Some(0.1);
        suspicious.feature_sketch.neighbor_distance = Some(4.5);
        suspicious.feature_sketch.global_norm = 10.0;

        assert!(screening_score(&policy, &suspicious) > 3.0);
        assert!(effective_norm(&policy.clipping_policy, &suspicious) <= 4.0);
    }

    #[test]
    fn adaptive_norm_bands_clip_layer_spikes_even_without_per_layer_cap() {
        let mut policy = RobustnessPolicy::balanced();
        policy.clipping_policy.global_norm_cap = 32.0;
        policy.clipping_policy.per_layer_norm_cap = None;
        policy.clipping_policy.adaptive_norm_bands = true;
        let mut suspicious = observation();
        suspicious.feature_sketch.global_norm = 12.0;
        suspicious.feature_sketch.per_layer_norms = std::collections::BTreeMap::from([
            (String::from("small-a"), 0.5),
            (String::from("small-b"), 0.6),
            (String::from("spike"), 6.0),
        ]);

        assert!(effective_norm(&policy.clipping_policy, &suspicious) < 12.0);
    }
}
