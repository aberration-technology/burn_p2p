use burn_p2p_core::{RejectionReason, ReputationPolicy, TrustScore};
use chrono::{DateTime, Utc};

/// Input bundle used to advance one peer trust score.
pub struct TrustUpdateInput<'a> {
    /// Active reputation policy.
    pub policy: &'a ReputationPolicy,
    /// Peer being updated.
    pub peer_id: burn_p2p_core::PeerId,
    /// Current score before the update.
    pub current_score: f64,
    /// Whether the update was accepted.
    pub accepted: bool,
    /// Whether the update was accepted but downweighted.
    pub downweighted: bool,
    /// Screening score attached to the decision.
    pub screen_score: f64,
    /// Optional rejection reason.
    pub rejection_reason: Option<&'a RejectionReason>,
    /// Whether the peer is quarantined after the decision.
    pub quarantined: bool,
    /// Timestamp of the score update.
    pub updated_at: DateTime<Utc>,
}

/// Applies the rolling trust update used by the robustness engine.
pub fn updated_trust_score(input: TrustUpdateInput<'_>) -> TrustScore {
    let mut score = input.current_score * input.policy.suspicion_decay;
    if input.accepted {
        score += (1.0 - input.screen_score.min(1.0)).max(0.0);
        if input.downweighted {
            score -= input.policy.rejection_weight * 0.25;
        }
    } else {
        score -= input.policy.rejection_weight + input.screen_score.max(0.0) * 0.25;
    }

    if matches!(
        input.rejection_reason,
        Some(RejectionReason::ReplicaDisagreement)
    ) {
        score -= input.policy.disagreement_penalty;
    }
    if matches!(
        input.rejection_reason,
        Some(RejectionReason::Replay | RejectionReason::TooStale)
    ) {
        score -= input.policy.delayed_attack_penalty;
    }

    TrustScore {
        peer_id: input.peer_id,
        score,
        reducer_eligible: score >= input.policy.reducer_demote_threshold,
        validator_eligible: score >= input.policy.validator_demote_threshold,
        quarantined: input.quarantined,
        ban_recommended: false,
        updated_at: input.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{RejectionReason, ReputationPolicy};
    use chrono::Utc;

    use super::{TrustUpdateInput, updated_trust_score};

    #[test]
    fn trust_score_penalizes_replay_more_than_clean_acceptance() {
        let policy = ReputationPolicy::default();
        let accepted = updated_trust_score(TrustUpdateInput {
            policy: &policy,
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            current_score: 0.0,
            accepted: true,
            downweighted: false,
            screen_score: 0.2,
            rejection_reason: None,
            quarantined: false,
            updated_at: Utc::now(),
        });
        let replay = updated_trust_score(TrustUpdateInput {
            policy: &policy,
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            current_score: accepted.score,
            accepted: false,
            downweighted: false,
            screen_score: 4.0,
            rejection_reason: Some(&RejectionReason::Replay),
            quarantined: true,
            updated_at: Utc::now(),
        });

        assert!(accepted.score > replay.score);
        assert!(replay.quarantined);
    }
}
