use burn_p2p_core::{QuarantinePolicy, RobustnessDecision};

/// Determines whether the peer should move into quarantine after a decision.
pub fn should_quarantine(
    policy: &QuarantinePolicy,
    consecutive_rejections: u16,
    decision: &RobustnessDecision,
) -> bool {
    if !policy.automatic_quarantine {
        return false;
    }
    if decision.accepted {
        return false;
    }
    consecutive_rejections.saturating_add(1) >= policy.quarantine_after_consecutive_rejections
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{QuarantinePolicy, RejectionReason, RobustnessDecision};

    use super::should_quarantine;

    #[test]
    fn quarantine_triggers_after_configured_rejection_streak() {
        let policy = QuarantinePolicy::default();
        let decision = RobustnessDecision {
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            accepted: false,
            hard_rejected: true,
            downweighted: false,
            quarantined: false,
            rejection_reason: Some(RejectionReason::Replay),
            screen_score: 5.0,
            effective_weight: 0.0,
            effective_norm: 0.0,
            trust_score: None,
        };

        assert!(!should_quarantine(&policy, 0, &decision));
        assert!(should_quarantine(&policy, 1, &decision));
    }
}
