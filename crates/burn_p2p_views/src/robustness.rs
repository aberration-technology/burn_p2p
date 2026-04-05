use burn_p2p_core::{
    CanaryEvalReport, CohortRobustnessReport, PeerId, RejectionReason, RobustnessAlert,
    RobustnessPolicy, RobustnessPreset, TrustScore,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Count of rejected updates for one canonical reason.
pub struct RobustnessReasonCountView {
    /// Rejection reason.
    pub reason: RejectionReason,
    /// Count of updates rejected for the reason.
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One trust-score point shown in operator or participant panels.
pub struct TrustScorePointView {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Current trust score.
    pub score: f64,
    /// Whether the peer is quarantined.
    pub quarantined: bool,
    /// Whether the peer is currently escalated to a ban recommendation.
    pub ban_recommended: bool,
    /// Timestamp of the score sample.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One quarantined peer entry shown in robustness panels.
pub struct QuarantinedPeerView {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Whether reducer duties remain allowed.
    pub reducer_eligible: bool,
    /// Whether validator duties remain allowed.
    pub validator_eligible: bool,
    /// Whether the runtime recommends operator escalation to a ban.
    pub ban_recommended: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One canary-regression row shown in robustness panels.
pub struct CanaryRegressionView {
    /// Candidate head identifier.
    pub candidate_head_id: burn_p2p_core::HeadId,
    /// Regression margin observed on the canary set.
    pub regression_margin: f64,
    /// Whether a backdoor trigger was detected.
    pub detected_backdoor_trigger: bool,
    /// Evaluation timestamp.
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Presentation-oriented robustness panel payload.
pub struct RobustnessPanelView {
    /// Active preset label.
    pub preset: RobustnessPreset,
    /// Active policy snapshot.
    pub policy: RobustnessPolicy,
    /// Rejection counts grouped by reason.
    pub rejection_reasons: Vec<RobustnessReasonCountView>,
    /// Trust score timeline points.
    pub trust_scores: Vec<TrustScorePointView>,
    /// Quarantined peers in the current snapshot.
    pub quarantined_peers: Vec<QuarantinedPeerView>,
    /// Canary regressions for the current candidate set.
    pub canary_regressions: Vec<CanaryRegressionView>,
    /// Latest robustness alerts.
    pub alerts: Vec<RobustnessAlert>,
}

impl RobustnessPanelView {
    /// Builds a robustness panel from cohort, trust, and canary state.
    pub fn from_reports(
        policy: RobustnessPolicy,
        cohort: &CohortRobustnessReport,
        trust_scores: &[TrustScore],
        canary_reports: &[CanaryEvalReport],
    ) -> Self {
        let mut alerts = cohort.alerts.clone();
        alerts.sort_by(|left, right| {
            right
                .severity
                .cmp(&left.severity)
                .then_with(|| right.emitted_at.cmp(&left.emitted_at))
                .then_with(|| left.message.cmp(&right.message))
        });
        let rejection_reasons = cohort
            .rejection_reasons
            .iter()
            .map(|(reason, count)| RobustnessReasonCountView {
                reason: reason.clone(),
                count: *count,
            })
            .collect::<Vec<_>>();

        let mut sorted_trust_scores = trust_scores.to_vec();
        sorted_trust_scores.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.peer_id.cmp(&right.peer_id))
        });
        let trust_scores_view = sorted_trust_scores
            .iter()
            .map(|score| TrustScorePointView {
                peer_id: score.peer_id.clone(),
                score: score.score,
                quarantined: score.quarantined,
                ban_recommended: score.ban_recommended,
                updated_at: score.updated_at,
            })
            .collect::<Vec<_>>();

        let mut quarantined_peers = sorted_trust_scores
            .iter()
            .filter(|score| score.quarantined)
            .map(|score| QuarantinedPeerView {
                peer_id: score.peer_id.clone(),
                reducer_eligible: score.reducer_eligible,
                validator_eligible: score.validator_eligible,
                ban_recommended: score.ban_recommended,
            })
            .collect::<Vec<_>>();
        quarantined_peers.sort_by(|left, right| {
            right
                .ban_recommended
                .cmp(&left.ban_recommended)
                .then_with(|| left.peer_id.cmp(&right.peer_id))
        });

        let mut canary_regressions = canary_reports
            .iter()
            .filter(|report| !report.accepted || report.detected_backdoor_trigger)
            .map(|report| CanaryRegressionView {
                candidate_head_id: report.candidate_head_id.clone(),
                regression_margin: report.regression_margin,
                detected_backdoor_trigger: report.detected_backdoor_trigger,
                evaluated_at: report.evaluated_at,
            })
            .collect::<Vec<_>>();
        canary_regressions.sort_by(|left, right| {
            right
                .evaluated_at
                .cmp(&left.evaluated_at)
                .then_with(|| left.candidate_head_id.cmp(&right.candidate_head_id))
        });

        Self {
            preset: policy.preset.clone(),
            policy,
            rejection_reasons,
            trust_scores: trust_scores_view,
            quarantined_peers,
            canary_regressions,
            alerts,
        }
    }
}
