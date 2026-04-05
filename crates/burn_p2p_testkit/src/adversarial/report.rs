use std::collections::BTreeMap;

use burn_p2p_core::{
    CohortRobustnessReport, PeerId, RejectionReason, RobustnessPolicy, RobustnessPreset,
};
use serde::{Deserialize, Serialize};

use super::AdversarialAttack;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One per-peer adversarial decision row.
pub struct AdversarialDecisionRecord {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Whether the peer was malicious in the oracle.
    pub malicious: bool,
    /// Whether the update was accepted.
    pub accepted: bool,
    /// Whether the update was downweighted.
    pub downweighted: bool,
    /// Whether the peer was quarantined.
    pub quarantined: bool,
    /// Rejection reason, when present.
    pub rejection_reason: Option<RejectionReason>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Scenario-level adversarial robustness summary.
pub struct AdversarialScenarioReport {
    /// Attack pattern under evaluation.
    pub attack: AdversarialAttack,
    /// Active robustness preset.
    pub preset: RobustnessPreset,
    /// Peer count in the scenario.
    pub peer_count: usize,
    /// Malicious peer fraction in the scenario.
    pub malicious_fraction: f64,
    /// Final defended aggregate cosine to the trusted reference.
    pub final_canonical_validation_score: f64,
    /// Attack success rate under the defended aggregate.
    pub attack_success_rate: f64,
    /// Fraction of malicious updates accepted.
    pub malicious_update_acceptance_rate: f64,
    /// Fraction of benign updates incorrectly rejected.
    pub benign_false_rejection_rate: f64,
    /// Fraction of benign updates incorrectly downweighted.
    pub benign_false_downweight_rate: f64,
    /// Canary regression rate among malicious candidates in the scenario.
    pub canary_regression_rate: f64,
    /// Synthetic head-promotion latency delta.
    pub head_promotion_latency_delta_ms: u64,
    /// Synthetic reducer CPU overhead.
    pub reducer_cpu_overhead_ms: u64,
    /// Synthetic validator CPU overhead.
    pub validator_cpu_overhead_ms: u64,
    /// Approximate bytes per accepted update.
    pub bytes_per_accepted_update: f64,
    /// Quarantine precision in the oracle.
    pub quarantine_precision: f64,
    /// Quarantine recall in the oracle.
    pub quarantine_recall: f64,
    /// Rejection counts by reason.
    pub rejection_reasons: BTreeMap<RejectionReason, u32>,
    /// Per-peer decision rows.
    pub decisions: Vec<AdversarialDecisionRecord>,
    /// Cohort-level robustness summary.
    pub cohort_report: CohortRobustnessReport,
    /// Snapshot of the active policy for reproducibility.
    pub policy: RobustnessPolicy,
}
