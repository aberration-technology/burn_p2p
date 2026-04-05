use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Named robustness presets exposed in revision policy and UI surfaces.
pub enum RobustnessPreset {
    /// Minimal hardening for trusted deployments.
    Minimal,
    /// Recommended default for mixed but bounded-adversary networks.
    Balanced,
    /// Stricter policy for public or higher-risk deployments.
    Strict,
    /// Research preset with heavier aggregation and telemetry.
    Research,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates supported cohort filtering strategies.
pub enum CohortFilterStrategy {
    /// Accept all updates after hard-reject and clipping phases.
    None,
    /// Screen by local similarity and bounded influence.
    SimilarityAware,
    /// Use a trusted-reference weighted filter.
    TrustedReferenceWeightedMean,
    /// Use a clipped mean over the screened cohort.
    ClippedMean,
    /// Use a coordinate-wise trimmed mean.
    TrimmedMean,
    /// Use a coordinate-wise median.
    Median,
    /// Use Krum for small cohorts.
    Krum,
    /// Use Multi-Krum for small cohorts.
    MultiKrum,
    /// Use Bulyan for small cohorts.
    Bulyan,
    /// Use Multi-Bulyan for small cohorts.
    MultiBulyan,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates supported final aggregation strategies after screening.
pub enum AggregationStrategy {
    /// Use a weighted mean with bounded weights.
    WeightedMean,
    /// Use a clipped weighted mean.
    ClippedWeightedMean,
    /// Use a trimmed mean.
    TrimmedMean,
    /// Use a median.
    Median,
    /// Use a trusted-reference weighted mean.
    TrustedReferenceWeightedMean,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Severity label attached to robustness alerts.
pub enum RobustnessAlertSeverity {
    /// Informational alert.
    Info,
    /// Warning alert.
    Warn,
    /// Critical alert requiring operator attention.
    Critical,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Canonical reasons for hard rejection or quarantine.
pub enum RejectionReason {
    /// Malformed or undecodable update material.
    Malformed,
    /// Schema version or layout mismatch.
    SchemaMismatch,
    /// Base-head lineage mismatch.
    BaseHeadMismatch,
    /// Revision mismatch.
    RevisionMismatch,
    /// Dataset-view mismatch.
    DatasetViewMismatch,
    /// Lease mismatch or missing lease binding.
    LeaseMismatch,
    /// Duplicate or replayed update.
    Replay,
    /// Update is too stale for the configured lag window.
    TooStale,
    /// Update contains NaN or Inf values.
    NanInf,
    /// Global or layer norm exceeded allowed bounds.
    NormTooLarge,
    /// Effective norm is too small to count as useful work.
    NormTooSmall,
    /// Similarity or density screening marked the update as an outlier.
    SimilarityOutlier,
    /// Canary evaluation regressed beyond allowed tolerance.
    CanaryRegression,
    /// Reducer replicas disagreed on robustness classification.
    ReplicaDisagreement,
    /// Peer or principal is currently quarantined.
    QuarantinedPrincipal,
    /// Operator explicitly blocked the update or peer.
    OperatorBlock,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures deterministic hard-reject checks.
pub struct HardRejectPolicy {
    /// Maximum tolerated staleness in merge windows.
    pub max_stale_windows: u16,
    /// Whether duplicate receipt roots or receipts are rejected.
    pub reject_replays: bool,
    /// Whether updates must bind to a current lease.
    pub require_lease_binding: bool,
    /// Whether schema and manifest compatibility is strict.
    pub strict_schema_compatibility: bool,
    /// Whether malformed updates are immediately rejected.
    pub reject_malformed_updates: bool,
    /// Maximum accepted artifact payload size for one update.
    pub max_update_bytes: Option<u64>,
}

impl Default for HardRejectPolicy {
    fn default() -> Self {
        Self {
            max_stale_windows: 2,
            reject_replays: true,
            require_lease_binding: true,
            strict_schema_compatibility: true,
            reject_malformed_updates: true,
            max_update_bytes: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures update normalization and bounded influence.
pub struct ClippingPolicy {
    /// Global norm cap applied before cohort aggregation.
    pub global_norm_cap: f64,
    /// Optional per-layer cap shared across layer norms.
    pub per_layer_norm_cap: Option<f64>,
    /// Minimum useful norm floor below which updates are rejected.
    pub minimum_useful_norm: f64,
    /// Maximum contribution weight after clipping and trust scaling.
    pub max_effective_weight: f64,
    /// Whether adaptive norm-band rescaling is enabled.
    pub adaptive_norm_bands: bool,
}

impl Default for ClippingPolicy {
    fn default() -> Self {
        Self {
            global_norm_cap: 8.0,
            per_layer_norm_cap: None,
            minimum_useful_norm: 1e-6,
            max_effective_weight: 1.0,
            adaptive_norm_bands: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures cheap feature extraction and screening thresholds.
pub struct ScreeningPolicy {
    /// Random projection dimensionality for feature sketches.
    pub sketch_dimensionality: u16,
    /// Minimum cosine similarity to the trusted or cohort reference.
    pub minimum_cosine_similarity: f64,
    /// Minimum sign-agreement fraction to the trusted or cohort reference.
    pub minimum_sign_agreement_fraction: f64,
    /// Maximum allowed neighbor distance before an outlier penalty applies.
    pub maximum_neighbor_distance: f64,
    /// Maximum canary-loss regression tolerated during cheap screening.
    pub maximum_canary_loss_delta: f64,
    /// Score threshold at or above which updates are hard rejected.
    pub hard_reject_score: f64,
    /// Score threshold at or above which updates are downweighted.
    pub downweight_score: f64,
    /// Whether peer-local historical centroids are consulted.
    pub use_historical_centroid: bool,
    /// Whether reducer replicas must agree on the screening verdict.
    pub require_replica_agreement: bool,
}

impl Default for ScreeningPolicy {
    fn default() -> Self {
        Self {
            sketch_dimensionality: 64,
            minimum_cosine_similarity: 0.05,
            minimum_sign_agreement_fraction: 0.45,
            maximum_neighbor_distance: 3.0,
            maximum_canary_loss_delta: 0.10,
            hard_reject_score: 4.0,
            downweight_score: 1.5,
            use_historical_centroid: true,
            require_replica_agreement: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures the reducer-cohort filter stage.
pub struct CohortFilterPolicy {
    /// Strategy used to select or weight candidate updates.
    pub strategy: CohortFilterStrategy,
    /// Fraction trimmed from each side for trimmed-mean style rules.
    pub trim_fraction: f64,
    /// Minimum number of surviving updates after screening.
    pub minimum_survivor_count: u16,
    /// Maximum pairwise or centroid distance tolerated by the filter.
    pub maximum_pairwise_distance: f64,
}

impl Default for CohortFilterPolicy {
    fn default() -> Self {
        Self {
            strategy: CohortFilterStrategy::SimilarityAware,
            trim_fraction: 0.1,
            minimum_survivor_count: 2,
            maximum_pairwise_distance: 3.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures the final screened aggregation stage.
pub struct AggregationPolicy {
    /// Final aggregation rule.
    pub strategy: AggregationStrategy,
    /// Maximum contribution weight for any one update.
    pub max_peer_weight: f64,
    /// Maximum cohort size considered at once.
    pub maximum_cohort_size: u16,
    /// Additional contribution cap applied to browser peers.
    pub browser_contribution_cap: f64,
}

impl Default for AggregationPolicy {
    fn default() -> Self {
        Self {
            strategy: AggregationStrategy::ClippedWeightedMean,
            max_peer_weight: 1.0,
            maximum_cohort_size: 32,
            browser_contribution_cap: 0.5,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures validator-side canary evaluation.
pub struct ValidatorCanaryPolicy {
    /// Minimum evaluator quorum required before promotion.
    pub minimum_evaluator_quorum: u16,
    /// Maximum tolerated aggregate regression before promotion is blocked.
    pub maximum_regression_delta: f64,
    /// Whether trigger-specific backdoor probes are mandatory.
    pub require_backdoor_probes: bool,
    /// Whether promotion is blocked on canary regression.
    pub block_on_regression: bool,
}

impl Default for ValidatorCanaryPolicy {
    fn default() -> Self {
        Self {
            minimum_evaluator_quorum: 2,
            maximum_regression_delta: 0.02,
            require_backdoor_probes: false,
            block_on_regression: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures rolling trust and suspicion updates.
pub struct ReputationPolicy {
    /// Suspicion decay applied between windows.
    pub suspicion_decay: f64,
    /// Penalty weight for a hard rejection.
    pub rejection_weight: f64,
    /// Penalty weight for reducer-replica disagreement.
    pub disagreement_penalty: f64,
    /// Penalty weight for delayed-attack behavior.
    pub delayed_attack_penalty: f64,
    /// Score below which reducer eligibility is revoked.
    pub reducer_demote_threshold: f64,
    /// Score below which validator eligibility is revoked.
    pub validator_demote_threshold: f64,
}

impl Default for ReputationPolicy {
    fn default() -> Self {
        Self {
            suspicion_decay: 0.95,
            rejection_weight: 1.0,
            disagreement_penalty: 0.5,
            delayed_attack_penalty: 1.5,
            reducer_demote_threshold: -2.0,
            validator_demote_threshold: -4.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures automatic quarantine behavior.
pub struct QuarantinePolicy {
    /// Whether quarantine is applied automatically after threshold breaches.
    pub automatic_quarantine: bool,
    /// Consecutive hard rejections before quarantine.
    pub quarantine_after_consecutive_rejections: u16,
    /// Quarantine duration in windows.
    pub quarantine_duration_windows: u16,
    /// Additional windows before a ban recommendation is emitted.
    pub ban_after_quarantine_windows: u16,
}

impl Default for QuarantinePolicy {
    fn default() -> Self {
        Self {
            automatic_quarantine: true,
            quarantine_after_consecutive_rejections: 2,
            quarantine_duration_windows: 4,
            ban_after_quarantine_windows: 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures automatic escalation and operator-visible pauses.
pub struct EscalationPolicy {
    /// Whether reducer-replica disagreement pauses promotion.
    pub pause_on_replica_disagreement: bool,
    /// Whether canary regression pauses promotion.
    pub pause_on_canary_regression: bool,
    /// Whether quarantine transitions emit operator alerts.
    pub alert_on_quarantine: bool,
    /// Maximum quarantined fraction before the revision is flagged unhealthy.
    pub maximum_quarantined_fraction: f64,
}

impl Default for EscalationPolicy {
    fn default() -> Self {
        Self {
            pause_on_replica_disagreement: true,
            pause_on_canary_regression: true,
            alert_on_quarantine: true,
            maximum_quarantined_fraction: 0.33,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Top-level robustness policy attached to a revision or runtime profile.
pub struct RobustnessPolicy {
    /// Named preset label for dashboards and diffs.
    pub preset: RobustnessPreset,
    /// Assumed bounded Byzantine fraction for the cohort.
    pub assumed_byzantine_fraction: f64,
    /// Deterministic hard-reject configuration.
    pub hard_reject_policy: HardRejectPolicy,
    /// Update normalization and clipping configuration.
    pub clipping_policy: ClippingPolicy,
    /// Cheap screening feature configuration.
    pub screening_policy: ScreeningPolicy,
    /// Reducer-cohort filter policy.
    pub cohort_filter_policy: CohortFilterPolicy,
    /// Final aggregation policy after screening.
    pub aggregation_policy: AggregationPolicy,
    /// Validator canary policy.
    pub validator_canary_policy: ValidatorCanaryPolicy,
    /// Rolling trust and suspicion policy.
    pub reputation_policy: ReputationPolicy,
    /// Quarantine policy.
    pub quarantine_policy: QuarantinePolicy,
    /// Escalation and pause policy.
    pub escalation_policy: EscalationPolicy,
}

impl RobustnessPolicy {
    /// Trusted-deployment preset with light screening.
    pub fn minimal() -> Self {
        Self {
            preset: RobustnessPreset::Minimal,
            assumed_byzantine_fraction: 0.05,
            hard_reject_policy: HardRejectPolicy::default(),
            clipping_policy: ClippingPolicy {
                minimum_useful_norm: 0.05,
                ..ClippingPolicy::default()
            },
            screening_policy: ScreeningPolicy {
                hard_reject_score: 5.0,
                downweight_score: 2.5,
                require_replica_agreement: false,
                ..ScreeningPolicy::default()
            },
            cohort_filter_policy: CohortFilterPolicy {
                strategy: CohortFilterStrategy::SimilarityAware,
                ..CohortFilterPolicy::default()
            },
            aggregation_policy: AggregationPolicy::default(),
            validator_canary_policy: ValidatorCanaryPolicy::default(),
            reputation_policy: ReputationPolicy::default(),
            quarantine_policy: QuarantinePolicy::default(),
            escalation_policy: EscalationPolicy::default(),
        }
    }

    /// Default bounded-minority preset for mixed deployments.
    pub fn balanced() -> Self {
        Self {
            preset: RobustnessPreset::Balanced,
            assumed_byzantine_fraction: 0.20,
            hard_reject_policy: HardRejectPolicy::default(),
            clipping_policy: ClippingPolicy {
                minimum_useful_norm: 0.10,
                ..ClippingPolicy::default()
            },
            screening_policy: ScreeningPolicy::default(),
            cohort_filter_policy: CohortFilterPolicy::default(),
            aggregation_policy: AggregationPolicy::default(),
            validator_canary_policy: ValidatorCanaryPolicy::default(),
            reputation_policy: ReputationPolicy::default(),
            quarantine_policy: QuarantinePolicy::default(),
            escalation_policy: EscalationPolicy::default(),
        }
    }

    /// Stricter preset for public or higher-risk deployments.
    pub fn strict() -> Self {
        Self {
            preset: RobustnessPreset::Strict,
            assumed_byzantine_fraction: 0.25,
            hard_reject_policy: HardRejectPolicy {
                max_stale_windows: 1,
                max_update_bytes: Some(64 * 1024 * 1024),
                ..HardRejectPolicy::default()
            },
            clipping_policy: ClippingPolicy {
                global_norm_cap: 4.0,
                per_layer_norm_cap: Some(2.0),
                minimum_useful_norm: 0.15,
                max_effective_weight: 0.75,
                ..ClippingPolicy::default()
            },
            screening_policy: ScreeningPolicy {
                minimum_cosine_similarity: 0.15,
                minimum_sign_agreement_fraction: 0.55,
                maximum_neighbor_distance: 2.0,
                hard_reject_score: 3.0,
                downweight_score: 1.0,
                require_replica_agreement: true,
                ..ScreeningPolicy::default()
            },
            cohort_filter_policy: CohortFilterPolicy {
                strategy: CohortFilterStrategy::TrustedReferenceWeightedMean,
                trim_fraction: 0.2,
                ..CohortFilterPolicy::default()
            },
            aggregation_policy: AggregationPolicy {
                maximum_cohort_size: 16,
                browser_contribution_cap: 0.25,
                ..AggregationPolicy::default()
            },
            validator_canary_policy: ValidatorCanaryPolicy {
                require_backdoor_probes: true,
                maximum_regression_delta: 0.01,
                ..ValidatorCanaryPolicy::default()
            },
            reputation_policy: ReputationPolicy {
                rejection_weight: 1.5,
                disagreement_penalty: 1.0,
                delayed_attack_penalty: 2.0,
                reducer_demote_threshold: -1.0,
                validator_demote_threshold: -2.0,
                ..ReputationPolicy::default()
            },
            quarantine_policy: QuarantinePolicy {
                quarantine_after_consecutive_rejections: 1,
                quarantine_duration_windows: 8,
                ban_after_quarantine_windows: 16,
                ..QuarantinePolicy::default()
            },
            escalation_policy: EscalationPolicy {
                maximum_quarantined_fraction: 0.20,
                ..EscalationPolicy::default()
            },
        }
    }

    /// Heavier research preset for robust-aggregation experiments.
    pub fn research() -> Self {
        Self {
            preset: RobustnessPreset::Research,
            assumed_byzantine_fraction: 0.33,
            hard_reject_policy: HardRejectPolicy::default(),
            clipping_policy: ClippingPolicy {
                minimum_useful_norm: 0.05,
                ..ClippingPolicy::default()
            },
            screening_policy: ScreeningPolicy {
                sketch_dimensionality: 128,
                ..ScreeningPolicy::default()
            },
            cohort_filter_policy: CohortFilterPolicy {
                strategy: CohortFilterStrategy::MultiBulyan,
                trim_fraction: 0.2,
                minimum_survivor_count: 4,
                maximum_pairwise_distance: 2.5,
            },
            aggregation_policy: AggregationPolicy {
                strategy: AggregationStrategy::TrimmedMean,
                maximum_cohort_size: 64,
                ..AggregationPolicy::default()
            },
            validator_canary_policy: ValidatorCanaryPolicy {
                minimum_evaluator_quorum: 3,
                require_backdoor_probes: true,
                ..ValidatorCanaryPolicy::default()
            },
            reputation_policy: ReputationPolicy::default(),
            quarantine_policy: QuarantinePolicy::default(),
            escalation_policy: EscalationPolicy::default(),
        }
    }
}

impl Default for RobustnessPolicy {
    fn default() -> Self {
        Self::balanced()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Compact feature sketch attached to or derived from an update.
pub struct UpdateFeatureSketch {
    /// Approximate payload size in bytes.
    pub artifact_size_bytes: u64,
    /// Global update norm after local computation and before validator clipping.
    pub global_norm: f64,
    /// Optional per-layer norm summary.
    pub per_layer_norms: BTreeMap<String, f64>,
    /// Random-projection feature vector used for cheap similarity checks.
    pub random_projection: Vec<f64>,
    /// Sign sketch over the same or a parallel projection.
    pub sign_projection: Vec<i8>,
    /// Top-k index sketch or heavy-hitter positions.
    pub top_k_indices: Vec<u32>,
    /// Estimated cosine similarity to a trusted or cohort reference.
    pub cosine_to_reference: Option<f64>,
    /// Estimated sign agreement fraction to a trusted or cohort reference.
    pub sign_agreement_fraction: Option<f64>,
    /// Estimated canary-loss delta if a cheap probe was available.
    pub canary_loss_delta: Option<f64>,
    /// Historical deviation score for the peer or principal.
    pub historical_deviation_score: Option<f64>,
    /// Local density or nearest-neighbor distance estimate.
    pub neighbor_distance: Option<f64>,
    /// Update staleness expressed in merge windows.
    pub staleness_windows: u16,
    /// Arrival or transfer delay in milliseconds.
    pub receive_delay_ms: u64,
    /// Number of non-finite tensors or values observed locally.
    pub non_finite_tensor_count: u32,
}

impl Default for UpdateFeatureSketch {
    fn default() -> Self {
        Self {
            artifact_size_bytes: 0,
            global_norm: 0.0,
            per_layer_norms: BTreeMap::new(),
            random_projection: Vec::new(),
            sign_projection: Vec::new(),
            top_k_indices: Vec::new(),
            cosine_to_reference: None,
            sign_agreement_fraction: None,
            canary_loss_delta: None,
            historical_deviation_score: None,
            neighbor_distance: None,
            staleness_windows: 0,
            receive_delay_ms: 0,
            non_finite_tensor_count: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Trust score emitted by robustness screening and reputation tracking.
pub struct TrustScore {
    /// Peer identifier the score applies to.
    pub peer_id: PeerId,
    /// Rolling trust score after the current decision.
    pub score: f64,
    /// Whether the peer remains eligible for reducer duties.
    pub reducer_eligible: bool,
    /// Whether the peer remains eligible for validator duties.
    pub validator_eligible: bool,
    /// Whether the peer is currently quarantined.
    pub quarantined: bool,
    /// Whether runtime policy has escalated the peer to an operator ban recommendation.
    #[serde(default)]
    pub ban_recommended: bool,
    /// Timestamp of the score update.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One screening or validation decision for an update.
pub struct RobustnessDecision {
    /// Peer that produced the update.
    pub peer_id: PeerId,
    /// Whether the update is accepted into the cohort.
    pub accepted: bool,
    /// Whether the update was rejected by a deterministic hard gate.
    pub hard_rejected: bool,
    /// Whether the update was accepted with a downweight.
    pub downweighted: bool,
    /// Whether the peer should enter quarantine after the decision.
    pub quarantined: bool,
    /// Canonical rejection reason, when not accepted.
    pub rejection_reason: Option<RejectionReason>,
    /// Aggregate screening score used by the policy.
    pub screen_score: f64,
    /// Effective contribution weight after clipping and trust scaling.
    pub effective_weight: f64,
    /// Effective norm after clipping.
    pub effective_norm: f64,
    /// Updated trust score, when one was computed.
    pub trust_score: Option<TrustScore>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Cohort-level screening and aggregation summary.
pub struct CohortRobustnessReport {
    /// Experiment identifier covered by the report.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the report.
    pub revision_id: RevisionId,
    /// Merge window identifier.
    pub window_id: WindowId,
    /// Cohort filter strategy that was applied.
    pub cohort_filter_strategy: CohortFilterStrategy,
    /// Final aggregation strategy that was applied.
    pub aggregation_strategy: AggregationStrategy,
    /// Number of updates screened in the cohort.
    pub total_updates: u32,
    /// Number of updates accepted into aggregation.
    pub accepted_updates: u32,
    /// Number of updates rejected before aggregation.
    pub rejected_updates: u32,
    /// Number of accepted but downweighted updates.
    pub downweighted_updates: u32,
    /// Sum of effective contribution weight across accepted updates.
    pub effective_weight_sum: f64,
    /// Mean screening score across the cohort.
    pub mean_screen_score: f64,
    /// Rejection counts grouped by reason.
    pub rejection_reasons: BTreeMap<RejectionReason, u32>,
    /// Alerts emitted while evaluating the cohort.
    pub alerts: Vec<RobustnessAlert>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Validator-side canary evaluation report.
pub struct CanaryEvalReport {
    /// Experiment identifier covered by the report.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the report.
    pub revision_id: RevisionId,
    /// Evaluation protocol identifier used for the report.
    pub eval_protocol_id: ContentId,
    /// Candidate head being evaluated.
    pub candidate_head_id: HeadId,
    /// Base or previous head used as the comparison anchor.
    pub base_head_id: Option<HeadId>,
    /// Whether the candidate passed the canary gate.
    pub accepted: bool,
    /// Metric deltas by key.
    pub metric_deltas: BTreeMap<String, f64>,
    /// Worst allowed-versus-observed regression margin.
    pub regression_margin: f64,
    /// Whether a trigger-specific probe indicated a backdoor.
    pub detected_backdoor_trigger: bool,
    /// Evaluator quorum that participated in the decision.
    pub evaluator_quorum: u16,
    /// Timestamp of the report.
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Operator-visible robustness alert.
pub struct RobustnessAlert {
    /// Experiment identifier covered by the alert.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the alert.
    pub revision_id: RevisionId,
    /// Window identifier, when applicable.
    pub window_id: Option<WindowId>,
    /// Peer responsible for the alert, when known.
    pub peer_id: Option<PeerId>,
    /// Canonical reason associated with the alert.
    pub reason: RejectionReason,
    /// Severity label.
    pub severity: RobustnessAlertSeverity,
    /// Human-readable alert message.
    pub message: String,
    /// Timestamp of the alert.
    pub emitted_at: DateTime<Utc>,
}
