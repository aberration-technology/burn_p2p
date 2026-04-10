use std::{collections::BTreeMap, error::Error as StdError, fmt};

use burn_p2p_core::{
    AggregateEnvelope, ArtifactDescriptor, ArtifactId, AssignmentLease, ContentId,
    ContributionReceipt, HeadDescriptor, HeadId, MergeCertificate, MetricValue, PeerId,
    ReducerLoadReport,
};
use burn_p2p_dataloader::CachedMicroShard;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Dataset split selector for evaluation calls.
pub enum EvalSplit {
    /// Train-split evaluation.
    Train,
    /// Validation-split evaluation.
    Validation,
    /// Test-split evaluation.
    Test,
    /// Backend-defined custom split.
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Structured metric report returned by backend evaluation.
pub struct MetricReport {
    /// Named metrics captured for the evaluation.
    pub metrics: BTreeMap<String, MetricValue>,
    /// Timestamp for when the evaluation completed.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Patch-application outcome for live runtime mutation requests.
pub enum PatchOutcome {
    /// Patch applied in-place.
    Applied,
    /// Patch requires a process restart to take effect.
    RequiresRestart,
    /// Patch was rejected with a reason.
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Backend training error surfaced through the runtime seam.
pub struct TrainError {
    /// Human-readable error message.
    pub message: String,
}

impl TrainError {
    /// Creates a new training error wrapper.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TrainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for TrainError {}

impl From<&str> for TrainError {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TrainError {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Mutable backend execution context for one training window.
pub struct WindowCtx<D, M, T> {
    /// The runtime device.
    pub device: D,
    /// The in-memory model instance.
    pub model: M,
    /// The lease being executed.
    pub lease: AssignmentLease,
    /// Cached microshard metadata materialized for the active lease.
    pub cached_microshards: Vec<CachedMicroShard>,
    /// Materialized batches for the lease.
    pub batches: Vec<T>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Report produced by one backend training window.
pub struct WindowReport<T> {
    /// Optional contribution receipt emitted directly by the backend.
    pub contribution: Option<ContributionReceipt>,
    /// Backend-defined statistics payload.
    pub stats: T,
    /// Completion timestamp.
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
/// One candidate model participating in a reducer/validator merge step.
pub struct MergeModelCandidate<'a, M> {
    /// Source peer identifier.
    pub peer_id: &'a PeerId,
    /// Candidate head identifier.
    pub head_id: &'a HeadId,
    /// Candidate artifact identifier.
    pub artifact_id: &'a ArtifactId,
    /// In-memory model value.
    pub model: &'a M,
    /// Sample-weight contribution.
    pub sample_weight: f64,
    /// Quality-weight contribution.
    pub quality_weight: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Timing details captured for one completed training window.
pub struct TrainingWindowTiming {
    /// Timestamp when the runtime started preparing and executing the window.
    pub window_started_at: DateTime<Utc>,
    /// Timestamp when the backend training loop finished.
    pub completed_at: DateTime<Utc>,
    /// Time spent fetching leased dataset shards before training.
    pub data_fetch_time_ms: u64,
    /// Time spent publishing the resulting update after training completed.
    pub publish_latency_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Selects how a long-running trainer reconciles a newly visible canonical head.
pub enum TrainerCanonicalReconcileStrategy {
    /// Replaces the local speculative model with the canonical model.
    Replace,
    /// Applies root EMA where `canonical_weight` is the weight of the incoming
    /// canonical model and `1 - canonical_weight` is the retained local weight.
    RootEma {
        /// The weight assigned to the incoming canonical model.
        canonical_weight: f64,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Configures the continuous trainer happy path.
pub struct ContinuousTrainerPolicy {
    /// How newly visible canonical heads are blended into the local warm model.
    pub canonical_reconcile: TrainerCanonicalReconcileStrategy,
    /// Preferred maximum speculative lead, in locally published windows ahead
    /// of the last visible canonical head, before the trainer briefly waits
    /// for canonical visibility to catch up.
    pub max_speculative_windows: u32,
    /// Bounded grace period spent waiting for canonical visibility once the
    /// speculative lead reaches the configured maximum.
    pub canonical_visibility_wait_ms: u64,
}

impl Default for ContinuousTrainerPolicy {
    fn default() -> Self {
        Self {
            canonical_reconcile: TrainerCanonicalReconcileStrategy::RootEma {
                canonical_weight: 0.25,
            },
            max_speculative_windows: 2,
            canonical_visibility_wait_ms: 750,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Result of one completed training window.
pub struct TrainingWindowOutcome<T> {
    /// Assigned lease that was executed.
    pub lease: AssignmentLease,
    /// Published head descriptor.
    pub head: HeadDescriptor,
    /// Materialized artifact descriptor.
    pub artifact: ArtifactDescriptor,
    /// Accepted contribution receipt.
    pub contribution: ContributionReceipt,
    /// Runtime timing details for the window.
    pub timing: TrainingWindowTiming,
    /// Backend-specific window report.
    pub report: WindowReport<T>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Result of one completed validator merge/selection pass.
pub struct ValidationOutcome {
    /// Source peer selected as the winning contribution origin.
    pub source_peer_id: PeerId,
    /// Published merged head descriptor.
    pub merged_head: HeadDescriptor,
    /// Issued merge certificate.
    pub merge_certificate: MergeCertificate,
    /// Accepted contribution receipt.
    pub contribution: ContributionReceipt,
    /// Evaluation of the merged result.
    pub evaluation: MetricReport,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Default)]
/// Observed validator coordination state for one aggregate/merged-head pair.
pub struct ValidationCoordinationState {
    /// Validators that have published reduction certificates for the aggregate.
    pub attesters: Vec<PeerId>,
    /// Reduction certificate ids associated with the visible attesters.
    pub reduction_ids: Vec<ContentId>,
    /// Whether the aggregate proposal is visible in the control plane.
    pub aggregate_proposal_announced: bool,
    /// Whether a quorum certificate for the merged head is visible.
    pub quorum_announced: bool,
    /// Whether a merge certificate for the merged head is visible.
    pub merge_announced: bool,
    /// First visible merge certificate, when one exists.
    pub merge_certificate: Option<MergeCertificate>,
}

impl ValidationCoordinationState {
    /// Returns whether validator quorum is visible either explicitly or by attester count.
    pub fn quorum_reached(&self, quorum: usize) -> bool {
        self.quorum_announced || self.attesters.len() >= quorum
    }

    /// Returns whether the aggregate has reached a settled post-validation state.
    pub fn settled(&self, quorum: usize) -> bool {
        self.merge_announced || self.quorum_reached(quorum)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Result of driving validator execution until the aggregate settles or times out.
pub struct ValidationDriveOutcome {
    /// Number of local validation attempts executed.
    pub attempts: usize,
    /// Locally promoted validation outcome, when this node won promotion.
    pub promoted: Option<ValidationOutcome>,
    /// Latest observed coordination state at the end of the drive loop.
    pub coordination: ValidationCoordinationState,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Result of one completed reducer aggregate-proposal pass.
pub struct ReducerOutcome {
    /// Source peer selected as the winning contribution origin.
    pub source_peer_id: PeerId,
    /// Candidate merged head selected by the reducer.
    pub merged_head: HeadDescriptor,
    /// Published aggregate proposal.
    pub aggregate: AggregateEnvelope,
    /// Reducer-side load report for the published proposal.
    pub reducer_load_report: ReducerLoadReport,
    /// Evaluation of the selected aggregate result.
    pub evaluation: MetricReport,
}
