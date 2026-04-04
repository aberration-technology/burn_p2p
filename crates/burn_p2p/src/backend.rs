use super::*;

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
