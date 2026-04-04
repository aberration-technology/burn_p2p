use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels the scope at which one artifact alias should be interpreted.
pub enum ArtifactAliasScope {
    /// Alias resolves within one training-compatible run.
    Run,
    /// Alias resolves across the currently active run for one experiment.
    Experiment,
    /// Alias resolves within one specific revision.
    Revision,
    /// Alias resolves at a deployment-global or operator-global scope.
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Describes the downloadable artifact shape referenced by an alias or publication record.
pub enum ArtifactProfile {
    /// Full resumable training checkpoint material.
    FullTrainingCheckpoint,
    /// Compact canonical serving checkpoint material.
    ServeCheckpoint,
    /// Browser-oriented snapshot material.
    BrowserSnapshot,
    /// Metadata-only manifest bundle without full checkpoint bytes.
    ManifestOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures why one alias currently resolves to its head and artifact profile.
pub enum ArtifactAliasSourceReason {
    /// Updated because a new canonical head was certified for the run.
    LatestCanonicalHead,
    /// Updated because a canonical head produced the best validation score for one protocol.
    BestValidation {
        /// Evaluation protocol that defined the best-validation comparison.
        eval_protocol_id: ContentId,
    },
    /// Updated or created by explicit operator action.
    ManualOverride,
    /// Updated or created by an on-demand export request.
    OnDemandExport,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported publication target kinds.
pub enum PublicationTargetKind {
    /// Publication is disabled.
    None,
    /// Artifacts are mirrored into a local filesystem directory.
    LocalFilesystem,
    /// Artifacts are mirrored into an S3-compatible object store.
    S3Compatible,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Controls who may access published artifacts from one target or alias family.
pub enum PublicationAccessMode {
    /// Only internal operator access is allowed.
    Private,
    /// Authenticated experiment-scoped access is allowed.
    Authenticated,
    /// Anonymous public access is allowed.
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Configures when artifacts should be exported to external stores.
pub enum PublicationMode {
    /// No external publication is performed.
    Disabled,
    /// Matching aliases are exported immediately after resolution.
    Eager,
    /// Artifacts are exported only on explicit request.
    LazyOnDemand,
    /// A selected subset is eager while the rest stays on-demand.
    Hybrid,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the availability state of one published artifact record.
pub enum PublishedArtifactStatus {
    /// Publication is queued or not yet materialized.
    Pending,
    /// Publication completed successfully and the artifact is ready to serve.
    Ready,
    /// Publication expired under the configured retention policy.
    Expired,
    /// Publication failed.
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the lifecycle state of one export job.
pub enum ExportJobStatus {
    /// The job is queued but not started.
    Queued,
    /// The publisher is materializing the requested artifact shape.
    Materializing,
    /// The publisher is uploading or mirroring the artifact.
    Uploading,
    /// The job completed successfully.
    Ready,
    /// The job failed.
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels the type of transition carried by a live artifact-publication event.
pub enum ArtifactLiveEventKind {
    /// One alias was created or updated to point at a new canonical head.
    AliasUpdated,
    /// One export job was queued.
    ExportJobQueued,
    /// One export job finished successfully.
    ExportJobReady,
    /// One export job failed.
    ExportJobFailed,
    /// One published artifact record was pruned from the registry.
    PublicationPruned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Lightweight live event emitted by the publication registry and edge services.
pub struct ArtifactLiveEvent {
    /// Content-addressed identifier for the event payload.
    pub event_id: ContentId,
    /// Type of transition signaled by the event.
    pub kind: ArtifactLiveEventKind,
    /// Experiment identifier covered by the event, when one is known.
    pub experiment_id: Option<ExperimentId>,
    /// Run identifier covered by the event, when one is known.
    pub run_id: Option<RunId>,
    /// Head identifier covered by the event, when one is known.
    pub head_id: Option<HeadId>,
    /// Artifact profile covered by the event, when one is known.
    pub artifact_profile: Option<ArtifactProfile>,
    /// Publication target covered by the event, when one is known.
    pub publication_target_id: Option<PublicationTargetId>,
    /// Alias label covered by the event, when one is known.
    pub alias_name: Option<String>,
    /// Export job attached to the event, when one exists.
    pub export_job_id: Option<ExportJobId>,
    /// Published artifact attached to the event, when one exists.
    pub published_artifact_id: Option<PublishedArtifactId>,
    /// Human-readable event detail.
    pub detail: Option<String>,
    /// Timestamp when the event was generated.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels how a download ticket will deliver the artifact bytes.
pub enum DownloadDeliveryMode {
    /// The caller should be redirected to a signed or direct object-store URL.
    RedirectToObjectStore,
    /// The portal should stream the artifact body directly.
    PortalStream,
    /// The requested artifact is still being exported.
    DeferredPendingExport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes one configured artifact publication target.
pub struct PublicationTarget {
    /// Stable publication target identifier.
    pub publication_target_id: PublicationTargetId,
    /// Human-readable target label.
    pub label: String,
    /// Concrete target kind.
    pub kind: PublicationTargetKind,
    /// Publication mode used for the target.
    pub publication_mode: PublicationMode,
    /// Access mode enforced for the target.
    pub access_mode: PublicationAccessMode,
    /// Whether anonymous reads are allowed.
    pub allow_public_reads: bool,
    /// Whether the target supports signed URLs.
    pub supports_signed_urls: bool,
    /// Whether portal proxying is required for downloads.
    pub portal_proxy_required: bool,
    /// Maximum artifact size accepted by the target, when configured.
    pub max_artifact_size_bytes: Option<u64>,
    /// Retention TTL applied by the target, when configured.
    pub retention_ttl_secs: Option<u64>,
    /// Artifact profiles allowed on the target.
    pub allowed_artifact_profiles: BTreeSet<ArtifactProfile>,
    /// Alias names that should be eagerly published when `publication_mode` is `Hybrid`.
    #[serde(default)]
    pub eager_alias_names: BTreeSet<String>,
    /// Local root directory for filesystem targets, when configured.
    pub local_root: Option<String>,
    /// Bucket name for object-store targets, when configured.
    pub bucket: Option<String>,
    /// Endpoint or region label for object-store targets, when configured.
    pub endpoint: Option<String>,
    /// Region name for S3-compatible targets, when configured.
    pub region: Option<String>,
    /// Static access key identifier for S3-compatible targets, when configured.
    pub access_key_id: Option<String>,
    /// Static secret access key for S3-compatible targets, when configured.
    pub secret_access_key: Option<String>,
    /// Optional session token for short-lived S3-compatible credentials.
    pub session_token: Option<String>,
    /// Path prefix applied under the target root or bucket.
    pub path_prefix: Option<String>,
    /// Multipart upload threshold in bytes, when configured.
    pub multipart_threshold_bytes: Option<u64>,
    /// Requested server-side encryption mode, when configured.
    pub server_side_encryption: Option<String>,
    /// TTL for generated signed URLs, when configured.
    pub signed_url_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Named pointer from a human-meaningful alias to one canonical head and artifact profile.
pub struct ArtifactAlias {
    /// Stable alias identifier.
    pub artifact_alias_id: ArtifactAliasId,
    /// Experiment covered by the alias.
    pub experiment_id: ExperimentId,
    /// Run covered by the alias, when the alias is run-scoped.
    pub run_id: Option<RunId>,
    /// Alias scope.
    pub scope: ArtifactAliasScope,
    /// Human-readable alias name.
    pub alias_name: String,
    /// Head resolved by the alias.
    pub head_id: HeadId,
    /// Artifact profile resolved by the alias.
    pub artifact_profile: ArtifactProfile,
    /// Timestamp when the alias resolved to the current head.
    pub resolved_at: DateTime<Utc>,
    /// Reason the alias resolved to the current head.
    pub source_reason: ArtifactAliasSourceReason,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One externally published or mirrored artifact record.
pub struct PublishedArtifactRecord {
    /// Stable published-artifact identifier.
    pub published_artifact_id: PublishedArtifactId,
    /// Alias associated with the publication, when published from an alias.
    pub artifact_alias_id: Option<ArtifactAliasId>,
    /// Experiment covered by the publication.
    pub experiment_id: ExperimentId,
    /// Run covered by the publication, when applicable.
    pub run_id: Option<RunId>,
    /// Canonical head exported by the publication.
    pub head_id: HeadId,
    /// Artifact profile materialized by the publication.
    pub artifact_profile: ArtifactProfile,
    /// Publication target that stores the exported object.
    pub publication_target_id: PublicationTargetId,
    /// Target-local object key or path.
    pub object_key: String,
    /// Content hash of the published bytes.
    pub content_hash: ContentId,
    /// Byte length of the published artifact.
    pub content_length: u64,
    /// Timestamp when the published artifact was created.
    pub created_at: DateTime<Utc>,
    /// Expiration timestamp, when the publication is temporary.
    pub expires_at: Option<DateTime<Utc>>,
    /// Publication status.
    pub status: PublishedArtifactStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One export or download-preparation job.
pub struct ExportJob {
    /// Stable export-job identifier.
    pub export_job_id: ExportJobId,
    /// Principal that requested the export, when known.
    pub requested_by_principal_id: Option<PrincipalId>,
    /// Experiment covered by the export.
    pub experiment_id: ExperimentId,
    /// Run covered by the export, when applicable.
    pub run_id: Option<RunId>,
    /// Head to export.
    pub head_id: HeadId,
    /// Artifact profile to materialize.
    pub artifact_profile: ArtifactProfile,
    /// Target that will receive the exported artifact.
    pub publication_target_id: PublicationTargetId,
    /// Current lifecycle state.
    pub status: ExportJobStatus,
    /// Timestamp when the export was queued.
    pub queued_at: DateTime<Utc>,
    /// Timestamp when export work started, when present.
    pub started_at: Option<DateTime<Utc>>,
    /// Timestamp when export work finished, when present.
    pub finished_at: Option<DateTime<Utc>>,
    /// Failure reason when the export failed.
    pub failure_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Short-lived authorization record for one portal or object-store download.
pub struct DownloadTicket {
    /// Stable download-ticket identifier.
    pub download_ticket_id: DownloadTicketId,
    /// Published artifact referenced by the ticket.
    pub published_artifact_id: PublishedArtifactId,
    /// Principal authorized by the ticket.
    pub principal_id: PrincipalId,
    /// Ticket issuance timestamp.
    pub issued_at: DateTime<Utc>,
    /// Ticket expiration timestamp.
    pub expires_at: DateTime<Utc>,
    /// Delivery mode that should be used for the download.
    pub delivery_mode: DownloadDeliveryMode,
}
