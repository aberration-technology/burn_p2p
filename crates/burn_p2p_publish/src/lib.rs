//! Optional checkpoint aliasing, publication, and download orchestration for `burn_p2p`.
//!
//! This crate mirrors canonical checkpoint artifacts out of the content-addressed
//! checkpoint store without becoming part of the training or promotion hot path.
//! The checkpoint CAS remains canonical; publication state is a durable read-model
//! that tracks human-meaningful aliases, export jobs, and downloadable mirrors.
#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactAlias, ArtifactAliasId, ArtifactAliasScope, ArtifactAliasSourceReason,
    ArtifactDescriptor, ArtifactId, ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile,
    ContentId, DownloadDeliveryMode, DownloadTicket, DownloadTicketId, EvalProtocolManifest,
    ExperimentId, ExportJob, ExportJobId, ExportJobStatus, HeadDescriptor, HeadEvalReport,
    HeadEvalStatus, HeadId, PrincipalId, PublicationAccessMode, PublicationMode, PublicationTarget,
    PublicationTargetId, PublicationTargetKind, PublishedArtifactId, PublishedArtifactRecord,
    PublishedArtifactStatus, RevisionId, RunId,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(feature = "s3")]
use {
    hmac::{Hmac, Mac},
    percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode},
    reqwest::blocking::Client,
    sha2::{Digest, Sha256},
    url::Url,
};

/// Default publication target identifier used by the filesystem-backed publisher.
pub const DEFAULT_PUBLICATION_TARGET_ID: &str = "local-default";

const DEFAULT_DOWNLOAD_TICKET_TTL_SECS: i64 = 300;
const DEFAULT_TARGET_RETENTION_TTL_SECS: u64 = 86_400;
const DEFAULT_BROWSER_SNAPSHOT_MAX_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_PUBLICATION_EVENT_HISTORY: usize = 256;
#[cfg(feature = "s3")]
const DEFAULT_SIGNED_URL_TTL_SECS: u64 = 300;
#[cfg(feature = "s3")]
const QUERY_PERCENT_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Errors returned by publication orchestration and download handling.
#[derive(Debug, Error)]
pub enum PublishError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("checkpoint error: {0}")]
    Checkpoint(#[from] burn_p2p_checkpoint::CheckpointError),
    #[error("artifact publication is disabled for target {0}")]
    DisabledPublicationTarget(PublicationTargetId),
    #[error("artifact publication requires a filesystem target")]
    MissingFilesystemTarget,
    #[error("artifact publication requires the optional `s3` feature for target {0}")]
    S3FeatureDisabled(PublicationTargetId),
    #[error("artifact publication requires S3 configuration `{field}` for target {target_id}")]
    MissingS3Config {
        target_id: PublicationTargetId,
        field: &'static str,
    },
    #[cfg(feature = "s3")]
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[cfg(feature = "s3")]
    #[error("invalid s3 endpoint for target {target_id}: {endpoint}")]
    InvalidS3Endpoint {
        target_id: PublicationTargetId,
        endpoint: String,
    },
    #[cfg(feature = "s3")]
    #[error("s3 request failed for target {target_id} with status {status}: {body}")]
    S3RequestFailed {
        target_id: PublicationTargetId,
        status: u16,
        body: String,
    },
    #[error("unknown publication target: {0}")]
    UnknownPublicationTarget(PublicationTargetId),
    #[error("target {target_id} does not allow profile {profile:?}")]
    DisallowedArtifactProfile {
        target_id: PublicationTargetId,
        profile: ArtifactProfile,
    },
    #[error("head not found: {0}")]
    UnknownHead(HeadId),
    #[error("artifact alias not found: {0}")]
    UnknownAlias(ArtifactAliasId),
    #[error("download ticket not found: {0}")]
    UnknownDownloadTicket(DownloadTicketId),
    #[error("published artifact not found: {0}")]
    UnknownPublishedArtifact(PublishedArtifactId),
    #[error("export job not found: {0}")]
    UnknownExportJob(ExportJobId),
    #[error("artifact {artifact_id} exceeds target {target_id} max size of {max_bytes} bytes")]
    TargetSizeLimitExceeded {
        artifact_id: ArtifactId,
        target_id: PublicationTargetId,
        max_bytes: u64,
    },
    #[error("head {head_id} cannot materialize browser snapshots")]
    BrowserSnapshotUnsupported { head_id: HeadId },
    #[error("best-validation aliasing requires a protocol with at least one metric definition")]
    MissingPrimaryEvalMetric,
}

/// Request to export one head and artifact profile to a publication target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportRequest {
    /// Principal that requested the export, when known.
    pub requested_by_principal_id: Option<PrincipalId>,
    /// Experiment covered by the export.
    pub experiment_id: ExperimentId,
    /// Run covered by the export, when known.
    pub run_id: Option<RunId>,
    /// Head to export.
    pub head_id: HeadId,
    /// Artifact profile to materialize.
    pub artifact_profile: ArtifactProfile,
    /// Publication target receiving the export.
    pub publication_target_id: PublicationTargetId,
    /// Alias associated with the export, when present.
    pub artifact_alias_id: Option<ArtifactAliasId>,
}

/// Request to create a short-lived download ticket for one artifact export.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadTicketRequest {
    /// Principal requesting the download.
    pub principal_id: PrincipalId,
    /// Experiment covered by the download.
    pub experiment_id: ExperimentId,
    /// Run covered by the download, when known.
    pub run_id: Option<RunId>,
    /// Head being downloaded.
    pub head_id: HeadId,
    /// Artifact profile requested by the download.
    pub artifact_profile: ArtifactProfile,
    /// Target that should deliver or host the publication.
    pub publication_target_id: PublicationTargetId,
    /// Alias associated with the download, when present.
    pub artifact_alias_id: Option<ArtifactAliasId>,
}

/// Response returned after issuing a download ticket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadTicketResponse {
    /// Created ticket.
    pub ticket: DownloadTicket,
    /// Backing published artifact record.
    pub published_artifact: PublishedArtifactRecord,
    /// Download route that should be called by the client.
    pub download_path: String,
}

/// Lightweight status row for one alias together with publication state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactAliasStatus {
    /// Alias record.
    pub alias: ArtifactAlias,
    /// Latest publication record backing the alias, when available.
    pub published_artifact: Option<PublishedArtifactRecord>,
    /// Latest export job associated with the alias, when available.
    pub latest_job: Option<ExportJob>,
    /// Number of recorded resolutions for this alias.
    pub history_count: usize,
    /// Previously resolved head for this alias, when one exists.
    pub previous_head_id: Option<HeadId>,
}

/// Summary of one run-scoped publication view within one experiment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRunSummary {
    /// Experiment covered by the run.
    pub experiment_id: ExperimentId,
    /// Run covered by the summary.
    pub run_id: RunId,
    /// Most recent head visible for the run.
    pub latest_head_id: HeadId,
    /// Number of current aliases visible for the run.
    pub alias_count: usize,
    /// Number of historical alias resolutions recorded for the run.
    pub alias_history_count: usize,
    /// Number of publication records visible for the run.
    pub published_artifact_count: usize,
}

/// Detailed run-scoped publication view.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRunView {
    /// Experiment covered by the run.
    pub experiment_id: ExperimentId,
    /// Run being described.
    pub run_id: RunId,
    /// Heads currently known for the run.
    pub heads: Vec<HeadDescriptor>,
    /// Current alias rows that resolve within the run.
    pub aliases: Vec<ArtifactAliasStatus>,
    /// Historical alias resolutions recorded for the run.
    pub alias_history: Vec<ArtifactAlias>,
    /// Evaluation reports attached to heads in the run.
    pub eval_reports: Vec<HeadEvalReport>,
    /// Publication records attached to the run.
    pub published_artifacts: Vec<PublishedArtifactRecord>,
}

/// Detail view for one head and the aliases and publications attached to it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeadArtifactView {
    /// Head being described.
    pub head: HeadDescriptor,
    /// Run derived for the head.
    pub run_id: RunId,
    /// Evaluation reports attached to the head.
    pub eval_reports: Vec<HeadEvalReport>,
    /// Aliases that currently resolve to the head.
    pub aliases: Vec<ArtifactAlias>,
    /// Publication records that point at the head.
    pub published_artifacts: Vec<PublishedArtifactRecord>,
    /// Artifact profiles supported by the current publisher configuration.
    pub available_profiles: BTreeSet<ArtifactProfile>,
    /// Alias transition history for this head's experiment and run context.
    pub alias_history: Vec<ArtifactAlias>,
}

/// Filter applied to artifact backfill, prune, and run/history queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactPublicationFilter {
    /// Restrict to one experiment.
    pub experiment_id: Option<ExperimentId>,
    /// Restrict to one run.
    pub run_id: Option<RunId>,
    /// Restrict to one head.
    pub head_id: Option<HeadId>,
    /// Restrict to one artifact profile.
    pub artifact_profile: Option<ArtifactProfile>,
    /// Restrict to one publication target.
    pub publication_target_id: Option<PublicationTargetId>,
    /// Restrict to one alias name.
    pub alias_name: Option<String>,
}

/// Request used by admin backfill flows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactBackfillRequest {
    /// Optional selection filter for aliases to materialize.
    #[serde(default)]
    pub filter: ArtifactPublicationFilter,
}

/// Response returned after an admin backfill request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactBackfillResult {
    /// Export jobs queued or reused by the backfill.
    pub jobs: Vec<ExportJob>,
}

/// Request used by admin prune flows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPruneRequest {
    /// Optional selection filter for records to delete.
    #[serde(default)]
    pub filter: ArtifactPublicationFilter,
    /// Whether matching export jobs should be deleted as well.
    #[serde(default = "default_true")]
    pub prune_jobs: bool,
    /// Whether matching download tickets should be deleted as well.
    #[serde(default = "default_true")]
    pub prune_tickets: bool,
}

impl Default for ArtifactPruneRequest {
    fn default() -> Self {
        Self {
            filter: ArtifactPublicationFilter::default(),
            prune_jobs: true,
            prune_tickets: true,
        }
    }
}

/// Summary returned after pruning publication state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactPruneResult {
    /// Number of publication records removed.
    pub pruned_publications: usize,
    /// Number of export jobs removed.
    pub pruned_jobs: usize,
    /// Number of download tickets removed.
    pub pruned_tickets: usize,
}

/// Delivery body for a resolved artifact download.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadArtifactBody {
    /// No body is needed because the caller should follow `redirect_url`.
    Empty,
    /// Small artifacts already materialized in memory.
    Bytes(Vec<u8>),
    /// Stream from one local filesystem mirror path.
    LocalFile {
        /// Local path to stream from.
        path: PathBuf,
        /// Declared content length of the file.
        content_length: u64,
    },
    /// Stream from one remote object-store URL through the portal.
    RemoteProxy {
        /// Remote URL to fetch from.
        url: String,
        /// Declared content length of the proxied object.
        content_length: u64,
    },
}

/// Streamable artifact payload resolved from a valid download ticket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadArtifact {
    /// Metadata for the published artifact being served.
    pub published_artifact: PublishedArtifactRecord,
    /// Suggested download filename.
    pub file_name: String,
    /// Content type for the streamed response.
    pub content_type: String,
    /// Redirect URL for object-store delivery, when portal streaming is not used.
    pub redirect_url: Option<String>,
    /// Delivery body for portal-stream mode.
    pub body: DownloadArtifactBody,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PublishedBrowserSnapshotBundle {
    head: HeadDescriptor,
    artifact_manifest: ArtifactDescriptor,
    eval_reports: Vec<HeadEvalReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationRegistryState {
    targets: Vec<PublicationTarget>,
    aliases: Vec<ArtifactAlias>,
    alias_history: Vec<ArtifactAlias>,
    published_artifacts: Vec<PublishedArtifactRecord>,
    export_jobs: Vec<ExportJob>,
    download_tickets: Vec<DownloadTicket>,
    #[serde(default)]
    publication_events: Vec<ArtifactLiveEvent>,
}

impl PublicationRegistryState {
    fn new(root_dir: &Path) -> Self {
        Self {
            targets: vec![default_filesystem_target(root_dir)],
            aliases: Vec::new(),
            alias_history: Vec::new(),
            published_artifacts: Vec::new(),
            export_jobs: Vec::new(),
            download_tickets: Vec::new(),
            publication_events: Vec::new(),
        }
    }
}

/// Durable filesystem-backed publication registry and exporter.
#[derive(Clone, Debug)]
pub struct PublicationStore {
    root_dir: PathBuf,
    state: PublicationRegistryState,
}

impl PublicationStore {
    /// Opens or creates a publication store rooted at the provided path.
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self, PublishError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(root_dir.join("mirror"))?;
        let state_path = state_path(&root_dir);
        let mut state = if state_path.exists() {
            serde_json::from_slice::<PublicationRegistryState>(&fs::read(&state_path)?)?
        } else {
            PublicationRegistryState::new(&root_dir)
        };
        if state.targets.is_empty() {
            state.targets.push(default_filesystem_target(&root_dir));
        }
        let mut store = Self { root_dir, state };
        store.prune_expired_records()?;
        Ok(store)
    }

    /// Returns the configured publication targets.
    pub fn targets(&self) -> &[PublicationTarget] {
        &self.state.targets
    }

    /// Replaces the configured publication targets for the store.
    pub fn configure_targets(
        &mut self,
        targets: impl IntoIterator<Item = PublicationTarget>,
    ) -> Result<(), PublishError> {
        let mut normalized = targets
            .into_iter()
            .map(|target| normalize_target(&self.root_dir, target))
            .collect::<Vec<_>>();
        if normalized.is_empty() {
            normalized.push(default_filesystem_target(&self.root_dir));
        }
        normalized
            .sort_by(|left, right| left.publication_target_id.cmp(&right.publication_target_id));
        normalized
            .dedup_by(|left, right| left.publication_target_id == right.publication_target_id);
        self.state.targets = normalized;
        self.persist_state()?;
        Ok(())
    }

    /// Returns the bounded recent artifact publication event log.
    pub fn live_events(&self) -> &[ArtifactLiveEvent] {
        &self.state.publication_events
    }

    /// Returns the latest artifact publication event, when one exists.
    pub fn latest_live_event(&self) -> Option<ArtifactLiveEvent> {
        self.state.publication_events.last().cloned()
    }

    /// Returns the current alias statuses ordered by experiment, scope, and alias name.
    pub fn alias_statuses(&self) -> Vec<ArtifactAliasStatus> {
        let latest_jobs_by_alias = self
            .state
            .export_jobs
            .iter()
            .filter_map(|job| {
                self.state
                    .aliases
                    .iter()
                    .find(|alias| {
                        alias.head_id == job.head_id
                            && alias.artifact_profile == job.artifact_profile
                    })
                    .map(|alias| (alias.artifact_alias_id.clone(), job.clone()))
            })
            .fold(
                BTreeMap::<ArtifactAliasId, ExportJob>::new(),
                |mut acc, (alias_id, job)| {
                    acc.entry(alias_id)
                        .and_modify(|existing| {
                            if job.queued_at > existing.queued_at {
                                *existing = job.clone();
                            }
                        })
                        .or_insert(job);
                    acc
                },
            );
        let mut rows = self
            .state
            .aliases
            .iter()
            .cloned()
            .map(|alias| {
                let alias_id = alias.artifact_alias_id.clone();
                let alias_history = self.alias_history_for_alias(&alias_id);
                let previous_head_id = alias_history
                    .iter()
                    .rev()
                    .find(|entry| entry.head_id != alias.head_id)
                    .map(|entry| entry.head_id.clone());
                ArtifactAliasStatus {
                    alias,
                    published_artifact: self
                        .state
                        .published_artifacts
                        .iter()
                        .filter(|record| {
                            record.artifact_alias_id.as_ref() == Some(&alias_id)
                                && record.status == PublishedArtifactStatus::Ready
                        })
                        .max_by_key(|record| record.created_at)
                        .cloned(),
                    latest_job: latest_jobs_by_alias.get(&alias_id).cloned(),
                    history_count: alias_history.len(),
                    previous_head_id,
                }
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.alias
                .experiment_id
                .cmp(&right.alias.experiment_id)
                .then_with(|| left.alias.scope.cmp(&right.alias.scope))
                .then_with(|| left.alias.alias_name.cmp(&right.alias.alias_name))
                .then_with(|| left.alias.head_id.cmp(&right.alias.head_id))
        });
        rows
    }

    /// Returns alias statuses filtered to one experiment.
    pub fn alias_statuses_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Vec<ArtifactAliasStatus> {
        self.alias_statuses()
            .into_iter()
            .filter(|row| &row.alias.experiment_id == experiment_id)
            .collect()
    }

    /// Returns publication records across all experiments.
    pub fn published_artifacts(&self) -> &[PublishedArtifactRecord] {
        &self.state.published_artifacts
    }

    /// Returns run summaries for one experiment.
    pub fn run_summaries(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
    ) -> Result<Vec<ArtifactRunSummary>, PublishError> {
        let mut by_run = BTreeMap::<RunId, Vec<HeadDescriptor>>::new();
        for head in heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
        {
            by_run
                .entry(run_id_for_head(head)?)
                .or_default()
                .push(head.clone());
        }
        Ok(by_run
            .into_iter()
            .filter_map(|(run_id, run_heads)| {
                let latest_head_id = latest_head(&run_heads)?.head_id;
                let aliases = self.alias_statuses_for_run(heads, experiment_id, &run_id);
                let alias_history = self.alias_history_for_run(heads, experiment_id, &run_id);
                let published_artifact_count = self
                    .state
                    .published_artifacts
                    .iter()
                    .filter(|record| {
                        &record.experiment_id == experiment_id
                            && record.run_id.as_ref() == Some(&run_id)
                    })
                    .count();
                Some(ArtifactRunSummary {
                    experiment_id: experiment_id.clone(),
                    run_id,
                    latest_head_id,
                    alias_count: aliases.len(),
                    alias_history_count: alias_history.len(),
                    published_artifact_count,
                })
            })
            .collect())
    }

    /// Returns one run-scoped publication view.
    pub fn run_view(
        &self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Result<ArtifactRunView, PublishError> {
        let run_heads = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.clone())
            })
            .collect::<Vec<_>>();
        Ok(ArtifactRunView {
            experiment_id: experiment_id.clone(),
            run_id: run_id.clone(),
            alias_history: self.alias_history_for_run(heads, experiment_id, run_id),
            aliases: self.alias_statuses_for_run(heads, experiment_id, run_id),
            eval_reports: reports
                .iter()
                .filter(|report| {
                    report.experiment_id == *experiment_id
                        && run_heads.iter().any(|head| head.head_id == report.head_id)
                })
                .cloned()
                .collect(),
            published_artifacts: self
                .state
                .published_artifacts
                .iter()
                .filter(|record| {
                    &record.experiment_id == experiment_id && record.run_id.as_ref() == Some(run_id)
                })
                .cloned()
                .collect(),
            heads: run_heads,
        })
    }

    /// Returns export jobs across all experiments.
    pub fn export_jobs(&self) -> &[ExportJob] {
        &self.state.export_jobs
    }

    /// Returns detail for one head together with its aliases and publications.
    pub fn head_view(
        &self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        head_id: &HeadId,
    ) -> Result<HeadArtifactView, PublishError> {
        let head = heads
            .iter()
            .find(|head| &head.head_id == head_id)
            .cloned()
            .ok_or_else(|| PublishError::UnknownHead(head_id.clone()))?;
        let run_id = run_id_for_head(&head)?;
        let alias_history = self
            .state
            .alias_history
            .iter()
            .filter(|alias| {
                alias.experiment_id == head.experiment_id
                    && (alias.run_id.as_ref() == Some(&run_id) || alias.run_id.is_none())
            })
            .cloned()
            .collect::<Vec<_>>();
        let available_profiles = self
            .state
            .targets
            .iter()
            .flat_map(|target| target.allowed_artifact_profiles.iter().cloned())
            .collect::<BTreeSet<_>>();
        Ok(HeadArtifactView {
            eval_reports: reports
                .iter()
                .filter(|report| &report.head_id == head_id)
                .cloned()
                .collect(),
            aliases: self
                .state
                .aliases
                .iter()
                .filter(|alias| &alias.head_id == head_id)
                .cloned()
                .collect(),
            published_artifacts: self
                .state
                .published_artifacts
                .iter()
                .filter(|record| &record.head_id == head_id)
                .cloned()
                .collect(),
            head,
            run_id,
            available_profiles,
            alias_history,
        })
    }

    /// Synchronizes the built-in latest and best-validation aliases from canonical state.
    pub fn sync_aliases(
        &mut self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        protocols: &[EvalProtocolManifest],
    ) -> Result<(), PublishError> {
        let mut next_aliases = Vec::<ArtifactAlias>::new();
        let grouped_heads = heads.iter().fold(
            BTreeMap::<(ExperimentId, RevisionId), Vec<HeadDescriptor>>::new(),
            |mut acc, head| {
                acc.entry((head.experiment_id.clone(), head.revision_id.clone()))
                    .or_default()
                    .push(head.clone());
                acc
            },
        );

        for ((experiment_id, _revision_id), revision_heads) in grouped_heads {
            let Some(latest_head) = latest_head(&revision_heads) else {
                continue;
            };
            let run_id = run_id_for_head(&latest_head)?;
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: Some(&run_id),
                scope: ArtifactAliasScope::Run,
                alias_name: "latest/full",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: Some(&run_id),
                scope: ArtifactAliasScope::Run,
                alias_name: "latest/serve",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::ServeCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);

            for protocol in protocols {
                if let Some(best_head) =
                    best_eval_head_for_protocol(&revision_heads, reports, protocol)?
                {
                    let alias_name =
                        format!("best_val/{}/full", protocol.eval_protocol_id.as_str());
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: Some(&run_id),
                        scope: ArtifactAliasScope::Run,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                    let alias_name =
                        format!("best_val/{}/serve", protocol.eval_protocol_id.as_str());
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: Some(&run_id),
                        scope: ArtifactAliasScope::Run,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::ServeCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                }
            }
        }

        let latest_by_experiment = heads.iter().fold(
            BTreeMap::<ExperimentId, Vec<HeadDescriptor>>::new(),
            |mut acc, head| {
                acc.entry(head.experiment_id.clone())
                    .or_default()
                    .push(head.clone());
                acc
            },
        );
        for (experiment_id, experiment_heads) in latest_by_experiment {
            let Some(latest_head) = latest_head(&experiment_heads) else {
                continue;
            };
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: None,
                scope: ArtifactAliasScope::Experiment,
                alias_name: "current/latest/full",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: None,
                scope: ArtifactAliasScope::Experiment,
                alias_name: "current/latest/serve",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::ServeCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            let current_run_id = run_id_for_head(&latest_head)?;
            let current_run_heads = experiment_heads
                .iter()
                .filter(|head| run_id_for_head(head).ok().as_ref() == Some(&current_run_id))
                .cloned()
                .collect::<Vec<_>>();
            for protocol in protocols {
                if let Some(best_head) =
                    best_eval_head_for_protocol(&current_run_heads, reports, protocol)?
                {
                    let alias_name = format!(
                        "current/best_val/{}/full",
                        protocol.eval_protocol_id.as_str()
                    );
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: None,
                        scope: ArtifactAliasScope::Experiment,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                    let alias_name = format!(
                        "current/best_val/{}/serve",
                        protocol.eval_protocol_id.as_str()
                    );
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: None,
                        scope: ArtifactAliasScope::Experiment,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::ServeCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                }
            }
        }

        next_aliases.sort_by(|left, right| {
            left.experiment_id
                .cmp(&right.experiment_id)
                .then_with(|| left.scope.cmp(&right.scope))
                .then_with(|| left.alias_name.cmp(&right.alias_name))
                .then_with(|| left.head_id.cmp(&right.head_id))
                .then_with(|| left.artifact_profile.cmp(&right.artifact_profile))
        });
        next_aliases.dedup_by(|left, right| left.artifact_alias_id == right.artifact_alias_id);

        let previous = self
            .state
            .aliases
            .iter()
            .map(|alias| (alias.artifact_alias_id.clone(), alias.clone()))
            .collect::<BTreeMap<_, _>>();
        for alias in &next_aliases {
            if previous
                .get(&alias.artifact_alias_id)
                .is_none_or(|existing| existing != alias)
            {
                self.state.alias_history.push(alias.clone());
                self.push_live_event(ArtifactLiveEvent {
                    event_id: ContentId::derive(&(
                        "artifact-live",
                        "alias-updated",
                        alias.artifact_alias_id.as_str(),
                        alias.head_id.as_str(),
                        alias.resolved_at.timestamp_micros(),
                    ))?,
                    kind: ArtifactLiveEventKind::AliasUpdated,
                    experiment_id: Some(alias.experiment_id.clone()),
                    run_id: alias.run_id.clone(),
                    head_id: Some(alias.head_id.clone()),
                    artifact_profile: Some(alias.artifact_profile.clone()),
                    publication_target_id: None,
                    alias_name: Some(alias.alias_name.clone()),
                    export_job_id: None,
                    published_artifact_id: None,
                    detail: Some(format!("{:?}", alias.source_reason)),
                    generated_at: Utc::now(),
                });
            }
        }
        self.state.aliases = next_aliases;
        self.persist_state()?;
        Ok(())
    }

    /// Synchronizes aliases and eagerly publishes aliases selected by the target policy.
    pub fn sync_aliases_and_eager_publications(
        &mut self,
        artifact_store: &FsArtifactStore,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        protocols: &[EvalProtocolManifest],
    ) -> Result<(), PublishError> {
        self.sync_aliases(heads, reports, protocols)?;
        let aliases = self.state.aliases.clone();
        let targets = self.state.targets.clone();
        for target in targets {
            if matches!(
                target.publication_mode,
                PublicationMode::Disabled | PublicationMode::LazyOnDemand
            ) {
                continue;
            }
            for alias in aliases
                .iter()
                .filter(|alias| should_eager_publish(alias, &target))
            {
                let request = ExportRequest {
                    requested_by_principal_id: None,
                    experiment_id: alias.experiment_id.clone(),
                    run_id: alias.run_id.clone(),
                    head_id: alias.head_id.clone(),
                    artifact_profile: alias.artifact_profile.clone(),
                    publication_target_id: target.publication_target_id.clone(),
                    artifact_alias_id: Some(alias.artifact_alias_id.clone()),
                };
                match self.request_export(artifact_store, heads, reports, request) {
                    Ok(_) => {}
                    Err(PublishError::DisallowedArtifactProfile { .. })
                    | Err(PublishError::TargetSizeLimitExceeded { .. })
                    | Err(PublishError::BrowserSnapshotUnsupported { .. })
                    | Err(PublishError::DisabledPublicationTarget(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Re-materializes aliases that match the provided admin filter.
    pub fn backfill_aliases(
        &mut self,
        artifact_store: &FsArtifactStore,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        protocols: &[EvalProtocolManifest],
        request: ArtifactBackfillRequest,
    ) -> Result<ArtifactBackfillResult, PublishError> {
        self.sync_aliases(heads, reports, protocols)?;
        let aliases = self
            .state
            .aliases
            .iter()
            .filter(|alias| publication_filter_matches_alias(&request.filter, alias))
            .cloned()
            .collect::<Vec<_>>();
        let target_ids = if let Some(target_id) = request.filter.publication_target_id.clone() {
            vec![target_id]
        } else {
            self.state
                .targets
                .iter()
                .filter(|target| target.publication_mode != PublicationMode::Disabled)
                .map(|target| target.publication_target_id.clone())
                .collect()
        };
        let mut jobs = Vec::new();
        for alias in aliases {
            for target_id in &target_ids {
                let request = ExportRequest {
                    requested_by_principal_id: None,
                    experiment_id: alias.experiment_id.clone(),
                    run_id: alias.run_id.clone(),
                    head_id: alias.head_id.clone(),
                    artifact_profile: alias.artifact_profile.clone(),
                    publication_target_id: target_id.clone(),
                    artifact_alias_id: Some(alias.artifact_alias_id.clone()),
                };
                match self.request_export(artifact_store, heads, reports, request) {
                    Ok(job) => jobs.push(job),
                    Err(PublishError::DisallowedArtifactProfile { .. })
                    | Err(PublishError::TargetSizeLimitExceeded { .. })
                    | Err(PublishError::BrowserSnapshotUnsupported { .. })
                    | Err(PublishError::DisabledPublicationTarget(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(ArtifactBackfillResult { jobs })
    }

    /// Prunes publication state using the provided admin filter.
    pub fn prune_matching(
        &mut self,
        request: ArtifactPruneRequest,
    ) -> Result<ArtifactPruneResult, PublishError> {
        let filter = request.filter;
        let mut pruned_publications = 0usize;
        let published = std::mem::take(&mut self.state.published_artifacts);
        self.state.published_artifacts = published
            .into_iter()
            .filter(|record| {
                let matches =
                    publication_filter_matches_record(&filter, record, &self.state.aliases);
                if matches {
                    pruned_publications += 1;
                    let _ = self.delete_published_artifact(record);
                    self.push_live_event(ArtifactLiveEvent {
                        event_id: ContentId::derive(&(
                            "artifact-live",
                            "publication-pruned",
                            record.published_artifact_id.as_str(),
                            record.head_id.as_str(),
                            Utc::now().timestamp_micros(),
                        ))
                        .expect("artifact prune event id"),
                        kind: ArtifactLiveEventKind::PublicationPruned,
                        experiment_id: Some(record.experiment_id.clone()),
                        run_id: record.run_id.clone(),
                        head_id: Some(record.head_id.clone()),
                        artifact_profile: Some(record.artifact_profile.clone()),
                        publication_target_id: Some(record.publication_target_id.clone()),
                        alias_name: record.artifact_alias_id.as_ref().and_then(|alias_id| {
                            self.state
                                .aliases
                                .iter()
                                .find(|alias| &alias.artifact_alias_id == alias_id)
                                .map(|alias| alias.alias_name.clone())
                        }),
                        export_job_id: None,
                        published_artifact_id: Some(record.published_artifact_id.clone()),
                        detail: Some("pruned".into()),
                        generated_at: Utc::now(),
                    });
                }
                !matches
            })
            .collect();

        let mut pruned_jobs = 0usize;
        if request.prune_jobs {
            let jobs = std::mem::take(&mut self.state.export_jobs);
            self.state.export_jobs = jobs
                .into_iter()
                .filter(|job| {
                    let matches = publication_filter_matches_job(&filter, job, &self.state.aliases);
                    if matches {
                        pruned_jobs += 1;
                    }
                    !matches
                })
                .collect();
        }

        let mut pruned_tickets = 0usize;
        if request.prune_tickets {
            let published_ids = self
                .state
                .published_artifacts
                .iter()
                .map(|record| record.published_artifact_id.clone())
                .collect::<BTreeSet<_>>();
            let tickets = std::mem::take(&mut self.state.download_tickets);
            self.state.download_tickets = tickets
                .into_iter()
                .filter(|ticket| {
                    let keep = published_ids.contains(&ticket.published_artifact_id);
                    if !keep {
                        pruned_tickets += 1;
                    }
                    keep
                })
                .collect();
        }

        self.persist_state()?;
        Ok(ArtifactPruneResult {
            pruned_publications,
            pruned_jobs,
            pruned_tickets,
        })
    }

    /// Exports one head and artifact profile to the configured publication target.
    pub fn request_export(
        &mut self,
        artifact_store: &FsArtifactStore,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        request: ExportRequest,
    ) -> Result<ExportJob, PublishError> {
        self.prune_expired_records()?;
        let target = self
            .target(&request.publication_target_id)?
            .ok_or(PublishError::MissingFilesystemTarget)?
            .clone();
        ensure_profile_allowed(&target, &request.artifact_profile)?;

        if let Some(existing) = self
            .state
            .published_artifacts
            .iter()
            .find(|record| {
                record.head_id == request.head_id
                    && record.artifact_profile == request.artifact_profile
                    && record.publication_target_id == request.publication_target_id
                    && record.status == PublishedArtifactStatus::Ready
            })
            .cloned()
            && let Some(job) = self
                .state
                .export_jobs
                .iter()
                .filter(|job| {
                    job.head_id == existing.head_id
                        && job.artifact_profile == existing.artifact_profile
                        && job.publication_target_id == existing.publication_target_id
                })
                .max_by_key(|job| job.queued_at)
                .cloned()
        {
            return Ok(job);
        }

        let head = heads
            .iter()
            .find(|head| head.head_id == request.head_id)
            .cloned()
            .ok_or_else(|| PublishError::UnknownHead(request.head_id.clone()))?;
        let now = Utc::now();
        let mut job = ExportJob {
            export_job_id: ExportJobId::derive(&(
                request.head_id.as_str(),
                format!("{:?}", request.artifact_profile),
                request.publication_target_id.as_str(),
                now.timestamp_micros(),
            ))?,
            requested_by_principal_id: request.requested_by_principal_id.clone(),
            experiment_id: request.experiment_id.clone(),
            run_id: request
                .run_id
                .clone()
                .or_else(|| run_id_for_head(&head).ok()),
            head_id: request.head_id.clone(),
            artifact_profile: request.artifact_profile.clone(),
            publication_target_id: request.publication_target_id.clone(),
            status: ExportJobStatus::Queued,
            queued_at: now,
            started_at: None,
            finished_at: None,
            failure_reason: None,
        };

        if let Some(existing_job) = self
            .state
            .export_jobs
            .iter()
            .find(|existing| {
                existing.head_id == job.head_id
                    && existing.artifact_profile == job.artifact_profile
                    && existing.publication_target_id == job.publication_target_id
                    && matches!(
                        existing.status,
                        ExportJobStatus::Queued
                            | ExportJobStatus::Materializing
                            | ExportJobStatus::Uploading
                            | ExportJobStatus::Ready
                    )
            })
            .cloned()
        {
            return Ok(existing_job);
        }

        job.status = ExportJobStatus::Materializing;
        job.started_at = Some(Utc::now());
        self.state.export_jobs.push(job.clone());
        self.push_live_event(ArtifactLiveEvent {
            event_id: ContentId::derive(&(
                "artifact-live",
                "export-queued",
                job.export_job_id.as_str(),
                job.head_id.as_str(),
                job.queued_at.timestamp_micros(),
            ))?,
            kind: ArtifactLiveEventKind::ExportJobQueued,
            experiment_id: Some(job.experiment_id.clone()),
            run_id: job.run_id.clone(),
            head_id: Some(job.head_id.clone()),
            artifact_profile: Some(job.artifact_profile.clone()),
            publication_target_id: Some(job.publication_target_id.clone()),
            alias_name: request.artifact_alias_id.as_ref().and_then(|alias_id| {
                self.state
                    .aliases
                    .iter()
                    .find(|alias| &alias.artifact_alias_id == alias_id)
                    .map(|alias| alias.alias_name.clone())
            }),
            export_job_id: Some(job.export_job_id.clone()),
            published_artifact_id: None,
            detail: Some("queued".into()),
            generated_at: Utc::now(),
        });
        self.persist_state()?;

        let export_result = materialize_publication(
            &self.root_dir,
            artifact_store,
            heads,
            reports,
            &request,
            &head,
            &target,
        );
        match export_result {
            Ok(publication) => {
                job.status = ExportJobStatus::Ready;
                job.finished_at = Some(Utc::now());
                update_job(&mut self.state.export_jobs, &job);
                self.push_live_event(ArtifactLiveEvent {
                    event_id: ContentId::derive(&(
                        "artifact-live",
                        "export-ready",
                        job.export_job_id.as_str(),
                        publication.published_artifact_id.as_str(),
                        job.finished_at
                            .expect("ready export job should have a finish time")
                            .timestamp_micros(),
                    ))?,
                    kind: ArtifactLiveEventKind::ExportJobReady,
                    experiment_id: Some(job.experiment_id.clone()),
                    run_id: job.run_id.clone(),
                    head_id: Some(job.head_id.clone()),
                    artifact_profile: Some(job.artifact_profile.clone()),
                    publication_target_id: Some(job.publication_target_id.clone()),
                    alias_name: request.artifact_alias_id.as_ref().and_then(|alias_id| {
                        self.state
                            .aliases
                            .iter()
                            .find(|alias| &alias.artifact_alias_id == alias_id)
                            .map(|alias| alias.alias_name.clone())
                    }),
                    export_job_id: Some(job.export_job_id.clone()),
                    published_artifact_id: Some(publication.published_artifact_id.clone()),
                    detail: Some("ready".into()),
                    generated_at: Utc::now(),
                });
                self.state.published_artifacts.push(publication);
                self.persist_state()?;
                Ok(job)
            }
            Err(error) => {
                job.status = ExportJobStatus::Failed;
                job.finished_at = Some(Utc::now());
                job.failure_reason = Some(error.to_string());
                update_job(&mut self.state.export_jobs, &job);
                self.push_live_event(ArtifactLiveEvent {
                    event_id: ContentId::derive(&(
                        "artifact-live",
                        "export-failed",
                        job.export_job_id.as_str(),
                        job.head_id.as_str(),
                        job.finished_at
                            .expect("failed export job should have a finish time")
                            .timestamp_micros(),
                    ))?,
                    kind: ArtifactLiveEventKind::ExportJobFailed,
                    experiment_id: Some(job.experiment_id.clone()),
                    run_id: job.run_id.clone(),
                    head_id: Some(job.head_id.clone()),
                    artifact_profile: Some(job.artifact_profile.clone()),
                    publication_target_id: Some(job.publication_target_id.clone()),
                    alias_name: request.artifact_alias_id.as_ref().and_then(|alias_id| {
                        self.state
                            .aliases
                            .iter()
                            .find(|alias| &alias.artifact_alias_id == alias_id)
                            .map(|alias| alias.alias_name.clone())
                    }),
                    export_job_id: Some(job.export_job_id.clone()),
                    published_artifact_id: None,
                    detail: job.failure_reason.clone(),
                    generated_at: Utc::now(),
                });
                self.persist_state()?;
                Err(error)
            }
        }
    }

    /// Issues a short-lived download ticket, exporting the artifact first when needed.
    pub fn issue_download_ticket(
        &mut self,
        artifact_store: &FsArtifactStore,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        request: DownloadTicketRequest,
    ) -> Result<DownloadTicketResponse, PublishError> {
        self.prune_expired_records()?;
        let published = if let Some(existing) = self
            .state
            .published_artifacts
            .iter()
            .find(|record| {
                record.head_id == request.head_id
                    && record.artifact_profile == request.artifact_profile
                    && record.publication_target_id == request.publication_target_id
                    && record.status == PublishedArtifactStatus::Ready
            })
            .cloned()
        {
            existing
        } else {
            let export_request = ExportRequest {
                requested_by_principal_id: Some(request.principal_id.clone()),
                experiment_id: request.experiment_id.clone(),
                run_id: request.run_id.clone(),
                head_id: request.head_id.clone(),
                artifact_profile: request.artifact_profile.clone(),
                publication_target_id: request.publication_target_id.clone(),
                artifact_alias_id: request.artifact_alias_id.clone(),
            };
            self.request_export(artifact_store, heads, reports, export_request)?;
            self.state
                .published_artifacts
                .iter()
                .find(|record| {
                    record.head_id == request.head_id
                        && record.artifact_profile == request.artifact_profile
                        && record.publication_target_id == request.publication_target_id
                        && record.status == PublishedArtifactStatus::Ready
                })
                .cloned()
                .ok_or_else(|| {
                    PublishError::UnknownPublishedArtifact(PublishedArtifactId::new(
                        "missing-ready",
                    ))
                })?
        };
        let target = self
            .target(&published.publication_target_id)?
            .ok_or(PublishError::MissingFilesystemTarget)?
            .clone();
        let now = Utc::now();
        let ticket = DownloadTicket {
            download_ticket_id: DownloadTicketId::derive(&(
                published.published_artifact_id.as_str(),
                request.principal_id.as_str(),
                now.timestamp_micros(),
            ))?,
            published_artifact_id: published.published_artifact_id.clone(),
            principal_id: request.principal_id,
            issued_at: now,
            expires_at: now + Duration::seconds(DEFAULT_DOWNLOAD_TICKET_TTL_SECS),
            delivery_mode: delivery_mode_for_target(&target),
        };
        self.state.download_tickets.push(ticket.clone());
        self.persist_state()?;
        Ok(DownloadTicketResponse {
            download_path: format!("/artifacts/download/{}", ticket.download_ticket_id.as_str()),
            published_artifact: published,
            ticket,
        })
    }

    /// Resolves a download ticket to a streamable artifact payload.
    pub fn resolve_download(
        &mut self,
        ticket_id: &DownloadTicketId,
    ) -> Result<DownloadArtifact, PublishError> {
        self.prune_expired_records()?;
        let ticket = self
            .state
            .download_tickets
            .iter()
            .find(|ticket| &ticket.download_ticket_id == ticket_id)
            .cloned()
            .ok_or_else(|| PublishError::UnknownDownloadTicket(ticket_id.clone()))?;
        let published = self
            .state
            .published_artifacts
            .iter()
            .find(|record| record.published_artifact_id == ticket.published_artifact_id)
            .cloned()
            .ok_or_else(|| {
                PublishError::UnknownPublishedArtifact(ticket.published_artifact_id.clone())
            })?;
        let target = self
            .target(&published.publication_target_id)?
            .ok_or_else(|| {
                PublishError::UnknownPublicationTarget(published.publication_target_id.clone())
            })?;
        let (redirect_url, bytes) = match delivery_mode_for_target(target) {
            DownloadDeliveryMode::RedirectToObjectStore => {
                let redirect_url = Some(download_redirect_url(target, &published.object_key)?);
                (redirect_url, DownloadArtifactBody::Empty)
            }
            DownloadDeliveryMode::PortalStream | DownloadDeliveryMode::DeferredPendingExport => {
                let body = match target.kind {
                    PublicationTargetKind::None => {
                        return Err(PublishError::DisabledPublicationTarget(
                            target.publication_target_id.clone(),
                        ));
                    }
                    PublicationTargetKind::LocalFilesystem => {
                        let path = self
                            .target_local_path(
                                &published.publication_target_id,
                                &published.object_key,
                            )?
                            .ok_or(PublishError::MissingFilesystemTarget)?;
                        DownloadArtifactBody::LocalFile {
                            path,
                            content_length: published.content_length,
                        }
                    }
                    PublicationTargetKind::S3Compatible => {
                        #[cfg(feature = "s3")]
                        {
                            DownloadArtifactBody::RemoteProxy {
                                url: proxy_download_url(target, &published.object_key)?,
                                content_length: published.content_length,
                            }
                        }
                        #[cfg(not(feature = "s3"))]
                        {
                            return Err(PublishError::S3FeatureDisabled(
                                target.publication_target_id.clone(),
                            ));
                        }
                    }
                };
                (None, body)
            }
        };
        Ok(DownloadArtifact {
            file_name: download_file_name(&published),
            content_type: content_type_for_profile(&published.artifact_profile).into(),
            redirect_url,
            body: bytes,
            published_artifact: published,
        })
    }

    /// Removes expired published artifacts and download tickets from the registry.
    pub fn prune_expired_records(&mut self) -> Result<(), PublishError> {
        let now = Utc::now();
        self.state
            .download_tickets
            .retain(|ticket| ticket.expires_at > now);
        let existing_records = std::mem::take(&mut self.state.published_artifacts);
        let mut retained = Vec::<PublishedArtifactRecord>::new();
        for record in existing_records {
            if record
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
            {
                let _ = self.delete_published_artifact(&record);
                continue;
            }
            retained.push(record);
        }
        self.state.published_artifacts = retained;
        self.persist_state()?;
        Ok(())
    }

    fn target(
        &self,
        publication_target_id: &PublicationTargetId,
    ) -> Result<Option<&PublicationTarget>, PublishError> {
        Ok(self
            .state
            .targets
            .iter()
            .find(|target| &target.publication_target_id == publication_target_id))
    }

    fn target_local_path(
        &self,
        publication_target_id: &PublicationTargetId,
        object_key: &str,
    ) -> Result<Option<PathBuf>, PublishError> {
        let Some(target) = self.target(publication_target_id)? else {
            return Err(PublishError::UnknownPublicationTarget(
                publication_target_id.clone(),
            ));
        };
        if target.kind != PublicationTargetKind::LocalFilesystem {
            return Ok(None);
        }
        let local_root = target
            .local_root
            .as_ref()
            .ok_or(PublishError::MissingFilesystemTarget)?;
        Ok(Some(PathBuf::from(local_root).join(object_key)))
    }

    fn delete_published_artifact(
        &self,
        record: &PublishedArtifactRecord,
    ) -> Result<(), PublishError> {
        let Some(target) = self.target(&record.publication_target_id)? else {
            return Ok(());
        };
        match target.kind {
            PublicationTargetKind::None => {}
            PublicationTargetKind::LocalFilesystem => {
                if let Some(path) =
                    self.target_local_path(&record.publication_target_id, &record.object_key)?
                    && path.exists()
                {
                    let _ = fs::remove_file(path);
                }
            }
            PublicationTargetKind::S3Compatible => {
                #[cfg(feature = "s3")]
                {
                    let _ = delete_s3_object(target, &record.object_key);
                }
                #[cfg(not(feature = "s3"))]
                {
                    let _ = target;
                }
            }
        }
        Ok(())
    }

    fn persist_state(&self) -> Result<(), PublishError> {
        fs::create_dir_all(self.root_dir.join("mirror"))?;
        let bytes = serde_json::to_vec_pretty(&self.state)?;
        let mut file = fs::File::create(state_path(&self.root_dir))?;
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }

    fn push_live_event(&mut self, event: ArtifactLiveEvent) {
        self.state.publication_events.push(event);
        if self.state.publication_events.len() > DEFAULT_PUBLICATION_EVENT_HISTORY {
            let drop_count =
                self.state.publication_events.len() - DEFAULT_PUBLICATION_EVENT_HISTORY;
            self.state.publication_events.drain(..drop_count);
        }
    }

    fn alias_history_for_alias(&self, alias_id: &ArtifactAliasId) -> Vec<ArtifactAlias> {
        self.state
            .alias_history
            .iter()
            .filter(|alias| &alias.artifact_alias_id == alias_id)
            .cloned()
            .collect()
    }
}

fn default_filesystem_target(root_dir: &Path) -> PublicationTarget {
    PublicationTarget {
        publication_target_id: PublicationTargetId::new(DEFAULT_PUBLICATION_TARGET_ID),
        label: "local mirror".into(),
        kind: PublicationTargetKind::LocalFilesystem,
        publication_mode: PublicationMode::LazyOnDemand,
        access_mode: PublicationAccessMode::Authenticated,
        allow_public_reads: false,
        supports_signed_urls: false,
        portal_proxy_required: true,
        max_artifact_size_bytes: None,
        retention_ttl_secs: Some(DEFAULT_TARGET_RETENTION_TTL_SECS),
        allowed_artifact_profiles: BTreeSet::from([
            ArtifactProfile::FullTrainingCheckpoint,
            ArtifactProfile::ServeCheckpoint,
            ArtifactProfile::ManifestOnly,
        ]),
        eager_alias_names: BTreeSet::new(),
        local_root: Some(root_dir.join("mirror").display().to_string()),
        bucket: None,
        endpoint: None,
        region: None,
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        path_prefix: Some("artifacts".into()),
        multipart_threshold_bytes: None,
        server_side_encryption: None,
        signed_url_ttl_secs: None,
    }
}

fn normalize_target(root_dir: &Path, mut target: PublicationTarget) -> PublicationTarget {
    if target.kind == PublicationTargetKind::LocalFilesystem && target.local_root.is_none() {
        target.local_root = Some(root_dir.join("mirror").display().to_string());
    }
    target
}

fn state_path(root_dir: &Path) -> PathBuf {
    root_dir.join("registry.json")
}

fn run_id_for_head(head: &HeadDescriptor) -> Result<RunId, burn_p2p_core::SchemaError> {
    RunId::derive(&(head.experiment_id.as_str(), head.revision_id.as_str()))
}

struct AliasBuildSpec<'a> {
    experiment_id: &'a ExperimentId,
    run_id: Option<&'a RunId>,
    scope: ArtifactAliasScope,
    alias_name: &'a str,
    head_id: &'a HeadId,
    artifact_profile: ArtifactProfile,
    resolved_at: DateTime<Utc>,
    source_reason: ArtifactAliasSourceReason,
}

fn build_alias(spec: AliasBuildSpec<'_>) -> Result<ArtifactAlias, burn_p2p_core::SchemaError> {
    let artifact_alias_id = ArtifactAliasId::derive(&(
        spec.experiment_id.as_str(),
        spec.run_id.map(RunId::as_str),
        format!("{:?}", spec.scope),
        spec.alias_name,
        format!("{:?}", spec.artifact_profile),
    ))?;
    Ok(ArtifactAlias {
        artifact_alias_id,
        experiment_id: spec.experiment_id.clone(),
        run_id: spec.run_id.cloned(),
        scope: spec.scope,
        alias_name: spec.alias_name.into(),
        head_id: spec.head_id.clone(),
        artifact_profile: spec.artifact_profile,
        resolved_at: spec.resolved_at,
        source_reason: spec.source_reason,
    })
}

fn latest_head(heads: &[HeadDescriptor]) -> Option<HeadDescriptor> {
    heads
        .iter()
        .max_by(|left, right| {
            left.global_step
                .cmp(&right.global_step)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.head_id.cmp(&right.head_id))
        })
        .cloned()
}

fn best_eval_head_for_protocol(
    heads: &[HeadDescriptor],
    reports: &[HeadEvalReport],
    protocol: &EvalProtocolManifest,
) -> Result<Option<HeadEvalReport>, PublishError> {
    let primary = protocol
        .metric_defs
        .first()
        .ok_or(PublishError::MissingPrimaryEvalMetric)?;
    let head_ids = heads
        .iter()
        .map(|head| head.head_id.clone())
        .collect::<BTreeSet<_>>();
    let mut candidates = reports
        .iter()
        .filter(|report| {
            report.eval_protocol_id == protocol.eval_protocol_id
                && report.status == HeadEvalStatus::Completed
                && report.head_id.as_str() != ""
                && head_ids.contains(&report.head_id)
        })
        .filter_map(|report| {
            numeric_metric(report, &primary.metric_key).map(|value| (report, value))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by(|(left, left_value), (right, right_value)| {
        if primary.higher_is_better {
            left_value
                .partial_cmp(right_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        } else {
            right_value
                .partial_cmp(left_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        .then_with(|| left.finished_at.cmp(&right.finished_at))
        .then_with(|| right.head_id.cmp(&left.head_id))
    });
    Ok(candidates.last().map(|(report, _)| (*report).clone()))
}

fn numeric_metric(report: &HeadEvalReport, metric_key: &str) -> Option<f64> {
    match report.metric_values.get(metric_key) {
        Some(burn_p2p_core::MetricValue::Integer(value)) => Some(*value as f64),
        Some(burn_p2p_core::MetricValue::Float(value)) => Some(*value),
        Some(burn_p2p_core::MetricValue::Bool(_))
        | Some(burn_p2p_core::MetricValue::Text(_))
        | None => None,
    }
}

fn ensure_profile_allowed(
    target: &PublicationTarget,
    artifact_profile: &ArtifactProfile,
) -> Result<(), PublishError> {
    if target.allowed_artifact_profiles.contains(artifact_profile) {
        Ok(())
    } else {
        Err(PublishError::DisallowedArtifactProfile {
            target_id: target.publication_target_id.clone(),
            profile: artifact_profile.clone(),
        })
    }
}

fn should_eager_publish(alias: &ArtifactAlias, target: &PublicationTarget) -> bool {
    match target.publication_mode {
        PublicationMode::Disabled | PublicationMode::LazyOnDemand => false,
        PublicationMode::Eager => true,
        PublicationMode::Hybrid => {
            if !target.eager_alias_names.is_empty() {
                target.eager_alias_names.contains(&alias.alias_name)
            } else {
                alias.artifact_profile == ArtifactProfile::ServeCheckpoint
            }
        }
    }
}

fn materialize_publication(
    _root_dir: &Path,
    artifact_store: &FsArtifactStore,
    _heads: &[HeadDescriptor],
    reports: &[HeadEvalReport],
    request: &ExportRequest,
    head: &HeadDescriptor,
    target: &PublicationTarget,
) -> Result<PublishedArtifactRecord, PublishError> {
    let (bytes, content_hash, extension) = match request.artifact_profile {
        ArtifactProfile::FullTrainingCheckpoint | ArtifactProfile::ServeCheckpoint => {
            let descriptor = artifact_store.load_manifest(&head.artifact_id)?;
            let bytes = artifact_store.materialize_artifact_bytes(&descriptor)?;
            let max_bytes = target.max_artifact_size_bytes;
            if let Some(max_bytes) = max_bytes
                && bytes.len() as u64 > max_bytes
            {
                return Err(PublishError::TargetSizeLimitExceeded {
                    artifact_id: head.artifact_id.clone(),
                    target_id: target.publication_target_id.clone(),
                    max_bytes,
                });
            }
            (bytes, descriptor.root_hash, "bin")
        }
        ArtifactProfile::ManifestOnly => {
            let payload = PublishedManifestBundle {
                head: head.clone(),
                eval_reports: reports
                    .iter()
                    .filter(|report| report.head_id == head.head_id)
                    .cloned()
                    .collect(),
            };
            let bytes = serde_json::to_vec_pretty(&payload)?;
            let content_hash = ContentId::derive(&payload)?;
            (bytes, content_hash, "json")
        }
        ArtifactProfile::BrowserSnapshot => {
            let descriptor = artifact_store.load_manifest(&head.artifact_id)?;
            if descriptor.bytes_len > DEFAULT_BROWSER_SNAPSHOT_MAX_BYTES {
                return Err(PublishError::BrowserSnapshotUnsupported {
                    head_id: head.head_id.clone(),
                });
            }
            let payload = PublishedBrowserSnapshotBundle {
                head: head.clone(),
                artifact_manifest: descriptor,
                eval_reports: reports
                    .iter()
                    .filter(|report| report.head_id == head.head_id)
                    .cloned()
                    .collect(),
            };
            let bytes = serde_json::to_vec_pretty(&payload)?;
            let content_hash = ContentId::derive(&payload)?;
            (bytes, content_hash, "json")
        }
    };

    let run_id = request
        .run_id
        .clone()
        .or_else(|| run_id_for_head(head).ok());
    let object_key = publication_object_key(
        target,
        request.experiment_id.as_str(),
        run_id.as_ref(),
        &head.revision_id,
        &request.head_id,
        &request.artifact_profile,
        extension,
    );
    match target.kind {
        PublicationTargetKind::None => {
            return Err(PublishError::DisabledPublicationTarget(
                target.publication_target_id.clone(),
            ));
        }
        PublicationTargetKind::LocalFilesystem => {
            let target_root = target
                .local_root
                .as_ref()
                .ok_or(PublishError::MissingFilesystemTarget)?;
            let path = PathBuf::from(target_root).join(&object_key);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, &bytes)?;
        }
        PublicationTargetKind::S3Compatible => {
            #[cfg(feature = "s3")]
            upload_s3_object(target, &object_key, &bytes)?;
            #[cfg(not(feature = "s3"))]
            return Err(PublishError::S3FeatureDisabled(
                target.publication_target_id.clone(),
            ));
        }
    }

    let now = Utc::now();
    let published_artifact_id = PublishedArtifactId::derive(&(
        request.head_id.as_str(),
        format!("{:?}", request.artifact_profile),
        request.publication_target_id.as_str(),
        object_key.as_str(),
    ))?;
    let record = PublishedArtifactRecord {
        published_artifact_id,
        artifact_alias_id: request.artifact_alias_id.clone(),
        experiment_id: request.experiment_id.clone(),
        run_id,
        head_id: request.head_id.clone(),
        artifact_profile: request.artifact_profile.clone(),
        publication_target_id: request.publication_target_id.clone(),
        object_key,
        content_hash,
        content_length: bytes.len() as u64,
        created_at: now,
        expires_at: target
            .retention_ttl_secs
            .map(|ttl| now + Duration::seconds(ttl as i64)),
        status: PublishedArtifactStatus::Ready,
    };
    Ok(record)
}

fn publication_object_key(
    target: &PublicationTarget,
    experiment_id: &str,
    run_id: Option<&RunId>,
    revision_id: &RevisionId,
    head_id: &HeadId,
    artifact_profile: &ArtifactProfile,
    extension: &str,
) -> String {
    let run_segment = run_id
        .map(|run_id| format!("run/{}", run_id.as_str()))
        .unwrap_or_else(|| format!("revision/{}", revision_id.as_str()));
    let profile = match artifact_profile {
        ArtifactProfile::FullTrainingCheckpoint => "full",
        ArtifactProfile::ServeCheckpoint => "serve",
        ArtifactProfile::BrowserSnapshot => "browser",
        ArtifactProfile::ManifestOnly => "manifest",
    };
    let base_key = format!(
        "exp/{experiment_id}/{run_segment}/{profile}/{}.{}",
        head_id.as_str(),
        extension
    );
    target
        .path_prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| format!("{}/{}", prefix.trim_matches('/'), base_key))
        .unwrap_or(base_key)
}

fn delivery_mode_for_target(target: &PublicationTarget) -> DownloadDeliveryMode {
    match target.kind {
        PublicationTargetKind::S3Compatible
            if target.supports_signed_urls && !target.portal_proxy_required =>
        {
            DownloadDeliveryMode::RedirectToObjectStore
        }
        PublicationTargetKind::None
        | PublicationTargetKind::LocalFilesystem
        | PublicationTargetKind::S3Compatible => DownloadDeliveryMode::PortalStream,
    }
}

fn download_redirect_url(
    target: &PublicationTarget,
    object_key: &str,
) -> Result<String, PublishError> {
    match target.kind {
        PublicationTargetKind::S3Compatible if target.supports_signed_urls => {
            #[cfg(feature = "s3")]
            {
                presign_s3_get_url(
                    target,
                    object_key,
                    target
                        .signed_url_ttl_secs
                        .unwrap_or(DEFAULT_SIGNED_URL_TTL_SECS),
                )
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = object_key;
                Err(PublishError::S3FeatureDisabled(
                    target.publication_target_id.clone(),
                ))
            }
        }
        _ => Err(PublishError::DisabledPublicationTarget(
            target.publication_target_id.clone(),
        )),
    }
}

#[cfg(feature = "s3")]
fn proxy_download_url(
    target: &PublicationTarget,
    object_key: &str,
) -> Result<String, PublishError> {
    match target.kind {
        PublicationTargetKind::S3Compatible => {
            #[cfg(feature = "s3")]
            {
                presign_s3_get_url(
                    target,
                    object_key,
                    target
                        .signed_url_ttl_secs
                        .unwrap_or(DEFAULT_SIGNED_URL_TTL_SECS),
                )
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = object_key;
                Err(PublishError::S3FeatureDisabled(
                    target.publication_target_id.clone(),
                ))
            }
        }
        _ => Err(PublishError::DisabledPublicationTarget(
            target.publication_target_id.clone(),
        )),
    }
}

#[cfg(feature = "s3")]
type HmacSha256 = Hmac<Sha256>;

#[cfg(feature = "s3")]
struct S3Credentials<'a> {
    endpoint: Url,
    bucket: &'a str,
    region: &'a str,
    access_key_id: &'a str,
    secret_access_key: &'a str,
    session_token: Option<&'a str>,
}

#[cfg(feature = "s3")]
fn s3_credentials(target: &PublicationTarget) -> Result<S3Credentials<'_>, PublishError> {
    let endpoint_raw = target
        .endpoint
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "endpoint",
        })?;
    let endpoint = Url::parse(endpoint_raw).map_err(|_| PublishError::InvalidS3Endpoint {
        target_id: target.publication_target_id.clone(),
        endpoint: endpoint_raw.to_owned(),
    })?;
    let bucket = target
        .bucket
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "bucket",
        })?;
    let region = target
        .region
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "region",
        })?;
    let access_key_id =
        target
            .access_key_id
            .as_deref()
            .ok_or_else(|| PublishError::MissingS3Config {
                target_id: target.publication_target_id.clone(),
                field: "access_key_id",
            })?;
    let secret_access_key =
        target
            .secret_access_key
            .as_deref()
            .ok_or_else(|| PublishError::MissingS3Config {
                target_id: target.publication_target_id.clone(),
                field: "secret_access_key",
            })?;
    Ok(S3Credentials {
        endpoint,
        bucket,
        region,
        access_key_id,
        secret_access_key,
        session_token: target.session_token.as_deref(),
    })
}

#[cfg(feature = "s3")]
fn upload_s3_object(
    target: &PublicationTarget,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), PublishError> {
    let url = s3_object_url(target, object_key)?;
    let payload_hash = sha256_hex(bytes);
    let mut additional_headers = Vec::<(String, String)>::new();
    if let Some(sse) = target.server_side_encryption.clone() {
        additional_headers.push(("x-amz-server-side-encryption".into(), sse));
    }
    let headers = s3_authorized_headers(target, "PUT", &url, &payload_hash, &additional_headers)?;
    let client = Client::new();
    let mut request = client.put(url).body(bytes.to_vec());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send()?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    Err(PublishError::S3RequestFailed {
        target_id: target.publication_target_id.clone(),
        status,
        body,
    })
}

#[cfg(feature = "s3")]
fn delete_s3_object(target: &PublicationTarget, object_key: &str) -> Result<(), PublishError> {
    let url = s3_object_url(target, object_key)?;
    let headers = s3_authorized_headers(
        target,
        "DELETE",
        &url,
        &sha256_hex(&[]),
        &Vec::<(String, String)>::new(),
    )?;
    let client = Client::new();
    let mut request = client.delete(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send()?;
    if response.status().is_success() || response.status().as_u16() == 404 {
        return Ok(());
    }
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    Err(PublishError::S3RequestFailed {
        target_id: target.publication_target_id.clone(),
        status,
        body,
    })
}

#[cfg(feature = "s3")]
fn presign_s3_get_url(
    target: &PublicationTarget,
    object_key: &str,
    ttl_secs: u64,
) -> Result<String, PublishError> {
    let creds = s3_credentials(target)?;
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let mut url = s3_object_url(target, object_key)?;
    let credential_scope = format!("{}/{}/s3/aws4_request", datestamp, creds.region);
    let credential = format!("{}/{}", creds.access_key_id, credential_scope);
    let mut query_params = vec![
        ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        ("X-Amz-Credential".to_owned(), credential),
        ("X-Amz-Date".to_owned(), amz_date.clone()),
        ("X-Amz-Expires".to_owned(), ttl_secs.to_string()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ];
    if let Some(token) = creds.session_token {
        query_params.push(("X-Amz-Security-Token".to_owned(), token.to_owned()));
    }
    let canonical_query = canonical_query_string(&query_params);
    url.set_query(Some(&canonical_query));
    let host = url_host_header(&url);
    let canonical_request = format!(
        "GET\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        url.path(),
        canonical_query,
        host
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hmac_signature(
        creds.secret_access_key,
        &datestamp,
        creds.region,
        "s3",
        &string_to_sign,
    );
    let signed_query = if canonical_query.is_empty() {
        format!("X-Amz-Signature={signature}")
    } else {
        format!("{canonical_query}&X-Amz-Signature={signature}")
    };
    url.set_query(Some(&signed_query));
    Ok(url.to_string())
}

#[cfg(feature = "s3")]
fn s3_authorized_headers(
    target: &PublicationTarget,
    method: &str,
    url: &Url,
    payload_hash: &str,
    additional_headers: &[(String, String)],
) -> Result<Vec<(String, String)>, PublishError> {
    let creds = s3_credentials(target)?;
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let mut headers = BTreeMap::<String, String>::from([
        ("host".into(), url_host_header(url)),
        ("x-amz-content-sha256".into(), payload_hash.to_owned()),
        ("x-amz-date".into(), amz_date.clone()),
    ]);
    if let Some(token) = creds.session_token {
        headers.insert("x-amz-security-token".into(), token.to_owned());
    }
    for (name, value) in additional_headers {
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let credential_scope = format!("{}/{}/s3/aws4_request", datestamp, creds.region);
    let canonical_request = format!(
        "{method}\n{}\n{}\n{}\n{}\n{}",
        url.path(),
        canonical_query_from_url(url),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hmac_signature(
        creds.secret_access_key,
        &datestamp,
        creds.region,
        "s3",
        &string_to_sign,
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        creds.access_key_id, credential_scope, signed_headers, signature
    );
    headers.insert("authorization".into(), authorization);
    Ok(headers.into_iter().collect())
}

#[cfg(feature = "s3")]
fn s3_object_url(target: &PublicationTarget, object_key: &str) -> Result<Url, PublishError> {
    let creds = s3_credentials(target)?;
    let mut url = creds.endpoint;
    {
        let mut segments =
            url.path_segments_mut()
                .map_err(|_| PublishError::InvalidS3Endpoint {
                    target_id: target.publication_target_id.clone(),
                    endpoint: target.endpoint.clone().unwrap_or_default(),
                })?;
        segments.pop_if_empty();
        segments.push(creds.bucket);
        for segment in object_key.split('/') {
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
    }
    Ok(url)
}

#[cfg(feature = "s3")]
fn canonical_query_from_url(url: &Url) -> String {
    let params = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    canonical_query_string(&params)
}

#[cfg(feature = "s3")]
fn canonical_query_string(params: &[(String, String)]) -> String {
    let mut encoded = params
        .iter()
        .map(|(key, value)| {
            (
                utf8_percent_encode(key, QUERY_PERCENT_ENCODE_SET).to_string(),
                utf8_percent_encode(value, QUERY_PERCENT_ENCODE_SET).to_string(),
            )
        })
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(feature = "s3")]
fn url_host_header(url: &Url) -> String {
    match url.port() {
        Some(port) => format!(
            "{}:{}",
            url.host_str().expect("url should always include host"),
            port
        ),
        None => url
            .host_str()
            .expect("url should always include host")
            .to_owned(),
    }
}

#[cfg(feature = "s3")]
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(feature = "s3")]
fn hmac_signature(
    secret_access_key: &str,
    datestamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let k_date = hmac_bytes(format!("AWS4{secret_access_key}").as_bytes(), datestamp);
    let k_region = hmac_bytes(&k_date, region);
    let k_service = hmac_bytes(&k_region, service);
    let k_signing = hmac_bytes(&k_service, "aws4_request");
    hex::encode(hmac_bytes(&k_signing, string_to_sign))
}

#[cfg(feature = "s3")]
fn hmac_bytes(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key should be valid");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn content_type_for_profile(artifact_profile: &ArtifactProfile) -> &'static str {
    match artifact_profile {
        ArtifactProfile::ManifestOnly | ArtifactProfile::BrowserSnapshot => {
            "application/json; charset=utf-8"
        }
        ArtifactProfile::FullTrainingCheckpoint | ArtifactProfile::ServeCheckpoint => {
            "application/octet-stream"
        }
    }
}

fn download_file_name(record: &PublishedArtifactRecord) -> String {
    let suffix = match record.artifact_profile {
        ArtifactProfile::FullTrainingCheckpoint => "full.bin",
        ArtifactProfile::ServeCheckpoint => "serve.bin",
        ArtifactProfile::BrowserSnapshot => "browser.json",
        ArtifactProfile::ManifestOnly => "manifest.json",
    };
    format!("{}-{}", record.head_id.as_str(), suffix)
}

fn default_true() -> bool {
    true
}

fn publication_filter_matches_alias(
    filter: &ArtifactPublicationFilter,
    alias: &ArtifactAlias,
) -> bool {
    filter
        .experiment_id
        .as_ref()
        .is_none_or(|experiment_id| &alias.experiment_id == experiment_id)
        && filter
            .run_id
            .as_ref()
            .is_none_or(|run_id| alias.run_id.as_ref() == Some(run_id))
        && filter
            .head_id
            .as_ref()
            .is_none_or(|head_id| &alias.head_id == head_id)
        && filter
            .artifact_profile
            .as_ref()
            .is_none_or(|profile| &alias.artifact_profile == profile)
        && filter
            .alias_name
            .as_ref()
            .is_none_or(|alias_name| &alias.alias_name == alias_name)
}

fn publication_filter_matches_record(
    filter: &ArtifactPublicationFilter,
    record: &PublishedArtifactRecord,
    aliases: &[ArtifactAlias],
) -> bool {
    filter
        .experiment_id
        .as_ref()
        .is_none_or(|experiment_id| &record.experiment_id == experiment_id)
        && filter
            .run_id
            .as_ref()
            .is_none_or(|run_id| record.run_id.as_ref() == Some(run_id))
        && filter
            .head_id
            .as_ref()
            .is_none_or(|head_id| &record.head_id == head_id)
        && filter
            .artifact_profile
            .as_ref()
            .is_none_or(|profile| &record.artifact_profile == profile)
        && filter
            .publication_target_id
            .as_ref()
            .is_none_or(|target_id| &record.publication_target_id == target_id)
        && filter.alias_name.as_ref().is_none_or(|alias_name| {
            record
                .artifact_alias_id
                .as_ref()
                .and_then(|alias_id| {
                    aliases
                        .iter()
                        .find(|alias| &alias.artifact_alias_id == alias_id)
                })
                .is_some_and(|alias| &alias.alias_name == alias_name)
        })
}

fn publication_filter_matches_job(
    filter: &ArtifactPublicationFilter,
    job: &ExportJob,
    aliases: &[ArtifactAlias],
) -> bool {
    filter
        .experiment_id
        .as_ref()
        .is_none_or(|experiment_id| &job.experiment_id == experiment_id)
        && filter
            .run_id
            .as_ref()
            .is_none_or(|run_id| job.run_id.as_ref() == Some(run_id))
        && filter
            .head_id
            .as_ref()
            .is_none_or(|head_id| &job.head_id == head_id)
        && filter
            .artifact_profile
            .as_ref()
            .is_none_or(|profile| &job.artifact_profile == profile)
        && filter
            .publication_target_id
            .as_ref()
            .is_none_or(|target_id| &job.publication_target_id == target_id)
        && filter.alias_name.as_ref().is_none_or(|alias_name| {
            aliases.iter().any(|alias| {
                alias.head_id == job.head_id
                    && alias.artifact_profile == job.artifact_profile
                    && alias.alias_name == *alias_name
            })
        })
}

impl PublicationStore {
    fn alias_statuses_for_run(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Vec<ArtifactAliasStatus> {
        let run_head_ids = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.head_id.clone())
            })
            .collect::<BTreeSet<_>>();
        self.alias_statuses()
            .into_iter()
            .filter(|row| {
                row.alias.experiment_id == *experiment_id
                    && (row.alias.run_id.as_ref() == Some(run_id)
                        || (row.alias.run_id.is_none()
                            && run_head_ids.contains(&row.alias.head_id)))
            })
            .collect()
    }

    fn alias_history_for_run(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Vec<ArtifactAlias> {
        let run_head_ids = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.head_id.clone())
            })
            .collect::<BTreeSet<_>>();
        self.state
            .alias_history
            .iter()
            .filter(|alias| {
                alias.experiment_id == *experiment_id
                    && (alias.run_id.as_ref() == Some(run_id)
                        || (alias.run_id.is_none() && run_head_ids.contains(&alias.head_id)))
            })
            .cloned()
            .collect()
    }
}

fn update_job(export_jobs: &mut [ExportJob], updated: &ExportJob) {
    if let Some(existing) = export_jobs
        .iter_mut()
        .find(|job| job.export_job_id == updated.export_job_id)
    {
        *existing = updated.clone();
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PublishedManifestBundle {
    head: HeadDescriptor,
    eval_reports: Vec<HeadEvalReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p_checkpoint::{
        ArtifactBuildSpec, ChunkingScheme, build_artifact_descriptor_from_bytes,
    };
    use burn_p2p_core::{
        ArtifactDescriptor, ArtifactKind, ArtifactLiveEventKind, DatasetViewId,
        EvalAggregationRule, EvalMetricDef, EvalProtocolOptions, ExperimentId, MetricTrustClass,
        MetricValue, NetworkId, Precision, PublicationAccessMode, PublicationMode,
        PublicationTargetKind, RevisionId, WorkloadId,
    };
    use std::collections::BTreeSet;
    #[cfg(feature = "s3")]
    use std::{
        io::{ErrorKind, Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
        time::Duration as StdDuration,
    };
    use tempfile::tempdir;

    #[cfg(feature = "s3")]
    type S3ObjectMap = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

    fn sample_artifact(
        artifact_store: &FsArtifactStore,
        artifact_id: &str,
        head_id: &str,
    ) -> (ArtifactDescriptor, HeadDescriptor) {
        let payload = b"checkpoint-bytes";
        let descriptor = build_artifact_descriptor_from_bytes(
            &ArtifactBuildSpec::new(
                ArtifactKind::FullHead,
                Precision::Fp32,
                ContentId::new("model-schema-a"),
                "checkpoint-format",
            ),
            payload,
            ChunkingScheme::new(4).expect("chunking"),
        )
        .expect("descriptor");
        let descriptor = ArtifactDescriptor {
            artifact_id: ArtifactId::new(artifact_id),
            head_id: Some(HeadId::new(head_id)),
            ..descriptor
        };
        artifact_store
            .store_prebuilt_artifact_bytes(&descriptor, payload)
            .expect("persist artifact");
        let head = HeadDescriptor {
            study_id: burn_p2p_core::StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            head_id: HeadId::new(head_id),
            artifact_id: descriptor.artifact_id.clone(),
            parent_head_id: None,
            global_step: if head_id == "head-a" { 1 } else { 2 },
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        };
        (descriptor, head)
    }

    fn sample_protocol() -> EvalProtocolManifest {
        EvalProtocolManifest::new(
            "validation",
            DatasetViewId::new("view-a"),
            "validation",
            vec![EvalMetricDef {
                metric_key: "loss".into(),
                display_name: "Loss".into(),
                unit: None,
                higher_is_better: false,
            }],
            EvalProtocolOptions::new(EvalAggregationRule::Mean, 32, 7, "v1"),
        )
        .expect("protocol")
    }

    fn sample_report(head_id: &str, protocol: &EvalProtocolManifest, loss: f64) -> HeadEvalReport {
        HeadEvalReport {
            network_id: NetworkId::new("net-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            head_id: HeadId::new(head_id),
            base_head_id: None,
            eval_protocol_id: protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-a"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
            sample_count: 32,
            dataset_view_id: DatasetViewId::new("view-a"),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }
    }

    #[cfg(feature = "s3")]
    fn sample_s3_target(endpoint: &str) -> PublicationTarget {
        PublicationTarget {
            publication_target_id: PublicationTargetId::new("s3-default"),
            label: "s3 mirror".into(),
            kind: PublicationTargetKind::S3Compatible,
            publication_mode: PublicationMode::LazyOnDemand,
            access_mode: PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: true,
            portal_proxy_required: false,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(DEFAULT_TARGET_RETENTION_TTL_SECS),
            allowed_artifact_profiles: BTreeSet::from([
                ArtifactProfile::FullTrainingCheckpoint,
                ArtifactProfile::ServeCheckpoint,
                ArtifactProfile::ManifestOnly,
            ]),
            eager_alias_names: BTreeSet::new(),
            local_root: None,
            bucket: Some("artifacts".into()),
            endpoint: Some(endpoint.into()),
            region: Some("us-east-1".into()),
            access_key_id: Some("test-access-key".into()),
            secret_access_key: Some("test-secret-key".into()),
            session_token: None,
            path_prefix: Some("exports".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: Some("AES256".into()),
            signed_url_ttl_secs: Some(120),
        }
    }

    #[cfg(feature = "s3")]
    fn spawn_s3_test_server() -> (String, S3ObjectMap, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind s3 test listener");
        listener
            .set_nonblocking(true)
            .expect("set s3 listener nonblocking");
        let addr = listener.local_addr().expect("local addr");
        let objects = Arc::new(Mutex::new(BTreeMap::<String, Vec<u8>>::new()));
        let shared_objects = Arc::clone(&objects);
        let handle = thread::spawn(move || {
            let mut handled_requests = 0usize;
            let mut idle_rounds = 0usize;
            loop {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => {
                        handled_requests += 1;
                        idle_rounds = 0;
                        pair
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        if handled_requests > 0 && idle_rounds >= 20 {
                            break;
                        }
                        idle_rounds += 1;
                        thread::sleep(StdDuration::from_millis(10));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 8192];
                loop {
                    let bytes_read = stream.read(&mut chunk).expect("read s3 request");
                    if bytes_read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..bytes_read]);
                    if let Some(header_end) =
                        buffer.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        let header_len = header_end + 4;
                        let header_text =
                            String::from_utf8_lossy(&buffer[..header_len]).to_string();
                        let content_length = header_text
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("Content-Length: ")
                                    .or_else(|| line.strip_prefix("content-length: "))
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        while buffer.len() < header_len + content_length {
                            let more = stream.read(&mut chunk).expect("read s3 body");
                            if more == 0 {
                                break;
                            }
                            buffer.extend_from_slice(&chunk[..more]);
                        }
                        let mut lines = header_text.lines();
                        let request_line = lines.next().expect("request line");
                        let mut parts = request_line.split_whitespace();
                        let method = parts.next().expect("method");
                        let raw_path = parts.next().expect("path");
                        let path = raw_path.split('?').next().unwrap_or(raw_path);
                        let body = buffer[header_len..header_len + content_length].to_vec();
                        let status = match method {
                            "PUT" => {
                                shared_objects
                                    .lock()
                                    .expect("s3 object map should not be poisoned")
                                    .insert(path.to_owned(), body);
                                "200 OK"
                            }
                            "GET" => {
                                let body = shared_objects
                                    .lock()
                                    .expect("s3 object map should not be poisoned")
                                    .get(path)
                                    .cloned()
                                    .unwrap_or_default();
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                );
                                stream
                                    .write_all(response.as_bytes())
                                    .expect("write s3 get response");
                                stream.write_all(&body).expect("write s3 get body");
                                stream.flush().expect("flush s3 get response");
                                continue;
                            }
                            "DELETE" => {
                                shared_objects
                                    .lock()
                                    .expect("s3 object map should not be poisoned")
                                    .remove(path);
                                "204 No Content"
                            }
                            _ => "405 Method Not Allowed",
                        };
                        let response = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write s3 response");
                        stream.flush().expect("flush s3 response");
                        break;
                    }
                }
            }
        });
        (format!("http://{addr}"), objects, handle)
    }

    #[test]
    fn sync_aliases_builds_latest_and_best_validation_aliases() {
        let root = tempdir().expect("tempdir");
        let store = PublicationStore::open(root.path()).expect("open store");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_a, head_a) = sample_artifact(&artifact_store, "artifact-a", "head-a");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![
            sample_report("head-a", &protocol, 0.42),
            sample_report("head-b", &protocol, 0.21),
        ];
        let mut store = store;
        store
            .sync_aliases(
                &[head_a.clone(), head_b.clone()],
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");

        let alias_names = store
            .alias_statuses()
            .into_iter()
            .map(|row| (row.alias.alias_name, row.alias.head_id))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(alias_names["latest/full"], HeadId::new("head-b"));
        assert_eq!(
            alias_names[&format!("best_val/{}/full", protocol.eval_protocol_id.as_str())],
            HeadId::new("head-b")
        );
        assert_eq!(alias_names["current/latest/serve"], HeadId::new("head-b"));
    }

    #[test]
    fn request_export_deduplicates_ready_publications_and_survives_restart() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];

        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .sync_aliases(
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");
        let request = ExportRequest {
            requested_by_principal_id: Some(PrincipalId::new("alice")),
            experiment_id: ExperimentId::new("exp-a"),
            run_id: Some(run_id_for_head(&head_b).expect("run id")),
            head_id: HeadId::new("head-b"),
            artifact_profile: ArtifactProfile::ServeCheckpoint,
            publication_target_id: PublicationTargetId::new(DEFAULT_PUBLICATION_TARGET_ID),
            artifact_alias_id: None,
        };
        let first_job = store
            .request_export(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                request.clone(),
            )
            .expect("first export");
        let second_job = store
            .request_export(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                request,
            )
            .expect("second export");
        assert_eq!(first_job.export_job_id, second_job.export_job_id);
        assert_eq!(store.published_artifacts().len(), 1);

        let reopened = PublicationStore::open(root.path()).expect("reopen");
        assert_eq!(reopened.published_artifacts().len(), 1);
        assert_eq!(reopened.export_jobs().len(), 1);
    }

    #[test]
    fn download_ticket_streams_published_artifact_bytes() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];

        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .sync_aliases(
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");
        let response = store
            .issue_download_ticket(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                DownloadTicketRequest {
                    principal_id: PrincipalId::new("alice"),
                    experiment_id: ExperimentId::new("exp-a"),
                    run_id: Some(run_id_for_head(&head_b).expect("run id")),
                    head_id: HeadId::new("head-b"),
                    artifact_profile: ArtifactProfile::ServeCheckpoint,
                    publication_target_id: PublicationTargetId::new(DEFAULT_PUBLICATION_TARGET_ID),
                    artifact_alias_id: None,
                },
            )
            .expect("ticket");
        let artifact = store
            .resolve_download(&response.ticket.download_ticket_id)
            .expect("resolve download");
        match artifact.body {
            DownloadArtifactBody::LocalFile {
                ref path,
                content_length,
            } => {
                assert_eq!(content_length, 16);
                assert_eq!(
                    fs::read(path).expect("read streamed file"),
                    b"checkpoint-bytes"
                );
            }
            other => panic!("expected local file body, got {other:?}"),
        }
        assert_eq!(artifact.content_type, "application/octet-stream");
    }

    #[test]
    fn alias_statuses_and_head_views_include_resolution_history() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_a, head_a) = sample_artifact(&artifact_store, "artifact-a", "head-a");
        let (_descriptor_b, mut head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        head_b.experiment_id = head_a.experiment_id.clone();
        head_b.revision_id = head_a.revision_id.clone();
        head_b.study_id = head_a.study_id.clone();
        head_b.created_at = head_a.created_at + Duration::seconds(5);
        head_b.global_step = head_a.global_step + 1;
        let protocol = sample_protocol();
        let reports = vec![
            sample_report("head-a", &protocol, 0.22),
            sample_report("head-b", &protocol, 0.18),
        ];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .sync_aliases(
                std::slice::from_ref(&head_a),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases for first head");
        store
            .sync_aliases(
                &[head_a.clone(), head_b.clone()],
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases for second head");

        let latest_serve = store
            .alias_statuses()
            .into_iter()
            .find(|row| {
                row.alias.alias_name == "latest/serve" && row.alias.scope == ArtifactAliasScope::Run
            })
            .expect("latest serve alias");
        assert_eq!(latest_serve.history_count, 2);
        assert_eq!(
            latest_serve
                .previous_head_id
                .as_ref()
                .expect("previous head")
                .as_str(),
            "head-a"
        );

        let head_view = store
            .head_view(&[head_a.clone(), head_b.clone()], &reports, &head_b.head_id)
            .expect("head view");
        assert!(head_view.alias_history.iter().any(|alias| {
            alias.alias_name == "latest/serve" && alias.head_id.as_str() == "head-a"
        }));
        assert!(head_view.alias_history.iter().any(|alias| {
            alias.alias_name == "latest/serve" && alias.head_id.as_str() == "head-b"
        }));
    }

    #[test]
    fn configured_targets_survive_reopen() {
        let root = tempdir().expect("tempdir");
        let mut store = PublicationStore::open(root.path()).expect("open store");
        let target = PublicationTarget {
            publication_target_id: PublicationTargetId::new("fs-secondary"),
            label: "secondary".into(),
            kind: PublicationTargetKind::LocalFilesystem,
            publication_mode: PublicationMode::Eager,
            access_mode: PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: false,
            portal_proxy_required: true,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(120),
            allowed_artifact_profiles: BTreeSet::from([ArtifactProfile::ServeCheckpoint]),
            eager_alias_names: BTreeSet::new(),
            local_root: Some(root.path().join("secondary").display().to_string()),
            bucket: None,
            endpoint: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            path_prefix: Some("custom".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: None,
            signed_url_ttl_secs: None,
        };
        store
            .configure_targets(vec![target.clone()])
            .expect("configure targets");
        let reopened = PublicationStore::open(root.path()).expect("reopen");
        assert_eq!(reopened.targets(), &[target]);
    }

    #[test]
    fn eager_sync_exports_serve_aliases_for_hybrid_targets() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        let mut hybrid_target = default_filesystem_target(root.path());
        hybrid_target.publication_target_id = PublicationTargetId::new("hybrid");
        hybrid_target.publication_mode = PublicationMode::Hybrid;
        hybrid_target.local_root = Some(root.path().join("hybrid").display().to_string());
        store
            .configure_targets(vec![hybrid_target])
            .expect("configure targets");
        store
            .sync_aliases_and_eager_publications(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync and eager publish");
        assert!(
            store
                .published_artifacts()
                .iter()
                .any(|record| record.artifact_profile == ArtifactProfile::ServeCheckpoint)
        );
        assert!(
            store
                .published_artifacts()
                .iter()
                .all(|record| record.artifact_profile != ArtifactProfile::FullTrainingCheckpoint)
        );
    }

    #[test]
    fn hybrid_targets_can_use_explicit_eager_alias_names() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        let mut hybrid_target = default_filesystem_target(root.path());
        hybrid_target.publication_target_id = PublicationTargetId::new("hybrid-manifest");
        hybrid_target.publication_mode = PublicationMode::Hybrid;
        hybrid_target.allowed_artifact_profiles = BTreeSet::from([ArtifactProfile::ManifestOnly]);
        hybrid_target.eager_alias_names = BTreeSet::from(["current/latest/full".into()]);
        hybrid_target.local_root = Some(root.path().join("hybrid-manifest").display().to_string());
        store
            .configure_targets(vec![hybrid_target])
            .expect("configure targets");
        store
            .sync_aliases_and_eager_publications(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync and eager publish");
        assert!(
            store
                .published_artifacts()
                .iter()
                .all(|record| record.artifact_profile != ArtifactProfile::ServeCheckpoint)
        );
    }

    #[test]
    fn browser_snapshot_exports_small_manifest_bundle() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        let mut target = default_filesystem_target(root.path());
        target.publication_target_id = PublicationTargetId::new("browser-target");
        target.allowed_artifact_profiles = BTreeSet::from([ArtifactProfile::BrowserSnapshot]);
        store
            .configure_targets(vec![target])
            .expect("configure target");
        let job = store
            .request_export(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                ExportRequest {
                    requested_by_principal_id: None,
                    experiment_id: ExperimentId::new("exp-a"),
                    run_id: Some(run_id_for_head(&head_b).expect("run id")),
                    head_id: head_b.head_id.clone(),
                    artifact_profile: ArtifactProfile::BrowserSnapshot,
                    publication_target_id: PublicationTargetId::new("browser-target"),
                    artifact_alias_id: None,
                },
            )
            .expect("export browser snapshot");
        assert_eq!(job.status, ExportJobStatus::Ready);
        let record = store
            .published_artifacts()
            .iter()
            .find(|record| record.artifact_profile == ArtifactProfile::BrowserSnapshot)
            .expect("browser snapshot record");
        assert!(record.object_key.ends_with(".json"));
    }

    #[test]
    fn backfill_and_prune_support_filtered_admin_workflows() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .sync_aliases(
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");
        let result = store
            .backfill_aliases(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
                ArtifactBackfillRequest {
                    filter: ArtifactPublicationFilter {
                        alias_name: Some("latest/serve".into()),
                        ..ArtifactPublicationFilter::default()
                    },
                },
            )
            .expect("backfill aliases");
        assert_eq!(result.jobs.len(), 1);
        let pruned = store
            .prune_matching(ArtifactPruneRequest {
                filter: ArtifactPublicationFilter {
                    artifact_profile: Some(ArtifactProfile::ServeCheckpoint),
                    ..ArtifactPublicationFilter::default()
                },
                ..ArtifactPruneRequest::default()
            })
            .expect("prune");
        assert_eq!(pruned.pruned_publications, 1);
    }

    #[test]
    fn publication_store_records_live_events_for_alias_export_and_prune() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .sync_aliases(
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");
        let alias = store
            .alias_statuses()
            .into_iter()
            .find(|row| row.alias.alias_name == "latest/serve")
            .expect("latest serve alias");
        store
            .request_export(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                ExportRequest {
                    requested_by_principal_id: Some(PrincipalId::new("alice")),
                    experiment_id: ExperimentId::new("exp-a"),
                    run_id: Some(run_id_for_head(&head_b).expect("run id")),
                    head_id: head_b.head_id.clone(),
                    artifact_profile: ArtifactProfile::ServeCheckpoint,
                    publication_target_id: PublicationTargetId::new(DEFAULT_PUBLICATION_TARGET_ID),
                    artifact_alias_id: Some(alias.alias.artifact_alias_id.clone()),
                },
            )
            .expect("export");
        store
            .prune_matching(ArtifactPruneRequest {
                filter: ArtifactPublicationFilter {
                    artifact_profile: Some(ArtifactProfile::ServeCheckpoint),
                    ..ArtifactPublicationFilter::default()
                },
                ..ArtifactPruneRequest::default()
            })
            .expect("prune");

        let kinds = store
            .live_events()
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>();
        assert!(kinds.contains(&ArtifactLiveEventKind::AliasUpdated));
        assert!(kinds.contains(&ArtifactLiveEventKind::ExportJobQueued));
        assert!(kinds.contains(&ArtifactLiveEventKind::ExportJobReady));
        assert!(kinds.contains(&ArtifactLiveEventKind::PublicationPruned));
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_exports_upload_and_download_redirects_use_presigned_urls() {
        let root = tempdir().expect("tempdir");
        let artifact_store = FsArtifactStore::new(root.path().join("cas"));
        artifact_store.ensure_layout().expect("layout");
        let (_descriptor_b, head_b) = sample_artifact(&artifact_store, "artifact-b", "head-b");
        let protocol = sample_protocol();
        let reports = vec![sample_report("head-b", &protocol, 0.21)];
        let (endpoint, objects, handle) = spawn_s3_test_server();
        let mut store = PublicationStore::open(root.path()).expect("open store");
        store
            .configure_targets(vec![sample_s3_target(&endpoint)])
            .expect("configure s3 target");
        store
            .sync_aliases(
                std::slice::from_ref(&head_b),
                &reports,
                std::slice::from_ref(&protocol),
            )
            .expect("sync aliases");

        let export = store
            .request_export(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                ExportRequest {
                    requested_by_principal_id: Some(PrincipalId::new("alice")),
                    experiment_id: ExperimentId::new("exp-a"),
                    run_id: Some(run_id_for_head(&head_b).expect("run id")),
                    head_id: HeadId::new("head-b"),
                    artifact_profile: ArtifactProfile::ServeCheckpoint,
                    publication_target_id: PublicationTargetId::new("s3-default"),
                    artifact_alias_id: None,
                },
            )
            .expect("request export");
        assert_eq!(export.status, ExportJobStatus::Ready);
        assert_eq!(
            objects
                .lock()
                .expect("s3 objects should not be poisoned")
                .values()
                .next()
                .cloned()
                .expect("uploaded object"),
            b"checkpoint-bytes"
        );

        let response = store
            .issue_download_ticket(
                &artifact_store,
                std::slice::from_ref(&head_b),
                &reports,
                DownloadTicketRequest {
                    principal_id: PrincipalId::new("alice"),
                    experiment_id: ExperimentId::new("exp-a"),
                    run_id: Some(run_id_for_head(&head_b).expect("run id")),
                    head_id: HeadId::new("head-b"),
                    artifact_profile: ArtifactProfile::ServeCheckpoint,
                    publication_target_id: PublicationTargetId::new("s3-default"),
                    artifact_alias_id: None,
                },
            )
            .expect("issue ticket");
        let resolved = store
            .resolve_download(&response.ticket.download_ticket_id)
            .expect("resolve download");
        assert!(resolved.redirect_url.is_some());
        assert_eq!(resolved.body, DownloadArtifactBody::Empty);
        handle.join().expect("join s3 server");
    }
}
