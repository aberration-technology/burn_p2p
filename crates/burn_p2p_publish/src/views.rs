use std::collections::BTreeSet;

use burn_p2p_core::{
    ArtifactAlias, ArtifactAliasId, ArtifactId, ArtifactProfile, ExperimentId, ExportJob,
    HeadDescriptor, HeadEvalReport, HeadId, PeerId, PrincipalId, PublicationTargetId,
    PublishedArtifactRecord, RunId,
};
use serde::{Deserialize, Serialize};

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
    pub ticket: burn_p2p_core::DownloadTicket,
    /// Backing published artifact record.
    pub published_artifact: PublishedArtifactRecord,
    /// Download route that should be called by the client.
    pub download_path: String,
}

/// Request to mirror one peer-advertised artifact into the receiver's P2P artifact service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerArtifactMirrorRequest {
    /// Artifact to mirror.
    pub artifact_id: ArtifactId,
    /// Provider peers to try in order.
    pub provider_peer_ids: Vec<PeerId>,
    /// Optional total timeout for the mirror operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Result returned after mirroring a peer artifact locally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerArtifactMirrorResponse {
    /// Mirrored artifact.
    pub artifact_id: ArtifactId,
    /// Provider that served the artifact.
    pub mirrored_from: PeerId,
    /// Number of bytes described by the mirrored artifact manifest.
    pub bytes_len: u64,
    /// Number of chunks published into the receiver's P2P artifact service.
    pub chunk_count: usize,
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
    #[serde(default)]
    /// Provider peers currently known to advertise the head artifact on the live control plane.
    pub provider_peer_ids: Vec<PeerId>,
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

fn default_true() -> bool {
    true
}

pub(crate) fn publication_filter_matches_alias(
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

pub(crate) fn publication_filter_matches_record(
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

pub(crate) fn publication_filter_matches_job(
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
