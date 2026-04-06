use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile, ContentId,
    DownloadDeliveryMode, DownloadTicket, DownloadTicketId, ExportJob, ExportJobId,
    ExportJobStatus, HeadDescriptor, HeadEvalReport, PublicationTarget, PublicationTargetKind,
    PublishedArtifactId, PublishedArtifactRecord, PublishedArtifactStatus,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    DEFAULT_BROWSER_SNAPSHOT_MAX_BYTES, DEFAULT_DOWNLOAD_TICKET_TTL_SECS, DownloadArtifact,
    DownloadArtifactBody, DownloadTicketRequest, DownloadTicketResponse, ExportRequest,
    PublicationStore, PublishError,
    aliases::run_id_for_head,
    backends::{
        delivery_mode_for_target, download_redirect_url, ensure_profile_allowed,
        publication_object_key, write_local_object,
    },
    download::{content_type_for_profile, download_file_name},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PublishedBrowserSnapshotBundle {
    head: HeadDescriptor,
    artifact_manifest: ArtifactDescriptor,
    eval_reports: Vec<HeadEvalReport>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PublishedManifestBundle {
    head: HeadDescriptor,
    eval_reports: Vec<HeadEvalReport>,
}

impl PublicationStore {
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

        let export_result =
            materialize_publication(artifact_store, reports, &request, &head, &target);
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
            DownloadDeliveryMode::EdgeStream | DownloadDeliveryMode::DeferredPendingExport => {
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
                                url: crate::backends::proxy_download_url(
                                    target,
                                    &published.object_key,
                                )?,
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
}

fn materialize_publication(
    artifact_store: &FsArtifactStore,
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
            write_local_object(target, &object_key, &bytes)?;
        }
        PublicationTargetKind::S3Compatible => {
            #[cfg(feature = "s3")]
            crate::backends::upload_s3_object(target, &object_key, &bytes)?;
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
    Ok(PublishedArtifactRecord {
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
    })
}

fn update_job(export_jobs: &mut [ExportJob], updated: &ExportJob) {
    if let Some(existing) = export_jobs
        .iter_mut()
        .find(|job| job.export_job_id == updated.export_job_id)
    {
        *existing = updated.clone();
    }
}
