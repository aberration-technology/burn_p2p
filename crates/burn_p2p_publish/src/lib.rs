//! Optional checkpoint aliasing, publication, and download orchestration for `burn_p2p`.
//!
//! This crate mirrors canonical checkpoint artifacts out of the content-addressed
//! checkpoint store without becoming part of the training or promotion hot path.
//! The checkpoint CAS remains canonical; publication state is a durable read-model
//! that tracks human-meaningful aliases, export jobs, and downloadable mirrors.
#![forbid(unsafe_code)]

mod aliases;
mod backends;
mod download;
mod jobs;
mod registry;
mod views;

use std::{collections::BTreeSet, path::PathBuf};

use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactAliasId, ArtifactId, ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile,
    ContentId, DownloadTicketId, EvalProtocolManifest, ExportJob, ExportJobId, HeadDescriptor,
    HeadEvalReport, HeadId, PublicationMode, PublicationTargetId, PublishedArtifactId,
};
use chrono::Utc;
use thiserror::Error;

pub use crate::download::{DownloadArtifact, DownloadArtifactBody};
pub use crate::views::{
    ArtifactAliasStatus, ArtifactBackfillRequest, ArtifactBackfillResult, ArtifactPruneRequest,
    ArtifactPruneResult, ArtifactPublicationFilter, ArtifactRunSummary, ArtifactRunView,
    DownloadTicketRequest, DownloadTicketResponse, ExportRequest, HeadArtifactView,
};

use crate::{
    registry::PublicationRegistryState,
    views::{
        publication_filter_matches_alias, publication_filter_matches_job,
        publication_filter_matches_record,
    },
};

/// Default publication target identifier used by the filesystem-backed publisher.
pub const DEFAULT_PUBLICATION_TARGET_ID: &str = "local-default";

const DEFAULT_DOWNLOAD_TICKET_TTL_SECS: i64 = 300;
const DEFAULT_TARGET_RETENTION_TTL_SECS: u64 = 86_400;
const DEFAULT_BROWSER_SNAPSHOT_MAX_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_PUBLICATION_EVENT_HISTORY: usize = 256;

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

/// Durable filesystem-backed publication registry and exporter.
#[derive(Clone, Debug)]
pub struct PublicationStore {
    root_dir: PathBuf,
    state: PublicationRegistryState,
}

impl PublicationStore {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p_checkpoint::{
        ArtifactBuildSpec, ChunkingScheme, build_artifact_descriptor_from_bytes,
    };
    use burn_p2p_core::{
        ArtifactAliasScope, ArtifactDescriptor, ArtifactId, ArtifactKind, ArtifactLiveEventKind,
        DatasetViewId, EvalAggregationRule, EvalMetricDef, EvalProtocolOptions, ExperimentId,
        ExportJobStatus, HeadEvalStatus, MetricTrustClass, MetricValue, NetworkId, Precision,
        PrincipalId, PublicationAccessMode, PublicationMode, PublicationTarget,
        PublicationTargetKind, RevisionId, WorkloadId,
    };
    use chrono::Duration;
    use std::collections::BTreeSet;
    use std::{collections::BTreeMap, fs};
    #[cfg(feature = "s3")]
    use std::{
        io::{ErrorKind, Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
        time::Duration as StdDuration,
    };
    use tempfile::tempdir;

    use crate::{aliases::run_id_for_head, backends::default_filesystem_target};

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
