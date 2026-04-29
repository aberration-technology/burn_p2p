use burn_p2p::{ArtifactDescriptor, ContentId, ExperimentId, HeadId, RevisionId};
#[cfg(target_arch = "wasm32")]
use burn_p2p::{ContributionReceiptId, UpdateAnnounce, UpdateNormStats};
use thiserror::Error;

use crate::{
    BrowserAuthClientError, BrowserCapabilityReport, BrowserEdgeClient, BrowserEdgeSnapshot,
    BrowserEnrollmentConfig, BrowserRuntimeConfig, BrowserRuntimeRole, BrowserSessionState,
    BrowserTrainingPlan, BrowserTransportKind, BrowserTransportPolicy, BrowserTransportStatus,
    BrowserUiBindings, BrowserWorkerCommand, BrowserWorkerEvent, BrowserWorkerRuntime,
};
#[cfg(target_arch = "wasm32")]
use crate::{
    BrowserTransportFamily,
    app::{establish_direct_swarm_runtime, refresh_worker_runtime_preferring_direct_swarm},
};
#[cfg(target_arch = "wasm32")]
use burn_p2p_swarm::{ArtifactChunkPayload, UpdateEnvelopeAnnouncement};
#[cfg(target_arch = "wasm32")]
use burn_p2p_swarm::{BrowserSwarmRuntime, ExperimentOverlaySet, WasmBrowserSwarmRuntime};
#[cfg(target_arch = "wasm32")]
use chrono::Utc;
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;

#[cfg(target_arch = "wasm32")]
const DIRECT_TRANSPORT_HANDOFF_POLL_MS: u32 = 250;
#[cfg(target_arch = "wasm32")]
const DIRECT_TRANSPORT_HANDOFF_WAIT_MS: u32 = 8_000;

#[derive(Clone, Debug)]
/// Generic configuration for bootstrapping one browser worker runtime from an
/// authenticated browser session.
pub struct BrowserSessionRuntimeConfig {
    /// The edge base URL.
    pub edge_base_url: String,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The runtime role.
    pub role: BrowserRuntimeRole,
    /// The transport policy.
    pub transport: BrowserTransportPolicy,
    /// The selected experiment, when pinned.
    pub selected_experiment: Option<ExperimentId>,
    /// The selected revision, when pinned.
    pub selected_revision: Option<RevisionId>,
    /// The browser capability report.
    pub capability: BrowserCapabilityReport,
    /// Whether to sync leaderboard state during bootstrap.
    pub include_leaderboard: bool,
    /// Whether to establish and refresh a direct swarm runtime for this session.
    pub enable_direct_swarm: bool,
}

impl BrowserSessionRuntimeConfig {
    /// Builds one browser runtime config using the current edge snapshot.
    pub fn runtime_config(&self, snapshot: &BrowserEdgeSnapshot) -> BrowserRuntimeConfig {
        let mut runtime_config = BrowserRuntimeConfig::new(
            self.edge_base_url.clone(),
            snapshot.network_id.clone(),
            self.release_train_hash.clone(),
            self.target_artifact_id.clone(),
            self.target_artifact_hash.clone(),
        );
        runtime_config.role = self.role.clone();
        runtime_config.receipt_submit_path = snapshot.paths.receipt_submit_path.clone();
        runtime_config.transport = self.transport.clone();
        runtime_config.selected_experiment = self.selected_experiment.clone();
        runtime_config.selected_revision = self.selected_revision.clone();
        runtime_config
    }
}

#[derive(Debug, Error)]
/// Errors returned while bootstrapping or driving one browser session runtime.
pub enum BrowserSessionRuntimeError {
    /// The browser session was missing.
    #[error("browser session runtime requires an authenticated session")]
    MissingSession,
    /// One browser edge client failure.
    #[error(transparent)]
    Client(#[from] BrowserAuthClientError),
    /// One browser worker command failed.
    #[error("browser worker command failed: {0}")]
    Worker(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Summary returned after executing one training plan through the browser
/// session runtime harness.
pub struct BrowserSessionTrainingOutcome {
    /// The emitted receipt id, when the worker produced one.
    pub emitted_receipt_id: Option<String>,
    /// Receipt ids acknowledged by the edge.
    pub accepted_receipt_ids: Vec<String>,
    /// Whether at least one receipt submission was accepted.
    pub receipt_submission_accepted: bool,
    /// Whether receipt submission completed local training but deferred edge acknowledgement.
    pub receipt_submission_deferred: bool,
    /// Pending receipt count after submission/deferral.
    pub pending_receipt_count: usize,
    /// Last receipt submission error, when acknowledgement was deferred.
    pub receipt_submission_error: Option<String>,
    /// Whether the local training artifact was published through peer-visible transport.
    pub artifact_published: bool,
    /// Whether the peer-visible update announcement was published.
    pub update_announced: bool,
    /// Final runtime state after training.
    pub runtime_state: Option<crate::BrowserRuntimeState>,
    /// Final active transport after training.
    pub transport: Option<BrowserTransportKind>,
}

/// Shared browser worker/client/session state prepared from one authenticated
/// session and one browser edge snapshot.
pub struct BrowserSessionRuntimeHandle {
    /// The browser edge client.
    pub client: BrowserEdgeClient,
    /// The authenticated browser session.
    pub session: BrowserSessionState,
    /// The wrapped browser worker runtime.
    pub runtime: BrowserWorkerRuntime,
    include_leaderboard: bool,
    #[cfg(target_arch = "wasm32")]
    direct_swarm_runtime: Option<WasmBrowserSwarmRuntime>,
}

impl BrowserSessionRuntimeHandle {
    /// Starts one synchronized browser runtime from an existing authenticated
    /// session.
    pub async fn start(
        snapshot: &BrowserEdgeSnapshot,
        config: BrowserSessionRuntimeConfig,
        session: BrowserSessionState,
    ) -> Result<Self, BrowserSessionRuntimeError> {
        if session.session.is_none() {
            return Err(BrowserSessionRuntimeError::MissingSession);
        }

        let client = BrowserEdgeClient::new(
            BrowserUiBindings::new(&config.edge_base_url),
            BrowserEnrollmentConfig::for_runtime_sync(snapshot),
        );
        let mut runtime = BrowserWorkerRuntime::start(
            config.runtime_config(snapshot),
            config.capability,
            BrowserTransportStatus::from_transport_surface(&snapshot.transports),
        );
        runtime.remember_session(session.clone());
        client
            .sync_worker_runtime(&mut runtime, Some(&session), config.include_leaderboard)
            .await?;
        #[cfg(target_arch = "wasm32")]
        let direct_swarm_runtime = if config.enable_direct_swarm {
            let (direct_swarm_runtime, _) = establish_direct_swarm_runtime(&mut runtime).await;
            direct_swarm_runtime
        } else {
            None
        };

        Ok(Self {
            client,
            session,
            runtime,
            include_leaderboard: config.include_leaderboard,
            #[cfg(target_arch = "wasm32")]
            direct_swarm_runtime,
        })
    }

    /// Refreshes the runtime from the edge client using the stored session.
    pub async fn refresh(&mut self) -> Result<Vec<BrowserWorkerEvent>, BrowserSessionRuntimeError> {
        #[cfg(target_arch = "wasm32")]
        {
            let (events, hard_error) = refresh_worker_runtime_preferring_direct_swarm(
                &self.client,
                &mut self.runtime,
                Some(&self.session),
                self.direct_swarm_runtime.as_mut(),
                self.include_leaderboard,
            )
            .await;
            if let Some(error) = hard_error {
                return Err(error.into());
            }
            Ok(events)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            self.client
                .sync_worker_runtime(
                    &mut self.runtime,
                    Some(&self.session),
                    self.include_leaderboard,
                )
                .await
                .map_err(Into::into)
        }
    }

    /// Returns active head artifact bytes retained by the browser replay cache.
    pub fn active_head_artifact_bytes(&self) -> Option<(HeadId, ArtifactDescriptor, Vec<u8>)> {
        self.runtime.storage.active_head_artifact_bytes()
    }

    /// Executes one training plan and flushes any emitted receipts.
    pub async fn run_training_plan(
        &mut self,
        mut plan: BrowserTrainingPlan,
    ) -> Result<BrowserSessionTrainingOutcome, BrowserSessionRuntimeError> {
        self.refresh().await?;
        #[cfg(target_arch = "wasm32")]
        self.wait_for_direct_transport_handoff().await?;
        let artifact_published = self.publish_training_artifact(&mut plan).await?;
        let executed_plan = plan.clone();
        let training_events =
            self.runtime
                .apply_command(BrowserWorkerCommand::Train(Box::new(plan)), None, None);
        if let Some(message) = worker_error_message(&training_events) {
            return Err(BrowserSessionRuntimeError::Worker(message));
        }
        let emitted_receipt_id = training_events.iter().find_map(|event| match event {
            BrowserWorkerEvent::TrainingCompleted(result) => result
                .receipt_id
                .as_ref()
                .map(|receipt_id| receipt_id.as_str().to_owned()),
            _ => None,
        });

        let update_announced = if artifact_published {
            self.publish_training_update(&executed_plan, emitted_receipt_id.as_deref())
                .await?
        } else {
            false
        };

        let flush_events = self.client.flush_worker_receipts(&mut self.runtime).await?;
        let accepted_receipt_ids = flush_events
            .iter()
            .find_map(|event| match event {
                BrowserWorkerEvent::ReceiptsAcknowledged { receipt_ids, .. } => Some(
                    receipt_ids
                        .iter()
                        .map(|receipt_id| receipt_id.as_str().to_owned())
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        let deferred_submission = flush_events.iter().find_map(|event| match event {
            BrowserWorkerEvent::ReceiptSubmissionDeferred {
                pending_receipts,
                reason,
                ..
            } => Some((*pending_receipts, reason.clone())),
            _ => None,
        });
        let pending_receipt_count = flush_events
            .iter()
            .rev()
            .find_map(|event| match event {
                BrowserWorkerEvent::ReceiptsAcknowledged {
                    pending_receipts, ..
                }
                | BrowserWorkerEvent::ReceiptSubmissionDeferred {
                    pending_receipts, ..
                } => Some(*pending_receipts),
                _ => None,
            })
            .unwrap_or_else(|| self.runtime.storage.pending_receipts.len());

        Ok(BrowserSessionTrainingOutcome {
            receipt_submission_accepted: !accepted_receipt_ids.is_empty(),
            receipt_submission_deferred: deferred_submission.is_some(),
            pending_receipt_count,
            receipt_submission_error: deferred_submission.map(|(_, reason)| reason),
            accepted_receipt_ids,
            emitted_receipt_id,
            artifact_published,
            update_announced,
            runtime_state: self.runtime.state.clone(),
            transport: self.runtime.transport.active.clone(),
        })
    }

    async fn publish_training_artifact(
        &mut self,
        plan: &mut BrowserTrainingPlan,
    ) -> Result<bool, BrowserSessionRuntimeError> {
        let Some(contribution) = plan.contribution.as_mut() else {
            return Ok(false);
        };
        let Some(artifact) = contribution.published_artifact.clone() else {
            return Ok(false);
        };
        let peer_id = self
            .runtime
            .storage
            .stored_certificate_peer_id
            .clone()
            .ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training requires an enrolled node certificate".into(),
                )
            })?;
        if self.runtime.storage.session.session.is_none() {
            return Err(BrowserSessionRuntimeError::MissingSession);
        }

        let descriptor = artifact.descriptor;
        contribution.artifact_id = descriptor.artifact_id.clone();
        contribution.base_head_id = contribution
            .base_head_id
            .clone()
            .or_else(|| descriptor.base_head_id.clone())
            .or_else(|| self.runtime.storage.last_head_id.clone());

        #[cfg(target_arch = "wasm32")]
        {
            let chunks = artifact
                .chunks
                .into_iter()
                .map(|chunk| ArtifactChunkPayload {
                    artifact_id: descriptor.artifact_id.clone(),
                    chunk: chunk.chunk,
                    bytes: chunk.bytes,
                    generated_at: Utc::now(),
                })
                .collect::<Vec<_>>();
            let direct_swarm = self.direct_swarm_runtime.as_mut().ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training requires an active direct swarm runtime".into(),
                )
            })?;
            direct_swarm
                .publish_artifact(descriptor, chunks)
                .await
                .map_err(|error| {
                    BrowserSessionRuntimeError::Worker(format!(
                        "browser artifact publication failed for {}: {error}",
                        peer_id.as_str()
                    ))
                })?;
            contribution.artifact_published = true;
            Ok(true)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = peer_id;
            let _ = descriptor;
            Err(BrowserSessionRuntimeError::Worker(
                "browser canonical training artifact publication is only available in wasm".into(),
            ))
        }
    }

    async fn publish_training_update(
        &mut self,
        plan: &BrowserTrainingPlan,
        emitted_receipt_id: Option<&str>,
    ) -> Result<bool, BrowserSessionRuntimeError> {
        let Some(contribution) = plan.contribution.as_ref() else {
            return Ok(false);
        };
        if contribution.published_artifact.is_none() || !contribution.artifact_published {
            return Ok(false);
        }
        let Some(receipt_id) = emitted_receipt_id else {
            return Err(BrowserSessionRuntimeError::Worker(
                "browser canonical training update requires a local receipt id".into(),
            ));
        };
        let Some(lease) = plan.lease.as_ref() else {
            return Err(BrowserSessionRuntimeError::Worker(
                "browser canonical training update requires an active training lease".into(),
            ));
        };
        let peer_id = self
            .runtime
            .storage
            .stored_certificate_peer_id
            .clone()
            .ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training update requires an enrolled node certificate"
                        .into(),
                )
            })?;
        let base_head_id = contribution
            .base_head_id
            .clone()
            .or_else(|| self.runtime.storage.last_head_id.clone())
            .ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training update requires a synced base head".into(),
                )
            })?;

        #[cfg(target_arch = "wasm32")]
        {
            let config = self.runtime.config.as_ref().ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training update requires runtime config".into(),
                )
            })?;
            let overlay = ExperimentOverlaySet::new(
                config.network_id.clone(),
                plan.study_id.clone(),
                plan.experiment_id.clone(),
            )
            .map_err(|error| {
                BrowserSessionRuntimeError::Worker(format!(
                    "browser update overlay construction failed: {error}"
                ))
            })?
            .heads;
            let receipt_ids = vec![ContributionReceiptId::new(receipt_id.to_owned())];
            let receipt_root = ContentId::derive(&receipt_ids).map_err(|error| {
                BrowserSessionRuntimeError::Worker(format!(
                    "browser update receipt root derivation failed: {error}"
                ))
            })?;
            let sample_weight = contribution
                .completed_tokens
                .max(contribution.completed_examples)
                .max(contribution.completed_batches)
                .max(1) as f64;
            let announcement = UpdateEnvelopeAnnouncement {
                overlay,
                update: UpdateAnnounce {
                    peer_id: peer_id.clone(),
                    study_id: plan.study_id.clone(),
                    experiment_id: plan.experiment_id.clone(),
                    revision_id: plan.revision_id.clone(),
                    window_id: lease.window_id,
                    base_head_id,
                    lease_id: Some(lease.lease_id.clone()),
                    delta_artifact_id: contribution.artifact_id.clone(),
                    sample_weight,
                    quality_weight: 1.0,
                    norm_stats: UpdateNormStats {
                        l2_norm: 0.0,
                        max_abs: 0.0,
                        clipped: false,
                        non_finite_tensors: 0,
                    },
                    feature_sketch: None,
                    receipt_root,
                    receipt_ids,
                    providers: vec![peer_id],
                    announced_at: Utc::now(),
                },
            };
            let direct_swarm = self.direct_swarm_runtime.as_mut().ok_or_else(|| {
                BrowserSessionRuntimeError::Worker(
                    "browser canonical training update requires an active direct swarm runtime"
                        .into(),
                )
            })?;
            direct_swarm
                .publish_update(announcement)
                .await
                .map_err(|error| {
                    BrowserSessionRuntimeError::Worker(format!(
                        "browser update announcement failed: {error}"
                    ))
                })?;
            Ok(true)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = peer_id;
            let _ = base_head_id;
            let _ = receipt_id;
            let _ = lease;
            Err(BrowserSessionRuntimeError::Worker(
                "browser canonical training update publication is only available in wasm".into(),
            ))
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn wait_for_direct_transport_handoff(
        &mut self,
    ) -> Result<(), BrowserSessionRuntimeError> {
        let polls = DIRECT_TRANSPORT_HANDOFF_WAIT_MS / DIRECT_TRANSPORT_HANDOFF_POLL_MS;
        for _ in 0..polls {
            if matches!(
                self.runtime.transport.active,
                Some(BrowserTransportKind::WebRtcDirect | BrowserTransportKind::WebTransport)
            ) {
                return Ok(());
            }
            let swarm_status = self.runtime.swarm_status();
            let direct_desired = matches!(
                swarm_status.desired_transport,
                Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
            );
            if !direct_desired {
                return Ok(());
            }
            TimeoutFuture::new(DIRECT_TRANSPORT_HANDOFF_POLL_MS).await;
            self.refresh().await?;
        }
        Ok(())
    }
}

fn worker_error_message(events: &[BrowserWorkerEvent]) -> Option<String> {
    events.iter().find_map(|event| match event {
        BrowserWorkerEvent::Error { message } => Some(message.clone()),
        _ => None,
    })
}
