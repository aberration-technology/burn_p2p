#[cfg(feature = "artifact-publish")]
use std::collections::BTreeSet;

#[cfg(feature = "artifact-publish")]
use burn_p2p::FsArtifactStore;
#[cfg(feature = "artifact-publish")]
use burn_p2p_core::{
    ArtifactLiveEvent, DownloadTicketId, EvalProtocolManifest, ExperimentId, ExportJob,
    ExportJobId, HeadId, PublishedArtifactRecord, RunId,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::{
    ArtifactAliasStatus, ArtifactBackfillRequest, ArtifactBackfillResult, ArtifactPruneRequest,
    ArtifactPruneResult, ArtifactRunSummary, ArtifactRunView, DownloadArtifact,
    DownloadTicketRequest, DownloadTicketResponse, ExportRequest, HeadArtifactView,
    PublicationStore,
};

#[cfg(feature = "artifact-publish")]
use crate::{BootstrapAdminState, operator_store::OperatorStore};

#[cfg(feature = "artifact-publish")]
impl BootstrapAdminState {
    fn fallback_head_artifact_view(
        &self,
        heads: &[burn_p2p::HeadDescriptor],
        head_eval_reports: &[burn_p2p_core::HeadEvalReport],
        head_id: &HeadId,
    ) -> anyhow::Result<Option<HeadArtifactView>> {
        let Some(head) = heads.iter().find(|head| &head.head_id == head_id).cloned() else {
            return Ok(None);
        };
        Ok(Some(HeadArtifactView {
            run_id: RunId::derive(&(head.experiment_id.as_str(), head.revision_id.as_str()))?,
            eval_reports: head_eval_reports
                .iter()
                .filter(|report| &report.head_id == head_id)
                .cloned()
                .collect(),
            aliases: Vec::new(),
            published_artifacts: Vec::new(),
            available_profiles: BTreeSet::new(),
            alias_history: Vec::new(),
            provider_peer_ids: self.provider_peer_ids_for_head(head_id),
            head,
        }))
    }

    /// Exports artifact alias statuses across all loaded experiments.
    pub fn export_artifact_alias_statuses(&self) -> anyhow::Result<Vec<ArtifactAliasStatus>> {
        Ok(self
            .publication_store()?
            .map(|store| store.alias_statuses())
            .transpose()?
            .unwrap_or_default())
    }

    /// Exports artifact alias statuses for one experiment.
    pub fn export_artifact_alias_statuses_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ArtifactAliasStatus>> {
        Ok(self
            .publication_store()?
            .map(|store| store.alias_statuses_for_experiment(experiment_id))
            .transpose()?
            .unwrap_or_default())
    }

    /// Exports run-scoped artifact summaries for one experiment.
    pub fn export_artifact_run_summaries(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ArtifactRunSummary>> {
        let heads = self.stored_heads()?;
        Ok(self
            .publication_store()?
            .map(|store| store.run_summaries(&heads, experiment_id))
            .transpose()?
            .unwrap_or_default())
    }

    /// Exports one run-scoped artifact detail view.
    pub fn export_artifact_run_view(
        &self,
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> anyhow::Result<Option<ArtifactRunView>> {
        let Some(store) = self.publication_store()? else {
            return Ok(None);
        };
        let heads = self.stored_heads()?;
        let head_eval_reports = self.stored_head_eval_reports()?;
        Ok(Some(store.run_view(
            &heads,
            &head_eval_reports,
            experiment_id,
            run_id,
        )?))
    }

    /// Exports one head-scoped artifact detail view.
    pub fn export_head_artifact_view(
        &self,
        head_id: &HeadId,
    ) -> anyhow::Result<Option<HeadArtifactView>> {
        let heads = self.visible_head_descriptors();
        let head_eval_reports = self.stored_head_eval_reports()?;
        let store = match self.publication_store_without_sync() {
            Ok(Some(store)) => store,
            Ok(None) | Err(_) => {
                return self.fallback_head_artifact_view(&heads, &head_eval_reports, head_id);
            }
        };
        match store.head_view(&heads, &head_eval_reports, head_id) {
            Ok(mut view) => {
                view.provider_peer_ids = self.provider_peer_ids_for_head(head_id);
                Ok(Some(view))
            }
            Err(_) => self.fallback_head_artifact_view(&heads, &head_eval_reports, head_id),
        }
    }

    /// Queues or deduplicates one export job.
    pub fn request_artifact_export(&self, request: ExportRequest) -> anyhow::Result<ExportJob> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let heads = self.stored_heads()?;
        let head_eval_reports = self.stored_head_eval_reports()?;
        Ok(store.request_export(&artifact_store, &heads, &head_eval_reports, request)?)
    }

    /// Backfills matching aliases into publication targets.
    pub fn backfill_artifact_aliases(
        &self,
        request: ArtifactBackfillRequest,
    ) -> anyhow::Result<ArtifactBackfillResult> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let heads = self.stored_heads()?;
        let head_eval_reports = self.stored_head_eval_reports()?;
        let protocols = self.publication_protocols()?;
        Ok(store.backfill_aliases(
            &artifact_store,
            &heads,
            &head_eval_reports,
            &protocols,
            request,
        )?)
    }

    /// Prunes matching publication state without touching canonical CAS state.
    pub fn prune_artifact_publications(
        &self,
        request: ArtifactPruneRequest,
    ) -> anyhow::Result<ArtifactPruneResult> {
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.prune_matching(request)?)
    }

    /// Returns one export job by identifier.
    pub fn export_artifact_job(
        &self,
        export_job_id: &ExportJobId,
    ) -> anyhow::Result<Option<ExportJob>> {
        let Some(store) = self.publication_store()? else {
            return Ok(None);
        };
        Ok(store
            .export_jobs()?
            .into_iter()
            .find(|job| &job.export_job_id == export_job_id))
    }

    /// Exports publication records across the current store.
    pub fn export_published_artifacts(&self) -> anyhow::Result<Vec<PublishedArtifactRecord>> {
        Ok(self
            .publication_store()?
            .map(|store| store.published_artifacts())
            .transpose()?
            .unwrap_or_default())
    }

    /// Exports known publication jobs across the current store.
    pub fn export_artifact_jobs(&self) -> anyhow::Result<Vec<ExportJob>> {
        Ok(self
            .publication_store()?
            .map(|store| store.export_jobs())
            .transpose()?
            .unwrap_or_default())
    }

    /// Exports recent live artifact publication events.
    pub fn export_artifact_live_events(&self) -> anyhow::Result<Vec<ArtifactLiveEvent>> {
        Ok(self
            .publication_store()?
            .map(|store| store.live_events().to_vec())
            .unwrap_or_default())
    }

    /// Exports the latest live artifact publication event, when one exists.
    pub fn export_latest_artifact_live_event(&self) -> anyhow::Result<Option<ArtifactLiveEvent>> {
        Ok(self
            .publication_store()?
            .and_then(|store| store.latest_live_event()))
    }

    /// Issues one short-lived download ticket, exporting on demand when needed.
    pub fn request_artifact_download_ticket(
        &self,
        request: DownloadTicketRequest,
    ) -> anyhow::Result<DownloadTicketResponse> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let heads = self.stored_heads()?;
        let head_eval_reports = self.stored_head_eval_reports()?;
        Ok(store.issue_download_ticket(&artifact_store, &heads, &head_eval_reports, request)?)
    }

    /// Resolves a short-lived download ticket to a streamable artifact payload.
    pub fn resolve_artifact_download(
        &self,
        ticket_id: &DownloadTicketId,
    ) -> anyhow::Result<Option<DownloadArtifact>> {
        let Some(mut store) = self.publication_store()? else {
            return Ok(None);
        };
        Ok(Some(store.resolve_download(ticket_id)?))
    }

    /// Forces alias synchronization and expired-record pruning.
    pub fn refresh_publication_views(&self) -> anyhow::Result<()> {
        self.sync_publication_store()
    }

    fn artifact_store(&self) -> anyhow::Result<Option<FsArtifactStore>> {
        self.operator_store().artifact_store()
    }

    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>> {
        self.operator_store().publication_store()
    }

    fn publication_store_without_sync(&self) -> anyhow::Result<Option<PublicationStore>> {
        let Some(root) = self.publication_store_root.as_ref() else {
            return Ok(None);
        };
        let mut store = PublicationStore::open_without_prune(root)?;
        store.configure_targets(self.publication_targets.clone())?;
        Ok(Some(store))
    }

    pub(crate) fn sync_publication_store(&self) -> anyhow::Result<()> {
        let Some(root) = self.publication_store_root.as_ref() else {
            return Ok(());
        };
        let mut store = PublicationStore::open(root)?;
        store.configure_targets(self.publication_targets.clone())?;
        let heads = self.stored_heads()?;
        let head_eval_reports = self.stored_head_eval_reports()?;
        let protocols = self.publication_protocols()?;
        if let Some(artifact_store) = self.artifact_store()? {
            store.sync_aliases_and_eager_publications(
                &artifact_store,
                &heads,
                &head_eval_reports,
                &protocols,
            )?;
        } else {
            store.sync_aliases(&heads, &head_eval_reports, &protocols)?;
        }
        store.prune_expired_records()?;
        Ok(())
    }

    fn publication_protocols(&self) -> anyhow::Result<Vec<EvalProtocolManifest>> {
        Ok(self
            .stored_eval_protocol_manifests()?
            .iter()
            .map(|record| record.manifest.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use burn_p2p::{
        ControlPlaneSnapshot, HeadAnnouncement, HeadDescriptor, LagPolicy, LagState,
        NodeRuntimeState, NodeTelemetrySnapshot, OverlayChannel, OverlayTopic, PeerId, PeerRoleSet,
        RuntimeStatus,
    };
    use burn_p2p_core::{ExperimentId, NetworkId, RevisionId, StudyId};
    use chrono::Utc;
    use tempfile::tempdir;

    #[test]
    fn export_head_artifact_view_includes_live_runtime_head_without_publication_record() {
        let publication_store = tempdir().expect("publication store tempdir");
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let state = BootstrapAdminState {
            publication_store_root: Some(publication_store.path().to_path_buf()),
            runtime_snapshot: Some(NodeTelemetrySnapshot {
                status: RuntimeStatus::Running,
                node_state: NodeRuntimeState::IdleReady,
                slot_states: Vec::new(),
                lag_state: LagState::Current,
                head_lag_steps: 0,
                lag_policy: LagPolicy::default(),
                network_id: Some(NetworkId::new("demo")),
                local_peer_id: Some(PeerId::new("bootstrap")),
                configured_roles: PeerRoleSet::default_trainer(),
                connected_peers: 1,
                connected_peer_ids: BTreeSet::new(),
                observed_peer_ids: BTreeSet::new(),
                known_peer_addresses: BTreeSet::new(),
                runtime_boundary: None,
                listen_addresses: Vec::new(),
                control_plane: ControlPlaneSnapshot {
                    head_announcements: vec![HeadAnnouncement {
                        overlay: OverlayTopic::experiment(
                            NetworkId::new("demo"),
                            StudyId::new("study"),
                            ExperimentId::new("exp"),
                            OverlayChannel::Heads,
                        )
                        .expect("heads overlay"),
                        provider_peer_id: Some(PeerId::new("mirror")),
                        head: runtime_head.clone(),
                        announced_at: now,
                    }],
                    ..ControlPlaneSnapshot::default()
                },
                recent_events: Vec::new(),
                last_snapshot_peer_id: None,
                last_snapshot: None,
                admitted_peers: BTreeMap::new(),
                rejected_peers: BTreeMap::new(),
                peer_reputation: BTreeMap::new(),
                minimum_revocation_epoch: None,
                trust_bundle: None,
                in_flight_transfers: BTreeMap::new(),
                request_failures: Vec::new(),
                robustness_policy: None,
                latest_cohort_robustness: None,
                trust_scores: Vec::new(),
                canary_reports: Vec::new(),
                applied_control_cert_ids: BTreeSet::new(),
                effective_limit_profile: None,
                last_error: None,
                started_at: now,
                updated_at: now,
            }),
            ..BootstrapAdminState::default()
        };

        let view = state
            .export_head_artifact_view(&runtime_head.head_id)
            .expect("export head artifact view")
            .expect("head artifact view should be present");
        assert_eq!(view.head.head_id, runtime_head.head_id);
        assert!(view.published_artifacts.is_empty());
        assert_eq!(view.provider_peer_ids, vec![PeerId::new("mirror")]);
    }

    #[test]
    fn export_head_artifact_view_includes_directly_registered_live_head_without_publication_record()
    {
        let publication_store = tempdir().expect("publication store tempdir");
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let mut state = BootstrapAdminState {
            publication_store_root: Some(publication_store.path().to_path_buf()),
            ..BootstrapAdminState::default()
        };
        state.register_live_head_announcement(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("demo"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new("mirror")),
            head: runtime_head.clone(),
            announced_at: now,
        });

        let view = state
            .export_head_artifact_view(&runtime_head.head_id)
            .expect("export head artifact view")
            .expect("head artifact view should be present");
        assert_eq!(view.head.head_id, runtime_head.head_id);
        assert!(view.published_artifacts.is_empty());
        assert_eq!(view.provider_peer_ids, vec![PeerId::new("mirror")]);
    }

    #[test]
    fn export_head_artifact_view_falls_back_when_publication_history_is_corrupt() {
        let publication_store = tempdir().expect("publication store tempdir");
        std::fs::write(
            publication_store.path().join("published-artifacts.jsonl"),
            "{",
        )
        .expect("write corrupt publication history");
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let mut state = BootstrapAdminState {
            publication_store_root: Some(publication_store.path().to_path_buf()),
            ..BootstrapAdminState::default()
        };
        state.register_live_head_announcement(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("demo"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new("mirror")),
            head: runtime_head.clone(),
            announced_at: now,
        });

        let view = state
            .export_head_artifact_view(&runtime_head.head_id)
            .expect("export head artifact view")
            .expect("head artifact view should be present");
        assert_eq!(view.head.head_id, runtime_head.head_id);
        assert!(view.published_artifacts.is_empty());
        assert_eq!(view.provider_peer_ids, vec![PeerId::new("mirror")]);
    }

    #[test]
    fn export_head_artifact_view_recovers_when_registry_json_is_corrupt() {
        let publication_store = tempdir().expect("publication store tempdir");
        std::fs::write(publication_store.path().join("registry.json"), "")
            .expect("write corrupt registry");
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let mut state = BootstrapAdminState {
            publication_store_root: Some(publication_store.path().to_path_buf()),
            ..BootstrapAdminState::default()
        };
        state.register_live_head_announcement(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("demo"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new("mirror")),
            head: runtime_head.clone(),
            announced_at: now,
        });

        let view = state
            .export_head_artifact_view(&runtime_head.head_id)
            .expect("export head artifact view")
            .expect("head artifact view should be present");
        assert_eq!(view.head.head_id, runtime_head.head_id);
        assert_eq!(view.provider_peer_ids, vec![PeerId::new("mirror")]);
    }
}
