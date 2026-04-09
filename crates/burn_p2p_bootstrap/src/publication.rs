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
use crate::BootstrapAdminState;

#[cfg(feature = "artifact-publish")]
impl BootstrapAdminState {
    /// Exports artifact alias statuses across all loaded experiments.
    pub fn export_artifact_alias_statuses(&self) -> anyhow::Result<Vec<ArtifactAliasStatus>> {
        Ok(self
            .publication_store()?
            .map(|store| store.alias_statuses())
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
            .unwrap_or_default())
    }

    /// Exports run-scoped artifact summaries for one experiment.
    pub fn export_artifact_run_summaries(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ArtifactRunSummary>> {
        Ok(self
            .publication_store()?
            .map(|store| store.run_summaries(&self.head_descriptors, experiment_id))
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
        Ok(Some(store.run_view(
            &self.head_descriptors,
            &self.head_eval_reports,
            experiment_id,
            run_id,
        )?))
    }

    /// Exports one head-scoped artifact detail view.
    pub fn export_head_artifact_view(
        &self,
        head_id: &HeadId,
    ) -> anyhow::Result<Option<HeadArtifactView>> {
        let Some(store) = self.publication_store()? else {
            return Ok(None);
        };
        let mut view = store.head_view(&self.head_descriptors, &self.head_eval_reports, head_id)?;
        view.provider_peer_ids = self.provider_peer_ids_for_head(head_id);
        Ok(Some(view))
    }

    /// Queues or deduplicates one export job.
    pub fn request_artifact_export(&self, request: ExportRequest) -> anyhow::Result<ExportJob> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.request_export(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            request,
        )?)
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
        Ok(store.backfill_aliases(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            &self.publication_protocols(),
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
        Ok(self.publication_store()?.and_then(|store| {
            store
                .export_jobs()
                .iter()
                .find(|job| &job.export_job_id == export_job_id)
                .cloned()
        }))
    }

    /// Exports publication records across the current store.
    pub fn export_published_artifacts(&self) -> anyhow::Result<Vec<PublishedArtifactRecord>> {
        Ok(self
            .publication_store()?
            .map(|store| store.published_artifacts().to_vec())
            .unwrap_or_default())
    }

    /// Exports known publication jobs across the current store.
    pub fn export_artifact_jobs(&self) -> anyhow::Result<Vec<ExportJob>> {
        Ok(self
            .publication_store()?
            .map(|store| store.export_jobs().to_vec())
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
        Ok(store.issue_download_ticket(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            request,
        )?)
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
        self.artifact_store_root
            .as_ref()
            .map(|root| {
                let store = FsArtifactStore::new(root.clone());
                store.ensure_layout().map_err(anyhow::Error::from)?;
                Ok(store)
            })
            .transpose()
    }

    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>> {
        self.publication_store_root
            .as_ref()
            .map(|root| {
                let mut store = PublicationStore::open(root)?;
                store.configure_targets(self.publication_targets.clone())?;
                let protocols = self.publication_protocols();
                if let Some(artifact_store) = self.artifact_store()? {
                    store.sync_aliases_and_eager_publications(
                        &artifact_store,
                        &self.head_descriptors,
                        &self.head_eval_reports,
                        &protocols,
                    )?;
                } else {
                    store.sync_aliases(
                        &self.head_descriptors,
                        &self.head_eval_reports,
                        &protocols,
                    )?;
                }
                Ok(store)
            })
            .transpose()
    }

    pub(crate) fn sync_publication_store(&self) -> anyhow::Result<()> {
        let Some(root) = self.publication_store_root.as_ref() else {
            return Ok(());
        };
        let mut store = PublicationStore::open(root)?;
        store.configure_targets(self.publication_targets.clone())?;
        let protocols = self.publication_protocols();
        if let Some(artifact_store) = self.artifact_store()? {
            store.sync_aliases_and_eager_publications(
                &artifact_store,
                &self.head_descriptors,
                &self.head_eval_reports,
                &protocols,
            )?;
        } else {
            store.sync_aliases(&self.head_descriptors, &self.head_eval_reports, &protocols)?;
        }
        store.prune_expired_records()?;
        Ok(())
    }

    fn publication_protocols(&self) -> Vec<EvalProtocolManifest> {
        self.eval_protocol_manifests
            .iter()
            .map(|record| record.manifest.clone())
            .collect()
    }
}
