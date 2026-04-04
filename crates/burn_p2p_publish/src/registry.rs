use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    ArtifactAlias, ArtifactLiveEvent, DownloadTicket, PublicationTarget, PublicationTargetId,
    PublicationTargetKind, PublishedArtifactRecord,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    DEFAULT_PUBLICATION_EVENT_HISTORY, ExportJob, PublicationStore, PublishError,
    backends::{default_filesystem_target, normalize_target},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublicationRegistryState {
    pub(crate) targets: Vec<PublicationTarget>,
    pub(crate) aliases: Vec<ArtifactAlias>,
    pub(crate) alias_history: Vec<ArtifactAlias>,
    pub(crate) published_artifacts: Vec<PublishedArtifactRecord>,
    pub(crate) export_jobs: Vec<ExportJob>,
    pub(crate) download_tickets: Vec<DownloadTicket>,
    #[serde(default)]
    pub(crate) publication_events: Vec<ArtifactLiveEvent>,
}

impl PublicationRegistryState {
    pub(crate) fn new(root_dir: &Path) -> Self {
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

    /// Returns publication records across all experiments.
    pub fn published_artifacts(&self) -> &[PublishedArtifactRecord] {
        &self.state.published_artifacts
    }

    /// Returns export jobs across all experiments.
    pub fn export_jobs(&self) -> &[ExportJob] {
        &self.state.export_jobs
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

    pub(crate) fn target(
        &self,
        publication_target_id: &PublicationTargetId,
    ) -> Result<Option<&PublicationTarget>, PublishError> {
        Ok(self
            .state
            .targets
            .iter()
            .find(|target| &target.publication_target_id == publication_target_id))
    }

    pub(crate) fn target_local_path(
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

    pub(crate) fn delete_published_artifact(
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
                    let _ = crate::backends::delete_s3_object(target, &record.object_key);
                }
                #[cfg(not(feature = "s3"))]
                {
                    let _ = target;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn persist_state(&self) -> Result<(), PublishError> {
        fs::create_dir_all(self.root_dir.join("mirror"))?;
        let bytes = serde_json::to_vec_pretty(&self.state)?;
        let mut file = fs::File::create(state_path(&self.root_dir))?;
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }

    pub(crate) fn push_live_event(&mut self, event: ArtifactLiveEvent) {
        self.state.publication_events.push(event);
        if self.state.publication_events.len() > DEFAULT_PUBLICATION_EVENT_HISTORY {
            let drop_count =
                self.state.publication_events.len() - DEFAULT_PUBLICATION_EVENT_HISTORY;
            self.state.publication_events.drain(..drop_count);
        }
    }
}

pub(crate) fn state_path(root_dir: &Path) -> PathBuf {
    root_dir.join("registry.json")
}
