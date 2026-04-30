use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    ArtifactAlias, ArtifactLiveEvent, DownloadTicket, Page, PageRequest, PublicationTarget,
    PublicationTargetId, PublicationTargetKind, PublishedArtifactRecord,
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
    #[serde(default)]
    pub(crate) recent_published_artifacts: Vec<PublishedArtifactRecord>,
    #[serde(default)]
    pub(crate) recent_export_jobs: Vec<ExportJob>,
    #[serde(default)]
    pub(crate) active_download_tickets: Vec<DownloadTicket>,
    #[serde(default)]
    pub(crate) publication_events: Vec<ArtifactLiveEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
struct PublicationRegistryStateDisk {
    #[serde(default)]
    targets: Vec<PublicationTarget>,
    #[serde(default)]
    aliases: Vec<ArtifactAlias>,
    #[serde(default)]
    alias_history: Vec<ArtifactAlias>,
    #[serde(default)]
    published_artifacts: Vec<PublishedArtifactRecord>,
    #[serde(default)]
    recent_published_artifacts: Vec<PublishedArtifactRecord>,
    #[serde(default)]
    export_jobs: Vec<ExportJob>,
    #[serde(default)]
    recent_export_jobs: Vec<ExportJob>,
    #[serde(default)]
    download_tickets: Vec<DownloadTicket>,
    #[serde(default)]
    active_download_tickets: Vec<DownloadTicket>,
    #[serde(default)]
    publication_events: Vec<ArtifactLiveEvent>,
}

impl PublicationRegistryState {
    pub(crate) const MAX_RECENT_PUBLISHED_ARTIFACTS: usize = 256;
    pub(crate) const MAX_RECENT_EXPORT_JOBS: usize = 256;
    pub(crate) const MAX_ACTIVE_DOWNLOAD_TICKETS: usize = 512;

    pub(crate) fn new(root_dir: &Path) -> Self {
        Self {
            targets: vec![default_filesystem_target(root_dir)],
            aliases: Vec::new(),
            recent_published_artifacts: Vec::new(),
            recent_export_jobs: Vec::new(),
            active_download_tickets: Vec::new(),
            publication_events: Vec::new(),
        }
    }

    fn clamp_previews(&mut self) {
        clamp_recent(
            &mut self.recent_published_artifacts,
            Self::MAX_RECENT_PUBLISHED_ARTIFACTS,
            |record| record.created_at,
        );
        clamp_recent(
            &mut self.recent_export_jobs,
            Self::MAX_RECENT_EXPORT_JOBS,
            |job| job.queued_at,
        );
        clamp_recent(
            &mut self.active_download_tickets,
            Self::MAX_ACTIVE_DOWNLOAD_TICKETS,
            |ticket| ticket.issued_at,
        );
        if self.publication_events.len() > DEFAULT_PUBLICATION_EVENT_HISTORY {
            let drop_count = self.publication_events.len() - DEFAULT_PUBLICATION_EVENT_HISTORY;
            self.publication_events.drain(..drop_count);
        }
    }
}

impl PublicationRegistryStateDisk {
    fn into_state(self, root_dir: &Path) -> PublicationRegistryState {
        let mut state = PublicationRegistryState {
            targets: if self.targets.is_empty() {
                vec![default_filesystem_target(root_dir)]
            } else {
                self.targets
            },
            aliases: self.aliases,
            recent_published_artifacts: if self.recent_published_artifacts.is_empty() {
                self.published_artifacts
            } else {
                self.recent_published_artifacts
            },
            recent_export_jobs: if self.recent_export_jobs.is_empty() {
                self.export_jobs
            } else {
                self.recent_export_jobs
            },
            active_download_tickets: if self.active_download_tickets.is_empty() {
                self.download_tickets
            } else {
                self.active_download_tickets
            },
            publication_events: self.publication_events,
        };
        state.clamp_previews();
        state
    }
}

impl PublicationStore {
    fn open_impl(
        root_dir: impl AsRef<Path>,
        prune_expired_records: bool,
    ) -> Result<Self, PublishError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(root_dir.join("mirror"))?;
        let state_path = state_path(&root_dir);
        let mut state = if state_path.exists() {
            match serde_json::from_slice::<PublicationRegistryStateDisk>(&fs::read(&state_path)?) {
                Ok(legacy) => {
                    let legacy_alias_history = legacy.alias_history.clone();
                    let legacy_published = legacy.published_artifacts.clone();
                    let legacy_jobs = legacy.export_jobs.clone();
                    let mut state = legacy.into_state(&root_dir);
                    let mut store = Self {
                        root_dir: root_dir.clone(),
                        state: state.clone(),
                    };
                    if !legacy_alias_history.is_empty() && !alias_history_path(&root_dir).exists() {
                        store.replace_alias_history(&legacy_alias_history)?;
                    }
                    if !legacy_published.is_empty()
                        && !published_artifact_history_path(&root_dir).exists()
                    {
                        store.replace_published_artifacts(&legacy_published)?;
                        state = store.state.clone();
                    }
                    if !legacy_jobs.is_empty() && !export_job_history_path(&root_dir).exists() {
                        store.replace_export_jobs(&legacy_jobs)?;
                        state = store.state.clone();
                    }
                    state
                }
                Err(error) => recover_state_from_history(&root_dir, error)?,
            }
        } else {
            PublicationRegistryState::new(&root_dir)
        };
        if state.targets.is_empty() {
            state.targets.push(default_filesystem_target(&root_dir));
        }
        state.clamp_previews();
        let mut store = Self { root_dir, state };
        if prune_expired_records {
            store.prune_expired_records()?;
        }
        Ok(store)
    }

    /// Opens or creates a publication store rooted at the provided path.
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self, PublishError> {
        Self::open_impl(root_dir, true)
    }

    /// Opens or creates a publication store without pruning or validating publication history.
    ///
    /// This is only appropriate for read-only surfaces that can tolerate stale/corrupt
    /// publication history and provide their own fallback behavior.
    pub fn open_without_prune(root_dir: impl AsRef<Path>) -> Result<Self, PublishError> {
        Self::open_impl(root_dir, false)
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
    pub fn published_artifacts(&self) -> Result<Vec<PublishedArtifactRecord>, PublishError> {
        self.load_published_artifacts()
    }

    /// Returns export jobs across all experiments.
    pub fn export_jobs(&self) -> Result<Vec<ExportJob>, PublishError> {
        self.load_export_jobs()
    }

    /// Removes expired published artifacts and download tickets from the registry.
    pub fn prune_expired_records(&mut self) -> Result<(), PublishError> {
        let now = Utc::now();
        self.state
            .active_download_tickets
            .retain(|ticket| ticket.expires_at > now);
        let existing_records = self.load_published_artifacts()?;
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
        self.replace_published_artifacts(&retained)?;
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
        let mut state = self.state.clone();
        state.clamp_previews();
        let bytes = serde_json::to_vec_pretty(&state)?;
        let state_path = state_path(&self.root_dir);
        let tmp_path = self.root_dir.join(format!(
            ".{}.{}.tmp",
            state_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("registry.json"),
            std::process::id()
        ));
        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.flush()?;
            file.sync_all()?;
        }
        if let Err(error) = fs::rename(&tmp_path, &state_path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(error.into());
        }
        Ok(())
    }

    pub(crate) fn push_live_event(&mut self, event: ArtifactLiveEvent) {
        self.state.publication_events.push(event);
        self.state.clamp_previews();
    }

    pub(crate) fn load_alias_history(&self) -> Result<Vec<ArtifactAlias>, PublishError> {
        load_jsonl(alias_history_path(&self.root_dir))
    }

    pub(crate) fn load_alias_history_page(
        &self,
        page: PageRequest,
    ) -> Result<Page<ArtifactAlias>, PublishError> {
        self.load_alias_history_page_filtered(page, |_| true)
    }

    pub(crate) fn load_alias_history_page_filtered<F>(
        &self,
        page: PageRequest,
        matches: F,
    ) -> Result<Page<ArtifactAlias>, PublishError>
    where
        F: Fn(&ArtifactAlias) -> bool,
    {
        load_jsonl_page(alias_history_path(&self.root_dir), page, matches)
    }

    pub(crate) fn record_alias_history(
        &mut self,
        alias: &ArtifactAlias,
    ) -> Result<(), PublishError> {
        append_jsonl(alias_history_path(&self.root_dir), alias)
    }

    pub(crate) fn replace_alias_history(
        &mut self,
        aliases: &[ArtifactAlias],
    ) -> Result<(), PublishError> {
        write_jsonl(alias_history_path(&self.root_dir), aliases)
    }

    pub(crate) fn load_published_artifacts(
        &self,
    ) -> Result<Vec<PublishedArtifactRecord>, PublishError> {
        let path = published_artifact_history_path(&self.root_dir);
        if path.exists() {
            return load_jsonl(path);
        }
        Ok(self.state.recent_published_artifacts.clone())
    }

    pub(crate) fn load_published_artifacts_page(
        &self,
        page: PageRequest,
    ) -> Result<Page<PublishedArtifactRecord>, PublishError> {
        let path = published_artifact_history_path(&self.root_dir);
        if path.exists() {
            return load_jsonl_page(path, page, |_| true);
        }
        Ok(page_from_slice(
            self.state.recent_published_artifacts.clone(),
            page,
        ))
    }

    pub(crate) fn replace_published_artifacts(
        &mut self,
        records: &[PublishedArtifactRecord],
    ) -> Result<(), PublishError> {
        write_jsonl(published_artifact_history_path(&self.root_dir), records)?;
        self.state.recent_published_artifacts = recent_preview(
            records,
            PublicationRegistryState::MAX_RECENT_PUBLISHED_ARTIFACTS,
            |record| record.created_at,
        );
        Ok(())
    }

    pub(crate) fn load_export_jobs(&self) -> Result<Vec<ExportJob>, PublishError> {
        let path = export_job_history_path(&self.root_dir);
        if path.exists() {
            return load_jsonl(path);
        }
        Ok(self.state.recent_export_jobs.clone())
    }

    pub(crate) fn load_export_jobs_page(
        &self,
        page: PageRequest,
    ) -> Result<Page<ExportJob>, PublishError> {
        let path = export_job_history_path(&self.root_dir);
        if path.exists() {
            return load_jsonl_page(path, page, |_| true);
        }
        Ok(page_from_slice(self.state.recent_export_jobs.clone(), page))
    }

    pub(crate) fn replace_export_jobs(&mut self, jobs: &[ExportJob]) -> Result<(), PublishError> {
        write_jsonl(export_job_history_path(&self.root_dir), jobs)?;
        self.state.recent_export_jobs = recent_preview(
            jobs,
            PublicationRegistryState::MAX_RECENT_EXPORT_JOBS,
            |job| job.queued_at,
        );
        Ok(())
    }
}

fn recover_state_from_history(
    root_dir: &Path,
    _error: serde_json::Error,
) -> Result<PublicationRegistryState, PublishError> {
    let mut state = PublicationRegistryState::new(root_dir);
    let alias_history = load_jsonl::<ArtifactAlias>(alias_history_path(root_dir))?;
    let published_artifacts =
        load_jsonl::<PublishedArtifactRecord>(published_artifact_history_path(root_dir))?;
    let export_jobs = load_jsonl::<ExportJob>(export_job_history_path(root_dir))?;

    let mut aliases_by_id = BTreeMap::<_, ArtifactAlias>::new();
    for alias in alias_history {
        let alias_id = alias.artifact_alias_id.clone();
        match aliases_by_id.get(&alias_id) {
            Some(existing) if existing.resolved_at >= alias.resolved_at => {}
            _ => {
                aliases_by_id.insert(alias_id, alias);
            }
        }
    }

    state.aliases = aliases_by_id.into_values().collect();
    state
        .aliases
        .sort_by(|left, right| left.artifact_alias_id.cmp(&right.artifact_alias_id));
    state.recent_published_artifacts = recent_preview(
        &published_artifacts,
        PublicationRegistryState::MAX_RECENT_PUBLISHED_ARTIFACTS,
        |record| record.created_at,
    );
    state.recent_export_jobs = recent_preview(
        &export_jobs,
        PublicationRegistryState::MAX_RECENT_EXPORT_JOBS,
        |job| job.queued_at,
    );
    Ok(state)
}

pub(crate) fn state_path(root_dir: &Path) -> PathBuf {
    root_dir.join("registry.json")
}

pub(crate) fn alias_history_path(root_dir: &Path) -> PathBuf {
    root_dir.join("alias-history.jsonl")
}

pub(crate) fn published_artifact_history_path(root_dir: &Path) -> PathBuf {
    root_dir.join("published-artifacts.jsonl")
}

pub(crate) fn export_job_history_path(root_dir: &Path) -> PathBuf {
    root_dir.join("export-jobs.jsonl")
}

fn load_jsonl<T>(path: PathBuf) -> Result<Vec<T>, PublishError>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let reader = BufReader::new(fs::File::open(path)?);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        entries.push(serde_json::from_str(&line)?);
    }
    Ok(entries)
}

fn load_jsonl_page<T, F>(
    path: PathBuf,
    page: PageRequest,
    matches: F,
) -> Result<Page<T>, PublishError>
where
    T: for<'de> Deserialize<'de>,
    F: Fn(&T) -> bool,
{
    let page = page.normalized();
    if !path.exists() {
        return Ok(Page::new(Vec::new(), page, 0));
    }
    let reader = BufReader::new(fs::File::open(path)?);
    let mut items = Vec::new();
    let mut total = 0usize;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry = serde_json::from_str::<T>(&line)?;
        if !matches(&entry) {
            continue;
        }
        if total >= page.offset && items.len() < page.limit {
            items.push(entry);
        }
        total += 1;
    }
    Ok(Page::new(items, page, total))
}

fn page_from_slice<T>(items: Vec<T>, page: PageRequest) -> Page<T> {
    let page = page.normalized();
    let total = items.len();
    let items = items
        .into_iter()
        .skip(page.offset)
        .take(page.limit)
        .collect();
    Page::new(items, page, total)
}

fn append_jsonl<T>(path: PathBuf, entry: &T) -> Result<(), PublishError>
where
    T: Serialize,
{
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, entry)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn write_jsonl<T>(path: PathBuf, entries: &[T]) -> Result<(), PublishError>
where
    T: Serialize,
{
    let mut writer = BufWriter::new(fs::File::create(path)?);
    for entry in entries {
        serde_json::to_writer(&mut writer, entry)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn recent_preview<T, F>(entries: &[T], max_entries: usize, key: F) -> Vec<T>
where
    T: Clone,
    F: Fn(&T) -> chrono::DateTime<Utc>,
{
    let mut retained = entries.to_vec();
    retained.sort_by_key(|left| key(left));
    if retained.len() > max_entries {
        retained.drain(0..retained.len() - max_entries);
    }
    retained
}

fn clamp_recent<T, F>(entries: &mut Vec<T>, max_entries: usize, key: F)
where
    F: Fn(&T) -> chrono::DateTime<Utc>,
{
    entries.sort_by_key(|left| key(left));
    if entries.len() > max_entries {
        entries.drain(0..entries.len() - max_entries);
    }
}
