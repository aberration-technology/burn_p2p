use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use burn_p2p_core::{AssignmentLease, ContentId, DatasetView, MicroShard, MicroShardId};
use serde::{Deserialize, Serialize};

use crate::{
    BurnDataLoaderAdapter, DataloaderError, DatasetRegistration, MicroShardPlan,
    ShardFetchManifest, adapters, fetch::verify_bytes,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a cached micro shard.
pub struct CachedMicroShard {
    /// The microshard.
    pub microshard: MicroShard,
    /// The path.
    pub path: PathBuf,
    /// The content hash.
    pub content_hash: ContentId,
    /// The bytes len.
    pub bytes_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a shard cache.
pub struct ShardCache {
    /// The root.
    pub root: PathBuf,
}

impl ShardCache {
    /// Creates a new value.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Performs the ensure layout operation.
    pub fn ensure_layout(&self, dataset_view: &DatasetView) -> Result<PathBuf, DataloaderError> {
        let path = self.dataset_dir(dataset_view);
        fs::create_dir_all(&path).map_err(|error| DataloaderError::Io(error.to_string()))?;
        Ok(path)
    }

    /// Performs the dataset dir operation.
    pub fn dataset_dir(&self, dataset_view: &DatasetView) -> PathBuf {
        self.root.join(dataset_view.dataset_view_id.as_str())
    }

    /// Performs the manifest path operation.
    pub fn manifest_path(&self, dataset_view: &DatasetView) -> PathBuf {
        self.dataset_dir(dataset_view).join("fetch-manifest.json")
    }

    /// Performs the shard path operation.
    pub fn shard_path(&self, dataset_view: &DatasetView, microshard: &MicroShard) -> PathBuf {
        self.dataset_dir(dataset_view)
            .join(format!("{:05}.bin", microshard.ordinal))
    }

    /// Performs the store fetch manifest operation.
    pub fn store_fetch_manifest(
        &self,
        dataset_view: &DatasetView,
        manifest: &ShardFetchManifest,
    ) -> Result<PathBuf, DataloaderError> {
        self.ensure_layout(dataset_view)?;
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| DataloaderError::Io(error.to_string()))?;
        let path = self.manifest_path(dataset_view);
        atomic_write(&path, &bytes)?;
        Ok(path)
    }

    /// Performs the load fetch manifest operation.
    pub fn load_fetch_manifest(
        &self,
        dataset_view: &DatasetView,
    ) -> Result<ShardFetchManifest, DataloaderError> {
        let bytes = fs::read(self.manifest_path(dataset_view))
            .map_err(|error| DataloaderError::Io(error.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|error| DataloaderError::Io(error.to_string()))
    }

    /// Fetches the lease microshards.
    pub fn fetch_lease_microshards(
        &self,
        registration: &DatasetRegistration,
        microshard_plan: &MicroShardPlan,
        lease: &AssignmentLease,
    ) -> Result<Vec<CachedMicroShard>, DataloaderError> {
        self.ensure_layout(&registration.view)?;
        let fetch_manifest = self.fetch_manifest(registration, microshard_plan)?;
        let by_id = microshard_plan
            .microshards
            .iter()
            .cloned()
            .map(|microshard| (microshard.microshard_id.clone(), microshard))
            .collect::<BTreeMap<_, _>>();

        lease
            .microshards
            .iter()
            .map(|microshard_id| {
                let microshard = by_id.get(microshard_id).cloned().ok_or_else(|| {
                    DataloaderError::UnknownLeasedMicroShard(microshard_id.clone())
                })?;
                let entry = fetch_manifest
                    .entry_for_microshard(microshard_id)
                    .ok_or_else(|| DataloaderError::MissingFetchEntry(microshard_id.clone()))?;
                let path = self.shard_path(&registration.view, &microshard);

                if !path.exists() {
                    let bytes =
                        adapters::fetch_entry_bytes(&registration.upstream, &entry.locator)?;
                    verify_bytes(&microshard.microshard_id, entry, &bytes)?;
                    atomic_write(&path, &bytes)?;
                }

                let bytes =
                    fs::read(&path).map_err(|error| DataloaderError::Io(error.to_string()))?;
                verify_bytes(&microshard.microshard_id, entry, &bytes)?;

                Ok(CachedMicroShard {
                    microshard,
                    path,
                    content_hash: entry.content_hash.clone(),
                    bytes_len: entry.bytes_len,
                })
            })
            .collect()
    }

    /// Performs the evict except operation.
    pub fn evict_except(
        &self,
        dataset_view: &DatasetView,
        keep_microshard_ids: &[MicroShardId],
    ) -> Result<usize, DataloaderError> {
        let dataset_dir = self.dataset_dir(dataset_view);
        if !dataset_dir.exists() {
            return Ok(0);
        }
        let fetch_manifest = self.load_fetch_manifest(dataset_view)?;
        let keep_locators = fetch_manifest
            .entries
            .iter()
            .filter(|entry| keep_microshard_ids.contains(&entry.microshard_id))
            .map(|entry| entry.locator.clone())
            .collect::<Vec<_>>();

        let mut removed = 0;
        for entry in
            fs::read_dir(dataset_dir).map_err(|error| DataloaderError::Io(error.to_string()))?
        {
            let entry = entry.map_err(|error| DataloaderError::Io(error.to_string()))?;
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if file_name == "fetch-manifest.json" {
                continue;
            }
            if keep_locators.contains(&file_name.to_owned()) {
                continue;
            }

            fs::remove_file(&path).map_err(|error| DataloaderError::Io(error.to_string()))?;
            removed += 1;
        }

        Ok(removed)
    }

    fn fetch_manifest(
        &self,
        registration: &DatasetRegistration,
        microshard_plan: &MicroShardPlan,
    ) -> Result<ShardFetchManifest, DataloaderError> {
        let manifest_path = self.manifest_path(&registration.view);
        if manifest_path.exists() {
            return self.load_fetch_manifest(&registration.view);
        }

        let bytes = adapters::fetch_manifest_bytes(&registration.upstream)?;
        let manifest: ShardFetchManifest = serde_json::from_slice(&bytes)
            .map_err(|error| DataloaderError::Io(error.to_string()))?;

        if manifest.dataset_view_id != registration.view.dataset_view_id {
            return Err(DataloaderError::Io(
                "fetch manifest dataset view does not match registration".into(),
            ));
        }
        let planned_ids = microshard_plan
            .microshards
            .iter()
            .map(|microshard| microshard.microshard_id.as_str())
            .collect::<Vec<_>>();
        if manifest
            .entries
            .iter()
            .any(|entry| !planned_ids.contains(&entry.microshard_id.as_str()))
        {
            return Err(DataloaderError::Io(
                "fetch manifest contains microshards not present in the microshard plan".into(),
            ));
        }

        self.store_fetch_manifest(&registration.view, &manifest)?;
        Ok(manifest)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a cached micro shard loader.
pub struct CachedMicroShardLoader {
    dataset_view: DatasetView,
    registration: DatasetRegistration,
    microshard_plan: MicroShardPlan,
    cache: ShardCache,
}

impl CachedMicroShardLoader {
    /// Creates a new value.
    pub fn new(
        registration: DatasetRegistration,
        microshard_plan: MicroShardPlan,
        cache: ShardCache,
    ) -> Self {
        Self {
            dataset_view: registration.view.clone(),
            registration,
            microshard_plan,
            cache,
        }
    }
}

impl<B> BurnDataLoaderAdapter<B> for CachedMicroShardLoader {
    type Batch = CachedMicroShard;
    type Error = DataloaderError;

    fn dataset_view(&self) -> &DatasetView {
        &self.dataset_view
    }

    fn load_lease(&mut self, lease: &AssignmentLease) -> Result<Vec<Self::Batch>, Self::Error> {
        self.cache
            .fetch_lease_microshards(&self.registration, &self.microshard_plan, lease)
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), DataloaderError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| DataloaderError::Io(error.to_string()))?;
    }

    let temp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("bin")
    ));
    let mut file =
        fs::File::create(&temp_path).map_err(|error| DataloaderError::Io(error.to_string()))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| DataloaderError::Io(error.to_string()))?;
    fs::rename(&temp_path, path).map_err(|error| DataloaderError::Io(error.to_string()))
}
