#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    AssignmentLease, ContentId, DataReceipt, DatasetManifest, DatasetView, ExperimentId, LeaseId,
    MicroShard, MicroShardId, NetworkId, PeerId, RevisionId, StudyId, WindowId, WorkDisposition,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum DataloaderError {
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("i/o error: {0}")]
    Io(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("target microshard bytes must be greater than zero")]
    InvalidTargetMicroshardBytes,
    #[error("lease duration seconds must be greater than zero")]
    InvalidLeaseDuration,
    #[error("budget work units must be greater than zero")]
    InvalidBudget,
    #[error("no microshards are available for planning")]
    NoMicroshards,
    #[error("fetch manifest is missing entry for microshard {0}")]
    MissingFetchEntry(MicroShardId),
    #[error("leased microshard {0} was not found in the microshard plan")]
    UnknownLeasedMicroShard(MicroShardId),
    #[error("cached microshard {microshard_id} failed hash verification")]
    HashMismatch { microshard_id: MicroShardId },
    #[error("adapter {0:?} is not supported by the disk cache fetch path")]
    UnsupportedAdapter(UpstreamAdapter),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpstreamAdapter {
    Hf {
        dataset: String,
        config: Option<String>,
        split: String,
        streaming: bool,
        num_shards: Option<u32>,
    },
    Http {
        base_url: String,
    },
    Local {
        root: String,
    },
    S3 {
        bucket: String,
        prefix: String,
    },
    Private {
        scheme: String,
        locator: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRegistration {
    pub manifest: DatasetManifest,
    pub view: DatasetView,
    pub upstream: UpstreamAdapter,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSizing {
    pub total_examples: u64,
    pub total_tokens: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardPlannerConfig {
    pub target_microshard_bytes: u64,
    pub min_microshards: u32,
    pub max_microshards: u32,
}

impl Default for MicroShardPlannerConfig {
    fn default() -> Self {
        Self {
            target_microshard_bytes: 64 * 1024 * 1024,
            min_microshards: 1,
            max_microshards: 16_384,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardPlan {
    pub dataset_view: DatasetView,
    pub sizing: DatasetSizing,
    pub microshards: Vec<MicroShard>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardPlanner {
    pub config: MicroShardPlannerConfig,
}

impl MicroShardPlanner {
    pub fn new(config: MicroShardPlannerConfig) -> Result<Self, DataloaderError> {
        if config.target_microshard_bytes == 0 {
            return Err(DataloaderError::InvalidTargetMicroshardBytes);
        }

        Ok(Self { config })
    }

    pub fn plan(
        &self,
        dataset_view: &DatasetView,
        sizing: DatasetSizing,
    ) -> Result<MicroShardPlan, DataloaderError> {
        let shard_count = shard_count_for(&self.config, &sizing);
        let shard_count_u64 = u64::from(shard_count);
        let mut microshards = Vec::with_capacity(shard_count as usize);

        for ordinal in 0..shard_count {
            let ordinal_u64 = u64::from(ordinal);
            let examples =
                evenly_distributed_share(sizing.total_examples, shard_count_u64, ordinal_u64);
            let tokens =
                evenly_distributed_share(sizing.total_tokens, shard_count_u64, ordinal_u64);
            let bytes = evenly_distributed_share(sizing.total_bytes, shard_count_u64, ordinal_u64);

            let microshard_id = MicroShardId::derive(&(
                dataset_view.dataset_view_id.as_str(),
                ordinal,
                examples,
                tokens,
                bytes,
            ))?;

            microshards.push(MicroShard {
                microshard_id,
                dataset_view_id: dataset_view.dataset_view_id.clone(),
                ordinal,
                estimated_examples: examples,
                estimated_tokens: tokens,
                estimated_bytes: bytes,
            });
        }

        Ok(MicroShardPlan {
            dataset_view: dataset_view.clone(),
            sizing,
            microshards,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCostModel {
    pub tokens_per_work_unit: u64,
    pub examples_per_work_unit: u64,
    pub bytes_per_work_unit: u64,
    pub minimum_work_units: u64,
}

impl Default for ShardCostModel {
    fn default() -> Self {
        Self {
            tokens_per_work_unit: 1_024,
            examples_per_work_unit: 16,
            bytes_per_work_unit: 256 * 1_024,
            minimum_work_units: 1,
        }
    }
}

impl ShardCostModel {
    pub fn estimate_work_units(&self, microshard: &MicroShard) -> u64 {
        let token_units = ceil_div(
            microshard.estimated_tokens.max(1),
            self.tokens_per_work_unit.max(1),
        );
        let example_units = ceil_div(
            microshard.estimated_examples.max(1),
            self.examples_per_work_unit.max(1),
        );
        let byte_units = ceil_div(
            microshard.estimated_bytes.max(1),
            self.bytes_per_work_unit.max(1),
        );

        token_units
            .max(example_units)
            .max(byte_units)
            .max(self.minimum_work_units.max(1))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePlannerConfig {
    pub lease_duration_seconds: i64,
    pub max_microshards_per_lease: usize,
    pub cost_model: ShardCostModel,
}

impl Default for LeasePlannerConfig {
    fn default() -> Self {
        Self {
            lease_duration_seconds: 300,
            max_microshards_per_lease: 128,
            cost_model: ShardCostModel::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSelection {
    pub microshards: Vec<MicroShard>,
    pub budget_work_units: u64,
    pub estimated_work_units: u64,
    pub estimated_examples: u64,
    pub estimated_tokens: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedLease {
    pub lease: AssignmentLease,
    pub selection: LeaseSelection,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePlanner {
    pub config: LeasePlannerConfig,
}

impl LeasePlanner {
    pub fn new(config: LeasePlannerConfig) -> Result<Self, DataloaderError> {
        if config.lease_duration_seconds <= 0 {
            return Err(DataloaderError::InvalidLeaseDuration);
        }

        Ok(Self { config })
    }

    pub fn select_microshards(
        &self,
        peer_id: &PeerId,
        budget_work_units: u64,
        microshards: &[MicroShard],
    ) -> Result<LeaseSelection, DataloaderError> {
        if budget_work_units == 0 {
            return Err(DataloaderError::InvalidBudget);
        }
        if microshards.is_empty() {
            return Err(DataloaderError::NoMicroshards);
        }

        let mut ranked = microshards
            .iter()
            .cloned()
            .map(|microshard| {
                let score =
                    ContentId::derive(&(peer_id.as_str(), microshard.microshard_id.as_str()))?;
                Ok::<_, DataloaderError>((score, microshard))
            })
            .collect::<Result<Vec<_>, _>>()?;

        ranked.sort_by(|left, right| {
            right
                .0
                .as_str()
                .cmp(left.0.as_str())
                .then(left.1.ordinal.cmp(&right.1.ordinal))
        });

        let mut selected = Vec::new();
        let mut estimated_work_units = 0_u64;
        let mut estimated_examples = 0_u64;
        let mut estimated_tokens = 0_u64;
        let mut estimated_bytes = 0_u64;

        for (_, microshard) in ranked {
            if selected.len() >= self.config.max_microshards_per_lease {
                break;
            }

            let shard_cost = self.config.cost_model.estimate_work_units(&microshard);
            let would_fit = estimated_work_units + shard_cost <= budget_work_units;

            if would_fit || selected.is_empty() {
                estimated_work_units += shard_cost;
                estimated_examples += microshard.estimated_examples;
                estimated_tokens += microshard.estimated_tokens;
                estimated_bytes += microshard.estimated_bytes;
                selected.push(microshard);
            }
        }

        Ok(LeaseSelection {
            microshards: selected,
            budget_work_units,
            estimated_work_units,
            estimated_examples,
            estimated_tokens,
            estimated_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_lease(
        &self,
        network_id: NetworkId,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        dataset_view: &DatasetView,
        peer_id: PeerId,
        window_id: WindowId,
        granted_at: DateTime<Utc>,
        budget_work_units: u64,
        microshards: &[MicroShard],
    ) -> Result<PlannedLease, DataloaderError> {
        let selection = self.select_microshards(&peer_id, budget_work_units, microshards)?;
        let microshard_ids = selection
            .microshards
            .iter()
            .map(|microshard| microshard.microshard_id.clone())
            .collect::<Vec<_>>();

        let expires_at = granted_at + Duration::seconds(self.config.lease_duration_seconds);
        let lease_id = LeaseId::derive(&(
            network_id.as_str(),
            study_id.as_str(),
            experiment_id.as_str(),
            revision_id.as_str(),
            peer_id.as_str(),
            dataset_view.dataset_view_id.as_str(),
            window_id.0,
            microshard_ids
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        let assignment_hash = ContentId::derive(&(
            lease_id.as_str(),
            network_id.as_str(),
            study_id.as_str(),
            experiment_id.as_str(),
            revision_id.as_str(),
            peer_id.as_str(),
            dataset_view.dataset_view_id.as_str(),
            window_id.0,
            granted_at,
            expires_at,
            selection.budget_work_units,
            microshard_ids
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        let lease = AssignmentLease {
            lease_id,
            network_id,
            study_id,
            experiment_id,
            revision_id,
            peer_id,
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            window_id,
            granted_at,
            expires_at,
            budget_work_units: selection.budget_work_units,
            microshards: microshard_ids,
            assignment_hash,
        };

        Ok(PlannedLease { lease, selection })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCache {
    leases_by_window: BTreeMap<WindowId, BTreeMap<PeerId, AssignmentLease>>,
}

impl LeaseCache {
    pub fn insert(&mut self, lease: AssignmentLease) -> Option<AssignmentLease> {
        self.leases_by_window
            .entry(lease.window_id)
            .or_default()
            .insert(lease.peer_id.clone(), lease)
    }

    pub fn get(&self, window_id: WindowId, peer_id: &PeerId) -> Option<&AssignmentLease> {
        self.leases_by_window.get(&window_id)?.get(peer_id)
    }

    pub fn leases_for_window(
        &self,
        window_id: WindowId,
    ) -> Option<&BTreeMap<PeerId, AssignmentLease>> {
        self.leases_by_window.get(&window_id)
    }

    pub fn evict_before(&mut self, earliest_window: WindowId) -> usize {
        let original_len = self.leases_by_window.len();
        self.leases_by_window
            .retain(|window_id, _| *window_id >= earliest_window);
        original_len - self.leases_by_window.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAwareSampler {
    ordered_microshards: Vec<MicroShardId>,
    next_index: usize,
}

impl ShardAwareSampler {
    pub fn from_lease(lease: &AssignmentLease) -> Result<Self, DataloaderError> {
        let mut ordered_scored = lease
            .microshards
            .iter()
            .cloned()
            .map(|microshard_id| {
                let score = ContentId::derive(&(lease.window_id.0, microshard_id.as_str()))?;
                Ok::<_, DataloaderError>((score, microshard_id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        ordered_scored.sort_by(|left, right| {
            right
                .0
                .as_str()
                .cmp(left.0.as_str())
                .then(left.1.as_str().cmp(right.1.as_str()))
        });
        let ordered_microshards = ordered_scored
            .into_iter()
            .map(|(_, microshard_id)| microshard_id)
            .collect();

        Ok(Self {
            ordered_microshards,
            next_index: 0,
        })
    }

    pub fn ordered_microshards(&self) -> &[MicroShardId] {
        &self.ordered_microshards
    }

    pub fn next_microshard(&mut self) -> Option<&MicroShardId> {
        let microshard = self.ordered_microshards.get(self.next_index)?;
        self.next_index += 1;
        Some(microshard)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataReceiptBuilder {
    pub disposition: WorkDisposition,
    pub examples_processed: u64,
    pub tokens_processed: u64,
}

impl DataReceiptBuilder {
    pub fn accepted(examples_processed: u64, tokens_processed: u64) -> Self {
        Self {
            disposition: WorkDisposition::Accepted,
            examples_processed,
            tokens_processed,
        }
    }

    pub fn build(
        &self,
        lease: &AssignmentLease,
        completed_at: DateTime<Utc>,
    ) -> Result<DataReceipt, DataloaderError> {
        let receipt_id = ContentId::derive(&(
            lease.lease_id.as_str(),
            lease.peer_id.as_str(),
            completed_at,
            self.examples_processed,
            self.tokens_processed,
            &self.disposition,
            lease
                .microshards
                .iter()
                .map(MicroShardId::as_str)
                .collect::<Vec<_>>(),
        ))?;

        Ok(DataReceipt {
            receipt_id,
            lease_id: lease.lease_id.clone(),
            peer_id: lease.peer_id.clone(),
            completed_at,
            microshards: lease.microshards.clone(),
            examples_processed: self.examples_processed,
            tokens_processed: self.tokens_processed,
            disposition: self.disposition.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFetchEntry {
    pub microshard_id: MicroShardId,
    pub ordinal: u32,
    pub locator: String,
    pub content_hash: ContentId,
    pub bytes_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFetchManifest {
    pub dataset_view_id: burn_p2p_core::DatasetViewId,
    pub entries: Vec<ShardFetchEntry>,
}

impl ShardFetchManifest {
    pub fn entry_for_microshard(&self, microshard_id: &MicroShardId) -> Option<&ShardFetchEntry> {
        self.entries
            .iter()
            .find(|entry| &entry.microshard_id == microshard_id)
    }

    pub fn from_microshards(
        dataset_view: &DatasetView,
        microshards: &[MicroShard],
        bytes_for_ordinal: impl Fn(u32) -> Vec<u8>,
    ) -> Self {
        Self {
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            entries: microshards
                .iter()
                .map(|microshard| {
                    let bytes = bytes_for_ordinal(microshard.ordinal);
                    ShardFetchEntry {
                        microshard_id: microshard.microshard_id.clone(),
                        ordinal: microshard.ordinal,
                        locator: format!("{:05}.bin", microshard.ordinal),
                        content_hash: ContentId::from_multihash(
                            burn_p2p_core::codec::multihash_sha256(&bytes),
                        ),
                        bytes_len: bytes.len() as u64,
                    }
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedMicroShard {
    pub microshard: MicroShard,
    pub path: PathBuf,
    pub content_hash: ContentId,
    pub bytes_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCache {
    pub root: PathBuf,
}

impl ShardCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn ensure_layout(&self, dataset_view: &DatasetView) -> Result<PathBuf, DataloaderError> {
        let path = self.dataset_dir(dataset_view);
        fs::create_dir_all(&path).map_err(|error| DataloaderError::Io(error.to_string()))?;
        Ok(path)
    }

    pub fn dataset_dir(&self, dataset_view: &DatasetView) -> PathBuf {
        self.root.join(dataset_view.dataset_view_id.as_str())
    }

    pub fn manifest_path(&self, dataset_view: &DatasetView) -> PathBuf {
        self.dataset_dir(dataset_view).join("fetch-manifest.json")
    }

    pub fn shard_path(&self, dataset_view: &DatasetView, microshard: &MicroShard) -> PathBuf {
        self.dataset_dir(dataset_view)
            .join(format!("{:05}.bin", microshard.ordinal))
    }

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

    pub fn load_fetch_manifest(
        &self,
        dataset_view: &DatasetView,
    ) -> Result<ShardFetchManifest, DataloaderError> {
        let bytes = fs::read(self.manifest_path(dataset_view))
            .map_err(|error| DataloaderError::Io(error.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|error| DataloaderError::Io(error.to_string()))
    }

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
                    let bytes = self.fetch_entry_bytes(&registration.upstream, entry)?;
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

        let manifest: ShardFetchManifest = match &registration.upstream {
            UpstreamAdapter::Local { root } => {
                let bytes = fs::read(Path::new(root).join("fetch-manifest.json"))
                    .map_err(|error| DataloaderError::Io(error.to_string()))?;
                serde_json::from_slice(&bytes)
                    .map_err(|error| DataloaderError::Io(error.to_string()))?
            }
            UpstreamAdapter::Http { base_url } => {
                let bytes = http_get_bytes(format!(
                    "{}/fetch-manifest.json",
                    base_url.trim_end_matches('/')
                ))?;
                serde_json::from_slice(bytes.as_ref())
                    .map_err(|error| DataloaderError::Io(error.to_string()))?
            }
            unsupported => return Err(DataloaderError::UnsupportedAdapter(unsupported.clone())),
        };

        // Keep the fetch manifest scoped to the planned dataset view.
        if manifest.dataset_view_id != registration.view.dataset_view_id {
            return Err(DataloaderError::Io(
                "fetch manifest dataset view does not match registration".into(),
            ));
        }
        // Ensure cache manifest and plan stay aligned at least by microshard ids.
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

    fn fetch_entry_bytes(
        &self,
        upstream: &UpstreamAdapter,
        entry: &ShardFetchEntry,
    ) -> Result<Vec<u8>, DataloaderError> {
        match upstream {
            UpstreamAdapter::Local { root } => fs::read(Path::new(root).join(&entry.locator))
                .map_err(|error| DataloaderError::Io(error.to_string())),
            UpstreamAdapter::Http { base_url } => http_get_bytes(format!(
                "{}/{}",
                base_url.trim_end_matches('/'),
                entry.locator.trim_start_matches('/')
            )),
            unsupported => Err(DataloaderError::UnsupportedAdapter(unsupported.clone())),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn http_get_bytes(url: String) -> Result<Vec<u8>, DataloaderError> {
    let response =
        reqwest::blocking::get(url).map_err(|error| DataloaderError::Http(error.to_string()))?;
    let bytes = response
        .error_for_status()
        .map_err(|error| DataloaderError::Http(error.to_string()))?
        .bytes()
        .map_err(|error| DataloaderError::Http(error.to_string()))?;
    Ok(bytes.to_vec())
}

#[cfg(target_arch = "wasm32")]
fn http_get_bytes(url: String) -> Result<Vec<u8>, DataloaderError> {
    futures::executor::block_on(async move {
        let response = reqwest::get(url)
            .await
            .map_err(|error| DataloaderError::Http(error.to_string()))?;
        let bytes = response
            .error_for_status()
            .map_err(|error| DataloaderError::Http(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| DataloaderError::Http(error.to_string()))?;
        Ok(bytes.to_vec())
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedMicroShardLoader {
    dataset_view: DatasetView,
    registration: DatasetRegistration,
    microshard_plan: MicroShardPlan,
    cache: ShardCache,
}

impl CachedMicroShardLoader {
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

pub trait BurnDataLoaderAdapter<B> {
    type Batch;
    type Error;

    fn dataset_view(&self) -> &DatasetView;
    fn load_lease(&mut self, lease: &AssignmentLease) -> Result<Vec<Self::Batch>, Self::Error>;
}

fn shard_count_for(config: &MicroShardPlannerConfig, sizing: &DatasetSizing) -> u32 {
    let sizing_bytes = if sizing.total_bytes > 0 {
        sizing.total_bytes
    } else if sizing.total_tokens > 0 {
        sizing.total_tokens
    } else {
        sizing.total_examples
    };
    let unclamped = ceil_div(sizing_bytes.max(1), config.target_microshard_bytes.max(1));
    unclamped.clamp(
        u64::from(config.min_microshards.max(1)),
        u64::from(config.max_microshards.max(config.min_microshards.max(1))),
    ) as u32
}

fn evenly_distributed_share(total: u64, count: u64, index: u64) -> u64 {
    let base = total / count.max(1);
    let remainder = total % count.max(1);
    base + u64::from(index < remainder)
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if divisor <= 1 {
        value
    } else {
        value.div_ceil(divisor)
    }
}

fn verify_bytes(
    microshard_id: &MicroShardId,
    entry: &ShardFetchEntry,
    bytes: &[u8],
) -> Result<(), DataloaderError> {
    if entry.bytes_len != bytes.len() as u64 {
        return Err(DataloaderError::HashMismatch {
            microshard_id: microshard_id.clone(),
        });
    }

    let content_hash = ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(bytes));
    if content_hash != entry.content_hash {
        return Err(DataloaderError::HashMismatch {
            microshard_id: microshard_id.clone(),
        });
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use burn_p2p_core::{DatasetId, WindowId};
    use chrono::Utc;
    use tempfile::tempdir;

    use crate::{
        CachedMicroShardLoader, DataReceiptBuilder, DatasetRegistration, DatasetSizing, LeaseCache,
        LeasePlanner, LeasePlannerConfig, MicroShardPlanner, MicroShardPlannerConfig,
        ShardAwareSampler, ShardCache, ShardFetchManifest, UpstreamAdapter,
    };

    fn dataset_view() -> burn_p2p_core::DatasetView {
        burn_p2p_core::DatasetView {
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            dataset_id: DatasetId::new("dataset-1"),
            preprocessing_hash: burn_p2p_core::ContentId::new("prep-1"),
            tokenizer_hash: Some(burn_p2p_core::ContentId::new("tok-1")),
            manifest_hash: burn_p2p_core::ContentId::new("manifest-1"),
            metadata: BTreeMap::new(),
        }
    }

    fn microshard_plan() -> crate::MicroShardPlan {
        MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 10,
            min_microshards: 2,
            max_microshards: 2,
        })
        .expect("planner")
        .plan(
            &dataset_view(),
            DatasetSizing {
                total_examples: 20,
                total_tokens: 20,
                total_bytes: 20,
            },
        )
        .expect("plan")
    }

    fn make_registration(upstream: UpstreamAdapter) -> DatasetRegistration {
        DatasetRegistration {
            manifest: burn_p2p_core::DatasetManifest {
                dataset_id: DatasetId::new("dataset-1"),
                source_uri: "test".into(),
                format: "microshards".into(),
                manifest_hash: burn_p2p_core::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            view: dataset_view(),
            upstream,
        }
    }

    #[test]
    fn planner_splits_dataset_into_deterministic_microshards() {
        let planner = MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 100,
            min_microshards: 1,
            max_microshards: 32,
        })
        .expect("planner");

        let view = dataset_view();
        let first = planner
            .plan(
                &view,
                DatasetSizing {
                    total_examples: 10,
                    total_tokens: 1000,
                    total_bytes: 250,
                },
            )
            .expect("plan");
        let second = planner
            .plan(
                &view,
                DatasetSizing {
                    total_examples: 10,
                    total_tokens: 1000,
                    total_bytes: 250,
                },
            )
            .expect("plan");

        assert_eq!(first, second);
        assert_eq!(first.microshards.len(), 3);
        assert_eq!(
            first
                .microshards
                .iter()
                .map(|microshard| microshard.estimated_bytes)
                .sum::<u64>(),
            250
        );
    }

    #[test]
    fn lease_planner_uses_stable_selection_and_builds_assignment() {
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 128,
                    total_tokens: 16_384,
                    total_bytes: 256 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;

        let planner = LeasePlanner::new(LeasePlannerConfig {
            lease_duration_seconds: 120,
            max_microshards_per_lease: 8,
            ..LeasePlannerConfig::default()
        })
        .expect("planner");

        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(9),
                Utc::now(),
                64,
                &microshards,
            )
            .expect("lease");

        assert!(!planned.selection.microshards.is_empty());
        assert_eq!(
            planned.lease.microshards.len(),
            planned.selection.microshards.len()
        );
        assert_eq!(planned.lease.budget_work_units, 64);
        assert_eq!(planned.lease.window_id, WindowId(9));
    }

    #[test]
    fn lease_cache_indexes_by_window_and_peer() {
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 32,
                    total_tokens: 4_096,
                    total_bytes: 64 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(2),
                Utc::now(),
                32,
                &microshards,
            )
            .expect("lease");

        let mut cache = LeaseCache::default();
        cache.insert(planned.lease.clone());

        assert!(
            cache
                .get(WindowId(2), &burn_p2p_core::PeerId::new("peer-1"))
                .is_some()
        );
        assert_eq!(cache.evict_before(WindowId(2)), 0);
        assert_eq!(cache.evict_before(WindowId(3)), 1);
    }

    #[test]
    fn sampler_order_is_deterministic_for_a_lease() {
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 64,
                    total_tokens: 8_192,
                    total_bytes: 96 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(7),
                Utc::now(),
                32,
                &microshards,
            )
            .expect("lease");

        let first = ShardAwareSampler::from_lease(&planned.lease).expect("sampler");
        let second = ShardAwareSampler::from_lease(&planned.lease).expect("sampler");

        assert_eq!(first.ordered_microshards(), second.ordered_microshards());
    }

    #[test]
    fn data_receipt_builder_tracks_completed_microshards() {
        let _source = UpstreamAdapter::Local {
            root: "/tmp/dataset".into(),
        };
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 16,
                    total_tokens: 2_048,
                    total_bytes: 32 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                16,
                &microshards,
            )
            .expect("lease");

        let receipt = DataReceiptBuilder::accepted(16, 2_048)
            .build(&planned.lease, Utc::now())
            .expect("receipt");

        assert_eq!(receipt.lease_id, planned.lease.lease_id);
        assert_eq!(receipt.microshards, planned.lease.microshards);
    }

    #[test]
    fn local_cache_fetches_only_leased_microshards() {
        let upstream_dir = tempdir().expect("upstream dir");
        let cache_dir = tempdir().expect("cache dir");
        let plan = microshard_plan();
        let bytes_for_ordinal = |ordinal| format!("local-shard-{ordinal}").into_bytes();
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            bytes_for_ordinal,
        );
        fs::write(
            upstream_dir.path().join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            fs::write(
                upstream_dir.path().join(&entry.locator),
                bytes_for_ordinal(entry.ordinal),
            )
            .expect("write shard");
        }

        let registration = make_registration(UpstreamAdapter::Local {
            root: upstream_dir.path().display().to_string(),
        });
        let lease = LeasePlanner::default()
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &plan.dataset_view,
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                1,
                &plan.microshards[..1],
            )
            .expect("lease")
            .lease;

        let mut loader = CachedMicroShardLoader::new(
            registration,
            plan.clone(),
            ShardCache::new(cache_dir.path()),
        );
        let cached = <CachedMicroShardLoader as crate::BurnDataLoaderAdapter<()>>::load_lease(
            &mut loader,
            &lease,
        )
        .expect("load lease");

        assert_eq!(cached.len(), 1);
        assert!(cached[0].path.exists());
        assert!(
            !cache_dir
                .path()
                .join(plan.dataset_view.dataset_view_id.as_str())
                .join("00001.bin")
                .exists()
        );
    }

    #[test]
    fn http_cache_reuses_manifest_and_shard_without_refetching() {
        let upstream_dir = tempdir().expect("upstream dir");
        let cache_dir = tempdir().expect("cache dir");
        let plan = microshard_plan();
        let bytes_for_ordinal = |ordinal| format!("http-shard-{ordinal}").into_bytes();
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            bytes_for_ordinal,
        );
        fs::write(
            upstream_dir.path().join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            fs::write(
                upstream_dir.path().join(&entry.locator),
                bytes_for_ordinal(entry.ordinal),
            )
            .expect("write shard");
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let addr = listener.local_addr().expect("addr");
        let hits = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let root = upstream_dir.path().to_path_buf();
        let hits_for_thread = Arc::clone(&hits);
        let stop_for_thread = Arc::clone(&stop);
        let server = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        hits_for_thread.fetch_add(1, Ordering::Relaxed);
                        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                        let mut line = String::new();
                        reader.read_line(&mut line).expect("request line");
                        let path = line
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or("/")
                            .trim_start_matches('/');
                        loop {
                            let mut header = String::new();
                            reader.read_line(&mut header).expect("header");
                            if header == "\r\n" || header.is_empty() {
                                break;
                            }
                        }
                        let body = fs::read(root.join(path)).expect("read served file");
                        stream
                            .write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                )
                                .as_bytes(),
                            )
                            .expect("write head");
                        stream.write_all(&body).expect("write body");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("server error: {error}"),
                }
            }
        });

        let registration = make_registration(UpstreamAdapter::Http {
            base_url: format!("http://{addr}"),
        });
        let lease = LeasePlanner::default()
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &plan.dataset_view,
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                1,
                &plan.microshards[..1],
            )
            .expect("lease")
            .lease;
        let cache = ShardCache::new(cache_dir.path());

        cache
            .fetch_lease_microshards(&registration, &plan, &lease)
            .expect("first fetch");
        cache
            .fetch_lease_microshards(&registration, &plan, &lease)
            .expect("second fetch");

        stop.store(true, Ordering::Relaxed);
        let _ = server.join();

        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }
}
