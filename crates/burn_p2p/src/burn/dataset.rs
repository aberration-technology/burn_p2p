use std::{collections::BTreeMap, fs, marker::PhantomData, path::Path};

use anyhow::Context;
use burn::data::dataloader::batcher::Batcher;
use serde::{Serialize, de::DeserializeOwned};

use super::*;

#[derive(Clone, Debug)]
/// Configures a shard-backed dataset prepared for burn_p2p training windows.
///
/// Use this when the project wants a first-class p2p dataset surface instead
/// of wiring `DatasetRegistration`, `MicroShardPlan`, fetch manifests, and
/// cached shard decoding by hand.
pub struct BurnShardedDatasetConfig {
    dataset_name: String,
    source_uri: Option<String>,
    format: String,
    sizing: Option<crate::DatasetSizing>,
    fixed_microshard_count: Option<u32>,
    planner: Option<crate::MicroShardPlannerConfig>,
    manifest_metadata: BTreeMap<String, String>,
    view_metadata: BTreeMap<String, String>,
    preprocessing_hash: Option<ContentId>,
    tokenizer_hash: Option<ContentId>,
}

impl BurnShardedDatasetConfig {
    /// Creates a new config for a named dataset.
    pub fn new(dataset_name: impl Into<String>) -> Self {
        Self {
            dataset_name: dataset_name.into(),
            source_uri: None,
            format: "record-shards".into(),
            sizing: None,
            fixed_microshard_count: None,
            planner: None,
            manifest_metadata: BTreeMap::new(),
            view_metadata: BTreeMap::new(),
            preprocessing_hash: None,
            tokenizer_hash: None,
        }
    }

    /// Sets the source uri recorded in the dataset manifest.
    pub fn with_source_uri(mut self, source_uri: impl Into<String>) -> Self {
        self.source_uri = Some(source_uri.into());
        self
    }

    /// Sets the manifest format string.
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = format.into();
        self
    }

    /// Overrides dataset sizing when the default record-derived estimate is not
    /// representative enough.
    pub fn with_sizing(mut self, sizing: crate::DatasetSizing) -> Self {
        self.sizing = Some(sizing);
        self
    }

    /// Forces a fixed number of microshards.
    pub fn with_microshards(mut self, count: u32) -> Self {
        self.fixed_microshard_count = Some(count.max(1));
        self
    }

    /// Sets an explicit planner config.
    pub fn with_planner(mut self, planner: crate::MicroShardPlannerConfig) -> Self {
        self.planner = Some(planner);
        self
    }

    /// Adds one manifest metadata entry.
    pub fn with_manifest_metadata_entry(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.manifest_metadata.insert(key.into(), value.into());
        self
    }

    /// Adds one dataset-view metadata entry.
    pub fn with_view_metadata_entry(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.view_metadata.insert(key.into(), value.into());
        self
    }

    /// Overrides the preprocessing hash recorded in the dataset view.
    pub fn with_preprocessing_hash(mut self, preprocessing_hash: ContentId) -> Self {
        self.preprocessing_hash = Some(preprocessing_hash);
        self
    }

    /// Overrides the tokenizer hash recorded in the dataset view.
    pub fn with_tokenizer_hash(mut self, tokenizer_hash: ContentId) -> Self {
        self.tokenizer_hash = Some(tokenizer_hash);
        self
    }

    fn source_uri_for(&self, root: &Path) -> String {
        self.source_uri
            .clone()
            .unwrap_or_else(|| root.display().to_string())
    }

    fn planning_config(&self, sizing: &crate::DatasetSizing) -> crate::MicroShardPlannerConfig {
        if let Some(planner) = self.planner.clone() {
            return planner;
        }
        if let Some(count) = self.fixed_microshard_count {
            return crate::MicroShardPlannerConfig {
                target_microshard_bytes: (sizing.total_bytes / u64::from(count)).max(1),
                min_microshards: count.max(1),
                max_microshards: count.max(1),
            };
        }
        crate::MicroShardPlannerConfig::default()
    }
}

#[derive(Clone, Debug)]
/// Prepared shard-backed dataset metadata plus helpers for cached-shard loading.
///
/// `write_local(...)` materializes the shard payloads and fetch manifest once.
/// Call [`Self::with_http_upstream`] when browser or wasm peers should fetch
/// assigned shards through an HTTP origin instead of a local filesystem root.
pub struct BurnShardedDataset<Record> {
    registration: crate::DatasetRegistration,
    microshard_plan: crate::MicroShardPlan,
    shard_examples: BTreeMap<crate::MicroShardId, usize>,
    _record: PhantomData<fn() -> Record>,
}

impl<Record> BurnShardedDataset<Record> {
    /// Writes a shard-backed dataset under `root` from in-memory records.
    ///
    /// Records are partitioned deterministically across the planned
    /// microshards. The resulting directory contains `fetch-manifest.json`
    /// plus one file per microshard.
    pub fn write_local(
        root: impl AsRef<Path>,
        records: &[Record],
        config: BurnShardedDatasetConfig,
    ) -> anyhow::Result<Self>
    where
        Record: Serialize + Clone,
    {
        let root = root.as_ref();
        if records.is_empty() {
            anyhow::bail!("sharded dataset requires at least one record");
        }
        fs::create_dir_all(root)
            .with_context(|| format!("failed to create dataset root {}", root.display()))?;

        let sizing = config
            .sizing
            .clone()
            .unwrap_or(derive_record_sizing(records)?);
        let registration = dataset_registration(root, &config, &sizing)?;
        let microshard_plan = crate::MicroShardPlanner::new(config.planning_config(&sizing))?
            .plan(&registration.view, sizing)?;

        let mut shard_payloads = Vec::with_capacity(microshard_plan.microshards.len());
        let mut shard_examples = BTreeMap::new();
        for (index, microshard) in microshard_plan.microshards.iter().enumerate() {
            let range = distributed_range(records.len(), microshard_plan.microshards.len(), index);
            let shard_records = records[range].to_vec();
            shard_examples.insert(microshard.microshard_id.clone(), shard_records.len());
            shard_payloads.push(serde_json::to_vec_pretty(&shard_records)?);
        }

        let manifest = crate::ShardFetchManifest::from_microshards(
            &microshard_plan.dataset_view,
            &microshard_plan.microshards,
            |ordinal| shard_payloads[ordinal as usize].clone(),
        );
        fs::write(
            root.join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )
        .with_context(|| {
            format!(
                "failed to write {}",
                root.join("fetch-manifest.json").display()
            )
        })?;
        for entry in &manifest.entries {
            fs::write(
                root.join(&entry.locator),
                &shard_payloads[entry.ordinal as usize],
            )
            .with_context(|| {
                format!(
                    "failed to write dataset shard {}",
                    root.join(&entry.locator).display()
                )
            })?;
        }

        Ok(Self {
            registration,
            microshard_plan,
            shard_examples,
            _record: PhantomData,
        })
    }

    /// Returns a clone with a different upstream adapter.
    pub fn with_upstream(mut self, upstream: crate::UpstreamAdapter) -> Self {
        self.registration.upstream = upstream;
        self
    }

    /// Returns a clone that fetches shards from a local filesystem root.
    pub fn with_local_upstream(mut self, root: impl Into<String>) -> Self {
        let root = root.into();
        self.registration.manifest.source_uri = root.clone();
        self.registration.upstream = crate::UpstreamAdapter::Local { root };
        self
    }

    /// Returns a clone that fetches shards from an HTTP origin.
    ///
    /// Browser and wasm peers should usually use this form so assigned shards
    /// are downloaded through the normal HTTP fetch path.
    pub fn with_http_upstream(mut self, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        self.registration.manifest.source_uri = base_url.clone();
        self.registration.upstream = crate::UpstreamAdapter::Http { base_url };
        self
    }

    /// Returns the dataset registration exposed to the runtime.
    pub fn registration(&self) -> &crate::DatasetRegistration {
        &self.registration
    }

    /// Returns the planned microshards exposed to the runtime.
    pub fn microshard_plan(&self) -> &crate::MicroShardPlan {
        &self.microshard_plan
    }

    /// Returns example counts per microshard.
    pub fn shard_examples(&self) -> &BTreeMap<crate::MicroShardId, usize> {
        &self.shard_examples
    }

    /// Returns the total example count covered by a lease.
    pub fn examples_for_lease(&self, lease: &AssignmentLease) -> usize {
        lease
            .microshards
            .iter()
            .fold(0_usize, |sum, microshard_id| {
                sum + self
                    .shard_examples
                    .get(microshard_id)
                    .copied()
                    .unwrap_or_default()
            })
    }

    /// Decodes cached microshards back into records.
    pub fn load_records(
        &self,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Record>>
    where
        Record: DeserializeOwned,
    {
        let mut records = Vec::new();
        for shard in cached_microshards {
            let bytes = fs::read(&shard.path)
                .with_context(|| format!("failed to read shard {}", shard.path.display()))?;
            let mut shard_records: Vec<Record> = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to decode shard {}", shard.path.display()))?;
            records.append(&mut shard_records);
        }
        Ok(records)
    }

    /// Builds batches from cached microshards with a normal Burn batcher.
    pub fn load_batches<B, Ba, Batch>(
        &self,
        cached_microshards: &[CachedMicroShard],
        batcher: Ba,
        batch_size: usize,
        device: &B::Device,
    ) -> anyhow::Result<Vec<Batch>>
    where
        B: Backend,
        Record: DeserializeOwned + Clone,
        Ba: Batcher<B, Record, Batch>,
    {
        let records = self.load_records(cached_microshards)?;
        let chunk_size = batch_size.max(1);
        let mut batches = Vec::new();
        for chunk in records.chunks(chunk_size) {
            batches.push(batcher.batch(chunk.to_vec(), device));
        }
        Ok(batches)
    }
}

fn derive_record_sizing<Record: Serialize>(
    records: &[Record],
) -> anyhow::Result<crate::DatasetSizing> {
    let total_bytes = records.iter().try_fold(0_u64, |sum, record| {
        Ok::<_, anyhow::Error>(sum + serde_json::to_vec(record)?.len() as u64)
    })?;
    Ok(crate::DatasetSizing {
        total_examples: records.len() as u64,
        total_tokens: records.len() as u64,
        total_bytes: total_bytes.max(1),
    })
}

fn dataset_registration(
    root: &Path,
    config: &BurnShardedDatasetConfig,
    sizing: &crate::DatasetSizing,
) -> anyhow::Result<crate::DatasetRegistration> {
    let source_uri = config.source_uri_for(root);
    let dataset_id = crate::DatasetId::derive(&("burn-p2p-sharded-dataset", &config.dataset_name))?;
    let dataset_view_id =
        crate::DatasetViewId::derive(&(dataset_id.as_str(), &config.dataset_name, "view"))?;
    let manifest_hash = ContentId::derive(&(
        "burn-p2p-sharded-manifest",
        dataset_id.as_str(),
        &source_uri,
        &config.format,
        sizing,
        &config.manifest_metadata,
        &config.view_metadata,
    ))?;
    let preprocessing_hash = match config.preprocessing_hash.clone() {
        Some(hash) => hash,
        None => ContentId::derive(&("burn-p2p-sharded-preprocess", dataset_view_id.as_str()))?,
    };

    Ok(crate::DatasetRegistration {
        manifest: crate::DatasetManifest {
            dataset_id: dataset_id.clone(),
            source_uri,
            format: config.format.clone(),
            manifest_hash: manifest_hash.clone(),
            metadata: config.manifest_metadata.clone(),
        },
        view: crate::DatasetView {
            dataset_view_id,
            dataset_id,
            preprocessing_hash,
            tokenizer_hash: config.tokenizer_hash.clone(),
            manifest_hash,
            metadata: config.view_metadata.clone(),
        },
        upstream: crate::UpstreamAdapter::Local {
            root: root.display().to_string(),
        },
    })
}

fn distributed_range(total: usize, count: usize, index: usize) -> std::ops::Range<usize> {
    let count = count.max(1);
    let base = total / count;
    let remainder = total % count;
    let start = index * base + remainder.min(index);
    let len = base + usize::from(index < remainder);
    start..start + len
}
