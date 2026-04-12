use std::{collections::BTreeMap, fmt, sync::Arc};

use burn_p2p_core::AssignmentLease;
use burn_p2p_dataloader::{CachedMicroShard, DatasetRegistration, MicroShardPlan};
use serde::{Deserialize, Serialize};

type DatasetRegistrationFn = dyn Fn() -> anyhow::Result<DatasetRegistration> + Send + Sync;
type MicroShardPlanFn =
    dyn Fn(&DatasetRegistration) -> anyhow::Result<MicroShardPlan> + Send + Sync;
type LeaseBatchLoaderFn<D, B> =
    dyn Fn(&AssignmentLease, &[CachedMicroShard], &D) -> anyhow::Result<Vec<B>> + Send + Sync;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Describes one generated input provider in a serializable way.
pub struct GeneratedWorkloadInputDescriptor {
    /// Stable provider identifier exposed to downstream apps and operators.
    pub provider: String,
    /// Optional freeform metadata describing the generated input shape.
    pub metadata: BTreeMap<String, String>,
}

/// Describes a generated-input provider that can advertise one stable descriptor.
pub trait GeneratedWorkloadInputProvider {
    /// Returns the stable provider identifier.
    fn provider_id(&self) -> String;

    /// Returns optional freeform metadata for the generated input source.
    fn metadata(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// Builds the serializable descriptor for the provider.
    fn descriptor(&self) -> GeneratedWorkloadInputDescriptor {
        GeneratedWorkloadInputDescriptor {
            provider: self.provider_id(),
            metadata: self.metadata(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
/// Captures one serializable input-source description layered on top of a
/// `LeaseDataPipeline`.
pub enum WorkloadInputSource {
    /// The workload reads small inline records already bundled into app state.
    InlineRecords {
        /// Human-readable record format label.
        record_format: String,
        /// Optional number of bundled records.
        record_count: Option<u64>,
    },
    /// The workload fetches JSON-encoded records over HTTP.
    HttpJson {
        /// Source URL.
        url: String,
        /// Optional expected record count.
        record_count: Option<u64>,
    },
    /// The workload fetches a prepared shard-manifest document over HTTP.
    ShardManifestHttp {
        /// Manifest URL.
        manifest_url: String,
        /// Optional expected shard count.
        shard_count: Option<u64>,
    },
    /// The workload derives batches from one deterministic generated-input provider.
    Generated {
        /// Provider descriptor.
        descriptor: GeneratedWorkloadInputDescriptor,
    },
    /// The workload uses a custom downstream-managed input source.
    Custom {
        /// Stable custom source identifier.
        source_kind: String,
        /// Optional freeform metadata.
        metadata: BTreeMap<String, String>,
    },
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
/// Declares the high-level data flow used to derive one training micro-epoch.
pub enum LeaseDataPipelineKind {
    /// Leases resolve to cached microshard files or remote shard fetches.
    #[default]
    ShardedStatic,
    /// Leases resolve to indices or sampler scopes over an existing dataset.
    IndexedDataset,
    /// Leases resolve to deterministic generator seeds, recipes, or rollouts.
    GeneratedDataset,
    /// Leases are resolved by a custom downstream pipeline.
    Custom,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Stable metadata describing how one workload derives batches from a lease.
pub struct LeaseDataPipelineDescriptor {
    /// Human-facing pipeline label published in runtime metadata.
    pub pipeline_name: String,
    /// High-level pipeline shape.
    pub kind: LeaseDataPipelineKind,
    /// Optional serializable description of the input source backing the pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_source: Option<WorkloadInputSource>,
    /// Arbitrary metadata useful for workload-specific inspection.
    pub metadata: BTreeMap<String, String>,
}

impl LeaseDataPipelineDescriptor {
    /// Creates a new descriptor.
    pub fn new(pipeline_name: impl Into<String>, kind: LeaseDataPipelineKind) -> Self {
        Self {
            pipeline_name: pipeline_name.into(),
            kind,
            input_source: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Attaches one serializable input-source description.
    pub fn with_input_source(mut self, input_source: WorkloadInputSource) -> Self {
        self.input_source = Some(input_source);
        self
    }

    /// Declares that the pipeline consumes inline records.
    pub fn with_inline_records_source(
        self,
        record_format: impl Into<String>,
        record_count: Option<u64>,
    ) -> Self {
        self.with_input_source(WorkloadInputSource::InlineRecords {
            record_format: record_format.into(),
            record_count,
        })
    }

    /// Declares that the pipeline fetches JSON records over HTTP.
    pub fn with_http_json_source(self, url: impl Into<String>, record_count: Option<u64>) -> Self {
        self.with_input_source(WorkloadInputSource::HttpJson {
            url: url.into(),
            record_count,
        })
    }

    /// Declares that the pipeline fetches a shard manifest over HTTP.
    pub fn with_shard_manifest_http_source(
        self,
        manifest_url: impl Into<String>,
        shard_count: Option<u64>,
    ) -> Self {
        self.with_input_source(WorkloadInputSource::ShardManifestHttp {
            manifest_url: manifest_url.into(),
            shard_count,
        })
    }

    /// Declares that the pipeline uses one generated-input provider.
    pub fn with_generated_input_source(
        self,
        provider: &impl GeneratedWorkloadInputProvider,
    ) -> Self {
        self.with_input_source(WorkloadInputSource::Generated {
            descriptor: provider.descriptor(),
        })
    }

    /// Declares that the pipeline uses one custom downstream-managed source.
    pub fn with_custom_input_source(
        self,
        source_kind: impl Into<String>,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        self.with_input_source(WorkloadInputSource::Custom {
            source_kind: source_kind.into(),
            metadata,
        })
    }

    /// Adds one metadata entry.
    pub fn with_metadata_entry(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Backend-neutral lease/micro-epoch data pipeline.
pub struct LeaseDataPipeline<D, B> {
    descriptor: LeaseDataPipelineDescriptor,
    dataset_registration: Arc<DatasetRegistrationFn>,
    microshard_plan: Arc<MicroShardPlanFn>,
    load_batches: Arc<LeaseBatchLoaderFn<D, B>>,
}

impl<D, B> Clone for LeaseDataPipeline<D, B> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            dataset_registration: Arc::clone(&self.dataset_registration),
            microshard_plan: Arc::clone(&self.microshard_plan),
            load_batches: Arc::clone(&self.load_batches),
        }
    }
}

impl<D, B> LeaseDataPipeline<D, B> {
    /// Creates a new pipeline from the provided hooks.
    pub fn new(
        descriptor: LeaseDataPipelineDescriptor,
        dataset_registration: impl Fn() -> anyhow::Result<DatasetRegistration> + Send + Sync + 'static,
        microshard_plan: impl Fn(&DatasetRegistration) -> anyhow::Result<MicroShardPlan>
        + Send
        + Sync
        + 'static,
        load_batches: impl Fn(&AssignmentLease, &[CachedMicroShard], &D) -> anyhow::Result<Vec<B>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            descriptor,
            dataset_registration: Arc::new(dataset_registration),
            microshard_plan: Arc::new(microshard_plan),
            load_batches: Arc::new(load_batches),
        }
    }

    /// Returns the static pipeline descriptor.
    pub fn descriptor(&self) -> &LeaseDataPipelineDescriptor {
        &self.descriptor
    }

    /// Returns the pipeline kind.
    pub fn kind(&self) -> LeaseDataPipelineKind {
        self.descriptor.kind
    }

    /// Returns the optional serializable input-source description.
    pub fn input_source(&self) -> Option<&WorkloadInputSource> {
        self.descriptor.input_source.as_ref()
    }

    /// Returns the stable dataset registration.
    pub fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        (self.dataset_registration)()
    }

    /// Plans microshards for the provided dataset registration.
    pub fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<MicroShardPlan> {
        (self.microshard_plan)(registration)
    }

    /// Loads batches for one assigned lease.
    pub fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
        device: &D,
    ) -> anyhow::Result<Vec<B>> {
        (self.load_batches)(lease, cached_microshards, device)
    }
}

impl<D, B> fmt::Debug for LeaseDataPipeline<D, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeaseDataPipeline")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{local_upstream_root, local_upstream_root_for_pipeline};
    use burn_p2p_core::{
        ContentId, DatasetId, DatasetManifest, DatasetView, DatasetViewId, ExperimentId, LeaseId,
        NetworkId, PeerId, RevisionId, StudyId, WindowId,
    };
    use burn_p2p_dataloader::{
        DatasetSizing, MicroShardPlanner, MicroShardPlannerConfig, UpstreamAdapter,
    };

    #[derive(Clone)]
    struct SyntheticProvider;

    impl GeneratedWorkloadInputProvider for SyntheticProvider {
        fn provider_id(&self) -> String {
            "synthetic-provider".into()
        }

        fn metadata(&self) -> BTreeMap<String, String> {
            BTreeMap::from([("recipe".into(), "nca".into())])
        }
    }

    fn registration() -> DatasetRegistration {
        DatasetRegistration {
            manifest: DatasetManifest {
                dataset_id: DatasetId::new("pipeline-dataset"),
                source_uri: "runtime-local://pipeline".into(),
                format: "runtime-local".into(),
                manifest_hash: ContentId::new("pipeline-manifest"),
                metadata: BTreeMap::new(),
            },
            view: DatasetView {
                dataset_view_id: DatasetViewId::new("pipeline-view"),
                dataset_id: DatasetId::new("pipeline-dataset"),
                preprocessing_hash: ContentId::new("pipeline-preprocess"),
                tokenizer_hash: None,
                manifest_hash: ContentId::new("pipeline-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local { root: ".".into() },
        }
    }

    #[test]
    fn lease_data_pipeline_exposes_descriptor_and_hooks() {
        let descriptor = LeaseDataPipelineDescriptor::new(
            "generated-seed-bank",
            LeaseDataPipelineKind::GeneratedDataset,
        )
        .with_generated_input_source(&SyntheticProvider)
        .with_metadata_entry("planner", "fixed");
        let pipeline = LeaseDataPipeline::new(
            descriptor.clone(),
            || Ok(registration()),
            |registration| {
                Ok(MicroShardPlanner::new(MicroShardPlannerConfig {
                    target_microshard_bytes: 8,
                    min_microshards: 2,
                    max_microshards: 2,
                })?
                .plan(
                    &registration.view,
                    DatasetSizing {
                        total_examples: 8,
                        total_tokens: 8,
                        total_bytes: 16,
                    },
                )?)
            },
            |_lease, _cached, device: &String| Ok(vec![device.clone()]),
        );

        assert_eq!(pipeline.descriptor(), &descriptor);
        assert_eq!(pipeline.kind(), LeaseDataPipelineKind::GeneratedDataset);
        assert_eq!(
            pipeline.input_source(),
            Some(&WorkloadInputSource::Generated {
                descriptor: GeneratedWorkloadInputDescriptor {
                    provider: "synthetic-provider".into(),
                    metadata: BTreeMap::from([("recipe".into(), "nca".into())]),
                },
            })
        );
        let registration = pipeline
            .dataset_registration()
            .expect("dataset registration");
        let plan = pipeline
            .microshard_plan(&registration)
            .expect("microshard plan");
        assert_eq!(plan.microshards.len(), 2);
        let batches = pipeline
            .load_batches(
                &AssignmentLease {
                    lease_id: LeaseId::new("lease"),
                    network_id: NetworkId::new("network"),
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("experiment"),
                    revision_id: RevisionId::new("revision"),
                    peer_id: PeerId::new("peer"),
                    dataset_view_id: registration.view.dataset_view_id.clone(),
                    window_id: WindowId(1),
                    granted_at: chrono::Utc::now(),
                    expires_at: chrono::Utc::now(),
                    budget_work_units: 1,
                    microshards: plan
                        .microshards
                        .iter()
                        .map(|entry| entry.microshard_id.clone())
                        .collect(),
                    assignment_hash: ContentId::new("assignment"),
                },
                &[],
                &"cpu".to_owned(),
            )
            .expect("load batches");
        assert_eq!(batches, vec!["cpu".to_owned()]);
        assert_eq!(
            local_upstream_root(&registration),
            Some(std::path::PathBuf::from("."))
        );
        assert_eq!(
            local_upstream_root_for_pipeline(&pipeline).expect("pipeline root"),
            Some(std::path::PathBuf::from("."))
        );
    }

    #[test]
    fn lease_data_pipeline_descriptor_serializes_input_source() {
        let descriptor = LeaseDataPipelineDescriptor::new(
            "inline-records",
            LeaseDataPipelineKind::IndexedDataset,
        )
        .with_inline_records_source("token-window-json", Some(32));

        let json = serde_json::to_string(&descriptor).expect("serialize descriptor");
        let decoded: LeaseDataPipelineDescriptor =
            serde_json::from_str(&json).expect("deserialize descriptor");

        assert_eq!(decoded, descriptor);
    }
}
