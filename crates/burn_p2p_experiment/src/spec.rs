use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    ContentId, DatasetViewId, EvalAggregationRule, EvalMetricDef, EvalProtocolManifest,
    EvalProtocolOptions, ExperimentId, HeadId, MergePolicy, MergeTopologyPolicy, NetworkId,
    RevisionId, RobustnessPolicy, StudyId, TrainingProtocol,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a study spec.
pub struct StudySpec {
    /// The study ID.
    pub study_id: StudyId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The name.
    pub name: String,
    /// The description.
    pub description: Option<String>,
    /// The tags.
    pub tags: BTreeSet<String>,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
    /// The created at.
    pub created_at: DateTime<Utc>,
}

impl StudySpec {
    /// Creates a new value.
    pub fn new(
        network_id: NetworkId,
        name: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, burn_p2p_core::SchemaError> {
        let name = name.into();
        let study_id = StudyId::derive(&(network_id.as_str(), &name))?;
        Ok(Self {
            study_id,
            network_id,
            name,
            description: None,
            tags: BTreeSet::new(),
            metadata: BTreeMap::new(),
            created_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment spec.
pub struct ExperimentSpec {
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The study ID.
    pub study_id: StudyId,
    /// The name.
    pub name: String,
    /// The description.
    pub description: Option<String>,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The merge policy.
    pub merge_policy: MergePolicy,
    /// The merge topology.
    pub merge_topology: MergeTopologyPolicy,
    #[serde(default)]
    /// The training protocol.
    pub training_protocol: TrainingProtocol,
    /// The tags.
    pub tags: BTreeSet<String>,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}

impl ExperimentSpec {
    /// Creates a new value.
    pub fn new(
        study_id: StudyId,
        name: impl Into<String>,
        dataset_view_id: DatasetViewId,
        model_schema_hash: ContentId,
        merge_policy: MergePolicy,
    ) -> Result<Self, burn_p2p_core::SchemaError> {
        let name = name.into();
        let experiment_id =
            ExperimentId::derive(&(study_id.as_str(), &name, dataset_view_id.as_str()))?;

        Ok(Self {
            experiment_id,
            study_id,
            name,
            description: None,
            base_head_id: None,
            dataset_view_id,
            model_schema_hash,
            merge_policy,
            merge_topology: MergeTopologyPolicy::default(),
            training_protocol: TrainingProtocol::default(),
            tags: BTreeSet::new(),
            metadata: BTreeMap::new(),
        })
    }

    /// Returns a copy configured with the merge topology.
    pub fn with_merge_topology(mut self, merge_topology: MergeTopologyPolicy) -> Self {
        self.merge_topology = merge_topology;
        self
    }

    /// Returns a copy configured with the training protocol.
    pub fn with_training_protocol(mut self, training_protocol: TrainingProtocol) -> Self {
        self.training_protocol = training_protocol;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a revision compatibility.
pub struct RevisionCompatibility {
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The required client capabilities.
    pub required_client_capabilities: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a patch support.
pub struct PatchSupport {
    /// The hot.
    pub hot: bool,
    /// The warm.
    pub warm: bool,
    /// The cold.
    pub cold: bool,
}

impl PatchSupport {
    /// Performs the supports operation.
    pub fn supports(&self, patch_class: PatchClass) -> bool {
        match patch_class {
            PatchClass::Hot => self.hot,
            PatchClass::Warm => self.warm,
            PatchClass::Cold => self.cold,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a revision spec.
pub struct RevisionSpec {
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The parent revision ID.
    pub parent_revision_id: Option<RevisionId>,
    /// The project hash.
    pub project_hash: ContentId,
    /// The config hash.
    pub config_hash: ContentId,
    /// The compatibility.
    pub compatibility: RevisionCompatibility,
    /// The patch support.
    pub patch_support: PatchSupport,
    #[serde(default)]
    /// The training protocol pinned for this revision.
    pub training_protocol: TrainingProtocol,
    /// The created at.
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Metrics policy and registered evaluation protocols for this revision.
    pub metrics_policy: Option<RevisionMetricsPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Robustness policy attached to the revision.
    pub robustness_policy: Option<RobustnessPolicy>,
}

impl RevisionSpec {
    /// Creates a new value.
    pub fn new(
        experiment_id: ExperimentId,
        parent_revision_id: Option<RevisionId>,
        project_hash: ContentId,
        config_hash: ContentId,
        compatibility: RevisionCompatibility,
        patch_support: PatchSupport,
        created_at: DateTime<Utc>,
    ) -> Result<Self, burn_p2p_core::SchemaError> {
        let revision_id = RevisionId::derive(&(
            experiment_id.as_str(),
            parent_revision_id.as_ref().map(RevisionId::as_str),
            project_hash.as_str(),
            config_hash.as_str(),
            compatibility.dataset_view_id.as_str(),
        ))?;

        Ok(Self {
            revision_id,
            experiment_id,
            parent_revision_id,
            project_hash,
            config_hash,
            compatibility,
            patch_support,
            training_protocol: TrainingProtocol::default(),
            created_at,
            metrics_policy: None,
            robustness_policy: None,
        })
    }

    /// Returns a copy configured with a metrics policy.
    pub fn with_metrics_policy(mut self, metrics_policy: RevisionMetricsPolicy) -> Self {
        self.metrics_policy = Some(metrics_policy);
        self
    }

    /// Returns a copy configured with the training protocol.
    pub fn with_training_protocol(mut self, training_protocol: TrainingProtocol) -> Self {
        self.training_protocol = training_protocol;
        self
    }

    /// Returns a copy configured with a robustness policy.
    pub fn with_robustness_policy(mut self, robustness_policy: RobustnessPolicy) -> Self {
        self.robustness_policy = Some(robustness_policy);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the evaluation and lag semantics used to interpret one revision's metrics.
pub struct RevisionMetricsPolicy {
    /// Version label for charts derived from this policy.
    pub metric_series_version: String,
    /// Registered evaluation protocols for the revision.
    pub eval_protocols: Vec<EvalProtocolManifest>,
    /// Canonical validation protocol identifier, when configured.
    pub canonical_validation_protocol_id: Option<ContentId>,
    /// Canonical train-proxy protocol identifier, when configured.
    pub train_proxy_protocol_id: Option<ContentId>,
    /// Browser-verifier protocol identifier, when configured.
    pub browser_verifier_protocol_id: Option<ContentId>,
    /// Maximum head lag in steps considered fresh for stale-work analytics.
    pub stale_work_head_lag_threshold: Option<u64>,
    /// Maximum head lag in milliseconds considered fresh for stale-work analytics.
    pub stale_work_time_lag_ms_threshold: Option<u64>,
}

impl RevisionMetricsPolicy {
    /// Creates an empty metrics policy for a revision.
    pub fn new(metric_series_version: impl Into<String>) -> Self {
        Self {
            metric_series_version: metric_series_version.into(),
            eval_protocols: Vec::new(),
            canonical_validation_protocol_id: None,
            train_proxy_protocol_id: None,
            browser_verifier_protocol_id: None,
            stale_work_head_lag_threshold: None,
            stale_work_time_lag_ms_threshold: None,
        }
    }

    /// Adds one evaluation protocol to the policy.
    pub fn with_eval_protocol(mut self, protocol: EvalProtocolManifest) -> Self {
        self.eval_protocols.push(protocol);
        self
    }

    /// Marks the canonical validation protocol.
    pub fn with_canonical_validation_protocol_id(mut self, eval_protocol_id: ContentId) -> Self {
        self.canonical_validation_protocol_id = Some(eval_protocol_id);
        self
    }

    /// Marks the canonical train-proxy protocol.
    pub fn with_train_proxy_protocol_id(mut self, eval_protocol_id: ContentId) -> Self {
        self.train_proxy_protocol_id = Some(eval_protocol_id);
        self
    }

    /// Marks the browser verifier protocol.
    pub fn with_browser_verifier_protocol_id(mut self, eval_protocol_id: ContentId) -> Self {
        self.browser_verifier_protocol_id = Some(eval_protocol_id);
        self
    }

    /// Sets lag thresholds used by metrics and stale-work analytics.
    pub fn with_stale_work_thresholds(
        mut self,
        head_lag_steps: Option<u64>,
        head_lag_time_ms: Option<u64>,
    ) -> Self {
        self.stale_work_head_lag_threshold = head_lag_steps;
        self.stale_work_time_lag_ms_threshold = head_lag_time_ms;
        self
    }

    /// Builds a canonical validation protocol for the revision dataset view.
    pub fn canonical_validation_protocol(
        dataset_view_id: DatasetViewId,
        metric_defs: Vec<EvalMetricDef>,
        sample_budget: u64,
        deterministic_sampler_seed: u64,
        version: impl Into<String>,
    ) -> Result<EvalProtocolManifest, burn_p2p_core::SchemaError> {
        EvalProtocolManifest::new(
            "canonical-validation",
            dataset_view_id,
            "validation",
            metric_defs,
            EvalProtocolOptions::new(
                EvalAggregationRule::Mean,
                sample_budget,
                deterministic_sampler_seed,
                version,
            ),
        )
    }

    /// Builds a deterministic train-proxy protocol for canonical trend charts.
    pub fn train_proxy_protocol(
        dataset_view_id: DatasetViewId,
        metric_defs: Vec<EvalMetricDef>,
        sample_budget: u64,
        deterministic_sampler_seed: u64,
        version: impl Into<String>,
    ) -> Result<EvalProtocolManifest, burn_p2p_core::SchemaError> {
        EvalProtocolManifest::new(
            "canonical-train-proxy",
            dataset_view_id,
            "train-proxy",
            metric_defs,
            EvalProtocolOptions::new(
                EvalAggregationRule::Mean,
                sample_budget,
                deterministic_sampler_seed,
                version,
            ),
        )
    }

    /// Builds a browser-verifier protocol for lighter-weight browser-side evaluation.
    pub fn browser_verifier_protocol(
        dataset_view_id: DatasetViewId,
        metric_defs: Vec<EvalMetricDef>,
        sample_budget: u64,
        deterministic_sampler_seed: u64,
        version: impl Into<String>,
    ) -> Result<EvalProtocolManifest, burn_p2p_core::SchemaError> {
        EvalProtocolManifest::new(
            "browser-verifier",
            dataset_view_id,
            "browser-verifier",
            metric_defs,
            EvalProtocolOptions::new(
                EvalAggregationRule::Mean,
                sample_budget,
                deterministic_sampler_seed,
                version,
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported patch class values.
pub enum PatchClass {
    /// Uses the hot variant.
    Hot,
    /// Uses the warm variant.
    Warm,
    /// Uses the cold variant.
    Cold,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// Enumerates the supported patch value values.
pub enum PatchValue {
    /// Uses the bool variant.
    Bool(bool),
    /// Uses the integer variant.
    Integer(i64),
    /// Uses the float variant.
    Float(f64),
    /// Uses the text variant.
    Text(String),
    /// Uses the text list variant.
    TextList(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a runtime patch.
pub struct RuntimePatch {
    /// The patch ID.
    pub patch_id: ContentId,
    /// The class.
    pub class: PatchClass,
    /// The summary.
    pub summary: String,
    /// The values.
    pub values: BTreeMap<String, PatchValue>,
    /// The required client capabilities.
    pub required_client_capabilities: BTreeSet<String>,
}

impl RuntimePatch {
    /// Creates a new value.
    pub fn new(
        class: PatchClass,
        summary: impl Into<String>,
        values: BTreeMap<String, PatchValue>,
        required_client_capabilities: BTreeSet<String>,
    ) -> Result<Self, burn_p2p_core::SchemaError> {
        let summary = summary.into();
        let patch_id =
            ContentId::derive(&(&summary, &class, &values, &required_client_capabilities))?;

        Ok(Self {
            patch_id,
            class,
            summary,
            values,
            required_client_capabilities,
        })
    }

    /// Returns whether the value requires new revision.
    pub fn requires_new_revision(&self) -> bool {
        !matches!(self.class, PatchClass::Hot)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use burn_p2p_core::{
        ContentId, DatasetViewId, ExperimentId, RobustnessPolicy, RobustnessPreset,
    };
    use chrono::Utc;

    use super::{PatchSupport, RevisionCompatibility, RevisionMetricsPolicy, RevisionSpec};

    #[test]
    fn metrics_policy_builds_protocol_presets_and_attaches_to_revision() {
        let validation = RevisionMetricsPolicy::canonical_validation_protocol(
            DatasetViewId::new("view-a"),
            vec![burn_p2p_core::EvalMetricDef {
                metric_key: "loss".into(),
                display_name: "Loss".into(),
                unit: None,
                higher_is_better: false,
            }],
            256,
            7,
            "v1",
        )
        .expect("validation protocol");
        let train_proxy = RevisionMetricsPolicy::train_proxy_protocol(
            DatasetViewId::new("view-a"),
            vec![burn_p2p_core::EvalMetricDef {
                metric_key: "loss".into(),
                display_name: "Loss".into(),
                unit: None,
                higher_is_better: false,
            }],
            128,
            11,
            "v1",
        )
        .expect("train proxy protocol");
        let browser = RevisionMetricsPolicy::browser_verifier_protocol(
            DatasetViewId::new("view-a"),
            vec![burn_p2p_core::EvalMetricDef {
                metric_key: "accuracy".into(),
                display_name: "Accuracy".into(),
                unit: Some("%".into()),
                higher_is_better: true,
            }],
            64,
            13,
            "v1",
        )
        .expect("browser protocol");

        let policy = RevisionMetricsPolicy::new("series-v1")
            .with_eval_protocol(validation.clone())
            .with_eval_protocol(train_proxy.clone())
            .with_eval_protocol(browser.clone())
            .with_canonical_validation_protocol_id(validation.eval_protocol_id.clone())
            .with_train_proxy_protocol_id(train_proxy.eval_protocol_id.clone())
            .with_browser_verifier_protocol_id(browser.eval_protocol_id.clone())
            .with_stale_work_thresholds(Some(4), Some(30_000));
        let revision = RevisionSpec::new(
            ExperimentId::new("exp-a"),
            None,
            ContentId::new("project-a"),
            ContentId::new("config-a"),
            RevisionCompatibility {
                model_schema_hash: ContentId::new("schema-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                required_client_capabilities: BTreeSet::new(),
            },
            PatchSupport::default(),
            Utc::now(),
        )
        .expect("revision")
        .with_metrics_policy(policy.clone());

        let attached = revision.metrics_policy.expect("metrics policy");
        assert_eq!(attached.metric_series_version, "series-v1");
        assert_eq!(attached.eval_protocols.len(), 3);
        assert_eq!(
            attached.canonical_validation_protocol_id,
            Some(validation.eval_protocol_id)
        );
        assert_eq!(
            attached.train_proxy_protocol_id,
            Some(train_proxy.eval_protocol_id)
        );
        assert_eq!(
            attached.browser_verifier_protocol_id,
            Some(browser.eval_protocol_id)
        );
        assert_eq!(attached.stale_work_head_lag_threshold, Some(4));
        assert_eq!(attached.stale_work_time_lag_ms_threshold, Some(30_000));
    }

    #[test]
    fn revision_can_attach_robustness_policy() {
        let revision = RevisionSpec::new(
            ExperimentId::new("exp-a"),
            None,
            ContentId::new("project-a"),
            ContentId::new("config-a"),
            RevisionCompatibility {
                model_schema_hash: ContentId::new("schema-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                required_client_capabilities: BTreeSet::new(),
            },
            PatchSupport::default(),
            Utc::now(),
        )
        .expect("revision")
        .with_robustness_policy(RobustnessPolicy::strict());

        let attached = revision.robustness_policy.expect("robustness policy");
        assert_eq!(attached.preset, RobustnessPreset::Strict);
        assert!(attached.screening_policy.require_replica_agreement);
    }
}
