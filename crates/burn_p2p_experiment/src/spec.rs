use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    ContentId, DatasetViewId, ExperimentId, HeadId, MergePolicy, MergeTopologyPolicy, NetworkId,
    RevisionId, StudyId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StudySpec {
    pub study_id: StudyId,
    pub network_id: NetworkId,
    pub name: String,
    pub description: Option<String>,
    pub tags: BTreeSet<String>,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl StudySpec {
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
pub struct ExperimentSpec {
    pub experiment_id: ExperimentId,
    pub study_id: StudyId,
    pub name: String,
    pub description: Option<String>,
    pub base_head_id: Option<HeadId>,
    pub dataset_view_id: DatasetViewId,
    pub model_schema_hash: ContentId,
    pub merge_policy: MergePolicy,
    pub merge_topology: MergeTopologyPolicy,
    pub tags: BTreeSet<String>,
    pub metadata: BTreeMap<String, String>,
}

impl ExperimentSpec {
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
            tags: BTreeSet::new(),
            metadata: BTreeMap::new(),
        })
    }

    pub fn with_merge_topology(mut self, merge_topology: MergeTopologyPolicy) -> Self {
        self.merge_topology = merge_topology;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionCompatibility {
    pub model_schema_hash: ContentId,
    pub dataset_view_id: DatasetViewId,
    pub required_client_capabilities: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchSupport {
    pub hot: bool,
    pub warm: bool,
    pub cold: bool,
}

impl PatchSupport {
    pub fn supports(&self, patch_class: PatchClass) -> bool {
        match patch_class {
            PatchClass::Hot => self.hot,
            PatchClass::Warm => self.warm,
            PatchClass::Cold => self.cold,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionSpec {
    pub revision_id: RevisionId,
    pub experiment_id: ExperimentId,
    pub parent_revision_id: Option<RevisionId>,
    pub project_hash: ContentId,
    pub config_hash: ContentId,
    pub compatibility: RevisionCompatibility,
    pub patch_support: PatchSupport,
    pub created_at: DateTime<Utc>,
}

impl RevisionSpec {
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
            created_at,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PatchClass {
    Hot,
    Warm,
    Cold,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PatchValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    TextList(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimePatch {
    pub patch_id: ContentId,
    pub class: PatchClass,
    pub summary: String,
    pub values: BTreeMap<String, PatchValue>,
    pub required_client_capabilities: BTreeSet<String>,
}

impl RuntimePatch {
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

    pub fn requires_new_revision(&self) -> bool {
        !matches!(self.class, PatchClass::Hot)
    }
}
