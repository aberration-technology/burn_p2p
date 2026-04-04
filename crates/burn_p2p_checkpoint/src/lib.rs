//! Checkpoint manifests, artifact storage, and head reconstruction helpers.
#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufReader, Cursor, Read},
    path::Path,
};

use burn_p2p_core::{
    ArtifactDescriptor, ArtifactId, ArtifactKind, ChunkDescriptor, ChunkId, ContentId,
    ContributionReceipt, ContributionReceiptId, HeadDescriptor, HeadId, MergePolicy, MetricValue,
    Precision, codec::multihash_sha256,
};
use chrono::{DateTime, Utc};
use multihash::Multihash;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Public APIs for store.
pub mod store;

pub use store::{FsArtifactStore, GarbageCollectionReport};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
/// Enumerates the supported checkpoint error values.
pub enum CheckpointError {
    #[error("artifact already exists: {0}")]
    /// Uses the duplicate artifact variant.
    DuplicateArtifact(ArtifactId),
    #[error("head already exists: {0}")]
    /// Uses the duplicate head variant.
    DuplicateHead(HeadId),
    #[error("artifact not found: {0}")]
    /// Uses the unknown artifact variant.
    UnknownArtifact(ArtifactId),
    #[error("head not found: {0}")]
    /// Uses the unknown head variant.
    UnknownHead(HeadId),
    #[error("parent head not found: {0}")]
    /// Uses the missing parent head variant.
    MissingParentHead(HeadId),
    #[error("head {head_id} references artifact {artifact_id} with incompatible head binding")]
    /// Uses the head artifact mismatch variant.
    HeadArtifactMismatch {
        /// The head ID.
        head_id: HeadId,
        /// The artifact ID.
        artifact_id: ArtifactId,
    },
    #[error("delta artifact {artifact_id} must reference base head {expected_base_head_id}")]
    /// Uses the delta base head mismatch variant.
    DeltaBaseHeadMismatch {
        /// The artifact ID.
        artifact_id: ArtifactId,
        /// The expected base head ID.
        expected_base_head_id: HeadId,
    },
    #[error("merge candidate contains no contribution receipts")]
    /// Uses the empty merge candidate variant.
    EmptyMergeCandidate,
    #[error("receipt {receipt_id} points at unknown artifact {artifact_id}")]
    /// Uses the unknown contribution artifact variant.
    UnknownContributionArtifact {
        /// The receipt ID.
        receipt_id: ContributionReceiptId,
        /// The artifact ID.
        artifact_id: ArtifactId,
    },
    #[error("receipt {receipt_id} has non-positive accepted weight")]
    /// Uses the non positive contribution weight variant.
    NonPositiveContributionWeight {
        /// The receipt ID.
        receipt_id: ContributionReceiptId,
    },
    #[error("receipt {receipt_id} targets a different base head")]
    /// Uses the mixed base head variant.
    MixedBaseHead {
        /// The receipt ID.
        receipt_id: ContributionReceiptId,
        /// The expected.
        expected: HeadId,
        /// The found.
        found: HeadId,
    },
    #[error("receipt {receipt_id} uses a different study/experiment/revision tuple")]
    /// Uses the mixed contribution scope variant.
    MixedContributionScope {
        /// The receipt ID.
        receipt_id: ContributionReceiptId,
    },
    #[error("EMA decay must be in the inclusive range [0.0, 1.0]")]
    /// Uses the invalid EMA decay variant.
    InvalidEmaDecay,
    #[error("chunk size must be greater than zero")]
    /// Uses the invalid chunk size variant.
    InvalidChunkSize,
    #[error("chunk {chunk_id} length mismatch: expected {expected} bytes, found {found} bytes")]
    /// Uses the chunk length mismatch variant.
    ChunkLengthMismatch {
        /// The chunk ID.
        chunk_id: ChunkId,
        /// The expected.
        expected: u64,
        /// The found.
        found: u64,
    },
    #[error("chunk {chunk_id} hash mismatch")]
    /// Uses the chunk hash mismatch variant.
    ChunkHashMismatch {
        /// The chunk ID.
        chunk_id: ChunkId,
    },
    #[error("artifact {artifact_id} root hash mismatch after materialization")]
    /// Uses the artifact root hash mismatch variant.
    ArtifactRootHashMismatch {
        /// The artifact ID.
        artifact_id: ArtifactId,
    },
    #[error(
        "artifact {artifact_id} length mismatch: expected {expected} bytes, found {found} bytes"
    )]
    /// Uses the artifact length mismatch variant.
    ArtifactLengthMismatch {
        /// The artifact ID.
        artifact_id: ArtifactId,
        /// The expected.
        expected: u64,
        /// The found.
        found: u64,
    },
    #[error("i/o error: {0}")]
    /// Uses the io variant.
    Io(String),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Represents a checkpoint catalog.
pub struct CheckpointCatalog {
    /// The artifacts.
    pub artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    /// The heads.
    pub heads: BTreeMap<HeadId, HeadDescriptor>,
}

impl CheckpointCatalog {
    /// Performs the insert artifact operation.
    pub fn insert_artifact(
        &mut self,
        artifact: ArtifactDescriptor,
    ) -> Result<&ArtifactDescriptor, CheckpointError> {
        let artifact_id = artifact.artifact_id.clone();
        if self.artifacts.contains_key(&artifact_id) {
            return Err(CheckpointError::DuplicateArtifact(artifact_id));
        }

        self.artifacts.insert(artifact_id.clone(), artifact);
        Ok(self.artifacts.get(&artifact_id).expect("inserted artifact"))
    }

    /// Performs the insert head operation.
    pub fn insert_head(
        &mut self,
        head: HeadDescriptor,
    ) -> Result<&HeadDescriptor, CheckpointError> {
        let head_id = head.head_id.clone();
        if self.heads.contains_key(&head_id) {
            return Err(CheckpointError::DuplicateHead(head_id));
        }

        let artifact = self
            .artifacts
            .get(&head.artifact_id)
            .ok_or_else(|| CheckpointError::UnknownArtifact(head.artifact_id.clone()))?;

        if let Some(bound_head_id) = &artifact.head_id
            && bound_head_id != &head.head_id
        {
            return Err(CheckpointError::HeadArtifactMismatch {
                head_id: head.head_id.clone(),
                artifact_id: head.artifact_id.clone(),
            });
        }

        if let Some(parent_head_id) = &head.parent_head_id {
            if !self.heads.contains_key(parent_head_id) {
                return Err(CheckpointError::MissingParentHead(parent_head_id.clone()));
            }

            if artifact.kind == ArtifactKind::DeltaPack
                && artifact.base_head_id.as_ref() != Some(parent_head_id)
            {
                return Err(CheckpointError::DeltaBaseHeadMismatch {
                    artifact_id: artifact.artifact_id.clone(),
                    expected_base_head_id: parent_head_id.clone(),
                });
            }
        }

        self.heads.insert(head_id.clone(), head);
        Ok(self.heads.get(&head_id).expect("inserted head"))
    }

    /// Performs the artifact operation.
    pub fn artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<&ArtifactDescriptor, CheckpointError> {
        self.artifacts
            .get(artifact_id)
            .ok_or_else(|| CheckpointError::UnknownArtifact(artifact_id.clone()))
    }

    /// Performs the head operation.
    pub fn head(&self, head_id: &HeadId) -> Result<&HeadDescriptor, CheckpointError> {
        self.heads
            .get(head_id)
            .ok_or_else(|| CheckpointError::UnknownHead(head_id.clone()))
    }

    /// Performs the head chain operation.
    pub fn head_chain(&self, head_id: &HeadId) -> Result<Vec<HeadDescriptor>, CheckpointError> {
        let mut chain = Vec::new();
        let mut cursor = self.head(head_id)?.clone();

        loop {
            let next_parent = cursor.parent_head_id.clone();
            chain.push(cursor);

            match next_parent {
                Some(parent_head_id) => {
                    cursor = self.head(&parent_head_id)?.clone();
                }
                None => break,
            }
        }

        chain.reverse();
        Ok(chain)
    }

    /// Performs the lineage operation.
    pub fn lineage(
        &self,
        head_id: &HeadId,
        policy: MergePolicy,
    ) -> Result<CheckpointLineage, CheckpointError> {
        let heads = self.head_chain(head_id)?;
        let artifacts = heads
            .iter()
            .map(|head| self.artifact(&head.artifact_id).cloned())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CheckpointLineage {
            target_head_id: head_id.clone(),
            heads,
            artifacts,
            policy,
        })
    }

    /// Performs the plan sync operation.
    pub fn plan_sync(&self, request: SyncRequest) -> Result<SyncPlan, CheckpointError> {
        let chain = self.head_chain(&request.target_head_id)?;
        let mut missing_heads = Vec::new();
        let mut missing_artifacts = Vec::new();
        let mut missing_chunks = BTreeSet::new();

        for head in &chain {
            if !request.local_heads.contains(&head.head_id) {
                missing_heads.push(head.head_id.clone());
            }

            let artifact = self.artifact(&head.artifact_id)?;
            if !request.local_artifacts.contains(&artifact.artifact_id) {
                missing_artifacts.push(artifact.artifact_id.clone());
            }

            for chunk in &artifact.chunks {
                if !request.local_chunks.contains(&chunk.chunk_id) {
                    missing_chunks.insert(chunk.chunk_id.clone());
                }
            }
        }

        Ok(SyncPlan {
            target_head_id: request.target_head_id,
            missing_heads,
            missing_artifacts,
            missing_chunks: missing_chunks.into_iter().collect(),
        })
    }

    /// Performs the plan merge operation.
    pub fn plan_merge(&self, candidate: MergeCandidate) -> Result<MergePlan, CheckpointError> {
        if candidate.contributions.is_empty() {
            return Err(CheckpointError::EmptyMergeCandidate);
        }

        let mut contributions = candidate.contributions;
        contributions
            .sort_by(|left, right| left.receipt_id.as_str().cmp(right.receipt_id.as_str()));

        let mut total_weight = 0.0_f64;
        let mut metric_sums = BTreeMap::<String, f64>::new();
        let mut metric_weights = BTreeMap::<String, f64>::new();
        let mut receipt_ids = Vec::new();
        let mut artifact_ids = Vec::new();

        for contribution in contributions {
            if contribution.study_id != candidate.study_id
                || contribution.experiment_id != candidate.experiment_id
                || contribution.revision_id != candidate.revision_id
            {
                return Err(CheckpointError::MixedContributionScope {
                    receipt_id: contribution.receipt_id.clone(),
                });
            }

            if contribution.base_head_id != candidate.base_head_id {
                return Err(CheckpointError::MixedBaseHead {
                    receipt_id: contribution.receipt_id.clone(),
                    expected: candidate.base_head_id.clone(),
                    found: contribution.base_head_id.clone(),
                });
            }

            if contribution.accepted_weight <= 0.0 {
                return Err(CheckpointError::NonPositiveContributionWeight {
                    receipt_id: contribution.receipt_id.clone(),
                });
            }

            let artifact = self
                .artifacts
                .get(&contribution.artifact_id)
                .ok_or_else(|| CheckpointError::UnknownContributionArtifact {
                    receipt_id: contribution.receipt_id.clone(),
                    artifact_id: contribution.artifact_id.clone(),
                })?;

            if artifact.kind == ArtifactKind::DeltaPack
                && artifact.base_head_id.as_ref() != Some(&candidate.base_head_id)
            {
                return Err(CheckpointError::MixedBaseHead {
                    receipt_id: contribution.receipt_id.clone(),
                    expected: candidate.base_head_id.clone(),
                    found: artifact
                        .base_head_id
                        .clone()
                        .unwrap_or_else(|| HeadId::new("<missing-base-head>")),
                });
            }

            total_weight += contribution.accepted_weight;
            receipt_ids.push(contribution.receipt_id.clone());
            artifact_ids.push(contribution.artifact_id.clone());

            for (name, value) in contribution.metrics {
                let numeric = match value {
                    MetricValue::Integer(value) => value as f64,
                    MetricValue::Float(value) => value,
                    MetricValue::Bool(_) | MetricValue::Text(_) => continue,
                };

                *metric_sums.entry(name.clone()).or_insert(0.0) +=
                    numeric * contribution.accepted_weight;
                *metric_weights.entry(name).or_insert(0.0) += contribution.accepted_weight;
            }
        }

        let aggregated_numeric_metrics = metric_sums
            .into_iter()
            .map(|(name, weighted_sum)| {
                let weight = metric_weights.get(&name).copied().unwrap_or(1.0);
                (name, weighted_sum / weight)
            })
            .collect();

        Ok(MergePlan {
            study_id: candidate.study_id,
            experiment_id: candidate.experiment_id,
            revision_id: candidate.revision_id,
            base_head_id: candidate.base_head_id,
            policy: candidate.policy,
            contribution_receipt_ids: receipt_ids,
            artifact_ids,
            total_weight,
            aggregated_numeric_metrics,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a checkpoint lineage.
pub struct CheckpointLineage {
    /// The target head ID.
    pub target_head_id: HeadId,
    /// The heads.
    pub heads: Vec<HeadDescriptor>,
    /// The artifacts.
    pub artifacts: Vec<ArtifactDescriptor>,
    /// The policy.
    pub policy: MergePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a sync request.
pub struct SyncRequest {
    /// The target head ID.
    pub target_head_id: HeadId,
    /// The local heads.
    pub local_heads: BTreeSet<HeadId>,
    /// The local artifacts.
    pub local_artifacts: BTreeSet<ArtifactId>,
    /// The local chunks.
    pub local_chunks: BTreeSet<ChunkId>,
}

impl SyncRequest {
    /// Performs the empty operation.
    pub fn empty(target_head_id: HeadId) -> Self {
        Self {
            target_head_id,
            local_heads: BTreeSet::new(),
            local_artifacts: BTreeSet::new(),
            local_chunks: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a sync plan.
pub struct SyncPlan {
    /// The target head ID.
    pub target_head_id: HeadId,
    /// The missing heads.
    pub missing_heads: Vec<HeadId>,
    /// The missing artifacts.
    pub missing_artifacts: Vec<ArtifactId>,
    /// The missing chunks.
    pub missing_chunks: Vec<ChunkId>,
}

impl SyncPlan {
    /// Returns whether the value is complete.
    pub fn is_complete(&self) -> bool {
        self.missing_heads.is_empty()
            && self.missing_artifacts.is_empty()
            && self.missing_chunks.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge candidate.
pub struct MergeCandidate {
    /// The study ID.
    pub study_id: burn_p2p_core::StudyId,
    /// The experiment ID.
    pub experiment_id: burn_p2p_core::ExperimentId,
    /// The revision ID.
    pub revision_id: burn_p2p_core::RevisionId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The policy.
    pub policy: MergePolicy,
    /// The contributions.
    pub contributions: Vec<ContributionReceipt>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge plan.
pub struct MergePlan {
    /// The study ID.
    pub study_id: burn_p2p_core::StudyId,
    /// The experiment ID.
    pub experiment_id: burn_p2p_core::ExperimentId,
    /// The revision ID.
    pub revision_id: burn_p2p_core::RevisionId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The policy.
    pub policy: MergePolicy,
    /// The contribution receipt IDs.
    pub contribution_receipt_ids: Vec<ContributionReceiptId>,
    /// The artifact IDs.
    pub artifact_ids: Vec<ArtifactId>,
    /// The total weight.
    pub total_weight: f64,
    /// The aggregated numeric metrics.
    pub aggregated_numeric_metrics: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an aggregate artifact input.
pub struct AggregateArtifactInput {
    /// The peer ID.
    pub peer_id: burn_p2p_core::PeerId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The sample weight.
    pub sample_weight: f64,
    /// The quality weight.
    pub quality_weight: f64,
    /// The receipt IDs.
    pub receipt_ids: Vec<ContributionReceiptId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an aggregate artifact record.
pub struct AggregateArtifactRecord {
    /// The aggregate ID.
    pub aggregate_id: ContentId,
    /// The study ID.
    pub study_id: burn_p2p_core::StudyId,
    /// The experiment ID.
    pub experiment_id: burn_p2p_core::ExperimentId,
    /// The revision ID.
    pub revision_id: burn_p2p_core::RevisionId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The policy.
    pub policy: MergePolicy,
    /// The aggregate stats.
    pub aggregate_stats: burn_p2p_core::AggregateStats,
    /// The merge plan.
    pub merge_plan: MergePlan,
    /// The inputs.
    pub inputs: Vec<AggregateArtifactInput>,
    /// The created at.
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an aggregate artifact bytes.
pub struct AggregateArtifactBytes {
    /// The descriptor.
    pub descriptor: ArtifactDescriptor,
    /// The bytes.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an EMA flow.
pub struct EmaFlow {
    /// The decay.
    pub decay: f64,
    /// The included heads.
    pub included_heads: Vec<HeadId>,
}

impl EmaFlow {
    /// Creates a new value.
    pub fn new(decay: f64) -> Result<Self, CheckpointError> {
        if !(0.0..=1.0).contains(&decay) {
            return Err(CheckpointError::InvalidEmaDecay);
        }

        Ok(Self {
            decay,
            included_heads: Vec::new(),
        })
    }

    /// Performs the include certified head operation.
    pub fn include_certified_head(&mut self, head_id: HeadId) {
        if !self.included_heads.contains(&head_id) {
            self.included_heads.push(head_id);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a chunking scheme.
pub struct ChunkingScheme {
    /// The chunk size bytes.
    pub chunk_size_bytes: u64,
}

impl Default for ChunkingScheme {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 8 * 1024 * 1024,
        }
    }
}

impl ChunkingScheme {
    /// Creates a new value.
    pub fn new(chunk_size_bytes: u64) -> Result<Self, CheckpointError> {
        if chunk_size_bytes == 0 {
            return Err(CheckpointError::InvalidChunkSize);
        }

        Ok(Self { chunk_size_bytes })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an artifact build spec.
pub struct ArtifactBuildSpec {
    /// The kind.
    pub kind: ArtifactKind,
    /// The head ID.
    pub head_id: Option<HeadId>,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The precision.
    pub precision: Precision,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The record format.
    pub record_format: String,
}

impl ArtifactBuildSpec {
    /// Creates a new value.
    pub fn new(
        kind: ArtifactKind,
        precision: Precision,
        model_schema_hash: ContentId,
        record_format: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            head_id: None,
            base_head_id: None,
            precision,
            model_schema_hash,
            record_format: record_format.into(),
        }
    }

    /// Returns a copy configured with the head.
    pub fn with_head(mut self, head_id: HeadId) -> Self {
        self.head_id = Some(head_id);
        self
    }

    /// Returns a copy configured with the base head.
    pub fn with_base_head(mut self, base_head_id: HeadId) -> Self {
        self.base_head_id = Some(base_head_id);
        self
    }
}

/// Builds the artifact descriptor from bytes.
pub fn build_artifact_descriptor_from_bytes(
    spec: &ArtifactBuildSpec,
    bytes: impl AsRef<[u8]>,
    chunking: ChunkingScheme,
) -> Result<ArtifactDescriptor, CheckpointError> {
    build_artifact_descriptor_from_reader(spec, Cursor::new(bytes.as_ref()), chunking)
}

/// Builds the artifact descriptor from reader.
pub fn build_artifact_descriptor_from_reader(
    spec: &ArtifactBuildSpec,
    reader: impl Read,
    chunking: ChunkingScheme,
) -> Result<ArtifactDescriptor, CheckpointError> {
    stream_artifact_from_reader(spec, reader, chunking, |_chunk, _bytes| Ok(()))
}

/// Builds the artifact descriptor from file.
pub fn build_artifact_descriptor_from_file(
    spec: &ArtifactBuildSpec,
    path: impl AsRef<Path>,
    chunking: ChunkingScheme,
) -> Result<ArtifactDescriptor, CheckpointError> {
    let file = fs::File::open(path).map_err(|error| CheckpointError::Io(error.to_string()))?;
    build_artifact_descriptor_from_reader(spec, BufReader::new(file), chunking)
}

/// Performs the materialize aggregate artifact bytes operation.
pub fn materialize_aggregate_artifact_bytes(
    record: &AggregateArtifactRecord,
    chunking: ChunkingScheme,
) -> Result<AggregateArtifactBytes, CheckpointError> {
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| CheckpointError::Io(error.to_string()))?;
    let descriptor = build_artifact_descriptor_from_bytes(
        &ArtifactBuildSpec::new(
            ArtifactKind::DeltaPack,
            Precision::Fp32,
            ContentId::derive(&(
                "aggregate-artifact",
                record.study_id.as_str(),
                record.experiment_id.as_str(),
                record.revision_id.as_str(),
                record.base_head_id.as_str(),
            ))
            .map_err(|error| CheckpointError::Io(error.to_string()))?,
            "burn-p2p:aggregate-envelope+json",
        )
        .with_base_head(record.base_head_id.clone()),
        &bytes,
        chunking,
    )?;

    Ok(AggregateArtifactBytes { descriptor, bytes })
}

pub(crate) fn stream_artifact_from_reader<R, F>(
    spec: &ArtifactBuildSpec,
    mut reader: R,
    chunking: ChunkingScheme,
    mut on_chunk: F,
) -> Result<ArtifactDescriptor, CheckpointError>
where
    R: Read,
    F: FnMut(&ChunkDescriptor, &[u8]) -> Result<(), CheckpointError>,
{
    if chunking.chunk_size_bytes == 0 {
        return Err(CheckpointError::InvalidChunkSize);
    }

    let chunk_size = chunking.chunk_size_bytes as usize;
    let mut root_hasher = Sha256::new();
    let mut chunks = Vec::new();
    let mut total_bytes = 0_u64;

    loop {
        let mut buffer = vec![0_u8; chunk_size];
        let mut filled = 0_usize;

        while filled < chunk_size {
            let read = reader
                .read(&mut buffer[filled..])
                .map_err(|error| CheckpointError::Io(error.to_string()))?;
            if read == 0 {
                break;
            }
            filled += read;
        }

        if filled == 0 {
            break;
        }

        buffer.truncate(filled);
        root_hasher.update(&buffer);

        let chunk_hash = ContentId::from_multihash(multihash_sha256(&buffer));
        let descriptor = ChunkDescriptor {
            chunk_id: ChunkId::from(chunk_hash.clone()),
            offset_bytes: total_bytes,
            length_bytes: buffer.len() as u64,
            chunk_hash,
        };

        on_chunk(&descriptor, &buffer)?;

        total_bytes += descriptor.length_bytes;
        chunks.push(descriptor);
    }

    let root_hash = content_id_from_sha256_digest(root_hasher.finalize());
    let artifact_id = ArtifactId::derive(&(
        &spec.kind,
        spec.head_id.as_ref().map(HeadId::as_str),
        spec.base_head_id.as_ref().map(HeadId::as_str),
        &spec.precision,
        spec.model_schema_hash.as_str(),
        spec.record_format.as_str(),
        root_hash.as_str(),
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.chunk_id.as_str().to_owned(),
                    chunk.offset_bytes,
                    chunk.length_bytes,
                    chunk.chunk_hash.as_str().to_owned(),
                )
            })
            .collect::<Vec<_>>(),
    ))
    .map_err(|error| CheckpointError::Io(error.to_string()))?;

    Ok(ArtifactDescriptor {
        artifact_id,
        kind: spec.kind.clone(),
        head_id: spec.head_id.clone(),
        base_head_id: spec.base_head_id.clone(),
        precision: spec.precision.clone(),
        model_schema_hash: spec.model_schema_hash.clone(),
        record_format: spec.record_format.clone(),
        bytes_len: total_bytes,
        chunks,
        root_hash,
    })
}

pub(crate) fn content_id_from_sha256_digest(digest: impl AsRef<[u8]>) -> ContentId {
    let multihash =
        Multihash::<64>::wrap(0x12, digest.as_ref()).expect("sha256 digest should be valid");
    ContentId::from_multihash(multihash)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        AggregateStats, ArtifactDescriptor, ArtifactId, ArtifactKind, ChunkDescriptor, ChunkId,
        ContentId, ContributionReceipt, ContributionReceiptId, HeadDescriptor, HeadId, MergePolicy,
        MetricValue, Precision,
    };
    use chrono::Utc;

    use crate::{
        AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec, CheckpointCatalog,
        CheckpointError, ChunkingScheme, EmaFlow, MergeCandidate, MergePlan, SyncRequest,
        build_artifact_descriptor_from_bytes, materialize_aggregate_artifact_bytes,
    };

    fn chunk(id: &str, offset_bytes: u64) -> ChunkDescriptor {
        ChunkDescriptor {
            chunk_id: ChunkId::new(id),
            offset_bytes,
            length_bytes: 16,
            chunk_hash: burn_p2p_core::ContentId::new(format!("hash-{id}")),
        }
    }

    fn artifact(
        artifact_id: &str,
        kind: ArtifactKind,
        head_id: Option<&str>,
        base_head_id: Option<&str>,
        chunks: Vec<ChunkDescriptor>,
    ) -> ArtifactDescriptor {
        ArtifactDescriptor {
            artifact_id: ArtifactId::new(artifact_id),
            kind,
            head_id: head_id.map(HeadId::new),
            base_head_id: base_head_id.map(HeadId::new),
            precision: Precision::Fp32,
            model_schema_hash: burn_p2p_core::ContentId::new("model"),
            record_format: "burn-record".into(),
            bytes_len: 64,
            chunks,
            root_hash: burn_p2p_core::ContentId::new(format!("root-{artifact_id}")),
        }
    }

    fn head(head_id: &str, artifact_id: &str, parent_head_id: Option<&str>) -> HeadDescriptor {
        HeadDescriptor {
            head_id: HeadId::new(head_id),
            study_id: burn_p2p_core::StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
            revision_id: burn_p2p_core::RevisionId::new("revision"),
            artifact_id: ArtifactId::new(artifact_id),
            parent_head_id: parent_head_id.map(HeadId::new),
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }
    }

    fn receipt(
        receipt_id: &str,
        artifact_id: &str,
        base_head_id: &str,
        loss: f64,
    ) -> ContributionReceipt {
        ContributionReceipt {
            receipt_id: ContributionReceiptId::new(receipt_id),
            peer_id: burn_p2p_core::PeerId::new(format!("peer-{receipt_id}")),
            study_id: burn_p2p_core::StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
            revision_id: burn_p2p_core::RevisionId::new("revision"),
            base_head_id: HeadId::new(base_head_id),
            artifact_id: ArtifactId::new(artifact_id),
            accepted_at: Utc::now(),
            accepted_weight: 1.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
            merge_cert_id: None,
        }
    }

    #[test]
    fn inserting_head_requires_known_artifact() {
        let mut catalog = CheckpointCatalog::default();
        let error = catalog
            .insert_head(head("head-1", "artifact-1", None))
            .expect_err("must fail");

        assert_eq!(
            error,
            CheckpointError::UnknownArtifact(ArtifactId::new("artifact-1"))
        );
    }

    #[test]
    fn delta_head_requires_matching_base_head() {
        let mut catalog = CheckpointCatalog::default();
        catalog
            .insert_artifact(artifact(
                "artifact-root",
                ArtifactKind::FullHead,
                Some("head-root"),
                None,
                vec![chunk("chunk-root", 0)],
            ))
            .expect("artifact");
        catalog
            .insert_head(head("head-root", "artifact-root", None))
            .expect("head");
        catalog
            .insert_artifact(artifact(
                "artifact-delta",
                ArtifactKind::DeltaPack,
                Some("head-next"),
                Some("other-head"),
                vec![chunk("chunk-delta", 16)],
            ))
            .expect("artifact");

        let error = catalog
            .insert_head(head("head-next", "artifact-delta", Some("head-root")))
            .expect_err("must fail");

        assert_eq!(
            error,
            CheckpointError::DeltaBaseHeadMismatch {
                artifact_id: ArtifactId::new("artifact-delta"),
                expected_base_head_id: HeadId::new("head-root"),
            }
        );
    }

    #[test]
    fn sync_plan_collects_missing_lineage_and_chunks() {
        let mut catalog = CheckpointCatalog::default();
        catalog
            .insert_artifact(artifact(
                "artifact-root",
                ArtifactKind::FullHead,
                Some("head-root"),
                None,
                vec![chunk("chunk-root-a", 0), chunk("chunk-root-b", 16)],
            ))
            .expect("artifact");
        catalog
            .insert_head(head("head-root", "artifact-root", None))
            .expect("head");
        catalog
            .insert_artifact(artifact(
                "artifact-next",
                ArtifactKind::DeltaPack,
                Some("head-next"),
                Some("head-root"),
                vec![chunk("chunk-next", 32)],
            ))
            .expect("artifact");
        catalog
            .insert_head(head("head-next", "artifact-next", Some("head-root")))
            .expect("head");

        let plan = catalog
            .plan_sync(SyncRequest {
                target_head_id: HeadId::new("head-next"),
                local_heads: BTreeSet::from([HeadId::new("head-root")]),
                local_artifacts: BTreeSet::from([ArtifactId::new("artifact-root")]),
                local_chunks: BTreeSet::from([ChunkId::new("chunk-root-a")]),
            })
            .expect("plan");

        assert_eq!(plan.missing_heads, vec![HeadId::new("head-next")]);
        assert_eq!(
            plan.missing_artifacts,
            vec![ArtifactId::new("artifact-next")]
        );
        assert_eq!(
            plan.missing_chunks,
            vec![ChunkId::new("chunk-next"), ChunkId::new("chunk-root-b"),]
        );
    }

    #[test]
    fn merge_plan_rejects_mixed_base_heads() {
        let mut catalog = CheckpointCatalog::default();
        catalog
            .insert_artifact(artifact(
                "artifact-a",
                ArtifactKind::DeltaPack,
                None,
                Some("head-root"),
                vec![chunk("chunk-a", 0)],
            ))
            .expect("artifact");
        catalog
            .insert_artifact(artifact(
                "artifact-b",
                ArtifactKind::DeltaPack,
                None,
                Some("other-head"),
                vec![chunk("chunk-b", 16)],
            ))
            .expect("artifact");

        let error = catalog
            .plan_merge(MergeCandidate {
                study_id: burn_p2p_core::StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
                revision_id: burn_p2p_core::RevisionId::new("revision"),
                base_head_id: HeadId::new("head-root"),
                policy: MergePolicy::WeightedMean,
                contributions: vec![
                    receipt("receipt-a", "artifact-a", "head-root", 0.5),
                    receipt("receipt-b", "artifact-b", "other-head", 0.25),
                ],
            })
            .expect_err("must fail");

        assert!(matches!(error, CheckpointError::MixedBaseHead { .. }));
    }

    #[test]
    fn merge_plan_aggregates_numeric_metrics() {
        let mut catalog = CheckpointCatalog::default();
        catalog
            .insert_artifact(artifact(
                "artifact-a",
                ArtifactKind::DeltaPack,
                None,
                Some("head-root"),
                vec![chunk("chunk-a", 0)],
            ))
            .expect("artifact");
        catalog
            .insert_artifact(artifact(
                "artifact-b",
                ArtifactKind::DeltaPack,
                None,
                Some("head-root"),
                vec![chunk("chunk-b", 16)],
            ))
            .expect("artifact");

        let mut second = receipt("receipt-b", "artifact-b", "head-root", 0.25);
        second.accepted_weight = 3.0;

        let plan = catalog
            .plan_merge(MergeCandidate {
                study_id: burn_p2p_core::StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
                revision_id: burn_p2p_core::RevisionId::new("revision"),
                base_head_id: HeadId::new("head-root"),
                policy: MergePolicy::WeightedMean,
                contributions: vec![receipt("receipt-a", "artifact-a", "head-root", 1.0), second],
            })
            .expect("plan");

        assert_eq!(plan.contribution_receipt_ids.len(), 2);
        assert_eq!(plan.total_weight, 4.0);
        assert_eq!(plan.aggregated_numeric_metrics["loss"], 0.4375);
    }

    #[test]
    fn ema_flow_requires_valid_decay() {
        assert!(EmaFlow::new(0.95).is_ok());
        assert_eq!(
            EmaFlow::new(1.5).expect_err("must fail"),
            CheckpointError::InvalidEmaDecay
        );
    }

    #[test]
    fn artifact_descriptor_builder_chunks_and_hashes_bytes() {
        let spec = ArtifactBuildSpec::new(
            ArtifactKind::ServeHead,
            Precision::Fp16,
            ContentId::new("model-schema"),
            "burn-store:safetensors",
        )
        .with_head(HeadId::new("serve-head"));

        let descriptor = build_artifact_descriptor_from_bytes(
            &spec,
            b"abcdefgh",
            ChunkingScheme::new(3).expect("chunking"),
        )
        .expect("descriptor");

        assert_eq!(descriptor.kind, ArtifactKind::ServeHead);
        assert_eq!(descriptor.precision, Precision::Fp16);
        assert_eq!(descriptor.bytes_len, 8);
        assert_eq!(descriptor.chunks.len(), 3);
        assert_eq!(descriptor.chunks[0].offset_bytes, 0);
        assert_eq!(descriptor.chunks[1].offset_bytes, 3);
        assert_eq!(descriptor.chunks[2].offset_bytes, 6);
        assert_eq!(descriptor.chunks[0].length_bytes, 3);
        assert_eq!(descriptor.chunks[2].length_bytes, 2);
    }

    #[test]
    fn chunking_scheme_rejects_zero_sized_chunks() {
        assert_eq!(
            ChunkingScheme::new(0).expect_err("must fail"),
            CheckpointError::InvalidChunkSize
        );
    }

    #[test]
    fn aggregate_artifact_materialization_builds_delta_scoped_descriptor() {
        let record = AggregateArtifactRecord {
            aggregate_id: ContentId::new("aggregate-1"),
            study_id: burn_p2p_core::StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
            revision_id: burn_p2p_core::RevisionId::new("revision"),
            base_head_id: HeadId::new("head-root"),
            policy: MergePolicy::WeightedMean,
            aggregate_stats: AggregateStats {
                accepted_updates: 2,
                duplicate_updates: 0,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 3.0,
                sum_quality_weight: 1.8,
                sum_weighted_delta_norm: 2.4,
                max_update_norm: 1.4,
                accepted_sample_coverage: 1.0,
            },
            merge_plan: MergePlan {
                study_id: burn_p2p_core::StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("experiment"),
                revision_id: burn_p2p_core::RevisionId::new("revision"),
                base_head_id: HeadId::new("head-root"),
                policy: MergePolicy::WeightedMean,
                contribution_receipt_ids: vec![
                    ContributionReceiptId::new("receipt-a"),
                    ContributionReceiptId::new("receipt-b"),
                ],
                artifact_ids: vec![ArtifactId::new("artifact-a"), ArtifactId::new("artifact-b")],
                total_weight: 3.0,
                aggregated_numeric_metrics: BTreeMap::from([("loss".into(), 0.25)]),
            },
            inputs: vec![
                AggregateArtifactInput {
                    peer_id: burn_p2p_core::PeerId::new("peer-a"),
                    artifact_id: ArtifactId::new("artifact-a"),
                    sample_weight: 1.0,
                    quality_weight: 0.8,
                    receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
                },
                AggregateArtifactInput {
                    peer_id: burn_p2p_core::PeerId::new("peer-b"),
                    artifact_id: ArtifactId::new("artifact-b"),
                    sample_weight: 2.0,
                    quality_weight: 1.0,
                    receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
                },
            ],
            created_at: Utc::now(),
        };

        let artifact =
            materialize_aggregate_artifact_bytes(&record, ChunkingScheme::new(32).expect("chunk"))
                .expect("aggregate artifact");

        assert_eq!(artifact.descriptor.kind, ArtifactKind::DeltaPack);
        assert_eq!(
            artifact.descriptor.base_head_id,
            Some(HeadId::new("head-root"))
        );
        assert_eq!(
            artifact.descriptor.record_format,
            "burn-p2p:aggregate-envelope+json"
        );
        assert!(!artifact.bytes.is_empty());
    }
}
