use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use burn_p2p_core::{ArtifactDescriptor, ArtifactId, ChunkDescriptor, ChunkId, ContentId, HeadId};
use sha2::{Digest, Sha256};

use crate::{
    ArtifactBuildSpec, CheckpointError, ChunkingScheme, content_id_from_sha256_digest,
    stream_artifact_from_reader,
};

#[derive(Clone, Debug, PartialEq, Eq)]
/// Represents a fs artifact store.
pub struct FsArtifactStore {
    root: PathBuf,
}

impl FsArtifactStore {
    /// Creates a new value.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Performs the root operation.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Performs the state dir operation.
    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    /// Performs the artifacts dir operation.
    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    /// Performs the chunks dir operation.
    pub fn chunks_dir(&self) -> PathBuf {
        self.artifacts_dir().join("chunks")
    }

    /// Performs the manifests dir operation.
    pub fn manifests_dir(&self) -> PathBuf {
        self.artifacts_dir().join("manifests")
    }

    /// Performs the pins dir operation.
    pub fn pins_dir(&self) -> PathBuf {
        self.artifacts_dir().join("pins")
    }

    /// Performs the receipts dir operation.
    pub fn receipts_dir(&self) -> PathBuf {
        self.root.join("receipts")
    }

    /// Performs the heads dir operation.
    pub fn heads_dir(&self) -> PathBuf {
        self.root.join("heads")
    }

    /// Performs the ensure layout operation.
    pub fn ensure_layout(&self) -> Result<(), CheckpointError> {
        for path in [
            self.root.clone(),
            self.state_dir(),
            self.artifacts_dir(),
            self.chunks_dir(),
            self.manifests_dir(),
            self.pins_dir(),
            self.receipts_dir(),
            self.heads_dir(),
        ] {
            fs::create_dir_all(&path).map_err(|error| CheckpointError::Io(error.to_string()))?;
        }

        Ok(())
    }

    /// Performs the chunk path operation.
    pub fn chunk_path(&self, chunk_id: &ChunkId) -> PathBuf {
        self.chunks_dir()
            .join(format!("{}.chunk", chunk_id.as_str()))
    }

    /// Performs the manifest path operation.
    pub fn manifest_path(&self, artifact_id: &ArtifactId) -> PathBuf {
        self.manifests_dir()
            .join(format!("{}.json", artifact_id.as_str()))
    }

    /// Performs the head pin path operation.
    pub fn head_pin_path(&self, head_id: &HeadId) -> PathBuf {
        self.pins_dir()
            .join(format!("head-{}.pin", head_id.as_str()))
    }

    /// Performs the artifact pin path operation.
    pub fn artifact_pin_path(&self, artifact_id: &ArtifactId) -> PathBuf {
        self.pins_dir()
            .join(format!("artifact-{}.pin", artifact_id.as_str()))
    }

    /// Returns whether the value has chunk.
    pub fn has_chunk(&self, chunk_id: &ChunkId) -> bool {
        self.chunk_path(chunk_id).exists()
    }

    /// Returns whether the value has manifest.
    pub fn has_manifest(&self, artifact_id: &ArtifactId) -> bool {
        self.manifest_path(artifact_id).exists()
    }

    /// Performs the store manifest operation.
    pub fn store_manifest(
        &self,
        artifact: &ArtifactDescriptor,
    ) -> Result<PathBuf, CheckpointError> {
        self.ensure_layout()?;
        let path = self.manifest_path(&artifact.artifact_id);
        let json = serde_json::to_vec_pretty(artifact)
            .map_err(|error| CheckpointError::Io(error.to_string()))?;
        atomic_write(&path, &json)?;
        Ok(path)
    }

    /// Performs the load manifest operation.
    pub fn load_manifest(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<ArtifactDescriptor, CheckpointError> {
        let bytes = fs::read(self.manifest_path(artifact_id))
            .map_err(|error| CheckpointError::Io(error.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|error| CheckpointError::Io(error.to_string()))
    }

    /// Performs the store chunk bytes operation.
    pub fn store_chunk_bytes(
        &self,
        descriptor: &ChunkDescriptor,
        bytes: &[u8],
    ) -> Result<PathBuf, CheckpointError> {
        self.ensure_layout()?;
        verify_chunk(descriptor, bytes)?;
        let path = self.chunk_path(&descriptor.chunk_id);
        if path.exists() {
            return Ok(path);
        }

        atomic_write(&path, bytes)?;
        Ok(path)
    }

    /// Performs the load chunk bytes operation.
    pub fn load_chunk_bytes(
        &self,
        descriptor: &ChunkDescriptor,
    ) -> Result<Vec<u8>, CheckpointError> {
        let bytes = fs::read(self.chunk_path(&descriptor.chunk_id))
            .map_err(|error| CheckpointError::Io(error.to_string()))?;
        verify_chunk(descriptor, &bytes)?;
        Ok(bytes)
    }

    /// Performs the pin head operation.
    pub fn pin_head(&self, head_id: &HeadId) -> Result<(), CheckpointError> {
        self.ensure_layout()?;
        atomic_write(self.head_pin_path(head_id), head_id.as_str().as_bytes())
    }

    /// Performs the pin artifact operation.
    pub fn pin_artifact(&self, artifact_id: &ArtifactId) -> Result<(), CheckpointError> {
        self.ensure_layout()?;
        atomic_write(
            self.artifact_pin_path(artifact_id),
            artifact_id.as_str().as_bytes(),
        )
    }

    /// Performs the pinned heads operation.
    pub fn pinned_heads(&self) -> Result<BTreeSet<HeadId>, CheckpointError> {
        self.read_pins("head-", HeadId::new)
    }

    /// Performs the pinned artifacts operation.
    pub fn pinned_artifacts(&self) -> Result<BTreeSet<ArtifactId>, CheckpointError> {
        self.read_pins("artifact-", ArtifactId::new)
    }

    /// Performs the store artifact reader operation.
    pub fn store_artifact_reader(
        &self,
        spec: &ArtifactBuildSpec,
        reader: impl Read,
        chunking: ChunkingScheme,
    ) -> Result<ArtifactDescriptor, CheckpointError> {
        self.ensure_layout()?;
        let descriptor = stream_artifact_from_reader(spec, reader, chunking, |chunk, bytes| {
            self.store_chunk_bytes(chunk, bytes)?;
            Ok(())
        })?;
        self.store_manifest(&descriptor)?;
        Ok(descriptor)
    }

    /// Performs the store prebuilt artifact bytes operation.
    pub fn store_prebuilt_artifact_bytes(
        &self,
        descriptor: &ArtifactDescriptor,
        bytes: &[u8],
    ) -> Result<(), CheckpointError> {
        self.ensure_layout()?;
        if descriptor.bytes_len != bytes.len() as u64 {
            return Err(CheckpointError::ArtifactLengthMismatch {
                artifact_id: descriptor.artifact_id.clone(),
                expected: descriptor.bytes_len,
                found: bytes.len() as u64,
            });
        }

        let mut root_hasher = Sha256::new();
        root_hasher.update(bytes);
        let root_hash = content_id_from_sha256_digest(root_hasher.finalize());
        if root_hash != descriptor.root_hash {
            return Err(CheckpointError::ArtifactRootHashMismatch {
                artifact_id: descriptor.artifact_id.clone(),
            });
        }

        for chunk in &descriptor.chunks {
            let start = chunk.offset_bytes as usize;
            let end = start
                .checked_add(chunk.length_bytes as usize)
                .ok_or_else(|| CheckpointError::ArtifactLengthMismatch {
                    artifact_id: descriptor.artifact_id.clone(),
                    expected: descriptor.bytes_len,
                    found: bytes.len() as u64,
                })?;
            if end > bytes.len() {
                return Err(CheckpointError::ArtifactLengthMismatch {
                    artifact_id: descriptor.artifact_id.clone(),
                    expected: descriptor.bytes_len,
                    found: bytes.len() as u64,
                });
            }
            self.store_chunk_bytes(chunk, &bytes[start..end])?;
        }

        self.store_manifest(descriptor)?;
        Ok(())
    }

    /// Performs the store artifact file operation.
    pub fn store_artifact_file(
        &self,
        spec: &ArtifactBuildSpec,
        path: impl AsRef<Path>,
        chunking: ChunkingScheme,
    ) -> Result<ArtifactDescriptor, CheckpointError> {
        let file = File::open(path).map_err(|error| CheckpointError::Io(error.to_string()))?;
        self.store_artifact_reader(spec, BufReader::new(file), chunking)
    }

    /// Performs the materialize artifact bytes operation.
    pub fn materialize_artifact_bytes(
        &self,
        artifact: &ArtifactDescriptor,
    ) -> Result<Vec<u8>, CheckpointError> {
        let mut bytes = Vec::with_capacity(artifact.bytes_len as usize);
        let mut root_hasher = Sha256::new();

        for chunk in &artifact.chunks {
            let chunk_bytes = self.load_chunk_bytes(chunk)?;
            root_hasher.update(&chunk_bytes);
            bytes.extend_from_slice(&chunk_bytes);
        }

        let root_hash = content_id_from_sha256_digest(root_hasher.finalize());
        if root_hash != artifact.root_hash {
            return Err(CheckpointError::ArtifactRootHashMismatch {
                artifact_id: artifact.artifact_id.clone(),
            });
        }

        Ok(bytes)
    }

    /// Performs the garbage collect operation.
    pub fn garbage_collect(&self) -> Result<GarbageCollectionReport, CheckpointError> {
        self.ensure_layout()?;
        let pinned_artifacts = self.pinned_artifacts()?;
        let pinned_heads = self.pinned_heads()?;

        let mut pinned_chunk_ids = BTreeSet::new();
        let mut removed_manifests = Vec::new();
        let mut removed_chunks = Vec::new();

        for artifact_id in self.manifest_ids()? {
            let descriptor = self.load_manifest(&artifact_id)?;
            let pinned_by_head = descriptor
                .head_id
                .as_ref()
                .is_some_and(|head_id| pinned_heads.contains(head_id));
            if pinned_artifacts.contains(&artifact_id) || pinned_by_head {
                for chunk in &descriptor.chunks {
                    pinned_chunk_ids.insert(chunk.chunk_id.clone());
                }
                continue;
            }

            let path = self.manifest_path(&artifact_id);
            if path.exists() {
                fs::remove_file(&path).map_err(|error| CheckpointError::Io(error.to_string()))?;
                removed_manifests.push(artifact_id);
            }
        }

        for chunk_id in self.chunk_ids()? {
            if pinned_chunk_ids.contains(&chunk_id) {
                continue;
            }

            let path = self.chunk_path(&chunk_id);
            if path.exists() {
                fs::remove_file(&path).map_err(|error| CheckpointError::Io(error.to_string()))?;
                removed_chunks.push(chunk_id);
            }
        }

        Ok(GarbageCollectionReport {
            removed_manifests,
            removed_chunks,
        })
    }

    fn read_pins<T, F>(&self, prefix: &str, ctor: F) -> Result<BTreeSet<T>, CheckpointError>
    where
        T: Ord,
        F: Fn(String) -> T,
    {
        self.ensure_layout()?;
        let mut result = BTreeSet::new();
        for entry in
            fs::read_dir(self.pins_dir()).map_err(|error| CheckpointError::Io(error.to_string()))?
        {
            let entry = entry.map_err(|error| CheckpointError::Io(error.to_string()))?;
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if !file_name.starts_with(prefix) || !file_name.ends_with(".pin") {
                continue;
            }
            let id = file_name
                .strip_prefix(prefix)
                .expect("already filtered by prefix")
                .trim_end_matches(".pin")
                .to_owned();
            result.insert(ctor(id));
        }
        Ok(result)
    }

    fn manifest_ids(&self) -> Result<Vec<ArtifactId>, CheckpointError> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(self.manifests_dir())
            .map_err(|error| CheckpointError::Io(error.to_string()))?
        {
            let entry = entry.map_err(|error| CheckpointError::Io(error.to_string()))?;
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            ids.push(ArtifactId::new(stem.to_owned()));
        }
        Ok(ids)
    }

    fn chunk_ids(&self) -> Result<Vec<ChunkId>, CheckpointError> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(self.chunks_dir())
            .map_err(|error| CheckpointError::Io(error.to_string()))?
        {
            let entry = entry.map_err(|error| CheckpointError::Io(error.to_string()))?;
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            ids.push(ChunkId::new(stem.to_owned()));
        }
        Ok(ids)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Reports garbage collection details.
pub struct GarbageCollectionReport {
    /// The removed manifests.
    pub removed_manifests: Vec<ArtifactId>,
    /// The removed chunks.
    pub removed_chunks: Vec<ChunkId>,
}

fn verify_chunk(descriptor: &ChunkDescriptor, bytes: &[u8]) -> Result<(), CheckpointError> {
    if descriptor.length_bytes != bytes.len() as u64 {
        return Err(CheckpointError::ChunkLengthMismatch {
            chunk_id: descriptor.chunk_id.clone(),
            expected: descriptor.length_bytes,
            found: bytes.len() as u64,
        });
    }

    let chunk_hash = ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(bytes));
    if chunk_hash != descriptor.chunk_hash {
        return Err(CheckpointError::ChunkHashMismatch {
            chunk_id: descriptor.chunk_id.clone(),
        });
    }

    Ok(())
}

fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> Result<(), CheckpointError> {
    let path = path.as_ref();
    let temp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("tmp")
    ));
    let mut file =
        File::create(&temp_path).map_err(|error| CheckpointError::Io(error.to_string()))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| CheckpointError::Io(error.to_string()))?;
    fs::rename(&temp_path, path).map_err(|error| CheckpointError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};

    use burn_p2p_core::{ArtifactKind, ContentId, Precision};
    use tempfile::tempdir;

    use super::FsArtifactStore;
    use crate::{ArtifactBuildSpec, ChunkingScheme};

    #[test]
    fn store_streams_chunks_and_materializes_artifacts() {
        let dir = tempdir().expect("tempdir");
        let store = FsArtifactStore::new(dir.path());
        let payload = vec![42_u8; (1024 * 1024) + 333];
        let spec = ArtifactBuildSpec::new(
            ArtifactKind::ServeHead,
            Precision::Fp16,
            ContentId::new("schema"),
            "burn-record:bin",
        );

        let descriptor = store
            .store_artifact_reader(
                &spec,
                std::io::Cursor::new(&payload),
                ChunkingScheme::new(64 * 1024).expect("chunking"),
            )
            .expect("store artifact");

        assert!(store.has_manifest(&descriptor.artifact_id));
        assert!(descriptor.chunks.len() > 1);
        let materialized = store
            .materialize_artifact_bytes(&descriptor)
            .expect("materialize");
        assert_eq!(materialized, payload);
    }

    #[test]
    fn store_artifact_file_uses_streaming_file_reads() {
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("artifact.bin");
        let mut file = File::create(&file_path).expect("file");
        file.write_all(&vec![7_u8; 128 * 1024 + 7]).expect("write");

        let store = FsArtifactStore::new(dir.path().join("store"));
        let descriptor = store
            .store_artifact_file(
                &ArtifactBuildSpec::new(
                    ArtifactKind::FullHead,
                    Precision::Fp32,
                    ContentId::new("schema"),
                    "burn-record:bin",
                ),
                &file_path,
                ChunkingScheme::new(32 * 1024).expect("chunking"),
            )
            .expect("store");

        assert_eq!(descriptor.bytes_len, 128 * 1024 + 7);
        assert!(descriptor.chunks.len() >= 4);
    }

    #[test]
    fn garbage_collection_keeps_pinned_artifacts_and_removes_unpinned() {
        let dir = tempdir().expect("tempdir");
        let store = FsArtifactStore::new(dir.path());
        let chunking = ChunkingScheme::new(8).expect("chunking");
        let pinned = store
            .store_artifact_reader(
                &ArtifactBuildSpec::new(
                    ArtifactKind::ServeHead,
                    Precision::Fp16,
                    ContentId::new("schema"),
                    "burn-record:bin",
                )
                .with_head(burn_p2p_core::HeadId::new("head-1")),
                std::io::Cursor::new(b"0123456789abcdef"),
                chunking,
            )
            .expect("pinned");
        let unpinned = store
            .store_artifact_reader(
                &ArtifactBuildSpec::new(
                    ArtifactKind::DeltaPack,
                    Precision::Fp16,
                    ContentId::new("schema"),
                    "burn-record:bin",
                ),
                std::io::Cursor::new(b"fedcba9876543210"),
                chunking,
            )
            .expect("unpinned");

        store
            .pin_head(&burn_p2p_core::HeadId::new("head-1"))
            .expect("pin");
        let report = store.garbage_collect().expect("gc");

        assert!(report.removed_manifests.contains(&unpinned.artifact_id));
        assert!(!report.removed_manifests.contains(&pinned.artifact_id));
        assert!(store.has_manifest(&pinned.artifact_id));
        assert!(!store.has_manifest(&unpinned.artifact_id));
    }

    #[test]
    fn store_prebuilt_artifact_bytes_persists_existing_descriptor() {
        let dir = tempdir().expect("tempdir");
        let store = FsArtifactStore::new(dir.path());
        let payload = b"prebuilt-artifact-payload".to_vec();
        let descriptor = crate::build_artifact_descriptor_from_bytes(
            &ArtifactBuildSpec::new(
                ArtifactKind::ServeHead,
                Precision::Fp16,
                ContentId::new("schema"),
                "burn-record:bin",
            ),
            &payload,
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("descriptor");

        store
            .store_prebuilt_artifact_bytes(&descriptor, &payload)
            .expect("store prebuilt");

        assert!(store.has_manifest(&descriptor.artifact_id));
        assert_eq!(
            store
                .materialize_artifact_bytes(&descriptor)
                .expect("materialize"),
            payload
        );
    }
}
