use std::path::PathBuf;
use std::time::{Duration, Instant};

use burn_p2p::{
    ArtifactChunkPayload, ArtifactDescriptor, ArtifactId, ControlHandle, FsArtifactStore,
};
use burn_p2p_publish::{PeerArtifactMirrorRequest, PeerArtifactMirrorResponse};

pub const DEFAULT_PEER_ARTIFACT_MIRROR_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub const PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type SourceRootArtifact = (PathBuf, ArtifactDescriptor, Vec<ArtifactChunkPayload>);

pub fn mirror_peer_artifact(
    control: &ControlHandle,
    request: PeerArtifactMirrorRequest,
) -> Result<PeerArtifactMirrorResponse, Box<dyn std::error::Error>> {
    mirror_peer_artifact_into_store(control, request, None)
}

pub fn mirror_peer_artifact_into_store(
    control: &ControlHandle,
    request: PeerArtifactMirrorRequest,
    store: Option<&FsArtifactStore>,
) -> Result<PeerArtifactMirrorResponse, Box<dyn std::error::Error>> {
    mirror_peer_artifact_into_store_with_source_roots(control, request, store, &[])
}

pub fn mirror_peer_artifact_into_store_with_source_roots(
    control: &ControlHandle,
    request: PeerArtifactMirrorRequest,
    store: Option<&FsArtifactStore>,
    source_roots: &[PathBuf],
) -> Result<PeerArtifactMirrorResponse, Box<dyn std::error::Error>> {
    if let Some(response) =
        mirror_artifact_from_source_roots(control, &request.artifact_id, store, source_roots)?
    {
        return Ok(response);
    }

    if request.provider_peer_ids.is_empty() {
        return Err(format!(
            "artifact {} does not have provider peers to mirror from",
            request.artifact_id.as_str()
        )
        .into());
    }

    let timeout = request
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_PEER_ARTIFACT_MIRROR_TIMEOUT)
        .max(Duration::from_millis(1));
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        for provider in &request.provider_peer_ids {
            let Some(manifest_timeout) = bounded_peer_artifact_mirror_timeout(deadline) else {
                break;
            };
            let descriptor = match control.fetch_artifact_manifest(
                provider.as_str(),
                request.artifact_id.clone(),
                manifest_timeout,
            ) {
                Ok(Some(descriptor)) => descriptor,
                Ok(None) => {
                    last_error = Some(format!(
                        "{} did not advertise artifact {}",
                        provider.as_str(),
                        request.artifact_id.as_str()
                    ));
                    continue;
                }
                Err(error) => {
                    last_error = Some(format!(
                        "could not fetch artifact {} manifest from {}: {error}",
                        request.artifact_id.as_str(),
                        provider.as_str()
                    ));
                    continue;
                }
            };
            if descriptor.artifact_id != request.artifact_id {
                last_error = Some(format!(
                    "{} returned descriptor for unexpected artifact {}",
                    provider.as_str(),
                    descriptor.artifact_id.as_str()
                ));
                continue;
            }

            let mut chunks = Vec::with_capacity(descriptor.chunks.len());
            let mut failed_chunk = None;
            for chunk in &descriptor.chunks {
                let Some(chunk_timeout) = bounded_peer_artifact_mirror_timeout(deadline) else {
                    failed_chunk = Some("mirror timeout expired while fetching chunks".to_owned());
                    break;
                };
                match control.fetch_artifact_chunk(
                    provider.as_str(),
                    descriptor.artifact_id.clone(),
                    chunk.chunk_id.clone(),
                    chunk_timeout,
                ) {
                    Ok(Some(payload))
                        if payload.artifact_id == descriptor.artifact_id
                            && payload.chunk.chunk_id == chunk.chunk_id =>
                    {
                        chunks.push(payload);
                    }
                    Ok(Some(payload)) => {
                        failed_chunk = Some(format!(
                            "{} returned unexpected chunk {} for artifact {}",
                            provider.as_str(),
                            payload.chunk.chunk_id.as_str(),
                            payload.artifact_id.as_str()
                        ));
                        break;
                    }
                    Ok(None) => {
                        failed_chunk = Some(format!(
                            "{} did not serve chunk {} for artifact {}",
                            provider.as_str(),
                            chunk.chunk_id.as_str(),
                            descriptor.artifact_id.as_str()
                        ));
                        break;
                    }
                    Err(error) => {
                        failed_chunk = Some(format!(
                            "could not fetch chunk {} for artifact {} from {}: {error}",
                            chunk.chunk_id.as_str(),
                            descriptor.artifact_id.as_str(),
                            provider.as_str()
                        ));
                        break;
                    }
                }
            }
            if let Some(error) = failed_chunk {
                last_error = Some(error);
                continue;
            }

            if let Some(store) = store {
                persist_mirrored_artifact(store, &descriptor, &chunks)?;
            }
            control.publish_artifact(descriptor.clone(), chunks)?;
            return Ok(PeerArtifactMirrorResponse {
                artifact_id: descriptor.artifact_id,
                mirrored_from: provider.clone(),
                mirrored_provider_peer_id: control.local_peer_id(),
                bytes_len: descriptor.bytes_len,
                chunk_count: descriptor.chunks.len(),
            });
        }
        sleep_until_next_peer_artifact_mirror_attempt(deadline);
    }

    Err(last_error
        .unwrap_or_else(|| {
            format!(
                "timed out mirroring artifact {} from provider peers",
                request.artifact_id.as_str()
            )
        })
        .into())
}

fn mirror_artifact_from_source_roots(
    control: &ControlHandle,
    artifact_id: &ArtifactId,
    store: Option<&FsArtifactStore>,
    source_roots: &[PathBuf],
) -> Result<Option<PeerArtifactMirrorResponse>, Box<dyn std::error::Error>> {
    let Some((_root, descriptor, chunks)) =
        load_artifact_from_source_roots(artifact_id, source_roots)?
    else {
        return Ok(None);
    };
    if let Some(store) = store {
        persist_mirrored_artifact(store, &descriptor, &chunks)?;
    }
    control.publish_artifact(descriptor.clone(), chunks)?;
    let local_peer_id = control.local_peer_id().ok_or_else(|| {
        "edge mirror source-root publish finished without a local peer id".to_owned()
    })?;
    let chunk_count = descriptor.chunks.len();
    Ok(Some(PeerArtifactMirrorResponse {
        artifact_id: descriptor.artifact_id,
        mirrored_from: local_peer_id.clone(),
        mirrored_provider_peer_id: Some(local_peer_id),
        bytes_len: descriptor.bytes_len,
        chunk_count,
    }))
}

fn load_artifact_from_source_roots(
    artifact_id: &ArtifactId,
    source_roots: &[PathBuf],
) -> Result<Option<SourceRootArtifact>, Box<dyn std::error::Error>> {
    for root in source_roots {
        let source = FsArtifactStore::new(root.clone());
        match source.has_complete_artifact(artifact_id) {
            Ok(true) => {
                let descriptor = source.load_manifest(artifact_id)?;
                let chunks = descriptor
                    .chunks
                    .iter()
                    .map(|chunk| {
                        Ok(ArtifactChunkPayload {
                            artifact_id: descriptor.artifact_id.clone(),
                            chunk: chunk.clone(),
                            bytes: source.load_chunk_bytes(chunk)?,
                            generated_at: chrono::Utc::now(),
                        })
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
                return Ok(Some((root.clone(), descriptor, chunks)));
            }
            Ok(false) => {}
            Err(error) => {
                eprintln!(
                    "peer-artifact-mirror-source-root-unavailable root={} artifact={}: {error}",
                    root.display(),
                    artifact_id.as_str()
                );
            }
        }
    }
    Ok(None)
}

fn persist_mirrored_artifact(
    store: &FsArtifactStore,
    descriptor: &ArtifactDescriptor,
    chunks: &[ArtifactChunkPayload],
) -> Result<(), Box<dyn std::error::Error>> {
    store.ensure_layout()?;
    for payload in chunks {
        store.store_chunk_bytes(&payload.chunk, &payload.bytes)?;
    }
    store.store_manifest(descriptor)?;
    store.pin_artifact(&descriptor.artifact_id)?;
    if let Some(head_id) = descriptor.head_id.as_ref() {
        store.pin_head(head_id)?;
    }
    Ok(())
}

fn bounded_peer_artifact_mirror_timeout(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    if remaining.is_zero() {
        return None;
    }
    Some(remaining.min(PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT))
}

fn sleep_until_next_peer_artifact_mirror_attempt(deadline: Instant) {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return;
    };
    std::thread::sleep(remaining.min(Duration::from_millis(250)));
}

pub fn peer_artifact_mirror_error_status(error: &str) -> &'static str {
    if error.contains("timed out") || error.contains("timeout") {
        "504 Gateway Timeout"
    } else {
        "502 Bad Gateway"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p::{
        ArtifactBuildSpec, ArtifactChunkPayload, ArtifactKind, ChunkingScheme, ContentId, HeadId,
        Precision,
    };
    use chrono::Utc;

    #[test]
    fn peer_artifact_mirror_timeouts_cover_large_head_checkpoints() {
        assert_eq!(
            DEFAULT_PEER_ARTIFACT_MIRROR_TIMEOUT,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT,
            Duration::from_secs(60)
        );

        let bounded =
            bounded_peer_artifact_mirror_timeout(Instant::now() + Duration::from_secs(120))
                .expect("deadline should allow a bounded request timeout");
        assert_eq!(bounded, PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT);
    }

    #[test]
    fn peer_artifact_mirror_errors_are_http_status_mapped() {
        assert_eq!(
            peer_artifact_mirror_error_status("timed out waiting for artifact-chunk"),
            "504 Gateway Timeout"
        );
        assert_eq!(
            peer_artifact_mirror_error_status("provider did not advertise artifact"),
            "502 Bad Gateway"
        );
    }

    #[test]
    fn mirrored_artifact_persistence_pins_head_artifact_and_chunks() {
        let source_root = tempfile::tempdir().expect("source tempdir");
        let source_store = FsArtifactStore::new(source_root.path().join("source"));
        let head_id = HeadId::new("head-persist");
        let descriptor = source_store
            .store_artifact_reader(
                &ArtifactBuildSpec::new(
                    ArtifactKind::FullHead,
                    Precision::Fp32,
                    ContentId::new("schema-persist"),
                    "application/octet-stream",
                )
                .with_head(head_id.clone()),
                b"checkpoint bytes".as_slice(),
                ChunkingScheme::new(8).expect("chunking"),
            )
            .expect("source artifact");
        let chunks = descriptor
            .chunks
            .iter()
            .map(|chunk| ArtifactChunkPayload {
                artifact_id: descriptor.artifact_id.clone(),
                chunk: chunk.clone(),
                bytes: source_store.load_chunk_bytes(chunk).expect("chunk bytes"),
                generated_at: Utc::now(),
            })
            .collect::<Vec<_>>();

        let mirror_root = tempfile::tempdir().expect("mirror tempdir");
        let mirror_store = FsArtifactStore::new(mirror_root.path().join("mirror"));
        persist_mirrored_artifact(&mirror_store, &descriptor, &chunks).expect("persist mirror");

        assert!(
            mirror_store
                .has_complete_artifact(&descriptor.artifact_id)
                .expect("complete check")
        );
        assert!(
            mirror_store
                .pinned_artifacts()
                .expect("pinned artifacts")
                .contains(&descriptor.artifact_id)
        );
        assert!(
            mirror_store
                .pinned_heads()
                .expect("pinned heads")
                .contains(&head_id)
        );
    }

    #[test]
    fn source_root_artifact_loads_complete_manifest_and_chunks() {
        let source_root = tempfile::tempdir().expect("source tempdir");
        let source_store = FsArtifactStore::new(source_root.path().join("source"));
        let descriptor = source_store
            .store_artifact_reader(
                &ArtifactBuildSpec::new(
                    ArtifactKind::FullHead,
                    Precision::Fp32,
                    ContentId::new("schema-source-root"),
                    "application/octet-stream",
                ),
                b"source root checkpoint bytes".as_slice(),
                ChunkingScheme::new(8).expect("chunking"),
            )
            .expect("source artifact");

        let loaded = load_artifact_from_source_roots(
            &descriptor.artifact_id,
            &[
                source_root.path().join("missing"),
                source_store.root().to_path_buf(),
            ],
        )
        .expect("source root load")
        .expect("artifact loaded");

        assert_eq!(loaded.1.artifact_id, descriptor.artifact_id);
        assert_eq!(loaded.2.len(), descriptor.chunks.len());
    }
}
