use std::time::{Duration, Instant};

use burn_p2p::ControlHandle;
use burn_p2p_publish::{PeerArtifactMirrorRequest, PeerArtifactMirrorResponse};

pub const DEFAULT_PEER_ARTIFACT_MIRROR_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub const PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub fn mirror_peer_artifact(
    control: &ControlHandle,
    request: PeerArtifactMirrorRequest,
) -> Result<PeerArtifactMirrorResponse, Box<dyn std::error::Error>> {
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

        control.publish_artifact(descriptor.clone(), chunks)?;
        return Ok(PeerArtifactMirrorResponse {
            artifact_id: descriptor.artifact_id,
            mirrored_from: provider.clone(),
            mirrored_provider_peer_id: control.local_peer_id(),
            bytes_len: descriptor.bytes_len,
            chunk_count: descriptor.chunks.len(),
        });
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

fn bounded_peer_artifact_mirror_timeout(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    if remaining.is_zero() {
        return None;
    }
    Some(remaining.min(PEER_ARTIFACT_MIRROR_REQUEST_TIMEOUT))
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
}
