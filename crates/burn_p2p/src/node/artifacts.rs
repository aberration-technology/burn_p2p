use super::*;
use crate::runtime_support::trace_to_stderr;

const ARTIFACT_TRACE_ENV: &str = "BURN_P2P_ARTIFACT_TRACE";

fn artifact_trace(args: std::fmt::Arguments<'_>) {
    trace_to_stderr(ARTIFACT_TRACE_ENV, "burn_p2p artifact", args);
}

impl<P> RunningNode<P> {
    /// Performs the artifact store operation.
    pub fn artifact_store(&self) -> Option<FsArtifactStore> {
        self.config()
            .storage
            .as_ref()
            .map(|storage| FsArtifactStore::new(storage.root.clone()))
    }

    /// Performs the publish artifact from store operation.
    pub fn publish_artifact_from_store(
        &self,
        artifact_id: &ArtifactId,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let store = self
            .artifact_store()
            .ok_or_else(|| anyhow::anyhow!("artifact publishing requires configured storage"))?;
        let descriptor = store.load_manifest(artifact_id)?;
        artifact_trace(format_args!(
            "publish-start artifact={} chunks={}",
            descriptor.artifact_id.as_str(),
            descriptor.chunks.len()
        ));
        let started = Instant::now();
        let chunks = descriptor
            .chunks
            .iter()
            .map(|chunk| {
                Ok(ArtifactChunkPayload {
                    artifact_id: descriptor.artifact_id.clone(),
                    chunk: chunk.clone(),
                    bytes: store.load_chunk_bytes(chunk)?,
                    generated_at: Utc::now(),
                })
            })
            .collect::<Result<Vec<_>, CheckpointError>>()?;
        self.control.publish_artifact(descriptor.clone(), chunks)?;
        artifact_trace(format_args!(
            "publish-complete artifact={} chunks={} elapsed_ms={}",
            descriptor.artifact_id.as_str(),
            descriptor.chunks.len(),
            started.elapsed().as_millis()
        ));
        Ok(descriptor)
    }

    /// Synchronizes the artifact from peer.
    pub fn sync_artifact_from_peer(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let artifact_provider_locate_timeout =
            ci_scaled_timeout(Duration::from_secs(20), Duration::from_secs(45));
        let artifact_chunk_fetch_timeout =
            ci_scaled_timeout(Duration::from_secs(20), Duration::from_secs(45));
        let artifact_request_timeout =
            ci_scaled_timeout(Duration::from_secs(5), Duration::from_secs(12));

        self.sync_artifact_from_peer_with_timeouts(
            peer_id,
            artifact_id,
            artifact_provider_locate_timeout,
            artifact_chunk_fetch_timeout,
            artifact_request_timeout,
        )
    }

    /// Synchronizes the artifact from peer with bounded per-attempt timeouts.
    pub fn sync_artifact_from_peer_bounded(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let budget = timeout.max(Duration::from_secs(1));
        let request_timeout = budget.min(ci_scaled_timeout(
            Duration::from_secs(3),
            Duration::from_secs(15),
        ));

        self.sync_artifact_from_peer_with_timeouts(
            peer_id,
            artifact_id,
            budget,
            budget,
            request_timeout,
        )
    }

    fn sync_artifact_from_peer_with_timeouts(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
        artifact_provider_locate_timeout: Duration,
        artifact_chunk_fetch_timeout: Duration,
        artifact_request_timeout: Duration,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let admission_policy = self.effective_admission_policy();
        let mut admitted_provider_snapshot = None;
        if let Some(policy) = admission_policy.as_ref() {
            let snapshot = self
                .control
                .fetch_snapshot(peer_id.as_str(), artifact_request_timeout)?;
            let report = verify_snapshot_admission(policy, peer_id, &snapshot)?;
            if !matches!(report.decision(), AdmissionDecision::Allow) {
                return Err(anyhow::anyhow!(
                    "peer {} is not admitted for artifact sync",
                    peer_id.as_str()
                ));
            }
            admitted_provider_snapshot = Some(snapshot);
        }

        let store = self
            .artifact_store()
            .ok_or_else(|| anyhow::anyhow!("artifact sync requires configured storage"))?;
        store.ensure_layout()?;

        if store.has_complete_artifact(&artifact_id)? {
            let descriptor = store.load_manifest(&artifact_id)?;
            self.clear_transfer_state(&artifact_id);
            artifact_trace(format_args!(
                "sync-skip-complete peer={} artifact={} chunks={}",
                peer_id.as_str(),
                artifact_id.as_str(),
                descriptor.chunks.len()
            ));
            return Ok(descriptor);
        }

        let sync_started = Instant::now();
        let telemetry_snapshot = self.telemetry().snapshot();
        let connected_peers = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut transfer_state = telemetry_snapshot
            .in_flight_transfers
            .get(&artifact_id)
            .cloned()
            .unwrap_or_else(|| ArtifactTransferState::new(artifact_id.clone()));
        let previous_source_peers = transfer_state.source_peers.clone();
        let previous_provider_peer_id = transfer_state.provider_peer_id.clone();
        transfer_state.source_peers = prioritized_artifact_source_peers(
            peer_id,
            previous_provider_peer_id.as_ref(),
            &previous_source_peers,
            &connected_peers,
        );
        artifact_trace(format_args!(
            "sync-start peer={} artifact={} budget_ms={} source_peers={:?} connected_peers={}",
            peer_id.as_str(),
            artifact_id.as_str(),
            artifact_provider_locate_timeout.as_millis(),
            transfer_state
                .source_peers
                .iter()
                .map(|peer_id| peer_id.as_str())
                .collect::<Vec<_>>(),
            connected_peers.len()
        ));
        if transfer_state.descriptor.is_none() && store.has_manifest(&artifact_id) {
            transfer_state.descriptor = Some(store.load_manifest(&artifact_id)?);
            transfer_state.set_phase(ArtifactTransferPhase::FetchingChunks);
        }
        if let Some(descriptor) = transfer_state.descriptor.clone() {
            for chunk in &descriptor.chunks {
                if store.has_chunk(&chunk.chunk_id) {
                    transfer_state.note_completed_chunk(&chunk.chunk_id);
                }
            }
        }
        self.record_transfer_state(transfer_state.clone());

        let provider_addresses = telemetry_snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .filter(|announcement| transfer_state.source_peers.contains(&announcement.peer_id))
            .filter(|announcement| !connected_peers.contains(&announcement.peer_id))
            .flat_map(|announcement| announcement.addresses.iter().cloned())
            .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
            .collect::<BTreeSet<_>>();
        for address in provider_addresses {
            let _ = self.control.dial_address(address);
        }

        let auth_policy = admission_policy.as_ref();
        let mut selected_provider = transfer_state.provider_peer_id.clone();
        let mut descriptor = transfer_state.descriptor.clone();
        let mut candidate_manifest_results = BTreeMap::<PeerId, String>::new();
        if descriptor.is_none() {
            transfer_state.set_phase(ArtifactTransferPhase::LocatingProvider);
            self.record_transfer_state(transfer_state.clone());

            let deadline = Instant::now() + artifact_provider_locate_timeout;
            while Instant::now() < deadline && selected_provider.is_none() {
                self.redial_known_candidates(&transfer_state.source_peers);
                for candidate in &transfer_state.source_peers {
                    let Some(request_timeout) = fair_request_timeout(
                        deadline,
                        artifact_request_timeout,
                        transfer_state.source_peers.len(),
                    ) else {
                        break;
                    };
                    if let Some(policy) = auth_policy {
                        let snapshot = if candidate == peer_id {
                            admitted_provider_snapshot.clone().unwrap_or_else(|| {
                                unreachable!(
                                    "admitted provider snapshot must exist when auth policy is enabled"
                                )
                            })
                        } else {
                            match self
                                .control
                                .fetch_snapshot(candidate.as_str(), request_timeout)
                            {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    candidate_manifest_results.insert(
                                        candidate.clone(),
                                        format!("snapshot-error:{error}"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let Ok(report) = verify_snapshot_admission(policy, candidate, &snapshot)
                        else {
                            candidate_manifest_results
                                .insert(candidate.clone(), "snapshot-admission-error".to_owned());
                            continue;
                        };
                        if !matches!(report.decision(), AdmissionDecision::Allow) {
                            candidate_manifest_results
                                .insert(candidate.clone(), "snapshot-not-admitted".to_owned());
                            continue;
                        }
                    }

                    match self.control.fetch_artifact_manifest(
                        candidate.as_str(),
                        artifact_id.clone(),
                        request_timeout,
                    ) {
                        Ok(Some(found)) => {
                            candidate_manifest_results
                                .insert(candidate.clone(), "found".to_owned());
                            artifact_trace(format_args!(
                                "manifest-found artifact={} provider={} chunks={} elapsed_ms={}",
                                artifact_id.as_str(),
                                candidate.as_str(),
                                found.chunks.len(),
                                sync_started.elapsed().as_millis()
                            ));
                            selected_provider = Some(candidate.clone());
                            descriptor = Some(found.clone());
                            transfer_state.set_provider(candidate.clone(), found);
                            self.record_transfer_state(transfer_state.clone());
                            break;
                        }
                        Ok(None) => {
                            candidate_manifest_results.insert(candidate.clone(), "none".to_owned());
                            continue;
                        }
                        Err(error) => {
                            candidate_manifest_results
                                .insert(candidate.clone(), format!("error:{error}"));
                            continue;
                        }
                    }
                }

                if selected_provider.is_none() {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }

        let provider = selected_provider
            .clone()
            .or_else(|| transfer_state.source_peers.first().cloned())
            .ok_or_else(|| {
                let snapshot = self.telemetry().snapshot();
                anyhow::anyhow!(
                    "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; peer_directory={:?}; candidate_results={:?}",
                    artifact_id.as_str(),
                    connected_peer_ids(&snapshot)
                        .into_iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    transfer_state
                        .source_peers
                        .iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .map(|announcement| (
                            announcement.peer_id.to_string(),
                            announcement
                                .addresses
                                .iter()
                                .map(|address| address.as_str().to_owned())
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>(),
                    candidate_manifest_results
                        .iter()
                        .map(|(peer_id, result)| (peer_id.to_string(), result.clone()))
                        .collect::<Vec<_>>(),
                )
            })?;
        let descriptor = descriptor.ok_or_else(|| {
            let snapshot = self.telemetry().snapshot();
            anyhow::anyhow!(
                "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; selected_provider={:?}; peer_directory={:?}; candidate_results={:?}",
                artifact_id.as_str(),
                connected_peer_ids(&snapshot)
                    .into_iter()
                    .map(|peer_id| peer_id.to_string())
                    .collect::<Vec<_>>(),
                transfer_state
                    .source_peers
                    .iter()
                    .map(|peer_id| peer_id.to_string())
                    .collect::<Vec<_>>(),
                selected_provider.as_ref().map(|peer_id| peer_id.to_string()),
                snapshot
                    .control_plane
                    .peer_directory_announcements
                    .iter()
                    .map(|announcement| (
                        announcement.peer_id.to_string(),
                        announcement
                            .addresses
                            .iter()
                            .map(|address| address.as_str().to_owned())
                            .collect::<Vec<_>>()
                    ))
                    .collect::<Vec<_>>(),
                candidate_manifest_results
                    .iter()
                    .map(|(peer_id, result)| (peer_id.to_string(), result.clone()))
                    .collect::<Vec<_>>(),
            )
        })?;
        let prioritized_source_peers = prioritized_artifact_source_peers(
            peer_id,
            Some(&provider),
            &transfer_state.source_peers,
            &connected_peers,
        );
        if transfer_state.provider_peer_id.as_ref() != Some(&provider)
            || transfer_state.descriptor.as_ref() != Some(&descriptor)
            || transfer_state.source_peers != prioritized_source_peers
        {
            transfer_state.set_provider(provider.clone(), descriptor.clone());
            transfer_state.source_peers = prioritized_source_peers.clone();
            self.record_transfer_state(transfer_state.clone());
        }
        let provider_candidates = prioritized_source_peers;

        for chunk in &descriptor.chunks {
            if store.has_chunk(&chunk.chunk_id) {
                if transfer_state.note_completed_chunk(&chunk.chunk_id) {
                    self.record_transfer_state(transfer_state.clone());
                }
                continue;
            }
            let chunk_deadline = Instant::now() + artifact_chunk_fetch_timeout;
            let mut stored = false;
            let mut candidate_attempt_counts = BTreeMap::<String, usize>::new();
            let mut candidate_last_results = BTreeMap::<String, String>::new();
            while Instant::now() < chunk_deadline && !stored {
                self.redial_known_candidates(&provider_candidates);
                for candidate in &provider_candidates {
                    let Some(request_timeout) = fair_request_timeout(
                        chunk_deadline,
                        artifact_request_timeout,
                        provider_candidates.len(),
                    ) else {
                        break;
                    };
                    let candidate_key = candidate.to_string();
                    *candidate_attempt_counts
                        .entry(candidate_key.clone())
                        .or_default() += 1;
                    match self.control.fetch_artifact_chunk(
                        candidate.as_str(),
                        descriptor.artifact_id.clone(),
                        chunk.chunk_id.clone(),
                        request_timeout,
                    ) {
                        Ok(Some(payload)) => {
                            store.store_chunk_bytes(&payload.chunk, &payload.bytes)?;
                            transfer_state.note_completed_chunk(&payload.chunk.chunk_id);
                            self.record_transfer_state(transfer_state.clone());
                            candidate_last_results.insert(candidate_key, "ok".into());
                            stored = true;
                            break;
                        }
                        Ok(None) => {
                            candidate_last_results.insert(candidate_key, "none".into());
                            continue;
                        }
                        Err(error) => {
                            candidate_last_results.insert(candidate_key, error.to_string());
                            continue;
                        }
                    }
                }

                if !stored {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }

            if !stored {
                let snapshot = self.telemetry().snapshot();
                artifact_trace(format_args!(
                    "chunk-failed artifact={} chunk={} provider_candidates={:?} attempts={:?} last_results={:?}",
                    descriptor.artifact_id.as_str(),
                    chunk.chunk_id.as_str(),
                    provider_candidates
                        .iter()
                        .map(|peer_id| peer_id.as_str())
                        .collect::<Vec<_>>(),
                    candidate_attempt_counts,
                    candidate_last_results
                ));
                return Err(anyhow::anyhow!(
                    "no connected peer provided chunk {} for artifact {}; provider_candidates={:?}; candidate_attempts={:?}; candidate_last_results={:?}; connected_peers={:?}; peer_directory={:?}",
                    chunk.chunk_id.as_str(),
                    descriptor.artifact_id.as_str(),
                    provider_candidates
                        .iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    candidate_attempt_counts,
                    candidate_last_results,
                    connected_peer_ids(&snapshot)
                        .into_iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .map(|announcement| (
                            announcement.peer_id.to_string(),
                            announcement
                                .addresses
                                .iter()
                                .map(|address| address.as_str().to_owned())
                                .collect::<Vec<_>>(),
                        ))
                        .collect::<Vec<_>>(),
                ));
            }
        }

        transfer_state.set_phase(ArtifactTransferPhase::Finalizing);
        self.record_transfer_state(transfer_state);
        store.store_manifest(&descriptor)?;
        self.publish_artifact_from_store(&descriptor.artifact_id)?;
        self.clear_transfer_state(&descriptor.artifact_id);
        artifact_trace(format_args!(
            "sync-complete artifact={} provider={} chunks={}",
            descriptor.artifact_id.as_str(),
            provider.as_str(),
            descriptor.chunks.len()
        ));
        Ok(descriptor)
    }

    pub(crate) fn effective_admission_policy(&self) -> Option<AdmissionPolicy> {
        let mut policy = self
            .config()
            .auth
            .as_ref()
            .and_then(|auth| auth.admission_policy.clone())?;
        let snapshot = self.telemetry().snapshot();
        if let Some(minimum_revocation_epoch) = snapshot.minimum_revocation_epoch
            && minimum_revocation_epoch > policy.minimum_revocation_epoch
        {
            policy.minimum_revocation_epoch = minimum_revocation_epoch;
        }
        if let Some(trust_bundle) = snapshot.trust_bundle
            && !trust_bundle.trusted_issuers.is_empty()
        {
            policy.trusted_issuers = trust_bundle.trusted_issuers;
            if trust_bundle.minimum_revocation_epoch > policy.minimum_revocation_epoch {
                policy.minimum_revocation_epoch = trust_bundle.minimum_revocation_epoch;
            }
        }
        Some(policy)
    }
}

pub(super) fn is_transient_artifact_sync_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    [
        "timed out waiting for snapshot",
        "timed out waiting for artifact-manifest",
        "timed out waiting for artifact-chunk",
        "no connected peer provided artifact",
        "no connected peer provided chunk",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

pub(crate) fn fair_request_timeout(
    deadline: Instant,
    request_timeout: Duration,
    candidate_count: usize,
) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }

    let candidate_count = candidate_count.max(1) as u32;
    let minimum_slice = Duration::from_millis(250);
    let divided = remaining / candidate_count;
    let slice = if remaining > minimum_slice {
        divided.max(minimum_slice)
    } else {
        remaining
    };
    Some(request_timeout.min(slice).min(remaining))
}

pub(crate) fn ci_scaled_timeout(base: Duration, ci: Duration) -> Duration {
    if std::env::var_os("CI").is_some() || std::env::var_os("GITHUB_ACTIONS").is_some() {
        ci
    } else {
        base
    }
}

pub(crate) fn prioritized_artifact_source_peers(
    requested_peer_id: &PeerId,
    provider_peer_id: Option<&PeerId>,
    existing_source_peers: &[PeerId],
    _connected_peers: &BTreeSet<PeerId>,
) -> Vec<PeerId> {
    dedupe_peer_ids(
        std::iter::once(requested_peer_id.clone())
            .chain(provider_peer_id.into_iter().cloned())
            .chain(existing_source_peers.iter().cloned()),
    )
}
