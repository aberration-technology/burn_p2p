use super::*;
use super::transport::{sidecar_peer_addresses, split_fetch_timeout};

#[derive(Clone)]
/// Represents a control handle.
pub struct ControlHandle {
    pub(crate) tx: mpsc::Sender<RuntimeCommand>,
    pub(crate) telemetry: TelemetryHandle,
    pub(crate) runtime_boundary: RuntimeBoundary,
}

impl fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlHandle").finish_non_exhaustive()
    }
}

impl ControlHandle {
    fn retry_runtime_request<T>(
        &self,
        timeout: Duration,
        mut request: impl FnMut(Duration) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        const RUNTIME_FETCH_RETRY_SLICE: Duration = Duration::from_millis(250);
        const RUNTIME_FETCH_RETRY_DELAY: Duration = Duration::from_millis(25);

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let attempt_timeout = deadline
                .saturating_duration_since(now)
                .min(RUNTIME_FETCH_RETRY_SLICE);
            match request(attempt_timeout) {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error);
                    if Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(RUNTIME_FETCH_RETRY_DELAY);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("runtime request timed out")))
    }

    fn recv_runtime_reply<T>(
        &self,
        reply_rx: mpsc::Receiver<Result<T, String>>,
        context: &str,
    ) -> anyhow::Result<T> {
        reply_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("{context} reply channel closed: {error}"))?
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&self, topic: OverlayTopic) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::SubscribeTopic(topic))
            .map_err(|error| anyhow::anyhow!("failed to subscribe topic: {error}"))
    }

    /// Performs the publish control operation.
    pub fn publish_control(&self, announcement: ControlAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishControl(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send control announcement: {error}"))
    }

    /// Performs the publish head operation.
    pub fn publish_head(&self, announcement: HeadAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishHead(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send head announcement: {error}"))
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&self, announcement: LeaseAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishLease(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send lease announcement: {error}"))
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&self, announcement: MergeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMerge(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge announcement: {error}"))
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(
        &self,
        announcement: MergeWindowAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMergeWindow(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge window announcement: {error}"))
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(
        &self,
        announcement: ReducerAssignmentAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReducerAssignment(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send reducer assignment announcement: {error}")
            })
    }

    /// Performs the publish update operation.
    pub fn publish_update(&self, announcement: UpdateEnvelopeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishUpdate(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send update announcement: {error}"))
    }

    /// Performs the publish aggregate proposal operation.
    pub fn publish_aggregate_proposal(
        &self,
        announcement: AggregateProposalAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAggregateProposal(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send aggregate proposal announcement: {error}")
            })
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &self,
        announcement: ReductionCertificateAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReductionCertificate(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send reduction certificate announcement: {error}")
            })
    }

    /// Performs the publish validation quorum operation.
    pub fn publish_validation_quorum(
        &self,
        announcement: ValidationQuorumAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishValidationQuorum(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send validation quorum announcement: {error}")
            })
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(
        &self,
        announcement: ReducerLoadAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReducerLoad(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send reducer load announcement: {error}"))
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&self, announcement: PeerAuthAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAuth(Box::new(announcement)))
            .map_err(|error| anyhow::anyhow!("failed to send auth announcement: {error}"))
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(
        &self,
        announcement: ExperimentDirectoryAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDirectory(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send directory announcement: {error}"))
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&self, announcement: MetricsAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMetrics(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send metrics announcement: {error}"))
    }

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&self, peer_id: impl Into<String>) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::RequestSnapshot {
                peer_id: peer_id.into(),
            })
            .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))
    }

    /// Requests an outbound dial to a swarm address.
    pub fn dial_address(&self, address: SwarmAddress) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::DialAddress { address })
            .map_err(|error| anyhow::anyhow!("failed to request dial: {error}"))
    }

    /// Fetches the snapshot.
    pub fn fetch_snapshot(
        &self,
        peer_id: impl Into<String>,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneSnapshot> {
        let peer_id = peer_id.into();
        let (runtime_timeout, fallback_timeout) = split_fetch_timeout(timeout);
        let runtime_result = self.retry_runtime_request(runtime_timeout, |attempt_timeout| {
            let (reply_tx, reply_rx) = mpsc::channel();
            self.tx
                .send(RuntimeCommand::FetchSnapshot {
                    peer_id: peer_id.clone(),
                    timeout: attempt_timeout,
                    reply: reply_tx,
                })
                .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))?;
            self.recv_runtime_reply(reply_rx, "snapshot")
        });

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => self
                .fetch_snapshot_via_sidecar(&peer_id, fallback_timeout)
                .map_err(|fallback_error| {
                    anyhow::anyhow!(
                        "runtime snapshot fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                    )
                }),
            Err(error) => Err(error),
        }
    }

    /// Performs the publish artifact operation.
    pub fn publish_artifact(
        &self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::PublishArtifact {
                descriptor,
                chunks,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to publish artifact: {error}"))?;

        reply_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("artifact publish reply channel closed: {error}"))?
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Fetches the artifact manifest.
    pub fn fetch_artifact_manifest(
        &self,
        peer_id: impl Into<String>,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        let peer_id = peer_id.into();
        let (runtime_timeout, fallback_timeout) = split_fetch_timeout(timeout);
        let runtime_result = self.retry_runtime_request(runtime_timeout, |attempt_timeout| {
            let (reply_tx, reply_rx) = mpsc::channel();
            self.tx
                .send(RuntimeCommand::FetchArtifactManifest {
                    peer_id: peer_id.clone(),
                    artifact_id: artifact_id.clone(),
                    timeout: attempt_timeout,
                    reply: reply_tx,
                })
                .map_err(|error| anyhow::anyhow!("failed to request artifact manifest: {error}"))?;
            self.recv_runtime_reply(reply_rx, "artifact manifest")
        });

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => self
                .fetch_artifact_manifest_via_sidecar(&peer_id, artifact_id, fallback_timeout)
                .map_err(|fallback_error| {
                    anyhow::anyhow!(
                        "runtime artifact manifest fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                    )
                }),
            Err(error) => Err(error),
        }
    }

    /// Fetches the artifact chunk.
    pub fn fetch_artifact_chunk(
        &self,
        peer_id: impl Into<String>,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactChunkPayload>> {
        let peer_id = peer_id.into();
        let (runtime_timeout, fallback_timeout) = split_fetch_timeout(timeout);
        let runtime_result = self.retry_runtime_request(runtime_timeout, |attempt_timeout| {
            let (reply_tx, reply_rx) = mpsc::channel();
            self.tx
                .send(RuntimeCommand::FetchArtifactChunk {
                    peer_id: peer_id.clone(),
                    artifact_id: artifact_id.clone(),
                    chunk_id: chunk_id.clone(),
                    timeout: attempt_timeout,
                    reply: reply_tx,
                })
                .map_err(|error| anyhow::anyhow!("failed to request artifact chunk: {error}"))?;
            self.recv_runtime_reply(reply_rx, "artifact chunk")
        });

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => self
                .fetch_artifact_chunk_via_sidecar(
                    &peer_id,
                    artifact_id,
                    chunk_id,
                    fallback_timeout,
                )
                .map_err(|fallback_error| {
                    anyhow::anyhow!(
                        "runtime artifact chunk fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                    )
                }),
            Err(error) => Err(error),
        }
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.tx.send(RuntimeCommand::Shutdown) {
            Ok(()) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    fn fetch_artifact_manifest_via_sidecar(
        &self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        let mut shell = self.connect_fetch_sidecar(peer_id, timeout)?;
        shell
            .fetch_artifact_manifest(peer_id, artifact_id, timeout)
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    fn fetch_snapshot_via_sidecar(
        &self,
        peer_id: &str,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneSnapshot> {
        let mut shell = self.connect_fetch_sidecar(peer_id, timeout)?;
        shell
            .fetch_snapshot(peer_id, timeout)
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    fn fetch_artifact_chunk_via_sidecar(
        &self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactChunkPayload>> {
        let mut shell = self.connect_fetch_sidecar(peer_id, timeout)?;
        shell
            .fetch_artifact_chunk(peer_id, artifact_id, chunk_id, timeout)
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    fn connect_fetch_sidecar(
        &self,
        peer_id: &str,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneShell> {
        let peer_id = PeerId::new(peer_id.to_owned());
        let addresses = sidecar_peer_addresses(&self.telemetry.snapshot(), &peer_id);
        if addresses.is_empty() {
            anyhow::bail!("no known address for peer {}", peer_id.as_str());
        }

        let mut shell = ControlPlaneShell::new(
            self.runtime_boundary.protocols.control.clone(),
            Keypair::generate_ed25519(),
            addresses.clone(),
            self.runtime_boundary.transport_policy.clone(),
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;

        for address in &addresses {
            let _ = shell.dial(address.clone());
        }

        let deadline = Instant::now() + timeout.min(Duration::from_secs(2));
        while Instant::now() < deadline {
            if let Some(LiveControlPlaneEvent::ConnectionEstablished { peer_id: connected }) =
                shell.wait_event(Duration::from_millis(50))
                && connected == peer_id.as_str()
            {
                return Ok(shell);
            }
        }

        anyhow::bail!(
            "timed out connecting fetch sidecar to peer {} via {:?}",
            peer_id.as_str(),
            addresses
                .iter()
                .map(|address| address.as_str().to_owned())
                .collect::<Vec<_>>()
        )
    }
}
