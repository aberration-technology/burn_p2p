use super::transport::{sidecar_peer_addresses, split_fetch_timeout};
use super::*;

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
    /// Returns the runtime local peer id observed by telemetry.
    pub fn local_peer_id(&self) -> Option<PeerId> {
        self.telemetry.snapshot().local_peer_id
    }

    fn retry_runtime_request<T>(
        &self,
        timeout: Duration,
        request: impl FnMut(Duration) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        self.retry_runtime_request_with_slice(timeout, runtime_fetch_attempt_timeout, request)
    }

    fn retry_runtime_request_with_slice<T>(
        &self,
        timeout: Duration,
        attempt_timeout: impl Fn(Duration) -> Duration,
        mut request: impl FnMut(Duration) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        const RUNTIME_FETCH_RETRY_DELAY: Duration = Duration::from_millis(25);

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let attempt_timeout = attempt_timeout(deadline.saturating_duration_since(now));
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
        reply_rx: mpsc::Receiver<Result<T, String>>,
        context: &str,
        timeout: Duration,
    ) -> anyhow::Result<T> {
        match reply_rx.recv_timeout(timeout.max(Duration::from_millis(1))) {
            Ok(result) => result.map_err(|error| anyhow::anyhow!("{error}")),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
                "{context} runtime reply timed out after {} ms",
                timeout.as_millis()
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow::anyhow!("{context} reply channel closed"))
            }
        }
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

    /// Performs the publish lifecycle operation.
    pub fn publish_lifecycle(
        &self,
        announcement: ExperimentLifecycleAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishLifecycle(Box::new(announcement)))
            .map_err(|error| anyhow::anyhow!("failed to send lifecycle announcement: {error}"))
    }

    /// Performs the publish schedule operation.
    pub fn publish_schedule(&self, announcement: FleetScheduleAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishSchedule(Box::new(announcement)))
            .map_err(|error| anyhow::anyhow!("failed to send schedule announcement: {error}"))
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

    /// Performs the publish trainer promotion attestation operation.
    pub fn publish_trainer_promotion_attestation(
        &self,
        announcement: TrainerPromotionAttestationAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishTrainerPromotionAttestation(
                announcement,
            ))
            .map_err(|error| {
                anyhow::anyhow!("failed to send trainer promotion attestation: {error}")
            })
    }

    /// Performs the publish diffusion promotion certificate operation.
    pub fn publish_diffusion_promotion_certificate(
        &self,
        announcement: DiffusionPromotionCertificateAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDiffusionPromotionCertificate(
                announcement,
            ))
            .map_err(|error| {
                anyhow::anyhow!("failed to send diffusion promotion certificate: {error}")
            })
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

    /// Publishes the local DiLoCo state snapshot, outer optimizer state, and current parameters.
    pub fn publish_diloco_state(
        &self,
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDiLoCoState {
                snapshot,
                outer_optimizer_state,
                current_parameters,
            })
            .map_err(|error| anyhow::anyhow!("failed to publish DiLoCo state: {error}"))
    }

    /// Publishes one encoded DiLoCo pseudo-gradient manifest and chunk set.
    pub fn publish_diloco_gradient(
        &self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDiLoCoGradient { manifest, chunks })
            .map_err(|error| anyhow::anyhow!("failed to publish DiLoCo gradient: {error}"))
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
        let telemetry_snapshot = self.telemetry.snapshot();
        let peer = PeerId::new(peer_id.clone());
        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&telemetry_snapshot, &peer, timeout);
        let runtime_result = self.retry_runtime_request(runtime_timeout, |attempt_timeout| {
            let (reply_tx, reply_rx) = mpsc::channel();
            self.tx
                .send(RuntimeCommand::FetchSnapshot {
                    peer_id: peer_id.clone(),
                    timeout: attempt_timeout,
                    reply: reply_tx,
                })
                .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))?;
            Self::recv_runtime_reply(reply_rx, "snapshot", attempt_timeout)
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

        const ARTIFACT_PUBLISH_REPLY_TIMEOUT: Duration = Duration::from_secs(30);
        match reply_rx.recv_timeout(ARTIFACT_PUBLISH_REPLY_TIMEOUT) {
            Ok(result) => result.map_err(|error| anyhow::anyhow!("{error}")),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
                "artifact publish runtime reply timed out after {} ms",
                ARTIFACT_PUBLISH_REPLY_TIMEOUT.as_millis()
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow::anyhow!("artifact publish reply channel closed"))
            }
        }
    }

    /// Fetches the artifact manifest.
    pub fn fetch_artifact_manifest(
        &self,
        peer_id: impl Into<String>,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        let peer_id = peer_id.into();
        let telemetry_snapshot = self.telemetry.snapshot();
        let peer = PeerId::new(peer_id.clone());
        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&telemetry_snapshot, &peer, timeout);
        let runtime_result = self.retry_runtime_request_with_slice(
            runtime_timeout,
            artifact_runtime_fetch_attempt_timeout,
            |attempt_timeout| {
                let (reply_tx, reply_rx) = mpsc::channel();
                self.tx
                    .send(RuntimeCommand::FetchArtifactManifest {
                        peer_id: peer_id.clone(),
                        artifact_id: artifact_id.clone(),
                        timeout: attempt_timeout,
                        reply: reply_tx,
                    })
                    .map_err(|error| {
                        anyhow::anyhow!("failed to request artifact manifest: {error}")
                    })?;
                Self::recv_runtime_reply(reply_rx, "artifact manifest", attempt_timeout)
            },
        );

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
        let telemetry_snapshot = self.telemetry.snapshot();
        let peer = PeerId::new(peer_id.clone());
        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&telemetry_snapshot, &peer, timeout);
        let runtime_result = self.retry_runtime_request_with_slice(
            runtime_timeout,
            artifact_runtime_fetch_attempt_timeout,
            |attempt_timeout| {
                let (reply_tx, reply_rx) = mpsc::channel();
                self.tx
                    .send(RuntimeCommand::FetchArtifactChunk {
                        peer_id: peer_id.clone(),
                        artifact_id: artifact_id.clone(),
                        chunk_id: chunk_id.clone(),
                        timeout: attempt_timeout,
                        reply: reply_tx,
                    })
                    .map_err(|error| {
                        anyhow::anyhow!("failed to request artifact chunk: {error}")
                    })?;
                Self::recv_runtime_reply(reply_rx, "artifact chunk", attempt_timeout)
            },
        );

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

    /// Fetches a generic DiLoCo request/response payload.
    pub fn fetch_diloco(
        &self,
        peer_id: impl Into<String>,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        let peer_id = peer_id.into();
        let telemetry_snapshot = self.telemetry.snapshot();
        let peer = PeerId::new(peer_id.clone());
        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&telemetry_snapshot, &peer, timeout);
        let request_for_runtime = request.clone();
        let runtime_result = self.retry_runtime_request(runtime_timeout, |attempt_timeout| {
            let (reply_tx, reply_rx) = mpsc::channel();
            self.tx
                .send(RuntimeCommand::FetchDiLoCo {
                    peer_id: peer_id.clone(),
                    request: request_for_runtime.clone(),
                    timeout: attempt_timeout,
                    reply: reply_tx,
                })
                .map_err(|error| anyhow::anyhow!("failed to request DiLoCo payload: {error}"))?;
            Self::recv_runtime_reply(reply_rx, "diloco", attempt_timeout)
        });

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => self
                .fetch_diloco_via_sidecar(&peer_id, request, fallback_timeout)
                .map_err(|fallback_error| {
                    anyhow::anyhow!(
                        "runtime DiLoCo fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                    )
                }),
            Err(error) => Err(error),
        }
    }

    pub fn fetch_diloco_state_snapshot(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> anyhow::Result<Option<DiLoCoStateSnapshot>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::StateSnapshot {
                experiment_id,
                revision_id,
            },
            timeout,
        )? {
            DiLoCoResponse::StateSnapshot(snapshot) => Ok(snapshot),
            other => anyhow::bail!("unexpected DiLoCo state response: {other:?}"),
        }
    }

    pub fn fetch_diloco_outer_optimizer_state(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> anyhow::Result<Option<StateBlob>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::OuterOptimizerState {
                experiment_id,
                revision_id,
            },
            timeout,
        )? {
            DiLoCoResponse::OuterOptimizerState(state) => Ok(state),
            other => anyhow::bail!("unexpected DiLoCo outer-state response: {other:?}"),
        }
    }

    pub fn fetch_diloco_current_parameters(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> anyhow::Result<Option<FlattenedTensorPack>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::CurrentParameters {
                experiment_id,
                revision_id,
            },
            timeout,
        )? {
            DiLoCoResponse::CurrentParameters(parameters) => Ok(parameters),
            other => anyhow::bail!("unexpected DiLoCo parameter response: {other:?}"),
        }
    }

    pub fn fetch_diloco_gradient_manifest(
        &self,
        peer_id: impl Into<String>,
        manifest_id: ContentId,
        timeout: Duration,
    ) -> anyhow::Result<Option<PseudoGradientManifest>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::GradientManifest { manifest_id },
            timeout,
        )? {
            DiLoCoResponse::GradientManifest(manifest) => Ok(manifest),
            other => anyhow::bail!("unexpected DiLoCo manifest response: {other:?}"),
        }
    }

    pub fn fetch_diloco_gradient_chunk(
        &self,
        peer_id: impl Into<String>,
        manifest_id: ContentId,
        chunk_index: u32,
        timeout: Duration,
    ) -> anyhow::Result<Option<PseudoGradientChunk>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::GradientChunk {
                manifest_id,
                chunk_index,
            },
            timeout,
        )? {
            DiLoCoResponse::GradientChunk(chunk) => Ok(chunk),
            other => anyhow::bail!("unexpected DiLoCo chunk response: {other:?}"),
        }
    }

    pub fn send_diloco_round_offer(
        &self,
        peer_id: impl Into<String>,
        offer: DiLoCoRoundOffer,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        self.fetch_diloco(peer_id, DiLoCoRequest::RoundOffer(Box::new(offer)), timeout)
    }

    pub fn send_diloco_round_heartbeat(
        &self,
        peer_id: impl Into<String>,
        heartbeat: DiLoCoRoundHeartbeat,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        self.fetch_diloco(
            peer_id,
            DiLoCoRequest::RoundHeartbeat(Box::new(heartbeat)),
            timeout,
        )
    }

    pub fn send_diloco_round_finalize(
        &self,
        peer_id: impl Into<String>,
        finalize: DiLoCoRoundFinalize,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        self.fetch_diloco(
            peer_id,
            DiLoCoRequest::RoundFinalize(Box::new(finalize)),
            timeout,
        )
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.tx.send(RuntimeCommand::Shutdown) {
            Ok(()) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    fn fetch_diloco_via_sidecar(
        &self,
        peer_id: &str,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        let mut shell = self.connect_fetch_sidecar(peer_id, timeout)?;
        shell
            .fetch_diloco(peer_id, request, timeout)
            .map_err(|error| anyhow::anyhow!("{error}"))
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

    /// Fetches a control-plane snapshot by dialing a concrete swarm address with
    /// a short-lived sidecar. This prewarms from configured bootstrap seeds when
    /// the main runtime has not yet gossiped enough state to know which peer IDs
    /// should be queried.
    pub fn fetch_snapshot_from_address(
        &self,
        address: SwarmAddress,
        timeout: Duration,
    ) -> anyhow::Result<(PeerId, ControlPlaneSnapshot)> {
        let mut shell = ControlPlaneShell::new(
            self.runtime_boundary.protocols.control.clone(),
            Keypair::generate_ed25519(),
            [address.clone()],
            self.runtime_boundary.transport_policy.clone(),
            None,
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        shell
            .dial(address.clone())
            .map_err(|error| anyhow::anyhow!("{error}"))?;

        let deadline = Instant::now() + timeout;
        let mut connected_peer_id = None;
        let mut last_error = None;
        while Instant::now() < deadline {
            let wait_for = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100));
            let Some(event) = shell.wait_event(wait_for) else {
                continue;
            };
            match event {
                LiveControlPlaneEvent::ConnectionEstablished { peer_id } => {
                    connected_peer_id = Some(PeerId::new(peer_id));
                    break;
                }
                LiveControlPlaneEvent::OutgoingConnectionError { message, .. }
                | LiveControlPlaneEvent::IncomingConnectionError { message }
                | LiveControlPlaneEvent::InboundFailure { message, .. }
                | LiveControlPlaneEvent::ResponseSendFailure { message, .. }
                | LiveControlPlaneEvent::RequestFailure { message, .. } => {
                    last_error = Some(message);
                }
                _ => {}
            }
        }

        let connected_peer_id = connected_peer_id.ok_or_else(|| {
            let detail = last_error
                .map(|error| format!(": {error}"))
                .unwrap_or_default();
            anyhow::anyhow!(
                "timed out connecting bootstrap snapshot sidecar to {}{}",
                address.as_str(),
                detail
            )
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!(
                "bootstrap snapshot sidecar connected to {} but no time remained for snapshot fetch",
                connected_peer_id.as_str()
            );
        }

        let snapshot = shell
            .fetch_snapshot(connected_peer_id.as_str(), remaining)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok((connected_peer_id, snapshot))
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
            None,
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;

        for address in &addresses {
            let _ = shell.dial(address.clone());
        }

        let deadline = Instant::now() + timeout.min(Duration::from_secs(5));
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

fn runtime_fetch_attempt_timeout(remaining: Duration) -> Duration {
    const RUNTIME_FETCH_RETRY_SLICE: Duration = Duration::from_secs(5);

    remaining.min(RUNTIME_FETCH_RETRY_SLICE)
}

fn artifact_runtime_fetch_attempt_timeout(remaining: Duration) -> Duration {
    const ARTIFACT_RUNTIME_FETCH_RETRY_SLICE: Duration = Duration::from_secs(60);

    remaining.min(ARTIFACT_RUNTIME_FETCH_RETRY_SLICE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_fetch_attempt_timeout_preserves_short_sidecar_probe() {
        assert_eq!(
            runtime_fetch_attempt_timeout(Duration::from_millis(750)),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn runtime_fetch_attempt_timeout_allows_network_round_trip_under_load() {
        assert_eq!(
            runtime_fetch_attempt_timeout(Duration::from_secs(45)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn artifact_runtime_fetch_attempt_timeout_allows_large_chunk_transfers() {
        assert_eq!(
            artifact_runtime_fetch_attempt_timeout(Duration::from_secs(180)),
            Duration::from_secs(60)
        );
        assert_eq!(
            artifact_runtime_fetch_attempt_timeout(Duration::from_millis(750)),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn runtime_reply_wait_is_bounded() {
        let (_reply_tx, reply_rx) = mpsc::channel::<Result<(), String>>();
        let started = Instant::now();
        let error =
            ControlHandle::recv_runtime_reply(reply_rx, "snapshot", Duration::from_millis(10))
                .expect_err("reply wait should time out");

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "reply wait was not bounded"
        );
        assert!(
            error
                .to_string()
                .contains("snapshot runtime reply timed out")
        );
    }
}
