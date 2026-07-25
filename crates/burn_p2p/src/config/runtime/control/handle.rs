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

    fn record_request_failure(&self, kind: RequestFailureKind) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.record_request_failure(kind);
    }

    fn record_fetch_error(&self, operation: RequestFailureOperation, error: &anyhow::Error) {
        self.record_request_failure(RequestFailureKind::new(
            operation,
            classify_request_failure(error),
        ));
    }

    fn record_missing_payload(&self, operation: RequestFailureOperation) {
        self.record_request_failure(RequestFailureKind::new(
            operation,
            RequestFailureReason::NotFound,
        ));
    }

    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&self, topic: OverlayTopic) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::SubscribeTopic(topic))
            .map_err(|error| anyhow::anyhow!("failed to subscribe topic: {error}"))
    }

    /// Replaces the active runtime roles without rebuilding the transport.
    ///
    /// Compute roles may only be re-enabled when they were part of the node's
    /// startup capability set. Read-only roles can always be selected.
    pub fn update_roles(&self, roles: PeerRoleSet, timeout: Duration) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::UpdateRoles {
                roles,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to request runtime role update: {error}"))?;
        Self::recv_runtime_reply(reply_rx, "runtime role update", timeout)
    }

    /// Clears one already-observed runtime error if it still matches exactly.
    ///
    /// The compare-and-clear contract prevents a capability controller from
    /// accidentally acknowledging a newer failure that arrived during a
    /// recovery probe.
    pub fn acknowledge_runtime_error(
        &self,
        expected: impl Into<String>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::AcknowledgeRuntimeError {
                expected: expected.into(),
                reply: reply_tx,
            })
            .map_err(|error| {
                anyhow::anyhow!("failed to request runtime error acknowledgement: {error}")
            })?;
        Self::recv_runtime_reply(reply_rx, "runtime error acknowledgement", timeout)
    }

    /// Performs the publish control operation.
    pub fn publish_control(&self, announcement: ControlAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishControl(Box::new(announcement)))
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
            .send(RuntimeCommand::PublishUpdate(Box::new(announcement)))
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
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::PublishDiLoCoState {
                snapshot,
                outer_optimizer_state,
                current_parameters,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to publish DiLoCo state: {error}"))?;
        Self::recv_runtime_reply(
            reply_rx,
            "publish DiLoCo state",
            burn_p2p_swarm::CONTROL_REQUEST_RESPONSE_TIMEOUT,
        )
    }

    /// Publishes one encoded DiLoCo pseudo-gradient manifest and chunk set.
    pub fn publish_diloco_gradient(
        &self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::PublishDiLoCoGradient {
                manifest,
                chunks,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to publish DiLoCo gradient: {error}"))?;
        Self::recv_runtime_reply(
            reply_rx,
            "publish DiLoCo gradient",
            burn_p2p_swarm::CONTROL_REQUEST_RESPONSE_TIMEOUT,
        )
    }

    /// Waits for one aggregate-ready release through the local runtime event loop.
    pub fn wait_for_diloco_aggregate_ready(
        &self,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        reducer_peer_id: PeerId,
        round_cursor: RoundCursor,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoAggregateReady> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::WaitDiLoCoAggregateReady {
                experiment_id,
                revision_id,
                reducer_peer_id,
                round_cursor,
                timeout,
                reply: reply_tx,
            })
            .map_err(|error| {
                anyhow::anyhow!("failed to wait for DiLoCo aggregate readiness: {error}")
            })?;
        Self::recv_runtime_reply(
            reply_rx,
            "wait for DiLoCo aggregate readiness",
            runtime_reply_completion_timeout(timeout),
        )
    }

    /// Publishes one reduced DiLoCo aggregate and its exact cohort commitment.
    pub fn publish_diloco_aggregate(
        &self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        participant_peer_ids: Vec<PeerId>,
        contribution_manifest_ids: Vec<ContentId>,
    ) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::PublishDiLoCoAggregate {
                manifest,
                chunks,
                participant_peer_ids,
                contribution_manifest_ids,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to publish DiLoCo aggregate: {error}"))?;
        Self::recv_runtime_reply(
            reply_rx,
            "publish DiLoCo aggregate",
            burn_p2p_swarm::CONTROL_REQUEST_RESPONSE_TIMEOUT,
        )
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
            Self::recv_runtime_reply(
                reply_rx,
                "snapshot",
                runtime_reply_completion_timeout(attempt_timeout),
            )
        });

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => {
                match self.fetch_snapshot_via_sidecar(&peer_id, fallback_timeout) {
                    Ok(result) => Ok(result),
                    Err(fallback_error) => {
                        let error = anyhow::anyhow!(
                            "runtime snapshot fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                        );
                        self.record_fetch_error(RequestFailureOperation::SnapshotFetch, &error);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.record_fetch_error(RequestFailureOperation::SnapshotFetch, &error);
                Err(error)
            }
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
                Self::recv_runtime_reply(
                    reply_rx,
                    "artifact manifest",
                    runtime_reply_completion_timeout(attempt_timeout),
                )
            },
        );

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => {
                match self.fetch_artifact_manifest_via_sidecar(
                    &peer_id,
                    artifact_id,
                    fallback_timeout,
                ) {
                    Ok(result) => Ok(result),
                    Err(fallback_error) => {
                        let error = anyhow::anyhow!(
                            "runtime artifact manifest fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                        );
                        self.record_fetch_error(
                            RequestFailureOperation::ArtifactManifestFetch,
                            &error,
                        );
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.record_fetch_error(RequestFailureOperation::ArtifactManifestFetch, &error);
                Err(error)
            }
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
                Self::recv_runtime_reply(
                    reply_rx,
                    "artifact chunk",
                    runtime_reply_completion_timeout(attempt_timeout),
                )
            },
        );

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => {
                match self.fetch_artifact_chunk_via_sidecar(
                    &peer_id,
                    artifact_id,
                    chunk_id,
                    fallback_timeout,
                ) {
                    Ok(result) => Ok(result),
                    Err(fallback_error) => {
                        let error = anyhow::anyhow!(
                            "runtime artifact chunk fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                        );
                        self.record_fetch_error(
                            RequestFailureOperation::ArtifactChunkFetch,
                            &error,
                        );
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.record_fetch_error(RequestFailureOperation::ArtifactChunkFetch, &error);
                Err(error)
            }
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
        let operation = diloco_request_failure_operation(&request);
        let telemetry_snapshot = self.telemetry.snapshot();
        let peer = PeerId::new(peer_id.clone());
        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&telemetry_snapshot, &peer, timeout);
        let runtime_result = self.fetch_diloco_from_runtime(&peer_id, &request, runtime_timeout);

        match runtime_result {
            Ok(result) => Ok(result),
            Err(primary_error) if fallback_timeout > Duration::ZERO => {
                match self.fetch_diloco_via_sidecar(&peer_id, request, fallback_timeout) {
                    Ok(result) => Ok(result),
                    Err(fallback_error) => {
                        let error = anyhow::anyhow!(
                            "runtime DiLoCo fetch failed: {primary_error}; sidecar fallback failed: {fallback_error}"
                        );
                        self.record_fetch_error(operation, &error);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.record_fetch_error(operation, &error);
                Err(error)
            }
        }
    }

    fn fetch_diloco_from_runtime(
        &self,
        peer_id: &str,
        request: &DiLoCoRequest,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        let logical_timeout = timeout.max(Duration::from_millis(1));
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::FetchDiLoCo {
                peer_id: peer_id.to_owned(),
                request: request.clone(),
                timeout: logical_timeout,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to request DiLoCo payload: {error}"))?;
        Self::recv_runtime_reply(
            reply_rx,
            "diloco",
            runtime_reply_completion_timeout(logical_timeout),
        )
    }

    fn fetch_diloco_runtime_only(
        &self,
        peer_id: impl Into<String>,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        let peer_id = peer_id.into();
        let operation = diloco_request_failure_operation(&request);
        let result = self.fetch_diloco_from_runtime(&peer_id, &request, timeout);
        if let Err(error) = result.as_ref() {
            self.record_fetch_error(operation, error);
        }
        result
    }

    pub(crate) fn fetch_diloco_concurrently(
        &self,
        requests: Vec<(PeerId, DiLoCoRequest)>,
        timeout: Duration,
    ) -> Vec<(PeerId, anyhow::Result<DiLoCoResponse>)> {
        const POLL_INTERVAL: Duration = Duration::from_millis(2);

        let logical_timeout = timeout.max(Duration::from_millis(1));
        let runtime_timeout = logical_timeout;
        let runtime_deadline = Instant::now() + runtime_reply_completion_timeout(runtime_timeout);
        let mut results = Vec::with_capacity(requests.len());
        let mut pending = Vec::with_capacity(requests.len());
        for (peer_id, request) in requests {
            let operation = diloco_request_failure_operation(&request);
            let (reply, response) = mpsc::channel();
            let command = RuntimeCommand::FetchDiLoCo {
                peer_id: peer_id.as_str().to_owned(),
                request: request.clone(),
                timeout: runtime_timeout,
                reply,
            };
            match self.tx.send(command) {
                Ok(()) => pending.push((peer_id, operation, response)),
                Err(error) => {
                    let error =
                        anyhow::anyhow!("failed to request concurrent DiLoCo payload: {error}");
                    self.record_fetch_error(operation, &error);
                    results.push((peer_id, Err(error)));
                }
            }
        }

        while !pending.is_empty() && Instant::now() < runtime_deadline {
            let mut progressed = false;
            let mut index = pending.len();
            while index > 0 {
                index -= 1;
                let response = match pending[index].2.try_recv() {
                    Ok(response) => Some(response.map_err(anyhow::Error::msg)),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        Some(Err(anyhow::anyhow!("DiLoCo reply channel closed")))
                    }
                    Err(mpsc::TryRecvError::Empty) => None,
                };
                let Some(response) = response else {
                    continue;
                };
                progressed = true;
                let (peer_id, operation, _) = pending.swap_remove(index);
                if let Err(error) = response.as_ref() {
                    self.record_fetch_error(operation, error);
                }
                results.push((peer_id, response));
            }
            if !progressed {
                std::thread::sleep(POLL_INTERVAL);
            }
        }

        for (peer_id, operation, _) in pending {
            let response = Err(anyhow::anyhow!(
                "DiLoCo concurrent persistent-path reply timed out after {} ms",
                runtime_timeout.as_millis()
            ));
            if let Err(error) = response.as_ref() {
                self.record_fetch_error(operation, error);
            }
            results.push((peer_id, response));
        }
        results
    }

    pub fn fetch_diloco_state_snapshot(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> anyhow::Result<Option<DiLoCoStateSnapshot>> {
        match self.fetch_diloco_runtime_only(
            peer_id,
            DiLoCoRequest::StateSnapshot {
                experiment_id,
                revision_id,
            },
            timeout,
        )? {
            DiLoCoResponse::StateSnapshot(Some(snapshot)) => Ok(Some(snapshot)),
            DiLoCoResponse::StateSnapshot(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoStateFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoStateFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo state response: {other:?}")
            }
        }
    }

    pub(crate) fn fetch_diloco_state_bundle(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> anyhow::Result<Option<DiLoCoStateBundle>> {
        match self.fetch_diloco(
            peer_id,
            DiLoCoRequest::StateBundle {
                experiment_id,
                revision_id,
            },
            timeout,
        )? {
            DiLoCoResponse::StateBundle(Some(bundle)) => Ok(Some(*bundle)),
            DiLoCoResponse::StateBundle(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoParameterStateFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoParameterStateFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo state-bundle response: {other:?}")
            }
        }
    }

    pub(crate) fn fetch_diloco_state_snapshots_concurrently(
        &self,
        peer_ids: &[PeerId],
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        timeout: Duration,
    ) -> Vec<(PeerId, anyhow::Result<Option<DiLoCoStateSnapshot>>)> {
        const POLL_INTERVAL: Duration = Duration::from_millis(2);

        let deadline = Instant::now() + timeout.max(Duration::from_millis(1));
        let mut results = Vec::with_capacity(peer_ids.len());
        let mut pending = Vec::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            let (reply, response) = mpsc::channel();
            let command = RuntimeCommand::FetchDiLoCo {
                peer_id: peer_id.as_str().to_owned(),
                request: DiLoCoRequest::StateSnapshot {
                    experiment_id: experiment_id.clone(),
                    revision_id: revision_id.clone(),
                },
                timeout,
                reply,
            };
            match self.tx.send(command) {
                Ok(()) => pending.push((peer_id.clone(), response)),
                Err(error) => results.push((
                    peer_id.clone(),
                    Err(anyhow::anyhow!(
                        "failed to request DiLoCo state snapshot: {error}"
                    )),
                )),
            }
        }

        while !pending.is_empty() && Instant::now() < deadline {
            let mut progressed = false;
            let mut index = pending.len();
            while index > 0 {
                index -= 1;
                let response = match pending[index].1.try_recv() {
                    Ok(response) => Some(response.map_err(anyhow::Error::msg)),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        Some(Err(anyhow::anyhow!("DiLoCo state reply channel closed")))
                    }
                    Err(mpsc::TryRecvError::Empty) => None,
                };
                let Some(response) = response else {
                    continue;
                };
                progressed = true;
                let (peer_id, _) = pending.swap_remove(index);
                let response = response.and_then(|response| match response {
                    DiLoCoResponse::StateSnapshot(snapshot) => Ok(snapshot),
                    other => Err(anyhow::anyhow!(
                        "unexpected DiLoCo state response: {other:?}"
                    )),
                });
                results.push((peer_id, response));
            }

            if !progressed {
                std::thread::sleep(POLL_INTERVAL);
            }
        }

        for (peer_id, _) in pending {
            let error = anyhow::anyhow!(
                "DiLoCo state snapshot batch timed out after {} ms",
                timeout.as_millis()
            );
            self.record_fetch_error(RequestFailureOperation::DiLoCoStateFetch, &error);
            results.push((peer_id, Err(error)));
        }
        results
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
            DiLoCoResponse::OuterOptimizerState(Some(state)) => Ok(Some(state)),
            DiLoCoResponse::OuterOptimizerState(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoParameterStateFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoParameterStateFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo outer-state response: {other:?}")
            }
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
            DiLoCoResponse::CurrentParameters(Some(parameters)) => Ok(Some(parameters)),
            DiLoCoResponse::CurrentParameters(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoParameterStateFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoParameterStateFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo parameter response: {other:?}")
            }
        }
    }

    pub fn fetch_diloco_gradient_manifest(
        &self,
        peer_id: impl Into<String>,
        manifest_id: ContentId,
        timeout: Duration,
    ) -> anyhow::Result<Option<PseudoGradientManifest>> {
        match self.fetch_diloco_runtime_only(
            peer_id,
            DiLoCoRequest::GradientManifest { manifest_id },
            timeout,
        )? {
            DiLoCoResponse::GradientManifest(Some(manifest)) => Ok(Some(manifest)),
            DiLoCoResponse::GradientManifest(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoGradientManifestFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoGradientManifestFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo manifest response: {other:?}")
            }
        }
    }

    pub fn fetch_diloco_gradient_chunk(
        &self,
        peer_id: impl Into<String>,
        manifest_id: ContentId,
        chunk_index: u32,
        timeout: Duration,
    ) -> anyhow::Result<Option<PseudoGradientChunk>> {
        match self.fetch_diloco_runtime_only(
            peer_id,
            DiLoCoRequest::GradientChunk {
                manifest_id,
                chunk_index,
            },
            timeout,
        )? {
            DiLoCoResponse::GradientChunk(Some(chunk)) => Ok(Some(chunk)),
            DiLoCoResponse::GradientChunk(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoGradientChunkFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoGradientChunkFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo chunk response: {other:?}")
            }
        }
    }

    pub fn fetch_diloco_gradient_slice(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        round_cursor: RoundCursor,
        chunk_index: u32,
        timeout: Duration,
    ) -> anyhow::Result<Option<DiLoCoGradientSlice>> {
        let peer_id = peer_id.into();
        match self.fetch_diloco(
            &peer_id,
            DiLoCoRequest::GradientSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index,
            },
            timeout,
        )? {
            DiLoCoResponse::GradientSlice(Some(slice)) => Ok(Some(*slice)),
            DiLoCoResponse::GradientSlice(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoGradientChunkFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoGradientChunkFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo gradient-slice response: {other:?}")
            }
        }
    }

    pub fn fetch_diloco_aggregate_slice(
        &self,
        peer_id: impl Into<String>,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        round_cursor: RoundCursor,
        chunk_index: u32,
        timeout: Duration,
    ) -> anyhow::Result<Option<DiLoCoAggregateSlice>> {
        let peer_id = peer_id.into();
        match self.fetch_diloco(
            &peer_id,
            DiLoCoRequest::AggregateSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index,
            },
            timeout,
        )? {
            DiLoCoResponse::AggregateSlice(Some(slice)) => Ok(Some(*slice)),
            DiLoCoResponse::AggregateSlice(None) => {
                self.record_missing_payload(RequestFailureOperation::DiLoCoAggregateChunkFetch);
                Ok(None)
            }
            other => {
                self.record_request_failure(RequestFailureKind::new(
                    RequestFailureOperation::DiLoCoAggregateChunkFetch,
                    RequestFailureReason::UnexpectedResponse,
                ));
                anyhow::bail!("unexpected DiLoCo aggregate-slice response: {other:?}")
            }
        }
    }

    pub fn send_diloco_round_offer(
        &self,
        peer_id: impl Into<String>,
        offer: DiLoCoRoundOffer,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        self.fetch_diloco_runtime_only(peer_id, DiLoCoRequest::RoundOffer(Box::new(offer)), timeout)
    }

    pub fn send_diloco_round_heartbeat(
        &self,
        peer_id: impl Into<String>,
        heartbeat: DiLoCoRoundHeartbeat,
        timeout: Duration,
    ) -> anyhow::Result<DiLoCoResponse> {
        self.fetch_diloco_runtime_only(
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
        self.fetch_diloco_runtime_only(
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
        let transport_policy =
            fetch_sidecar_transport_policy(self.runtime_boundary.transport_policy.clone());
        let mut shell = ControlPlaneShell::new(
            self.runtime_boundary.protocols.control.clone(),
            Keypair::generate_ed25519(),
            [address.clone()],
            transport_policy,
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

        let transport_policy =
            fetch_sidecar_transport_policy(self.runtime_boundary.transport_policy.clone());
        let mut shell = ControlPlaneShell::new(
            self.runtime_boundary.protocols.control.clone(),
            Keypair::generate_ed25519(),
            addresses.clone(),
            transport_policy,
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

fn fetch_sidecar_transport_policy(
    mut policy: burn_p2p_swarm::RuntimeTransportPolicy,
) -> burn_p2p_swarm::RuntimeTransportPolicy {
    // A sidecar exists for one bounded direct fetch. Registering its ephemeral
    // identity with discovery services leaves dead peers behind for the full
    // rendezvous TTL and can make healthy runtimes spend their connection
    // budget redialing addresses that can never return.
    policy.target_connected_peers = 1;
    policy.target_bootstrap_seed_connections = 0;
    policy.enable_local_discovery = false;
    policy.enable_relay_server = false;
    policy.enable_hole_punching = false;
    policy.enable_autonat = false;
    policy.enable_rendezvous_client = false;
    policy.enable_rendezvous_server = false;
    policy.enable_kademlia = false;
    policy.advertise_for_discovery = false;
    policy.export_openmetrics = false;
    policy
}

fn classify_request_failure(error: &anyhow::Error) -> RequestFailureReason {
    let message = error.to_string().to_lowercase();
    if message.contains("timed out") || message.contains("timeout") {
        RequestFailureReason::Timeout
    } else if message.contains("no known address")
        || message.contains("no connected peer")
        || message.contains("provider unavailable")
        || message.contains("unavailable")
    {
        RequestFailureReason::ProviderUnavailable
    } else if message.contains("not found") || message.contains("missing") {
        RequestFailureReason::NotFound
    } else if message.contains("unexpected") || message.contains("mismatch") {
        RequestFailureReason::UnexpectedResponse
    } else if message.contains("admission") || message.contains("not admitted") {
        RequestFailureReason::AdmissionRejected
    } else if message.contains("transport")
        || message.contains("connection")
        || message.contains("channel closed")
        || message.contains("request")
    {
        RequestFailureReason::Transport
    } else {
        RequestFailureReason::Unknown
    }
}

fn diloco_request_failure_operation(request: &DiLoCoRequest) -> RequestFailureOperation {
    match request {
        DiLoCoRequest::StateSnapshot { .. } => RequestFailureOperation::DiLoCoStateFetch,
        DiLoCoRequest::StateBundle { .. }
        | DiLoCoRequest::OuterOptimizerState { .. }
        | DiLoCoRequest::CurrentParameters { .. } => {
            RequestFailureOperation::DiLoCoParameterStateFetch
        }
        DiLoCoRequest::GradientManifest { .. } => {
            RequestFailureOperation::DiLoCoGradientManifestFetch
        }
        DiLoCoRequest::GradientChunk { .. } | DiLoCoRequest::GradientSlice { .. } => {
            RequestFailureOperation::DiLoCoGradientChunkFetch
        }
        DiLoCoRequest::AggregateSlice { .. } => RequestFailureOperation::DiLoCoAggregateChunkFetch,
        DiLoCoRequest::AggregateReady(_) => RequestFailureOperation::DiLoCoRoundRequest,
        DiLoCoRequest::RoundOffer(_)
        | DiLoCoRequest::RoundHeartbeat(_)
        | DiLoCoRequest::RoundFinalize(_) => RequestFailureOperation::DiLoCoRoundRequest,
    }
}

fn runtime_fetch_attempt_timeout(remaining: Duration) -> Duration {
    // Logical retries are coalesced by the runtime while the underlying
    // request-response stream remains active.
    const RUNTIME_FETCH_RETRY_SLICE: Duration = Duration::from_secs(12);

    remaining.min(RUNTIME_FETCH_RETRY_SLICE)
}

fn runtime_reply_completion_timeout(operation_timeout: Duration) -> Duration {
    const RUNTIME_REPLY_COMPLETION_GRACE: Duration = Duration::from_millis(500);

    operation_timeout.saturating_add(RUNTIME_REPLY_COMPLETION_GRACE)
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
            Duration::from_secs(12)
        );
    }

    #[test]
    fn runtime_reply_completion_timeout_keeps_worker_and_caller_deadlines_ordered() {
        assert_eq!(
            runtime_reply_completion_timeout(Duration::from_secs(5)),
            Duration::from_millis(5_500)
        );
    }

    #[test]
    fn fetch_sidecar_disables_ephemeral_discovery_registration() {
        let mut base = burn_p2p_swarm::RuntimeTransportPolicy::native_for_roles(
            &PeerRoleSet::default_trainer(),
        );
        base.enable_local_discovery = true;
        let sidecar = fetch_sidecar_transport_policy(base.clone());

        assert_eq!(sidecar.target_connected_peers, 1);
        assert_eq!(sidecar.target_bootstrap_seed_connections, 0);
        assert!(!sidecar.enable_local_discovery);
        assert!(!sidecar.enable_rendezvous_client);
        assert!(!sidecar.enable_rendezvous_server);
        assert!(!sidecar.enable_kademlia);
        assert!(!sidecar.enable_hole_punching);
        assert!(!sidecar.enable_autonat);
        assert!(!sidecar.enable_relay_server);
        assert!(!sidecar.advertise_for_discovery);
        assert_eq!(sidecar.enable_relay_client, base.enable_relay_client);
        assert!(!sidecar.export_openmetrics);
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
