use super::*;

enum PendingControlRequest {
    Snapshot {
        peer_id: String,
        deadline: Instant,
        reply: mpsc::Sender<Result<ControlPlaneSnapshot, String>>,
    },
    ArtifactManifest {
        peer_id: String,
        deadline: Instant,
        reply: mpsc::Sender<Result<Option<ArtifactDescriptor>, String>>,
    },
    ArtifactChunk {
        peer_id: String,
        deadline: Instant,
        reply: mpsc::Sender<Result<Option<ArtifactChunkPayload>, String>>,
    },
    DiLoCo {
        peer_id: String,
        coalesce_key: String,
        deadline: Instant,
        replies: Vec<mpsc::Sender<Result<DiLoCoResponse, String>>>,
    },
}

impl PendingControlRequest {
    fn deadline(&self) -> Instant {
        match self {
            Self::Snapshot { deadline, .. }
            | Self::ArtifactManifest { deadline, .. }
            | Self::ArtifactChunk { deadline, .. }
            | Self::DiLoCo { deadline, .. } => *deadline,
        }
    }

    fn operation(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::ArtifactManifest { .. } => "artifact manifest",
            Self::ArtifactChunk { .. } => "artifact chunk",
            Self::DiLoCo { .. } => "diloco",
        }
    }

    fn fail(self, message: String) {
        match self {
            Self::Snapshot { reply, .. } => {
                let _ = reply.send(Err(message));
            }
            Self::ArtifactManifest { reply, .. } => {
                let _ = reply.send(Err(message));
            }
            Self::ArtifactChunk { reply, .. } => {
                let _ = reply.send(Err(message));
            }
            Self::DiLoCo { replies, .. } => {
                for reply in replies {
                    let _ = reply.send(Err(message.clone()));
                }
            }
        }
    }
}

struct PendingDiLoCoAggregateReady {
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    reducer_peer_id: PeerId,
    round_cursor: RoundCursor,
    deadline: Instant,
    reply: mpsc::Sender<Result<DiLoCoAggregateReady, String>>,
}

fn settle_pending_diloco_aggregate_ready(
    shell: &ControlPlaneShell,
    pending: &mut Vec<PendingDiLoCoAggregateReady>,
) {
    let mut index = pending.len();
    while index > 0 {
        index -= 1;
        let ready = {
            let waiter = &pending[index];
            shell.diloco_aggregate_ready(
                &waiter.experiment_id,
                &waiter.revision_id,
                &waiter.reducer_peer_id,
                &waiter.round_cursor,
            )
        };
        if ready.is_none() && Instant::now() < pending[index].deadline {
            continue;
        }
        let waiter = pending.swap_remove(index);
        let result = ready.ok_or_else(|| {
            format!(
                "timed out waiting for aggregate-ready release from reducer {} for round {}",
                waiter.reducer_peer_id.as_str(),
                waiter.round_cursor.round_id
            )
        });
        let _ = waiter.reply.send(result);
    }
}

fn diloco_request_coalesce_key(peer_id: &str, request: &DiLoCoRequest) -> String {
    let semantic_request = match request {
        DiLoCoRequest::RoundOffer(offer) => serde_json::json!({
            "kind": "round-offer",
            "experiment_id": offer.experiment_id,
            "revision_id": offer.revision_id,
            "peer_id": offer.peer_id,
            "round_cursor": offer.round_cursor,
            "target_group_size": offer.target_group_size,
        }),
        DiLoCoRequest::RoundHeartbeat(heartbeat) => serde_json::json!({
            "kind": "round-heartbeat",
            "experiment_id": heartbeat.experiment_id,
            "revision_id": heartbeat.revision_id,
            "peer_id": heartbeat.peer_id,
            "round_cursor": heartbeat.round_cursor,
            "observed_participants": heartbeat.observed_participants,
        }),
        DiLoCoRequest::RoundFinalize(finalize) => serde_json::json!({
            "kind": "round-finalize",
            "experiment_id": finalize.experiment_id,
            "revision_id": finalize.revision_id,
            "peer_id": finalize.peer_id,
            "round_cursor": finalize.round_cursor,
            "participant_count": finalize.participant_count,
            "aggregate_checksum": finalize.aggregate_checksum,
        }),
        _ => serde_json::to_value(request).unwrap_or_else(|_| {
            serde_json::json!({
                "debug": format!("{request:?}"),
            })
        }),
    };
    ContentId::derive(&(peer_id, semantic_request))
        .map(|content_id| format!("diloco:{peer_id}:{}", content_id.as_str()))
        .unwrap_or_else(|_| format!("diloco:{peer_id}:{request:?}"))
}

fn attach_to_pending_diloco_request(
    pending: &mut BTreeMap<String, PendingControlRequest>,
    pending_diloco_request_ids_by_key: &BTreeMap<String, String>,
    coalesce_key: &str,
    reply: mpsc::Sender<Result<DiLoCoResponse, String>>,
) -> Result<(), mpsc::Sender<Result<DiLoCoResponse, String>>> {
    let Some(request_id) = pending_diloco_request_ids_by_key.get(coalesce_key) else {
        return Err(reply);
    };
    let Some(PendingControlRequest::DiLoCo { replies, .. }) = pending.get_mut(request_id) else {
        return Err(reply);
    };
    replies.push(reply);
    Ok(())
}

fn remove_pending_control_request(
    pending: &mut BTreeMap<String, PendingControlRequest>,
    pending_diloco_request_ids_by_key: &mut BTreeMap<String, String>,
    request_id: &str,
) -> Option<PendingControlRequest> {
    let request = pending.remove(request_id)?;
    if let PendingControlRequest::DiLoCo { coalesce_key, .. } = &request {
        pending_diloco_request_ids_by_key.remove(coalesce_key);
    }
    Some(request)
}

fn settle_pending_control_requests(
    shell: &mut ControlPlaneShell,
    pending: &mut BTreeMap<String, PendingControlRequest>,
    pending_diloco_request_ids_by_key: &mut BTreeMap<String, String>,
) {
    let request_ids = pending.keys().cloned().collect::<Vec<_>>();
    for request_id in request_ids {
        let response = pending
            .get(&request_id)
            .is_some_and(|request| matches!(request, PendingControlRequest::DiLoCo { .. }))
            .then(|| shell.take_diloco_response(&request_id))
            .flatten();
        let timed_out = pending
            .get(&request_id)
            .is_some_and(|request| Instant::now() >= request.deadline());
        if response.is_none() && !timed_out {
            continue;
        }
        let Some(request) =
            remove_pending_control_request(pending, pending_diloco_request_ids_by_key, &request_id)
        else {
            continue;
        };
        match request {
            PendingControlRequest::DiLoCo {
                peer_id, replies, ..
            } => {
                let result = match response {
                    Some((response_peer_id, response)) if response_peer_id == peer_id => {
                        Ok(response)
                    }
                    Some((response_peer_id, _)) => Err(format!(
                        "DiLoCo response peer mismatch: expected {peer_id}, got {response_peer_id}"
                    )),
                    None => Err("timed out waiting for diloco".into()),
                };
                for reply in replies {
                    let _ = reply.send(result.clone());
                }
            }
            request => {
                let operation = request.operation();
                request.fail(format!("timed out waiting for {operation}"));
            }
        }
    }
    // The transport may still deliver a response after its logical request
    // deadline. No future retry may consume that stale payload.
    shell.discard_completed_diloco_responses();
}

fn route_pending_control_response(
    pending: &mut BTreeMap<String, PendingControlRequest>,
    pending_diloco_request_ids_by_key: &mut BTreeMap<String, String>,
    event: LiveControlPlaneEvent,
) -> Option<LiveControlPlaneEvent> {
    if let LiveControlPlaneEvent::RequestFailure {
        request_id: Some(request_id),
        message,
        ..
    } = &event
    {
        if let Some(request) =
            remove_pending_control_request(pending, pending_diloco_request_ids_by_key, request_id)
        {
            request.fail(message.clone());
        }
        return Some(event);
    }

    match event {
        LiveControlPlaneEvent::SnapshotReceived {
            peer_id,
            request_id,
            snapshot,
        } => {
            let Some(request) = remove_pending_control_request(
                pending,
                pending_diloco_request_ids_by_key,
                &request_id,
            ) else {
                return Some(LiveControlPlaneEvent::SnapshotReceived {
                    peer_id,
                    request_id,
                    snapshot,
                });
            };
            match request {
                PendingControlRequest::Snapshot {
                    peer_id: expected,
                    reply,
                    ..
                } => {
                    let result = if peer_id == expected {
                        Ok(snapshot)
                    } else {
                        Err(format!(
                            "snapshot response peer mismatch: expected {expected}, got {peer_id}"
                        ))
                    };
                    let _ = reply.send(result);
                    None
                }
                other => {
                    other.fail("snapshot response type did not match pending request".into());
                    Some(LiveControlPlaneEvent::SnapshotReceived {
                        peer_id,
                        request_id,
                        snapshot,
                    })
                }
            }
        }
        LiveControlPlaneEvent::ArtifactManifestReceived {
            peer_id,
            request_id,
            descriptor,
        } => {
            let Some(request) = remove_pending_control_request(
                pending,
                pending_diloco_request_ids_by_key,
                &request_id,
            ) else {
                return Some(LiveControlPlaneEvent::ArtifactManifestReceived {
                    peer_id,
                    request_id,
                    descriptor,
                });
            };
            match request {
                PendingControlRequest::ArtifactManifest {
                    peer_id: expected,
                    reply,
                    ..
                } => {
                    let result = if peer_id == expected {
                        Ok(descriptor)
                    } else {
                        Err(format!(
                            "artifact manifest response peer mismatch: expected {expected}, got {peer_id}"
                        ))
                    };
                    let _ = reply.send(result);
                    None
                }
                other => {
                    other.fail(
                        "artifact manifest response type did not match pending request".into(),
                    );
                    Some(LiveControlPlaneEvent::ArtifactManifestReceived {
                        peer_id,
                        request_id,
                        descriptor,
                    })
                }
            }
        }
        LiveControlPlaneEvent::ArtifactChunkReceived {
            peer_id,
            request_id,
            payload,
        } => {
            let Some(request) = remove_pending_control_request(
                pending,
                pending_diloco_request_ids_by_key,
                &request_id,
            ) else {
                return Some(LiveControlPlaneEvent::ArtifactChunkReceived {
                    peer_id,
                    request_id,
                    payload,
                });
            };
            match request {
                PendingControlRequest::ArtifactChunk {
                    peer_id: expected,
                    reply,
                    ..
                } => {
                    let result = if peer_id == expected {
                        Ok(payload)
                    } else {
                        Err(format!(
                            "artifact chunk response peer mismatch: expected {expected}, got {peer_id}"
                        ))
                    };
                    let _ = reply.send(result);
                    None
                }
                other => {
                    other.fail("artifact chunk response type did not match pending request".into());
                    Some(LiveControlPlaneEvent::ArtifactChunkReceived {
                        peer_id,
                        request_id,
                        payload,
                    })
                }
            }
        }
        event => Some(event),
    }
}

pub(crate) fn run_control_plane(
    boundary: RuntimeBoundary,
    keypair: Keypair,
    storage: Option<StorageConfig>,
    auth: Option<AuthConfig>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    state: Arc<Mutex<NodeTelemetrySnapshot>>,
    startup_roles: PeerRoleSet,
) {
    const CONNECTIVITY_REPAIR_INTERVAL: Duration = Duration::from_secs(1);
    const PEER_DIRECTORY_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(15);
    const TRUST_BUNDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
    const DIFFUSION_SETTLEMENT_INTERVAL: Duration = Duration::from_millis(100);
    const PENDING_DIAL_DEBOUNCE: Duration = Duration::from_secs(30);
    const COMMAND_BATCH_LIMIT: usize = 128;
    let signing_keypair = keypair.clone();
    let mut auth = auth;
    let mut diffusion_state = crate::promotion::diffusion::DiffusionStateCache::default();
    let mut shell = match ControlPlaneShell::new(
        boundary.protocols.control.clone(),
        keypair,
        boundary
            .listen_addresses
            .iter()
            .chain(boundary.bootstrap_addresses.iter())
            .cloned(),
        boundary.transport_policy.clone(),
        boundary.webrtc_certificate_pem_path.clone(),
    ) {
        Ok(shell) => shell,
        Err(error) => {
            let mut snapshot = lock_telemetry_state(&state);
            snapshot.set_error(error.to_string());
            return;
        }
    };
    {
        let mut snapshot = lock_telemetry_state(&state);
        snapshot.local_peer_id = Some(PeerId::new(shell.local_peer_id().to_string()));
        if !matches!(
            snapshot.node_state,
            NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked
        ) {
            snapshot.set_node_state(NodeRuntimeState::Connecting);
        }
    }

    if let Some(storage) = storage.as_ref()
        && let Err(error) = seed_shell_control_plane_state(storage, &mut shell)
    {
        let mut snapshot = lock_telemetry_state(&state);
        snapshot.last_error = Some(format!("failed to restore control plane state: {error}"));
    }

    for address in &boundary.listen_addresses {
        if let Err(error) = shell.listen_on(address.clone()) {
            let mut snapshot = lock_telemetry_state(&state);
            snapshot.set_error(error.to_string());
            return;
        }
    }

    let mut pending_dial_keys = BTreeMap::<String, Instant>::new();
    for address in &boundary.bootstrap_addresses {
        if let Err(error) = shell.dial(address.clone()) {
            let mut snapshot = lock_telemetry_state(&state);
            snapshot.push_event(LiveControlPlaneEvent::Other {
                kind: format!("bootstrap-dial-error:{error}"),
            });
        } else {
            pending_dial_keys.insert(
                connectivity_address_key(address),
                Instant::now() + PENDING_DIAL_DEBOUNCE,
            );
        }
    }

    if let Err(error) = shell.subscribe_topic(boundary.control_overlay.clone()) {
        let mut snapshot = lock_telemetry_state(&state);
        snapshot.set_error(error.to_string());
        return;
    }
    if let Some(auth_config) = auth.as_ref()
        && let Err(error) =
            subscribe_experiment_directory_topics(&mut shell, &auth_config.experiment_directory)
    {
        let mut snapshot = lock_telemetry_state(&state);
        snapshot.set_error(format!(
            "failed to subscribe experiment directory topics: {error}"
        ));
        return;
    }

    {
        let mut snapshot = lock_telemetry_state(&state);
        snapshot.status = RuntimeStatus::Running;
        if let Some(storage) = storage.as_ref()
            && let Err(error) = restore_runtime_security_state(storage, &mut snapshot)
        {
            snapshot.last_error = Some(format!("failed to restore security state: {error}"));
        }
        prune_tracked_peer_security_state(&mut snapshot);
        if !matches!(
            snapshot.node_state,
            NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked
        ) {
            snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
        }
        sync_connected_peer_snapshot(&mut snapshot, &shell);
        sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
        reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
        let trust_bundle_changed =
            reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
        if trust_bundle_changed {
            for peer_id in connected_peer_ids(&snapshot) {
                let _ = shell.request_snapshot(peer_id.as_str());
            }
        }
        if let Some(storage) = storage.as_ref() {
            let _ = persist_runtime_security_state(storage, &snapshot);
        }
    }

    if let Some(auth_config) = auth.as_ref() {
        if shell.snapshot().auth_announcements.is_empty()
            && let Some(local_peer_auth) = auth_config.local_peer_auth.clone()
        {
            shell.publish_auth(PeerAuthAnnouncement {
                peer_id: local_peer_auth.peer_id.clone(),
                envelope: local_peer_auth,
                announced_at: Utc::now(),
            });
        }
        if shell.snapshot().directory_announcements.is_empty()
            && !auth_config.experiment_directory.is_empty()
        {
            let network_id = auth_config
                .local_peer_auth
                .as_ref()
                .map(|envelope| envelope.certificate.claims().network_id.clone())
                .or_else(|| {
                    auth_config
                        .experiment_directory
                        .first()
                        .map(|entry| entry.network_id.clone())
                })
                .or_else(|| {
                    state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .network_id
                        .clone()
                })
                .unwrap_or_else(|| NetworkId::new("unknown"));
            shell.publish_directory(ExperimentDirectoryAnnouncement {
                network_id,
                entries: auth_config.experiment_directory.clone(),
                announced_at: Utc::now(),
            });
        }

        let mut snapshot = lock_telemetry_state(&state);
        sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
        reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
        let trust_bundle_changed =
            reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
        if trust_bundle_changed {
            for peer_id in connected_peer_ids(&snapshot) {
                let _ = shell.request_snapshot(peer_id.as_str());
            }
        }
        if let Some(storage) = storage.as_ref() {
            let _ = persist_runtime_security_state(storage, &snapshot);
        }
    }

    let mut last_connectivity_repair_at = Instant::now()
        .checked_sub(CONNECTIVITY_REPAIR_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut last_peer_directory_reannounce_at = Instant::now()
        .checked_sub(PEER_DIRECTORY_REANNOUNCE_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut last_trust_bundle_sync_at = Instant::now()
        .checked_sub(TRUST_BUNDLE_REFRESH_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut last_diffusion_settlement_at = Instant::now()
        .checked_sub(DIFFUSION_SETTLEMENT_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut pending_control_requests = BTreeMap::<String, PendingControlRequest>::new();
    let mut pending_diloco_request_ids_by_key = BTreeMap::<String, String>::new();
    let mut pending_diloco_aggregate_ready = Vec::<PendingDiLoCoAggregateReady>::new();
    let mut snapshot_synchronized_peers = BTreeSet::<PeerId>::new();
    loop {
        let mut shutdown_requested = false;
        for _ in 0..COMMAND_BATCH_LIMIT {
            match command_rx.try_recv() {
                Ok(RuntimeCommand::SubscribeTopic(topic)) => {
                    if let Err(error) = shell.subscribe_topic(topic.clone()) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.push_event(LiveControlPlaneEvent::Other {
                            kind: format!("topic-subscribe-error:{}:{error}", topic.as_str()),
                        });
                        snapshot.last_error = Some(error.to_string());
                    }
                }
                Ok(RuntimeCommand::UpdateRoles { roles, reply }) => {
                    let result = apply_runtime_role_update(
                        &mut shell,
                        &boundary,
                        &state,
                        storage.as_ref(),
                        &startup_roles,
                        roles,
                    );
                    let _ = reply.send(result.map_err(|error| error.to_string()));
                }
                Ok(RuntimeCommand::AcknowledgeRuntimeError { expected, reply }) => {
                    let mut snapshot = lock_telemetry_state(&state);
                    let result = if snapshot.last_error.as_deref() == Some(expected.as_str()) {
                        snapshot.last_error = None;
                        snapshot.updated_at = Utc::now();
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "runtime error changed before it could be acknowledged"
                        ))
                    };
                    let _ = reply.send(result.map_err(|error| error.to_string()));
                }
                Ok(RuntimeCommand::PublishControl(announcement)) => {
                    let announcement = *announcement;
                    shell.publish_control(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Control(Box::new(announcement)),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    let revocation_changed = reconcile_live_revocation_policy(
                        &mut auth,
                        &mut snapshot,
                        storage.as_ref(),
                    );
                    if revocation_changed {
                        for peer_id in connected_peer_ids(&snapshot) {
                            let _ = shell.request_snapshot(peer_id.as_str());
                        }
                    }
                }
                Ok(RuntimeCommand::PublishLifecycle(announcement)) => {
                    let local_announcement = (*announcement).clone();
                    shell.publish_lifecycle(local_announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Lifecycle(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishSchedule(announcement)) => {
                    let local_announcement = (*announcement).clone();
                    shell.publish_schedule(local_announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Schedule(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishHead(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_head(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Head(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishLease(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_lease(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Lease(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMerge(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_merge(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Merge(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishMergeWindow(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_merge_window(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::MergeWindow(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReducerAssignment(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reducer_assignment(announcement.clone());
                    if let Err(error) = shell
                        .publish_pubsub(overlay, PubsubPayload::ReducerAssignment(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishUpdate(announcement)) => {
                    let announcement = *announcement;
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_update(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Update(Box::new(announcement)))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishTrainerPromotionAttestation(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_trainer_promotion_attestation(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        overlay,
                        PubsubPayload::TrainerPromotionAttestation(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishDiffusionPromotionCertificate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_diffusion_promotion_certificate(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        overlay,
                        PubsubPayload::DiffusionPromotionCertificate(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAggregateProposal(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_aggregate_proposal(announcement.clone());
                    if let Err(error) = shell
                        .publish_pubsub(overlay, PubsubPayload::AggregateProposal(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReductionCertificate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reduction_certificate(announcement.clone());
                    if let Err(error) = shell
                        .publish_pubsub(overlay, PubsubPayload::ReductionCertificate(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishValidationQuorum(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_validation_quorum(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::ValidationQuorum(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReducerLoad(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reducer_load(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::ReducerLoad(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAuth(announcement)) => {
                    let local_announcement = (*announcement).clone();
                    shell.publish_auth(local_announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Auth(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    if local_announcement.peer_id == PeerId::new(shell.local_peer_id().to_string())
                        && let Some(auth_config) = auth.as_mut()
                    {
                        auth_config.local_peer_auth = Some(local_announcement.envelope.clone());
                    }
                    reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
                    if local_announcement.peer_id == PeerId::new(shell.local_peer_id().to_string())
                        && let Some(storage) = storage.as_ref()
                        && let Err(error) =
                            persist_local_peer_auth(storage, local_announcement.envelope.clone())
                    {
                        snapshot.last_error =
                            Some(format!("failed to persist local peer auth: {error}"));
                    }
                }
                Ok(RuntimeCommand::PublishDirectory(announcement)) => {
                    if let Err(error) =
                        subscribe_experiment_directory_topics(&mut shell, &announcement.entries)
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error =
                            Some(format!("failed to subscribe directory topics: {error}"));
                    }
                    shell.publish_directory(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Directory(announcement),
                    ) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMetrics(announcement)) => {
                    let mut announcement = announcement;
                    let local_peer_id = PeerId::new(shell.local_peer_id().to_string());
                    if announcement.placement_snapshot.is_none() {
                        let mut placement_snapshot = {
                            let snapshot = lock_telemetry_state(&state);
                            build_fleet_placement_snapshot(
                                &snapshot,
                                &snapshot.configured_roles,
                                &local_peer_id,
                                &announcement.peer_window_hints,
                            )
                        };
                        if let Some(placement) = placement_snapshot.as_mut()
                            && let Ok(signature) =
                                sign_fleet_placement_snapshot(&signing_keypair, placement)
                        {
                            placement.signature_bundle.push(signature);
                        }
                        announcement.placement_snapshot = placement_snapshot;
                    }
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_metrics(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Metrics(announcement))
                    {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishDiLoCoState {
                    snapshot: diloco_snapshot,
                    outer_optimizer_state,
                    current_parameters,
                    reply,
                }) => {
                    let mut diloco_snapshot = diloco_snapshot;
                    if let Ok(signature) =
                        sign_diloco_state_snapshot(&signing_keypair, &diloco_snapshot)
                    {
                        diloco_snapshot.signature_bundle.push(signature);
                    }
                    shell.publish_diloco_state(
                        diloco_snapshot,
                        outer_optimizer_state,
                        current_parameters,
                    );
                    let _ = reply.send(Ok(()));
                }
                Ok(RuntimeCommand::PublishDiLoCoGradient {
                    manifest,
                    chunks,
                    reply,
                }) => {
                    let mut manifest = manifest;
                    if let Ok(signature) =
                        sign_diloco_gradient_manifest(&signing_keypair, &manifest)
                    {
                        manifest.signature_bundle.push(signature);
                    }
                    shell.publish_diloco_gradient(manifest, chunks);
                    let _ = reply.send(Ok(()));
                }
                Ok(RuntimeCommand::WaitDiLoCoAggregateReady {
                    experiment_id,
                    revision_id,
                    reducer_peer_id,
                    round_cursor,
                    timeout,
                    reply,
                }) => {
                    if let Some(ready) = shell.diloco_aggregate_ready(
                        &experiment_id,
                        &revision_id,
                        &reducer_peer_id,
                        &round_cursor,
                    ) {
                        let _ = reply.send(Ok(ready));
                    } else {
                        pending_diloco_aggregate_ready.push(PendingDiLoCoAggregateReady {
                            experiment_id,
                            revision_id,
                            reducer_peer_id,
                            round_cursor,
                            deadline: Instant::now() + timeout.max(Duration::from_millis(1)),
                            reply,
                        });
                    }
                }
                Ok(RuntimeCommand::PublishDiLoCoAggregate {
                    manifest,
                    chunks,
                    participant_peer_ids,
                    contribution_manifest_ids,
                    reply,
                }) => {
                    let mut manifest = manifest;
                    if let Ok(signature) =
                        sign_diloco_gradient_manifest(&signing_keypair, &manifest)
                    {
                        manifest.signature_bundle.push(signature);
                    }
                    shell.publish_diloco_aggregate(
                        manifest,
                        chunks,
                        participant_peer_ids,
                        contribution_manifest_ids,
                    );
                    let _ = reply.send(Ok(()));
                }
                Ok(RuntimeCommand::PublishArtifact {
                    descriptor,
                    chunks,
                    reply,
                }) => {
                    shell.publish_artifact(descriptor, chunks);
                    let _ = reply.send(Ok(()));
                }
                Ok(RuntimeCommand::FetchSnapshot {
                    peer_id,
                    timeout,
                    reply,
                }) => match shell.request_snapshot_id(&peer_id) {
                    Ok(request_id) => {
                        pending_control_requests.insert(
                            request_id,
                            PendingControlRequest::Snapshot {
                                peer_id,
                                deadline: Instant::now() + timeout.max(Duration::from_millis(1)),
                                reply,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.to_string()));
                    }
                },
                Ok(RuntimeCommand::FetchArtifactManifest {
                    peer_id,
                    artifact_id,
                    timeout,
                    reply,
                }) => match shell.request_artifact_manifest_id(&peer_id, artifact_id) {
                    Ok(request_id) => {
                        pending_control_requests.insert(
                            request_id,
                            PendingControlRequest::ArtifactManifest {
                                peer_id,
                                deadline: Instant::now() + timeout.max(Duration::from_millis(1)),
                                reply,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.to_string()));
                    }
                },
                Ok(RuntimeCommand::FetchArtifactChunk {
                    peer_id,
                    artifact_id,
                    chunk_id,
                    timeout,
                    reply,
                }) => match shell.request_artifact_chunk_id(&peer_id, artifact_id, chunk_id) {
                    Ok(request_id) => {
                        pending_control_requests.insert(
                            request_id,
                            PendingControlRequest::ArtifactChunk {
                                peer_id,
                                deadline: Instant::now() + timeout.max(Duration::from_millis(1)),
                                reply,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.to_string()));
                    }
                },
                Ok(RuntimeCommand::FetchDiLoCo {
                    peer_id,
                    request,
                    timeout,
                    reply,
                }) => {
                    let coalesce_key = diloco_request_coalesce_key(&peer_id, &request);
                    let reply = match attach_to_pending_diloco_request(
                        &mut pending_control_requests,
                        &pending_diloco_request_ids_by_key,
                        &coalesce_key,
                        reply,
                    ) {
                        Ok(()) => continue,
                        Err(reply) => reply,
                    };
                    pending_diloco_request_ids_by_key.remove(&coalesce_key);
                    match shell.start_diloco_request(&peer_id, request) {
                        Ok(request_id) => {
                            pending_diloco_request_ids_by_key
                                .insert(coalesce_key.clone(), request_id.clone());
                            pending_control_requests.insert(
                                request_id,
                                PendingControlRequest::DiLoCo {
                                    peer_id,
                                    coalesce_key,
                                    deadline: Instant::now()
                                        + timeout.max(Duration::from_millis(1)),
                                    replies: vec![reply],
                                },
                            );
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error.to_string()));
                        }
                    }
                }
                Ok(RuntimeCommand::DialAddress { address }) => {
                    if let Err(error) = shell.dial(address.clone()) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error =
                            Some(format!("failed to dial provider address: {error}"));
                    } else {
                        pending_dial_keys.insert(
                            connectivity_address_key(&address),
                            Instant::now() + PENDING_DIAL_DEBOUNCE,
                        );
                    }
                }
                Ok(RuntimeCommand::RequestSnapshot { peer_id }) => {
                    if let Err(error) = shell.request_snapshot(&peer_id) {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
                            peer_id,
                            request_id: None,
                            kind: Some(RequestFailureKind::new(
                                RequestFailureOperation::SnapshotFetch,
                                RequestFailureReason::Transport,
                            )),
                            message: error.to_string(),
                        });
                    }
                }
                Ok(RuntimeCommand::Shutdown) => {
                    shutdown_requested = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    shutdown_requested = true;
                    break;
                }
            }
        }

        if shutdown_requested {
            for (_, request) in std::mem::take(&mut pending_control_requests) {
                request.fail("control plane shut down during network request".into());
            }
            pending_diloco_request_ids_by_key.clear();
            for waiter in pending_diloco_aggregate_ready.drain(..) {
                let _ = waiter.reply.send(Err(
                    "control plane shut down while waiting for DiLoCo aggregate readiness".into(),
                ));
            }
            let peer_ids = {
                let snapshot = lock_telemetry_state(&state);
                connected_peer_ids(&snapshot)
                    .into_iter()
                    .collect::<Vec<_>>()
            };
            for peer_id in peer_ids {
                let _ = shell.disconnect_peer(peer_id.as_str());
            }
            let drain_deadline = Instant::now() + Duration::from_millis(500);
            while shell.connected_peer_count() > 0 && Instant::now() < drain_deadline {
                if let Some(event) = shell.wait_event(Duration::from_millis(50)) {
                    let mut snapshot = lock_telemetry_state(&state);
                    sync_connected_peer_snapshot(&mut snapshot, &shell);
                    snapshot.push_event(event);
                    snapshot.updated_at = Utc::now();
                }
            }
            let mut snapshot = lock_telemetry_state(&state);
            snapshot.set_node_state(NodeRuntimeState::ShuttingDown);
            snapshot.connected_peers = 0;
            snapshot.connected_peer_ids.clear();
            snapshot.status = RuntimeStatus::Stopped;
            snapshot.updated_at = Utc::now();
            if let Some(storage) = storage.as_ref()
                && let Err(error) = persist_runtime_security_state(storage, &snapshot)
            {
                snapshot.last_error = Some(format!(
                    "failed to persist security state during shutdown: {error}"
                ));
            }
            return;
        }

        settle_pending_diloco_aggregate_ready(&shell, &mut pending_diloco_aggregate_ready);
        let latency_sensitive =
            !pending_control_requests.is_empty() || !pending_diloco_aggregate_ready.is_empty();

        if last_connectivity_repair_at.elapsed() >= CONNECTIVITY_REPAIR_INTERVAL {
            pending_dial_keys.retain(|_, expires_at| *expires_at > Instant::now());
            let pending_dial_key_set = pending_dial_keys.keys().cloned().collect::<BTreeSet<_>>();
            let (dial_targets, offload_targets) = {
                let snapshot = lock_telemetry_state(&state);
                let mut offload_targets = Vec::new();
                if !latency_sensitive {
                    offload_targets = bootstrap_offload_targets(&boundary, &snapshot);
                    offload_targets
                        .extend(excess_connected_peer_offload_targets(&boundary, &snapshot));
                    offload_targets.sort();
                    offload_targets.dedup();
                }
                (
                    connectivity_repair_targets(
                        &boundary,
                        &snapshot,
                        shell.connected_peer_count(),
                        &pending_dial_key_set,
                    ),
                    offload_targets,
                )
            };
            // A collective must be able to recover a dropped cohort connection
            // while requests are pending. Offloading remains an idle-path task
            // so maintenance never intentionally disconnects an active round.
            for address in dial_targets {
                if shell.dial(address.clone()).is_ok() {
                    pending_dial_keys.insert(
                        connectivity_address_key(&address),
                        Instant::now() + PENDING_DIAL_DEBOUNCE,
                    );
                }
            }
            for peer_id in offload_targets {
                let _ = shell.disconnect_peer(peer_id.as_str());
            }
            last_connectivity_repair_at = Instant::now();
        }

        if !latency_sensitive
            && last_peer_directory_reannounce_at.elapsed() >= PEER_DIRECTORY_REANNOUNCE_INTERVAL
        {
            let mut snapshot = lock_telemetry_state(&state);
            publish_local_peer_directory(&mut shell, &boundary, &mut snapshot);
            if let Some(storage) = storage.as_ref()
                && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
            {
                snapshot.last_error =
                    Some(format!("failed to persist control plane state: {error}"));
            }
            last_peer_directory_reannounce_at = Instant::now();
        }

        if !latency_sensitive
            && last_trust_bundle_sync_at.elapsed() >= TRUST_BUNDLE_REFRESH_INTERVAL
        {
            let mut snapshot = lock_telemetry_state(&state);
            let trust_bundle_changed =
                reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
            if trust_bundle_changed {
                for peer_id in connected_peer_ids(&snapshot) {
                    let _ = shell.request_snapshot(peer_id.as_str());
                }
            }
            if let Some(storage) = storage.as_ref()
                && let Err(error) = persist_runtime_security_state(storage, &snapshot)
            {
                snapshot.last_error = Some(format!("failed to persist security state: {error}"));
            }
            last_trust_bundle_sync_at = Instant::now();
        }

        if !latency_sensitive
            && last_diffusion_settlement_at.elapsed() >= DIFFUSION_SETTLEMENT_INTERVAL
        {
            let shell_snapshot = shell.snapshot().clone();
            let (network_id, local_peer_id) = {
                let snapshot = lock_telemetry_state(&state);
                (snapshot.network_id.clone(), snapshot.local_peer_id.clone())
            };
            if let (Some(storage), Some(network_id), Some(local_peer_id)) =
                (storage.as_ref(), network_id, local_peer_id)
            {
                match crate::promotion::diffusion::observe_diffusion_steady_state_from_snapshot(
                    storage,
                    &network_id,
                    &shell_snapshot,
                    &local_peer_id,
                    &mut diffusion_state,
                ) {
                    Ok(publications) => {
                        if !publications.is_empty() {
                            let mut snapshot = lock_telemetry_state(&state);
                            for publication in publications {
                                publish_diffusion_settlement(
                                    &mut shell,
                                    &mut snapshot,
                                    publication,
                                );
                            }
                        }
                    }
                    Err(error) => {
                        let mut snapshot = lock_telemetry_state(&state);
                        snapshot.last_error =
                            Some(format!("diffusion settlement observation failed: {error}"));
                    }
                }
            }
            last_diffusion_settlement_at = Instant::now();
        }

        let mut processed_event = false;
        // Responders do not have an outbound request in `pending_diloco_requests`,
        // so they must retain enough event budget to service request-response
        // traffic even while discovery protocols have queued background events.
        let event_batch_limit = 256;
        for batch_index in 0..event_batch_limit {
            let wait = if batch_index == 0 {
                if latency_sensitive {
                    Duration::from_millis(10)
                } else {
                    Duration::from_millis(50)
                }
            } else {
                Duration::from_millis(1)
            };
            let event = if latency_sensitive {
                shell.wait_priority_event(wait)
            } else {
                shell.wait_event(wait)
            };
            let Some(event) = event else {
                break;
            };
            processed_event = true;
            if event_releases_pending_dial_debounce(&event) {
                // `Swarm::dial` only confirms that a dial was queued. An
                // asynchronous transport failure must make the target eligible
                // for the next connectivity-repair pass instead of suppressing
                // startup recovery for the full successful-dial debounce.
                pending_dial_keys.clear();
            }
            // DiLoCo payloads are deposited in the shell before the observable
            // event is returned. Complete their reply channels before telemetry
            // processing or persistence can add unbounded latency.
            settle_pending_control_requests(
                &mut shell,
                &mut pending_control_requests,
                &mut pending_diloco_request_ids_by_key,
            );
            settle_pending_diloco_aggregate_ready(&shell, &mut pending_diloco_aggregate_ready);
            if let Some(event) = route_pending_control_response(
                &mut pending_control_requests,
                &mut pending_diloco_request_ids_by_key,
                event,
            ) {
                handle_control_plane_event(
                    &mut shell,
                    &boundary,
                    storage.as_ref(),
                    &mut auth,
                    &state,
                    &mut snapshot_synchronized_peers,
                    event,
                );
            }
            settle_pending_control_requests(
                &mut shell,
                &mut pending_control_requests,
                &mut pending_diloco_request_ids_by_key,
            );
        }
        settle_pending_control_requests(
            &mut shell,
            &mut pending_control_requests,
            &mut pending_diloco_request_ids_by_key,
        );
        settle_pending_diloco_aggregate_ready(&shell, &mut pending_diloco_aggregate_ready);
        if !processed_event {
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn event_releases_pending_dial_debounce(event: &LiveControlPlaneEvent) -> bool {
    matches!(event, LiveControlPlaneEvent::OutgoingConnectionError { .. })
}

fn sync_connected_peer_snapshot(snapshot: &mut NodeTelemetrySnapshot, shell: &ControlPlaneShell) {
    snapshot.connected_peer_ids = shell.connected_peer_ids().into_iter().collect();
    snapshot.connected_peers = snapshot.connected_peer_ids.len();
}

fn publish_diffusion_settlement(
    shell: &mut ControlPlaneShell,
    snapshot: &mut NodeTelemetrySnapshot,
    publication: crate::promotion::diffusion::DiffusionSettlementPublication,
) {
    let crate::promotion::diffusion::DiffusionSettlementPublication {
        overlay,
        certificate,
        merge_certificate,
    } = publication;
    let certificate_announcement = DiffusionPromotionCertificateAnnouncement {
        overlay: overlay.clone(),
        certificate,
        announced_at: Utc::now(),
    };
    shell.publish_diffusion_promotion_certificate(certificate_announcement.clone());
    if let Err(error) = shell.publish_pubsub(
        overlay.clone(),
        PubsubPayload::DiffusionPromotionCertificate(certificate_announcement),
    ) {
        snapshot.last_error = Some(error.to_string());
    }
    let merge_announcement = MergeAnnouncement {
        overlay,
        certificate: merge_certificate,
        announced_at: Utc::now(),
    };
    shell.publish_merge(merge_announcement.clone());
    if let Err(error) = shell.publish_pubsub(
        merge_announcement.overlay.clone(),
        PubsubPayload::Merge(merge_announcement),
    ) {
        snapshot.last_error = Some(error.to_string());
    }
    snapshot.control_plane = shell.snapshot().clone();
    snapshot.updated_at = Utc::now();
}

fn peer_directory_announcement_adds_information(
    snapshot: &ControlPlaneSnapshot,
    announcement: &PeerDirectoryAnnouncement,
) -> bool {
    let Some(current) = snapshot
        .peer_directory_announcements
        .iter()
        .find(|current| {
            current.network_id == announcement.network_id && current.peer_id == announcement.peer_id
        })
    else {
        return true;
    };

    current.advertised_roles != announcement.advertised_roles
        || announcement
            .addresses
            .iter()
            .any(|address| !current.addresses.contains(address))
}

fn snapshot_sync_peer<'a>(
    event: &'a LiveControlPlaneEvent,
    local_peer_id: &str,
) -> Option<&'a str> {
    match event {
        LiveControlPlaneEvent::PeerIdentified {
            peer_id,
            listen_addresses,
            ..
        } if peer_id != local_peer_id && !listen_addresses.is_empty() => Some(peer_id),
        _ => None,
    }
}

fn handle_control_plane_event(
    shell: &mut ControlPlaneShell,
    boundary: &RuntimeBoundary,
    storage: Option<&StorageConfig>,
    auth: &mut Option<AuthConfig>,
    state: &Arc<Mutex<NodeTelemetrySnapshot>>,
    snapshot_synchronized_peers: &mut BTreeSet<PeerId>,
    event: LiveControlPlaneEvent,
) {
    let persist_security_state = event_requires_security_state_persistence(&event);
    let mut connection_request_error = None;
    // Identify classifies fetch sidecars and transport probes before they can
    // be admitted as durable control-plane peers. Requesting a full snapshot
    // at ConnectionEstablished races that classification.
    if let Some(peer_id) = snapshot_sync_peer(&event, &shell.local_peer_id().to_string()) {
        let peer_id = PeerId::new(peer_id.to_owned());
        if snapshot_synchronized_peers.insert(peer_id.clone())
            && let Err(error) = shell.request_snapshot(peer_id.as_str())
        {
            snapshot_synchronized_peers.remove(&peer_id);
            connection_request_error = Some((peer_id.as_str().to_owned(), error.to_string()));
        }
    } else if let LiveControlPlaneEvent::ConnectionClosed { peer_id } = &event {
        let peer_id = PeerId::new(peer_id.clone());
        if !shell.connected_peer_ids().contains(&peer_id) {
            snapshot_synchronized_peers.remove(&peer_id);
        }
    }

    let mut snapshot = lock_telemetry_state(state);
    sync_connected_peer_snapshot(&mut snapshot, shell);
    // The swarm applies every decoded pubsub payload before emitting the event.
    // Refresh the public projection for every payload kind, not only the
    // discovery/control subset, or heads and training updates remain hidden
    // until an unrelated snapshot happens to refresh telemetry.
    let control_plane_changed = matches!(event, LiveControlPlaneEvent::PubsubMessage { .. })
        || matches!(
            event,
            LiveControlPlaneEvent::PeerDirectoryRecordReceived { .. }
        );
    if control_plane_changed {
        snapshot.control_plane = shell.snapshot().clone();
    }
    if let Some((peer_id, message)) = connection_request_error {
        snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
            peer_id,
            request_id: None,
            kind: None,
            message: message.clone(),
        });
        snapshot.last_error = Some(message);
    }
    match &event {
        LiveControlPlaneEvent::NewListenAddr { address } => {
            if !snapshot.listen_addresses.contains(address) {
                snapshot.listen_addresses.push(address.clone());
            }
            publish_configured_external_addresses(shell, boundary, &mut snapshot, address);
            publish_local_peer_directory(shell, boundary, &mut snapshot);
            snapshot.control_plane = shell.snapshot().clone();
        }
        LiveControlPlaneEvent::ReachableAddressConfirmed { address } => {
            if !snapshot.listen_addresses.contains(address) {
                snapshot.listen_addresses.push(address.clone());
            }
            publish_local_peer_directory(shell, boundary, &mut snapshot);
            snapshot.control_plane = shell.snapshot().clone();
        }
        LiveControlPlaneEvent::ReachableAddressExpired { address } => {
            if let Some(position) = snapshot
                .listen_addresses
                .iter()
                .position(|entry| entry == address)
            {
                snapshot.listen_addresses.remove(position);
            }
            publish_local_peer_directory(shell, boundary, &mut snapshot);
            snapshot.control_plane = shell.snapshot().clone();
        }
        LiveControlPlaneEvent::PeersDiscovered { peers } => {
            remember_known_peer_addresses(
                &mut snapshot,
                storage,
                peers.iter().map(|(_, address)| address.clone()),
            );
        }
        LiveControlPlaneEvent::PeerDirectoryRecordReceived { announcement } => {
            remember_peer_directory_addresses(
                &mut snapshot,
                storage,
                std::slice::from_ref(announcement),
            );
        }
        LiveControlPlaneEvent::PeerIdentified {
            listen_addresses, ..
        } => {
            remember_known_peer_addresses(&mut snapshot, storage, listen_addresses.iter().cloned());
        }
        LiveControlPlaneEvent::SnapshotReceived {
            peer_id,
            snapshot: remote_snapshot,
            ..
        } => {
            let new_peer_directory_announcements = remote_snapshot
                .peer_directory_announcements
                .iter()
                .filter(|announcement| {
                    peer_directory_announcement_adds_information(
                        &snapshot.control_plane,
                        announcement,
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            shell.merge_snapshot(remote_snapshot);
            let remote_directory_entries = remote_snapshot
                .directory_announcements
                .iter()
                .flat_map(|announcement| announcement.entries.iter().cloned())
                .collect::<Vec<_>>();
            if let Err(error) =
                subscribe_experiment_directory_topics(shell, &remote_directory_entries)
            {
                snapshot.last_error = Some(format!(
                    "failed to subscribe remote directory topics: {error}"
                ));
            }
            snapshot.control_plane = shell.snapshot().clone();
            for announcement in &new_peer_directory_announcements {
                shell.publish_peer_directory(announcement.clone());
                if let Err(error) = shell.publish_pubsub(
                    boundary.control_overlay.clone(),
                    PubsubPayload::PeerDirectory(announcement.clone()),
                ) {
                    snapshot.last_error = Some(error.to_string());
                }
            }
            remember_peer_directory_addresses(
                &mut snapshot,
                storage,
                &remote_snapshot.peer_directory_announcements,
            );
            snapshot.last_snapshot_peer_id = Some(PeerId::new(peer_id.clone()));
            snapshot.last_snapshot = Some(remote_snapshot.clone());
            if let Some(policy) = auth
                .as_ref()
                .and_then(|auth| auth.admission_policy.as_ref())
            {
                match verify_snapshot_admission(
                    policy,
                    &PeerId::new(peer_id.clone()),
                    remote_snapshot,
                ) {
                    Ok(report) if matches!(report.decision(), AdmissionDecision::Allow) => {
                        note_admitted_peer(&mut snapshot, report);
                    }
                    Ok(report) => {
                        note_rejected_peer(
                            &mut snapshot,
                            PeerId::new(peer_id.clone()),
                            admission_rejection_reason(&report),
                            1,
                            0,
                        );
                        snapshot.last_error = Some(format!(
                            "peer {} failed admission with {} findings",
                            peer_id,
                            report.findings.len()
                        ));
                    }
                    Err(error) => {
                        note_rejected_peer(
                            &mut snapshot,
                            PeerId::new(peer_id.clone()),
                            error.to_string(),
                            0,
                            1,
                        );
                        snapshot.last_error =
                            Some(format!("peer {} admission error: {error}", peer_id));
                    }
                }
            }
            reconcile_live_revocation_policy(auth, &mut snapshot, storage);
        }
        LiveControlPlaneEvent::RequestFailure { message, .. }
        | LiveControlPlaneEvent::InboundFailure { message, .. }
        | LiveControlPlaneEvent::ResponseSendFailure { message, .. }
        | LiveControlPlaneEvent::OutgoingConnectionError { message, .. }
        | LiveControlPlaneEvent::IncomingConnectionError { message } => {
            snapshot.last_error = Some(message.clone());
        }
        LiveControlPlaneEvent::PubsubMessage { .. }
        | LiveControlPlaneEvent::TopicSubscribed { .. }
        | LiveControlPlaneEvent::PeersExpired { .. }
        | LiveControlPlaneEvent::ConnectionClosed { .. }
        | LiveControlPlaneEvent::RelayReservationAccepted { .. }
        | LiveControlPlaneEvent::DirectConnectionUpgradeSucceeded { .. } => {}
        LiveControlPlaneEvent::Other { .. }
        | LiveControlPlaneEvent::ConnectionEstablished { .. }
        | LiveControlPlaneEvent::ArtifactManifestRequested { .. }
        | LiveControlPlaneEvent::ArtifactManifestReceived { .. }
        | LiveControlPlaneEvent::ArtifactChunkRequested { .. }
        | LiveControlPlaneEvent::ArtifactChunkReceived { .. }
        | LiveControlPlaneEvent::SnapshotRequested { .. }
        | LiveControlPlaneEvent::SnapshotResponseSent { .. }
        | LiveControlPlaneEvent::DirectConnectionUpgradeFailed { .. } => {}
    }
    if matches!(
        &event,
        LiveControlPlaneEvent::PubsubMessage { kind, .. }
            if kind == "control"
                || kind == "auth"
                || kind == "directory"
                || kind == "peer-directory"
    ) || matches!(
        event,
        LiveControlPlaneEvent::PeerDirectoryRecordReceived { .. }
    ) {
        if matches!(
            &event,
            LiveControlPlaneEvent::PubsubMessage { kind, .. } if kind == "directory"
        ) {
            let entries = snapshot
                .control_plane
                .directory_announcements
                .iter()
                .flat_map(|announcement| announcement.entries.iter().cloned())
                .collect::<Vec<_>>();
            if let Err(error) = subscribe_experiment_directory_topics(shell, &entries) {
                snapshot.last_error = Some(format!(
                    "failed to subscribe live directory topics: {error}"
                ));
            }
        }
        let peer_directory_announcements =
            snapshot.control_plane.peer_directory_announcements.clone();
        remember_peer_directory_addresses(&mut snapshot, storage, &peer_directory_announcements);
        let revocation_changed = reconcile_live_revocation_policy(auth, &mut snapshot, storage);
        if revocation_changed {
            for peer_id in connected_peer_ids(&snapshot) {
                let _ = shell.request_snapshot(peer_id.as_str());
            }
        }
    }
    snapshot.push_event(event);
    if persist_security_state
        && let Some(storage) = storage
        && let Err(error) = persist_runtime_security_state(storage, &snapshot)
    {
        snapshot.last_error = Some(format!("failed to persist security state: {error}"));
    }
}

fn event_requires_security_state_persistence(event: &LiveControlPlaneEvent) -> bool {
    matches!(event, LiveControlPlaneEvent::SnapshotReceived { .. })
        || matches!(
            event,
            LiveControlPlaneEvent::PubsubMessage { kind, .. }
                if matches!(
                    kind.as_str(),
                    "control" | "auth" | "directory" | "peer-directory"
                )
        )
        || matches!(
            event,
            LiveControlPlaneEvent::PeerDirectoryRecordReceived { .. }
        )
}

fn subscribe_experiment_directory_topics(
    shell: &mut ControlPlaneShell,
    entries: &[ExperimentDirectoryEntry],
) -> anyhow::Result<()> {
    let mut topics = BTreeSet::new();
    for entry in entries {
        let experiment = ExperimentHandle {
            network_id: entry.network_id.clone(),
            study_id: entry.study_id.clone(),
            experiment_id: entry.experiment_id.clone(),
            revision_id: entry.current_revision_id.clone(),
        };
        let overlays = experiment.overlay_set()?;
        topics.insert(overlays.control.clone());
        topics.extend(overlays.experiment_topics());
    }
    for topic in topics {
        shell.subscribe_topic(topic)?;
    }
    Ok(())
}

#[cfg(test)]
mod pending_request_tests {
    use super::*;

    #[test]
    fn snapshot_response_completes_typed_pending_request_without_retaining_payload_event() {
        let (reply, response) = mpsc::channel();
        let mut pending = BTreeMap::from([(
            "request-1".into(),
            PendingControlRequest::Snapshot {
                peer_id: "peer-a".into(),
                deadline: Instant::now() + Duration::from_secs(1),
                reply,
            },
        )]);
        let snapshot = ControlPlaneSnapshot::default();
        let mut pending_diloco_request_ids_by_key = BTreeMap::new();

        let routed = route_pending_control_response(
            &mut pending,
            &mut pending_diloco_request_ids_by_key,
            LiveControlPlaneEvent::SnapshotReceived {
                peer_id: "peer-a".into(),
                request_id: "request-1".into(),
                snapshot: snapshot.clone(),
            },
        );

        assert!(routed.is_none());
        assert!(pending.is_empty());
        assert_eq!(
            response
                .recv_timeout(Duration::from_secs(1))
                .expect("snapshot reply")
                .expect("successful snapshot reply"),
            snapshot
        );
    }

    #[test]
    fn request_failure_completes_any_typed_pending_request_and_remains_observable() {
        let (reply, response) = mpsc::channel();
        let mut pending = BTreeMap::from([(
            "request-2".into(),
            PendingControlRequest::ArtifactManifest {
                peer_id: "peer-b".into(),
                deadline: Instant::now() + Duration::from_secs(1),
                reply,
            },
        )]);
        let mut pending_diloco_request_ids_by_key = BTreeMap::new();

        let routed = route_pending_control_response(
            &mut pending,
            &mut pending_diloco_request_ids_by_key,
            LiveControlPlaneEvent::RequestFailure {
                peer_id: "peer-b".into(),
                request_id: Some("request-2".into()),
                kind: None,
                message: "transport closed".into(),
            },
        );

        assert!(matches!(
            routed,
            Some(LiveControlPlaneEvent::RequestFailure { .. })
        ));
        assert!(pending.is_empty());
        assert_eq!(
            response
                .recv_timeout(Duration::from_secs(1))
                .expect("failure reply")
                .expect_err("request should fail"),
            "transport closed"
        );
    }

    #[test]
    fn duplicate_diloco_fetches_share_one_transport_request() {
        let request = DiLoCoRequest::StateSnapshot {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
        };
        let coalesce_key = diloco_request_coalesce_key("peer-c", &request);
        let (first_reply, first_response) = mpsc::channel();
        let (second_reply, second_response) = mpsc::channel();
        let mut pending = BTreeMap::from([(
            "request-3".into(),
            PendingControlRequest::DiLoCo {
                peer_id: "peer-c".into(),
                coalesce_key: coalesce_key.clone(),
                deadline: Instant::now() + Duration::from_secs(30),
                replies: vec![first_reply],
            },
        )]);
        let mut pending_diloco_request_ids_by_key =
            BTreeMap::from([(coalesce_key.clone(), "request-3".into())]);

        attach_to_pending_diloco_request(
            &mut pending,
            &pending_diloco_request_ids_by_key,
            &coalesce_key,
            second_reply,
        )
        .expect("duplicate should attach");

        assert!(matches!(
            pending.get("request-3"),
            Some(PendingControlRequest::DiLoCo { replies, .. }) if replies.len() == 2
        ));
        remove_pending_control_request(
            &mut pending,
            &mut pending_diloco_request_ids_by_key,
            "request-3",
        )
        .expect("pending request")
        .fail("shared failure".into());
        assert!(pending_diloco_request_ids_by_key.is_empty());
        for response in [first_response, second_response] {
            assert_eq!(
                response
                    .recv_timeout(Duration::from_secs(1))
                    .expect("coalesced reply")
                    .expect_err("shared request should fail"),
                "shared failure"
            );
        }
    }

    #[test]
    fn diloco_round_retry_keys_ignore_observation_timestamps() {
        let mut cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        cursor.group_id = Some(GroupId::new("group"));
        cursor.phase = RoundPhase::BuildPseudoGradient;
        let heartbeat = DiLoCoRoundHeartbeat {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            peer_id: PeerId::new("peer-a"),
            round_cursor: cursor.clone(),
            observed_participants: 3,
            emitted_at: Utc::now(),
        };
        let mut later = heartbeat.clone();
        later.emitted_at += chrono::Duration::seconds(1);

        assert_eq!(
            diloco_request_coalesce_key(
                "peer-b",
                &DiLoCoRequest::RoundHeartbeat(Box::new(heartbeat))
            ),
            diloco_request_coalesce_key("peer-b", &DiLoCoRequest::RoundHeartbeat(Box::new(later)))
        );

        cursor.group_id = Some(GroupId::new("other-group"));
        let other_group = DiLoCoRoundHeartbeat {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            peer_id: PeerId::new("peer-a"),
            round_cursor: cursor,
            observed_participants: 3,
            emitted_at: Utc::now(),
        };
        assert_ne!(
            diloco_request_coalesce_key(
                "peer-b",
                &DiLoCoRequest::RoundHeartbeat(Box::new(other_group))
            ),
            diloco_request_coalesce_key(
                "peer-b",
                &DiLoCoRequest::RoundHeartbeat(Box::new(DiLoCoRoundHeartbeat {
                    experiment_id: ExperimentId::new("experiment"),
                    revision_id: RevisionId::new("revision"),
                    peer_id: PeerId::new("peer-a"),
                    round_cursor: RoundCursor {
                        round_id: RoundId::new(0),
                        group_id: Some(GroupId::new("group")),
                        base_checkpoint_id: BaseCheckpointId::new("base"),
                        phase: RoundPhase::BuildPseudoGradient,
                        num_inner_steps: 4,
                    },
                    observed_participants: 3,
                    emitted_at: Utc::now(),
                }))
            )
        );
    }

    #[test]
    fn peer_directory_snapshot_diff_ignores_timestamp_only_updates() {
        let current = PeerDirectoryAnnouncement {
            network_id: NetworkId::new("network"),
            peer_id: PeerId::new("peer-a"),
            addresses: vec![SwarmAddress::new("/memory/1").expect("address")],
            advertised_roles: None,
            announced_at: Utc::now(),
        };
        let snapshot = ControlPlaneSnapshot {
            peer_directory_announcements: vec![current.clone()],
            ..ControlPlaneSnapshot::default()
        };
        let mut timestamp_only = current.clone();
        timestamp_only.announced_at += chrono::Duration::seconds(1);
        assert!(!peer_directory_announcement_adds_information(
            &snapshot,
            &timestamp_only
        ));

        let mut new_address = timestamp_only;
        new_address
            .addresses
            .push(SwarmAddress::new("/memory/2").expect("address"));
        assert!(peer_directory_announcement_adds_information(
            &snapshot,
            &new_address
        ));
    }

    #[test]
    fn snapshot_sync_waits_for_durable_peer_identification() {
        assert!(
            snapshot_sync_peer(
                &LiveControlPlaneEvent::ConnectionEstablished {
                    peer_id: "ephemeral".into(),
                },
                "local",
            )
            .is_none()
        );
        assert!(
            snapshot_sync_peer(
                &LiveControlPlaneEvent::PeerIdentified {
                    peer_id: "ephemeral".into(),
                    listen_addresses: Vec::new(),
                    protocols: Vec::new(),
                },
                "local",
            )
            .is_none()
        );
        assert_eq!(
            snapshot_sync_peer(
                &LiveControlPlaneEvent::PeerIdentified {
                    peer_id: "durable".into(),
                    listen_addresses: vec![
                        SwarmAddress::new("/memory/1").expect("durable address")
                    ],
                    protocols: Vec::new(),
                },
                "local",
            ),
            Some("durable")
        );
        assert!(
            snapshot_sync_peer(
                &LiveControlPlaneEvent::PeerIdentified {
                    peer_id: "local".into(),
                    listen_addresses: vec![
                        SwarmAddress::new("/memory/1").expect("durable address")
                    ],
                    protocols: Vec::new(),
                },
                "local",
            )
            .is_none()
        );
    }

    #[test]
    fn security_persistence_is_limited_to_security_relevant_events() {
        assert!(event_requires_security_state_persistence(
            &LiveControlPlaneEvent::SnapshotReceived {
                peer_id: "peer-a".into(),
                request_id: "request-a".into(),
                snapshot: ControlPlaneSnapshot::default(),
            }
        ));
        for kind in ["control", "auth", "directory", "peer-directory"] {
            assert!(event_requires_security_state_persistence(
                &LiveControlPlaneEvent::PubsubMessage {
                    peer_id: "peer-a".into(),
                    topic: "control".into(),
                    kind: kind.into(),
                }
            ));
        }
        assert!(!event_requires_security_state_persistence(
            &LiveControlPlaneEvent::PubsubMessage {
                peer_id: "peer-a".into(),
                topic: "updates".into(),
                kind: "update".into(),
            }
        ));
        assert!(!event_requires_security_state_persistence(
            &LiveControlPlaneEvent::ConnectionEstablished {
                peer_id: "peer-a".into(),
            }
        ));
    }

    #[test]
    fn asynchronous_dial_failure_releases_connectivity_repair_debounce() {
        assert!(event_releases_pending_dial_debounce(
            &LiveControlPlaneEvent::OutgoingConnectionError {
                peer_id: None,
                message: "connection refused".into(),
            }
        ));
        assert!(!event_releases_pending_dial_debounce(
            &LiveControlPlaneEvent::ConnectionClosed {
                peer_id: "peer-a".into(),
            }
        ));
    }
}

fn connectivity_repair_targets(
    boundary: &RuntimeBoundary,
    snapshot: &NodeTelemetrySnapshot,
    connected_peers: usize,
    pending_dial_keys: &BTreeSet<String>,
) -> Vec<SwarmAddress> {
    const STALE_PEER_DIRECTORY_AFTER: chrono::Duration = chrono::Duration::minutes(5);

    let target = boundary.transport_policy.target_connected_peers.max(1);
    if connected_peers >= target {
        return Vec::new();
    }

    let now = Utc::now();
    let local_peer_id = snapshot.local_peer_id.as_ref();
    let bootstrap_addresses = boundary
        .bootstrap_addresses
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let connected_peer_ids = if connected_peers == 0 {
        BTreeSet::new()
    } else {
        connected_peer_ids(snapshot)
    };
    let connected_peer_addresses = if connected_peers == 0 {
        BTreeSet::new()
    } else {
        snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .filter(|announcement| announcement.announced_at + STALE_PEER_DIRECTORY_AFTER > now)
            .filter(|announcement| connected_peer_ids.contains(&announcement.peer_id))
            .flat_map(|announcement| announcement.addresses.iter().cloned())
            .collect::<BTreeSet<_>>()
    };
    let connected_peer_address_keys = connected_peer_addresses
        .iter()
        .map(connectivity_address_key)
        .collect::<BTreeSet<_>>();
    let listen_address_keys = snapshot
        .listen_addresses
        .iter()
        .map(connectivity_address_key)
        .collect::<BTreeSet<_>>();
    let mut peer_directory_targets = snapshot
        .control_plane
        .peer_directory_announcements
        .iter()
        .filter(|announcement| announcement.announced_at + STALE_PEER_DIRECTORY_AFTER > now)
        .filter(|announcement| local_peer_id.is_none_or(|local| local != &announcement.peer_id))
        .filter(|announcement| !connected_peer_ids.contains(&announcement.peer_id))
        .filter_map(|announcement| {
            announcement
                .addresses
                .iter()
                .filter(|address| !bootstrap_addresses.contains(*address))
                .filter(|address| !listen_address_keys.contains(&connectivity_address_key(address)))
                .filter(|address| !pending_dial_keys.contains(&connectivity_address_key(address)))
                .min_by(|left, right| {
                    left.is_relay_circuit()
                        .cmp(&right.is_relay_circuit())
                        .then_with(|| left.cmp(right))
                })
                .cloned()
        })
        .collect::<Vec<_>>();
    let mut known_peer_targets = snapshot
        .known_peer_addresses
        .iter()
        .filter(|address| {
            !local_peer_id.is_some_and(|peer_id| address_targets_peer(address, peer_id))
        })
        .filter(|address| !bootstrap_addresses.contains(*address))
        .filter(|address| !connected_peer_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !listen_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !pending_dial_keys.contains(&connectivity_address_key(address)))
        .cloned()
        .collect::<Vec<_>>();
    let mut bootstrap_targets = boundary
        .bootstrap_addresses
        .iter()
        .filter(|address| {
            !local_peer_id.is_some_and(|peer_id| address_targets_peer(address, peer_id))
        })
        .filter(|address| !connected_peer_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !listen_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !pending_dial_keys.contains(&connectivity_address_key(address)))
        .cloned()
        .collect::<Vec<_>>();
    peer_directory_targets.sort_by(|left, right| {
        left.is_relay_circuit()
            .cmp(&right.is_relay_circuit())
            .then_with(|| left.cmp(right))
    });
    known_peer_targets.sort_by(|left, right| {
        left.is_relay_circuit()
            .cmp(&right.is_relay_circuit())
            .then_with(|| left.cmp(right))
    });
    bootstrap_targets.sort_by(|left, right| {
        left.is_relay_circuit()
            .cmp(&right.is_relay_circuit())
            .then_with(|| left.cmp(right))
    });

    let mut targets = Vec::new();
    let mut target_keys = BTreeSet::new();
    if connected_peers == 0 {
        for address in &bootstrap_targets {
            if target_keys.insert(connectivity_address_key(address)) {
                targets.push(address.clone());
            }
        }
    }
    for address in peer_directory_targets.into_iter().chain(known_peer_targets) {
        if target_keys.insert(connectivity_address_key(&address)) {
            targets.push(address);
        }
    }
    if targets.is_empty() {
        targets = bootstrap_targets;
    }

    targets
        .into_iter()
        .take(target.saturating_sub(connected_peers))
        .collect()
}

fn connectivity_address_key(address: &SwarmAddress) -> String {
    address
        .as_str()
        .rsplit_once("/p2p/")
        .filter(|(_, suffix)| !suffix.contains('/'))
        .map(|(prefix, _)| prefix.to_owned())
        .unwrap_or_else(|| address.as_str().to_owned())
}

fn address_targets_peer(address: &SwarmAddress, peer_id: &PeerId) -> bool {
    address
        .as_str()
        .strip_suffix(peer_id.as_str())
        .is_some_and(|prefix| prefix.ends_with("/p2p/"))
}

fn publish_configured_external_addresses(
    shell: &mut ControlPlaneShell,
    boundary: &RuntimeBoundary,
    snapshot: &mut NodeTelemetrySnapshot,
    listen_address: &SwarmAddress,
) {
    for external_address in configured_external_addresses_for(boundary, listen_address) {
        if let Err(error) = shell.add_external_address(external_address.clone()) {
            snapshot.last_error = Some(format!(
                "failed to register configured external address {}: {error}",
                external_address.as_str()
            ));
            continue;
        }
        if !snapshot.listen_addresses.contains(&external_address) {
            snapshot.listen_addresses.push(external_address);
        }
    }
}

fn configured_external_addresses_for(
    boundary: &RuntimeBoundary,
    listen_address: &SwarmAddress,
) -> Vec<SwarmAddress> {
    let mut addresses = Vec::new();
    for external_address in &boundary.external_addresses {
        if let Some(address) = rewrite_configured_external_address(external_address, listen_address)
            && !addresses.contains(&address)
        {
            addresses.push(address);
        }
    }
    addresses
}

fn rewrite_configured_external_address(
    external_address: &SwarmAddress,
    listen_address: &SwarmAddress,
) -> Option<SwarmAddress> {
    let external_segments = external_address
        .as_str()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let listen_segments = listen_address
        .as_str()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if external_segments.len() < 3 || listen_segments.len() < 3 {
        return None;
    }
    let external_suffix = &external_segments[2..];
    let listen_suffix = &listen_segments[2..];
    if external_suffix.len() > listen_suffix.len()
        || listen_suffix[..external_suffix.len()] != *external_suffix
    {
        return None;
    }
    SwarmAddress::new(format!(
        "/{}/{}",
        external_segments[..2].join("/"),
        listen_suffix.join("/")
    ))
    .ok()
}

fn bootstrap_offload_targets(
    boundary: &RuntimeBoundary,
    snapshot: &NodeTelemetrySnapshot,
) -> Vec<PeerId> {
    const STALE_PEER_DIRECTORY_AFTER: chrono::Duration = chrono::Duration::minutes(5);

    let connected = connected_peer_ids(snapshot);
    if connected.is_empty() {
        return Vec::new();
    }

    let now = Utc::now();
    let target_connected_peers = boundary.transport_policy.target_connected_peers.max(1);
    let target_bootstrap_seed_connections =
        boundary.transport_policy.target_bootstrap_seed_connections;
    let bootstrap_addresses = boundary
        .bootstrap_addresses
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut bootstrap_peers = snapshot
        .control_plane
        .peer_directory_announcements
        .iter()
        .filter(|announcement| announcement.announced_at + STALE_PEER_DIRECTORY_AFTER > now)
        .filter(|announcement| connected.contains(&announcement.peer_id))
        .filter(|announcement| {
            if let Some(roles) = announcement.advertised_roles.as_ref() {
                roles.contains(&PeerRole::Bootstrap) || roles.contains(&PeerRole::RelayHelper)
            } else {
                announcement
                    .addresses
                    .iter()
                    .any(|address| bootstrap_addresses.contains(address))
            }
        })
        .map(|announcement| announcement.peer_id.clone())
        .collect::<Vec<_>>();
    bootstrap_peers.sort();
    bootstrap_peers.dedup();

    if bootstrap_peers.len() <= target_bootstrap_seed_connections {
        return Vec::new();
    }

    let non_bootstrap_connected = connected.len().saturating_sub(bootstrap_peers.len());
    if non_bootstrap_connected < target_connected_peers {
        return Vec::new();
    }

    bootstrap_peers
        .into_iter()
        .skip(target_bootstrap_seed_connections)
        .collect()
}

fn excess_connected_peer_offload_targets(
    boundary: &RuntimeBoundary,
    snapshot: &NodeTelemetrySnapshot,
) -> Vec<PeerId> {
    let Some(max_connected_peers) = boundary
        .transport_policy
        .max_established_total
        .map(|max| max as usize)
    else {
        return Vec::new();
    };
    let connected = connected_peer_ids(snapshot);
    let excess = connected.len().saturating_sub(max_connected_peers);
    if excess == 0 {
        return Vec::new();
    }

    let protected = protected_connected_peer_ids(boundary, snapshot);
    connected
        .into_iter()
        .filter(|peer_id| !protected.contains(peer_id))
        .take(excess)
        .collect()
}

fn protected_connected_peer_ids(
    boundary: &RuntimeBoundary,
    snapshot: &NodeTelemetrySnapshot,
) -> BTreeSet<PeerId> {
    const STALE_PEER_DIRECTORY_AFTER: chrono::Duration = chrono::Duration::minutes(5);

    let now = Utc::now();
    let connected = connected_peer_ids(snapshot);
    let bootstrap_addresses = boundary
        .bootstrap_addresses
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    snapshot
        .control_plane
        .peer_directory_announcements
        .iter()
        .filter(|announcement| announcement.announced_at + STALE_PEER_DIRECTORY_AFTER > now)
        .filter(|announcement| connected.contains(&announcement.peer_id))
        .filter(|announcement| {
            announcement.advertised_roles.as_ref().is_some_and(|roles| {
                roles.contains(&PeerRole::Bootstrap)
                    || roles.contains(&PeerRole::RelayHelper)
                    || roles.contains(&PeerRole::Authority)
            }) || announcement
                .addresses
                .iter()
                .any(|address| bootstrap_addresses.contains(address))
        })
        .map(|announcement| announcement.peer_id.clone())
        .collect()
}

fn apply_runtime_role_update(
    shell: &mut ControlPlaneShell,
    boundary: &RuntimeBoundary,
    state: &Arc<Mutex<NodeTelemetrySnapshot>>,
    storage: Option<&StorageConfig>,
    startup_roles: &PeerRoleSet,
    roles: PeerRoleSet,
) -> anyhow::Result<()> {
    anyhow::ensure!(!roles.roles.is_empty(), "runtime roles cannot be empty");
    anyhow::ensure!(
        roles.roles.iter().all(|role| {
            startup_roles.roles.contains(role)
                || matches!(
                    role,
                    PeerRole::Viewer | PeerRole::BrowserObserver | PeerRole::BrowserFallback
                )
        }),
        "runtime role update requested a role outside the startup capability set"
    );

    let mut snapshot = lock_telemetry_state(state);
    if snapshot.configured_roles == roles {
        return Ok(());
    }
    snapshot.configured_roles = roles;
    if !matches!(
        snapshot.node_state,
        NodeRuntimeState::TrainingWindow
            | NodeRuntimeState::PublishingUpdate
            | NodeRuntimeState::Quarantined
            | NodeRuntimeState::Revoked
            | NodeRuntimeState::ShuttingDown
    ) {
        snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
    }
    publish_local_peer_directory(shell, boundary, &mut snapshot);
    if let Some(storage) = storage {
        persist_control_plane_state(storage, &snapshot.control_plane)?;
    }
    Ok(())
}

fn publish_local_peer_directory(
    shell: &mut ControlPlaneShell,
    boundary: &RuntimeBoundary,
    snapshot: &mut NodeTelemetrySnapshot,
) {
    let Some(local_peer_id) = snapshot.local_peer_id.clone() else {
        return;
    };
    if snapshot.listen_addresses.is_empty() {
        return;
    }

    let announcement = PeerDirectoryAnnouncement {
        network_id: boundary.control_overlay.network_id.clone(),
        peer_id: local_peer_id,
        addresses: snapshot.listen_addresses.clone(),
        advertised_roles: Some(snapshot.configured_roles.clone()),
        announced_at: Utc::now(),
    };
    shell.publish_peer_directory(announcement.clone());
    if let Err(error) = shell.publish_pubsub(
        boundary.control_overlay.clone(),
        PubsubPayload::PeerDirectory(announcement),
    ) {
        snapshot.last_error = Some(error.to_string());
    }
    snapshot.control_plane = shell.snapshot().clone();
    snapshot.updated_at = Utc::now();
}

fn remember_peer_directory_addresses(
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
    announcements: &[PeerDirectoryAnnouncement],
) {
    const STALE_PEER_DIRECTORY_AFTER: chrono::Duration = chrono::Duration::minutes(5);

    let now = Utc::now();
    let local_peer_id = snapshot.local_peer_id.clone();
    let addresses = announcements
        .iter()
        .filter(|announcement| announcement.announced_at + STALE_PEER_DIRECTORY_AFTER > now)
        .filter(|announcement| Some(&announcement.peer_id) != local_peer_id.as_ref())
        .flat_map(|announcement| announcement.addresses.iter().cloned())
        .collect::<Vec<_>>();
    remember_known_peer_addresses(snapshot, storage, addresses);
}

pub(crate) fn remember_known_peer_addresses(
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
    addresses: impl IntoIterator<Item = SwarmAddress>,
) {
    let mut changed = false;
    for address in addresses {
        if snapshot.listen_addresses.contains(&address)
            || snapshot
                .local_peer_id
                .as_ref()
                .is_some_and(|peer_id| address_targets_peer(&address, peer_id))
        {
            continue;
        }
        if snapshot.known_peer_addresses.insert(address) {
            changed = true;
        }
    }

    if changed
        && let Some(storage) = storage
        && let Err(error) = persist_known_peers(storage, &snapshot.known_peer_addresses)
    {
        snapshot.last_error = Some(format!("failed to persist known peers: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connect_peer(snapshot: &mut NodeTelemetrySnapshot, peer_id: PeerId) {
        snapshot.connected_peer_ids.insert(peer_id.clone());
        snapshot.observed_peer_ids.insert(peer_id);
        snapshot.connected_peers = snapshot.connected_peer_ids.len();
    }

    fn test_snapshot(roles: impl IntoIterator<Item = PeerRole>) -> NodeTelemetrySnapshot {
        NodeTelemetrySnapshot::starting(
            &MainnetHandle {
                genesis: GenesisSpec {
                    network_id: NetworkId::new("repair-test"),
                    protocol_version: Version::new(1, 0, 0),
                    display_name: String::from("repair-test"),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                roles: PeerRoleSet::new(roles),
            },
            &NodeConfig::default(),
        )
    }

    fn test_boundary(bootstrap_addresses: Vec<SwarmAddress>) -> RuntimeBoundary {
        let network_id = NetworkId::new("repair-test");
        RuntimeBoundary {
            environment: RuntimeEnvironment::Native,
            transport_policy: RuntimeTransportPolicy::native_for_roles(
                &PeerRoleSet::default_trainer(),
            ),
            bootstrap_addresses,
            listen_addresses: Vec::new(),
            external_addresses: Vec::new(),
            webrtc_certificate_pem_path: None,
            protocols: ProtocolSet::for_network(&network_id).expect("protocols"),
            control_overlay: OverlayTopic::control(network_id),
        }
    }

    fn peer_directory_record(
        peer_id: PeerId,
        addresses: Vec<SwarmAddress>,
        roles: impl IntoIterator<Item = PeerRole>,
    ) -> PeerDirectoryAnnouncement {
        PeerDirectoryAnnouncement {
            network_id: NetworkId::new("repair-test"),
            peer_id,
            addresses,
            advertised_roles: Some(PeerRoleSet::new(roles)),
            announced_at: Utc::now(),
        }
    }

    #[test]
    fn configured_external_address_rewrites_webrtc_certhash_suffix() {
        let external =
            SwarmAddress::new("/dns4/bootstrap.example/udp/4003/webrtc-direct").expect("external");
        let listen = SwarmAddress::new(
            "/ip4/10.42.1.10/udp/4003/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
        )
        .expect("listen");
        let rewritten = rewrite_configured_external_address(&external, &listen).expect("rewritten");
        assert_eq!(
            rewritten.as_str(),
            "/dns4/bootstrap.example/udp/4003/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
        );
    }

    #[test]
    fn configured_external_address_rejects_mismatched_transport_suffix() {
        let external = SwarmAddress::new("/dns4/bootstrap.example/tcp/443/wss").expect("external");
        let listen = SwarmAddress::new(
            "/ip4/10.42.1.10/udp/4003/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
        )
        .expect("listen");
        assert!(rewrite_configured_external_address(&external, &listen).is_none());
    }

    #[test]
    fn connectivity_repair_skips_bootstrap_when_connected_to_seed() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/31001").expect("bootstrap");
        let trainer = SwarmAddress::new("/ip4/127.0.0.1/tcp/31002").expect("trainer");
        let seed_peer = PeerId::new("12D3KooWSeedRepair1111111111111111111111111111111");
        let trainer_peer = PeerId::new("12D3KooWTrainerRepair111111111111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot.known_peer_addresses.insert(bootstrap.clone());
        snapshot.known_peer_addresses.insert(trainer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer,
                addresses: vec![bootstrap.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: trainer_peer,
                addresses: vec![trainer.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap]),
            &snapshot,
            1,
            &BTreeSet::new(),
        );
        assert_eq!(targets, vec![trainer]);
    }

    #[test]
    fn connectivity_repair_never_dials_the_local_peer_identity() {
        let local_peer = PeerId::new(
            libp2p_identity::PeerId::from_public_key(
                &libp2p_identity::Keypair::generate_ed25519().public(),
            )
            .to_string(),
        );
        let remote_peer = PeerId::new(
            libp2p_identity::PeerId::from_public_key(
                &libp2p_identity::Keypair::generate_ed25519().public(),
            )
            .to_string(),
        );
        let local = SwarmAddress::new(format!(
            "/dns4/local.example/tcp/41001/p2p/{}",
            local_peer.as_str()
        ))
        .expect("local");
        let remote = SwarmAddress::new(format!(
            "/ip4/127.0.0.1/tcp/41002/p2p/{}",
            remote_peer.as_str()
        ))
        .expect("remote");
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.local_peer_id = Some(local_peer.clone());
        snapshot.control_plane.peer_directory_announcements.extend([
            peer_directory_record(
                local_peer.clone(),
                vec![local.clone()],
                [PeerRole::TrainerCpu],
            ),
            peer_directory_record(remote_peer, vec![remote.clone()], [PeerRole::TrainerCpu]),
        ]);
        snapshot.known_peer_addresses.insert(local);
        snapshot.known_peer_addresses.insert(remote.clone());

        let targets =
            connectivity_repair_targets(&test_boundary(Vec::new()), &snapshot, 0, &BTreeSet::new());

        assert_eq!(targets, vec![remote]);
        assert!(
            targets
                .iter()
                .all(|address| !address_targets_peer(address, &local_peer))
        );
    }

    #[test]
    fn connectivity_repair_uses_bootstrap_when_disconnected() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/32001").expect("bootstrap");
        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap.clone()]),
            &test_snapshot([PeerRole::TrainerCpu]),
            0,
            &BTreeSet::new(),
        );
        assert_eq!(targets, vec![bootstrap]);
    }

    #[test]
    fn connectivity_repair_prefers_configured_bootstrap_when_disconnected() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/32501").expect("bootstrap");
        let discovered = SwarmAddress::new("/ip4/127.0.0.1/tcp/32502").expect("discovered");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.known_peer_addresses.insert(discovered.clone());

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap.clone()]),
            &snapshot,
            0,
            &BTreeSet::new(),
        );
        assert_eq!(targets.first(), Some(&bootstrap));
        assert!(targets.contains(&discovered));
    }

    #[test]
    fn connectivity_repair_uses_known_peers_when_bootstrap_dial_is_pending() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/32503").expect("bootstrap");
        let discovered = SwarmAddress::new("/ip4/127.0.0.1/tcp/32504").expect("discovered");
        let pending = BTreeSet::from([connectivity_address_key(&bootstrap)]);

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.known_peer_addresses.insert(discovered.clone());

        let targets =
            connectivity_repair_targets(&test_boundary(vec![bootstrap]), &snapshot, 0, &pending);
        assert_eq!(targets, vec![discovered]);
    }

    #[test]
    fn connectivity_repair_skips_addresses_with_pending_dials() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/32701").expect("bootstrap");
        let pending = BTreeSet::from([connectivity_address_key(&bootstrap)]);

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap.clone()]),
            &test_snapshot([PeerRole::TrainerCpu]),
            0,
            &pending,
        );
        assert!(targets.is_empty());
    }

    #[test]
    fn connectivity_repair_uses_bootstrap_when_under_connected_without_mesh_targets() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/33001").expect("bootstrap");
        let trainer_peer = PeerId::new("12D3KooWTrainerRepairMesh11111111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, trainer_peer);

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap.clone()]),
            &snapshot,
            1,
            &BTreeSet::new(),
        );
        assert_eq!(targets, vec![bootstrap]);
    }

    #[test]
    fn connectivity_repair_ignores_stale_peer_directory_targets() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/34001").expect("bootstrap");
        let stale_trainer = SwarmAddress::new("/ip4/127.0.0.1/tcp/34002").expect("trainer");
        let seed_peer = PeerId::new("12D3KooWSeedRepairFresh111111111111111111111111");
        let trainer_peer = PeerId::new("12D3KooWTrainerRepairFresh11111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer,
                addresses: vec![bootstrap.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: trainer_peer,
                addresses: vec![stale_trainer],
                advertised_roles: None,
                announced_at: Utc::now() - chrono::Duration::minutes(10),
            });

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap]),
            &snapshot,
            1,
            &BTreeSet::new(),
        );
        assert!(targets.is_empty());
    }

    #[test]
    fn connectivity_repair_prefers_direct_addresses_before_relay_paths() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/35001").expect("bootstrap");
        let seed_peer = PeerId::new("12D3KooWSeedRepairDirect11111111111111111111111");
        let trainer_peer = PeerId::new("12D3KooWTrainerRepairDirect1111111111111111111");
        let direct = SwarmAddress::new("/ip4/127.0.0.1/tcp/35002").expect("direct");
        let relay = SwarmAddress::new("/ip4/127.0.0.1/tcp/35001/p2p-circuit").expect("relay");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer,
                addresses: vec![bootstrap.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: trainer_peer,
                addresses: vec![relay, direct.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap]),
            &snapshot,
            1,
            &BTreeSet::new(),
        );
        assert_eq!(targets, vec![direct]);
    }

    #[test]
    fn connectivity_repair_falls_back_to_relay_path_when_no_direct_address_exists() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/36001").expect("bootstrap");
        let seed_peer = PeerId::new("12D3KooWSeedRepairRelay111111111111111111111111");
        let trainer_peer = PeerId::new("12D3KooWTrainerRepairRelay11111111111111111111");
        let relay = SwarmAddress::new("/ip4/127.0.0.1/tcp/36001/p2p-circuit").expect("relay");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer,
                addresses: vec![bootstrap.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: trainer_peer,
                addresses: vec![relay.clone()],
                advertised_roles: None,
                announced_at: Utc::now(),
            });

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap]),
            &snapshot,
            1,
            &BTreeSet::new(),
        );
        assert_eq!(targets, vec![relay]);
    }

    #[test]
    fn bootstrap_offload_disconnects_seed_after_mesh_target_is_met() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/37001").expect("bootstrap");
        let seed_peer = PeerId::new("12D3KooWSeedOffload11111111111111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer.clone(),
                addresses: vec![bootstrap.clone()],
                advertised_roles: Some(PeerRoleSet::new([
                    PeerRole::Bootstrap,
                    PeerRole::RelayHelper,
                ])),
                announced_at: Utc::now(),
            });

        for index in 0..4 {
            let peer_id = PeerId::new(format!(
                "12D3KooWMeshOffload{index:02}1111111111111111111111111111"
            ));
            let address =
                SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/37{:03}", index + 2)).expect("mesh");
            connect_peer(&mut snapshot, peer_id.clone());
            snapshot
                .control_plane
                .peer_directory_announcements
                .push(PeerDirectoryAnnouncement {
                    network_id: NetworkId::new("repair-test"),
                    peer_id,
                    addresses: vec![address],
                    advertised_roles: Some(PeerRoleSet::new([PeerRole::TrainerCpu])),
                    announced_at: Utc::now(),
                });
        }

        let targets = bootstrap_offload_targets(&test_boundary(vec![bootstrap]), &snapshot);
        assert_eq!(targets, vec![seed_peer]);
    }

    #[test]
    fn bootstrap_offload_keeps_seed_when_mesh_target_is_not_met() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/38001").expect("bootstrap");
        let seed_peer = PeerId::new("12D3KooWSeedRetain111111111111111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, seed_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: seed_peer,
                addresses: vec![bootstrap.clone()],
                advertised_roles: Some(PeerRoleSet::new([
                    PeerRole::Bootstrap,
                    PeerRole::RelayHelper,
                ])),
                announced_at: Utc::now(),
            });

        for index in 0..3 {
            let peer_id = PeerId::new(format!(
                "12D3KooWMeshRetain{index:02}11111111111111111111111111111"
            ));
            let address =
                SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/38{:03}", index + 2)).expect("mesh");
            connect_peer(&mut snapshot, peer_id.clone());
            snapshot
                .control_plane
                .peer_directory_announcements
                .push(PeerDirectoryAnnouncement {
                    network_id: NetworkId::new("repair-test"),
                    peer_id,
                    addresses: vec![address],
                    advertised_roles: Some(PeerRoleSet::new([PeerRole::TrainerCpu])),
                    announced_at: Utc::now(),
                });
        }

        let targets = bootstrap_offload_targets(&test_boundary(vec![bootstrap]), &snapshot);
        assert!(targets.is_empty());
    }

    #[test]
    fn bootstrap_offload_does_not_disconnect_non_bootstrap_peer_used_as_initial_seed() {
        let validator_addr = SwarmAddress::new("/ip4/127.0.0.1/tcp/39001").expect("validator");
        let validator_peer = PeerId::new("12D3KooWValidatorSeed11111111111111111111111111111");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        connect_peer(&mut snapshot, validator_peer.clone());
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(PeerDirectoryAnnouncement {
                network_id: NetworkId::new("repair-test"),
                peer_id: validator_peer,
                addresses: vec![validator_addr.clone()],
                advertised_roles: Some(PeerRoleSet::new([
                    PeerRole::Authority,
                    PeerRole::Validator,
                ])),
                announced_at: Utc::now(),
            });

        for index in 0..4 {
            let peer_id = PeerId::new(format!(
                "12D3KooWMeshNoDrop{index:02}1111111111111111111111111111"
            ));
            let address =
                SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/39{:03}", index + 2)).expect("mesh");
            connect_peer(&mut snapshot, peer_id.clone());
            snapshot
                .control_plane
                .peer_directory_announcements
                .push(PeerDirectoryAnnouncement {
                    network_id: NetworkId::new("repair-test"),
                    peer_id,
                    addresses: vec![address],
                    advertised_roles: Some(PeerRoleSet::new([PeerRole::TrainerCpu])),
                    announced_at: Utc::now(),
                });
        }

        let targets = bootstrap_offload_targets(&test_boundary(vec![validator_addr]), &snapshot);
        assert!(targets.is_empty());
    }

    #[test]
    fn excess_connected_peer_offload_prunes_non_protected_peers() {
        let mut boundary = test_boundary(Vec::new());
        boundary.transport_policy.max_established_total = Some(3);

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        let peers = (0..5)
            .map(|index| {
                PeerId::new(format!(
                    "12D3KooWExcessPeer{index:02}1111111111111111111111111"
                ))
            })
            .collect::<Vec<_>>();
        for peer_id in peers.iter().cloned() {
            connect_peer(&mut snapshot, peer_id);
        }

        let targets = excess_connected_peer_offload_targets(&boundary, &snapshot);
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|peer_id| peers.contains(peer_id)));
    }

    #[test]
    fn excess_connected_peer_offload_preserves_protected_peers() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/40001").expect("bootstrap");
        let protected_peer = PeerId::new("12D3KooWProtectedSeed1111111111111111111111111111");
        let trainer_a = PeerId::new("12D3KooWExcessTrainerA111111111111111111111111111");
        let trainer_b = PeerId::new("12D3KooWExcessTrainerB111111111111111111111111111");

        let mut boundary = test_boundary(vec![bootstrap.clone()]);
        boundary.transport_policy.max_established_total = Some(2);

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        for peer_id in [protected_peer.clone(), trainer_a.clone(), trainer_b.clone()] {
            connect_peer(&mut snapshot, peer_id);
        }
        snapshot
            .control_plane
            .peer_directory_announcements
            .push(peer_directory_record(
                protected_peer.clone(),
                vec![bootstrap],
                [PeerRole::Bootstrap, PeerRole::RelayHelper],
            ));

        let targets = excess_connected_peer_offload_targets(&boundary, &snapshot);
        assert_eq!(targets.len(), 1);
        assert!(!targets.contains(&protected_peer));
        assert!(
            targets
                .iter()
                .all(|peer_id| [trainer_a.clone(), trainer_b.clone()].contains(peer_id))
        );
    }

    #[test]
    fn diloco_coalescing_keeps_chunk_indices_distinct() {
        let peer_id = "12D3KooWChunkKeyPeer111111111111111111111111111111";
        let cursor = RoundCursor::new(BaseCheckpointId::new("base"), 1);
        let keys = (0..16)
            .map(|chunk_index| {
                diloco_request_coalesce_key(
                    peer_id,
                    &DiLoCoRequest::GradientSlice {
                        experiment_id: ExperimentId::new("experiment"),
                        revision_id: RevisionId::new("revision"),
                        round_cursor: cursor.clone(),
                        chunk_index,
                    },
                )
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(keys.len(), 16);
    }
}
