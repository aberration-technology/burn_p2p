use super::*;

pub(crate) fn run_control_plane(
    boundary: RuntimeBoundary,
    keypair: Keypair,
    storage: Option<StorageConfig>,
    auth: Option<AuthConfig>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    state: Arc<Mutex<NodeTelemetrySnapshot>>,
) {
    const REDIAL_INTERVAL: Duration = Duration::from_secs(1);
    const TRUST_BUNDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
    let mut auth = auth;
    let mut shell = match ControlPlaneShell::new(
        boundary.protocols.control.clone(),
        keypair,
        boundary
            .listen_addresses
            .iter()
            .chain(boundary.bootstrap_addresses.iter())
            .cloned(),
    ) {
        Ok(shell) => shell,
        Err(error) => {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_error(error.to_string());
            return;
        }
    };
    {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
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
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.last_error = Some(format!("failed to restore control plane state: {error}"));
    }

    for address in &boundary.listen_addresses {
        if let Err(error) = shell.listen_on(address.clone()) {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_error(error.to_string());
            return;
        }
    }

    for address in &boundary.bootstrap_addresses {
        if let Err(error) = shell.dial(address.clone()) {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.push_event(LiveControlPlaneEvent::Other {
                kind: format!("bootstrap-dial-error:{error}"),
            });
        }
    }

    if let Err(error) = shell.subscribe_topic(boundary.control_overlay.clone()) {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.set_error(error.to_string());
        return;
    }

    {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.status = RuntimeStatus::Running;
        if let Some(storage) = storage.as_ref()
            && let Err(error) = restore_runtime_security_state(storage, &mut snapshot)
        {
            snapshot.last_error = Some(format!("failed to restore security state: {error}"));
        }
        if !matches!(
            snapshot.node_state,
            NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked
        ) {
            snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
        }
        snapshot.connected_peers = shell.connected_peer_count();
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
                        .expect("telemetry state lock should not be poisoned")
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

        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
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

    let mut last_redial_at = Instant::now();
    let mut last_trust_bundle_sync_at = Instant::now()
        .checked_sub(TRUST_BUNDLE_REFRESH_INTERVAL)
        .unwrap_or_else(Instant::now);

    loop {
        let mut shutdown_requested = false;
        loop {
            match command_rx.try_recv() {
                Ok(RuntimeCommand::SubscribeTopic(topic)) => {
                    if let Err(error) = shell.subscribe_topic(topic.clone()) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.push_event(LiveControlPlaneEvent::Other {
                            kind: format!("topic-subscribe-error:{}:{error}", topic.as_str()),
                        });
                        snapshot.last_error = Some(error.to_string());
                    }
                }
                Ok(RuntimeCommand::PublishControl(announcement)) => {
                    shell.publish_control(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Control(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                Ok(RuntimeCommand::PublishHead(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_head(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Head(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishLease(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_lease(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Lease(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMerge(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_merge(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Merge(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishUpdate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_update(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Update(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAggregate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_aggregate(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Aggregate(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAuth(announcement)) => {
                    let local_announcement = announcement.clone();
                    shell.publish_auth(local_announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Auth(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
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
                    shell.publish_directory(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Directory(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMetrics(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_metrics(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Metrics(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishArtifact { descriptor, chunks }) => {
                    shell.publish_artifact(descriptor, chunks);
                }
                Ok(RuntimeCommand::FetchSnapshot {
                    peer_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_snapshot(&peer_id, timeout)
                        .map_err(|error| error.to_string());
                    if let Ok(remote_snapshot) = &result {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_snapshot_peer_id = Some(PeerId::new(peer_id.clone()));
                        snapshot.last_snapshot = Some(remote_snapshot.clone());
                        snapshot.updated_at = Utc::now();
                    }
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::FetchArtifactManifest {
                    peer_id,
                    artifact_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_artifact_manifest(&peer_id, artifact_id, timeout)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::FetchArtifactChunk {
                    peer_id,
                    artifact_id,
                    chunk_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_artifact_chunk(&peer_id, artifact_id, chunk_id, timeout)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::RequestSnapshot { peer_id }) => {
                    if let Err(error) = shell.request_snapshot(&peer_id) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
                            peer_id,
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
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_node_state(NodeRuntimeState::ShuttingDown);
            snapshot.status = RuntimeStatus::Stopped;
            snapshot.updated_at = Utc::now();
            return;
        }

        if shell.connected_peer_count() == 0 && last_redial_at.elapsed() >= REDIAL_INTERVAL {
            let known_addresses = {
                let snapshot = state
                    .lock()
                    .expect("telemetry state lock should not be poisoned");
                snapshot.known_peer_addresses.clone()
            };
            for address in boundary
                .bootstrap_addresses
                .iter()
                .cloned()
                .chain(known_addresses)
            {
                let _ = shell.dial(address);
            }
            last_redial_at = Instant::now();
        }

        if last_trust_bundle_sync_at.elapsed() >= TRUST_BUNDLE_REFRESH_INTERVAL {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
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

        match shell.wait_event(Duration::from_millis(50)) {
            Some(event) => {
                let mut connection_request_error = None;
                if let LiveControlPlaneEvent::ConnectionEstablished { peer_id } = &event
                    && let Err(error) = shell.request_snapshot(peer_id)
                {
                    connection_request_error = Some((peer_id.clone(), error.to_string()));
                }

                let mut snapshot = state
                    .lock()
                    .expect("telemetry state lock should not be poisoned");
                snapshot.connected_peers = shell.connected_peer_count();
                snapshot.control_plane = shell.snapshot().clone();
                if matches!(
                    &event,
                    LiveControlPlaneEvent::PubsubMessage { kind, .. }
                        if kind == "control"
                            || kind == "auth"
                            || kind == "directory"
                            || kind == "lease"
                ) && let Some(storage) = storage.as_ref()
                    && let Err(error) =
                        persist_control_plane_state(storage, &snapshot.control_plane)
                {
                    snapshot.last_error =
                        Some(format!("failed to persist control plane state: {error}"));
                }
                if let Some((peer_id, message)) = connection_request_error {
                    snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
                        peer_id,
                        message: message.clone(),
                    });
                    snapshot.last_error = Some(message);
                }
                match &event {
                    LiveControlPlaneEvent::NewListenAddr { address } => {
                        if !snapshot.listen_addresses.contains(address) {
                            snapshot.listen_addresses.push(address.clone());
                        }
                    }
                    LiveControlPlaneEvent::PeersDiscovered { peers } => {
                        remember_known_peer_addresses(
                            &mut snapshot,
                            storage.as_ref(),
                            peers.iter().map(|(_, address)| address.clone()),
                        );
                    }
                    LiveControlPlaneEvent::PeerIdentified {
                        listen_addresses, ..
                    } => {
                        remember_known_peer_addresses(
                            &mut snapshot,
                            storage.as_ref(),
                            listen_addresses.iter().cloned(),
                        );
                    }
                    LiveControlPlaneEvent::SnapshotReceived {
                        peer_id,
                        snapshot: remote_snapshot,
                    } => {
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
                                Ok(report)
                                    if matches!(report.decision(), AdmissionDecision::Allow) =>
                                {
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
                        reconcile_live_revocation_policy(
                            &mut auth,
                            &mut snapshot,
                            storage.as_ref(),
                        );
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
                    | LiveControlPlaneEvent::PeersExpired { .. } => {}
                    LiveControlPlaneEvent::Other { .. }
                    | LiveControlPlaneEvent::ConnectionEstablished { .. }
                    | LiveControlPlaneEvent::ArtifactManifestRequested { .. }
                    | LiveControlPlaneEvent::ArtifactManifestReceived { .. }
                    | LiveControlPlaneEvent::ArtifactChunkRequested { .. }
                    | LiveControlPlaneEvent::ArtifactChunkReceived { .. }
                    | LiveControlPlaneEvent::SnapshotRequested { .. }
                    | LiveControlPlaneEvent::SnapshotResponseSent { .. } => {}
                }
                if matches!(
                    &event,
                    LiveControlPlaneEvent::PubsubMessage { kind, .. }
                        if kind == "control" || kind == "auth" || kind == "directory"
                ) {
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
                snapshot.push_event(event);
                if let Some(storage) = storage.as_ref()
                    && let Err(error) = persist_runtime_security_state(storage, &snapshot)
                {
                    snapshot.last_error =
                        Some(format!("failed to persist security state: {error}"));
                }
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}

pub(crate) fn remember_known_peer_addresses(
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
    addresses: impl IntoIterator<Item = SwarmAddress>,
) {
    let mut changed = false;
    for address in addresses {
        if snapshot.listen_addresses.contains(&address) {
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
