use super::*;

pub(crate) fn run_control_plane(
    boundary: RuntimeBoundary,
    keypair: Keypair,
    storage: Option<StorageConfig>,
    auth: Option<AuthConfig>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    state: Arc<Mutex<NodeTelemetrySnapshot>>,
) {
    const CONNECTIVITY_REPAIR_INTERVAL: Duration = Duration::from_secs(1);
    const PEER_DIRECTORY_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(15);
    const TRUST_BUNDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
    const DIFFUSION_SETTLEMENT_INTERVAL: Duration = Duration::from_millis(100);
    const PENDING_DIAL_DEBOUNCE: Duration = Duration::from_secs(5);
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
    loop {
        let mut shutdown_requested = false;
        loop {
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
                Ok(RuntimeCommand::PublishControl(announcement)) => {
                    shell.publish_control(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Control(announcement),
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
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_update(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Update(announcement))
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
                }) => {
                    let result = shell
                        .fetch_snapshot(&peer_id, timeout)
                        .map_err(|error| error.to_string());
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
                    snapshot.connected_peers = shell.connected_peer_count();
                    snapshot.push_event(event);
                    snapshot.updated_at = Utc::now();
                }
            }
            let mut snapshot = lock_telemetry_state(&state);
            snapshot.set_node_state(NodeRuntimeState::ShuttingDown);
            snapshot.status = RuntimeStatus::Stopped;
            snapshot.updated_at = Utc::now();
            return;
        }

        if last_connectivity_repair_at.elapsed() >= CONNECTIVITY_REPAIR_INTERVAL {
            pending_dial_keys.retain(|_, expires_at| *expires_at > Instant::now());
            let pending_dial_key_set = pending_dial_keys.keys().cloned().collect::<BTreeSet<_>>();
            let (dial_targets, offload_targets) = {
                let snapshot = lock_telemetry_state(&state);
                (
                    connectivity_repair_targets(
                        &boundary,
                        &snapshot,
                        shell.connected_peer_count(),
                        &pending_dial_key_set,
                    ),
                    bootstrap_offload_targets(&boundary, &snapshot),
                )
            };
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

        if last_peer_directory_reannounce_at.elapsed() >= PEER_DIRECTORY_REANNOUNCE_INTERVAL {
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

        if last_trust_bundle_sync_at.elapsed() >= TRUST_BUNDLE_REFRESH_INTERVAL {
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

        if last_diffusion_settlement_at.elapsed() >= DIFFUSION_SETTLEMENT_INTERVAL {
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
        for batch_index in 0..32 {
            let wait = if batch_index == 0 {
                Duration::from_millis(50)
            } else {
                Duration::from_millis(1)
            };
            let Some(event) = shell.wait_event(wait) else {
                break;
            };
            processed_event = true;
            handle_control_plane_event(
                &mut shell,
                &boundary,
                storage.as_ref(),
                &mut auth,
                &state,
                event,
            );
        }
        if !processed_event {
            thread::sleep(Duration::from_millis(10));
        }
    }
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

fn handle_control_plane_event(
    shell: &mut ControlPlaneShell,
    boundary: &RuntimeBoundary,
    storage: Option<&StorageConfig>,
    auth: &mut Option<AuthConfig>,
    state: &Arc<Mutex<NodeTelemetrySnapshot>>,
    event: LiveControlPlaneEvent,
) {
    let mut connection_request_error = None;
    match &event {
        LiveControlPlaneEvent::ConnectionEstablished { peer_id }
        | LiveControlPlaneEvent::PeerIdentified { peer_id, .. } => {
            if let Err(error) = shell.request_snapshot(peer_id) {
                connection_request_error = Some((peer_id.clone(), error.to_string()));
            }
        }
        _ => {}
    }

    let mut snapshot = lock_telemetry_state(state);
    snapshot.connected_peers = shell.connected_peer_count();
    snapshot.control_plane = shell.snapshot().clone();
    let should_persist_control_plane = matches!(
        &event,
        LiveControlPlaneEvent::PubsubMessage { kind, .. }
            if kind == "control"
                || kind == "auth"
                || kind == "directory"
                || kind == "peer-directory"
                || kind == "lease"
    ) || matches!(
        event,
        LiveControlPlaneEvent::PeerDirectoryRecordReceived { .. }
    );
    if should_persist_control_plane
        && let Some(storage) = storage
        && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
    {
        snapshot.last_error = Some(format!("failed to persist control plane state: {error}"));
    }
    if let Some((peer_id, message)) = connection_request_error {
        snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
            peer_id,
            request_id: None,
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
            if let Some(storage) = storage
                && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
            {
                snapshot.last_error =
                    Some(format!("failed to persist control plane state: {error}"));
            }
        }
        LiveControlPlaneEvent::ReachableAddressConfirmed { address } => {
            if !snapshot.listen_addresses.contains(address) {
                snapshot.listen_addresses.push(address.clone());
            }
            publish_local_peer_directory(shell, boundary, &mut snapshot);
            snapshot.control_plane = shell.snapshot().clone();
            if let Some(storage) = storage
                && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
            {
                snapshot.last_error =
                    Some(format!("failed to persist control plane state: {error}"));
            }
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
                    !snapshot
                        .control_plane
                        .peer_directory_announcements
                        .contains(announcement)
                })
                .cloned()
                .collect::<Vec<_>>();
            merge_control_plane_snapshot(&mut snapshot.control_plane, remote_snapshot);
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
            if let Some(storage) = storage
                && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
            {
                snapshot.last_error =
                    Some(format!("failed to persist control plane state: {error}"));
            }
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
    if let Some(storage) = storage
        && let Err(error) = persist_runtime_security_state(storage, &snapshot)
    {
        snapshot.last_error = Some(format!("failed to persist security state: {error}"));
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
        .filter(|address| !bootstrap_addresses.contains(*address))
        .filter(|address| !connected_peer_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !listen_address_keys.contains(&connectivity_address_key(address)))
        .filter(|address| !pending_dial_keys.contains(&connectivity_address_key(address)))
        .cloned()
        .collect::<Vec<_>>();
    let mut bootstrap_targets = boundary
        .bootstrap_addresses
        .iter()
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

    let mut targets = peer_directory_targets
        .into_iter()
        .chain(known_peer_targets)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if targets.is_empty() {
        targets.extend(bootstrap_targets);
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
        if let Some(address) =
            rewrite_configured_external_address(external_address, listen_address)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            protocols: ProtocolSet::for_network(&network_id).expect("protocols"),
            control_overlay: OverlayTopic::control(network_id),
        }
    }

    #[test]
    fn configured_external_address_rewrites_webrtc_certhash_suffix() {
        let external = SwarmAddress::new("/dns4/bootstrap.example/udp/4003/webrtc-direct")
            .expect("external");
        let listen = SwarmAddress::new(
            "/ip4/10.42.1.10/udp/4003/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        )
        .expect("listen");
        let rewritten =
            rewrite_configured_external_address(&external, &listen).expect("rewritten");
        assert_eq!(
            rewritten.as_str(),
            "/dns4/bootstrap.example/udp/4003/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
    }

    #[test]
    fn configured_external_address_rejects_mismatched_transport_suffix() {
        let external = SwarmAddress::new("/dns4/bootstrap.example/tcp/443/wss").expect("external");
        let listen = SwarmAddress::new(
            "/ip4/10.42.1.10/udp/4003/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
    fn connectivity_repair_prefers_known_peer_addresses_before_bootstrap_seed_fallback() {
        let bootstrap = SwarmAddress::new("/ip4/127.0.0.1/tcp/32501").expect("bootstrap");
        let discovered = SwarmAddress::new("/ip4/127.0.0.1/tcp/32502").expect("discovered");

        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.known_peer_addresses.insert(discovered.clone());

        let targets = connectivity_repair_targets(
            &test_boundary(vec![bootstrap]),
            &snapshot,
            0,
            &BTreeSet::new(),
        );
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
        snapshot.observed_peer_ids.insert(trainer_peer);

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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
            snapshot.observed_peer_ids.insert(peer_id.clone());
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
        snapshot.observed_peer_ids.insert(seed_peer.clone());
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
            snapshot.observed_peer_ids.insert(peer_id.clone());
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
        snapshot.observed_peer_ids.insert(validator_peer.clone());
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
            snapshot.observed_peer_ids.insert(peer_id.clone());
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
}
