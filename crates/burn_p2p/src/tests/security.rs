use super::support::*;

#[test]
fn control_handle_updates_local_head_announcements() {
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "runtime did not start",
    );

    let experiment = running.experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    );
    let control = running.control_handle();
    control
        .publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set().expect("overlays").heads,
            provider_peer_id: None,
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-1"),
                study_id: crate::StudyId::new("study-1"),
                experiment_id: crate::ExperimentId::new("exp-1"),
                revision_id: crate::RevisionId::new("rev-1"),
                artifact_id: crate::ArtifactId::new("artifact-1"),
                parent_head_id: Some(crate::HeadId::new("head-0")),
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish head");

    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().control_plane.head_announcements.len() == 1,
        "head announcement was not reflected in local telemetry",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.head_announcements.len(), 1);
    assert!(snapshot.local_peer_id.is_some());

    running.shutdown().expect("shutdown");
    let _ = running.await_termination().expect("await termination");
}

#[test]
fn control_handle_updates_auth_and_directory_announcements() {
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "runtime did not start",
    );

    let experiment_id = crate::ExperimentId::new("exp-auth");
    let control = running.control_handle();
    control
        .publish_auth(PeerAuthAnnouncement {
            peer_id: crate::PeerId::new("peer-auth"),
            envelope: PeerAuthEnvelope {
                peer_id: crate::PeerId::new("peer-auth"),
                certificate: NodeCertificate::new(
                    semver::Version::new(0, 1, 0),
                    NodeCertificateClaims {
                        network_id: crate::NetworkId::new("net-1"),
                        project_family_id: crate::ProjectFamilyId::new("family-auth"),
                        release_train_hash: crate::ContentId::new("train-auth"),
                        target_artifact_hash: crate::ContentId::new("artifact-native-auth"),
                        peer_id: crate::PeerId::new("peer-auth"),
                        peer_public_key_hex: "001122".into(),
                        principal_id: crate::PrincipalId::new("principal-auth"),
                        provider: crate::AuthProvider::Static {
                            authority: "lab-auth".into(),
                        },
                        granted_roles: crate::PeerRoleSet::new([crate::PeerRole::TrainerGpu]),
                        experiment_scopes: BTreeSet::from([
                            crate::ExperimentScope::Connect,
                            crate::ExperimentScope::Train {
                                experiment_id: experiment_id.clone(),
                            },
                        ]),
                        client_policy_hash: Some(crate::ContentId::new("policy-auth")),
                        not_before: Utc::now(),
                        not_after: Utc::now(),
                        serial: 5,
                        revocation_epoch: crate::RevocationEpoch(2),
                    },
                    burn_p2p_core::SignatureMetadata {
                        signer: crate::PeerId::new("authority-auth"),
                        key_id: "authority-key".into(),
                        algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                        signed_at: Utc::now(),
                        signature_hex: "deadbeef".into(),
                    },
                )
                .expect("node certificate"),
                client_manifest_id: Some(crate::ContentId::new("manifest-auth")),
                requested_scopes: BTreeSet::from([crate::ExperimentScope::Train {
                    experiment_id: experiment_id.clone(),
                }]),
                nonce_hash: crate::ContentId::new("nonce-auth"),
                challenge_signature_hex: "feedbead".into(),
                presented_at: Utc::now(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish auth");
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: crate::NetworkId::new("net-1"),
            entries: vec![ExperimentDirectoryEntry {
                network_id: crate::NetworkId::new("net-1"),
                study_id: crate::StudyId::new("study-auth"),
                experiment_id: experiment_id.clone(),
                workload_id: crate::WorkloadId::new("auth-demo"),
                display_name: "Auth Demo".into(),
                model_schema_hash: crate::ContentId::new("model-auth"),
                dataset_view_id: crate::DatasetViewId::new("view-auth"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([crate::PeerRole::TrainerGpu]),
                    minimum_device_memory_bytes: Some(1024),
                    minimum_system_memory_bytes: Some(2048),
                    estimated_download_bytes: 8192,
                    estimated_window_seconds: 45,
                },
                visibility: crate::ExperimentVisibility::OptIn,
                opt_in_policy: crate::ExperimentOptInPolicy::Scoped,
                current_revision_id: crate::RevisionId::new("rev-auth"),
                current_head_id: Some(crate::HeadId::new("head-auth")),
                allowed_roles: crate::PeerRoleSet::new([crate::PeerRole::TrainerGpu]),
                allowed_scopes: BTreeSet::from([crate::ExperimentScope::Train { experiment_id }]),
                metadata: BTreeMap::from([("owner".into(), "lab-auth".into())]),
            }],
            announced_at: Utc::now(),
        })
        .expect("publish directory");

    wait_for(
        Duration::from_secs(2),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.auth_announcements.len() == 1
                && snapshot.control_plane.directory_announcements.len() == 1
        },
        "auth or directory announcements were not reflected in local telemetry",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.auth_announcements.len(), 1);
    assert_eq!(snapshot.control_plane.directory_announcements.len(), 1);

    running.shutdown().expect("shutdown");
    let _ = running.await_termination().expect("await termination");
}

#[test]
fn persisted_local_auth_and_policy_survive_restart_without_with_auth() {
    let storage = tempdir().expect("persisted auth storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-restored");

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy.clone()))
        .spawn()
        .expect("first spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = first_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.local_peer_id.is_some()
        },
        "first runtime did not start",
    );
    let local_peer_id = first_telemetry
        .snapshot()
        .local_peer_id
        .expect("local peer id");
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let control = first.control_handle();
    control
        .publish_auth(PeerAuthAnnouncement {
            peer_id: local_peer_id.clone(),
            envelope: local_peer_auth.clone(),
            announced_at: Utc::now(),
        })
        .expect("publish local auth");
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![auth_scoped_directory_entry(experiment_id.clone())],
            announced_at: Utc::now(),
        })
        .expect("publish scoped directory");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = first_telemetry.snapshot();
            snapshot
                .control_plane
                .auth_announcements
                .iter()
                .any(|announcement| announcement.peer_id == local_peer_id)
                && !snapshot.control_plane.directory_announcements.is_empty()
        },
        "first runtime did not publish persisted auth state",
    );

    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");
    assert!(
        fs::read(storage_config.auth_state_path()).is_ok(),
        "auth state should be persisted"
    );

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id == Some(local_peer_id.clone())
                && snapshot
                    .control_plane
                    .directory_announcements
                    .iter()
                    .any(|announcement| {
                        announcement
                            .entries
                            .iter()
                            .any(|entry| entry.experiment_id == experiment_id)
                    })
        },
        "restarted runtime did not restore persisted auth state",
    );

    let restored_auth = restarted
        .config()
        .auth
        .as_ref()
        .expect("restored auth config");
    assert_eq!(
        restored_auth.admission_policy.as_ref(),
        Some(&admission_policy)
    );
    assert_eq!(
        restored_auth.local_peer_auth.as_ref(),
        Some(&local_peer_auth)
    );
    let visible = restarted.list_experiments();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].experiment_id, experiment_id);

    restarted.shutdown().expect("restarted shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted termination");
}

#[test]
fn admitted_peer_security_state_survives_restart() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-admitted");
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy.clone()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer_auth = issue_local_test_peer_auth(
        &dialer_storage_config,
        &authority,
        &dialer_peer_id,
        experiment_id.clone(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new()
                .with_local_peer_auth(dialer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("dialer spawn");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.admitted_peers.contains_key(&dialer_peer_id)
                && snapshot.peer_reputation.contains_key(&dialer_peer_id)
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(1))
        },
        "listener did not persist admitted peer security state",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");

    assert!(
        fs::read(listener_storage_config.security_state_path()).is_ok(),
        "security state should be persisted"
    );

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .spawn()
        .expect("restarted listener spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.admitted_peers.contains_key(&dialer_peer_id)
                && snapshot.peer_reputation.contains_key(&dialer_peer_id)
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(1))
        },
        "restarted listener did not restore admitted security state",
    );

    restarted.shutdown().expect("restarted listener shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted listener termination");
}

#[test]
fn rejected_peer_reason_survives_restart() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config)
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");

    wait_for(
        Duration::from_secs(5),
        || {
            listener_telemetry
                .snapshot()
                .rejected_peers
                .get(&dialer_peer_id)
                .is_some_and(|reason| reason.contains("did not publish an auth envelope"))
        },
        "listener did not persist rejected peer reason",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config)
        .spawn()
        .expect("restarted listener spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            restarted_telemetry
                .snapshot()
                .rejected_peers
                .get(&dialer_peer_id)
                .is_some_and(|reason| reason.contains("did not publish an auth envelope"))
        },
        "restarted listener did not restore rejected peer reason",
    );

    restarted.shutdown().expect("restarted listener shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted listener termination");
}

#[test]
fn live_revocation_epoch_updates_reject_stale_peer_sync() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-revoke");
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");
    let payload = b"artifact-bytes-before-revocation".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config)
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer_auth = issue_local_test_peer_auth(
        &dialer_storage_config,
        &authority,
        &dialer_peer_id,
        experiment_id.clone(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new()
                .with_local_peer_auth(dialer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot
                    .control_plane
                    .auth_announcements
                    .iter()
                    .any(|announcement| announcement.peer_id == dialer_peer_id)
        },
        "dialer did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || listener_telemetry.snapshot().connected_peers >= 1,
        "listener did not observe dialer connection",
    );
    listener
        .control_handle()
        .request_snapshot(dialer_peer_id.as_str())
        .expect("request dialer snapshot");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.admitted_peers.contains_key(&dialer_peer_id)
        },
        "listener did not admit dialer",
    );

    let dialer_store = dialer.artifact_store().expect("dialer store");
    let descriptor = dialer_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema-auth-revoke"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    dialer
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let synced_descriptor = listener
        .sync_artifact_from_peer(&dialer_peer_id, descriptor.artifact_id.clone())
        .expect("initial artifact sync should succeed");
    assert_eq!(synced_descriptor, descriptor);

    listener
        .control_handle()
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::RevokePeer {
                peer_id: dialer_peer_id.clone(),
                minimum_revocation_epoch: crate::RevocationEpoch(3),
                reason: "rotated certs".into(),
            },
        ))
        .expect("publish revoke control");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(3))
                && !snapshot.admitted_peers.contains_key(&dialer_peer_id)
        },
        "listener did not apply live revocation epoch",
    );

    let error = listener
        .sync_artifact_from_peer(&dialer_peer_id, descriptor.artifact_id.clone())
        .expect_err("revoked peer should be rejected for artifact sync");
    assert!(
        error.to_string().contains("not admitted for artifact sync")
            || error
                .to_string()
                .contains("request failure: Failed to dial the requested peer"),
        "unexpected error after live revocation: {error}",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn local_revocation_control_moves_node_into_revoked_state() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-local-revoke");
    let storage = tempdir().expect("local revoke storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("local revoke storage layout");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &authority,
        &local_peer_id,
        experiment_id.clone(),
    );

    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config)
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(admission_policy)
                .with_local_peer_auth(local_peer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("local revoke runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id == Some(local_peer_id.clone())
                && snapshot
                    .control_plane
                    .auth_announcements
                    .iter()
                    .any(|announcement| announcement.peer_id == local_peer_id)
        },
        "local auth runtime did not publish its auth envelope",
    );

    running
        .control_handle()
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::RevokePeer {
                peer_id: local_peer_id.clone(),
                minimum_revocation_epoch: crate::RevocationEpoch(3),
                reason: "local certificate revoked".into(),
            },
        ))
        .expect("publish local revoke control");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state == crate::NodeRuntimeState::Revoked
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(3))
                && matches!(
                    snapshot.slot_states.first(),
                    Some(crate::SlotRuntimeState::Blocked { reason, .. })
                        if reason.contains("local certificate revoked")
                )
        },
        "local runtime did not move into revoked state",
    );

    running.shutdown().expect("local revoke shutdown");
    let _ = running
        .await_termination()
        .expect("local revoke termination");
}

#[test]
fn automatic_trust_bundle_rollout_updates_policy_and_revokes_legacy_local_certificate() {
    let legacy_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-legacy",
    )
    .expect("legacy authority");
    let rotated_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-rotated",
    )
    .expect("rotated authority");
    let storage = tempdir().expect("trust bundle storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("trust bundle storage layout");
    let experiment_id = crate::ExperimentId::new("exp-auth-trust-rollout");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local trust identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &legacy_authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let initial_bundle = test_trust_bundle_export(
        &legacy_authority,
        Vec::new(),
        crate::RevocationEpoch(1),
        None,
    );
    let trust_server = TestTrustBundleServer::start(&initial_bundle);

    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(auth_test_admission_policy(&legacy_authority))
                .with_local_peer_auth(local_peer_auth)
                .with_trust_bundle_endpoint(trust_server.url().to_owned())
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("trust rollout runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot
                    .trust_bundle
                    .as_ref()
                    .is_some_and(|bundle| bundle.source_url == trust_server.url())
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == legacy_authority.issuer_peer_id()
                        && bundle.trusted_issuers.len() == 1
                })
        },
        "runtime did not ingest initial trust bundle",
    );

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        vec![(
            legacy_authority.issuer_peer_id(),
            legacy_authority.issuer_public_key_hex().into(),
            true,
        )],
        crate::RevocationEpoch(2),
        Some(ClientReenrollmentStatus {
            reason: "authority rotation".into(),
            rotated_at: Some(Utc::now()),
            retired_issuer_peer_ids: BTreeSet::from([legacy_authority.issuer_peer_id()]),
            login_path: "/login/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
        }),
    ));

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state != crate::NodeRuntimeState::Revoked
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(2))
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                        && bundle
                            .trusted_issuers
                            .contains_key(&legacy_authority.issuer_peer_id())
                        && bundle
                            .trusted_issuers
                            .contains_key(&rotated_authority.issuer_peer_id())
                        && bundle.reenrollment.as_ref().is_some_and(|reenrollment| {
                            reenrollment
                                .retired_issuer_peer_ids
                                .contains(&legacy_authority.issuer_peer_id())
                        })
                })
        },
        "runtime did not ingest rotated trust bundle",
    );

    let persisted_auth: serde_json::Value = serde_json::from_slice(
        &fs::read(storage_config.auth_state_path()).expect("read persisted auth state"),
    )
    .expect("decode persisted auth state");
    assert_eq!(
        persisted_auth["trust_bundle_endpoints"][0],
        serde_json::Value::String(trust_server.url().to_owned())
    );
    assert_eq!(
        persisted_auth["admission_policy"]["trusted_issuers"]
            .as_object()
            .map(|issuers| issuers.len()),
        Some(2)
    );

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        Vec::new(),
        crate::RevocationEpoch(2),
        None,
    ));

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state == crate::NodeRuntimeState::Revoked
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                        && bundle.trusted_issuers.len() == 1
                        && !bundle
                            .trusted_issuers
                            .contains_key(&legacy_authority.issuer_peer_id())
                })
                && matches!(
                    snapshot.slot_states.first(),
                    Some(crate::SlotRuntimeState::Blocked { reason, .. })
                        if reason.contains("untrusted issuer")
                )
        },
        "runtime did not revoke legacy local certificate after issuer retirement",
    );

    running.shutdown().expect("trust rollout shutdown");
    let _ = running
        .await_termination()
        .expect("trust rollout termination");
}

#[test]
fn trust_bundle_endpoints_survive_restart_and_resume_ingestion() {
    let legacy_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-legacy",
    )
    .expect("legacy authority");
    let rotated_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-rotated",
    )
    .expect("rotated authority");
    let storage = tempdir().expect("trust restart storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("trust restart storage layout");
    let experiment_id = crate::ExperimentId::new("exp-auth-trust-restart");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &crate::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local trust restart identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &legacy_authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let trust_server = TestTrustBundleServer::start(&test_trust_bundle_export(
        &legacy_authority,
        Vec::new(),
        crate::RevocationEpoch(1),
        None,
    ));

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(auth_test_admission_policy(&legacy_authority))
                .with_local_peer_auth(local_peer_auth)
                .with_trust_bundle_endpoint(trust_server.url().to_owned())
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("first trust restart runtime");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            first_telemetry
                .snapshot()
                .trust_bundle
                .as_ref()
                .is_some_and(|bundle| {
                    bundle.source_url == trust_server.url()
                        && bundle.active_issuer_peer_id == legacy_authority.issuer_peer_id()
                })
        },
        "first runtime did not ingest initial trust bundle",
    );
    first.shutdown().expect("first trust restart shutdown");
    let _ = first
        .await_termination()
        .expect("first trust restart termination");

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        vec![(
            legacy_authority.issuer_peer_id(),
            legacy_authority.issuer_public_key_hex().into(),
            true,
        )],
        crate::RevocationEpoch(2),
        None,
    ));

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted trust runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                bundle.source_url == trust_server.url()
                    && bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                    && bundle
                        .trusted_issuers
                        .contains_key(&legacy_authority.issuer_peer_id())
                    && bundle
                        .trusted_issuers
                        .contains_key(&rotated_authority.issuer_peer_id())
            }) && restarted.config().auth.as_ref().is_some_and(|auth| {
                auth.trust_bundle_endpoints == vec![trust_server.url().to_owned()]
            })
        },
        "restarted runtime did not restore trust bundle source or resume ingestion",
    );

    restarted.shutdown().expect("restarted trust shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted trust termination");
}
