use super::support::*;

#[test]
fn fair_request_timeout_splits_budget_across_candidates() {
    let timeout = crate::node::fair_request_timeout(
        Instant::now() + Duration::from_millis(1000),
        Duration::from_secs(10),
        4,
    )
    .expect("timeout");

    assert!(
        timeout <= Duration::from_millis(250),
        "timeout should not exceed one quarter of the remaining budget"
    );
    assert!(
        timeout >= Duration::from_millis(200),
        "timeout should leave a meaningful slice for later candidates"
    );
}

#[test]
fn fair_request_timeout_respects_small_remaining_budget() {
    let timeout = crate::node::fair_request_timeout(
        Instant::now() + Duration::from_millis(120),
        Duration::from_secs(10),
        8,
    )
    .expect("timeout");

    assert!(timeout <= Duration::from_millis(120));
    assert!(timeout >= Duration::from_millis(50));
}

#[test]
fn persisted_runtime_binding_survives_restart_without_with_network() {
    let storage = tempdir().expect("persisted runtime binding storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let network_manifest = switching_network_manifest();

    let first = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(network_manifest.clone())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .spawn()
        .expect("first switching runtime");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "first switching runtime did not start",
    );
    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");

    assert!(
        fs::read(storage_config.runtime_binding_state_path()).is_ok(),
        "runtime binding should be persisted"
    );

    let restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "restarted switching runtime did not start",
    );

    assert_eq!(
        restarted.config().network_manifest.as_ref(),
        Some(&network_manifest)
    );
    assert_eq!(
        restarted
            .config()
            .client_release_manifest
            .as_ref()
            .map(|manifest| manifest.release_train_hash.clone()),
        Some(crate::ContentId::new("train-switch"))
    );
    assert_eq!(
        restarted.mainnet().network_id(),
        &network_manifest.network_id
    );
    assert_eq!(
        restarted.telemetry().snapshot().network_id,
        Some(network_manifest.network_id.clone())
    );

    restarted.shutdown().expect("restarted shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted termination");
}

#[test]
fn native_running_nodes_exchange_snapshots_over_tcp() {
    let _guard = native_swarm_test_guard();
    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let experiment = listener.experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    );
    listener
        .control_handle()
        .publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set().expect("overlays").heads,
            provider_peer_id: Some(listener_peer_id.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-native"),
                study_id: crate::StudyId::new("study-1"),
                experiment_id: crate::ExperimentId::new("exp-1"),
                revision_id: crate::RevisionId::new("rev-1"),
                artifact_id: crate::ArtifactId::new("artifact-native"),
                parent_head_id: Some(crate::HeadId::new("head-0")),
                global_step: 2,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish head");
    wait_for(
        Duration::from_secs(5),
        || {
            listener_telemetry
                .snapshot()
                .control_plane
                .head_announcements
                .len()
                == 1
        },
        "listener head announcement was not reflected locally",
    );

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot.local_peer_id.is_some()
        },
        "dialer runtime did not connect to listener",
    );

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.last_snapshot_peer_id == Some(listener_peer_id.clone())
                && snapshot
                    .last_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.head_announcements.len() == 1)
                    .unwrap_or(false)
        },
        "dialer did not receive listener snapshot automatically",
    );

    let last_snapshot = dialer_telemetry
        .snapshot()
        .last_snapshot
        .expect("last snapshot");
    assert_eq!(last_snapshot.head_announcements.len(), 1);
    assert_eq!(
        last_snapshot.head_announcements[0].head.head_id,
        crate::HeadId::new("head-native")
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn native_running_nodes_sync_artifacts_over_tcp() {
    let _guard = native_swarm_test_guard();
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-listener-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-dialer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let payload = b"artifact-bytes-over-native-sync".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage.clone()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(dialer_storage))
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let synced_descriptor = dialer
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect("sync artifact");
    assert_eq!(synced_descriptor, descriptor);

    let dialer_store = dialer.artifact_store().expect("dialer store");
    let materialized = dialer_store
        .materialize_artifact_bytes(&synced_descriptor)
        .expect("materialize");
    assert_eq!(materialized, payload);

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn synced_artifact_becomes_available_for_second_hop_sync() {
    let _guard = native_swarm_test_guard();
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-origin-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let middle_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-middle-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-second-hop-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let payload = b"artifact-bytes-over-second-hop-sync".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let middle = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(middle_storage))
        .with_bootstrap_peer(listener_addr.clone())
        .spawn()
        .expect("middle spawn");
    let middle_telemetry = middle.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = middle_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "middle runtime did not connect to listener",
    );

    let synced_descriptor = middle
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect("middle sync artifact");
    assert_eq!(synced_descriptor, descriptor);

    let middle_snapshot = middle_telemetry.snapshot();
    let middle_peer_id = middle_snapshot
        .local_peer_id
        .clone()
        .expect("middle peer id");
    let middle_addr = middle_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(dialer_storage))
        .with_bootstrap_peer(middle_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to middle",
    );

    let second_hop_descriptor = dialer
        .sync_artifact_from_peer(&middle_peer_id, descriptor.artifact_id.clone())
        .expect("dialer sync artifact from middle");
    assert_eq!(second_hop_descriptor, descriptor);

    let dialer_store = dialer.artifact_store().expect("dialer store");
    let materialized = dialer_store
        .materialize_artifact_bytes(&second_hop_descriptor)
        .expect("materialize");
    assert_eq!(materialized, payload);

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    middle.shutdown().expect("middle shutdown");
    let _ = middle.await_termination().expect("middle termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn cached_connected_snapshots_filter_peer_scoped_announcements() {
    let peer_a = crate::PeerId::new("peer-a");
    let peer_b = crate::PeerId::new("peer-b");
    let peer_c = crate::PeerId::new("peer-c");
    let experiment = experiment();
    let overlay_set = experiment.overlay_set().expect("overlay set");
    let mut snapshot =
        crate::NodeTelemetrySnapshot::starting(&mainnet(), &crate::NodeConfig::default());
    snapshot.observed_peer_ids.insert(peer_a.clone());
    snapshot.observed_peer_ids.insert(peer_b.clone());
    snapshot.control_plane.head_announcements = vec![
        HeadAnnouncement {
            overlay: overlay_set.heads.clone(),
            provider_peer_id: Some(peer_a.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-a"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: crate::ArtifactId::new("artifact-a"),
                parent_head_id: Some(crate::HeadId::new("head-0")),
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        },
        HeadAnnouncement {
            overlay: overlay_set.heads.clone(),
            provider_peer_id: Some(peer_b.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-b"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: crate::ArtifactId::new("artifact-b"),
                parent_head_id: Some(crate::HeadId::new("head-a")),
                global_step: 2,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        },
        HeadAnnouncement {
            overlay: overlay_set.heads.clone(),
            provider_peer_id: Some(peer_c.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-c"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: crate::ArtifactId::new("artifact-c"),
                parent_head_id: Some(crate::HeadId::new("head-b")),
                global_step: 3,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        },
    ];
    snapshot.control_plane.update_announcements = vec![
        crate::UpdateEnvelopeAnnouncement {
            overlay: overlay_set.heads.clone(),
            update: crate::UpdateAnnounce {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(1),
                base_head_id: crate::HeadId::new("head-0"),
                peer_id: peer_a.clone(),
                lease_id: None,
                delta_artifact_id: crate::ArtifactId::new("delta-a"),
                sample_weight: 1.0,
                quality_weight: 1.0,
                norm_stats: crate::UpdateNormStats {
                    l2_norm: 0.0,
                    max_abs: 0.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: crate::ContentId::new("receipt-root-a"),
                receipt_ids: vec![crate::ContributionReceiptId::new("receipt-a")],
                providers: vec![peer_a.clone()],
                announced_at: Utc::now(),
            },
        },
        crate::UpdateEnvelopeAnnouncement {
            overlay: overlay_set.heads.clone(),
            update: crate::UpdateAnnounce {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(1),
                base_head_id: crate::HeadId::new("head-0"),
                peer_id: peer_b.clone(),
                lease_id: None,
                delta_artifact_id: crate::ArtifactId::new("delta-b"),
                sample_weight: 1.0,
                quality_weight: 1.0,
                norm_stats: crate::UpdateNormStats {
                    l2_norm: 0.0,
                    max_abs: 0.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: crate::ContentId::new("receipt-root-b"),
                receipt_ids: vec![crate::ContributionReceiptId::new("receipt-b")],
                providers: vec![peer_b.clone()],
                announced_at: Utc::now(),
            },
        },
        crate::UpdateEnvelopeAnnouncement {
            overlay: overlay_set.heads.clone(),
            update: crate::UpdateAnnounce {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(1),
                base_head_id: crate::HeadId::new("head-b"),
                peer_id: peer_c.clone(),
                lease_id: None,
                delta_artifact_id: crate::ArtifactId::new("delta-c"),
                sample_weight: 1.0,
                quality_weight: 1.0,
                norm_stats: crate::UpdateNormStats {
                    l2_norm: 0.0,
                    max_abs: 0.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: crate::ContentId::new("receipt-root-c"),
                receipt_ids: vec![crate::ContributionReceiptId::new("receipt-c")],
                providers: vec![peer_c.clone()],
                announced_at: Utc::now(),
            },
        },
    ];
    snapshot.control_plane.merge_announcements = vec![crate::MergeAnnouncement {
        overlay: overlay_set.heads,
        certificate: MergeCertificate {
            merge_cert_id: crate::MergeCertId::new("merge-b"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            base_head_id: crate::HeadId::new("head-a"),
            merged_head_id: crate::HeadId::new("head-b"),
            merged_artifact_id: crate::ArtifactId::new("artifact-b"),
            policy: crate::MergePolicy::QualityWeightedEma,
            issued_at: Utc::now(),
            promoter_peer_id: peer_b.clone(),
            promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
            contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-b")],
        },
        announced_at: Utc::now(),
    }];

    let cached = crate::runtime_support::cached_connected_snapshots(&snapshot);
    assert_eq!(cached.len(), 3);

    let cached_a = cached
        .iter()
        .find(|(peer_id, _)| peer_id == &peer_a)
        .expect("peer a snapshot");
    assert_eq!(cached_a.1.head_announcements.len(), 1);
    assert_eq!(
        cached_a.1.head_announcements[0].provider_peer_id.as_ref(),
        Some(&peer_a)
    );
    assert_eq!(cached_a.1.update_announcements.len(), 1);
    assert_eq!(cached_a.1.update_announcements[0].update.peer_id, peer_a);
    assert_eq!(cached_a.1.merge_announcements.len(), 1);

    let cached_b = cached
        .iter()
        .find(|(peer_id, _)| peer_id == &peer_b)
        .expect("peer b snapshot");
    assert_eq!(cached_b.1.head_announcements.len(), 1);
    assert_eq!(
        cached_b.1.head_announcements[0].provider_peer_id.as_ref(),
        Some(&peer_b)
    );
    assert_eq!(cached_b.1.update_announcements.len(), 1);
    assert_eq!(cached_b.1.update_announcements[0].update.peer_id, peer_b);
    assert_eq!(cached_b.1.merge_announcements.len(), 1);

    let cached_c = cached
        .iter()
        .find(|(peer_id, _)| peer_id == &peer_c)
        .expect("peer c snapshot");
    assert_eq!(cached_c.1.head_announcements.len(), 1);
    assert_eq!(
        cached_c.1.head_announcements[0].provider_peer_id.as_ref(),
        Some(&peer_c)
    );
    assert_eq!(cached_c.1.update_announcements.len(), 1);
    assert_eq!(cached_c.1.update_announcements[0].update.peer_id, peer_c);
    assert_eq!(cached_c.1.merge_announcements.len(), 1);
}

#[test]
fn resolve_canonical_head_uses_merge_artifact_id_not_only_head_id() {
    let experiment = experiment();
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let validator_a = crate::PeerId::new("validator-a");
    let validator_b = crate::PeerId::new("validator-b");
    let merged_head_id = crate::HeadId::new("merged-window-1");
    let merged_artifact_a = crate::ArtifactId::new("merged-artifact-a");
    let merged_artifact_b = crate::ArtifactId::new("merged-artifact-b");
    let created_at = Utc::now();

    let head_a = HeadDescriptor {
        head_id: merged_head_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: merged_artifact_a.clone(),
        parent_head_id: Some(crate::HeadId::new("parent")),
        global_step: 2,
        created_at,
        metrics: BTreeMap::new(),
    };
    let head_b = HeadDescriptor {
        head_id: merged_head_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: merged_artifact_b,
        parent_head_id: Some(crate::HeadId::new("parent")),
        global_step: 2,
        created_at: created_at + chrono::Duration::milliseconds(1),
        metrics: BTreeMap::new(),
    };

    let snapshot_a = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(validator_a.clone()),
            head: head_a.clone(),
            announced_at: created_at,
        }],
        merge_announcements: vec![crate::MergeAnnouncement {
            overlay: overlay.clone(),
            certificate: MergeCertificate {
                merge_cert_id: crate::MergeCertId::new("merge-a"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                base_head_id: crate::HeadId::new("parent"),
                merged_head_id: merged_head_id.clone(),
                merged_artifact_id: merged_artifact_a.clone(),
                policy: crate::MergePolicy::WeightedMean,
                issued_at: created_at,
                promoter_peer_id: validator_a.clone(),
                promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
                contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-a")],
            },
            announced_at: created_at,
        }],
        ..Default::default()
    };

    let snapshot_b = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay,
            provider_peer_id: Some(validator_b.clone()),
            head: head_b,
            announced_at: created_at + chrono::Duration::milliseconds(1),
        }],
        ..Default::default()
    };

    let storage_root = tempdir().expect("tempdir");
    let resolved = crate::runtime_support::resolve_canonical_head(
        &StorageConfig::new(storage_root.path()),
        &experiment,
        &[(validator_a.clone(), snapshot_a), (validator_b, snapshot_b)],
    )
    .expect("resolve canonical head")
    .expect("merged head");

    assert_eq!(resolved.0, validator_a);
    assert_eq!(resolved.1.head_id, merged_head_id);
    assert_eq!(resolved.1.artifact_id, merged_artifact_a);
}

#[test]
fn resolve_canonical_head_prefers_diffusion_certificate_over_later_diffusion_merge_timing() {
    let experiment = experiment();
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let promoter = crate::PeerId::new("trainer-promoter");
    let later_merger = crate::PeerId::new("trainer-later");
    let base_head_id = crate::HeadId::new("parent");
    let created_at = Utc::now();

    let diffusion_head = HeadDescriptor {
        head_id: crate::HeadId::new("diffusion-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("diffusion-artifact"),
        parent_head_id: Some(base_head_id.clone()),
        global_step: 2,
        created_at,
        metrics: BTreeMap::new(),
    };
    let later_head = HeadDescriptor {
        head_id: crate::HeadId::new("later-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("later-artifact"),
        parent_head_id: Some(base_head_id.clone()),
        global_step: 2,
        created_at: created_at + chrono::Duration::milliseconds(10),
        metrics: BTreeMap::new(),
    };

    let snapshot_a = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(promoter.clone()),
            head: diffusion_head.clone(),
            announced_at: created_at,
        }],
        diffusion_promotion_certificate_announcements: vec![
            crate::DiffusionPromotionCertificateAnnouncement {
                overlay: overlay.clone(),
                certificate: crate::DiffusionPromotionCertificate {
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(2),
                    base_head_id: base_head_id.clone(),
                    merged_head_id: diffusion_head.head_id.clone(),
                    merged_artifact_id: diffusion_head.artifact_id.clone(),
                    promotion_mode: crate::HeadPromotionMode::DiffusionSteadyState,
                    attesting_trainers: vec![
                        crate::PeerId::new("trainer-a"),
                        crate::PeerId::new("trainer-b"),
                        crate::PeerId::new("trainer-c"),
                    ],
                    attestation_ids: vec![
                        crate::ContentId::new("attestation-a"),
                        crate::ContentId::new("attestation-b"),
                        crate::ContentId::new("attestation-c"),
                    ],
                    attester_count: 3,
                    cumulative_sample_weight: 24.0,
                    settled_at: created_at,
                    promoter_peer_id: promoter.clone(),
                },
                announced_at: created_at,
            },
        ],
        ..Default::default()
    };

    let snapshot_b = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(later_merger.clone()),
            head: later_head.clone(),
            announced_at: later_head.created_at,
        }],
        merge_announcements: vec![crate::MergeAnnouncement {
            overlay,
            certificate: MergeCertificate {
                merge_cert_id: crate::MergeCertId::new("diffusion-merge-later"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                base_head_id: base_head_id.clone(),
                merged_head_id: later_head.head_id.clone(),
                merged_artifact_id: later_head.artifact_id.clone(),
                policy: crate::MergePolicy::WeightedMean,
                issued_at: later_head.created_at,
                promoter_peer_id: later_merger.clone(),
                promotion_mode: crate::HeadPromotionMode::DiffusionSteadyState,
                contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-later")],
            },
            announced_at: later_head.created_at,
        }],
        ..Default::default()
    };

    let storage_root = tempdir().expect("tempdir");
    let resolved = crate::runtime_support::resolve_canonical_head(
        &StorageConfig::new(storage_root.path()),
        &experiment,
        &[(promoter.clone(), snapshot_a), (later_merger, snapshot_b)],
    )
    .expect("resolve canonical head")
    .expect("diffusion canonical head");

    assert_eq!(resolved.0, promoter);
    assert_eq!(resolved.1.head_id, diffusion_head.head_id);
    assert_eq!(resolved.1.artifact_id, diffusion_head.artifact_id);
}

#[test]
fn resolve_canonical_head_allows_later_validator_merge_to_advance_past_diffusion() {
    let experiment = experiment();
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let promoter = crate::PeerId::new("trainer-promoter");
    let validator = crate::PeerId::new("validator-promoter");
    let base_head_id = crate::HeadId::new("parent");
    let created_at = Utc::now();

    let diffusion_head = HeadDescriptor {
        head_id: crate::HeadId::new("diffusion-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("diffusion-artifact"),
        parent_head_id: Some(base_head_id.clone()),
        global_step: 2,
        created_at,
        metrics: BTreeMap::new(),
    };
    let validator_head = HeadDescriptor {
        head_id: crate::HeadId::new("validator-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("validator-artifact"),
        parent_head_id: Some(diffusion_head.head_id.clone()),
        global_step: 3,
        created_at: created_at + chrono::Duration::milliseconds(10),
        metrics: BTreeMap::new(),
    };

    let snapshot_a = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(promoter.clone()),
            head: diffusion_head.clone(),
            announced_at: created_at,
        }],
        diffusion_promotion_certificate_announcements: vec![
            crate::DiffusionPromotionCertificateAnnouncement {
                overlay: overlay.clone(),
                certificate: crate::DiffusionPromotionCertificate {
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(2),
                    base_head_id: base_head_id.clone(),
                    merged_head_id: diffusion_head.head_id.clone(),
                    merged_artifact_id: diffusion_head.artifact_id.clone(),
                    promotion_mode: crate::HeadPromotionMode::DiffusionSteadyState,
                    attesting_trainers: vec![
                        crate::PeerId::new("trainer-a"),
                        crate::PeerId::new("trainer-b"),
                    ],
                    attestation_ids: vec![
                        crate::ContentId::new("attestation-a"),
                        crate::ContentId::new("attestation-b"),
                    ],
                    attester_count: 2,
                    cumulative_sample_weight: 16.0,
                    settled_at: created_at,
                    promoter_peer_id: promoter,
                },
                announced_at: created_at,
            },
        ],
        ..Default::default()
    };
    let snapshot_b = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(validator.clone()),
            head: validator_head.clone(),
            announced_at: validator_head.created_at,
        }],
        merge_announcements: vec![crate::MergeAnnouncement {
            overlay,
            certificate: MergeCertificate {
                merge_cert_id: crate::MergeCertId::new("validator-merge"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                base_head_id: diffusion_head.head_id.clone(),
                merged_head_id: validator_head.head_id.clone(),
                merged_artifact_id: validator_head.artifact_id.clone(),
                policy: crate::MergePolicy::WeightedMean,
                issued_at: validator_head.created_at,
                promoter_peer_id: validator.clone(),
                promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
                contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-merge")],
            },
            announced_at: validator_head.created_at,
        }],
        ..Default::default()
    };

    let storage_root = tempdir().expect("tempdir");
    let resolved = crate::runtime_support::resolve_canonical_head(
        &StorageConfig::new(storage_root.path()),
        &experiment,
        &[
            (validator.clone(), snapshot_b),
            (crate::PeerId::new("trainer-promoter"), snapshot_a),
        ],
    )
    .expect("resolve canonical head")
    .expect("validator canonical head");

    assert_eq!(resolved.0, validator);
    assert_eq!(resolved.1.head_id, validator_head.head_id);
    assert_eq!(resolved.1.artifact_id, validator_head.artifact_id);
}

#[test]
fn resolve_canonical_head_allows_later_diffusion_window_to_advance_past_earlier_support() {
    let experiment = experiment();
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let promoter_a = crate::PeerId::new("trainer-promoter-a");
    let promoter_b = crate::PeerId::new("trainer-promoter-b");
    let genesis_head = crate::HeadId::new("genesis");
    let created_at = Utc::now();

    let window_one_head = HeadDescriptor {
        head_id: crate::HeadId::new("diffusion-head-1"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("diffusion-artifact-1"),
        parent_head_id: Some(genesis_head.clone()),
        global_step: 1,
        created_at,
        metrics: BTreeMap::new(),
    };
    let window_two_head = HeadDescriptor {
        head_id: crate::HeadId::new("diffusion-head-2"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: crate::ArtifactId::new("diffusion-artifact-2"),
        parent_head_id: Some(window_one_head.head_id.clone()),
        global_step: 2,
        created_at: created_at + chrono::Duration::milliseconds(10),
        metrics: BTreeMap::new(),
    };

    let snapshot_a = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(promoter_a.clone()),
            head: window_one_head.clone(),
            announced_at: created_at,
        }],
        diffusion_promotion_certificate_announcements: vec![
            crate::DiffusionPromotionCertificateAnnouncement {
                overlay: overlay.clone(),
                certificate: crate::DiffusionPromotionCertificate {
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(1),
                    base_head_id: genesis_head.clone(),
                    merged_head_id: window_one_head.head_id.clone(),
                    merged_artifact_id: window_one_head.artifact_id.clone(),
                    promotion_mode: crate::HeadPromotionMode::DiffusionSteadyState,
                    attesting_trainers: vec![
                        crate::PeerId::new("trainer-a"),
                        crate::PeerId::new("trainer-b"),
                        crate::PeerId::new("trainer-c"),
                    ],
                    attestation_ids: vec![
                        crate::ContentId::new("attestation-a"),
                        crate::ContentId::new("attestation-b"),
                        crate::ContentId::new("attestation-c"),
                    ],
                    attester_count: 3,
                    cumulative_sample_weight: 24.0,
                    settled_at: created_at,
                    promoter_peer_id: promoter_a.clone(),
                },
                announced_at: created_at,
            },
        ],
        ..Default::default()
    };
    let snapshot_b = crate::ControlPlaneSnapshot {
        head_announcements: vec![HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(promoter_b.clone()),
            head: window_two_head.clone(),
            announced_at: window_two_head.created_at,
        }],
        diffusion_promotion_certificate_announcements: vec![
            crate::DiffusionPromotionCertificateAnnouncement {
                overlay,
                certificate: crate::DiffusionPromotionCertificate {
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: WindowId(2),
                    base_head_id: window_one_head.head_id.clone(),
                    merged_head_id: window_two_head.head_id.clone(),
                    merged_artifact_id: window_two_head.artifact_id.clone(),
                    promotion_mode: crate::HeadPromotionMode::DiffusionSteadyState,
                    attesting_trainers: vec![
                        crate::PeerId::new("trainer-a"),
                        crate::PeerId::new("trainer-b"),
                    ],
                    attestation_ids: vec![
                        crate::ContentId::new("attestation-d"),
                        crate::ContentId::new("attestation-e"),
                    ],
                    attester_count: 2,
                    cumulative_sample_weight: 16.0,
                    settled_at: window_two_head.created_at,
                    promoter_peer_id: promoter_b.clone(),
                },
                announced_at: window_two_head.created_at,
            },
        ],
        ..Default::default()
    };

    let storage_root = tempdir().expect("tempdir");
    let resolved = crate::runtime_support::resolve_canonical_head(
        &StorageConfig::new(storage_root.path()),
        &experiment,
        &[(promoter_a, snapshot_a), (promoter_b.clone(), snapshot_b)],
    )
    .expect("resolve canonical head")
    .expect("later diffusion head");

    assert_eq!(resolved.0, promoter_b);
    assert_eq!(resolved.1.head_id, window_two_head.head_id);
    assert_eq!(resolved.1.artifact_id, window_two_head.artifact_id);
}

#[test]
fn experiment_snapshot_peer_ids_only_include_relevant_experiment_peers() {
    let experiment = experiment();
    let other_experiment = mainnet().experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-2"),
        crate::RevisionId::new("rev-1"),
    );
    let mut snapshot =
        crate::NodeTelemetrySnapshot::starting(&mainnet(), &crate::NodeConfig::default());
    let relevant_provider = crate::PeerId::new("peer-provider");
    let relevant_updater = crate::PeerId::new("peer-updater");
    let relevant_validator = crate::PeerId::new("peer-validator");
    let irrelevant_peer = crate::PeerId::new("peer-irrelevant");
    snapshot.observed_peer_ids = BTreeSet::from([irrelevant_peer.clone()]);

    snapshot.control_plane.head_announcements = vec![
        HeadAnnouncement {
            overlay: experiment.overlay_set().expect("overlay").heads,
            provider_peer_id: Some(relevant_provider.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-relevant"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: crate::ArtifactId::new("artifact-relevant"),
                parent_head_id: Some(crate::HeadId::new("parent")),
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        },
        HeadAnnouncement {
            overlay: other_experiment.overlay_set().expect("overlay").heads,
            provider_peer_id: Some(irrelevant_peer.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-irrelevant"),
                study_id: other_experiment.study_id.clone(),
                experiment_id: other_experiment.experiment_id.clone(),
                revision_id: other_experiment.revision_id.clone(),
                artifact_id: crate::ArtifactId::new("artifact-irrelevant"),
                parent_head_id: Some(crate::HeadId::new("parent")),
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        },
    ];
    snapshot.control_plane.update_announcements = vec![crate::UpdateEnvelopeAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").heads,
        update: crate::UpdateAnnounce {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(1),
            base_head_id: crate::HeadId::new("parent"),
            peer_id: relevant_updater.clone(),
            lease_id: None,
            delta_artifact_id: crate::ArtifactId::new("delta-relevant"),
            sample_weight: 1.0,
            quality_weight: 1.0,
            norm_stats: crate::UpdateNormStats {
                l2_norm: 0.0,
                max_abs: 0.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: crate::ContentId::new("receipt-root-relevant"),
            receipt_ids: vec![crate::ContributionReceiptId::new("receipt-relevant")],
            providers: vec![relevant_provider.clone()],
            announced_at: Utc::now(),
        },
    }];
    snapshot.control_plane.merge_window_announcements = vec![crate::MergeWindowAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").heads,
        merge_window: crate::MergeWindowState {
            merge_window_id: crate::ContentId::new("window-1"),
            network_id: experiment.network_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(1),
            base_head_id: crate::HeadId::new("parent"),
            policy: crate::MergeTopologyPolicy::default(),
            reducers: vec![crate::PeerId::new("peer-reducer")],
            validators: vec![relevant_validator.clone()],
            opened_at: Utc::now(),
            closes_at: Utc::now(),
        },
        announced_at: Utc::now(),
    }];
    snapshot.control_plane.validation_quorum_announcements =
        vec![crate::ValidationQuorumAnnouncement {
            overlay: experiment.overlay_set().expect("overlay").heads,
            certificate: crate::ValidationQuorumCertificate {
                quorum_cert_id: crate::ContentId::new("quorum-1"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(1),
                base_head_id: crate::HeadId::new("parent"),
                aggregate_id: crate::ContentId::new("aggregate-1"),
                aggregate_artifact_id: crate::ArtifactId::new("aggregate-artifact-1"),
                merged_head_id: crate::HeadId::new("merged-1"),
                promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
                validator_quorum: 2,
                coordinator: relevant_validator.clone(),
                attesting_validators: vec![relevant_validator.clone()],
                reduction_ids: vec![crate::ContentId::new("reduction-1")],
                issued_at: Utc::now(),
            },
            announced_at: Utc::now(),
        }];

    let peer_ids = crate::runtime_support::experiment_snapshot_peer_ids(&snapshot, &experiment);
    assert!(peer_ids.contains(&relevant_provider));
    assert!(peer_ids.contains(&relevant_updater));
    assert!(peer_ids.contains(&relevant_validator));
    assert!(!peer_ids.contains(&irrelevant_peer));
}

#[test]
fn prioritized_experiment_snapshot_peer_ids_lead_with_merge_and_quorum_peers() {
    let experiment = experiment();
    let mut snapshot =
        crate::NodeTelemetrySnapshot::starting(&mainnet(), &crate::NodeConfig::default());
    let merge_validator = crate::PeerId::new("peer-merge");
    let quorum_coordinator = crate::PeerId::new("peer-coordinator");
    let quorum_attester = crate::PeerId::new("peer-attester");
    let reducer = crate::PeerId::new("peer-reducer");
    let provider = crate::PeerId::new("peer-provider");
    let updater = crate::PeerId::new("peer-updater");
    let lease_peer = crate::PeerId::new("peer-lease");
    let connected_only = crate::PeerId::new("peer-connected");
    snapshot.observed_peer_ids = BTreeSet::from([connected_only.clone()]);

    snapshot.control_plane.merge_announcements = vec![crate::MergeAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").heads.clone(),
        certificate: crate::MergeCertificate {
            merge_cert_id: crate::MergeCertId::new("merge-1"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            base_head_id: crate::HeadId::new("parent"),
            merged_head_id: crate::HeadId::new("merged"),
            merged_artifact_id: crate::ArtifactId::new("merged-artifact"),
            policy: crate::MergePolicy::WeightedMean,
            issued_at: Utc::now(),
            promoter_peer_id: merge_validator.clone(),
            promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
            contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-1")],
        },
        announced_at: Utc::now(),
    }];
    snapshot.control_plane.validation_quorum_announcements =
        vec![crate::ValidationQuorumAnnouncement {
            overlay: experiment.overlay_set().expect("overlay").heads.clone(),
            certificate: crate::ValidationQuorumCertificate {
                quorum_cert_id: crate::ContentId::new("quorum-1"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(3),
                base_head_id: crate::HeadId::new("parent"),
                aggregate_id: crate::ContentId::new("aggregate-1"),
                aggregate_artifact_id: crate::ArtifactId::new("aggregate-artifact-1"),
                merged_head_id: crate::HeadId::new("merged"),
                promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
                validator_quorum: 2,
                coordinator: quorum_coordinator.clone(),
                attesting_validators: vec![quorum_attester.clone()],
                reduction_ids: vec![crate::ContentId::new("reduction-1")],
                issued_at: Utc::now(),
            },
            announced_at: Utc::now(),
        }];
    snapshot.control_plane.aggregate_proposal_announcements =
        vec![crate::AggregateProposalAnnouncement {
            overlay: experiment.overlay_set().expect("overlay").heads.clone(),
            proposal: crate::AggregateEnvelope {
                aggregate_id: crate::ContentId::new("aggregate-1"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(3),
                base_head_id: crate::HeadId::new("parent"),
                aggregate_artifact_id: crate::ArtifactId::new("aggregate-artifact-1"),
                tier: crate::AggregateTier::RootCandidate,
                reducer_peer_id: reducer.clone(),
                contributor_peers: vec![updater.clone()],
                child_aggregate_ids: Vec::new(),
                stats: crate::AggregateStats {
                    accepted_updates: 1,
                    duplicate_updates: 0,
                    dropped_updates: 0,
                    late_updates: 0,
                    sum_sample_weight: 1.0,
                    sum_quality_weight: 1.0,
                    sum_weighted_delta_norm: 1.0,
                    max_update_norm: 1.0,
                    accepted_sample_coverage: 1.0,
                },
                providers: vec![provider.clone()],
                published_at: Utc::now(),
            },
            announced_at: Utc::now(),
        }];
    snapshot.control_plane.head_announcements = vec![HeadAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").heads.clone(),
        provider_peer_id: Some(provider.clone()),
        head: HeadDescriptor {
            head_id: crate::HeadId::new("head-relevant"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: crate::ArtifactId::new("artifact-relevant"),
            parent_head_id: Some(crate::HeadId::new("parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        announced_at: Utc::now(),
    }];
    snapshot.control_plane.update_announcements = vec![crate::UpdateEnvelopeAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").heads.clone(),
        update: crate::UpdateAnnounce {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: WindowId(3),
            base_head_id: crate::HeadId::new("parent"),
            peer_id: updater.clone(),
            lease_id: None,
            delta_artifact_id: crate::ArtifactId::new("delta-1"),
            sample_weight: 1.0,
            quality_weight: 1.0,
            norm_stats: crate::UpdateNormStats {
                l2_norm: 0.0,
                max_abs: 0.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: crate::ContentId::new("receipt-root"),
            receipt_ids: vec![crate::ContributionReceiptId::new("receipt-1")],
            providers: vec![provider.clone()],
            announced_at: Utc::now(),
        },
    }];
    snapshot.control_plane.lease_announcements = vec![crate::LeaseAnnouncement {
        overlay: experiment.overlay_set().expect("overlay").leases,
        lease: crate::AssignmentLease {
            lease_id: crate::LeaseId::new("lease-1"),
            network_id: experiment.network_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            peer_id: lease_peer.clone(),
            dataset_view_id: crate::DatasetViewId::new("view"),
            window_id: WindowId(4),
            granted_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(30),
            budget_work_units: 1,
            microshards: vec![crate::MicroShardId::new("micro-1")],
            assignment_hash: crate::ContentId::new("lease-hash"),
        },
        announced_at: Utc::now(),
    }];

    let prioritized =
        crate::runtime_support::prioritized_experiment_snapshot_peer_ids(&snapshot, &experiment);
    assert_eq!(
        prioritized[..6],
        [
            merge_validator,
            quorum_coordinator,
            quorum_attester,
            reducer,
            provider.clone(),
            updater
        ]
    );
    assert!(prioritized.contains(&lease_peer));
    assert!(prioritized.contains(&connected_only));
    assert_eq!(
        prioritized
            .iter()
            .filter(|peer_id| *peer_id == &provider)
            .count(),
        1
    );
}

#[test]
fn prioritized_artifact_source_peers_keep_requested_provider_first_without_unrelated_connected_peers()
 {
    let requested_peer = crate::PeerId::new("peer-requested");
    let selected_provider = crate::PeerId::new("peer-provider");
    let stale_peer = crate::PeerId::new("peer-stale");
    let connected_peer = crate::PeerId::new("peer-connected");

    let prioritized = crate::prioritized_artifact_source_peers(
        &requested_peer,
        Some(&selected_provider),
        &[stale_peer.clone(), selected_provider.clone()],
        &BTreeSet::from([
            selected_provider.clone(),
            connected_peer.clone(),
            requested_peer.clone(),
        ]),
    );

    assert_eq!(
        prioritized,
        vec![requested_peer, selected_provider, stale_peer]
    );
}

#[test]
fn artifact_sync_resumes_from_persisted_transfer_state_after_restart() {
    let listener_storage = tempdir().expect("listener storage");
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    let payload = b"artifact-transfer-restart-resume".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage.path().to_path_buf()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema-transfer-resume"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr.clone())
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let control = dialer.control_handle();
    let descriptor_from_peer = control
        .fetch_artifact_manifest(
            listener_peer_id.as_str(),
            descriptor.artifact_id.clone(),
            Duration::from_secs(2),
        )
        .expect("fetch artifact manifest")
        .expect("artifact manifest");
    assert_eq!(descriptor_from_peer, descriptor);

    let first_chunk = descriptor
        .chunks
        .first()
        .cloned()
        .expect("artifact should have at least one chunk");
    let first_payload = control
        .fetch_artifact_chunk(
            listener_peer_id.as_str(),
            descriptor.artifact_id.clone(),
            first_chunk.chunk_id.clone(),
            Duration::from_secs(2),
        )
        .expect("fetch first chunk")
        .expect("first chunk payload");
    let dialer_store = dialer.artifact_store().expect("dialer store");
    dialer_store
        .store_chunk_bytes(&first_payload.chunk, &first_payload.bytes)
        .expect("store first chunk");

    let mut transfer_state = ArtifactTransferState::new(descriptor.artifact_id.clone());
    transfer_state.source_peers = vec![listener_peer_id.clone()];
    transfer_state.set_provider(listener_peer_id.clone(), descriptor.clone());
    transfer_state.note_completed_chunk(&first_chunk.chunk_id);
    crate::runtime_support::persist_artifact_transfer_state(
        &dialer_storage_config,
        &transfer_state,
    )
    .expect("persist transfer state");
    assert!(
        fs::read(dialer_storage_config.scoped_transfer_path(&descriptor.artifact_id)).is_ok(),
        "transfer state should be persisted before restart",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("restarted dialer spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot
                    .in_flight_transfers
                    .get(&descriptor.artifact_id)
                    .is_some_and(|state| {
                        state.descriptor.as_ref() == Some(&descriptor)
                            && state.completed_chunks.contains(&first_chunk.chunk_id)
                    })
        },
        "restarted dialer did not restore persisted transfer state",
    );

    let synced = restarted
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect("resume artifact sync");
    assert_eq!(synced, descriptor);

    let restarted_store = restarted.artifact_store().expect("restarted store");
    let materialized = restarted_store
        .materialize_artifact_bytes(&synced)
        .expect("materialize resumed artifact");
    assert_eq!(materialized, payload);

    wait_for(
        Duration::from_secs(5),
        || {
            !restarted_telemetry
                .snapshot()
                .in_flight_transfers
                .contains_key(&descriptor.artifact_id)
        },
        "completed transfer should be cleared from telemetry",
    );
    assert!(
        crate::runtime_support::load_artifact_transfer_state(
            &dialer_storage_config,
            &descriptor.artifact_id,
        )
        .expect("load cleared transfer state")
        .is_none(),
        "completed transfer state should be removed after manifest finalize",
    );

    restarted.shutdown().expect("restarted dialer shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn wait_for_artifact_from_peers_repairs_partial_local_artifact() {
    let _guard = native_swarm_test_guard();
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-partial-artifact-listener-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-partial-artifact-dialer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage_config = StorageConfig::new(dialer_storage.clone());
    let payload = (0..4096)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema-partial"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(128).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let dialer_store = dialer.artifact_store().expect("dialer store");
    dialer_store
        .store_manifest(&descriptor)
        .expect("store partial manifest");
    let first_chunk = descriptor
        .chunks
        .first()
        .cloned()
        .expect("artifact should have at least one chunk");
    let first_payload = dialer
        .control_handle()
        .fetch_artifact_chunk(
            listener_peer_id.as_str(),
            descriptor.artifact_id.clone(),
            first_chunk.chunk_id.clone(),
            Duration::from_secs(2),
        )
        .expect("fetch first chunk")
        .expect("first chunk payload");
    dialer_store
        .store_chunk_bytes(&first_payload.chunk, &first_payload.bytes)
        .expect("store first chunk");

    assert!(dialer_store.has_manifest(&descriptor.artifact_id));
    assert!(
        !dialer_store
            .has_complete_artifact(&descriptor.artifact_id)
            .expect("partial artifact completeness"),
        "partial artifact should not count as locally materialized"
    );

    dialer
        .wait_for_artifact_from_peers(
            std::slice::from_ref(&listener_peer_id),
            &descriptor.artifact_id,
            Duration::from_secs(10),
        )
        .expect("repair partial artifact");

    assert!(
        dialer_store
            .has_complete_artifact(&descriptor.artifact_id)
            .expect("complete artifact after repair"),
        "artifact prewarm should fetch missing chunks instead of stopping at the manifest"
    );
    let materialized = dialer_store
        .materialize_artifact_bytes(&descriptor)
        .expect("materialize repaired artifact");
    assert_eq!(materialized, payload);

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn native_artifact_sync_rejects_unadmitted_peer() {
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-auth-artifact-listener-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-auth-artifact-dialer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let payload = b"artifact-bytes-over-native-sync".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(dialer_storage))
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new().with_admission_policy(crate::AdmissionPolicy {
                network_id: mainnet().genesis.network_id.clone(),
                project_family_id: crate::ProjectFamilyId::new("family-auth"),
                required_release_train_hash: crate::ContentId::new("train-auth"),
                allowed_target_artifact_hashes: BTreeSet::from([crate::ContentId::new(
                    "artifact-native-auth",
                )]),
                trusted_issuers: BTreeMap::new(),
                minimum_revocation_epoch: crate::RevocationEpoch(0),
            }),
        )
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let error = dialer
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect_err("artifact sync should reject unauthenticated peer");
    assert!(
        error
            .to_string()
            .contains("did not publish an auth envelope"),
        "unexpected error: {error}",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}
