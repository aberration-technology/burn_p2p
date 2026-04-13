use super::support::*;

#[test]
fn mainnet_and_experiment_handles_build_overlay_topics() {
    let mainnet = mainnet();
    let experiment = experiment();

    assert_eq!(mainnet.control_topic(), "/burn-p2p/net-1/control");

    let topics = experiment.overlay_topics();
    assert_eq!(
        topics.heads,
        "/burn-p2p/net-1/study/study-1/exp/exp-1/heads"
    );
    assert_eq!(
        topics.telemetry,
        "/burn-p2p/net-1/study/study-1/exp/exp-1/telemetry"
    );
}

#[test]
fn experiment_handle_matches_scoped_objects() {
    let experiment = experiment();

    let receipt = ContributionReceipt {
        receipt_id: crate::ContributionReceiptId::new("receipt-1"),
        peer_id: crate::PeerId::new("peer-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        base_head_id: crate::HeadId::new("head-0"),
        artifact_id: crate::ArtifactId::new("artifact-1"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.5))]),
        merge_cert_id: None,
    };

    let merge = MergeCertificate {
        merge_cert_id: crate::MergeCertId::new("merge-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        base_head_id: crate::HeadId::new("head-0"),
        merged_head_id: crate::HeadId::new("head-1"),
        merged_artifact_id: crate::ArtifactId::new("artifact-merged"),
        policy: crate::MergePolicy::WeightedMean,
        issued_at: Utc::now(),
        promoter_peer_id: crate::PeerId::new("validator-1"),
        promotion_mode: crate::HeadPromotionMode::ValidatorQuorum,
        contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-1")],
    };

    let telemetry = TelemetrySummary {
        network_id: crate::NetworkId::new("net-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        window_id: WindowId(4),
        active_peers: 8,
        accepted_contributions: 12,
        throughput_work_units_per_second: 42.0,
        network: NetworkEstimate {
            connected_peers: 6,
            observed_peers: 9,
            estimated_network_size: 10.5,
            estimated_total_vram_bytes: None,
            estimated_total_flops: None,
            eta_lower_seconds: Some(20),
            eta_upper_seconds: Some(45),
        },
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.4))]),
        captured_at: Utc::now(),
    };

    assert!(experiment.matches_receipt(&receipt));
    assert!(experiment.matches_merge_certificate(&merge));
    assert!(experiment.matches_telemetry(&telemetry));
}

#[test]
fn prepare_keeps_the_old_handle_only_behavior() {
    let node = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .prepare()
        .expect("prepare");

    assert_eq!(node.mainnet().network_id().as_str(), "net-1");
    assert!(node.config().bootstrap_peers.is_empty());
}

#[test]
fn spawn_starts_a_live_runtime_and_shutdown_returns_node() {
    let _guard = native_swarm_test_guard();
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-facade-runtime-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.local_peer_id.is_some()
                && snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "runtime did not reach running state",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.network_id, Some(crate::NetworkId::new("net-1")));
    assert!(snapshot.local_peer_id.is_some());
    assert!(storage_root.join("state").exists());
    assert!(storage_root.join("artifacts/chunks").exists());

    running.shutdown().expect("shutdown");
    let node = running
        .await_termination_timeout(test_timeout(Duration::from_secs(5)))
        .expect("await termination");
    assert_eq!(node.mainnet().network_id().as_str(), "net-1");
}

#[test]
fn persistent_identity_survives_restart() {
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-facade-identity-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("first spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first runtime did not publish a local peer id",
    );
    let first_peer_id = first_telemetry
        .snapshot()
        .local_peer_id
        .expect("first peer id");
    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");

    let second = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root))
        .spawn()
        .expect("second spawn");
    let second_telemetry = second.telemetry();
    wait_for(
        Duration::from_secs(5),
        || second_telemetry.snapshot().local_peer_id.is_some(),
        "second runtime did not publish a local peer id",
    );
    let second_peer_id = second_telemetry
        .snapshot()
        .local_peer_id
        .expect("second peer id");
    second.shutdown().expect("second shutdown");
    let _ = second.await_termination().expect("second termination");

    assert_eq!(first_peer_id, second_peer_id);
}

#[test]
fn discovered_bootstrap_peer_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-discovery-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-discovery-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(validator_storage.clone()))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init genesis");

    let first_trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(trainer_storage.clone()))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("first trainer spawn");
    let first_trainer_telemetry = first_trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_trainer_telemetry.snapshot().connected_peers >= 1,
        "first trainer did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            first_trainer_telemetry
                .snapshot()
                .known_peer_addresses
                .contains(&validator_addr)
        },
        "first trainer did not persist discovered bootstrap address",
    );

    first_trainer.shutdown().expect("first trainer shutdown");
    let _ = first_trainer
        .await_termination()
        .expect("first trainer termination");

    let restarted_trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(trainer_storage))
    .spawn()
    .expect("restarted trainer spawn");
    let restarted_telemetry = restarted_trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().connected_peers >= 1,
        "restarted trainer did not reconnect from persisted peer list",
    );
    assert!(
        restarted_telemetry
            .snapshot()
            .known_peer_addresses
            .contains(&validator_addr)
    );
    let synced = restarted_trainer
        .sync_experiment_head(&experiment)
        .expect("sync canonical head after restart")
        .expect("synced head after restart");
    assert_eq!(synced.head_id, genesis_head.head_id);

    restarted_trainer
        .shutdown()
        .expect("restarted trainer shutdown");
    let _ = restarted_trainer
        .await_termination()
        .expect("restarted trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn peers_fan_out_beyond_bootstrap_seed_and_survive_seed_shutdown() {
    let _guard = native_swarm_test_guard();
    let bootstrap = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_roles(crate::PeerRoleSet::new([
            crate::PeerRole::Bootstrap,
            crate::PeerRole::RelayHelper,
        ]))
        .spawn()
        .expect("bootstrap peer spawn");
    let bootstrap_telemetry = bootstrap.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !bootstrap_telemetry.snapshot().listen_addresses.is_empty(),
        "bootstrap peer did not start listening",
    );
    let bootstrap_addr = bootstrap_telemetry.snapshot().listen_addresses[0].clone();

    let trainer_a = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_bootstrap_peer(bootstrap_addr.clone())
        .spawn()
        .expect("trainer a spawn");
    let trainer_b = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_bootstrap_peer(bootstrap_addr.clone())
        .spawn()
        .expect("trainer b spawn");

    let trainer_a_telemetry = trainer_a.telemetry();
    let trainer_b_telemetry = trainer_b.telemetry();

    wait_for(
        Duration::from_secs(20),
        || bootstrap_telemetry.snapshot().connected_peers >= 2,
        "bootstrap peer did not accept the initial seed connections",
    );
    wait_for(
        Duration::from_secs(20),
        || {
            let snapshot = trainer_a_telemetry.snapshot();
            snapshot.connected_peers >= 2
                && snapshot
                    .known_peer_addresses
                    .iter()
                    .any(|address| address != &bootstrap_addr)
        },
        "trainer a did not fan out beyond the bootstrap seed",
    );
    wait_for(
        Duration::from_secs(20),
        || {
            let snapshot = trainer_b_telemetry.snapshot();
            snapshot.connected_peers >= 2
                && snapshot
                    .known_peer_addresses
                    .iter()
                    .any(|address| address != &bootstrap_addr)
        },
        "trainer b did not fan out beyond the bootstrap seed",
    );

    bootstrap.shutdown().expect("bootstrap peer shutdown");
    let _ = bootstrap
        .await_termination()
        .expect("bootstrap peer termination");

    wait_for(
        Duration::from_secs(30),
        || trainer_a_telemetry.snapshot().connected_peers >= 1,
        "trainer a lost the mesh after bootstrap shutdown",
    );
    wait_for(
        Duration::from_secs(30),
        || trainer_b_telemetry.snapshot().connected_peers >= 1,
        "trainer b lost the mesh after bootstrap shutdown",
    );

    trainer_a.shutdown().expect("trainer a shutdown");
    let _ = trainer_a
        .await_termination()
        .expect("trainer a termination");
    trainer_b.shutdown().expect("trainer b shutdown");
    let _ = trainer_b
        .await_termination()
        .expect("trainer b termination");
}
