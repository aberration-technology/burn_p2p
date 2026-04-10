use super::support::*;

#[test]
fn native_runtime_training_and_validation_progresses_across_peers() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_a_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-a-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_b_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-b-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let late_joiner_storage = std::env::temp_dir().join(format!(
        "burn-p2p-late-joiner-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let experiment = experiment();
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
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "validator runtime did not start",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init genesis head");
    assert_eq!(genesis_head.global_step, 0);

    let trainer_a = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_a_storage))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("trainer a spawn");
    let trainer_b = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.5,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_b_storage))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("trainer b spawn");

    let trainer_a_telemetry = trainer_a.telemetry();
    let trainer_b_telemetry = trainer_b.telemetry();
    wait_for(
        Duration::from_secs(5),
        || validator_telemetry.snapshot().connected_peers >= 2,
        "validator did not connect to trainers",
    );
    wait_for(
        Duration::from_secs(5),
        || trainer_a_telemetry.snapshot().connected_peers >= 1,
        "trainer a did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || trainer_b_telemetry.snapshot().connected_peers >= 1,
        "trainer b did not connect",
    );

    for trainer in [&trainer_a, &trainer_b] {
        wait_for(
            Duration::from_secs(10),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("sync trainer head")
                    .is_some()
            },
            "trainer did not sync the canonical genesis head",
        );
    }

    let mut trainer_a = trainer_a;
    let mut trainer_b = trainer_b;
    let trainer_a_window = trainer_a
        .train_window_once(&experiment)
        .expect("trainer a window");
    let trainer_b_window = trainer_b
        .train_window_once(&experiment)
        .expect("trainer b window");

    assert_eq!(
        trainer_a_window.head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    assert_eq!(
        trainer_b_window.head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    let trainer_a_state = trainer_a.telemetry().snapshot();
    assert_eq!(
        trainer_a_state.node_state,
        crate::NodeRuntimeState::WaitingMerge
    );
    assert_eq!(
        trainer_a_state.slot_states.first(),
        Some(&crate::SlotRuntimeState::CoolingDown(
            crate::SlotAssignmentState::from_experiment(&experiment)
        ))
    );
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.control_plane.update_announcements.len() >= 2
                && !snapshot.control_plane.merge_window_announcements.is_empty()
                && snapshot
                    .control_plane
                    .reducer_assignment_announcements
                    .len()
                    >= 2
        },
        "validator did not observe live merge topology announcements",
    );
    let topology_snapshot = validator_telemetry.snapshot();
    assert!(
        topology_snapshot
            .control_plane
            .update_announcements
            .iter()
            .all(|announcement| announcement.update.base_head_id == genesis_head.head_id)
    );

    let validation = validator
        .validate_candidates_once(&experiment)
        .expect("validate candidates")
        .expect("validation outcome");
    let validator_state = validator.telemetry().snapshot();
    assert_eq!(
        validator_state.node_state,
        crate::NodeRuntimeState::IdleReady
    );
    assert_eq!(
        validator_state.slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            crate::SlotAssignmentState::from_experiment(&experiment)
        ))
    );
    assert_eq!(validation.merged_head.global_step, 1);
    assert_ne!(
        validation.merged_head.head_id,
        trainer_a_window.head.head_id
    );
    assert_ne!(
        validation.merged_head.head_id,
        trainer_b_window.head.head_id
    );
    assert_eq!(
        validation.merged_head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    let expected_model = {
        let trainer_a_model = metric_float(&trainer_a_window.report.stats, "model");
        let trainer_b_model = metric_float(&trainer_b_window.report.stats, "model");
        let trainer_a_quality =
            1.0 / (1.0 + metric_float(&trainer_a_window.report.stats, "loss").abs());
        let trainer_b_quality =
            1.0 / (1.0 + metric_float(&trainer_b_window.report.stats, "loss").abs());
        ((trainer_a_model * trainer_a_quality) + (trainer_b_model * trainer_b_quality))
            / (trainer_a_quality + trainer_b_quality)
    };
    let merged_model = metric_float(&validation.merged_head.metrics, "model");
    assert!(
        (merged_model - expected_model).abs() < 1e-9,
        "expected merged model {expected_model}, got {merged_model}"
    );
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            !snapshot
                .control_plane
                .aggregate_proposal_announcements
                .is_empty()
                && !snapshot
                    .control_plane
                    .reduction_certificate_announcements
                    .is_empty()
                && !snapshot.control_plane.reducer_load_announcements.is_empty()
        },
        "validator did not publish aggregate and reduction metadata",
    );
    let topology_snapshot = validator_telemetry.snapshot();
    let aggregate = topology_snapshot
        .control_plane
        .aggregate_proposal_announcements
        .last()
        .expect("aggregate proposal announcement")
        .proposal
        .clone();
    assert_eq!(aggregate.base_head_id, genesis_head.head_id);
    assert!(aggregate.stats.accepted_updates >= 1);
    assert_ne!(
        aggregate.aggregate_artifact_id,
        validation.merged_head.artifact_id
    );
    let validator_store = validator.artifact_store().expect("validator store");
    assert!(validator_store.has_manifest(&aggregate.aggregate_artifact_id));

    let late_joiner = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(late_joiner_storage))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("late joiner spawn");
    let late_joiner_telemetry = late_joiner.telemetry();
    wait_for(
        Duration::from_secs(5),
        || late_joiner_telemetry.snapshot().connected_peers >= 1,
        "late joiner did not connect",
    );

    let late_joiner = late_joiner;
    wait_for(
        Duration::from_secs(30),
        || {
            late_joiner
                .sync_experiment_head(&experiment)
                .expect("sync experiment head")
                .is_some()
        },
        "late joiner did not sync the promoted canonical head",
    );
    let synced_head = late_joiner
        .sync_experiment_head(&experiment)
        .expect("sync experiment head")
        .expect("synced head");
    assert_eq!(synced_head.head_id, validation.merged_head.head_id);

    let mut late_joiner = late_joiner;
    let late_window = late_joiner
        .train_window_once(&experiment)
        .expect("late joiner window");
    assert_eq!(
        late_window.head.parent_head_id,
        Some(validation.merged_head.head_id.clone())
    );

    late_joiner.shutdown().expect("late joiner shutdown");
    let _ = late_joiner
        .await_termination()
        .expect("late joiner termination");
    trainer_b.shutdown().expect("trainer b shutdown");
    let _ = trainer_b
        .await_termination()
        .expect("trainer b termination");
    trainer_a.shutdown().expect("trainer a shutdown");
    let _ = trainer_a
        .await_termination()
        .expect("trainer a termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn training_uses_revision_lag_policy_from_directory_entry() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-lag-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-lag-trainer-{}",
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
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "validator runtime did not start",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let experiment = experiment();
    let mut lag_directory_entry = ExperimentDirectoryEntry {
        network_id: mainnet().genesis.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        workload_id: crate::WorkloadId::new("synthetic-workload"),
        display_name: "Synthetic".into(),
        model_schema_hash: crate::ContentId::new("model-synthetic"),
        dataset_view_id: crate::DatasetViewId::new("view-synthetic"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([crate::PeerRole::TrainerCpu]),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 4096,
            estimated_window_seconds: 30,
        },
        visibility: crate::ExperimentVisibility::Public,
        opt_in_policy: crate::ExperimentOptInPolicy::Open,
        current_revision_id: experiment.revision_id.clone(),
        current_head_id: None,
        allowed_roles: crate::PeerRoleSet::default_trainer(),
        allowed_scopes: BTreeSet::from([crate::ExperimentScope::Train {
            experiment_id: experiment.experiment_id.clone(),
        }]),
        metadata: BTreeMap::new(),
    };
    lag_directory_entry.apply_revision_policy(&crate::RevisionManifest {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        workload_id: lag_directory_entry.workload_id.clone(),
        required_release_train_hash: crate::ContentId::new("train-synthetic"),
        model_schema_hash: lag_directory_entry.model_schema_hash.clone(),
        checkpoint_format_hash: crate::ContentId::new("checkpoint-synthetic"),
        dataset_view_id: lag_directory_entry.dataset_view_id.clone(),
        training_config_hash: crate::ContentId::new("training-synthetic"),
        merge_topology_policy_hash: crate::ContentId::new("topology-synthetic"),
        slot_requirements: lag_directory_entry.resource_requirements.clone(),
        activation_window: crate::WindowActivation {
            activation_window: crate::WindowId(1),
            grace_windows: 0,
        },
        lag_policy: crate::LagPolicy {
            max_head_lag_before_catchup: 1,
            max_head_lag_before_block: 2,
            max_head_lag_before_full_rebase: 8,
            max_window_skew_before_lease_revoke: 1,
        },
        merge_window_miss_policy: crate::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: false,
        browser_role_policy: crate::BrowserRolePolicy::default(),
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: crate::BrowserVisibilityPolicy::Hidden,
        description: "synthetic lag policy".into(),
    });

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init validator genesis head");
    let lagged_head = HeadDescriptor {
        head_id: crate::HeadId::new("validator-lagged-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: genesis_head.artifact_id.clone(),
        parent_head_id: Some(genesis_head.head_id.clone()),
        global_step: 3,
        created_at: Utc::now(),
        metrics: genesis_head.metrics.clone(),
    };
    crate::runtime_support::persist_head_state(
        &StorageConfig::new(validator_storage.clone()),
        &experiment,
        &lagged_head,
    )
    .expect("persist lagged head state");
    crate::runtime_support::persist_json(
        StorageConfig::new(validator_storage.clone()).scoped_head_path(&lagged_head.head_id),
        &lagged_head,
    )
    .expect("persist lagged head descriptor");
    let restored_lagged_head = validator
        .restore_experiment_head(&experiment)
        .expect("restore lagged head")
        .expect("restored lagged head");
    assert_eq!(restored_lagged_head.global_step, 3);

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_storage))
    .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![lag_directory_entry]))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );

    let mut trainer = trainer;
    let error = trainer
        .train_window_once(&experiment)
        .expect_err("training should block on excessive lag");
    assert!(
        error
            .to_string()
            .contains("training blocked: local node is 3 head steps behind")
    );

    let snapshot = trainer_telemetry.snapshot();
    assert_eq!(snapshot.lag_state, crate::LagState::LeaseBlocked);
    assert_eq!(snapshot.head_lag_steps, 3);
    assert_eq!(
        snapshot.lag_policy,
        crate::LagPolicy {
            max_head_lag_before_catchup: 1,
            max_head_lag_before_block: 2,
            max_head_lag_before_full_rebase: 8,
            max_window_skew_before_lease_revoke: 1,
        }
    );
    match snapshot
        .slot_states
        .first()
        .expect("slot state should exist")
    {
        crate::SlotRuntimeState::Blocked { assignment, reason } => {
            assert_eq!(
                assignment.as_ref(),
                Some(&crate::SlotAssignmentState::from_experiment(&experiment))
            );
            assert!(reason.contains("3 head steps behind"));
        }
        other => panic!("expected blocked slot state, got {other:?}"),
    }

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn validator_restart_restores_canonical_head_for_late_joiners() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-validator-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let late_joiner_storage = std::env::temp_dir().join(format!(
        "burn-p2p-late-joiner-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let experiment = experiment();
    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
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
    validator
        .initialize_local_head(&experiment)
        .expect("init genesis");

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_storage))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );
    let mut trainer = trainer;
    let trained = trainer
        .train_window_once(&experiment)
        .expect("trainer window");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot
                .control_plane
                .head_announcements
                .iter()
                .any(|announcement| announcement.head.head_id == trained.head.head_id)
                && snapshot
                    .control_plane
                    .update_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.update.delta_artifact_id == trained.artifact.artifact_id
                    })
        },
        "validator did not observe trainer head/update before validation",
    );
    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");
    assert_ne!(validated.merged_head.head_id, trained.head.head_id);
    assert_eq!(
        validated.merged_head.parent_head_id,
        trained.head.parent_head_id
    );
    assert!(
        (metric_float(&validated.merged_head.metrics, "model")
            - metric_float(&trained.report.stats, "model"))
        .abs()
            < 1e-9
    );

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");

    let restarted_validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(validator_storage))
    .spawn()
    .expect("restart validator");
    let restarted_telemetry = restarted_validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !restarted_telemetry.snapshot().listen_addresses.is_empty(),
        "restarted validator did not start listening",
    );
    let restarted_addr = restarted_telemetry.snapshot().listen_addresses[0].clone();
    let restarted_peer_id = restarted_telemetry
        .snapshot()
        .local_peer_id
        .clone()
        .expect("restarted peer id");
    assert_eq!(
        restarted_peer_id,
        validator_telemetry
            .snapshot()
            .local_peer_id
            .expect("original validator peer id")
    );

    restarted_validator
        .restore_experiment_head(&experiment)
        .expect("restore canonical head")
        .expect("restored head");

    let late_joiner = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(late_joiner_storage))
    .with_bootstrap_peer(restarted_addr)
    .spawn()
    .expect("late joiner spawn");
    let late_joiner_telemetry = late_joiner.telemetry();
    wait_for(
        Duration::from_secs(5),
        || late_joiner_telemetry.snapshot().connected_peers >= 1,
        "late joiner did not connect to restarted validator",
    );

    let synced = {
        let deadline = Instant::now() + test_timeout(Duration::from_secs(5));
        loop {
            if let Some(head) = late_joiner
                .sync_experiment_head(&experiment)
                .expect("sync head after restart")
            {
                break head;
            }
            assert!(Instant::now() < deadline, "synced head after restart");
            thread::sleep(Duration::from_millis(25));
        }
    };
    assert_eq!(synced.head_id, validated.merged_head.head_id);

    late_joiner.shutdown().expect("late joiner shutdown");
    let _ = late_joiner
        .await_termination()
        .expect("late joiner termination");
    restarted_validator
        .shutdown()
        .expect("restarted validator shutdown");
    let _ = restarted_validator
        .await_termination()
        .expect("restarted validator termination");
}

#[test]
fn validator_can_fan_in_many_native_trainers_in_one_round() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-fanin-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
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

    let learning_rates = [0.2, 0.4, 0.6, 0.8, 1.0, 0.3];
    let mut trainers = Vec::new();
    for (index, learning_rate) in learning_rates.iter().enumerate() {
        let trainer = NodeBuilder::new(SyntheticRuntimeProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate: *learning_rate,
            target_model: 10.0,
        })
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
            "burn-p2p-fanin-trainer-{index}-{}",
            Utc::now().timestamp_nanos_opt().expect("nanos")
        ))))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()
        .expect("trainer spawn");
        trainers.push(trainer);
    }

    wait_for(
        Duration::from_secs(5),
        || validator_telemetry.snapshot().connected_peers >= learning_rates.len(),
        "validator did not connect to all trainers",
    );
    for trainer in &trainers {
        wait_for(
            Duration::from_secs(10),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("sync trainer head")
                    .is_some()
            },
            "trainer did not sync the canonical genesis head",
        );
    }

    let mut trainer_outcomes = Vec::new();
    for trainer in &mut trainers {
        let outcome = trainer
            .train_window_once(&experiment)
            .expect("trainer window");
        trainer_outcomes.push(outcome);
    }
    assert_ne!(
        trainer_outcomes[0].lease.microshards, trainer_outcomes[1].lease.microshards,
        "same-window trainers should avoid already-announced shard assignments when alternatives exist",
    );

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.control_plane.update_announcements.len() >= learning_rates.len()
                && !snapshot.control_plane.merge_window_announcements.is_empty()
                && snapshot
                    .control_plane
                    .reducer_assignment_announcements
                    .len()
                    >= learning_rates.len()
        },
        "validator did not observe all trainer updates for the merge window",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            validator_telemetry
                .snapshot()
                .control_plane
                .head_announcements
                .len()
                > learning_rates.len()
        },
        "validator did not observe all trainer head announcements for the merge window",
    );

    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");
    assert_eq!(
        validated.merged_head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    assert!(
        trainer_outcomes
            .iter()
            .all(|outcome| outcome.head.head_id != validated.merged_head.head_id)
    );
    let expected_model = {
        let (weighted_sum, total_weight) = trainer_outcomes.iter().fold(
            (0.0_f64, 0.0_f64),
            |(weighted_sum, total_weight), outcome| {
                let model = metric_float(&outcome.report.stats, "model");
                let quality = 1.0 / (1.0 + metric_float(&outcome.report.stats, "loss").abs());
                (weighted_sum + (model * quality), total_weight + quality)
            },
        );
        weighted_sum / total_weight
    };
    let merged_model = metric_float(&validated.merged_head.metrics, "model");
    assert!(
        (merged_model - expected_model).abs() < 1e-9,
        "expected merged model {expected_model}, got {merged_model}"
    );

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn same_window_training_leases_are_fair_share_capped() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-fair-share-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut trainers = Vec::new();
    for index in 0..3 {
        trainers.push(
            NodeBuilder::new(SyntheticRuntimeProject {
                dataset_root: dataset_dir.path().to_path_buf(),
                learning_rate: 0.25 + (index as f64 * 0.1),
                target_model: 10.0,
            })
            .with_mainnet(mainnet().genesis.clone())
            .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
                "burn-p2p-fair-share-trainer-{index}-{}",
                Utc::now().timestamp_nanos_opt().expect("nanos")
            ))))
            .with_bootstrap_peer(validator_addr.clone())
            .spawn()
            .expect("trainer spawn"),
        );
    }

    wait_for(
        Duration::from_secs(5),
        || validator_telemetry.snapshot().connected_peers >= trainers.len(),
        "validator did not connect to all trainers",
    );

    let mut lease_lengths = Vec::new();
    for trainer in &mut trainers {
        let outcome = trainer
            .train_window_once(&experiment)
            .expect("trainer window");
        lease_lengths.push(outcome.lease.microshards.len());
    }

    assert_eq!(lease_lengths, vec![1, 1, 1]);

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    let validator = validator;
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn training_rejects_experiment_minimum_role_mismatch() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();
    let mut entry = runtime_directory_entry(&experiment);
    entry.resource_requirements.minimum_roles = BTreeSet::from([crate::PeerRole::TrainerCpu]);

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![entry]))
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-placement-role-mismatch-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
    .spawn()
    .expect("trainer spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "trainer runtime did not start",
    );

    let mut running = running;
    let error = running
        .train_window_once(&experiment)
        .expect_err("training should reject role mismatch");
    assert!(
        error
            .to_string()
            .contains("training placement rejected: local roles"),
        "unexpected error: {error}",
    );

    running.shutdown().expect("trainer shutdown");
    let _ = running.await_termination().expect("trainer termination");
}

#[test]
fn training_rejects_insufficient_throughput_for_estimated_window() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-placement-throughput-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 0.25,
        benchmark_target_window_seconds: 60,
        simulated_window_seconds: 1,
        microshard_count: 8,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    create_adaptive_runtime_dataset(&project);
    let experiment = experiment();
    let mut entry = runtime_directory_entry(&experiment);
    entry.resource_requirements.estimated_window_seconds = 1;

    let running = NodeBuilder::new(project)
        .with_mainnet(mainnet().genesis.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![entry]))
        .with_storage(StorageConfig::new(storage_root))
        .spawn()
        .expect("adaptive trainer spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "adaptive runtime did not start",
    );

    let mut running = running;
    let error = running
        .train_window_once(&experiment)
        .expect_err("training should reject insufficient throughput");
    assert!(
        error
            .to_string()
            .contains("cannot satisfy estimated window 1s"),
        "unexpected error: {error}",
    );

    running.shutdown().expect("adaptive shutdown");
    let _ = running.await_termination().expect("adaptive termination");
}
