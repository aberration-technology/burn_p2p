use super::support::*;

#[test]
fn validator_quorum_two_emits_one_merge_promotion_and_one_aggregate_proposal() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();
    let validator_a_addr = loopback_listen_address();

    let validator_a = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Validator]))
    .with_listen_address(validator_a_addr.clone())
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-quorum-validator-a-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
    .spawn()
    .expect("validator a spawn");
    let validator_a_telemetry = validator_a.telemetry();

    let validator_b = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Validator]))
    .with_listen_address(loopback_listen_address())
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-quorum-validator-b-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
    .with_bootstrap_peer(validator_a_addr.clone())
    .spawn()
    .expect("validator b spawn");
    let validator_b_telemetry = validator_b.telemetry();

    wait_for(
        Duration::from_secs(5),
        || validator_a_telemetry.snapshot().connected_peers >= 1,
        "validator a did not connect to validator b",
    );
    wait_for(
        Duration::from_secs(5),
        || validator_b_telemetry.snapshot().connected_peers >= 1,
        "validator b did not connect to validator a",
    );

    let mut validator_a = validator_a;
    let mut validator_b = validator_b;
    let genesis_head = validator_a
        .initialize_local_head(&experiment)
        .expect("init genesis");
    wait_for(
        Duration::from_secs(10),
        || {
            validator_b
                .sync_experiment_head(&experiment)
                .expect("validator b sync")
                .is_some()
        },
        "validator b did not sync genesis head",
    );

    let mut trainers = Vec::new();
    for (index, learning_rate) in [0.25, 0.75].into_iter().enumerate() {
        let trainer = NodeBuilder::new(SyntheticRuntimeProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate,
            target_model: 10.0,
        })
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(loopback_listen_address())
        .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
            "burn-p2p-quorum-trainer-{index}-{}",
            Utc::now().timestamp_nanos_opt().expect("nanos")
        ))))
        .with_bootstrap_peer(validator_a_addr.clone())
        .spawn()
        .expect("trainer spawn");
        trainers.push(trainer);
    }

    wait_for(
        Duration::from_secs(5),
        || validator_a_telemetry.snapshot().connected_peers >= 3,
        "validator a did not connect to validator b and both trainers",
    );
    for trainer in &trainers {
        wait_for(
            Duration::from_secs(10),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("trainer sync")
                    .is_some()
            },
            "trainer did not sync genesis head",
        );
    }

    let mut trainer_outcomes = Vec::new();
    for trainer in &mut trainers {
        trainer_outcomes.push(
            trainer
                .train_window_once(&experiment)
                .expect("trainer training window"),
        );
    }

    let validator_a_peer_id = validator_a_telemetry
        .snapshot()
        .local_peer_id
        .expect("validator a peer id");
    for outcome in &trainer_outcomes {
        wait_for(
            Duration::from_secs(5),
            || {
                validator_a
                    .sync_artifact_from_peer(
                        &outcome.contribution.peer_id,
                        outcome.head.artifact_id.clone(),
                    )
                    .is_ok()
            },
            "validator a did not warm the trainer artifact from the live network",
        );
        validator_a
            .publish_artifact_from_store(&outcome.head.artifact_id)
            .expect("validator a republish trainer artifact");
        wait_for(
            Duration::from_secs(5),
            || {
                validator_b
                    .sync_artifact_from_peer(&validator_a_peer_id, outcome.head.artifact_id.clone())
                    .is_ok()
            },
            "validator b did not warm the trainer artifact from validator a",
        );
    }

    let outcome_a = validator_a
        .validate_candidates_once(&experiment)
        .expect("validator a validate");
    wait_for(
        Duration::from_secs(5),
        || {
            let a = validator_a_telemetry.snapshot();
            a.control_plane.aggregate_proposal_announcements.len() == 1
                && a.control_plane.reduction_certificate_announcements.len() == 1
                && a.control_plane.validation_quorum_announcements.is_empty()
                && a.control_plane.merge_announcements.is_empty()
        },
        "validator a did not publish the aggregate proposal before quorum and merge promotion",
    );
    let outcome_b = validator_b
        .validate_candidates_once(&experiment)
        .expect("validator b validate");
    let promoted_results = vec![outcome_a, outcome_b]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(
        promoted_results.len() <= 1,
        "at most one validator should report itself as the promotion winner",
    );

    let convergence_deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        let a = validator_a_telemetry.snapshot();
        let b = validator_b_telemetry.snapshot();
        let observed_attesters = a
            .control_plane
            .reduction_certificate_announcements
            .iter()
            .chain(b.control_plane.reduction_certificate_announcements.iter())
            .map(|announcement| announcement.certificate.promoter_peer_id.clone())
            .collect::<BTreeSet<_>>();
        if observed_attesters.len() >= 2
            && a.control_plane.merge_announcements.len() == 1
            && b.control_plane.merge_announcements.len() == 1
            && a.control_plane.aggregate_proposal_announcements.len() == 1
            && b.control_plane.aggregate_proposal_announcements.len() == 1
            && a.control_plane.validation_quorum_announcements.len() == 1
            && b.control_plane.validation_quorum_announcements.len() == 1
        {
            break;
        }
        assert!(
            Instant::now() < convergence_deadline,
            "validators did not converge: attesters={:?}; a(reduction={}, aggregate={}, quorum={}, merge={}) b(reduction={}, aggregate={}, quorum={}, merge={})",
            observed_attesters
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            a.control_plane.reduction_certificate_announcements.len(),
            a.control_plane.aggregate_proposal_announcements.len(),
            a.control_plane.validation_quorum_announcements.len(),
            a.control_plane.merge_announcements.len(),
            b.control_plane.reduction_certificate_announcements.len(),
            b.control_plane.aggregate_proposal_announcements.len(),
            b.control_plane.validation_quorum_announcements.len(),
            b.control_plane.merge_announcements.len(),
        );
        thread::sleep(Duration::from_millis(25));
    }
    let quorum_certificate = validator_a_telemetry
        .snapshot()
        .control_plane
        .validation_quorum_announcements
        .last()
        .expect("validation quorum")
        .certificate
        .clone();
    let promoted_merge = validator_a_telemetry
        .snapshot()
        .control_plane
        .merge_announcements
        .last()
        .expect("promoted merge")
        .certificate
        .clone();
    assert_eq!(promoted_merge.base_head_id, genesis_head.head_id);
    assert_eq!(
        quorum_certificate.merged_head_id,
        promoted_merge.merged_head_id
    );
    assert_eq!(quorum_certificate.attesting_validators.len(), 2);

    let sync_deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        let synced_a = validator_a
            .sync_experiment_head(&experiment)
            .expect("validator a sync");
        let synced_b = validator_b
            .sync_experiment_head(&experiment)
            .expect("validator b sync");
        if let (Some(a), Some(b)) = (synced_a, synced_b)
            && a.head_id == promoted_merge.merged_head_id
            && b.head_id == promoted_merge.merged_head_id
        {
            break;
        }
        assert!(
            Instant::now() < sync_deadline,
            "validators did not sync the promoted merge head before timeout"
        );
        thread::sleep(Duration::from_millis(25));
    }

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    validator_b.shutdown().expect("validator b shutdown");
    let _ = validator_b
        .await_termination()
        .expect("validator b termination");
    validator_a.shutdown().expect("validator a shutdown");
    let _ = validator_a
        .await_termination()
        .expect("validator a termination");
}

#[test]
fn adopt_known_head_if_present_promotes_materialized_head() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let leader_storage = std::env::temp_dir().join(format!(
        "burn-p2p-adopt-head-leader-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let follower_storage = std::env::temp_dir().join(format!(
        "burn-p2p-adopt-head-follower-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let leader = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_listen_address(loopback_listen_address())
    .with_storage(StorageConfig::new(leader_storage))
    .spawn()
    .expect("leader spawn");
    let leader_telemetry = leader.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = leader_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.local_peer_id.is_some()
        },
        "leader did not start",
    );
    let leader_addr = leader
        .config()
        .listen_addresses
        .first()
        .expect("leader listen address")
        .clone();
    let leader_peer_id = leader_telemetry
        .snapshot()
        .local_peer_id
        .expect("leader peer id");

    let experiment = experiment();
    let mut leader = leader;
    let _ = leader
        .initialize_local_head(&experiment)
        .expect("leader genesis head");

    let follower = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_listen_address(loopback_listen_address())
    .with_storage(StorageConfig::new(follower_storage.clone()))
    .with_bootstrap_peer(leader_addr)
    .spawn()
    .expect("follower spawn");
    let follower_telemetry = follower.telemetry();
    wait_for(
        Duration::from_secs(5),
        || follower_telemetry.snapshot().connected_peers >= 1,
        "follower did not connect",
    );

    let follower = follower;
    wait_for(
        Duration::from_secs(10),
        || {
            follower
                .sync_experiment_head(&experiment)
                .expect("follower sync genesis")
                .is_some()
        },
        "follower did not sync genesis head",
    );

    let promoted = leader
        .train_window_once(&experiment)
        .expect("leader training window");
    wait_for(
        Duration::from_secs(10),
        || {
            follower
                .sync_artifact_from_peer(&leader_peer_id, promoted.head.artifact_id.clone())
                .is_ok()
        },
        "follower did not fetch promoted head artifact",
    );

    let adopted = follower
        .adopt_known_head_if_present(&experiment, &promoted.head)
        .expect("adopt known head");
    assert!(adopted, "follower should adopt the materialized head");

    let persisted = crate::runtime_support::load_head_state(
        &StorageConfig::new(follower_storage.clone()),
        &experiment,
    )
    .expect("load follower head state")
    .expect("follower persisted adopted head");
    assert_eq!(persisted.head_id, promoted.head.head_id);
    assert_eq!(persisted.artifact_id, promoted.head.artifact_id);

    let restored = follower
        .restore_experiment_head(&experiment)
        .expect("restore adopted head")
        .expect("restored adopted head");
    assert_eq!(restored.head_id, promoted.head.head_id);

    follower.shutdown().expect("follower shutdown");
    let _ = follower.await_termination().expect("follower termination");
    leader.shutdown().expect("leader shutdown");
    let _ = leader.await_termination().expect("leader termination");
}

#[test]
fn dedicated_reducer_publishes_proposal_and_validators_only_attest_and_promote() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();
    let validator_a_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-dedicated-validator-a-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let reducer_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-dedicated-reducer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let validator_b_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-dedicated-validator-b-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let validator_a_addr = loopback_listen_address();

    let validator_a = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Validator]))
    .with_listen_address(validator_a_addr.clone())
    .with_storage(validator_a_storage.clone())
    .spawn()
    .expect("validator a spawn");
    let validator_a_telemetry = validator_a.telemetry();

    let reducer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Reducer]))
    .with_listen_address(loopback_listen_address())
    .with_storage(reducer_storage)
    .with_bootstrap_peer(validator_a_addr.clone())
    .spawn()
    .expect("reducer spawn");
    let reducer_telemetry = reducer.telemetry();

    let validator_b = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Validator]))
    .with_listen_address(loopback_listen_address())
    .with_storage(validator_b_storage.clone())
    .with_bootstrap_peer(validator_a_addr.clone())
    .spawn()
    .expect("validator b spawn");
    let validator_b_telemetry = validator_b.telemetry();

    wait_for(
        Duration::from_secs(5),
        || validator_a_telemetry.snapshot().connected_peers >= 2,
        "validator a did not connect to reducer and validator b",
    );
    wait_for(
        Duration::from_secs(5),
        || reducer_telemetry.snapshot().connected_peers >= 1,
        "reducer did not connect to validator a",
    );
    wait_for(
        Duration::from_secs(5),
        || validator_b_telemetry.snapshot().connected_peers >= 1,
        "validator b did not connect to validator a",
    );

    let mut validator_a = validator_a;
    let mut reducer = reducer;
    let mut validator_b = validator_b;
    let genesis_head = validator_a
        .initialize_local_head(&experiment)
        .expect("init genesis");
    wait_for(
        Duration::from_secs(10),
        || {
            reducer
                .sync_experiment_head(&experiment)
                .expect("reducer sync")
                .is_some()
                && validator_b
                    .sync_experiment_head(&experiment)
                    .expect("validator b sync")
                    .is_some()
        },
        "reducer and validator b did not sync genesis head",
    );

    let mut trainers = Vec::new();
    for (index, learning_rate) in [0.25, 0.75].into_iter().enumerate() {
        let trainer = NodeBuilder::new(SyntheticRuntimeProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate,
            target_model: 10.0,
        })
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(loopback_listen_address())
        .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
            "burn-p2p-dedicated-trainer-{index}-{}",
            Utc::now().timestamp_nanos_opt().expect("nanos")
        ))))
        .with_bootstrap_peer(validator_a_addr.clone())
        .spawn()
        .expect("trainer spawn");
        trainers.push(trainer);
    }

    wait_for(
        Duration::from_secs(5),
        || validator_a_telemetry.snapshot().connected_peers >= 4,
        "validator a did not connect to reducer, validator b, and both trainers",
    );
    for trainer in &trainers {
        wait_for(
            Duration::from_secs(10),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("trainer sync")
                    .is_some()
            },
            "trainer did not sync genesis head",
        );
    }

    let mut trainer_outcomes = Vec::new();
    for trainer in &mut trainers {
        trainer_outcomes.push(
            trainer
                .train_window_once(&experiment)
                .expect("trainer training window"),
        );
    }

    let validator_a_peer_id = validator_a_telemetry
        .snapshot()
        .local_peer_id
        .expect("validator a peer id");
    let reducer_peer_id = reducer_telemetry
        .snapshot()
        .local_peer_id
        .expect("reducer peer id");
    for outcome in &trainer_outcomes {
        wait_for(
            Duration::from_secs(5),
            || {
                reducer
                    .sync_artifact_from_peer(
                        &outcome.contribution.peer_id,
                        outcome.head.artifact_id.clone(),
                    )
                    .is_ok()
            },
            "reducer did not warm the trainer artifact from the live network",
        );
        reducer
            .publish_artifact_from_store(&outcome.head.artifact_id)
            .expect("reducer republish trainer artifact");
        wait_for(
            Duration::from_secs(5),
            || {
                validator_a
                    .sync_artifact_from_peer(&reducer_peer_id, outcome.head.artifact_id.clone())
                    .is_ok()
            },
            "validator a did not warm the trainer artifact from the reducer",
        );
        validator_a
            .publish_artifact_from_store(&outcome.head.artifact_id)
            .expect("validator a republish trainer artifact");
        wait_for(
            Duration::from_secs(5),
            || {
                validator_b
                    .sync_artifact_from_peer(&validator_a_peer_id, outcome.head.artifact_id.clone())
                    .is_ok()
            },
            "validator b did not warm the trainer artifact from validator a",
        );
    }

    let reduced = reducer
        .reduce_candidates_once(&experiment)
        .expect("reducer aggregate proposal")
        .expect("reducer outcome");
    assert_eq!(reduced.aggregate.reducer_peer_id, reducer_peer_id);

    wait_for(
        Duration::from_secs(5),
        || {
            let a = validator_a_telemetry.snapshot();
            let b = validator_b_telemetry.snapshot();
            a.control_plane.aggregate_proposal_announcements.len() == 1
                && b.control_plane.aggregate_proposal_announcements.len() == 1
                && a.control_plane.aggregate_proposal_announcements[0]
                    .proposal
                    .reducer_peer_id
                    == reducer_peer_id
                && b.control_plane.aggregate_proposal_announcements[0]
                    .proposal
                    .reducer_peer_id
                    == reducer_peer_id
        },
        "validators did not observe the dedicated reducer proposal",
    );

    let outcome_a = validator_a
        .validate_candidates_once(&experiment)
        .expect("validator a validate");
    let outcome_b = validator_b
        .validate_candidates_once(&experiment)
        .expect("validator b validate");
    let promoted_results = vec![outcome_a, outcome_b]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(
        promoted_results.len() <= 1,
        "at most one validator should report itself as the promotion winner",
    );

    let convergence_deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        let a = validator_a_telemetry.snapshot();
        let b = validator_b_telemetry.snapshot();
        let observed_attesters = a
            .control_plane
            .reduction_certificate_announcements
            .iter()
            .chain(b.control_plane.reduction_certificate_announcements.iter())
            .map(|announcement| announcement.certificate.promoter_peer_id.clone())
            .collect::<BTreeSet<_>>();
        if observed_attesters.len() >= 2
            && a.control_plane.merge_announcements.len() == 1
            && b.control_plane.merge_announcements.len() == 1
            && a.control_plane.aggregate_proposal_announcements.len() == 1
            && b.control_plane.aggregate_proposal_announcements.len() == 1
            && a.control_plane.validation_quorum_announcements.len() == 1
            && b.control_plane.validation_quorum_announcements.len() == 1
        {
            break;
        }
        assert!(
            Instant::now() < convergence_deadline,
            "reducer/validator split did not converge: attesters={:?}; a(reduction={}, aggregate={}, quorum={}, merge={}) b(reduction={}, aggregate={}, quorum={}, merge={})",
            observed_attesters
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            a.control_plane.reduction_certificate_announcements.len(),
            a.control_plane.aggregate_proposal_announcements.len(),
            a.control_plane.validation_quorum_announcements.len(),
            a.control_plane.merge_announcements.len(),
            b.control_plane.reduction_certificate_announcements.len(),
            b.control_plane.aggregate_proposal_announcements.len(),
            b.control_plane.validation_quorum_announcements.len(),
            b.control_plane.merge_announcements.len(),
        );
        thread::sleep(Duration::from_millis(25));
    }

    for snapshot in [
        validator_a_telemetry.snapshot(),
        validator_b_telemetry.snapshot(),
        reducer_telemetry.snapshot(),
    ] {
        let reducer_load_peers = snapshot
            .control_plane
            .reducer_load_announcements
            .iter()
            .map(|announcement| announcement.report.peer_id.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            snapshot
                .control_plane
                .aggregate_proposal_announcements
                .len(),
            1
        );
        assert_eq!(
            snapshot.control_plane.aggregate_proposal_announcements[0]
                .proposal
                .reducer_peer_id,
            reducer_peer_id
        );
        assert!(
            snapshot
                .control_plane
                .reducer_load_announcements
                .iter()
                .all(|announcement| announcement.report.peer_id == reducer_peer_id),
            "only the dedicated reducer should publish reducer-load telemetry: {:?}",
            reducer_load_peers,
        );
    }

    let sync_deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    let first_promoted_head = loop {
        let synced_a = validator_a
            .sync_experiment_head(&experiment)
            .expect("validator a sync during convergence");
        let synced_b = validator_b
            .sync_experiment_head(&experiment)
            .expect("validator b sync during convergence");
        if let (Some(a), Some(b)) = (synced_a, synced_b)
            && a.head_id == b.head_id
            && a.parent_head_id == Some(genesis_head.head_id.clone())
        {
            break a;
        }
        assert!(
            Instant::now() < sync_deadline,
            "validators did not converge on the promoted canonical head",
        );
        thread::sleep(Duration::from_millis(25));
    };
    wait_for(
        Duration::from_secs(5),
        || {
            let persisted_a =
                crate::runtime_support::load_head_state(&validator_a_storage, &experiment)
                    .expect("load validator a persisted head during convergence");
            let persisted_b =
                crate::runtime_support::load_head_state(&validator_b_storage, &experiment)
                    .expect("load validator b persisted head during convergence");
            matches!(
                (persisted_a.as_ref(), persisted_b.as_ref()),
                (Some(a), Some(b))
                    if a.head_id == first_promoted_head.head_id
                        && b.head_id == first_promoted_head.head_id
            )
        },
        "validators did not persist the promoted canonical head",
    );
    let persisted_a = crate::runtime_support::load_head_state(&validator_a_storage, &experiment)
        .expect("load validator a persisted head")
        .expect("validator a persisted canonical head");
    let persisted_b = crate::runtime_support::load_head_state(&validator_b_storage, &experiment)
        .expect("load validator b persisted head")
        .expect("validator b persisted canonical head");
    assert_eq!(persisted_a.head_id, persisted_b.head_id);
    assert_eq!(persisted_a.parent_head_id, Some(genesis_head.head_id));

    for trainer in &trainers {
        wait_for(
            Duration::from_secs(5),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("trainer sync after first promotion")
                    .is_some_and(|head| head.head_id == first_promoted_head.head_id)
            },
            "trainer did not sync the first promoted head",
        );
    }

    let mut second_round_outcomes = Vec::new();
    for trainer in &mut trainers {
        second_round_outcomes.push(
            trainer
                .train_window_once(&experiment)
                .expect("trainer second training window"),
        );
    }

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_a_telemetry.snapshot();
            snapshot
                .control_plane
                .update_announcements
                .iter()
                .filter(|announcement| {
                    announcement.update.base_head_id == first_promoted_head.head_id
                })
                .map(|announcement| announcement.update.peer_id.clone())
                .collect::<BTreeSet<_>>()
                .len()
                >= 2
        },
        "validator a did not observe second-round trainer updates",
    );

    for outcome in &second_round_outcomes {
        wait_for(
            Duration::from_secs(5),
            || {
                reducer
                    .sync_artifact_from_peer(
                        &outcome.contribution.peer_id,
                        outcome.head.artifact_id.clone(),
                    )
                    .is_ok()
            },
            "reducer did not warm the second-round trainer artifact from the live network",
        );
        reducer
            .publish_artifact_from_store(&outcome.head.artifact_id)
            .expect("reducer republish second-round trainer artifact");
        wait_for(
            Duration::from_secs(5),
            || {
                validator_a
                    .sync_artifact_from_peer(&reducer_peer_id, outcome.head.artifact_id.clone())
                    .is_ok()
            },
            "validator a did not warm the second-round trainer artifact from the reducer",
        );
        validator_a
            .publish_artifact_from_store(&outcome.head.artifact_id)
            .expect("validator a republish second-round trainer artifact");
        wait_for(
            Duration::from_secs(5),
            || {
                validator_b
                    .sync_artifact_from_peer(&validator_a_peer_id, outcome.head.artifact_id.clone())
                    .is_ok()
            },
            "validator b did not warm the second-round trainer artifact from validator a",
        );
    }

    let reduced_second = reducer
        .reduce_candidates_once(&experiment)
        .expect("reducer second aggregate proposal")
        .expect("reducer second outcome");
    assert_eq!(reduced_second.aggregate.reducer_peer_id, reducer_peer_id);
    assert_eq!(
        reduced_second.aggregate.base_head_id,
        first_promoted_head.head_id
    );

    wait_for(
        Duration::from_secs(5),
        || {
            let aggregate_id = &reduced_second.aggregate.aggregate_id;
            let a = validator_a_telemetry.snapshot();
            let b = validator_b_telemetry.snapshot();
            a.control_plane
                .aggregate_proposal_announcements
                .iter()
                .any(|announcement| announcement.proposal.aggregate_id == *aggregate_id)
                && b.control_plane
                    .aggregate_proposal_announcements
                    .iter()
                    .any(|announcement| announcement.proposal.aggregate_id == *aggregate_id)
        },
        "validators did not observe the second dedicated reducer proposal",
    );

    let outcome_a = validator_a
        .validate_candidates_once(&experiment)
        .expect("validator a second validate");
    let outcome_b = validator_b
        .validate_candidates_once(&experiment)
        .expect("validator b second validate");
    let mut second_promoted_results = vec![outcome_a, outcome_b]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(
        second_promoted_results.len() <= 1,
        "at most one validator should report itself as the second promotion winner",
    );
    let second_promoted = second_promoted_results
        .pop()
        .expect("second round should produce one promoted merge");

    let second_convergence_deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        let snapshots = [
            reducer_telemetry.snapshot(),
            validator_a_telemetry.snapshot(),
            validator_b_telemetry.snapshot(),
        ];
        if snapshots.iter().all(|snapshot| {
            snapshot
                .control_plane
                .merge_announcements
                .iter()
                .any(|announcement| {
                    announcement.certificate.merged_head_id == second_promoted.merged_head.head_id
                })
        }) {
            break;
        }
        assert!(
            Instant::now() < second_convergence_deadline,
            "second reducer/validator round did not converge on merge {}",
            second_promoted.merged_head.head_id,
        );
        thread::sleep(Duration::from_millis(25));
    }

    wait_for(
        Duration::from_secs(5),
        || {
            let reducer_head = reducer
                .sync_experiment_head(&experiment)
                .expect("reducer second sync during convergence");
            let validator_a_head = validator_a
                .sync_experiment_head(&experiment)
                .expect("validator a second sync during convergence");
            let validator_b_head = validator_b
                .sync_experiment_head(&experiment)
                .expect("validator b second sync during convergence");
            matches!(
                (
                    reducer_head.as_ref(),
                    validator_a_head.as_ref(),
                    validator_b_head.as_ref()
                ),
                (Some(reducer_head), Some(validator_a_head), Some(validator_b_head))
                    if reducer_head.head_id == second_promoted.merged_head.head_id
                        && validator_a_head.head_id == second_promoted.merged_head.head_id
                        && validator_b_head.head_id == second_promoted.merged_head.head_id
                        && reducer_head.parent_head_id == Some(first_promoted_head.head_id.clone())
                        && validator_a_head.parent_head_id
                            == Some(first_promoted_head.head_id.clone())
                        && validator_b_head.parent_head_id
                            == Some(first_promoted_head.head_id.clone())
            )
        },
        "second reducer/validator round did not adopt the promoted canonical head",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            let persisted_a =
                crate::runtime_support::load_head_state(&validator_a_storage, &experiment)
                    .expect("load validator a second persisted head during convergence");
            let persisted_b =
                crate::runtime_support::load_head_state(&validator_b_storage, &experiment)
                    .expect("load validator b second persisted head during convergence");
            matches!(
                (persisted_a.as_ref(), persisted_b.as_ref()),
                (Some(a), Some(b))
                    if a.head_id == second_promoted.merged_head.head_id
                        && b.head_id == second_promoted.merged_head.head_id
                        && a.parent_head_id == Some(first_promoted_head.head_id.clone())
                        && b.parent_head_id == Some(first_promoted_head.head_id.clone())
            )
        },
        "validators did not persist the second promoted canonical head",
    );

    let synced_reducer_second = reducer
        .sync_experiment_head(&experiment)
        .expect("reducer second sync")
        .expect("reducer second canonical head");
    let synced_a_second = validator_a
        .sync_experiment_head(&experiment)
        .expect("validator a second sync")
        .expect("validator a second canonical head");
    let synced_b_second = validator_b
        .sync_experiment_head(&experiment)
        .expect("validator b second sync")
        .expect("validator b second canonical head");
    assert_eq!(
        synced_reducer_second.head_id,
        second_promoted.merged_head.head_id
    );
    assert_eq!(synced_a_second.head_id, second_promoted.merged_head.head_id);
    assert_eq!(synced_b_second.head_id, second_promoted.merged_head.head_id);
    assert_eq!(
        synced_reducer_second.parent_head_id,
        Some(first_promoted_head.head_id)
    );

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    reducer.shutdown().expect("reducer shutdown");
    let _ = reducer.await_termination().expect("reducer termination");
    validator_b.shutdown().expect("validator b shutdown");
    let _ = validator_b
        .await_termination()
        .expect("validator b termination");
    validator_a.shutdown().expect("validator a shutdown");
    let _ = validator_a
        .await_termination()
        .expect("validator a termination");
}

#[test]
fn reducer_authority_promotes_without_validators_and_skips_head_eval() {
    let _guard = native_swarm_test_guard();
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();
    let bootstrap_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-reducer-authority-bootstrap-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let reducer_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-reducer-authority-reducer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let bootstrap_addr = loopback_listen_address();

    let mut directory_entry = runtime_directory_entry(&experiment);
    directory_entry.allowed_roles = crate::PeerRoleSet::new([
        crate::PeerRole::Bootstrap,
        crate::PeerRole::Reducer,
        crate::PeerRole::TrainerCpu,
    ]);
    let merge_topology = crate::MergeTopologyPolicy {
        reducer_replication: 1,
        target_leaf_cohort: 2,
        promotion_policy: crate::HeadPromotionPolicy {
            mode: crate::HeadPromotionMode::ReducerAuthority,
            validator_quorum: 1,
            ..crate::HeadPromotionPolicy::default()
        },
        ..crate::MergeTopologyPolicy::default()
    };
    directory_entry.metadata.insert(
        "burn_p2p.revision.merge_topology.policy_json".into(),
        serde_json::to_string(&merge_topology).expect("merge topology json"),
    );
    let auth = crate::AuthConfig::new().with_experiment_directory(vec![directory_entry]);

    let bootstrap = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Bootstrap]))
    .with_auth(auth.clone())
    .with_listen_address(bootstrap_addr.clone())
    .with_storage(bootstrap_storage.clone())
    .spawn()
    .expect("bootstrap spawn");
    let bootstrap_telemetry = bootstrap.telemetry();

    let reducer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_roles(crate::PeerRoleSet::new([crate::PeerRole::Reducer]))
    .with_auth(auth.clone())
    .with_listen_address(loopback_listen_address())
    .with_storage(reducer_storage.clone())
    .with_bootstrap_peer(bootstrap_addr.clone())
    .spawn()
    .expect("reducer spawn");
    let reducer_telemetry = reducer.telemetry();

    let mut trainers = Vec::new();
    for (index, learning_rate) in [0.25, 0.75].into_iter().enumerate() {
        let trainer = NodeBuilder::new(SyntheticRuntimeProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate,
            target_model: 10.0,
        })
        .with_mainnet(mainnet().genesis.clone())
        .with_roles(crate::PeerRoleSet::new([crate::PeerRole::TrainerCpu]))
        .with_auth(auth.clone())
        .with_listen_address(loopback_listen_address())
        .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
            "burn-p2p-reducer-authority-trainer-{index}-{}",
            Utc::now().timestamp_nanos_opt().expect("nanos")
        ))))
        .with_bootstrap_peer(bootstrap_addr.clone())
        .spawn()
        .expect("trainer spawn");
        trainers.push(trainer);
    }

    wait_for(
        Duration::from_secs(15),
        || bootstrap_telemetry.snapshot().connected_peers >= 3,
        "bootstrap did not connect to reducer and trainers",
    );
    wait_for(
        Duration::from_secs(15),
        || reducer_telemetry.snapshot().connected_peers >= 3,
        "reducer did not connect to bootstrap and both trainers",
    );

    let mut bootstrap = bootstrap;
    let mut reducer = reducer;
    let bootstrap_peer_id = bootstrap_telemetry
        .snapshot()
        .local_peer_id
        .expect("bootstrap peer id");
    let reducer_peer_id = reducer_telemetry
        .snapshot()
        .local_peer_id
        .expect("reducer peer id");
    let genesis_head = bootstrap
        .initialize_local_head(&experiment)
        .expect("bootstrap genesis head");

    wait_for(
        Duration::from_secs(10),
        || {
            reducer
                .sync_experiment_head(&experiment)
                .expect("reducer sync genesis")
                .is_some()
                && trainers.iter().all(|trainer| {
                    trainer
                        .sync_experiment_head(&experiment)
                        .expect("trainer sync genesis")
                        .is_some()
                })
        },
        "reducer and trainers did not sync genesis head",
    );

    let mut trainer_outcomes = Vec::new();
    for trainer in &mut trainers {
        trainer_outcomes.push(
            trainer
                .train_window_once(&experiment)
                .expect("trainer training window"),
        );
    }

    for outcome in &trainer_outcomes {
        let artifact_deadline = Instant::now() + test_timeout(Duration::from_secs(30));
        loop {
            match reducer.sync_artifact_from_peer(
                &outcome.contribution.peer_id,
                outcome.head.artifact_id.clone(),
            ) {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        Instant::now() < artifact_deadline,
                        "reducer did not warm trainer artifact {} from {}: {}",
                        outcome.head.artifact_id.as_str(),
                        outcome.contribution.peer_id.as_str(),
                        error,
                    );
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    let reduced = reducer
        .reduce_candidates_once(&experiment)
        .expect("reducer authority promotion")
        .expect("reducer authority outcome");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = reducer_telemetry.snapshot();
            snapshot
                .control_plane
                .aggregate_proposal_announcements
                .len()
                == 1
                && snapshot
                    .control_plane
                    .reduction_certificate_announcements
                    .len()
                    == 1
                && snapshot
                    .control_plane
                    .validation_quorum_announcements
                    .is_empty()
                && snapshot.control_plane.merge_announcements.len() == 1
                && snapshot
                    .control_plane
                    .merge_window_announcements
                    .last()
                    .is_some_and(|announcement| announcement.merge_window.validators.is_empty())
        },
        "reducer authority node did not publish the expected canonical promotion state",
    );

    let reducer_snapshot = reducer_telemetry.snapshot();
    let reduction_certificate = reducer_snapshot
        .control_plane
        .reduction_certificate_announcements
        .last()
        .expect("reduction certificate")
        .certificate
        .clone();
    let merge_certificate = reducer_snapshot
        .control_plane
        .merge_announcements
        .last()
        .expect("merge certificate")
        .certificate
        .clone();
    assert_eq!(
        reduction_certificate.promotion_mode,
        crate::HeadPromotionMode::ReducerAuthority
    );
    assert_eq!(reduction_certificate.promotion_quorum, 1);
    assert_eq!(
        merge_certificate.promotion_mode,
        crate::HeadPromotionMode::ReducerAuthority
    );
    assert_eq!(
        merge_certificate.merged_head_id,
        reduced.merged_head.head_id
    );
    assert_eq!(merge_certificate.base_head_id, genesis_head.head_id);
    assert!(
        reducer_snapshot
            .control_plane
            .validation_quorum_announcements
            .is_empty(),
        "reducer-authority promotion should not emit validation quorum certificates",
    );

    bootstrap
        .ingest_peer_snapshot(&reducer_peer_id, test_timeout(Duration::from_secs(5)))
        .expect("bootstrap ingest reducer-authority snapshot");
    bootstrap
        .wait_for_known_head(
            &experiment,
            &reduced.merged_head,
            test_timeout(Duration::from_secs(10)),
        )
        .expect("bootstrap did not adopt the reducer-authority canonical head");
    bootstrap
        .publish_head_provider(&experiment, &reduced.merged_head)
        .expect("bootstrap did not publish the reducer-authority head provider");
    let reducer_authority_head_providers = [bootstrap_peer_id.clone(), reducer_peer_id.clone()];
    for trainer in &trainers {
        trainer
            .ingest_peer_snapshot(&bootstrap_peer_id, test_timeout(Duration::from_secs(5)))
            .expect("trainer ingest bootstrap reducer-authority snapshot");
        trainer
            .wait_for_artifact_from_peers(
                &reducer_authority_head_providers,
                &reduced.merged_head.artifact_id,
                test_timeout(Duration::from_secs(30)),
            )
            .expect("trainer did not materialize the reducer-authority head artifact");
        trainer
            .wait_for_known_head(
                &experiment,
                &reduced.merged_head,
                test_timeout(Duration::from_secs(10)),
            )
            .expect("trainer did not adopt the reducer-authority canonical head");
    }

    let head_eval_reports = load_metric_artifacts::<HeadEvalReport>(&reducer_storage, "head-eval-");
    assert_eq!(head_eval_reports.len(), 1);
    assert_eq!(head_eval_reports[0].head_id, reduced.merged_head.head_id);
    assert_eq!(head_eval_reports[0].status, crate::HeadEvalStatus::Skipped);
    assert_eq!(head_eval_reports[0].sample_count, 0);
    assert!(head_eval_reports[0].metric_values.is_empty());

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    reducer.shutdown().expect("reducer shutdown");
    let _ = reducer.await_termination().expect("reducer termination");
    bootstrap.shutdown().expect("bootstrap shutdown");
    let _ = bootstrap
        .await_termination()
        .expect("bootstrap termination");
}
