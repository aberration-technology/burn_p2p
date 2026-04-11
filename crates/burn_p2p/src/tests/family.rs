use super::support::*;

#[test]
fn family_runtime_switches_experiments_and_restores_selection() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("switching storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let expected_assignment = crate::SlotAssignmentState::new(
        crate::StudyId::new("study-switch"),
        crate::ExperimentId::new("exp-b"),
        crate::RevisionId::new("rev-b"),
    );

    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new().with_experiment_directory(switching_directory_entries()),
        )
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let switched = running
        .switch_experiment(
            expected_assignment.study_id.clone(),
            expected_assignment.experiment_id.clone(),
            expected_assignment.revision_id.clone(),
        )
        .expect("switch experiment");
    assert_eq!(switched.experiment_id, expected_assignment.experiment_id);
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            expected_assignment.clone()
        ))
    );

    let persisted_assignment: crate::SlotAssignmentState = serde_json::from_slice(
        &fs::read(storage_config.primary_slot_assignment_path())
            .expect("read persisted slot assignment"),
    )
    .expect("decode persisted slot assignment");
    assert_eq!(persisted_assignment, expected_assignment);

    running.shutdown().expect("shutdown switching runtime");
    let node = running
        .await_termination()
        .expect("await switching runtime");
    assert_eq!(
        node.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        node.into_project().selected_workload_id(),
        &crate::WorkloadId::new("alternate")
    );

    let mut restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new().with_experiment_directory(switching_directory_entries()),
        )
        .spawn()
        .expect("respawn switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "restarted switching runtime did not start",
    );
    assert_eq!(
        restarted_telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            expected_assignment.clone()
        ))
    );

    let restored = restarted
        .restore_selected_experiment()
        .expect("restore selected experiment")
        .expect("persisted experiment should restore");
    assert_eq!(restored.experiment_id, expected_assignment.experiment_id);
    assert_eq!(
        restarted.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    restarted.shutdown().expect("shutdown restarted runtime");
    let restarted_node = restarted
        .await_termination()
        .expect("await restarted runtime");
    assert_eq!(
        restarted_node.into_project().selected_workload_id(),
        &crate::WorkloadId::new("alternate")
    );
}

#[test]
fn family_runtime_follows_live_directory_revision_updates() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("directory switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a",
                "rev-a",
                "compiled",
                "schema-a",
                "dataset-view",
                "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    assert_eq!(initial.revision_id, crate::RevisionId::new("rev-a"));
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("compiled"))
    );

    running
        .control_handle()
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![switching_directory_entry(
                "exp-a",
                "rev-b",
                "alternate",
                "schema-b",
                "view-b",
                "Switch A Rev B",
            )],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .directory_announcements
                .len()
                >= 2
        },
        "updated directory was not reflected in telemetry",
    );

    let followed = running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane")
        .expect("active experiment");
    assert_eq!(followed.experiment_id, crate::ExperimentId::new("exp-a"));
    assert_eq!(followed.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            crate::SlotAssignmentState::new(
                crate::StudyId::new("study-switch"),
                crate::ExperimentId::new("exp-a"),
                crate::RevisionId::new("rev-b"),
            )
        ))
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");
}

#[test]
fn family_runtime_waits_for_authoritative_lifecycle_activation_before_switching() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("lifecycle switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a",
                "rev-a",
                "compiled",
                "schema-a",
                "switch-dataset-view",
                "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "lifecycle switching runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    assert_eq!(initial.revision_id, crate::RevisionId::new("rev-a"));

    let rev_b_entry = switching_directory_entry(
        "exp-a",
        "rev-b",
        "alternate",
        "schema-b",
        "dataset-view",
        "Switch A Rev B",
    );
    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![rev_b_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: initial.study_id.clone(),
                experiment_id: initial.experiment_id.clone(),
                base_revision_id: Some(initial.revision_id.clone()),
                target_entry: rev_b_entry.clone(),
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(3),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 1,
                reason: Some("activate rev-b at window 3".into()),
            },
        ))
        .expect("publish lifecycle plan");
    wait_for(
        Duration::from_secs(5),
        || {
            !telemetry
                .snapshot()
                .control_plane
                .lifecycle_announcements
                .is_empty()
        },
        "lifecycle plan was not reflected in telemetry",
    );

    let before_activation = running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane before activation")
        .expect("active experiment before activation");
    assert_eq!(
        before_activation.revision_id,
        crate::RevisionId::new("rev-a")
    );
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("compiled"))
    );

    let after_activation = running
        .follow_control_plane_once(crate::WindowId(3))
        .expect("follow control plane at activation")
        .expect("active experiment after activation");
    assert_eq!(
        after_activation.revision_id,
        crate::RevisionId::new("rev-b")
    );
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    running
        .shutdown()
        .expect("shutdown lifecycle switching runtime");
    let _ = running
        .await_termination()
        .expect("await lifecycle switching runtime");
}

#[test]
fn family_runtime_train_window_applies_authoritative_lifecycle_activation() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("training lifecycle switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn training lifecycle runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "training lifecycle runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    let first = running
        .train_window_once(&initial)
        .expect("first training window on rev-a");
    assert_eq!(first.head.revision_id, crate::RevisionId::new("rev-a"));

    let rev_b_entry = switching_directory_entry(
        "exp-a",
        "rev-b",
        "alternate",
        "schema-b",
        "switch-dataset-view",
        "Switch A Rev B",
    );
    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![rev_b_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: initial.study_id.clone(),
                experiment_id: initial.experiment_id.clone(),
                base_revision_id: Some(initial.revision_id.clone()),
                target_entry: rev_b_entry,
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 1,
                reason: Some("activate rev-b at window 2".into()),
            },
        ))
        .expect("publish lifecycle plan");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && !snapshot.control_plane.lifecycle_announcements.is_empty()
        },
        "lifecycle control plane state was not reflected in telemetry",
    );

    let second = running
        .train_window_once(&initial)
        .expect("second training window should follow lifecycle plan");
    assert_eq!(second.head.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        running
            .active_experiment()
            .expect("active experiment after lifecycle training")
            .revision_id,
        crate::RevisionId::new("rev-b")
    );
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    running
        .shutdown()
        .expect("shutdown training lifecycle runtime");
    let _ = running
        .await_termination()
        .expect("await training lifecycle runtime");
}

#[test]
fn family_runtime_continuous_trainer_applies_authoritative_lifecycle_activation() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("continuous lifecycle switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn continuous lifecycle runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "continuous lifecycle runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");

    let rev_b_entry = switching_directory_entry(
        "exp-a",
        "rev-b",
        "alternate",
        "schema-b",
        "switch-dataset-view",
        "Switch A Rev B",
    );
    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![rev_b_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: initial.study_id.clone(),
                experiment_id: initial.experiment_id.clone(),
                base_revision_id: Some(initial.revision_id.clone()),
                target_entry: rev_b_entry,
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 1,
                reason: Some("activate rev-b at window 2".into()),
            },
        ))
        .expect("publish lifecycle plan");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && !snapshot.control_plane.lifecycle_announcements.is_empty()
        },
        "continuous lifecycle control plane state was not reflected in telemetry",
    );

    {
        let mut trainer = running
            .continuous_trainer(&initial)
            .expect("continuous trainer session");

        let first = trainer
            .train_next_window()
            .expect("first continuous training window");
        assert_eq!(first.head.revision_id, crate::RevisionId::new("rev-a"));
        assert_eq!(
            trainer.experiment().revision_id,
            crate::RevisionId::new("rev-a")
        );

        let second = trainer
            .train_next_window()
            .expect("second continuous training window");
        assert_eq!(second.head.revision_id, crate::RevisionId::new("rev-b"));
        assert_eq!(
            trainer.experiment().revision_id,
            crate::RevisionId::new("rev-b")
        );
    }

    assert_eq!(
        running
            .active_experiment()
            .expect("active experiment after continuous lifecycle training")
            .revision_id,
        crate::RevisionId::new("rev-b")
    );
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    running
        .shutdown()
        .expect("shutdown continuous lifecycle runtime");
    let _ = running
        .await_termination()
        .expect("await continuous lifecycle runtime");
}

#[test]
fn family_runtime_reassigns_primary_slot_across_experiments_via_authoritative_lifecycle() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("cross-experiment lifecycle storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn cross-experiment lifecycle runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "cross-experiment lifecycle runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    let target_entry = switching_directory_entry(
        "exp-b",
        "rev-b",
        "alternate",
        "schema-b",
        "view-b",
        "Switch B",
    );
    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![target_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish reassignment directory entry");
    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: initial.study_id.clone(),
                experiment_id: initial.experiment_id.clone(),
                base_revision_id: Some(initial.revision_id.clone()),
                target_entry,
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 1,
                reason: Some("reassign primary slot to exp-b".into()),
            },
        ))
        .expect("publish cross-experiment lifecycle plan");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && !snapshot.control_plane.lifecycle_announcements.is_empty()
        },
        "cross-experiment lifecycle control plane state was not reflected in telemetry",
    );

    let before_activation = running
        .follow_control_plane_once(crate::WindowId(1))
        .expect("follow control plane before cross-experiment activation")
        .expect("active experiment before cross-experiment activation");
    assert_eq!(
        before_activation.experiment_id,
        crate::ExperimentId::new("exp-a")
    );
    assert_eq!(
        before_activation.revision_id,
        crate::RevisionId::new("rev-a")
    );

    let after_activation = running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane at cross-experiment activation")
        .expect("active experiment after cross-experiment activation");
    assert_eq!(
        after_activation.experiment_id,
        crate::ExperimentId::new("exp-b")
    );
    assert_eq!(
        after_activation.revision_id,
        crate::RevisionId::new("rev-b")
    );
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            crate::SlotAssignmentState::new(
                crate::StudyId::new("study-switch"),
                crate::ExperimentId::new("exp-b"),
                crate::RevisionId::new("rev-b"),
            )
        ))
    );

    running
        .shutdown()
        .expect("shutdown cross-experiment lifecycle runtime");
    let _ = running
        .await_termination()
        .expect("await cross-experiment lifecycle runtime");
}

#[test]
fn family_runtime_multi_node_lifecycle_rollout_prewarms_before_activation() {
    let _guard = native_swarm_test_guard();
    let initial_directory = vec![switching_directory_entry(
        "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
    )];
    let provider_storage = tempdir().expect("provider lifecycle storage");
    let mut provider = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(provider_storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(initial_directory.clone()))
        .spawn()
        .expect("spawn provider lifecycle runtime");
    let provider_telemetry = provider.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = provider_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "provider lifecycle runtime did not start",
    );
    let provider_addr = provider_telemetry.snapshot().listen_addresses[0].clone();
    let provider_peer_id = provider_telemetry
        .snapshot()
        .local_peer_id
        .expect("provider peer id");

    let follower_storage = tempdir().expect("follower lifecycle storage");
    let mut follower = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(follower_storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(initial_directory.clone()))
        .with_bootstrap_peer(provider_addr)
        .spawn()
        .expect("spawn follower lifecycle runtime");
    let follower_telemetry = follower.telemetry();
    wait_for(
        Duration::from_secs(5),
        || provider_telemetry.snapshot().connected_peers >= 1,
        "provider lifecycle runtime did not connect to follower",
    );
    wait_for(
        Duration::from_secs(5),
        || follower_telemetry.snapshot().connected_peers >= 1,
        "follower lifecycle runtime did not connect to provider",
    );

    let initial = follower
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial follower experiment selection");
    let target_experiment = mainnet().experiment(
        crate::StudyId::new("study-switch"),
        crate::ExperimentId::new("exp-a"),
        crate::RevisionId::new("rev-b"),
    );
    let target_head = provider
        .initialize_local_head(&target_experiment)
        .expect("initialize provider target head");
    provider
        .publish_head_provider(&target_experiment, &target_head)
        .expect("publish provider target head");
    follower
        .ingest_peer_snapshot(&provider_peer_id, Duration::from_secs(5))
        .expect("ingest provider snapshot");

    let target_entry = switching_directory_entry(
        "exp-a",
        "rev-b",
        "alternate",
        "schema-b",
        "view-b",
        "Switch A Rev B",
    );
    let control = provider.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![target_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish rollout directory entry");

    for (plan_epoch, phase) in [
        (1_u64, burn_p2p_experiment::ExperimentLifecyclePhase::Staged),
        (
            2_u64,
            burn_p2p_experiment::ExperimentLifecyclePhase::Prewarming,
        ),
        (3_u64, burn_p2p_experiment::ExperimentLifecyclePhase::Ready),
    ] {
        let reason = format!("rollout phase {phase:?}");
        control
            .publish_lifecycle(lifecycle_announcement(
                burn_p2p_experiment::ExperimentLifecyclePlan {
                    study_id: initial.study_id.clone(),
                    experiment_id: initial.experiment_id.clone(),
                    base_revision_id: Some(initial.revision_id.clone()),
                    target_entry: target_entry.clone(),
                    phase,
                    target: burn_p2p_experiment::ActivationTarget {
                        activation: crate::WindowActivation {
                            activation_window: crate::WindowId(3),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::new(),
                    },
                    plan_epoch,
                    reason: Some(reason),
                },
            ))
            .expect("publish lifecycle phase");
        follower
            .ingest_peer_snapshot(&provider_peer_id, Duration::from_secs(5))
            .expect("refresh follower snapshot");
    }
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = follower_telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && snapshot.control_plane.lifecycle_announcements.len() >= 3
        },
        "follower did not observe staged lifecycle phases",
    );

    let before_activation = follower
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane before activation")
        .expect("active experiment before activation");
    assert_eq!(
        before_activation.revision_id,
        crate::RevisionId::new("rev-a")
    );
    assert_eq!(
        follower.config().selected_workload_id,
        Some(crate::WorkloadId::new("compiled"))
    );

    follower
        .wait_for_artifact_from_peers(
            std::slice::from_ref(&provider_peer_id),
            &target_head.artifact_id,
            Duration::from_secs(10),
        )
        .expect("prewarm target artifact from provider");
    assert!(
        follower
            .artifact_store()
            .expect("follower artifact store")
            .has_manifest(&target_head.artifact_id)
    );
    assert!(
        follower
            .adopt_known_head_if_present(&target_experiment, &target_head)
            .expect("adopt prewarmed target head"),
        "prewarmed target head should be adoptable before activation"
    );

    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: initial.study_id.clone(),
                experiment_id: initial.experiment_id.clone(),
                base_revision_id: Some(initial.revision_id.clone()),
                target_entry,
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(3),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 4,
                reason: Some("activate prewarmed rev-b".into()),
            },
        ))
        .expect("publish activating lifecycle phase");
    follower
        .ingest_peer_snapshot(&provider_peer_id, Duration::from_secs(5))
        .expect("refresh follower activating snapshot");

    let after_activation = follower
        .follow_control_plane_once(crate::WindowId(3))
        .expect("follow control plane at activation")
        .expect("active experiment after activation");
    assert_eq!(
        after_activation.revision_id,
        crate::RevisionId::new("rev-b")
    );
    assert_eq!(
        follower.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    let adopted = follower
        .sync_experiment_head(&after_activation)
        .expect("sync active rev-b head")
        .expect("active rev-b head");
    assert_eq!(adopted.head_id, target_head.head_id);

    follower
        .shutdown()
        .expect("shutdown follower lifecycle runtime");
    let _ = follower
        .await_termination()
        .expect("await follower lifecycle runtime");
    provider
        .shutdown()
        .expect("shutdown provider lifecycle runtime");
    let _ = provider
        .await_termination()
        .expect("await provider lifecycle runtime");
}

#[test]
fn family_runtime_applies_control_plane_pause_and_resume_at_window_boundaries() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("control switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let experiment = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    let assignment = crate::SlotAssignmentState::from_experiment(&experiment);

    let control = running.control_handle();
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                reason: Some("maintenance".into()),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish pause control");
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(3),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish resume control");
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .control_announcements
                .len()
                >= 2
        },
        "control announcements were not reflected in telemetry",
    );

    running
        .follow_control_plane_once(crate::WindowId(1))
        .expect("follow control plane before pause");
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(assignment.clone()))
    );

    running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane at pause");
    match telemetry
        .snapshot()
        .slot_states
        .first()
        .expect("slot state should exist")
    {
        crate::SlotRuntimeState::Blocked {
            assignment: blocked_assignment,
            reason,
        } => {
            assert_eq!(blocked_assignment.as_ref(), Some(&assignment));
            assert!(reason.contains("maintenance"));
        }
        other => panic!("expected blocked slot state, got {other:?}"),
    }

    running
        .follow_control_plane_once(crate::WindowId(3))
        .expect("follow control plane at resume");
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(assignment))
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");
}

#[test]
fn family_runtime_restores_persisted_control_plane_state_after_restart() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("persisted control storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let stale_directory = vec![switching_directory_entry(
        "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
    )];
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory.clone()))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let experiment = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");

    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![switching_directory_entry(
                "exp-a",
                "rev-b",
                "alternate",
                "schema-b",
                "view-b",
                "Switch A Rev B",
            )],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                reason: Some("maintenance".into()),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish pause control");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && !snapshot.control_plane.control_announcements.is_empty()
        },
        "updated control plane state was not reflected in telemetry",
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");

    assert!(
        fs::read(storage_config.control_plane_state_path()).is_ok(),
        "control plane state should be persisted before restart"
    );

    let mut restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory))
        .spawn()
        .expect("respawn switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot
                    .control_plane
                    .directory_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.entries.iter().any(|entry| {
                            entry.experiment_id == crate::ExperimentId::new("exp-a")
                                && entry.current_revision_id == crate::RevisionId::new("rev-b")
                                && entry.workload_id == crate::WorkloadId::new("alternate")
                        })
                    })
                && snapshot
                    .control_plane
                    .control_announcements
                    .iter()
                    .any(|announcement| {
                        matches!(
                            announcement.certificate.body.payload.payload.command,
                            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment { .. }
                        )
                    })
        },
        "persisted control plane state was not restored after restart",
    );

    let followed = restarted
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow restored control plane")
        .expect("active experiment after restore");
    assert_eq!(followed.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        restarted.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        restarted_telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Blocked {
            assignment: Some(crate::SlotAssignmentState::new(
                crate::StudyId::new("study-switch"),
                crate::ExperimentId::new("exp-a"),
                crate::RevisionId::new("rev-b"),
            )),
            reason: "maintenance".into(),
        })
    );

    restarted.shutdown().expect("shutdown restarted runtime");
    let _ = restarted
        .await_termination()
        .expect("await restarted runtime");
}

#[test]
fn family_runtime_restores_persisted_lifecycle_plans_after_restart() {
    let _guard = native_swarm_test_guard();
    let storage = tempdir().expect("persisted lifecycle storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let stale_directory = vec![switching_directory_entry(
        "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
    )];
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory.clone()))
        .spawn()
        .expect("spawn lifecycle runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "lifecycle runtime did not start",
    );

    let experiment = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");

    let rev_b_entry = switching_directory_entry(
        "exp-a",
        "rev-b",
        "alternate",
        "schema-b",
        "view-b",
        "Switch A Rev B",
    );
    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![rev_b_entry.clone()],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_lifecycle(lifecycle_announcement(
            burn_p2p_experiment::ExperimentLifecyclePlan {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                base_revision_id: Some(experiment.revision_id.clone()),
                target_entry: rev_b_entry,
                phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                plan_epoch: 1,
                reason: Some("restore rev-b on restart".into()),
            },
        ))
        .expect("publish lifecycle plan");
    wait_for(
        Duration::from_secs(5),
        || {
            !telemetry
                .snapshot()
                .control_plane
                .lifecycle_announcements
                .is_empty()
        },
        "lifecycle plan was not reflected in telemetry",
    );

    running.shutdown().expect("shutdown lifecycle runtime");
    let _ = running
        .await_termination()
        .expect("await lifecycle runtime");

    let mut restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config)
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory))
        .spawn()
        .expect("respawn lifecycle runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.control_plane.lifecycle_announcements.is_empty()
        },
        "persisted lifecycle state was not restored after restart",
    );

    let followed = restarted
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow restored lifecycle plan")
        .expect("active experiment after lifecycle restore");
    assert_eq!(followed.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        restarted.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    restarted
        .shutdown()
        .expect("shutdown restarted lifecycle runtime");
    let _ = restarted
        .await_termination()
        .expect("await restarted lifecycle runtime");
}
