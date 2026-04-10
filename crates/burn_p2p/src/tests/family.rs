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
