use super::support::*;

#[test]
fn runtime_rebudgets_next_window_after_slow_observed_throughput() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-slow-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let observed_budgets = Arc::new(Mutex::new(Vec::new()));
    let project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::clone(&observed_budgets),
    };
    create_adaptive_runtime_dataset(&project);

    let running = NodeBuilder::new(project)
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("adaptive spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "adaptive runtime did not start",
    );

    let mut running = running;
    let first = running
        .train_window_once(&experiment())
        .expect("first window");
    let second = running
        .train_window_once(&experiment())
        .expect("second window");

    assert_eq!(first.lease.budget_work_units, 64);
    assert!(
        second.lease.budget_work_units < first.lease.budget_work_units,
        "expected second budget {} to be smaller than first {}",
        second.lease.budget_work_units,
        first.lease.budget_work_units
    );
    let snapshot = running.telemetry().snapshot();
    assert!(
        snapshot
            .effective_limit_profile
            .as_ref()
            .map(|profile| profile.recommended_budget.budget_work_units
                < first.lease.budget_work_units)
            .unwrap_or(false)
    );
    let persisted = crate::runtime_support::load_limit_profile(
        &StorageConfig::new(storage_root),
        &experiment(),
    )
    .expect("load limit profile")
    .expect("persisted profile");
    assert!(persisted.recommended_budget.budget_work_units < first.lease.budget_work_units);
    assert_eq!(
        observed_budgets.lock().expect("budget log").as_slice(),
        &[
            first.lease.budget_work_units,
            second.lease.budget_work_units
        ]
    );

    running.shutdown().expect("adaptive shutdown");
    let _ = running.await_termination().expect("adaptive termination");
}

#[test]
fn continuous_trainer_reconciles_new_canonical_head_before_next_window() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-continuous-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let project = SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    };
    create_runtime_dataset(dataset_dir.path());

    let running = NodeBuilder::new(project.clone())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("continuous trainer spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "continuous trainer runtime did not start",
    );

    let mut running = running;
    let experiment = experiment();
    let genesis = running
        .initialize_local_head(&experiment)
        .expect("initialize genesis head");

    let storage = StorageConfig::new(storage_root.clone());
    {
        let mut trainer = running
            .continuous_trainer_with_policy(
                &experiment,
                crate::ContinuousTrainerPolicy {
                    canonical_reconcile: crate::TrainerCanonicalReconcileStrategy::RootEma {
                        canonical_weight: 0.25,
                    },
                    ..crate::ContinuousTrainerPolicy::default()
                },
            )
            .expect("continuous trainer session");

        let first = trainer
            .train_next_window()
            .expect("first continuous training window");
        assert_eq!(first.contribution.base_head_id, genesis.head_id);
        assert!((metric_float(&first.report.stats, "model") - 3.0).abs() < 1e-6);

        let canonical_head = persist_synthetic_runtime_head(
            &project,
            &storage,
            &experiment,
            11.0,
            "exp-1-canonical-1",
            Some(genesis.head_id.clone()),
            1,
        );

        let second = trainer
            .train_next_window()
            .expect("second continuous training window");
        assert_eq!(second.contribution.base_head_id, canonical_head.head_id);
        assert!((metric_float(&second.report.stats, "model") - 8.0).abs() < 1e-6);
    }

    running.shutdown().expect("continuous trainer shutdown");
    let _ = running
        .await_termination()
        .expect("continuous trainer termination");
}

#[test]
fn continuous_trainer_keeps_local_head_chain_when_canonical_has_not_advanced() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-continuous-local-chain-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    create_runtime_dataset(dataset_dir.path());

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(storage_root))
    .spawn()
    .expect("continuous trainer spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "continuous trainer runtime did not start",
    );

    let mut running = running;
    let experiment = experiment();
    let genesis = running
        .initialize_local_head(&experiment)
        .expect("initialize genesis head");

    {
        let mut trainer = running
            .continuous_trainer(&experiment)
            .expect("continuous trainer session");

        let first = trainer
            .train_next_window()
            .expect("first continuous training window");
        assert_eq!(first.contribution.base_head_id, genesis.head_id);
        assert_eq!(
            trainer
                .training_head()
                .expect("training head after first window")
                .head_id,
            first.head.head_id
        );

        let second = trainer
            .train_next_window()
            .expect("second continuous training window");
        assert_eq!(second.contribution.base_head_id, first.head.head_id);
        assert!((metric_float(&second.report.stats, "model") - 6.0).abs() < 1e-6);
    }

    running.shutdown().expect("continuous trainer shutdown");
    let _ = running
        .await_termination()
        .expect("continuous trainer termination");
}

#[test]
fn runtime_rebudgets_next_window_after_fast_observed_throughput() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-fast-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 4.0,
        benchmark_target_window_seconds: 60,
        simulated_window_seconds: 1,
        microshard_count: 512,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    create_adaptive_runtime_dataset(&project);

    let running = NodeBuilder::new(project)
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root))
        .spawn()
        .expect("adaptive spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "adaptive runtime did not start",
    );

    let mut running = running;
    let first = running
        .train_window_once(&experiment())
        .expect("first window");
    let second = running
        .train_window_once(&experiment())
        .expect("second window");

    assert_eq!(first.lease.budget_work_units, 240);
    assert!(
        second.lease.budget_work_units > first.lease.budget_work_units,
        "expected second budget {} to be larger than first {}",
        second.lease.budget_work_units,
        first.lease.budget_work_units
    );

    running.shutdown().expect("adaptive shutdown");
    let _ = running.await_termination().expect("adaptive termination");
}

#[test]
fn rebudgeted_limit_profile_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let first_project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    create_adaptive_runtime_dataset(&first_project);

    let first = NodeBuilder::new(first_project)
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("first adaptive spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first adaptive runtime did not start",
    );
    let mut first = first;
    let first_window = first
        .train_window_once(&experiment())
        .expect("first training window");
    first.shutdown().expect("first adaptive shutdown");
    let _ = first
        .await_termination()
        .expect("first adaptive termination");

    let restarted_project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    let restarted = NodeBuilder::new(restarted_project)
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("restarted adaptive spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().local_peer_id.is_some(),
        "restarted adaptive runtime did not start",
    );
    let mut restarted = restarted;
    let restarted_window = restarted
        .train_window_once(&experiment())
        .expect("restarted training window");
    assert!(
        restarted_window.lease.budget_work_units < first_window.lease.budget_work_units,
        "expected restarted budget {} to reuse rebudgeted profile from first budget {}",
        restarted_window.lease.budget_work_units,
        first_window.lease.budget_work_units
    );
    let persisted = crate::runtime_support::load_limit_profile(
        &StorageConfig::new(storage_root),
        &experiment(),
    )
    .expect("load restarted profile")
    .expect("persisted restarted profile");
    assert_eq!(
        persisted.recommended_budget.budget_work_units,
        restarted
            .telemetry()
            .snapshot()
            .effective_limit_profile
            .expect("telemetry limit profile")
            .recommended_budget
            .budget_work_units
    );

    restarted.shutdown().expect("restarted adaptive shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted adaptive termination");
}

#[test]
fn active_lease_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-lease-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let experiment = experiment();

    let first = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(storage_root.clone()))
    .spawn()
    .expect("first runtime spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first runtime did not start",
    );
    let mut first = first;
    let window = first
        .train_window_once(&experiment)
        .expect("training window");
    let persisted = crate::runtime_support::load_scoped_lease_announcement(
        &StorageConfig::new(storage_root.clone()),
        &experiment,
    )
    .expect("load persisted lease")
    .expect("persisted lease announcement");
    assert_eq!(persisted.lease, window.lease);
    assert!(storage_root.join("leases").exists());

    first.shutdown().expect("first runtime shutdown");
    let _ = first
        .await_termination()
        .expect("first runtime termination");

    let restarted = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(storage_root.clone()))
    .spawn()
    .expect("restarted runtime spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            restarted_telemetry
                .snapshot()
                .control_plane
                .lease_announcements
                .iter()
                .any(|announcement| announcement.lease.lease_id == window.lease.lease_id)
        },
        "restarted runtime did not restore persisted lease",
    );
    let restored = crate::runtime_support::load_scoped_lease_announcement(
        &StorageConfig::new(storage_root),
        &experiment,
    )
    .expect("load restored lease")
    .expect("restored lease announcement");
    assert_eq!(restored.lease, window.lease);

    restarted.shutdown().expect("restarted runtime shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted runtime termination");
}

#[test]
fn training_window_persists_peer_window_metrics() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .spawn()
    .expect("runtime spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );

    let mut running = running;
    let trained = running
        .train_window_once(&experiment())
        .expect("training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);
    let peer_window = &metrics[0];
    assert_eq!(peer_window.lease_id, trained.lease.lease_id);
    assert_eq!(peer_window.base_head_id, trained.contribution.base_head_id);
    assert_eq!(
        peer_window.accepted_tokens_or_samples,
        Some(trained.lease.budget_work_units)
    );
    assert_eq!(peer_window.dataset_view_id, trained.lease.dataset_view_id);
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .metrics_announcements
                .len()
                == 1
        },
        "runtime did not publish training metrics announcement",
    );
    let snapshot = telemetry.snapshot();
    let metrics_events = &snapshot.control_plane.metrics_announcements;
    assert_eq!(metrics_events.len(), 1);
    assert_eq!(
        metrics_events[0].event.kind,
        MetricsLiveEventKind::LedgerAppend
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].experiment_id,
        experiment().experiment_id
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].revision_id,
        experiment().revision_id
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].latest_merge_window_id,
        Some(
            snapshot.control_plane.merge_window_announcements[0]
                .merge_window
                .merge_window_id
                .clone()
        )
    );
    assert_eq!(
        metrics_events[0].overlay,
        experiment().overlay_set().expect("overlays").metrics
    );
    assert!(
        peer_window.compute_time_ms <= peer_window.compute_time_ms + peer_window.data_fetch_time_ms
    );

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn metrics_retention_auto_scales_with_node_roles() {
    let trainer_budget =
        MetricsRetentionConfig::default().resolve_for_roles(&crate::PeerRoleSet::default_trainer());
    let bootstrap_budget =
        MetricsRetentionConfig::default().resolve_for_roles(&crate::PeerRoleSet::new([
            crate::PeerRole::Bootstrap,
            crate::PeerRole::RelayHelper,
        ]));
    let operator_budget =
        MetricsRetentionConfig::default().resolve_for_roles(&crate::PeerRoleSet::new([
            crate::PeerRole::Bootstrap,
            crate::PeerRole::Authority,
            crate::PeerRole::Validator,
        ]));

    assert_eq!(bootstrap_budget, crate::MetricsRetentionBudget::peer_lean());
    assert!(
        trainer_budget.max_peer_window_entries_per_revision
            < operator_budget.max_peer_window_entries_per_revision
    );
    assert!(
        trainer_budget.max_reducer_cohort_entries_per_revision
            < operator_budget.max_reducer_cohort_entries_per_revision
    );
    assert!(
        trainer_budget.max_head_eval_reports_per_revision
            < operator_budget.max_head_eval_reports_per_revision
    );
    assert!(
        trainer_budget.max_metric_revisions_per_experiment
            < operator_budget.max_metric_revisions_per_experiment
    );
    assert!(
        trainer_budget.max_peer_window_detail_windows
            < operator_budget.max_peer_window_detail_windows
    );
}

#[test]
fn training_window_prunes_raw_metrics_archive_to_configured_budget() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-pruned-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .with_metrics_retention(MetricsRetentionConfig {
        max_peer_window_entries_per_revision: Some(1),
        ..MetricsRetentionConfig::default()
    })
    .spawn()
    .expect("runtime spawn");

    let mut running = running;
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );
    running
        .train_window_once(&experiment())
        .expect("first training window");
    running
        .train_window_once(&experiment())
        .expect("second training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn training_window_prunes_old_metric_revisions_to_configured_budget() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-revisions-pruned-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);
    let revision_a = experiment();
    let revision_b = mainnet().experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-2"),
    );

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .with_metrics_retention(MetricsRetentionConfig {
        max_peer_window_entries_per_revision: Some(2),
        max_metric_revisions_per_experiment: Some(1),
        ..MetricsRetentionConfig::default()
    })
    .spawn()
    .expect("runtime spawn");

    let mut running = running;
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );
    running
        .train_window_once(&revision_a)
        .expect("first revision training window");
    running
        .train_window_once(&revision_b)
        .expect("second revision training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].revision_id, revision_b.revision_id);

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn validation_persists_cohort_and_head_eval_metrics() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-validation-metrics-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let trainer_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-validation-metrics-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(validator_storage.clone())
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let experiment = experiment();
    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init validator genesis head");

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.5,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(trainer_storage)
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );
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
                .update_announcements
                .iter()
                .any(|announcement| {
                    announcement.update.delta_artifact_id == trained.artifact.artifact_id
                })
                && snapshot
                    .control_plane
                    .head_announcements
                    .iter()
                    .any(|announcement| announcement.head.head_id == trained.head.head_id)
        },
        "validator did not observe trainer artifacts",
    );

    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");

    let peer_windows =
        load_metric_artifacts::<PeerWindowMetrics>(&validator_storage, "peer-window-");
    let reducer_cohorts =
        load_metric_artifacts::<ReducerCohortMetrics>(&validator_storage, "reducer-cohort-");
    let head_eval_reports =
        load_metric_artifacts::<HeadEvalReport>(&validator_storage, "head-eval-");
    let eval_protocol_manifests = load_metric_artifacts::<StoredEvalProtocolManifestArtifact>(
        &validator_storage,
        "eval-protocol-",
    );

    assert_eq!(peer_windows.len(), 1);
    assert_eq!(reducer_cohorts.len(), 1);
    assert_eq!(head_eval_reports.len(), 1);
    assert_eq!(eval_protocol_manifests.len(), 1);
    assert_eq!(peer_windows[0].base_head_id, genesis_head.head_id);
    assert_eq!(
        reducer_cohorts[0].candidate_head_id,
        Some(validated.merged_head.head_id.clone())
    );
    assert_eq!(head_eval_reports[0].head_id, validated.merged_head.head_id);
    assert_eq!(
        head_eval_reports[0].base_head_id,
        validated.merged_head.parent_head_id
    );
    assert_eq!(
        eval_protocol_manifests[0].experiment_id,
        experiment.experiment_id.clone()
    );
    assert_eq!(
        eval_protocol_manifests[0].revision_id,
        experiment.revision_id.clone()
    );
    assert_eq!(
        eval_protocol_manifests[0].manifest.eval_protocol_id,
        head_eval_reports[0].eval_protocol_id
    );
    assert!(eval_protocol_manifests[0].captured_at <= Utc::now());
    wait_for(
        Duration::from_secs(5),
        || {
            validator_telemetry
                .snapshot()
                .control_plane
                .metrics_announcements
                .len()
                >= 2
        },
        "validator did not observe training and validation metrics announcements",
    );
    let metrics_events = &validator_telemetry
        .snapshot()
        .control_plane
        .metrics_announcements;
    assert_eq!(metrics_events.len(), 2);
    assert_eq!(
        metrics_events[0].event.kind,
        MetricsLiveEventKind::LedgerAppend
    );
    assert_eq!(
        metrics_events[1].event.kind,
        MetricsLiveEventKind::CatchupRefresh
    );
    assert_eq!(
        metrics_events[1].event.cursors[0].latest_head_id,
        Some(validated.merged_head.head_id.clone())
    );
    assert_eq!(
        metrics_events[1].event.cursors[0].latest_merge_window_id,
        Some(reducer_cohorts[0].merge_window_id.clone())
    );
    assert_eq!(
        metrics_events[1].overlay,
        experiment.overlay_set().expect("overlays").metrics
    );

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}
