//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
use std::{
    fs,
    path::Path,
    process::{Child, Command},
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

use anyhow::Context;
use burn_p2p::SwarmAddress;
use burn_p2p_testkit::multiprocess::{
    SyntheticNativeBackend, SyntheticProcessConfig, SyntheticProcessReport, SyntheticSoakConfig,
    SyntheticWorkloadKind, create_synthetic_runtime_dataset, run_synthetic_process_soak,
};
use tempfile::tempdir;

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(config_path: &Path) -> anyhow::Result<Self> {
        let child = Command::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node"))
            .arg(config_path)
            .spawn()?;
        Ok(Self { child })
    }

    fn wait_success(&mut self) -> anyhow::Result<()> {
        let status = self.child.wait()?;
        anyhow::ensure!(status.success(), "child exited with status {status}");
        Ok(())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn multiprocess_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("multiprocess test guard")
}

fn timeout_scale() -> u32 {
    std::env::var("BURN_P2P_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| {
            if std::env::var_os("CI").is_some() {
                3
            } else {
                1
            }
        })
        .max(1)
}

fn test_timeout(timeout: Duration) -> Duration {
    timeout.checked_mul(timeout_scale()).unwrap_or(timeout)
}

fn test_timeout_secs(seconds: u64) -> u64 {
    seconds.saturating_mul(u64::from(timeout_scale()))
}

#[test]
fn smoke_cluster_runs_across_processes() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let dataset_root = root.path().join("dataset");
    create_synthetic_runtime_dataset(&dataset_root)?;

    let validator_report_path = root.path().join("validator-report.json");
    let validator_config_path = root.path().join("validator.json");
    let validator_shutdown = root.path().join("validator.stop");
    let mut validator_config = SyntheticProcessConfig::validator(
        root.path().join("validator-storage"),
        dataset_root.clone(),
        validator_report_path.clone(),
    );
    validator_config.shutdown_sentinel = Some(validator_shutdown.clone());
    validator_config.startup_timeout_secs = test_timeout_secs(30);
    validator_config.sync_timeout_secs = test_timeout_secs(30);
    validator_config.merge_wait_timeout_secs = test_timeout_secs(45);
    fs::write(
        &validator_config_path,
        serde_json::to_vec_pretty(&validator_config)?,
    )?;

    let mut validator = ChildGuard::spawn(&validator_config_path)?;
    let validator_report =
        wait_for_report(&validator_report_path, Duration::from_secs(15), |report| {
            !report.listen_addresses.is_empty()
                && report.initialized_head_id.is_some()
                && report.local_peer_id.is_some()
        })?;
    let validator_addr = SwarmAddress::new(validator_report.listen_addresses[0].clone())?;

    let trainer_a_report_path = root.path().join("trainer-a-report.json");
    let trainer_a_config_path = root.path().join("trainer-a.json");
    let mut trainer_a_config = SyntheticProcessConfig::trainer(
        root.path().join("trainer-a-storage"),
        dataset_root.clone(),
        trainer_a_report_path.clone(),
        vec![validator_addr.clone()],
    );
    trainer_a_config.learning_rate = 1.0;
    fs::write(
        &trainer_a_config_path,
        serde_json::to_vec_pretty(&trainer_a_config)?,
    )?;

    let trainer_b_report_path = root.path().join("trainer-b-report.json");
    let trainer_b_config_path = root.path().join("trainer-b.json");
    let mut trainer_b_config = SyntheticProcessConfig::trainer(
        root.path().join("trainer-b-storage"),
        dataset_root,
        trainer_b_report_path.clone(),
        vec![validator_addr],
    );
    trainer_b_config.learning_rate = 0.5;
    fs::write(
        &trainer_b_config_path,
        serde_json::to_vec_pretty(&trainer_b_config)?,
    )?;

    let mut trainer_a = ChildGuard::spawn(&trainer_a_config_path)?;
    let mut trainer_b = ChildGuard::spawn(&trainer_b_config_path)?;
    trainer_a.wait_success()?;
    trainer_b.wait_success()?;

    let trainer_a_report = read_report(&trainer_a_report_path)?;
    let trainer_b_report = read_report(&trainer_b_report_path)?;
    let merged_validator =
        wait_for_report(&validator_report_path, Duration::from_secs(15), |report| {
            report.merge_count >= 1 && report.merged_head_id.is_some()
        })?;
    let merged_head_id = merged_validator
        .merged_head_id
        .clone()
        .expect("merged head id");
    let valid_parent_heads = [
        validator_report.initialized_head_id.clone(),
        trainer_a_report.trained_head_id.clone(),
        trainer_b_report.trained_head_id.clone(),
        Some(merged_head_id.clone()),
    ];
    assert!(
        valid_parent_heads.contains(&trainer_a_report.trained_parent_head_id),
        "unexpected trainer A parent head: {:?}",
        trainer_a_report.trained_parent_head_id
    );
    assert!(
        valid_parent_heads.contains(&trainer_b_report.trained_parent_head_id),
        "unexpected trainer B parent head: {:?}",
        trainer_b_report.trained_parent_head_id
    );
    assert_ne!(
        Some(merged_head_id.clone()),
        trainer_a_report.trained_head_id
    );
    assert_ne!(
        Some(merged_head_id.clone()),
        trainer_b_report.trained_head_id
    );

    fs::write(&validator_shutdown, b"stop")?;
    validator.wait_success()?;
    let final_report = read_report(&validator_report_path)?;
    assert_eq!(final_report.merged_head_id, Some(merged_head_id));
    assert!(final_report.receipt_count >= 1);
    assert!(final_report.merge_count >= 1);
    Ok(())
}

#[test]
fn validator_restart_restores_head_across_processes() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let dataset_root = root.path().join("dataset");
    create_synthetic_runtime_dataset(&dataset_root)?;
    let storage_root = root.path().join("validator-storage");

    let validator_report_path = root.path().join("validator-report.json");
    let validator_config_path = root.path().join("validator.json");
    let validator_shutdown = root.path().join("validator.stop");
    let mut validator_config = SyntheticProcessConfig::validator(
        storage_root.clone(),
        dataset_root.clone(),
        validator_report_path.clone(),
    );
    validator_config.shutdown_sentinel = Some(validator_shutdown.clone());
    validator_config.startup_timeout_secs = test_timeout_secs(30);
    validator_config.sync_timeout_secs = test_timeout_secs(30);
    validator_config.merge_wait_timeout_secs = test_timeout_secs(45);
    fs::write(
        &validator_config_path,
        serde_json::to_vec_pretty(&validator_config)?,
    )?;

    let mut validator = ChildGuard::spawn(&validator_config_path)?;
    let first_report =
        wait_for_report(&validator_report_path, Duration::from_secs(15), |report| {
            report.initialized_head_id.is_some() && report.local_peer_id.is_some()
        })?;
    let validator_addr = SwarmAddress::new(first_report.listen_addresses[0].clone())?;

    let trainer_report_path = root.path().join("trainer-report.json");
    let trainer_config_path = root.path().join("trainer.json");
    let trainer_config = SyntheticProcessConfig::trainer(
        root.path().join("trainer-storage"),
        dataset_root.clone(),
        trainer_report_path.clone(),
        vec![validator_addr],
    );
    let mut trainer_config = trainer_config;
    trainer_config.startup_timeout_secs = test_timeout_secs(30);
    trainer_config.sync_timeout_secs = test_timeout_secs(30);
    trainer_config.merge_wait_timeout_secs = test_timeout_secs(45);
    fs::write(
        &trainer_config_path,
        serde_json::to_vec_pretty(&trainer_config)?,
    )?;

    let mut trainer = ChildGuard::spawn(&trainer_config_path)?;
    trainer.wait_success()?;
    let trained_report = read_report(&trainer_report_path)?;
    let merged_before_restart =
        wait_for_report(&validator_report_path, Duration::from_secs(15), |report| {
            report.merged_head_id.is_some()
        })?;
    let merged_head_id = merged_before_restart
        .merged_head_id
        .clone()
        .expect("merged head id");
    assert_eq!(
        Some(merged_head_id.clone()),
        trained_report.observed_canonical_head_id
    );

    fs::write(&validator_shutdown, b"stop")?;
    validator.wait_success()?;
    fs::remove_file(&validator_shutdown)?;

    let restarted_report_path = root.path().join("validator-restarted-report.json");
    let restarted_config_path = root.path().join("validator-restarted.json");
    let restarted_shutdown = root.path().join("validator-restarted.stop");
    let mut restarted_config = SyntheticProcessConfig::validator(
        storage_root,
        dataset_root.clone(),
        restarted_report_path.clone(),
    );
    restarted_config.shutdown_sentinel = Some(restarted_shutdown.clone());
    restarted_config.startup_timeout_secs = test_timeout_secs(30);
    restarted_config.sync_timeout_secs = test_timeout_secs(30);
    restarted_config.merge_wait_timeout_secs = test_timeout_secs(90);
    fs::write(
        &restarted_config_path,
        serde_json::to_vec_pretty(&restarted_config)?,
    )?;

    let mut restarted_validator = ChildGuard::spawn(&restarted_config_path)?;
    let restarted_report =
        wait_for_report(&restarted_report_path, Duration::from_secs(15), |report| {
            report.initialized_head_id.is_some() && report.local_peer_id.is_some()
        })?;
    assert_eq!(
        restarted_report.initialized_head_id.as_deref(),
        Some(merged_head_id.as_str())
    );
    assert_eq!(restarted_report.local_peer_id, first_report.local_peer_id);

    let restarted_addr = SwarmAddress::new(restarted_report.listen_addresses[0].clone())?;
    let late_joiner_report_path = root.path().join("late-joiner-report.json");
    let late_joiner_config_path = root.path().join("late-joiner.json");
    let mut late_joiner_config = SyntheticProcessConfig::trainer(
        root.path().join("late-joiner-storage"),
        dataset_root,
        late_joiner_report_path.clone(),
        vec![restarted_addr],
    );
    late_joiner_config.learning_rate = 0.25;
    late_joiner_config.startup_timeout_secs = test_timeout_secs(30);
    late_joiner_config.sync_timeout_secs = test_timeout_secs(30);
    late_joiner_config.merge_wait_timeout_secs = test_timeout_secs(90);
    // This restart-path test is validating that the restarted validator recovers
    // and merges a fresh post-restart contribution. The late joiner only needs to
    // publish and remain reachable for artifact fetch, not block on its own
    // canonical-advance observation.
    late_joiner_config.wait_for_canonical_advance = false;
    fs::write(
        &late_joiner_config_path,
        serde_json::to_vec_pretty(&late_joiner_config)?,
    )?;

    let mut late_joiner = ChildGuard::spawn(&late_joiner_config_path)?;
    late_joiner.wait_success()?;
    let late_report = read_report(&late_joiner_report_path)?;
    assert_eq!(
        late_report.synced_head_id.as_deref(),
        Some(merged_head_id.as_str())
    );
    assert_eq!(
        late_report.trained_parent_head_id.as_deref(),
        Some(merged_head_id.as_str())
    );
    let baseline_receipt_count = restarted_report.receipt_count;
    let baseline_merge_count = restarted_report.merge_count;
    let post_restart_merge =
        wait_for_report(&restarted_report_path, Duration::from_secs(90), |report| {
            report.receipt_count > baseline_receipt_count
                || report.merge_count > baseline_merge_count
        })?;
    assert!(post_restart_merge.receipt_count > baseline_receipt_count);
    assert!(post_restart_merge.merge_count > baseline_merge_count);
    assert!(post_restart_merge.merged_head_id.is_some());

    fs::write(&restarted_shutdown, b"stop")?;
    restarted_validator.wait_success()?;
    Ok(())
}

#[test]
fn validator_can_fan_in_many_process_trainers() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let trainer_count = 3_u32;
    let root = tempdir()?;
    let dataset_root = root.path().join("dataset");
    create_synthetic_runtime_dataset(&dataset_root)?;

    let validator_report_path = root.path().join("validator-report.json");
    let validator_config_path = root.path().join("validator.json");
    let validator_shutdown = root.path().join("validator.stop");
    let mut validator_config = SyntheticProcessConfig::validator(
        root.path().join("validator-storage"),
        dataset_root.clone(),
        validator_report_path.clone(),
    );
    validator_config.shutdown_sentinel = Some(validator_shutdown.clone());
    fs::write(
        &validator_config_path,
        serde_json::to_vec_pretty(&validator_config)?,
    )?;

    let mut validator = ChildGuard::spawn(&validator_config_path)?;
    let validator_report =
        wait_for_report(&validator_report_path, Duration::from_secs(45), |report| {
            report.initialized_head_id.is_some() && !report.listen_addresses.is_empty()
        })?;
    let validator_addr = SwarmAddress::new(validator_report.listen_addresses[0].clone())?;

    let mut children = Vec::new();
    let start_sentinel = root.path().join("trainers.start");
    for index in 0..trainer_count {
        let report_path = root.path().join(format!("trainer-{index}-report.json"));
        let config_path = root.path().join(format!("trainer-{index}.json"));
        let mut config = SyntheticProcessConfig::trainer(
            root.path().join(format!("trainer-{index}-storage")),
            dataset_root.clone(),
            report_path,
            vec![validator_addr.clone()],
        );
        config.start_sentinel = Some(start_sentinel.clone());
        config.startup_timeout_secs = 30;
        config.sync_timeout_secs = 30;
        config.merge_wait_timeout_secs = 45;
        config.learning_rate = if index + 1 == trainer_count {
            1.0
        } else {
            ((index + 1) as f64) / 16.0
        };
        fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
        children.push(ChildGuard::spawn(&config_path)?);
    }

    for index in 0..trainer_count {
        let report_path = root.path().join(format!("trainer-{index}-report.json"));
        let _ = wait_for_report(&report_path, Duration::from_secs(60), |report| {
            report.synced_head_id.is_some()
        })?;
    }
    fs::write(&start_sentinel, b"start")?;

    for child in &mut children {
        child.wait_success()?;
    }
    let merged = wait_for_report(&validator_report_path, Duration::from_secs(90), |report| {
        report.merged_head_id.is_some()
    })?;
    assert!(merged.merge_count >= 1);

    fs::write(&validator_shutdown, b"stop")?;
    validator.wait_success()?;
    Ok(())
}

#[test]
fn native_process_soak_runner_reports_persistent_multi_window_progress() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak-multi-window"),
            workload_kind: SyntheticWorkloadKind::Scalar,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 1,
            trainer_window_count: 3,
            persistent_trainers: true,
            continuous_training: false,
            startup_timeout_secs: test_timeout_secs(30),
            poll_interval_ms: 50,
            sync_timeout_secs: test_timeout_secs(30),
            merge_wait_timeout_secs: test_timeout_secs(45),
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert_eq!(summary.trainer_count, 1);
    assert_eq!(summary.trainer_window_count, 3);
    assert_eq!(summary.completed_rounds, 3);
    assert_eq!(summary.trainer_process_count, 1);
    assert_eq!(summary.trainer_reports.len(), 1);
    assert!(summary.total_receipts >= 3);
    assert!(summary.total_merges >= 3);
    assert!(summary.elapsed_millis > 0);
    assert!(
        summary
            .trainer_reports
            .iter()
            .all(|report| report.trained_head_id.is_some() && report.window_timelines.len() >= 3)
    );
    assert_eq!(
        summary.total_receipts,
        summary.validator_report.receipt_count
    );
    assert_eq!(summary.total_merges, summary.validator_report.merge_count);
    let performance = summary
        .performance_summary
        .as_ref()
        .context("expected synthetic soak performance summary")?;
    assert!(performance.training.window_count >= 3);
    assert!(performance.training.accepted_work_units >= 1);
    let dynamics = summary
        .dynamics_summary
        .as_ref()
        .context("expected synthetic soak dynamics summary")?;
    assert!(dynamics.peer_startup_latency_ms.sample_count >= 2);
    assert!(dynamics.trainer_training_latency_ms.sample_count >= 3);
    assert!(dynamics.receipt_rate_per_sec > 0.0);
    Ok(())
}

#[test]
fn native_process_soak_runner_supports_continuous_speculative_training() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak-continuous"),
            workload_kind: SyntheticWorkloadKind::Scalar,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 1,
            trainer_window_count: 3,
            persistent_trainers: true,
            continuous_training: true,
            startup_timeout_secs: test_timeout_secs(30),
            poll_interval_ms: 50,
            sync_timeout_secs: test_timeout_secs(30),
            merge_wait_timeout_secs: test_timeout_secs(45),
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert!(summary.continuous_training);
    assert_eq!(summary.completed_rounds, 3);
    assert_eq!(summary.trainer_process_count, 1);
    assert_eq!(summary.trainer_reports.len(), 1);
    assert!(summary.total_receipts >= 1);
    assert!(summary.total_merges >= 1);
    assert_eq!(summary.trainer_reports[0].window_timelines.len(), 3);
    let performance = summary
        .performance_summary
        .as_ref()
        .context("expected speculative synthetic soak performance summary")?;
    assert!(performance.training.window_count >= 3);
    Ok(())
}

#[cfg(feature = "native-gpu-probe")]
#[test]
fn burn_native_process_soak_runner_reports_persistent_multi_window_progress() -> anyhow::Result<()>
{
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak-burn-multi-window"),
            workload_kind: SyntheticWorkloadKind::BurnLinear,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 1,
            trainer_window_count: 2,
            persistent_trainers: true,
            continuous_training: false,
            startup_timeout_secs: test_timeout_secs(30),
            poll_interval_ms: 50,
            sync_timeout_secs: test_timeout_secs(30),
            merge_wait_timeout_secs: test_timeout_secs(45),
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert_eq!(summary.workload_kind, SyntheticWorkloadKind::BurnLinear);
    assert_eq!(summary.trainer_backend, SyntheticNativeBackend::NdArray);
    assert_eq!(summary.completed_rounds, 2);
    assert!(summary.total_receipts >= 2);
    assert!(summary.total_merges >= 2);
    let performance = summary
        .performance_summary
        .as_ref()
        .context("expected burn synthetic soak performance summary")?;
    assert!(performance.training.window_count >= 2);
    Ok(())
}

#[cfg(feature = "native-gpu-probe")]
#[test]
fn burn_native_process_soak_runner_supports_persistent_multi_trainer_rounds() -> anyhow::Result<()>
{
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak-burn-persistent-multi-trainer"),
            workload_kind: SyntheticWorkloadKind::BurnLinear,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 3,
            trainer_window_count: 2,
            persistent_trainers: true,
            continuous_training: false,
            startup_timeout_secs: test_timeout_secs(30),
            poll_interval_ms: 50,
            sync_timeout_secs: test_timeout_secs(45),
            merge_wait_timeout_secs: test_timeout_secs(45),
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert_eq!(summary.workload_kind, SyntheticWorkloadKind::BurnLinear);
    assert_eq!(summary.trainer_count, 3);
    assert_eq!(summary.trainer_window_count, 2);
    assert_eq!(summary.completed_rounds, 2);
    assert_eq!(summary.trainer_process_count, 3);
    assert!(summary.total_receipts >= 2);
    assert!(summary.total_merges >= 2);
    let performance = summary
        .performance_summary
        .as_ref()
        .context("expected burn persistent multi-trainer soak performance summary")?;
    assert!(performance.training.window_count >= 2);
    Ok(())
}

#[test]
fn native_process_soak_runner_reports_concurrent_round_progress() -> anyhow::Result<()> {
    let _guard = multiprocess_test_guard();
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak"),
            workload_kind: SyntheticWorkloadKind::Scalar,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 3,
            trainer_window_count: 2,
            persistent_trainers: false,
            continuous_training: false,
            startup_timeout_secs: test_timeout_secs(30),
            poll_interval_ms: 50,
            sync_timeout_secs: test_timeout_secs(45),
            merge_wait_timeout_secs: test_timeout_secs(45),
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert!(summary.validator_report.merge_count >= 2);
    assert_eq!(summary.completed_rounds, 2);
    assert_eq!(summary.trainer_count, 3);
    assert_eq!(summary.trainer_window_count, 2);
    assert_eq!(summary.trainer_process_count, 6);
    assert_eq!(summary.trainer_reports.len(), 6);
    assert!(summary.total_receipts >= 2);
    assert!(summary.total_merges >= 2);
    assert!(summary.elapsed_millis > 0);
    assert!(
        summary
            .trainer_reports
            .iter()
            .all(|report| report.trained_head_id.is_some())
    );
    assert!(summary.total_receipts >= summary.total_merges);
    let performance = summary
        .performance_summary
        .as_ref()
        .context("expected synthetic soak performance summary")?;
    assert!(performance.total_peer_count >= 4);
    let dynamics = summary
        .dynamics_summary
        .as_ref()
        .context("expected synthetic soak dynamics summary")?;
    assert!(dynamics.trainer_canonical_advance_latency_ms.sample_count >= 3);
    assert!(dynamics.merge_rate_per_sec > 0.0);
    Ok(())
}

fn wait_for_report(
    path: &Path,
    timeout: Duration,
    predicate: impl Fn(&SyntheticProcessReport) -> bool,
) -> anyhow::Result<SyntheticProcessReport> {
    let deadline = Instant::now() + test_timeout(timeout);
    let mut last_report = None;
    while Instant::now() < deadline {
        if path.exists() {
            let report = read_report(path)?;
            last_report = Some(report.clone());
            if predicate(&report) {
                return Ok(report);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    if let Some(report) = last_report {
        Err(anyhow::anyhow!(
            "timed out waiting for report {} (last status: {:?})",
            path.display(),
            report
        ))
    } else {
        Err(anyhow::anyhow!(
            "timed out waiting for report {}",
            path.display()
        ))
    }
}

fn read_report(path: &Path) -> anyhow::Result<SyntheticProcessReport> {
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}
