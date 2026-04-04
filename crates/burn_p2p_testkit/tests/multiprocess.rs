//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
use std::{
    fs,
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use burn_p2p::SwarmAddress;
use burn_p2p_testkit::multiprocess::{
    SyntheticProcessConfig, SyntheticProcessReport, SyntheticSoakConfig,
    create_synthetic_runtime_dataset, run_synthetic_process_soak,
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

#[test]
fn smoke_cluster_runs_across_processes() -> anyhow::Result<()> {
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

    fs::write(&restarted_shutdown, b"stop")?;
    restarted_validator.wait_success()?;
    Ok(())
}

#[test]
fn validator_can_fan_in_many_process_trainers() -> anyhow::Result<()> {
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
        wait_for_report(&validator_report_path, Duration::from_secs(15), |report| {
            report.initialized_head_id.is_some() && !report.listen_addresses.is_empty()
        })?;
    let validator_addr = SwarmAddress::new(validator_report.listen_addresses[0].clone())?;

    let mut children = Vec::new();
    for index in 0..8_u32 {
        let report_path = root.path().join(format!("trainer-{index}-report.json"));
        let config_path = root.path().join(format!("trainer-{index}.json"));
        let mut config = SyntheticProcessConfig::trainer(
            root.path().join(format!("trainer-{index}-storage")),
            dataset_root.clone(),
            report_path,
            vec![validator_addr.clone()],
        );
        config.learning_rate = if index == 7 {
            1.0
        } else {
            ((index + 1) as f64) / 16.0
        };
        fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
        children.push(ChildGuard::spawn(&config_path)?);
    }

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
fn native_process_soak_runner_reports_multi_window_progress() -> anyhow::Result<()> {
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak-multi-window"),
            trainer_count: 1,
            trainer_window_count: 3,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
        },
        Path::new(env!("CARGO_BIN_EXE_burn-p2p-testkit-node")),
    )?;

    assert_eq!(summary.trainer_count, 1);
    assert_eq!(summary.trainer_window_count, 3);
    assert_eq!(summary.completed_rounds, 3);
    assert_eq!(summary.trainer_process_count, 3);
    assert_eq!(summary.trainer_reports.len(), 3);
    assert!(summary.total_receipts >= 3);
    assert!(summary.total_merges >= 3);
    assert!(summary.elapsed_millis > 0);
    assert!(
        summary
            .trainer_reports
            .iter()
            .all(|report| report.trained_head_id.is_some())
    );
    assert_eq!(
        summary.total_receipts,
        summary.validator_report.receipt_count
    );
    assert_eq!(summary.total_merges, summary.validator_report.merge_count);
    Ok(())
}

#[test]
fn native_process_soak_runner_reports_concurrent_round_progress() -> anyhow::Result<()> {
    let root = tempdir()?;
    let summary = run_synthetic_process_soak(
        &SyntheticSoakConfig {
            root: root.path().join("soak"),
            trainer_count: 3,
            trainer_window_count: 2,
            startup_timeout_secs: 20,
            poll_interval_ms: 50,
            sync_timeout_secs: 30,
            merge_wait_timeout_secs: 30,
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
    Ok(())
}

fn wait_for_report(
    path: &Path,
    timeout: Duration,
    predicate: impl Fn(&SyntheticProcessReport) -> bool,
) -> anyhow::Result<SyntheticProcessReport> {
    let deadline = Instant::now() + timeout;
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
