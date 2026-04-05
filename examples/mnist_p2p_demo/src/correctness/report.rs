use anyhow::Context;
use chrono::Utc;

use crate::{
    core::{
        CoreMnistRun, dynamics_summary, experiment_directory_entries, leaderboard_entries,
        metric_float, trainer_lease_summary,
    },
    correctness::{
        browser::{browser_scenarios, exercise_browser_roles, probe_browser_http_shard_fetch},
        export::{
            CoreMnistSummary, DemoAssessmentSummary, ExperimentRunSummary, MnistCorrectnessSummary,
            MnistRunExport, NodeSummary, ResilienceDrillSummary,
        },
    },
};

pub(crate) fn build_run_export(run: &CoreMnistRun) -> anyhow::Result<MnistRunExport> {
    let baseline_accuracy = metric_float(&run.baseline_head.metrics, "accuracy");
    let low_lr_accuracy = metric_float(&run.low_lr_head.metrics, "accuracy");
    let baseline_initial_accuracy = metric_float(&run.baseline_genesis.metrics, "accuracy");
    let low_lr_initial_accuracy = metric_float(&run.low_lr_genesis.metrics, "accuracy");

    let directory_entries = experiment_directory_entries(
        &run.network_manifest,
        &run.supported_workload,
        &run.prepared_data,
        [&run.baseline_head, &run.low_lr_head],
    );
    let leaderboard = leaderboard_entries(
        [
            ("trainer-a1", run.node_records[3].peer_id.clone()),
            ("trainer-a2", run.node_records[4].peer_id.clone()),
            ("trainer-b", run.node_records[5].peer_id.clone()),
            ("validator", run.node_records[1].peer_id.clone()),
            ("viewer", run.node_records[2].peer_id.clone()),
            ("helper", run.node_records[0].peer_id.clone()),
            ("trainer-late", run.node_records[6].peer_id.clone()),
        ],
        &run.baseline_outcomes,
        &run.low_lr_outcomes,
        &run.merge_certificates,
    );
    let browser_scenarios = browser_scenarios(
        &run.network_manifest,
        &run.release_manifest,
        directory_entries,
        vec![
            run.baseline_genesis.clone(),
            run.baseline_head.clone(),
            run.low_lr_genesis.clone(),
            run.low_lr_head.clone(),
        ],
        leaderboard,
        run.metrics_catchup.clone(),
    );
    let browser_roles = exercise_browser_roles(
        &run.network_manifest,
        &run.release_manifest,
        &experiment_directory_entries(
            &run.network_manifest,
            &run.supported_workload,
            &run.prepared_data,
            [&run.baseline_head, &run.low_lr_head],
        ),
        &[run.baseline_head.clone(), run.low_lr_head.clone()],
    )?;
    let browser_dataset_access = probe_browser_http_shard_fetch(
        &run.prepared_data.dataset_root,
        &run.prepared_data,
        &run.baseline_outcomes
            .first()
            .context("mnist demo did not produce a baseline trainer lease")?
            .training
            .lease,
    )?;
    let resilience = ResilienceDrillSummary {
        restarted_trainer_label: run.restarted_trainer_label.clone(),
        trainer_restart_reconnected: run.trainer_restart_reconnected,
        trainer_restart_resumed_training: run.trainer_restart_resumed_training,
    };
    let assessment = DemoAssessmentSummary {
        live_native_training: true,
        live_browser_training: false,
        browser_runtime_roles_exercised: browser_roles.iter().all(|role| {
            role.active_assignment
                && (!role.role.contains("trainer") && !role.role.contains("verifier")
                    || role.command_completed)
        }),
        browser_dataset_transport: browser_dataset_access.upstream_mode.clone(),
        browser_shards_distributed_over_p2p: browser_dataset_access.shards_distributed_over_p2p,
        notes: vec![
            "native trainer and validator nodes run real burn mnist training on one machine".into(),
            "browser viewer, verifier, and trainer roles are exercised through the browser runtime state machine and portal surfaces".into(),
            "the core demo keeps browser runtime drills lightweight; xtask correctness runs the live burn/webgpu wasm probe separately".into(),
            "dataset shards are fetched from the prepared http dataset origin lease-by-lease; they are not gossiped over the peer overlay".into(),
        ],
    };
    let shard_assignments_are_distinct = run
        .baseline_outcomes
        .first()
        .zip(run.baseline_outcomes.get(1))
        .map(|(left, right)| left.training.lease.microshards != right.training.lease.microshards)
        .unwrap_or(false);

    let summary = CoreMnistSummary {
        generated_at: Utc::now(),
        network_id: run.network_manifest.network_id.as_str().into(),
        dataset_root: run.prepared_data.dataset_root.display().to_string(),
        experiments: vec![
            ExperimentRunSummary {
                experiment_id: run.baseline_head.experiment_id.as_str().into(),
                revision_id: run.baseline_head.revision_id.as_str().into(),
                display_name: "MNIST baseline".into(),
                trainer_labels: vec!["trainer-a1".into(), "trainer-a2".into()],
                initial_head_id: run.baseline_genesis.head_id.clone(),
                final_head_id: run.baseline_head.head_id.clone(),
                initial_accuracy: baseline_initial_accuracy,
                final_accuracy: baseline_accuracy,
                final_loss: metric_float(&run.baseline_head.metrics, "loss"),
                digit_zero_accuracy: metric_float(
                    &run.baseline_head.metrics,
                    "digit_zero_accuracy",
                ),
                merge_count: run
                    .merge_certificates
                    .iter()
                    .filter(|certificate| {
                        certificate.experiment_id == run.baseline_head.experiment_id
                    })
                    .count(),
                accepted_receipt_count: run.baseline_outcomes.len(),
            },
            ExperimentRunSummary {
                experiment_id: run.low_lr_head.experiment_id.as_str().into(),
                revision_id: run.low_lr_head.revision_id.as_str().into(),
                display_name: "MNIST lower learning rate".into(),
                trainer_labels: vec!["trainer-b".into()],
                initial_head_id: run.low_lr_genesis.head_id.clone(),
                final_head_id: run.low_lr_head.head_id.clone(),
                initial_accuracy: low_lr_initial_accuracy,
                final_accuracy: low_lr_accuracy,
                final_loss: metric_float(&run.low_lr_head.metrics, "loss"),
                digit_zero_accuracy: metric_float(&run.low_lr_head.metrics, "digit_zero_accuracy"),
                merge_count: run
                    .merge_certificates
                    .iter()
                    .filter(|certificate| {
                        certificate.experiment_id == run.low_lr_head.experiment_id
                    })
                    .count(),
                accepted_receipt_count: run.low_lr_outcomes.len(),
            },
        ],
        nodes: run
            .node_records
            .iter()
            .map(|record| NodeSummary {
                label: record.label.clone(),
                peer_id: record.peer_id.clone(),
                roles: record.roles.clone(),
                storage_root: record.storage_root.display().to_string(),
            })
            .collect(),
    };

    let correctness = MnistCorrectnessSummary {
        generated_at: Utc::now(),
        baseline_outperformed_low_lr: baseline_accuracy > low_lr_accuracy,
        late_joiner_synced_checkpoint: run.late_joiner_synced_checkpoint,
        shard_assignments_are_distinct,
        trainer_leases: run
            .baseline_outcomes
            .iter()
            .chain(run.low_lr_outcomes.iter())
            .map(|outcome| trainer_lease_summary(outcome, &run.prepared_data))
            .collect(),
        browser_roles,
        browser_dataset_access,
        resilience,
        dynamics: dynamics_summary(
            &run.baseline_outcomes,
            &run.low_lr_outcomes,
            baseline_initial_accuracy,
            baseline_accuracy,
            low_lr_initial_accuracy,
            low_lr_accuracy,
            &run.prepared_data,
        ),
        assessment,
    };

    Ok(MnistRunExport {
        summary,
        correctness,
        browser_scenarios,
    })
}
