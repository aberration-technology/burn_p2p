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
            BrowserExecutionSummary, CoreMnistSummary, DemoAssessmentSummary,
            ExperimentRunSummary, MnistCorrectnessSummary, MnistRunExport, NodeSummary,
            ResilienceDrillSummary,
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
    let browser_execution =
        browser_execution_summary(run, &browser_roles, run.browser_probe_summary.as_ref())?;
    let resilience = ResilienceDrillSummary {
        restarted_trainer_label: run.restarted_trainer_label.clone(),
        trainer_restart_reconnected: run.trainer_restart_reconnected,
        trainer_restart_resumed_training: run.trainer_restart_resumed_training,
    };
    let assessment = DemoAssessmentSummary {
        live_native_training: true,
        live_browser_training: browser_execution.trainer_runtime_and_wasm_training_coherent,
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
            "when the hidden live-browser hook is enabled, the mnist e2e run keeps the native fleet alive while a browser burn/webgpu worker enrolls against the same browser edge, trains on the leased shard slice, and submits a live receipt".into(),
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
        browser_execution,
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

fn browser_execution_summary(
    run: &CoreMnistRun,
    browser_roles: &[crate::correctness::export::BrowserRoleExerciseSummary],
    browser_probe_summary: Option<&serde_json::Value>,
) -> anyhow::Result<BrowserExecutionSummary> {
    let Some(browser_probe_summary) = browser_probe_summary else {
        return Ok(BrowserExecutionSummary {
            live_browser_training: false,
            browser_latency_emulated: false,
            slower_profile_increased_total_time: false,
            same_network_context: false,
            same_experiment_context: false,
            same_revision_context: false,
            same_head_context: false,
            same_lease_context: false,
            same_leased_microshards: false,
            session_enrolled: false,
            receipt_submission_accepted: false,
            runtime_state: None,
            transport: None,
            active_assignment: false,
            emitted_receipt_id: None,
            accepted_receipt_ids: Vec::new(),
            trainer_runtime_and_wasm_training_coherent: false,
            notes: vec![
                "live browser participation was not requested for this demo run".into(),
            ],
        });
    };

    let network_id = run.network_manifest.network_id.as_str();
    let experiment_id = run.baseline_head.experiment_id.as_str();
    let revision_id = run.baseline_head.revision_id.as_str();
    let selected_head_id = run.baseline_head.head_id.as_str();
    let lease = &run
        .baseline_outcomes
        .first()
        .context("mnist demo did not produce a baseline trainer lease")?
        .training
        .lease;
    let browser_trainer = browser_roles
        .iter()
        .find(|role| role.role == "browser-trainer-wgpu")
        .context("mnist correctness summary missing browser trainer role drill")?;

    let probe_network_id = browser_probe_summary
        .pointer("/run_context/network_id")
        .and_then(serde_json::Value::as_str)
        .context("browser probe summary missing run_context.network_id")?;
    let probe_experiment_id = browser_probe_summary
        .pointer("/run_context/experiment_id")
        .and_then(serde_json::Value::as_str)
        .context("browser probe summary missing run_context.experiment_id")?;
    let probe_revision_id = browser_probe_summary
        .pointer("/run_context/revision_id")
        .and_then(serde_json::Value::as_str)
        .context("browser probe summary missing run_context.revision_id")?;
    let probe_selected_head_id = browser_probe_summary
        .pointer("/run_context/selected_head_id")
        .and_then(serde_json::Value::as_str)
        .context("browser probe summary missing run_context.selected_head_id")?;
    let probe_lease_id = browser_probe_summary
        .pointer("/run_context/lease_id")
        .and_then(serde_json::Value::as_str)
        .context("browser probe summary missing run_context.lease_id")?;
    let probe_leased_microshards = browser_probe_summary
        .get("leased_microshards")
        .and_then(serde_json::Value::as_array)
        .context("browser probe summary missing leased_microshards")?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();

    let accepted_receipt_ids = browser_probe_summary
        .pointer("/browser_execution/accepted_receipt_ids")
        .and_then(serde_json::Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let emitted_receipt_id = browser_probe_summary
        .pointer("/browser_execution/emitted_receipt_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let runtime_state = browser_probe_summary
        .pointer("/browser_execution/runtime_state")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let transport = browser_probe_summary
        .pointer("/browser_execution/transport")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let active_assignment = browser_probe_summary
        .pointer("/browser_execution/active_assignment")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let live_browser_training = browser_probe_summary
        .pointer("/browser_execution/live_browser_training")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let browser_latency_emulated = browser_probe_summary
        .pointer("/browser_execution/browser_latency_emulated")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let slower_profile_increased_total_time = browser_probe_summary
        .pointer("/browser_execution/slower_profile_increased_total_time")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let session_enrolled = browser_probe_summary
        .pointer("/browser_execution/session_enrolled")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let receipt_submission_accepted = browser_probe_summary
        .pointer("/browser_execution/receipt_submission_accepted")
        .and_then(serde_json::Value::as_bool)
        == Some(true);

    let same_network_context = probe_network_id == network_id;
    let same_experiment_context = probe_experiment_id == experiment_id;
    let same_revision_context = probe_revision_id == revision_id;
    let same_head_context = probe_selected_head_id == selected_head_id;
    let same_lease_context = probe_lease_id == lease.lease_id.as_str();
    let same_leased_microshards = probe_leased_microshards
        == lease
            .microshards
            .iter()
            .map(|microshard| microshard.as_str())
            .collect::<Vec<_>>();
    let trainer_runtime_and_wasm_training_coherent = live_browser_training
        && same_network_context
        && same_experiment_context
        && same_revision_context
        && same_head_context
        && same_lease_context
        && same_leased_microshards
        && session_enrolled
        && receipt_submission_accepted
        && browser_trainer.command_completed
        && browser_trainer.active_assignment
        && active_assignment;

    Ok(BrowserExecutionSummary {
        live_browser_training,
        browser_latency_emulated,
        slower_profile_increased_total_time,
        same_network_context,
        same_experiment_context,
        same_revision_context,
        same_head_context,
        same_lease_context,
        same_leased_microshards,
        session_enrolled,
        receipt_submission_accepted,
        runtime_state,
        transport,
        active_assignment,
        emitted_receipt_id,
        accepted_receipt_ids,
        trainer_runtime_and_wasm_training_coherent,
        notes: vec![
            "the browser worker enrolled against the live browser edge while the native mnist fleet was still running".into(),
            "browser burn/webgpu training consumed the same lease-scoped shard slice from the prepared dataset origin".into(),
            "browser shard bytes remained http-backed; shard transport was not exercised over the peer overlay".into(),
        ],
    })
}
