use std::collections::BTreeSet;

use anyhow::Context;
use burn_p2p::PeerRole;
use burn_p2p_metrics::derive_network_performance_summary;
use chrono::Utc;

use crate::{
    core::{
        CoreMnistRun, DynamicsSummaryInput, dynamics_summary, experiment_directory_entries,
        leaderboard_entries, leaderboard_node_pairs, metric_float, node_peer_id,
        trainer_lease_summary,
    },
    correctness::{
        browser::{browser_scenarios, exercise_browser_roles, probe_browser_http_shard_fetch},
        export::{
            BrowserDatasetAccessSummary, BrowserDeviceLimitSummary, BrowserExecutionSummary,
            CoreMnistSummary, DemoAssessmentSummary, DeviceLimitExerciseSummary,
            ExperimentRunSummary, MnistCorrectnessSummary, MnistPerformanceSummary, MnistRunExport,
            NativeNodeDeviceLimitSummary, NodeSummary, ResilienceDrillSummary,
            TopologyExerciseSummary,
        },
        live_browser::LiveBrowserProbeManifest,
    },
};

#[derive(Clone, Copy, Debug)]
struct VariantAccuracyComparison {
    baseline_outperformed_low_lr: bool,
    baseline_accuracy_delta_vs_low_lr: f64,
    baseline_accuracy_tolerance_vs_low_lr: f64,
    baseline_loss_advantage_vs_low_lr: f64,
}

fn compare_baseline_vs_low_lr(
    eval_examples: usize,
    baseline_accuracy: f64,
    baseline_loss: f64,
    low_lr_accuracy: f64,
    low_lr_loss: f64,
) -> VariantAccuracyComparison {
    let accuracy_delta = baseline_accuracy - low_lr_accuracy;
    let accuracy_tolerance = if eval_examples == 0 {
        0.0
    } else {
        2.0 / eval_examples as f64
    };
    let loss_advantage = low_lr_loss - baseline_loss;
    VariantAccuracyComparison {
        baseline_outperformed_low_lr: accuracy_delta > 0.0
            || (accuracy_delta >= -accuracy_tolerance && loss_advantage > 0.0),
        baseline_accuracy_delta_vs_low_lr: accuracy_delta,
        baseline_accuracy_tolerance_vs_low_lr: accuracy_tolerance,
        baseline_loss_advantage_vs_low_lr: loss_advantage,
    }
}

pub(crate) fn build_run_export(run: &CoreMnistRun) -> anyhow::Result<MnistRunExport> {
    let baseline_accuracy = metric_float(&run.baseline_head.metrics, "accuracy");
    let low_lr_accuracy = metric_float(&run.low_lr_head.metrics, "accuracy");
    let baseline_initial_accuracy = metric_float(&run.baseline_genesis.metrics, "accuracy");
    let low_lr_initial_accuracy = metric_float(&run.low_lr_genesis.metrics, "accuracy");
    let baseline_loss = metric_float(&run.baseline_head.metrics, "loss");
    let low_lr_loss = metric_float(&run.low_lr_head.metrics, "loss");
    let accuracy_comparison = compare_baseline_vs_low_lr(
        run.prepared_data.eval_records.len(),
        baseline_accuracy,
        baseline_loss,
        low_lr_accuracy,
        low_lr_loss,
    );

    let directory_entries = experiment_directory_entries(
        &run.network_manifest,
        &run.supported_workload,
        &run.prepared_data,
        [&run.baseline_head, &run.low_lr_head],
    );
    let leaderboard = leaderboard_entries(
        &leaderboard_node_pairs(&run.node_records)?,
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
    let browser_dataset_access = browser_dataset_access_summary(run)?;
    let browser_execution =
        browser_execution_summary(run, &browser_roles, run.browser_probe_summary.as_ref())?;
    let device_limits = device_limits_summary(run, run.browser_probe_summary.as_ref());
    let topology = topology_summary(run)?;
    let overall_performance = derive_network_performance_summary(
        &run.peer_window_metrics,
        &run.reducer_cohort_metrics,
        &run.head_eval_reports,
    )
    .context("mnist demo missing network performance summary")?;
    let mut performance_by_experiment = run
        .metrics_catchup
        .iter()
        .filter_map(|bundle| bundle.snapshot.performance_summary.clone())
        .collect::<Vec<_>>();
    performance_by_experiment.sort_by(|left, right| {
        left.experiment_id
            .as_ref()
            .map(|value| value.as_str())
            .cmp(&right.experiment_id.as_ref().map(|value| value.as_str()))
            .then_with(|| {
                left.revision_id
                    .as_ref()
                    .map(|value| value.as_str())
                    .cmp(&right.revision_id.as_ref().map(|value| value.as_str()))
            })
    });
    let performance = MnistPerformanceSummary {
        overall: overall_performance,
        per_experiment: performance_by_experiment,
    };
    let resilience = ResilienceDrillSummary {
        executed: run.resilience_drills_executed,
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
        split_topology_roles_exercised: topology.all_non_seed_nodes_bootstrap_via_seed
            && topology.mesh_fanout_beyond_seed_observed
            && topology.dedicated_reducer_participated
            && topology.aggregate_proposals_only_from_dedicated_reducer
            && topology.reducer_load_publishers_within_reducer_validation_tier
            && topology.reduction_attestations_only_from_validators
            && topology.merge_certificates_only_from_validators
            && topology.validators_observed_validation_quorum,
        browser_dataset_transport: browser_dataset_access.upstream_mode.clone(),
        browser_shards_distributed_over_p2p: browser_dataset_access.shards_distributed_over_p2p,
        notes: {
            let mut notes = vec![
            "native trainer and validator nodes run real burn mnist training on one machine".into(),
            "the active topology uses a helper seed for ingress, a dedicated reducer for aggregate proposals, and separate validators for attestation and promotion".into(),
            "browser viewer, verifier, and trainer roles are exercised through the browser runtime state machine and portal surfaces".into(),
            "when the hidden live-browser hook is enabled, the mnist e2e run keeps the native fleet alive while a browser burn/webgpu worker enrolls against the same browser edge, trains on the leased shard slice, and submits a live receipt".into(),
            ];
            if browser_dataset_access.upstream_mode == "p2p-signed-peer-bundle" {
                notes.push("the browser probe consumes a lease-scoped peer bundle materialized from a p2p-synced artifact instead of fetching dataset bytes back through the edge route".into());
            } else {
                notes.push("the browser edge now serves a lease-scoped browser data bundle that was first synced over the peer artifact transport from a live native peer".into());
            }
            if !run.resilience_drills_executed {
                notes.push("the hosted bounded CI path skips the restart and late-joiner resilience drills; run the full mnist profile locally or on a heavier runner to exercise those flows".into());
            }
            notes
        },
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
                final_loss: baseline_loss,
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
                final_loss: low_lr_loss,
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
        baseline_outperformed_low_lr: accuracy_comparison.baseline_outperformed_low_lr,
        baseline_accuracy_delta_vs_low_lr: accuracy_comparison.baseline_accuracy_delta_vs_low_lr,
        baseline_accuracy_tolerance_vs_low_lr: accuracy_comparison
            .baseline_accuracy_tolerance_vs_low_lr,
        baseline_loss_advantage_vs_low_lr: accuracy_comparison.baseline_loss_advantage_vs_low_lr,
        late_joiner_synced_checkpoint: run.late_joiner_synced_checkpoint,
        shard_assignments_are_distinct,
        phase_timeline: run.phase_timeline.clone(),
        device_limits,
        topology,
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
        dynamics: dynamics_summary(DynamicsSummaryInput {
            baseline_outcomes: &run.baseline_outcomes,
            low_lr_outcomes: &run.low_lr_outcomes,
            baseline_initial_accuracy,
            baseline_accuracy,
            low_lr_initial_accuracy,
            low_lr_accuracy,
            prepared_data: &run.prepared_data,
            performance,
        }),
        assessment,
    };

    Ok(MnistRunExport {
        summary,
        correctness,
        browser_scenarios,
    })
}

pub fn apply_live_browser_probe_results(
    export: &mut MnistRunExport,
    probe_manifest: &LiveBrowserProbeManifest,
    browser_probe_summary: &serde_json::Value,
) -> anyhow::Result<()> {
    if let Some(value) = browser_probe_summary.get("browser_dataset_access") {
        export.correctness.browser_dataset_access = serde_json::from_value(value.clone())
            .context("decode browser dataset access from live browser probe summary")?;
    }

    let browser_trainer = export
        .correctness
        .browser_roles
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
    let head_artifact_transport = browser_probe_summary
        .pointer("/browser_execution/head_artifact_transport")
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

    let same_network_context = probe_network_id == probe_manifest.network_id;
    let same_experiment_context = probe_experiment_id == probe_manifest.experiment_id;
    let same_revision_context = probe_revision_id == probe_manifest.revision_id;
    let same_head_context = probe_selected_head_id == probe_manifest.selected_head_id;
    let same_lease_context = probe_lease_id == probe_manifest.lease_id;
    let same_leased_microshards = probe_leased_microshards
        == probe_manifest
            .leased_microshards
            .iter()
            .map(String::as_str)
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

    export.correctness.browser_execution = BrowserExecutionSummary {
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
        head_artifact_transport: head_artifact_transport.clone(),
        active_assignment,
        emitted_receipt_id,
        accepted_receipt_ids,
        trainer_runtime_and_wasm_training_coherent,
        notes: {
            let mut notes = vec![
                "the browser worker enrolled against the live browser edge while the native mnist fleet was still running".into(),
                "browser burn/webgpu training consumed the same lease-scoped shard slice exported into a browser data bundle".into(),
            ];
            if probe_manifest.peer_dataset_root.is_some() {
                notes.push("the browser data bundle was materialized locally from the p2p-synced artifact, so dataset bytes did not need to flow back through the edge route".into());
            } else {
                notes.push("the browser data bundle was synced from a native peer over the artifact control plane before the browser edge served it".into());
            }
            if head_artifact_transport.as_deref() == Some("peer-native-swarm") {
                notes.push("the browser runtime reconstructed the active head artifact through manifest and chunk requests over the browser-native peer artifact swarm before any edge download route was needed".into());
            } else if head_artifact_transport.as_deref() == Some("peer-native") {
                notes.push("the browser runtime fetched the active head artifact through the configured peer-native transport before any edge download route was needed".into());
            } else if head_artifact_transport.as_deref() == Some("edge-download-ticket") {
                notes.push("the browser runtime still fell back to the edge artifact download ticket for the active head artifact".into());
            }
            notes
        },
    };
    export.correctness.device_limits.browser =
        browser_device_limit_summary(Some(browser_probe_summary));
    export.correctness.assessment.live_browser_training = export
        .correctness
        .browser_execution
        .trainer_runtime_and_wasm_training_coherent;
    export.correctness.assessment.browser_dataset_transport = export
        .correctness
        .browser_dataset_access
        .upstream_mode
        .clone();
    export
        .correctness
        .assessment
        .browser_shards_distributed_over_p2p = export
        .correctness
        .browser_dataset_access
        .shards_distributed_over_p2p;
    export.correctness.generated_at = Utc::now();
    Ok(())
}

fn device_limits_summary(
    run: &CoreMnistRun,
    browser_probe_summary: Option<&serde_json::Value>,
) -> DeviceLimitExerciseSummary {
    let active_trainer_labels = run
        .baseline_outcomes
        .iter()
        .chain(run.low_lr_outcomes.iter())
        .map(|outcome| outcome.label)
        .collect::<BTreeSet<_>>();
    let mut native_nodes = run
        .node_records
        .iter()
        .filter_map(|record| {
            let snapshot = run.final_snapshots.get(&record.label)?;
            let profile = snapshot.effective_limit_profile.as_ref();
            Some(NativeNodeDeviceLimitSummary {
                label: record.label.clone(),
                backend_preference: profile
                    .and_then(|profile| profile.estimate.preferred_backends.first().cloned()),
                preferred_backends: profile
                    .map(|profile| profile.estimate.preferred_backends.clone())
                    .unwrap_or_default(),
                configured_roles: record.roles.clone(),
                recommended_roles: profile
                    .map(|profile| {
                        profile
                            .recommended_roles
                            .roles
                            .iter()
                            .map(peer_role_label)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                target_window_seconds: profile
                    .map(|profile| profile.recommended_budget.target_window_seconds),
                budget_work_units: profile
                    .map(|profile| profile.recommended_budget.budget_work_units),
                device_memory_bytes: profile.and_then(|profile| profile.card.device_memory_bytes),
                system_memory_bytes: profile.map(|profile| profile.card.system_memory_bytes),
            })
        })
        .collect::<Vec<_>>();
    native_nodes.sort_by(|left, right| left.label.cmp(&right.label));

    let native_limit_profiles_present = native_nodes.iter().all(|node| {
        !active_trainer_labels.contains(node.label.as_str()) || node.backend_preference.is_some()
    });
    let native_trainer_backends = native_nodes
        .iter()
        .filter(|node| active_trainer_labels.contains(node.label.as_str()))
        .filter_map(|node| node.backend_preference.clone())
        .collect::<Vec<_>>();
    let native_validator_backends = native_nodes
        .iter()
        .filter(|node| {
            node.configured_roles
                .iter()
                .any(|role| matches!(role.as_str(), "Validator" | "Reducer"))
        })
        .filter_map(|node| node.backend_preference.clone())
        .collect::<Vec<_>>();

    let browser = browser_device_limit_summary(browser_probe_summary);

    DeviceLimitExerciseSummary {
        native_limit_profiles_present,
        native_trainer_backend_visible: !native_trainer_backends.is_empty(),
        native_validator_backend_visible: !native_validator_backends.is_empty(),
        native_trainer_backends,
        native_validator_backends,
        native_nodes,
        browser,
    }
}

fn browser_device_limit_summary(
    browser_probe_summary: Option<&serde_json::Value>,
) -> BrowserDeviceLimitSummary {
    let wasm_backend = browser_probe_summary
        .and_then(|summary| summary.pointer("/profiles/0/wasm/backend"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let gpu_adapter_available = browser_probe_summary
        .and_then(|summary| summary.pointer("/browser_runtime/adapterAvailable"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let capability = browser_probe_summary
        .and_then(|summary| summary.pointer("/profiles/0/wasm/live_participant/capability"));
    let training_budget = browser_probe_summary
        .and_then(|summary| summary.pointer("/profiles/0/wasm/live_participant/training_budget"));
    let gpu_reported_available = capability
        .and_then(|capability| capability.get("gpu_support"))
        .and_then(serde_json::Value::as_str)
        == Some("Available");
    let navigator_gpu_exposed = capability
        .and_then(|capability| capability.get("navigator_gpu_exposed"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let worker_gpu_exposed = capability
        .and_then(|capability| capability.get("worker_gpu_exposed"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let capability_max_training_window_secs = capability
        .and_then(|capability| capability.get("max_training_window_secs"))
        .and_then(serde_json::Value::as_u64);
    let capability_max_checkpoint_bytes = capability
        .and_then(|capability| capability.get("max_checkpoint_bytes"))
        .and_then(serde_json::Value::as_u64);
    let capability_max_shard_bytes = capability
        .and_then(|capability| capability.get("max_shard_bytes"))
        .and_then(serde_json::Value::as_u64);
    let training_budget_requires_webgpu = training_budget
        .and_then(|budget| budget.get("requires_webgpu"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let training_budget_max_window_secs = training_budget
        .and_then(|budget| budget.get("max_window_secs"))
        .and_then(serde_json::Value::as_u64);
    let training_budget_max_checkpoint_bytes = training_budget
        .and_then(|budget| budget.get("max_checkpoint_bytes"))
        .and_then(serde_json::Value::as_u64);
    let training_budget_max_shard_bytes = training_budget
        .and_then(|budget| budget.get("max_shard_bytes"))
        .and_then(serde_json::Value::as_u64);
    let training_budget_max_batch_size = training_budget
        .and_then(|budget| budget.get("max_batch_size"))
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as u32);
    let training_budget_precision = training_budget
        .and_then(|budget| budget.get("precision"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let budget_within_capability_limits = training_budget_max_window_secs
        .zip(capability_max_training_window_secs)
        .map(|(budget, capability)| budget <= capability)
        .unwrap_or(false)
        && training_budget_max_checkpoint_bytes
            .zip(capability_max_checkpoint_bytes)
            .map(|(budget, capability)| budget <= capability)
            .unwrap_or(false)
        && training_budget_max_shard_bytes
            .zip(capability_max_shard_bytes)
            .map(|(budget, capability)| budget <= capability)
            .unwrap_or(false);

    BrowserDeviceLimitSummary {
        wasm_backend: wasm_backend.clone(),
        gpu_adapter_available,
        gpu_reported_available,
        navigator_gpu_exposed,
        worker_gpu_exposed,
        capability_recommended_role: capability
            .and_then(|capability| capability.get("recommended_role"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        capability_max_training_window_secs,
        capability_max_checkpoint_bytes,
        capability_max_shard_bytes,
        training_budget_requires_webgpu,
        training_budget_max_window_secs,
        training_budget_max_checkpoint_bytes,
        training_budget_max_shard_bytes,
        training_budget_max_batch_size,
        training_budget_precision,
        webgpu_backend_confirmed: wasm_backend.as_deref() == Some("burn-webgpu-wasm")
            && gpu_adapter_available
            && gpu_reported_available
            && navigator_gpu_exposed
            && worker_gpu_exposed
            && training_budget_requires_webgpu,
        budget_within_capability_limits,
    }
}

fn peer_role_label(role: &PeerRole) -> String {
    match role {
        PeerRole::Bootstrap => "bootstrap",
        PeerRole::Authority => "authority",
        PeerRole::Validator => "validator",
        PeerRole::Archive => "archive",
        PeerRole::Reducer => "reducer",
        PeerRole::TrainerGpu => "trainer-gpu",
        PeerRole::TrainerCpu => "trainer-cpu",
        PeerRole::Evaluator => "evaluator",
        PeerRole::Viewer => "viewer",
        PeerRole::BrowserObserver => "browser-observer",
        PeerRole::BrowserVerifier => "browser-verifier",
        PeerRole::BrowserTrainerWgpu => "browser-trainer-wgpu",
        PeerRole::BrowserFallback => "browser-fallback",
        PeerRole::BrowserTrainer => "browser-trainer",
        PeerRole::RelayHelper => "relay-helper",
    }
    .into()
}

fn topology_summary(run: &CoreMnistRun) -> anyhow::Result<TopologyExerciseSummary> {
    let seed_label = run.bootstrap_seed_label.as_str();
    let reducer_label = run.dedicated_reducer_label.as_str();
    let seed_snapshot = run
        .final_snapshots
        .get(seed_label)
        .with_context(|| format!("mnist demo missing final snapshot for {seed_label}"))?;
    let reducer_peer_id = node_peer_id(&run.node_records, reducer_label)?;
    let validator_peer_ids = run
        .validator_labels
        .iter()
        .map(|label| node_peer_id(&run.node_records, label))
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
    let topology_labels = run
        .validator_labels
        .iter()
        .cloned()
        .chain(std::iter::once(run.dedicated_reducer_label.clone()))
        .collect::<Vec<_>>();
    let aggregate_proposal_reducers = topology_labels
        .iter()
        .filter_map(|label| run.final_snapshots.get(label))
        .flat_map(|snapshot| {
            snapshot
                .control_plane
                .aggregate_proposal_announcements
                .iter()
        })
        .map(|announcement| announcement.proposal.reducer_peer_id.clone())
        .collect::<BTreeSet<_>>();
    let reducer_load_publishers = topology_labels
        .iter()
        .filter_map(|label| run.final_snapshots.get(label))
        .flat_map(|snapshot| snapshot.control_plane.reducer_load_announcements.iter())
        .map(|announcement| announcement.report.peer_id.clone())
        .collect::<BTreeSet<_>>();
    let allowed_reducer_load_publishers = validator_peer_ids
        .iter()
        .cloned()
        .chain(std::iter::once(reducer_peer_id.clone()))
        .collect::<BTreeSet<_>>();
    let reduction_attesters = topology_labels
        .iter()
        .filter_map(|label| run.final_snapshots.get(label))
        .flat_map(|snapshot| {
            snapshot
                .control_plane
                .reduction_certificate_announcements
                .iter()
        })
        .map(|announcement| announcement.certificate.validator.clone())
        .collect::<BTreeSet<_>>();
    let merge_certificate_validators = run
        .merge_certificates
        .iter()
        .map(|certificate| certificate.validator.clone())
        .collect::<BTreeSet<_>>();
    let seed_addresses = seed_snapshot
        .listen_addresses
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let non_seed_labels = run
        .bootstrap_plan
        .keys()
        .filter(|label| label.as_str() != seed_label)
        .cloned()
        .collect::<Vec<_>>();
    let all_non_seed_nodes_bootstrap_via_seed = non_seed_labels.iter().all(|label| {
        run.bootstrap_plan
            .get(label)
            .is_some_and(|bootstraps| bootstraps == std::slice::from_ref(&run.bootstrap_seed_label))
    });
    let mesh_fanout_beyond_seed_observed = non_seed_labels.iter().all(|label| {
        run.final_snapshots.get(label).is_some_and(|snapshot| {
            snapshot
                .known_peer_addresses
                .iter()
                .any(|address| !seed_addresses.contains(address))
        })
    });
    let validators_observed_validation_quorum = run.validator_labels.iter().all(|label| {
        run.final_snapshots.get(label).is_some_and(|snapshot| {
            !snapshot
                .control_plane
                .validation_quorum_announcements
                .is_empty()
        })
    });

    Ok(TopologyExerciseSummary {
        bootstrap_seed_label: run.bootstrap_seed_label.clone(),
        dedicated_reducer_label: run.dedicated_reducer_label.clone(),
        validator_labels: run.validator_labels.clone(),
        all_non_seed_nodes_bootstrap_via_seed,
        mesh_fanout_beyond_seed_observed,
        dedicated_reducer_participated: aggregate_proposal_reducers.contains(&reducer_peer_id)
            && reducer_load_publishers.contains(&reducer_peer_id),
        aggregate_proposals_only_from_dedicated_reducer: !aggregate_proposal_reducers.is_empty()
            && aggregate_proposal_reducers == BTreeSet::from([reducer_peer_id.clone()]),
        reducer_load_only_from_dedicated_reducer: !reducer_load_publishers.is_empty()
            && reducer_load_publishers == BTreeSet::from([reducer_peer_id.clone()]),
        reducer_load_publishers_within_reducer_validation_tier: !reducer_load_publishers
            .is_empty()
            && reducer_load_publishers
                .iter()
                .all(|peer_id| allowed_reducer_load_publishers.contains(peer_id)),
        reduction_attestations_only_from_validators: !reduction_attesters.is_empty()
            && reduction_attesters
                .iter()
                .all(|peer_id| validator_peer_ids.contains(peer_id)),
        merge_certificates_only_from_validators: !merge_certificate_validators.is_empty()
            && merge_certificate_validators
                .iter()
                .all(|peer_id| validator_peer_ids.contains(peer_id)),
        validators_observed_validation_quorum,
        notes: vec![
            "all non-seed nodes join the run through the helper seed instead of dialing the validator directly".into(),
            "the dedicated reducer publishes aggregate proposals while validators only attest and promote".into(),
            "validators may locally materialize aggregates during verification, so reducer-load telemetry can appear from validator peers even when reducer authority remains isolated".into(),
            "healthy peers are expected to learn non-seed addresses and fan out beyond the helper seed".into(),
        ],
    })
}

fn browser_dataset_access_summary(
    run: &CoreMnistRun,
) -> anyhow::Result<BrowserDatasetAccessSummary> {
    if let Some(browser_probe_summary) = run.browser_probe_summary.as_ref()
        && let Some(value) = browser_probe_summary.get("browser_dataset_access")
    {
        return serde_json::from_value(value.clone())
            .context("decode browser dataset access from live browser probe summary");
    }

    probe_browser_http_shard_fetch(
        &run.prepared_data.dataset_root,
        &run.prepared_data,
        &run.baseline_outcomes
            .first()
            .context("mnist demo did not produce a baseline trainer lease")?
            .training
            .lease,
    )
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
            head_artifact_transport: None,
            active_assignment: false,
            emitted_receipt_id: None,
            accepted_receipt_ids: Vec::new(),
            trainer_runtime_and_wasm_training_coherent: false,
            notes: vec!["live browser participation was not requested for this demo run".into()],
        });
    };

    let probe_manifest = run
        .browser_probe_manifest
        .as_ref()
        .context("mnist correctness summary missing live browser probe manifest")?;
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
    let head_artifact_transport = browser_probe_summary
        .pointer("/browser_execution/head_artifact_transport")
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

    let same_network_context = probe_network_id == probe_manifest.network_id;
    let same_experiment_context = probe_experiment_id == probe_manifest.experiment_id;
    let same_revision_context = probe_revision_id == probe_manifest.revision_id;
    let same_head_context = probe_selected_head_id == probe_manifest.selected_head_id;
    let same_lease_context = probe_lease_id == probe_manifest.lease_id;
    let same_leased_microshards = probe_leased_microshards
        == probe_manifest
            .leased_microshards
            .iter()
            .map(String::as_str)
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
        head_artifact_transport: head_artifact_transport.clone(),
        active_assignment,
        emitted_receipt_id,
        accepted_receipt_ids,
        trainer_runtime_and_wasm_training_coherent,
        notes: {
            let mut notes = vec![
            "the browser worker enrolled against the live browser edge while the native mnist fleet was still running".into(),
            "browser burn/webgpu training consumed the same lease-scoped shard slice exported into a browser data bundle".into(),
            ];
            if run
                .browser_probe_manifest
                .as_ref()
                .and_then(|manifest| manifest.peer_dataset_root.as_ref())
                .is_some()
            {
                notes.push("the browser data bundle was materialized locally from the p2p-synced artifact, so dataset bytes did not need to flow back through the edge route".into());
            } else {
                notes.push("the browser data bundle was synced from a native peer over the artifact control plane before the browser edge served it".into());
            }
            if head_artifact_transport.as_deref() == Some("peer-native-swarm") {
                notes.push("the browser runtime reconstructed the active head artifact through manifest and chunk requests over the browser-native peer artifact swarm before any edge download route was needed".into());
            } else if head_artifact_transport.as_deref() == Some("peer-native") {
                notes.push("the browser runtime fetched the active head artifact through the configured peer-native transport before any edge download route was needed".into());
            } else if head_artifact_transport.as_deref() == Some("edge-download-ticket") {
                notes.push("the browser runtime still fell back to the edge artifact download ticket for the active head artifact".into());
            }
            notes
        },
    })
}

#[cfg(test)]
mod tests {
    use super::compare_baseline_vs_low_lr;

    #[test]
    fn comparison_prefers_clear_accuracy_advantage() {
        let comparison = compare_baseline_vs_low_lr(320, 0.45, 2.10, 0.40, 2.20);
        assert!(comparison.baseline_outperformed_low_lr);
        assert!(comparison.baseline_accuracy_delta_vs_low_lr > 0.0);
    }

    #[test]
    fn comparison_uses_loss_tiebreak_within_two_eval_examples() {
        let comparison = compare_baseline_vs_low_lr(320, 0.496875, 2.18, 0.5, 2.24);
        assert!(comparison.baseline_accuracy_delta_vs_low_lr < 0.0);
        assert!(
            comparison.baseline_accuracy_delta_vs_low_lr
                >= -comparison.baseline_accuracy_tolerance_vs_low_lr
        );
        assert!(comparison.baseline_loss_advantage_vs_low_lr > 0.0);
        assert!(comparison.baseline_outperformed_low_lr);
    }

    #[test]
    fn comparison_rejects_material_accuracy_regression_even_with_better_loss() {
        let comparison = compare_baseline_vs_low_lr(320, 0.46, 2.10, 0.48, 2.11);
        assert!(comparison.baseline_accuracy_delta_vs_low_lr < 0.0);
        assert!(
            comparison.baseline_accuracy_delta_vs_low_lr
                < -comparison.baseline_accuracy_tolerance_vs_low_lr
        );
        assert!(comparison.baseline_loss_advantage_vs_low_lr > 0.0);
        assert!(!comparison.baseline_outperformed_low_lr);
    }
}
