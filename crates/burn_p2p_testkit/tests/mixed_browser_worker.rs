use std::collections::{BTreeMap, BTreeSet};

use burn_p2p::{
    BrowserRolePolicy, BrowserVisibilityPolicy, ContentId, ExperimentDirectoryEntry,
    ExperimentDirectoryPolicyExt, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadId, NetworkId,
    PeerRoleSet, Precision, PrincipalClaims, PrincipalId, PrincipalSession, RevisionId,
    RevisionManifest, WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_bootstrap::BrowserDirectorySnapshot;
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserGpuSupport, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserRuntimeState, BrowserSessionState, BrowserTrainingBudget, BrowserTrainingPlan,
    BrowserTransportStatus, BrowserValidationPlan, BrowserWorkerCommand, BrowserWorkerEvent,
    BrowserWorkerRuntime,
};
use burn_p2p_testkit::{PeerFixtureMode, SimulationRunner, SimulationSpec};
use chrono::Utc;

fn browser_revision_manifest(
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    workload_id: WorkloadId,
) -> RevisionManifest {
    RevisionManifest {
        experiment_id,
        revision_id,
        workload_id,
        required_release_train_hash: ContentId::new("synthetic-train"),
        model_schema_hash: ContentId::new("synthetic-model-schema"),
        checkpoint_format_hash: ContentId::new("synthetic-checkpoint"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        training_config_hash: ContentId::new("synthetic-training-config"),
        merge_topology_policy_hash: ContentId::new("synthetic-merge-topology"),
        slot_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 1,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::default(),
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: true,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser worker smoke revision".into(),
    }
}

fn browser_directory_from_spec(spec: &SimulationSpec) -> BrowserDirectorySnapshot {
    let workload_id = WorkloadId::new("browser-demo");
    let revision = browser_revision_manifest(
        spec.experiment_id.clone(),
        spec.revision_id.clone(),
        workload_id.clone(),
    );
    let mut entry = ExperimentDirectoryEntry {
        network_id: spec.network_id.clone(),
        study_id: spec.study_id.clone(),
        experiment_id: spec.experiment_id.clone(),
        workload_id,
        display_name: "Mixed Fleet Browser".into(),
        model_schema_hash: ContentId::new("synthetic-model-schema"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: spec.revision_id.clone(),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([
            ExperimentScope::Train {
                experiment_id: spec.experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: spec.experiment_id.clone(),
            },
            ExperimentScope::Connect,
        ]),
        metadata: BTreeMap::new(),
    };
    entry.apply_revision_policy(&revision);

    BrowserDirectorySnapshot {
        network_id: spec.network_id.clone(),
        generated_at: Utc::now(),
        entries: vec![entry],
    }
}

fn browser_session(spec: &SimulationSpec) -> BrowserSessionState {
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("browser-session"),
            network_id: spec.network_id.clone(),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("browser-principal"),
                provider: burn_p2p::AuthProvider::Static {
                    authority: "browser-test-authority".into(),
                },
                display_name: "Browser Principal".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: spec.experiment_id.clone(),
                    },
                    ExperimentScope::Validate {
                        experiment_id: spec.experiment_id.clone(),
                    },
                ]),
                custom_claims: BTreeMap::new(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(30),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
        }),
        certificate: None,
        trust_bundle: None,
        enrolled_at: Some(Utc::now()),
        reenrollment_required: false,
    }
}

fn browser_capability() -> BrowserCapabilityReport {
    BrowserCapabilityReport {
        navigator_gpu_exposed: true,
        worker_gpu_exposed: true,
        gpu_support: BrowserGpuSupport::Available,
        recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
        ..BrowserCapabilityReport::default()
    }
}

#[test]
fn mixed_fleet_simulation_drives_browser_worker_training_and_validation() {
    let spec = SimulationSpec::default();
    let runner = SimulationRunner::default();
    let outcome = runner.run(spec.clone()).expect("simulation outcome");
    let browser_fixture = outcome
        .peer_fixtures
        .iter()
        .find(|fixture| matches!(fixture.mode, PeerFixtureMode::HonestBrowser))
        .expect("browser fixture");
    assert!(
        outcome
            .browser_harness
            .peer_ids
            .contains(&browser_fixture.peer_id)
    );

    let latest_head_id = outcome
        .windows
        .last()
        .and_then(|window| {
            window
                .merge_certificate
                .as_ref()
                .map(|certificate| certificate.merged_head_id.clone())
                .or_else(|| {
                    window
                        .accepted_receipts
                        .last()
                        .map(|receipt| receipt.base_head_id.clone())
                })
        })
        .unwrap_or_else(|| HeadId::new("genesis-head"));

    let directory = browser_directory_from_spec(&spec);
    let session = browser_session(&spec);
    let transport = BrowserTransportStatus::default();
    let capability = browser_capability();

    let mut trainer_runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                NetworkId::new(spec.network_id.as_str()),
                ContentId::new("synthetic-train"),
                "browser-wasm",
                ContentId::new("approved-artifact-browser"),
            )
        },
        capability.clone(),
        transport.clone(),
    );
    trainer_runtime.remember_session(session.clone());
    trainer_runtime.storage.stored_certificate_peer_id = Some(browser_fixture.peer_id.clone());
    trainer_runtime.apply_directory_snapshot(&directory, Some(&session));
    assert_eq!(
        trainer_runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            stage: burn_p2p_browser::BrowserJoinStage::HeadSync,
        })
    );
    trainer_runtime.apply_head_snapshot(&[burn_p2p::HeadDescriptor {
        head_id: latest_head_id.clone(),
        study_id: spec.study_id.clone(),
        experiment_id: spec.experiment_id.clone(),
        revision_id: spec.revision_id.clone(),
        artifact_id: burn_p2p::ArtifactId::new("mixed-fleet-head-artifact"),
        parent_head_id: None,
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }]);
    assert_eq!(trainer_runtime.state, Some(BrowserRuntimeState::Trainer));
    assert!(trainer_runtime.storage.active_assignment.is_some());

    let training_events = trainer_runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: spec.study_id.clone(),
            experiment_id: spec.experiment_id.clone(),
            revision_id: spec.revision_id.clone(),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
        }),
        Some(&directory),
        Some(&session),
    );
    let trained_receipt_id = training_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::TrainingCompleted(result) => result.receipt_id.clone(),
            _ => None,
        })
        .expect("training receipt id");
    assert!(
        trainer_runtime
            .storage
            .pending_receipts
            .iter()
            .any(|receipt| receipt.receipt_id == trained_receipt_id
                && receipt.peer_id == browser_fixture.peer_id)
    );

    let flush_events = trainer_runtime.apply_command(
        BrowserWorkerCommand::FlushReceiptOutbox,
        Some(&directory),
        Some(&session),
    );
    let (session_id, receipts) = flush_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::ReceiptOutboxReady {
                session_id,
                receipts,
                ..
            } => Some((session_id, receipts)),
            _ => None,
        })
        .expect("training outbox");
    assert_eq!(session_id.as_str(), "browser-session");
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.receipt_id == trained_receipt_id)
    );

    let ack_events = trainer_runtime.apply_command(
        BrowserWorkerCommand::AcknowledgeSubmittedReceipts {
            receipt_ids: vec![trained_receipt_id.clone()],
        },
        Some(&directory),
        Some(&session),
    );
    assert!(ack_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::ReceiptsAcknowledged {
            pending_receipts: 0,
            ..
        }
    )));

    let mut verifier_runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserVerifier,
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                NetworkId::new(spec.network_id.as_str()),
                ContentId::new("synthetic-train"),
                "browser-wasm",
                ContentId::new("approved-artifact-browser"),
            )
        },
        capability,
        transport,
    );
    verifier_runtime.remember_session(session.clone());
    verifier_runtime.storage.stored_certificate_peer_id = Some(browser_fixture.peer_id.clone());
    verifier_runtime.apply_directory_snapshot(&directory, Some(&session));
    assert_eq!(
        verifier_runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserVerifier,
            stage: burn_p2p_browser::BrowserJoinStage::HeadSync,
        })
    );
    verifier_runtime.apply_head_snapshot(&[burn_p2p::HeadDescriptor {
        head_id: latest_head_id.clone(),
        study_id: spec.study_id.clone(),
        experiment_id: spec.experiment_id.clone(),
        revision_id: spec.revision_id.clone(),
        artifact_id: burn_p2p::ArtifactId::new("mixed-fleet-head-artifact"),
        parent_head_id: None,
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }]);
    assert_eq!(verifier_runtime.state, Some(BrowserRuntimeState::Verifier));

    let validation_events = verifier_runtime.apply_command(
        BrowserWorkerCommand::Verify(BrowserValidationPlan {
            head_id: latest_head_id,
            max_checkpoint_bytes: 16 * 1024 * 1024,
            sample_budget: 8,
            emit_receipt: true,
        }),
        Some(&directory),
        Some(&session),
    );
    let validation_receipt_id = validation_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::ValidationCompleted(result) => result.emitted_receipt_id.clone(),
            _ => None,
        })
        .expect("validation receipt id");
    assert!(
        verifier_runtime
            .storage
            .pending_receipts
            .iter()
            .any(|receipt| receipt.receipt_id == validation_receipt_id
                && receipt.peer_id == browser_fixture.peer_id)
    );
}
