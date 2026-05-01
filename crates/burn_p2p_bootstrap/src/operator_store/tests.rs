// Operator store tests live here to keep operator_store.rs focused on store implementation.
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    process::{Child, Command, Stdio},
    thread,
    time::Duration as StdDuration,
};

use super::*;
use crate::state::HeadQuery;
use burn_p2p::{HeadDescriptor, MetricValue, PeerRole, PeerWindowStatus};
use burn_p2p_core::{
    ArtifactId, ContentId, DatasetViewId, ExperimentId, HeadEvalStatus, MetricTrustClass,
    NetworkId, PeerId, RevisionId, SignatureAlgorithm, SignatureMetadata, StudyId,
    WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_experiment::{
    ActivationTarget, ExperimentLifecycleEnvelope, ExperimentLifecyclePhase,
    ExperimentLifecyclePlan, FleetScheduleEpochBuilder, FleetScheduleEpochEnvelope,
};
use semver::Version;
use tempfile::{TempDir, tempdir};

struct RedisTestServer {
    url: String,
    port: u16,
    child: Option<Child>,
    state: TempDir,
}

struct PostgresTestServer {
    url: String,
    container_name: String,
}

impl RedisTestServer {
    fn spawn() -> Option<Self> {
        let status = Command::new("redis-server")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }

        let state = tempdir().expect("redis temp dir");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind redis test port");
        let port = listener.local_addr().expect("redis local addr").port();
        drop(listener);

        let mut server = Self {
            url: format!("redis://127.0.0.1:{port}/0"),
            port,
            child: None,
            state,
        };
        server.start();
        Some(server)
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "redis test server already started");
        let child = Command::new("redis-server")
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--dir")
            .arg(self.state.path())
            .arg("--dbfilename")
            .arg("dump.rdb")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn redis-server");

        let deadline = std::time::Instant::now() + StdDuration::from_secs(15);
        loop {
            if let Ok(client) = redis::Client::open(self.url.as_str())
                && let Ok(mut connection) = client.get_connection()
                && redis::cmd("PING")
                    .query::<String>(&mut connection)
                    .map(|pong| pong == "PONG")
                    .unwrap_or(false)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "redis test server did not become ready at {}",
                self.url
            );
            thread::sleep(StdDuration::from_millis(25));
        }
        self.child = Some(child);
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for RedisTestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl PostgresTestServer {
    fn spawn() -> Option<Self> {
        let has_docker = Command::new("docker")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .is_some_and(|status| status.success());
        if !has_docker {
            return None;
        }
        let daemon_ready = Command::new("docker")
            .arg("info")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .is_some_and(|status| status.success());
        if !daemon_ready {
            return None;
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind postgres test port");
        let port = listener.local_addr().expect("postgres local addr").port();
        drop(listener);

        let container_name = format!(
            "burn-p2p-operator-store-postgres-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let port_mapping = format!("127.0.0.1:{port}:5432");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &container_name,
                "-e",
                "POSTGRES_USER=burn_p2p",
                "-e",
                "POSTGRES_PASSWORD=burn-p2p-dev",
                "-e",
                "POSTGRES_DB=burn_p2p",
                "-p",
                &port_mapping,
                "postgres:16-alpine",
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }

        let url = format!("postgres://burn_p2p:burn-p2p-dev@127.0.0.1:{port}/burn_p2p");
        let deadline = std::time::Instant::now() + StdDuration::from_secs(20);
        loop {
            if let Ok(mut client) = postgres::Client::connect(url.as_str(), postgres::NoTls)
                && client.simple_query("SELECT 1").is_ok()
            {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = Command::new("docker")
                    .args(["rm", "-f", &container_name])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                return None;
            }
            thread::sleep(StdDuration::from_millis(100));
        }

        Some(Self {
            url,
            container_name,
        })
    }
}

impl Drop for PostgresTestServer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn sample_replay_preview(index: usize, captured_at: DateTime<Utc>) -> FileOperatorStorePreview {
    let study_id = StudyId::new(if index.is_multiple_of(2) {
        "study-a"
    } else {
        "study-b"
    });
    let experiment_id = ExperimentId::new(if index.is_multiple_of(2) {
        "exp-a"
    } else {
        "exp-b"
    });
    let revision_id = RevisionId::new(format!("rev-{}", index % 5));
    let head = HeadDescriptor {
        head_id: burn_p2p_core::HeadId::new(format!("head-{index}")),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        artifact_id: ArtifactId::new(format!("artifact-{index}")),
        parent_head_id: Some(burn_p2p_core::HeadId::new(format!("base-{}", index % 11))),
        global_step: u64::try_from(index).expect("global step"),
        created_at: captured_at,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(index as f64 / 100.0))]),
    };
    let receipt = ContributionReceipt {
        receipt_id: burn_p2p::ContributionReceiptId::new(format!("receipt-{index}")),
        peer_id: PeerId::new(format!("trainer-{}", index % 7)),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        base_head_id: burn_p2p_core::HeadId::new(format!("base-{}", index % 11)),
        artifact_id: ArtifactId::new(format!("artifact-input-{index}")),
        accepted_at: captured_at - chrono::Duration::seconds(4),
        accepted_weight: 1.0 + (index as f64 / 1000.0),
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
        merge_cert_id: None,
    };
    let merge = MergeCertificate {
        merge_cert_id: burn_p2p::MergeCertId::new(format!("merge-{index}")),
        study_id,
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        base_head_id: burn_p2p_core::HeadId::new(format!("base-{}", index % 11)),
        merged_head_id: head.head_id.clone(),
        merged_artifact_id: head.artifact_id.clone(),
        policy: burn_p2p::MergePolicy::WeightedMean,
        issued_at: captured_at - chrono::Duration::seconds(3),
        promoter_peer_id: PeerId::new(format!("validator-{}", index % 5)),
        promotion_mode: burn_p2p::HeadPromotionMode::ValidatorQuorum,
        contribution_receipts: vec![receipt.receipt_id.clone()],
    };
    let peer_window_metrics = PeerWindowMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        peer_id: PeerId::new(format!("trainer-{}", index % 7)),
        principal_id: None,
        lease_id: burn_p2p::LeaseId::new(format!("lease-{index}")),
        base_head_id: burn_p2p_core::HeadId::new(format!("base-{}", index % 11)),
        window_started_at: captured_at - chrono::Duration::seconds(5),
        window_finished_at: captured_at - chrono::Duration::seconds(1),
        attempted_tokens_or_samples: 128,
        accepted_tokens_or_samples: Some(120),
        local_train_loss_mean: Some(0.25),
        local_train_loss_last: Some(0.2),
        grad_or_delta_norm: Some(0.9),
        optimizer_step_count: 8,
        compute_time_ms: 1_200,
        data_fetch_time_ms: 200,
        publish_latency_ms: 80,
        head_lag_at_start: 0,
        head_lag_at_finish: 1,
        backend_class: burn_p2p_core::BackendClass::Ndarray,
        role: PeerRole::TrainerCpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    };
    let reducer_cohort_metrics = ReducerCohortMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        merge_window_id: ContentId::new(format!("merge-window-{index}")),
        reducer_group_id: ContentId::new(format!("reducers-{}", index % 3)),
        captured_at: captured_at - chrono::Duration::milliseconds(500),
        base_head_id: burn_p2p_core::HeadId::new(format!("base-{}", index % 11)),
        candidate_head_id: Some(head.head_id.clone()),
        received_updates: 2,
        accepted_updates: 2,
        rejected_updates: 0,
        sum_weight: 2.0,
        accepted_tokens_or_samples: 128,
        staleness_mean: 0.1,
        staleness_max: 0.5,
        window_close_delay_ms: 40,
        cohort_duration_ms: 500,
        aggregate_norm: 0.7,
        reducer_load: 0.25,
        ingress_bytes: 1024,
        egress_bytes: 512,
        replica_agreement: Some(1.0),
        late_arrival_count: Some(0),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::new(),
        status: burn_p2p_core::ReducerCohortStatus::Closed,
    };
    let report = HeadEvalReport {
        network_id: NetworkId::new("mainnet"),
        experiment_id,
        revision_id,
        workload_id: WorkloadId::new("workload"),
        head_id: head.head_id.clone(),
        base_head_id: head.parent_head_id.clone(),
        eval_protocol_id: ContentId::new(format!("protocol-{index}")),
        evaluator_set_id: ContentId::new("validators"),
        metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
        sample_count: 128,
        dataset_view_id: DatasetViewId::new("validation"),
        started_at: captured_at - chrono::Duration::seconds(2),
        finished_at: captured_at - chrono::Duration::seconds(1),
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    };

    FileOperatorStorePreview {
        receipts: vec![receipt],
        heads: vec![head],
        merges: vec![merge],
        lifecycle_announcements: Vec::new(),
        schedule_announcements: Vec::new(),
        peer_window_metrics: vec![peer_window_metrics],
        reducer_cohort_metrics: vec![reducer_cohort_metrics],
        head_eval_reports: vec![report],
        eval_protocol_manifests: Vec::new(),
    }
}

fn sample_operator_preview(now: DateTime<Utc>) -> FileOperatorStorePreview {
    let head = HeadDescriptor {
        head_id: burn_p2p_core::HeadId::new("head-1"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        artifact_id: ArtifactId::new("artifact-1"),
        parent_head_id: Some(burn_p2p_core::HeadId::new("base")),
        global_step: 42,
        created_at: now,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.125))]),
    };
    let receipt = ContributionReceipt {
        receipt_id: burn_p2p::ContributionReceiptId::new("receipt-1"),
        peer_id: PeerId::new("trainer-a"),
        study_id: head.study_id.clone(),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        artifact_id: ArtifactId::new("artifact-input"),
        accepted_at: now - chrono::Duration::seconds(4),
        accepted_weight: 1.5,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
        merge_cert_id: None,
    };
    let merge = MergeCertificate {
        merge_cert_id: burn_p2p::MergeCertId::new("merge-1"),
        study_id: head.study_id.clone(),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        merged_head_id: head.head_id.clone(),
        merged_artifact_id: head.artifact_id.clone(),
        policy: burn_p2p::MergePolicy::WeightedMean,
        issued_at: now - chrono::Duration::seconds(3),
        promoter_peer_id: PeerId::new("validator-a"),
        promotion_mode: burn_p2p::HeadPromotionMode::ValidatorQuorum,
        contribution_receipts: vec![receipt.receipt_id.clone()],
    };
    let peer_window_metrics = PeerWindowMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        peer_id: PeerId::new("trainer-a"),
        principal_id: None,
        lease_id: burn_p2p::LeaseId::new("lease-a"),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        window_started_at: now - chrono::Duration::seconds(5),
        window_finished_at: now - chrono::Duration::seconds(1),
        attempted_tokens_or_samples: 128,
        accepted_tokens_or_samples: Some(120),
        local_train_loss_mean: Some(0.25),
        local_train_loss_last: Some(0.2),
        grad_or_delta_norm: Some(0.9),
        optimizer_step_count: 8,
        compute_time_ms: 1_200,
        data_fetch_time_ms: 200,
        publish_latency_ms: 80,
        head_lag_at_start: 0,
        head_lag_at_finish: 1,
        backend_class: burn_p2p_core::BackendClass::Ndarray,
        role: PeerRole::TrainerCpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    };
    let reducer_cohort_metrics = ReducerCohortMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        merge_window_id: ContentId::new("merge-window-a"),
        reducer_group_id: ContentId::new("reducers-a"),
        captured_at: now - chrono::Duration::milliseconds(500),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        candidate_head_id: Some(head.head_id.clone()),
        received_updates: 2,
        accepted_updates: 2,
        rejected_updates: 0,
        sum_weight: 2.0,
        accepted_tokens_or_samples: 128,
        staleness_mean: 0.1,
        staleness_max: 0.5,
        window_close_delay_ms: 40,
        cohort_duration_ms: 500,
        aggregate_norm: 0.7,
        reducer_load: 0.25,
        ingress_bytes: 1024,
        egress_bytes: 512,
        replica_agreement: Some(1.0),
        late_arrival_count: Some(0),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::new(),
        status: burn_p2p_core::ReducerCohortStatus::Closed,
    };
    let report = HeadEvalReport {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        head_id: head.head_id.clone(),
        base_head_id: head.parent_head_id.clone(),
        eval_protocol_id: ContentId::new("protocol"),
        evaluator_set_id: ContentId::new("validators"),
        metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
        sample_count: 128,
        dataset_view_id: DatasetViewId::new("validation"),
        started_at: now - chrono::Duration::seconds(2),
        finished_at: now - chrono::Duration::seconds(1),
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    };
    FileOperatorStorePreview {
        receipts: vec![receipt],
        heads: vec![head],
        merges: vec![merge],
        lifecycle_announcements: Vec::new(),
        schedule_announcements: Vec::new(),
        peer_window_metrics: vec![peer_window_metrics],
        reducer_cohort_metrics: vec![reducer_cohort_metrics],
        head_eval_reports: vec![report],
        eval_protocol_manifests: Vec::new(),
    }
}

fn sample_lifecycle_and_schedule_preview(now: DateTime<Utc>) -> FileOperatorStorePreview {
    let signer = SignatureMetadata {
        signer: PeerId::new("authority"),
        key_id: "authority-key".into(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at: now,
        signature_hex: "abcd".into(),
    };
    let lifecycle = ExperimentLifecycleEnvelope {
        network_id: NetworkId::new("mainnet"),
        plan: ExperimentLifecyclePlan {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp-a"),
            base_revision_id: Some(RevisionId::new("rev-a")),
            target_entry: burn_p2p::ExperimentDirectoryEntry {
                network_id: NetworkId::new("mainnet"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp-b"),
                workload_id: WorkloadId::new("alternate"),
                display_name: "exp-b rev-b".into(),
                model_schema_hash: ContentId::new("schema-b"),
                dataset_view_id: DatasetViewId::new("view-b"),
                resource_requirements: burn_p2p::ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                    minimum_device_memory_bytes: None,
                    minimum_system_memory_bytes: None,
                    estimated_download_bytes: 0,
                    estimated_window_seconds: 60,
                },
                visibility: burn_p2p::ExperimentVisibility::Public,
                opt_in_policy: burn_p2p::ExperimentOptInPolicy::Open,
                current_revision_id: RevisionId::new("rev-b"),
                current_head_id: Some(HeadId::new("head-b")),
                allowed_roles: burn_p2p_core::PeerRoleSet::default_trainer(),
                allowed_scopes: BTreeSet::new(),
                metadata: BTreeMap::new(),
            },
            phase: ExperimentLifecyclePhase::Activating,
            target: ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(7),
                    grace_windows: 0,
                },
                required_client_capabilities: BTreeSet::new(),
            },
            plan_epoch: 3,
            reason: Some("roll exp-b".into()),
        },
    }
    .into_signed_cert(signer.clone(), Version::new(0, 1, 0))
    .expect("lifecycle cert");
    let schedule = FleetScheduleEpochEnvelope {
        network_id: NetworkId::new("mainnet"),
        epoch: FleetScheduleEpochBuilder::new()
            .with_activation(WindowActivation {
                activation_window: WindowId(8),
                grace_windows: 0,
            })
            .ending_before(WindowId(12))
            .with_plan_epoch(4)
            .with_reason("planner reshuffle")
            .assign_peer_slot_scaled(
                PeerId::new("trainer-a"),
                0,
                StudyId::new("study"),
                ExperimentId::new("exp-a"),
                RevisionId::new("rev-a"),
                Some(0.5),
                Some(0.25),
            )
            .build(),
    }
    .into_signed_cert(signer, Version::new(0, 1, 0))
    .expect("schedule cert");

    FileOperatorStorePreview {
        lifecycle_announcements: vec![ExperimentLifecycleAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("mainnet")),
            certificate: lifecycle,
            announced_at: now - chrono::Duration::seconds(2),
        }],
        schedule_announcements: vec![FleetScheduleAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("mainnet")),
            certificate: schedule,
            announced_at: now - chrono::Duration::seconds(1),
        }],
        ..FileOperatorStorePreview::default()
    }
}

#[test]
fn build_audit_records_from_preview_flattens_replayable_snapshot_rows() {
    let now = Utc::now();
    let preview = sample_operator_preview(now);
    let records = build_audit_records_from_preview(&preview, Some(now));

    assert_eq!(records.len(), 7);
    assert!(
        records
            .iter()
            .any(|record| record.kind == OperatorAuditKind::Receipt
                && record.record_id == "receipt-1"
                && record.summary.get("artifact_id") == Some(&"artifact-input".to_string()))
    );
    assert!(
        records
            .iter()
            .any(|record| record.kind == OperatorAuditKind::PeerWindowMetric
                && record.summary.get("status") == Some(&"Completed".to_string()))
    );
    let snapshot = records
        .iter()
        .find(|record| record.kind == OperatorAuditKind::Snapshot)
        .expect("snapshot record");
    assert_eq!(snapshot.summary.get("heads"), Some(&"1".to_string()));
    assert_eq!(
        snapshot.summary.get("peer_window_metrics"),
        Some(&"1".to_string())
    );
}

#[test]
fn build_audit_records_from_preview_flattens_lifecycle_and_schedule_rows() {
    let now = Utc::now();
    let preview = sample_lifecycle_and_schedule_preview(now);
    let records = build_audit_records_from_preview(&preview, Some(now));

    assert!(
        records.iter().any(|record| {
            record.kind == OperatorAuditKind::LifecyclePlan
                && record.experiment_id.as_ref() == Some(&ExperimentId::new("exp-a"))
                && record.revision_id.as_ref() == Some(&RevisionId::new("rev-b"))
                && record.summary.get("target_experiment_id") == Some(&"exp-b".to_string())
        }),
        "expected lifecycle audit row"
    );
    assert!(
        records.iter().any(|record| {
            record.kind == OperatorAuditKind::ScheduleEpoch
                && record.peer_id.as_ref() == Some(&PeerId::new("trainer-a"))
                && record.summary.get("slot_index") == Some(&"0".to_string())
        }),
        "expected schedule audit row"
    );
    let snapshot = records
        .iter()
        .find(|record| record.kind == OperatorAuditKind::Snapshot)
        .expect("snapshot record");
    assert_eq!(
        snapshot.summary.get("lifecycle_plans"),
        Some(&"1".to_string())
    );
    assert_eq!(
        snapshot.summary.get("schedule_epochs"),
        Some(&"1".to_string())
    );
}

#[test]
fn build_control_replay_records_from_preview_preserves_typed_control_fields() {
    let now = Utc::now();
    let preview = sample_lifecycle_and_schedule_preview(now);
    let records = build_control_replay_records_from_preview(&preview, now);

    let lifecycle = records
        .iter()
        .find(|record| record.kind == OperatorControlReplayKind::LifecyclePlan)
        .expect("lifecycle record");
    assert_eq!(lifecycle.network_id, NetworkId::new("mainnet"));
    assert_eq!(lifecycle.study_id, StudyId::new("study"));
    assert_eq!(lifecycle.experiment_id, ExperimentId::new("exp-b"));
    assert_eq!(lifecycle.revision_id, RevisionId::new("rev-b"));
    assert_eq!(
        lifecycle.source_experiment_id,
        Some(ExperimentId::new("exp-a"))
    );
    assert_eq!(lifecycle.source_revision_id, Some(RevisionId::new("rev-a")));
    assert_eq!(lifecycle.window_id, WindowId(7));
    assert_eq!(lifecycle.plan_epoch, 3);

    let schedule = records
        .iter()
        .find(|record| record.kind == OperatorControlReplayKind::ScheduleEpoch)
        .expect("schedule record");
    assert_eq!(schedule.network_id, NetworkId::new("mainnet"));
    assert_eq!(schedule.experiment_id, ExperimentId::new("exp-a"));
    assert_eq!(schedule.revision_id, RevisionId::new("rev-a"));
    assert_eq!(schedule.peer_id, Some(PeerId::new("trainer-a")));
    assert_eq!(schedule.window_id, WindowId(8));
    assert_eq!(schedule.ends_before_window, Some(WindowId(12)));
    assert_eq!(schedule.slot_index, Some(0));
    assert_eq!(schedule.plan_epoch, 4);
}

#[test]
fn file_operator_store_control_replay_page_filters_network_and_window() {
    let now = Utc::now();
    let preview = sample_lifecycle_and_schedule_preview(now);
    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: None,
        },
        preview,
    );

    let page = store
        .control_replay_page(
            &OperatorControlReplayQuery {
                kind: Some(OperatorControlReplayKind::ScheduleEpoch),
                network_id: Some(NetworkId::new("mainnet")),
                window_id: Some(WindowId(8)),
                experiment_id: Some(ExperimentId::new("exp-a")),
                peer_id: Some(PeerId::new("trainer-a")),
                ..OperatorControlReplayQuery::default()
            },
            PageRequest::default(),
        )
        .expect("control replay page");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].kind, OperatorControlReplayKind::ScheduleEpoch);
    assert_eq!(page.items[0].slot_index, Some(0));
    assert_eq!(page.items[0].window_id, WindowId(8));
}

#[test]
fn file_operator_store_control_replay_summary_counts_distinct_scope_values() {
    let now = Utc::now();
    let preview = sample_lifecycle_and_schedule_preview(now);
    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: None,
        },
        preview,
    );

    let summary = store
        .control_replay_summary(&OperatorControlReplayQuery::default())
        .expect("control replay summary");

    assert_eq!(summary.backend, "file");
    assert_eq!(summary.record_count, 2);
    assert_eq!(summary.counts_by_kind.get("lifecycle-plan"), Some(&1));
    assert_eq!(summary.counts_by_kind.get("schedule-epoch"), Some(&1));
    assert_eq!(summary.distinct_network_count, 1);
    assert_eq!(summary.distinct_study_count, 1);
    assert_eq!(summary.distinct_experiment_count, 2);
    assert_eq!(summary.distinct_revision_count, 2);
    assert_eq!(summary.distinct_peer_count, 1);
    assert_eq!(summary.distinct_window_count, 2);
}

#[test]
fn file_operator_store_audit_page_filters_kind_text_and_time() {
    let now = Utc::now();
    let preview = sample_operator_preview(now);
    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: None,
        },
        preview,
    );

    let page = store
        .audit_page(
            &OperatorAuditQuery {
                kind: Some(OperatorAuditKind::PeerWindowMetric),
                since: Some(now - chrono::Duration::seconds(2)),
                until: Some(now),
                text: Some("completed".into()),
                ..OperatorAuditQuery::default()
            },
            PageRequest::default(),
        )
        .expect("audit page");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].kind, OperatorAuditKind::PeerWindowMetric);
    assert_eq!(
        page.items[0].summary.get("status"),
        Some(&"Completed".to_string())
    );
}

#[test]
fn file_operator_store_audit_summary_counts_kinds_and_scopes() {
    let now = Utc::now();
    let preview = sample_operator_preview(now);
    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: None,
        },
        preview,
    );

    let summary = store
        .audit_summary(&OperatorAuditQuery {
            since: Some(now - chrono::Duration::seconds(10)),
            until: Some(now),
            ..OperatorAuditQuery::default()
        })
        .expect("audit summary");

    assert_eq!(summary.backend, "file");
    assert_eq!(summary.record_count, 6);
    assert_eq!(summary.counts_by_kind.get("receipt"), Some(&1));
    assert_eq!(summary.counts_by_kind.get("snapshot"), None);
    assert_eq!(summary.distinct_study_count, 1);
    assert_eq!(summary.distinct_experiment_count, 1);
    assert_eq!(summary.distinct_revision_count, 1);
    assert_eq!(summary.distinct_head_count, 2);
    assert!(summary.earliest_captured_at.is_some());
    assert!(summary.latest_captured_at.is_some());
}

#[test]
fn file_operator_store_audit_facets_surface_top_buckets() {
    let now = Utc::now();
    let preview = sample_operator_preview(now);
    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: None,
        },
        preview,
    );

    let facets = store
        .audit_facets(&OperatorAuditQuery::default(), 4)
        .expect("audit facets");

    assert_eq!(facets.backend, "file");
    assert_eq!(facets.limit, 4);
    assert_eq!(
        facets.kinds.first().map(|bucket| bucket.value.as_str()),
        Some("head")
    );
    assert!(
        facets
            .experiments
            .iter()
            .any(|bucket| bucket.value == "exp" && bucket.count >= 1)
    );
    assert!(
        facets
            .heads
            .iter()
            .any(|bucket| bucket.value == "head-1" && bucket.count >= 1)
    );
}

#[test]
fn operator_retention_limits_scale_with_metrics_budget() {
    let default_budget = MetricsRetentionBudget::default();
    let expanded_budget = MetricsRetentionBudget {
        max_metric_revisions_per_experiment: default_budget.max_metric_revisions_per_experiment * 4,
        ..default_budget
    };

    assert!(
        operator_snapshot_retention_limit(expanded_budget)
            > operator_snapshot_retention_limit(default_budget)
    );
    assert!(
        operator_audit_retention_limit(expanded_budget)
            > operator_audit_retention_limit(default_budget)
    );
}

#[test]
fn postgres_operator_replay_retention_supports_long_horizon_search_and_admin_prune() {
    let Some(postgres) = PostgresTestServer::spawn() else {
        eprintln!(
            "skipping postgres-backed operator replay test because docker or postgres container startup is unavailable"
        );
        return;
    };

    let backend = OperatorStateBackendConfig::Postgres {
        url: postgres.url.clone(),
        table_name: format!("operator_state_{}", std::process::id()),
        snapshot_key: "replay-history".into(),
    };
    let retained_budget = MetricsRetentionBudget {
        max_metric_revisions_per_experiment: 16,
        ..MetricsRetentionBudget::operator()
    };
    for index in 0..400 {
        persist_operator_state_snapshot(
            Some(&backend),
            retained_budget,
            &sample_replay_preview(
                index,
                Utc::now() - chrono::Duration::seconds((400 - index) as i64),
            ),
        )
        .expect("persist postgres-backed operator snapshot");
    }

    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::peer_lean(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: Some(backend.clone()),
        },
        FileOperatorStorePreview::default(),
    );

    let initial_retention = store.retention_summary().expect("retention summary");
    assert_eq!(initial_retention.backend, "postgres");
    assert_eq!(initial_retention.persisted_snapshot_count, 400);
    assert!(initial_retention.persisted_audit_record_count >= 2_800);

    let replay_page = store
        .replay_page(
            &OperatorReplayQuery {
                experiment_id: Some(ExperimentId::new("exp-a")),
                ..OperatorReplayQuery::default()
            },
            PageRequest::new(0, 25),
        )
        .expect("replay page");
    assert_eq!(replay_page.limit, 25);
    assert_eq!(replay_page.total, 200);
    assert_eq!(replay_page.items.len(), 25);
    assert!(replay_page.items.iter().all(|item| {
        item.experiment_ids
            .iter()
            .any(|experiment_id| experiment_id == &ExperimentId::new("exp-a"))
    }));

    let replay_snapshot = store
        .replay_snapshot(Some(replay_page.items[10].captured_at))
        .expect("replay snapshot")
        .expect("retained replay snapshot");
    assert_eq!(replay_snapshot.heads.len(), 1);
    assert_eq!(replay_snapshot.receipts.len(), 1);
    assert_eq!(replay_snapshot.merges.len(), 1);

    let plan = crate::BootstrapSpec {
        preset: crate::BootstrapPreset::BootstrapOnly,
        genesis: burn_p2p::GenesisSpec {
            network_id: NetworkId::new("mainnet"),
            protocol_version: semver::Version::new(0, 1, 0),
            display_name: "Operator Replay".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        platform: burn_p2p_core::ClientPlatform::Native,
        bootstrap_addresses: Vec::new(),
        listen_addresses: Vec::new(),
        authority: None,
        archive: crate::ArchivePlan::default(),
        admin_api: crate::AdminApiPlan::default(),
    }
    .plan()
    .expect("bootstrap plan");
    let mut state = crate::BootstrapAdminState {
        operator_state_backend: Some(backend),
        metrics_retention: MetricsRetentionBudget::peer_lean(),
        ..crate::BootstrapAdminState::default()
    };
    let prune = plan
        .execute_admin_action(
            crate::AdminAction::PruneOperatorRetention,
            &mut state,
            None,
            Utc::now(),
            None,
        )
        .expect("prune operator retention");
    let crate::AdminResult::OperatorRetentionPruned(prune) = prune else {
        panic!("expected operator retention prune result");
    };
    assert_eq!(prune.backend, "postgres");
    assert_eq!(prune.remaining_snapshot_count, 128);
    assert!(prune.pruned_snapshot_count >= 272);

    let post_prune = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::peer_lean(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: state.operator_state_backend.clone(),
        },
        FileOperatorStorePreview::default(),
    )
    .retention_summary()
    .expect("post-prune retention summary");
    assert_eq!(post_prune.persisted_snapshot_count, 128);
    assert!(
        post_prune.persisted_audit_record_count <= initial_retention.persisted_audit_record_count
    );
}

#[test]
fn redis_operator_snapshot_shares_heads_metrics_and_head_eval_reports_across_edges() {
    let Some(redis) = RedisTestServer::spawn() else {
        eprintln!("skipping redis-backed operator-store test because redis-server is unavailable");
        return;
    };

    let now = Utc::now();
    let head = HeadDescriptor {
        head_id: burn_p2p_core::HeadId::new("head-1"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        artifact_id: ArtifactId::new("artifact-1"),
        parent_head_id: Some(burn_p2p_core::HeadId::new("base")),
        global_step: 42,
        created_at: now,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.125))]),
    };
    let report = HeadEvalReport {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        head_id: head.head_id.clone(),
        base_head_id: head.parent_head_id.clone(),
        eval_protocol_id: ContentId::new("protocol"),
        evaluator_set_id: ContentId::new("evalset"),
        metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
        sample_count: 128,
        dataset_view_id: DatasetViewId::new("validation"),
        started_at: now,
        finished_at: now,
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    };
    let peer_window_metrics = PeerWindowMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        peer_id: PeerId::new("trainer-a"),
        principal_id: None,
        lease_id: burn_p2p::LeaseId::new("lease-a"),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        window_started_at: now - chrono::Duration::seconds(5),
        window_finished_at: now - chrono::Duration::seconds(1),
        attempted_tokens_or_samples: 128,
        accepted_tokens_or_samples: Some(128),
        local_train_loss_mean: Some(0.25),
        local_train_loss_last: Some(0.2),
        grad_or_delta_norm: Some(0.9),
        optimizer_step_count: 8,
        compute_time_ms: 1_200,
        data_fetch_time_ms: 200,
        publish_latency_ms: 80,
        head_lag_at_start: 0,
        head_lag_at_finish: 1,
        backend_class: burn_p2p_core::BackendClass::Ndarray,
        role: PeerRole::TrainerCpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    };
    let reducer_cohort_metrics = ReducerCohortMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        merge_window_id: ContentId::new("merge-window-a"),
        reducer_group_id: ContentId::new("reducers-a"),
        captured_at: now,
        base_head_id: burn_p2p_core::HeadId::new("base"),
        candidate_head_id: Some(head.head_id.clone()),
        received_updates: 2,
        accepted_updates: 2,
        rejected_updates: 0,
        sum_weight: 2.0,
        accepted_tokens_or_samples: 128,
        staleness_mean: 0.1,
        staleness_max: 0.5,
        window_close_delay_ms: 40,
        cohort_duration_ms: 500,
        aggregate_norm: 0.7,
        reducer_load: 0.25,
        ingress_bytes: 1024,
        egress_bytes: 512,
        replica_agreement: Some(1.0),
        late_arrival_count: Some(0),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::new(),
        status: burn_p2p_core::ReducerCohortStatus::Closed,
    };
    let preview = FileOperatorStorePreview {
        heads: vec![head.clone()],
        peer_window_metrics: vec![peer_window_metrics.clone()],
        reducer_cohort_metrics: vec![reducer_cohort_metrics.clone()],
        head_eval_reports: vec![report.clone()],
        ..FileOperatorStorePreview::default()
    };
    let backend = OperatorStateBackendConfig::Redis {
        url: redis.url.clone(),
        snapshot_key: "burn-p2p:test:operator-state:snapshot".into(),
    };

    persist_operator_state_snapshot(Some(&backend), MetricsRetentionBudget::default(), &preview)
        .expect("persist redis-backed operator snapshot");

    let reader = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::default(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: Some(backend),
        },
        FileOperatorStorePreview::default(),
    );

    assert_eq!(
        reader
            .heads(&HeadQuery {
                head_id: Some(head.head_id.clone()),
                ..HeadQuery::default()
            })
            .expect("load heads from redis snapshot"),
        vec![head.clone()]
    );
    assert_eq!(
        reader
            .head_eval_reports(&head.head_id)
            .expect("load head eval reports from redis snapshot"),
        vec![report]
    );
    assert_eq!(
        reader
            .all_peer_window_metrics()
            .expect("load peer window metrics from redis snapshot"),
        vec![peer_window_metrics]
    );
    assert_eq!(
        reader
            .all_reducer_cohort_metrics()
            .expect("load reducer cohort metrics from redis snapshot"),
        vec![reducer_cohort_metrics]
    );
    let replay = reader
        .replay_snapshot(None)
        .expect("load replay snapshot from redis snapshot")
        .expect("replay snapshot");
    assert_eq!(replay.heads, vec![head]);
}

#[cfg(feature = "metrics-indexer")]
#[test]
fn redis_operator_snapshot_drives_metrics_exports_across_edges() {
    let Some(redis) = RedisTestServer::spawn() else {
        eprintln!("skipping redis-backed metrics export test because redis-server is unavailable");
        return;
    };

    let now = Utc::now();
    let backend = OperatorStateBackendConfig::Redis {
        url: redis.url.clone(),
        snapshot_key: "burn-p2p:test:operator-state:metrics-snapshot".into(),
    };
    let peer_window_metrics = PeerWindowMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        peer_id: PeerId::new("trainer-a"),
        principal_id: None,
        lease_id: burn_p2p::LeaseId::new("lease-a"),
        base_head_id: burn_p2p_core::HeadId::new("base"),
        window_started_at: now - chrono::Duration::seconds(5),
        window_finished_at: now - chrono::Duration::seconds(1),
        attempted_tokens_or_samples: 128,
        accepted_tokens_or_samples: Some(128),
        local_train_loss_mean: Some(0.25),
        local_train_loss_last: Some(0.2),
        grad_or_delta_norm: Some(0.9),
        optimizer_step_count: 8,
        compute_time_ms: 1_200,
        data_fetch_time_ms: 200,
        publish_latency_ms: 80,
        head_lag_at_start: 0,
        head_lag_at_finish: 1,
        backend_class: burn_p2p_core::BackendClass::Ndarray,
        role: PeerRole::TrainerCpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    };
    let reducer_cohort_metrics = ReducerCohortMetrics {
        network_id: NetworkId::new("mainnet"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        workload_id: WorkloadId::new("workload"),
        dataset_view_id: DatasetViewId::new("validation"),
        merge_window_id: ContentId::new("merge-window-a"),
        reducer_group_id: ContentId::new("reducers-a"),
        captured_at: now,
        base_head_id: burn_p2p_core::HeadId::new("base"),
        candidate_head_id: Some(burn_p2p_core::HeadId::new("candidate")),
        received_updates: 2,
        accepted_updates: 2,
        rejected_updates: 0,
        sum_weight: 2.0,
        accepted_tokens_or_samples: 128,
        staleness_mean: 0.1,
        staleness_max: 0.5,
        window_close_delay_ms: 40,
        cohort_duration_ms: 500,
        aggregate_norm: 0.7,
        reducer_load: 0.25,
        ingress_bytes: 1024,
        egress_bytes: 512,
        replica_agreement: Some(1.0),
        late_arrival_count: Some(0),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::new(),
        status: burn_p2p_core::ReducerCohortStatus::Closed,
    };
    let report = HeadEvalReport {
        network_id: NetworkId::new("mainnet"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        workload_id: WorkloadId::new("workload"),
        head_id: burn_p2p_core::HeadId::new("candidate"),
        base_head_id: Some(burn_p2p_core::HeadId::new("base")),
        eval_protocol_id: ContentId::new("protocol"),
        evaluator_set_id: ContentId::new("validators"),
        metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
        sample_count: 128,
        dataset_view_id: DatasetViewId::new("validation"),
        started_at: now - chrono::Duration::seconds(2),
        finished_at: now - chrono::Duration::seconds(1),
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    };

    persist_operator_state_snapshot(
        Some(&backend),
        MetricsRetentionBudget::default(),
        &FileOperatorStorePreview {
            peer_window_metrics: vec![peer_window_metrics.clone()],
            reducer_cohort_metrics: vec![reducer_cohort_metrics.clone()],
            head_eval_reports: vec![report],
            ..FileOperatorStorePreview::default()
        },
    )
    .expect("persist redis-backed operator metrics snapshot");

    let state = crate::BootstrapAdminState {
        operator_state_backend: Some(backend),
        metrics_retention: MetricsRetentionBudget::default(),
        ..crate::BootstrapAdminState::default()
    };

    let snapshots = state
        .export_metrics_snapshots()
        .expect("export metrics snapshots from redis-backed state");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].peer_window_metrics, vec![peer_window_metrics]);
    assert_eq!(
        snapshots[0].reducer_cohort_metrics,
        vec![reducer_cohort_metrics]
    );
}

#[test]
fn postgres_operator_control_replay_search_filters_by_window() {
    let Some(postgres) = PostgresTestServer::spawn() else {
        eprintln!(
            "skipping postgres-backed control replay test because docker or postgres container startup is unavailable"
        );
        return;
    };

    let backend = OperatorStateBackendConfig::Postgres {
        url: postgres.url.clone(),
        table_name: format!("operator_control_state_{}", std::process::id()),
        snapshot_key: "control-history".into(),
    };
    persist_operator_state_snapshot(
        Some(&backend),
        MetricsRetentionBudget::operator(),
        &sample_lifecycle_and_schedule_preview(Utc::now()),
    )
    .expect("persist postgres-backed control snapshot");

    let store = FileOperatorStore::new(
        FileOperatorStoreConfig {
            history_root: None,
            metrics_store_root: None,
            metrics_retention: MetricsRetentionBudget::operator(),
            publication_store_root: None,
            publication_targets: Vec::new(),
            artifact_store_root: None,
            operator_state_backend: Some(backend),
        },
        FileOperatorStorePreview::default(),
    );

    let page = store
        .control_replay_page(
            &OperatorControlReplayQuery {
                window_id: Some(WindowId(7)),
                ..OperatorControlReplayQuery::default()
            },
            PageRequest::default(),
        )
        .expect("postgres control replay page");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].kind, OperatorControlReplayKind::LifecyclePlan);
    assert_eq!(page.items[0].window_id, WindowId(7));
}
