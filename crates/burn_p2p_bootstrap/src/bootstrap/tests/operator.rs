use super::shared::*;
use burn_p2p::{
    ExperimentId, HeadId, LagPolicy, LagState, NodeRuntimeState, RevisionId, RuntimeStatus,
    SlotAssignmentState, SlotRuntimeState, StudyId,
};
use burn_p2p_experiment::{
    ActivationTarget, ExperimentLifecycleEnvelope, FleetScheduleEpochEnvelope,
};

fn sample_control_plane_snapshot() -> burn_p2p::ControlPlaneSnapshot {
    let now = Utc::now();
    let signer = burn_p2p_core::SignatureMetadata {
        signer: PeerId::new("bootstrap-authority"),
        key_id: "bootstrap-control-key".into(),
        algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
        signed_at: now,
        signature_hex: "abcd".into(),
    };
    let lifecycle = ExperimentLifecycleEnvelope {
        network_id: NetworkId::new("secure-demo"),
        plan: burn_p2p::ExperimentLifecyclePlan {
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            base_revision_id: Some(RevisionId::new("rev-a")),
            target_entry: ExperimentDirectoryEntry {
                network_id: NetworkId::new("secure-demo"),
                study_id: StudyId::new("study-a"),
                experiment_id: ExperimentId::new("exp-b"),
                workload_id: burn_p2p::WorkloadId::new("alternate"),
                display_name: "exp-b".into(),
                model_schema_hash: ContentId::new("schema-b"),
                dataset_view_id: burn_p2p::DatasetViewId::new("view-b"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                    minimum_device_memory_bytes: None,
                    minimum_system_memory_bytes: None,
                    estimated_download_bytes: 0,
                    estimated_window_seconds: 30,
                },
                visibility: ExperimentVisibility::Public,
                opt_in_policy: ExperimentOptInPolicy::Open,
                current_revision_id: RevisionId::new("rev-b"),
                current_head_id: Some(HeadId::new("head-b")),
                allowed_roles: PeerRoleSet::default_trainer(),
                allowed_scopes: BTreeSet::new(),
                metadata: BTreeMap::new(),
            },
            phase: burn_p2p::ExperimentLifecyclePhase::Activating,
            target: ActivationTarget {
                activation: burn_p2p::WindowActivation {
                    activation_window: burn_p2p::WindowId(7),
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
        network_id: NetworkId::new("secure-demo"),
        epoch: burn_p2p::FleetScheduleEpochBuilder::new()
            .with_activation(burn_p2p::WindowActivation {
                activation_window: burn_p2p::WindowId(8),
                grace_windows: 0,
            })
            .ending_before(burn_p2p::WindowId(12))
            .with_plan_epoch(4)
            .with_reason("reshuffle")
            .assign_peer_slot_scaled(
                PeerId::new("trainer-a"),
                0,
                StudyId::new("study-a"),
                ExperimentId::new("exp-a"),
                RevisionId::new("rev-a"),
                Some(0.5),
                Some(0.25),
            )
            .build(),
    }
    .into_signed_cert(signer, Version::new(0, 1, 0))
    .expect("schedule cert");

    burn_p2p::ControlPlaneSnapshot {
        lifecycle_announcements: vec![burn_p2p::ExperimentLifecycleAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("secure-demo")),
            certificate: lifecycle,
            announced_at: now - Duration::seconds(2),
        }],
        schedule_announcements: vec![burn_p2p::FleetScheduleAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("secure-demo")),
            certificate: schedule,
            announced_at: now - Duration::seconds(1),
        }],
        ..burn_p2p::ControlPlaneSnapshot::default()
    }
}

fn sample_runtime_snapshot() -> burn_p2p::NodeTelemetrySnapshot {
    let now = Utc::now();
    burn_p2p::NodeTelemetrySnapshot {
        status: RuntimeStatus::Running,
        node_state: NodeRuntimeState::IdleReady,
        slot_states: vec![SlotRuntimeState::Assigned(SlotAssignmentState::new(
            StudyId::new("study-a"),
            ExperimentId::new("exp-b"),
            RevisionId::new("rev-b"),
        ))],
        lag_state: LagState::Current,
        head_lag_steps: 0,
        lag_policy: LagPolicy::default(),
        network_id: Some(NetworkId::new("secure-demo")),
        local_peer_id: Some(PeerId::new("bootstrap-authority")),
        configured_roles: PeerRoleSet::default_trainer(),
        connected_peers: 1,
        connected_peer_ids: BTreeSet::new(),
        observed_peer_ids: BTreeSet::new(),
        known_peer_addresses: BTreeSet::new(),
        runtime_boundary: None,
        listen_addresses: Vec::new(),
        control_plane: sample_control_plane_snapshot(),
        recent_events: Vec::new(),
        last_snapshot_peer_id: None,
        last_snapshot: None,
        admitted_peers: BTreeMap::new(),
        rejected_peers: BTreeMap::new(),
        peer_reputation: BTreeMap::new(),
        minimum_revocation_epoch: None,
        trust_bundle: None,
        in_flight_transfers: BTreeMap::new(),
        robustness_policy: None,
        latest_cohort_robustness: None,
        trust_scores: Vec::new(),
        canary_reports: Vec::new(),
        applied_control_cert_ids: BTreeSet::new(),
        effective_limit_profile: None,
        last_error: None,
        started_at: now,
        updated_at: now,
    }
}

#[test]
fn operator_control_routes_expose_typed_control_replay_filters() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-control-routes.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let control_page = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/operator/control/page?kind=schedule-epoch&network_id=secure-demo&window_id=8&peer_id=trainer-a",
            body: None,
            headers: &[],
        },
    ));
    let items = control_page["items"]
        .as_array()
        .expect("control page items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["kind"], "ScheduleEpoch");
    assert_eq!(items[0]["window_id"], 8);
    assert_eq!(items[0]["peer_id"], "trainer-a");

    let control_summary = response_json(&issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/control/summary?experiment_id=exp-b&window_id=7",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(control_summary["record_count"], 1);
    assert_eq!(control_summary["distinct_network_count"], 1);
    assert_eq!(control_summary["counts_by_kind"]["lifecycle-plan"], 1);
}

#[test]
fn operator_control_html_route_renders_human_facing_page() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-control-view.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let html = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/control?kind=schedule-epoch&network_id=secure-demo&window_id=8&peer_id=trainer-a",
            body: None,
            headers: &[],
        },
    );
    assert!(html.starts_with("HTTP/1.1 200 OK"));
    let body = response_body(&html);
    assert!(body.contains("Lifecycle and schedule history"));
    assert!(body.contains("trainer-a"));
    assert!(body.contains("/operator/control/page?kind=schedule-epoch"));
    assert!(body.contains("/operator/control/summary?kind=schedule-epoch"));
    assert!(body.contains("/operator/audit?study_id=study-a"));
    assert!(body.contains("experiment_id=exp-a"));
    assert!(body.contains("revision_id=rev-a"));
    assert!(body.contains("peer_id=trainer-a"));
    assert!(body.contains("/operator/replay?study_id=study-a"));
    assert!(body.contains("Reset query"));
}

#[test]
fn operator_audit_html_route_renders_human_facing_page() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-audit-view.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let html = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/audit?kind=lifecycle-plan&experiment_id=exp-b",
            body: None,
            headers: &[],
        },
    );
    assert!(html.starts_with("HTTP/1.1 200 OK"));
    let body = response_body(&html);
    assert!(body.contains("Operator audit history"));
    assert!(body.contains("kind=lifecycle-plan"));
    assert!(body.contains("/operator/audit/page?kind=lifecycle-plan"));
    assert!(body.contains("/operator/audit/facets?kind=lifecycle-plan"));
    assert!(body.contains("No audit rows matched the current query."));
    assert!(body.contains("Reset query"));
}

#[test]
fn operator_replay_html_route_renders_human_facing_page() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-replay-view.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let html = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/replay?experiment_id=exp-b",
            body: None,
            headers: &[],
        },
    );
    assert!(html.starts_with("HTTP/1.1 200 OK"));
    let body = response_body(&html);
    assert!(body.contains("Retained replay snapshots"));
    assert!(body.contains("experiment_id=exp-b"));
    assert!(body.contains("/operator/replay/page?experiment_id=exp-b"));
    assert!(body.contains("No replay snapshots matched the current query."));
    assert!(body.contains("Reset query"));
}

#[test]
fn operator_replay_snapshot_html_route_renders_human_facing_page() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-replay-snapshot-view.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let html = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/replay/snapshot/view",
            body: None,
            headers: &[],
        },
    );
    assert!(html.starts_with("HTTP/1.1 200 OK"));
    let body = response_body(&html);
    assert!(body.contains("Retained replay snapshot"));
    assert!(body.contains("/operator/replay"));
    assert!(body.contains("/operator/replay/snapshot?captured_at="));
}

#[test]
fn operator_retention_html_route_renders_human_facing_page() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(sample_runtime_snapshot()),
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("operator-retention-view.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let html = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/operator/retention/view",
            body: None,
            headers: &[],
        },
    );
    assert!(html.starts_with("HTTP/1.1 200 OK"));
    let body = response_body(&html);
    assert!(body.contains("Retention policy"));
    assert!(body.contains("/operator/retention"));
}
