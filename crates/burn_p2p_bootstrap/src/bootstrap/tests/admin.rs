use super::shared::*;

#[test]
fn admin_route_accepts_operator_session_and_rejects_unprivileged_session() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
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
        config_path: Arc::new(temp.path().join("admin-rbac.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };
    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let session_header = session.session_id.as_str().to_owned();
    let allowed = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[("x-session-id", session_header.as_str())],
        },
    );
    assert!(allowed.starts_with("HTTP/1.1 200 OK"));

    let temp = tempdir().expect("temp dir");
    let mut unprivileged = sample_auth_config(temp.path());
    unprivileged.principals[0]
        .custom_claims
        .remove("operator_role");
    let auth = Arc::new(
        build_auth_portal(
            &unprivileged,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
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
        config_path: Arc::new(temp.path().join("admin-rbac-unprivileged.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };
    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let denied = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[("x-session-id", session.session_id.as_str())],
        },
    );
    assert!(denied.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(response_body(&denied).contains("ExportDiagnostics"));
}

#[test]
fn admin_token_is_dev_only_and_disabled_by_default() {
    let temp = tempdir().expect("temp dir");
    let disabled_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
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
        config_path: Arc::new(temp.path().join("admin-token-disabled.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let disabled = issue_request(
        disabled_context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[],
        },
    );
    assert!(disabled.starts_with("HTTP/1.1 401 Unauthorized"));

    let enabled_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-token-enabled.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let enabled = issue_request(
        enabled_context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[],
        },
    );
    assert!(enabled.starts_with("HTTP/1.1 200 OK"));
}

#[test]
fn admin_route_publishes_lifecycle_certificate_to_runtime_control_plane() {
    let temp = tempdir().expect("temp dir");
    let genesis = burn_p2p::GenesisSpec {
        network_id: NetworkId::new("secure-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Secure Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::new(),
    };
    let running = burn_p2p::NodeBuilder::new(())
        .with_mainnet(genesis)
        .with_listen_address(burn_p2p::SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn runtime");
    let telemetry = running.telemetry();
    let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry.snapshot().status != burn_p2p::RuntimeStatus::Running {
        assert!(
            std::time::Instant::now() < deadline,
            "runtime did not start"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let release_policy = burn_p2p::ReleasePolicy::new(
        Version::new(0, 1, 0),
        vec![semver::VersionReq::parse("^0.1").expect("version req")],
    )
    .expect("release policy");
    let spec = BootstrapSpec {
        preset: BootstrapPreset::AuthorityValidator,
        authority: Some(burn_p2p_bootstrap::AuthorityPlan {
            release_policy: release_policy.clone(),
            validator_policy: burn_p2p::ValidatorPolicy {
                release_policy,
                evidence_requirement: burn_p2p::MergeEvidenceRequirement::default(),
            },
            validator_set_manifest: None,
            authority_epoch_manifest: None,
        }),
        ..sample_spec()
    };
    let context = HttpServerContext {
        plan: Arc::new(spec.clone().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec,
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-lifecycle.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: Some(running.control_handle()),
    };

    let response = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(
                serde_json::to_value(burn_p2p_bootstrap::AdminAction::Lifecycle(Box::new(
                    burn_p2p_experiment::ExperimentLifecyclePlan {
                        study_id: burn_p2p::StudyId::new("study"),
                        experiment_id: burn_p2p::ExperimentId::new("exp"),
                        base_revision_id: Some(burn_p2p::RevisionId::new("rev-a")),
                        target_entry: ExperimentDirectoryEntry {
                            network_id: NetworkId::new("secure-demo"),
                            study_id: burn_p2p::StudyId::new("study"),
                            experiment_id: burn_p2p::ExperimentId::new("exp"),
                            workload_id: burn_p2p::WorkloadId::new("compiled"),
                            display_name: "exp rev-b".into(),
                            model_schema_hash: ContentId::new("schema"),
                            dataset_view_id: burn_p2p::DatasetViewId::new("view"),
                            resource_requirements: ExperimentResourceRequirements {
                                minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                                minimum_device_memory_bytes: None,
                                minimum_system_memory_bytes: None,
                                estimated_download_bytes: 0,
                                estimated_window_seconds: 60,
                            },
                            visibility: ExperimentVisibility::Public,
                            opt_in_policy: ExperimentOptInPolicy::Open,
                            current_revision_id: burn_p2p::RevisionId::new("rev-b"),
                            current_head_id: None,
                            allowed_roles: PeerRoleSet::default_trainer(),
                            allowed_scopes: BTreeSet::new(),
                            training_protocol: Default::default(),
                            metadata: BTreeMap::new(),
                        },
                        phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                        target: burn_p2p_experiment::ActivationTarget {
                            activation: burn_p2p::WindowActivation {
                                activation_window: burn_p2p::WindowId(9),
                                grace_windows: 0,
                            },
                            required_client_capabilities: BTreeSet::new(),
                        },
                        plan_epoch: 7,
                        reason: Some("publish lifecycle from admin route".into()),
                    },
                )))
                .expect("serialize lifecycle action"),
            ),
            headers: &[],
        },
    );
    assert!(response.starts_with("HTTP/1.1 200 OK"));

    let lifecycle_deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry
        .snapshot()
        .control_plane
        .lifecycle_announcements
        .len()
        != 1
    {
        assert!(
            std::time::Instant::now() < lifecycle_deadline,
            "lifecycle announcement was not published to the runtime control plane"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.lifecycle_announcements.len(), 1);
    assert_eq!(
        snapshot.control_plane.lifecycle_announcements[0]
            .certificate
            .body
            .payload
            .payload
            .plan
            .plan_epoch,
        7
    );

    running.shutdown().expect("shutdown runtime");
    let _ = running.await_termination().expect("await runtime");
}

#[test]
fn admin_route_publishes_registered_live_head_to_runtime_control_plane() {
    let temp = tempdir().expect("temp dir");
    let genesis = burn_p2p::GenesisSpec {
        network_id: NetworkId::new("secure-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Secure Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::new(),
    };
    let running = burn_p2p::NodeBuilder::new(())
        .with_mainnet(genesis)
        .with_listen_address(burn_p2p::SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn runtime");
    let telemetry = running.telemetry();
    let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry.snapshot().status != burn_p2p::RuntimeStatus::Running {
        assert!(
            std::time::Instant::now() < deadline,
            "runtime did not start"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-live-head.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: Some(running.control_handle()),
    };
    let study_id = burn_p2p::StudyId::new("study");
    let experiment_id = burn_p2p::ExperimentId::new("exp");
    let head = HeadDescriptor {
        head_id: burn_p2p::HeadId::new("head-live"),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: burn_p2p::RevisionId::new("rev-a"),
        artifact_id: burn_p2p::ArtifactId::new("artifact-live"),
        parent_head_id: None,
        global_step: 3,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    };
    let provider_peer_id = PeerId::new("edge-mirror-provider");
    let announcement = burn_p2p::HeadAnnouncement {
        overlay: burn_p2p::OverlayTopic::experiment(
            NetworkId::new("secure-demo"),
            study_id,
            experiment_id,
            burn_p2p::OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(provider_peer_id.clone()),
        head: head.clone(),
        announced_at: Utc::now(),
    };

    let response = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(
                serde_json::to_value(burn_p2p_bootstrap::AdminAction::RegisterLiveHead(
                    announcement,
                ))
                .expect("serialize live head action"),
            ),
            headers: &[],
        },
    );
    assert!(response.starts_with("HTTP/1.1 200 OK"));

    let head_deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry
        .snapshot()
        .control_plane
        .head_announcements
        .iter()
        .all(|candidate| candidate.head.head_id != head.head_id)
    {
        assert!(
            std::time::Instant::now() < head_deadline,
            "registered live head was not published to the runtime control plane"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let snapshot = telemetry.snapshot();
    let runtime_head = snapshot
        .control_plane
        .head_announcements
        .iter()
        .find(|candidate| candidate.head.head_id == head.head_id)
        .expect("runtime head announcement");
    assert_eq!(
        runtime_head.provider_peer_id.as_ref(),
        Some(&provider_peer_id)
    );
    assert_eq!(runtime_head.head.artifact_id, head.artifact_id);

    running.shutdown().expect("shutdown runtime");
    let _ = running.await_termination().expect("await runtime");
}

#[test]
fn admin_route_publishes_cross_experiment_lifecycle_reassignment_certificate() {
    let temp = tempdir().expect("temp dir");
    let genesis = burn_p2p::GenesisSpec {
        network_id: NetworkId::new("secure-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Secure Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::new(),
    };
    let running = burn_p2p::NodeBuilder::new(())
        .with_mainnet(genesis)
        .with_listen_address(burn_p2p::SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn runtime");
    let telemetry = running.telemetry();
    let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry.snapshot().status != burn_p2p::RuntimeStatus::Running {
        assert!(
            std::time::Instant::now() < deadline,
            "runtime did not start"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let release_policy = burn_p2p::ReleasePolicy::new(
        Version::new(0, 1, 0),
        vec![semver::VersionReq::parse("^0.1").expect("version req")],
    )
    .expect("release policy");
    let spec = BootstrapSpec {
        preset: BootstrapPreset::AuthorityValidator,
        authority: Some(burn_p2p_bootstrap::AuthorityPlan {
            release_policy: release_policy.clone(),
            validator_policy: burn_p2p::ValidatorPolicy {
                release_policy,
                evidence_requirement: burn_p2p::MergeEvidenceRequirement::default(),
            },
            validator_set_manifest: None,
            authority_epoch_manifest: None,
        }),
        ..sample_spec()
    };
    let context = HttpServerContext {
        plan: Arc::new(spec.clone().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec,
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-cross-experiment-lifecycle.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: Some(running.control_handle()),
    };

    let response = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(
                serde_json::to_value(burn_p2p_bootstrap::AdminAction::Lifecycle(Box::new(
                    burn_p2p_experiment::ExperimentLifecyclePlan {
                        study_id: burn_p2p::StudyId::new("study"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-a"),
                        base_revision_id: Some(burn_p2p::RevisionId::new("rev-a")),
                        target_entry: ExperimentDirectoryEntry {
                            network_id: NetworkId::new("secure-demo"),
                            study_id: burn_p2p::StudyId::new("study"),
                            experiment_id: burn_p2p::ExperimentId::new("exp-b"),
                            workload_id: burn_p2p::WorkloadId::new("alternate"),
                            display_name: "exp-b rev-b".into(),
                            model_schema_hash: ContentId::new("schema-b"),
                            dataset_view_id: burn_p2p::DatasetViewId::new("view-b"),
                            resource_requirements: ExperimentResourceRequirements {
                                minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                                minimum_device_memory_bytes: None,
                                minimum_system_memory_bytes: None,
                                estimated_download_bytes: 0,
                                estimated_window_seconds: 60,
                            },
                            visibility: ExperimentVisibility::Public,
                            opt_in_policy: ExperimentOptInPolicy::Open,
                            current_revision_id: burn_p2p::RevisionId::new("rev-b"),
                            current_head_id: None,
                            allowed_roles: PeerRoleSet::default_trainer(),
                            allowed_scopes: BTreeSet::new(),
                            training_protocol: Default::default(),
                            metadata: BTreeMap::new(),
                        },
                        phase: burn_p2p_experiment::ExperimentLifecyclePhase::Activating,
                        target: burn_p2p_experiment::ActivationTarget {
                            activation: burn_p2p::WindowActivation {
                                activation_window: burn_p2p::WindowId(4),
                                grace_windows: 0,
                            },
                            required_client_capabilities: BTreeSet::new(),
                        },
                        plan_epoch: 11,
                        reason: Some("reassign exp-a slot to exp-b".into()),
                    },
                )))
                .expect("serialize cross-experiment lifecycle action"),
            ),
            headers: &[],
        },
    );
    assert!(response.starts_with("HTTP/1.1 200 OK"));

    let lifecycle_deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry
        .snapshot()
        .control_plane
        .lifecycle_announcements
        .len()
        != 1
    {
        assert!(
            std::time::Instant::now() < lifecycle_deadline,
            "cross-experiment lifecycle announcement was not published to the runtime control plane"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let snapshot = telemetry.snapshot();
    let plan = &snapshot.control_plane.lifecycle_announcements[0]
        .certificate
        .body
        .payload
        .payload
        .plan;
    assert_eq!(plan.experiment_id, burn_p2p::ExperimentId::new("exp-a"));
    assert_eq!(
        plan.target_entry.experiment_id,
        burn_p2p::ExperimentId::new("exp-b")
    );
    assert_eq!(
        plan.target_entry.current_revision_id,
        burn_p2p::RevisionId::new("rev-b")
    );

    running.shutdown().expect("shutdown runtime");
    let _ = running.await_termination().expect("await runtime");
}

#[test]
fn admin_route_publishes_schedule_certificate_to_runtime_control_plane() {
    let temp = tempdir().expect("temp dir");
    let genesis = burn_p2p::GenesisSpec {
        network_id: NetworkId::new("secure-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Secure Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::new(),
    };
    let running = burn_p2p::NodeBuilder::new(())
        .with_mainnet(genesis)
        .with_listen_address(burn_p2p::SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn runtime");
    let telemetry = running.telemetry();
    let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry.snapshot().status != burn_p2p::RuntimeStatus::Running {
        assert!(
            std::time::Instant::now() < deadline,
            "runtime did not start"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let release_policy = burn_p2p::ReleasePolicy::new(
        Version::new(0, 1, 0),
        vec![semver::VersionReq::parse("^0.1").expect("version req")],
    )
    .expect("release policy");
    let spec = BootstrapSpec {
        preset: BootstrapPreset::AuthorityValidator,
        authority: Some(burn_p2p_bootstrap::AuthorityPlan {
            release_policy: release_policy.clone(),
            validator_policy: burn_p2p::ValidatorPolicy {
                release_policy,
                evidence_requirement: burn_p2p::MergeEvidenceRequirement::default(),
            },
            validator_set_manifest: None,
            authority_epoch_manifest: None,
        }),
        ..sample_spec()
    };
    let context = HttpServerContext {
        plan: Arc::new(spec.clone().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec,
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-schedule.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: Some(running.control_handle()),
    };

    let response = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(
                serde_json::to_value(burn_p2p_bootstrap::AdminAction::Schedule(Box::new(
                    burn_p2p_experiment::FleetScheduleEpochBuilder::new()
                        .with_activation(burn_p2p::WindowActivation {
                            activation_window: burn_p2p::WindowId(6),
                            grace_windows: 0,
                        })
                        .ending_before(burn_p2p::WindowId(10))
                        .with_plan_epoch(9)
                        .with_reason("rebalance local validator tier")
                        .assign_peer_slot_scaled(
                            PeerId::new("trainer-a"),
                            0,
                            burn_p2p::StudyId::new("study"),
                            burn_p2p::ExperimentId::new("exp"),
                            burn_p2p::RevisionId::new("rev-b"),
                            Some(0.5),
                            Some(0.25),
                        )
                        .build(),
                )))
                .expect("serialize schedule action"),
            ),
            headers: &[],
        },
    );
    assert!(response.starts_with("HTTP/1.1 200 OK"));

    let schedule_deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    while telemetry
        .snapshot()
        .control_plane
        .schedule_announcements
        .len()
        != 1
    {
        assert!(
            std::time::Instant::now() < schedule_deadline,
            "schedule announcement was not published to the runtime control plane"
        );
        thread::sleep(StdDuration::from_millis(25));
    }

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.schedule_announcements.len(), 1);
    let epoch = &snapshot.control_plane.schedule_announcements[0]
        .certificate
        .body
        .payload
        .payload
        .epoch;
    assert_eq!(epoch.plan_epoch, 9);
    assert_eq!(epoch.assignments.len(), 1);
    assert_eq!(epoch.assignments[0].peer_id, PeerId::new("trainer-a"));

    running.shutdown().expect("shutdown runtime");
    let _ = running.await_termination().expect("await runtime");
}

#[test]
fn auth_portal_rotation_and_policy_rollout_persist_and_reissue() {
    let temp = tempdir().expect("temp dir");
    let auth_config = sample_auth_config(temp.path());
    let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(auth_config.clone()),
        operator_state_backend: None,
        artifact_publication: None,
    }));
    let config_path = Arc::new(temp.path().join("bootstrap-config.json"));
    std::fs::write(
        &*config_path,
        serde_json::to_vec_pretty(
            &*daemon_config
                .lock()
                .expect("daemon config should not be poisoned"),
        )
        .expect("serialize daemon config"),
    )
    .expect("write daemon config");
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let plan = sample_spec().plan().expect("bootstrap plan");
    let state = Arc::new(Mutex::new(BootstrapAdminState::default()));

    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let node_keypair = Keypair::generate_ed25519();
    let peer_id = burn_p2p::PeerId::new(node_keypair.public().to_peer_id().to_string());
    let first_cert = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issue_certificate(NodeEnrollmentRequest {
            session: session.clone(),
            project_family_id: auth.project_family_id.clone(),
            release_train_hash: auth.required_release_train_hash.clone(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: session.claims.granted_roles.clone(),
            requested_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            client_policy_hash: Some(ContentId::new("policy-auth")),
            serial: 1,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(5),
            revocation_epoch: current_revocation_epoch(&auth, &state),
        })
        .expect("first certificate");

    let rolled_directory = vec![ExperimentDirectoryEntry {
        network_id: NetworkId::new("secure-demo"),
        study_id: burn_p2p::StudyId::new("study-auth"),
        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        workload_id: burn_p2p::WorkloadId::new("auth-demo"),
        display_name: "Rolled Demo".into(),
        model_schema_hash: burn_p2p::ContentId::new("model-auth-rolled"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-auth-rolled"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: Some(4096),
            estimated_download_bytes: 98_304,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: burn_p2p::RevisionId::new("rev-auth-rolled"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        }]),
        training_protocol: Default::default(),
        metadata: BTreeMap::from([("rollout".into(), "true".into())]),
    }];
    let rollout = rollout_auth_policy(
        &plan,
        &auth,
        &state,
        AuthPolicyRollout {
            minimum_revocation_epoch: Some(RevocationEpoch(9)),
            directory_entries: Some(rolled_directory.clone()),
            trusted_issuers: None,
            reenrollment: None,
        },
        None,
    )
    .expect("rollout auth policy");
    match rollout {
        burn_p2p_bootstrap::AdminResult::AuthPolicyRolledOut {
            minimum_revocation_epoch,
            directory_entries,
            ..
        } => {
            assert_eq!(minimum_revocation_epoch, Some(RevocationEpoch(9)));
            assert_eq!(directory_entries, 1);
        }
        other => panic!("unexpected rollout result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.minimum_revocation_epoch = auth_config.minimum_revocation_epoch.max(9);
            auth_config.directory_entries = rolled_directory.clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist rollout");
    }

    assert_eq!(current_revocation_epoch(&auth, &state), RevocationEpoch(9));
    let directory_request = HttpRequest {
        method: "GET".into(),
        path: "/directory".into(),
        headers: BTreeMap::from([(
            "x-session-id".into(),
            session.session_id.as_str().to_owned(),
        )]),
        body: Vec::new(),
    };
    let directory = auth_directory_entries(&auth, &directory_request).expect("directory");
    assert_eq!(directory[0].display_name, "Rolled Demo");
    assert_eq!(
        directory[0].current_revision_id,
        burn_p2p::RevisionId::new("rev-auth-rolled")
    );

    let rotate = rotate_authority_material(
        &auth,
        &state,
        Some("rotated-key".into()),
        true,
        true,
        Some("rotate for trust rollout".into()),
    )
    .expect("rotate authority material");
    match rotate {
        burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
            issuer_key_id,
            trusted_issuers,
            reenrollment_required,
            ..
        } => {
            assert_eq!(issuer_key_id, "rotated-key");
            assert_eq!(trusted_issuers, 2);
            assert!(reenrollment_required);
        }
        other => panic!("unexpected rotation result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.issuer_key_id = "rotated-key".into();
            auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                .issuers
                .iter()
                .map(|issuer| TrustedIssuer {
                    issuer_peer_id: issuer.issuer_peer_id.clone(),
                    issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                })
                .collect();
            auth_config.reenrollment = auth
                .reenrollment
                .lock()
                .expect("auth reenrollment state should not be poisoned")
                .clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist rotate");
    }
    let trust_bundle = current_trust_bundle(&auth, &state);
    assert_eq!(trust_bundle.issuers.len(), 2);
    assert!(
        trust_bundle
            .reenrollment
            .as_ref()
            .is_some_and(|status| !status.retired_issuer_peer_ids.is_empty())
    );

    let second_cert = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issue_certificate(NodeEnrollmentRequest {
            session,
            project_family_id: auth.project_family_id.clone(),
            release_train_hash: auth.required_release_train_hash.clone(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            peer_id,
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
            requested_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            client_policy_hash: Some(ContentId::new("policy-auth")),
            serial: 2,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(5),
            revocation_epoch: current_revocation_epoch(&auth, &state),
        })
        .expect("second certificate");
    assert_ne!(
        first_cert.body.signature.signer,
        second_cert.body.signature.signer
    );
    assert_eq!(second_cert.body.signature.key_id, "rotated-key");
    assert_eq!(second_cert.claims().revocation_epoch, RevocationEpoch(9));

    let retired = retire_trusted_issuers(
        &auth,
        &state,
        &BTreeSet::from([first_cert.body.signature.signer.clone()]),
    )
    .expect("retire trusted issuer");
    match retired {
        burn_p2p_bootstrap::AdminResult::TrustedIssuersRetired {
            retired_issuers,
            remaining_issuers,
            reenrollment_required,
        } => {
            assert_eq!(retired_issuers, 1);
            assert_eq!(remaining_issuers, 1);
            assert!(!reenrollment_required);
        }
        other => panic!("unexpected retired result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                .issuers
                .iter()
                .map(|issuer| TrustedIssuer {
                    issuer_peer_id: issuer.issuer_peer_id.clone(),
                    issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                })
                .collect();
            auth_config.reenrollment = auth
                .reenrollment
                .lock()
                .expect("auth reenrollment state should not be poisoned")
                .clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist retire");
    }

    let persisted: BootstrapDaemonConfig = serde_json::from_slice(
        &std::fs::read(&*config_path).expect("read persisted daemon config"),
    )
    .expect("deserialize persisted daemon config");
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .minimum_revocation_epoch,
        9
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .issuer_key_id,
        "rotated-key"
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .directory_entries[0]
            .current_revision_id,
        burn_p2p::RevisionId::new("rev-auth-rolled")
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .trusted_issuers
            .len(),
        1
    );
    assert!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .reenrollment
            .is_none()
    );
}
