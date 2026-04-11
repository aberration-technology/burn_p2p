use super::shared::*;

#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
#[test]
fn http_routes_serve_status_and_static_auth_flow() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: Some(120),
        admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config(temp.path())),
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
    let plan = Arc::new(sample_spec().plan().expect("bootstrap plan"));
    let principal_id = PrincipalId::new("alice");
    let receipt_accepted_at = Utc::now();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            head_id: burn_p2p::HeadId::new("head-auth"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        reducer_load_announcements: vec![ReducerLoadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("secure-demo"),
                burn_p2p::StudyId::new("study-auth"),
                burn_p2p::ExperimentId::new("exp-auth"),
                OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p::ReducerLoadReport {
                peer_id: burn_p2p::PeerId::new("peer-auth"),
                window_id: burn_p2p::WindowId(1),
                assigned_leaf_updates: 1,
                assigned_aggregate_inputs: 0,
                ingress_bytes: 64,
                egress_bytes: 32,
                duplicate_transfer_ratio: 0.0,
                overload_ratio: 0.0,
                reported_at: Utc::now(),
            },
        }],
        contribution_receipts: vec![ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-auth"),
            peer_id: burn_p2p::PeerId::new("peer-auth"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            base_head_id: burn_p2p::HeadId::new("head-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
            accepted_at: receipt_accepted_at,
            accepted_weight: 4.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            merge_cert_id: None,
        }],
        peer_admission_reports: BTreeMap::from([(
            burn_p2p::PeerId::new("peer-auth"),
            PeerAdmissionReport {
                peer_id: burn_p2p::PeerId::new("peer-auth"),
                principal_id: principal_id.clone(),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
                trust_level: PeerTrustLevel::ScopeAuthorized,
                issuer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
                findings: Vec::new(),
                verified_at: Utc::now(),
            },
        )]),
        ..BootstrapAdminState::default()
    }));
    let context = HttpServerContext {
        plan: plan.clone(),
        state: state.clone(),
        config: daemon_config,
        config_path,
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: Some(120),
        admin_signer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };

    let status = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/status",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(status["network_id"], "secure-demo");
    let bundle = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/diagnostics/bundle",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(bundle["plan"]["genesis"]["network_id"], "secure-demo");
    assert_eq!(bundle["diagnostics"]["network_id"], "secure-demo");
    assert_eq!(bundle["heads"][0]["head_id"], "head-auth");
    assert_eq!(
        bundle["reducer_load_announcements"][0]["report"]["peer_id"],
        "peer-auth"
    );
    let heads = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/heads",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(heads.as_array().expect("heads array").len(), 1);
    assert_eq!(heads[0]["head_id"], "head-auth");
    let reducer_load = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/reducers/load",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        reducer_load.as_array().expect("reducer load array").len(),
        1
    );
    assert_eq!(reducer_load[0]["report"]["peer_id"], "peer-auth");
    let trust = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/trust",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(trust["network_id"], "secure-demo");
    assert_eq!(trust["issuers"].as_array().expect("issuer array").len(), 1);
    assert_eq!(trust["reenrollment"], Value::Null);
    let reenrollment = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/reenrollment",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(reenrollment, Value::Null);

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect", {"Train": {"experiment_id": "exp-auth"}}],
            })),
            headers: &[],
        },
    ));
    let session = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_header = session["session_id"].as_str().expect("session id string");
    let directory_headers = [("x-session-id", session_header)];
    let directory = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/directory",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(directory.as_array().expect("directory array").len(), 1);
    assert_eq!(directory[0]["experiment_id"], "exp-auth");

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(portal_snapshot["network_id"], "secure-demo");
    assert_eq!(portal_snapshot["auth_enabled"], true);
    assert_eq!(
        portal_snapshot["leaderboard"]["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(
        portal_snapshot["paths"]["app_snapshot_path"],
        "/portal/snapshot"
    );
    assert_eq!(
        portal_snapshot["paths"]["receipt_submit_path"],
        "/receipts/browser"
    );
    assert_eq!(
        portal_snapshot["directory"]["entries"][0]["experiment_id"],
        "exp-auth"
    );

    let portal_response = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &directory_headers,
        },
    );
    assert!(portal_response.starts_with("HTTP/1.1 404 Not Found"));

    let leaderboard = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(leaderboard["score_version"], "leaderboard_score_v1");
    assert_eq!(
        leaderboard["entries"]
            .as_array()
            .expect("entries array")
            .len(),
        1
    );
    assert_eq!(
        leaderboard["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(leaderboard["entries"][0]["accepted_receipt_count"], 1);

    let receipt_submission = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/receipts/browser",
            body: Some(serde_json::json!([{
                "receipt_id": "receipt-browser-submitted",
                "peer_id": "peer-auth",
                "study_id": "study-auth",
                "experiment_id": "exp-auth",
                "revision_id": "rev-auth",
                "base_head_id": "head-auth",
                "artifact_id": "artifact-browser",
                "accepted_at": Utc::now(),
                "accepted_weight": 2.0,
                "metrics": {"loss": 0.2},
                "merge_cert_id": null
            }])),
            headers: &directory_headers,
        },
    ));
    assert_eq!(
        receipt_submission["accepted_receipt_ids"][0],
        "receipt-browser-submitted"
    );
    assert_eq!(receipt_submission["pending_receipt_count"], 0);

    let signed_directory = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/directory/signed",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(
        signed_directory["payload"]["schema"],
        "burn_p2p.browser_directory_snapshot"
    );
    assert_eq!(
        signed_directory["payload"]["payload"]["entries"][0]["experiment_id"],
        "exp-auth"
    );
    assert_eq!(
        signed_directory["signature"]["signer"],
        "bootstrap-authority"
    );

    let signed_leaderboard = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard/signed",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        signed_leaderboard["payload"]["schema"],
        "burn_p2p.browser_leaderboard_snapshot"
    );
    assert_eq!(
        signed_leaderboard["payload"]["payload"]["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(
        signed_leaderboard["signature"]["signer"],
        "bootstrap-authority"
    );

    let receipts = response_json(&issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/receipts",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(receipts.as_array().expect("receipts array").len(), 2);
}

#[cfg(all(
    feature = "browser-edge",
    feature = "auth-github",
    feature = "auth-oidc"
))]
#[test]
fn github_and_oidc_routes_issue_provider_specific_sessions() {
    let temp = tempdir().expect("temp dir");
    let github_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: None,
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: None,
                    refresh_url: None,
                    revoke_url: None,
                    jwks_url: None,
                    api_base_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github auth portal"),
    );
    let github_context = HttpServerContext {
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
        config_path: Arc::new(temp.path().join("github-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(github_auth),
        control_handle: None,
    };
    let github_login = response_json(&issue_request(
        github_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/github",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(github_login["provider"], "GitHub");
    assert!(
        github_login["authorize_url"]
            .as_str()
            .expect("authorize url")
            .starts_with("https://github.example/login/oauth/authorize")
    );
    let github_session = response_json(&issue_request(
        github_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/github",
            body: Some(serde_json::json!({
                "login_id": github_login["login_id"],
                "state": github_login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    assert_eq!(github_session["claims"]["provider"], "GitHub");
    let github_snapshot = response_json(&issue_request(
        github_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        github_snapshot["login_providers"][0]["login_path"],
        "/login/github"
    );
    assert_eq!(
        github_snapshot["login_providers"][0]["callback_path"],
        "/callback/github"
    );

    let temp = tempdir().expect("temp dir");
    let oidc_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::Oidc {
                    issuer: "https://issuer.example".into(),
                    authorize_base_url: Some("https://issuer.example/authorize".into()),
                    exchange_url: None,
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: None,
                    refresh_url: None,
                    revoke_url: None,
                    jwks_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build oidc auth portal"),
    );
    let oidc_context = HttpServerContext {
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
        config_path: Arc::new(temp.path().join("oidc-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(oidc_auth),
        control_handle: None,
    };
    let oidc_login = response_json(&issue_request(
        oidc_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/device/oidc",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_login["provider"],
        serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
    );
    let oidc_session = response_json(&issue_request(
        oidc_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/oidc",
            body: Some(serde_json::json!({
                "login_id": oidc_login["login_id"],
                "state": oidc_login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_session["claims"]["provider"],
        serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
    );
    let oidc_snapshot = response_json(&issue_request(
        oidc_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_snapshot["login_providers"][0]["login_path"],
        "/login/oidc"
    );
    assert_eq!(
        oidc_snapshot["login_providers"][0]["device_path"],
        "/device/oidc"
    );
}

#[cfg(feature = "auth-external")]
#[test]
fn external_routes_issue_trusted_header_sessions() {
    let temp = tempdir().expect("temp dir");
    let external_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::External {
                    authority: "corp-sso".into(),
                    trusted_principal_header: "x-corp-principal".into(),
                    trusted_internal_only: true,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build external auth portal"),
    );
    let external_context = HttpServerContext {
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
        config_path: Arc::new(temp.path().join("external-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(external_auth),
        control_handle: None,
    };
    let external_login = response_json(&issue_request(
        external_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/external",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        external_login["provider"],
        serde_json::json!({"External": {"authority": "corp-sso"}})
    );
    let external_session = response_json(&issue_request(
        external_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/external",
            body: Some(serde_json::json!({
                "login_id": external_login["login_id"],
                "state": external_login["state"],
            })),
            headers: &[("x-corp-principal", "alice")],
        },
    ));
    assert_eq!(
        external_session["claims"]["provider"],
        serde_json::json!({"External": {"authority": "corp-sso"}})
    );
    let external_snapshot = response_json(&issue_request(
        external_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        external_snapshot["login_providers"][0]["login_path"],
        "/login/external"
    );
    assert_eq!(
        external_snapshot["login_providers"][0]["callback_path"],
        "/callback/external"
    );
}

#[test]
fn auth_portal_refreshes_and_logs_out_sessions() {
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
        config_path: Arc::new(temp.path().join("refresh-logout.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    let session = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_id = ContentId::new(
        session["session_id"]
            .as_str()
            .expect("session id")
            .to_owned(),
    );
    assert!(
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .contains_key(&session_id)
    );

    let refreshed = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/refresh",
            body: Some(serde_json::json!({
                "session_id": session_id,
            })),
            headers: &[],
        },
    ));
    let refreshed_session_id = ContentId::new(
        refreshed["session_id"]
            .as_str()
            .expect("refreshed session id")
            .to_owned(),
    );
    let sessions = auth
        .sessions
        .lock()
        .expect("auth session state should not be poisoned");
    assert!(sessions.contains_key(&refreshed_session_id));
    drop(sessions);

    let logout = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/logout",
            body: Some(serde_json::json!({
                "session_id": refreshed_session_id,
            })),
            headers: &[],
        },
    ));
    assert_eq!(logout["logged_out"], true);
    assert!(
        !auth
            .sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .contains_key(&ContentId::new(
                refreshed["session_id"]
                    .as_str()
                    .expect("refreshed session id")
                    .to_owned(),
            ))
    );
}

#[test]
fn auth_portal_persists_pending_logins_and_sessions_across_restart() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("auth-state.cbor"));

    let auth = Arc::new(
        build_auth_portal(
            &config,
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
        config_path: Arc::new(temp.path().join("persist-auth-state.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert!(
        temp.path().join("auth-state.cbor").exists(),
        "auth state file should be created after begin_login"
    );

    let restored_auth = Arc::new(
        build_auth_portal(
            &config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("restore auth portal"),
    );
    let restored_context = HttpServerContext {
        auth_state: Some(restored_auth.clone()),
        ..context.clone()
    };
    let session = response_json(&issue_request(
        restored_context,
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_id = ContentId::new(
        session["session_id"]
            .as_str()
            .expect("session id")
            .to_owned(),
    );

    let restarted_auth = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("restart auth portal");
    assert!(
        restarted_auth
            .get_session(&session_id)
            .expect("load persisted session")
            .is_some()
    );
}

#[test]
fn auth_portal_shares_pending_logins_and_sessions_across_edges() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("shared-auth-state.cbor"));

    let edge_a = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build first auth portal");
    let edge_b = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build second auth portal");

    let login = edge_a
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin shared login");
    let session = edge_b
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete shared login");
    assert!(
        edge_a
            .get_session(&session.session_id)
            .expect("load session on first edge")
            .is_some()
    );

    let refreshed = edge_a
        .refresh_session(&session.session_id)
        .expect("refresh shared session");
    if refreshed.session_id != session.session_id {
        assert!(
            edge_b
                .get_session(&session.session_id)
                .expect("old session removed on second edge")
                .is_none()
        );
    }
    assert!(
        edge_b
            .get_session(&refreshed.session_id)
            .expect("refreshed session visible on second edge")
            .is_some()
    );

    assert!(
        edge_b
            .revoke_session(&refreshed.session_id)
            .expect("revoke shared session")
    );
    assert!(
        edge_a
            .get_session(&refreshed.session_id)
            .expect("revoked session removed on first edge")
            .is_none()
    );
}

#[test]
fn auth_portal_redis_shares_pending_logins_and_sessions_across_edges() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let edge_a = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build first redis-backed auth portal");
    let edge_b = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build second redis-backed auth portal");

    let login = edge_a
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed shared login");
    let session = edge_b
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed shared login");
    assert!(
        edge_a
            .get_session(&session.session_id)
            .expect("load session on first redis-backed edge")
            .is_some()
    );

    let edge_c = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build restarted redis-backed auth portal");
    let refreshed = edge_c
        .refresh_session(&session.session_id)
        .expect("refresh redis-backed shared session");
    if refreshed.session_id != session.session_id {
        assert!(
            edge_b
                .get_session(&session.session_id)
                .expect("old redis-backed session removed on second edge")
                .is_none()
        );
    }
    assert!(
        edge_b
            .get_session(&refreshed.session_id)
            .expect("refreshed redis-backed session visible on second edge")
            .is_some()
    );

    assert!(
        edge_a
            .revoke_session(&refreshed.session_id)
            .expect("revoke redis-backed shared session")
    );
    let edge_d = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build final redis-backed auth portal");
    assert!(
        edge_d
            .get_session(&refreshed.session_id)
            .expect("revoked redis-backed session removed after restart")
            .is_none()
    );
}

#[test]
fn auth_portal_redis_preserves_pending_login_across_edge_restart() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let login = {
        let edge = build_auth_portal(
            &config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build redis-backed auth portal");
        edge.begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed login before restart")
    };

    let restarted_edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build restarted redis-backed auth portal");
    let session = restarted_edge
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed login after edge restart");
    assert!(
        restarted_edge
            .get_session(&session.session_id)
            .expect("load session after edge restart")
            .is_some()
    );
}

#[test]
fn auth_portal_redis_times_out_when_lock_is_held() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let lock_key = match auth_session_state_store(&config, &NetworkId::new("secure-demo")) {
        AuthSessionStateStore::Redis { lock_key, .. } => lock_key,
        AuthSessionStateStore::File { .. } => panic!("expected redis session state store"),
    };
    let client = redis::Client::open(redis.url.as_str()).expect("redis client");
    let mut connection = client.get_connection().expect("redis connection");
    let acquired = redis::cmd("SET")
        .arg(&lock_key)
        .arg("held-by-test")
        .arg("NX")
        .arg("PX")
        .arg(10_000)
        .query::<Option<String>>(&mut connection)
        .expect("acquire redis test lock");
    assert!(acquired.is_some(), "expected to acquire redis test lock");

    let edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build redis-backed auth portal");
    let error = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect_err("login should time out while redis lock is held");
    assert!(
        error
            .to_string()
            .contains("timed out acquiring redis auth session lock"),
        "unexpected error: {error}",
    );
}

#[test]
fn auth_portal_redis_recovers_after_transient_connection_loss() {
    let mut redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build redis-backed auth portal");
    edge.begin_login(LoginRequest {
        network_id: NetworkId::new("secure-demo"),
        principal_hint: Some("alice".into()),
        requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
    })
    .expect("begin redis-backed login before redis outage");

    redis.stop();
    let error = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect_err("login should fail while redis is unavailable");
    let message = error.to_string();
    assert!(
        message.contains("Connection refused")
            || message.contains("connection refused")
            || message.contains("Broken pipe")
            || message.contains("connection reset")
            || message.contains("No such file or directory"),
        "unexpected redis outage error: {message}",
    );

    redis.restart();
    let login = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed login after redis restart");
    let session = edge
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed login after redis restart");
    assert!(
        edge.get_session(&session.session_id)
            .expect("load redis-backed session after redis restart")
            .is_some()
    );
}
