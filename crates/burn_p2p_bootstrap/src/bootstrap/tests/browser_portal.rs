use super::shared::*;

#[test]
fn browser_portal_client_round_trips_against_live_http_router() {
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
        config_path: Arc::new(temp.path().join("browser-client-e2e.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "native-test-client".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let signed_seeds = client
            .fetch_browser_seed_advertisement()
            .await
            .expect("fetch browser seed advertisement")
            .expect("browser seed advertisement");
        assert_eq!(
            signed_seeds.payload.schema,
            "burn_p2p.browser_seed_advertisement"
        );
        assert_eq!(
            signed_seeds.payload.payload.network_id.as_str(),
            "secure-demo"
        );
        assert_eq!(
            signed_seeds
                .payload
                .payload
                .transport_policy
                .preferred
                .first()
                .expect("preferred browser transport"),
            &burn_p2p_core::BrowserSeedTransportKind::WssFallback
        );
        assert!(
            !signed_seeds
                .payload
                .payload
                .transport_policy
                .preferred
                .contains(&burn_p2p_core::BrowserSeedTransportKind::WebRtcDirect),
            "browser seed advertisement should not claim webrtc-direct without an explicit native WebRTC listener"
        );
        assert!(
            !signed_seeds
                .payload
                .payload
                .transport_policy
                .preferred
                .contains(&burn_p2p_core::BrowserSeedTransportKind::WebTransport),
            "browser seed advertisement should not claim webtransport until native runtime support exists"
        );
        assert_eq!(signed_seeds.payload.payload.seeds.len(), 1);
        let seed_multiaddrs = &signed_seeds.payload.payload.seeds[0].multiaddrs;
        assert!(
            seed_multiaddrs.contains(&"/ip4/127.0.0.1/tcp/443/wss".to_owned()),
            "expected browser-capable public wss seed in signed advertisement, got {seed_multiaddrs:?}"
        );

        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin login");
        let session = client
            .complete_static_login(&login, PrincipalId::new("alice"))
            .await
            .expect("complete static login");
        let trust_bundle = client.fetch_trust_bundle().await.expect("trust bundle");
        assert_eq!(trust_bundle.network_id.as_str(), "secure-demo");

        let enrolled = client
            .enroll_static_principal(
                Some("alice".into()),
                PrincipalId::new("alice"),
                &BrowserWorkerIdentity {
                    peer_id: PeerId::new("browser-http-peer"),
                    peer_public_key_hex: "001122".into(),
                    serial: 7,
                    client_policy_hash: Some(ContentId::new("policy-browser-http")),
                },
            )
            .await
            .expect("enroll static principal");
        assert_eq!(
            enrolled.certificate.claims().peer_id.as_str(),
            "browser-http-peer"
        );

        let directory = client
            .fetch_directory(Some(&session.session_id))
            .await
            .expect("fetch directory");
        assert_eq!(directory.len(), 1);
        assert_eq!(directory[0].experiment_id.as_str(), "exp-auth");

        let signed_directory = client
            .fetch_signed_directory(Some(&session.session_id))
            .await
            .expect("fetch signed directory");
        assert_eq!(
            signed_directory.payload.schema,
            "burn_p2p.browser_directory_snapshot"
        );

        let signed_leaderboard = client
            .fetch_signed_leaderboard()
            .await
            .expect("fetch signed leaderboard");
        assert_eq!(
            signed_leaderboard.payload.schema,
            "burn_p2p.browser_leaderboard_snapshot"
        );

        let receipt = ContributionReceipt {
            receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser-http"),
            peer_id: PeerId::new("browser-http-peer"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            base_head_id: burn_p2p::HeadId::new("head-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-browser-http"),
            accepted_at: Utc::now(),
            accepted_weight: 2.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            merge_cert_id: None,
        };
        let submission = client
            .submit_receipts(&session.session_id, &[receipt])
            .await
            .expect("submit browser receipts");
        assert_eq!(submission.accepted_receipt_ids.len(), 1);
        assert_eq!(
            submission.accepted_receipt_ids[0].as_str(),
            "receipt-browser-http"
        );

        let refreshed = client
            .refresh_session(&session.session_id)
            .await
            .expect("refresh session");
        assert_eq!(refreshed.claims.principal_id.as_str(), "alice");

        let logout = client
            .logout_session(&refreshed.session_id)
            .await
            .expect("logout session");
        assert!(logout.logged_out);
    });
}

#[test]
fn browser_seed_advertisement_includes_webrtc_direct_when_native_listener_is_configured() {
    let temp = tempdir().expect("temp dir");
    let now = Utc::now();
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
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(burn_p2p::NodeTelemetrySnapshot {
                status: burn_p2p::RuntimeStatus::Running,
                node_state: burn_p2p::NodeRuntimeState::IdleReady,
                slot_states: Vec::new(),
                lag_state: burn_p2p::LagState::Current,
                head_lag_steps: 0,
                lag_policy: burn_p2p::LagPolicy::default(),
                network_id: Some(NetworkId::new("secure-demo")),
                local_peer_id: Some(PeerId::new("bootstrap-authority")),
                configured_roles: PeerRoleSet::default_trainer(),
                connected_peers: 1,
                observed_peer_ids: BTreeSet::new(),
                known_peer_addresses: BTreeSet::new(),
                runtime_boundary: None,
                listen_addresses: vec![
                    burn_p2p::SwarmAddress::new("/ip4/198.51.100.10/udp/4101/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w").expect("webrtc certhash addr"),
                ],
                control_plane: burn_p2p::ControlPlaneSnapshot::default(),
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
            }),
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
        config_path: Arc::new(temp.path().join("browser-client-webrtc.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        assert!(snapshot.transports.webrtc_direct);
        assert!(snapshot.transports.wss_fallback);

        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "native-test-client".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let signed_seeds = client
            .fetch_browser_seed_advertisement()
            .await
            .expect("fetch browser seed advertisement")
            .expect("browser seed advertisement");
        let preferred = &signed_seeds.payload.payload.transport_policy.preferred;
        assert!(
            preferred.contains(&burn_p2p_core::BrowserSeedTransportKind::WebRtcDirect),
            "browser seed advertisement should include webrtc-direct when a native listener is configured"
        );
        assert!(
            preferred.contains(&burn_p2p_core::BrowserSeedTransportKind::WssFallback),
            "browser seed advertisement should continue to include wss fallback"
        );
        let seed_multiaddrs = &signed_seeds.payload.payload.seeds[0].multiaddrs;
        assert_eq!(
            seed_multiaddrs.first().map(String::as_str),
            Some(
                "/ip4/127.0.0.1/udp/4101/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
            ),
            "browser seed advertisement should publish direct browser transports before fallback seeds"
        );
        assert!(
            seed_multiaddrs.contains(&"/ip4/127.0.0.1/udp/4101/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w".to_owned()),
            "expected public WebRTC direct seed in signed advertisement, got {seed_multiaddrs:?}"
        );
        assert!(
            seed_multiaddrs.contains(&"/ip4/127.0.0.1/tcp/443/wss".to_owned()),
            "expected public wss fallback seed in signed advertisement, got {seed_multiaddrs:?}"
        );

        let forwarded_signed_seeds: burn_p2p_core::SignedPayload<
            burn_p2p_core::SchemaEnvelope<burn_p2p_core::BrowserSeedAdvertisement>,
        > =
            reqwest::Client::new()
                .get(format!("{}/browser/seeds/signed", server.base_url()))
                .header("x-forwarded-host", "edge.example")
                .send()
                .await
                .expect("fetch forwarded browser seed advertisement")
                .error_for_status()
                .expect("forwarded browser seed advertisement status")
                .json()
                .await
                .expect("decode forwarded browser seed advertisement");
        let forwarded_multiaddrs = &forwarded_signed_seeds.payload.payload.seeds[0].multiaddrs;
        assert!(
            forwarded_multiaddrs.contains(
                &"/ip4/198.51.100.10/udp/4101/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
                    .to_owned()
            ),
            "browser seed advertisement should preserve publishable ip-based webrtc-direct seeds for dns hosts, got {forwarded_multiaddrs:?}"
        );
        assert!(
            forwarded_multiaddrs.contains(&"/dns4/edge.example/tcp/443/wss".to_owned()),
            "browser seed advertisement should still rewrite wss fallback to the public dns host, got {forwarded_multiaddrs:?}"
        );
    });
}

#[test]
fn browser_portal_snapshot_includes_live_runtime_heads() {
    let temp = tempdir().expect("temp dir");
    let now = Utc::now();
    let runtime_head = HeadDescriptor {
        head_id: burn_p2p::HeadId::new("head-runtime"),
        study_id: burn_p2p::StudyId::new("study-auth"),
        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        revision_id: burn_p2p::RevisionId::new("rev-auth"),
        artifact_id: burn_p2p::ArtifactId::new("artifact-runtime"),
        parent_head_id: None,
        global_step: 3,
        created_at: now,
        metrics: BTreeMap::new(),
    };
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            runtime_snapshot: Some(burn_p2p::NodeTelemetrySnapshot {
                status: burn_p2p::RuntimeStatus::Running,
                node_state: burn_p2p::NodeRuntimeState::IdleReady,
                slot_states: Vec::new(),
                lag_state: burn_p2p::LagState::Current,
                head_lag_steps: 0,
                lag_policy: burn_p2p::LagPolicy::default(),
                network_id: Some(NetworkId::new("secure-demo")),
                local_peer_id: Some(PeerId::new("bootstrap-authority")),
                configured_roles: PeerRoleSet::default_trainer(),
                connected_peers: 1,
                observed_peer_ids: BTreeSet::new(),
                known_peer_addresses: BTreeSet::new(),
                runtime_boundary: None,
                listen_addresses: Vec::new(),
                control_plane: burn_p2p::ControlPlaneSnapshot {
                    head_announcements: vec![burn_p2p::HeadAnnouncement {
                        overlay: burn_p2p::OverlayTopic::experiment(
                            NetworkId::new("secure-demo"),
                            burn_p2p::StudyId::new("study-auth"),
                            burn_p2p::ExperimentId::new("exp-auth"),
                            burn_p2p::OverlayChannel::Heads,
                        )
                        .expect("heads overlay"),
                        provider_peer_id: Some(PeerId::new("head-mirror")),
                        head: runtime_head.clone(),
                        announced_at: now,
                    }],
                    ..burn_p2p::ControlPlaneSnapshot::default()
                },
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
            }),
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
        config_path: Arc::new(temp.path().join("browser-runtime-live-head.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        assert_eq!(snapshot.heads, vec![runtime_head]);
    });
}

#[test]
fn browser_portal_client_syncs_worker_runtime_and_flushes_receipts_against_live_http_router() {
    let temp = tempdir().expect("temp dir");
    let mut auth_config = sample_auth_config(temp.path());
    auth_config.directory_entries[0].current_head_id = Some(burn_p2p::HeadId::new("head-auth"));
    let slot_requirements = auth_config.directory_entries[0]
        .resource_requirements
        .clone();
    auth_config.directory_entries[0].apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        revision_id: burn_p2p::RevisionId::new("rev-auth"),
        workload_id: burn_p2p::WorkloadId::new("auth-demo"),
        required_release_train_hash: ContentId::new("demo-train"),
        model_schema_hash: burn_p2p::ContentId::new("model-auth"),
        checkpoint_format_hash: ContentId::new("checkpoint-auth"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-auth"),
        training_config_hash: ContentId::new("training-auth"),
        merge_topology_policy_hash: ContentId::new("topology-auth"),
        slot_requirements,
        activation_window: burn_p2p::WindowActivation {
            activation_window: burn_p2p::WindowId(1),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: burn_p2p::BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(65_536),
        max_browser_window_secs: Some(45),
        max_browser_shard_bytes: Some(32_768),
        requires_webgpu: false,
        max_browser_batch_size: Some(8),
        recommended_browser_precision: None,
        visibility_policy: burn_p2p::BrowserVisibilityPolicy::SwarmEligible,
        description: "authenticated browser demo".into(),
    });
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            head_descriptors: vec![HeadDescriptor {
                head_id: burn_p2p::HeadId::new("head-auth"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                revision_id: burn_p2p::RevisionId::new("rev-auth"),
                artifact_id: burn_p2p::ArtifactId::new("artifact-head-auth"),
                parent_head_id: None,
                global_step: 7,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            }],
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
        config_path: Arc::new(temp.path().join("browser-runtime-sync.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let enrolled = client
            .enroll_static_principal(
                Some("alice".into()),
                PrincipalId::new("alice"),
                &BrowserWorkerIdentity {
                    peer_id: PeerId::new("browser-http-peer"),
                    peer_public_key_hex: "001122".into(),
                    serial: 7,
                    client_policy_hash: Some(ContentId::new("policy-browser-http")),
                },
            )
            .await
            .expect("enroll static principal");
        let mut session_state = BrowserSessionState::default();
        session_state.apply_enrollment(&enrolled);

        let mut worker = BrowserWorkerRuntime::start(
            BrowserRuntimeConfig {
                role: BrowserRuntimeRole::BrowserObserver,
                ..BrowserRuntimeConfig::new(
                    server.base_url(),
                    NetworkId::new("secure-demo"),
                    ContentId::new("demo-train"),
                    "browser-wasm",
                    ContentId::new("demo-artifact-native"),
                )
            },
            BrowserCapabilityReport::default(),
            BrowserTransportStatus::enabled(false, true, true),
        );
        worker.remember_session(session_state.clone());

        let sync_events = client
            .sync_worker_runtime(&mut worker, Some(&session_state), true)
            .await
            .expect("sync worker runtime");
        assert_eq!(worker.state, Some(BrowserRuntimeState::Observer));
        assert_eq!(
            worker.storage.last_head_id,
            Some(burn_p2p::HeadId::new("head-auth"))
        );
        assert!(sync_events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::HeadUpdated { head_id }
                if head_id == &burn_p2p::HeadId::new("head-auth")
        )));

        let verify_events = worker.apply_command(
            BrowserWorkerCommand::Verify(BrowserValidationPlan {
                head_id: burn_p2p::HeadId::new("head-auth"),
                max_checkpoint_bytes: 65_536,
                sample_budget: 3,
                emit_receipt: true,
            }),
            None,
            None,
        );
        assert!(
            verify_events
                .iter()
                .any(|event| matches!(event, BrowserWorkerEvent::ValidationCompleted(_)))
        );

        let flush_events = client
            .flush_worker_receipts(&mut worker)
            .await
            .expect("flush worker receipts");
        assert!(flush_events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::ReceiptsAcknowledged {
                pending_receipts,
                ..
            } if *pending_receipts == 0
        )));

        let signed_leaderboard = client
            .fetch_signed_leaderboard()
            .await
            .expect("fetch signed leaderboard after receipt");
        assert_eq!(
            signed_leaderboard.payload.schema,
            "burn_p2p.browser_leaderboard_snapshot"
        );
    });
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_completes_github_login_via_exchange_callback() {
    let (exchange_url, exchange_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-123\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice Browser",
            "org_memberships": ["oss"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            }
        })
        .to_string(),
    );
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: Some(exchange_url),
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: Some(userinfo_url),
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
        config_path: Arc::new(temp.path().join("github-browser-client-exchange.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        assert_eq!(snapshot.login_providers[0].login_path, "/login/github");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github login");
        let session = client
            .complete_provider_login(&login, "github-code-123")
            .await
            .expect("complete github login");
        assert_eq!(session.claims.principal_id.as_str(), "alice");
        assert_eq!(session.claims.provider, AuthProvider::GitHub);
        assert_eq!(session.claims.display_name, "Alice Browser");
        assert!(session.claims.org_memberships.contains("oss"));
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );
    });
    exchange_server
        .join()
        .expect("join provider exchange server");
    userinfo_server
        .join()
        .expect("join provider userinfo server");
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_completes_github_login_via_upstream_token_exchange() {
    let (token_url, token_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("grant_type=authorization_code"));
            assert!(request.contains("code=github-upstream-code"));
            assert!(request.contains("client_id=github-client"));
            assert!(request.contains("client_secret=github-secret"));
        },
        "200 OK",
        serde_json::json!({
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "expires_in": 3600
        })
        .to_string(),
    );
    let (github_api_base_url, github_api_server) = spawn_provider_mock_server(|request| {
        let request = request.to_ascii_lowercase();
        assert!(request.contains("authorization: bearer access-token-1"));
        if request.starts_with("get /user/orgs?per_page=100 ") {
            return (
                "200 OK",
                serde_json::json!([{ "login": "oss" }]).to_string(),
            );
        }
        if request.starts_with("get /user/teams?per_page=100 ") {
            return (
                "200 OK",
                serde_json::json!([{
                    "slug": "maintainers",
                    "organization": { "login": "oss" }
                }])
                .to_string(),
            );
        }
        if request.starts_with(
            "get /user/repos?per_page=100&affiliation=owner,collaborator,organization_member ",
        ) {
            return (
                "200 OK",
                serde_json::json!([{
                    "full_name": "burn-p2p/community-web",
                    "permissions": {
                        "pull": true,
                        "push": true,
                        "admin": false
                    }
                }])
                .to_string(),
            );
        }
        assert!(request.starts_with("get /user "));
        (
            "200 OK",
            serde_json::json!({
                "id": 42,
                "login": "alice-gh",
                "email": "alice@example.com",
                "name": "Alice Upstream Browser",
                "avatar_url": "https://avatars.example/alice-upstream.png"
            })
            .to_string(),
        )
    });
    let temp = tempdir().expect("temp dir");
    let mut auth_config = sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::GitHub {
            authorize_base_url: Some("https://github.com/login/oauth/authorize".into()),
            exchange_url: None,
            token_url: Some(token_url),
            client_id: Some("github-client".into()),
            client_secret: Some("github-secret".into()),
            redirect_uri: None,
            userinfo_url: None,
            refresh_url: None,
            revoke_url: None,
            jwks_url: None,
            api_base_url: Some(github_api_base_url),
        },
    );
    auth_config.principals[0]
        .custom_claims
        .insert("provider_login".into(), "alice-gh".into());
    auth_config.principals[0]
        .custom_claims
        .insert("provider_email".into(), "alice@example.com".into());

    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github upstream auth portal"),
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
        config_path: Arc::new(temp.path().join("github-browser-client-upstream.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github upstream login");
        let authorize_url = url::Url::parse(
            login
                .authorize_url
                .as_deref()
                .expect("github authorize url should be present"),
        )
        .expect("parse github authorize url");
        let authorize_pairs = authorize_url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            authorize_pairs.get("scope"),
            Some(&"read:org user:email".to_owned())
        );
        let session = client
            .complete_provider_login(&login, "github-upstream-code")
            .await
            .expect("complete github upstream login");
        assert_eq!(session.claims.principal_id.as_str(), "alice");
        assert_eq!(session.claims.provider, AuthProvider::GitHub);
        assert_eq!(session.claims.display_name, "Alice Upstream Browser");
        assert!(session.claims.org_memberships.contains("oss"));
        assert!(session.claims.group_memberships.contains("oss/maintainers"));
        assert_eq!(
            session.claims.custom_claims.get("provider_login"),
            Some(&"alice-gh".to_owned())
        );
        assert_eq!(
            session.claims.custom_claims.get("provider_email"),
            Some(&"alice@example.com".to_owned())
        );
    });

    token_server.join().expect("join provider token server");
    github_api_server.join().expect("join github api server");
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_refreshes_and_logs_out_provider_session_via_live_http_router() {
    let (exchange_url, exchange_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-refresh\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "display_name": "Alice GitHub",
            "org_memberships": ["oss"],
            "group_memberships": ["maintainers"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            },
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (refresh_url, refresh_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice Refreshed",
            "group_memberships": ["operators"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice-2.png"
            },
            "access_token": "access-token-2",
            "refresh_token": "refresh-token-2",
            "session_handle": "session-handle-2"
        })
        .to_string(),
    );
    let (revoke_url, revoke_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-2\""));
            assert!(request.contains("\"session_handle\":\"session-handle-2\""));
        },
        "200 OK",
        "{}".into(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_mock_server(|request| {
        assert!(request.contains("\"network_id\":\"secure-demo\""));
        assert!(request.contains("\"principal_id\":\"alice\""));
        assert!(request.contains("\"provider\":\"GitHub\""));
        assert!(
            request.contains("\"access_token\":\"access-token-1\"")
                || request.contains("\"access_token\":\"access-token-2\"")
        );
        ("200 OK", "{}".into())
    });
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: Some(exchange_url),
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: Some(userinfo_url),
                    refresh_url: Some(refresh_url),
                    revoke_url: Some(revoke_url),
                    jwks_url: None,
                    api_base_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github auth portal"),
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
        config_path: Arc::new(temp.path().join("github-browser-client-refresh.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github login");
        let session = client
            .complete_provider_login(&login, "github-code-refresh")
            .await
            .expect("complete github login");
        assert_eq!(session.claims.display_name, "Alice GitHub");
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );

        let refreshed = client
            .refresh_session(&session.session_id)
            .await
            .expect("refresh provider session");
        assert_eq!(refreshed.claims.display_name, "Alice Refreshed");
        assert!(refreshed.claims.group_memberships.contains("operators"));
        assert_eq!(
            refreshed.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice-2.png".to_owned())
        );

        let logout = client
            .logout_session(&refreshed.session_id)
            .await
            .expect("logout provider session");
        assert!(logout.logged_out);
    });

    exchange_server.join().expect("join exchange server");
    refresh_server.join().expect("join refresh server");
    revoke_server.join().expect("join revoke server");
    userinfo_server.join().expect("join userinfo server");
}

#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "rbac",
    feature = "auth-static"
))]
#[test]
fn capabilities_endpoint_reports_compiled_and_active_services() {
    let temp = tempdir().expect("temp dir");
    let auth_config = sample_auth_config(temp.path());
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
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
            auth: Some(auth_config),
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("capabilities.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };

    let manifest = response_json(&issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/capabilities",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        manifest["payload"]["schema"],
        "burn_p2p.edge_service_manifest"
    );
    let payload = &manifest["payload"]["payload"];
    assert_eq!(payload["app_mode"], "Interactive");
    assert_eq!(payload["browser_mode"], "Trainer");
    assert_eq!(payload["social_mode"], "Public");
    assert_eq!(payload["admin_mode"], "Rbac");
    assert_eq!(payload["metrics_mode"], "OpenMetrics");
    assert_eq!(
        payload["available_auth_providers"],
        serde_json::json!(["Static"])
    );
    let active = payload["active_feature_set"]["features"]
        .as_array()
        .expect("active features array");
    assert!(active.contains(&serde_json::json!("App")));
    assert!(active.contains(&serde_json::json!("BrowserEdge")));
    assert!(active.contains(&serde_json::json!("Social")));
    assert!(active.contains(&serde_json::json!("AuthStatic")));
}

#[test]
fn disabled_optional_services_hide_routes_and_capabilities() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig {
                browser_edge_enabled: false,
                browser_mode: BrowserMode::Disabled,
                social_mode: SocialMode::Disabled,
                profile_mode: ProfileMode::Disabled,
            },
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("capabilities-disabled.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let manifest = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/.well-known/burn-p2p-capabilities",
            body: None,
            headers: &[],
        },
    ));
    let payload = &manifest["payload"]["payload"];
    assert_eq!(payload["app_mode"], "Disabled");
    assert_eq!(payload["browser_mode"], "Disabled");
    assert_eq!(payload["social_mode"], "Disabled");
    let active = payload["active_feature_set"]["features"]
        .as_array()
        .expect("active features array");
    assert!(!active.contains(&serde_json::json!("App")));
    assert!(!active.contains(&serde_json::json!("BrowserEdge")));
    assert!(!active.contains(&serde_json::json!("Social")));

    let portal = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal.starts_with("HTTP/1.1 404 Not Found"));

    let leaderboard = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard",
            body: None,
            headers: &[],
        },
    );
    assert!(leaderboard.starts_with("HTTP/1.1 404 Not Found"));

    let browser_receipts = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/receipts/browser",
            body: Some(serde_json::json!([])),
            headers: &[],
        },
    );
    assert!(browser_receipts.starts_with("HTTP/1.1 404 Not Found"));
}

#[cfg(all(feature = "browser-edge", feature = "browser-join", feature = "social"))]
#[test]
fn portal_hides_disabled_browser_auth_and_social_flows() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig {
                browser_edge_enabled: true,
                browser_mode: BrowserMode::Disabled,
                social_mode: SocialMode::Disabled,
                profile_mode: ProfileMode::Disabled,
            },
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            operator_state_backend: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("portal-hidden-flows.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(portal_snapshot["browser_mode"], "Disabled");
    assert_eq!(portal_snapshot["social_mode"], "Disabled");
    assert_eq!(portal_snapshot["profile_mode"], "Disabled");
    assert_eq!(portal_snapshot["auth_enabled"], false);
    assert_eq!(portal_snapshot["transports"]["wss_fallback"], false);

    let portal = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal.starts_with("HTTP/1.1 404 Not Found"));
}
