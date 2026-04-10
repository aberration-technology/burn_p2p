#[cfg(all(
    feature = "metrics-indexer",
    feature = "artifact-publish",
    feature = "artifact-download",
    feature = "browser-edge",
    feature = "browser-join"
))]
use super::shared::*;

#[cfg(all(
    feature = "metrics-indexer",
    feature = "artifact-publish",
    feature = "artifact-download",
    feature = "browser-edge",
    feature = "browser-join"
))]
#[test]
fn metrics_routes_export_snapshots_ledger_and_head_views() {
    let storage = burn_p2p::StorageConfig::new(tempdir().expect("artifact metrics tempdir").keep());
    let artifact_store = burn_p2p::FsArtifactStore::new(storage.root.clone());
    artifact_store
        .ensure_layout()
        .expect("ensure artifact store layout");
    let artifact_payload = b"checkpoint-bytes";
    let artifact_descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p_core::Precision::Fp32,
            ContentId::new("model-schema-metrics"),
            "checkpoint-format",
        ),
        artifact_payload,
        burn_p2p::ChunkingScheme::new(4).expect("chunking"),
    )
    .expect("artifact descriptor");
    let artifact_descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-metrics"),
        head_id: Some(burn_p2p::HeadId::new("head-candidate")),
        ..artifact_descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&artifact_descriptor, artifact_payload)
        .expect("store checkpoint artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-metrics"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            128,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-metrics"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            head_id: burn_p2p::HeadId::new("head-candidate"),
            artifact_id: artifact_descriptor.artifact_id.clone(),
            parent_head_id: Some(burn_p2p::HeadId::new("head-base")),
            global_step: 12,
            created_at: Utc::now() - Duration::seconds(1),
            metrics: BTreeMap::new(),
        }],
        peer_window_metrics: vec![
            PeerWindowMetrics {
                network_id: NetworkId::new("secure-demo"),
                experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                revision_id: burn_p2p::RevisionId::new("rev-metrics"),
                workload_id: WorkloadId::new("metrics-demo"),
                dataset_view_id: DatasetViewId::new("view-metrics"),
                peer_id: PeerId::new("peer-metrics"),
                principal_id: None,
                lease_id: LeaseId::new("lease-metrics"),
                base_head_id: burn_p2p::HeadId::new("head-base"),
                window_started_at: Utc::now() - Duration::seconds(4),
                window_finished_at: Utc::now() - Duration::seconds(2),
                attempted_tokens_or_samples: 128,
                accepted_tokens_or_samples: Some(128),
                local_train_loss_mean: Some(0.25),
                local_train_loss_last: Some(0.2),
                grad_or_delta_norm: Some(1.1),
                optimizer_step_count: 8,
                compute_time_ms: 1_500,
                data_fetch_time_ms: 250,
                publish_latency_ms: 100,
                head_lag_at_start: 0,
                head_lag_at_finish: 1,
                backend_class: BackendClass::Ndarray,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("secure-demo"),
                experiment_id: burn_p2p::ExperimentId::new("exp-secondary"),
                revision_id: burn_p2p::RevisionId::new("rev-secondary"),
                workload_id: WorkloadId::new("metrics-demo-secondary"),
                dataset_view_id: DatasetViewId::new("view-secondary"),
                peer_id: PeerId::new("peer-secondary"),
                principal_id: None,
                lease_id: LeaseId::new("lease-secondary"),
                base_head_id: burn_p2p::HeadId::new("head-secondary"),
                window_started_at: Utc::now() - Duration::seconds(8),
                window_finished_at: Utc::now() - Duration::seconds(6),
                attempted_tokens_or_samples: 64,
                accepted_tokens_or_samples: Some(64),
                local_train_loss_mean: Some(0.4),
                local_train_loss_last: Some(0.35),
                grad_or_delta_norm: Some(0.8),
                optimizer_step_count: 4,
                compute_time_ms: 900,
                data_fetch_time_ms: 150,
                publish_latency_ms: 80,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Ndarray,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("secure-demo"),
                experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                revision_id: burn_p2p::RevisionId::new("rev-metrics"),
                workload_id: WorkloadId::new("metrics-demo"),
                dataset_view_id: DatasetViewId::new("view-metrics"),
                peer_id: PeerId::new("peer-metrics-latest"),
                principal_id: None,
                lease_id: LeaseId::new("lease-metrics-latest"),
                base_head_id: burn_p2p::HeadId::new("head-candidate"),
                window_started_at: Utc::now(),
                window_finished_at: Utc::now() + Duration::seconds(1),
                attempted_tokens_or_samples: 64,
                accepted_tokens_or_samples: Some(64),
                local_train_loss_mean: Some(0.18),
                local_train_loss_last: Some(0.16),
                grad_or_delta_norm: Some(0.7),
                optimizer_step_count: 4,
                compute_time_ms: 800,
                data_fetch_time_ms: 120,
                publish_latency_ms: 70,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Ndarray,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
        ],
        reducer_cohort_metrics: vec![ReducerCohortMetrics {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("metrics-demo"),
            dataset_view_id: DatasetViewId::new("view-metrics"),
            merge_window_id: ContentId::new("merge-window-metrics"),
            reducer_group_id: ContentId::new("reducers-metrics"),
            captured_at: Utc::now() - Duration::seconds(1),
            base_head_id: burn_p2p::HeadId::new("head-base"),
            candidate_head_id: Some(burn_p2p::HeadId::new("head-candidate")),
            received_updates: 2,
            accepted_updates: 1,
            rejected_updates: 1,
            sum_weight: 2.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.5,
            staleness_max: 1.0,
            window_close_delay_ms: 50,
            cohort_duration_ms: 1_000,
            aggregate_norm: 0.9,
            reducer_load: 0.2,
            ingress_bytes: 2_048,
            egress_bytes: 1_024,
            replica_agreement: Some(0.72),
            late_arrival_count: Some(1),
            missing_peer_count: Some(1),
            rejection_reasons: BTreeMap::from([("stale".into(), 1)]),
            status: ReducerCohortStatus::Inconsistent,
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("metrics-demo"),
            head_id: burn_p2p::HeadId::new("head-candidate"),
            base_head_id: Some(burn_p2p::HeadId::new("head-base")),
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-metrics"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("view-metrics"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            captured_at: Utc::now() - Duration::seconds(1),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.root.clone()),
        publication_store_root: Some(storage.publication_dir()),
        ..BootstrapAdminState::default()
    }));
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state,
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
            artifact_publication: None,
        })),
        config_path: Arc::new(std::env::temp_dir().join("burn-p2p-bootstrap-metrics.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let snapshots = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(snapshots.as_array().expect("snapshots array").len(), 2);
    assert_eq!(snapshots[0]["manifest"]["experiment_id"], "exp-metrics");

    let ledger = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/ledger",
            body: None,
            headers: &[],
        },
    ));
    assert!(!ledger.as_array().expect("ledger array").is_empty());

    let catchup = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/catchup",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(catchup.as_array().expect("catchup array").len(), 2);
    assert_eq!(
        catchup[0]["snapshot"]["manifest"]["experiment_id"],
        "exp-metrics"
    );

    let live_event = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/live/latest",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(live_event["kind"], "CatchupRefresh");
    assert_eq!(live_event["cursors"].as_array().expect("cursors").len(), 2);

    let head_reports = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/heads/head-candidate",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        head_reports.as_array().expect("head reports array").len(),
        1
    );
    assert_eq!(
        head_reports[0]["eval_protocol_id"],
        eval_protocol.eval_protocol_id.as_str()
    );

    let experiment_snapshots = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/experiments/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_snapshots
            .as_array()
            .expect("experiment snapshots array")
            .len(),
        1
    );
    assert_eq!(
        experiment_snapshots[0]["manifest"]["revision_id"],
        "rev-metrics"
    );

    let experiment_catchup = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/catchup/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_catchup
            .as_array()
            .expect("experiment catchup array")
            .len(),
        1
    );

    let candidates = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/candidates",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(candidates.as_array().expect("candidates array").len(), 1);
    assert_eq!(candidates[0]["candidate_head_id"], "head-candidate");

    let experiment_candidates = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/candidates/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_candidates
            .as_array()
            .expect("experiment candidates array")
            .len(),
        1
    );

    let disagreements = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/disagreements",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        disagreements.as_array().expect("disagreements array").len(),
        1
    );
    assert_eq!(disagreements[0]["status"], "Inconsistent");

    let experiment_disagreements = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/disagreements/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_disagreements
            .as_array()
            .expect("experiment disagreements array")
            .len(),
        1
    );

    let peer_windows = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        peer_windows
            .as_array()
            .expect("peer-window distributions array")
            .len(),
        3
    );
    let peer_window_heads = peer_windows
        .as_array()
        .expect("peer-window distributions array")
        .iter()
        .filter_map(|value| value["base_head_id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(peer_window_heads.contains("head-base"));
    assert!(peer_window_heads.contains("head-candidate"));

    let experiment_peer_windows = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_peer_windows
            .as_array()
            .expect("experiment peer-window distributions array")
            .len(),
        2
    );

    let peer_window_detail = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows/exp-metrics/rev-metrics/head-base",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(peer_window_detail["summary"]["base_head_id"], "head-base");
    assert_eq!(
        peer_window_detail["windows"]
            .as_array()
            .expect("peer-window detail windows array")
            .len(),
        1
    );

    let head_adoption_curves = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/head-adoption-curves",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        head_adoption_curves
            .as_array()
            .expect("head adoption curves array")
            .len(),
        1
    );
    assert_eq!(
        head_adoption_curves[0]["canonical_head_id"],
        "head-candidate"
    );

    let experiment_head_adoption_curves = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/head-adoption-curves/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_head_adoption_curves
            .as_array()
            .expect("experiment head adoption curves array")
            .len(),
        1
    );

    let head_populations = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/head-populations",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        head_populations
            .as_array()
            .expect("head populations array")
            .len(),
        1
    );
    assert_eq!(head_populations[0]["canonical_head_id"], "head-candidate");

    let experiment_head_populations = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/head-populations/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_head_populations
            .as_array()
            .expect("experiment head populations array")
            .len(),
        1
    );

    let portal_html = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal_html.starts_with("HTTP/1.1 404 Not Found"));

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(portal_snapshot["network_id"], "secure-demo");
    assert!(portal_snapshot["captured_at"].as_str().is_some());

    let artifact_live_event = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/artifacts/live/latest",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(artifact_live_event["kind"], "AliasUpdated");
    assert!(artifact_live_event["alias_name"].as_str().is_some());

    let server = HttpTestServer::spawn(context);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
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
            let client = BrowserEdgeClient::new(
                BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot),
                BrowserEnrollmentConfig {
                    network_id: NetworkId::new("secure-demo"),
                    project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                    release_train_hash: ContentId::new("demo-train"),
                    target_artifact_id: "native-test-client".into(),
                    target_artifact_hash: ContentId::new("demo-artifact-native"),
                    login_path: "/login/static".into(),
                    callback_path: "/callback/static".into(),
                    enroll_path: "/enroll".into(),
                    trust_bundle_path: "/trust".into(),
                    requested_scopes: BTreeSet::new(),
                    session_ttl_secs: 300,
                },
            );

            let snapshots = client
                .fetch_metrics_snapshots()
                .await
                .expect("fetch metrics snapshots");
            assert_eq!(snapshots.len(), 2);
            assert_eq!(snapshots[0].manifest.experiment_id.as_str(), "exp-metrics");

            let experiment_snapshots = client
                .fetch_metrics_snapshots_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment metrics snapshots");
            assert_eq!(experiment_snapshots.len(), 1);

            let catchup = client
                .fetch_metrics_catchup()
                .await
                .expect("fetch metrics catchup");
            assert_eq!(catchup.len(), 2);
            assert_eq!(
                catchup[0].snapshot.manifest.experiment_id.as_str(),
                "exp-metrics"
            );

            let experiment_catchup = client
                .fetch_metrics_catchup_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment metrics catchup");
            assert_eq!(experiment_catchup.len(), 1);

            let candidates = client
                .fetch_metrics_candidates()
                .await
                .expect("fetch metrics candidates");
            assert_eq!(candidates.len(), 1);
            assert_eq!(
                candidates[0]
                    .candidate_head_id
                    .as_ref()
                    .expect("candidate head")
                    .as_str(),
                "head-candidate"
            );

            let experiment_candidates = client
                .fetch_metrics_candidates_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment metrics candidates");
            assert_eq!(experiment_candidates.len(), 1);

            let disagreements = client
                .fetch_metrics_disagreements()
                .await
                .expect("fetch metrics disagreements");
            assert_eq!(disagreements.len(), 1);
            assert_eq!(disagreements[0].status, ReducerCohortStatus::Inconsistent);

            let experiment_disagreements = client
                .fetch_metrics_disagreements_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment metrics disagreements");
            assert_eq!(experiment_disagreements.len(), 1);

            let peer_windows = client
                .fetch_metrics_peer_windows()
                .await
                .expect("fetch peer-window distributions");
            assert_eq!(peer_windows.len(), 3);
            let peer_window_heads = peer_windows
                .iter()
                .map(|summary| summary.base_head_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            assert!(peer_window_heads.contains("head-base"));
            assert!(peer_window_heads.contains("head-candidate"));

            let experiment_peer_windows = client
                .fetch_metrics_peer_windows_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment peer-window distributions");
            assert_eq!(experiment_peer_windows.len(), 2);

            let peer_window_detail = client
                .fetch_metrics_peer_window_detail(
                    &burn_p2p::ExperimentId::new("exp-metrics"),
                    &burn_p2p::RevisionId::new("rev-metrics"),
                    &burn_p2p::HeadId::new("head-base"),
                )
                .await
                .expect("fetch peer-window detail");
            assert_eq!(
                peer_window_detail.summary.base_head_id.as_str(),
                "head-base"
            );
            assert_eq!(peer_window_detail.windows.len(), 1);

            let head_adoption_curves = client
                .fetch_metrics_head_adoption_curves()
                .await
                .expect("fetch head adoption curves");
            assert_eq!(head_adoption_curves.len(), 1);
            assert_eq!(
                head_adoption_curves[0].canonical_head_id.as_str(),
                "head-candidate"
            );

            let experiment_head_adoption_curves = client
                .fetch_metrics_head_adoption_curves_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment head adoption curves");
            assert_eq!(experiment_head_adoption_curves.len(), 1);

            let head_populations = client
                .fetch_metrics_head_populations()
                .await
                .expect("fetch head populations");
            assert_eq!(head_populations.len(), 1);
            assert_eq!(
                head_populations[0].canonical_head_id.as_str(),
                "head-candidate"
            );

            let experiment_head_populations = client
                .fetch_metrics_head_populations_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment head populations");
            assert_eq!(experiment_head_populations.len(), 1);

            let ledger = client
                .fetch_metrics_ledger()
                .await
                .expect("fetch metrics ledger");
            assert!(!ledger.is_empty());

            let live_event = client
                .fetch_latest_metrics_live_event()
                .await
                .expect("fetch latest metrics live event");
            assert_eq!(live_event.cursors.len(), 2);

            let mut worker = BrowserWorkerRuntime::start(
                BrowserRuntimeConfig {
                    role: BrowserRuntimeRole::BrowserObserver,
                    selected_experiment: Some(burn_p2p::ExperimentId::new("exp-metrics")),
                    ..BrowserRuntimeConfig::new(
                        server.base_url(),
                        NetworkId::new("secure-demo"),
                        ContentId::new("demo-train"),
                        "browser-wasm",
                        ContentId::new("demo-artifact-native"),
                    )
                },
                BrowserCapabilityReport::default(),
                BrowserTransportStatus::default(),
            );
            client
                .sync_worker_runtime(&mut worker, None, false)
                .await
                .expect("sync worker runtime with metrics");
            assert_eq!(worker.storage.metrics_catchup_bundles.len(), 1);
            assert!(worker.storage.last_metrics_live_event.is_some());

            let reports = client
                .fetch_head_eval_reports(&burn_p2p::HeadId::new("head-candidate"))
                .await
                .expect("fetch head eval reports");
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].eval_protocol_id.as_str(),
                eval_protocol.eval_protocol_id.as_str()
            );

            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            assert!(
                aliases
                    .iter()
                    .any(|row| row.alias.alias_name == "latest/serve")
            );
            assert!(aliases.iter().any(|row| {
                row.alias.alias_name
                    == format!("best_val/{}/serve", eval_protocol.eval_protocol_id.as_str())
            }));

            let experiment_aliases = client
                .fetch_artifact_aliases_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment artifact aliases");
            assert_eq!(experiment_aliases.len(), aliases.len());

            let initial_artifact_live_event = client
                .fetch_latest_artifact_live_event()
                .await
                .expect("fetch latest artifact live event");
            assert_eq!(
                initial_artifact_live_event.kind,
                ArtifactLiveEventKind::AliasUpdated
            );
            assert!(initial_artifact_live_event.alias_name.is_some());

            let run_summaries = client
                .fetch_artifact_runs_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch artifact run summaries");
            assert_eq!(run_summaries.len(), 1);
            let run_view = client
                .fetch_artifact_run_view(
                    &burn_p2p::ExperimentId::new("exp-metrics"),
                    &burn_p2p_core::RunId::derive(&("exp-metrics", "rev-metrics")).expect("run id"),
                )
                .await
                .expect("fetch artifact run view");
            assert!(!run_view.alias_history.is_empty());

            let head_view = client
                .fetch_head_artifact_view(&burn_p2p::HeadId::new("head-candidate"))
                .await
                .expect("fetch head artifact view");
            assert_eq!(head_view.head.head_id.as_str(), "head-candidate");
            assert!(!head_view.alias_history.is_empty());

            let run_history_html = reqwest::get(format!(
                "{}/portal/artifacts/runs/exp-metrics/{}",
                server.base_url(),
                burn_p2p_core::RunId::derive(&("exp-metrics", "rev-metrics"))
                    .expect("run id")
                    .as_str()
            ))
            .await
            .expect("fetch run history html")
            .text()
            .await
            .expect("run history html text");
            assert!(run_history_html.contains("Artifact run"));
            assert!(run_history_html.contains("Alias transition history"));
            assert!(run_history_html.contains("latest/serve"));

            let head_history_html = reqwest::get(format!(
                "{}/portal/artifacts/heads/head-candidate",
                server.base_url()
            ))
            .await
            .expect("fetch head artifact html")
            .text()
            .await
            .expect("head artifact html text");
            assert!(head_history_html.contains("Artifact head head-candidate"));
            assert!(head_history_html.contains("Available profiles"));
            assert!(head_history_html.contains("Run history"));

            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("latest serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-candidate"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            burn_p2p_publish::DEFAULT_PUBLICATION_TARGET_ID,
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request artifact export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);

            let export_job_view = client
                .fetch_artifact_export_job(&export_job.export_job_id)
                .await
                .expect("fetch artifact export job");
            assert_eq!(
                export_job_view.status,
                burn_p2p_core::ExportJobStatus::Ready
            );

            let ready_live_event = client
                .fetch_latest_artifact_live_event()
                .await
                .expect("fetch ready artifact live event");
            assert_eq!(ready_live_event.kind, ArtifactLiveEventKind::ExportJobReady);
            assert_eq!(
                ready_live_event
                    .export_job_id
                    .as_ref()
                    .expect("ready export job id"),
                &export_job.export_job_id
            );

            let streamed_live_event = client
                .subscribe_once_artifact_live_event()
                .await
                .expect("stream artifact live event");
            assert_eq!(
                streamed_live_event.kind,
                ArtifactLiveEventKind::ExportJobReady
            );
            assert_eq!(
                streamed_live_event
                    .published_artifact_id
                    .as_ref()
                    .expect("published artifact id"),
                ready_live_event
                    .published_artifact_id
                    .as_ref()
                    .expect("latest published artifact id")
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-candidate"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            burn_p2p_publish::DEFAULT_PUBLICATION_TARGET_ID,
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request artifact download ticket");
            assert!(ticket.download_path.starts_with("/artifacts/download/"));
            assert_eq!(
                client.artifact_download_url(&ticket.ticket.download_ticket_id),
                format!(
                    "{}/artifacts/download/{}",
                    server.base_url(),
                    ticket.ticket.download_ticket_id.as_str()
                )
            );

            let download_response = reqwest::Client::new()
                .get(client.artifact_download_url(&ticket.ticket.download_ticket_id))
                .send()
                .await
                .expect("download artifact")
                .error_for_status()
                .expect("artifact download status");
            let bytes = download_response.bytes().await.expect("artifact bytes");
            assert_eq!(bytes.as_ref(), artifact_payload);
        });
}
