use super::shared::*;

#[cfg(feature = "artifact-s3")]
#[test]
fn artifact_download_redirects_to_signed_s3_url_when_target_supports_redirect() {
    let storage = tempdir().expect("tempdir");
    let artifact_store = burn_p2p_checkpoint::FsArtifactStore::new(storage.path().join("cas"));
    artifact_store.ensure_layout().expect("layout");
    let artifact_payload = b"checkpoint-bytes";
    let descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p_checkpoint::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p::Precision::Fp32,
            ContentId::new("schema-s3"),
            "checkpoint-format",
        ),
        artifact_payload,
        burn_p2p_checkpoint::ChunkingScheme::new(4).expect("chunking"),
    )
    .expect("descriptor");
    let descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-s3"),
        head_id: Some(burn_p2p::HeadId::new("head-s3")),
        ..descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&descriptor, artifact_payload)
        .expect("store artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-s3"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            32,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let s3_server = spawn_artifact_s3_server();
    let s3_endpoint = s3_server.endpoint().to_owned();
    let uploaded = s3_server.objects();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-s3"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            head_id: burn_p2p::HeadId::new("head-s3"),
            artifact_id: descriptor.artifact_id.clone(),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            workload_id: WorkloadId::new("workload-s3"),
            head_id: burn_p2p::HeadId::new("head-s3"),
            base_head_id: None,
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-s3"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.1))]),
            sample_count: 32,
            dataset_view_id: DatasetViewId::new("view-s3"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            captured_at: Utc::now(),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.path().join("cas")),
        publication_store_root: Some(storage.path().join("publication")),
        publication_targets: vec![burn_p2p_core::PublicationTarget {
            publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-default"),
            label: "s3".into(),
            kind: burn_p2p_core::PublicationTargetKind::S3Compatible,
            publication_mode: burn_p2p_core::PublicationMode::LazyOnDemand,
            access_mode: burn_p2p_core::PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: true,
            edge_proxy_required: false,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(300),
            allowed_artifact_profiles: BTreeSet::from([
                burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                burn_p2p_core::ArtifactProfile::ManifestOnly,
            ]),
            eager_alias_names: BTreeSet::new(),
            local_root: None,
            bucket: Some("artifacts".into()),
            endpoint: Some(s3_endpoint.clone()),
            region: Some("us-east-1".into()),
            access_key_id: Some("test-access-key".into()),
            secret_access_key: Some("test-secret-key".into()),
            session_token: None,
            path_prefix: Some("exports".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: Some("AES256".into()),
            signed_url_ttl_secs: Some(120),
        }],
        ..BootstrapAdminState::default()
    }));
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .refresh_publication_views()
        .expect("sync publication views");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::clone(&state),
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
            artifact_publication: Some(BootstrapArtifactPublicationConfig {
                targets: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .publication_targets
                    .clone(),
            }),
        })),
        config_path: Arc::new(storage.path().join("artifact-s3.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
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
                    protocol_major: 0,
                    app_semver: semver::Version::new(0, 1, 0),
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
            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            "s3-default",
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);
            assert_eq!(
                uploaded
                    .lock()
                    .expect("uploaded map should not be poisoned")
                    .values()
                    .next()
                    .cloned()
                    .expect("uploaded object"),
                artifact_payload
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            "s3-default",
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request download ticket");

            let response = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("build reqwest client")
                .get(client.artifact_download_url(&ticket.ticket.download_ticket_id))
                .send()
                .await
                .expect("artifact download request");
            assert_eq!(response.status(), reqwest::StatusCode::FOUND);
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .expect("redirect location")
                .to_str()
                .expect("location header text")
                .to_owned();
            assert!(location.starts_with(&s3_endpoint));
            assert!(location.contains("X-Amz-Signature="));
        });

    s3_server.join();
}

#[cfg(feature = "artifact-s3")]
#[test]
fn artifact_download_streams_large_s3_proxy_payload_when_target_requires_portal_proxy() {
    let storage = tempdir().expect("tempdir");
    let artifact_store = burn_p2p_checkpoint::FsArtifactStore::new(storage.path().join("cas"));
    artifact_store.ensure_layout().expect("layout");
    let artifact_payload = vec![b'z'; 2 * 1024 * 1024 + 17];
    let descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p_checkpoint::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p::Precision::Fp32,
            ContentId::new("schema-s3-proxy"),
            "checkpoint-format",
        ),
        &artifact_payload,
        burn_p2p_checkpoint::ChunkingScheme::new(64 * 1024).expect("chunking"),
    )
    .expect("descriptor");
    let descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-s3-proxy"),
        head_id: Some(burn_p2p::HeadId::new("head-s3-proxy")),
        ..descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&descriptor, &artifact_payload)
        .expect("store artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-s3-proxy"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            32,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let s3_server = spawn_artifact_s3_server();
    let s3_endpoint = s3_server.endpoint().to_owned();
    let uploaded = s3_server.objects();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-s3-proxy"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            head_id: burn_p2p::HeadId::new("head-s3-proxy"),
            artifact_id: descriptor.artifact_id.clone(),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            workload_id: WorkloadId::new("workload-s3-proxy"),
            head_id: burn_p2p::HeadId::new("head-s3-proxy"),
            base_head_id: None,
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-s3-proxy"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.1))]),
            sample_count: 32,
            dataset_view_id: DatasetViewId::new("view-s3-proxy"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            captured_at: Utc::now(),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.path().join("cas")),
        publication_store_root: Some(storage.path().join("publication")),
        publication_targets: vec![burn_p2p_core::PublicationTarget {
            publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
            label: "s3-proxy".into(),
            kind: burn_p2p_core::PublicationTargetKind::S3Compatible,
            publication_mode: burn_p2p_core::PublicationMode::LazyOnDemand,
            access_mode: burn_p2p_core::PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: true,
            edge_proxy_required: true,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(300),
            allowed_artifact_profiles: BTreeSet::from([
                burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                burn_p2p_core::ArtifactProfile::ManifestOnly,
            ]),
            eager_alias_names: BTreeSet::new(),
            local_root: None,
            bucket: Some("artifacts".into()),
            endpoint: Some(s3_endpoint.clone()),
            region: Some("us-east-1".into()),
            access_key_id: Some("test-access-key".into()),
            secret_access_key: Some("test-secret-key".into()),
            session_token: None,
            path_prefix: Some("exports".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: Some("AES256".into()),
            signed_url_ttl_secs: Some(120),
        }],
        ..BootstrapAdminState::default()
    }));
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .refresh_publication_views()
        .expect("sync publication views");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::clone(&state),
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
            artifact_publication: Some(BootstrapArtifactPublicationConfig {
                targets: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .publication_targets
                    .clone(),
            }),
        })),
        config_path: Arc::new(storage.path().join("artifact-s3-proxy.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
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
                    protocol_major: 0,
                    app_semver: semver::Version::new(0, 1, 0),
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
            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3-proxy"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);
            assert_eq!(
                uploaded
                    .lock()
                    .expect("uploaded map should not be poisoned")
                    .values()
                    .next()
                    .map(Vec::len)
                    .expect("uploaded object size"),
                artifact_payload.len()
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3-proxy"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request download ticket");

            let body = client
                .download_artifact_bytes(&ticket.ticket.download_ticket_id)
                .await
                .expect("artifact proxy bytes");
            assert_eq!(body.len(), artifact_payload.len());
            assert_eq!(body.as_slice(), artifact_payload.as_slice());
        });

    s3_server.join();
}
