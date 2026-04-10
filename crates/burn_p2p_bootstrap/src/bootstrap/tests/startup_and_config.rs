use super::shared::*;

#[test]
fn deployment_profile_examples_deserialize() {
    for (contents, network_id) in [
        (
            include_str!("../../../examples/trusted-minimal.json"),
            "trusted-minimal-demo",
        ),
        (
            include_str!("../../../examples/enterprise-sso.json"),
            "enterprise-sso-demo",
        ),
        (
            include_str!("../../../examples/trusted-browser.json"),
            "trusted-browser-demo",
        ),
        (
            include_str!("../../../examples/community-web.json"),
            "community-web-demo",
        ),
    ] {
        let config: BootstrapDaemonConfig =
            serde_json::from_str(contents).expect("deserialize profile example");
        assert_eq!(config.spec.genesis.network_id, NetworkId::new(network_id));
    }
}

#[test]
fn auth_session_state_store_prefers_explicit_redis_backend_config() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("file-auth-state.cbor"));
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: "redis://127.0.0.1:6379/0".into(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let store = auth_session_state_store(&config, &NetworkId::new("secure-demo"));
    match store {
        AuthSessionStateStore::Redis {
            url,
            state_key,
            lock_key,
        } => {
            assert_eq!(url, "redis://127.0.0.1:6379/0");
            assert_eq!(state_key, "burn-p2p:test-auth:secure-demo:local-auth:state");
            assert_eq!(lock_key, "burn-p2p:test-auth:secure-demo:local-auth:lock");
        }
        AuthSessionStateStore::File { .. } => {
            panic!("redis backend config should override file backend path")
        }
    }
}

#[test]
fn startup_validation_rejects_uncompiled_optional_services() {
    let temp = tempdir().expect("temp dir");
    let config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig {
            browser_edge_enabled: true,
            browser_mode: BrowserMode::Disabled,
            social_mode: SocialMode::Public,
            profile_mode: ProfileMode::Disabled,
        },
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config(temp.path())),
        artifact_publication: None,
    };
    let compiled = CompiledFeatureSet {
        features: BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]),
    };
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("browser edge and social should require compiled features");
    assert!(error.to_string().contains("browser edge"));
}

#[test]
fn startup_validation_rejects_mixed_bootstrap_peer_and_embedded_runtime() {
    let config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: Some(BootstrapPeerDaemonConfig::default()),
        embedded_runtime: Some(BootstrapEmbeddedDaemonConfig::default()),
        auth: None,
        artifact_publication: None,
    };
    let error = validate_compiled_feature_support_with(&compiled_feature_set(), &config)
        .expect_err("mixed bootstrap peer and embedded runtime should be rejected");
    assert!(error.to_string().contains("mutually exclusive"));
}

#[test]
fn startup_validation_rejects_untrusted_external_auth_config() {
    let temp = tempdir().expect("temp dir");
    let mut config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config_with_connector(
            temp.path(),
            BootstrapAuthConnectorConfig::External {
                authority: "corp-sso".into(),
                trusted_principal_header: String::new(),
                trusted_internal_only: false,
            },
        )),
        artifact_publication: None,
    };
    let compiled = CompiledFeatureSet {
        features: BTreeSet::from([
            EdgeFeature::AdminHttp,
            EdgeFeature::Metrics,
            EdgeFeature::App,
            EdgeFeature::BrowserEdge,
            EdgeFeature::Rbac,
            EdgeFeature::AuthExternal,
            EdgeFeature::Social,
            EdgeFeature::Profiles,
        ]),
    };
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("external auth should require trusted internal mode");
    assert!(error.to_string().contains("trusted_internal_only"));

    config.auth = Some(sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::External {
            authority: "corp-sso".into(),
            trusted_principal_header: String::new(),
            trusted_internal_only: true,
        },
    ));
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("external auth should require a trusted principal header");
    assert!(error.to_string().contains("trusted_principal_header"));

    config.http_bind_addr = Some("0.0.0.0:8787".into());
    config.auth = Some(sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::External {
            authority: "corp-sso".into(),
            trusted_principal_header: "x-corp-principal".into(),
            trusted_internal_only: true,
        },
    ));
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("external auth should reject wildcard browser-edge http bind");
    assert!(error.to_string().contains("wildcard"));
}

#[test]
fn startup_validation_rejects_non_tls_provider_auth_urls_except_localhost_dev() {
    let temp = tempdir().expect("temp dir");
    let compiled = CompiledFeatureSet {
        features: BTreeSet::from([
            EdgeFeature::AdminHttp,
            EdgeFeature::Metrics,
            EdgeFeature::App,
            EdgeFeature::BrowserEdge,
            EdgeFeature::Rbac,
            EdgeFeature::AuthOidc,
        ]),
    };
    let mut config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: Some("127.0.0.1:8787".into()),
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
        auth: Some(sample_auth_config_with_connector(
            temp.path(),
            BootstrapAuthConnectorConfig::Oidc {
                issuer: "https://issuer.example".into(),
                authorize_base_url: Some("http://issuer.example/authorize".into()),
                exchange_url: None,
                token_url: Some("http://issuer.example/token".into()),
                client_id: Some("client-id".into()),
                client_secret: None,
                redirect_uri: Some("http://edge.example/callback/oidc".into()),
                userinfo_url: None,
                refresh_url: None,
                revoke_url: None,
                jwks_url: Some("http://issuer.example/jwks".into()),
            },
        )),
        artifact_publication: None,
    };
    validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("public/provider urls should require tls");

    config.auth = Some(sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::Oidc {
            issuer: "https://issuer.example".into(),
            authorize_base_url: Some("http://localhost:9999/authorize".into()),
            exchange_url: None,
            token_url: Some("http://127.0.0.1:9999/token".into()),
            client_id: Some("client-id".into()),
            client_secret: None,
            redirect_uri: Some("http://localhost:8787/callback/oidc".into()),
            userinfo_url: None,
            refresh_url: None,
            revoke_url: None,
            jwks_url: Some("http://localhost:9999/jwks".into()),
        },
    ));
    validate_compiled_feature_support_with(&compiled, &config)
        .expect("localhost dev urls should remain allowed");
}

#[test]
fn auth_portal_issues_certificates_and_filters_directory_by_session_scope() {
    let temp = tempdir().expect("temp dir");
    let auth = build_auth_portal(
        &sample_auth_config(temp.path()),
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build auth portal");

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
    let certificate = auth
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
            revocation_epoch: RevocationEpoch(1),
        })
        .expect("issue certificate");
    assert_eq!(certificate.claims().peer_id, peer_id);
    assert_eq!(certificate.claims().principal_id, PrincipalId::new("alice"));

    let request = HttpRequest {
        method: "GET".into(),
        path: "/directory".into(),
        headers: BTreeMap::from([(
            "x-session-id".into(),
            session.session_id.as_str().to_owned(),
        )]),
        body: Vec::new(),
    };
    let entries = auth_directory_entries(&auth, &request).expect("directory entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].experiment_id,
        burn_p2p::ExperimentId::new("exp-auth")
    );
}

#[test]
fn load_or_create_keypair_is_persistent() {
    let temp = tempdir().expect("temp dir");
    let key_path = temp.path().join("authority.key");

    let first = load_or_create_keypair(&key_path).expect("first keypair");
    let second = load_or_create_keypair(&key_path).expect("second keypair");

    assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
}

#[test]
fn authenticated_bootstrap_example_deserializes() {
    let config: BootstrapDaemonConfig = serde_json::from_str(include_str!(
        "../../../examples/authenticated-bootstrap.json"
    ))
    .expect("deserialize auth example");
    assert!(config.auth.is_some());
    assert_eq!(
        config.spec.genesis.network_id,
        NetworkId::new("secure-demo")
    );
}
