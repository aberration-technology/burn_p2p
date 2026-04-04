fn build_auth_portal(
    config: &BootstrapAuthConfig,
    network_id: NetworkId,
    protocol_version: semver::Version,
) -> Result<AuthPortalState, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let provider = match &config.connector {
        BootstrapAuthConnectorConfig::Static => AuthProvider::Static {
            authority: config.authority_name.clone(),
        },
        BootstrapAuthConnectorConfig::GitHub { .. } => AuthProvider::GitHub,
        BootstrapAuthConnectorConfig::Oidc { issuer, .. } => AuthProvider::Oidc {
            issuer: issuer.clone(),
        },
        BootstrapAuthConnectorConfig::OAuth { provider, .. } => AuthProvider::OAuth {
            provider: provider.clone(),
        },
        BootstrapAuthConnectorConfig::External { authority, .. } => AuthProvider::External {
            authority: authority.clone(),
        },
    };
    let principals = config
        .principals
        .iter()
        .map(|principal| {
            Ok((
                principal.principal_id.clone(),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: principal.principal_id.clone(),
                        provider: provider.clone(),
                        display_name: principal.display_name.clone(),
                        org_memberships: principal.org_memberships.clone(),
                        group_memberships: principal.group_memberships.clone(),
                        granted_roles: principal.granted_roles.clone(),
                        granted_scopes: principal.granted_scopes.clone(),
                        custom_claims: principal.custom_claims.clone(),
                        issued_at: now,
                        expires_at: now
                            + chrono::Duration::seconds(config.session_ttl_seconds.max(1)),
                    },
                    allowed_networks: principal.allowed_networks.clone(),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;

    let session_ttl = chrono::Duration::seconds(config.session_ttl_seconds.max(1));
    let connector = match &config.connector {
        BootstrapAuthConnectorConfig::Static => PortalIdentityConnector::new(
            vec![BrowserLoginProvider {
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: None,
            }],
            None,
            Box::new(StaticIdentityConnector::new(
                config.authority_name.clone(),
                session_ttl,
                principals.clone(),
            )),
        ),
        BootstrapAuthConnectorConfig::GitHub {
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_github_portal_connector(
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::Oidc {
            issuer,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_oidc_portal_connector(
            issuer.clone(),
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::OAuth {
            provider,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_oauth_portal_connector(
            provider.clone(),
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::External {
            authority,
            trusted_principal_header,
            ..
        } => build_external_portal_connector(
            authority.clone(),
            trusted_principal_header.clone(),
            session_ttl,
            principals,
        )?,
    };
    let login_providers = connector.login_providers();
    let authority_keypair = load_or_create_keypair(&config.authority_key_path)?;
    let authority = NodeCertificateAuthority::new(
        network_id.clone(),
        config.project_family_id.clone(),
        config.required_release_train_hash.clone(),
        protocol_version.clone(),
        authority_keypair,
        config.issuer_key_id.clone(),
    )?;
    let active_issuer = TrustedIssuer {
        issuer_peer_id: authority.issuer_peer_id(),
        issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
    };
    let mut trusted_issuers = config
        .trusted_issuers
        .iter()
        .cloned()
        .map(|issuer| (issuer.issuer_peer_id.clone(), issuer))
        .collect::<BTreeMap<_, _>>();
    trusted_issuers.insert(active_issuer.issuer_peer_id.clone(), active_issuer);

    Ok(AuthPortalState {
        connector,
        login_providers,
        authority_key_path: config.authority_key_path.clone(),
        network_id: network_id.clone(),
        protocol_version,
        issuer_key_id: Mutex::new(config.issuer_key_id.clone()),
        authority: Mutex::new(authority),
        trusted_issuers: Mutex::new(trusted_issuers),
        sessions: Mutex::new(BTreeMap::new()),
        directory: Mutex::new(ExperimentDirectory {
            network_id,
            generated_at: now,
            entries: config.directory_entries.clone(),
        }),
        minimum_revocation_epoch: Mutex::new(RevocationEpoch(config.minimum_revocation_epoch)),
        reenrollment: Mutex::new(config.reenrollment.clone()),
        project_family_id: config.project_family_id.clone(),
        required_release_train_hash: config.required_release_train_hash.clone(),
        allowed_target_artifact_hashes: config.allowed_target_artifact_hashes.clone(),
    })
}

#[derive(Clone, Debug, Default)]
struct PortalConnectorEndpoints {
    authorize_base_url: Option<String>,
    exchange_url: Option<String>,
    token_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    userinfo_url: Option<String>,
    refresh_url: Option<String>,
    revoke_url: Option<String>,
}

#[cfg(feature = "auth-github")]
fn build_github_portal_connector(
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "GitHub".into(),
            login_path: "/login/github".into(),
            callback_path: Some("/callback/github".into()),
            device_path: None,
        }],
        None,
        Box::new(
            GitHubIdentityConnector::new(session_ttl, principals, endpoints.authorize_base_url)
                .with_exchange_url(endpoints.exchange_url)
                .with_token_url(endpoints.token_url)
                .with_client_credentials(endpoints.client_id, endpoints.client_secret)
                .with_userinfo_url(endpoints.userinfo_url)
                .with_refresh_url(endpoints.refresh_url)
                .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-github"))]
fn build_github_portal_connector(
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-github feature not compiled").into())
}

#[cfg(feature = "auth-oidc")]
fn build_oidc_portal_connector(
    issuer: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OIDC".into(),
            login_path: "/login/oidc".into(),
            callback_path: Some("/callback/oidc".into()),
            device_path: Some("/device/oidc".into()),
        }],
        None,
        Box::new(
            OidcIdentityConnector::new(
                issuer,
                session_ttl,
                principals,
                endpoints.authorize_base_url,
            )
            .with_exchange_url(endpoints.exchange_url)
            .with_token_url(endpoints.token_url)
            .with_client_credentials(endpoints.client_id, endpoints.client_secret)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-oidc"))]
fn build_oidc_portal_connector(
    _issuer: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-oidc feature not compiled").into())
}

#[cfg(feature = "auth-oauth")]
fn build_oauth_portal_connector(
    provider: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OAuth".into(),
            login_path: "/login/oauth".into(),
            callback_path: Some("/callback/oauth".into()),
            device_path: Some("/device/oauth".into()),
        }],
        None,
        Box::new(
            OAuthIdentityConnector::new(
                provider,
                session_ttl,
                principals,
                endpoints.authorize_base_url,
            )
            .with_exchange_url(endpoints.exchange_url)
            .with_token_url(endpoints.token_url)
            .with_client_credentials(endpoints.client_id, endpoints.client_secret)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-oauth"))]
fn build_oauth_portal_connector(
    _provider: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-oauth feature not compiled").into())
}

#[cfg(feature = "auth-external")]
fn build_external_portal_connector(
    authority: String,
    trusted_principal_header: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: format!("External ({authority})"),
            login_path: "/login/external".into(),
            callback_path: Some("/callback/external".into()),
            device_path: None,
        }],
        Some(trusted_principal_header.clone()),
        Box::new(ExternalProxyIdentityConnector::new(
            authority,
            trusted_principal_header,
            session_ttl,
            principals,
        )),
    ))
}

#[cfg(not(feature = "auth-external"))]
fn build_external_portal_connector(
    _authority: String,
    _trusted_principal_header: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-external feature not compiled").into())
}

fn auth_directory_entries(
    auth: &AuthPortalState,
    request: &HttpRequest,
) -> Result<Vec<ExperimentDirectoryEntry>, Box<dyn std::error::Error>> {
    let scopes = request
        .headers
        .get("x-session-id")
        .and_then(|session_id| {
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&ContentId::new(session_id.clone()))
                .cloned()
        })
        .map(|session| session.claims.granted_scopes)
        .unwrap_or_default();

    let directory = auth
        .directory
        .lock()
        .expect("auth directory should not be poisoned");
    Ok(directory.visible_to(&scopes).into_iter().cloned().collect())
}

fn session_allows_receipt_submission(
    session: &PrincipalSession,
    receipt: &burn_p2p::ContributionReceipt,
) -> bool {
    session
        .claims
        .granted_scopes
        .iter()
        .any(|scope| match scope {
            ExperimentScope::Connect => true,
            ExperimentScope::Train { experiment_id }
            | ExperimentScope::Validate { experiment_id } => {
                experiment_id == &receipt.experiment_id
            }
            _ => false,
        })
}

fn load_or_create_keypair(path: &Path) -> Result<Keypair, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if path.exists() {
        let bytes = std::fs::read(path)?;
        return Ok(Keypair::from_protobuf_encoding(&bytes)?);
    }

    let keypair = Keypair::generate_ed25519();
    std::fs::write(path, keypair.to_protobuf_encoding()?)?;
    Ok(keypair)
}

fn ensure_synthetic_dataset(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    let project = SyntheticBootstrapProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 1.0,
    };
    let registration = project.dataset_registration()?;
    let plan = project.microshard_plan(&registration)?;
    let manifest = root.join("fetch-manifest.json");
    if !manifest.exists() {
        let manifest_value = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            |ordinal| match ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            },
        );
        std::fs::write(&manifest, serde_json::to_vec_pretty(&manifest_value)?)?;
        for entry in &manifest_value.entries {
            let bytes = match entry.ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            };
            std::fs::write(root.join(&entry.locator), bytes)?;
        }
    }
    Ok(())
}
