use super::*;

fn url_must_use_tls(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return true;
    };
    match parsed.scheme() {
        "https" => false,
        "http" => !matches!(
            parsed.host_str(),
            Some("localhost") | Some("127.0.0.1") | Some("::1")
        ),
        _ => true,
    }
}

fn validate_optional_tls_url(
    label: &'static str,
    url: Option<&String>,
) -> Result<(), BootstrapCompositionError> {
    let Some(url) = url else {
        return Ok(());
    };
    if url_must_use_tls(url) {
        return Err(BootstrapCompositionError::InvalidServiceConfig(label));
    }
    Ok(())
}

fn http_bind_is_wildcard(bind_addr: Option<&String>) -> bool {
    bind_addr
        .map(|addr| {
            let addr = addr.trim();
            addr.starts_with("0.0.0.0:")
                || addr.starts_with("[::]:")
                || addr == "0.0.0.0"
                || addr == "::"
                || addr == "[::]"
        })
        .unwrap_or(false)
}

pub(super) fn compiled_feature_set() -> CompiledFeatureSet {
    let mut features = BTreeSet::new();
    if cfg!(feature = "admin-http") {
        features.insert(EdgeFeature::AdminHttp);
    }
    if cfg!(feature = "metrics") {
        features.insert(EdgeFeature::Metrics);
    }
    if cfg!(feature = "browser-edge") {
        features.insert(EdgeFeature::App);
    }
    if cfg!(feature = "browser-join") {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if cfg!(feature = "rbac") {
        features.insert(EdgeFeature::Rbac);
    }
    if cfg!(feature = "auth-static") {
        features.insert(EdgeFeature::AuthStatic);
    }
    if cfg!(feature = "auth-github") {
        features.insert(EdgeFeature::AuthGitHub);
    }
    if cfg!(feature = "auth-oidc") {
        features.insert(EdgeFeature::AuthOidc);
    }
    if cfg!(feature = "auth-oauth") {
        features.insert(EdgeFeature::AuthOAuth);
    }
    if cfg!(feature = "auth-external") {
        features.insert(EdgeFeature::AuthExternal);
    }
    if cfg!(feature = "social") {
        features.insert(EdgeFeature::Social);
        features.insert(EdgeFeature::Profiles);
    }
    CompiledFeatureSet { features }
}

pub(super) fn configured_auth_providers(
    config: &BootstrapDaemonConfig,
) -> BTreeSet<EdgeAuthProvider> {
    match config.auth.as_ref().map(|auth| &auth.connector) {
        Some(BootstrapAuthConnectorConfig::Static) => BTreeSet::from([EdgeAuthProvider::Static]),
        Some(BootstrapAuthConnectorConfig::GitHub { .. }) => {
            BTreeSet::from([EdgeAuthProvider::GitHub])
        }
        Some(BootstrapAuthConnectorConfig::Oidc { .. }) => BTreeSet::from([EdgeAuthProvider::Oidc]),
        Some(BootstrapAuthConnectorConfig::OAuth { .. }) => {
            BTreeSet::from([EdgeAuthProvider::OAuth])
        }
        Some(BootstrapAuthConnectorConfig::External { .. }) => {
            BTreeSet::from([EdgeAuthProvider::External])
        }
        None => BTreeSet::new(),
    }
}

pub(super) fn configured_service_set(config: &BootstrapDaemonConfig) -> ConfiguredServiceSet {
    let mut features = BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]);
    if config.optional_services.browser_edge_enabled {
        features.insert(EdgeFeature::App);
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if config.optional_services.social_mode != SocialMode::Disabled {
        features.insert(EdgeFeature::Social);
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled {
        features.insert(EdgeFeature::Profiles);
    }
    for provider in configured_auth_providers(config) {
        match provider {
            EdgeAuthProvider::Static => {
                features.insert(EdgeFeature::AuthStatic);
            }
            EdgeAuthProvider::GitHub => {
                features.insert(EdgeFeature::AuthGitHub);
            }
            EdgeAuthProvider::Oidc => {
                features.insert(EdgeFeature::AuthOidc);
            }
            EdgeAuthProvider::OAuth => {
                features.insert(EdgeFeature::AuthOAuth);
            }
            EdgeAuthProvider::External => {
                features.insert(EdgeFeature::AuthExternal);
            }
        }
    }
    if config.auth.is_some() {
        features.insert(EdgeFeature::Rbac);
    }
    ConfiguredServiceSet { features }
}

pub(super) fn active_service_set(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> ActiveServiceSet {
    let compiled = compiled_feature_set();
    let configured = configured_service_set(config);
    let features = configured
        .features
        .into_iter()
        .filter(|feature| compiled.features.contains(feature))
        .filter(|feature| match feature {
            EdgeFeature::Rbac => auth_state.is_some(),
            _ => true,
        })
        .collect();
    ActiveServiceSet { features }
}

pub(super) fn validate_compiled_feature_support(
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    validate_compiled_feature_support_with(&compiled_feature_set(), config)
}

pub(super) fn validate_compiled_feature_support_with(
    compiled: &CompiledFeatureSet,
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    if config.bootstrap_peer.is_some() && config.embedded_runtime.is_some() {
        return Err(BootstrapCompositionError::InvalidServiceConfig(
            "bootstrap_peer and embedded_runtime are mutually exclusive; split cheap coherence nodes from workload runtimes",
        ));
    }
    if config.optional_services.browser_edge_enabled
        && !compiled.features.contains(&EdgeFeature::App)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "browser edge",
            feature: "browser-edge",
        });
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled
        && !compiled.features.contains(&EdgeFeature::BrowserEdge)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "browser join",
            feature: "browser-join",
        });
    }
    if config.optional_services.social_mode != SocialMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Social)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "social",
            feature: "social",
        });
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && config.optional_services.social_mode == SocialMode::Disabled
    {
        return Err(BootstrapCompositionError::InvalidServiceConfig(
            "profile_mode requires social_mode to be enabled",
        ));
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Profiles)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "profiles",
            feature: "social",
        });
    }
    if let Some(auth) = config.auth.as_ref() {
        let required = match &auth.connector {
            BootstrapAuthConnectorConfig::Static => EdgeFeature::AuthStatic,
            BootstrapAuthConnectorConfig::GitHub { .. } => EdgeFeature::AuthGitHub,
            BootstrapAuthConnectorConfig::Oidc { .. } => EdgeFeature::AuthOidc,
            BootstrapAuthConnectorConfig::OAuth { .. } => EdgeFeature::AuthOAuth,
            BootstrapAuthConnectorConfig::External { .. } => EdgeFeature::AuthExternal,
        };
        let feature = match required {
            EdgeFeature::AuthStatic => "auth-static",
            EdgeFeature::AuthGitHub => "auth-github",
            EdgeFeature::AuthOidc => "auth-oidc",
            EdgeFeature::AuthOAuth => "auth-oauth",
            EdgeFeature::AuthExternal => "auth-external",
            _ => unreachable!("auth feature mapping should stay exhaustive"),
        };
        if !compiled.features.contains(&required) {
            return Err(BootstrapCompositionError::MissingCompiledFeature {
                service: "auth connector",
                feature,
            });
        }
        if let BootstrapAuthConnectorConfig::External {
            trusted_principal_header,
            trusted_internal_only,
            ..
        } = &auth.connector
        {
            if !trusted_internal_only {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires trusted_internal_only = true",
                ));
            }
            if trusted_principal_header.trim().is_empty() {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires a non-empty trusted_principal_header",
                ));
            }
            if http_bind_is_wildcard(config.http_bind_addr.as_ref()) {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external should not bind browser-edge http to a wildcard address; place it behind trusted ingress and bind loopback or a private interface",
                ));
            }
        }
        match &auth.connector {
            BootstrapAuthConnectorConfig::GitHub {
                authorize_base_url,
                exchange_url,
                token_url,
                redirect_uri,
                userinfo_url,
                refresh_url,
                revoke_url,
                jwks_url,
                ..
            }
            | BootstrapAuthConnectorConfig::OAuth {
                authorize_base_url,
                exchange_url,
                token_url,
                redirect_uri,
                userinfo_url,
                refresh_url,
                revoke_url,
                jwks_url,
                ..
            }
            | BootstrapAuthConnectorConfig::Oidc {
                authorize_base_url,
                exchange_url,
                token_url,
                redirect_uri,
                userinfo_url,
                refresh_url,
                revoke_url,
                jwks_url,
                ..
            } => {
                validate_optional_tls_url(
                    "authorize_base_url should use https except for localhost dev",
                    authorize_base_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "exchange_url should use https except for localhost dev",
                    exchange_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "token_url should use https except for localhost dev",
                    token_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "redirect_uri should use https except for localhost dev",
                    redirect_uri.as_ref(),
                )?;
                validate_optional_tls_url(
                    "userinfo_url should use https except for localhost dev",
                    userinfo_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "refresh_url should use https except for localhost dev",
                    refresh_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "revoke_url should use https except for localhost dev",
                    revoke_url.as_ref(),
                )?;
                validate_optional_tls_url(
                    "jwks_url should use https except for localhost dev",
                    jwks_url.as_ref(),
                )?;
            }
            BootstrapAuthConnectorConfig::Static
            | BootstrapAuthConnectorConfig::External { .. } => {}
        }
    }
    Ok(())
}

pub(super) fn app_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> AppMode {
    if !config.optional_services.browser_edge_enabled {
        AppMode::Disabled
    } else if auth_state.is_some() {
        AppMode::Interactive
    } else {
        AppMode::Readonly
    }
}

pub(super) fn profile_mode(config: &BootstrapDaemonConfig) -> ProfileMode {
    if config.optional_services.social_mode == SocialMode::Disabled {
        ProfileMode::Disabled
    } else {
        config.optional_services.profile_mode.clone()
    }
}

pub(super) fn admin_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> AdminMode {
    if auth_state.is_some() && cfg!(feature = "rbac") {
        AdminMode::Rbac
    } else if config.allow_dev_admin_token {
        AdminMode::Token
    } else {
        AdminMode::Disabled
    }
}

pub(super) fn metrics_mode() -> MetricsMode {
    if cfg!(feature = "metrics") {
        MetricsMode::OpenMetrics
    } else {
        MetricsMode::Disabled
    }
}

pub(super) fn edge_service_manifest(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
    edge_id: &PeerId,
) -> EdgeServiceManifest {
    let compiled_feature_set = compiled_feature_set();
    let configured_service_set = configured_service_set(config);
    let active_feature_set = active_service_set(config, auth_state);
    EdgeServiceManifest {
        edge_id: edge_id.clone(),
        network_id: plan.network_id().clone(),
        app_mode: app_mode(config, auth_state),
        browser_mode: config.optional_services.browser_mode.clone(),
        available_auth_providers: configured_auth_providers(config),
        social_mode: config.optional_services.social_mode.clone(),
        profile_mode: profile_mode(config),
        admin_mode: admin_mode(config, auth_state),
        metrics_mode: metrics_mode(),
        compiled_feature_set,
        configured_service_set,
        active_feature_set,
        generated_at: Utc::now(),
    }
}

pub(super) fn browser_edge_route_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.browser_edge_enabled
}

pub(super) fn browser_join_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.browser_mode != BrowserMode::Disabled
}

pub(super) fn social_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.social_mode != SocialMode::Disabled
}
