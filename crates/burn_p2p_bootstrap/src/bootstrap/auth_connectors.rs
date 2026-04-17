use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct EdgeConnectorEndpoints {
    pub(super) authorize_base_url: Option<String>,
    pub(super) exchange_url: Option<String>,
    pub(super) token_url: Option<String>,
    pub(super) api_base_url: Option<String>,
    pub(super) client_id: Option<String>,
    pub(super) client_secret: Option<String>,
    pub(super) redirect_uri: Option<String>,
    pub(super) userinfo_url: Option<String>,
    pub(super) refresh_url: Option<String>,
    pub(super) revoke_url: Option<String>,
    pub(super) jwks_url: Option<String>,
    pub(super) persist_remote_tokens: bool,
    pub(super) trusted_callback: Option<BootstrapTrustedCallbackConfig>,
}

#[cfg(feature = "auth-github")]
pub(super) fn build_github_portal_connector(
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let EdgeConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    } = endpoints;
    let userinfo_url = userinfo_url.or_else(|| {
        api_base_url
            .as_ref()
            .map(|base| format!("{}/user", base.trim_end_matches('/')))
    });
    let connector = EdgeIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "GitHub".into(),
            login_path: "/login/github".into(),
            callback_path: Some("/callback/github".into()),
            device_path: None,
        }],
        Box::new(
            GitHubIdentityConnector::new(session_ttl, principals, authorize_base_url)
                .with_api_base_url(api_base_url)
                .with_exchange_url(exchange_url)
                .with_token_url(token_url)
                .with_client_credentials(client_id, client_secret)
                .with_redirect_uri(redirect_uri)
                .with_userinfo_url(userinfo_url)
                .with_refresh_url(refresh_url)
                .with_revoke_url(revoke_url)
                .with_jwks_url(jwks_url)
                .with_persist_remote_tokens(persist_remote_tokens),
        ),
    );
    Ok(match trusted_callback {
        Some(trusted_callback) => connector.with_trusted_callback(trusted_callback),
        None => connector,
    })
}

#[cfg(not(feature = "auth-github"))]
pub(super) fn build_github_portal_connector(
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let EdgeConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    );
    Err(std::io::Error::other("auth-github feature not compiled").into())
}

#[cfg(feature = "auth-oidc")]
pub(super) fn build_oidc_portal_connector(
    issuer: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let connector = EdgeIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OIDC".into(),
            login_path: "/login/oidc".into(),
            callback_path: Some("/callback/oidc".into()),
            device_path: Some("/device/oidc".into()),
        }],
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
            .with_redirect_uri(endpoints.redirect_uri)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url)
            .with_jwks_url(endpoints.jwks_url)
            .with_persist_remote_tokens(endpoints.persist_remote_tokens),
        ),
    );
    Ok(match endpoints.trusted_callback {
        Some(trusted_callback) => connector.with_trusted_callback(trusted_callback),
        None => connector,
    })
}

#[cfg(not(feature = "auth-oidc"))]
pub(super) fn build_oidc_portal_connector(
    _issuer: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let EdgeConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    );
    Err(std::io::Error::other("auth-oidc feature not compiled").into())
}

#[cfg(feature = "auth-oauth")]
pub(super) fn build_oauth_portal_connector(
    provider: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let connector = EdgeIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OAuth".into(),
            login_path: "/login/oauth".into(),
            callback_path: Some("/callback/oauth".into()),
            device_path: Some("/device/oauth".into()),
        }],
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
            .with_redirect_uri(endpoints.redirect_uri)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url)
            .with_jwks_url(endpoints.jwks_url)
            .with_persist_remote_tokens(endpoints.persist_remote_tokens),
        ),
    );
    Ok(match endpoints.trusted_callback {
        Some(trusted_callback) => connector.with_trusted_callback(trusted_callback),
        None => connector,
    })
}

#[cfg(not(feature = "auth-oauth"))]
pub(super) fn build_oauth_portal_connector(
    _provider: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: EdgeConnectorEndpoints,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    let EdgeConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        api_base_url,
        client_id,
        client_secret,
        redirect_uri,
        userinfo_url,
        refresh_url,
        revoke_url,
        jwks_url,
        persist_remote_tokens,
        trusted_callback,
    );
    Err(std::io::Error::other("auth-oauth feature not compiled").into())
}

#[cfg(feature = "auth-external")]
pub(super) fn build_external_portal_connector(
    authority: String,
    trusted_principal_header: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    Ok(EdgeIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: format!("External ({authority})"),
            login_path: "/login/external".into(),
            callback_path: Some("/callback/external".into()),
            device_path: None,
        }],
        Box::new(ExternalProxyIdentityConnector::new(
            authority,
            trusted_principal_header,
            session_ttl,
            principals,
        )),
    )
    .with_trusted_callback_header(trusted_principal_header.clone()))
}

#[cfg(not(feature = "auth-external"))]
pub(super) fn build_external_portal_connector(
    _authority: String,
    _trusted_principal_header: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<EdgeIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-external feature not compiled").into())
}
