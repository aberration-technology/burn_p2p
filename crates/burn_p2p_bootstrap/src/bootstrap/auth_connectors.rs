use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct PortalConnectorEndpoints {
    pub(super) authorize_base_url: Option<String>,
    pub(super) exchange_url: Option<String>,
    pub(super) token_url: Option<String>,
    pub(super) client_id: Option<String>,
    pub(super) client_secret: Option<String>,
    pub(super) userinfo_url: Option<String>,
    pub(super) refresh_url: Option<String>,
    pub(super) revoke_url: Option<String>,
}

#[cfg(feature = "auth-github")]
pub(super) fn build_github_portal_connector(
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
pub(super) fn build_github_portal_connector(
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    let PortalConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    );
    Err(std::io::Error::other("auth-github feature not compiled").into())
}

#[cfg(feature = "auth-oidc")]
pub(super) fn build_oidc_portal_connector(
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
pub(super) fn build_oidc_portal_connector(
    _issuer: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    let PortalConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    );
    Err(std::io::Error::other("auth-oidc feature not compiled").into())
}

#[cfg(feature = "auth-oauth")]
pub(super) fn build_oauth_portal_connector(
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
pub(super) fn build_oauth_portal_connector(
    _provider: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    let PortalConnectorEndpoints {
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    } = endpoints;
    let _ = (
        authorize_base_url,
        exchange_url,
        token_url,
        client_id,
        client_secret,
        userinfo_url,
        refresh_url,
        revoke_url,
    );
    Err(std::io::Error::other("auth-oauth feature not compiled").into())
}

#[cfg(feature = "auth-external")]
pub(super) fn build_external_portal_connector(
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
pub(super) fn build_external_portal_connector(
    _authority: String,
    _trusted_principal_header: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-external feature not compiled").into())
}
