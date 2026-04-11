use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ContentId, NetworkId, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession, StaticPrincipalRecord,
    auth::{DEFAULT_PENDING_LOGIN_LIMIT, prune_expiring_entries, validate_principal_record_access},
    random_login_state_token,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use url::Url;

use crate::oidc::{
    OidcDiscoveryDocument, generate_pkce_pair, pkce_challenge_for_verifier,
    validate_and_decode_id_token,
};
use crate::shared::{
    PendingLogin, ProviderConnectorState, ProviderExchangeOutcome, ProviderExchangeRequest,
    ProviderExchangeResponse, ProviderRefreshRequest, ProviderRefreshResponse,
    ProviderRevokeRequest, ProviderSessionMaterial, ProviderUserInfoRequest,
    ProviderUserInfoResponse, StandardTokenResponse, StoredProviderSession,
};

const MAX_PROVIDER_SESSIONS: usize = 256;

fn parse_github_next_link(link_header: &str) -> Option<String> {
    link_header.split(',').find_map(|segment| {
        let segment = segment.trim();
        let (url, rel) = segment.split_once(';')?;
        if !rel.contains("rel=\"next\"") {
            return None;
        }
        url.strip_prefix('<')
            .and_then(|value| value.strip_suffix('>'))
            .map(ToOwned::to_owned)
    })
}

/// Authenticates principals through a provider-backed enrollment flow.
///
/// This connector supports either a custom exchange endpoint or a more standard
/// OAuth-style token, userinfo, refresh, and revoke flow. After the provider
/// round-trip completes, it maps the resulting provider identity onto one of the
/// statically configured principal records allowed for the current network.
#[derive(Debug)]
pub struct ProviderMappedIdentityConnector {
    provider: AuthProvider,
    session_ttl: Duration,
    pending: Mutex<BTreeMap<ContentId, PendingLogin>>,
    provider_sessions: Mutex<BTreeMap<ContentId, StoredProviderSession>>,
    persist_remote_tokens: bool,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    authorize_base_url: Option<String>,
    exchange_url: Option<String>,
    token_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    redirect_uri: Option<String>,
    userinfo_url: Option<String>,
    github_orgs_url: Option<String>,
    github_teams_url: Option<String>,
    github_repo_access_url: Option<String>,
    refresh_url: Option<String>,
    revoke_url: Option<String>,
    jwks_url: Option<String>,
    oidc_discovery: Mutex<Option<OidcDiscoveryDocument>>,
}

impl ProviderMappedIdentityConnector {
    /// Creates a connector that maps provider-authenticated identities onto the
    /// supplied principal records.
    pub fn new(
        provider: AuthProvider,
        session_ttl: Duration,
        principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
        authorize_base_url: Option<String>,
    ) -> Self {
        Self {
            provider,
            session_ttl,
            pending: Mutex::new(BTreeMap::new()),
            provider_sessions: Mutex::new(BTreeMap::new()),
            persist_remote_tokens: false,
            principals,
            authorize_base_url,
            exchange_url: None,
            token_url: None,
            client_id: None,
            client_secret: None,
            redirect_uri: None,
            userinfo_url: None,
            github_orgs_url: None,
            github_teams_url: None,
            github_repo_access_url: None,
            refresh_url: None,
            revoke_url: None,
            jwks_url: None,
            oidc_discovery: Mutex::new(None),
        }
    }

    /// Returns a copy configured to resolve callback codes through a custom
    /// exchange endpoint.
    pub fn with_exchange_url(mut self, exchange_url: Option<String>) -> Self {
        self.exchange_url = exchange_url;
        self
    }

    /// Returns a copy configured to exchange provider authorization codes or
    /// refresh tokens against a standard token endpoint.
    pub fn with_token_url(mut self, token_url: Option<String>) -> Self {
        self.token_url = token_url;
        self
    }

    /// Returns a copy configured with client credentials for standard OAuth-like
    /// token and revoke requests.
    pub fn with_client_credentials(
        mut self,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        self.client_id = client_id;
        self.client_secret = client_secret;
        self
    }

    /// Returns a copy configured with an explicit redirect URI for standard
    /// OAuth/OIDC authorization-code exchanges.
    pub fn with_redirect_uri(mut self, redirect_uri: Option<String>) -> Self {
        self.redirect_uri = redirect_uri;
        self
    }

    /// Returns a copy configured to hydrate provider claims from a userinfo
    /// endpoint after login or refresh.
    pub fn with_userinfo_url(mut self, userinfo_url: Option<String>) -> Self {
        self.userinfo_url = userinfo_url;
        self
    }

    /// Returns a copy configured with the GitHub organization membership
    /// endpoint used to enrich provider claims after login or refresh.
    pub fn with_github_orgs_url(mut self, github_orgs_url: Option<String>) -> Self {
        self.github_orgs_url = github_orgs_url;
        self
    }

    /// Returns a copy configured with the GitHub team membership endpoint used
    /// to enrich provider claims after login or refresh.
    pub fn with_github_teams_url(mut self, github_teams_url: Option<String>) -> Self {
        self.github_teams_url = github_teams_url;
        self
    }

    /// Returns a copy configured with the GitHub repository-access endpoint
    /// used to enrich provider claims after login or refresh.
    pub fn with_github_repo_access_url(mut self, github_repo_access_url: Option<String>) -> Self {
        self.github_repo_access_url = github_repo_access_url;
        self
    }

    /// Returns a copy configured to refresh remote provider sessions through a
    /// custom refresh endpoint.
    pub fn with_refresh_url(mut self, refresh_url: Option<String>) -> Self {
        self.refresh_url = refresh_url;
        self
    }

    /// Returns a copy configured to revoke remote provider sessions through a
    /// custom revoke endpoint.
    pub fn with_revoke_url(mut self, revoke_url: Option<String>) -> Self {
        self.revoke_url = revoke_url;
        self
    }

    /// Returns a copy configured with a JWKS endpoint used to validate OIDC
    /// `id_token` payloads.
    pub fn with_jwks_url(mut self, jwks_url: Option<String>) -> Self {
        self.jwks_url = jwks_url;
        self
    }

    /// Returns a copy configured to persist provider bearer/session material
    /// alongside shared auth state. This remains disabled by default so shared
    /// state files do not silently capture upstream IdP secrets.
    pub fn with_persist_remote_tokens(mut self, persist_remote_tokens: bool) -> Self {
        self.persist_remote_tokens = persist_remote_tokens;
        self
    }

    fn provider(&self) -> AuthProvider {
        self.provider.clone()
    }

    fn authorize_url(&self, pending: &PendingLogin) -> Result<Option<String>, AuthError> {
        let base = if let Some(base) = self.authorize_base_url.clone() {
            Some(base)
        } else {
            self.oidc_discovery_document()?
                .and_then(|document| document.authorization_endpoint)
        };
        let Some(base) = base else {
            return Ok(None);
        };
        let mut url = Url::parse(&base).map_err(|error| {
            AuthError::ProviderExchange(format!("invalid authorize url: {error}"))
        })?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("login_id", pending.login_id.as_str());
            pairs.append_pair("state", &pending.state);
            if self.uses_standard_token_flow() {
                pairs.append_pair("response_type", "code");
                if let Some(client_id) = self.client_id.as_deref() {
                    pairs.append_pair("client_id", client_id);
                }
                if let Some(redirect_uri) = self.redirect_uri.as_deref() {
                    pairs.append_pair("redirect_uri", redirect_uri);
                }
                if self.is_oidc_provider() {
                    pairs.append_pair("scope", "openid profile email");
                }
                if let Some(oidc_nonce) = pending.oidc_nonce.as_deref() {
                    pairs.append_pair("nonce", oidc_nonce);
                }
                if let Some(pkce_verifier) = pending.pkce_verifier.as_deref() {
                    let code_challenge = self.pkce_challenge(pkce_verifier);
                    pairs.append_pair("code_challenge", &code_challenge);
                    pairs.append_pair("code_challenge_method", "S256");
                }
            }
        }
        Ok(Some(url.into()))
    }

    fn uses_standard_token_flow(&self) -> bool {
        self.token_url.is_some() || self.is_oidc_provider()
    }

    fn is_oidc_provider(&self) -> bool {
        matches!(self.provider, AuthProvider::Oidc { .. })
    }

    fn oidc_issuer(&self) -> Option<&str> {
        match &self.provider {
            AuthProvider::Oidc { issuer } => Some(issuer.as_str()),
            _ => None,
        }
    }

    fn pkce_challenge(&self, verifier: &str) -> String {
        pkce_challenge_for_verifier(verifier)
    }

    fn standard_refresh_form_pairs<'a>(
        &'a self,
        refresh_token: &'a str,
    ) -> Vec<(&'a str, &'a str)> {
        let mut pairs = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ];
        if let Some(client_id) = self.client_id.as_deref() {
            pairs.push(("client_id", client_id));
        }
        if let Some(client_secret) = self.client_secret.as_deref() {
            pairs.push(("client_secret", client_secret));
        }
        pairs
    }

    fn standard_authorization_code_form_pairs<'a>(
        &'a self,
        provider_code: &'a str,
        pending: &'a PendingLogin,
    ) -> Vec<(&'a str, &'a str)> {
        let mut pairs = vec![
            ("grant_type", "authorization_code"),
            ("code", provider_code),
        ];
        if let Some(client_id) = self.client_id.as_deref() {
            pairs.push(("client_id", client_id));
        }
        if let Some(client_secret) = self.client_secret.as_deref() {
            pairs.push(("client_secret", client_secret));
        }
        if let Some(redirect_uri) = self.redirect_uri.as_deref() {
            pairs.push(("redirect_uri", redirect_uri));
        }
        if let Some(code_verifier) = pending.pkce_verifier.as_deref() {
            pairs.push(("code_verifier", code_verifier));
        }
        pairs
    }

    fn standard_session_from_token_response(
        &self,
        pending: Option<&PendingLogin>,
        response: StandardTokenResponse,
    ) -> Result<ProviderSessionMaterial, AuthError> {
        let mut session = ProviderSessionMaterial {
            access_token: response.access_token,
            refresh_token: response.refresh_token,
            provider_expires_at: response
                .expires_in
                .map(|secs| Utc::now() + Duration::seconds(secs.max(0))),
            ..Default::default()
        };

        if let Some(id_token) = response.id_token.as_deref() {
            let oidc_claims = self.validate_oidc_id_token(id_token, pending)?;
            session = session.merge_update(self.provider_session_from_userinfo_value(oidc_claims));
        }

        if session.access_token.is_none() && session.provider_subject.is_none() {
            return Err(AuthError::ProviderExchange(
                "provider token response omitted both access_token and a valid oidc id_token"
                    .into(),
            ));
        }

        Ok(session)
    }

    fn provider_session_from_userinfo_value(&self, value: Value) -> ProviderSessionMaterial {
        let mut session = ProviderSessionMaterial::default();
        let Some(object) = value.as_object() else {
            return session;
        };

        session.provider_subject = object
            .get("sub")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                object.get("id").map(|value| match value {
                    Value::String(value) => value.clone(),
                    Value::Number(value) => value.to_string(),
                    _ => String::new(),
                })
            })
            .filter(|value| !value.is_empty());

        session.profile.display_name = object
            .get("display_name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                object
                    .get("preferred_username")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                object
                    .get("login")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });

        if let Some(login) = object
            .get("login")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                object
                    .get("preferred_username")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        {
            session
                .profile
                .custom_claims
                .insert("provider_login".into(), login);
        }
        if let Some(email) = object
            .get("email")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
        {
            session
                .profile
                .custom_claims
                .insert("provider_email".into(), email);
        }
        if let Some(avatar) = object
            .get("avatar_url")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                object
                    .get("picture")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        {
            session
                .profile
                .custom_claims
                .insert("avatar_url".into(), avatar);
        }

        for key in ["organizations", "orgs", "org_memberships"] {
            if let Some(values) = object.get(key).and_then(Value::as_array) {
                session.profile.org_memberships.extend(
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned),
                );
            }
        }
        for key in ["groups", "roles", "group_memberships"] {
            if let Some(values) = object.get(key).and_then(Value::as_array) {
                session.profile.group_memberships.extend(
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned),
                );
            }
        }

        for (key, value) in object {
            if session.profile.custom_claims.contains_key(key) {
                continue;
            }
            if matches!(
                key.as_str(),
                "sub"
                    | "id"
                    | "name"
                    | "display_name"
                    | "preferred_username"
                    | "login"
                    | "email"
                    | "organizations"
                    | "orgs"
                    | "org_memberships"
                    | "groups"
                    | "roles"
                    | "group_memberships"
                    | "avatar_url"
                    | "picture"
            ) {
                continue;
            }
            if let Some(value) = value.as_str() {
                session
                    .profile
                    .custom_claims
                    .insert(key.clone(), value.to_owned());
            }
        }

        session
    }

    fn oidc_discovery_url(&self) -> Option<String> {
        self.oidc_issuer().map(|issuer| {
            format!(
                "{}/.well-known/openid-configuration",
                issuer.trim_end_matches('/')
            )
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn oidc_discovery_document(&self) -> Result<Option<OidcDiscoveryDocument>, AuthError> {
        if !self.is_oidc_provider() {
            return Ok(None);
        }
        if let Some(document) = self
            .oidc_discovery
            .lock()
            .expect("oidc discovery cache should not be poisoned")
            .clone()
        {
            return Ok(Some(document));
        }

        let Some(discovery_url) = self.oidc_discovery_url() else {
            return Ok(None);
        };
        let document = reqwest::blocking::Client::new()
            .get(discovery_url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
            .send()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .json::<OidcDiscoveryDocument>()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?;
        self.oidc_discovery
            .lock()
            .expect("oidc discovery cache should not be poisoned")
            .replace(document.clone());
        Ok(Some(document))
    }

    #[cfg(target_arch = "wasm32")]
    fn oidc_discovery_document(&self) -> Result<Option<OidcDiscoveryDocument>, AuthError> {
        Ok(None)
    }

    fn resolved_token_url(&self) -> Result<Option<String>, AuthError> {
        if let Some(token_url) = self.token_url.clone() {
            return Ok(Some(token_url));
        }
        Ok(self
            .oidc_discovery_document()?
            .and_then(|document| document.token_endpoint))
    }

    fn resolved_userinfo_url(&self) -> Result<Option<String>, AuthError> {
        if let Some(userinfo_url) = self.userinfo_url.clone() {
            return Ok(Some(userinfo_url));
        }
        if matches!(self.provider, AuthProvider::GitHub) {
            return Ok(Some("https://api.github.com/user".into()));
        }
        Ok(self
            .oidc_discovery_document()?
            .and_then(|document| document.userinfo_endpoint))
    }

    fn resolved_github_orgs_url(&self) -> Option<String> {
        if !matches!(self.provider, AuthProvider::GitHub) {
            return None;
        }
        self.github_orgs_url
            .clone()
            .or_else(|| Some("https://api.github.com/user/orgs?per_page=100".into()))
    }

    fn resolved_github_teams_url(&self) -> Option<String> {
        if !matches!(self.provider, AuthProvider::GitHub) {
            return None;
        }
        self.github_teams_url
            .clone()
            .or_else(|| Some("https://api.github.com/user/teams?per_page=100".into()))
    }

    fn resolved_github_repo_access_url(&self) -> Option<String> {
        if !matches!(self.provider, AuthProvider::GitHub) {
            return None;
        }
        self.github_repo_access_url.clone().or_else(|| {
            Some(
                "https://api.github.com/user/repos?per_page=100&affiliation=owner,collaborator,organization_member"
                    .into(),
            )
        })
    }

    fn resolved_revoke_url(&self) -> Result<Option<String>, AuthError> {
        if let Some(revoke_url) = self.revoke_url.clone() {
            return Ok(Some(revoke_url));
        }
        Ok(self
            .oidc_discovery_document()?
            .and_then(|document| document.revocation_endpoint))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn resolved_oidc_jwks(&self) -> Result<Option<jsonwebtoken::jwk::JwkSet>, AuthError> {
        if !self.is_oidc_provider() {
            return Ok(None);
        }
        let jwks_url = if let Some(jwks_url) = self.jwks_url.clone() {
            Some(jwks_url)
        } else {
            self.oidc_discovery_document()?
                .and_then(|document| document.jwks_uri)
        };
        let Some(jwks_url) = jwks_url else {
            return Ok(None);
        };
        let jwks = reqwest::blocking::Client::new()
            .get(jwks_url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
            .send()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .json::<jsonwebtoken::jwk::JwkSet>()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?;
        Ok(Some(jwks))
    }

    #[cfg(target_arch = "wasm32")]
    fn resolved_oidc_jwks(&self) -> Result<Option<jsonwebtoken::jwk::JwkSet>, AuthError> {
        Ok(None)
    }

    fn validate_oidc_id_token(
        &self,
        id_token: &str,
        pending: Option<&PendingLogin>,
    ) -> Result<Value, AuthError> {
        if !self.is_oidc_provider() {
            return Err(AuthError::ProviderExchange(
                "unexpected oidc id_token for a non-oidc connector".into(),
            ));
        }
        let issuer = self.oidc_issuer().ok_or_else(|| {
            AuthError::ProviderExchange("oidc provider is missing an issuer".into())
        })?;
        let client_id = self.client_id.as_deref().ok_or_else(|| {
            AuthError::ProviderExchange("oidc id_token validation requires client_id".into())
        })?;
        let jwks = self.resolved_oidc_jwks()?.ok_or_else(|| {
            AuthError::ProviderExchange("oidc id_token validation requires jwks support".into())
        })?;
        validate_and_decode_id_token(
            id_token,
            issuer,
            client_id,
            &jwks,
            pending.and_then(|pending| pending.oidc_nonce.as_deref()),
        )
    }

    fn split_provider_claim_list(value: Option<&String>) -> Vec<String> {
        value
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn record_has_provider_match_requirements(record: &StaticPrincipalRecord) -> bool {
        record.claims.custom_claims.keys().any(|key| {
            matches!(
                key.as_str(),
                "provider_subject"
                    | "provider_login"
                    | "provider_email"
                    | "provider_orgs"
                    | "provider_groups"
                    | "provider_repo_access"
            ) || key.starts_with("provider_claim:")
        })
    }

    fn principal_matches_provider_session(
        record: &StaticPrincipalRecord,
        session: &ProviderSessionMaterial,
    ) -> bool {
        let claims = &record.claims.custom_claims;
        let mut has_match_requirements = false;
        let mut matches = true;

        if let Some(expected) = claims.get("provider_subject") {
            has_match_requirements = true;
            matches &= session.provider_subject.as_deref() == Some(expected.as_str());
        }
        if let Some(expected) = claims.get("provider_login") {
            has_match_requirements = true;
            matches &= session.profile.custom_claims.get("provider_login") == Some(expected);
        }
        if let Some(expected) = claims.get("provider_email") {
            has_match_requirements = true;
            matches &= session.profile.custom_claims.get("provider_email") == Some(expected);
        }

        let required_orgs = Self::split_provider_claim_list(claims.get("provider_orgs"));
        if !required_orgs.is_empty() {
            has_match_requirements = true;
            matches &= required_orgs
                .iter()
                .all(|org| session.profile.org_memberships.contains(org));
        }

        let required_groups = Self::split_provider_claim_list(claims.get("provider_groups"));
        if !required_groups.is_empty() {
            has_match_requirements = true;
            matches &= required_groups
                .iter()
                .all(|group| session.profile.group_memberships.contains(group));
        }

        let required_repo_access =
            Self::split_provider_claim_list(claims.get("provider_repo_access"));
        if !required_repo_access.is_empty() {
            has_match_requirements = true;
            let available_repo_access = Self::split_provider_claim_list(
                session.profile.custom_claims.get("provider_repo_access"),
            );
            matches &= required_repo_access.iter().all(|repo| {
                available_repo_access
                    .iter()
                    .any(|candidate| candidate == repo)
            });
        }

        for (claim_key, expected_value) in claims.iter().filter_map(|(key, value)| {
            key.strip_prefix("provider_claim:")
                .map(|name| (name, value))
        }) {
            has_match_requirements = true;
            matches &= session
                .profile
                .custom_claims
                .get(claim_key)
                .is_some_and(|actual| actual == expected_value);
        }

        has_match_requirements && matches
    }

    fn resolve_principal_from_provider_session(
        &self,
        session: &ProviderSessionMaterial,
    ) -> Result<PrincipalId, AuthError> {
        let mut matches = self
            .principals
            .iter()
            .filter(|(_, record)| Self::principal_matches_provider_session(record, session))
            .map(|(principal_id, _)| principal_id.clone());
        let Some(principal_id) = matches.next() else {
            return Err(AuthError::MissingProviderPrincipal);
        };
        if matches.next().is_some() {
            return Err(AuthError::ProviderExchange(
                "provider profile matched multiple principals".into(),
            ));
        }
        Ok(principal_id)
    }

    fn issue_session(
        &self,
        network_id: NetworkId,
        principal_id: PrincipalId,
        record: &StaticPrincipalRecord,
        provider_session: Option<ProviderSessionMaterial>,
    ) -> Result<PrincipalSession, AuthError> {
        let issued_at = Utc::now();
        let expires_at = issued_at + self.session_ttl;
        let session_id = ContentId::derive(&(
            principal_id.as_str(),
            network_id.as_str(),
            issued_at.timestamp_millis(),
            self.provider(),
        ))?;
        let claims = self.compose_claims(record, provider_session.as_ref(), issued_at, expires_at);

        let session = PrincipalSession {
            session_id: session_id.clone(),
            network_id,
            claims,
            issued_at,
            expires_at,
        };

        let mut provider_sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        prune_provider_sessions(&mut provider_sessions, issued_at);
        if let Some(provider_session) = provider_session.filter(|state| !state.is_empty()) {
            provider_sessions.insert(
                session_id,
                StoredProviderSession {
                    material: provider_session,
                    local_expires_at: expires_at,
                },
            );
        } else {
            provider_sessions.remove(&session.session_id);
        }

        Ok(session)
    }

    fn compose_claims(
        &self,
        record: &StaticPrincipalRecord,
        provider_session: Option<&ProviderSessionMaterial>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> PrincipalClaims {
        let mut claims: PrincipalClaims = record.claims.clone();
        claims.provider = self.provider();
        if let Some(provider_session) = provider_session {
            if let Some(display_name) = provider_session.profile.display_name.as_ref() {
                claims.display_name = display_name.clone();
            }
            claims
                .org_memberships
                .extend(provider_session.profile.org_memberships.iter().cloned());
            claims
                .group_memberships
                .extend(provider_session.profile.group_memberships.iter().cloned());
            claims
                .custom_claims
                .extend(provider_session.profile.custom_claims.clone());
            if let Some(provider_subject) = provider_session.provider_subject.as_ref() {
                claims
                    .custom_claims
                    .insert("provider_subject".into(), provider_subject.clone());
            }
            claims
                .custom_claims
                .insert("auth_policy_source".into(), self.auth_policy_source_label());
            for (key, value) in record.claims.custom_claims.iter().filter(|(key, _)| {
                key.starts_with("provider_") || key.starts_with("provider_claim:")
            }) {
                claims
                    .custom_claims
                    .insert(format!("auth_policy_match:{key}"), value.clone());
            }
        }
        claims.issued_at = issued_at;
        claims.expires_at = expires_at;
        claims
    }

    fn auth_policy_source_label(&self) -> String {
        match &self.provider {
            AuthProvider::GitHub => "github-live".into(),
            AuthProvider::Oidc { issuer } => format!("oidc:{issuer}"),
            AuthProvider::OAuth { provider } => format!("oauth:{provider}"),
            AuthProvider::External { authority } => format!("external:{authority}"),
            AuthProvider::Static { authority } => format!("static:{authority}"),
        }
    }

    fn provider_session(&self, session_id: &ContentId) -> Option<ProviderSessionMaterial> {
        let now = Utc::now();
        let mut provider_sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        prune_provider_sessions(&mut provider_sessions, now);
        provider_sessions
            .get(session_id)
            .map(|stored| stored.material.clone())
    }

    fn carry_provider_session(
        &self,
        prior_session_id: &ContentId,
        refreshed_session_id: &ContentId,
        refreshed_expires_at: DateTime<Utc>,
        provider_session: Option<ProviderSessionMaterial>,
    ) {
        let mut sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        prune_provider_sessions(&mut sessions, Utc::now());
        sessions.remove(prior_session_id);
        if let Some(provider_session) = provider_session.filter(|state| !state.is_empty()) {
            sessions.insert(
                refreshed_session_id.clone(),
                StoredProviderSession {
                    material: provider_session,
                    local_expires_at: refreshed_expires_at,
                },
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn exchange_provider_session(
        &self,
        pending: &PendingLogin,
        provider_code: &str,
    ) -> Result<ProviderExchangeOutcome, AuthError> {
        if let Some(exchange_url) = self.exchange_url.as_ref() {
            let response = reqwest::blocking::Client::new()
                .post(exchange_url)
                .json(&ProviderExchangeRequest {
                    login_id: pending.login_id.clone(),
                    state: pending.state.clone(),
                    network_id: pending.network_id.clone(),
                    provider: self.provider(),
                    provider_code: provider_code.to_owned(),
                    requested_scopes: pending.requested_scopes.clone(),
                })
                .send()
                .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
                .error_for_status()
                .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
                .json::<ProviderExchangeResponse>()
                .map_err(|error| AuthError::ProviderExchange(error.to_string()))?;
            return Ok(ProviderExchangeOutcome::Mapped {
                principal_id: response.principal_id,
                session: response.session,
            });
        }

        let token_url = self
            .resolved_token_url()?
            .ok_or(AuthError::MissingProviderCode)?;
        let response = reqwest::blocking::Client::new()
            .post(token_url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
            .form(&self.standard_authorization_code_form_pairs(provider_code, pending))
            .send()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .json::<StandardTokenResponse>()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?;
        Ok(ProviderExchangeOutcome::SessionOnly(
            self.standard_session_from_token_response(Some(pending), response)?,
        ))
    }

    #[cfg(target_arch = "wasm32")]
    fn exchange_provider_session(
        &self,
        _pending: &PendingLogin,
        _provider_code: &str,
    ) -> Result<ProviderExchangeOutcome, AuthError> {
        Err(AuthError::ProviderExchange(
            "provider exchange is unavailable on wasm targets".to_owned(),
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn refresh_provider_session(
        &self,
        session: &PrincipalSession,
        prior: Option<ProviderSessionMaterial>,
    ) -> Result<Option<ProviderSessionMaterial>, AuthError> {
        let Some(prior) = prior else {
            return Ok(None);
        };
        if !prior.has_remote_material() {
            return Ok(Some(prior));
        }
        if self.uses_standard_token_flow() {
            let Some(refresh_token) = prior.refresh_token.as_deref() else {
                return self.hydrate_provider_profile(
                    Some(&session.network_id),
                    Some(&session.claims.principal_id),
                    Some(prior),
                );
            };
            let token_url = self.resolved_token_url()?.ok_or_else(|| {
                AuthError::ProviderRefresh("provider token endpoint is not configured".into())
            })?;
            let response = reqwest::blocking::Client::new()
                .post(token_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
                .form(&self.standard_refresh_form_pairs(refresh_token))
                .send()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
                .error_for_status()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
                .json::<StandardTokenResponse>()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?;
            return self.hydrate_provider_profile(
                Some(&session.network_id),
                Some(&session.claims.principal_id),
                Some(
                    prior.merge_update(self.standard_session_from_token_response(None, response)?),
                ),
            );
        }

        let Some(refresh_url) = self.refresh_url.as_ref() else {
            return self.hydrate_provider_profile(
                Some(&session.network_id),
                Some(&session.claims.principal_id),
                Some(prior),
            );
        };
        let response = reqwest::blocking::Client::new()
            .post(refresh_url)
            .json(&ProviderRefreshRequest {
                network_id: session.network_id.clone(),
                principal_id: session.claims.principal_id.clone(),
                provider: self.provider(),
                provider_subject: prior.provider_subject.clone(),
                access_token: prior.access_token.clone(),
                refresh_token: prior.refresh_token.clone(),
                session_handle: prior.session_handle.clone(),
            })
            .send()
            .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
            .json::<ProviderRefreshResponse>()
            .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?;
        self.hydrate_provider_profile(
            Some(&session.network_id),
            Some(&session.claims.principal_id),
            Some(prior.merge_update(response.session)),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fetch_github_paginated_array(
        &self,
        url: &str,
        access_token: &str,
    ) -> Result<Vec<Value>, AuthError> {
        let client = reqwest::blocking::Client::new();
        let mut next_url = Some(url.to_owned());
        let mut pages = 0_u8;
        let mut values = Vec::new();

        while let Some(current_url) = next_url.take() {
            pages = pages.saturating_add(1);
            if pages > 32 {
                return Err(AuthError::ProviderUserInfo(
                    "github api pagination exceeded 32 pages".into(),
                ));
            }
            let response = client
                .get(&current_url)
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {access_token}"),
                )
                .header(reqwest::header::ACCEPT, "application/vnd.github+json")
                .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .send()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?
                .error_for_status()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?;
            let next = response
                .headers()
                .get(reqwest::header::LINK)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_github_next_link);
            let payload = response
                .json::<Value>()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?;
            let items = payload.as_array().ok_or_else(|| {
                AuthError::ProviderUserInfo(
                    "github api returned a non-array response for a paginated endpoint".into(),
                )
            })?;
            values.extend(items.iter().cloned());
            next_url = next;
        }

        Ok(values)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn hydrate_github_profile(
        &self,
        prior: ProviderSessionMaterial,
    ) -> Result<ProviderSessionMaterial, AuthError> {
        if !matches!(self.provider, AuthProvider::GitHub) {
            return Ok(prior);
        }
        let Some(access_token) = prior.access_token.clone() else {
            return Ok(prior);
        };

        let mut hydrated = prior;

        if let Some(orgs_url) = self.resolved_github_orgs_url() {
            hydrated.profile.org_memberships = self
                .fetch_github_paginated_array(&orgs_url, &access_token)?
                .into_iter()
                .filter_map(|value| {
                    value
                        .get("login")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect();
        }

        if let Some(teams_url) = self.resolved_github_teams_url() {
            hydrated.profile.group_memberships = self
                .fetch_github_paginated_array(&teams_url, &access_token)?
                .into_iter()
                .filter_map(|value| {
                    let slug = value.get("slug").and_then(Value::as_str)?;
                    let org = value
                        .get("organization")
                        .and_then(Value::as_object)
                        .and_then(|org| org.get("login"))
                        .and_then(Value::as_str)?;
                    Some(format!("{org}/{slug}"))
                })
                .collect();
        }

        if let Some(repo_access_url) = self.resolved_github_repo_access_url() {
            let repo_access = self
                .fetch_github_paginated_array(&repo_access_url, &access_token)?
                .into_iter()
                .flat_map(|value| {
                    let Some(full_name) = value
                        .get("full_name")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                    else {
                        return Vec::new();
                    };

                    let mut entries = vec![full_name.clone()];
                    if let Some(permissions) = value.get("permissions").and_then(Value::as_object) {
                        let access_level = if permissions.get("admin").and_then(Value::as_bool)
                            == Some(true)
                        {
                            Some("admin")
                        } else if permissions.get("push").and_then(Value::as_bool) == Some(true) {
                            Some("write")
                        } else if permissions.get("pull").and_then(Value::as_bool) == Some(true) {
                            Some("read")
                        } else {
                            None
                        };
                        if let Some(access_level) = access_level {
                            entries.push(format!("{full_name}:{access_level}"));
                        }
                    }
                    entries
                })
                .collect::<BTreeSet<_>>();
            if repo_access.is_empty() {
                hydrated
                    .profile
                    .custom_claims
                    .remove("provider_repo_access");
            } else {
                hydrated.profile.custom_claims.insert(
                    "provider_repo_access".into(),
                    repo_access.into_iter().collect::<Vec<_>>().join(","),
                );
            }
        }

        Ok(hydrated)
    }

    #[cfg(target_arch = "wasm32")]
    fn refresh_provider_session(
        &self,
        _session: &PrincipalSession,
        _prior: Option<ProviderSessionMaterial>,
    ) -> Result<Option<ProviderSessionMaterial>, AuthError> {
        Err(AuthError::ProviderRefresh(
            "provider refresh is unavailable on wasm targets".to_owned(),
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn revoke_provider_session(
        &self,
        session: &PrincipalSession,
        prior: Option<ProviderSessionMaterial>,
    ) -> Result<(), AuthError> {
        let Some(prior) = prior else {
            return Ok(());
        };
        let Some(revoke_url) = self.resolved_revoke_url()? else {
            return Ok(());
        };
        if !prior.has_remote_material() {
            return Ok(());
        }
        let client = reqwest::blocking::Client::new();
        let response = if self.uses_standard_token_flow() {
            let token = prior
                .refresh_token
                .clone()
                .or(prior.access_token.clone())
                .ok_or_else(|| {
                    AuthError::ProviderRevoke("provider revoke requires a token".into())
                })?;
            client
                .post(revoke_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
                .form(&{
                    let mut pairs = Vec::new();
                    if let Some(client_id) = self.client_id.as_deref() {
                        pairs.push(("client_id", client_id));
                    }
                    if let Some(client_secret) = self.client_secret.as_deref() {
                        pairs.push(("client_secret", client_secret));
                    }
                    pairs.push(("token", token.as_str()));
                    pairs
                })
                .send()
        } else {
            client
                .post(revoke_url)
                .json(&ProviderRevokeRequest {
                    network_id: session.network_id.clone(),
                    principal_id: session.claims.principal_id.clone(),
                    provider: self.provider(),
                    provider_subject: prior.provider_subject,
                    access_token: prior.access_token,
                    refresh_token: prior.refresh_token,
                    session_handle: prior.session_handle,
                })
                .send()
        };
        response
            .map_err(|error| AuthError::ProviderRevoke(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderRevoke(error.to_string()))?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn hydrate_provider_profile(
        &self,
        network_id: Option<&NetworkId>,
        principal_id: Option<&PrincipalId>,
        prior: Option<ProviderSessionMaterial>,
    ) -> Result<Option<ProviderSessionMaterial>, AuthError> {
        let Some(prior) = prior else {
            return Ok(None);
        };
        if !prior.has_remote_material() {
            return Ok(Some(prior));
        }
        if self.uses_standard_token_flow() {
            let Some(access_token) = prior.access_token.as_deref() else {
                return Ok(Some(prior));
            };
            let Some(userinfo_url) = self.resolved_userinfo_url()? else {
                return Ok(Some(prior));
            };
            let response = reqwest::blocking::Client::new()
                .get(userinfo_url)
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {access_token}"),
                )
                .header(reqwest::header::ACCEPT, "application/json")
                .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
                .send()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?
                .error_for_status()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?
                .json::<Value>()
                .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?;
            return Ok(Some(self.hydrate_github_profile(prior.merge_update(
                self.provider_session_from_userinfo_value(response),
            ))?));
        }

        let network_id = network_id.ok_or_else(|| {
            AuthError::ProviderUserInfo("custom provider userinfo requires a network id".into())
        })?;
        let principal_id = principal_id.ok_or_else(|| {
            AuthError::ProviderUserInfo("custom provider userinfo requires a principal id".into())
        })?;
        let Some(userinfo_url) = self.resolved_userinfo_url()? else {
            return Ok(Some(prior));
        };
        let response = reqwest::blocking::Client::new()
            .post(userinfo_url)
            .json(&ProviderUserInfoRequest {
                network_id: network_id.clone(),
                principal_id: principal_id.clone(),
                provider: self.provider(),
                provider_subject: prior.provider_subject.clone(),
                access_token: prior.access_token.clone(),
                session_handle: prior.session_handle.clone(),
            })
            .send()
            .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?
            .json::<ProviderUserInfoResponse>()
            .map_err(|error| AuthError::ProviderUserInfo(error.to_string()))?;
        Ok(Some(prior.merge_update(response.session)))
    }

    #[cfg(target_arch = "wasm32")]
    fn hydrate_provider_profile(
        &self,
        _network_id: Option<&NetworkId>,
        _principal_id: Option<&PrincipalId>,
        _prior: Option<ProviderSessionMaterial>,
    ) -> Result<Option<ProviderSessionMaterial>, AuthError> {
        Err(AuthError::ProviderUserInfo(
            "provider userinfo is unavailable on wasm targets".to_owned(),
        ))
    }

    #[cfg(target_arch = "wasm32")]
    fn revoke_provider_session(
        &self,
        _session: &PrincipalSession,
        _prior: Option<ProviderSessionMaterial>,
    ) -> Result<(), AuthError> {
        Err(AuthError::ProviderRevoke(
            "provider revoke is unavailable on wasm targets".to_owned(),
        ))
    }
}

impl IdentityConnector for ProviderMappedIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError> {
        let expires_at = Utc::now() + self.session_ttl;
        let state = random_login_state_token("provider login state")?;
        let (pkce_verifier, oidc_nonce) =
            if self.uses_standard_token_flow() && self.is_oidc_provider() {
                let (pkce_verifier, _) = generate_pkce_pair()?;
                (
                    Some(pkce_verifier),
                    Some(random_login_state_token("oidc login nonce")?),
                )
            } else {
                (None, None)
            };
        let login_id = ContentId::derive(&(
            req.network_id.as_str(),
            &state,
            &req.principal_hint,
            &req.requested_scopes,
            self.provider(),
        ))?;
        let pending = PendingLogin {
            login_id: login_id.clone(),
            state: state.clone(),
            network_id: req.network_id,
            requested_scopes: req.requested_scopes,
            expires_at,
            oidc_nonce,
            pkce_verifier,
        };
        let mut pending_logins = self
            .pending
            .lock()
            .expect("provider identity pending-login lock should not be poisoned");
        prune_pending_logins(&mut pending_logins, Utc::now());
        pending_logins.insert(login_id.clone(), pending.clone());
        prune_pending_logins(&mut pending_logins, Utc::now());

        Ok(LoginStart {
            login_id: login_id.clone(),
            provider: self.provider(),
            state: state.clone(),
            authorize_url: self.authorize_url(&pending)?,
            expires_at,
        })
    }

    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError> {
        let pending = {
            let mut pending_logins = self
                .pending
                .lock()
                .expect("provider identity pending-login lock should not be poisoned");
            prune_pending_logins(&mut pending_logins, Utc::now());
            pending_logins
                .remove(&callback.login_id)
                .ok_or_else(|| AuthError::UnknownLogin(callback.login_id.clone()))?
        };

        if callback.state != pending.state {
            return Err(AuthError::StateMismatch);
        }
        if pending.expires_at < Utc::now() {
            return Err(AuthError::LoginExpired(pending.login_id));
        }

        let (principal_id, provider_session) = if let Some(principal_id) = callback.principal_id {
            (principal_id, None)
        } else if let Some(provider_code) = callback.provider_code.as_deref() {
            match self.exchange_provider_session(&pending, provider_code)? {
                ProviderExchangeOutcome::Mapped {
                    principal_id,
                    session,
                } => {
                    let provider_session = self.hydrate_provider_profile(
                        Some(&pending.network_id),
                        Some(&principal_id),
                        Some(session),
                    )?;
                    (principal_id, provider_session)
                }
                ProviderExchangeOutcome::SessionOnly(session) => {
                    let provider_session = self.hydrate_provider_profile(
                        Some(&pending.network_id),
                        None,
                        Some(session),
                    )?;
                    let principal_id = provider_session
                        .as_ref()
                        .map(|session| self.resolve_principal_from_provider_session(session))
                        .transpose()?
                        .ok_or(AuthError::MissingProviderPrincipal)?;
                    (principal_id, provider_session)
                }
            }
        } else {
            return Err(AuthError::MissingProviderPrincipal);
        };

        let record = self
            .principals
            .get(&principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(principal_id.clone()))?;
        validate_principal_record_access(record, &pending.network_id, &pending.requested_scopes)?;
        self.issue_session(pending.network_id, principal_id, record, provider_session)
    }

    fn refresh(&self, session: &PrincipalSession) -> Result<PrincipalSession, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        let record = self
            .principals
            .get(&session.claims.principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(session.claims.principal_id.clone()))?;
        let provider_session =
            self.refresh_provider_session(session, self.provider_session(&session.session_id))?;
        let principal_id = if let Some(provider_session) = provider_session.as_ref() {
            if provider_session.has_remote_material()
                && Self::record_has_provider_match_requirements(record)
            {
                let resolved = self
                    .resolve_principal_from_provider_session(provider_session)
                    .map_err(|_| {
                        AuthError::ProviderRefresh(
                            "provider profile no longer matches the enrolled principal".into(),
                        )
                    })?;
                if resolved != session.claims.principal_id {
                    return Err(AuthError::ProviderRefresh(
                        "provider profile no longer matches the enrolled principal".into(),
                    ));
                }
            }
            session.claims.principal_id.clone()
        } else {
            session.claims.principal_id.clone()
        };
        validate_principal_record_access(
            record,
            &session.network_id,
            &session.claims.granted_scopes,
        )?;
        let refreshed = self.issue_session(
            session.network_id.clone(),
            principal_id,
            record,
            provider_session.clone(),
        )?;
        self.carry_provider_session(
            &session.session_id,
            &refreshed.session_id,
            refreshed.expires_at,
            provider_session,
        );
        Ok(refreshed)
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        let record = self
            .principals
            .get(&session.claims.principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(session.claims.principal_id.clone()))?;
        let Some(provider_session) = self.provider_session(&session.session_id) else {
            let mut claims = session.claims.clone();
            claims.provider = self.provider();
            return Ok(claims);
        };
        let Some(provider_session) = self.hydrate_provider_profile(
            Some(&session.network_id),
            Some(&session.claims.principal_id),
            Some(provider_session),
        )?
        else {
            let mut claims = session.claims.clone();
            claims.provider = self.provider();
            return Ok(claims);
        };
        if provider_session.has_remote_material()
            && Self::record_has_provider_match_requirements(record)
        {
            let resolved = self
                .resolve_principal_from_provider_session(&provider_session)
                .map_err(|_| {
                    AuthError::ProviderUserInfo(
                        "provider profile no longer matches the enrolled principal".into(),
                    )
                })?;
            if resolved != session.claims.principal_id {
                return Err(AuthError::ProviderUserInfo(
                    "provider profile no longer matches the enrolled principal".into(),
                ));
            }
        }
        validate_principal_record_access(
            record,
            &session.network_id,
            &session.claims.granted_scopes,
        )?;
        Ok(self.compose_claims(
            record,
            Some(&provider_session),
            session.issued_at,
            session.expires_at,
        ))
    }

    fn revoke(&self, session: &PrincipalSession) -> Result<(), AuthError> {
        let provider_session = self.provider_session(&session.session_id);
        self.revoke_provider_session(session, provider_session)?;
        let mut provider_sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        prune_provider_sessions(&mut provider_sessions, Utc::now());
        provider_sessions.remove(&session.session_id);
        Ok(())
    }

    fn export_persistent_state(&self) -> Result<Option<Vec<u8>>, AuthError> {
        let now = Utc::now();
        let pending = {
            let mut pending = self
                .pending
                .lock()
                .expect("provider identity pending-login lock should not be poisoned");
            prune_pending_logins(&mut pending, now);
            pending.clone()
        };
        let provider_sessions = {
            let mut provider_sessions = self
                .provider_sessions
                .lock()
                .expect("provider identity session lock should not be poisoned");
            prune_provider_sessions(&mut provider_sessions, now);
            provider_sessions
                .iter()
                .map(|(session_id, session)| {
                    let persisted = if self.persist_remote_tokens {
                        session.material.clone()
                    } else {
                        session.material.redact_remote_secrets()
                    };
                    (
                        session_id.clone(),
                        StoredProviderSession {
                            material: persisted,
                            local_expires_at: session.local_expires_at,
                        },
                    )
                })
                .collect()
        };
        let state = ProviderConnectorState {
            pending,
            provider_sessions,
        };
        Ok(Some(burn_p2p_core::deterministic_cbor(&state)?))
    }

    fn import_persistent_state(&self, state: Option<&[u8]>) -> Result<(), AuthError> {
        let now = Utc::now();
        let mut pending = self
            .pending
            .lock()
            .expect("provider identity pending-login lock should not be poisoned");
        let restored = state
            .map(burn_p2p_core::from_cbor_slice::<ProviderConnectorState>)
            .transpose()?;
        *pending = restored
            .as_ref()
            .map(|restored| {
                restored
                    .pending
                    .clone()
                    .into_iter()
                    .filter(|(_, login)| login.expires_at >= now)
                    .collect()
            })
            .unwrap_or_default();
        prune_pending_logins(&mut pending, now);
        let mut provider_sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        *provider_sessions = restored
            .map(|restored| restored.provider_sessions)
            .unwrap_or_default();
        prune_provider_sessions(&mut provider_sessions, now);
        Ok(())
    }
}

#[cfg(test)]
impl ProviderMappedIdentityConnector {
    pub(crate) fn complete_login_with_standard_token_response_for_test(
        &self,
        login_id: ContentId,
        state: String,
        response: StandardTokenResponse,
    ) -> Result<PrincipalSession, AuthError> {
        let pending = self
            .pending
            .lock()
            .expect("provider identity pending-login lock should not be poisoned")
            .remove(&login_id)
            .ok_or_else(|| AuthError::UnknownLogin(login_id.clone()))?;

        if state != pending.state {
            return Err(AuthError::StateMismatch);
        }
        if pending.expires_at < Utc::now() {
            return Err(AuthError::LoginExpired(pending.login_id));
        }

        let provider_session = self.hydrate_provider_profile(
            Some(&pending.network_id),
            None,
            Some(self.standard_session_from_token_response(Some(&pending), response)?),
        )?;
        let principal_id = provider_session
            .as_ref()
            .map(|session| self.resolve_principal_from_provider_session(session))
            .transpose()?
            .ok_or(AuthError::MissingProviderPrincipal)?;
        let record = self
            .principals
            .get(&principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(principal_id.clone()))?;
        validate_principal_record_access(record, &pending.network_id, &pending.requested_scopes)?;
        self.issue_session(pending.network_id, principal_id, record, provider_session)
    }
}

fn prune_pending_logins(pending: &mut BTreeMap<ContentId, PendingLogin>, now: DateTime<Utc>) {
    prune_expiring_entries(pending, now, DEFAULT_PENDING_LOGIN_LIMIT, |login| {
        login.expires_at
    });
}

fn prune_provider_sessions(
    provider_sessions: &mut BTreeMap<ContentId, StoredProviderSession>,
    now: DateTime<Utc>,
) {
    provider_sessions.retain(|_, session| !session.is_expired(now));
    if provider_sessions.len() <= MAX_PROVIDER_SESSIONS {
        return;
    }
    let mut retained = provider_sessions
        .iter()
        .map(|(session_id, session)| (session_id.clone(), session.local_expires_at))
        .collect::<Vec<_>>();
    retained.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let retained_ids = retained
        .into_iter()
        .take(MAX_PROVIDER_SESSIONS)
        .map(|(session_id, _)| session_id)
        .collect::<std::collections::BTreeSet<_>>();
    provider_sessions.retain(|session_id, _| retained_ids.contains(session_id));
}
