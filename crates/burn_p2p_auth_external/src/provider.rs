use std::collections::BTreeMap;
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ContentId, NetworkId, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession, StaticPrincipalRecord,
};
use chrono::{Duration, Utc};
use serde_json::Value;

use crate::shared::{
    PendingLogin, ProviderExchangeOutcome, ProviderExchangeRequest, ProviderExchangeResponse,
    ProviderRefreshRequest, ProviderRefreshResponse, ProviderRevokeRequest,
    ProviderSessionMaterial, ProviderUserInfoRequest, ProviderUserInfoResponse,
    StandardTokenResponse, validate_record_access,
};

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
    provider_sessions: Mutex<BTreeMap<ContentId, ProviderSessionMaterial>>,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    authorize_base_url: Option<String>,
    exchange_url: Option<String>,
    token_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    userinfo_url: Option<String>,
    refresh_url: Option<String>,
    revoke_url: Option<String>,
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
            principals,
            authorize_base_url,
            exchange_url: None,
            token_url: None,
            client_id: None,
            client_secret: None,
            userinfo_url: None,
            refresh_url: None,
            revoke_url: None,
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

    /// Returns a copy configured to hydrate provider claims from a userinfo
    /// endpoint after login or refresh.
    pub fn with_userinfo_url(mut self, userinfo_url: Option<String>) -> Self {
        self.userinfo_url = userinfo_url;
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

    fn provider(&self) -> AuthProvider {
        self.provider.clone()
    }

    fn authorize_url(&self, login_id: &ContentId, state: &str) -> Option<String> {
        self.authorize_base_url
            .as_ref()
            .map(|base| format!("{base}?login_id={}&state={state}", login_id.as_str()))
    }

    fn uses_standard_token_flow(&self) -> bool {
        self.token_url.is_some()
    }

    fn standard_form_pairs<'a>(
        &'a self,
        grant_type: &'a str,
        code: Option<&'a str>,
        refresh_token: Option<&'a str>,
    ) -> Vec<(&'a str, &'a str)> {
        let mut pairs = vec![("grant_type", grant_type)];
        if let Some(code) = code {
            pairs.push(("code", code));
        }
        if let Some(refresh_token) = refresh_token {
            pairs.push(("refresh_token", refresh_token));
        }
        if let Some(client_id) = self.client_id.as_deref() {
            pairs.push(("client_id", client_id));
        }
        if let Some(client_secret) = self.client_secret.as_deref() {
            pairs.push(("client_secret", client_secret));
        }
        pairs
    }

    fn standard_session_from_token_response(
        &self,
        response: StandardTokenResponse,
    ) -> Result<ProviderSessionMaterial, AuthError> {
        let access_token = response.access_token.ok_or_else(|| {
            AuthError::ProviderExchange("provider token response omitted access_token".into())
        })?;
        Ok(ProviderSessionMaterial {
            access_token: Some(access_token),
            refresh_token: response.refresh_token,
            provider_expires_at: response
                .expires_in
                .map(|secs| Utc::now() + Duration::seconds(secs.max(0))),
            ..Default::default()
        })
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

    fn principal_matches_provider_session(
        record: &StaticPrincipalRecord,
        session: &ProviderSessionMaterial,
    ) -> bool {
        let claims = &record.claims.custom_claims;
        session
            .provider_subject
            .as_ref()
            .is_some_and(|subject| claims.get("provider_subject") == Some(subject))
            || session
                .profile
                .custom_claims
                .get("provider_login")
                .is_some_and(|login| claims.get("provider_login") == Some(login))
            || session
                .profile
                .custom_claims
                .get("provider_email")
                .is_some_and(|email| claims.get("provider_email") == Some(email))
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
        let mut claims: PrincipalClaims = record.claims.clone();
        claims.provider = self.provider();
        if let Some(provider_session) = provider_session.as_ref() {
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
        }
        claims.issued_at = issued_at;
        claims.expires_at = expires_at;

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
        if let Some(provider_session) = provider_session.filter(|state| !state.is_empty()) {
            provider_sessions.insert(session_id, provider_session);
        } else {
            provider_sessions.remove(&session.session_id);
        }

        Ok(session)
    }

    fn provider_session(&self, session_id: &ContentId) -> Option<ProviderSessionMaterial> {
        self.provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned")
            .get(session_id)
            .cloned()
    }

    fn carry_provider_session(
        &self,
        prior_session_id: &ContentId,
        refreshed_session_id: &ContentId,
        provider_session: Option<ProviderSessionMaterial>,
    ) {
        let mut sessions = self
            .provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned");
        sessions.remove(prior_session_id);
        if let Some(provider_session) = provider_session.filter(|state| !state.is_empty()) {
            sessions.insert(refreshed_session_id.clone(), provider_session);
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
            .token_url
            .as_ref()
            .ok_or(AuthError::MissingProviderCode)?;
        let response = reqwest::blocking::Client::new()
            .post(token_url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
            .form(&self.standard_form_pairs("authorization_code", Some(provider_code), None))
            .send()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .error_for_status()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?
            .json::<StandardTokenResponse>()
            .map_err(|error| AuthError::ProviderExchange(error.to_string()))?;
        Ok(ProviderExchangeOutcome::SessionOnly(
            self.standard_session_from_token_response(response)?,
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
            let token_url = self.token_url.as_ref().ok_or_else(|| {
                AuthError::ProviderRefresh("provider token endpoint is not configured".into())
            })?;
            let response = reqwest::blocking::Client::new()
                .post(token_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .header(reqwest::header::USER_AGENT, "burn_p2p-auth")
                .form(&self.standard_form_pairs("refresh_token", None, Some(refresh_token)))
                .send()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
                .error_for_status()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?
                .json::<StandardTokenResponse>()
                .map_err(|error| AuthError::ProviderRefresh(error.to_string()))?;
            return self.hydrate_provider_profile(
                Some(&session.network_id),
                Some(&session.claims.principal_id),
                Some(prior.merge_update(self.standard_session_from_token_response(response)?)),
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
        let Some(revoke_url) = self.revoke_url.as_ref() else {
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
                .form(
                    &self
                        .standard_form_pairs("revoke", None, None)
                        .into_iter()
                        .chain(std::iter::once(("token", token.as_str())))
                        .collect::<Vec<_>>(),
                )
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
        let Some(userinfo_url) = self.userinfo_url.as_ref() else {
            return Ok(Some(prior));
        };
        if !prior.has_remote_material() {
            return Ok(Some(prior));
        }
        if self.uses_standard_token_flow() {
            let Some(access_token) = prior.access_token.as_deref() else {
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
            return Ok(Some(prior.merge_update(
                self.provider_session_from_userinfo_value(response),
            )));
        }

        let network_id = network_id.ok_or_else(|| {
            AuthError::ProviderUserInfo("custom provider userinfo requires a network id".into())
        })?;
        let principal_id = principal_id.ok_or_else(|| {
            AuthError::ProviderUserInfo("custom provider userinfo requires a principal id".into())
        })?;
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
        let state = format!(
            "{}-{}-{}",
            req.network_id,
            match self.provider() {
                AuthProvider::GitHub => "github",
                AuthProvider::Oidc { .. } => "oidc",
                AuthProvider::OAuth { .. } => "oauth",
                AuthProvider::External { .. } => "external",
                AuthProvider::Static { .. } => "static",
            },
            expires_at.timestamp_nanos_opt().unwrap_or(0)
        );
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
        };
        self.pending
            .lock()
            .expect("provider identity pending-login lock should not be poisoned")
            .insert(login_id.clone(), pending);

        Ok(LoginStart {
            login_id: login_id.clone(),
            provider: self.provider(),
            state: state.clone(),
            authorize_url: self.authorize_url(&login_id, &state),
            expires_at,
        })
    }

    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError> {
        let pending = self
            .pending
            .lock()
            .expect("provider identity pending-login lock should not be poisoned")
            .remove(&callback.login_id)
            .ok_or_else(|| AuthError::UnknownLogin(callback.login_id.clone()))?;

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
        validate_record_access(record, &pending.network_id, &pending.requested_scopes)?;
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
        let refreshed = self.issue_session(
            session.network_id.clone(),
            session.claims.principal_id.clone(),
            record,
            provider_session.clone(),
        )?;
        self.carry_provider_session(&session.session_id, &refreshed.session_id, provider_session);
        Ok(refreshed)
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        let mut claims = session.claims.clone();
        claims.provider = self.provider();
        Ok(claims)
    }

    fn revoke(&self, session: &PrincipalSession) -> Result<(), AuthError> {
        let provider_session = self.provider_session(&session.session_id);
        self.revoke_provider_session(session, provider_session)?;
        self.provider_sessions
            .lock()
            .expect("provider identity session lock should not be poisoned")
            .remove(&session.session_id);
        Ok(())
    }
}
