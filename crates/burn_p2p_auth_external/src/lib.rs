#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ContentId, ExperimentScope, NetworkId, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession, StaticPrincipalRecord,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingLogin {
    login_id: ContentId,
    state: String,
    network_id: NetworkId,
    requested_scopes: BTreeSet<ExperimentScope>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderExchangeRequest {
    login_id: ContentId,
    state: String,
    network_id: NetworkId,
    provider: AuthProvider,
    provider_code: String,
    requested_scopes: BTreeSet<ExperimentScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderExchangeResponse {
    principal_id: PrincipalId,
    #[serde(default, flatten)]
    session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StandardTokenResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_in: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id_token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderProfileClaims {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(default)]
    org_memberships: BTreeSet<String>,
    #[serde(default)]
    group_memberships: BTreeSet<String>,
    #[serde(default)]
    custom_claims: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderSessionMaterial {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_handle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_expires_at: Option<DateTime<Utc>>,
    #[serde(default, flatten)]
    profile: ProviderProfileClaims,
}

impl ProviderSessionMaterial {
    fn is_empty(&self) -> bool {
        self.provider_subject.is_none()
            && self.access_token.is_none()
            && self.refresh_token.is_none()
            && self.session_handle.is_none()
            && self.provider_expires_at.is_none()
            && self.profile.display_name.is_none()
            && self.profile.org_memberships.is_empty()
            && self.profile.group_memberships.is_empty()
            && self.profile.custom_claims.is_empty()
    }

    fn merge_update(&self, update: Self) -> Self {
        Self {
            provider_subject: update
                .provider_subject
                .or_else(|| self.provider_subject.clone()),
            access_token: update.access_token.or_else(|| self.access_token.clone()),
            refresh_token: update.refresh_token.or_else(|| self.refresh_token.clone()),
            session_handle: update
                .session_handle
                .or_else(|| self.session_handle.clone()),
            provider_expires_at: update.provider_expires_at.or(self.provider_expires_at),
            profile: ProviderProfileClaims {
                display_name: update
                    .profile
                    .display_name
                    .or_else(|| self.profile.display_name.clone()),
                org_memberships: if update.profile.org_memberships.is_empty() {
                    self.profile.org_memberships.clone()
                } else {
                    update.profile.org_memberships
                },
                group_memberships: if update.profile.group_memberships.is_empty() {
                    self.profile.group_memberships.clone()
                } else {
                    update.profile.group_memberships
                },
                custom_claims: if update.profile.custom_claims.is_empty() {
                    self.profile.custom_claims.clone()
                } else {
                    let mut claims = self.profile.custom_claims.clone();
                    claims.extend(update.profile.custom_claims);
                    claims
                },
            },
        }
    }

    fn has_remote_material(&self) -> bool {
        self.provider_subject.is_some()
            || self.access_token.is_some()
            || self.refresh_token.is_some()
            || self.session_handle.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderRefreshRequest {
    network_id: NetworkId,
    principal_id: PrincipalId,
    provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_handle: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderRefreshResponse {
    #[serde(default, flatten)]
    session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderUserInfoRequest {
    network_id: NetworkId,
    principal_id: PrincipalId,
    provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_handle: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderUserInfoResponse {
    #[serde(default, flatten)]
    session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderRevokeRequest {
    network_id: NetworkId,
    principal_id: PrincipalId,
    provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_handle: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProviderExchangeOutcome {
    Mapped {
        principal_id: PrincipalId,
        session: ProviderSessionMaterial,
    },
    SessionOnly(ProviderSessionMaterial),
}

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

    pub fn with_exchange_url(mut self, exchange_url: Option<String>) -> Self {
        self.exchange_url = exchange_url;
        self
    }

    pub fn with_token_url(mut self, token_url: Option<String>) -> Self {
        self.token_url = token_url;
        self
    }

    pub fn with_client_credentials(
        mut self,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        self.client_id = client_id;
        self.client_secret = client_secret;
        self
    }

    pub fn with_userinfo_url(mut self, userinfo_url: Option<String>) -> Self {
        self.userinfo_url = userinfo_url;
        self
    }

    pub fn with_refresh_url(mut self, refresh_url: Option<String>) -> Self {
        self.refresh_url = refresh_url;
        self
    }

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

        if !record.allowed_networks.contains(&pending.network_id) {
            return Err(AuthError::NetworkNotGranted(pending.network_id));
        }

        for scope in &pending.requested_scopes {
            if !record.claims.granted_scopes.contains(scope) && !scope.allows_directory_discovery()
            {
                return Err(AuthError::ScopeNotGranted(scope.clone()));
            }
        }

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

#[derive(Debug)]
pub struct ExternalProxyIdentityConnector {
    authority_name: String,
    trusted_principal_header: String,
    session_ttl: Duration,
    pending: Mutex<BTreeMap<ContentId, PendingLogin>>,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
}

impl ExternalProxyIdentityConnector {
    pub fn new(
        authority_name: impl Into<String>,
        trusted_principal_header: impl Into<String>,
        session_ttl: Duration,
        principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    ) -> Self {
        Self {
            authority_name: authority_name.into(),
            trusted_principal_header: trusted_principal_header.into().to_ascii_lowercase(),
            session_ttl,
            pending: Mutex::new(BTreeMap::new()),
            principals,
        }
    }

    fn provider(&self) -> AuthProvider {
        AuthProvider::External {
            authority: self.authority_name.clone(),
        }
    }

    pub fn trusted_principal_header(&self) -> &str {
        &self.trusted_principal_header
    }

    pub fn principal_from_headers(
        &self,
        headers: &BTreeMap<String, String>,
    ) -> Option<PrincipalId> {
        headers
            .get(&self.trusted_principal_header)
            .map(|value| PrincipalId::new(value.clone()))
    }
}

impl IdentityConnector for ExternalProxyIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError> {
        let expires_at = Utc::now() + self.session_ttl;
        let state = format!(
            "{}-external-{}",
            req.network_id,
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
            .expect("external identity pending-login lock should not be poisoned")
            .insert(login_id.clone(), pending);

        Ok(LoginStart {
            login_id,
            provider: self.provider(),
            state,
            authorize_url: None,
            expires_at,
        })
    }

    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError> {
        let pending = self
            .pending
            .lock()
            .expect("external identity pending-login lock should not be poisoned")
            .remove(&callback.login_id)
            .ok_or_else(|| AuthError::UnknownLogin(callback.login_id.clone()))?;

        if callback.state != pending.state {
            return Err(AuthError::StateMismatch);
        }
        if pending.expires_at < Utc::now() {
            return Err(AuthError::LoginExpired(pending.login_id));
        }

        let principal_id = callback
            .principal_id
            .ok_or(AuthError::MissingProviderPrincipal)?;

        let record = self
            .principals
            .get(&principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(principal_id.clone()))?;

        if !record.allowed_networks.contains(&pending.network_id) {
            return Err(AuthError::NetworkNotGranted(pending.network_id));
        }

        for scope in &pending.requested_scopes {
            if !record.claims.granted_scopes.contains(scope) && !scope.allows_directory_discovery()
            {
                return Err(AuthError::ScopeNotGranted(scope.clone()));
            }
        }

        let issued_at = Utc::now();
        let expires_at = issued_at + self.session_ttl;
        let session_id = ContentId::derive(&(
            principal_id.as_str(),
            pending.network_id.as_str(),
            issued_at.timestamp_millis(),
            self.provider(),
        ))?;
        let mut claims = record.claims.clone();
        claims.provider = self.provider();
        claims.issued_at = issued_at;
        claims.expires_at = expires_at;

        Ok(PrincipalSession {
            session_id,
            network_id: pending.network_id,
            claims,
            issued_at,
            expires_at,
        })
    }

    fn refresh(&self, session: &PrincipalSession) -> Result<PrincipalSession, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }

        let issued_at = Utc::now();
        let expires_at = issued_at + self.session_ttl;
        let mut claims = session.claims.clone();
        claims.provider = self.provider();
        claims.issued_at = issued_at;
        claims.expires_at = expires_at;

        Ok(PrincipalSession {
            session_id: ContentId::derive(&(
                claims.principal_id.as_str(),
                session.network_id.as_str(),
                issued_at.timestamp_millis(),
                self.provider(),
            ))?,
            network_id: session.network_id.clone(),
            claims,
            issued_at,
            expires_at,
        })
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        let mut claims = session.claims.clone();
        claims.provider = self.provider();
        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration as StdDuration,
    };

    use burn_p2p_core::{
        AuthProvider, ExperimentScope, NetworkId, PeerRole, PeerRoleSet, PrincipalId,
    };
    use burn_p2p_security::{
        CallbackPayload, IdentityConnector, LoginRequest, PrincipalClaims, StaticPrincipalRecord,
    };
    use chrono::{Duration, Utc};

    use crate::{ExternalProxyIdentityConnector, ProviderMappedIdentityConnector};

    fn spawn_provider_response_server(
        assert_request: impl Fn(&str) + Send + 'static,
        response_status: &'static str,
        response_body: String,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider response listener");
        let addr = listener.local_addr().expect("local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept provider response");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(2)))
                .expect("set provider response read timeout");
            let mut buffer = [0_u8; 8192];
            let bytes_read = stream
                .read(&mut buffer)
                .expect("read provider response request");
            let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            assert_request(&request);
            let response = format!(
                "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write provider response");
            stream.flush().expect("flush provider response");
        });
        (format!("http://{addr}"), handle)
    }

    #[test]
    fn external_proxy_connector_uses_external_provider_and_trusted_header() {
        let now = Utc::now();
        let connector = ExternalProxyIdentityConnector::new(
            "corp-proxy",
            "x-auth-principal",
            Duration::minutes(10),
            BTreeMap::from([(
                PrincipalId::new("alice"),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: PrincipalId::new("alice"),
                        provider: AuthProvider::External {
                            authority: "corp-proxy".into(),
                        },
                        display_name: "Alice".into(),
                        org_memberships: BTreeSet::new(),
                        group_memberships: BTreeSet::from(["operators".into()]),
                        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                        granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        custom_claims: BTreeMap::new(),
                        issued_at: now,
                        expires_at: now + Duration::hours(1),
                    },
                    allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
                },
            )]),
        );

        assert_eq!(connector.trusted_principal_header(), "x-auth-principal");
        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: None,
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("login");
        let session = connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("session");
        assert_eq!(
            session.claims.provider,
            AuthProvider::External {
                authority: "corp-proxy".into()
            }
        );
    }

    #[test]
    fn provider_mapped_connector_refreshes_and_revokes_remote_sessions() {
        let now = Utc::now();
        let principals = BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::GitHub,
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::from(["burn-core".into()]),
                    group_memberships: BTreeSet::from(["contributors".into()]),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::from([("profile".into(), "static".into())]),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]);
        let (exchange_url, exchange_server) = spawn_provider_response_server(
            |request| {
                assert!(request.contains("\"provider_code\":\"github-code-123\""));
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
        let (refresh_url, refresh_server) = spawn_provider_response_server(
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
        let (revoke_url, revoke_server) = spawn_provider_response_server(
            |request| {
                assert!(request.contains("\"refresh_token\":\"refresh-token-2\""));
                assert!(request.contains("\"session_handle\":\"session-handle-2\""));
            },
            "200 OK",
            "{}".into(),
        );

        let connector = ProviderMappedIdentityConnector::new(
            AuthProvider::GitHub,
            Duration::minutes(10),
            principals,
            Some("https://github.example/login/oauth/authorize".into()),
        )
        .with_exchange_url(Some(exchange_url))
        .with_refresh_url(Some(refresh_url))
        .with_revoke_url(Some(revoke_url));

        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin provider login");
        let session = connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: None,
                provider_code: Some("github-code-123".into()),
            })
            .expect("complete provider login");
        assert_eq!(session.claims.display_name, "Alice GitHub");
        assert!(session.claims.org_memberships.contains("burn-core"));
        assert!(session.claims.org_memberships.contains("oss"));
        assert!(session.claims.group_memberships.contains("maintainers"));
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );

        let refreshed = connector
            .refresh(&session)
            .expect("refresh provider session");
        assert_eq!(refreshed.claims.display_name, "Alice Refreshed");
        assert!(refreshed.claims.group_memberships.contains("operators"));
        assert_eq!(
            refreshed.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice-2.png".to_owned())
        );

        connector
            .revoke(&refreshed)
            .expect("revoke provider session");

        exchange_server.join().expect("join exchange server");
        refresh_server.join().expect("join refresh server");
        revoke_server.join().expect("join revoke server");
    }

    #[test]
    fn provider_mapped_connector_hydrates_claims_via_userinfo_endpoint() {
        let now = Utc::now();
        let principals = BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::GitHub,
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::from(["burn-core".into()]),
                    group_memberships: BTreeSet::from(["contributors".into()]),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::from([("profile".into(), "static".into())]),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]);
        let (exchange_url, exchange_server) = spawn_provider_response_server(
            |request| {
                assert!(request.contains("\"provider_code\":\"github-code-userinfo\""));
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
        let (userinfo_url, userinfo_server) = spawn_provider_response_server(
            |request| {
                assert!(request.contains("\"access_token\":\"access-token-1\""));
                assert!(request.contains("\"session_handle\":\"session-handle-1\""));
            },
            "200 OK",
            serde_json::json!({
                "display_name": "Alice Profile",
                "org_memberships": ["oss"],
                "group_memberships": ["maintainers"],
                "custom_claims": {
                    "avatar_url": "https://avatars.example/alice.png"
                }
            })
            .to_string(),
        );
        let connector = ProviderMappedIdentityConnector::new(
            AuthProvider::GitHub,
            Duration::minutes(10),
            principals,
            Some("https://github.example/login/oauth/authorize".into()),
        )
        .with_exchange_url(Some(exchange_url))
        .with_userinfo_url(Some(userinfo_url));

        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin provider login");
        let session = connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: None,
                provider_code: Some("github-code-userinfo".into()),
            })
            .expect("complete provider login");
        assert_eq!(session.claims.display_name, "Alice Profile");
        assert!(session.claims.org_memberships.contains("burn-core"));
        assert!(session.claims.org_memberships.contains("oss"));
        assert!(session.claims.group_memberships.contains("maintainers"));
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );

        exchange_server.join().expect("join exchange server");
        userinfo_server.join().expect("join userinfo server");
    }

    #[test]
    fn provider_mapped_connector_supports_standard_token_exchange_and_userinfo_mapping() {
        let now = Utc::now();
        let principals = BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::GitHub,
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::from(["burn-core".into()]),
                    group_memberships: BTreeSet::from(["contributors".into()]),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::from([
                        ("provider_login".into(), "alice-gh".into()),
                        ("provider_email".into(), "alice@example.com".into()),
                    ]),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]);
        let (token_url, token_server) = spawn_provider_response_server(
            |request| {
                assert!(request.contains("grant_type=authorization_code"));
                assert!(request.contains("code=github-standard-code"));
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
        let (userinfo_url, userinfo_server) = spawn_provider_response_server(
            |request| {
                let request = request.to_ascii_lowercase();
                assert!(request.contains("authorization: bearer access-token-1"));
            },
            "200 OK",
            serde_json::json!({
                "id": 42,
                "login": "alice-gh",
                "email": "alice@example.com",
                "name": "Alice Upstream",
                "organizations": ["oss"],
                "groups": ["maintainers"],
                "avatar_url": "https://avatars.example/alice-upstream.png"
            })
            .to_string(),
        );
        let connector = ProviderMappedIdentityConnector::new(
            AuthProvider::GitHub,
            Duration::minutes(10),
            principals,
            Some("https://github.com/login/oauth/authorize".into()),
        )
        .with_token_url(Some(token_url))
        .with_client_credentials(Some("github-client".into()), Some("github-secret".into()))
        .with_userinfo_url(Some(userinfo_url));

        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: None,
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin provider login");
        let session = connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: None,
                provider_code: Some("github-standard-code".into()),
            })
            .expect("complete provider login");

        assert_eq!(session.claims.principal_id.as_str(), "alice");
        assert_eq!(session.claims.display_name, "Alice Upstream");
        assert!(session.claims.org_memberships.contains("oss"));
        assert!(session.claims.group_memberships.contains("maintainers"));
        assert_eq!(
            session.claims.custom_claims.get("provider_login"),
            Some(&"alice-gh".to_owned())
        );
        assert_eq!(
            session.claims.custom_claims.get("provider_email"),
            Some(&"alice@example.com".to_owned())
        );
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice-upstream.png".to_owned())
        );

        token_server.join().expect("join token server");
        userinfo_server.join().expect("join userinfo server");
    }
}
