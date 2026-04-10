use std::collections::BTreeMap;
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ContentId, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession, StaticPrincipalRecord,
    auth::{DEFAULT_PENDING_LOGIN_LIMIT, prune_expiring_entries, validate_principal_record_access},
    random_login_state_token,
};
use chrono::{Duration, Utc};

use crate::shared::{PendingLogin, ProxyConnectorState};

/// Authenticates principals through a trusted upstream proxy header.
///
/// This connector is intended for private deployments that terminate identity at
/// a trusted ingress layer such as an internal SSO gateway or mTLS-authenticated
/// reverse proxy. It does not perform remote provider exchanges itself.
#[derive(Debug)]
pub struct ExternalProxyIdentityConnector {
    authority_name: String,
    trusted_principal_header: String,
    session_ttl: Duration,
    pending: Mutex<BTreeMap<ContentId, PendingLogin>>,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
}

impl ExternalProxyIdentityConnector {
    /// Creates a connector that trusts the given upstream principal header.
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

    /// Returns the lower-cased trusted ingress header name that carries the
    /// authenticated principal identity.
    pub fn trusted_principal_header(&self) -> &str {
        &self.trusted_principal_header
    }

    /// Extracts the trusted principal from a lower-cased header map.
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
        let state = random_login_state_token("external proxy login state")?;
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
            oidc_nonce: None,
            pkce_verifier: None,
        };
        let mut pending_logins = self
            .pending
            .lock()
            .expect("external identity pending-login lock should not be poisoned");
        prune_pending_logins(&mut pending_logins, Utc::now());
        pending_logins.insert(login_id.clone(), pending);
        prune_pending_logins(&mut pending_logins, Utc::now());

        Ok(LoginStart {
            login_id,
            provider: self.provider(),
            state,
            authorize_url: None,
            expires_at,
        })
    }

    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError> {
        let pending = {
            let mut pending_logins = self
                .pending
                .lock()
                .expect("external identity pending-login lock should not be poisoned");
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

        let principal_id = callback
            .principal_id
            .ok_or(AuthError::MissingProviderPrincipal)?;

        let record = self
            .principals
            .get(&principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(principal_id.clone()))?;
        validate_principal_record_access(record, &pending.network_id, &pending.requested_scopes)?;

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
        let record = self
            .principals
            .get(&session.claims.principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(session.claims.principal_id.clone()))?;
        validate_principal_record_access(
            record,
            &session.network_id,
            &session.claims.granted_scopes,
        )?;

        let issued_at = Utc::now();
        let expires_at = issued_at + self.session_ttl;
        let mut claims = record.claims.clone();
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

    fn export_persistent_state(&self) -> Result<Option<Vec<u8>>, AuthError> {
        let now = Utc::now();
        let pending = {
            let mut pending = self
                .pending
                .lock()
                .expect("external identity pending-login lock should not be poisoned");
            prune_pending_logins(&mut pending, now);
            pending.clone()
        };
        let state = ProxyConnectorState { pending };
        Ok(Some(burn_p2p_core::deterministic_cbor(&state)?))
    }

    fn import_persistent_state(&self, state: Option<&[u8]>) -> Result<(), AuthError> {
        let now = Utc::now();
        let mut pending = self
            .pending
            .lock()
            .expect("external identity pending-login lock should not be poisoned");
        *pending = match state {
            Some(state) => burn_p2p_core::from_cbor_slice::<ProxyConnectorState>(state)?
                .pending
                .into_iter()
                .filter(|(_, login)| login.expires_at >= now)
                .collect(),
            None => BTreeMap::new(),
        };
        prune_pending_logins(&mut pending, now);
        Ok(())
    }
}

fn prune_pending_logins(
    pending: &mut BTreeMap<ContentId, PendingLogin>,
    now: chrono::DateTime<Utc>,
) {
    prune_expiring_entries(pending, now, DEFAULT_PENDING_LOGIN_LIMIT, |login| {
        login.expires_at
    });
}
