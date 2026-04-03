use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ExperimentScope, NetworkId, PrincipalId};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticPrincipalRecord {
    pub claims: PrincipalClaims,
    pub allowed_networks: BTreeSet<NetworkId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PendingLogin {
    login_id: burn_p2p_core::ContentId,
    state: String,
    network_id: NetworkId,
    requested_scopes: BTreeSet<ExperimentScope>,
    expires_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct StaticIdentityConnector {
    authority_name: String,
    session_ttl: Duration,
    pending: Mutex<BTreeMap<burn_p2p_core::ContentId, PendingLogin>>,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
}

impl StaticIdentityConnector {
    pub fn new(
        authority_name: impl Into<String>,
        session_ttl: Duration,
        principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    ) -> Self {
        Self {
            authority_name: authority_name.into(),
            session_ttl,
            pending: Mutex::new(BTreeMap::new()),
            principals,
        }
    }

    fn provider(&self) -> AuthProvider {
        AuthProvider::Static {
            authority: self.authority_name.clone(),
        }
    }
}

impl IdentityConnector for StaticIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError> {
        let expires_at = Utc::now() + self.session_ttl;
        let state = format!(
            "{}-{}",
            req.network_id,
            expires_at.timestamp_nanos_opt().unwrap_or(0)
        );
        let login_id = burn_p2p_core::ContentId::derive(&(
            req.network_id.as_str(),
            &state,
            &req.principal_hint,
            &req.requested_scopes,
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
            .expect("static identity pending-login lock should not be poisoned")
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
            .expect("static identity pending-login lock should not be poisoned")
            .remove(&callback.login_id)
            .ok_or_else(|| AuthError::UnknownLogin(callback.login_id.clone()))?;

        if callback.state != pending.state {
            return Err(AuthError::StateMismatch);
        }

        if pending.expires_at < Utc::now() {
            return Err(AuthError::LoginExpired(pending.login_id));
        }

        let record = self
            .principals
            .get(&callback.principal_id)
            .ok_or_else(|| AuthError::UnknownPrincipal(callback.principal_id.clone()))?;

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
        let session_id = burn_p2p_core::ContentId::derive(&(
            callback.principal_id.as_str(),
            pending.network_id.as_str(),
            issued_at.timestamp_millis(),
        ))?;

        Ok(PrincipalSession {
            session_id,
            network_id: pending.network_id,
            claims: record.claims.clone(),
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
        Ok(PrincipalSession {
            session_id: burn_p2p_core::ContentId::derive(&(
                session.claims.principal_id.as_str(),
                session.network_id.as_str(),
                issued_at.timestamp_millis(),
            ))?,
            network_id: session.network_id.clone(),
            claims: session.claims.clone(),
            issued_at,
            expires_at,
        })
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        Ok(session.claims.clone())
    }
}
