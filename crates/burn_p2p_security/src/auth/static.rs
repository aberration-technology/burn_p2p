use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use burn_p2p_core::{AuthProvider, ContentId, ExperimentScope, NetworkId, PrincipalId};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::{
    AuthError, CallbackPayload, DEFAULT_PENDING_LOGIN_LIMIT, IdentityConnector, LoginRequest,
    LoginStart, PrincipalClaims, PrincipalSession, prune_expiring_entries,
    validate_principal_record_access,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a static principal record.
pub struct StaticPrincipalRecord {
    /// The claims.
    pub claims: PrincipalClaims,
    /// The allowed networks.
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
/// Represents a static identity connector.
pub struct StaticIdentityConnector {
    authority_name: String,
    session_ttl: Duration,
    pending: Mutex<BTreeMap<ContentId, PendingLogin>>,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
}

impl StaticIdentityConnector {
    /// Creates a new value.
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

    fn issue_session(
        &self,
        network_id: NetworkId,
        principal_id: PrincipalId,
        record: &StaticPrincipalRecord,
    ) -> Result<PrincipalSession, AuthError> {
        let issued_at = Utc::now();
        let expires_at = issued_at + self.session_ttl;
        let session_id = ContentId::derive(&(
            principal_id.as_str(),
            network_id.as_str(),
            issued_at.timestamp_millis(),
        ))?;
        let mut claims = record.claims.clone();
        claims.provider = self.provider();
        claims.issued_at = issued_at;
        claims.expires_at = expires_at;

        Ok(PrincipalSession {
            session_id,
            network_id,
            claims,
            issued_at,
            expires_at,
        })
    }
}

impl IdentityConnector for StaticIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError> {
        let expires_at = Utc::now() + self.session_ttl;
        let state = super::random_login_state_token("static login state")?;
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
        let mut pending_logins = self
            .pending
            .lock()
            .expect("static identity pending-login lock should not be poisoned");
        prune_expiring_entries(
            &mut pending_logins,
            Utc::now(),
            DEFAULT_PENDING_LOGIN_LIMIT,
            |login| login.expires_at,
        );
        pending_logins.insert(login_id.clone(), pending);
        prune_expiring_entries(
            &mut pending_logins,
            Utc::now(),
            DEFAULT_PENDING_LOGIN_LIMIT,
            |login| login.expires_at,
        );

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
                .expect("static identity pending-login lock should not be poisoned");
            prune_expiring_entries(
                &mut pending_logins,
                Utc::now(),
                DEFAULT_PENDING_LOGIN_LIMIT,
                |login| login.expires_at,
            );
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
        self.issue_session(pending.network_id, principal_id, record)
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
        self.issue_session(
            session.network_id.clone(),
            session.claims.principal_id.clone(),
            record,
        )
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        if session.expires_at < Utc::now() {
            return Err(AuthError::SessionExpired(session.session_id.clone()));
        }
        Ok(session.claims.clone())
    }

    fn export_persistent_state(&self) -> Result<Option<Vec<u8>>, AuthError> {
        let pending = {
            let mut pending = self
                .pending
                .lock()
                .expect("static identity pending-login lock should not be poisoned");
            prune_expiring_entries(
                &mut pending,
                Utc::now(),
                DEFAULT_PENDING_LOGIN_LIMIT,
                |login| login.expires_at,
            );
            pending.clone()
        };
        Ok(Some(burn_p2p_core::deterministic_cbor(&pending)?))
    }

    fn import_persistent_state(&self, state: Option<&[u8]>) -> Result<(), AuthError> {
        let now = Utc::now();
        let mut pending = self
            .pending
            .lock()
            .expect("static identity pending-login lock should not be poisoned");
        *pending = match state {
            Some(state) => {
                burn_p2p_core::from_cbor_slice::<BTreeMap<ContentId, PendingLogin>>(state)?
                    .into_iter()
                    .collect()
            }
            None => BTreeMap::new(),
        };
        prune_expiring_entries(&mut pending, now, DEFAULT_PENDING_LOGIN_LIMIT, |login| {
            login.expires_at
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        AuthProvider, ContentId, ExperimentScope, NetworkId, PeerRole, PeerRoleSet, PrincipalId,
    };
    use chrono::{Duration, Utc};

    use super::{PendingLogin, StaticIdentityConnector, StaticPrincipalRecord};
    use crate::auth::{
        CallbackPayload, DEFAULT_PENDING_LOGIN_LIMIT, IdentityConnector, LoginRequest,
        PrincipalClaims,
    };

    fn static_connector() -> StaticIdentityConnector {
        let now = Utc::now();
        StaticIdentityConnector::new(
            "lab-auth",
            Duration::minutes(10),
            BTreeMap::from([(
                PrincipalId::new("alice"),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: PrincipalId::new("alice"),
                        provider: AuthProvider::Static {
                            authority: "lab-auth".into(),
                        },
                        display_name: "Alice".into(),
                        org_memberships: BTreeSet::new(),
                        group_memberships: BTreeSet::new(),
                        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                        granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        custom_claims: BTreeMap::new(),
                        issued_at: now - Duration::hours(4),
                        expires_at: now - Duration::hours(3),
                    },
                    allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
                },
            )]),
        )
    }

    #[test]
    fn static_connector_prunes_and_bounds_pending_logins() {
        let now = Utc::now();
        let connector = static_connector();
        let seeded = (0..300)
            .map(|index| {
                (
                    ContentId::new(format!("login-{index:03}")),
                    PendingLogin {
                        login_id: ContentId::new(format!("login-{index:03}")),
                        state: format!("state-{index:03}"),
                        network_id: NetworkId::new("network-a"),
                        requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        expires_at: if index < 32 {
                            now - Duration::minutes(1)
                        } else {
                            now + Duration::minutes(index as i64)
                        },
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let bytes = burn_p2p_core::deterministic_cbor(&seeded).expect("encode pending state");
        connector
            .import_persistent_state(Some(&bytes))
            .expect("import pending state");

        let exported = connector
            .export_persistent_state()
            .expect("export pending state")
            .expect("state bytes");
        let restored =
            burn_p2p_core::from_cbor_slice::<BTreeMap<ContentId, PendingLogin>>(&exported)
                .expect("decode pending state");

        assert_eq!(restored.len(), DEFAULT_PENDING_LOGIN_LIMIT);
        assert!(restored.values().all(|login| login.expires_at >= now));
    }

    #[test]
    fn static_connector_issues_session_claims_with_current_timestamps() {
        let connector = static_connector();
        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin login");
        let session = connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("complete login");

        assert_eq!(
            session.claims.provider,
            AuthProvider::Static {
                authority: "lab-auth".into()
            }
        );
        assert_eq!(session.claims.issued_at, session.issued_at);
        assert_eq!(session.claims.expires_at, session.expires_at);
        assert!(session.claims.expires_at > Utc::now());
    }
}
