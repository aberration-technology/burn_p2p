//! OAuth-backed identity connector implementations for burn_p2p.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use burn_p2p_auth_external::ProviderMappedIdentityConnector;
use burn_p2p_core::{AuthProvider, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart, PrincipalClaims,
    PrincipalSession, StaticPrincipalRecord,
};
use chrono::Duration;

#[derive(Debug)]
/// Represents an o auth identity connector.
pub struct OAuthIdentityConnector(ProviderMappedIdentityConnector);

impl OAuthIdentityConnector {
    /// Creates a new value.
    pub fn new(
        provider: impl Into<String>,
        session_ttl: Duration,
        principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
        authorize_base_url: Option<String>,
    ) -> Self {
        Self(ProviderMappedIdentityConnector::new(
            AuthProvider::OAuth {
                provider: provider.into(),
            },
            session_ttl,
            principals,
            authorize_base_url,
        ))
    }

    /// Returns a copy configured with the exchange URL.
    pub fn with_exchange_url(mut self, exchange_url: Option<String>) -> Self {
        self.0 = self.0.with_exchange_url(exchange_url);
        self
    }

    /// Returns a copy configured with the token URL.
    pub fn with_token_url(mut self, token_url: Option<String>) -> Self {
        self.0 = self.0.with_token_url(token_url);
        self
    }

    /// Returns a copy configured with the client credentials.
    pub fn with_client_credentials(
        mut self,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        self.0 = self.0.with_client_credentials(client_id, client_secret);
        self
    }

    /// Returns a copy configured with the userinfo URL.
    pub fn with_userinfo_url(mut self, userinfo_url: Option<String>) -> Self {
        self.0 = self.0.with_userinfo_url(userinfo_url);
        self
    }

    /// Returns a copy configured with the refresh URL.
    pub fn with_refresh_url(mut self, refresh_url: Option<String>) -> Self {
        self.0 = self.0.with_refresh_url(refresh_url);
        self
    }

    /// Returns a copy configured with the revoke URL.
    pub fn with_revoke_url(mut self, revoke_url: Option<String>) -> Self {
        self.0 = self.0.with_revoke_url(revoke_url);
        self
    }
}

impl IdentityConnector for OAuthIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError> {
        self.0.begin_login(req)
    }

    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError> {
        self.0.complete_login(callback)
    }

    fn refresh(&self, session: &PrincipalSession) -> Result<PrincipalSession, AuthError> {
        self.0.refresh(session)
    }

    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError> {
        self.0.fetch_claims(session)
    }

    fn revoke(&self, session: &PrincipalSession) -> Result<(), AuthError> {
        self.0.revoke(session)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        AuthProvider, ExperimentScope, NetworkId, PeerRole, PeerRoleSet, PrincipalId,
    };
    use burn_p2p_security::{
        CallbackPayload, IdentityConnector, LoginRequest, PrincipalClaims, StaticPrincipalRecord,
    };
    use chrono::{Duration, Utc};

    use crate::OAuthIdentityConnector;

    #[test]
    fn oauth_connector_issues_oauth_sessions() {
        let now = Utc::now();
        let connector = OAuthIdentityConnector::new(
            "research-sso",
            Duration::minutes(10),
            BTreeMap::from([(
                PrincipalId::new("alice"),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: PrincipalId::new("alice"),
                        provider: AuthProvider::OAuth {
                            provider: "research-sso".into(),
                        },
                        display_name: "Alice".into(),
                        org_memberships: BTreeSet::new(),
                        group_memberships: BTreeSet::from(["contributors".into()]),
                        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                        granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        custom_claims: BTreeMap::new(),
                        issued_at: now,
                        expires_at: now + Duration::hours(1),
                    },
                    allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
                },
            )]),
            Some("https://oauth.example/authorize".into()),
        );

        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
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
            AuthProvider::OAuth {
                provider: "research-sso".into()
            }
        );
    }
}
