use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{AuthProvider, ContentId, ExperimentScope, NetworkId, PrincipalId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingLogin {
    pub(crate) login_id: ContentId,
    pub(crate) state: String,
    pub(crate) network_id: NetworkId,
    pub(crate) requested_scopes: BTreeSet<ExperimentScope>,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) oidc_nonce: Option<String>,
    pub(crate) pkce_verifier: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProxyConnectorState {
    #[serde(default)]
    pub(crate) pending: BTreeMap<ContentId, PendingLogin>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderConnectorState {
    #[serde(default)]
    pub(crate) pending: BTreeMap<ContentId, PendingLogin>,
    #[serde(default)]
    pub(crate) provider_sessions: BTreeMap<ContentId, StoredProviderSession>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderExchangeRequest {
    pub(crate) login_id: ContentId,
    pub(crate) state: String,
    pub(crate) network_id: NetworkId,
    pub(crate) provider: AuthProvider,
    pub(crate) provider_code: String,
    pub(crate) requested_scopes: BTreeSet<ExperimentScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderExchangeResponse {
    pub(crate) principal_id: PrincipalId,
    #[serde(default, flatten)]
    pub(crate) session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StandardTokenResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) token_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expires_in: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) id_token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderProfileClaims {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) org_memberships: BTreeSet<String>,
    #[serde(default)]
    pub(crate) group_memberships: BTreeSet<String>,
    #[serde(default)]
    pub(crate) custom_claims: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderSessionMaterial {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_handle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_expires_at: Option<DateTime<Utc>>,
    #[serde(default, flatten)]
    pub(crate) profile: ProviderProfileClaims,
}

impl ProviderSessionMaterial {
    pub(crate) fn is_empty(&self) -> bool {
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

    pub(crate) fn merge_update(&self, update: Self) -> Self {
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

    pub(crate) fn has_remote_material(&self) -> bool {
        self.provider_subject.is_some()
            || self.access_token.is_some()
            || self.refresh_token.is_some()
            || self.session_handle.is_some()
    }

    pub(crate) fn redact_remote_secrets(&self) -> Self {
        let mut redacted = self.clone();
        redacted.access_token = None;
        redacted.refresh_token = None;
        redacted.session_handle = None;
        redacted
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredProviderSession {
    pub(crate) material: ProviderSessionMaterial,
    pub(crate) local_expires_at: DateTime<Utc>,
}

impl StoredProviderSession {
    pub(crate) fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.local_expires_at < now
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderRefreshRequest {
    pub(crate) network_id: NetworkId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_handle: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderRefreshResponse {
    #[serde(default, flatten)]
    pub(crate) session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderUserInfoRequest {
    pub(crate) network_id: NetworkId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_handle: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderUserInfoResponse {
    #[serde(default, flatten)]
    pub(crate) session: ProviderSessionMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProviderRevokeRequest {
    pub(crate) network_id: NetworkId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) provider: AuthProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_handle: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProviderExchangeOutcome {
    Mapped {
        principal_id: PrincipalId,
        session: ProviderSessionMaterial,
    },
    SessionOnly(ProviderSessionMaterial),
}
