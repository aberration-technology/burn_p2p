use super::*;

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
/// Represents a revocation epoch.
pub struct RevocationEpoch(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported auth providers.
pub enum AuthProvider {
    /// Uses GitHub integration.
    GitHub,
    /// Uses OIDC integration.
    Oidc {
        /// The issuer.
        issuer: String,
    },
    /// Uses OAuth integration.
    OAuth {
        /// The provider.
        provider: String,
    },
    /// Uses an external integration.
    External {
        /// The authority.
        authority: String,
    },
    /// Uses static provisioning.
    Static {
        /// The authority.
        authority: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported edge auth providers.
pub enum EdgeAuthProvider {
    /// Uses static provisioning.
    Static,
    /// Uses GitHub integration.
    GitHub,
    /// Uses OIDC integration.
    Oidc,
    /// Uses OAuth integration.
    OAuth,
    /// Uses an external integration.
    External,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported portal modes.
pub enum PortalMode {
    /// Disables this capability.
    Disabled,
    /// Exposes read-only access.
    Readonly,
    /// Exposes interactive access.
    Interactive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported browser modes.
pub enum BrowserMode {
    /// Disables this capability.
    Disabled,
    /// Allows observer behavior.
    Observer,
    /// Allows verifier behavior.
    Verifier,
    /// Allows trainer behavior.
    Trainer,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported social modes.
pub enum SocialMode {
    /// Disables this capability.
    Disabled,
    /// Exposes the private setting.
    Private,
    /// Exposes the public setting.
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported profile modes.
pub enum ProfileMode {
    /// Disables this capability.
    Disabled,
    /// Exposes the private setting.
    Private,
    /// Exposes the public setting.
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported admin modes.
pub enum AdminMode {
    /// Disables this capability.
    Disabled,
    /// Runs in token mode.
    Token,
    /// Runs in RBAC mode.
    Rbac,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported metrics modes.
pub enum MetricsMode {
    /// Disables this capability.
    Disabled,
    /// Runs in open metrics mode.
    OpenMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported edge feature values.
pub enum EdgeFeature {
    /// Uses the admin HTTP variant.
    AdminHttp,
    /// Uses the metrics variant.
    Metrics,
    /// Uses the portal variant.
    Portal,
    /// Uses the browser edge variant.
    BrowserEdge,
    /// Uses the RBAC variant.
    Rbac,
    /// Uses the auth static variant.
    AuthStatic,
    /// Uses the auth git hub variant.
    AuthGitHub,
    /// Uses the auth OIDC variant.
    AuthOidc,
    /// Uses the auth o auth variant.
    AuthOAuth,
    /// Uses the auth external variant.
    AuthExternal,
    /// Uses the social variant.
    Social,
    /// Uses the profiles variant.
    Profiles,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a compiled feature set.
pub struct CompiledFeatureSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a configured service set.
pub struct ConfiguredServiceSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an active service set.
pub struct ActiveServiceSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Signed capability manifest for one bootstrap or edge deployment.
///
/// This is intentionally separate from [`NetworkManifest`]. Network manifests
/// describe trust-domain invariants and compatibility, while edge service
/// manifests advertise optional deployment surfaces such as portal mode,
/// browser ingress, auth providers, social/profile visibility, and the active
/// compiled feature set for one edge.
pub struct EdgeServiceManifest {
    /// The edge ID.
    pub edge_id: PeerId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The portal mode.
    pub portal_mode: PortalMode,
    /// The browser mode.
    pub browser_mode: BrowserMode,
    /// The available auth providers.
    pub available_auth_providers: BTreeSet<EdgeAuthProvider>,
    /// The social mode.
    pub social_mode: SocialMode,
    /// The profile mode.
    pub profile_mode: ProfileMode,
    /// The admin mode.
    pub admin_mode: AdminMode,
    /// The metrics mode.
    pub metrics_mode: MetricsMode,
    /// The compiled feature set.
    pub compiled_feature_set: CompiledFeatureSet,
    /// The configured service set.
    pub configured_service_set: ConfiguredServiceSet,
    /// The active feature set.
    pub active_feature_set: ActiveServiceSet,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}
