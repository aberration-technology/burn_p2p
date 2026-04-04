#[path = "static.rs"]
mod static_connector;

use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AuthProvider, ContentId, ExperimentScope, NetworkId, NodeCertificate, NodeCertificateClaims,
    PeerAuthEnvelope, PeerId, PeerRoleSet, PrincipalId, ProjectFamilyId, RevocationEpoch,
    SchemaEnvelope, SignatureAlgorithm, SignatureMetadata,
};
use chrono::{DateTime, Utc};
use libp2p_identity::{Keypair, PublicKey};
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::{AdmissionDecision, AuditFinding};

pub use static_connector::{StaticIdentityConnector, StaticPrincipalRecord};

#[derive(Debug, thiserror::Error)]
/// Enumerates the supported auth error values.
pub enum AuthError {
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("failed to sign payload: {0}")]
    /// Uses the signing variant.
    Signing(String),
    #[error("failed to decode public key: {0}")]
    /// Uses the public key decode variant.
    PublicKeyDecode(String),
    #[error("failed to decode signature hex: {0}")]
    /// Uses the signature decode variant.
    SignatureDecode(String),
    #[error("unknown login: {0}")]
    /// Uses the unknown login variant.
    UnknownLogin(ContentId),
    #[error("state mismatch")]
    /// Uses the state mismatch variant.
    StateMismatch,
    #[error("login expired: {0}")]
    /// Uses the login expired variant.
    LoginExpired(ContentId),
    #[error("session expired: {0}")]
    /// Uses the session expired variant.
    SessionExpired(ContentId),
    #[error("unknown principal: {0}")]
    /// Uses the unknown principal variant.
    UnknownPrincipal(PrincipalId),
    #[error("provider callback is missing a principal and no provider code exchange succeeded")]
    /// Uses the missing provider principal variant.
    MissingProviderPrincipal,
    #[error("provider callback is missing an authorization or device code")]
    /// Uses the missing provider code variant.
    MissingProviderCode,
    #[error("provider exchange failed: {0}")]
    /// Uses the provider exchange variant.
    ProviderExchange(String),
    #[error("provider userinfo fetch failed: {0}")]
    /// Uses the provider user info variant.
    ProviderUserInfo(String),
    #[error("provider refresh failed: {0}")]
    /// Uses the provider refresh variant.
    ProviderRefresh(String),
    #[error("provider revoke failed: {0}")]
    /// Uses the provider revoke variant.
    ProviderRevoke(String),
    #[error("untrusted issuer: {0}")]
    /// Uses the untrusted issuer variant.
    UntrustedIssuer(PeerId),
    #[error("network not granted: {0}")]
    /// Uses the network not granted variant.
    NetworkNotGranted(NetworkId),
    #[error("project family mismatch: expected {expected}, found {found}")]
    /// Uses the project family mismatch variant.
    ProjectFamilyMismatch {
        /// The expected.
        expected: ProjectFamilyId,
        /// The found.
        found: ProjectFamilyId,
    },
    #[error("release train hash mismatch: expected {expected}, found {found}")]
    /// Uses the release train hash mismatch variant.
    ReleaseTrainHashMismatch {
        /// The expected.
        expected: ContentId,
        /// The found.
        found: ContentId,
    },
    #[error("scope not granted: {0:?}")]
    /// Uses the scope not granted variant.
    ScopeNotGranted(ExperimentScope),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a login request.
pub struct LoginRequest {
    /// The network ID.
    pub network_id: NetworkId,
    /// The principal hint.
    pub principal_hint: Option<String>,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a login start.
pub struct LoginStart {
    /// The login ID.
    pub login_id: ContentId,
    /// The provider.
    pub provider: AuthProvider,
    /// The state.
    pub state: String,
    /// The authorize URL.
    pub authorize_url: Option<String>,
    /// The expires at.
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a callback payload.
pub struct CallbackPayload {
    /// The login ID.
    pub login_id: ContentId,
    /// The state.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The principal ID.
    pub principal_id: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The provider code.
    pub provider_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a principal claims.
pub struct PrincipalClaims {
    /// The principal ID.
    pub principal_id: PrincipalId,
    /// The provider.
    pub provider: AuthProvider,
    /// The display name.
    pub display_name: String,
    /// The org memberships.
    pub org_memberships: BTreeSet<String>,
    /// The group memberships.
    pub group_memberships: BTreeSet<String>,
    /// The granted roles.
    pub granted_roles: PeerRoleSet,
    /// The granted scopes.
    pub granted_scopes: BTreeSet<ExperimentScope>,
    /// The custom claims.
    pub custom_claims: BTreeMap<String, String>,
    /// The issued at.
    pub issued_at: DateTime<Utc>,
    /// The expires at.
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a principal session.
pub struct PrincipalSession {
    /// The session ID.
    pub session_id: ContentId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The claims.
    pub claims: PrincipalClaims,
    /// The issued at.
    pub issued_at: DateTime<Utc>,
    /// The expires at.
    pub expires_at: DateTime<Utc>,
}

/// Defines behavior for identity connector.
pub trait IdentityConnector {
    /// Begins the login flow.
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError>;
    /// Completes the login flow.
    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError>;
    /// Performs the refresh operation.
    fn refresh(&self, session: &PrincipalSession) -> Result<PrincipalSession, AuthError>;
    /// Fetches the claims.
    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError>;
    /// Performs the revoke operation.
    fn revoke(&self, _session: &PrincipalSession) -> Result<(), AuthError> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a node enrollment request.
pub struct NodeEnrollmentRequest {
    /// The session.
    pub session: PrincipalSession,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The peer public key hex.
    pub peer_public_key_hex: String,
    /// The granted roles.
    pub granted_roles: PeerRoleSet,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// The client policy hash.
    pub client_policy_hash: Option<ContentId>,
    /// The serial.
    pub serial: u64,
    /// The not before.
    pub not_before: DateTime<Utc>,
    /// The not after.
    pub not_after: DateTime<Utc>,
    /// The revocation epoch.
    pub revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug)]
/// Represents a node certificate authority.
pub struct NodeCertificateAuthority {
    network_id: NetworkId,
    project_family_id: ProjectFamilyId,
    required_release_train_hash: ContentId,
    protocol_version: Version,
    issuer_keypair: Keypair,
    issuer_key_id: String,
    issuer_public_key_hex: String,
}

impl NodeCertificateAuthority {
    /// Creates a new value.
    pub fn new(
        network_id: NetworkId,
        project_family_id: ProjectFamilyId,
        required_release_train_hash: ContentId,
        protocol_version: Version,
        issuer_keypair: Keypair,
        issuer_key_id: impl Into<String>,
    ) -> Result<Self, AuthError> {
        let issuer_public_key_hex = hex::encode(issuer_keypair.public().encode_protobuf());
        Ok(Self {
            network_id,
            project_family_id,
            required_release_train_hash,
            protocol_version,
            issuer_keypair,
            issuer_key_id: issuer_key_id.into(),
            issuer_public_key_hex,
        })
    }

    /// Performs the issuer peer ID operation.
    pub fn issuer_peer_id(&self) -> PeerId {
        PeerId::new(
            libp2p_identity::PeerId::from_public_key(&self.issuer_keypair.public()).to_string(),
        )
    }

    /// Performs the issuer public key hex operation.
    pub fn issuer_public_key_hex(&self) -> &str {
        &self.issuer_public_key_hex
    }

    /// Issues the certificate.
    pub fn issue_certificate(
        &self,
        request: NodeEnrollmentRequest,
    ) -> Result<NodeCertificate, AuthError> {
        if request.project_family_id != self.project_family_id {
            return Err(AuthError::ProjectFamilyMismatch {
                expected: self.project_family_id.clone(),
                found: request.project_family_id,
            });
        }

        if request.release_train_hash != self.required_release_train_hash {
            return Err(AuthError::ReleaseTrainHashMismatch {
                expected: self.required_release_train_hash.clone(),
                found: request.release_train_hash,
            });
        }

        let claims = NodeCertificateClaims {
            network_id: self.network_id.clone(),
            project_family_id: request.project_family_id,
            release_train_hash: request.release_train_hash,
            target_artifact_hash: request.target_artifact_hash,
            peer_id: request.peer_id,
            peer_public_key_hex: request.peer_public_key_hex,
            principal_id: request.session.claims.principal_id.clone(),
            provider: request.session.claims.provider.clone(),
            granted_roles: request.granted_roles,
            experiment_scopes: request.requested_scopes,
            client_policy_hash: request.client_policy_hash,
            not_before: request.not_before,
            not_after: request.not_after,
            serial: request.serial,
            revocation_epoch: request.revocation_epoch,
        };

        let signature = sign_envelope(
            &self.issuer_keypair,
            &self.issuer_key_id,
            self.protocol_version.clone(),
            "burn_p2p.node_certificate",
            &claims,
        )?;

        NodeCertificate::new(self.protocol_version.clone(), claims, signature)
            .map_err(AuthError::from)
    }

    /// Performs the create peer auth envelope operation.
    pub fn create_peer_auth_envelope(
        &self,
        node_keypair: &Keypair,
        certificate: NodeCertificate,
        client_manifest_id: Option<ContentId>,
        requested_scopes: BTreeSet<ExperimentScope>,
        nonce_hash: ContentId,
        presented_at: DateTime<Utc>,
    ) -> Result<PeerAuthEnvelope, AuthError> {
        create_peer_auth_envelope(
            node_keypair,
            certificate,
            client_manifest_id,
            requested_scopes,
            nonce_hash,
            presented_at,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a trusted issuer.
pub struct TrustedIssuer {
    /// The issuer peer ID.
    pub issuer_peer_id: PeerId,
    /// The issuer public key hex.
    pub issuer_public_key_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the admission policy.
pub struct AdmissionPolicy {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The trusted issuers.
    pub trusted_issuers: BTreeMap<PeerId, TrustedIssuer>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported peer trust level values.
pub enum PeerTrustLevel {
    /// Uses the transport authenticated variant.
    TransportAuthenticated,
    /// Uses the network authenticated variant.
    NetworkAuthenticated,
    /// Uses the scope authorized variant.
    ScopeAuthorized,
    /// Uses the policy compliant variant.
    PolicyCompliant,
    /// Uses the reputable variant.
    Reputable,
    /// Uses the privileged variant.
    Privileged,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Reports peer admission details.
pub struct PeerAdmissionReport {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The principal ID.
    pub principal_id: PrincipalId,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// The trust level.
    pub trust_level: PeerTrustLevel,
    /// The issuer peer ID.
    pub issuer_peer_id: PeerId,
    /// The findings.
    pub findings: Vec<AuditFinding>,
    /// The verified at.
    pub verified_at: DateTime<Utc>,
}

impl PeerAdmissionReport {
    /// Performs the decision operation.
    pub fn decision(&self) -> AdmissionDecision {
        if self.findings.is_empty() {
            AdmissionDecision::Allow
        } else {
            AdmissionDecision::Reject(self.findings.clone())
        }
    }
}

impl AdmissionPolicy {
    /// Verifies the peer auth.
    pub fn verify_peer_auth(
        &self,
        envelope: &PeerAuthEnvelope,
        transport_peer_id: &PeerId,
        now: DateTime<Utc>,
    ) -> Result<PeerAdmissionReport, AuthError> {
        let mut findings = Vec::new();
        let claims = envelope.certificate.claims();

        if &envelope.peer_id != transport_peer_id {
            findings.push(AuditFinding::TransportPeerMismatch {
                expected: transport_peer_id.clone(),
                found: envelope.peer_id.clone(),
            });
        }

        if &claims.peer_id != transport_peer_id {
            findings.push(AuditFinding::CertificatePeerMismatch {
                expected: transport_peer_id.clone(),
                found: claims.peer_id.clone(),
            });
        }

        if claims.network_id != self.network_id {
            findings.push(AuditFinding::NetworkMismatch {
                expected: self.network_id.clone(),
                found: claims.network_id.clone(),
            });
        }

        if claims.project_family_id != self.project_family_id {
            findings.push(AuditFinding::ProjectFamilyMismatch {
                expected: Some(self.project_family_id.clone()),
                found: claims.project_family_id.clone(),
            });
        }

        if claims.release_train_hash != self.required_release_train_hash {
            findings.push(AuditFinding::ReleaseTrainHashMismatch {
                expected: Some(self.required_release_train_hash.clone()),
                found: claims.release_train_hash.clone(),
            });
        }

        if !self.allowed_target_artifact_hashes.is_empty()
            && !self
                .allowed_target_artifact_hashes
                .contains(&claims.target_artifact_hash)
        {
            findings.push(AuditFinding::TargetArtifactHashMismatch {
                expected: Some(self.allowed_target_artifact_hashes.clone()),
                found: claims.target_artifact_hash.clone(),
            });
        }

        if claims.not_before > now {
            findings.push(AuditFinding::CertificateNotYetValid {
                not_before: claims.not_before,
            });
        }

        if claims.not_after < now {
            findings.push(AuditFinding::CertificateExpired {
                not_after: claims.not_after,
            });
        }

        if claims.revocation_epoch < self.minimum_revocation_epoch {
            findings.push(AuditFinding::RevocationEpochStale {
                minimum: self.minimum_revocation_epoch,
                found: claims.revocation_epoch,
            });
        }

        let trusted_issuer = self
            .trusted_issuers
            .get(&envelope.certificate.body.signature.signer)
            .ok_or_else(|| {
                AuthError::UntrustedIssuer(envelope.certificate.body.signature.signer.clone())
            })?;
        if !verify_certificate_signature(&envelope.certificate, trusted_issuer)? {
            findings.push(AuditFinding::InvalidCertificateSignature);
        }

        let requested_scopes = envelope.requested_scopes.clone();
        for scope in &requested_scopes {
            if !claims.experiment_scopes.contains(scope) && !scope.allows_directory_discovery() {
                findings.push(AuditFinding::ScopeNotAuthorized(scope.clone()));
            }
        }

        if !verify_challenge_signature(envelope, claims)? {
            findings.push(AuditFinding::InvalidChallengeSignature);
        }

        let trust_level = if findings.is_empty() {
            if claims.granted_roles.roles.iter().any(|role| {
                matches!(
                    role,
                    burn_p2p_core::PeerRole::Authority | burn_p2p_core::PeerRole::Validator
                )
            }) {
                PeerTrustLevel::Privileged
            } else {
                PeerTrustLevel::PolicyCompliant
            }
        } else {
            PeerTrustLevel::TransportAuthenticated
        };

        Ok(PeerAdmissionReport {
            peer_id: claims.peer_id.clone(),
            principal_id: claims.principal_id.clone(),
            requested_scopes,
            trust_level,
            issuer_peer_id: envelope.certificate.body.signature.signer.clone(),
            findings,
            verified_at: now,
        })
    }
}

fn sign_envelope<T: serde::Serialize>(
    keypair: &Keypair,
    key_id: &str,
    protocol_version: Version,
    schema: &str,
    payload: &T,
) -> Result<SignatureMetadata, AuthError> {
    let envelope = SchemaEnvelope::new(schema, protocol_version, payload);
    let message = burn_p2p_core::deterministic_cbor(&envelope)?;
    let signature = keypair
        .sign(&message)
        .map_err(|error| AuthError::Signing(error.to_string()))?;

    Ok(SignatureMetadata {
        signer: PeerId::new(
            libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string(),
        ),
        key_id: key_id.to_owned(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: hex::encode(signature),
    })
}

/// Performs the create peer auth envelope operation.
pub fn create_peer_auth_envelope(
    node_keypair: &Keypair,
    certificate: NodeCertificate,
    client_manifest_id: Option<ContentId>,
    requested_scopes: BTreeSet<ExperimentScope>,
    nonce_hash: ContentId,
    presented_at: DateTime<Utc>,
) -> Result<PeerAuthEnvelope, AuthError> {
    let peer_id =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string());
    let message = auth_challenge_message(
        &peer_id,
        client_manifest_id.as_ref(),
        &requested_scopes,
        &nonce_hash,
    )?;
    let signature = node_keypair
        .sign(&message)
        .map_err(|error| AuthError::Signing(error.to_string()))?;

    Ok(PeerAuthEnvelope {
        peer_id,
        certificate,
        client_manifest_id,
        requested_scopes,
        nonce_hash,
        challenge_signature_hex: hex::encode(signature),
        presented_at,
    })
}

fn verify_certificate_signature(
    certificate: &NodeCertificate,
    trusted_issuer: &TrustedIssuer,
) -> Result<bool, AuthError> {
    let public_key = decode_public_key(&trusted_issuer.issuer_public_key_hex)?;
    let message = burn_p2p_core::deterministic_cbor(&certificate.body.payload)?;
    let signature = hex::decode(&certificate.body.signature.signature_hex)
        .map_err(|error| AuthError::SignatureDecode(error.to_string()))?;
    Ok(public_key.verify(&message, &signature))
}

fn verify_challenge_signature(
    envelope: &PeerAuthEnvelope,
    claims: &NodeCertificateClaims,
) -> Result<bool, AuthError> {
    let public_key = decode_public_key(&claims.peer_public_key_hex)?;
    let message = auth_challenge_message(
        &envelope.peer_id,
        envelope.client_manifest_id.as_ref(),
        &envelope.requested_scopes,
        &envelope.nonce_hash,
    )?;
    let signature = hex::decode(&envelope.challenge_signature_hex)
        .map_err(|error| AuthError::SignatureDecode(error.to_string()))?;
    Ok(public_key.verify(&message, &signature))
}

fn auth_challenge_message(
    peer_id: &PeerId,
    client_manifest_id: Option<&ContentId>,
    requested_scopes: &BTreeSet<ExperimentScope>,
    nonce_hash: &ContentId,
) -> Result<Vec<u8>, AuthError> {
    burn_p2p_core::deterministic_cbor(&(
        peer_id.as_str(),
        client_manifest_id.map(ContentId::as_str),
        requested_scopes,
        nonce_hash.as_str(),
    ))
    .map_err(AuthError::from)
}

fn decode_public_key(hex_bytes: &str) -> Result<PublicKey, AuthError> {
    let bytes =
        hex::decode(hex_bytes).map_err(|error| AuthError::PublicKeyDecode(error.to_string()))?;
    PublicKey::try_decode_protobuf(&bytes)
        .map_err(|error| AuthError::PublicKeyDecode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        ContentId, ExperimentScope, NetworkId, PeerId, PeerRole, PeerRoleSet, PrincipalId,
        ProjectFamilyId, RevocationEpoch,
    };
    use chrono::{Duration, Utc};
    use libp2p_identity::Keypair;
    use semver::Version;

    use super::{
        AdmissionPolicy, CallbackPayload, IdentityConnector, LoginRequest,
        NodeCertificateAuthority, NodeEnrollmentRequest, PrincipalClaims, PrincipalSession,
        StaticIdentityConnector, StaticPrincipalRecord, TrustedIssuer,
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
                        provider: burn_p2p_core::AuthProvider::Static {
                            authority: "lab-auth".into(),
                        },
                        display_name: "Alice".into(),
                        org_memberships: BTreeSet::from(["ml-lab".into()]),
                        group_memberships: BTreeSet::from(["trainers".into()]),
                        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                        granted_scopes: BTreeSet::from([
                            ExperimentScope::Connect,
                            ExperimentScope::Discover,
                            ExperimentScope::Train {
                                experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
                            },
                        ]),
                        custom_claims: BTreeMap::new(),
                        issued_at: now,
                        expires_at: now + Duration::hours(1),
                    },
                    allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
                },
            )]),
        )
    }

    #[test]
    fn static_identity_connector_issues_sessions() {
        let connector = static_connector();
        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
                }]),
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

        assert_eq!(session.claims.display_name, "Alice");
    }

    #[test]
    fn issued_certificate_and_peer_auth_verify_against_trusted_issuer() {
        let connector = static_connector();
        let login = connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("network-a"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
                    },
                ]),
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

        let authority_keypair = Keypair::generate_ed25519();
        let authority = NodeCertificateAuthority::new(
            NetworkId::new("network-a"),
            ProjectFamilyId::new("family-a"),
            ContentId::new("train-a"),
            Version::new(0, 1, 0),
            authority_keypair,
            "authority-1",
        )
        .expect("authority");

        let node_keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string(),
        );
        let certificate = authority
            .issue_certificate(NodeEnrollmentRequest {
                session,
                project_family_id: ProjectFamilyId::new("family-a"),
                release_train_hash: ContentId::new("train-a"),
                target_artifact_hash: ContentId::new("artifact-native-a"),
                peer_id: peer_id.clone(),
                peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
                    },
                ]),
                client_policy_hash: Some(ContentId::new("policy-a")),
                serial: 1,
                not_before: Utc::now() - Duration::seconds(5),
                not_after: Utc::now() + Duration::minutes(10),
                revocation_epoch: RevocationEpoch(4),
            })
            .expect("certificate");

        let envelope = authority
            .create_peer_auth_envelope(
                &node_keypair,
                certificate,
                Some(ContentId::new("manifest-a")),
                BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
                }]),
                ContentId::new("nonce-a"),
                Utc::now(),
            )
            .expect("auth envelope");

        let policy = AdmissionPolicy {
            network_id: NetworkId::new("network-a"),
            project_family_id: ProjectFamilyId::new("family-a"),
            required_release_train_hash: ContentId::new("train-a"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-a")]),
            trusted_issuers: BTreeMap::from([(
                authority.issuer_peer_id(),
                TrustedIssuer {
                    issuer_peer_id: authority.issuer_peer_id(),
                    issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
                },
            )]),
            minimum_revocation_epoch: RevocationEpoch(4),
        };

        let report = policy
            .verify_peer_auth(&envelope, &peer_id, Utc::now())
            .expect("verify");
        assert!(report.findings.is_empty());
    }

    #[test]
    fn peer_auth_rejects_scope_not_granted() {
        let authority_keypair = Keypair::generate_ed25519();
        let authority = NodeCertificateAuthority::new(
            NetworkId::new("network-a"),
            ProjectFamilyId::new("family-a"),
            ContentId::new("train-a"),
            Version::new(0, 1, 0),
            authority_keypair,
            "authority-1",
        )
        .expect("authority");
        let now = Utc::now();
        let session = PrincipalSession {
            session_id: ContentId::new("session-a"),
            network_id: NetworkId::new("network-a"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: burn_p2p_core::AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now + Duration::minutes(5),
            },
            issued_at: now,
            expires_at: now + Duration::minutes(5),
        };
        let node_keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string(),
        );
        let certificate = authority
            .issue_certificate(NodeEnrollmentRequest {
                session,
                project_family_id: ProjectFamilyId::new("family-a"),
                release_train_hash: ContentId::new("train-a"),
                target_artifact_hash: ContentId::new("artifact-native-a"),
                peer_id: peer_id.clone(),
                peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                client_policy_hash: None,
                serial: 7,
                not_before: now - Duration::seconds(5),
                not_after: now + Duration::minutes(5),
                revocation_epoch: RevocationEpoch(1),
            })
            .expect("cert");
        let envelope = authority
            .create_peer_auth_envelope(
                &node_keypair,
                certificate,
                None,
                BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p_core::ExperimentId::new("exp-b"),
                }]),
                ContentId::new("nonce-a"),
                now,
            )
            .expect("envelope");
        let policy = AdmissionPolicy {
            network_id: NetworkId::new("network-a"),
            project_family_id: ProjectFamilyId::new("family-a"),
            required_release_train_hash: ContentId::new("train-a"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-a")]),
            trusted_issuers: BTreeMap::from([(
                authority.issuer_peer_id(),
                TrustedIssuer {
                    issuer_peer_id: authority.issuer_peer_id(),
                    issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
                },
            )]),
            minimum_revocation_epoch: RevocationEpoch(1),
        };
        let report = policy
            .verify_peer_auth(&envelope, &peer_id, now)
            .expect("verify");
        assert!(report.findings.iter().any(|finding| matches!(
            finding,
            crate::AuditFinding::ScopeNotAuthorized(ExperimentScope::Train { .. })
        )));
    }
}
