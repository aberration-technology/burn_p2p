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
pub enum AuthError {
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("failed to sign payload: {0}")]
    Signing(String),
    #[error("failed to decode public key: {0}")]
    PublicKeyDecode(String),
    #[error("failed to decode signature hex: {0}")]
    SignatureDecode(String),
    #[error("unknown login: {0}")]
    UnknownLogin(ContentId),
    #[error("state mismatch")]
    StateMismatch,
    #[error("login expired: {0}")]
    LoginExpired(ContentId),
    #[error("session expired: {0}")]
    SessionExpired(ContentId),
    #[error("unknown principal: {0}")]
    UnknownPrincipal(PrincipalId),
    #[error("untrusted issuer: {0}")]
    UntrustedIssuer(PeerId),
    #[error("network not granted: {0}")]
    NetworkNotGranted(NetworkId),
    #[error("project family mismatch: expected {expected}, found {found}")]
    ProjectFamilyMismatch {
        expected: ProjectFamilyId,
        found: ProjectFamilyId,
    },
    #[error("client release hash mismatch: expected {expected}, found {found}")]
    ClientReleaseHashMismatch {
        expected: ContentId,
        found: ContentId,
    },
    #[error("scope not granted: {0:?}")]
    ScopeNotGranted(ExperimentScope),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginRequest {
    pub network_id: NetworkId,
    pub principal_hint: Option<String>,
    pub requested_scopes: BTreeSet<ExperimentScope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginStart {
    pub login_id: ContentId,
    pub provider: AuthProvider,
    pub state: String,
    pub authorize_url: Option<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackPayload {
    pub login_id: ContentId,
    pub state: String,
    pub principal_id: PrincipalId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalClaims {
    pub principal_id: PrincipalId,
    pub provider: AuthProvider,
    pub display_name: String,
    pub org_memberships: BTreeSet<String>,
    pub group_memberships: BTreeSet<String>,
    pub granted_roles: PeerRoleSet,
    pub granted_scopes: BTreeSet<ExperimentScope>,
    pub custom_claims: BTreeMap<String, String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalSession {
    pub session_id: ContentId,
    pub network_id: NetworkId,
    pub claims: PrincipalClaims,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub trait IdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<LoginStart, AuthError>;
    fn complete_login(&self, callback: CallbackPayload) -> Result<PrincipalSession, AuthError>;
    fn refresh(&self, session: &PrincipalSession) -> Result<PrincipalSession, AuthError>;
    fn fetch_claims(&self, session: &PrincipalSession) -> Result<PrincipalClaims, AuthError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEnrollmentRequest {
    pub session: PrincipalSession,
    pub project_family_id: ProjectFamilyId,
    pub client_release_hash: ContentId,
    pub peer_id: PeerId,
    pub peer_public_key_hex: String,
    pub granted_roles: PeerRoleSet,
    pub requested_scopes: BTreeSet<ExperimentScope>,
    pub client_policy_hash: Option<ContentId>,
    pub serial: u64,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug)]
pub struct NodeCertificateAuthority {
    network_id: NetworkId,
    project_family_id: ProjectFamilyId,
    required_client_release_hash: ContentId,
    protocol_version: Version,
    issuer_keypair: Keypair,
    issuer_key_id: String,
    issuer_public_key_hex: String,
}

impl NodeCertificateAuthority {
    pub fn new(
        network_id: NetworkId,
        project_family_id: ProjectFamilyId,
        required_client_release_hash: ContentId,
        protocol_version: Version,
        issuer_keypair: Keypair,
        issuer_key_id: impl Into<String>,
    ) -> Result<Self, AuthError> {
        let issuer_public_key_hex = hex::encode(issuer_keypair.public().encode_protobuf());
        Ok(Self {
            network_id,
            project_family_id,
            required_client_release_hash,
            protocol_version,
            issuer_keypair,
            issuer_key_id: issuer_key_id.into(),
            issuer_public_key_hex,
        })
    }

    pub fn issuer_peer_id(&self) -> PeerId {
        PeerId::new(
            libp2p_identity::PeerId::from_public_key(&self.issuer_keypair.public()).to_string(),
        )
    }

    pub fn issuer_public_key_hex(&self) -> &str {
        &self.issuer_public_key_hex
    }

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

        if request.client_release_hash != self.required_client_release_hash {
            return Err(AuthError::ClientReleaseHashMismatch {
                expected: self.required_client_release_hash.clone(),
                found: request.client_release_hash,
            });
        }

        let claims = NodeCertificateClaims {
            network_id: self.network_id.clone(),
            project_family_id: request.project_family_id,
            client_release_hash: request.client_release_hash,
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
pub struct TrustedIssuer {
    pub issuer_peer_id: PeerId,
    pub issuer_public_key_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionPolicy {
    pub network_id: NetworkId,
    pub project_family_id: ProjectFamilyId,
    pub required_client_release_hash: ContentId,
    pub trusted_issuers: BTreeMap<PeerId, TrustedIssuer>,
    pub minimum_revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerTrustLevel {
    TransportAuthenticated,
    NetworkAuthenticated,
    ScopeAuthorized,
    PolicyCompliant,
    Reputable,
    Privileged,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerAdmissionReport {
    pub peer_id: PeerId,
    pub principal_id: PrincipalId,
    pub requested_scopes: BTreeSet<ExperimentScope>,
    pub trust_level: PeerTrustLevel,
    pub issuer_peer_id: PeerId,
    pub findings: Vec<AuditFinding>,
    pub verified_at: DateTime<Utc>,
}

impl PeerAdmissionReport {
    pub fn decision(&self) -> AdmissionDecision {
        if self.findings.is_empty() {
            AdmissionDecision::Allow
        } else {
            AdmissionDecision::Reject(self.findings.clone())
        }
    }
}

impl AdmissionPolicy {
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

        if claims.client_release_hash != self.required_client_release_hash {
            findings.push(AuditFinding::ClientReleaseHashMismatch {
                expected: Some(self.required_client_release_hash.clone()),
                found: claims.client_release_hash.clone(),
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
                principal_id: PrincipalId::new("alice"),
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
                principal_id: PrincipalId::new("alice"),
            })
            .expect("session");

        let authority_keypair = Keypair::generate_ed25519();
        let authority = NodeCertificateAuthority::new(
            NetworkId::new("network-a"),
            ProjectFamilyId::new("family-a"),
            ContentId::new("release-a"),
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
                client_release_hash: ContentId::new("release-a"),
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
            required_client_release_hash: ContentId::new("release-a"),
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
            ContentId::new("release-a"),
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
                client_release_hash: ContentId::new("release-a"),
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
            required_client_release_hash: ContentId::new("release-a"),
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
