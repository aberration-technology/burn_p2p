//! Security, admission, identity, and release-policy primitives for burn_p2p.
#![forbid(unsafe_code)]

/// Authentication and session helpers.
pub mod auth;
/// Robustness screening, bounded influence, and quarantine helpers.
pub mod robust;

pub use auth::{
    AdmissionPolicy, AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart,
    NodeCertificateAuthority, NodeEnrollmentRequest, PeerAdmissionReport, PeerTrustLevel,
    PrincipalClaims, PrincipalSession, StaticIdentityConnector, StaticPrincipalRecord,
    TrustedIssuer, create_peer_auth_envelope, random_login_state_token,
};
pub use robust::{
    CohortWeightDecision, FeatureLayer, RobustUpdateObservation, RobustnessEngine,
    TrustUpdateInput, aggregate_updates_with_policy, coordinate_median, extract_feature_sketch,
    filter_update_sketches_with_policy, multi_krum_indices, should_quarantine, trimmed_mean,
    updated_trust_score,
};

use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AssignmentLease, AttestationLevel, CapabilityCard, ClientPlatform, ContentId,
    ContributionReceipt, ControlCertId, DataReceipt, ExperimentScope, HeadId, MergeCertId,
    MetricValue, NetworkId, PeerId, ProjectFamilyId, RevocationEpoch, SignatureMetadata,
};
use chrono::{DateTime, Utc};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
/// Enumerates the supported security error values.
pub enum SecurityError {
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("auth error: {0}")]
    /// Uses the auth variant.
    Auth(#[from] auth::AuthError),
    #[error("release policy must include at least one allowed protocol requirement")]
    /// Uses the empty protocol requirements variant.
    EmptyProtocolRequirements,
    #[error("reputation thresholds must satisfy allow >= quarantine >= ban")]
    /// Uses the invalid reputation thresholds variant.
    InvalidReputationThresholds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the release.
pub struct ReleaseManifest {
    /// The release ID.
    pub release_id: ContentId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The version.
    pub version: Version,
    /// The protocol version.
    pub protocol_version: Version,
    /// The build hash.
    pub build_hash: ContentId,
    /// The project hash.
    pub project_hash: Option<ContentId>,
    /// The features.
    pub features: BTreeSet<String>,
    /// The published at.
    pub published_at: DateTime<Utc>,
}

impl ReleaseManifest {
    #[allow(clippy::too_many_arguments)]
    /// Creates a new value.
    pub fn new(
        project_family_id: ProjectFamilyId,
        release_train_hash: ContentId,
        target_artifact_hash: ContentId,
        version: Version,
        protocol_version: Version,
        build_hash: ContentId,
        project_hash: Option<ContentId>,
        features: BTreeSet<String>,
        published_at: DateTime<Utc>,
    ) -> Result<Self, SecurityError> {
        let release_id = ContentId::derive(&(
            project_family_id.as_str(),
            release_train_hash.as_str(),
            target_artifact_hash.as_str(),
            version.clone(),
            protocol_version.clone(),
            build_hash.as_str(),
            project_hash.as_ref().map(ContentId::as_str),
            &features,
        ))?;

        Ok(Self {
            release_id,
            project_family_id,
            release_train_hash,
            target_artifact_hash,
            version,
            protocol_version,
            build_hash,
            project_hash,
            features,
            published_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the client.
pub struct ClientManifest {
    /// The manifest ID.
    pub manifest_id: ContentId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The client version.
    pub client_version: Version,
    /// The protocol version.
    pub protocol_version: Version,
    /// The build hash.
    pub build_hash: ContentId,
    /// The project hash.
    pub project_hash: Option<ContentId>,
    /// The feature set.
    pub feature_set: BTreeSet<String>,
    /// The platform.
    pub platform: ClientPlatform,
    /// The binary hash.
    pub binary_hash: Option<ContentId>,
    /// The WASM hash.
    pub wasm_hash: Option<ContentId>,
    /// The declared at.
    pub declared_at: DateTime<Utc>,
}

impl ClientManifest {
    #[allow(clippy::too_many_arguments)]
    /// Creates a new value.
    pub fn new(
        peer_id: PeerId,
        project_family_id: ProjectFamilyId,
        release_train_hash: ContentId,
        target_artifact_hash: ContentId,
        target_artifact_id: impl Into<String>,
        client_version: Version,
        protocol_version: Version,
        build_hash: ContentId,
        project_hash: Option<ContentId>,
        feature_set: BTreeSet<String>,
        platform: ClientPlatform,
        binary_hash: Option<ContentId>,
        wasm_hash: Option<ContentId>,
        declared_at: DateTime<Utc>,
    ) -> Result<Self, SecurityError> {
        let target_artifact_id = target_artifact_id.into();
        let manifest_id = ContentId::derive(&(
            peer_id.as_str(),
            project_family_id.as_str(),
            release_train_hash.as_str(),
            target_artifact_hash.as_str(),
            target_artifact_id.as_str(),
            client_version.clone(),
            protocol_version.clone(),
            build_hash.as_str(),
            project_hash.as_ref().map(ContentId::as_str),
            &feature_set,
            &platform,
            binary_hash.as_ref().map(ContentId::as_str),
            wasm_hash.as_ref().map(ContentId::as_str),
        ))?;

        Ok(Self {
            manifest_id,
            peer_id,
            project_family_id,
            release_train_hash,
            target_artifact_hash,
            target_artifact_id,
            client_version,
            protocol_version,
            build_hash,
            project_hash,
            feature_set,
            platform,
            binary_hash,
            wasm_hash,
            declared_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a challenge response.
pub struct ChallengeResponse {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The nonce hash.
    pub nonce_hash: ContentId,
    /// The response hash.
    pub response_hash: ContentId,
    /// The answered at.
    pub answered_at: DateTime<Utc>,
    /// The signature.
    pub signature: SignatureMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the release policy.
pub struct ReleasePolicy {
    /// The required project family ID.
    pub required_project_family_id: Option<ProjectFamilyId>,
    /// The required release train hash.
    pub required_release_train_hash: Option<ContentId>,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The minimum client version.
    pub minimum_client_version: Version,
    /// The allowed protocol versions.
    pub allowed_protocol_versions: Vec<VersionReq>,
    /// The approved build hashes.
    pub approved_build_hashes: BTreeSet<ContentId>,
    /// The required project hash.
    pub required_project_hash: Option<ContentId>,
    /// The allowed features.
    pub allowed_features: Option<BTreeSet<String>>,
    /// The banned features.
    pub banned_features: BTreeSet<String>,
}

impl ReleasePolicy {
    /// Creates a new value.
    pub fn new(
        minimum_client_version: Version,
        allowed_protocol_versions: Vec<VersionReq>,
    ) -> Result<Self, SecurityError> {
        if allowed_protocol_versions.is_empty() {
            return Err(SecurityError::EmptyProtocolRequirements);
        }

        Ok(Self {
            required_project_family_id: None,
            required_release_train_hash: None,
            allowed_target_artifact_hashes: BTreeSet::new(),
            minimum_client_version,
            allowed_protocol_versions,
            approved_build_hashes: BTreeSet::new(),
            required_project_hash: None,
            allowed_features: None,
            banned_features: BTreeSet::new(),
        })
    }

    fn client_matches_policy(&self, manifest: &ClientManifest) -> bool {
        if let Some(required_project_family_id) = self.required_project_family_id.as_ref()
            && required_project_family_id != &manifest.project_family_id
        {
            return false;
        }

        if let Some(required_release_train_hash) = self.required_release_train_hash.as_ref()
            && required_release_train_hash != &manifest.release_train_hash
        {
            return false;
        }

        if !self.allowed_target_artifact_hashes.is_empty()
            && !self
                .allowed_target_artifact_hashes
                .contains(&manifest.target_artifact_hash)
        {
            return false;
        }

        if manifest.client_version < self.minimum_client_version {
            return false;
        }

        if !self
            .allowed_protocol_versions
            .iter()
            .any(|requirement| requirement.matches(&manifest.protocol_version))
        {
            return false;
        }

        if !self.approved_build_hashes.is_empty()
            && !self.approved_build_hashes.contains(&manifest.build_hash)
        {
            return false;
        }

        if let Some(required_project_hash) = self.required_project_hash.as_ref()
            && manifest.project_hash.as_ref() != Some(required_project_hash)
        {
            return false;
        }

        if let Some(allowed_features) = &self.allowed_features
            && manifest
                .feature_set
                .iter()
                .any(|feature| !allowed_features.contains(feature))
        {
            return false;
        }

        if manifest
            .feature_set
            .iter()
            .any(|feature| self.banned_features.contains(feature))
        {
            return false;
        }

        true
    }

    fn append_client_findings(&self, manifest: &ClientManifest, findings: &mut Vec<AuditFinding>) {
        if let Some(required_project_family_id) = self.required_project_family_id.as_ref()
            && required_project_family_id != &manifest.project_family_id
        {
            findings.push(AuditFinding::ProjectFamilyMismatch {
                expected: Some(required_project_family_id.clone()),
                found: manifest.project_family_id.clone(),
            });
        }

        if let Some(required_release_train_hash) = self.required_release_train_hash.as_ref()
            && required_release_train_hash != &manifest.release_train_hash
        {
            findings.push(AuditFinding::ReleaseTrainHashMismatch {
                expected: Some(required_release_train_hash.clone()),
                found: manifest.release_train_hash.clone(),
            });
        }

        if !self.allowed_target_artifact_hashes.is_empty()
            && !self
                .allowed_target_artifact_hashes
                .contains(&manifest.target_artifact_hash)
        {
            findings.push(AuditFinding::TargetArtifactHashMismatch {
                expected: Some(self.allowed_target_artifact_hashes.clone()),
                found: manifest.target_artifact_hash.clone(),
            });
        }

        if manifest.client_version < self.minimum_client_version {
            findings.push(AuditFinding::ClientVersionTooOld {
                minimum: self.minimum_client_version.clone(),
                found: manifest.client_version.clone(),
            });
        }

        if !self
            .allowed_protocol_versions
            .iter()
            .any(|requirement| requirement.matches(&manifest.protocol_version))
        {
            findings.push(AuditFinding::ProtocolVersionMismatch {
                found: manifest.protocol_version.clone(),
            });
        }

        if !self.approved_build_hashes.is_empty()
            && !self.approved_build_hashes.contains(&manifest.build_hash)
        {
            findings.push(AuditFinding::BuildHashNotApproved(
                manifest.build_hash.clone(),
            ));
        }

        if let Some(required_project_hash) = self.required_project_hash.as_ref()
            && manifest.project_hash.as_ref() != Some(required_project_hash)
        {
            findings.push(AuditFinding::ProjectHashMismatch {
                expected: Some(required_project_hash.clone()),
                found: manifest.project_hash.clone(),
            });
        }

        if let Some(allowed_features) = &self.allowed_features {
            for feature in &manifest.feature_set {
                if !allowed_features.contains(feature) {
                    findings.push(AuditFinding::UnsupportedFeature(feature.clone()));
                }
            }
        }

        for feature in &manifest.feature_set {
            if self.banned_features.contains(feature) {
                findings.push(AuditFinding::BannedFeature(feature.clone()));
            }
        }
    }

    /// Performs the evaluate client operation.
    pub fn evaluate_client(&self, manifest: &ClientManifest) -> AdmissionDecision {
        if self.client_matches_policy(manifest) {
            return AdmissionDecision::Allow;
        }

        let mut findings = Vec::new();
        self.append_client_findings(manifest, &mut findings);
        debug_assert!(!findings.is_empty());

        AdmissionDecision::Reject(findings)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge evidence requirement.
pub struct MergeEvidenceRequirement {
    /// The minimum attestation level.
    pub minimum_attestation_level: AttestationLevel,
    /// The minimum reputation score.
    pub minimum_reputation_score: f64,
    /// The require data receipt.
    pub require_data_receipt: bool,
    /// The require challenge response.
    pub require_challenge_response: bool,
    /// The require holdout metrics.
    pub require_holdout_metrics: bool,
}

impl Default for MergeEvidenceRequirement {
    fn default() -> Self {
        Self {
            minimum_attestation_level: AttestationLevel::Challenge,
            minimum_reputation_score: 0.0,
            require_data_receipt: true,
            require_challenge_response: true,
            require_holdout_metrics: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge evidence.
pub struct MergeEvidence {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The client manifest.
    pub client_manifest: ClientManifest,
    /// The capability card.
    pub capability_card: CapabilityCard,
    /// The challenge response.
    pub challenge_response: Option<ChallengeResponse>,
    /// The data receipt.
    pub data_receipt: Option<DataReceipt>,
    /// The holdout metrics.
    pub holdout_metrics: BTreeMap<String, MetricValue>,
    /// The reputation score.
    pub reputation_score: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures the validator policy.
pub struct ValidatorPolicy {
    /// The release policy.
    pub release_policy: ReleasePolicy,
    /// The evidence requirement.
    pub evidence_requirement: MergeEvidenceRequirement,
}

impl ValidatorPolicy {
    /// Performs the evaluate admission operation.
    pub fn evaluate_admission(&self, manifest: &ClientManifest) -> AdmissionDecision {
        self.release_policy.evaluate_client(manifest)
    }

    /// Performs the audit data receipt operation.
    pub fn audit_data_receipt(
        &self,
        lease: &AssignmentLease,
        receipt: &DataReceipt,
        audited_at: DateTime<Utc>,
    ) -> DataAuditReport {
        let mut findings = Vec::new();

        if receipt.lease_id != lease.lease_id {
            findings.push(AuditFinding::LeaseMismatch {
                expected: lease.lease_id.clone(),
                found: receipt.lease_id.clone(),
            });
        }

        if receipt.peer_id != lease.peer_id {
            findings.push(AuditFinding::PeerMismatch {
                expected: lease.peer_id.clone(),
                found: receipt.peer_id.clone(),
            });
        }

        if receipt.completed_at < lease.granted_at || receipt.completed_at > lease.expires_at {
            findings.push(AuditFinding::ReceiptOutsideLeaseWindow);
        }

        let leased = lease.microshards.iter().collect::<BTreeSet<_>>();
        let extra = receipt
            .microshards
            .iter()
            .filter(|microshard_id| !leased.contains(microshard_id))
            .cloned()
            .collect::<Vec<_>>();
        if !extra.is_empty() {
            findings.push(AuditFinding::UnexpectedMicroshards(extra));
        }

        DataAuditReport {
            peer_id: receipt.peer_id.clone(),
            lease_id: lease.lease_id.clone(),
            findings,
            audited_at,
        }
    }

    /// Performs the audit update receipt operation.
    pub fn audit_update_receipt(
        &self,
        receipt: &ContributionReceipt,
        audited_at: DateTime<Utc>,
    ) -> UpdateAuditReport {
        let mut findings = Vec::new();

        if receipt.accepted_weight <= 0.0 {
            findings.push(AuditFinding::NonPositiveAcceptedWeight);
        }

        for (metric_name, metric_value) in &receipt.metrics {
            if matches!(metric_value, MetricValue::Float(value) if !value.is_finite()) {
                findings.push(AuditFinding::NonFiniteMetric(metric_name.clone()));
            }
        }

        UpdateAuditReport {
            peer_id: receipt.peer_id.clone(),
            contribution_receipt_id: receipt.receipt_id.clone(),
            artifact_id: receipt.artifact_id.clone(),
            findings,
            audited_at,
        }
    }

    /// Performs the evaluate merge evidence operation.
    pub fn evaluate_merge_evidence(
        &self,
        receipt: &ContributionReceipt,
        evidence: &MergeEvidence,
    ) -> MergeEvidenceDecision {
        let mut findings = Vec::new();

        if evidence.peer_id != receipt.peer_id {
            findings.push(AuditFinding::PeerMismatch {
                expected: receipt.peer_id.clone(),
                found: evidence.peer_id.clone(),
            });
        }

        if !self
            .release_policy
            .client_matches_policy(&evidence.client_manifest)
        {
            self.release_policy
                .append_client_findings(&evidence.client_manifest, &mut findings);
        }

        if evidence.capability_card.peer_id != receipt.peer_id {
            findings.push(AuditFinding::PeerMismatch {
                expected: receipt.peer_id.clone(),
                found: evidence.capability_card.peer_id.clone(),
            });
        }

        if evidence.capability_card.attestation_level
            < self.evidence_requirement.minimum_attestation_level
        {
            findings.push(AuditFinding::InsufficientAttestation {
                required: self.evidence_requirement.minimum_attestation_level.clone(),
                found: evidence.capability_card.attestation_level.clone(),
            });
        }

        if self.evidence_requirement.require_challenge_response
            && evidence.challenge_response.is_none()
        {
            findings.push(AuditFinding::MissingChallengeResponse);
        }

        if self.evidence_requirement.require_data_receipt && evidence.data_receipt.is_none() {
            findings.push(AuditFinding::MissingDataReceipt);
        }

        if self.evidence_requirement.require_holdout_metrics && evidence.holdout_metrics.is_empty()
        {
            findings.push(AuditFinding::MissingHoldoutMetrics);
        }

        if evidence.reputation_score < self.evidence_requirement.minimum_reputation_score {
            findings.push(AuditFinding::ReputationTooLow {
                minimum: self.evidence_requirement.minimum_reputation_score,
                found: evidence.reputation_score,
            });
        }

        if findings.is_empty() {
            MergeEvidenceDecision::Eligible
        } else {
            MergeEvidenceDecision::Insufficient(findings)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures the reputation policy.
pub struct ReputationPolicy {
    /// The allow threshold.
    pub allow_threshold: f64,
    /// The quarantine threshold.
    pub quarantine_threshold: f64,
    /// The ban threshold.
    pub ban_threshold: f64,
    /// The accepted work reward.
    pub accepted_work_reward: f64,
    /// The warning penalty.
    pub warning_penalty: f64,
    /// The failure penalty.
    pub failure_penalty: f64,
    /// The decay factor.
    pub decay_factor: f64,
}

impl Default for ReputationPolicy {
    fn default() -> Self {
        Self {
            allow_threshold: 0.0,
            quarantine_threshold: -20.0,
            ban_threshold: -80.0,
            accepted_work_reward: 0.25,
            warning_penalty: 4.0,
            failure_penalty: 12.0,
            decay_factor: 0.98,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures reputation state.
pub struct ReputationState {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The score.
    pub score: f64,
    /// The accepted work.
    pub accepted_work: u64,
    /// The warning count.
    pub warning_count: u64,
    /// The failure count.
    pub failure_count: u64,
    /// The last control cert ID.
    pub last_control_cert_id: Option<ControlCertId>,
    /// The last merge cert ID.
    pub last_merge_cert_id: Option<MergeCertId>,
    /// The last updated at.
    pub last_updated_at: DateTime<Utc>,
}

impl ReputationState {
    /// Creates a new value.
    pub fn new(peer_id: PeerId, now: DateTime<Utc>) -> Self {
        Self {
            peer_id,
            score: 0.0,
            accepted_work: 0,
            warning_count: 0,
            failure_count: 0,
            last_control_cert_id: None,
            last_merge_cert_id: None,
            last_updated_at: now,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported reputation decision values.
pub enum ReputationDecision {
    /// Uses the allow variant.
    Allow,
    /// Uses the quarantine variant.
    Quarantine,
    /// Uses the ban variant.
    Ban,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reputation observation.
pub struct ReputationObservation {
    /// The accepted work units.
    pub accepted_work_units: u64,
    /// The warning count.
    pub warning_count: u32,
    /// The failure count.
    pub failure_count: u32,
    /// The last control cert ID.
    pub last_control_cert_id: Option<ControlCertId>,
    /// The last merge cert ID.
    pub last_merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Represents a reputation engine.
pub struct ReputationEngine {
    /// The policy.
    pub policy: ReputationPolicy,
}

impl ReputationEngine {
    /// Creates a new value.
    pub fn new(policy: ReputationPolicy) -> Result<Self, SecurityError> {
        if policy.allow_threshold < policy.quarantine_threshold
            || policy.quarantine_threshold < policy.ban_threshold
        {
            return Err(SecurityError::InvalidReputationThresholds);
        }

        Ok(Self { policy })
    }

    /// Performs the apply operation.
    pub fn apply(
        &self,
        state: &mut ReputationState,
        observation: ReputationObservation,
        now: DateTime<Utc>,
    ) -> ReputationDecision {
        state.score *= self.policy.decay_factor;
        state.score += observation.accepted_work_units as f64 * self.policy.accepted_work_reward;
        state.score -= f64::from(observation.warning_count) * self.policy.warning_penalty;
        state.score -= f64::from(observation.failure_count) * self.policy.failure_penalty;

        state.accepted_work += observation.accepted_work_units;
        state.warning_count += u64::from(observation.warning_count);
        state.failure_count += u64::from(observation.failure_count);
        state.last_control_cert_id = observation.last_control_cert_id;
        state.last_merge_cert_id = observation.last_merge_cert_id;
        state.last_updated_at = now;

        self.decision(state.score)
    }

    /// Performs the decision operation.
    pub fn decision(&self, score: f64) -> ReputationDecision {
        if score <= self.policy.ban_threshold {
            ReputationDecision::Ban
        } else if score < self.policy.allow_threshold {
            ReputationDecision::Quarantine
        } else {
            ReputationDecision::Allow
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported audit finding values.
pub enum AuditFinding {
    /// Uses the client version too old variant.
    ClientVersionTooOld {
        /// The minimum.
        minimum: Version,
        /// The found.
        found: Version,
    },
    /// Uses the protocol version mismatch variant.
    ProtocolVersionMismatch {
        /// The found.
        found: Version,
    },
    /// Uses the build hash not approved variant.
    BuildHashNotApproved(ContentId),
    /// Uses the project hash mismatch variant.
    ProjectHashMismatch {
        /// The expected.
        expected: Option<ContentId>,
        /// The found.
        found: Option<ContentId>,
    },
    /// Uses the project family mismatch variant.
    ProjectFamilyMismatch {
        /// The expected.
        expected: Option<ProjectFamilyId>,
        /// The found.
        found: ProjectFamilyId,
    },
    /// Uses the release train hash mismatch variant.
    ReleaseTrainHashMismatch {
        /// The expected.
        expected: Option<ContentId>,
        /// The found.
        found: ContentId,
    },
    /// Uses the target artifact hash mismatch variant.
    TargetArtifactHashMismatch {
        /// The expected.
        expected: Option<BTreeSet<ContentId>>,
        /// The found.
        found: ContentId,
    },
    /// Uses the unsupported feature variant.
    UnsupportedFeature(String),
    /// Uses the banned feature variant.
    BannedFeature(String),
    /// Uses the peer mismatch variant.
    PeerMismatch {
        /// The expected.
        expected: PeerId,
        /// The found.
        found: PeerId,
    },
    /// Uses the transport peer mismatch variant.
    TransportPeerMismatch {
        /// The expected.
        expected: PeerId,
        /// The found.
        found: PeerId,
    },
    /// Uses the certificate peer mismatch variant.
    CertificatePeerMismatch {
        /// The expected.
        expected: PeerId,
        /// The found.
        found: PeerId,
    },
    /// Uses the network mismatch variant.
    NetworkMismatch {
        /// The expected.
        expected: NetworkId,
        /// The found.
        found: NetworkId,
    },
    /// Uses the certificate not yet valid variant.
    CertificateNotYetValid {
        /// The not before.
        not_before: DateTime<Utc>,
    },
    /// Uses the certificate expired variant.
    CertificateExpired {
        /// The not after.
        not_after: DateTime<Utc>,
    },
    /// Uses the revocation epoch stale variant.
    RevocationEpochStale {
        /// The minimum.
        minimum: RevocationEpoch,
        /// The found.
        found: RevocationEpoch,
    },
    /// Uses the scope not authorized variant.
    ScopeNotAuthorized(ExperimentScope),
    /// Uses the invalid certificate signature variant.
    InvalidCertificateSignature,
    /// Uses the invalid challenge signature variant.
    InvalidChallengeSignature,
    /// Uses the lease mismatch variant.
    LeaseMismatch {
        /// The expected.
        expected: burn_p2p_core::LeaseId,
        /// The found.
        found: burn_p2p_core::LeaseId,
    },
    /// Uses the base head mismatch variant.
    BaseHeadMismatch {
        /// The expected.
        expected: HeadId,
        /// The found.
        found: HeadId,
    },
    /// Uses the receipt outside lease window variant.
    ReceiptOutsideLeaseWindow,
    /// Uses the unexpected microshards variant.
    UnexpectedMicroshards(Vec<burn_p2p_core::MicroShardId>),
    /// Uses the non positive accepted weight variant.
    NonPositiveAcceptedWeight,
    /// Uses the non finite metric variant.
    NonFiniteMetric(String),
    /// Uses the missing data receipt variant.
    MissingDataReceipt,
    /// Uses the missing challenge response variant.
    MissingChallengeResponse,
    /// Uses the missing holdout metrics variant.
    MissingHoldoutMetrics,
    /// Uses the insufficient attestation variant.
    InsufficientAttestation {
        /// The required.
        required: AttestationLevel,
        /// The found.
        found: AttestationLevel,
    },
    /// Uses the reputation too low variant.
    ReputationTooLow {
        /// The minimum.
        minimum: f64,
        /// The found.
        found: f64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported admission decision values.
pub enum AdmissionDecision {
    /// Uses the allow variant.
    Allow,
    /// Uses the quarantine variant.
    Quarantine(Vec<AuditFinding>),
    /// Uses the reject variant.
    Reject(Vec<AuditFinding>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported merge evidence decision values.
pub enum MergeEvidenceDecision {
    /// Uses the eligible variant.
    Eligible,
    /// Uses the insufficient variant.
    Insufficient(Vec<AuditFinding>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Reports data audit details.
pub struct DataAuditReport {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The lease ID.
    pub lease_id: burn_p2p_core::LeaseId,
    /// The findings.
    pub findings: Vec<AuditFinding>,
    /// The audited at.
    pub audited_at: DateTime<Utc>,
}

impl DataAuditReport {
    /// Performs the passed operation.
    pub fn passed(&self) -> bool {
        self.findings.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Reports update audit details.
pub struct UpdateAuditReport {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The contribution receipt ID.
    pub contribution_receipt_id: burn_p2p_core::ContributionReceiptId,
    /// The artifact ID.
    pub artifact_id: burn_p2p_core::ArtifactId,
    /// The findings.
    pub findings: Vec<AuditFinding>,
    /// The audited at.
    pub audited_at: DateTime<Utc>,
}

impl UpdateAuditReport {
    /// Performs the passed operation.
    pub fn passed(&self) -> bool {
        self.findings.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        CapabilityCard, CapabilityCardId, CapabilityClass, ClientPlatform, ContentId,
        ContributionReceipt, DataReceipt, MetricValue, PersistenceClass, ProjectFamilyId,
        SignatureAlgorithm, SignatureMetadata,
    };
    use chrono::Utc;
    use semver::{Version, VersionReq};

    use crate::{
        AdmissionDecision, AuditFinding, ChallengeResponse, ClientManifest, MergeEvidence,
        MergeEvidenceDecision, MergeEvidenceRequirement, ReleasePolicy, ReputationEngine,
        ReputationObservation, ReputationState, ValidatorPolicy,
    };

    fn client_manifest() -> ClientManifest {
        ClientManifest::new(
            burn_p2p_core::PeerId::new("peer-1"),
            ProjectFamilyId::new("family-1"),
            ContentId::new("train-1"),
            ContentId::new("artifact-native-1"),
            "native-linux-x86_64",
            Version::new(0, 4, 0),
            Version::new(0, 1, 2),
            ContentId::new("build-1"),
            Some(ContentId::new("project-1")),
            BTreeSet::from(["gpu".into()]),
            ClientPlatform::Native,
            Some(ContentId::new("bin-1")),
            None,
            Utc::now(),
        )
        .expect("manifest")
    }

    fn capability_card() -> CapabilityCard {
        CapabilityCard {
            card_id: CapabilityCardId::new("card-1"),
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            platform: ClientPlatform::Native,
            roles: burn_p2p_core::PeerRoleSet::new([burn_p2p_core::PeerRole::TrainerGpu]),
            preferred_backends: vec!["cuda".into()],
            browser_capabilities: BTreeSet::new(),
            recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
            device_memory_bytes: Some(24),
            system_memory_bytes: 64,
            disk_bytes: 1000,
            upload_mbps: 100.0,
            download_mbps: 100.0,
            persistence: PersistenceClass::Durable,
            work_units_per_second: 1000.0,
            attestation_level: burn_p2p_core::AttestationLevel::Strong,
            benchmark_hash: None,
            reported_at: Utc::now(),
        }
    }

    fn validator_policy() -> ValidatorPolicy {
        let mut release_policy = ReleasePolicy::new(
            Version::new(0, 3, 0),
            vec![VersionReq::parse("^0.1").expect("req")],
        )
        .expect("policy");
        release_policy.required_project_family_id = Some(ProjectFamilyId::new("family-1"));
        release_policy.required_release_train_hash = Some(ContentId::new("train-1"));
        release_policy
            .allowed_target_artifact_hashes
            .insert(ContentId::new("artifact-native-1"));
        release_policy
            .approved_build_hashes
            .insert(ContentId::new("build-1"));
        release_policy.required_project_hash = Some(ContentId::new("project-1"));

        ValidatorPolicy {
            release_policy,
            evidence_requirement: MergeEvidenceRequirement::default(),
        }
    }

    #[test]
    fn release_policy_rejects_old_or_unapproved_clients() {
        let mut policy = validator_policy().release_policy;
        let mut manifest = client_manifest();
        manifest.client_version = Version::new(0, 2, 0);

        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));

        manifest.client_version = Version::new(0, 4, 0);
        manifest.build_hash = ContentId::new("unknown-build");
        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));

        manifest.build_hash = ContentId::new("build-1");
        manifest.release_train_hash = ContentId::new("wrong-train");
        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));

        manifest.release_train_hash = ContentId::new("train-1");
        manifest.target_artifact_hash = ContentId::new("artifact-browser-1");
        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));

        manifest.target_artifact_hash = ContentId::new("artifact-native-1");

        policy.banned_features.insert("gpu".into());
        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));
    }

    #[test]
    fn data_audit_detects_out_of_lease_work() {
        let policy = validator_policy();
        let lease = burn_p2p_core::AssignmentLease {
            lease_id: burn_p2p_core::LeaseId::new("lease-1"),
            network_id: burn_p2p_core::NetworkId::new("net-1"),
            study_id: burn_p2p_core::StudyId::new("study-1"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp-1"),
            revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            window_id: burn_p2p_core::WindowId(1),
            granted_at: Utc::now(),
            expires_at: Utc::now(),
            budget_work_units: 10,
            microshards: vec![burn_p2p_core::MicroShardId::new("shard-1")],
            assignment_hash: ContentId::new("assignment-1"),
        };
        let receipt = DataReceipt {
            receipt_id: ContentId::new("receipt-1"),
            lease_id: burn_p2p_core::LeaseId::new("lease-1"),
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            completed_at: Utc::now(),
            microshards: vec![
                burn_p2p_core::MicroShardId::new("shard-1"),
                burn_p2p_core::MicroShardId::new("shard-x"),
            ],
            examples_processed: 10,
            tokens_processed: 100,
            disposition: burn_p2p_core::WorkDisposition::Accepted,
        };

        let report = policy.audit_data_receipt(&lease, &receipt, Utc::now());
        assert!(!report.passed());
        assert!(
            report
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::UnexpectedMicroshards(_)))
        );
    }

    #[test]
    fn update_audit_rejects_non_finite_metrics() {
        let policy = validator_policy();
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-1"),
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            study_id: burn_p2p_core::StudyId::new("study-1"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp-1"),
            revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            base_head_id: burn_p2p_core::HeadId::new("head-0"),
            artifact_id: burn_p2p_core::ArtifactId::new("artifact-1"),
            accepted_at: Utc::now(),
            accepted_weight: 1.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(f64::NAN))]),
            merge_cert_id: None,
        };

        let report = policy.audit_update_receipt(&receipt, Utc::now());
        assert!(!report.passed());
        assert!(report.findings.iter().any(
            |finding| matches!(finding, AuditFinding::NonFiniteMetric(name) if name == "loss")
        ));
    }

    #[test]
    fn merge_evidence_requires_receipt_challenge_and_reputation() {
        let policy = validator_policy();
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-1"),
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            study_id: burn_p2p_core::StudyId::new("study-1"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp-1"),
            revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            base_head_id: burn_p2p_core::HeadId::new("head-0"),
            artifact_id: burn_p2p_core::ArtifactId::new("artifact-1"),
            accepted_at: Utc::now(),
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };

        let evidence = MergeEvidence {
            peer_id: burn_p2p_core::PeerId::new("peer-1"),
            client_manifest: client_manifest(),
            capability_card: capability_card(),
            challenge_response: Some(ChallengeResponse {
                peer_id: burn_p2p_core::PeerId::new("peer-1"),
                nonce_hash: ContentId::new("nonce-1"),
                response_hash: ContentId::new("response-1"),
                answered_at: Utc::now(),
                signature: SignatureMetadata {
                    signer: burn_p2p_core::PeerId::new("peer-1"),
                    key_id: "key-1".into(),
                    algorithm: SignatureAlgorithm::Ed25519,
                    signed_at: Utc::now(),
                    signature_hex: "deadbeef".into(),
                },
            }),
            data_receipt: None,
            holdout_metrics: BTreeMap::new(),
            reputation_score: -1.0,
        };

        let decision = policy.evaluate_merge_evidence(&receipt, &evidence);
        assert!(matches!(decision, MergeEvidenceDecision::Insufficient(_)));
    }

    #[test]
    fn reputation_engine_moves_peer_between_states() {
        let engine = ReputationEngine::default();
        let now = Utc::now();
        let mut state = ReputationState::new(burn_p2p_core::PeerId::new("peer-1"), now);

        let decision = engine.apply(
            &mut state,
            ReputationObservation {
                accepted_work_units: 100,
                warning_count: 0,
                failure_count: 0,
                last_control_cert_id: None,
                last_merge_cert_id: None,
            },
            now,
        );
        assert_eq!(decision, crate::ReputationDecision::Allow);

        let decision = engine.apply(
            &mut state,
            ReputationObservation {
                accepted_work_units: 0,
                warning_count: 0,
                failure_count: 10,
                last_control_cert_id: None,
                last_merge_cert_id: None,
            },
            now,
        );
        assert!(matches!(
            decision,
            crate::ReputationDecision::Quarantine | crate::ReputationDecision::Ban
        ));
    }
}
