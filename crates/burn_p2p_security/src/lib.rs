#![forbid(unsafe_code)]

pub mod auth;

pub use auth::{
    AdmissionPolicy, AuthError, CallbackPayload, IdentityConnector, LoginRequest, LoginStart,
    NodeCertificateAuthority, NodeEnrollmentRequest, PeerAdmissionReport, PeerTrustLevel,
    PrincipalClaims, PrincipalSession, StaticIdentityConnector, StaticPrincipalRecord,
    TrustedIssuer, create_peer_auth_envelope,
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
pub enum SecurityError {
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),
    #[error("release policy must include at least one allowed protocol requirement")]
    EmptyProtocolRequirements,
    #[error("reputation thresholds must satisfy allow >= quarantine >= ban")]
    InvalidReputationThresholds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseManifest {
    pub release_id: ContentId,
    pub project_family_id: ProjectFamilyId,
    pub client_release_hash: ContentId,
    pub version: Version,
    pub protocol_version: Version,
    pub build_hash: ContentId,
    pub project_hash: Option<ContentId>,
    pub features: BTreeSet<String>,
    pub published_at: DateTime<Utc>,
}

impl ReleaseManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        project_family_id: ProjectFamilyId,
        client_release_hash: ContentId,
        version: Version,
        protocol_version: Version,
        build_hash: ContentId,
        project_hash: Option<ContentId>,
        features: BTreeSet<String>,
        published_at: DateTime<Utc>,
    ) -> Result<Self, SecurityError> {
        let release_id = ContentId::derive(&(
            project_family_id.as_str(),
            client_release_hash.as_str(),
            version.clone(),
            protocol_version.clone(),
            build_hash.as_str(),
            project_hash.as_ref().map(ContentId::as_str),
            &features,
        ))?;

        Ok(Self {
            release_id,
            project_family_id,
            client_release_hash,
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
pub struct ClientManifest {
    pub manifest_id: ContentId,
    pub peer_id: PeerId,
    pub project_family_id: ProjectFamilyId,
    pub client_release_hash: ContentId,
    pub client_version: Version,
    pub protocol_version: Version,
    pub build_hash: ContentId,
    pub project_hash: Option<ContentId>,
    pub feature_set: BTreeSet<String>,
    pub platform: ClientPlatform,
    pub binary_hash: Option<ContentId>,
    pub wasm_hash: Option<ContentId>,
    pub declared_at: DateTime<Utc>,
}

impl ClientManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        peer_id: PeerId,
        project_family_id: ProjectFamilyId,
        client_release_hash: ContentId,
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
        let manifest_id = ContentId::derive(&(
            peer_id.as_str(),
            project_family_id.as_str(),
            client_release_hash.as_str(),
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
            client_release_hash,
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
pub struct ChallengeResponse {
    pub peer_id: PeerId,
    pub nonce_hash: ContentId,
    pub response_hash: ContentId,
    pub answered_at: DateTime<Utc>,
    pub signature: SignatureMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePolicy {
    pub required_project_family_id: Option<ProjectFamilyId>,
    pub required_client_release_hash: Option<ContentId>,
    pub minimum_client_version: Version,
    pub allowed_protocol_versions: Vec<VersionReq>,
    pub approved_build_hashes: BTreeSet<ContentId>,
    pub required_project_hash: Option<ContentId>,
    pub allowed_features: Option<BTreeSet<String>>,
    pub banned_features: BTreeSet<String>,
}

impl ReleasePolicy {
    pub fn new(
        minimum_client_version: Version,
        allowed_protocol_versions: Vec<VersionReq>,
    ) -> Result<Self, SecurityError> {
        if allowed_protocol_versions.is_empty() {
            return Err(SecurityError::EmptyProtocolRequirements);
        }

        Ok(Self {
            required_project_family_id: None,
            required_client_release_hash: None,
            minimum_client_version,
            allowed_protocol_versions,
            approved_build_hashes: BTreeSet::new(),
            required_project_hash: None,
            allowed_features: None,
            banned_features: BTreeSet::new(),
        })
    }

    pub fn evaluate_client(&self, manifest: &ClientManifest) -> AdmissionDecision {
        let mut findings = Vec::new();

        if self.required_project_family_id.as_ref() != Some(&manifest.project_family_id)
            && self.required_project_family_id.is_some()
        {
            findings.push(AuditFinding::ProjectFamilyMismatch {
                expected: self.required_project_family_id.clone(),
                found: manifest.project_family_id.clone(),
            });
        }

        if self.required_client_release_hash.as_ref() != Some(&manifest.client_release_hash)
            && self.required_client_release_hash.is_some()
        {
            findings.push(AuditFinding::ClientReleaseHashMismatch {
                expected: self.required_client_release_hash.clone(),
                found: manifest.client_release_hash.clone(),
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

        if self.required_project_hash.is_some()
            && manifest.project_hash != self.required_project_hash
        {
            findings.push(AuditFinding::ProjectHashMismatch {
                expected: self.required_project_hash.clone(),
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

        if findings.is_empty() {
            AdmissionDecision::Allow
        } else {
            AdmissionDecision::Reject(findings)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeEvidenceRequirement {
    pub minimum_attestation_level: AttestationLevel,
    pub minimum_reputation_score: f64,
    pub require_data_receipt: bool,
    pub require_challenge_response: bool,
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
pub struct MergeEvidence {
    pub peer_id: PeerId,
    pub client_manifest: ClientManifest,
    pub capability_card: CapabilityCard,
    pub challenge_response: Option<ChallengeResponse>,
    pub data_receipt: Option<DataReceipt>,
    pub holdout_metrics: BTreeMap<String, MetricValue>,
    pub reputation_score: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidatorPolicy {
    pub release_policy: ReleasePolicy,
    pub evidence_requirement: MergeEvidenceRequirement,
}

impl ValidatorPolicy {
    pub fn evaluate_admission(&self, manifest: &ClientManifest) -> AdmissionDecision {
        self.release_policy.evaluate_client(manifest)
    }

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

        match self
            .release_policy
            .evaluate_client(&evidence.client_manifest)
        {
            AdmissionDecision::Allow => {}
            AdmissionDecision::Quarantine(mut reasons) | AdmissionDecision::Reject(mut reasons) => {
                findings.append(&mut reasons);
            }
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
pub struct ReputationPolicy {
    pub allow_threshold: f64,
    pub quarantine_threshold: f64,
    pub ban_threshold: f64,
    pub accepted_work_reward: f64,
    pub warning_penalty: f64,
    pub failure_penalty: f64,
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
pub struct ReputationState {
    pub peer_id: PeerId,
    pub score: f64,
    pub accepted_work: u64,
    pub warning_count: u64,
    pub failure_count: u64,
    pub last_control_cert_id: Option<ControlCertId>,
    pub last_merge_cert_id: Option<MergeCertId>,
    pub last_updated_at: DateTime<Utc>,
}

impl ReputationState {
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
pub enum ReputationDecision {
    Allow,
    Quarantine,
    Ban,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReputationObservation {
    pub accepted_work_units: u64,
    pub warning_count: u32,
    pub failure_count: u32,
    pub last_control_cert_id: Option<ControlCertId>,
    pub last_merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReputationEngine {
    pub policy: ReputationPolicy,
}

impl ReputationEngine {
    pub fn new(policy: ReputationPolicy) -> Result<Self, SecurityError> {
        if policy.allow_threshold < policy.quarantine_threshold
            || policy.quarantine_threshold < policy.ban_threshold
        {
            return Err(SecurityError::InvalidReputationThresholds);
        }

        Ok(Self { policy })
    }

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
pub enum AuditFinding {
    ClientVersionTooOld {
        minimum: Version,
        found: Version,
    },
    ProtocolVersionMismatch {
        found: Version,
    },
    BuildHashNotApproved(ContentId),
    ProjectHashMismatch {
        expected: Option<ContentId>,
        found: Option<ContentId>,
    },
    ProjectFamilyMismatch {
        expected: Option<ProjectFamilyId>,
        found: ProjectFamilyId,
    },
    ClientReleaseHashMismatch {
        expected: Option<ContentId>,
        found: ContentId,
    },
    UnsupportedFeature(String),
    BannedFeature(String),
    PeerMismatch {
        expected: PeerId,
        found: PeerId,
    },
    TransportPeerMismatch {
        expected: PeerId,
        found: PeerId,
    },
    CertificatePeerMismatch {
        expected: PeerId,
        found: PeerId,
    },
    NetworkMismatch {
        expected: NetworkId,
        found: NetworkId,
    },
    CertificateNotYetValid {
        not_before: DateTime<Utc>,
    },
    CertificateExpired {
        not_after: DateTime<Utc>,
    },
    RevocationEpochStale {
        minimum: RevocationEpoch,
        found: RevocationEpoch,
    },
    ScopeNotAuthorized(ExperimentScope),
    InvalidCertificateSignature,
    InvalidChallengeSignature,
    LeaseMismatch {
        expected: burn_p2p_core::LeaseId,
        found: burn_p2p_core::LeaseId,
    },
    BaseHeadMismatch {
        expected: HeadId,
        found: HeadId,
    },
    ReceiptOutsideLeaseWindow,
    UnexpectedMicroshards(Vec<burn_p2p_core::MicroShardId>),
    NonPositiveAcceptedWeight,
    NonFiniteMetric(String),
    MissingDataReceipt,
    MissingChallengeResponse,
    MissingHoldoutMetrics,
    InsufficientAttestation {
        required: AttestationLevel,
        found: AttestationLevel,
    },
    ReputationTooLow {
        minimum: f64,
        found: f64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdmissionDecision {
    Allow,
    Quarantine(Vec<AuditFinding>),
    Reject(Vec<AuditFinding>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MergeEvidenceDecision {
    Eligible,
    Insufficient(Vec<AuditFinding>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataAuditReport {
    pub peer_id: PeerId,
    pub lease_id: burn_p2p_core::LeaseId,
    pub findings: Vec<AuditFinding>,
    pub audited_at: DateTime<Utc>,
}

impl DataAuditReport {
    pub fn passed(&self) -> bool {
        self.findings.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateAuditReport {
    pub peer_id: PeerId,
    pub contribution_receipt_id: burn_p2p_core::ContributionReceiptId,
    pub artifact_id: burn_p2p_core::ArtifactId,
    pub findings: Vec<AuditFinding>,
    pub audited_at: DateTime<Utc>,
}

impl UpdateAuditReport {
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
            ContentId::new("release-1"),
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
        release_policy.required_client_release_hash = Some(ContentId::new("release-1"));
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
        manifest.client_release_hash = ContentId::new("wrong-release");
        let decision = policy.evaluate_client(&manifest);
        assert!(matches!(decision, AdmissionDecision::Reject(_)));

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
