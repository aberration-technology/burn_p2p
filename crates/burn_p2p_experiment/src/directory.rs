use std::collections::BTreeSet;

use burn_p2p_core::{
    ArtifactTargetKind, BrowserCapability, BrowserRole, BrowserRolePolicy, BrowserVisibilityPolicy,
    CapabilityCard, ClientPlatform, ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy,
    ExperimentScope, ExperimentVisibility, LagPolicy, MergeWindowMissPolicy, NetworkId, Precision,
    RevisionManifest, RobustnessPolicy,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const LAG_CATCHUP_KEY: &str = "burn_p2p.revision.lag.max_head_lag_before_catchup";
const LAG_BLOCK_KEY: &str = "burn_p2p.revision.lag.max_head_lag_before_block";
const LAG_REBASE_KEY: &str = "burn_p2p.revision.lag.max_head_lag_before_full_rebase";
const LAG_SKEW_KEY: &str = "burn_p2p.revision.lag.max_window_skew_before_lease_revoke";
const MERGE_WINDOW_MISS_POLICY_KEY: &str = "burn_p2p.revision.merge_window_miss_policy";
const BROWSER_ENABLED_KEY: &str = "burn_p2p.revision.browser.enabled";
const BROWSER_VISIBILITY_POLICY_KEY: &str = "burn_p2p.revision.browser.visibility_policy";
const BROWSER_ROLE_OBSERVER_KEY: &str = "burn_p2p.revision.browser.role.observer";
const BROWSER_ROLE_VERIFIER_KEY: &str = "burn_p2p.revision.browser.role.verifier";
const BROWSER_ROLE_TRAINER_WGPU_KEY: &str = "burn_p2p.revision.browser.role.trainer_wgpu";
const BROWSER_ROLE_FALLBACK_KEY: &str = "burn_p2p.revision.browser.role.fallback";
const BROWSER_REQUIRES_WEBGPU_KEY: &str = "burn_p2p.revision.browser.requires_webgpu";
const BROWSER_MAX_CHECKPOINT_BYTES_KEY: &str = "burn_p2p.revision.browser.max_checkpoint_bytes";
const BROWSER_MAX_WINDOW_SECS_KEY: &str = "burn_p2p.revision.browser.max_window_secs";
const BROWSER_MAX_SHARD_BYTES_KEY: &str = "burn_p2p.revision.browser.max_shard_bytes";
const BROWSER_MAX_BATCH_SIZE_KEY: &str = "burn_p2p.revision.browser.max_batch_size";
const BROWSER_RECOMMENDED_PRECISION_KEY: &str = "burn_p2p.revision.browser.recommended_precision";
const ROBUSTNESS_POLICY_JSON_KEY: &str = "burn_p2p.revision.robustness.policy_json";
const ROBUSTNESS_PRESET_KEY: &str = "burn_p2p.revision.robustness.preset";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment directory.
pub struct ExperimentDirectory {
    /// The network ID.
    pub network_id: NetworkId,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
    /// The entries.
    pub entries: Vec<ExperimentDirectoryEntry>,
}

impl ExperimentDirectory {
    /// Performs the visible to operation.
    pub fn visible_to(&self, scopes: &BTreeSet<ExperimentScope>) -> Vec<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_visible_to(scopes))
            .collect()
    }

    /// Performs the find operation.
    pub fn find(&self, experiment_id: &ExperimentId) -> Option<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .find(|entry| &entry.experiment_id == experiment_id)
    }

    /// Performs the compatible with capability operation.
    pub fn compatible_with_capability(
        &self,
        capability: &CapabilityCard,
    ) -> Vec<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_compatible_with_capability(capability))
            .collect()
    }

    /// Performs the compatible with target artifact operation.
    pub fn compatible_with_target_artifact(
        &self,
        target: &ArtifactTargetKind,
    ) -> Vec<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_compatible_with_target_artifact(target))
            .collect()
    }

    /// Performs the browser eligible operation.
    pub fn browser_eligible(
        &self,
        role: BrowserRole,
        capabilities: &BTreeSet<BrowserCapability>,
    ) -> Vec<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_browser_eligible(role, capabilities))
            .collect()
    }
}

/// Defines behavior for experiment directory access.
pub trait ExperimentDirectoryAccess {
    /// Returns whether the value is visible to.
    fn is_visible_to(&self, scopes: &BTreeSet<ExperimentScope>) -> bool;
    /// Performs the permits scope operation.
    fn permits_scope(&self, scope: &ExperimentScope) -> bool;
}

/// Defines behavior for experiment directory policy ext.
pub trait ExperimentDirectoryPolicyExt {
    /// Performs the lag policy operation.
    fn lag_policy(&self) -> LagPolicy;
    /// Performs the merge window miss policy operation.
    fn merge_window_miss_policy(&self) -> MergeWindowMissPolicy;
    /// Performs the browser enabled operation.
    fn browser_enabled(&self) -> bool;
    /// Performs the browser role policy operation.
    fn browser_role_policy(&self) -> BrowserRolePolicy;
    /// Performs the browser visibility policy operation.
    fn browser_visibility_policy(&self) -> BrowserVisibilityPolicy;
    /// Returns whether the value requires WebGPU.
    fn requires_webgpu(&self) -> bool;
    /// Performs the max browser checkpoint bytes operation.
    fn max_browser_checkpoint_bytes(&self) -> Option<u64>;
    /// Performs the max browser window secs operation.
    fn max_browser_window_secs(&self) -> Option<u64>;
    /// Performs the max browser shard bytes operation.
    fn max_browser_shard_bytes(&self) -> Option<u64>;
    /// Performs the max browser batch size operation.
    fn max_browser_batch_size(&self) -> Option<u32>;
    /// Performs the recommended browser precision operation.
    fn recommended_browser_precision(&self) -> Option<Precision>;
    /// Performs the robustness policy operation.
    fn robustness_policy(&self) -> Option<RobustnessPolicy>;
    /// Performs the browser role allowed operation.
    fn browser_role_allowed(&self, role: BrowserRole) -> bool;
    /// Returns whether the value is browser eligible.
    fn is_browser_eligible(
        &self,
        role: BrowserRole,
        capabilities: &BTreeSet<BrowserCapability>,
    ) -> bool;
    /// Returns whether the value is compatible with target artifact.
    fn is_compatible_with_target_artifact(&self, target: &ArtifactTargetKind) -> bool;
    /// Returns whether the value is compatible with capability.
    fn is_compatible_with_capability(&self, capability: &CapabilityCard) -> bool;
    /// Performs the apply revision policy operation.
    fn apply_revision_policy(&mut self, revision: &RevisionManifest);
}

impl ExperimentDirectoryAccess for ExperimentDirectoryEntry {
    fn is_visible_to(&self, scopes: &BTreeSet<ExperimentScope>) -> bool {
        match self.visibility {
            ExperimentVisibility::Public => true,
            ExperimentVisibility::OptIn => {
                scopes
                    .iter()
                    .any(ExperimentScope::allows_directory_discovery)
                    || scopes.iter().any(|scope| self.permits_scope(scope))
            }
            ExperimentVisibility::InviteOnly | ExperimentVisibility::AuthorityAssigned => {
                scopes.iter().any(|scope| self.permits_scope(scope))
            }
        }
    }

    fn permits_scope(&self, scope: &ExperimentScope) -> bool {
        if self.allowed_scopes.contains(scope) {
            return true;
        }

        match scope {
            ExperimentScope::Connect => false,
            ExperimentScope::Discover => {
                !matches!(
                    self.visibility,
                    ExperimentVisibility::InviteOnly | ExperimentVisibility::AuthorityAssigned
                ) && !matches!(
                    self.opt_in_policy,
                    ExperimentOptInPolicy::InviteOnly | ExperimentOptInPolicy::AuthorityAssigned
                )
            }
            ExperimentScope::Train { experiment_id }
            | ExperimentScope::Validate { experiment_id }
            | ExperimentScope::Archive { experiment_id } => {
                experiment_id == &self.experiment_id
                    && self.allowed_scopes.iter().any(|allowed| allowed == scope)
            }
            ExperimentScope::Admin { study_id } => study_id == &self.study_id,
        }
    }
}

impl ExperimentDirectoryPolicyExt for ExperimentDirectoryEntry {
    fn lag_policy(&self) -> LagPolicy {
        fn parse_u64(entry: &ExperimentDirectoryEntry, key: &str, default: u64) -> u64 {
            entry
                .metadata
                .get(key)
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }

        let default = LagPolicy::default();
        LagPolicy {
            max_head_lag_before_catchup: parse_u64(
                self,
                LAG_CATCHUP_KEY,
                default.max_head_lag_before_catchup,
            ),
            max_head_lag_before_block: parse_u64(
                self,
                LAG_BLOCK_KEY,
                default.max_head_lag_before_block,
            ),
            max_head_lag_before_full_rebase: parse_u64(
                self,
                LAG_REBASE_KEY,
                default.max_head_lag_before_full_rebase,
            ),
            max_window_skew_before_lease_revoke: parse_u64(
                self,
                LAG_SKEW_KEY,
                default.max_window_skew_before_lease_revoke,
            ),
        }
    }

    fn merge_window_miss_policy(&self) -> MergeWindowMissPolicy {
        self.metadata
            .get(MERGE_WINDOW_MISS_POLICY_KEY)
            .and_then(|value| MergeWindowMissPolicy::parse(value))
            .unwrap_or_default()
    }

    fn browser_enabled(&self) -> bool {
        self.metadata
            .get(BROWSER_ENABLED_KEY)
            .and_then(|value| value.parse().ok())
            .unwrap_or(false)
    }

    fn browser_role_policy(&self) -> BrowserRolePolicy {
        fn parse_bool(entry: &ExperimentDirectoryEntry, key: &str, default: bool) -> bool {
            entry
                .metadata
                .get(key)
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }

        let default = BrowserRolePolicy::default();
        BrowserRolePolicy {
            observer: parse_bool(self, BROWSER_ROLE_OBSERVER_KEY, default.observer),
            verifier: parse_bool(self, BROWSER_ROLE_VERIFIER_KEY, default.verifier),
            trainer_wgpu: parse_bool(self, BROWSER_ROLE_TRAINER_WGPU_KEY, default.trainer_wgpu),
            fallback: parse_bool(self, BROWSER_ROLE_FALLBACK_KEY, default.fallback),
        }
    }

    fn browser_visibility_policy(&self) -> BrowserVisibilityPolicy {
        self.metadata
            .get(BROWSER_VISIBILITY_POLICY_KEY)
            .and_then(|value| match value.as_str() {
                "hidden" => Some(BrowserVisibilityPolicy::Hidden),
                "app-listed" => Some(BrowserVisibilityPolicy::AppListed),
                "authenticated-portal" => Some(BrowserVisibilityPolicy::AuthenticatedApp),
                "swarm-eligible" => Some(BrowserVisibilityPolicy::SwarmEligible),
                _ => None,
            })
            .unwrap_or(BrowserVisibilityPolicy::Hidden)
    }

    fn requires_webgpu(&self) -> bool {
        self.metadata
            .get(BROWSER_REQUIRES_WEBGPU_KEY)
            .and_then(|value| value.parse().ok())
            .unwrap_or(false)
    }

    fn max_browser_checkpoint_bytes(&self) -> Option<u64> {
        self.metadata
            .get(BROWSER_MAX_CHECKPOINT_BYTES_KEY)
            .and_then(|value| value.parse().ok())
    }

    fn max_browser_window_secs(&self) -> Option<u64> {
        self.metadata
            .get(BROWSER_MAX_WINDOW_SECS_KEY)
            .and_then(|value| value.parse().ok())
    }

    fn max_browser_shard_bytes(&self) -> Option<u64> {
        self.metadata
            .get(BROWSER_MAX_SHARD_BYTES_KEY)
            .and_then(|value| value.parse().ok())
    }

    fn max_browser_batch_size(&self) -> Option<u32> {
        self.metadata
            .get(BROWSER_MAX_BATCH_SIZE_KEY)
            .and_then(|value| value.parse().ok())
    }

    fn recommended_browser_precision(&self) -> Option<Precision> {
        self.metadata
            .get(BROWSER_RECOMMENDED_PRECISION_KEY)
            .and_then(|value| match value.as_str() {
                "fp16" => Some(Precision::Fp16),
                "fp32" => Some(Precision::Fp32),
                "bf16" => Some(Precision::Bf16),
                "int8" => Some(Precision::Int8),
                _ => None,
            })
    }

    fn robustness_policy(&self) -> Option<RobustnessPolicy> {
        self.metadata
            .get(ROBUSTNESS_POLICY_JSON_KEY)
            .and_then(|value| serde_json::from_str(value).ok())
    }

    fn browser_role_allowed(&self, role: BrowserRole) -> bool {
        let policy = self.browser_role_policy();
        match role {
            BrowserRole::Viewer => {
                self.browser_visibility_policy() != BrowserVisibilityPolicy::Hidden
            }
            BrowserRole::Observer => policy.observer,
            BrowserRole::Verifier => policy.verifier,
            BrowserRole::TrainerWgpu => policy.trainer_wgpu,
            BrowserRole::Fallback => policy.fallback,
        }
    }

    fn is_browser_eligible(
        &self,
        role: BrowserRole,
        capabilities: &BTreeSet<BrowserCapability>,
    ) -> bool {
        let visibility = self.browser_visibility_policy();
        if !self.browser_role_allowed(role) {
            return false;
        }

        match role {
            BrowserRole::Viewer => {
                matches!(
                    visibility,
                    BrowserVisibilityPolicy::AppListed
                        | BrowserVisibilityPolicy::AuthenticatedApp
                        | BrowserVisibilityPolicy::SwarmEligible
                )
            }
            BrowserRole::Observer | BrowserRole::Verifier | BrowserRole::Fallback => {
                self.browser_enabled()
                    && visibility == BrowserVisibilityPolicy::SwarmEligible
                    && capabilities.contains(&BrowserCapability::DedicatedWorker)
            }
            BrowserRole::TrainerWgpu => {
                self.browser_enabled()
                    && visibility == BrowserVisibilityPolicy::SwarmEligible
                    && capabilities.contains(&BrowserCapability::DedicatedWorker)
                    && capabilities.contains(&BrowserCapability::WebGpu)
                    && (!self.requires_webgpu()
                        || capabilities.contains(&BrowserCapability::WebGpu))
            }
        }
    }

    fn is_compatible_with_target_artifact(&self, target: &ArtifactTargetKind) -> bool {
        match target.platform() {
            ClientPlatform::Native => true,
            ClientPlatform::Browser => self.browser_enabled(),
        }
    }

    fn is_compatible_with_capability(&self, capability: &CapabilityCard) -> bool {
        let minimum_roles_satisfied = self.resource_requirements.minimum_roles.is_empty()
            || self
                .resource_requirements
                .minimum_roles
                .iter()
                .any(|role| capability.roles.contains(role));
        if !minimum_roles_satisfied {
            return false;
        }

        if let Some(min_device_memory) = self.resource_requirements.minimum_device_memory_bytes
            && capability.device_memory_bytes.unwrap_or_default() < min_device_memory
        {
            return false;
        }

        if let Some(min_system_memory) = self.resource_requirements.minimum_system_memory_bytes
            && capability.system_memory_bytes < min_system_memory
        {
            return false;
        }

        if capability.platform == ClientPlatform::Browser {
            if !self.browser_enabled() {
                return false;
            }

            if self.requires_webgpu()
                && !capability
                    .preferred_backends
                    .iter()
                    .any(|backend| backend.eq_ignore_ascii_case("wgpu"))
            {
                return false;
            }
        }

        true
    }

    fn apply_revision_policy(&mut self, revision: &RevisionManifest) {
        let lag_policy = revision.effective_lag_policy();
        self.metadata.insert(
            LAG_CATCHUP_KEY.into(),
            lag_policy.max_head_lag_before_catchup.to_string(),
        );
        self.metadata.insert(
            LAG_BLOCK_KEY.into(),
            lag_policy.max_head_lag_before_block.to_string(),
        );
        self.metadata.insert(
            LAG_REBASE_KEY.into(),
            lag_policy.max_head_lag_before_full_rebase.to_string(),
        );
        self.metadata.insert(
            LAG_SKEW_KEY.into(),
            lag_policy.max_window_skew_before_lease_revoke.to_string(),
        );
        self.metadata.insert(
            MERGE_WINDOW_MISS_POLICY_KEY.into(),
            revision.merge_window_miss_policy.as_str().into(),
        );
        self.metadata.insert(
            BROWSER_ENABLED_KEY.into(),
            revision.browser_enabled.to_string(),
        );
        self.metadata.insert(
            BROWSER_VISIBILITY_POLICY_KEY.into(),
            match revision.visibility_policy {
                BrowserVisibilityPolicy::Hidden => "hidden",
                BrowserVisibilityPolicy::AppListed => "app-listed",
                BrowserVisibilityPolicy::AuthenticatedApp => "authenticated-portal",
                BrowserVisibilityPolicy::SwarmEligible => "swarm-eligible",
            }
            .into(),
        );
        self.metadata.insert(
            BROWSER_ROLE_OBSERVER_KEY.into(),
            revision.browser_role_policy.observer.to_string(),
        );
        self.metadata.insert(
            BROWSER_ROLE_VERIFIER_KEY.into(),
            revision.browser_role_policy.verifier.to_string(),
        );
        self.metadata.insert(
            BROWSER_ROLE_TRAINER_WGPU_KEY.into(),
            revision.browser_role_policy.trainer_wgpu.to_string(),
        );
        self.metadata.insert(
            BROWSER_ROLE_FALLBACK_KEY.into(),
            revision.browser_role_policy.fallback.to_string(),
        );
        self.metadata.insert(
            BROWSER_REQUIRES_WEBGPU_KEY.into(),
            revision.requires_webgpu.to_string(),
        );
        if let Some(bytes) = revision.max_browser_checkpoint_bytes {
            self.metadata
                .insert(BROWSER_MAX_CHECKPOINT_BYTES_KEY.into(), bytes.to_string());
        }
        if let Some(seconds) = revision.max_browser_window_secs {
            self.metadata
                .insert(BROWSER_MAX_WINDOW_SECS_KEY.into(), seconds.to_string());
        }
        if let Some(bytes) = revision.max_browser_shard_bytes {
            self.metadata
                .insert(BROWSER_MAX_SHARD_BYTES_KEY.into(), bytes.to_string());
        }
        if let Some(batch) = revision.max_browser_batch_size {
            self.metadata
                .insert(BROWSER_MAX_BATCH_SIZE_KEY.into(), batch.to_string());
        }
        if let Some(precision) = &revision.recommended_browser_precision {
            self.metadata.insert(
                BROWSER_RECOMMENDED_PRECISION_KEY.into(),
                match precision {
                    Precision::Fp16 => "fp16",
                    Precision::Fp32 => "fp32",
                    Precision::Bf16 => "bf16",
                    Precision::Int8 => "int8",
                    Precision::Custom(value) => value.as_str(),
                }
                .into(),
            );
        }
        if let Some(policy) = revision.robustness_policy.as_ref() {
            self.metadata.insert(
                ROBUSTNESS_POLICY_JSON_KEY.into(),
                serde_json::to_string(policy)
                    .expect("revision robustness policy should serialize to directory metadata"),
            );
            self.metadata.insert(
                ROBUSTNESS_PRESET_KEY.into(),
                format!("{:?}", policy.preset).to_lowercase(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        ArtifactTargetKind, AttestationLevel, BrowserCapability, BrowserRole, BrowserRolePolicy,
        BrowserVisibilityPolicy, CapabilityCard, CapabilityCardId, CapabilityClass, ClientPlatform,
        ContentId, ExperimentDirectoryEntry, ExperimentOptInPolicy, ExperimentResourceRequirements,
        ExperimentScope, ExperimentVisibility, HeadId, LagPolicy, MergeWindowMissPolicy, NetworkId,
        PeerId, PeerRole, PeerRoleSet, PersistenceClass, Precision, RevisionId, RevisionManifest,
        RobustnessPolicy, StudyId, WindowActivation, WindowId,
    };
    use chrono::Utc;

    use super::{ExperimentDirectory, ExperimentDirectoryAccess, ExperimentDirectoryPolicyExt};

    fn entry() -> ExperimentDirectoryEntry {
        let experiment_id = burn_p2p_core::ExperimentId::new("exp-a");
        ExperimentDirectoryEntry {
            network_id: NetworkId::new("net-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: experiment_id.clone(),
            workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
            display_name: "Demo".into(),
            model_schema_hash: ContentId::new("model-a"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-a"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                minimum_device_memory_bytes: Some(1024),
                minimum_system_memory_bytes: Some(2048),
                estimated_download_bytes: 8192,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::OptIn,
            opt_in_policy: ExperimentOptInPolicy::Scoped,
            current_revision_id: RevisionId::new("rev-a"),
            current_head_id: Some(HeadId::new("head-a")),
            allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            allowed_scopes: BTreeSet::from([
                ExperimentScope::Train {
                    experiment_id: experiment_id.clone(),
                },
                ExperimentScope::Validate { experiment_id },
            ]),
            metadata: BTreeMap::new(),
        }
    }

    fn capability_card(
        peer_id: &str,
        platform: ClientPlatform,
        roles: impl IntoIterator<Item = PeerRole>,
        preferred_backends: Vec<&str>,
        device_memory_bytes: Option<u64>,
        system_memory_bytes: u64,
    ) -> CapabilityCard {
        CapabilityCard {
            card_id: CapabilityCardId::new(format!("card-{peer_id}")),
            peer_id: PeerId::new(peer_id),
            platform,
            roles: PeerRoleSet::new(roles),
            preferred_backends: preferred_backends.into_iter().map(str::to_string).collect(),
            browser_capabilities: BTreeSet::new(),
            recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
            device_memory_bytes,
            system_memory_bytes,
            disk_bytes: 1024,
            upload_mbps: 10.0,
            download_mbps: 10.0,
            persistence: PersistenceClass::Session,
            work_units_per_second: 32.0,
            attestation_level: AttestationLevel::Challenge,
            benchmark_hash: None,
            reported_at: Utc::now(),
        }
    }

    #[test]
    fn directory_filters_entries_by_scope() {
        let directory = ExperimentDirectory {
            network_id: NetworkId::new("net-a"),
            generated_at: Utc::now(),
            entries: vec![entry()],
        };

        let visible = directory.visible_to(&BTreeSet::from([ExperimentScope::Discover]));
        assert_eq!(visible.len(), 1);

        let hidden = directory.visible_to(&BTreeSet::new());
        assert!(hidden.is_empty());
    }

    #[test]
    fn entry_permits_matching_train_scope() {
        let entry = entry();
        assert!(entry.permits_scope(&ExperimentScope::Train {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
        }));
        assert!(!entry.permits_scope(&ExperimentScope::Train {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-b"),
        }));
    }

    #[test]
    fn discover_scope_does_not_bypass_invite_only_visibility() {
        let mut invite_only = entry();
        invite_only.visibility = ExperimentVisibility::InviteOnly;
        invite_only.opt_in_policy = ExperimentOptInPolicy::Scoped;

        let directory = ExperimentDirectory {
            network_id: NetworkId::new("net-a"),
            generated_at: Utc::now(),
            entries: vec![invite_only],
        };

        assert!(
            directory
                .visible_to(&BTreeSet::from([ExperimentScope::Discover]))
                .is_empty()
        );
    }

    #[test]
    fn connect_scope_does_not_bypass_invite_only_visibility() {
        let mut invite_only = entry();
        invite_only.visibility = ExperimentVisibility::InviteOnly;
        invite_only.opt_in_policy = ExperimentOptInPolicy::Scoped;

        let directory = ExperimentDirectory {
            network_id: NetworkId::new("net-a"),
            generated_at: Utc::now(),
            entries: vec![invite_only],
        };

        assert!(
            directory
                .visible_to(&BTreeSet::from([ExperimentScope::Connect]))
                .is_empty()
        );
    }

    #[test]
    fn entry_projects_and_recovers_revision_policy_from_metadata() {
        let mut entry = entry();
        let revision = RevisionManifest {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
            required_release_train_hash: ContentId::new("train-a"),
            model_schema_hash: ContentId::new("model-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-a"),
            training_config_hash: ContentId::new("training-a"),
            merge_topology_policy_hash: ContentId::new("topology-a"),
            slot_requirements: entry.resource_requirements.clone(),
            activation_window: WindowActivation {
                activation_window: WindowId(2),
                grace_windows: 1,
            },
            lag_policy: LagPolicy {
                max_head_lag_before_catchup: 2,
                max_head_lag_before_block: 6,
                max_head_lag_before_full_rebase: 12,
                max_window_skew_before_lease_revoke: 4,
            },
            merge_window_miss_policy: MergeWindowMissPolicy::RebaseRequired,
            robustness_policy: Some(RobustnessPolicy::strict()),
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy::default(),
            max_browser_checkpoint_bytes: Some(1024),
            max_browser_window_secs: Some(30),
            max_browser_shard_bytes: Some(512),
            requires_webgpu: true,
            max_browser_batch_size: Some(8),
            recommended_browser_precision: Some(Precision::Fp16),
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "demo".into(),
        };

        entry.apply_revision_policy(&revision);

        assert_eq!(entry.lag_policy(), revision.effective_lag_policy());
        assert_eq!(
            entry.merge_window_miss_policy(),
            MergeWindowMissPolicy::RebaseRequired
        );
        assert_eq!(entry.robustness_policy(), revision.robustness_policy);
        assert!(entry.browser_enabled());
        assert_eq!(
            entry.browser_visibility_policy(),
            BrowserVisibilityPolicy::SwarmEligible
        );
        assert_eq!(entry.recommended_browser_precision(), Some(Precision::Fp16));
        assert_eq!(entry.max_browser_window_secs(), Some(30));
    }

    #[test]
    fn directory_filters_browser_eligibility_and_artifact_target() {
        let mut browser_entry = entry();
        let browser_revision = RevisionManifest {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
            required_release_train_hash: ContentId::new("train-a"),
            model_schema_hash: ContentId::new("model-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-a"),
            training_config_hash: ContentId::new("training-a"),
            merge_topology_policy_hash: ContentId::new("topology-a"),
            slot_requirements: browser_entry.resource_requirements.clone(),
            activation_window: WindowActivation {
                activation_window: WindowId(1),
                grace_windows: 0,
            },
            lag_policy: LagPolicy::default(),
            merge_window_miss_policy: MergeWindowMissPolicy::LeaseBlocked,
            robustness_policy: None,
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy {
                observer: true,
                verifier: true,
                trainer_wgpu: true,
                fallback: true,
            },
            max_browser_checkpoint_bytes: Some(1024),
            max_browser_window_secs: Some(60),
            max_browser_shard_bytes: Some(512),
            requires_webgpu: true,
            max_browser_batch_size: Some(4),
            recommended_browser_precision: Some(Precision::Fp16),
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "browser".into(),
        };
        browser_entry.apply_revision_policy(&browser_revision);

        let native_entry = entry();
        let directory = ExperimentDirectory {
            network_id: NetworkId::new("net-a"),
            generated_at: Utc::now(),
            entries: vec![browser_entry, native_entry],
        };

        let observer_caps = BTreeSet::from([BrowserCapability::DedicatedWorker]);
        let trainer_caps = BTreeSet::from([
            BrowserCapability::DedicatedWorker,
            BrowserCapability::WebGpu,
        ]);

        assert_eq!(
            directory
                .browser_eligible(BrowserRole::Observer, &observer_caps)
                .len(),
            1
        );
        assert_eq!(
            directory
                .browser_eligible(BrowserRole::TrainerWgpu, &observer_caps)
                .len(),
            0
        );
        assert_eq!(
            directory
                .browser_eligible(BrowserRole::TrainerWgpu, &trainer_caps)
                .len(),
            1
        );
        assert_eq!(
            directory
                .compatible_with_target_artifact(&ArtifactTargetKind::BrowserWasm)
                .len(),
            1
        );
        assert_eq!(
            directory
                .compatible_with_target_artifact(&ArtifactTargetKind::NativeLinuxX86_64)
                .len(),
            2
        );
    }

    #[test]
    fn directory_filters_entries_by_capability_card() {
        let mut browser_entry = entry();
        browser_entry.apply_revision_policy(&RevisionManifest {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
            required_release_train_hash: ContentId::new("train-a"),
            model_schema_hash: ContentId::new("model-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-a"),
            training_config_hash: ContentId::new("training-a"),
            merge_topology_policy_hash: ContentId::new("topology-a"),
            slot_requirements: browser_entry.resource_requirements.clone(),
            activation_window: WindowActivation {
                activation_window: WindowId(1),
                grace_windows: 0,
            },
            lag_policy: LagPolicy::default(),
            merge_window_miss_policy: MergeWindowMissPolicy::LeaseBlocked,
            robustness_policy: None,
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy::default(),
            max_browser_checkpoint_bytes: None,
            max_browser_window_secs: None,
            max_browser_shard_bytes: None,
            requires_webgpu: true,
            max_browser_batch_size: None,
            recommended_browser_precision: None,
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "browser".into(),
        });

        let directory = ExperimentDirectory {
            network_id: NetworkId::new("net-a"),
            generated_at: Utc::now(),
            entries: vec![browser_entry],
        };

        let browser_ok = capability_card(
            "browser-ok",
            ClientPlatform::Browser,
            [PeerRole::TrainerGpu],
            vec!["wgpu"],
            Some(2048),
            4096,
        );
        let browser_low_mem = capability_card(
            "browser-low-mem",
            ClientPlatform::Browser,
            [PeerRole::TrainerGpu],
            vec!["wgpu"],
            Some(512),
            4096,
        );
        let browser_no_wgpu = capability_card(
            "browser-no-wgpu",
            ClientPlatform::Browser,
            [PeerRole::TrainerGpu],
            vec!["cpu"],
            Some(2048),
            4096,
        );

        assert_eq!(directory.compatible_with_capability(&browser_ok).len(), 1);
        assert!(
            directory
                .compatible_with_capability(&browser_low_mem)
                .is_empty()
        );
        assert!(
            directory
                .compatible_with_capability(&browser_no_wgpu)
                .is_empty()
        );
    }
}
