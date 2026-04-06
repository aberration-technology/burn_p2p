use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Browser-facing join-policy summary derived from one directory entry.
pub struct BrowserJoinPolicy {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// Target artifact requested by the browser client.
    pub target_artifact: ArtifactTargetKind,
    /// Entry visibility policy.
    pub visibility_policy: BrowserVisibilityPolicy,
    /// Roles the browser may currently assume.
    pub eligible_roles: Vec<BrowserRole>,
    /// Concrete reasons that blocked stronger participation.
    pub blocked_reasons: Vec<String>,
}

impl BrowserJoinPolicy {
    /// Returns whether any browser join path is currently allowed.
    pub fn allows_join(&self) -> bool {
        !self.eligible_roles.is_empty()
    }

    /// Returns whether the browser may join as an actual peer role.
    pub fn allows_peer_join(&self) -> bool {
        self.eligible_roles.iter().any(|role| {
            matches!(
                role,
                BrowserRole::Observer
                    | BrowserRole::Verifier
                    | BrowserRole::TrainerWgpu
                    | BrowserRole::Fallback
            )
        })
    }

    /// Returns whether the browser is limited to portal-only access.
    pub fn allows_viewer_only(&self) -> bool {
        self.eligible_roles.contains(&BrowserRole::Viewer)
    }

    /// Returns the best role to recommend for the requested preference.
    pub fn recommended_role(&self, preferred_role: BrowserRole) -> Option<BrowserRole> {
        if self.eligible_roles.contains(&preferred_role) {
            return Some(preferred_role);
        }

        [
            BrowserRole::TrainerWgpu,
            BrowserRole::Verifier,
            BrowserRole::Observer,
            BrowserRole::Fallback,
            BrowserRole::Viewer,
        ]
        .into_iter()
        .find(|role| self.eligible_roles.contains(role))
    }
}

/// Derives browser join policy from a directory entry and detected browser capabilities.
pub fn browser_join_policy_for_entry(
    entry: &ExperimentDirectoryEntry,
    target_artifact: &ArtifactTargetKind,
    capabilities: &BTreeSet<BrowserCapability>,
) -> BrowserJoinPolicy {
    let mut blocked_reasons = Vec::new();
    if !entry.is_compatible_with_target_artifact(target_artifact) {
        blocked_reasons.push(format!(
            "revision is not approved for target artifact {}",
            target_artifact.as_target_artifact_id()
        ));
    }

    if !entry.browser_enabled() {
        blocked_reasons.push("revision is not browser enabled".into());
    }

    if !capabilities.contains(&BrowserCapability::DedicatedWorker) {
        blocked_reasons.push("browser peer requires dedicated worker support".into());
    }

    if entry.requires_webgpu() && !capabilities.contains(&BrowserCapability::WebGpu) {
        blocked_reasons.push("revision requires WebGPU support".into());
    }

    let eligible_roles = [
        BrowserRole::Viewer,
        BrowserRole::Observer,
        BrowserRole::Verifier,
        BrowserRole::TrainerWgpu,
        BrowserRole::Fallback,
    ]
    .into_iter()
    .filter(|role| entry.is_browser_eligible(*role, capabilities))
    .collect();

    BrowserJoinPolicy {
        study_id: entry.study_id.clone(),
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        target_artifact: target_artifact.clone(),
        visibility_policy: entry.browser_visibility_policy(),
        eligible_roles,
        blocked_reasons,
    }
}
