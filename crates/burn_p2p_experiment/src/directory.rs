use std::collections::BTreeSet;

use burn_p2p_core::{
    ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy, ExperimentScope,
    ExperimentVisibility, NetworkId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentDirectory {
    pub network_id: NetworkId,
    pub generated_at: DateTime<Utc>,
    pub entries: Vec<ExperimentDirectoryEntry>,
}

impl ExperimentDirectory {
    pub fn visible_to(&self, scopes: &BTreeSet<ExperimentScope>) -> Vec<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.is_visible_to(scopes))
            .collect()
    }

    pub fn find(&self, experiment_id: &ExperimentId) -> Option<&ExperimentDirectoryEntry> {
        self.entries
            .iter()
            .find(|entry| &entry.experiment_id == experiment_id)
    }
}

pub trait ExperimentDirectoryAccess {
    fn is_visible_to(&self, scopes: &BTreeSet<ExperimentScope>) -> bool;
    fn permits_scope(&self, scope: &ExperimentScope) -> bool;
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        ContentId, ExperimentDirectoryEntry, ExperimentOptInPolicy, ExperimentResourceRequirements,
        ExperimentScope, ExperimentVisibility, HeadId, NetworkId, PeerRole, PeerRoleSet,
        RevisionId, StudyId,
    };
    use chrono::Utc;

    use super::{ExperimentDirectory, ExperimentDirectoryAccess};

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
}
