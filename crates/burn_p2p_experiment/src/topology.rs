use std::collections::BTreeSet;

use burn_p2p_core::{
    ContentId, HeadId, MergePolicy, MergeStrategy, MergeTopologyPolicy, MergeWindowState,
    NetworkId, PeerId, ReducerAssignment, WindowId,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::spec::{ExperimentSpec, RevisionSpec};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyError {
    NotEnoughReducers { required: usize, available: usize },
    NestedEmaNotAllowed,
}

fn rendezvous_score(
    window_id: WindowId,
    source_peer_id: &PeerId,
    candidate_peer_id: &PeerId,
    purpose: &str,
) -> ContentId {
    ContentId::derive(&(
        window_id.0,
        source_peer_id.as_str(),
        candidate_peer_id.as_str(),
        purpose,
    ))
    .expect("rendezvous score")
}

pub fn validate_merge_topology(
    merge_policy: &MergePolicy,
    topology_policy: &MergeTopologyPolicy,
) -> Result<(), TopologyError> {
    let hierarchical = matches!(
        topology_policy.strategy,
        MergeStrategy::FixedTreeReduce
            | MergeStrategy::RotatingRendezvousTree
            | MergeStrategy::ReplicatedRendezvousDag
            | MergeStrategy::LocalGossipPlusPeriodicGlobal
            | MergeStrategy::MicrocohortReducePlusValidatorPromotion
    );
    let ema_like = matches!(
        merge_policy,
        MergePolicy::Ema | MergePolicy::QualityWeightedEma
    );
    if hierarchical && ema_like && !topology_policy.promotion_policy.apply_single_root_ema {
        return Err(TopologyError::NestedEmaNotAllowed);
    }

    Ok(())
}

pub fn open_merge_window(
    experiment: &ExperimentSpec,
    revision: &RevisionSpec,
    window_id: WindowId,
    base_head_id: HeadId,
    reducers: Vec<PeerId>,
    validators: Vec<PeerId>,
) -> Result<MergeWindowState, burn_p2p_core::SchemaError> {
    let opened_at = Utc::now();
    let closes_at =
        opened_at + Duration::seconds(i64::from(experiment.merge_topology.window_duration_secs));
    let merge_window_id = ContentId::derive(&(
        experiment.experiment_id.as_str(),
        revision.revision_id.as_str(),
        window_id.0,
        base_head_id.as_str(),
    ))?;

    Ok(MergeWindowState {
        merge_window_id,
        network_id: NetworkId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
        ))
        .expect("network id"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: revision.revision_id.clone(),
        window_id,
        base_head_id,
        policy: experiment.merge_topology.clone(),
        reducers,
        validators,
        opened_at,
        closes_at,
    })
}

pub fn assign_reducers(
    merge_window: &MergeWindowState,
    source_peer_id: &PeerId,
    reducer_pool: &[PeerId],
) -> Result<ReducerAssignment, TopologyError> {
    let replication = usize::from(merge_window.policy.reducer_replication.max(1));
    let required = replication * 2;
    if reducer_pool.len() < required {
        return Err(TopologyError::NotEnoughReducers {
            required,
            available: reducer_pool.len(),
        });
    }

    let mut ordered = reducer_pool.to_vec();
    ordered.sort_by(|left, right| {
        rendezvous_score(merge_window.window_id, source_peer_id, left, "leaf")
            .as_str()
            .cmp(rendezvous_score(merge_window.window_id, source_peer_id, right, "leaf").as_str())
    });

    let assigned_reducers = ordered
        .iter()
        .take(replication)
        .cloned()
        .collect::<Vec<_>>();
    let repair_reducers = ordered
        .iter()
        .skip(replication)
        .take(replication)
        .cloned()
        .collect::<Vec<_>>();

    let mut upper_candidates = merge_window.reducers.clone();
    upper_candidates.sort_by(|left, right| {
        rendezvous_score(merge_window.window_id, source_peer_id, left, "upper")
            .as_str()
            .cmp(rendezvous_score(merge_window.window_id, source_peer_id, right, "upper").as_str())
    });
    let upper_tier_reducers = upper_candidates
        .into_iter()
        .filter(|peer_id| !assigned_reducers.contains(peer_id))
        .take(usize::from(merge_window.policy.upper_fanin.max(1)))
        .collect::<Vec<_>>();

    Ok(ReducerAssignment {
        assignment_id: ContentId::derive(&(
            merge_window.merge_window_id.as_str(),
            source_peer_id.as_str(),
            &assigned_reducers,
            &repair_reducers,
        ))
        .expect("assignment id"),
        window_id: merge_window.window_id,
        source_peer_id: source_peer_id.clone(),
        assigned_reducers,
        repair_reducers,
        upper_tier_reducers,
        assigned_at: Utc::now(),
    })
}

pub fn repair_reducer_candidates(
    assignment: &ReducerAssignment,
    unavailable_reducers: &BTreeSet<PeerId>,
) -> Vec<PeerId> {
    assignment
        .assigned_reducers
        .iter()
        .chain(assignment.repair_reducers.iter())
        .filter(|peer_id| !unavailable_reducers.contains(*peer_id))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use burn_p2p_core::{
        ContentId, DatasetViewId, ExperimentId, HeadPromotionPolicy, MergePolicy, MergeStrategy,
        MergeTopologyPolicy, PeerId, StudyId, WindowId,
    };
    use chrono::Utc;

    use super::{
        TopologyError, assign_reducers, repair_reducer_candidates, validate_merge_topology,
    };
    use crate::spec::{ExperimentSpec, PatchSupport, RevisionCompatibility, RevisionSpec};

    fn experiment() -> ExperimentSpec {
        ExperimentSpec::new(
            StudyId::new("study"),
            "demo",
            DatasetViewId::new("view"),
            ContentId::new("schema"),
            MergePolicy::WeightedMean,
        )
        .expect("experiment")
    }

    fn revision(experiment_id: ExperimentId) -> RevisionSpec {
        RevisionSpec::new(
            experiment_id,
            None,
            ContentId::new("project"),
            ContentId::new("config"),
            RevisionCompatibility {
                model_schema_hash: ContentId::new("schema"),
                dataset_view_id: DatasetViewId::new("view"),
                required_client_capabilities: BTreeSet::new(),
            },
            PatchSupport::default(),
            Utc::now(),
        )
        .expect("revision")
    }

    #[test]
    fn reducer_assignment_is_deterministic() {
        let experiment = experiment();
        let revision = revision(experiment.experiment_id.clone());
        let merge_window = super::open_merge_window(
            &experiment,
            &revision,
            WindowId(7),
            burn_p2p_core::HeadId::new("head-0"),
            vec![
                PeerId::new("reducer-a"),
                PeerId::new("reducer-b"),
                PeerId::new("reducer-c"),
                PeerId::new("reducer-d"),
            ],
            vec![PeerId::new("validator-a")],
        )
        .expect("window");

        let first = assign_reducers(
            &merge_window,
            &PeerId::new("trainer-1"),
            &merge_window.reducers,
        )
        .expect("assignment");
        let second = assign_reducers(
            &merge_window,
            &PeerId::new("trainer-1"),
            &merge_window.reducers,
        )
        .expect("assignment");

        assert_eq!(first.assigned_reducers, second.assigned_reducers);
        assert_eq!(first.repair_reducers, second.repair_reducers);
    }

    #[test]
    fn replicated_assignments_have_repair_candidates() {
        let experiment = experiment();
        let revision = revision(experiment.experiment_id.clone());
        let merge_window = super::open_merge_window(
            &experiment,
            &revision,
            WindowId(1),
            burn_p2p_core::HeadId::new("head-0"),
            vec![
                PeerId::new("reducer-a"),
                PeerId::new("reducer-b"),
                PeerId::new("reducer-c"),
                PeerId::new("reducer-d"),
            ],
            vec![PeerId::new("validator-a")],
        )
        .expect("window");

        let assignment = assign_reducers(
            &merge_window,
            &PeerId::new("trainer-1"),
            &merge_window.reducers,
        )
        .expect("assignment");
        let repaired = repair_reducer_candidates(
            &assignment,
            &BTreeSet::from([assignment.assigned_reducers[0].clone()]),
        );

        assert!(!repaired.is_empty());
        assert!(!repaired.contains(&assignment.assigned_reducers[0]));
    }

    #[test]
    fn hierarchical_ema_requires_single_root_semantics() {
        let merge_policy = MergePolicy::Ema;
        let topology_policy = MergeTopologyPolicy {
            strategy: MergeStrategy::ReplicatedRendezvousDag,
            promotion_policy: HeadPromotionPolicy {
                apply_single_root_ema: false,
                ..HeadPromotionPolicy::default()
            },
            ..MergeTopologyPolicy::default()
        };

        assert_eq!(
            validate_merge_topology(&merge_policy, &topology_policy),
            Err(TopologyError::NestedEmaNotAllowed)
        );
    }

    #[test]
    fn open_merge_window_derives_a_stable_id() {
        let mut experiment = experiment();
        experiment.study_id = StudyId::new("study-topology");
        experiment.merge_topology = MergeTopologyPolicy::default();
        let revision = revision(experiment.experiment_id.clone());
        let merge_window = super::open_merge_window(
            &experiment,
            &revision,
            WindowId(9),
            burn_p2p_core::HeadId::new("head-9"),
            vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")],
            vec![PeerId::new("validator-a")],
        )
        .expect("window");

        assert_eq!(merge_window.window_id, WindowId(9));
        assert_eq!(merge_window.experiment_id, experiment.experiment_id);
        assert!(merge_window.closes_at > merge_window.opened_at);
    }
}
