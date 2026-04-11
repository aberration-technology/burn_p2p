use std::collections::BTreeSet;

use super::PrefetchLeasePlanArgs;
use super::planning::{
    adaptive_budget_work_units, adaptive_microshard_cap, fair_share_budget_work_units,
    fair_share_microshard_cap, plan_prefetch_lease_for_window, prefetched_lease_is_reusable,
    supports_background_shard_prefetch, wait_for_prefetch_completion,
};
use crate::{
    ExperimentHandle, LimitProfile, PeerId, PeerRoleSet, TrainingPrefetchTask, WorkBudget,
};
use burn_p2p_core::{
    AttestationLevel, CapabilityCard, CapabilityCardId, CapabilityClass, CapabilityEstimate,
    ClientPlatform, ContentId, DatasetId, DatasetView, DatasetViewId, ExperimentId, MicroShard,
    NetworkId, PersistenceClass, RevisionId, StudyId, WindowId,
};
use chrono::Utc;

#[test]
fn fair_share_budget_tracks_microshard_share() {
    assert_eq!(fair_share_microshard_cap(3, 8), 3);
    assert_eq!(fair_share_budget_work_units(3, 8, 240), 90);
    assert_eq!(fair_share_budget_work_units(2, 5, 240), 144);
}

#[test]
fn fair_share_budget_never_drops_below_one() {
    assert_eq!(fair_share_budget_work_units(8, 1, 1), 1);
    assert_eq!(fair_share_budget_work_units(16, 16, 1), 1);
}

#[test]
fn adaptive_budget_and_microshard_cap_clamp_and_scale() {
    assert_eq!(adaptive_budget_work_units(100, 1.0), 100);
    assert_eq!(adaptive_budget_work_units(100, 0.5), 50);
    assert_eq!(adaptive_budget_work_units(100, 0.0), 20);

    assert_eq!(adaptive_microshard_cap(10, 1.0), 10);
    assert_eq!(adaptive_microshard_cap(10, 0.45), 5);
    assert_eq!(adaptive_microshard_cap(10, 0.0), 2);
}

#[test]
fn prefetch_lease_targets_next_window_peer_slice() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
    };
    let dataset_view = DatasetView {
        dataset_view_id: DatasetViewId::new("view"),
        dataset_id: DatasetId::new("dataset"),
        preprocessing_hash: ContentId::new("prep"),
        tokenizer_hash: None,
        manifest_hash: ContentId::new("manifest"),
        metadata: Default::default(),
    };
    let assignment_peers = vec![PeerId::new("peer-a"), PeerId::new("peer-b")];
    let microshards = (0..6)
        .map(|ordinal| MicroShard {
            microshard_id: burn_p2p_core::MicroShardId::new(format!("micro-{ordinal}")),
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            ordinal,
            estimated_examples: 16,
            estimated_tokens: 512,
            estimated_bytes: 1024,
        })
        .collect::<Vec<_>>();

    let planned = plan_prefetch_lease_for_window(PrefetchLeasePlanArgs {
        network_id: &experiment.network_id,
        experiment: &experiment,
        dataset_view: &dataset_view,
        local_peer_id: &assignment_peers[1],
        assignment_peers: &assignment_peers,
        microshards: &microshards,
        window_id: WindowId(9),
        budget_work_units: 128,
        adaptation_factor: 1.0,
    })
    .expect("prefetch lease");

    assert_eq!(planned.lease.window_id, WindowId(9));
    assert!(!planned.lease.microshards.is_empty());
    assert!(
        planned
            .lease
            .microshards
            .iter()
            .all(|microshard_id| microshard_id.as_str().starts_with("micro-"))
    );
    assert!(planned.lease.microshards.len() <= fair_share_microshard_cap(2, 3));
}

#[test]
fn background_prefetch_only_runs_for_compute_backends() {
    let gpu_profile = LimitProfile {
        card: CapabilityCard {
            card_id: CapabilityCardId::new("card-gpu"),
            peer_id: PeerId::new("peer-gpu"),
            platform: ClientPlatform::Native,
            roles: PeerRoleSet::default_trainer(),
            preferred_backends: vec!["cuda".into()],
            browser_capabilities: BTreeSet::new(),
            recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
            device_memory_bytes: Some(1),
            system_memory_bytes: 1,
            disk_bytes: 1,
            upload_mbps: 1.0,
            download_mbps: 1.0,
            persistence: PersistenceClass::Ephemeral,
            work_units_per_second: 1.0,
            attestation_level: AttestationLevel::None,
            benchmark_hash: None,
            reported_at: Utc::now(),
        },
        estimate: CapabilityEstimate {
            preferred_backends: vec!["cuda".into()],
            work_units_per_second: 1.0,
            target_window_seconds: 1,
        },
        recommended_roles: PeerRoleSet::default_trainer(),
        recommended_budget: WorkBudget {
            target_window_seconds: 1,
            budget_work_units: 1,
        },
    };
    let cpu_profile = LimitProfile {
        card: CapabilityCard {
            preferred_backends: vec!["ndarray".into()],
            ..gpu_profile.card.clone()
        },
        estimate: CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            ..gpu_profile.estimate.clone()
        },
        recommended_roles: gpu_profile.recommended_roles.clone(),
        recommended_budget: gpu_profile.recommended_budget.clone(),
    };

    assert!(supports_background_shard_prefetch(&gpu_profile));
    assert!(!supports_background_shard_prefetch(&cpu_profile));
}

#[test]
fn prefetched_lease_reuse_requires_matching_window_and_live_lease() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
    };
    let dataset_view = DatasetView {
        dataset_view_id: DatasetViewId::new("view"),
        dataset_id: DatasetId::new("dataset"),
        preprocessing_hash: ContentId::new("prep"),
        tokenizer_hash: None,
        manifest_hash: ContentId::new("manifest"),
        metadata: Default::default(),
    };
    let local_peer_id = PeerId::new("peer-a");
    let assignment_peers = vec![local_peer_id.clone(), PeerId::new("peer-b")];
    let microshards = (0..4)
        .map(|ordinal| MicroShard {
            microshard_id: burn_p2p_core::MicroShardId::new(format!("micro-{ordinal}")),
            dataset_view_id: dataset_view.dataset_view_id.clone(),
            ordinal,
            estimated_examples: 16,
            estimated_tokens: 512,
            estimated_bytes: 1024,
        })
        .collect::<Vec<_>>();
    let planned_lease = plan_prefetch_lease_for_window(PrefetchLeasePlanArgs {
        network_id: &experiment.network_id,
        experiment: &experiment,
        dataset_view: &dataset_view,
        local_peer_id: &local_peer_id,
        assignment_peers: &assignment_peers,
        microshards: &microshards,
        window_id: WindowId(3),
        budget_work_units: 128,
        adaptation_factor: 1.0,
    })
    .expect("prefetch lease");
    let reusable = TrainingPrefetchTask {
        planned_lease: planned_lease.clone(),
        handle: None,
    };

    assert!(prefetched_lease_is_reusable(
        &reusable,
        &experiment,
        &local_peer_id,
        WindowId(3),
        planned_lease.lease.budget_work_units,
        &microshards,
    ));
    assert!(!prefetched_lease_is_reusable(
        &reusable,
        &experiment,
        &local_peer_id,
        WindowId(4),
        planned_lease.lease.budget_work_units,
        &microshards,
    ));
    assert!(!prefetched_lease_is_reusable(
        &reusable,
        &experiment,
        &PeerId::new("peer-c"),
        WindowId(3),
        planned_lease.lease.budget_work_units,
        &microshards,
    ));
    assert!(!prefetched_lease_is_reusable(
        &reusable,
        &experiment,
        &local_peer_id,
        WindowId(3),
        planned_lease.lease.budget_work_units.saturating_add(1),
        &microshards,
    ));

    let mut expired = reusable;
    expired.planned_lease.lease.expires_at = Utc::now() - chrono::Duration::seconds(1);
    assert!(!prefetched_lease_is_reusable(
        &expired,
        &experiment,
        &local_peer_id,
        WindowId(3),
        planned_lease.lease.budget_work_units,
        &microshards,
    ));
}

#[test]
fn prefetch_completion_wait_observes_finished_worker() {
    let handle = std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(15));
    });
    assert!(wait_for_prefetch_completion(
        &handle,
        std::time::Duration::from_millis(100),
    ));
    let _ = handle.join();
}

#[test]
fn prefetch_completion_wait_does_not_block_past_grace() {
    let handle = std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(100));
    });
    assert!(!wait_for_prefetch_completion(
        &handle,
        std::time::Duration::from_millis(10),
    ));
    let _ = handle.join();
}
