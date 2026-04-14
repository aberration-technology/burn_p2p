use super::*;

#[test]
fn candidate_heads_keep_all_known_provider_peers_for_selected_head() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let announced_at = Utc::now();
    let head = HeadDescriptor {
        head_id: HeadId::new("head-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: ArtifactId::new("artifact-a"),
        parent_head_id: Some(HeadId::new("head-base")),
        global_step: 2,
        created_at: announced_at,
        metrics: BTreeMap::new(),
    };
    let update = UpdateAnnounce {
        peer_id: PeerId::new("trainer-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(2),
        base_head_id: HeadId::new("head-base"),
        lease_id: Some(LeaseId::new("lease-a")),
        delta_artifact_id: head.artifact_id.clone(),
        sample_weight: 8.0,
        quality_weight: 1.0,
        norm_stats: UpdateNormStats {
            l2_norm: 1.0,
            max_abs: 1.0,
            clipped: false,
            non_finite_tensors: 0,
        },
        feature_sketch: None,
        receipt_root: ContentId::new("receipt-root-a"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
        providers: vec![PeerId::new("provider-b"), PeerId::new("provider-c")],
        announced_at,
    };
    let snapshots = vec![
        (
            PeerId::new("observer-a"),
            ControlPlaneSnapshot {
                head_announcements: vec![HeadAnnouncement {
                    overlay: overlay.clone(),
                    head: head.clone(),
                    provider_peer_id: Some(PeerId::new("provider-b")),
                    announced_at,
                }],
                ..ControlPlaneSnapshot::default()
            },
        ),
        (
            PeerId::new("observer-b"),
            ControlPlaneSnapshot {
                head_announcements: vec![HeadAnnouncement {
                    overlay,
                    head: head.clone(),
                    provider_peer_id: Some(PeerId::new("provider-d")),
                    announced_at,
                }],
                ..ControlPlaneSnapshot::default()
            },
        ),
    ];

    let candidates = collect_validation_candidate_heads(
        &experiment,
        &snapshots,
        &PeerId::new("validator-a"),
        Some(&HeadId::new("head-base")),
        2,
        &[update],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0]
            .provider_peer_ids
            .iter()
            .map(|peer_id| peer_id.as_str().to_owned())
            .collect::<Vec<_>>(),
        vec!["trainer-a", "provider-b", "provider-c", "provider-d"]
    );
}

#[test]
fn candidate_heads_can_be_synthesized_from_updates_without_head_announcements() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let update = UpdateAnnounce {
        peer_id: PeerId::new("trainer-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(4),
        base_head_id: HeadId::new("head-base"),
        lease_id: Some(LeaseId::new("lease-a")),
        delta_artifact_id: ArtifactId::new("artifact-a"),
        sample_weight: 8.0,
        quality_weight: 1.0,
        norm_stats: UpdateNormStats {
            l2_norm: 1.0,
            max_abs: 1.0,
            clipped: false,
            non_finite_tensors: 0,
        },
        feature_sketch: None,
        receipt_root: ContentId::new("receipt-root-a"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
        providers: vec![PeerId::new("provider-b")],
        announced_at: Utc::now(),
    };

    let candidates = collect_validation_candidate_heads(
        &experiment,
        &[],
        &PeerId::new("validator-a"),
        Some(&HeadId::new("head-base")),
        7,
        &[update],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].head.head_id,
        HeadId::new("exp-a-trainer-a-window-4")
    );
    assert_eq!(
        candidates[0].head.parent_head_id,
        Some(HeadId::new("head-base"))
    );
    assert_eq!(candidates[0].head.global_step, 7);
    assert_eq!(
        candidates[0]
            .provider_peer_ids
            .iter()
            .map(|peer_id| peer_id.as_str().to_owned())
            .collect::<Vec<_>>(),
        vec!["trainer-a", "provider-b"]
    );
}

#[test]
fn candidate_heads_collapse_linear_same_peer_descendants_into_rooted_candidate() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let overlay = experiment.overlay_set().expect("overlay").heads;
    let announced_at = Utc::now();
    let root_update = UpdateAnnounce {
        peer_id: PeerId::new("trainer-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(4),
        base_head_id: HeadId::new("head-base"),
        lease_id: Some(LeaseId::new("lease-a")),
        delta_artifact_id: ArtifactId::new("artifact-a"),
        sample_weight: 8.0,
        quality_weight: 0.8,
        norm_stats: UpdateNormStats {
            l2_norm: 1.0,
            max_abs: 1.0,
            clipped: false,
            non_finite_tensors: 0,
        },
        feature_sketch: Some(UpdateFeatureSketch {
            artifact_size_bytes: 1,
            global_norm: 1.0,
            per_layer_norms: BTreeMap::new(),
            random_projection: vec![1.0],
            sign_projection: vec![1],
            top_k_indices: Vec::new(),
            cosine_to_reference: None,
            sign_agreement_fraction: None,
            canary_loss_delta: None,
            historical_deviation_score: None,
            neighbor_distance: None,
            staleness_windows: 0,
            receive_delay_ms: 0,
            non_finite_tensor_count: 0,
        }),
        receipt_root: ContentId::new("receipt-root-a"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
        providers: vec![PeerId::new("provider-a")],
        announced_at,
    };
    let descendant_update = UpdateAnnounce {
        peer_id: PeerId::new("trainer-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(5),
        base_head_id: HeadId::new("head-a"),
        lease_id: Some(LeaseId::new("lease-b")),
        delta_artifact_id: ArtifactId::new("artifact-b"),
        sample_weight: 6.0,
        quality_weight: 0.4,
        norm_stats: UpdateNormStats {
            l2_norm: 2.0,
            max_abs: 3.0,
            clipped: true,
            non_finite_tensors: 1,
        },
        feature_sketch: Some(UpdateFeatureSketch {
            artifact_size_bytes: 1,
            global_norm: 2.0,
            per_layer_norms: BTreeMap::new(),
            random_projection: vec![2.0],
            sign_projection: vec![1],
            top_k_indices: Vec::new(),
            cosine_to_reference: None,
            sign_agreement_fraction: None,
            canary_loss_delta: None,
            historical_deviation_score: None,
            neighbor_distance: None,
            staleness_windows: 0,
            receive_delay_ms: 0,
            non_finite_tensor_count: 1,
        }),
        receipt_root: ContentId::new("receipt-root-b"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
        providers: vec![PeerId::new("provider-b"), PeerId::new("provider-c")],
        announced_at: announced_at + chrono::Duration::milliseconds(50),
    };
    let snapshots = vec![(
        PeerId::new("observer-a"),
        ControlPlaneSnapshot {
            head_announcements: vec![
                HeadAnnouncement {
                    overlay: overlay.clone(),
                    head: HeadDescriptor {
                        head_id: HeadId::new("head-a"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: root_update.delta_artifact_id.clone(),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 7,
                        created_at: announced_at,
                        metrics: BTreeMap::new(),
                    },
                    provider_peer_id: Some(PeerId::new("provider-a")),
                    announced_at,
                },
                HeadAnnouncement {
                    overlay,
                    head: HeadDescriptor {
                        head_id: HeadId::new("head-b"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: descendant_update.delta_artifact_id.clone(),
                        parent_head_id: Some(HeadId::new("head-a")),
                        global_step: 8,
                        created_at: descendant_update.announced_at,
                        metrics: BTreeMap::new(),
                    },
                    provider_peer_id: Some(PeerId::new("provider-c")),
                    announced_at: descendant_update.announced_at,
                },
            ],
            update_announcements: vec![
                UpdateEnvelopeAnnouncement {
                    overlay: experiment.overlay_set().expect("overlay").heads.clone(),
                    update: root_update.clone(),
                },
                UpdateEnvelopeAnnouncement {
                    overlay: experiment.overlay_set().expect("overlay").heads,
                    update: descendant_update.clone(),
                },
            ],
            ..ControlPlaneSnapshot::default()
        },
    )];

    let candidates = collect_validation_candidate_heads(
        &experiment,
        &snapshots,
        &PeerId::new("validator-a"),
        Some(&HeadId::new("head-base")),
        7,
        &[root_update],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].head.head_id, HeadId::new("head-b"));
    assert_eq!(candidates[0].update.base_head_id, HeadId::new("head-base"));
    assert_eq!(candidates[0].update.window_id, WindowId(4));
    assert_eq!(
        candidates[0].update.delta_artifact_id,
        ArtifactId::new("artifact-b")
    );
    assert_eq!(candidates[0].update.sample_weight, 14.0);
    assert!(
        (candidates[0].update.quality_weight - (8.8 / 14.0)).abs() < 1e-9,
        "unexpected quality weight: {}",
        candidates[0].update.quality_weight
    );
    assert_eq!(candidates[0].update.norm_stats.l2_norm, 3.0);
    assert_eq!(candidates[0].update.norm_stats.max_abs, 3.0);
    assert!(candidates[0].update.feature_sketch.is_none());
    assert_eq!(
        candidates[0].update.receipt_ids,
        vec![
            ContributionReceiptId::new("receipt-a"),
            ContributionReceiptId::new("receipt-b")
        ]
    );
    assert_eq!(
        candidates[0]
            .provider_peer_ids
            .iter()
            .map(|peer_id| peer_id.as_str().to_owned())
            .collect::<Vec<_>>(),
        vec!["trainer-a", "provider-b", "provider-c"]
    );
}
