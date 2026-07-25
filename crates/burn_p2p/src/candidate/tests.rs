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
                    workload_update: None,
                },
                UpdateEnvelopeAnnouncement {
                    overlay: experiment.overlay_set().expect("overlay").heads,
                    update: descendant_update.clone(),
                    workload_update: None,
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

#[test]
fn typed_candidate_heads_are_not_collapsed_without_sequential_payloads() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let root_update = UpdateAnnounce {
        peer_id: PeerId::new("trainer-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(1),
        base_head_id: HeadId::new("head-base"),
        lease_id: Some(LeaseId::new("lease-a")),
        delta_artifact_id: ArtifactId::new("artifact-a"),
        sample_weight: 1.0,
        quality_weight: 1.0,
        norm_stats: UpdateNormStats {
            l2_norm: 1.0,
            max_abs: 1.0,
            clipped: false,
            non_finite_tensors: 0,
        },
        feature_sketch: None,
        receipt_root: ContentId::new("receipt-a"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-a")],
        providers: vec![PeerId::new("trainer-a")],
        announced_at: Utc::now(),
    };
    let descendant_update = UpdateAnnounce {
        window_id: WindowId(2),
        base_head_id: HeadId::new("exp-a-trainer-a-window-1"),
        lease_id: Some(LeaseId::new("lease-b")),
        delta_artifact_id: ArtifactId::new("artifact-b"),
        receipt_root: ContentId::new("receipt-b"),
        receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
        announced_at: root_update.announced_at + chrono::Duration::milliseconds(1),
        ..root_update.clone()
    };
    let descriptor = ArtifactDescriptor {
        artifact_id: root_update.delta_artifact_id.clone(),
        kind: ArtifactKind::DeltaPack,
        head_id: Some(HeadId::new("exp-a-trainer-a-window-1")),
        base_head_id: Some(root_update.base_head_id.clone()),
        precision: Precision::Fp32,
        model_schema_hash: ContentId::new("schema"),
        record_format: "compact-test".into(),
        bytes_len: 1,
        chunks: Vec::new(),
        root_hash: ContentId::new("root"),
    };
    let workload_update = WorkloadUpdateEnvelope {
        training_contract_id: ContentId::new("contract"),
        revision_id: experiment.revision_id.clone(),
        base_head_id: root_update.base_head_id.clone(),
        window_id: root_update.window_id,
        lease_id: root_update.lease_id.clone().expect("lease"),
        codec: UpdateCodec::DenseDelta,
        artifact: descriptor,
        decoded_tensor_digest: None,
        claimed_norm_stats: None,
        claimed_feature_sketch: None,
    };
    let snapshots = vec![(
        PeerId::new("observer"),
        ControlPlaneSnapshot {
            update_announcements: vec![
                UpdateEnvelopeAnnouncement {
                    overlay: experiment.overlay_set().expect("overlay").heads.clone(),
                    update: root_update.clone(),
                    workload_update: Some(workload_update),
                },
                UpdateEnvelopeAnnouncement {
                    overlay: experiment.overlay_set().expect("overlay").heads,
                    update: descendant_update,
                    workload_update: None,
                },
            ],
            ..ControlPlaneSnapshot::default()
        },
    )];

    let candidates = collect_validation_candidate_heads(
        &experiment,
        &snapshots,
        &PeerId::new("validator"),
        Some(&HeadId::new("head-base")),
        1,
        &[root_update],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].update.delta_artifact_id,
        ArtifactId::new("artifact-a")
    );
    assert!(candidates[0].workload_update.is_some());
}

#[test]
fn authenticated_update_lease_fails_closed_on_missing_conflicting_or_wrong_identity() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let trainer = PeerId::new("trainer-a");
    let dataset_view_id = DatasetViewId::new("view-a");
    let lease_id = LeaseId::new("lease-a");
    let window_id = WindowId(3);
    let descriptor = ArtifactDescriptor {
        artifact_id: ArtifactId::new("artifact-a"),
        kind: ArtifactKind::DeltaPack,
        head_id: Some(HeadId::new("head-a")),
        base_head_id: Some(HeadId::new("head-base")),
        precision: Precision::Fp32,
        model_schema_hash: ContentId::new("schema"),
        record_format: "compact-test".into(),
        bytes_len: 1,
        chunks: Vec::new(),
        root_hash: ContentId::new("root"),
    };
    let update = WorkloadUpdateEnvelope {
        training_contract_id: ContentId::new("contract"),
        revision_id: experiment.revision_id.clone(),
        base_head_id: HeadId::new("head-base"),
        window_id,
        lease_id: lease_id.clone(),
        codec: UpdateCodec::DenseDelta,
        artifact: descriptor,
        decoded_tensor_digest: None,
        claimed_norm_stats: None,
        claimed_feature_sketch: None,
    };

    let missing =
        model::authenticated_update_lease(&[], &experiment, &trainer, &update, &dataset_view_id)
            .expect_err("an update without a gossiped lease must fail closed");
    assert!(
        missing
            .to_string()
            .contains("missing its authenticated lease")
    );

    let now = Utc::now();
    let lease = AssignmentLease {
        lease_id,
        network_id: experiment.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        peer_id: trainer.clone(),
        dataset_view_id: dataset_view_id.clone(),
        window_id,
        granted_at: now,
        expires_at: now + chrono::Duration::minutes(1),
        budget_work_units: 4,
        microshards: vec![MicroShardId::new("micro-a")],
        assignment_hash: ContentId::new("assignment-a"),
    };
    let overlay = experiment.overlay_set().expect("overlay").leases;
    let snapshot = |leases: Vec<AssignmentLease>| {
        vec![(
            PeerId::new("observer"),
            ControlPlaneSnapshot {
                lease_announcements: leases
                    .into_iter()
                    .map(|lease| LeaseAnnouncement {
                        overlay: overlay.clone(),
                        lease,
                        announced_at: now,
                    })
                    .collect(),
                ..ControlPlaneSnapshot::default()
            },
        )]
    };

    let accepted = model::authenticated_update_lease(
        &snapshot(vec![lease.clone()]),
        &experiment,
        &trainer,
        &update,
        &dataset_view_id,
    )
    .expect("one exact lease should authenticate");
    assert_eq!(accepted.assignment_hash, lease.assignment_hash);

    let mut conflicting = lease.clone();
    conflicting.assignment_hash = ContentId::new("assignment-b");
    let conflict = model::authenticated_update_lease(
        &snapshot(vec![lease.clone(), conflicting]),
        &experiment,
        &trainer,
        &update,
        &dataset_view_id,
    )
    .expect_err("conflicting lease bodies must fail closed");
    assert!(
        conflict
            .to_string()
            .contains("conflicting lease announcements")
    );

    let identity = model::authenticated_update_lease(
        &snapshot(vec![lease]),
        &experiment,
        &PeerId::new("different-trainer"),
        &update,
        &dataset_view_id,
    )
    .expect_err("lease ownership must match the candidate origin");
    assert!(
        identity
            .to_string()
            .contains("lease identity does not match")
    );
}

#[derive(Clone, Debug)]
struct NoopCandidateWorkload;

impl P2pWorkload for NoopCandidateWorkload {
    type Device = ();
    type Model = ();
    type Batch = ();
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &Self::Device) -> Self::Model {}

    fn benchmark(&self, _model: &Self::Model, _device: &Self::Device) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["test".into()],
            work_units_per_second: 1.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        _ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        unreachable!("direct candidate selection does not train")
    }

    fn evaluate(&self, _model: &Self::Model, _split: EvalSplit) -> MetricReport {
        unreachable!("test injects candidate evaluation")
    }

    fn apply_patch(&mut self, _patch: &RuntimePatch) -> PatchOutcome {
        PatchOutcome::Applied
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }

    fn runtime_device(&self) -> Self::Device {}

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        anyhow::bail!("not used")
    }

    fn microshard_plan(
        &self,
        _registration: &DatasetRegistration,
    ) -> anyhow::Result<MicroShardPlan> {
        anyhow::bail!("not used")
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        _cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        anyhow::bail!("not used")
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        _descriptor: &ArtifactDescriptor,
        _store: &FsArtifactStore,
        _device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        Ok(model)
    }

    fn materialize_model_artifact(
        &self,
        _model: &Self::Model,
        _artifact_kind: ArtifactKind,
        _head_id: HeadId,
        _base_head_id: Option<HeadId>,
        _store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        anyhow::bail!("not used")
    }

    fn contribution_metrics(
        &self,
        _report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        BTreeMap::new()
    }

    fn supported_workload(&self) -> SupportedWorkload {
        SupportedWorkload {
            workload_id: WorkloadId::new("candidate-test"),
            workload_name: "Candidate Test".into(),
            model_program_hash: ContentId::new("program"),
            checkpoint_format_hash: ContentId::new("checkpoint"),
            supported_revision_family: ContentId::new("revision-family"),
            resource_class: "test".into(),
        }
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("schema")
    }
}

#[test]
fn delta_pack_candidate_without_typed_envelope_is_rejected() {
    let root = std::env::temp_dir().join(format!(
        "burn-p2p-candidate-delta-envelope-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let store = FsArtifactStore::new(&root);
    let base_head_id = HeadId::new("head-base");
    let candidate_head_id = HeadId::new("head-candidate");
    let descriptor = store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                ArtifactKind::DeltaPack,
                Precision::Fp32,
                ContentId::new("schema"),
                "compact-test",
            )
            .with_head(candidate_head_id.clone())
            .with_base_head(base_head_id.clone()),
            std::io::Cursor::new([7_u8]),
            ChunkingScheme::new(1).expect("chunking"),
        )
        .expect("store candidate");
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let base_head = HeadDescriptor {
        head_id: base_head_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: ArtifactId::new("base-artifact"),
        parent_head_id: None,
        global_step: 0,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    };
    let update = UpdateAnnounce {
        peer_id: PeerId::new("trainer"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(1),
        base_head_id: base_head_id.clone(),
        lease_id: Some(LeaseId::new("lease")),
        delta_artifact_id: descriptor.artifact_id.clone(),
        sample_weight: 1.0,
        quality_weight: 1.0,
        norm_stats: UpdateNormStats {
            l2_norm: 0.0,
            max_abs: 0.0,
            clipped: false,
            non_finite_tensors: 0,
        },
        feature_sketch: None,
        receipt_root: ContentId::new("receipt"),
        receipt_ids: Vec::new(),
        providers: Vec::new(),
        announced_at: Utc::now(),
    };
    let head = HeadDescriptor {
        head_id: candidate_head_id,
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: descriptor.artifact_id.clone(),
        parent_head_id: Some(base_head_id),
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    };
    let current_head = Some((PeerId::new("base-provider"), base_head));
    let mut project = NoopCandidateWorkload;
    let error = load_validation_candidate_model(
        &mut project,
        ValidationCandidateLoadArgs {
            experiment: &experiment,
            store: &store,
            device: &(),
            current_head: &current_head,
            revision_contract: None,
            baseline_metrics: None,
            canary_threshold: 0.0,
            evaluate_candidates: false,
            replay_snapshots: &[],
            dataset_cache_dir: root.join("dataset-cache"),
            validator_peer_id: &PeerId::new("validator"),
        },
        ValidationCandidateHead {
            origin_peer_id: PeerId::new("trainer"),
            provider_peer_ids: Vec::new(),
            head,
            update,
            workload_update: None,
        },
    )
    .err()
    .expect("delta pack without typed envelope must fail closed");
    assert!(
        error
            .to_string()
            .contains("missing a typed workload update envelope")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn direct_diffusion_promotion_surfaces_evaluation_metrics_on_head() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let base_head_id = HeadId::new("head-base");
    let base_head = HeadDescriptor {
        head_id: base_head_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: ArtifactId::new("artifact-base"),
        parent_head_id: None,
        global_step: 0,
        created_at: Utc::now(),
        metrics: BTreeMap::from([("train_loss".into(), MetricValue::Float(0.9))]),
    };
    let peer_id = PeerId::new("trainer-a");
    let head = HeadDescriptor {
        head_id: HeadId::new("head-a"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: ArtifactId::new("artifact-a"),
        parent_head_id: Some(base_head_id.clone()),
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::from([("train_loss".into(), MetricValue::Float(0.7))]),
    };
    let update = UpdateAnnounce {
        peer_id: peer_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: WindowId(1),
        base_head_id: base_head_id.clone(),
        lease_id: Some(LeaseId::new("lease-a")),
        delta_artifact_id: head.artifact_id.clone(),
        sample_weight: 1.0,
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
        providers: vec![peer_id.clone()],
        announced_at: Utc::now(),
    };
    let evaluation = MetricReport {
        metrics: BTreeMap::from([
            ("loss".into(), MetricValue::Float(0.25)),
            ("evaluation_batches".into(), MetricValue::Integer(4)),
        ]),
        captured_at: Utc::now(),
    };
    let candidate = ValidationCandidateView {
        peer_id: &peer_id,
        head: &head,
        update: &update,
        evaluation: &evaluation,
        canary_report: None,
        sample_weight: 1.0,
        quality_weight: 1.0,
        model: &(),
        update_evidence: None,
    };
    let mut project = NoopCandidateWorkload;
    let store = FsArtifactStore::new(std::env::temp_dir().join(format!(
        "burn-p2p-direct-promotion-metrics-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));

    let (_, promoted, promoted_evaluation) = select_validation_head(
        &mut project,
        &experiment,
        &store,
        &Some((PeerId::new("provider-base"), base_head)),
        &base_head_id,
        WindowId(1),
        &(),
        &[candidate],
        0,
        MergePolicy::WeightedMean,
        &PeerId::new("validator-a"),
        true,
    )
    .expect("direct candidate selection");

    assert_eq!(promoted.head_id, head.head_id);
    assert_eq!(promoted.artifact_id, head.artifact_id);
    assert_eq!(promoted.parent_head_id, head.parent_head_id);
    assert_eq!(promoted.global_step, head.global_step);
    assert_eq!(promoted.metrics, evaluation.metrics);
    assert_eq!(promoted_evaluation.metrics, evaluation.metrics);
    assert!(!promoted.metrics.contains_key("train_loss"));
}
