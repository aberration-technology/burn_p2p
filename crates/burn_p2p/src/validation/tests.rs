use super::*;

fn metric_report(loss: f64) -> MetricReport {
    MetricReport {
        metrics: BTreeMap::from([(String::from("loss"), MetricValue::Float(loss))]),
        captured_at: Utc::now(),
    }
}

fn prepared_state() -> ValidationPreparedState {
    let root = std::env::temp_dir().join(format!(
        "burn-p2p-validation-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let storage = StorageConfig::new(root);
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    ValidationPreparedState {
        assignment: SlotAssignmentState::new(
            experiment.study_id.clone(),
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
        ),
        storage: storage.clone(),
        store: FsArtifactStore::new(storage.root.clone()),
        local_peer_id: PeerId::new("validator-a"),
        snapshots: Vec::new(),
        visible_snapshots: Vec::new(),
        current_head: Some((
            PeerId::new("source-a"),
            HeadDescriptor {
                head_id: HeadId::new("head-base"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-base"),
                parent_head_id: None,
                global_step: 3,
                created_at: Utc::now(),
                metrics: metric_report(0.4).metrics,
            },
        )),
        base_head_id: HeadId::new("head-base"),
        dataset_view_id: DatasetViewId::new("view-a"),
        merge_window: open_runtime_merge_window(
            &experiment,
            WindowId(4),
            HeadId::new("head-base"),
            MergeTopologyPolicy::default(),
            vec![PeerId::new("reducer-a")],
            vec![PeerId::new("validator-a")],
        )
        .expect("merge window"),
        updates: vec![
            UpdateAnnounce {
                peer_id: PeerId::new("peer-good"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(4),
                base_head_id: HeadId::new("head-base"),
                lease_id: Some(LeaseId::new("lease-good")),
                delta_artifact_id: ArtifactId::new("artifact-good"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                norm_stats: UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new("receipt-root-good"),
                receipt_ids: vec![ContributionReceiptId::new("receipt-good")],
                providers: vec![PeerId::new("peer-good")],
                announced_at: Utc::now(),
            },
            UpdateAnnounce {
                peer_id: PeerId::new("peer-replay"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: WindowId(4),
                base_head_id: HeadId::new("head-base"),
                lease_id: Some(LeaseId::new("lease-replay")),
                delta_artifact_id: ArtifactId::new("artifact-replay"),
                sample_weight: 16.0,
                quality_weight: 1.0,
                norm_stats: UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new("receipt-root-good"),
                receipt_ids: vec![ContributionReceiptId::new("receipt-good")],
                providers: vec![PeerId::new("peer-replay")],
                announced_at: Utc::now(),
            },
        ],
        expected_training_peer_count: 2,
        first_update_announced_at: Some(Utc::now()),
        metrics_retention: MetricsRetentionBudget::default(),
        robustness_policy: RobustnessPolicy::strict(),
        robustness_state: PersistedRobustnessState::default(),
    }
}

#[test]
fn candidate_settle_waits_for_expected_training_peers_within_grace() {
    let mut prepared = prepared_state();
    prepared.expected_training_peer_count = 4;
    prepared.merge_window.policy.publish_jitter_ms = 750;
    let now = Utc::now();
    prepared.first_update_announced_at = Some(now);

    assert!(should_wait_for_candidate_settle(
        &prepared,
        1,
        now,
        AggregateResolutionMode::RequireLocalReduction,
    ));
    assert!(!should_wait_for_candidate_settle(
        &prepared,
        2,
        now + chrono::Duration::milliseconds(50),
        AggregateResolutionMode::RequireLocalReduction,
    ));
    assert!(!should_wait_for_candidate_settle(
        &prepared,
        1,
        now + chrono::Duration::milliseconds(300),
        AggregateResolutionMode::RequireLocalReduction,
    ));
}

#[test]
fn candidate_settle_stops_waiting_after_publish_jitter_grace() {
    let mut prepared = prepared_state();
    prepared.expected_training_peer_count = 5;
    prepared.merge_window.policy.publish_jitter_ms = 500;
    let now = Utc::now();
    prepared.first_update_announced_at = Some(now);

    assert!(!should_wait_for_candidate_settle(
        &prepared,
        1,
        now + chrono::Duration::milliseconds(600),
        AggregateResolutionMode::RequireLocalReduction
    ));
}

#[test]
fn candidate_settle_only_applies_to_reducer_reduction_pass() {
    let mut prepared = prepared_state();
    prepared.expected_training_peer_count = 4;
    prepared.merge_window.policy.publish_jitter_ms = 750;
    let now = Utc::now();
    prepared.first_update_announced_at = Some(now);

    assert!(!should_wait_for_candidate_settle(
        &prepared,
        1,
        now,
        AggregateResolutionMode::PreferRemoteReducerProposal,
    ));
}

#[test]
fn candidate_robustness_rejects_replay_and_keeps_clean_update() {
    let prepared = prepared_state();
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let candidates = [
        ValidationCandidate {
            peer_id: PeerId::new("peer-good"),
            head: HeadDescriptor {
                head_id: HeadId::new("head-good"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-good"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 4,
                created_at: Utc::now(),
                metrics: metric_report(0.35).metrics.clone(),
            },
            update: prepared.updates[0].clone(),
            evaluation: metric_report(0.35),
            canary_report: Some(
                build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new("head-good"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new("artifact-good"),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(0.35).metrics.clone(),
                    },
                    &metric_report(0.35),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
            ),
            sample_weight: 16.0,
            quality_weight: 1.0,
            model: (),
        },
        ValidationCandidate {
            peer_id: PeerId::new("peer-replay"),
            head: HeadDescriptor {
                head_id: HeadId::new("head-replay"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new("artifact-replay"),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 4,
                created_at: Utc::now(),
                metrics: metric_report(0.36).metrics.clone(),
            },
            update: prepared.updates[1].clone(),
            evaluation: metric_report(0.36),
            canary_report: Some(
                build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new("head-replay"),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new("artifact-replay"),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(0.36).metrics.clone(),
                    },
                    &metric_report(0.36),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
            ),
            sample_weight: 16.0,
            quality_weight: 1.0,
            model: (),
        },
    ];

    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let candidate_views = candidates
        .iter()
        .map(ValidationCandidateView::from)
        .collect::<Vec<_>>();
    let CandidateRobustnessOutcome {
        decisions,
        trust_scores: _,
        filtered_updates,
        accepted_weights,
    } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

    assert_eq!(filtered_updates.len(), 1);
    assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-good"));
    assert_eq!(
        decisions
            .iter()
            .find(|decision| decision.peer_id == PeerId::new("peer-replay"))
            .and_then(|decision| decision.rejection_reason.clone()),
        Some(RejectionReason::Replay)
    );
    assert!(
        accepted_weights
            .contains_key(&(PeerId::new("peer-good"), ArtifactId::new("artifact-good")))
    );
}

#[test]
fn candidate_robustness_allows_peer_after_inactive_quarantine_expires() {
    let mut prepared = prepared_state();
    prepared.merge_window.window_id = WindowId(7);
    prepared
        .robustness_policy
        .quarantine_policy
        .quarantine_duration_windows = 2;
    prepared.robustness_state = PersistedRobustnessState {
        peers: BTreeMap::from([(
            PeerId::new("peer-good"),
            PeerRobustnessState {
                trust_score: -3.0,
                consecutive_rejections: 2,
                quarantined: true,
                last_rejection_reason: Some(RejectionReason::Replay),
                updated_at: Some(Utc::now()),
                last_decision_window: Some(WindowId(4)),
                quarantine_started_window: Some(WindowId(4)),
                last_quarantine_window: Some(WindowId(4)),
                ban_recommended: false,
            },
        )]),
    };
    prepared.updates[0].window_id = WindowId(7);
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let candidate = ValidationCandidate {
        peer_id: PeerId::new("peer-good"),
        head: HeadDescriptor {
            head_id: HeadId::new("head-good"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-good"),
            parent_head_id: Some(HeadId::new("head-base")),
            global_step: 4,
            created_at: Utc::now(),
            metrics: metric_report(0.35).metrics.clone(),
        },
        update: prepared.updates[0].clone(),
        evaluation: metric_report(0.35),
        canary_report: Some(
            build_validation_canary_report(
                &experiment,
                &prepared.current_head,
                &HeadDescriptor {
                    head_id: HeadId::new("head-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-good"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.35).metrics.clone(),
                },
                &metric_report(0.35),
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                2,
            )
            .expect("canary"),
        ),
        sample_weight: 16.0,
        quality_weight: 1.0,
        model: (),
    };

    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let candidate_view = ValidationCandidateView::from(&candidate);
    let CandidateRobustnessOutcome {
        decisions,
        filtered_updates,
        ..
    } = evaluate_candidate_robustness(&engine, &prepared, &[candidate_view], Utc::now());

    assert_eq!(filtered_updates.len(), 1);
    assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-good"));
    assert!(
        decisions
            .iter()
            .find(|decision| decision.peer_id == PeerId::new("peer-good"))
            .is_some_and(|decision| decision.accepted)
    );
}

#[test]
fn candidate_robustness_caps_surviving_updates_to_maximum_cohort_size() {
    let mut prepared = prepared_state();
    prepared
        .robustness_policy
        .aggregation_policy
        .maximum_cohort_size = 1;
    prepared.updates = vec![
        UpdateAnnounce {
            peer_id: PeerId::new("peer-a"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            window_id: WindowId(4),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-a")),
            delta_artifact_id: ArtifactId::new("artifact-a"),
            sample_weight: 16.0,
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
            providers: vec![PeerId::new("peer-a")],
            announced_at: Utc::now(),
        },
        UpdateAnnounce {
            peer_id: PeerId::new("peer-b"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            window_id: WindowId(4),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-b")),
            delta_artifact_id: ArtifactId::new("artifact-b"),
            sample_weight: 16.0,
            quality_weight: 1.0,
            norm_stats: UpdateNormStats {
                l2_norm: 1.0,
                max_abs: 1.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: ContentId::new("receipt-root-b"),
            receipt_ids: vec![ContributionReceiptId::new("receipt-b")],
            providers: vec![PeerId::new("peer-b")],
            announced_at: Utc::now(),
        },
        UpdateAnnounce {
            peer_id: PeerId::new("peer-c"),
            study_id: StudyId::new("study-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            window_id: WindowId(4),
            base_head_id: HeadId::new("head-base"),
            lease_id: Some(LeaseId::new("lease-c")),
            delta_artifact_id: ArtifactId::new("artifact-c"),
            sample_weight: 16.0,
            quality_weight: 1.0,
            norm_stats: UpdateNormStats {
                l2_norm: 1.0,
                max_abs: 1.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: ContentId::new("receipt-root-c"),
            receipt_ids: vec![ContributionReceiptId::new("receipt-c")],
            providers: vec![PeerId::new("peer-c")],
            announced_at: Utc::now(),
        },
    ];
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let candidate =
        |peer_id: &str, artifact_id: &str, head_id: &str, loss: f64| ValidationCandidate {
            peer_id: PeerId::new(peer_id),
            head: HeadDescriptor {
                head_id: HeadId::new(head_id),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                artifact_id: ArtifactId::new(artifact_id),
                parent_head_id: Some(HeadId::new("head-base")),
                global_step: 4,
                created_at: Utc::now(),
                metrics: metric_report(loss).metrics.clone(),
            },
            update: prepared
                .updates
                .iter()
                .find(|update| update.peer_id == PeerId::new(peer_id))
                .cloned()
                .expect("candidate update"),
            evaluation: metric_report(loss),
            canary_report: Some(
                build_validation_canary_report(
                    &experiment,
                    &prepared.current_head,
                    &HeadDescriptor {
                        head_id: HeadId::new(head_id),
                        study_id: experiment.study_id.clone(),
                        experiment_id: experiment.experiment_id.clone(),
                        revision_id: experiment.revision_id.clone(),
                        artifact_id: ArtifactId::new(artifact_id),
                        parent_head_id: Some(HeadId::new("head-base")),
                        global_step: 4,
                        created_at: Utc::now(),
                        metrics: metric_report(loss).metrics.clone(),
                    },
                    &metric_report(loss),
                    prepared
                        .robustness_policy
                        .validator_canary_policy
                        .maximum_regression_delta,
                    2,
                )
                .expect("canary"),
            ),
            sample_weight: 16.0,
            quality_weight: 1.0,
            model: (),
        };
    let candidates = [
        candidate("peer-a", "artifact-a", "head-a", 0.35),
        candidate("peer-b", "artifact-b", "head-b", 0.36),
        candidate("peer-c", "artifact-c", "head-c", 0.37),
    ];

    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let candidate_views = candidates
        .iter()
        .map(ValidationCandidateView::from)
        .collect::<Vec<_>>();
    let CandidateRobustnessOutcome {
        decisions,
        filtered_updates,
        accepted_weights,
        ..
    } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

    assert_eq!(filtered_updates.len(), 1);
    assert_eq!(accepted_weights.len(), 1);
    assert_eq!(filtered_updates[0].peer_id, PeerId::new("peer-a"));
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| decision.accepted)
            .count(),
        1
    );
}

#[test]
fn candidate_robustness_caps_browser_contribution_weight() {
    let mut prepared = prepared_state();
    prepared
        .robustness_policy
        .aggregation_policy
        .browser_contribution_cap = 0.25;
    prepared.snapshots = vec![(
        PeerId::new("peer-good"),
        ControlPlaneSnapshot {
            auth_announcements: vec![PeerAuthAnnouncement {
                peer_id: PeerId::new("peer-good"),
                envelope: PeerAuthEnvelope {
                    peer_id: PeerId::new("peer-good"),
                    certificate: NodeCertificate::new(
                        semver::Version::new(0, 1, 0),
                        NodeCertificateClaims {
                            network_id: NetworkId::new("net-a"),
                            project_family_id: ProjectFamilyId::new("family-a"),
                            release_train_hash: ContentId::new("train-a"),
                            target_artifact_hash: ContentId::new("artifact-browser"),
                            peer_id: PeerId::new("peer-good"),
                            peer_public_key_hex: "001122".into(),
                            principal_id: PrincipalId::new("principal-browser"),
                            provider: AuthProvider::Static {
                                authority: "lab-browser".into(),
                            },
                            granted_roles: PeerRoleSet::new([PeerRole::BrowserTrainerWgpu]),
                            experiment_scopes: BTreeSet::from([
                                ExperimentScope::Connect,
                                ExperimentScope::Train {
                                    experiment_id: ExperimentId::new("exp-a"),
                                },
                            ]),
                            client_policy_hash: None,
                            auth_policy_snapshot: None,
                            not_before: Utc::now(),
                            not_after: Utc::now() + chrono::Duration::minutes(10),
                            serial: 1,
                            revocation_epoch: RevocationEpoch(1),
                        },
                        burn_p2p_core::SignatureMetadata {
                            signer: PeerId::new("authority-browser"),
                            key_id: "authority-key".into(),
                            algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                            signed_at: Utc::now(),
                            signature_hex: "deadbeef".into(),
                        },
                    )
                    .expect("node certificate"),
                    client_manifest_id: Some(ContentId::new("manifest-browser")),
                    requested_scopes: BTreeSet::from([ExperimentScope::Train {
                        experiment_id: ExperimentId::new("exp-a"),
                    }]),
                    nonce_hash: ContentId::new("nonce-browser"),
                    challenge_signature_hex: "feedbead".into(),
                    presented_at: Utc::now(),
                },
                announced_at: Utc::now(),
            }],
            ..ControlPlaneSnapshot::default()
        },
    )];
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let candidates = [ValidationCandidate {
        peer_id: PeerId::new("peer-good"),
        head: HeadDescriptor {
            head_id: HeadId::new("head-good"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-good"),
            parent_head_id: Some(HeadId::new("head-base")),
            global_step: 4,
            created_at: Utc::now(),
            metrics: metric_report(0.35).metrics.clone(),
        },
        update: prepared.updates[0].clone(),
        evaluation: metric_report(0.35),
        canary_report: Some(
            build_validation_canary_report(
                &experiment,
                &prepared.current_head,
                &HeadDescriptor {
                    head_id: HeadId::new("head-good"),
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    artifact_id: ArtifactId::new("artifact-good"),
                    parent_head_id: Some(HeadId::new("head-base")),
                    global_step: 4,
                    created_at: Utc::now(),
                    metrics: metric_report(0.35).metrics.clone(),
                },
                &metric_report(0.35),
                prepared
                    .robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta,
                2,
            )
            .expect("canary"),
        ),
        sample_weight: 16.0,
        quality_weight: 1.0,
        model: (),
    }];

    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let candidate_views = candidates
        .iter()
        .map(ValidationCandidateView::from)
        .collect::<Vec<_>>();
    let CandidateRobustnessOutcome {
        accepted_weights,
        filtered_updates,
        ..
    } = evaluate_candidate_robustness(&engine, &prepared, &candidate_views, Utc::now());

    let weight = accepted_weights
        .get(&(PeerId::new("peer-good"), ArtifactId::new("artifact-good")))
        .copied()
        .expect("accepted browser weight");
    assert_eq!(weight, 0.25);
    assert_eq!(filtered_updates[0].sample_weight, 16.0 * 0.25);
}

#[test]
fn quarantine_escalation_emits_peer_and_fraction_alerts() {
    let prepared = prepared_state();
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let mut robustness = ValidationRobustnessExecution {
        decisions: vec![RobustnessDecision {
            peer_id: PeerId::new("peer-suspicious"),
            accepted: false,
            hard_rejected: true,
            downweighted: false,
            quarantined: true,
            rejection_reason: Some(RejectionReason::Replay),
            screen_score: 5.0,
            effective_weight: 0.0,
            effective_norm: 0.0,
            trust_score: Some(TrustScore {
                peer_id: PeerId::new("peer-suspicious"),
                score: -3.0,
                reducer_eligible: false,
                validator_eligible: false,
                quarantined: true,
                ban_recommended: false,
                updated_at: Utc::now(),
            }),
        }],
        trust_scores: vec![TrustScore {
            peer_id: PeerId::new("peer-suspicious"),
            score: -3.0,
            reducer_eligible: false,
            validator_eligible: false,
            quarantined: true,
            ban_recommended: false,
            updated_at: Utc::now(),
        }],
        cohort_report: engine.summarize_cohort(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            prepared.merge_window.window_id,
            &[],
            Utc::now(),
        ),
        canary_report: None,
        replica_agreement: None,
    };

    append_quarantine_escalation_alerts(&experiment, &prepared, &mut robustness, Utc::now());

    assert!(robustness.cohort_report.alerts.iter().any(|alert| {
        alert.peer_id == Some(PeerId::new("peer-suspicious"))
            && alert.reason == RejectionReason::QuarantinedPrincipal
            && alert.severity == RobustnessAlertSeverity::Warn
    }));
    assert!(robustness.cohort_report.alerts.iter().any(|alert| {
        alert.peer_id.is_none()
            && alert.reason == RejectionReason::QuarantinedPrincipal
            && alert.severity == RobustnessAlertSeverity::Critical
    }));
}

#[test]
fn projected_robustness_state_expires_inactive_quarantine() {
    let previous = PersistedRobustnessState {
        peers: BTreeMap::from([(
            PeerId::new("peer-a"),
            PeerRobustnessState {
                trust_score: -3.0,
                consecutive_rejections: 3,
                quarantined: true,
                last_rejection_reason: Some(RejectionReason::Replay),
                updated_at: Some(Utc::now()),
                last_decision_window: Some(WindowId(1)),
                quarantine_started_window: Some(WindowId(1)),
                last_quarantine_window: Some(WindowId(1)),
                ban_recommended: false,
            },
        )]),
    };

    let (next, trust_scores) = project_robustness_state(
        &previous,
        &[],
        &burn_p2p_core::ReputationPolicy::default(),
        &QuarantinePolicy::default(),
        WindowId(5),
    );
    let peer = next.peers.get(&PeerId::new("peer-a")).expect("peer state");

    assert!(trust_scores.is_empty());
    assert!(!peer.quarantined);
    assert_eq!(peer.consecutive_rejections, 0);
    assert_eq!(peer.last_rejection_reason, None);
    assert!(peer.quarantine_started_window.is_none());
    assert!(peer.last_quarantine_window.is_none());
    assert!(!peer.ban_recommended);
}

#[test]
fn quarantine_escalation_emits_ban_recommendation_alert_once_due() {
    let mut prepared = prepared_state();
    prepared.merge_window.window_id = WindowId(9);
    prepared
        .robustness_policy
        .quarantine_policy
        .ban_after_quarantine_windows = 4;
    prepared.robustness_state = PersistedRobustnessState {
        peers: BTreeMap::from([(
            PeerId::new("peer-repeat"),
            PeerRobustnessState {
                trust_score: -4.5,
                consecutive_rejections: 4,
                quarantined: true,
                last_rejection_reason: Some(RejectionReason::Replay),
                updated_at: Some(Utc::now()),
                last_decision_window: Some(WindowId(9)),
                quarantine_started_window: Some(WindowId(1)),
                last_quarantine_window: Some(WindowId(9)),
                ban_recommended: false,
            },
        )]),
    };
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let mut robustness = ValidationRobustnessExecution {
        decisions: Vec::new(),
        trust_scores: Vec::new(),
        cohort_report: engine.summarize_cohort(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            prepared.merge_window.window_id,
            &[],
            Utc::now(),
        ),
        canary_report: None,
        replica_agreement: None,
    };

    append_quarantine_escalation_alerts(&experiment, &prepared, &mut robustness, Utc::now());

    assert!(robustness.cohort_report.alerts.iter().any(|alert| {
        alert.peer_id == Some(PeerId::new("peer-repeat"))
            && alert.reason == RejectionReason::QuarantinedPrincipal
            && alert.severity == RobustnessAlertSeverity::Critical
            && alert.message.contains("recommend operator ban")
    }));
}

#[test]
fn projected_robustness_state_is_idempotent_within_one_window() {
    let evaluated_at = Utc::now();
    let previous = PersistedRobustnessState::default();
    let decision = RobustnessDecision {
        peer_id: PeerId::new("peer-a"),
        accepted: false,
        hard_rejected: true,
        downweighted: false,
        quarantined: false,
        rejection_reason: Some(RejectionReason::Replay),
        screen_score: 4.0,
        effective_weight: 0.0,
        effective_norm: 0.0,
        trust_score: Some(TrustScore {
            peer_id: PeerId::new("peer-a"),
            score: -3.5,
            reducer_eligible: false,
            validator_eligible: false,
            quarantined: false,
            ban_recommended: false,
            updated_at: evaluated_at,
        }),
    };

    let (next, first_scores) = project_robustness_state(
        &previous,
        std::slice::from_ref(&decision),
        &burn_p2p_core::ReputationPolicy::default(),
        &QuarantinePolicy::default(),
        WindowId(1),
    );
    let first_peer = next.peers.get(&PeerId::new("peer-a")).expect("peer state");
    assert_eq!(first_peer.consecutive_rejections, 1);
    assert_eq!(first_peer.last_decision_window, Some(WindowId(1)));
    assert_eq!(first_scores.len(), 1);

    let (replayed, second_scores) = project_robustness_state(
        &next,
        &[decision],
        &burn_p2p_core::ReputationPolicy::default(),
        &QuarantinePolicy::default(),
        WindowId(1),
    );
    let replayed_peer = replayed
        .peers
        .get(&PeerId::new("peer-a"))
        .expect("peer state");

    assert_eq!(replayed_peer.consecutive_rejections, 1);
    assert!(!replayed_peer.quarantined);
    assert_eq!(second_scores.len(), 1);
    assert_eq!(second_scores[0].score, first_peer.trust_score);
    assert_eq!(second_scores[0].quarantined, first_peer.quarantined);
}

#[test]
fn canary_escalation_emits_critical_alert_when_promotion_pauses() {
    let prepared = prepared_state();
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let canary_report = build_validation_canary_report(
        &experiment,
        &prepared.current_head,
        &HeadDescriptor {
            head_id: HeadId::new("head-bad"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-bad"),
            parent_head_id: Some(HeadId::new("head-base")),
            global_step: 5,
            created_at: Utc::now(),
            metrics: metric_report(0.9).metrics.clone(),
        },
        &metric_report(0.9),
        prepared
            .robustness_policy
            .validator_canary_policy
            .maximum_regression_delta,
        1,
    )
    .expect("canary");
    let mut robustness = ValidationRobustnessExecution {
        decisions: Vec::new(),
        trust_scores: Vec::new(),
        cohort_report: engine.summarize_cohort(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            prepared.merge_window.window_id,
            &[],
            Utc::now(),
        ),
        canary_report: Some(canary_report.clone()),
        replica_agreement: None,
    };

    append_canary_escalation_alert(
        &experiment,
        &prepared,
        &mut robustness,
        &canary_report,
        Utc::now(),
    );

    assert!(robustness.cohort_report.alerts.iter().any(|alert| {
        alert.reason == RejectionReason::CanaryRegression
            && alert.severity == RobustnessAlertSeverity::Critical
    }));
}

#[test]
fn replica_disagreement_from_reducer_snapshots_emits_alert() {
    let mut prepared = prepared_state();
    prepared.merge_window.reducers = vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")];
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let aggregate = |peer_id: &str, artifact_id: &str| AggregateEnvelope {
        aggregate_id: ContentId::new(format!("aggregate-{artifact_id}")),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_artifact_id: ArtifactId::new(artifact_id),
        tier: AggregateTier::RootCandidate,
        reducer_peer_id: PeerId::new(peer_id),
        contributor_peers: vec![PeerId::new("peer-good")],
        child_aggregate_ids: Vec::new(),
        stats: AggregateStats {
            accepted_updates: 1,
            duplicate_updates: 0,
            dropped_updates: 0,
            late_updates: 0,
            sum_sample_weight: 1.0,
            sum_quality_weight: 1.0,
            sum_weighted_delta_norm: 1.0,
            max_update_norm: 1.0,
            accepted_sample_coverage: 1.0,
        },
        providers: vec![PeerId::new(peer_id)],
        published_at: Utc::now(),
    };
    prepared.snapshots = vec![
        (
            PeerId::new("reducer-a"),
            ControlPlaneSnapshot {
                aggregate_proposal_announcements: vec![AggregateProposalAnnouncement {
                    overlay: burn_p2p_swarm::OverlayTopic::control(experiment.network_id.clone()),
                    proposal: aggregate("reducer-a", "artifact-a"),
                    announced_at: Utc::now(),
                }],
                ..ControlPlaneSnapshot::default()
            },
        ),
        (
            PeerId::new("reducer-b"),
            ControlPlaneSnapshot {
                aggregate_proposal_announcements: vec![AggregateProposalAnnouncement {
                    overlay: burn_p2p_swarm::OverlayTopic::control(experiment.network_id.clone()),
                    proposal: aggregate("reducer-b", "artifact-b"),
                    announced_at: Utc::now(),
                }],
                ..ControlPlaneSnapshot::default()
            },
        ),
    ];
    let engine = burn_p2p_security::RobustnessEngine::new(prepared.robustness_policy.clone());
    let mut robustness = ValidationRobustnessExecution {
        decisions: Vec::new(),
        trust_scores: Vec::new(),
        cohort_report: engine.summarize_cohort(
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            prepared.merge_window.window_id,
            &[],
            Utc::now(),
        ),
        canary_report: None,
        replica_agreement: observed_replica_agreement(
            &prepared.snapshots,
            &experiment,
            &prepared.merge_window,
            &prepared.base_head_id,
        ),
    };

    append_replica_disagreement_alert(&experiment, &prepared, &mut robustness, Utc::now());

    assert_eq!(robustness.replica_agreement, Some(0.5));
    assert!(robustness.cohort_report.alerts.iter().any(|alert| {
        alert.reason == RejectionReason::ReplicaDisagreement
            && alert.severity == RobustnessAlertSeverity::Critical
    }));
}

#[test]
fn canary_report_flags_regression_above_threshold() {
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let current_head = Some((
        PeerId::new("peer-base"),
        HeadDescriptor {
            head_id: HeadId::new("head-base"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-base"),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: metric_report(0.2).metrics,
        },
    ));
    let report = build_validation_canary_report(
        &experiment,
        &current_head,
        &HeadDescriptor {
            head_id: HeadId::new("head-candidate"),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new("artifact-candidate"),
            parent_head_id: Some(HeadId::new("head-base")),
            global_step: 2,
            created_at: Utc::now(),
            metrics: metric_report(0.5).metrics.clone(),
        },
        &metric_report(0.5),
        0.05,
        2,
    )
    .expect("report");

    assert!(!report.accepted);
    assert!(report.regression_margin > 0.05);
}

#[test]
fn merge_certificate_ids_are_validator_scoped() {
    let prepared = prepared_state();
    let experiment = ExperimentHandle {
        network_id: NetworkId::new("net-a"),
        study_id: StudyId::new("study-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
    };
    let merged_head = HeadDescriptor {
        head_id: HeadId::new("head-merged"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: ArtifactId::new("artifact-merged"),
        parent_head_id: Some(prepared.base_head_id.clone()),
        global_step: 9,
        created_at: Utc::now(),
        metrics: metric_report(0.1).metrics,
    };
    let contribution = build_validation_contribution(
        &experiment,
        &PeerId::new("trainer-a"),
        &merged_head,
        &metric_report(0.1),
    );

    let first = build_validation_merge_certificate(
        &experiment,
        &prepared.merge_window,
        &PeerId::new("validator-a"),
        &prepared.base_head_id,
        &merged_head,
        MergePolicy::QualityWeightedEma,
        &contribution,
        &[],
    );
    let second = build_validation_merge_certificate(
        &experiment,
        &prepared.merge_window,
        &PeerId::new("validator-b"),
        &prepared.base_head_id,
        &merged_head,
        MergePolicy::QualityWeightedEma,
        &contribution,
        &[],
    );

    assert_ne!(first.merge_cert_id, second.merge_cert_id);
    assert_eq!(
        first.contribution_receipts,
        vec![contribution.receipt_id.clone()]
    );
}
