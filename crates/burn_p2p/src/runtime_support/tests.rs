use super::placement::{
    FLEET_PLACEMENT_PLANNER_VERSION, FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
    TRAINING_PLACEMENT_HISTORY_WINDOWS,
};
use super::*;
use burn_p2p_core::{
    AuthProvider, BackendClass, ContentId, FleetPlacementPeer, FleetPlacementSnapshot,
    MetricsLiveEvent, MetricsLiveEventKind, NodeCertificate, NodeCertificateClaims,
    PeerAuthEnvelope, PrincipalId, ProjectFamilyId, RevocationEpoch, SignatureAlgorithm,
    SignatureMetadata,
};
use burn_p2p_experiment::{
    ActivationTarget, FleetScheduleAssignment, FleetScheduleEpoch, FleetScheduleEpochEnvelope,
};

fn test_snapshot(roles: impl IntoIterator<Item = PeerRole>) -> NodeTelemetrySnapshot {
    let mut snapshot = NodeTelemetrySnapshot::starting(
        &MainnetHandle {
            genesis: GenesisSpec {
                network_id: NetworkId::new("net-test"),
                protocol_version: Version::new(1, 0, 0),
                display_name: String::from("test"),
                created_at: Utc::now(),
                metadata: BTreeMap::new(),
            },
            roles: PeerRoleSet::new(roles),
        },
        &NodeConfig::default(),
    );
    snapshot.local_peer_id = Some(PeerId::new("local"));
    snapshot
}

fn test_experiment_handle() -> ExperimentHandle {
    ExperimentHandle {
        network_id: NetworkId::new("net-test"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("experiment"),
        revision_id: RevisionId::new("revision"),
    }
}

fn trust_score(
    peer_id: &str,
    reducer_eligible: bool,
    validator_eligible: bool,
    quarantined: bool,
) -> TrustScore {
    TrustScore {
        peer_id: PeerId::new(peer_id),
        score: if quarantined { -4.0 } else { 0.5 },
        reducer_eligible,
        validator_eligible,
        quarantined,
        ban_recommended: false,
        updated_at: Utc::now(),
    }
}

fn advertise_peer(snapshot: &mut NodeTelemetrySnapshot, peer_id: &str, roles: &[PeerRole]) {
    snapshot
        .control_plane
        .peer_directory_announcements
        .push(PeerDirectoryAnnouncement {
            network_id: NetworkId::new("net-test"),
            peer_id: PeerId::new(peer_id),
            addresses: vec![
                SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/4{:03}", peer_id.len()))
                    .expect("peer directory address"),
            ],
            advertised_roles: Some(PeerRoleSet::new(roles.iter().cloned())),
            announced_at: Utc::now(),
        });
}

fn advertise_auth_peer(
    snapshot: &mut NodeTelemetrySnapshot,
    peer_id: &str,
    keypair: &Keypair,
    announced_at: DateTime<Utc>,
) {
    let peer_id = PeerId::new(peer_id);
    let certificate = NodeCertificate::new(
        Version::new(1, 0, 0),
        NodeCertificateClaims {
            network_id: NetworkId::new("net-test"),
            project_family_id: ProjectFamilyId::new("family-test"),
            release_train_hash: ContentId::new("train-test"),
            target_artifact_hash: ContentId::new("artifact-test"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: hex::encode(keypair.public().encode_protobuf()),
            principal_id: PrincipalId::new(format!("principal-{}", peer_id.as_str())),
            provider: AuthProvider::Static {
                authority: "test".into(),
            },
            granted_roles: PeerRoleSet::new([PeerRole::TrainerCpu]),
            experiment_scopes: BTreeSet::new(),
            client_policy_hash: None,
            auth_policy_snapshot: None,
            not_before: announced_at,
            not_after: announced_at + chrono::Duration::hours(1),
            serial: 1,
            revocation_epoch: RevocationEpoch(0),
        },
        SignatureMetadata {
            signer: peer_id.clone(),
            key_id: "test".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: announced_at,
            signature_hex: "00".into(),
        },
    )
    .expect("node certificate");
    snapshot
        .control_plane
        .auth_announcements
        .push(PeerAuthAnnouncement {
            peer_id: peer_id.clone(),
            envelope: PeerAuthEnvelope {
                peer_id,
                certificate,
                client_manifest_id: None,
                requested_scopes: BTreeSet::new(),
                nonce_hash: ContentId::new("nonce-test"),
                challenge_signature_hex: "00".into(),
                presented_at: announced_at,
            },
            announced_at,
        });
}

fn publish_schedule_epoch(
    snapshot: &mut NodeTelemetrySnapshot,
    epoch: FleetScheduleEpoch,
    announced_at: DateTime<Utc>,
) {
    let certificate = FleetScheduleEpochEnvelope {
        network_id: NetworkId::new("net-test"),
        epoch,
    }
    .into_signed_cert(
        SignatureMetadata {
            signer: PeerId::new("authority"),
            key_id: "schedule".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: announced_at,
            signature_hex: "00".into(),
        },
        Version::new(1, 0, 0),
    )
    .expect("schedule certificate");

    snapshot
        .control_plane
        .schedule_announcements
        .push(FleetScheduleAnnouncement {
            overlay: OverlayTopic::control(NetworkId::new("net-test")),
            certificate,
            announced_at,
        });
}

#[test]
fn runtime_topology_peers_exclude_quarantined_and_reducer_demoted_peers() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([
        PeerId::new("peer-good"),
        PeerId::new("peer-quarantined"),
        PeerId::new("peer-validator-only"),
    ]);
    snapshot.connected_peer_ids = snapshot.observed_peer_ids.clone();
    snapshot.connected_peers = snapshot.connected_peer_ids.len();
    snapshot.trust_scores = vec![
        trust_score("peer-quarantined", false, false, true),
        trust_score("peer-validator-only", false, true, false),
    ];
    advertise_peer(&mut snapshot, "peer-good", &[PeerRole::Reducer]);
    advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::Reducer]);
    advertise_peer(&mut snapshot, "peer-validator-only", &[PeerRole::Validator]);

    let peers = runtime_topology_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::Reducer]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("local")));
    assert!(peers.contains(&PeerId::new("peer-good")));
    assert!(!peers.contains(&PeerId::new("peer-quarantined")));
    assert!(!peers.contains(&PeerId::new("peer-validator-only")));
}

#[test]
fn runtime_training_peers_exclude_non_trainers_and_quarantined_peers() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([
        PeerId::new("peer-trainer"),
        PeerId::new("peer-browser-trainer"),
        PeerId::new("peer-validator"),
        PeerId::new("peer-quarantined"),
    ]);
    snapshot.trust_scores = vec![trust_score("peer-quarantined", false, false, true)];
    advertise_peer(&mut snapshot, "peer-trainer", &[PeerRole::TrainerGpu]);
    advertise_peer(
        &mut snapshot,
        "peer-browser-trainer",
        &[PeerRole::BrowserTrainerWgpu],
    );
    advertise_peer(&mut snapshot, "peer-validator", &[PeerRole::Validator]);
    advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::TrainerCpu]);

    let peers = runtime_training_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("local")));
    assert!(peers.contains(&PeerId::new("peer-trainer")));
    assert!(peers.contains(&PeerId::new("peer-browser-trainer")));
    assert!(!peers.contains(&PeerId::new("peer-validator")));
    assert!(!peers.contains(&PeerId::new("peer-quarantined")));
}

#[test]
fn reducer_authority_windows_clamp_to_one_deterministic_reducer() {
    let experiment = test_experiment_handle();
    let policy = MergeTopologyPolicy {
        reducer_replication: 3,
        promotion_policy: HeadPromotionPolicy {
            mode: HeadPromotionMode::ReducerAuthority,
            validator_quorum: 4,
            ..HeadPromotionPolicy::default()
        },
        ..MergeTopologyPolicy::default()
    };

    let first = open_runtime_merge_window(
        &experiment,
        WindowId(7),
        HeadId::new("head-a"),
        policy.clone(),
        vec![
            PeerId::new("reducer-c"),
            PeerId::new("reducer-a"),
            PeerId::new("reducer-b"),
        ],
        vec![PeerId::new("validator-a"), PeerId::new("validator-b")],
    )
    .expect("first reducer-authority window");
    let second = open_runtime_merge_window(
        &experiment,
        WindowId(7),
        HeadId::new("head-a"),
        policy,
        vec![
            PeerId::new("reducer-b"),
            PeerId::new("reducer-c"),
            PeerId::new("reducer-a"),
        ],
        vec![PeerId::new("validator-b"), PeerId::new("validator-a")],
    )
    .expect("second reducer-authority window");

    assert_eq!(first.policy.reducer_replication, 1);
    assert_eq!(first.policy.promotion_policy.validator_quorum, 1);
    assert!(first.validators.is_empty());
    assert_eq!(first.reducers.len(), 1);
    assert_eq!(first.reducers, second.reducers);
}

#[test]
fn diffusion_steady_state_windows_clear_reducers_and_validators() {
    let experiment = test_experiment_handle();
    let policy = MergeTopologyPolicy {
        strategy: MergeStrategy::KRegularGossip,
        reducer_replication: 3,
        upper_fanin: 5,
        promotion_policy: HeadPromotionPolicy {
            mode: HeadPromotionMode::DiffusionSteadyState,
            validator_quorum: 4,
            diffusion: Some(DiffusionSteadyStatePolicy::default()),
            ..HeadPromotionPolicy::default()
        },
        ..MergeTopologyPolicy::default()
    };

    let window = open_runtime_merge_window(
        &experiment,
        WindowId(9),
        HeadId::new("head-diffusion"),
        policy,
        vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")],
        vec![PeerId::new("validator-a"), PeerId::new("validator-b")],
    )
    .expect("diffusion steady-state window");

    assert!(window.reducers.is_empty());
    assert!(window.validators.is_empty());
    assert_eq!(window.policy.reducer_replication, 0);
    assert_eq!(window.policy.upper_fanin, 0);
    assert_eq!(window.policy.promotion_policy.validator_quorum, 0);
    assert_eq!(
        window.policy.promotion_policy.diffusion,
        Some(DiffusionSteadyStatePolicy::default())
    );
}

#[test]
fn diffusion_steady_state_rejects_reducer_centric_strategies() {
    let experiment = test_experiment_handle();
    let policy = MergeTopologyPolicy {
        strategy: MergeStrategy::ReplicatedRendezvousDag,
        promotion_policy: HeadPromotionPolicy {
            mode: HeadPromotionMode::DiffusionSteadyState,
            diffusion: Some(DiffusionSteadyStatePolicy::default()),
            ..HeadPromotionPolicy::default()
        },
        ..MergeTopologyPolicy::default()
    };

    let error = open_runtime_merge_window(
        &experiment,
        WindowId(3),
        HeadId::new("head-invalid"),
        policy,
        vec![PeerId::new("reducer-a")],
        vec![],
    )
    .expect_err("diffusion steady-state should reject reducer-centric strategy");

    assert!(
        error.to_string().contains(
            "diffusion steady-state promotion requires a trainer-only gossip or broadcast topology"
        ),
        "unexpected error: {error}",
    );
}

fn publish_training_hint(
    snapshot: &mut NodeTelemetrySnapshot,
    peer_id: &str,
    status: PeerWindowStatus,
    accepted_tokens_or_samples: u64,
    window_elapsed_ms: u64,
    head_lag_at_finish: u64,
    window_finished_at: DateTime<Utc>,
) {
    snapshot
        .control_plane
        .metrics_announcements
        .push(MetricsAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("net-test"),
                StudyId::new("study-test"),
                ExperimentId::new("exp-test"),
                OverlayChannel::Metrics,
            )
            .expect("metrics overlay"),
            event: MetricsLiveEvent {
                network_id: NetworkId::new("net-test"),
                kind: MetricsLiveEventKind::LedgerAppend,
                cursors: Vec::new(),
                generated_at: window_finished_at,
            },
            peer_window_hints: vec![PeerWindowPlacementHint {
                peer_id: PeerId::new(peer_id),
                role: PeerRole::TrainerCpu,
                backend_class: BackendClass::Cpu,
                status,
                accepted_tokens_or_samples,
                window_elapsed_ms,
                head_lag_at_finish,
                window_finished_at,
            }],
            placement_snapshot: None,
        });
}

fn publish_signed_placement_snapshot(
    snapshot: &mut NodeTelemetrySnapshot,
    planner_peer_id: &str,
    planner_keypair: &Keypair,
    selected_peer_ids: &[&str],
    generated_at: DateTime<Utc>,
) {
    let mut placement = FleetPlacementSnapshot {
        network_id: NetworkId::new("net-test"),
        planner_peer_id: PeerId::new(planner_peer_id),
        generated_at,
        freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
        retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
        selected_peer_ids: selected_peer_ids
            .iter()
            .map(|peer_id| PeerId::new(*peer_id))
            .collect(),
        ranked_candidates: Vec::new(),
        planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
        signature_bundle: Vec::new(),
    };
    placement
        .signature_bundle
        .push(sign_fleet_placement_snapshot(planner_keypair, &placement).expect("signature"));
    snapshot
        .control_plane
        .metrics_announcements
        .push(MetricsAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("net-test"),
                StudyId::new("study-test"),
                ExperimentId::new("exp-test"),
                OverlayChannel::Metrics,
            )
            .expect("metrics overlay"),
            event: MetricsLiveEvent {
                network_id: NetworkId::new("net-test"),
                kind: MetricsLiveEventKind::LedgerAppend,
                cursors: Vec::new(),
                generated_at,
            },
            peer_window_hints: Vec::new(),
            placement_snapshot: Some(placement),
        });
}

fn publish_ranked_signed_placement_snapshot(
    snapshot: &mut NodeTelemetrySnapshot,
    planner_peer_id: &str,
    planner_keypair: &Keypair,
    selected_peer_ids: &[&str],
    ranked_candidates: &[(&str, f64, f64, f64)],
    generated_at: DateTime<Utc>,
) {
    publish_scaled_signed_placement_snapshot(
        snapshot,
        planner_peer_id,
        planner_keypair,
        selected_peer_ids,
        &ranked_candidates
            .iter()
            .map(|(peer_id, health, lag, throughput)| {
                (*peer_id, *health, *lag, *throughput, 1.0, 1.0)
            })
            .collect::<Vec<_>>(),
        generated_at,
    );
}

fn publish_scaled_signed_placement_snapshot(
    snapshot: &mut NodeTelemetrySnapshot,
    planner_peer_id: &str,
    planner_keypair: &Keypair,
    selected_peer_ids: &[&str],
    ranked_candidates: &[(&str, f64, f64, f64, f64, f64)],
    generated_at: DateTime<Utc>,
) {
    let mut placement = FleetPlacementSnapshot {
        network_id: NetworkId::new("net-test"),
        planner_peer_id: PeerId::new(planner_peer_id),
        generated_at,
        freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
        retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
        selected_peer_ids: selected_peer_ids
            .iter()
            .map(|peer_id| PeerId::new(*peer_id))
            .collect(),
        ranked_candidates: ranked_candidates
            .iter()
            .map(
                |(peer_id, health, lag, throughput, budget_scale, microshard_scale)| {
                    FleetPlacementPeer {
                        peer_id: PeerId::new(*peer_id),
                        role: PeerRole::TrainerCpu,
                        backend_class: BackendClass::Cpu,
                        recent_window_count: 4,
                        completed_window_count: 4,
                        recent_failure_streak: 0,
                        latest_status: PeerWindowStatus::Completed,
                        latest_finished_at: generated_at,
                        weighted_health: *health,
                        weighted_head_lag: *lag,
                        weighted_throughput: *throughput,
                        trust_score: 0.0,
                        recommended_budget_scale: *budget_scale,
                        recommended_microshard_scale: *microshard_scale,
                    }
                },
            )
            .collect(),
        planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
        signature_bundle: Vec::new(),
    };
    placement
        .signature_bundle
        .push(sign_fleet_placement_snapshot(planner_keypair, &placement).expect("signature"));
    snapshot
        .control_plane
        .metrics_announcements
        .push(MetricsAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("net-test"),
                StudyId::new("study-test"),
                ExperimentId::new("exp-test"),
                OverlayChannel::Metrics,
            )
            .expect("metrics overlay"),
            event: MetricsLiveEvent {
                network_id: NetworkId::new("net-test"),
                kind: MetricsLiveEventKind::LedgerAppend,
                cursors: Vec::new(),
                generated_at,
            },
            peer_window_hints: Vec::new(),
            placement_snapshot: Some(placement),
        });
}

#[test]
fn runtime_training_assignment_peers_prune_recent_failed_trainers() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([
        PeerId::new("peer-fast"),
        PeerId::new("peer-failed"),
        PeerId::new("peer-unknown"),
    ]);
    advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-failed", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-unknown", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-fast",
        PeerWindowStatus::Completed,
        120,
        1_000,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-failed",
        PeerWindowStatus::Failed,
        0,
        1_000,
        0,
        now,
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("local")));
    assert!(peers.contains(&PeerId::new("peer-fast")));
    assert!(peers.contains(&PeerId::new("peer-unknown")));
    assert!(!peers.contains(&PeerId::new("peer-failed")));
}

#[test]
fn runtime_training_assignment_peers_ignore_stale_failed_hints() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-stale-failed")]);
    advertise_peer(&mut snapshot, "peer-stale-failed", &[PeerRole::TrainerCpu]);
    publish_training_hint(
        &mut snapshot,
        "peer-stale-failed",
        PeerWindowStatus::Failed,
        0,
        1_000,
        0,
        Utc::now() - chrono::Duration::minutes(60),
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("peer-stale-failed")));
}

#[test]
fn runtime_training_assignment_peers_keep_single_recent_failure_with_healthy_history() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-recovering")]);
    advertise_peer(&mut snapshot, "peer-recovering", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-recovering",
        PeerWindowStatus::Failed,
        0,
        1_000,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-recovering",
        PeerWindowStatus::Completed,
        128,
        900,
        0,
        now - chrono::Duration::seconds(30),
    );
    publish_training_hint(
        &mut snapshot,
        "peer-recovering",
        PeerWindowStatus::Completed,
        128,
        950,
        0,
        now - chrono::Duration::minutes(1),
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("peer-recovering")));
}

#[test]
fn runtime_training_assignment_peers_prefer_consistent_completed_history() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids =
        BTreeSet::from([PeerId::new("peer-consistent"), PeerId::new("peer-bursty")]);
    advertise_peer(&mut snapshot, "peer-consistent", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-bursty", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-consistent",
        PeerWindowStatus::Completed,
        128,
        1_000,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-consistent",
        PeerWindowStatus::Completed,
        120,
        950,
        0,
        now - chrono::Duration::seconds(30),
    );
    publish_training_hint(
        &mut snapshot,
        "peer-bursty",
        PeerWindowStatus::Completed,
        180,
        900,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-bursty",
        PeerWindowStatus::Failed,
        0,
        900,
        0,
        now - chrono::Duration::seconds(30),
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    let consistent_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-consistent"))
        .expect("peer-consistent present");
    let bursty_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-bursty"))
        .expect("peer-bursty present");

    assert!(consistent_index < bursty_index);
}

#[test]
fn runtime_training_assignment_peers_accept_verified_signed_snapshot_ranking() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
    advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
    let planner_keypair = Keypair::generate_ed25519();
    let planner_peer_id = PeerId::new(
        libp2p_identity::PeerId::from_public_key(&planner_keypair.public()).to_string(),
    );
    let now = Utc::now();
    advertise_auth_peer(
        &mut snapshot,
        planner_peer_id.as_str(),
        &planner_keypair,
        now,
    );
    publish_signed_placement_snapshot(
        &mut snapshot,
        planner_peer_id.as_str(),
        &planner_keypair,
        &["peer-b", "peer-a"],
        now,
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );
    let peer_a_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-a"))
        .expect("peer-a present");
    let peer_b_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-b"))
        .expect("peer-b present");

    assert!(peer_b_index < peer_a_index);
}

#[test]
fn runtime_training_assignment_peers_ignore_unverified_signed_snapshot_ranking() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
    advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
    let trusted_planner = Keypair::generate_ed25519();
    let untrusted_planner = Keypair::generate_ed25519();
    let planner_peer_id = PeerId::new(
        libp2p_identity::PeerId::from_public_key(&trusted_planner.public()).to_string(),
    );
    let now = Utc::now();
    advertise_auth_peer(
        &mut snapshot,
        planner_peer_id.as_str(),
        &trusted_planner,
        now,
    );
    publish_signed_placement_snapshot(
        &mut snapshot,
        planner_peer_id.as_str(),
        &untrusted_planner,
        &["peer-b", "peer-a"],
        now,
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );
    let peer_a_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-a"))
        .expect("peer-a present");
    let peer_b_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-b"))
        .expect("peer-b present");

    assert!(peer_a_index < peer_b_index);
}

#[test]
fn runtime_training_assignment_peers_use_decayed_multi_planner_history() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
    advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
    let planner_a = Keypair::generate_ed25519();
    let planner_b = Keypair::generate_ed25519();
    let planner_c = Keypair::generate_ed25519();
    let planner_a_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_a.public()).to_string());
    let planner_b_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_b.public()).to_string());
    let planner_c_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_c.public()).to_string());
    let now = Utc::now();
    advertise_auth_peer(&mut snapshot, planner_a_peer.as_str(), &planner_a, now);
    advertise_auth_peer(
        &mut snapshot,
        planner_b_peer.as_str(),
        &planner_b,
        now - chrono::Duration::seconds(30),
    );
    advertise_auth_peer(
        &mut snapshot,
        planner_c_peer.as_str(),
        &planner_c,
        now - chrono::Duration::seconds(60),
    );
    publish_ranked_signed_placement_snapshot(
        &mut snapshot,
        planner_a_peer.as_str(),
        &planner_a,
        &["peer-b"],
        &[("peer-b", 0.95, 0.0, 160.0), ("peer-a", 0.8, 0.0, 140.0)],
        now,
    );
    publish_ranked_signed_placement_snapshot(
        &mut snapshot,
        planner_b_peer.as_str(),
        &planner_b,
        &["peer-b"],
        &[("peer-b", 0.94, 0.0, 150.0), ("peer-a", 0.8, 0.0, 130.0)],
        now - chrono::Duration::seconds(30),
    );
    publish_ranked_signed_placement_snapshot(
        &mut snapshot,
        planner_c_peer.as_str(),
        &planner_c,
        &["peer-a"],
        &[("peer-a", 0.9, 0.0, 155.0), ("peer-b", 0.85, 0.0, 145.0)],
        now - chrono::Duration::seconds(60),
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );
    let peer_a_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-a"))
        .expect("peer-a present");
    let peer_b_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-b"))
        .expect("peer-b present");

    assert!(peer_b_index < peer_a_index);
}

#[test]
fn runtime_training_assignment_peers_prefer_distinct_planner_consensus_over_single_planner_volume()
{
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-a"), PeerId::new("peer-b")]);
    advertise_peer(&mut snapshot, "peer-a", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-b", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-a",
        PeerWindowStatus::Completed,
        128,
        950,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-b",
        PeerWindowStatus::Completed,
        126,
        955,
        0,
        now,
    );

    let planner_a = Keypair::generate_ed25519();
    let planner_b = Keypair::generate_ed25519();
    let planner_c = Keypair::generate_ed25519();
    let planner_a_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_a.public()).to_string());
    let planner_b_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_b.public()).to_string());
    let planner_c_peer =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner_c.public()).to_string());
    advertise_auth_peer(&mut snapshot, planner_a_peer.as_str(), &planner_a, now);
    advertise_auth_peer(
        &mut snapshot,
        planner_b_peer.as_str(),
        &planner_b,
        now - chrono::Duration::seconds(10),
    );
    advertise_auth_peer(
        &mut snapshot,
        planner_c_peer.as_str(),
        &planner_c,
        now - chrono::Duration::seconds(20),
    );

    for offset in [0, 15, 30, 45] {
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_a_peer.as_str(),
            &planner_a,
            &["peer-a"],
            &[("peer-a", 0.96, 0.0, 175.0), ("peer-b", 0.85, 0.0, 170.0)],
            now - chrono::Duration::seconds(offset),
        );
    }
    publish_ranked_signed_placement_snapshot(
        &mut snapshot,
        planner_b_peer.as_str(),
        &planner_b,
        &["peer-b"],
        &[("peer-b", 0.97, 0.0, 178.0), ("peer-a", 0.85, 0.0, 168.0)],
        now - chrono::Duration::seconds(5),
    );
    publish_ranked_signed_placement_snapshot(
        &mut snapshot,
        planner_c_peer.as_str(),
        &planner_c,
        &["peer-b"],
        &[("peer-b", 0.95, 0.0, 176.0), ("peer-a", 0.84, 0.0, 167.0)],
        now - chrono::Duration::seconds(12),
    );

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );
    let peer_a_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-a"))
        .expect("peer-a present");
    let peer_b_index = peers
        .iter()
        .position(|peer_id| peer_id == &PeerId::new("peer-b"))
        .expect("peer-b present");

    assert!(peer_b_index < peer_a_index);
}

#[test]
fn runtime_training_assignment_peers_do_not_prune_from_single_planner_backpressure() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids =
        BTreeSet::from([PeerId::new("peer-backed-off"), PeerId::new("peer-healthy")]);
    advertise_peer(&mut snapshot, "peer-backed-off", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-healthy", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-backed-off",
        PeerWindowStatus::Completed,
        96,
        1_000,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-healthy",
        PeerWindowStatus::Completed,
        128,
        950,
        0,
        now,
    );
    let planner = Keypair::generate_ed25519();
    let planner_peer_id =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
    advertise_auth_peer(&mut snapshot, planner_peer_id.as_str(), &planner, now);
    for offset in [0, 20, 40, 60] {
        publish_scaled_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            &["peer-healthy"],
            &[
                ("peer-healthy", 0.96, 0.0, 180.0, 1.2, 1.1),
                ("peer-backed-off", 0.7, 0.0, 90.0, 0.4, 0.4),
            ],
            now - chrono::Duration::seconds(offset),
        );
    }

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("peer-healthy")));
    assert!(peers.contains(&PeerId::new("peer-backed-off")));
}

#[test]
fn runtime_training_assignment_peers_prune_sustained_global_backpressure() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids =
        BTreeSet::from([PeerId::new("peer-backed-off"), PeerId::new("peer-healthy")]);
    advertise_peer(&mut snapshot, "peer-backed-off", &[PeerRole::TrainerCpu]);
    advertise_peer(&mut snapshot, "peer-healthy", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "peer-backed-off",
        PeerWindowStatus::Completed,
        96,
        1_000,
        0,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "peer-healthy",
        PeerWindowStatus::Completed,
        128,
        950,
        0,
        now,
    );
    for offset in [0, 20, 40, 60] {
        let planner = Keypair::generate_ed25519();
        let planner_peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
        let generated_at = now - chrono::Duration::seconds(offset);
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            generated_at,
        );
        publish_scaled_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            &["peer-healthy"],
            &[
                ("peer-healthy", 0.96, 0.0, 180.0, 1.2, 1.1),
                ("peer-backed-off", 0.7, 0.0, 90.0, 0.45, 0.45),
            ],
            generated_at,
        );
    }

    let peers = runtime_training_assignment_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::TrainerCpu]),
        &PeerId::new("local"),
    );

    assert!(peers.contains(&PeerId::new("peer-healthy")));
    assert!(!peers.contains(&PeerId::new("peer-backed-off")));
}

#[test]
fn local_training_adaptation_factor_uses_lag_history_and_planner_pressure() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.head_lag_steps = 4;
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-fast"), PeerId::new("local")]);
    advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Failed,
        0,
        1_200,
        3,
        now,
    );
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Failed,
        0,
        1_150,
        3,
        now - chrono::Duration::seconds(20),
    );
    for offset in [0, 15, 30] {
        let planner = Keypair::generate_ed25519();
        let planner_peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
        let generated_at = now - chrono::Duration::seconds(offset);
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            generated_at,
        );
        publish_ranked_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            &["peer-fast"],
            &[("peer-fast", 0.98, 0.0, 180.0), ("local", 0.4, 3.0, 60.0)],
            generated_at,
        );
    }

    let factor = local_training_adaptation_factor(&snapshot, &PeerId::new("local"));

    assert!(factor < 0.35, "unexpected adaptation factor: {factor}");
    assert!(
        factor >= 0.2,
        "factor should remain clamped to the safety floor"
    );
}

#[test]
fn local_training_schedule_hint_uses_verified_planner_scales() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    snapshot.observed_peer_ids = BTreeSet::from([PeerId::new("peer-fast"), PeerId::new("local")]);
    advertise_peer(&mut snapshot, "peer-fast", &[PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Completed,
        100,
        900,
        0,
        now,
    );
    for offset in [0, 20, 40] {
        let planner = Keypair::generate_ed25519();
        let planner_peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
        let generated_at = now - chrono::Duration::seconds(offset);
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            generated_at,
        );
        publish_scaled_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            &["local"],
            &[
                ("local", 0.95, 0.0, 180.0, 1.35, 1.25),
                ("peer-fast", 0.8, 0.0, 140.0, 0.85, 0.9),
            ],
            generated_at,
        );
    }

    let hint = local_training_schedule_hint(
        &snapshot,
        &test_experiment_handle(),
        &PeerId::new("local"),
        WindowId(2),
    );

    assert!(
        hint.budget_scale > 1.15,
        "unexpected budget scale: {}",
        hint.budget_scale
    );
    assert!(
        hint.microshard_scale > 1.05,
        "unexpected microshard scale: {}",
        hint.microshard_scale
    );
}

#[test]
fn local_training_schedule_hint_ignores_unverified_planner_scales() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Completed,
        100,
        900,
        0,
        now,
    );
    for offset in [0, 20, 40] {
        let trusted_planner = Keypair::generate_ed25519();
        let signing_planner = Keypair::generate_ed25519();
        let planner_peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&trusted_planner.public()).to_string(),
        );
        let generated_at = now - chrono::Duration::seconds(offset);
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &trusted_planner,
            generated_at,
        );
        publish_scaled_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &signing_planner,
            &["local"],
            &[("local", 0.95, 0.0, 180.0, 1.45, 1.35)],
            generated_at,
        );
    }

    let hint = local_training_schedule_hint(
        &snapshot,
        &test_experiment_handle(),
        &PeerId::new("local"),
        WindowId(2),
    );

    assert_eq!(hint.budget_scale, 1.0);
    assert_eq!(hint.microshard_scale, 1.0);
}

#[test]
fn local_training_schedule_hint_downscales_under_sustained_planner_backpressure() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Completed,
        96,
        1_000,
        0,
        now,
    );
    for offset in [0, 20, 40, 60] {
        let planner = Keypair::generate_ed25519();
        let planner_peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&planner.public()).to_string());
        let generated_at = now - chrono::Duration::seconds(offset);
        advertise_auth_peer(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            generated_at,
        );
        publish_scaled_signed_placement_snapshot(
            &mut snapshot,
            planner_peer_id.as_str(),
            &planner,
            &["peer-fast"],
            &[
                ("local", 0.75, 0.0, 90.0, 0.5, 0.5),
                ("peer-fast", 0.98, 0.0, 180.0, 1.2, 1.1),
            ],
            generated_at,
        );
    }

    let hint = local_training_schedule_hint(
        &snapshot,
        &test_experiment_handle(),
        &PeerId::new("local"),
        WindowId(2),
    );

    assert!(
        hint.budget_scale < 0.8,
        "unexpected budget scale: {}",
        hint.budget_scale
    );
    assert!(
        hint.microshard_scale < 0.8,
        "unexpected microshard scale: {}",
        hint.microshard_scale
    );
}

#[test]
fn local_training_schedule_hint_prefers_effective_schedule_epoch_scales() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_training_hint(
        &mut snapshot,
        "local",
        PeerWindowStatus::Completed,
        100,
        900,
        0,
        now,
    );
    publish_schedule_epoch(
        &mut snapshot,
        FleetScheduleEpoch {
            target: ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(3),
                    grace_windows: 0,
                },
                required_client_capabilities: BTreeSet::new(),
            },
            ends_before_window: None,
            plan_epoch: 4,
            assignments: vec![FleetScheduleAssignment {
                peer_id: PeerId::new("local"),
                slot_index: 0,
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
                budget_scale: Some(0.7),
                microshard_scale: Some(0.65),
            }],
            reason: Some("rebalance".into()),
        },
        now,
    );

    let hint = local_training_schedule_hint(
        &snapshot,
        &test_experiment_handle(),
        &PeerId::new("local"),
        WindowId(3),
    );

    assert_eq!(hint.budget_scale, 0.7);
    assert_eq!(hint.microshard_scale, 0.65);
}

#[test]
fn local_training_schedule_hint_clamps_when_epoch_assigns_peer_elsewhere() {
    let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
    let now = Utc::now();
    publish_schedule_epoch(
        &mut snapshot,
        FleetScheduleEpoch {
            target: ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(2),
                    grace_windows: 0,
                },
                required_client_capabilities: BTreeSet::new(),
            },
            ends_before_window: None,
            plan_epoch: 1,
            assignments: vec![FleetScheduleAssignment {
                peer_id: PeerId::new("local"),
                slot_index: 0,
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("other-experiment"),
                revision_id: RevisionId::new("other-revision"),
                budget_scale: Some(1.2),
                microshard_scale: Some(1.1),
            }],
            reason: None,
        },
        now,
    );

    let hint = local_training_schedule_hint(
        &snapshot,
        &test_experiment_handle(),
        &PeerId::new("local"),
        WindowId(2),
    );

    assert_eq!(hint.budget_scale, 0.25);
    assert_eq!(hint.microshard_scale, 0.25);
}

#[test]
fn runtime_validator_peers_and_quorum_respect_validator_eligibility() {
    let mut snapshot = test_snapshot([PeerRole::Validator]);
    snapshot.observed_peer_ids = BTreeSet::from([
        PeerId::new("peer-good"),
        PeerId::new("peer-reducer-only"),
        PeerId::new("peer-quarantined"),
    ]);
    snapshot.connected_peer_ids = snapshot.observed_peer_ids.clone();
    snapshot.connected_peers = snapshot.connected_peer_ids.len();
    snapshot.trust_scores = vec![
        trust_score("peer-reducer-only", true, false, false),
        trust_score("peer-quarantined", false, false, true),
    ];
    advertise_peer(&mut snapshot, "peer-good", &[PeerRole::Validator]);
    advertise_peer(&mut snapshot, "peer-reducer-only", &[PeerRole::Reducer]);
    advertise_peer(&mut snapshot, "peer-quarantined", &[PeerRole::Validator]);

    let validator_peers = runtime_validator_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::Validator]),
        &PeerId::new("local"),
    );
    let validators = runtime_validators(
        &PeerRoleSet::new([PeerRole::Validator]),
        &PeerId::new("local"),
        &validator_peers,
        2,
    );

    assert!(validator_peers.contains(&PeerId::new("local")));
    assert!(validator_peers.contains(&PeerId::new("peer-good")));
    assert!(!validator_peers.contains(&PeerId::new("peer-reducer-only")));
    assert!(!validator_peers.contains(&PeerId::new("peer-quarantined")));
    assert_eq!(
        validators,
        vec![PeerId::new("local"), PeerId::new("peer-good")]
    );
}

#[test]
fn runtime_validators_cap_selection_to_the_requested_quorum() {
    let peers = std::iter::once(PeerId::new("local"))
        .chain((0..25).map(|index| PeerId::new(format!("validator-{index:02}"))))
        .collect::<Vec<_>>();

    let validators = runtime_validators(
        &PeerRoleSet::new([PeerRole::Validator]),
        &PeerId::new("local"),
        &peers,
        2,
    );

    assert_eq!(validators.len(), 2);
    assert_eq!(validators[0], PeerId::new("local"));
    assert_eq!(validators[1], PeerId::new("validator-00"));
}

#[test]
fn runtime_validator_peers_ignore_transport_only_observed_peers() {
    let mut snapshot = test_snapshot([PeerRole::Validator]);
    snapshot.local_peer_id = Some(PeerId::new("local"));
    snapshot.observed_peer_ids = BTreeSet::from([
        PeerId::new("peer-validator"),
        PeerId::new("peer-transport-only"),
    ]);
    snapshot.connected_peer_ids = BTreeSet::from([PeerId::new("peer-validator")]);
    snapshot.connected_peers = snapshot.connected_peer_ids.len();
    advertise_peer(&mut snapshot, "peer-validator", &[PeerRole::Validator]);

    let validator_peers = runtime_validator_peers(
        &snapshot,
        &PeerRoleSet::new([PeerRole::Validator]),
        &PeerId::new("local"),
    );

    assert!(validator_peers.contains(&PeerId::new("local")));
    assert!(validator_peers.contains(&PeerId::new("peer-validator")));
    assert!(!validator_peers.contains(&PeerId::new("peer-transport-only")));
}
