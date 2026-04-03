use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AttestationLevel, CapabilityCard, CapabilityCardId, CapabilityClass, ClientPlatform, ContentId,
    ContributionReceipt, ContributionReceiptId, DataReceipt, ExperimentId, ExperimentScope,
    LeaseId, MetricValue, NetworkId, PeerId, PeerRole, PeerRoleSet, PersistenceClass, PrincipalId,
    ProjectFamilyId, RevocationEpoch, SignatureAlgorithm, SignatureMetadata, WorkDisposition,
};
use burn_p2p_security::{
    AdmissionPolicy, CallbackPayload, ChallengeResponse, ClientManifest, IdentityConnector,
    LoginRequest, MergeEvidence, MergeEvidenceRequirement, NodeCertificateAuthority,
    NodeEnrollmentRequest, PrincipalClaims, ReleasePolicy, ReputationEngine, ReputationObservation,
    ReputationState, StaticIdentityConnector, StaticPrincipalRecord, TrustedIssuer,
    ValidatorPolicy,
};
use chrono::{Duration, Utc};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use libp2p_identity::Keypair;
use semver::{Version, VersionReq};

fn deterministic_keypair(seed: u8) -> Keypair {
    Keypair::ed25519_from_bytes([seed; 32]).expect("deterministic ed25519 keypair")
}

fn static_connector() -> StaticIdentityConnector {
    let now = Utc::now();
    StaticIdentityConnector::new(
        "lab-auth",
        Duration::minutes(10),
        BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: burn_p2p_core::AuthProvider::Static {
                        authority: "lab-auth".into(),
                    },
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::from(["ml-lab".into()]),
                    group_memberships: BTreeSet::from(["trainers".into()]),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([
                        ExperimentScope::Connect,
                        ExperimentScope::Discover,
                        ExperimentScope::Train {
                            experiment_id: ExperimentId::new("exp-a"),
                        },
                    ]),
                    custom_claims: BTreeMap::new(),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]),
    )
}

fn synthetic_session(
    requested_scopes: BTreeSet<ExperimentScope>,
) -> burn_p2p_security::PrincipalSession {
    let now = Utc::now();
    burn_p2p_security::PrincipalSession {
        session_id: ContentId::new("session-a"),
        network_id: NetworkId::new("network-a"),
        claims: PrincipalClaims {
            principal_id: PrincipalId::new("alice"),
            provider: burn_p2p_core::AuthProvider::Static {
                authority: "lab-auth".into(),
            },
            display_name: "Alice".into(),
            org_memberships: BTreeSet::from(["ml-lab".into()]),
            group_memberships: BTreeSet::from(["trainers".into()]),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            granted_scopes: requested_scopes,
            custom_claims: BTreeMap::new(),
            issued_at: now,
            expires_at: now + Duration::hours(1),
        },
        issued_at: now,
        expires_at: now + Duration::hours(1),
    }
}

fn validator_policy() -> ValidatorPolicy {
    let mut release_policy = ReleasePolicy::new(
        Version::new(0, 3, 0),
        vec![VersionReq::parse("^0.1").expect("version req")],
    )
    .expect("release policy");
    release_policy.required_project_family_id = Some(ProjectFamilyId::new("family-1"));
    release_policy.required_release_train_hash = Some(ContentId::new("train-1"));
    release_policy
        .allowed_target_artifact_hashes
        .insert(ContentId::new("artifact-native-1"));
    release_policy
        .approved_build_hashes
        .insert(ContentId::new("build-1"));
    release_policy.required_project_hash = Some(ContentId::new("project-1"));

    ValidatorPolicy {
        release_policy,
        evidence_requirement: MergeEvidenceRequirement::default(),
    }
}

fn client_manifest() -> ClientManifest {
    ClientManifest::new(
        PeerId::new("peer-1"),
        ProjectFamilyId::new("family-1"),
        ContentId::new("train-1"),
        ContentId::new("artifact-native-1"),
        "native-linux-x86_64",
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
    .expect("client manifest")
}

fn capability_card() -> CapabilityCard {
    CapabilityCard {
        card_id: CapabilityCardId::new("card-1"),
        peer_id: PeerId::new("peer-1"),
        platform: ClientPlatform::Native,
        roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        preferred_backends: vec!["cuda".into()],
        recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
        device_memory_bytes: Some(24 * 1024 * 1024 * 1024),
        system_memory_bytes: 64 * 1024 * 1024 * 1024,
        disk_bytes: 1000 * 1024 * 1024 * 1024,
        upload_mbps: 100.0,
        download_mbps: 100.0,
        persistence: PersistenceClass::Durable,
        work_units_per_second: 1000.0,
        attestation_level: AttestationLevel::Strong,
        benchmark_hash: None,
        reported_at: Utc::now(),
    }
}

fn contribution_receipt(metric_count: usize) -> ContributionReceipt {
    let mut metrics = BTreeMap::new();
    metrics.insert("loss".into(), MetricValue::Float(0.25));
    metrics.insert("accuracy".into(), MetricValue::Float(0.9));
    for index in 0..metric_count {
        metrics.insert(
            format!("metric-{index}"),
            MetricValue::Float((index as f64) / (metric_count.max(1) as f64)),
        );
    }

    ContributionReceipt {
        receipt_id: ContributionReceiptId::new("receipt-1"),
        peer_id: PeerId::new("peer-1"),
        study_id: burn_p2p_core::StudyId::new("study-1"),
        experiment_id: ExperimentId::new("exp-1"),
        revision_id: burn_p2p_core::RevisionId::new("rev-1"),
        base_head_id: burn_p2p_core::HeadId::new("head-0"),
        artifact_id: burn_p2p_core::ArtifactId::new("artifact-1"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics,
        merge_cert_id: None,
    }
}

fn assignment_lease(shard_count: usize) -> burn_p2p_core::AssignmentLease {
    burn_p2p_core::AssignmentLease {
        lease_id: LeaseId::new("lease-1"),
        network_id: NetworkId::new("net-1"),
        study_id: burn_p2p_core::StudyId::new("study-1"),
        experiment_id: ExperimentId::new("exp-1"),
        revision_id: burn_p2p_core::RevisionId::new("rev-1"),
        peer_id: PeerId::new("peer-1"),
        dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
        window_id: burn_p2p_core::WindowId(1),
        granted_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(5),
        budget_work_units: shard_count.max(1) as u64,
        microshards: (0..shard_count)
            .map(|index| burn_p2p_core::MicroShardId::new(format!("shard-{index}")))
            .collect(),
        assignment_hash: ContentId::new("assignment-1"),
    }
}

fn data_receipt(shard_count: usize) -> DataReceipt {
    DataReceipt {
        receipt_id: ContentId::new("data-receipt-1"),
        lease_id: LeaseId::new("lease-1"),
        peer_id: PeerId::new("peer-1"),
        completed_at: Utc::now(),
        microshards: (0..shard_count)
            .map(|index| burn_p2p_core::MicroShardId::new(format!("shard-{index}")))
            .collect(),
        examples_processed: shard_count.max(1) as u64,
        tokens_processed: (shard_count.max(1) * 8) as u64,
        disposition: WorkDisposition::Accepted,
    }
}

fn merge_evidence(metric_count: usize) -> MergeEvidence {
    MergeEvidence {
        peer_id: PeerId::new("peer-1"),
        client_manifest: client_manifest(),
        capability_card: capability_card(),
        challenge_response: Some(ChallengeResponse {
            peer_id: PeerId::new("peer-1"),
            nonce_hash: ContentId::new("nonce-1"),
            response_hash: ContentId::new("response-1"),
            answered_at: Utc::now(),
            signature: SignatureMetadata {
                signer: PeerId::new("peer-1"),
                key_id: "key-1".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "deadbeef".into(),
            },
        }),
        data_receipt: Some(data_receipt(metric_count.max(1))),
        holdout_metrics: (0..metric_count)
            .map(|index| {
                (
                    format!("holdout-{index}"),
                    MetricValue::Float((index as f64) / (metric_count.max(1) as f64)),
                )
            })
            .collect(),
        reputation_score: 10.0,
    }
}

fn auth_fixture(
    scope_count: usize,
) -> (
    NodeCertificateAuthority,
    PeerId,
    burn_p2p_core::PeerAuthEnvelope,
    AdmissionPolicy,
) {
    let requested_scopes = std::iter::once(ExperimentScope::Connect)
        .chain((0..scope_count).map(|index| ExperimentScope::Train {
            experiment_id: ExperimentId::new(format!("exp-{index}")),
        }))
        .collect::<BTreeSet<_>>();
    let session = synthetic_session(requested_scopes.clone());

    let authority_keypair = deterministic_keypair(7);
    let authority = NodeCertificateAuthority::new(
        NetworkId::new("network-a"),
        ProjectFamilyId::new("family-1"),
        ContentId::new("train-1"),
        Version::new(0, 1, 0),
        authority_keypair,
        "authority-1",
    )
    .expect("authority");

    let node_keypair = deterministic_keypair(19);
    let peer_id =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string());
    let certificate = authority
        .issue_certificate(NodeEnrollmentRequest {
            session,
            project_family_id: ProjectFamilyId::new("family-1"),
            release_train_hash: ContentId::new("train-1"),
            target_artifact_hash: ContentId::new("artifact-native-1"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            requested_scopes: requested_scopes.clone(),
            client_policy_hash: Some(ContentId::new("policy-a")),
            serial: 1,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(10),
            revocation_epoch: RevocationEpoch(4),
        })
        .expect("certificate");

    let envelope = authority
        .create_peer_auth_envelope(
            &node_keypair,
            certificate,
            Some(ContentId::new("manifest-a")),
            requested_scopes,
            ContentId::new("nonce-a"),
            Utc::now(),
        )
        .expect("auth envelope");

    let policy = AdmissionPolicy {
        network_id: NetworkId::new("network-a"),
        project_family_id: ProjectFamilyId::new("family-1"),
        required_release_train_hash: ContentId::new("train-1"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-1")]),
        trusted_issuers: BTreeMap::from([(
            authority.issuer_peer_id(),
            TrustedIssuer {
                issuer_peer_id: authority.issuer_peer_id(),
                issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
            },
        )]),
        minimum_revocation_epoch: RevocationEpoch(4),
    };

    (authority, peer_id, envelope, policy)
}

fn bench_release_policy(c: &mut Criterion) {
    let policy = validator_policy().release_policy;
    let mut group = c.benchmark_group("security_release_policy");
    for feature_count in [4_usize, 32, 128] {
        let mut manifest = client_manifest();
        manifest.feature_set = (0..feature_count)
            .map(|index| format!("feature-{index}"))
            .collect();
        manifest.feature_set.insert("gpu".into());
        group.bench_with_input(
            BenchmarkId::new("evaluate_client", feature_count),
            &manifest,
            |b, manifest| {
                b.iter(|| policy.evaluate_client(black_box(manifest)));
            },
        );
    }
    group.finish();
}

fn bench_auth_admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("security_auth_admission");
    for scope_count in [1_usize, 8, 32] {
        let (authority, peer_id, envelope, policy) = auth_fixture(scope_count);
        group.bench_with_input(
            BenchmarkId::new("verify_peer_auth", scope_count),
            &(peer_id.clone(), envelope.clone(), policy.clone()),
            |b, (peer_id, envelope, policy)| {
                b.iter(|| {
                    policy
                        .verify_peer_auth(black_box(envelope), black_box(peer_id), Utc::now())
                        .expect("verify")
                });
            },
        );
        let requested_scopes = std::iter::once(ExperimentScope::Connect)
            .chain((0..scope_count).map(|index| ExperimentScope::Train {
                experiment_id: ExperimentId::new(format!("exp-{index}")),
            }))
            .collect::<BTreeSet<_>>();
        let session = synthetic_session(requested_scopes.clone());
        let node_keypair = deterministic_keypair((scope_count as u8).saturating_add(33));
        let peer_id = PeerId::new(
            libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string(),
        );
        let enrollment = NodeEnrollmentRequest {
            session,
            project_family_id: ProjectFamilyId::new("family-1"),
            release_train_hash: ContentId::new("train-1"),
            target_artifact_hash: ContentId::new("artifact-native-1"),
            peer_id,
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            requested_scopes,
            client_policy_hash: Some(ContentId::new("policy-a")),
            serial: 1,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(10),
            revocation_epoch: RevocationEpoch(4),
        };
        group.bench_with_input(
            BenchmarkId::new("issue_certificate", scope_count),
            &enrollment,
            |b, enrollment| {
                b.iter(|| {
                    authority
                        .issue_certificate(black_box(enrollment.clone()))
                        .expect("cert")
                });
            },
        );
    }
    let connector = static_connector();
    group.bench_function("static_connector_roundtrip", |b| {
        b.iter(|| {
            let login = connector
                .begin_login(LoginRequest {
                    network_id: NetworkId::new("network-a"),
                    principal_hint: Some("alice".into()),
                    requested_scopes: BTreeSet::from([ExperimentScope::Train {
                        experiment_id: ExperimentId::new("exp-a"),
                    }]),
                })
                .expect("login");
            connector
                .complete_login(CallbackPayload {
                    login_id: login.login_id,
                    state: login.state,
                    principal_id: Some(PrincipalId::new("alice")),
                    provider_code: None,
                })
                .expect("session")
        });
    });
    group.finish();
}

fn bench_validator_policy(c: &mut Criterion) {
    let policy = validator_policy();
    let mut group = c.benchmark_group("security_validator_policy");
    for item_count in [1_usize, 16, 128] {
        let lease = assignment_lease(item_count);
        let receipt = data_receipt(item_count);
        group.bench_with_input(
            BenchmarkId::new("audit_data_receipt", item_count),
            &(lease.clone(), receipt.clone()),
            |b, (lease, receipt)| {
                b.iter(|| {
                    policy.audit_data_receipt(black_box(lease), black_box(receipt), Utc::now())
                });
            },
        );

        let contribution = contribution_receipt(item_count);
        group.bench_with_input(
            BenchmarkId::new("audit_update_receipt", item_count),
            &contribution,
            |b, contribution| {
                b.iter(|| policy.audit_update_receipt(black_box(contribution), Utc::now()));
            },
        );

        let evidence = merge_evidence(item_count);
        group.bench_with_input(
            BenchmarkId::new("evaluate_merge_evidence", item_count),
            &(contribution.clone(), evidence),
            |b, (contribution, evidence)| {
                b.iter(|| {
                    policy.evaluate_merge_evidence(black_box(contribution), black_box(evidence))
                });
            },
        );
    }
    group.finish();
}

fn bench_reputation(c: &mut Criterion) {
    let engine = ReputationEngine::default();
    let mut group = c.benchmark_group("security_reputation");
    for accepted_work_units in [1_u64, 64, 4096] {
        let observation = ReputationObservation {
            accepted_work_units,
            warning_count: 2,
            failure_count: 1,
            last_control_cert_id: None,
            last_merge_cert_id: None,
        };
        group.bench_with_input(
            BenchmarkId::new("apply_observation", accepted_work_units),
            &observation,
            |b, observation| {
                b.iter_batched(
                    || ReputationState::new(PeerId::new("peer-1"), Utc::now()),
                    |mut state| {
                        engine.apply(&mut state, black_box(observation.clone()), Utc::now())
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_release_policy,
    bench_auth_admission,
    bench_validator_policy,
    bench_reputation
);
criterion_main!(benches);
