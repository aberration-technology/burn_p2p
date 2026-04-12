//! Swarm transport, control-plane, and overlay helpers for burn_p2p.
#![allow(missing_docs)]
use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AggregateEnvelope, AggregateStats, AggregateTier, ArtifactDescriptor, ArtifactId,
    AssignmentLease, ChunkDescriptor, ContentId, ContributionReceiptId, DatasetViewId,
    ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy, ExperimentResourceRequirements,
    ExperimentScope, ExperimentVisibility, HeadDescriptor, HeadId, MergeCertId, MergeCertificate,
    MergePolicy, MergeTopologyPolicy, MergeWindowState, MetricValue, NetworkId, PeerId, PeerRole,
    PeerRoleSet, Precision, ReducerAssignment, ReducerLoadReport, ReductionCertificate, RevisionId,
    StudyId, UpdateAnnounce, UpdateNormStats, WindowId, WorkloadId,
};
use burn_p2p_swarm::{
    AggregateProposalAnnouncement, ControlPlaneResponse, ControlPlaneSnapshot,
    ExperimentDirectoryAnnouncement, ExperimentOverlaySet, HeadAnnouncement, LeaseAnnouncement,
    MergeAnnouncement, MergeWindowAnnouncement, OverlayTopic, PubsubEnvelope, PubsubPayload,
    ReducerAssignmentAnnouncement, ReducerLoadAnnouncement, ReductionCertificateAnnouncement,
    UpdateEnvelopeAnnouncement,
};
use chrono::{Duration, Utc};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn ids() -> (NetworkId, StudyId, ExperimentId, RevisionId) {
    (
        NetworkId::new("bench-net"),
        StudyId::new("bench-study"),
        ExperimentId::new("bench-experiment"),
        RevisionId::new("bench-revision"),
    )
}

fn overlays() -> burn_p2p_swarm::ExperimentOverlaySet {
    let (network_id, study_id, experiment_id, _) = ids();
    ExperimentOverlaySet::new(network_id, study_id, experiment_id).expect("overlay set")
}

fn artifact_descriptor(chunk_count: usize) -> ArtifactDescriptor {
    let chunks = (0..chunk_count)
        .map(|index| ChunkDescriptor {
            chunk_id: burn_p2p_core::ChunkId::new(format!("chunk-{index:04}")),
            offset_bytes: (index as u64) * 1_048_576,
            length_bytes: 1_048_576,
            chunk_hash: ContentId::derive(&("chunk", index)).expect("chunk hash"),
        })
        .collect::<Vec<_>>();
    ArtifactDescriptor {
        artifact_id: ArtifactId::new(format!("artifact-{chunk_count:04}")),
        kind: burn_p2p_core::ArtifactKind::ServeHead,
        head_id: Some(HeadId::new("bench-head")),
        base_head_id: Some(HeadId::new("bench-base-head")),
        precision: Precision::Fp16,
        model_schema_hash: ContentId::derive(&("schema", chunk_count)).expect("schema hash"),
        record_format: "burn-store:burnpack".to_owned(),
        bytes_len: chunk_count as u64 * 1_048_576,
        chunks,
        root_hash: ContentId::derive(&("root", chunk_count)).expect("root hash"),
    }
}

fn merge_window() -> MergeWindowState {
    let (network_id, study_id, experiment_id, revision_id) = ids();
    let now = Utc::now();
    MergeWindowState {
        merge_window_id: ContentId::derive(&("merge-window", 1)).expect("window id"),
        network_id,
        study_id,
        experiment_id,
        revision_id,
        window_id: WindowId(1),
        base_head_id: HeadId::new("bench-base-head"),
        policy: MergeTopologyPolicy::default(),
        reducers: vec![PeerId::new("reducer-0001"), PeerId::new("reducer-0002")],
        validators: vec![PeerId::new("validator-0001"), PeerId::new("validator-0002")],
        opened_at: now,
        closes_at: now + Duration::seconds(300),
    }
}

fn head_descriptor(index: usize) -> HeadDescriptor {
    let (_, study_id, experiment_id, revision_id) = ids();
    HeadDescriptor {
        head_id: HeadId::new(format!("head-{index:04}")),
        study_id,
        experiment_id,
        revision_id,
        artifact_id: ArtifactId::new(format!("artifact-{index:04}")),
        parent_head_id: Some(HeadId::new("bench-base-head")),
        global_step: index as u64,
        created_at: Utc::now(),
        metrics: BTreeMap::from([
            (
                "loss".to_owned(),
                MetricValue::Float(0.125 + index as f64 / 1000.0),
            ),
            ("accuracy".to_owned(), MetricValue::Float(0.875)),
        ]),
    }
}

fn update_announcement(index: usize) -> UpdateEnvelopeAnnouncement {
    let topics = overlays();
    let (_, study_id, experiment_id, revision_id) = ids();
    UpdateEnvelopeAnnouncement {
        overlay: topics.telemetry,
        update: UpdateAnnounce {
            peer_id: PeerId::new(format!("peer-{index:04}")),
            study_id,
            experiment_id,
            revision_id,
            window_id: WindowId(1),
            base_head_id: HeadId::new("bench-base-head"),
            lease_id: None,
            delta_artifact_id: ArtifactId::new(format!("delta-{index:04}")),
            sample_weight: 32.0 + (index % 5) as f64,
            quality_weight: 0.8 + ((index % 3) as f64 * 0.05),
            norm_stats: UpdateNormStats {
                l2_norm: 1.0 + index as f64 / 100.0,
                max_abs: 1.0 + index as f64 / 200.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: ContentId::derive(&("receipt-root", index)).expect("receipt root"),
            receipt_ids: vec![ContributionReceiptId::new(format!("receipt-{index:04}"))],
            providers: vec![
                PeerId::new(format!("peer-{index:04}")),
                PeerId::new("archive-0001"),
            ],
            announced_at: Utc::now(),
        },
    }
}

fn aggregate_proposal_announcement(index: usize) -> AggregateProposalAnnouncement {
    let topics = overlays();
    let (_, study_id, experiment_id, revision_id) = ids();
    AggregateProposalAnnouncement {
        overlay: topics.telemetry,
        proposal: AggregateEnvelope {
            aggregate_id: ContentId::derive(&("aggregate", index)).expect("aggregate id"),
            study_id,
            experiment_id,
            revision_id,
            window_id: WindowId(1),
            base_head_id: HeadId::new("bench-base-head"),
            aggregate_artifact_id: ArtifactId::new(format!("aggregate-artifact-{index:04}")),
            tier: AggregateTier::RootCandidate,
            reducer_peer_id: PeerId::new(format!("reducer-{index:04}")),
            contributor_peers: vec![
                PeerId::new(format!("peer-{index:04}")),
                PeerId::new(format!("peer-{:04}", index + 1)),
            ],
            child_aggregate_ids: vec![ContentId::derive(&("child", index)).expect("child id")],
            stats: AggregateStats {
                accepted_updates: 16,
                duplicate_updates: 1,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 512.0,
                sum_quality_weight: 14.4,
                sum_weighted_delta_norm: 31.25,
                max_update_norm: 2.5,
                accepted_sample_coverage: 0.97,
            },
            providers: vec![
                PeerId::new(format!("reducer-{index:04}")),
                PeerId::new("archive-0001"),
            ],
            published_at: Utc::now(),
        },
        announced_at: Utc::now(),
    }
}

fn reduction_certificate_announcement(index: usize) -> ReductionCertificateAnnouncement {
    let topics = overlays();
    let (_, study_id, experiment_id, revision_id) = ids();
    ReductionCertificateAnnouncement {
        overlay: topics.alerts,
        certificate: ReductionCertificate {
            reduction_id: ContentId::derive(&("reduction", index)).expect("reduction id"),
            study_id,
            experiment_id,
            revision_id,
            window_id: WindowId(1),
            base_head_id: HeadId::new("bench-base-head"),
            aggregate_id: ContentId::derive(&("aggregate", index)).expect("aggregate id"),
            validator: PeerId::new("validator-0001"),
            validator_quorum: 2,
            cross_checked_reducers: vec![PeerId::new("reducer-0001"), PeerId::new("reducer-0002")],
            issued_at: Utc::now(),
        },
        announced_at: Utc::now(),
    }
}

fn directory_announcement(entry_count: usize) -> ExperimentDirectoryAnnouncement {
    let (network_id, study_id, experiment_id, revision_id) = ids();
    ExperimentDirectoryAnnouncement {
        network_id: network_id.clone(),
        entries: (0..entry_count)
            .map(|index| ExperimentDirectoryEntry {
                network_id: network_id.clone(),
                study_id: study_id.clone(),
                experiment_id: ExperimentId::new(format!("experiment-{index:04}")),
                workload_id: WorkloadId::new(format!("workload-{index:04}")),
                display_name: format!("Benchmark Experiment {index}"),
                model_schema_hash: ContentId::derive(&("model-schema", index))
                    .expect("model schema"),
                dataset_view_id: DatasetViewId::new(format!("dataset-view-{index:04}")),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                    minimum_device_memory_bytes: Some(8 * 1024 * 1024 * 1024),
                    minimum_system_memory_bytes: Some(16 * 1024 * 1024 * 1024),
                    estimated_download_bytes: 256 * 1024 * 1024,
                    estimated_window_seconds: 300,
                },
                visibility: if index % 2 == 0 {
                    ExperimentVisibility::OptIn
                } else {
                    ExperimentVisibility::Public
                },
                opt_in_policy: if index % 2 == 0 {
                    ExperimentOptInPolicy::Scoped
                } else {
                    ExperimentOptInPolicy::Open
                },
                current_revision_id: revision_id.clone(),
                current_head_id: Some(HeadId::new(format!("directory-head-{index:04}"))),
                allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
                allowed_scopes: BTreeSet::from([
                    ExperimentScope::Discover,
                    ExperimentScope::Train {
                        experiment_id: experiment_id.clone(),
                    },
                ]),
                metadata: BTreeMap::from([
                    ("owner".to_owned(), "bench".to_owned()),
                    ("slot".to_owned(), index.to_string()),
                ]),
            })
            .collect(),
        announced_at: Utc::now(),
    }
}

fn control_snapshot(update_count: usize) -> ControlPlaneSnapshot {
    let topics = overlays();
    let (_, study_id, experiment_id, revision_id) = ids();
    let now = Utc::now();
    let merge_window = merge_window();
    let merge_certificate = MergeCertificate {
        merge_cert_id: MergeCertId::new("merge-cert-0001"),
        study_id,
        experiment_id,
        revision_id,
        base_head_id: HeadId::new("bench-base-head"),
        merged_head_id: HeadId::new("bench-merged-head"),
        merged_artifact_id: ArtifactId::new("bench-merged-artifact"),
        policy: MergePolicy::QualityWeightedEma,
        issued_at: now,
        validator: PeerId::new("validator-0001"),
        contribution_receipts: (0..update_count)
            .map(|index| ContributionReceiptId::new(format!("receipt-{index:04}")))
            .collect(),
    };
    ControlPlaneSnapshot {
        control_announcements: Vec::new(),
        lifecycle_announcements: Vec::new(),
        schedule_announcements: Vec::new(),
        head_announcements: (0..4)
            .map(|index| HeadAnnouncement {
                overlay: topics.heads.clone(),
                provider_peer_id: Some(PeerId::new(format!("provider-{index:04}"))),
                head: head_descriptor(index),
                announced_at: now,
            })
            .collect(),
        lease_announcements: (0..4)
            .map(|index| LeaseAnnouncement {
                overlay: topics.leases.clone(),
                lease: AssignmentLease {
                    lease_id: burn_p2p_core::LeaseId::new(format!("lease-{index:04}")),
                    network_id: NetworkId::new("bench-net"),
                    study_id: StudyId::new("bench-study"),
                    experiment_id: ExperimentId::new("bench-experiment"),
                    revision_id: RevisionId::new("bench-revision"),
                    peer_id: PeerId::new(format!("peer-{index:04}")),
                    dataset_view_id: DatasetViewId::new("bench-dataset-view"),
                    window_id: WindowId(1),
                    granted_at: now,
                    expires_at: now + Duration::seconds(300),
                    budget_work_units: 128,
                    microshards: vec![burn_p2p_core::MicroShardId::new(format!(
                        "microshard-{index:04}"
                    ))],
                    assignment_hash: ContentId::derive(&("assignment", index))
                        .expect("assignment hash"),
                },
                announced_at: now,
            })
            .collect(),
        merge_announcements: vec![MergeAnnouncement {
            overlay: topics.alerts.clone(),
            certificate: merge_certificate,
            announced_at: now,
        }],
        merge_window_announcements: vec![MergeWindowAnnouncement {
            overlay: topics.control.clone(),
            merge_window,
            announced_at: now,
        }],
        reducer_assignment_announcements: (0..update_count)
            .map(|index| ReducerAssignmentAnnouncement {
                overlay: topics.control.clone(),
                assignment: ReducerAssignment {
                    assignment_id: ContentId::derive(&("assignment", index))
                        .expect("assignment id"),
                    window_id: WindowId(1),
                    source_peer_id: PeerId::new(format!("peer-{index:04}")),
                    assigned_reducers: vec![
                        PeerId::new("reducer-0001"),
                        PeerId::new("reducer-0002"),
                    ],
                    repair_reducers: vec![PeerId::new("reducer-0003")],
                    upper_tier_reducers: vec![PeerId::new("validator-0001")],
                    assigned_at: now,
                },
                announced_at: now,
            })
            .collect(),
        update_announcements: (0..update_count).map(update_announcement).collect(),
        aggregate_proposal_announcements: (0..(update_count / 8).max(1))
            .map(aggregate_proposal_announcement)
            .collect(),
        reduction_certificate_announcements: (0..2)
            .map(reduction_certificate_announcement)
            .collect(),
        validation_quorum_announcements: Vec::new(),
        reducer_load_announcements: (0..4)
            .map(|index| ReducerLoadAnnouncement {
                overlay: topics.telemetry.clone(),
                report: ReducerLoadReport {
                    peer_id: PeerId::new(format!("reducer-{index:04}")),
                    window_id: WindowId(1),
                    assigned_leaf_updates: 16,
                    assigned_aggregate_inputs: 4,
                    ingress_bytes: 32 * 1024 * 1024,
                    egress_bytes: 8 * 1024 * 1024,
                    duplicate_transfer_ratio: 0.1,
                    overload_ratio: 0.2,
                    reported_at: now,
                },
            })
            .collect(),
        auth_announcements: Vec::new(),
        directory_announcements: vec![directory_announcement(16)],
        peer_directory_announcements: Vec::new(),
        metrics_announcements: Vec::new(),
    }
}

fn bench_pubsub_envelope_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("swarm_pubsub_envelope_roundtrip");
    let topics = overlays();
    let payloads = vec![
        (
            "update",
            PubsubEnvelope {
                topic_path: topics.telemetry.path.clone(),
                payload: PubsubPayload::Update(update_announcement(0)),
                published_at: Utc::now(),
            },
        ),
        (
            "aggregate_proposal",
            PubsubEnvelope {
                topic_path: topics.telemetry.path.clone(),
                payload: PubsubPayload::AggregateProposal(aggregate_proposal_announcement(0)),
                published_at: Utc::now(),
            },
        ),
        (
            "reduction_certificate",
            PubsubEnvelope {
                topic_path: topics.alerts.path.clone(),
                payload: PubsubPayload::ReductionCertificate(reduction_certificate_announcement(0)),
                published_at: Utc::now(),
            },
        ),
        (
            "directory",
            PubsubEnvelope {
                topic_path: OverlayTopic::control(NetworkId::new("bench-net"))
                    .path
                    .clone(),
                payload: PubsubPayload::Directory(directory_announcement(32)),
                published_at: Utc::now(),
            },
        ),
    ];

    for (label, envelope) in payloads {
        let bytes = serde_json::to_vec(&envelope).expect("encode size");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &envelope,
            |b, envelope| {
                b.iter(|| {
                    let bytes = serde_json::to_vec(black_box(envelope)).expect("encode");
                    let _: PubsubEnvelope =
                        serde_json::from_slice(black_box(bytes.as_slice())).expect("decode");
                });
            },
        );
    }

    group.finish();
}

fn bench_control_snapshot_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("swarm_control_snapshot_roundtrip");

    for update_count in [16_usize, 64, 256] {
        let snapshot = control_snapshot(update_count);
        let response = ControlPlaneResponse::Snapshot(snapshot);
        let bytes = serde_json::to_vec(&response).expect("response size");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{update_count}_updates")),
            &response,
            |b, response| {
                b.iter(|| {
                    let bytes = serde_json::to_vec(black_box(response)).expect("encode");
                    let _: ControlPlaneResponse =
                        serde_json::from_slice(black_box(bytes.as_slice())).expect("decode");
                });
            },
        );
    }

    group.finish();
}

fn bench_artifact_manifest_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("swarm_artifact_manifest_roundtrip");

    for chunk_count in [64_usize, 256, 1024] {
        let response =
            ControlPlaneResponse::ArtifactManifest(Some(artifact_descriptor(chunk_count)));
        let bytes = serde_json::to_vec(&response).expect("manifest size");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{chunk_count}_chunks")),
            &response,
            |b, response| {
                b.iter(|| {
                    let bytes = serde_json::to_vec(black_box(response)).expect("encode");
                    let _: ControlPlaneResponse =
                        serde_json::from_slice(black_box(bytes.as_slice())).expect("decode");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_pubsub_envelope_roundtrip,
    bench_control_snapshot_roundtrip,
    bench_artifact_manifest_roundtrip
);
criterion_main!(benches);
