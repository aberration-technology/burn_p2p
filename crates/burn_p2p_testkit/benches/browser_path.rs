//! Browser-path latency and suspension benchmarks.
#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use burn_p2p::{
    ArtifactId, AuthProvider, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor, HeadId,
    MetricValue, NetworkId, PeerId, PeerRoleSet, Precision, PrincipalClaims, PrincipalId,
    PrincipalSession, RevisionId, RevisionManifest, StudyId, WindowActivation, WindowId,
    WorkloadId,
};
use burn_p2p_app::render_browser_app_static_html;
use burn_p2p_bootstrap::{
    BootstrapAdminState, BrowserDirectorySnapshot, BrowserLeaderboardSnapshot,
};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserGpuSupport, BrowserMetricsSyncState, BrowserRuntimeConfig,
    BrowserRuntimeRole, BrowserSessionState, BrowserTransportStatus, BrowserWorkerCommand,
    BrowserWorkerRuntime,
};
use burn_p2p_core::{
    ContributionReceipt, ContributionReceiptId, LeaderboardSnapshot, SchemaEnvelope,
    SignatureAlgorithm, SignatureMetadata, SignedPayload,
};
use burn_p2p_views::{BrowserAppStaticBootstrap, BrowserAppSurface};
use chrono::{Duration, Utc};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use semver::Version;

fn benchmark_directory(
    network_id: &NetworkId,
    study_id: &StudyId,
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
) -> BrowserDirectorySnapshot {
    let workload_id = WorkloadId::new("browser-wgpu-demo");
    let revision = RevisionManifest {
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        workload_id: workload_id.clone(),
        required_release_train_hash: ContentId::new("browser-train"),
        model_schema_hash: ContentId::new("browser-model"),
        checkpoint_format_hash: ContentId::new("browser-checkpoint"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        training_config_hash: ContentId::new("training-config"),
        merge_topology_policy_hash: ContentId::new("merge-topology"),
        slot_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 1,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: true,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser latency benchmark revision".into(),
    };

    let mut entry = ExperimentDirectoryEntry {
        network_id: network_id.clone(),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        workload_id,
        display_name: "Browser Benchmark".into(),
        model_schema_hash: ContentId::new("browser-model"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: revision_id.clone(),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: experiment_id.clone(),
            },
        ]),
        metadata: BTreeMap::new(),
    };
    entry.apply_revision_policy(&revision);

    BrowserDirectorySnapshot {
        network_id: network_id.clone(),
        generated_at: Utc::now(),
        entries: vec![entry],
    }
}

fn benchmark_head(
    study_id: &StudyId,
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
) -> HeadDescriptor {
    HeadDescriptor {
        head_id: HeadId::new("browser-head-1"),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        artifact_id: ArtifactId::new("artifact-browser-1"),
        parent_head_id: Some(HeadId::new("genesis-head")),
        global_step: 3,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }
}

fn signed_directory(
    snapshot: BrowserDirectorySnapshot,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "feed".into(),
        },
    )
    .expect("signed directory")
}

fn signed_leaderboard(
    network_id: &NetworkId,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            LeaderboardSnapshot {
                network_id: network_id.clone(),
                score_version: "leaderboard_score_v1".into(),
                entries: Vec::new(),
                captured_at: Utc::now(),
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "cafe".into(),
        },
    )
    .expect("signed leaderboard")
}

fn benchmark_session(network_id: &NetworkId, experiment_id: &ExperimentId) -> BrowserSessionState {
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("browser-session"),
            network_id: network_id.clone(),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("browser-principal"),
                provider: AuthProvider::Static {
                    authority: "browser-bench".into(),
                },
                display_name: "Browser Principal".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: experiment_id.clone(),
                    },
                    ExperimentScope::Validate {
                        experiment_id: experiment_id.clone(),
                    },
                ]),
                custom_claims: BTreeMap::new(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(30),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + Duration::minutes(30),
        }),
        certificate: None,
        trust_bundle: None,
        enrolled_at: Some(Utc::now()),
        reenrollment_required: false,
    }
}

fn benchmark_capability(role: BrowserRuntimeRole) -> BrowserCapabilityReport {
    BrowserCapabilityReport {
        navigator_gpu_exposed: role == BrowserRuntimeRole::BrowserTrainerWgpu,
        worker_gpu_exposed: role == BrowserRuntimeRole::BrowserTrainerWgpu,
        gpu_support: if role == BrowserRuntimeRole::BrowserTrainerWgpu {
            BrowserGpuSupport::Available
        } else {
            BrowserGpuSupport::Unavailable("gpu-disabled".into())
        },
        recommended_role: role,
        ..BrowserCapabilityReport::default()
    }
}

fn benchmark_runtime(network_id: &NetworkId, role: BrowserRuntimeRole) -> BrowserWorkerRuntime {
    BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: role.clone(),
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                network_id.clone(),
                ContentId::new("browser-train"),
                "browser-wasm",
                ContentId::new("approved-artifact-browser"),
            )
        },
        benchmark_capability(role),
        BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: false,
            webtransport_enabled: true,
            wss_fallback_enabled: true,
            last_error: None,
        },
    )
}

fn benchmark_receipt() -> ContributionReceipt {
    ContributionReceipt {
        receipt_id: ContributionReceiptId::new("receipt-bench-1"),
        peer_id: PeerId::new("browser-peer"),
        study_id: StudyId::new("study-bench"),
        experiment_id: ExperimentId::new("exp-bench"),
        revision_id: RevisionId::new("rev-bench"),
        base_head_id: HeadId::new("browser-head-1"),
        artifact_id: ArtifactId::new("artifact-browser-1"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.75))]),
        merge_cert_id: None,
    }
}

fn bench_portal_load(c: &mut Criterion) {
    let bootstrap = BrowserAppStaticBootstrap {
        app_name: "burn_p2p".into(),
        asset_base_url: "https://cdn.example/burn-p2p".into(),
        module_entry_path: "assets/browser-app.js".into(),
        stylesheet_path: Some("assets/browser-app.css".into()),
        default_edge_url: Some("https://edge.example".into()),
        default_surface: BrowserAppSurface::Viewer,
        refresh_interval_ms: 15_000,
    };
    c.bench_function("browser_path/static_shell_render", |b| {
        b.iter(|| render_browser_app_static_html(criterion::black_box(&bootstrap)));
    });
}

fn bench_join_directory_sync(c: &mut Criterion) {
    let network_id = NetworkId::new("browser-join-directory");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let directory = benchmark_directory(&network_id, &study_id, &experiment_id, &revision_id);
    let session = benchmark_session(&network_id, &experiment_id);

    c.bench_function("browser_path/join_to_directory_sync", |b| {
        b.iter_batched(
            || {
                let mut runtime =
                    benchmark_runtime(&network_id, BrowserRuntimeRole::BrowserTrainerWgpu);
                runtime.remember_session(session.clone());
                runtime.storage.stored_certificate_peer_id = Some(PeerId::new("browser-peer"));
                runtime
            },
            |mut runtime| {
                runtime.apply_directory_snapshot(
                    criterion::black_box(&directory),
                    criterion::black_box(Some(&session)),
                );
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_join_head_sync(c: &mut Criterion) {
    let network_id = NetworkId::new("browser-join-head");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let directory = benchmark_directory(&network_id, &study_id, &experiment_id, &revision_id);
    let head = benchmark_head(&study_id, &experiment_id, &revision_id);
    let session = benchmark_session(&network_id, &experiment_id);

    c.bench_function("browser_path/join_to_head_sync", |b| {
        b.iter_batched(
            || {
                let mut runtime =
                    benchmark_runtime(&network_id, BrowserRuntimeRole::BrowserTrainerWgpu);
                runtime.remember_session(session.clone());
                runtime.storage.stored_certificate_peer_id = Some(PeerId::new("browser-peer"));
                runtime.apply_directory_snapshot(&directory, Some(&session));
                runtime
            },
            |mut runtime| {
                runtime.apply_edge_sync(
                    criterion::black_box(signed_directory(directory.clone())),
                    criterion::black_box(std::slice::from_ref(&head)),
                    criterion::black_box(Some(signed_leaderboard(&network_id))),
                    criterion::black_box(BrowserMetricsSyncState::default()),
                    criterion::black_box(runtime.transport.clone()),
                    criterion::black_box(Some(&session)),
                );
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_receipt_submission(c: &mut Criterion) {
    c.bench_function("browser_path/receipt_submission", |b| {
        b.iter_batched(
            || (BootstrapAdminState::default(), benchmark_receipt()),
            |(mut state, receipt)| {
                state.ingest_contribution_receipts(criterion::black_box([receipt]));
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_suspend_resume(c: &mut Criterion) {
    let network_id = NetworkId::new("browser-suspend");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let directory = benchmark_directory(&network_id, &study_id, &experiment_id, &revision_id);
    let head = benchmark_head(&study_id, &experiment_id, &revision_id);
    let session = benchmark_session(&network_id, &experiment_id);

    for role in [
        BrowserRuntimeRole::BrowserObserver,
        BrowserRuntimeRole::BrowserVerifier,
        BrowserRuntimeRole::BrowserTrainerWgpu,
    ] {
        let role_name = format!("{role:?}");
        c.bench_function(&format!("browser_path/suspend_resume_{role_name}"), |b| {
            b.iter_batched(
                || {
                    let mut runtime = benchmark_runtime(&network_id, role.clone());
                    runtime.remember_session(session.clone());
                    runtime.storage.stored_certificate_peer_id = Some(PeerId::new("browser-peer"));
                    runtime.apply_directory_snapshot(&directory, Some(&session));
                    runtime.apply_edge_sync(
                        signed_directory(directory.clone()),
                        std::slice::from_ref(&head),
                        Some(signed_leaderboard(&network_id)),
                        BrowserMetricsSyncState::default(),
                        runtime.transport.clone(),
                        Some(&session),
                    );
                    runtime
                },
                |mut runtime| {
                    runtime.apply_command(
                        BrowserWorkerCommand::Suspend,
                        Some(&directory),
                        Some(&session),
                    );
                    runtime.apply_command(
                        BrowserWorkerCommand::Resume,
                        Some(&directory),
                        Some(&session),
                    );
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(
    benches,
    bench_portal_load,
    bench_join_directory_sync,
    bench_join_head_sync,
    bench_receipt_submission,
    bench_suspend_resume
);
criterion_main!(benches);
