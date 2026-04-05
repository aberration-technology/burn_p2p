use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::Result;
use burn_p2p::{
    ArtifactId, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId, DatasetViewId,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor, HeadId,
    LagPolicy, MergeWindowMissPolicy, MetricValue, NetworkId, PeerId, PeerRole, PeerRoleSet,
    Precision, PrincipalId, ProjectFamilyId, RevisionId, RevisionManifest, StudyId,
    WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_core::{
    BackendClass, BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths,
    BrowserLeaderboardEntry, BrowserLeaderboardIdentity, BrowserLeaderboardSnapshot,
    BrowserLoginProvider, BrowserPortalSnapshot, BrowserTransportSurface as EdgeTransportSurface,
    HeadEvalReport, HeadEvalStatus, LeaseId, MetricTrustClass, MetricsSnapshotManifest,
    PeerWindowMetrics, PeerWindowStatus, RevocationEpoch, SchemaEnvelope, SignatureAlgorithm,
    SignatureMetadata, SignedPayload, TrustBundleExport,
};
use burn_p2p_metrics::{MetricsCatchupBundle, MetricsSnapshot};
use burn_p2p_portal::{
    PortalArtifactRow, PortalDiagnosticsView, PortalExperimentRow, PortalHeadRow,
    PortalLeaderboardRow, PortalLoginProvider, PortalMetricRow, PortalMetricsPanel, PortalPaths,
    PortalPeerStatusRow, PortalRuntimeStateCard, PortalServiceStatusRow, PortalSnapshotView,
    PortalTransportSurface, PortalTrustView, render_browser_app_static_html,
};
use burn_p2p_ui::{BrowserAppStaticBootstrap, BrowserAppSurface};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One scripted browser interaction applied during Playwright capture.
pub struct PortalCaptureInteraction {
    /// Action type such as `goto_anchor`, `filter`, `click`, or `download`.
    pub action: String,
    /// Optional selector used by the action.
    #[serde(default)]
    pub selector: Option<String>,
    /// Optional value for text-entry actions.
    #[serde(default)]
    pub value: Option<String>,
    /// Optional wait text the runner should observe before continuing.
    #[serde(default)]
    pub wait_for_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Optional viewport override used by one Playwright capture scenario.
pub struct PortalCaptureViewport {
    /// Viewport width in CSS pixels.
    pub width: u32,
    /// Viewport height in CSS pixels.
    pub height: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Manifest row for one portal Playwright capture scenario.
pub struct PortalCaptureScenario {
    /// Stable scenario identifier used for folders and routes.
    pub slug: String,
    /// Human-readable scenario title.
    pub title: String,
    /// Short explanation of what the scenario demonstrates.
    pub description: String,
    /// Relative path to the rendered HTML entrypoint.
    pub html_path: String,
    /// Relative path to the raw snapshot JSON.
    pub snapshot_path: String,
    /// Relative path to the per-scenario metadata file.
    pub scenario_path: String,
    /// Number of peer rows in the rendered scenario.
    pub peer_count: usize,
    /// Number of experiment rows in the rendered scenario.
    pub experiment_count: usize,
    /// Estimated network size rendered by the scenario.
    pub estimated_network_size: usize,
    /// Browser/runtime state labels rendered in the scenario.
    pub runtime_states: Vec<String>,
    /// Optional viewport override for the capture.
    #[serde(default)]
    pub viewport: Option<PortalCaptureViewport>,
    /// Ordered interactions the Playwright runner should perform.
    pub interactions: Vec<PortalCaptureInteraction>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Root manifest for portal Playwright capture bundles.
pub struct PortalCaptureManifest {
    /// Timestamp when the bundle was generated.
    pub generated_at: DateTime<Utc>,
    /// Output root used for the bundle.
    pub root: String,
    /// All generated scenarios.
    pub scenarios: Vec<PortalCaptureScenario>,
}

#[derive(Clone, Debug)]
struct PortalScenarioSpec {
    scenario: PortalCaptureScenario,
    snapshot: PortalSnapshotView,
}

/// Writes the rendered portal scenario bundle consumed by the Playwright capture runner.
pub fn write_portal_capture_bundle(root: impl AsRef<Path>) -> Result<PortalCaptureManifest> {
    let root = root.as_ref();
    fs::create_dir_all(root)?;
    let scenarios_root = root.join("scenarios");
    fs::create_dir_all(&scenarios_root)?;

    let specs = build_portal_capture_scenarios();
    let scenarios = specs
        .iter()
        .map(|spec| spec.scenario.clone())
        .collect::<Vec<_>>();

    for spec in specs {
        let scenario_dir = scenarios_root.join(&spec.scenario.slug);
        fs::create_dir_all(&scenario_dir)?;
        fs::write(
            scenario_dir.join("index.html"),
            render_browser_app_static_html(&scenario_bootstrap(&spec.scenario.slug)),
        )?;
        let edge_snapshot = browser_portal_snapshot(&spec.snapshot);
        fs::write(
            scenario_dir.join("snapshot.json"),
            serde_json::to_vec_pretty(&edge_snapshot)?,
        )?;
        fs::write(
            scenario_dir.join("directory.json"),
            serde_json::to_vec_pretty(&edge_snapshot.directory.entries)?,
        )?;
        fs::write(
            scenario_dir.join("heads.json"),
            serde_json::to_vec_pretty(&edge_snapshot.heads)?,
        )?;
        fs::write(
            scenario_dir.join("signed-directory.json"),
            serde_json::to_vec_pretty(&signed_browser_directory_snapshot(
                &edge_snapshot.network_id,
                edge_snapshot.directory.entries.clone(),
            ))?,
        )?;
        fs::write(
            scenario_dir.join("signed-leaderboard.json"),
            serde_json::to_vec_pretty(&signed_browser_leaderboard_snapshot(
                &edge_snapshot.network_id,
                edge_snapshot.leaderboard.entries.clone(),
            ))?,
        )?;
        fs::write(
            scenario_dir.join("metrics-catchup.json"),
            serde_json::to_vec_pretty(&scenario_metrics_catchup(&spec.snapshot, &edge_snapshot))?,
        )?;
        fs::write(
            scenario_dir.join("scenario.json"),
            serde_json::to_vec_pretty(&spec.scenario)?,
        )?;
    }

    let manifest = PortalCaptureManifest {
        generated_at: Utc::now(),
        root: root.display().to_string(),
        scenarios,
    };
    fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

fn scenario_bootstrap(slug: &str) -> BrowserAppStaticBootstrap {
    BrowserAppStaticBootstrap {
        app_name: "burn_p2p".into(),
        asset_base_url: String::new(),
        module_entry_path: "../../assets/browser-app-loader.js".into(),
        stylesheet_path: Some("../../assets/browser-app.css".into()),
        default_edge_url: Some(format!("/{slug}")),
        default_surface: BrowserAppSurface::Viewer,
        refresh_interval_ms: 15_000,
    }
}

fn browser_portal_snapshot(snapshot: &PortalSnapshotView) -> BrowserPortalSnapshot {
    let network_id = NetworkId::new(&snapshot.network_id);
    let directory_entries =
        browser_directory_entries(snapshot, &network_id, &snapshot.browser_mode);
    let leaderboard_entries = browser_leaderboard_entries(snapshot);
    let heads = browser_heads(snapshot);
    let artifact_hash = ContentId::new("browser-capture-artifact");
    let paths = BrowserEdgePaths::default();

    BrowserPortalSnapshot {
        network_id: network_id.clone(),
        edge_mode: browser_edge_mode(snapshot.edge_mode.as_str()),
        browser_mode: browser_mode(snapshot.browser_mode.as_str()),
        social_mode: if snapshot.social_enabled {
            burn_p2p::SocialMode::Public
        } else {
            burn_p2p::SocialMode::Disabled
        },
        profile_mode: if snapshot.profile_enabled {
            burn_p2p::ProfileMode::Public
        } else {
            burn_p2p::ProfileMode::Disabled
        },
        transports: EdgeTransportSurface {
            webrtc_direct: snapshot.transports.webrtc_direct,
            webtransport_gateway: snapshot.transports.webtransport_gateway,
            wss_fallback: snapshot.transports.wss_fallback,
        },
        paths,
        auth_enabled: snapshot.auth_enabled,
        login_providers: snapshot
            .login_providers
            .iter()
            .map(|provider| BrowserLoginProvider {
                label: provider.label.clone(),
                login_path: provider.login_path.clone(),
                callback_path: provider.callback_path.clone(),
                device_path: None,
            })
            .collect(),
        required_release_train_hash: snapshot
            .trust
            .required_release_train_hash
            .clone()
            .map(ContentId::new),
        allowed_target_artifact_hashes: BTreeSet::from([artifact_hash.clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: network_id.clone(),
            generated_at: parse_timestamp(&snapshot.captured_at),
            entries: directory_entries,
        },
        heads,
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: leaderboard_entries,
            captured_at: parse_timestamp(&snapshot.captured_at),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id,
            project_family_id: ProjectFamilyId::new("browser-capture-family"),
            required_release_train_hash: snapshot
                .trust
                .required_release_train_hash
                .clone()
                .map(ContentId::new)
                .unwrap_or_else(|| ContentId::new("browser-capture-train")),
            allowed_target_artifact_hashes: BTreeSet::from([artifact_hash]),
            minimum_revocation_epoch: RevocationEpoch(
                snapshot.trust.minimum_revocation_epoch.unwrap_or_default(),
            ),
            active_issuer_peer_id: snapshot
                .trust
                .active_issuer_peer_id
                .as_ref()
                .map(PeerId::new)
                .unwrap_or_else(|| PeerId::new("browser-capture-issuer")),
            issuers: Vec::new(),
            reenrollment: None,
        }),
        captured_at: parse_timestamp(&snapshot.captured_at),
    }
}

fn scenario_metrics_catchup(
    snapshot: &PortalSnapshotView,
    edge_snapshot: &BrowserPortalSnapshot,
) -> Vec<MetricsCatchupBundle> {
    let Some(entry) = edge_snapshot.directory.entries.first() else {
        return Vec::new();
    };
    let generated_at = parse_timestamp(&snapshot.captured_at);
    let latest_head_id = entry
        .current_head_id
        .clone()
        .or_else(|| edge_snapshot.heads.first().map(|head| head.head_id.clone()))
        .unwrap_or_else(|| HeadId::new("head-capture"));

    let peer_window_metrics = if snapshot.browser_mode == "Trainer" {
        vec![PeerWindowMetrics {
            network_id: edge_snapshot.network_id.clone(),
            experiment_id: entry.experiment_id.clone(),
            revision_id: entry.current_revision_id.clone(),
            workload_id: entry.workload_id.clone(),
            dataset_view_id: entry.dataset_view_id.clone(),
            peer_id: PeerId::new("peer-browser-capture"),
            principal_id: Some(PrincipalId::new("principal-browser")),
            lease_id: LeaseId::new("lease-browser-capture"),
            base_head_id: latest_head_id.clone(),
            window_started_at: generated_at - chrono::Duration::seconds(24),
            window_finished_at: generated_at,
            attempted_tokens_or_samples: 2048,
            accepted_tokens_or_samples: Some(1984),
            local_train_loss_mean: Some(0.262),
            local_train_loss_last: Some(0.241),
            grad_or_delta_norm: Some(1.04),
            optimizer_step_count: 48,
            compute_time_ms: 28_500,
            data_fetch_time_ms: 3_800,
            publish_latency_ms: 340,
            head_lag_at_start: 1,
            head_lag_at_finish: 0,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        }]
    } else {
        Vec::new()
    };

    let head_eval_reports = if snapshot.browser_mode == "Verifier" {
        vec![HeadEvalReport {
            network_id: edge_snapshot.network_id.clone(),
            experiment_id: entry.experiment_id.clone(),
            revision_id: entry.current_revision_id.clone(),
            workload_id: entry.workload_id.clone(),
            head_id: latest_head_id.clone(),
            base_head_id: None,
            eval_protocol_id: ContentId::new("eval-main"),
            evaluator_set_id: ContentId::new("eval-set-browser"),
            metric_values: BTreeMap::from([("validation_loss".into(), MetricValue::Float(0.2412))]),
            sample_count: 128,
            dataset_view_id: entry.dataset_view_id.clone(),
            started_at: generated_at - chrono::Duration::seconds(18),
            finished_at: generated_at,
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }]
    } else {
        Vec::new()
    };

    vec![MetricsCatchupBundle {
        network_id: edge_snapshot.network_id.clone(),
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        snapshot: MetricsSnapshot {
            manifest: MetricsSnapshotManifest {
                network_id: edge_snapshot.network_id.clone(),
                experiment_id: entry.experiment_id.clone(),
                revision_id: entry.current_revision_id.clone(),
                snapshot_seq: 1,
                covers_until_head_id: Some(latest_head_id),
                covers_until_merge_window_id: None,
                canonical_head_metrics_ref: ContentId::new("capture-head-metrics"),
                window_rollups_ref: ContentId::new("capture-window-rollups"),
                network_rollups_ref: ContentId::new("capture-network-rollups"),
                leaderboard_ref: Some(ContentId::new("capture-leaderboard")),
                created_at: generated_at,
                signatures: Vec::new(),
            },
            head_eval_reports,
            peer_window_metrics,
            reducer_cohort_metrics: Vec::new(),
            derived_metrics: Vec::new(),
        },
        ledger_segments: Vec::new(),
        generated_at,
    }]
}

fn browser_edge_mode(value: &str) -> BrowserEdgeMode {
    match value {
        "Full" => BrowserEdgeMode::Full,
        "Minimal" => BrowserEdgeMode::Minimal,
        _ => BrowserEdgeMode::Peer,
    }
}

fn browser_mode(value: &str) -> burn_p2p::BrowserMode {
    match value {
        "Trainer" => burn_p2p::BrowserMode::Trainer,
        "Verifier" => burn_p2p::BrowserMode::Verifier,
        "Disabled" => burn_p2p::BrowserMode::Disabled,
        _ => burn_p2p::BrowserMode::Observer,
    }
}

fn browser_directory_entries(
    snapshot: &PortalSnapshotView,
    network_id: &NetworkId,
    browser_mode: &str,
) -> Vec<ExperimentDirectoryEntry> {
    snapshot
        .experiments
        .iter()
        .map(|row| {
            let experiment_id = ExperimentId::new(&row.experiment_id);
            let revision_id = RevisionId::new(&row.revision_id);
            let current_head_id = snapshot
                .heads
                .iter()
                .find(|head| {
                    head.experiment_id == row.experiment_id && head.revision_id == row.revision_id
                })
                .map(|head| HeadId::new(&head.head_id))
                .or_else(|| {
                    row.has_head
                        .then(|| HeadId::new(format!("{}-head", row.experiment_id)))
                });
            let mut entry = ExperimentDirectoryEntry {
                network_id: network_id.clone(),
                study_id: StudyId::new(format!("study-{}", row.experiment_id)),
                experiment_id: experiment_id.clone(),
                workload_id: WorkloadId::new(format!("workload-{}", row.experiment_id)),
                display_name: row.display_name.clone(),
                model_schema_hash: ContentId::new(format!("model-{}", row.experiment_id)),
                dataset_view_id: DatasetViewId::new(format!("dataset-{}", row.experiment_id)),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::BrowserObserver]),
                    minimum_device_memory_bytes: None,
                    minimum_system_memory_bytes: Some(512 * 1024 * 1024),
                    estimated_download_bytes: 4 * 1024 * 1024,
                    estimated_window_seconds: row.estimated_window_seconds,
                },
                visibility: ExperimentVisibility::Public,
                opt_in_policy: ExperimentOptInPolicy::Open,
                current_revision_id: revision_id,
                current_head_id,
                allowed_roles: PeerRoleSet::new([
                    PeerRole::BrowserObserver,
                    PeerRole::BrowserVerifier,
                    PeerRole::BrowserTrainerWgpu,
                ]),
                allowed_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Discover,
                    ExperimentScope::Validate {
                        experiment_id: experiment_id.clone(),
                    },
                    ExperimentScope::Train { experiment_id },
                ]),
                metadata: BTreeMap::new(),
            };
            apply_browser_revision_policy(&mut entry, browser_mode);
            entry
        })
        .collect()
}

fn apply_browser_revision_policy(entry: &mut ExperimentDirectoryEntry, browser_mode: &str) {
    let (verifier, trainer_wgpu, requires_webgpu) = match browser_mode {
        "Trainer" => (true, true, true),
        "Verifier" => (true, false, false),
        "Disabled" => (false, false, false),
        _ => (false, false, false),
    };

    entry.apply_revision_policy(&RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("capture-release-train"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("capture-checkpoint-format"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("capture-training-config"),
        merge_topology_policy_hash: ContentId::new("capture-merge-topology"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 0,
        },
        lag_policy: LagPolicy::default(),
        merge_window_miss_policy: MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier,
            trainer_wgpu,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(entry.resource_requirements.estimated_window_seconds),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: format!("browser capture {}", entry.display_name),
    });
}

fn browser_heads(snapshot: &PortalSnapshotView) -> Vec<HeadDescriptor> {
    snapshot
        .heads
        .iter()
        .map(|row| HeadDescriptor {
            head_id: HeadId::new(&row.head_id),
            study_id: StudyId::new(format!("study-{}", row.experiment_id)),
            experiment_id: ExperimentId::new(&row.experiment_id),
            revision_id: RevisionId::new(&row.revision_id),
            artifact_id: ArtifactId::new(format!("artifact-{}", row.head_id)),
            parent_head_id: None,
            global_step: row.global_step,
            created_at: parse_timestamp(&row.created_at),
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.125))]),
        })
        .collect()
}

fn browser_leaderboard_entries(snapshot: &PortalSnapshotView) -> Vec<BrowserLeaderboardEntry> {
    let desired_count = snapshot
        .leaderboard
        .len()
        .max(snapshot.diagnostics.estimated_network_size.max(1));
    if desired_count == 0 {
        return Vec::new();
    }

    (0..desired_count)
        .map(|index| {
            let source = snapshot
                .leaderboard
                .get(index % snapshot.leaderboard.len().max(1));
            let label = source
                .map(|row| row.principal_label.clone())
                .unwrap_or_else(|| format!("peer-{index:04}"));
            let accepted_receipt_count = source
                .map(|row| row.accepted_receipt_count as u64)
                .unwrap_or((index % 6) as u64);
            let leaderboard_score_v1 = source
                .map(|row| row.leaderboard_score_v1)
                .unwrap_or(1.0 + index as f64 * 0.01);

            BrowserLeaderboardEntry {
                identity: BrowserLeaderboardIdentity {
                    principal_id: Some(PrincipalId::new(format!("principal-{index:04}"))),
                    peer_ids: BTreeSet::from([PeerId::new(format!("peer-{index:04}"))]),
                    label,
                    social_profile: None,
                },
                accepted_work_score: leaderboard_score_v1,
                quality_weighted_impact_score: leaderboard_score_v1,
                validation_service_score: 0.0,
                artifact_serving_score: 0.0,
                leaderboard_score_v1,
                accepted_receipt_count,
                last_receipt_at: Some(Utc::now()),
                badges: Vec::new(),
            }
        })
        .collect()
}

fn signed_browser_directory_snapshot(
    network_id: &NetworkId,
    entries: Vec<ExperimentDirectoryEntry>,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            BrowserDirectorySnapshot {
                network_id: network_id.clone(),
                generated_at: Utc::now(),
                entries,
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("browser-capture-authority"),
            key_id: "browser-capture-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "feed".into(),
        },
    )
    .expect("signed browser directory snapshot")
}

fn signed_browser_leaderboard_snapshot(
    network_id: &NetworkId,
    entries: Vec<BrowserLeaderboardEntry>,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            BrowserLeaderboardSnapshot {
                network_id: network_id.clone(),
                score_version: "leaderboard_score_v1".into(),
                entries,
                captured_at: Utc::now(),
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("browser-capture-authority"),
            key_id: "browser-capture-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "cafe".into(),
        },
    )
    .expect("signed browser leaderboard snapshot")
}

fn parse_timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn build_portal_capture_scenarios() -> Vec<PortalScenarioSpec> {
    vec![
        scenario_spec(
            "unauthenticated-landing",
            "Unauthenticated landing",
            "Auth-disabled portal posture with native-only guidance.",
            scenario_snapshot("unauthenticated-landing")
                .with_auth(false)
                .with_browser_mode("Disabled")
                .with_service_states(vec![
                    service_row("portal", "healthy", "reference portal responding"),
                    service_row("browser-ingress", "unavailable", "browser join disabled on this edge"),
                ])
                .with_runtime_states(vec![runtime_card(
                    "Browser edge",
                    "Blocked",
                    Some("viewer"),
                    "Enrollment requires pre-provisioned credentials and a native client path.",
                    None,
                )])
                .with_experiments(vec![experiment_row(
                    "Native only baseline",
                    "exp-native",
                    "rev-native",
                    false,
                    60,
                )])
                .build(),
            vec![],
        ),
        scenario_spec(
            "authenticated-landing",
            "Authenticated landing",
            "Interactive auth, visible heads, and public leaderboard.",
            scenario_snapshot("authenticated-landing")
                .with_auth(true)
                .with_login_providers(vec![
                    login_provider("GitHub", "/login/github", Some("/callback/github")),
                    login_provider("OIDC", "/login/oidc", Some("/callback/oidc")),
                ])
                .with_service_states(vec![
                    service_row("portal", "healthy", "interactive portal available"),
                    service_row("auth", "healthy", "GitHub and OIDC callbacks configured"),
                    service_row("browser-ingress", "healthy", "WebRTC and WSS available"),
                ])
                .with_runtime_states(vec![runtime_card(
                    "Browser session",
                    "Authenticated",
                    Some("viewer"),
                    "Scoped experiment access is active for browser-visible revisions.",
                    Some(100),
                )])
                .with_experiments(sample_experiments(3))
                .with_heads(sample_heads(2))
                .with_leaderboard(sample_leaderboard(3))
                .build(),
            vec![],
        ),
        scenario_spec(
            "multi-experiment-directory",
            "Multi-experiment directory",
            "Dense directory with multiple visible experiments and filtered search.",
            scenario_snapshot("multi-experiment-directory")
                .with_auth(true)
                .with_browser_mode("Observer")
                .with_login_providers(vec![login_provider(
                    "GitHub",
                    "/login/github",
                    Some("/callback/github"),
                )])
                .with_experiments(sample_experiments(8))
                .with_heads(sample_heads(5))
                .build(),
            vec![
                PortalCaptureInteraction {
                    action: "click".into(),
                    selector: Some(r#"[data-surface-target="viewer"]"#.into()),
                    value: None,
                    wait_for_text: Some("Overview".into()),
                },
            ],
        ),
        scenario_spec(
            "joining",
            "Joining",
            "Browser worker joining and syncing against the current certified head.",
            scenario_snapshot("joining")
                .with_browser_mode("Observer")
                .with_runtime_states(vec![runtime_card(
                    "Browser worker",
                    "Joining",
                    Some("viewer"),
                    "Directory synced; waiting for head catchup and transport stabilization.",
                    Some(24),
                )])
                .with_service_states(vec![
                    service_row("portal", "healthy", "portal snapshot current"),
                    service_row("browser-ingress", "healthy", "edge transport accepting joins"),
                ])
                .with_peer_states(vec![
                    peer_row(
                        "peer-browser-join",
                        "browser-observer",
                        "browser",
                        "joining",
                        Some("2.0 windows"),
                        Some("head sync in progress"),
                    ),
                    peer_row(
                        "peer-native-validator",
                        "validator",
                        "native",
                        "healthy",
                        Some("0.0 windows"),
                        Some("certified head available"),
                    ),
                ])
                .with_experiments(sample_experiments(2))
                .with_heads(sample_heads(1))
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="viewer"]"#.into()),
                value: None,
                wait_for_text: Some("Overview".into()),
            }],
        ),
        scenario_spec(
            "synced",
            "Synced",
            "Observer runtime synced to the current head with low lag.",
            scenario_snapshot("synced")
                .with_browser_mode("Observer")
                .with_runtime_states(vec![runtime_card(
                    "Browser worker",
                    "Synced",
                    Some("viewer"),
                    "Head catchup complete; observing certified merges and receipt flow.",
                    Some(100),
                )])
                .with_peer_states(vec![peer_row(
                    "peer-browser-sync",
                    "browser-observer",
                    "browser",
                    "synced",
                    Some("0.1 windows"),
                    Some("following certified head head-main-1"),
                )])
                .with_heads(sample_heads(2))
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="viewer"]"#.into()),
                value: None,
                wait_for_text: Some("Top participants".into()),
            }],
        ),
        scenario_spec(
            "training-active",
            "Training active",
            "Browser trainer active on a current window with shard progress and publish intent.",
            scenario_snapshot("training-active")
                .with_browser_mode("Trainer")
                .with_runtime_states(vec![runtime_card(
                    "Browser trainer",
                    "Training",
                    Some("trainer"),
                    "Window 42 active · shard 3/8 hydrated · receipt finalization pending.",
                    Some(63),
                )])
                .with_peer_states(vec![
                    peer_row(
                        "peer-browser-train",
                        "browser-trainer",
                        "browser",
                        "training",
                        Some("0.6 windows"),
                        Some("local compute steady"),
                    ),
                    peer_row(
                        "peer-native-train-2",
                        "trainer",
                        "native",
                        "training",
                        Some("0.3 windows"),
                        Some("accepted update pending merge"),
                    ),
                ])
                .with_metric_panels(vec![quality_panel(
                    "progress",
                    "Training progress",
                    "Local and canonical progress signals for the active browser window.",
                    vec![
                        metric_row("shard_fetch_progress", "3 / 8", "window", "local", "window-42"),
                        metric_row("publish_latency_ms_p50", "340.0", "window", "local", "window-42"),
                    ],
                )])
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="train"]"#.into()),
                value: None,
                wait_for_text: Some("Training".into()),
            }],
        ),
        scenario_spec(
            "validating",
            "Validating",
            "Verifier runtime reviewing a certified head and emitting validation receipts.",
            scenario_snapshot("validating")
                .with_browser_mode("Verifier")
                .with_runtime_states(vec![runtime_card(
                    "Browser verifier",
                    "Validating",
                    Some("validator"),
                    "Validation plan active for head head-main-2 with canonical trust posture.",
                    Some(78),
                )])
                .with_peer_states(vec![peer_row(
                    "peer-browser-verify",
                    "browser-verifier",
                    "browser",
                    "validating",
                    Some("0.2 windows"),
                    Some("receipt acceptance expected on completion"),
                )])
                .with_heads(sample_heads(2))
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="validate"]"#.into()),
                value: None,
                wait_for_text: Some("Validation".into()),
            }],
        ),
        scenario_spec(
            "publishing-download",
            "Publishing and download",
            "Artifact aliases showing queued, uploading, and ready states with live export/download actions.",
            scenario_snapshot("publishing-download")
                .with_browser_mode("Trainer")
                .with_runtime_states(vec![runtime_card(
                    "Artifact flow",
                    "Publishing",
                    Some("trainer"),
                    "Latest alias resolves to a ready head while the next export is still uploading.",
                    Some(84),
                )])
                .with_artifact_rows(vec![
                    artifact_row(ArtifactRowSpec {
                        slug: "publishing-download",
                        alias_name: "latest/serve",
                        experiment_id: "exp-main-1",
                        run_id: Some("run-main-1"),
                        head_id: "head-main-2",
                        status: "Ready",
                        last_published_at: Some("2026-04-04T18:00:00Z"),
                        previous_head_id: Some("head-main-1"),
                    }),
                    artifact_row(ArtifactRowSpec {
                        slug: "publishing-download",
                        alias_name: "best_val/serve",
                        experiment_id: "exp-main-1",
                        run_id: Some("run-main-1"),
                        head_id: "head-main-3",
                        status: "Uploading",
                        last_published_at: None,
                        previous_head_id: Some("head-main-2"),
                    }),
                ])
                .build(),
            vec![
                PortalCaptureInteraction {
                    action: "click".into(),
                    selector: Some(r#"[data-surface-target="train"]"#.into()),
                    value: None,
                    wait_for_text: Some("Training".into()),
                },
            ],
        ),
        scenario_spec(
            "lagged-catchup",
            "Lagged catchup",
            "Browser runtime behind the frontier and explicitly in catchup mode.",
            scenario_snapshot("lagged-catchup")
                .with_browser_mode("Trainer")
                .with_runtime_states(vec![runtime_card(
                    "Browser worker",
                    "Catchup",
                    Some("trainer"),
                    "Peer fell behind the frontier and is rebasing against a newer certified head.",
                    Some(41),
                )])
                .with_peer_states(vec![
                    peer_row(
                        "peer-browser-lagged",
                        "browser-trainer",
                        "browser",
                        "catchup",
                        Some("5.4 windows"),
                        Some("base head rebasing in progress"),
                    ),
                    peer_row(
                        "peer-native-validator",
                        "validator",
                        "native",
                        "healthy",
                        Some("0.0 windows"),
                        Some("frontier unchanged"),
                    ),
                ])
                .with_metric_panels(vec![quality_panel(
                    "dynamics",
                    "Catchup dynamics",
                    "Lag and adoption signals for the current browser peer.",
                    vec![
                        metric_row("head_adoption_lag_p90", "5.4", "network", "canonical", "exp-main-1"),
                        metric_row("catchup_backlog", "12 updates", "peer", "local", "peer-browser-lagged"),
                    ],
                )])
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="train"]"#.into()),
                value: None,
                wait_for_text: Some("Training".into()),
            }],
        ),
        scenario_spec(
            "degraded-services",
            "Degraded services",
            "Portal up, but metrics, publication, and browser ingress are degraded or unavailable.",
            scenario_snapshot("degraded-services")
                .with_service_states(vec![
                    service_row("portal", "healthy", "reference portal responding"),
                    service_row("metrics-indexer", "degraded", "ledger replay stalled; using last durable snapshot"),
                    service_row("artifact-publish", "degraded", "export queue backed up behind object store retries"),
                    service_row("browser-ingress", "unavailable", "temporary edge ingress outage"),
                ])
                .with_runtime_states(vec![runtime_card(
                    "Browser edge",
                    "Degraded",
                    Some("network"),
                    "Interactive surface remains up while auxiliary services recover.",
                    None,
                )])
                .build(),
            vec![PortalCaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="network"]"#.into()),
                value: None,
                wait_for_text: Some("Connection".into()),
            }],
        ),
        scenario_spec_with_viewport(
            "mobile-viewer",
            "Mobile viewer",
            "Compact mobile capture for leaderboard and network-first viewing.",
            scenario_snapshot("mobile-viewer")
                .with_browser_mode("Observer")
                .with_runtime_states(vec![runtime_card(
                    "Browser session",
                    "Synced",
                    Some("viewer"),
                    "Mobile viewer is following the latest certified head without opting into compute work.",
                    Some(100),
                )])
                .with_heads(sample_heads(2))
                .with_leaderboard(sample_leaderboard(4))
                .build(),
            Some(PortalCaptureViewport {
                width: 390,
                height: 844,
            }),
            vec![],
        ),
        fleet_scenario("peers-32", "32-peer fleet", 32),
        fleet_scenario("peers-256", "256-peer fleet", 256),
        fleet_scenario("peers-1024", "1024-peer fleet", 1024),
    ]
}

fn fleet_scenario(slug: &str, title: &str, peer_count: usize) -> PortalScenarioSpec {
    let visible_peer_count = peer_count.min(14);
    let peers = (0..visible_peer_count)
        .map(|index| {
            let status = match index % 5 {
                0 => "healthy",
                1 => "training",
                2 => "validating",
                3 => "catchup",
                _ => "observer",
            };
            let role = match index % 4 {
                0 => "trainer",
                1 => "browser-trainer",
                2 => "verifier",
                _ => "observer",
            };
            let platform = if index % 3 == 0 { "browser" } else { "native" };
            peer_row(
                &format!("peer-{index:04}"),
                role,
                platform,
                status,
                Some(&format!("{:.1} windows", (index % 7) as f32 * 0.4)),
                Some("fleet scale sample"),
            )
        })
        .collect::<Vec<_>>();
    scenario_spec(
        slug,
        title,
        "Browser-scale peer preview showing a small direct set against a much larger estimated network.",
        scenario_snapshot(slug)
            .with_network_estimate(peer_count)
            .with_observed_peers(peer_count.min(64))
            .with_runtime_states(vec![runtime_card(
                "Fleet preview",
                "Visible",
                Some("network"),
                &format!(
                    "Showing {visible_peer_count} directly relevant peers while the wider network estimate remains {peer_count}."
                ),
                Some(100),
            )])
            .with_service_states(vec![
                service_row("portal", "healthy", "reference portal responding"),
                service_row("browser-ingress", "healthy", "browser fleet admitted"),
                service_row("metrics-indexer", "healthy", "summary data current"),
            ])
            .with_experiments(sample_experiments(6))
            .with_heads(sample_heads(4))
            .with_peer_states(peers)
            .build(),
        vec![PortalCaptureInteraction {
            action: "click".into(),
            selector: Some(r#"[data-surface-target="network"]"#.into()),
            value: None,
            wait_for_text: Some("Connection".into()),
        }],
    )
}

fn scenario_spec(
    slug: &str,
    title: &str,
    description: &str,
    snapshot: PortalSnapshotView,
    interactions: Vec<PortalCaptureInteraction>,
) -> PortalScenarioSpec {
    scenario_spec_with_viewport(slug, title, description, snapshot, None, interactions)
}

fn scenario_spec_with_viewport(
    slug: &str,
    title: &str,
    description: &str,
    snapshot: PortalSnapshotView,
    viewport: Option<PortalCaptureViewport>,
    interactions: Vec<PortalCaptureInteraction>,
) -> PortalScenarioSpec {
    let scenario_root = format!("scenarios/{slug}");
    PortalScenarioSpec {
        scenario: PortalCaptureScenario {
            slug: slug.into(),
            title: title.into(),
            description: description.into(),
            html_path: format!("{scenario_root}/index.html"),
            snapshot_path: format!("{scenario_root}/snapshot.json"),
            scenario_path: format!("{scenario_root}/scenario.json"),
            peer_count: snapshot.peer_statuses.len(),
            experiment_count: snapshot.experiments.len(),
            estimated_network_size: snapshot.diagnostics.estimated_network_size,
            runtime_states: snapshot
                .runtime_states
                .iter()
                .map(|card| card.state.clone())
                .collect(),
            viewport,
            interactions,
        },
        snapshot,
    }
}

#[derive(Clone, Debug)]
struct SnapshotBuilder {
    snapshot: PortalSnapshotView,
}

impl SnapshotBuilder {
    fn with_auth(mut self, auth_enabled: bool) -> Self {
        self.snapshot.auth_enabled = auth_enabled;
        self
    }

    fn with_browser_mode(mut self, browser_mode: &str) -> Self {
        self.snapshot.browser_mode = browser_mode.into();
        self
    }

    fn with_login_providers(mut self, login_providers: Vec<PortalLoginProvider>) -> Self {
        self.snapshot.login_providers = login_providers;
        self
    }

    fn with_network_estimate(mut self, estimated_network_size: usize) -> Self {
        self.snapshot.diagnostics.estimated_network_size = estimated_network_size;
        self
    }

    fn with_observed_peers(mut self, observed_peers: usize) -> Self {
        self.snapshot.diagnostics.observed_peers = observed_peers;
        self
    }

    fn with_experiments(mut self, experiments: Vec<PortalExperimentRow>) -> Self {
        self.snapshot.experiments = experiments;
        self
    }

    fn with_heads(mut self, heads: Vec<PortalHeadRow>) -> Self {
        self.snapshot.heads = heads;
        self
    }

    fn with_leaderboard(mut self, leaderboard: Vec<PortalLeaderboardRow>) -> Self {
        self.snapshot.leaderboard = leaderboard;
        self
    }

    fn with_artifact_rows(mut self, artifact_rows: Vec<PortalArtifactRow>) -> Self {
        self.snapshot.artifact_rows = artifact_rows;
        self
    }

    fn with_metric_panels(mut self, metrics_panels: Vec<PortalMetricsPanel>) -> Self {
        self.snapshot.metrics_panels = metrics_panels;
        self
    }

    fn with_runtime_states(mut self, runtime_states: Vec<PortalRuntimeStateCard>) -> Self {
        self.snapshot.runtime_states = runtime_states;
        self
    }

    fn with_service_states(mut self, service_statuses: Vec<PortalServiceStatusRow>) -> Self {
        self.snapshot.service_statuses = service_statuses;
        self
    }

    fn with_peer_states(mut self, peer_statuses: Vec<PortalPeerStatusRow>) -> Self {
        self.snapshot.peer_statuses = peer_statuses;
        self
    }

    fn build(mut self) -> PortalSnapshotView {
        if self.snapshot.service_statuses.is_empty() {
            self.snapshot.service_statuses = vec![service_row(
                "portal",
                "healthy",
                "reference portal responding",
            )];
        }
        if self.snapshot.experiments.is_empty() {
            self.snapshot.experiments = sample_experiments(2);
        }
        if self.snapshot.heads.is_empty() {
            self.snapshot.heads = sample_heads(1);
        }
        if self.snapshot.leaderboard.is_empty() && self.snapshot.social_enabled {
            self.snapshot.leaderboard = sample_leaderboard(2);
        }
        self.snapshot.diagnostics.connected_peers = self.snapshot.peer_statuses.len().max(1);
        self.snapshot.diagnostics.observed_peers = self
            .snapshot
            .diagnostics
            .observed_peers
            .max(self.snapshot.peer_statuses.len().max(1));
        self.snapshot.diagnostics.estimated_network_size = self
            .snapshot
            .diagnostics
            .estimated_network_size
            .max(self.snapshot.diagnostics.observed_peers);
        self.snapshot.diagnostics.admitted_peers = self.snapshot.peer_statuses.len().max(1);
        self.snapshot.diagnostics.rejected_peers = self
            .snapshot
            .service_statuses
            .iter()
            .filter(|row| row.status == "unavailable")
            .count();
        self.snapshot
    }
}

fn scenario_snapshot(slug: &str) -> SnapshotBuilder {
    SnapshotBuilder {
        snapshot: PortalSnapshotView {
            network_id: "capture-net".into(),
            captured_at: "2026-04-04T18:00:00Z".into(),
            auth_enabled: true,
            edge_mode: "Full".into(),
            browser_mode: "Observer".into(),
            social_enabled: true,
            profile_enabled: true,
            login_providers: Vec::new(),
            transports: PortalTransportSurface {
                webrtc_direct: true,
                webtransport_gateway: true,
                wss_fallback: true,
            },
            paths: scenario_paths(slug),
            diagnostics: PortalDiagnosticsView {
                connected_peers: 1,
                observed_peers: 8,
                estimated_network_size: 48,
                admitted_peers: 1,
                rejected_peers: 0,
                quarantined_peers: 0,
                banned_peers: 0,
                in_flight_transfers: 0,
                accepted_receipts: 18,
                certified_merges: 4,
                eta_lower_seconds: Some(180),
                eta_upper_seconds: Some(260),
                node_state: "Idle ready".into(),
                last_error: None,
                active_services: vec![
                    "portal".into(),
                    "browser-edge".into(),
                    "metrics-indexer".into(),
                    "artifact-publish".into(),
                ],
            },
            trust: PortalTrustView {
                required_release_train_hash: Some("release-train-main".into()),
                approved_target_artifact_count: 2,
                active_issuer_peer_id: Some("issuer-main".into()),
                minimum_revocation_epoch: Some(12),
                reenrollment_required: false,
            },
            runtime_states: Vec::new(),
            service_statuses: Vec::new(),
            peer_statuses: Vec::new(),
            experiments: Vec::new(),
            heads: Vec::new(),
            leaderboard: Vec::new(),
            artifact_rows: Vec::new(),
            metrics_panels: Vec::new(),
        },
    }
}

fn scenario_paths(slug: &str) -> PortalPaths {
    PortalPaths {
        portal_snapshot_path: format!("/{slug}/portal/snapshot"),
        signed_directory_path: format!("/{slug}/directory/signed"),
        signed_leaderboard_path: format!("/{slug}/leaderboard/signed"),
        artifacts_aliases_path: format!("/{slug}/artifacts/aliases"),
        artifacts_export_path: format!("/{slug}/artifacts/export"),
        artifacts_download_ticket_path: format!("/{slug}/artifacts/download-ticket"),
        trust_bundle_path: format!("/{slug}/trust"),
    }
}

fn login_provider(
    label: &str,
    login_path: &str,
    callback_path: Option<&str>,
) -> PortalLoginProvider {
    PortalLoginProvider {
        label: label.into(),
        login_path: login_path.into(),
        callback_path: callback_path.map(str::to_owned),
        device_path: None,
    }
}

fn experiment_row(
    display_name: &str,
    experiment_id: &str,
    revision_id: &str,
    has_head: bool,
    estimated_window_seconds: u64,
) -> PortalExperimentRow {
    PortalExperimentRow {
        display_name: display_name.into(),
        experiment_id: experiment_id.into(),
        revision_id: revision_id.into(),
        has_head,
        estimated_window_seconds,
    }
}

fn sample_experiments(count: usize) -> Vec<PortalExperimentRow> {
    (1..=count)
        .map(|index| {
            experiment_row(
                &format!("Experiment {index}"),
                &format!("exp-main-{index}"),
                &format!("rev-main-{index}"),
                true,
                30 + (index as u64 * 5),
            )
        })
        .collect()
}

fn sample_heads(count: usize) -> Vec<PortalHeadRow> {
    (1..=count)
        .map(|index| PortalHeadRow {
            experiment_id: format!("exp-main-{index}"),
            revision_id: format!("rev-main-{index}"),
            head_id: format!("head-main-{index}"),
            global_step: (index as u64) * 12,
            created_at: format!("2026-04-04T18:{:02}:00Z", index % 60),
        })
        .collect()
}

fn sample_leaderboard(count: usize) -> Vec<PortalLeaderboardRow> {
    (1..=count)
        .map(|index| PortalLeaderboardRow {
            principal_label: format!("participant-{index}"),
            leaderboard_score_v1: index as f64 * 1.75,
            accepted_receipt_count: index * 2,
        })
        .collect()
}

fn quality_panel(
    panel_id: &str,
    title: &str,
    description: &str,
    rows: Vec<PortalMetricRow>,
) -> PortalMetricsPanel {
    PortalMetricsPanel {
        panel_id: panel_id.into(),
        title: title.into(),
        description: description.into(),
        rows,
    }
}

fn metric_row(label: &str, value: &str, scope: &str, trust: &str, key: &str) -> PortalMetricRow {
    PortalMetricRow {
        label: label.into(),
        value: value.into(),
        scope: scope.into(),
        trust: trust.into(),
        key: key.into(),
        protocol: None,
        operator_hint: None,
        detail_path: None,
        freshness: "2026-04-04T18:00:00Z".into(),
    }
}

fn service_row(service: &str, status: &str, detail: &str) -> PortalServiceStatusRow {
    PortalServiceStatusRow {
        service: service.into(),
        status: status.into(),
        detail: detail.into(),
    }
}

fn runtime_card(
    label: &str,
    state: &str,
    role: Option<&str>,
    detail: &str,
    progress_percent: Option<u8>,
) -> PortalRuntimeStateCard {
    PortalRuntimeStateCard {
        label: label.into(),
        state: state.into(),
        role: role.map(str::to_owned),
        detail: detail.into(),
        progress_percent,
    }
}

fn peer_row(
    peer_label: &str,
    role: &str,
    platform: &str,
    status: &str,
    lag_label: Option<&str>,
    note: Option<&str>,
) -> PortalPeerStatusRow {
    PortalPeerStatusRow {
        peer_label: peer_label.into(),
        role: role.into(),
        platform: platform.into(),
        status: status.into(),
        lag_label: lag_label.map(str::to_owned),
        note: note.map(str::to_owned),
    }
}

struct ArtifactRowSpec<'a> {
    slug: &'a str,
    alias_name: &'a str,
    experiment_id: &'a str,
    run_id: Option<&'a str>,
    head_id: &'a str,
    status: &'a str,
    last_published_at: Option<&'a str>,
    previous_head_id: Option<&'a str>,
}

fn artifact_row(spec: ArtifactRowSpec<'_>) -> PortalArtifactRow {
    PortalArtifactRow {
        alias_name: spec.alias_name.into(),
        scope: "Run".into(),
        artifact_profile: "ServeCheckpoint".into(),
        experiment_id: spec.experiment_id.into(),
        run_id: spec.run_id.map(str::to_owned),
        head_id: spec.head_id.into(),
        publication_target_id: "local-default".into(),
        artifact_alias_id: Some(format!("alias-{}-{}", spec.slug, spec.head_id)),
        status: spec.status.into(),
        last_published_at: spec.last_published_at.map(str::to_owned),
        history_count: 3,
        previous_head_id: spec.previous_head_id.map(str::to_owned),
        head_view_path: format!("/{}/portal/artifacts/heads/{}", spec.slug, spec.head_id),
        run_view_path: spec.run_id.map(|run_id| {
            format!(
                "/{}/portal/artifacts/runs/{}/{}",
                spec.slug, spec.experiment_id, run_id
            )
        }),
        export_path: format!("/{}/artifacts/export", spec.slug),
        download_ticket_path: format!("/{}/artifacts/download-ticket", spec.slug),
    }
}
