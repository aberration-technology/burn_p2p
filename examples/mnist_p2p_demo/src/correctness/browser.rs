use std::{
    collections::BTreeSet,
    fs,
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::Context;
use burn_p2p::{
    AssignmentLease, AuthProvider, BrowserEdgeSnapshot, BrowserLoginProvider, BrowserMode,
    ContentId, ExperimentDirectoryEntry, ExperimentScope, HeadDescriptor, NetworkManifest, PeerId,
    PeerRoleSet, PrincipalClaims, PrincipalId, PrincipalSession, ShardCache, ShardFetchManifest,
};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserGpuSupport, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserRuntimeState, BrowserSessionState, BrowserTrainingBudget, BrowserTrainingPlan,
    BrowserTransportKind, BrowserTransportStatus, BrowserValidationPlan, BrowserWorkerCommand,
    BrowserWorkerEvent, BrowserWorkerRuntime,
};
use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths, BrowserLeaderboardEntry,
    BrowserLeaderboardSnapshot, BrowserTransportSurface, PeerRole, ProfileMode, SocialMode,
    TrustBundleExport,
};
use burn_p2p_metrics::MetricsCatchupBundle;
use burn_p2p_views::BrowserAppSurface;
use chrono::Utc;

use crate::{
    correctness::export::{
        BrowserDatasetAccessSummary, BrowserRoleExerciseSummary, BrowserScenarioExport,
        CaptureInteraction,
    },
    data::PreparedMnistData,
};

pub fn browser_scenarios(
    network_manifest: &NetworkManifest,
    release_manifest: &burn_p2p::ClientReleaseManifest,
    directory_entries: Vec<ExperimentDirectoryEntry>,
    heads: Vec<HeadDescriptor>,
    leaderboard_entries: Vec<BrowserLeaderboardEntry>,
    metrics_catchup: Vec<MetricsCatchupBundle>,
) -> Vec<BrowserScenarioExport> {
    let build_snapshot = |browser_mode: BrowserMode| BrowserEdgeSnapshot {
        network_id: network_manifest.network_id.clone(),
        edge_mode: BrowserEdgeMode::Full,
        browser_mode,
        social_mode: SocialMode::Public,
        profile_mode: ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: false,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths::default(),
        auth_enabled: false,
        login_providers: Vec::<BrowserLoginProvider>::new(),
        required_release_train_hash: Some(release_manifest.release_train_hash.clone()),
        allowed_target_artifact_hashes: BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: network_manifest.network_id.clone(),
            generated_at: Utc::now(),
            entries: directory_entries.clone(),
        },
        heads: heads.clone(),
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: network_manifest.network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: leaderboard_entries.clone(),
            captured_at: Utc::now(),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id: network_manifest.network_id.clone(),
            project_family_id: release_manifest.project_family_id.clone(),
            required_release_train_hash: release_manifest.release_train_hash.clone(),
            allowed_target_artifact_hashes: BTreeSet::from([release_manifest
                .target_artifact_hash
                .clone()]),
            minimum_revocation_epoch: burn_p2p::RevocationEpoch(0),
            active_issuer_peer_id: PeerId::new("mnist-validator"),
            issuers: Vec::new(),
            reenrollment: None,
        }),
        captured_at: Utc::now(),
    };

    vec![
        BrowserScenarioExport {
            slug: "mnist-viewer".into(),
            title: "mnist viewer".into(),
            description: "viewer surface with two experiments, leaderboard, and current heads"
                .into(),
            default_surface: BrowserAppSurface::Viewer,
            snapshot: build_snapshot(BrowserMode::Observer),
            metrics_catchup: metrics_catchup.clone(),
            runtime_states: vec!["observer".into()],
            interactions: vec![CaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="overview"]"#.into()),
                value: None,
                wait_for_text: Some("top participants".into()),
            }],
            viewport: None,
        },
        BrowserScenarioExport {
            slug: "mnist-validate".into(),
            title: "mnist validate".into(),
            description: "validator-facing surface with custom mnist evaluation metrics".into(),
            default_surface: BrowserAppSurface::Validate,
            snapshot: build_snapshot(BrowserMode::Verifier),
            metrics_catchup: metrics_catchup.clone(),
            runtime_states: vec!["verifier".into()],
            interactions: vec![CaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="validate"]"#.into()),
                value: None,
                wait_for_text: Some("digit_zero_accuracy".into()),
            }],
            viewport: None,
        },
        BrowserScenarioExport {
            slug: "mnist-train".into(),
            title: "mnist train".into(),
            description: "trainer-facing surface with shard-backed progress and loss metrics"
                .into(),
            default_surface: BrowserAppSurface::Train,
            snapshot: build_snapshot(BrowserMode::Trainer),
            metrics_catchup: metrics_catchup.clone(),
            runtime_states: vec!["trainer".into()],
            interactions: vec![CaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="train"]"#.into()),
                value: None,
                wait_for_text: Some("training".into()),
            }],
            viewport: None,
        },
        BrowserScenarioExport {
            slug: "mnist-network".into(),
            title: "mnist network".into(),
            description:
                "network surface showing helper, validator, viewer, and trainer roles on one net"
                    .into(),
            default_surface: BrowserAppSurface::Network,
            snapshot: build_snapshot(BrowserMode::Observer),
            metrics_catchup,
            runtime_states: vec!["network".into()],
            interactions: vec![CaptureInteraction {
                action: "click".into(),
                selector: Some(r#"[data-surface-target="network"]"#.into()),
                value: None,
                wait_for_text: Some("connection".into()),
            }],
            viewport: None,
        },
    ]
}

pub fn exercise_browser_roles(
    network_manifest: &NetworkManifest,
    release_manifest: &burn_p2p::ClientReleaseManifest,
    directory_entries: &[ExperimentDirectoryEntry],
    heads: &[HeadDescriptor],
) -> anyhow::Result<Vec<BrowserRoleExerciseSummary>> {
    let directory = BrowserDirectorySnapshot {
        network_id: network_manifest.network_id.clone(),
        generated_at: Utc::now(),
        entries: directory_entries.to_vec(),
    };
    let baseline_entry = directory_entries
        .first()
        .context("mnist demo missing browser-visible directory entry")?;
    let active_head = heads
        .iter()
        .filter(|head| {
            head.experiment_id == baseline_entry.experiment_id
                && head.revision_id == baseline_entry.current_revision_id
        })
        .max_by_key(|head| head.global_step)
        .context("mnist demo missing browser-visible head")?;

    Ok(vec![
        exercise_browser_role(
            release_manifest,
            &directory,
            heads,
            BrowserRuntimeRole::BrowserObserver,
            None,
            None,
        ),
        exercise_browser_role(
            release_manifest,
            &directory,
            heads,
            BrowserRuntimeRole::BrowserVerifier,
            Some(BrowserWorkerCommand::Verify(BrowserValidationPlan {
                head_id: active_head.head_id.clone(),
                max_checkpoint_bytes: 16 * 1024 * 1024,
                sample_budget: 16,
                emit_receipt: true,
            })),
            Some("browser verifier currently exercises receipt-producing validation flow over synced browser state, not full burn model eval".into()),
        ),
        exercise_browser_role(
            release_manifest,
            &directory,
            heads,
            BrowserRuntimeRole::BrowserTrainerWgpu,
            Some(BrowserWorkerCommand::Train(BrowserTrainingPlan {
                study_id: active_head.study_id.clone(),
                experiment_id: active_head.experiment_id.clone(),
                revision_id: active_head.revision_id.clone(),
                workload_id: baseline_entry.workload_id.clone(),
                budget: BrowserTrainingBudget {
                    max_window_secs: 12,
                    requires_webgpu: true,
                    ..BrowserTrainingBudget::default()
                },
                lease: None,
            })),
            Some("browser trainer exercises capability, assignment, transport, and receipt flow in the runtime drill; the e2e mnist harness pairs it with live burn/webgpu wasm execution against the same leased shards".into()),
        ),
    ])
}

pub fn probe_browser_http_shard_fetch(
    dataset_root: &Path,
    prepared_data: &PreparedMnistData,
    lease: &AssignmentLease,
) -> anyhow::Result<BrowserDatasetAccessSummary> {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener
        .set_nonblocking(true)
        .context("failed to set http probe listener nonblocking")?;
    let addr = listener.local_addr()?;
    let requests_for_thread = Arc::clone(&requests);
    let stop_for_thread = Arc::clone(&stop);
    let root = dataset_root.to_path_buf();
    let server = thread::spawn(move || {
        while !stop_for_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    let mut line = String::new();
                    reader.read_line(&mut line).expect("request line");
                    let path = line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/")
                        .trim_start_matches('/')
                        .to_owned();
                    requests_for_thread
                        .lock()
                        .expect("requests")
                        .push(path.clone());
                    loop {
                        let mut header = String::new();
                        reader.read_line(&mut header).expect("header");
                        if header == "\r\n" || header.is_empty() {
                            break;
                        }
                    }
                    let body = fs::read(root.join(&path)).expect("served file");
                    stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .expect("write head");
                    stream.write_all(&body).expect("write body");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("probe server error: {error}"),
            }
        }
    });

    let http_dataset = prepared_data
        .train_dataset
        .clone()
        .with_http_upstream(format!("http://{addr}"));
    let cache_root = dataset_root.join("browser-http-cache");
    if cache_root.exists() {
        fs::remove_dir_all(&cache_root)
            .with_context(|| format!("failed to reset {}", cache_root.display()))?;
    }
    let cache = ShardCache::new(&cache_root);
    cache
        .fetch_lease_microshards(
            http_dataset.registration(),
            http_dataset.microshard_plan(),
            lease,
        )
        .context("failed to fetch leased shards through browser http probe")?;

    stop.store(true, Ordering::Relaxed);
    let _ = server.join();

    let requests = requests.lock().expect("requests").clone();
    let manifest: ShardFetchManifest = serde_json::from_slice(
        &fs::read(dataset_root.join("fetch-manifest.json")).with_context(|| {
            format!(
                "failed to read {}",
                dataset_root.join("fetch-manifest.json").display()
            )
        })?,
    )?;
    let expected_paths = manifest
        .entries
        .iter()
        .filter(|entry| lease.microshards.contains(&entry.microshard_id))
        .map(|entry| entry.locator.clone())
        .collect::<Vec<_>>();
    let requested_shards = requests
        .iter()
        .filter(|path| *path != "fetch-manifest.json")
        .cloned()
        .collect::<Vec<_>>();

    Ok(BrowserDatasetAccessSummary {
        upstream_mode: "http".into(),
        browser_http_base_url: format!("http://{addr}"),
        fetch_manifest_requested: requests.iter().any(|path| path == "fetch-manifest.json"),
        leased_microshards: lease.microshards.clone(),
        requested_paths: requests.clone(),
        fetched_only_leased_shards: requested_shards == expected_paths,
        shards_distributed_over_p2p: false,
        notes: vec![
            "browser-style shard access uses the dataset upstream adapter plus fetch-manifest.json".into(),
            "the probe only requested the manifest and the locators for the leased microshards".into(),
            "mnist shards are served from the prepared dataset root over http, not broadcast over the peer overlay".into(),
        ],
    })
}

fn exercise_browser_role(
    release_manifest: &burn_p2p::ClientReleaseManifest,
    directory: &BrowserDirectorySnapshot,
    heads: &[HeadDescriptor],
    role: BrowserRuntimeRole,
    command: Option<BrowserWorkerCommand>,
    note: Option<String>,
) -> BrowserRoleExerciseSummary {
    let capability = browser_capability_for_role(&role);
    let session = browser_session(directory);
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: role.clone(),
            selected_experiment: directory
                .entries
                .first()
                .map(|entry| entry.experiment_id.clone()),
            selected_revision: directory
                .entries
                .first()
                .map(|entry| entry.current_revision_id.clone()),
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                directory.network_id.clone(),
                release_manifest.release_train_hash.clone(),
                release_manifest.target_artifact_id.clone(),
                release_manifest.target_artifact_hash.clone(),
            )
        },
        capability,
        BrowserTransportStatus::default(),
    );
    runtime.remember_session(session.clone());
    runtime.apply_directory_snapshot(directory, Some(&session));
    runtime.apply_head_snapshot(heads);

    let degraded = BrowserTransportStatus {
        active: None,
        webrtc_direct_enabled: false,
        webtransport_enabled: false,
        wss_fallback_enabled: false,
        last_error: Some("simulated transport stall".into()),
    };
    runtime.update_transport_status(degraded);
    let stalled = runtime.state.clone();
    runtime.update_transport_status(BrowserTransportStatus::default());
    let recovered_state = runtime.state.clone();
    let transport_recovered = matches!(
        recovered_state,
        Some(BrowserRuntimeState::Observer)
            | Some(BrowserRuntimeState::Verifier)
            | Some(BrowserRuntimeState::Trainer)
    ) && stalled
        .as_ref()
        .is_some_and(|state| matches!(state, BrowserRuntimeState::Joining { .. }));

    let mut command_completed = false;
    let mut emitted_receipt_id = None;
    if let Some(command) = command {
        for event in runtime.apply_command(command, Some(directory), Some(&session)) {
            match event {
                BrowserWorkerEvent::TrainingCompleted(result) => {
                    command_completed = true;
                    emitted_receipt_id = result.receipt_id.map(|id| id.as_str().to_owned());
                }
                BrowserWorkerEvent::ValidationCompleted(result) => {
                    command_completed = true;
                    emitted_receipt_id = result.emitted_receipt_id.map(|id| id.as_str().to_owned());
                }
                _ => {}
            }
        }
    }

    BrowserRoleExerciseSummary {
        role: browser_role_label(&role).into(),
        runtime_state: browser_runtime_state_label(runtime.state.as_ref()).into(),
        active_assignment: runtime.storage.active_assignment.is_some(),
        active_experiment_id: runtime
            .storage
            .active_assignment
            .as_ref()
            .map(|assignment| assignment.experiment_id.clone()),
        active_revision_id: runtime
            .storage
            .active_assignment
            .as_ref()
            .map(|assignment| assignment.revision_id.clone()),
        active_head_id: runtime.storage.last_head_id.clone(),
        transport: runtime
            .transport
            .active
            .as_ref()
            .map(browser_transport_label),
        command_completed,
        emitted_receipt_id,
        transport_recovered,
        notes: note.into_iter().collect(),
    }
}

fn browser_session(directory: &BrowserDirectorySnapshot) -> BrowserSessionState {
    let mut scopes = BTreeSet::from([ExperimentScope::Connect, ExperimentScope::Discover]);
    for entry in &directory.entries {
        scopes.insert(ExperimentScope::Train {
            experiment_id: entry.experiment_id.clone(),
        });
        scopes.insert(ExperimentScope::Validate {
            experiment_id: entry.experiment_id.clone(),
        });
    }
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("mnist-browser-session"),
            network_id: directory.network_id.clone(),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("mnist-browser-principal"),
                provider: AuthProvider::Static {
                    authority: "mnist-demo".into(),
                },
                display_name: "mnist browser".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([
                    PeerRole::BrowserObserver,
                    PeerRole::BrowserVerifier,
                    PeerRole::BrowserTrainerWgpu,
                ]),
                granted_scopes: scopes,
                custom_claims: Default::default(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(30),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
        }),
        certificate: None,
        trust_bundle: None,
        enrolled_at: Some(Utc::now()),
        reenrollment_required: false,
    }
}

fn browser_capability_for_role(role: &BrowserRuntimeRole) -> BrowserCapabilityReport {
    let recommended_role = match role {
        BrowserRuntimeRole::Viewer | BrowserRuntimeRole::BrowserObserver => {
            BrowserRuntimeRole::BrowserObserver
        }
        BrowserRuntimeRole::BrowserVerifier => BrowserRuntimeRole::BrowserVerifier,
        BrowserRuntimeRole::BrowserTrainerWgpu => BrowserRuntimeRole::BrowserTrainerWgpu,
        BrowserRuntimeRole::BrowserFallback => BrowserRuntimeRole::BrowserObserver,
    };
    BrowserCapabilityReport {
        navigator_gpu_exposed: matches!(role, BrowserRuntimeRole::BrowserTrainerWgpu),
        worker_gpu_exposed: matches!(role, BrowserRuntimeRole::BrowserTrainerWgpu),
        gpu_support: if matches!(role, BrowserRuntimeRole::BrowserTrainerWgpu) {
            BrowserGpuSupport::Available
        } else {
            BrowserGpuSupport::Unavailable("webgpu disabled for non-trainer role".into())
        },
        recommended_role,
        ..BrowserCapabilityReport::default()
    }
}

fn browser_role_label(role: &BrowserRuntimeRole) -> &'static str {
    match role {
        BrowserRuntimeRole::Viewer | BrowserRuntimeRole::BrowserObserver => "browser-viewer",
        BrowserRuntimeRole::BrowserVerifier => "browser-verifier",
        BrowserRuntimeRole::BrowserTrainerWgpu => "browser-trainer-wgpu",
        BrowserRuntimeRole::BrowserFallback => "browser-fallback",
    }
}

fn browser_runtime_state_label(state: Option<&BrowserRuntimeState>) -> &'static str {
    match state {
        Some(BrowserRuntimeState::ViewerOnly) => "portal-only",
        Some(BrowserRuntimeState::Joining { .. }) => "joining",
        Some(BrowserRuntimeState::Observer) => "observer",
        Some(BrowserRuntimeState::Verifier) => "verifier",
        Some(BrowserRuntimeState::Trainer) => "trainer",
        Some(BrowserRuntimeState::BackgroundSuspended { .. }) => "background-suspended",
        Some(BrowserRuntimeState::Catchup { .. }) => "catchup",
        Some(BrowserRuntimeState::Blocked { .. }) => "blocked",
        None => "stopped",
    }
}

fn browser_transport_label(kind: &BrowserTransportKind) -> String {
    match kind {
        BrowserTransportKind::WebRtcDirect => "webrtc-direct".into(),
        BrowserTransportKind::WebTransport => "webtransport".into(),
        BrowserTransportKind::WssFallback => "wss-fallback".into(),
    }
}
