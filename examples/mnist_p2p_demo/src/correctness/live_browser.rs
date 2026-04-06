use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use burn_p2p::{
    AuthProvider, BrowserEdgeSnapshot, BrowserLoginProvider, BrowserMode, ContentId,
    ContributionReceipt, ExperimentDirectoryEntry, ExperimentId, ExperimentScope, HeadDescriptor,
    HeadId, IdentityConnector, LeaseId, MicroShardId, NetworkManifest,
    NodeCertificateAuthority, NodeEnrollmentRequest, PeerId, PeerRole, PeerRoleSet,
    PrincipalClaims, PrincipalId, PrincipalSession, ProjectFamilyId, RevisionId,
    RevocationEpoch, StaticIdentityConnector, StaticPrincipalRecord, WorkloadId,
};
use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths, BrowserLeaderboardEntry,
    BrowserLeaderboardSnapshot, BrowserReceiptSubmissionResponse, BrowserTransportSurface,
    SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload, SocialMode,
    TrustBundleExport, TrustedIssuerStatus,
};
use burn_p2p_metrics::MetricsCatchupBundle;
use chrono::{Duration as ChronoDuration, Utc};
use libp2p_identity::Keypair;
use semver::Version;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const LOGIN_PATH: &str = "/login/static";
const CALLBACK_PATH: &str = "/callback/static";
const ENROLL_PATH: &str = "/enroll";
const RECEIPTS_PATH: &str = "/receipts/browser";
const PORTAL_SNAPSHOT_PATH: &str = "/portal/snapshot";
const DIRECTORY_PATH: &str = "/directory";
const SIGNED_DIRECTORY_PATH: &str = "/directory/signed";
const HEADS_PATH: &str = "/heads";
const LEADERBOARD_PATH: &str = "/leaderboard";
const SIGNED_LEADERBOARD_PATH: &str = "/leaderboard/signed";
const TRUST_PATH: &str = "/trust";
const METRICS_CATCHUP_PATH: &str = "/metrics/catchup";
const METRICS_LIVE_LATEST_PATH: &str = "/metrics/live/latest";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveBrowserProbeManifest {
    pub edge_base_url: String,
    pub network_id: String,
    pub experiment_id: String,
    pub revision_id: String,
    pub selected_head_id: String,
    pub lease_id: String,
    pub leased_microshards: Vec<String>,
    pub release_train_hash: String,
    pub target_artifact_id: String,
    pub target_artifact_hash: String,
    pub workload_id: String,
    pub principal_id: String,
}

#[derive(Clone)]
pub struct LiveBrowserEdgeConfig {
    pub network_manifest: NetworkManifest,
    pub release_manifest: burn_p2p::ClientReleaseManifest,
    pub workload_id: WorkloadId,
    pub directory_entries: Vec<ExperimentDirectoryEntry>,
    pub heads: Vec<HeadDescriptor>,
    pub leaderboard_entries: Vec<BrowserLeaderboardEntry>,
    pub metrics_catchup: Vec<MetricsCatchupBundle>,
    pub selected_head_id: HeadId,
    pub selected_experiment_id: ExperimentId,
    pub selected_revision_id: RevisionId,
    pub active_lease_id: LeaseId,
    pub leased_microshards: Vec<MicroShardId>,
}

struct LiveBrowserEdgeState {
    connector: StaticIdentityConnector,
    authority: NodeCertificateAuthority,
    sessions: BTreeMap<ContentId, PrincipalSession>,
    snapshot: BrowserEdgeSnapshot,
    signed_directory: SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>,
    signed_leaderboard: SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>,
    metrics_catchup: Vec<MetricsCatchupBundle>,
    accepted_receipts: Vec<ContributionReceipt>,
}

pub struct LiveBrowserEdgeServer {
    base_url: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<anyhow::Result<()>>>,
}

impl LiveBrowserEdgeServer {
    pub fn spawn(config: LiveBrowserEdgeConfig) -> anyhow::Result<(Self, LiveBrowserProbeManifest)> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind live browser edge")?;
        listener
            .set_nonblocking(true)
            .context("set live browser edge listener nonblocking")?;
        let addr = listener.local_addr().context("read live browser edge address")?;
        let base_url = format!("http://{addr}");

        let authority = build_authority(&config)?;
        let snapshot = build_snapshot(&config, &authority);
        let signed_directory = signed_directory(snapshot.directory.clone(), authority.issuer_peer_id());
        let signed_leaderboard =
            signed_leaderboard(snapshot.leaderboard.clone(), authority.issuer_peer_id());
        let principal_id = PrincipalId::new("mnist-browser-trainer");
        let connector = build_connector(&config, &principal_id);
        let manifest = LiveBrowserProbeManifest {
            edge_base_url: base_url.clone(),
            network_id: config.network_manifest.network_id.as_str().into(),
            experiment_id: config.selected_experiment_id.as_str().into(),
            revision_id: config.selected_revision_id.as_str().into(),
            selected_head_id: config.selected_head_id.as_str().into(),
            lease_id: config.active_lease_id.as_str().into(),
            leased_microshards: config
                .leased_microshards
                .iter()
                .map(|microshard| microshard.as_str().to_owned())
                .collect(),
            release_train_hash: config.release_manifest.release_train_hash.as_str().into(),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: config.release_manifest.target_artifact_hash.as_str().into(),
            workload_id: config.workload_id.as_str().into(),
            principal_id: principal_id.as_str().into(),
        };

        let state = Arc::new(Mutex::new(LiveBrowserEdgeState {
            connector,
            authority,
            sessions: BTreeMap::new(),
            snapshot,
            signed_directory,
            signed_leaderboard,
            metrics_catchup: config.metrics_catchup,
            accepted_receipts: Vec::new(),
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let state_for_thread = Arc::clone(&state);
        let thread = thread::spawn(move || run_http_server(listener, stop_for_thread, state_for_thread));

        Ok((
            Self {
                base_url,
                stop,
                thread: Some(thread),
            },
            manifest,
        ))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for LiveBrowserEdgeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn wait_for_live_browser_probe(
    output_root: &Path,
    timeout: Duration,
) -> anyhow::Result<serde_json::Value> {
    let result_path = output_root.join("browser-probe-result.json");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if result_path.exists() {
            let bytes = fs::read(&result_path)
                .with_context(|| format!("failed to read {}", result_path.display()))?;
            return serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to decode {}", result_path.display()));
        }
        thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!(
        "timed out waiting for live browser probe result at {}",
        result_path.display()
    );
}

pub fn write_live_browser_manifest(
    output_root: &Path,
    manifest: &LiveBrowserProbeManifest,
) -> anyhow::Result<PathBuf> {
    let path = output_root.join("browser-live.json");
    fs::write(&path, serde_json::to_vec_pretty(manifest)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

fn build_authority(config: &LiveBrowserEdgeConfig) -> anyhow::Result<NodeCertificateAuthority> {
    NodeCertificateAuthority::new(
        config.network_manifest.network_id.clone(),
        config.release_manifest.project_family_id.clone(),
        config.release_manifest.release_train_hash.clone(),
        Version::new(0, 1, 0),
        Keypair::generate_ed25519(),
        "mnist-browser-edge",
    )
    .context("build live browser certificate authority")
}

fn build_connector(
    config: &LiveBrowserEdgeConfig,
    principal_id: &PrincipalId,
) -> StaticIdentityConnector {
    let claims = PrincipalClaims {
        principal_id: principal_id.clone(),
        provider: AuthProvider::Static {
            authority: "mnist-browser-edge".into(),
        },
        display_name: "mnist browser trainer".into(),
        org_memberships: BTreeSet::new(),
        group_memberships: BTreeSet::new(),
        granted_roles: PeerRoleSet::new([
            PeerRole::BrowserTrainerWgpu,
            PeerRole::BrowserObserver,
        ]),
        granted_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: config.selected_experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: config.selected_experiment_id.clone(),
            },
        ]),
        custom_claims: BTreeMap::new(),
        issued_at: Utc::now(),
        expires_at: Utc::now() + ChronoDuration::minutes(30),
    };

    StaticIdentityConnector::new(
        "mnist-browser-edge",
        ChronoDuration::minutes(30),
        BTreeMap::from([(
            principal_id.clone(),
            StaticPrincipalRecord {
                claims,
                allowed_networks: BTreeSet::from([config.network_manifest.network_id.clone()]),
            },
        )]),
    )
}

fn build_snapshot(
    config: &LiveBrowserEdgeConfig,
    authority: &NodeCertificateAuthority,
) -> BrowserEdgeSnapshot {
    BrowserEdgeSnapshot {
        network_id: config.network_manifest.network_id.clone(),
        edge_mode: BrowserEdgeMode::Full,
        browser_mode: BrowserMode::Trainer,
        social_mode: SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: false,
            webtransport_gateway: true,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths {
            login_path: LOGIN_PATH.into(),
            callback_path: CALLBACK_PATH.into(),
            enroll_path: ENROLL_PATH.into(),
            receipt_submit_path: RECEIPTS_PATH.into(),
            ..BrowserEdgePaths::default()
        },
        auth_enabled: true,
        login_providers: vec![BrowserLoginProvider {
            label: "static".into(),
            login_path: LOGIN_PATH.into(),
            callback_path: Some(CALLBACK_PATH.into()),
            device_path: None,
        }],
        required_release_train_hash: Some(config.release_manifest.release_train_hash.clone()),
        allowed_target_artifact_hashes: BTreeSet::from([config
            .release_manifest
            .target_artifact_hash
            .clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: config.network_manifest.network_id.clone(),
            generated_at: Utc::now(),
            entries: config.directory_entries.clone(),
        },
        heads: config.heads.clone(),
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: config.network_manifest.network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: config.leaderboard_entries.clone(),
            captured_at: Utc::now(),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id: config.network_manifest.network_id.clone(),
            project_family_id: config.release_manifest.project_family_id.clone(),
            required_release_train_hash: config.release_manifest.release_train_hash.clone(),
            allowed_target_artifact_hashes: BTreeSet::from([config
                .release_manifest
                .target_artifact_hash
                .clone()]),
            minimum_revocation_epoch: RevocationEpoch(0),
            active_issuer_peer_id: authority.issuer_peer_id(),
            issuers: vec![TrustedIssuerStatus {
                issuer_peer_id: authority.issuer_peer_id(),
                issuer_public_key_hex: authority.issuer_public_key_hex().into(),
                active_for_new_certificates: true,
                accepted_for_admission: true,
            }],
            reenrollment: None,
        }),
        captured_at: Utc::now(),
    }
}

fn signed_directory(
    snapshot: BrowserDirectorySnapshot,
    signer: PeerId,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer,
            key_id: "mnist-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "mnist-browser-edge".into(),
        },
    )
    .expect("mnist live browser signed directory should be serializable")
}

fn signed_leaderboard(
    snapshot: BrowserLeaderboardSnapshot,
    signer: PeerId,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer,
            key_id: "mnist-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "mnist-browser-edge".into(),
        },
    )
    .expect("mnist live browser signed leaderboard should be serializable")
}

fn run_http_server(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<LiveBrowserEdgeState>>,
) -> anyhow::Result<()> {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = handle_connection(stream, &state) {
                    eprintln!("mnist live browser edge request failed: {error:#}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error).context("accept live browser edge request"),
        }
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    state: &Arc<Mutex<LiveBrowserEdgeState>>,
) -> anyhow::Result<()> {
    let request = read_request(&stream)?;
    let path = request.path.split('?').next().unwrap_or(&request.path);
    match (request.method.as_str(), path) {
        ("OPTIONS", _) => write_preflight_response(&mut stream),
        ("GET", PORTAL_SNAPSHOT_PATH) => {
            let snapshot = state.lock().expect("live browser state").snapshot.clone();
            write_json_response(&mut stream, 200, &snapshot)
        }
        ("GET", DIRECTORY_PATH) => {
            let directory = state
                .lock()
                .expect("live browser state")
                .snapshot
                .directory
                .entries
                .clone();
            write_json_response(&mut stream, 200, &directory)
        }
        ("GET", SIGNED_DIRECTORY_PATH) => {
            let signed = state
                .lock()
                .expect("live browser state")
                .signed_directory
                .clone();
            write_json_response(&mut stream, 200, &signed)
        }
        ("GET", HEADS_PATH) => {
            let heads = state.lock().expect("live browser state").snapshot.heads.clone();
            write_json_response(&mut stream, 200, &heads)
        }
        ("GET", LEADERBOARD_PATH) => {
            let leaderboard = state
                .lock()
                .expect("live browser state")
                .snapshot
                .leaderboard
                .clone();
            write_json_response(&mut stream, 200, &leaderboard)
        }
        ("GET", SIGNED_LEADERBOARD_PATH) => {
            let signed = state
                .lock()
                .expect("live browser state")
                .signed_leaderboard
                .clone();
            write_json_response(&mut stream, 200, &signed)
        }
        ("GET", TRUST_PATH) => {
            let trust_bundle = state
                .lock()
                .expect("live browser state")
                .snapshot
                .trust_bundle
                .clone()
                .context("live browser edge snapshot missing trust bundle")?;
            write_json_response(&mut stream, 200, &trust_bundle)
        }
        ("GET", METRICS_LIVE_LATEST_PATH) => write_empty_response(&mut stream, 404),
        ("GET", path) if path == METRICS_CATCHUP_PATH || path.starts_with("/metrics/catchup/") => {
            let experiment_filter = path
                .strip_prefix("/metrics/catchup/")
                .map(str::to_owned)
                .filter(|value| !value.is_empty());
            let bundles = {
                let state = state.lock().expect("live browser state");
                state
                    .metrics_catchup
                    .iter()
                    .filter(|bundle| {
                        experiment_filter
                            .as_ref()
                            .map(|experiment_id| bundle.experiment_id.as_str() == experiment_id)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            write_json_response(&mut stream, 200, &bundles)
        }
        ("POST", LOGIN_PATH) => {
            let request_body: burn_p2p::LoginRequest = parse_json_body(&request.body)?;
            let login = state
                .lock()
                .expect("live browser state")
                .connector
                .begin_login(request_body)
                .context("begin live browser login")?;
            write_json_response(&mut stream, 200, &login)
        }
        ("POST", CALLBACK_PATH) => {
            let callback: burn_p2p::CallbackPayload = parse_json_body(&request.body)?;
            let mut state = state.lock().expect("live browser state");
            let session = state
                .connector
                .complete_login(callback)
                .context("complete live browser login")?;
            state
                .sessions
                .insert(session.session_id.clone(), session.clone());
            write_json_response(&mut stream, 200, &session)
        }
        ("POST", ENROLL_PATH) => {
            let enrollment: burn_p2p_browser::BrowserPeerEnrollmentRequest =
                parse_json_body(&request.body)?;
            let state = state.lock().expect("live browser state");
            let session = state
                .sessions
                .get(&enrollment.session_id)
                .cloned()
                .with_context(|| {
                    format!(
                        "live browser edge missing session {} for enrollment",
                        enrollment.session_id.as_str()
                    )
                })?;
            let certificate = state
                .authority
                .issue_certificate(NodeEnrollmentRequest {
                    session,
                    project_family_id: state.snapshot.trust_bundle.as_ref().map_or_else(
                        || ProjectFamilyId::new("unknown"),
                        |bundle| bundle.project_family_id.clone(),
                    ),
                    release_train_hash: enrollment.release_train_hash,
                    target_artifact_hash: enrollment.target_artifact_hash,
                    peer_id: enrollment.peer_id,
                    peer_public_key_hex: enrollment.peer_public_key_hex,
                    granted_roles: PeerRoleSet::new([
                        PeerRole::BrowserTrainerWgpu,
                        PeerRole::BrowserObserver,
                    ]),
                    requested_scopes: enrollment.requested_scopes,
                    client_policy_hash: enrollment.client_policy_hash,
                    serial: enrollment.serial,
                    not_before: Utc::now(),
                    not_after: Utc::now() + ChronoDuration::minutes(30),
                    revocation_epoch: RevocationEpoch(0),
                })
                .context("issue live browser certificate")?;
            write_json_response(&mut stream, 200, &certificate)
        }
        ("POST", RECEIPTS_PATH) => {
            let session_id = request
                .headers
                .get("x-session-id")
                .cloned()
                .context("browser receipt submission missing x-session-id header")?;
            let session_id = ContentId::new(session_id);
            let receipts: Vec<ContributionReceipt> = parse_json_body(&request.body)?;
            let mut state = state.lock().expect("live browser state");
            ensure!(
                state.sessions.contains_key(&session_id),
                "live browser edge rejected receipts for unknown session {}",
                session_id.as_str()
            );
            let accepted_ids = receipts
                .iter()
                .map(|receipt| receipt.receipt_id.clone())
                .collect::<Vec<_>>();
            state.accepted_receipts.extend(receipts);
            write_json_response(
                &mut stream,
                200,
                &BrowserReceiptSubmissionResponse {
                    accepted_receipt_ids: accepted_ids,
                    pending_receipt_count: 0,
                },
            )
        }
        _ => write_empty_response(&mut stream, 404),
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &TcpStream) -> anyhow::Result<HttpRequest> {
    let mut reader = BufReader::new(stream.try_clone().context("clone live browser stream")?);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .context("read live browser request line")?;
    ensure!(!request_line.trim().is_empty(), "empty live browser request");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .context("live browser request missing method")?
        .to_owned();
    let path = request_parts
        .next()
        .context("live browser request missing path")?
        .to_owned();

    let mut headers = BTreeMap::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .context("read live browser request header")?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim().to_owned();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse::<usize>().unwrap_or(0);
            }
            headers.insert(name.trim().to_ascii_lowercase(), value);
        }
    }

    let mut body = vec![0; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .context("read live browser request body")?;
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn parse_json_body<T: DeserializeOwned>(body: &[u8]) -> anyhow::Result<T> {
    serde_json::from_slice(body).context("decode live browser json body")
}

fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    value: &impl Serialize,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(value).context("encode live browser json response")?;
    write_response(stream, status, "application/json", &body)
}

fn write_empty_response(stream: &mut TcpStream, status: u16) -> anyhow::Result<()> {
    write_response(stream, status, "text/plain; charset=utf-8", &[])
}

fn write_preflight_response(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_response(stream, 200, "text/plain; charset=utf-8", &[])
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> anyhow::Result<()> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 {status} {status_text}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\naccess-control-allow-methods: GET, POST, OPTIONS\r\naccess-control-allow-headers: content-type, x-session-id\r\naccess-control-max-age: 86400\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .context("write live browser response head")?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .context("write live browser response body")?;
    }
    stream.flush().context("flush live browser response")
}
