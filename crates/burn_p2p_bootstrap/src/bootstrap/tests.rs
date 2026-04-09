#[cfg(feature = "artifact-s3")]
use super::BootstrapArtifactPublicationConfig;
use super::{
    AuthSessionStateStore, BootstrapAuthConfig, BootstrapAuthConnectorConfig,
    BootstrapAuthPrincipal, BootstrapAuthSessionBackendConfig, BootstrapDaemonConfig,
    BootstrapEmbeddedDaemonConfig, BootstrapOptionalServicesConfig, BootstrapPeerDaemonConfig,
    HttpRequest, HttpServerContext, auth_directory_entries, auth_session_state_store,
    build_auth_portal, current_revocation_epoch, current_trust_bundle, default_issuer_key_id,
    handle_connection, load_or_create_keypair, persist_daemon_config, retire_trusted_issuers,
    rollout_auth_policy, rotate_authority_material, validate_compiled_feature_support_with,
};
use crate::compiled_feature_set;
#[cfg(feature = "artifact-s3")]
use std::io::ErrorKind;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration as StdDuration,
};

use burn_p2p::{
    BrowserMode, CallbackPayload, CompiledFeatureSet, ContributionReceipt, EdgeFeature,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
    LoginRequest, MetricValue, NodeEnrollmentRequest, PeerId, PeerRole, PeerRoleSet, PrincipalId,
    ProfileMode, SocialMode, TrustedIssuer,
};
#[cfg(any(feature = "metrics-indexer", feature = "artifact-s3"))]
use burn_p2p::{DatasetViewId, WorkloadId};
#[cfg(any(feature = "metrics-indexer", feature = "artifact-s3"))]
use burn_p2p::{HeadEvalReport, HeadEvalStatus, MetricTrustClass};
#[cfg(feature = "metrics-indexer")]
use burn_p2p::{
    LeaseId, PeerWindowMetrics, PeerWindowStatus, ReducerCohortMetrics, ReducerCohortStatus,
};
#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
use burn_p2p::{OverlayChannel, OverlayTopic, ReducerLoadAnnouncement};
use burn_p2p_bootstrap::{
    AdminApiPlan, ArchivePlan, AuthPolicyRollout, BootstrapAdminState, BootstrapPreset,
    BootstrapSpec,
};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserEdgeClient, BrowserEdgeSnapshot, BrowserEnrollmentConfig,
    BrowserRuntimeConfig, BrowserRuntimeRole, BrowserRuntimeState, BrowserSessionState,
    BrowserTransportStatus, BrowserUiBindings, BrowserValidationPlan, BrowserWorkerCommand,
    BrowserWorkerEvent, BrowserWorkerIdentity, BrowserWorkerRuntime,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_core::ArtifactLiveEventKind;
#[cfg(feature = "auth-github")]
use burn_p2p_core::AuthProvider;
#[cfg(feature = "metrics-indexer")]
use burn_p2p_core::BackendClass;
use burn_p2p_core::{ClientPlatform, ContentId, NetworkId, RevocationEpoch};
#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
use burn_p2p_security::{PeerAdmissionReport, PeerTrustLevel};
use chrono::{Duration, Utc};
use libp2p_identity::Keypair;
use semver::Version;
use serde_json::Value;
use tempfile::tempdir;

struct IssueRequestSpec<'a> {
    method: &'a str,
    path: &'a str,
    body: Option<Value>,
    headers: &'a [(&'a str, &'a str)],
}

struct HttpTestServer {
    base_url: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct RedisTestServer {
    url: String,
    port: u16,
    child: Option<Child>,
    state: tempfile::TempDir,
}

#[cfg(feature = "artifact-s3")]
type ArtifactS3ObjectMap = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

impl HttpTestServer {
    fn spawn(context: HttpServerContext) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let thread = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_connection(stream, context.clone()).expect("handle connection");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(StdDuration::from_millis(10));
                    }
                    Err(error) => panic!("accept connection: {error}"),
                }
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            stop,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for HttpTestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.base_url.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RedisTestServer {
    fn spawn() -> Self {
        let state = tempdir().expect("redis temp dir");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind redis test port");
        let port = listener.local_addr().expect("redis local addr").port();
        drop(listener);

        let mut server = Self {
            url: format!("redis://127.0.0.1:{port}/0"),
            port,
            child: None,
            state,
        };
        server.start();
        server
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "redis test server already started");
        let child = Command::new("redis-server")
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--dir")
            .arg(self.state.path())
            .arg("--dbfilename")
            .arg("dump.rdb")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn redis-server");

        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        loop {
            if let Ok(client) = redis::Client::open(self.url.as_str())
                && let Ok(mut connection) = client.get_connection()
                && redis::cmd("PING")
                    .query::<String>(&mut connection)
                    .map(|pong| pong == "PONG")
                    .unwrap_or(false)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "redis test server did not become ready at {}",
                self.url
            );
            thread::sleep(StdDuration::from_millis(25));
        }
        self.child = Some(child);
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn restart(&mut self) {
        self.stop();
        self.start();
    }
}

impl Drop for RedisTestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
fn spawn_provider_json_server(
    assert_request: impl Fn(&str) + Send + 'static,
    response_status: &'static str,
    response_body: String,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider exchange listener");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept provider exchange");
        stream
            .set_read_timeout(Some(StdDuration::from_secs(2)))
            .expect("set provider exchange read timeout");
        let mut buffer = [0_u8; 8192];
        let bytes_read = stream
            .read(&mut buffer)
            .expect("read provider exchange request");
        let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
        assert_request(&request);
        let response = format!(
            "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write provider exchange response");
        stream.flush().expect("flush provider exchange response");
    });
    (format!("http://{addr}"), handle)
}

#[cfg(feature = "artifact-s3")]
fn spawn_artifact_s3_server() -> (String, ArtifactS3ObjectMap, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind s3 listener");
    listener
        .set_nonblocking(true)
        .expect("set s3 listener nonblocking");
    let addr = listener.local_addr().expect("local addr");
    let objects = Arc::new(Mutex::new(BTreeMap::<String, Vec<u8>>::new()));
    let shared_objects = Arc::clone(&objects);
    let handle = thread::spawn(move || {
        let mut handled_requests = 0usize;
        let mut idle_rounds = 0usize;
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(pair) => {
                    handled_requests += 1;
                    idle_rounds = 0;
                    pair
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if handled_requests > 0 && idle_rounds >= 20 {
                        break;
                    }
                    idle_rounds += 1;
                    thread::sleep(StdDuration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("accept s3 connection: {error}"),
            };
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 8192];
            loop {
                let bytes_read = stream.read(&mut chunk).expect("read s3 request");
                if bytes_read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..bytes_read]);
                if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let header_len = header_end + 4;
                    let header_text = String::from_utf8_lossy(&buffer[..header_len]).to_string();
                    let content_length = header_text
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length: ")
                                .or_else(|| line.strip_prefix("content-length: "))
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    while buffer.len() < header_len + content_length {
                        let more = stream.read(&mut chunk).expect("read s3 body");
                        if more == 0 {
                            break;
                        }
                        buffer.extend_from_slice(&chunk[..more]);
                    }
                    let mut lines = header_text.lines();
                    let request_line = lines.next().expect("request line");
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().expect("method");
                    let raw_path = parts.next().expect("path");
                    let path = raw_path.split('?').next().unwrap_or(raw_path);
                    let body = buffer[header_len..header_len + content_length].to_vec();
                    let status = match method {
                        "PUT" => {
                            shared_objects
                                .lock()
                                .expect("s3 object map should not be poisoned")
                                .insert(path.to_owned(), body);
                            "200 OK"
                        }
                        "GET" => {
                            let body = shared_objects
                                .lock()
                                .expect("s3 object map should not be poisoned")
                                .get(path)
                                .cloned()
                                .unwrap_or_default();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            );
                            stream
                                .write_all(response.as_bytes())
                                .expect("write s3 get response");
                            stream.write_all(&body).expect("write s3 get body");
                            stream.flush().expect("flush s3 get response");
                            continue;
                        }
                        "DELETE" => {
                            shared_objects
                                .lock()
                                .expect("s3 object map should not be poisoned")
                                .remove(path);
                            "204 No Content"
                        }
                        _ => "405 Method Not Allowed",
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write s3 response");
                    stream.flush().expect("flush s3 response");
                    break;
                }
            }
        }
    });
    (format!("http://{addr}"), objects, handle)
}

fn sample_auth_config(root: &std::path::Path) -> BootstrapAuthConfig {
    BootstrapAuthConfig {
        authority_name: "local-auth".into(),
        connector: BootstrapAuthConnectorConfig::Static,
        authority_key_path: root.join("authority.key"),
        session_state_path: None,
        session_state_backend: None,
        persist_provider_tokens: false,
        issuer_key_id: default_issuer_key_id(),
        project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
        required_release_train_hash: ContentId::new("demo-train"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("demo-artifact-native")]),
        session_ttl_seconds: 300,
        minimum_revocation_epoch: 1,
        principals: vec![BootstrapAuthPrincipal {
            principal_id: PrincipalId::new("alice"),
            display_name: "Alice".into(),
            org_memberships: BTreeSet::from(["burn-lab".into()]),
            group_memberships: BTreeSet::from(["trainers".into()]),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
            granted_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Discover,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
            allowed_networks: BTreeSet::from([NetworkId::new("secure-demo")]),
            custom_claims: BTreeMap::from([
                ("team".into(), "research".into()),
                ("operator_role".into(), "admin".into()),
            ]),
        }],
        directory_entries: vec![
            ExperimentDirectoryEntry {
                network_id: NetworkId::new("secure-demo"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                workload_id: burn_p2p::WorkloadId::new("auth-demo"),
                display_name: "Authenticated Demo".into(),
                model_schema_hash: burn_p2p::ContentId::new("model-auth"),
                dataset_view_id: burn_p2p::DatasetViewId::new("view-auth"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                    minimum_device_memory_bytes: Some(1024),
                    minimum_system_memory_bytes: Some(4096),
                    estimated_download_bytes: 65_536,
                    estimated_window_seconds: 45,
                },
                visibility: ExperimentVisibility::OptIn,
                opt_in_policy: ExperimentOptInPolicy::Scoped,
                current_revision_id: burn_p2p::RevisionId::new("rev-auth"),
                current_head_id: None,
                allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                allowed_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                }]),
                metadata: BTreeMap::from([("owner".into(), "burn-lab".into())]),
            },
            ExperimentDirectoryEntry {
                network_id: NetworkId::new("secure-demo"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-hidden"),
                workload_id: burn_p2p::WorkloadId::new("hidden-demo"),
                display_name: "Hidden".into(),
                model_schema_hash: burn_p2p::ContentId::new("model-hidden"),
                dataset_view_id: burn_p2p::DatasetViewId::new("view-hidden"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::Validator]),
                    minimum_device_memory_bytes: Some(2048),
                    minimum_system_memory_bytes: Some(8192),
                    estimated_download_bytes: 131_072,
                    estimated_window_seconds: 60,
                },
                visibility: ExperimentVisibility::InviteOnly,
                opt_in_policy: ExperimentOptInPolicy::Scoped,
                current_revision_id: burn_p2p::RevisionId::new("rev-hidden"),
                current_head_id: None,
                allowed_roles: PeerRoleSet::new([PeerRole::Validator]),
                allowed_scopes: BTreeSet::from([ExperimentScope::Validate {
                    experiment_id: burn_p2p::ExperimentId::new("exp-hidden"),
                }]),
                metadata: BTreeMap::new(),
            },
        ],
        trusted_issuers: Vec::new(),
        reenrollment: None,
    }
}

fn sample_auth_config_with_connector(
    root: &std::path::Path,
    connector: BootstrapAuthConnectorConfig,
) -> BootstrapAuthConfig {
    let mut config = sample_auth_config(root);
    config.connector = connector;
    config
}

#[test]
fn auth_session_state_store_prefers_explicit_redis_backend_config() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("file-auth-state.cbor"));
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: "redis://127.0.0.1:6379/0".into(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let store = auth_session_state_store(&config, &NetworkId::new("secure-demo"));
    match store {
        AuthSessionStateStore::Redis {
            url,
            state_key,
            lock_key,
        } => {
            assert_eq!(url, "redis://127.0.0.1:6379/0");
            assert_eq!(state_key, "burn-p2p:test-auth:secure-demo:local-auth:state");
            assert_eq!(lock_key, "burn-p2p:test-auth:secure-demo:local-auth:lock");
        }
        AuthSessionStateStore::File { .. } => {
            panic!("redis backend config should override file backend path")
        }
    }
}

fn sample_spec() -> BootstrapSpec {
    BootstrapSpec {
        preset: BootstrapPreset::BootstrapOnly,
        genesis: burn_p2p::GenesisSpec {
            network_id: NetworkId::new("secure-demo"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "Secure Demo".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        platform: ClientPlatform::Native,
        bootstrap_addresses: Vec::new(),
        listen_addresses: Vec::new(),
        authority: None,
        archive: ArchivePlan::default(),
        admin_api: AdminApiPlan::default(),
    }
}

fn issue_request(context: HttpServerContext, request: IssueRequestSpec<'_>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");
    let server_context = context.clone();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept connection");
        handle_connection(stream, server_context).expect("handle connection");
    });

    let mut stream = TcpStream::connect(addr).expect("connect client");
    let payload = request
        .body
        .map(|value| serde_json::to_vec(&value).expect("serialize body"))
        .unwrap_or_default();
    let mut wire = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\n",
        request.method, request.path
    );
    if let Some(admin_token) = context.admin_token.as_deref() {
        wire.push_str(&format!("x-admin-token: {admin_token}\r\n"));
    }
    for (name, value) in request.headers {
        wire.push_str(&format!("{name}: {value}\r\n"));
    }
    if !payload.is_empty() {
        wire.push_str("Content-Type: application/json\r\n");
        wire.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    wire.push_str("\r\n");
    stream
        .write_all(wire.as_bytes())
        .expect("write request headers");
    if !payload.is_empty() {
        stream.write_all(&payload).expect("write request body");
    }
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown write");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    server.join().expect("join server");
    response
}

fn response_json(response: &str) -> serde_json::Value {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .expect("split response headers");
    serde_json::from_str(body).expect("deserialize response body")
}

fn response_body(response: &str) -> &str {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .expect("split response headers");
    body
}

#[test]
fn browser_portal_client_round_trips_against_live_http_router() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("browser-client-e2e.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "native-test-client".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);

        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin login");
        let session = client
            .complete_static_login(&login, PrincipalId::new("alice"))
            .await
            .expect("complete static login");
        let trust_bundle = client.fetch_trust_bundle().await.expect("trust bundle");
        assert_eq!(trust_bundle.network_id.as_str(), "secure-demo");

        let enrolled = client
            .enroll_static_principal(
                Some("alice".into()),
                PrincipalId::new("alice"),
                &BrowserWorkerIdentity {
                    peer_id: PeerId::new("browser-http-peer"),
                    peer_public_key_hex: "001122".into(),
                    serial: 7,
                    client_policy_hash: Some(ContentId::new("policy-browser-http")),
                },
            )
            .await
            .expect("enroll static principal");
        assert_eq!(
            enrolled.certificate.claims().peer_id.as_str(),
            "browser-http-peer"
        );

        let directory = client
            .fetch_directory(Some(&session.session_id))
            .await
            .expect("fetch directory");
        assert_eq!(directory.len(), 1);
        assert_eq!(directory[0].experiment_id.as_str(), "exp-auth");

        let signed_directory = client
            .fetch_signed_directory(Some(&session.session_id))
            .await
            .expect("fetch signed directory");
        assert_eq!(
            signed_directory.payload.schema,
            "burn_p2p.browser_directory_snapshot"
        );

        let signed_leaderboard = client
            .fetch_signed_leaderboard()
            .await
            .expect("fetch signed leaderboard");
        assert_eq!(
            signed_leaderboard.payload.schema,
            "burn_p2p.browser_leaderboard_snapshot"
        );

        let receipt = ContributionReceipt {
            receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser-http"),
            peer_id: PeerId::new("browser-http-peer"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            base_head_id: burn_p2p::HeadId::new("head-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-browser-http"),
            accepted_at: Utc::now(),
            accepted_weight: 2.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            merge_cert_id: None,
        };
        let submission = client
            .submit_receipts(&session.session_id, &[receipt])
            .await
            .expect("submit browser receipts");
        assert_eq!(submission.accepted_receipt_ids.len(), 1);
        assert_eq!(
            submission.accepted_receipt_ids[0].as_str(),
            "receipt-browser-http"
        );

        let refreshed = client
            .refresh_session(&session.session_id)
            .await
            .expect("refresh session");
        assert_eq!(refreshed.claims.principal_id.as_str(), "alice");

        let logout = client
            .logout_session(&refreshed.session_id)
            .await
            .expect("logout session");
        assert!(logout.logged_out);
    });
}

#[test]
fn browser_portal_client_syncs_worker_runtime_and_flushes_receipts_against_live_http_router() {
    let temp = tempdir().expect("temp dir");
    let mut auth_config = sample_auth_config(temp.path());
    auth_config.directory_entries[0].current_head_id = Some(burn_p2p::HeadId::new("head-auth"));
    let slot_requirements = auth_config.directory_entries[0]
        .resource_requirements
        .clone();
    auth_config.directory_entries[0].apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        revision_id: burn_p2p::RevisionId::new("rev-auth"),
        workload_id: burn_p2p::WorkloadId::new("auth-demo"),
        required_release_train_hash: ContentId::new("demo-train"),
        model_schema_hash: burn_p2p::ContentId::new("model-auth"),
        checkpoint_format_hash: ContentId::new("checkpoint-auth"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-auth"),
        training_config_hash: ContentId::new("training-auth"),
        merge_topology_policy_hash: ContentId::new("topology-auth"),
        slot_requirements,
        activation_window: burn_p2p::WindowActivation {
            activation_window: burn_p2p::WindowId(1),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: burn_p2p::BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(65_536),
        max_browser_window_secs: Some(45),
        max_browser_shard_bytes: Some(32_768),
        requires_webgpu: false,
        max_browser_batch_size: Some(8),
        recommended_browser_precision: None,
        visibility_policy: burn_p2p::BrowserVisibilityPolicy::SwarmEligible,
        description: "authenticated browser demo".into(),
    });
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState {
            head_descriptors: vec![HeadDescriptor {
                head_id: burn_p2p::HeadId::new("head-auth"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                revision_id: burn_p2p::RevisionId::new("rev-auth"),
                artifact_id: burn_p2p::ArtifactId::new("artifact-head-auth"),
                parent_head_id: None,
                global_step: 7,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            }],
            ..BootstrapAdminState::default()
        })),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("browser-runtime-sync.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let enrolled = client
            .enroll_static_principal(
                Some("alice".into()),
                PrincipalId::new("alice"),
                &BrowserWorkerIdentity {
                    peer_id: PeerId::new("browser-http-peer"),
                    peer_public_key_hex: "001122".into(),
                    serial: 7,
                    client_policy_hash: Some(ContentId::new("policy-browser-http")),
                },
            )
            .await
            .expect("enroll static principal");
        let mut session_state = BrowserSessionState::default();
        session_state.apply_enrollment(&enrolled);

        let mut worker = BrowserWorkerRuntime::start(
            BrowserRuntimeConfig {
                role: BrowserRuntimeRole::BrowserObserver,
                ..BrowserRuntimeConfig::new(
                    server.base_url(),
                    NetworkId::new("secure-demo"),
                    ContentId::new("demo-train"),
                    "browser-wasm",
                    ContentId::new("demo-artifact-native"),
                )
            },
            BrowserCapabilityReport::default(),
            BrowserTransportStatus {
                active: None,
                webrtc_direct_enabled: false,
                webtransport_enabled: true,
                wss_fallback_enabled: true,
                last_error: None,
            },
        );
        worker.remember_session(session_state.clone());

        let sync_events = client
            .sync_worker_runtime(&mut worker, Some(&session_state), true)
            .await
            .expect("sync worker runtime");
        assert_eq!(worker.state, Some(BrowserRuntimeState::Observer));
        assert_eq!(
            worker.storage.last_head_id,
            Some(burn_p2p::HeadId::new("head-auth"))
        );
        assert!(sync_events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::HeadUpdated { head_id }
                if head_id == &burn_p2p::HeadId::new("head-auth")
        )));

        let verify_events = worker.apply_command(
            BrowserWorkerCommand::Verify(BrowserValidationPlan {
                head_id: burn_p2p::HeadId::new("head-auth"),
                max_checkpoint_bytes: 65_536,
                sample_budget: 3,
                emit_receipt: true,
            }),
            None,
            None,
        );
        assert!(
            verify_events
                .iter()
                .any(|event| matches!(event, BrowserWorkerEvent::ValidationCompleted(_)))
        );

        let flush_events = client
            .flush_worker_receipts(&mut worker)
            .await
            .expect("flush worker receipts");
        assert!(flush_events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::ReceiptsAcknowledged {
                pending_receipts,
                ..
            } if *pending_receipts == 0
        )));

        let signed_leaderboard = client
            .fetch_signed_leaderboard()
            .await
            .expect("fetch signed leaderboard after receipt");
        assert_eq!(
            signed_leaderboard.payload.schema,
            "burn_p2p.browser_leaderboard_snapshot"
        );
    });
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_completes_github_login_via_exchange_callback() {
    let (exchange_url, exchange_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-123\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice Browser",
            "org_memberships": ["oss"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            }
        })
        .to_string(),
    );
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: Some(exchange_url),
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: Some(userinfo_url),
                    refresh_url: None,
                    revoke_url: None,
                    jwks_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("github-browser-client-exchange.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        assert_eq!(snapshot.login_providers[0].login_path, "/login/github");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github login");
        let session = client
            .complete_provider_login(&login, "github-code-123")
            .await
            .expect("complete github login");
        assert_eq!(session.claims.principal_id.as_str(), "alice");
        assert_eq!(session.claims.provider, AuthProvider::GitHub);
        assert_eq!(session.claims.display_name, "Alice Browser");
        assert!(session.claims.org_memberships.contains("oss"));
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );
    });
    exchange_server
        .join()
        .expect("join provider exchange server");
    userinfo_server
        .join()
        .expect("join provider userinfo server");
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_completes_github_login_via_upstream_token_exchange() {
    let (token_url, token_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("grant_type=authorization_code"));
            assert!(request.contains("code=github-upstream-code"));
            assert!(request.contains("client_id=github-client"));
            assert!(request.contains("client_secret=github-secret"));
        },
        "200 OK",
        serde_json::json!({
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "expires_in": 3600
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_json_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer access-token-1"));
        },
        "200 OK",
        serde_json::json!({
            "id": 42,
            "login": "alice-gh",
            "email": "alice@example.com",
            "name": "Alice Upstream Browser",
            "organizations": ["oss"],
            "groups": ["maintainers"],
            "avatar_url": "https://avatars.example/alice-upstream.png"
        })
        .to_string(),
    );
    let temp = tempdir().expect("temp dir");
    let mut auth_config = sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::GitHub {
            authorize_base_url: Some("https://github.com/login/oauth/authorize".into()),
            exchange_url: None,
            token_url: Some(token_url),
            client_id: Some("github-client".into()),
            client_secret: Some("github-secret".into()),
            redirect_uri: None,
            userinfo_url: Some(userinfo_url),
            refresh_url: None,
            revoke_url: None,
            jwks_url: None,
        },
    );
    auth_config.principals[0]
        .custom_claims
        .insert("provider_login".into(), "alice-gh".into());
    auth_config.principals[0]
        .custom_claims
        .insert("provider_email".into(), "alice@example.com".into());

    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github upstream auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("github-browser-client-upstream.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github upstream login");
        let session = client
            .complete_provider_login(&login, "github-upstream-code")
            .await
            .expect("complete github upstream login");
        assert_eq!(session.claims.principal_id.as_str(), "alice");
        assert_eq!(session.claims.provider, AuthProvider::GitHub);
        assert_eq!(session.claims.display_name, "Alice Upstream Browser");
        assert!(session.claims.org_memberships.contains("oss"));
        assert!(session.claims.group_memberships.contains("maintainers"));
        assert_eq!(
            session.claims.custom_claims.get("provider_login"),
            Some(&"alice-gh".to_owned())
        );
        assert_eq!(
            session.claims.custom_claims.get("provider_email"),
            Some(&"alice@example.com".to_owned())
        );
    });

    token_server.join().expect("join provider token server");
    userinfo_server
        .join()
        .expect("join provider userinfo server");
}

#[cfg(all(feature = "browser-edge", feature = "auth-github"))]
#[test]
fn browser_portal_client_refreshes_and_logs_out_provider_session_via_live_http_router() {
    let (exchange_url, exchange_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-refresh\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "display_name": "Alice GitHub",
            "org_memberships": ["oss"],
            "group_memberships": ["maintainers"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            },
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (refresh_url, refresh_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice Refreshed",
            "group_memberships": ["operators"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice-2.png"
            },
            "access_token": "access-token-2",
            "refresh_token": "refresh-token-2",
            "session_handle": "session-handle-2"
        })
        .to_string(),
    );
    let (revoke_url, revoke_server) = spawn_provider_json_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-2\""));
            assert!(request.contains("\"session_handle\":\"session-handle-2\""));
        },
        "200 OK",
        "{}".into(),
    );
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: Some(exchange_url),
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: None,
                    refresh_url: Some(refresh_url),
                    revoke_url: Some(revoke_url),
                    jwks_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("github-browser-client-refresh.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
            .get(format!("{}/portal/snapshot", server.base_url()))
            .send()
            .await
            .expect("fetch portal snapshot")
            .error_for_status()
            .expect("portal snapshot status")
            .json()
            .await
            .expect("decode portal snapshot");
        let bindings = BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot);
        let enrollment = BrowserEnrollmentConfig {
            network_id: NetworkId::new("secure-demo"),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            release_train_hash: ContentId::new("demo-train"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            login_path: "/login/github".into(),
            callback_path: "/callback/github".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            session_ttl_secs: 300,
        };
        let client = BrowserEdgeClient::new(bindings, enrollment);
        let login = client
            .begin_login(Some("alice".into()))
            .await
            .expect("begin github login");
        let session = client
            .complete_provider_login(&login, "github-code-refresh")
            .await
            .expect("complete github login");
        assert_eq!(session.claims.display_name, "Alice GitHub");
        assert_eq!(
            session.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice.png".to_owned())
        );

        let refreshed = client
            .refresh_session(&session.session_id)
            .await
            .expect("refresh provider session");
        assert_eq!(refreshed.claims.display_name, "Alice Refreshed");
        assert!(refreshed.claims.group_memberships.contains("operators"));
        assert_eq!(
            refreshed.claims.custom_claims.get("avatar_url"),
            Some(&"https://avatars.example/alice-2.png".to_owned())
        );

        let logout = client
            .logout_session(&refreshed.session_id)
            .await
            .expect("logout provider session");
        assert!(logout.logged_out);
    });

    exchange_server.join().expect("join exchange server");
    refresh_server.join().expect("join refresh server");
    revoke_server.join().expect("join revoke server");
}

#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "rbac",
    feature = "auth-static"
))]
#[test]
fn capabilities_endpoint_reports_compiled_and_active_services() {
    let temp = tempdir().expect("temp dir");
    let auth_config = sample_auth_config(temp.path());
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: Some(auth_config),
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("capabilities.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };

    let manifest = response_json(&issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/capabilities",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        manifest["payload"]["schema"],
        "burn_p2p.edge_service_manifest"
    );
    let payload = &manifest["payload"]["payload"];
    assert_eq!(payload["app_mode"], "Interactive");
    assert_eq!(payload["browser_mode"], "Trainer");
    assert_eq!(payload["social_mode"], "Public");
    assert_eq!(payload["admin_mode"], "Rbac");
    assert_eq!(payload["metrics_mode"], "OpenMetrics");
    assert_eq!(
        payload["available_auth_providers"],
        serde_json::json!(["Static"])
    );
    let active = payload["active_feature_set"]["features"]
        .as_array()
        .expect("active features array");
    assert!(active.contains(&serde_json::json!("App")));
    assert!(active.contains(&serde_json::json!("BrowserEdge")));
    assert!(active.contains(&serde_json::json!("Social")));
    assert!(active.contains(&serde_json::json!("AuthStatic")));
}

#[test]
fn disabled_optional_services_hide_routes_and_capabilities() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig {
                browser_edge_enabled: false,
                browser_mode: BrowserMode::Disabled,
                social_mode: SocialMode::Disabled,
                profile_mode: ProfileMode::Disabled,
            },
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("capabilities-disabled.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let manifest = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/.well-known/burn-p2p-capabilities",
            body: None,
            headers: &[],
        },
    ));
    let payload = &manifest["payload"]["payload"];
    assert_eq!(payload["app_mode"], "Disabled");
    assert_eq!(payload["browser_mode"], "Disabled");
    assert_eq!(payload["social_mode"], "Disabled");
    let active = payload["active_feature_set"]["features"]
        .as_array()
        .expect("active features array");
    assert!(!active.contains(&serde_json::json!("App")));
    assert!(!active.contains(&serde_json::json!("BrowserEdge")));
    assert!(!active.contains(&serde_json::json!("Social")));

    let portal = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal.starts_with("HTTP/1.1 404 Not Found"));

    let leaderboard = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard",
            body: None,
            headers: &[],
        },
    );
    assert!(leaderboard.starts_with("HTTP/1.1 404 Not Found"));

    let browser_receipts = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/receipts/browser",
            body: Some(serde_json::json!([])),
            headers: &[],
        },
    );
    assert!(browser_receipts.starts_with("HTTP/1.1 404 Not Found"));
}

#[cfg(all(feature = "browser-edge", feature = "browser-join", feature = "social"))]
#[test]
fn portal_hides_disabled_browser_auth_and_social_flows() {
    let temp = tempdir().expect("temp dir");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig {
                browser_edge_enabled: true,
                browser_mode: BrowserMode::Disabled,
                social_mode: SocialMode::Disabled,
                profile_mode: ProfileMode::Disabled,
            },
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("portal-hidden-flows.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(portal_snapshot["browser_mode"], "Disabled");
    assert_eq!(portal_snapshot["social_mode"], "Disabled");
    assert_eq!(portal_snapshot["profile_mode"], "Disabled");
    assert_eq!(portal_snapshot["auth_enabled"], false);
    assert_eq!(portal_snapshot["transports"]["wss_fallback"], false);

    let portal = issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal.starts_with("HTTP/1.1 404 Not Found"));
}

#[test]
fn startup_validation_rejects_uncompiled_optional_services() {
    let temp = tempdir().expect("temp dir");
    let config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig {
            browser_edge_enabled: true,
            browser_mode: BrowserMode::Disabled,
            social_mode: SocialMode::Public,
            profile_mode: ProfileMode::Disabled,
        },
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config(temp.path())),
        artifact_publication: None,
    };
    let compiled = CompiledFeatureSet {
        features: BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]),
    };
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("browser edge and social should require compiled features");
    assert!(error.to_string().contains("browser edge"));
}

#[test]
fn startup_validation_rejects_mixed_bootstrap_peer_and_embedded_runtime() {
    let config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: Some(BootstrapPeerDaemonConfig::default()),
        embedded_runtime: Some(BootstrapEmbeddedDaemonConfig::default()),
        auth: None,
        artifact_publication: None,
    };
    let error = validate_compiled_feature_support_with(&compiled_feature_set(), &config)
        .expect_err("mixed bootstrap peer and embedded runtime should be rejected");
    assert!(error.to_string().contains("mutually exclusive"));
}

#[test]
fn startup_validation_rejects_untrusted_external_auth_config() {
    let temp = tempdir().expect("temp dir");
    let mut config = BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config_with_connector(
            temp.path(),
            BootstrapAuthConnectorConfig::External {
                authority: "corp-sso".into(),
                trusted_principal_header: String::new(),
                trusted_internal_only: false,
            },
        )),
        artifact_publication: None,
    };
    let compiled = CompiledFeatureSet {
        features: BTreeSet::from([
            EdgeFeature::AdminHttp,
            EdgeFeature::Metrics,
            EdgeFeature::App,
            EdgeFeature::BrowserEdge,
            EdgeFeature::Rbac,
            EdgeFeature::AuthExternal,
            EdgeFeature::Social,
            EdgeFeature::Profiles,
        ]),
    };
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("external auth should require trusted internal mode");
    assert!(error.to_string().contains("trusted_internal_only"));

    config.auth = Some(sample_auth_config_with_connector(
        temp.path(),
        BootstrapAuthConnectorConfig::External {
            authority: "corp-sso".into(),
            trusted_principal_header: String::new(),
            trusted_internal_only: true,
        },
    ));
    let error = validate_compiled_feature_support_with(&compiled, &config)
        .expect_err("external auth should require a trusted principal header");
    assert!(error.to_string().contains("trusted_principal_header"));
}

#[test]
fn auth_portal_issues_certificates_and_filters_directory_by_session_scope() {
    let temp = tempdir().expect("temp dir");
    let auth = build_auth_portal(
        &sample_auth_config(temp.path()),
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build auth portal");

    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let node_keypair = Keypair::generate_ed25519();
    let peer_id = burn_p2p::PeerId::new(node_keypair.public().to_peer_id().to_string());
    let certificate = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issue_certificate(NodeEnrollmentRequest {
            session: session.clone(),
            project_family_id: auth.project_family_id.clone(),
            release_train_hash: auth.required_release_train_hash.clone(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: session.claims.granted_roles.clone(),
            requested_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            client_policy_hash: Some(ContentId::new("policy-auth")),
            serial: 1,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(5),
            revocation_epoch: RevocationEpoch(1),
        })
        .expect("issue certificate");
    assert_eq!(certificate.claims().peer_id, peer_id);
    assert_eq!(certificate.claims().principal_id, PrincipalId::new("alice"));

    let request = HttpRequest {
        method: "GET".into(),
        path: "/directory".into(),
        headers: BTreeMap::from([(
            "x-session-id".into(),
            session.session_id.as_str().to_owned(),
        )]),
        body: Vec::new(),
    };
    let entries = auth_directory_entries(&auth, &request).expect("directory entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].experiment_id,
        burn_p2p::ExperimentId::new("exp-auth")
    );
}

#[test]
fn load_or_create_keypair_is_persistent() {
    let temp = tempdir().expect("temp dir");
    let key_path = temp.path().join("authority.key");

    let first = load_or_create_keypair(&key_path).expect("first keypair");
    let second = load_or_create_keypair(&key_path).expect("second keypair");

    assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
}

#[test]
fn authenticated_bootstrap_example_deserializes() {
    let config: BootstrapDaemonConfig =
        serde_json::from_str(include_str!("../../examples/authenticated-bootstrap.json"))
            .expect("deserialize auth example");
    assert!(config.auth.is_some());
    assert_eq!(
        config.spec.genesis.network_id,
        NetworkId::new("secure-demo")
    );
}

#[test]
fn deployment_profile_examples_deserialize() {
    for (contents, network_id) in [
        (
            include_str!("../../examples/trusted-minimal.json"),
            "trusted-minimal-demo",
        ),
        (
            include_str!("../../examples/enterprise-sso.json"),
            "enterprise-sso-demo",
        ),
        (
            include_str!("../../examples/trusted-browser.json"),
            "trusted-browser-demo",
        ),
        (
            include_str!("../../examples/community-web.json"),
            "community-web-demo",
        ),
    ] {
        let config: BootstrapDaemonConfig =
            serde_json::from_str(contents).expect("deserialize profile example");
        assert_eq!(config.spec.genesis.network_id, NetworkId::new(network_id));
    }
}

#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
#[test]
fn http_routes_serve_status_and_static_auth_flow() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: None,
        allow_dev_admin_token: false,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: Some(120),
        admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(sample_auth_config(temp.path())),
        artifact_publication: None,
    }));
    let config_path = Arc::new(temp.path().join("bootstrap-config.json"));
    std::fs::write(
        &*config_path,
        serde_json::to_vec_pretty(
            &*daemon_config
                .lock()
                .expect("daemon config should not be poisoned"),
        )
        .expect("serialize daemon config"),
    )
    .expect("write daemon config");
    let plan = Arc::new(sample_spec().plan().expect("bootstrap plan"));
    let principal_id = PrincipalId::new("alice");
    let receipt_accepted_at = Utc::now();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            head_id: burn_p2p::HeadId::new("head-auth"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        reducer_load_announcements: vec![ReducerLoadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("secure-demo"),
                burn_p2p::StudyId::new("study-auth"),
                burn_p2p::ExperimentId::new("exp-auth"),
                OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p::ReducerLoadReport {
                peer_id: burn_p2p::PeerId::new("peer-auth"),
                window_id: burn_p2p::WindowId(1),
                assigned_leaf_updates: 1,
                assigned_aggregate_inputs: 0,
                ingress_bytes: 64,
                egress_bytes: 32,
                duplicate_transfer_ratio: 0.0,
                overload_ratio: 0.0,
                reported_at: Utc::now(),
            },
        }],
        contribution_receipts: vec![ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-auth"),
            peer_id: burn_p2p::PeerId::new("peer-auth"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            base_head_id: burn_p2p::HeadId::new("head-auth"),
            artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
            accepted_at: receipt_accepted_at,
            accepted_weight: 4.0,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            merge_cert_id: None,
        }],
        peer_admission_reports: BTreeMap::from([(
            burn_p2p::PeerId::new("peer-auth"),
            PeerAdmissionReport {
                peer_id: burn_p2p::PeerId::new("peer-auth"),
                principal_id: principal_id.clone(),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
                trust_level: PeerTrustLevel::ScopeAuthorized,
                issuer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
                findings: Vec::new(),
                verified_at: Utc::now(),
            },
        )]),
        ..BootstrapAdminState::default()
    }));
    let context = HttpServerContext {
        plan: plan.clone(),
        state: state.clone(),
        config: daemon_config,
        config_path,
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: Some(120),
        admin_signer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };

    let status = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/status",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(status["network_id"], "secure-demo");
    let bundle = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/diagnostics/bundle",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(bundle["plan"]["genesis"]["network_id"], "secure-demo");
    assert_eq!(bundle["diagnostics"]["network_id"], "secure-demo");
    assert_eq!(bundle["heads"][0]["head_id"], "head-auth");
    assert_eq!(
        bundle["reducer_load_announcements"][0]["report"]["peer_id"],
        "peer-auth"
    );
    let heads = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/heads",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(heads.as_array().expect("heads array").len(), 1);
    assert_eq!(heads[0]["head_id"], "head-auth");
    let reducer_load = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/reducers/load",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        reducer_load.as_array().expect("reducer load array").len(),
        1
    );
    assert_eq!(reducer_load[0]["report"]["peer_id"], "peer-auth");
    let trust = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/trust",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(trust["network_id"], "secure-demo");
    assert_eq!(trust["issuers"].as_array().expect("issuer array").len(), 1);
    assert_eq!(trust["reenrollment"], Value::Null);
    let reenrollment = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/reenrollment",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(reenrollment, Value::Null);

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect", {"Train": {"experiment_id": "exp-auth"}}],
            })),
            headers: &[],
        },
    ));
    let session = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_header = session["session_id"].as_str().expect("session id string");
    let directory_headers = [("x-session-id", session_header)];
    let directory = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/directory",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(directory.as_array().expect("directory array").len(), 1);
    assert_eq!(directory[0]["experiment_id"], "exp-auth");

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(portal_snapshot["network_id"], "secure-demo");
    assert_eq!(portal_snapshot["auth_enabled"], true);
    assert_eq!(
        portal_snapshot["leaderboard"]["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(
        portal_snapshot["paths"]["app_snapshot_path"],
        "/portal/snapshot"
    );
    assert_eq!(
        portal_snapshot["paths"]["receipt_submit_path"],
        "/receipts/browser"
    );
    assert_eq!(
        portal_snapshot["directory"]["entries"][0]["experiment_id"],
        "exp-auth"
    );

    let portal_response = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &directory_headers,
        },
    );
    assert!(portal_response.starts_with("HTTP/1.1 404 Not Found"));

    let leaderboard = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(leaderboard["score_version"], "leaderboard_score_v1");
    assert_eq!(
        leaderboard["entries"]
            .as_array()
            .expect("entries array")
            .len(),
        1
    );
    assert_eq!(
        leaderboard["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(leaderboard["entries"][0]["accepted_receipt_count"], 1);

    let receipt_submission = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/receipts/browser",
            body: Some(serde_json::json!([{
                "receipt_id": "receipt-browser-submitted",
                "peer_id": "peer-auth",
                "study_id": "study-auth",
                "experiment_id": "exp-auth",
                "revision_id": "rev-auth",
                "base_head_id": "head-auth",
                "artifact_id": "artifact-browser",
                "accepted_at": Utc::now(),
                "accepted_weight": 2.0,
                "metrics": {"loss": 0.2},
                "merge_cert_id": null
            }])),
            headers: &directory_headers,
        },
    ));
    assert_eq!(
        receipt_submission["accepted_receipt_ids"][0],
        "receipt-browser-submitted"
    );
    assert_eq!(receipt_submission["pending_receipt_count"], 0);

    let signed_directory = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/directory/signed",
            body: None,
            headers: &directory_headers,
        },
    ));
    assert_eq!(
        signed_directory["payload"]["schema"],
        "burn_p2p.browser_directory_snapshot"
    );
    assert_eq!(
        signed_directory["payload"]["payload"]["entries"][0]["experiment_id"],
        "exp-auth"
    );
    assert_eq!(
        signed_directory["signature"]["signer"],
        "bootstrap-authority"
    );

    let signed_leaderboard = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/leaderboard/signed",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        signed_leaderboard["payload"]["schema"],
        "burn_p2p.browser_leaderboard_snapshot"
    );
    assert_eq!(
        signed_leaderboard["payload"]["payload"]["entries"][0]["identity"]["principal_id"],
        "alice"
    );
    assert_eq!(
        signed_leaderboard["signature"]["signer"],
        "bootstrap-authority"
    );

    let receipts = response_json(&issue_request(
        context,
        IssueRequestSpec {
            method: "GET",
            path: "/receipts",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(receipts.as_array().expect("receipts array").len(), 2);
}

#[cfg(all(
    feature = "browser-edge",
    feature = "auth-github",
    feature = "auth-oidc"
))]
#[test]
fn github_and_oidc_routes_issue_provider_specific_sessions() {
    let temp = tempdir().expect("temp dir");
    let github_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::GitHub {
                    authorize_base_url: Some("https://github.example/login/oauth/authorize".into()),
                    exchange_url: None,
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: None,
                    refresh_url: None,
                    revoke_url: None,
                    jwks_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build github auth portal"),
    );
    let github_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("github-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(github_auth),
        control_handle: None,
    };
    let github_login = response_json(&issue_request(
        github_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/github",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(github_login["provider"], "GitHub");
    assert!(
        github_login["authorize_url"]
            .as_str()
            .expect("authorize url")
            .starts_with("https://github.example/login/oauth/authorize")
    );
    let github_session = response_json(&issue_request(
        github_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/github",
            body: Some(serde_json::json!({
                "login_id": github_login["login_id"],
                "state": github_login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    assert_eq!(github_session["claims"]["provider"], "GitHub");
    let github_snapshot = response_json(&issue_request(
        github_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        github_snapshot["login_providers"][0]["login_path"],
        "/login/github"
    );
    assert_eq!(
        github_snapshot["login_providers"][0]["callback_path"],
        "/callback/github"
    );

    let temp = tempdir().expect("temp dir");
    let oidc_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::Oidc {
                    issuer: "https://issuer.example".into(),
                    authorize_base_url: Some("https://issuer.example/authorize".into()),
                    exchange_url: None,
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    redirect_uri: None,
                    userinfo_url: None,
                    refresh_url: None,
                    revoke_url: None,
                    jwks_url: None,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build oidc auth portal"),
    );
    let oidc_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("oidc-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(oidc_auth),
        control_handle: None,
    };
    let oidc_login = response_json(&issue_request(
        oidc_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/device/oidc",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_login["provider"],
        serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
    );
    let oidc_session = response_json(&issue_request(
        oidc_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/oidc",
            body: Some(serde_json::json!({
                "login_id": oidc_login["login_id"],
                "state": oidc_login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_session["claims"]["provider"],
        serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
    );
    let oidc_snapshot = response_json(&issue_request(
        oidc_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        oidc_snapshot["login_providers"][0]["login_path"],
        "/login/oidc"
    );
    assert_eq!(
        oidc_snapshot["login_providers"][0]["device_path"],
        "/device/oidc"
    );
}

#[cfg(feature = "auth-external")]
#[test]
fn external_routes_issue_trusted_header_sessions() {
    let temp = tempdir().expect("temp dir");
    let external_auth = Arc::new(
        build_auth_portal(
            &sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::External {
                    authority: "corp-sso".into(),
                    trusted_principal_header: "x-corp-principal".into(),
                    trusted_internal_only: true,
                },
            ),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build external auth portal"),
    );
    let external_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("external-bootstrap.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(external_auth),
        control_handle: None,
    };
    let external_login = response_json(&issue_request(
        external_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/external",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert_eq!(
        external_login["provider"],
        serde_json::json!({"External": {"authority": "corp-sso"}})
    );
    let external_session = response_json(&issue_request(
        external_context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/external",
            body: Some(serde_json::json!({
                "login_id": external_login["login_id"],
                "state": external_login["state"],
            })),
            headers: &[("x-corp-principal", "alice")],
        },
    ));
    assert_eq!(
        external_session["claims"]["provider"],
        serde_json::json!({"External": {"authority": "corp-sso"}})
    );
    let external_snapshot = response_json(&issue_request(
        external_context,
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        external_snapshot["login_providers"][0]["login_path"],
        "/login/external"
    );
    assert_eq!(
        external_snapshot["login_providers"][0]["callback_path"],
        "/callback/external"
    );
}

#[test]
fn auth_portal_refreshes_and_logs_out_sessions() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("refresh-logout.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    let session = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_id = ContentId::new(
        session["session_id"]
            .as_str()
            .expect("session id")
            .to_owned(),
    );
    assert!(
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .contains_key(&session_id)
    );

    let refreshed = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/refresh",
            body: Some(serde_json::json!({
                "session_id": session_id,
            })),
            headers: &[],
        },
    ));
    let refreshed_session_id = ContentId::new(
        refreshed["session_id"]
            .as_str()
            .expect("refreshed session id")
            .to_owned(),
    );
    let sessions = auth
        .sessions
        .lock()
        .expect("auth session state should not be poisoned");
    assert!(sessions.contains_key(&refreshed_session_id));
    drop(sessions);

    let logout = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/logout",
            body: Some(serde_json::json!({
                "session_id": refreshed_session_id,
            })),
            headers: &[],
        },
    ));
    assert_eq!(logout["logged_out"], true);
    assert!(
        !auth
            .sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .contains_key(&ContentId::new(
                refreshed["session_id"]
                    .as_str()
                    .expect("refreshed session id")
                    .to_owned(),
            ))
    );
}

#[test]
fn auth_portal_persists_pending_logins_and_sessions_across_restart() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("auth-state.cbor"));

    let auth = Arc::new(
        build_auth_portal(
            &config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("persist-auth-state.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth),
        control_handle: None,
    };

    let login = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/login/static",
            body: Some(serde_json::json!({
                "network_id": "secure-demo",
                "principal_hint": "alice",
                "requested_scopes": ["Connect"],
            })),
            headers: &[],
        },
    ));
    assert!(
        temp.path().join("auth-state.cbor").exists(),
        "auth state file should be created after begin_login"
    );

    let restored_auth = Arc::new(
        build_auth_portal(
            &config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("restore auth portal"),
    );
    let restored_context = HttpServerContext {
        auth_state: Some(restored_auth.clone()),
        ..context.clone()
    };
    let session = response_json(&issue_request(
        restored_context,
        IssueRequestSpec {
            method: "POST",
            path: "/callback/static",
            body: Some(serde_json::json!({
                "login_id": login["login_id"],
                "state": login["state"],
                "principal_id": "alice",
            })),
            headers: &[],
        },
    ));
    let session_id = ContentId::new(
        session["session_id"]
            .as_str()
            .expect("session id")
            .to_owned(),
    );

    let restarted_auth = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("restart auth portal");
    assert!(
        restarted_auth
            .get_session(&session_id)
            .expect("load persisted session")
            .is_some()
    );
}

#[test]
fn auth_portal_shares_pending_logins_and_sessions_across_edges() {
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_path = Some(temp.path().join("shared-auth-state.cbor"));

    let edge_a = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build first auth portal");
    let edge_b = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build second auth portal");

    let login = edge_a
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin shared login");
    let session = edge_b
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete shared login");
    assert!(
        edge_a
            .get_session(&session.session_id)
            .expect("load session on first edge")
            .is_some()
    );

    let refreshed = edge_a
        .refresh_session(&session.session_id)
        .expect("refresh shared session");
    if refreshed.session_id != session.session_id {
        assert!(
            edge_b
                .get_session(&session.session_id)
                .expect("old session removed on second edge")
                .is_none()
        );
    }
    assert!(
        edge_b
            .get_session(&refreshed.session_id)
            .expect("refreshed session visible on second edge")
            .is_some()
    );

    assert!(
        edge_b
            .revoke_session(&refreshed.session_id)
            .expect("revoke shared session")
    );
    assert!(
        edge_a
            .get_session(&refreshed.session_id)
            .expect("revoked session removed on first edge")
            .is_none()
    );
}

#[test]
fn auth_portal_redis_shares_pending_logins_and_sessions_across_edges() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let edge_a = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build first redis-backed auth portal");
    let edge_b = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build second redis-backed auth portal");

    let login = edge_a
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed shared login");
    let session = edge_b
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed shared login");
    assert!(
        edge_a
            .get_session(&session.session_id)
            .expect("load session on first redis-backed edge")
            .is_some()
    );

    let edge_c = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build restarted redis-backed auth portal");
    let refreshed = edge_c
        .refresh_session(&session.session_id)
        .expect("refresh redis-backed shared session");
    if refreshed.session_id != session.session_id {
        assert!(
            edge_b
                .get_session(&session.session_id)
                .expect("old redis-backed session removed on second edge")
                .is_none()
        );
    }
    assert!(
        edge_b
            .get_session(&refreshed.session_id)
            .expect("refreshed redis-backed session visible on second edge")
            .is_some()
    );

    assert!(
        edge_a
            .revoke_session(&refreshed.session_id)
            .expect("revoke redis-backed shared session")
    );
    let edge_d = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build final redis-backed auth portal");
    assert!(
        edge_d
            .get_session(&refreshed.session_id)
            .expect("revoked redis-backed session removed after restart")
            .is_none()
    );
}

#[test]
fn auth_portal_redis_preserves_pending_login_across_edge_restart() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let login = {
        let edge = build_auth_portal(
            &config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build redis-backed auth portal");
        edge.begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed login before restart")
    };

    let restarted_edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build restarted redis-backed auth portal");
    let session = restarted_edge
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed login after edge restart");
    assert!(
        restarted_edge
            .get_session(&session.session_id)
            .expect("load session after edge restart")
            .is_some()
    );
}

#[test]
fn auth_portal_redis_times_out_when_lock_is_held() {
    let redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let lock_key = match auth_session_state_store(&config, &NetworkId::new("secure-demo")) {
        AuthSessionStateStore::Redis { lock_key, .. } => lock_key,
        AuthSessionStateStore::File { .. } => panic!("expected redis session state store"),
    };
    let client = redis::Client::open(redis.url.as_str()).expect("redis client");
    let mut connection = client.get_connection().expect("redis connection");
    let acquired = redis::cmd("SET")
        .arg(&lock_key)
        .arg("held-by-test")
        .arg("NX")
        .arg("PX")
        .arg(10_000)
        .query::<Option<String>>(&mut connection)
        .expect("acquire redis test lock");
    assert!(acquired.is_some(), "expected to acquire redis test lock");

    let edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build redis-backed auth portal");
    let error = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect_err("login should time out while redis lock is held");
    assert!(
        error
            .to_string()
            .contains("timed out acquiring redis auth session lock"),
        "unexpected error: {error}",
    );
}

#[test]
fn auth_portal_redis_recovers_after_transient_connection_loss() {
    let mut redis = RedisTestServer::spawn();
    let temp = tempdir().expect("temp dir");
    let mut config = sample_auth_config(temp.path());
    config.session_state_backend = Some(BootstrapAuthSessionBackendConfig::Redis {
        url: redis.url.clone(),
        key_prefix: "burn-p2p:test-auth".into(),
    });

    let edge = build_auth_portal(
        &config,
        NetworkId::new("secure-demo"),
        Version::new(0, 1, 0),
    )
    .expect("build redis-backed auth portal");
    edge.begin_login(LoginRequest {
        network_id: NetworkId::new("secure-demo"),
        principal_hint: Some("alice".into()),
        requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
    })
    .expect("begin redis-backed login before redis outage");

    redis.stop();
    let error = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect_err("login should fail while redis is unavailable");
    let message = error.to_string();
    assert!(
        message.contains("Connection refused")
            || message.contains("connection refused")
            || message.contains("Broken pipe")
            || message.contains("connection reset")
            || message.contains("No such file or directory"),
        "unexpected redis outage error: {message}",
    );

    redis.restart();
    let login = edge
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin redis-backed login after redis restart");
    let session = edge
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete redis-backed login after redis restart");
    assert!(
        edge.get_session(&session.session_id)
            .expect("load redis-backed session after redis restart")
            .is_some()
    );
}

#[test]
fn admin_route_accepts_operator_session_and_rejects_unprivileged_session() {
    let temp = tempdir().expect("temp dir");
    let auth = Arc::new(
        build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-rbac.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };
    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let session_header = session.session_id.as_str().to_owned();
    let allowed = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[("x-session-id", session_header.as_str())],
        },
    );
    assert!(allowed.starts_with("HTTP/1.1 200 OK"));

    let temp = tempdir().expect("temp dir");
    let mut unprivileged = sample_auth_config(temp.path());
    unprivileged.principals[0]
        .custom_claims
        .remove("operator_role");
    let auth = Arc::new(
        build_auth_portal(
            &unprivileged,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-rbac-unprivileged.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: Some(auth.clone()),
        control_handle: None,
    };
    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let denied = issue_request(
        context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[("x-session-id", session.session_id.as_str())],
        },
    );
    assert!(denied.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(response_body(&denied).contains("ExportDiagnostics"));
}

#[test]
fn admin_token_is_dev_only_and_disabled_by_default() {
    let temp = tempdir().expect("temp dir");
    let disabled_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-token-disabled.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let disabled = issue_request(
        disabled_context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[],
        },
    );
    assert!(disabled.starts_with("HTTP/1.1 401 Unauthorized"));

    let enabled_context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::new(Mutex::new(BootstrapAdminState::default())),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(temp.path().join("admin-token-enabled.json")),
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let enabled = issue_request(
        enabled_context,
        IssueRequestSpec {
            method: "POST",
            path: "/admin",
            body: Some(serde_json::json!("ExportDiagnostics")),
            headers: &[],
        },
    );
    assert!(enabled.starts_with("HTTP/1.1 200 OK"));
}

#[test]
fn auth_portal_rotation_and_policy_rollout_persist_and_reissue() {
    let temp = tempdir().expect("temp dir");
    let auth_config = sample_auth_config(temp.path());
    let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
        spec: sample_spec(),
        http_bind_addr: None,
        admin_token: Some("secret-token".into()),
        allow_dev_admin_token: true,
        optional_services: BootstrapOptionalServicesConfig::default(),
        remaining_work_units: None,
        admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
        bootstrap_peer: None,
        embedded_runtime: None,
        auth: Some(auth_config.clone()),
        artifact_publication: None,
    }));
    let config_path = Arc::new(temp.path().join("bootstrap-config.json"));
    std::fs::write(
        &*config_path,
        serde_json::to_vec_pretty(
            &*daemon_config
                .lock()
                .expect("daemon config should not be poisoned"),
        )
        .expect("serialize daemon config"),
    )
    .expect("write daemon config");
    let auth = Arc::new(
        build_auth_portal(
            &auth_config,
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal"),
    );
    let plan = sample_spec().plan().expect("bootstrap plan");
    let state = Arc::new(Mutex::new(BootstrapAdminState::default()));

    let login = auth
        .begin_login(LoginRequest {
            network_id: NetworkId::new("secure-demo"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                },
            ]),
        })
        .expect("begin login");
    let session = auth
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    let node_keypair = Keypair::generate_ed25519();
    let peer_id = burn_p2p::PeerId::new(node_keypair.public().to_peer_id().to_string());
    let first_cert = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issue_certificate(NodeEnrollmentRequest {
            session: session.clone(),
            project_family_id: auth.project_family_id.clone(),
            release_train_hash: auth.required_release_train_hash.clone(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: session.claims.granted_roles.clone(),
            requested_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            client_policy_hash: Some(ContentId::new("policy-auth")),
            serial: 1,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(5),
            revocation_epoch: current_revocation_epoch(&auth, &state),
        })
        .expect("first certificate");

    let rolled_directory = vec![ExperimentDirectoryEntry {
        network_id: NetworkId::new("secure-demo"),
        study_id: burn_p2p::StudyId::new("study-auth"),
        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        workload_id: burn_p2p::WorkloadId::new("auth-demo"),
        display_name: "Rolled Demo".into(),
        model_schema_hash: burn_p2p::ContentId::new("model-auth-rolled"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-auth-rolled"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: Some(4096),
            estimated_download_bytes: 98_304,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: burn_p2p::RevisionId::new("rev-auth-rolled"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
        }]),
        metadata: BTreeMap::from([("rollout".into(), "true".into())]),
    }];
    let rollout = rollout_auth_policy(
        &plan,
        &auth,
        &state,
        AuthPolicyRollout {
            minimum_revocation_epoch: Some(RevocationEpoch(9)),
            directory_entries: Some(rolled_directory.clone()),
            trusted_issuers: None,
            reenrollment: None,
        },
        None,
    )
    .expect("rollout auth policy");
    match rollout {
        burn_p2p_bootstrap::AdminResult::AuthPolicyRolledOut {
            minimum_revocation_epoch,
            directory_entries,
            ..
        } => {
            assert_eq!(minimum_revocation_epoch, Some(RevocationEpoch(9)));
            assert_eq!(directory_entries, 1);
        }
        other => panic!("unexpected rollout result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.minimum_revocation_epoch = auth_config.minimum_revocation_epoch.max(9);
            auth_config.directory_entries = rolled_directory.clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist rollout");
    }

    assert_eq!(current_revocation_epoch(&auth, &state), RevocationEpoch(9));
    let directory_request = HttpRequest {
        method: "GET".into(),
        path: "/directory".into(),
        headers: BTreeMap::from([(
            "x-session-id".into(),
            session.session_id.as_str().to_owned(),
        )]),
        body: Vec::new(),
    };
    let directory = auth_directory_entries(&auth, &directory_request).expect("directory");
    assert_eq!(directory[0].display_name, "Rolled Demo");
    assert_eq!(
        directory[0].current_revision_id,
        burn_p2p::RevisionId::new("rev-auth-rolled")
    );

    let rotate = rotate_authority_material(
        &auth,
        &state,
        Some("rotated-key".into()),
        true,
        true,
        Some("rotate for trust rollout".into()),
    )
    .expect("rotate authority material");
    match rotate {
        burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
            issuer_key_id,
            trusted_issuers,
            reenrollment_required,
            ..
        } => {
            assert_eq!(issuer_key_id, "rotated-key");
            assert_eq!(trusted_issuers, 2);
            assert!(reenrollment_required);
        }
        other => panic!("unexpected rotation result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.issuer_key_id = "rotated-key".into();
            auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                .issuers
                .iter()
                .map(|issuer| TrustedIssuer {
                    issuer_peer_id: issuer.issuer_peer_id.clone(),
                    issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                })
                .collect();
            auth_config.reenrollment = auth
                .reenrollment
                .lock()
                .expect("auth reenrollment state should not be poisoned")
                .clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist rotate");
    }
    let trust_bundle = current_trust_bundle(&auth, &state);
    assert_eq!(trust_bundle.issuers.len(), 2);
    assert!(
        trust_bundle
            .reenrollment
            .as_ref()
            .is_some_and(|status| !status.retired_issuer_peer_ids.is_empty())
    );

    let second_cert = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issue_certificate(NodeEnrollmentRequest {
            session,
            project_family_id: auth.project_family_id.clone(),
            release_train_hash: auth.required_release_train_hash.clone(),
            target_artifact_hash: ContentId::new("demo-artifact-native"),
            peer_id,
            peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
            requested_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            client_policy_hash: Some(ContentId::new("policy-auth")),
            serial: 2,
            not_before: Utc::now() - Duration::seconds(5),
            not_after: Utc::now() + Duration::minutes(5),
            revocation_epoch: current_revocation_epoch(&auth, &state),
        })
        .expect("second certificate");
    assert_ne!(
        first_cert.body.signature.signer,
        second_cert.body.signature.signer
    );
    assert_eq!(second_cert.body.signature.key_id, "rotated-key");
    assert_eq!(second_cert.claims().revocation_epoch, RevocationEpoch(9));

    let retired = retire_trusted_issuers(
        &auth,
        &state,
        &BTreeSet::from([first_cert.body.signature.signer.clone()]),
    )
    .expect("retire trusted issuer");
    match retired {
        burn_p2p_bootstrap::AdminResult::TrustedIssuersRetired {
            retired_issuers,
            remaining_issuers,
            reenrollment_required,
        } => {
            assert_eq!(retired_issuers, 1);
            assert_eq!(remaining_issuers, 1);
            assert!(!reenrollment_required);
        }
        other => panic!("unexpected retired result: {other:?}"),
    }
    {
        let mut config_guard = daemon_config
            .lock()
            .expect("daemon config should not be poisoned");
        if let Some(auth_config) = config_guard.auth.as_mut() {
            auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                .issuers
                .iter()
                .map(|issuer| TrustedIssuer {
                    issuer_peer_id: issuer.issuer_peer_id.clone(),
                    issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                })
                .collect();
            auth_config.reenrollment = auth
                .reenrollment
                .lock()
                .expect("auth reenrollment state should not be poisoned")
                .clone();
        }
        persist_daemon_config(&config_path, &config_guard).expect("persist retire");
    }

    let persisted: BootstrapDaemonConfig = serde_json::from_slice(
        &std::fs::read(&*config_path).expect("read persisted daemon config"),
    )
    .expect("deserialize persisted daemon config");
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .minimum_revocation_epoch,
        9
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .issuer_key_id,
        "rotated-key"
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .directory_entries[0]
            .current_revision_id,
        burn_p2p::RevisionId::new("rev-auth-rolled")
    );
    assert_eq!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .trusted_issuers
            .len(),
        1
    );
    assert!(
        persisted
            .auth
            .as_ref()
            .expect("persisted auth config")
            .reenrollment
            .is_none()
    );
}

#[cfg(all(
    feature = "metrics-indexer",
    feature = "artifact-publish",
    feature = "artifact-download",
    feature = "browser-edge",
    feature = "browser-join"
))]
#[test]
fn metrics_routes_export_snapshots_ledger_and_head_views() {
    let storage = burn_p2p::StorageConfig::new(tempdir().expect("artifact metrics tempdir").keep());
    let artifact_store = burn_p2p::FsArtifactStore::new(storage.root.clone());
    artifact_store
        .ensure_layout()
        .expect("ensure artifact store layout");
    let artifact_payload = b"checkpoint-bytes";
    let artifact_descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p_core::Precision::Fp32,
            ContentId::new("model-schema-metrics"),
            "checkpoint-format",
        ),
        artifact_payload,
        burn_p2p::ChunkingScheme::new(4).expect("chunking"),
    )
    .expect("artifact descriptor");
    let artifact_descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-metrics"),
        head_id: Some(burn_p2p::HeadId::new("head-candidate")),
        ..artifact_descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&artifact_descriptor, artifact_payload)
        .expect("store checkpoint artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-metrics"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            128,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-metrics"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            head_id: burn_p2p::HeadId::new("head-candidate"),
            artifact_id: artifact_descriptor.artifact_id.clone(),
            parent_head_id: Some(burn_p2p::HeadId::new("head-base")),
            global_step: 12,
            created_at: Utc::now() - Duration::seconds(1),
            metrics: BTreeMap::new(),
        }],
        peer_window_metrics: vec![
            PeerWindowMetrics {
                network_id: NetworkId::new("secure-demo"),
                experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                revision_id: burn_p2p::RevisionId::new("rev-metrics"),
                workload_id: WorkloadId::new("metrics-demo"),
                dataset_view_id: DatasetViewId::new("view-metrics"),
                peer_id: PeerId::new("peer-metrics"),
                principal_id: None,
                lease_id: LeaseId::new("lease-metrics"),
                base_head_id: burn_p2p::HeadId::new("head-base"),
                window_started_at: Utc::now() - Duration::seconds(4),
                window_finished_at: Utc::now() - Duration::seconds(2),
                attempted_tokens_or_samples: 128,
                accepted_tokens_or_samples: Some(128),
                local_train_loss_mean: Some(0.25),
                local_train_loss_last: Some(0.2),
                grad_or_delta_norm: Some(1.1),
                optimizer_step_count: 8,
                compute_time_ms: 1_500,
                data_fetch_time_ms: 250,
                publish_latency_ms: 100,
                head_lag_at_start: 0,
                head_lag_at_finish: 1,
                backend_class: BackendClass::Ndarray,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("secure-demo"),
                experiment_id: burn_p2p::ExperimentId::new("exp-secondary"),
                revision_id: burn_p2p::RevisionId::new("rev-secondary"),
                workload_id: WorkloadId::new("metrics-demo-secondary"),
                dataset_view_id: DatasetViewId::new("view-secondary"),
                peer_id: PeerId::new("peer-secondary"),
                principal_id: None,
                lease_id: LeaseId::new("lease-secondary"),
                base_head_id: burn_p2p::HeadId::new("head-secondary"),
                window_started_at: Utc::now() - Duration::seconds(8),
                window_finished_at: Utc::now() - Duration::seconds(6),
                attempted_tokens_or_samples: 64,
                accepted_tokens_or_samples: Some(64),
                local_train_loss_mean: Some(0.4),
                local_train_loss_last: Some(0.35),
                grad_or_delta_norm: Some(0.8),
                optimizer_step_count: 4,
                compute_time_ms: 900,
                data_fetch_time_ms: 150,
                publish_latency_ms: 80,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Ndarray,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
        ],
        reducer_cohort_metrics: vec![ReducerCohortMetrics {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("metrics-demo"),
            dataset_view_id: DatasetViewId::new("view-metrics"),
            merge_window_id: ContentId::new("merge-window-metrics"),
            reducer_group_id: ContentId::new("reducers-metrics"),
            captured_at: Utc::now() - Duration::seconds(1),
            base_head_id: burn_p2p::HeadId::new("head-base"),
            candidate_head_id: Some(burn_p2p::HeadId::new("head-candidate")),
            received_updates: 2,
            accepted_updates: 1,
            rejected_updates: 1,
            sum_weight: 2.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.5,
            staleness_max: 1.0,
            window_close_delay_ms: 50,
            cohort_duration_ms: 1_000,
            aggregate_norm: 0.9,
            reducer_load: 0.2,
            ingress_bytes: 2_048,
            egress_bytes: 1_024,
            replica_agreement: Some(0.72),
            late_arrival_count: Some(1),
            missing_peer_count: Some(1),
            rejection_reasons: BTreeMap::from([("stale".into(), 1)]),
            status: ReducerCohortStatus::Inconsistent,
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("metrics-demo"),
            head_id: burn_p2p::HeadId::new("head-candidate"),
            base_head_id: Some(burn_p2p::HeadId::new("head-base")),
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-metrics"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("view-metrics"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
            revision_id: burn_p2p::RevisionId::new("rev-metrics"),
            captured_at: Utc::now() - Duration::seconds(1),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.root.clone()),
        publication_store_root: Some(storage.publication_dir()),
        ..BootstrapAdminState::default()
    }));
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state,
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: None,
        })),
        config_path: Arc::new(std::env::temp_dir().join("burn-p2p-bootstrap-metrics.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };

    let snapshots = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(snapshots.as_array().expect("snapshots array").len(), 2);
    assert_eq!(snapshots[0]["manifest"]["experiment_id"], "exp-metrics");

    let ledger = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/ledger",
            body: None,
            headers: &[],
        },
    ));
    assert!(!ledger.as_array().expect("ledger array").is_empty());

    let catchup = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/catchup",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(catchup.as_array().expect("catchup array").len(), 2);
    assert_eq!(
        catchup[0]["snapshot"]["manifest"]["experiment_id"],
        "exp-metrics"
    );

    let live_event = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/live/latest",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(live_event["kind"], "CatchupRefresh");
    assert_eq!(live_event["cursors"].as_array().expect("cursors").len(), 2);

    let head_reports = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/heads/head-candidate",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        head_reports.as_array().expect("head reports array").len(),
        1
    );
    assert_eq!(
        head_reports[0]["eval_protocol_id"],
        eval_protocol.eval_protocol_id.as_str()
    );

    let experiment_snapshots = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/experiments/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_snapshots
            .as_array()
            .expect("experiment snapshots array")
            .len(),
        1
    );
    assert_eq!(
        experiment_snapshots[0]["manifest"]["revision_id"],
        "rev-metrics"
    );

    let experiment_catchup = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/catchup/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_catchup
            .as_array()
            .expect("experiment catchup array")
            .len(),
        1
    );

    let candidates = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/candidates",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(candidates.as_array().expect("candidates array").len(), 1);
    assert_eq!(candidates[0]["candidate_head_id"], "head-candidate");

    let experiment_candidates = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/candidates/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_candidates
            .as_array()
            .expect("experiment candidates array")
            .len(),
        1
    );

    let disagreements = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/disagreements",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        disagreements.as_array().expect("disagreements array").len(),
        1
    );
    assert_eq!(disagreements[0]["status"], "Inconsistent");

    let experiment_disagreements = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/disagreements/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_disagreements
            .as_array()
            .expect("experiment disagreements array")
            .len(),
        1
    );

    let peer_windows = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        peer_windows
            .as_array()
            .expect("peer-window distributions array")
            .len(),
        2
    );
    assert_eq!(peer_windows[0]["base_head_id"], "head-base");

    let experiment_peer_windows = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows/exp-metrics",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(
        experiment_peer_windows
            .as_array()
            .expect("experiment peer-window distributions array")
            .len(),
        1
    );

    let peer_window_detail = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/metrics/peer-windows/exp-metrics/rev-metrics/head-base",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(peer_window_detail["summary"]["base_head_id"], "head-base");
    assert_eq!(
        peer_window_detail["windows"]
            .as_array()
            .expect("peer-window detail windows array")
            .len(),
        1
    );

    let portal_html = issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal",
            body: None,
            headers: &[],
        },
    );
    assert!(portal_html.starts_with("HTTP/1.1 404 Not Found"));

    let portal_snapshot = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/portal/snapshot",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(portal_snapshot["network_id"], "secure-demo");
    assert!(portal_snapshot["captured_at"].as_str().is_some());

    let artifact_live_event = response_json(&issue_request(
        context.clone(),
        IssueRequestSpec {
            method: "GET",
            path: "/artifacts/live/latest",
            body: None,
            headers: &[],
        },
    ));
    assert_eq!(artifact_live_event["kind"], "AliasUpdated");
    assert!(artifact_live_event["alias_name"].as_str().is_some());

    let server = HttpTestServer::spawn(context);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let client = BrowserEdgeClient::new(
                BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot),
                BrowserEnrollmentConfig {
                    network_id: NetworkId::new("secure-demo"),
                    project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                    release_train_hash: ContentId::new("demo-train"),
                    target_artifact_id: "native-test-client".into(),
                    target_artifact_hash: ContentId::new("demo-artifact-native"),
                    login_path: "/login/static".into(),
                    callback_path: "/callback/static".into(),
                    enroll_path: "/enroll".into(),
                    trust_bundle_path: "/trust".into(),
                    requested_scopes: BTreeSet::new(),
                    session_ttl_secs: 300,
                },
            );

            let snapshots = client
                .fetch_metrics_snapshots()
                .await
                .expect("fetch metrics snapshots");
            assert_eq!(snapshots.len(), 2);
            assert_eq!(snapshots[0].manifest.experiment_id.as_str(), "exp-metrics");

            let experiment_snapshots = client
                .fetch_metrics_snapshots_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment metrics snapshots");
            assert_eq!(experiment_snapshots.len(), 1);

            let catchup = client
                .fetch_metrics_catchup()
                .await
                .expect("fetch metrics catchup");
            assert_eq!(catchup.len(), 2);
            assert_eq!(
                catchup[0].snapshot.manifest.experiment_id.as_str(),
                "exp-metrics"
            );

            let experiment_catchup = client
                .fetch_metrics_catchup_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment metrics catchup");
            assert_eq!(experiment_catchup.len(), 1);

            let candidates = client
                .fetch_metrics_candidates()
                .await
                .expect("fetch metrics candidates");
            assert_eq!(candidates.len(), 1);
            assert_eq!(
                candidates[0]
                    .candidate_head_id
                    .as_ref()
                    .expect("candidate head")
                    .as_str(),
                "head-candidate"
            );

            let experiment_candidates = client
                .fetch_metrics_candidates_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment metrics candidates");
            assert_eq!(experiment_candidates.len(), 1);

            let disagreements = client
                .fetch_metrics_disagreements()
                .await
                .expect("fetch metrics disagreements");
            assert_eq!(disagreements.len(), 1);
            assert_eq!(disagreements[0].status, ReducerCohortStatus::Inconsistent);

            let experiment_disagreements = client
                .fetch_metrics_disagreements_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment metrics disagreements");
            assert_eq!(experiment_disagreements.len(), 1);

            let peer_windows = client
                .fetch_metrics_peer_windows()
                .await
                .expect("fetch peer-window distributions");
            assert_eq!(peer_windows.len(), 2);
            assert_eq!(peer_windows[0].base_head_id.as_str(), "head-base");

            let experiment_peer_windows = client
                .fetch_metrics_peer_windows_for_experiment(&burn_p2p::ExperimentId::new(
                    "exp-metrics",
                ))
                .await
                .expect("fetch experiment peer-window distributions");
            assert_eq!(experiment_peer_windows.len(), 1);

            let peer_window_detail = client
                .fetch_metrics_peer_window_detail(
                    &burn_p2p::ExperimentId::new("exp-metrics"),
                    &burn_p2p::RevisionId::new("rev-metrics"),
                    &burn_p2p::HeadId::new("head-base"),
                )
                .await
                .expect("fetch peer-window detail");
            assert_eq!(
                peer_window_detail.summary.base_head_id.as_str(),
                "head-base"
            );
            assert_eq!(peer_window_detail.windows.len(), 1);

            let ledger = client
                .fetch_metrics_ledger()
                .await
                .expect("fetch metrics ledger");
            assert!(!ledger.is_empty());

            let live_event = client
                .fetch_latest_metrics_live_event()
                .await
                .expect("fetch latest metrics live event");
            assert_eq!(live_event.cursors.len(), 2);

            let mut worker = BrowserWorkerRuntime::start(
                BrowserRuntimeConfig {
                    role: BrowserRuntimeRole::BrowserObserver,
                    selected_experiment: Some(burn_p2p::ExperimentId::new("exp-metrics")),
                    ..BrowserRuntimeConfig::new(
                        server.base_url(),
                        NetworkId::new("secure-demo"),
                        ContentId::new("demo-train"),
                        "browser-wasm",
                        ContentId::new("demo-artifact-native"),
                    )
                },
                BrowserCapabilityReport::default(),
                BrowserTransportStatus::default(),
            );
            client
                .sync_worker_runtime(&mut worker, None, false)
                .await
                .expect("sync worker runtime with metrics");
            assert_eq!(worker.storage.metrics_catchup_bundles.len(), 1);
            assert!(worker.storage.last_metrics_live_event.is_some());

            let reports = client
                .fetch_head_eval_reports(&burn_p2p::HeadId::new("head-candidate"))
                .await
                .expect("fetch head eval reports");
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].eval_protocol_id.as_str(),
                eval_protocol.eval_protocol_id.as_str()
            );

            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            assert!(
                aliases
                    .iter()
                    .any(|row| row.alias.alias_name == "latest/serve")
            );
            assert!(aliases.iter().any(|row| {
                row.alias.alias_name
                    == format!("best_val/{}/serve", eval_protocol.eval_protocol_id.as_str())
            }));

            let experiment_aliases = client
                .fetch_artifact_aliases_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch experiment artifact aliases");
            assert_eq!(experiment_aliases.len(), aliases.len());

            let initial_artifact_live_event = client
                .fetch_latest_artifact_live_event()
                .await
                .expect("fetch latest artifact live event");
            assert_eq!(
                initial_artifact_live_event.kind,
                ArtifactLiveEventKind::AliasUpdated
            );
            assert!(initial_artifact_live_event.alias_name.is_some());

            let run_summaries = client
                .fetch_artifact_runs_for_experiment(&burn_p2p::ExperimentId::new("exp-metrics"))
                .await
                .expect("fetch artifact run summaries");
            assert_eq!(run_summaries.len(), 1);
            let run_view = client
                .fetch_artifact_run_view(
                    &burn_p2p::ExperimentId::new("exp-metrics"),
                    &burn_p2p_core::RunId::derive(&("exp-metrics", "rev-metrics")).expect("run id"),
                )
                .await
                .expect("fetch artifact run view");
            assert!(!run_view.alias_history.is_empty());

            let head_view = client
                .fetch_head_artifact_view(&burn_p2p::HeadId::new("head-candidate"))
                .await
                .expect("fetch head artifact view");
            assert_eq!(head_view.head.head_id.as_str(), "head-candidate");
            assert!(!head_view.alias_history.is_empty());

            let run_history_html = reqwest::get(format!(
                "{}/portal/artifacts/runs/exp-metrics/{}",
                server.base_url(),
                burn_p2p_core::RunId::derive(&("exp-metrics", "rev-metrics"))
                    .expect("run id")
                    .as_str()
            ))
            .await
            .expect("fetch run history html")
            .text()
            .await
            .expect("run history html text");
            assert!(run_history_html.contains("Artifact run"));
            assert!(run_history_html.contains("Alias transition history"));
            assert!(run_history_html.contains("latest/serve"));

            let head_history_html = reqwest::get(format!(
                "{}/portal/artifacts/heads/head-candidate",
                server.base_url()
            ))
            .await
            .expect("fetch head artifact html")
            .text()
            .await
            .expect("head artifact html text");
            assert!(head_history_html.contains("Artifact head head-candidate"));
            assert!(head_history_html.contains("Available profiles"));
            assert!(head_history_html.contains("Run history"));

            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("latest serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-candidate"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            burn_p2p_publish::DEFAULT_PUBLICATION_TARGET_ID,
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request artifact export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);

            let export_job_view = client
                .fetch_artifact_export_job(&export_job.export_job_id)
                .await
                .expect("fetch artifact export job");
            assert_eq!(
                export_job_view.status,
                burn_p2p_core::ExportJobStatus::Ready
            );

            let ready_live_event = client
                .fetch_latest_artifact_live_event()
                .await
                .expect("fetch ready artifact live event");
            assert_eq!(ready_live_event.kind, ArtifactLiveEventKind::ExportJobReady);
            assert_eq!(
                ready_live_event
                    .export_job_id
                    .as_ref()
                    .expect("ready export job id"),
                &export_job.export_job_id
            );

            let streamed_live_event = client
                .subscribe_once_artifact_live_event()
                .await
                .expect("stream artifact live event");
            assert_eq!(
                streamed_live_event.kind,
                ArtifactLiveEventKind::ExportJobReady
            );
            assert_eq!(
                streamed_live_event
                    .published_artifact_id
                    .as_ref()
                    .expect("published artifact id"),
                ready_live_event
                    .published_artifact_id
                    .as_ref()
                    .expect("latest published artifact id")
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-metrics"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-candidate"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            burn_p2p_publish::DEFAULT_PUBLICATION_TARGET_ID,
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request artifact download ticket");
            assert!(ticket.download_path.starts_with("/artifacts/download/"));
            assert_eq!(
                client.artifact_download_url(&ticket.ticket.download_ticket_id),
                format!(
                    "{}/artifacts/download/{}",
                    server.base_url(),
                    ticket.ticket.download_ticket_id.as_str()
                )
            );

            let download_response = reqwest::Client::new()
                .get(client.artifact_download_url(&ticket.ticket.download_ticket_id))
                .send()
                .await
                .expect("download artifact")
                .error_for_status()
                .expect("artifact download status");
            let bytes = download_response.bytes().await.expect("artifact bytes");
            assert_eq!(bytes.as_ref(), artifact_payload);
        });
}

#[cfg(feature = "artifact-s3")]
#[test]
fn artifact_download_redirects_to_signed_s3_url_when_target_supports_redirect() {
    let storage = tempdir().expect("tempdir");
    let artifact_store = burn_p2p_checkpoint::FsArtifactStore::new(storage.path().join("cas"));
    artifact_store.ensure_layout().expect("layout");
    let artifact_payload = b"checkpoint-bytes";
    let descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p_checkpoint::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p::Precision::Fp32,
            ContentId::new("schema-s3"),
            "checkpoint-format",
        ),
        artifact_payload,
        burn_p2p_checkpoint::ChunkingScheme::new(4).expect("chunking"),
    )
    .expect("descriptor");
    let descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-s3"),
        head_id: Some(burn_p2p::HeadId::new("head-s3")),
        ..descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&descriptor, artifact_payload)
        .expect("store artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-s3"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            32,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let (s3_endpoint, uploaded, handle) = spawn_artifact_s3_server();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-s3"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            head_id: burn_p2p::HeadId::new("head-s3"),
            artifact_id: descriptor.artifact_id.clone(),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            workload_id: WorkloadId::new("workload-s3"),
            head_id: burn_p2p::HeadId::new("head-s3"),
            base_head_id: None,
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-s3"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.1))]),
            sample_count: 32,
            dataset_view_id: DatasetViewId::new("view-s3"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
            revision_id: burn_p2p::RevisionId::new("rev-s3"),
            captured_at: Utc::now(),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.path().join("cas")),
        publication_store_root: Some(storage.path().join("publication")),
        publication_targets: vec![burn_p2p_core::PublicationTarget {
            publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-default"),
            label: "s3".into(),
            kind: burn_p2p_core::PublicationTargetKind::S3Compatible,
            publication_mode: burn_p2p_core::PublicationMode::LazyOnDemand,
            access_mode: burn_p2p_core::PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: true,
            edge_proxy_required: false,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(300),
            allowed_artifact_profiles: BTreeSet::from([
                burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                burn_p2p_core::ArtifactProfile::ManifestOnly,
            ]),
            eager_alias_names: BTreeSet::new(),
            local_root: None,
            bucket: Some("artifacts".into()),
            endpoint: Some(s3_endpoint.clone()),
            region: Some("us-east-1".into()),
            access_key_id: Some("test-access-key".into()),
            secret_access_key: Some("test-secret-key".into()),
            session_token: None,
            path_prefix: Some("exports".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: Some("AES256".into()),
            signed_url_ttl_secs: Some(120),
        }],
        ..BootstrapAdminState::default()
    }));
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .refresh_publication_views()
        .expect("sync publication views");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::clone(&state),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: Some(BootstrapArtifactPublicationConfig {
                targets: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .publication_targets
                    .clone(),
            }),
        })),
        config_path: Arc::new(storage.path().join("artifact-s3.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let client = BrowserEdgeClient::new(
                BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot),
                BrowserEnrollmentConfig {
                    network_id: NetworkId::new("secure-demo"),
                    project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                    release_train_hash: ContentId::new("demo-train"),
                    target_artifact_id: "native-test-client".into(),
                    target_artifact_hash: ContentId::new("demo-artifact-native"),
                    login_path: "/login/static".into(),
                    callback_path: "/callback/static".into(),
                    enroll_path: "/enroll".into(),
                    trust_bundle_path: "/trust".into(),
                    requested_scopes: BTreeSet::new(),
                    session_ttl_secs: 300,
                },
            );
            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            "s3-default",
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);
            assert_eq!(
                uploaded
                    .lock()
                    .expect("uploaded map should not be poisoned")
                    .values()
                    .next()
                    .cloned()
                    .expect("uploaded object"),
                artifact_payload
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new(
                            "s3-default",
                        ),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request download ticket");

            let response = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("build reqwest client")
                .get(client.artifact_download_url(&ticket.ticket.download_ticket_id))
                .send()
                .await
                .expect("artifact download request");
            assert_eq!(response.status(), reqwest::StatusCode::FOUND);
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .expect("redirect location")
                .to_str()
                .expect("location header text")
                .to_owned();
            assert!(location.starts_with(&s3_endpoint));
            assert!(location.contains("X-Amz-Signature="));
        });

    handle.join().expect("join s3 test server");
}

#[cfg(feature = "artifact-s3")]
#[test]
fn artifact_download_streams_large_s3_proxy_payload_when_target_requires_portal_proxy() {
    let storage = tempdir().expect("tempdir");
    let artifact_store = burn_p2p_checkpoint::FsArtifactStore::new(storage.path().join("cas"));
    artifact_store.ensure_layout().expect("layout");
    let artifact_payload = vec![b'z'; 2 * 1024 * 1024 + 17];
    let descriptor = burn_p2p_checkpoint::build_artifact_descriptor_from_bytes(
        &burn_p2p_checkpoint::ArtifactBuildSpec::new(
            burn_p2p::ArtifactKind::FullHead,
            burn_p2p::Precision::Fp32,
            ContentId::new("schema-s3-proxy"),
            "checkpoint-format",
        ),
        &artifact_payload,
        burn_p2p_checkpoint::ChunkingScheme::new(64 * 1024).expect("chunking"),
    )
    .expect("descriptor");
    let descriptor = burn_p2p::ArtifactDescriptor {
        artifact_id: burn_p2p::ArtifactId::new("artifact-s3-proxy"),
        head_id: Some(burn_p2p::HeadId::new("head-s3-proxy")),
        ..descriptor
    };
    artifact_store
        .store_prebuilt_artifact_bytes(&descriptor, &artifact_payload)
        .expect("store artifact");
    let eval_protocol = burn_p2p_core::EvalProtocolManifest::new(
        "validation",
        DatasetViewId::new("view-s3-proxy"),
        "validation",
        vec![burn_p2p_core::EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        burn_p2p_core::EvalProtocolOptions::new(
            burn_p2p_core::EvalAggregationRule::Mean,
            32,
            7,
            "v1",
        ),
    )
    .expect("eval protocol");
    let (s3_endpoint, uploaded, handle) = spawn_artifact_s3_server();
    let state = Arc::new(Mutex::new(BootstrapAdminState {
        head_descriptors: vec![HeadDescriptor {
            study_id: burn_p2p::StudyId::new("study-s3-proxy"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            head_id: burn_p2p::HeadId::new("head-s3-proxy"),
            artifact_id: descriptor.artifact_id.clone(),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        head_eval_reports: vec![HeadEvalReport {
            network_id: NetworkId::new("secure-demo"),
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            workload_id: WorkloadId::new("workload-s3-proxy"),
            head_id: burn_p2p::HeadId::new("head-s3-proxy"),
            base_head_id: None,
            eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
            evaluator_set_id: ContentId::new("validators-s3-proxy"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.1))]),
            sample_count: 32,
            dataset_view_id: DatasetViewId::new("view-s3-proxy"),
            started_at: Utc::now() - Duration::seconds(2),
            finished_at: Utc::now() - Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        }],
        eval_protocol_manifests: vec![burn_p2p_bootstrap::StoredEvalProtocolManifestRecord {
            experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
            revision_id: burn_p2p::RevisionId::new("rev-s3-proxy"),
            captured_at: Utc::now(),
            manifest: eval_protocol.clone(),
        }],
        artifact_store_root: Some(storage.path().join("cas")),
        publication_store_root: Some(storage.path().join("publication")),
        publication_targets: vec![burn_p2p_core::PublicationTarget {
            publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
            label: "s3-proxy".into(),
            kind: burn_p2p_core::PublicationTargetKind::S3Compatible,
            publication_mode: burn_p2p_core::PublicationMode::LazyOnDemand,
            access_mode: burn_p2p_core::PublicationAccessMode::Authenticated,
            allow_public_reads: false,
            supports_signed_urls: true,
            edge_proxy_required: true,
            max_artifact_size_bytes: None,
            retention_ttl_secs: Some(300),
            allowed_artifact_profiles: BTreeSet::from([
                burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                burn_p2p_core::ArtifactProfile::ManifestOnly,
            ]),
            eager_alias_names: BTreeSet::new(),
            local_root: None,
            bucket: Some("artifacts".into()),
            endpoint: Some(s3_endpoint.clone()),
            region: Some("us-east-1".into()),
            access_key_id: Some("test-access-key".into()),
            secret_access_key: Some("test-secret-key".into()),
            session_token: None,
            path_prefix: Some("exports".into()),
            multipart_threshold_bytes: None,
            server_side_encryption: Some("AES256".into()),
            signed_url_ttl_secs: Some(120),
        }],
        ..BootstrapAdminState::default()
    }));
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .refresh_publication_views()
        .expect("sync publication views");
    let context = HttpServerContext {
        plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
        state: Arc::clone(&state),
        config: Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            bootstrap_peer: None,
            embedded_runtime: None,
            auth: None,
            artifact_publication: Some(BootstrapArtifactPublicationConfig {
                targets: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .publication_targets
                    .clone(),
            }),
        })),
        config_path: Arc::new(storage.path().join("artifact-s3-proxy.json")),
        admin_token: None,
        allow_dev_admin_token: false,
        remaining_work_units: None,
        admin_signer_peer_id: PeerId::new("bootstrap-authority"),
        auth_state: None,
        control_handle: None,
    };
    let server = HttpTestServer::spawn(context);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let snapshot: BrowserEdgeSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let client = BrowserEdgeClient::new(
                BrowserUiBindings::from_edge_snapshot(server.base_url(), &snapshot),
                BrowserEnrollmentConfig {
                    network_id: NetworkId::new("secure-demo"),
                    project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                    release_train_hash: ContentId::new("demo-train"),
                    target_artifact_id: "native-test-client".into(),
                    target_artifact_hash: ContentId::new("demo-artifact-native"),
                    login_path: "/login/static".into(),
                    callback_path: "/callback/static".into(),
                    enroll_path: "/enroll".into(),
                    trust_bundle_path: "/trust".into(),
                    requested_scopes: BTreeSet::new(),
                    session_ttl_secs: 300,
                },
            );
            let aliases = client
                .fetch_artifact_aliases()
                .await
                .expect("fetch artifact aliases");
            let serve_alias = aliases
                .iter()
                .find(|row| row.alias.alias_name == "latest/serve")
                .expect("serve alias");
            let export_job = client
                .request_artifact_export(
                    &burn_p2p_publish::ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3-proxy"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request export");
            assert_eq!(export_job.status, burn_p2p_core::ExportJobStatus::Ready);
            assert_eq!(
                uploaded
                    .lock()
                    .expect("uploaded map should not be poisoned")
                    .values()
                    .next()
                    .map(Vec::len)
                    .expect("uploaded object size"),
                artifact_payload.len()
            );

            let ticket = client
                .request_artifact_download_ticket(
                    &burn_p2p_publish::DownloadTicketRequest {
                        principal_id: PrincipalId::new("anonymous"),
                        experiment_id: burn_p2p::ExperimentId::new("exp-s3-proxy"),
                        run_id: serve_alias.alias.run_id.clone(),
                        head_id: burn_p2p::HeadId::new("head-s3-proxy"),
                        artifact_profile: burn_p2p_core::ArtifactProfile::ServeCheckpoint,
                        publication_target_id: burn_p2p_core::PublicationTargetId::new("s3-proxy"),
                        artifact_alias_id: Some(serve_alias.alias.artifact_alias_id.clone()),
                    },
                    None,
                )
                .await
                .expect("request download ticket");

            let body = client
                .download_artifact_bytes(&ticket.ticket.download_ticket_id)
                .await
                .expect("artifact proxy bytes");
            assert_eq!(body.len(), artifact_payload.len());
            assert_eq!(body.as_slice(), artifact_payload.as_slice());
        });

    handle.join().expect("join s3 test server");
}
