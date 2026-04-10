#[cfg(feature = "artifact-s3")]
pub(super) use super::super::BootstrapArtifactPublicationConfig;
pub(super) use super::super::{
    AuthSessionStateStore, BootstrapAuthConfig, BootstrapAuthConnectorConfig,
    BootstrapAuthPrincipal, BootstrapAuthSessionBackendConfig, BootstrapDaemonConfig,
    BootstrapEmbeddedDaemonConfig, BootstrapOptionalServicesConfig, BootstrapPeerDaemonConfig,
    HttpRequest, HttpServerContext, auth_directory_entries, auth_session_state_store,
    build_auth_portal, current_revocation_epoch, current_trust_bundle, default_issuer_key_id,
    handle_connection, load_or_create_keypair, persist_daemon_config, retire_trusted_issuers,
    rollout_auth_policy, rotate_authority_material, validate_compiled_feature_support_with,
};
pub(super) use crate::compiled_feature_set;
#[cfg(feature = "artifact-s3")]
use std::io::ErrorKind;
pub(super) use std::{
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

pub(super) use burn_p2p::{
    BrowserMode, CallbackPayload, CompiledFeatureSet, ContributionReceipt, EdgeFeature,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
    LoginRequest, MetricValue, NodeEnrollmentRequest, PeerId, PeerRole, PeerRoleSet, PrincipalId,
    ProfileMode, SocialMode, TrustedIssuer,
};
#[cfg(any(feature = "metrics-indexer", feature = "artifact-s3"))]
pub(super) use burn_p2p::{DatasetViewId, WorkloadId};
#[cfg(any(feature = "metrics-indexer", feature = "artifact-s3"))]
pub(super) use burn_p2p::{HeadEvalReport, HeadEvalStatus, MetricTrustClass};
#[cfg(feature = "metrics-indexer")]
pub(super) use burn_p2p::{
    LeaseId, PeerWindowMetrics, PeerWindowStatus, ReducerCohortMetrics, ReducerCohortStatus,
};
#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
pub(super) use burn_p2p::{OverlayChannel, OverlayTopic, ReducerLoadAnnouncement};
pub(super) use burn_p2p_bootstrap::{
    AdminApiPlan, ArchivePlan, AuthPolicyRollout, BootstrapAdminState, BootstrapPreset,
    BootstrapSpec,
};
pub(super) use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserEdgeClient, BrowserEdgeSnapshot, BrowserEnrollmentConfig,
    BrowserRuntimeConfig, BrowserRuntimeRole, BrowserRuntimeState, BrowserSessionState,
    BrowserTransportStatus, BrowserUiBindings, BrowserValidationPlan, BrowserWorkerCommand,
    BrowserWorkerEvent, BrowserWorkerIdentity, BrowserWorkerRuntime,
};
#[cfg(feature = "artifact-publish")]
pub(super) use burn_p2p_core::ArtifactLiveEventKind;
#[cfg(feature = "auth-github")]
pub(super) use burn_p2p_core::AuthProvider;
#[cfg(feature = "metrics-indexer")]
pub(super) use burn_p2p_core::BackendClass;
pub(super) use burn_p2p_core::{ClientPlatform, ContentId, NetworkId, RevocationEpoch};
#[cfg(all(
    feature = "browser-edge",
    feature = "browser-join",
    feature = "social",
    feature = "auth-static"
))]
pub(super) use burn_p2p_security::{PeerAdmissionReport, PeerTrustLevel};
pub(super) use chrono::{Duration, Utc};
pub(super) use libp2p_identity::Keypair;
pub(super) use semver::Version;
pub(super) use serde_json::Value;
pub(super) use tempfile::tempdir;

pub(super) struct IssueRequestSpec<'a> {
    pub(super) method: &'a str,
    pub(super) path: &'a str,
    pub(super) body: Option<Value>,
    pub(super) headers: &'a [(&'a str, &'a str)],
}

pub(super) struct HttpTestServer {
    base_url: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

pub(super) struct RedisTestServer {
    pub(super) url: String,
    port: u16,
    child: Option<Child>,
    state: tempfile::TempDir,
}

#[cfg(feature = "artifact-s3")]
pub(super) type ArtifactS3ObjectMap = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

impl HttpTestServer {
    pub(super) fn spawn(context: HttpServerContext) -> Self {
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

    pub(super) fn base_url(&self) -> &str {
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
    pub(super) fn spawn() -> Self {
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

    pub(super) fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    pub(super) fn restart(&mut self) {
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
pub(super) fn spawn_provider_json_server(
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
pub(super) fn spawn_artifact_s3_server() -> (String, ArtifactS3ObjectMap, thread::JoinHandle<()>) {
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

pub(super) fn sample_auth_config(root: &std::path::Path) -> BootstrapAuthConfig {
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

pub(super) fn sample_auth_config_with_connector(
    root: &std::path::Path,
    connector: BootstrapAuthConnectorConfig,
) -> BootstrapAuthConfig {
    let mut config = sample_auth_config(root);
    config.connector = connector;
    config
}

pub(super) fn sample_spec() -> BootstrapSpec {
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

pub(super) fn issue_request(context: HttpServerContext, request: IssueRequestSpec<'_>) -> String {
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

pub(super) fn response_json(response: &str) -> serde_json::Value {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .expect("split response headers");
    serde_json::from_str(body).expect("deserialize response body")
}

pub(super) fn response_body(response: &str) -> &str {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .expect("split response headers");
    body
}
