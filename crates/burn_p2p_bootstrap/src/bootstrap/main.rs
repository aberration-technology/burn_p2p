//! Bootstrap, edge, and operator-facing services for burn_p2p deployments.
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use burn_p2p::{
    ActiveServiceSet, AdminMode, AppMode, ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind,
    AssignmentLease, BrowserMode, CachedMicroShard, CapabilityEstimate, ChunkingScheme,
    CompiledFeatureSet, ConfiguredServiceSet, ControlAnnouncement, ControlHandle, DatasetConfig,
    DatasetRegistration, DatasetSizing, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
    EvalSplit, ExperimentDirectory, ExperimentDirectoryAnnouncement, ExperimentDirectoryEntry,
    FsArtifactStore, IdentityConnector, LoginRequest, MetricReport, MetricValue, MetricsMode,
    NodeCertificateAuthority, NodeConfig, NodeEnrollmentRequest, OverlayTopic, P2pWorkload,
    PatchOutcome, PatchSupport, PrincipalClaims, PrincipalId, PrincipalSession, ProfileMode,
    RuntimePatch, ShardFetchManifest, SocialMode, StaticIdentityConnector, StaticPrincipalRecord,
    TrainError, TrustedIssuer, WindowCtx, WindowReport,
};
#[cfg(feature = "auth-external")]
use burn_p2p_auth_external::ExternalProxyIdentityConnector;
#[cfg(feature = "auth-github")]
use burn_p2p_auth_github::GitHubIdentityConnector;
#[cfg(feature = "auth-oauth")]
use burn_p2p_auth_oauth::OAuthIdentityConnector;
#[cfg(feature = "auth-oidc")]
use burn_p2p_auth_oidc::OidcIdentityConnector;
use burn_p2p_bootstrap::{
    AdminAction, AdminCapability, AuthPolicyRollout, BootstrapAdminState, BootstrapEmbeddedDaemon,
    BootstrapEmbeddedDaemonConfig, BootstrapPeerDaemon, BootstrapPeerDaemonConfig, BootstrapPlan,
    BootstrapService, BootstrapSpec, BrowserDirectorySnapshot, BrowserEdgeMode,
    BrowserEdgeSnapshotConfig, BrowserLoginProvider, BrowserReceiptSubmissionResponse,
    BrowserTransportSurface, ReceiptQuery, ReenrollmentStatus, TrustBundleExport,
    TrustedIssuerStatus, render_dashboard_html, render_openmetrics,
};
#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
use burn_p2p_bootstrap::{
    render_artifact_run_summaries_html, render_artifact_run_view_html,
    render_head_artifact_view_html,
};
use burn_p2p_core::{
    AuthProvider, ContentId, ExperimentScope, NetworkId, PeerId, PeerRoleSet, RevocationEpoch,
    SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::{DownloadTicketRequest, ExportRequest};
use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use serde::{Deserialize, Serialize};

mod auth_connectors;
mod auth_state;
mod browser_surface;
mod capability_manifest;
mod daemon_types;
mod key_material;
mod routes;
mod synthetic_dataset;

use auth_connectors::*;
use auth_state::*;
use browser_surface::*;
use capability_manifest::*;
use daemon_types::*;
use key_material::*;
use routes::*;
use synthetic_dataset::*;

fn expand_env_placeholders(input: &str) -> Result<String, String> {
    let mut expanded = String::new();
    let mut remaining = input;

    while let Some(start) = remaining.find("${") {
        expanded.push_str(&remaining[..start]);
        let placeholder = &remaining[start + 2..];
        let end = placeholder.find('}').ok_or_else(|| {
            format!("unterminated environment placeholder in bootstrap config string `{input}`")
        })?;
        let expression = &placeholder[..end];
        let (name, default) = expression
            .split_once(":-")
            .map(|(name, default)| (name.trim(), Some(default)))
            .unwrap_or((expression.trim(), None));
        if name.is_empty() {
            return Err(format!(
                "empty environment placeholder in bootstrap config string `{input}`"
            ));
        }
        let value = std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| default.map(str::to_owned))
            .ok_or_else(|| {
                format!("missing required environment variable `{name}` in bootstrap config")
            })?;
        expanded.push_str(&value);
        remaining = &placeholder[end + 1..];
    }

    expanded.push_str(remaining);
    Ok(expanded)
}

fn resolve_config_env_placeholders(value: &mut serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::String(string) => {
            *string = expand_env_placeholders(string)?;
        }
        serde_json::Value::Array(values) => {
            for value in values {
                resolve_config_env_placeholders(value)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                resolve_config_env_placeholders(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn load_bootstrap_daemon_config(
    path: &Path,
) -> Result<BootstrapDaemonConfig, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    resolve_config_env_placeholders(&mut value).map_err(|error| std::io::Error::other(error))?;
    Ok(serde_json::from_value(value)?)
}

fn resolve_operator_state_backend(
    config: &BootstrapDaemonConfig,
    network_id: &burn_p2p_core::NetworkId,
) -> Option<(String, String)> {
    if let Some(operator_state_backend) = config.operator_state_backend.as_ref() {
        return match operator_state_backend {
            BootstrapOperatorStateBackendConfig::Redis { url, key_prefix } => Some((
                url.clone(),
                format!("{key_prefix}:{}:snapshot", network_id.as_str()),
            )),
        };
    }

    let url = std::env::var("BURN_P2P_OPERATOR_STATE_REDIS_URL").ok()?;
    let key_prefix = std::env::var("BURN_P2P_OPERATOR_STATE_KEY_PREFIX")
        .unwrap_or_else(|_| "burn-p2p:operator-state".into());
    Some((
        url,
        format!("{key_prefix}:{}:snapshot", network_id.as_str()),
    ))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: burn-p2p-bootstrap <config.json>")?;
    let config = load_bootstrap_daemon_config(&config_path)?;
    validate_compiled_feature_support(&config)?;
    let shared_config = Arc::new(Mutex::new(config.clone()));
    let shared_config_path = Arc::new(config_path);
    let plan = Arc::new(config.spec.clone().plan()?);
    let state = Arc::new(Mutex::new(BootstrapAdminState::default()));
    let auth_state = config
        .auth
        .as_ref()
        .map(|auth| {
            build_auth_portal(
                auth,
                plan.network_id().clone(),
                plan.genesis.protocol_version.clone(),
            )
        })
        .transpose()?
        .map(Arc::new);
    let bootstrap_peer = if let Some(peer) = config.bootstrap_peer.clone().or_else(|| {
        if config.embedded_runtime.is_none()
            && plan.supports_service(&BootstrapService::CoherenceSeed)
        {
            Some(BootstrapPeerDaemonConfig::default())
        } else {
            None
        }
    }) {
        Some(plan.spawn_bootstrap_peer_daemon(peer)?)
    } else {
        None
    };
    let embedded_runtime = if let Some(runtime) = config.embedded_runtime.clone() {
        let dataset_root = runtime
            .node
            .storage
            .as_ref()
            .map(|storage| storage.root.join("synthetic-dataset"))
            .unwrap_or_else(|| PathBuf::from(".burn_p2p-bootstrap-dataset"));
        ensure_synthetic_dataset(&dataset_root)?;
        let runtime = BootstrapEmbeddedDaemonConfig {
            node: NodeConfig {
                dataset: Some(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
                    root: dataset_root.display().to_string(),
                })),
                ..runtime.node
            },
            ..runtime
        };
        let daemon = plan.spawn_embedded_daemon(
            SyntheticBootstrapProject {
                dataset_root,
                learning_rate: 0.1,
                target_model: 1.0,
            },
            runtime,
        )?;
        let daemon_state = daemon.admin_state();
        {
            let mut state_lock = state
                .lock()
                .expect("bootstrap daemon state should not be poisoned");
            *state_lock = daemon_state
                .lock()
                .expect("embedded daemon state should not be poisoned")
                .clone();
        }
        Some(daemon)
    } else {
        None
    };
    let state = if let Some(daemon) = embedded_runtime.as_ref() {
        daemon.admin_state()
    } else if let Some(daemon) = bootstrap_peer.as_ref() {
        daemon.admin_state()
    } else {
        state
    };
    {
        let mut state_lock = state
            .lock()
            .expect("bootstrap daemon state should not be poisoned");
        if let Some((url, snapshot_key)) =
            resolve_operator_state_backend(&config, &config.spec.genesis.network_id)
        {
            state_lock.configure_operator_state_redis_snapshot(url, snapshot_key);
        }
        state_lock.publication_targets = config
            .artifact_publication
            .as_ref()
            .map(|publication| publication.targets.clone())
            .unwrap_or_default();
        #[cfg(feature = "artifact-publish")]
        if let Err(error) = state_lock.refresh_publication_views() {
            state_lock.last_error = Some(format!("artifact publication sync failed: {error}"));
        }
    }
    if let Some(auth) = auth_state.as_ref() {
        sync_trust_bundle(auth, &state);
    }
    let control_handle = embedded_runtime
        .as_ref()
        .map(BootstrapEmbeddedDaemon::control_handle)
        .or_else(|| {
            bootstrap_peer
                .as_ref()
                .map(BootstrapPeerDaemon::control_handle)
        });
    let bind_addr = config
        .http_bind_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:8787".to_owned());
    let listener = TcpListener::bind(&bind_addr)?;

    eprintln!(
        "burn-p2p-bootstrap listening on http://{} for network {}",
        bind_addr,
        plan.network_id()
    );

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("accept error: {error}");
                continue;
            }
        };

        let context = HttpServerContext {
            plan: Arc::clone(&plan),
            state: Arc::clone(&state),
            config: Arc::clone(&shared_config),
            config_path: Arc::clone(&shared_config_path),
            admin_token: config.admin_token.clone(),
            allow_dev_admin_token: config.allow_dev_admin_token,
            remaining_work_units: config.remaining_work_units,
            auth_state: auth_state.clone(),
            control_handle: control_handle.clone(),
            admin_signer_peer_id: config
                .admin_signer_peer_id
                .clone()
                .unwrap_or_else(|| PeerId::new("bootstrap-authority")),
        };

        thread::spawn(move || {
            if let Err(error) = handle_connection(stream, context) {
                eprintln!("connection error: {error}");
            }
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests;
