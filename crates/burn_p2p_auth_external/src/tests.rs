use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    thread,
    time::Duration as StdDuration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use burn_p2p_core::{AuthProvider, ExperimentScope, NetworkId, PeerRole, PeerRoleSet, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, PrincipalClaims,
    StaticPrincipalRecord,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use url::Url;

use crate::{
    ExternalProxyIdentityConnector, ProviderMappedIdentityConnector,
    shared::{
        PendingLogin, ProviderConnectorState, ProviderSessionMaterial, ProxyConnectorState,
        StandardTokenResponse, StoredProviderSession,
    },
};

fn required_live_oidc_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required live oidc env var {name}"))
}

fn spawn_provider_response_server(
    assert_request: impl Fn(&str) + Send + 'static,
    response_status: &'static str,
    response_body: String,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider response listener");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept provider response");
        stream
            .set_read_timeout(Some(StdDuration::from_secs(2)))
            .expect("set provider response read timeout");
        let mut buffer = [0_u8; 8192];
        let bytes_read = stream
            .read(&mut buffer)
            .expect("read provider response request");
        let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
        assert_request(&request);
        let response = format!(
            "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write provider response");
        stream.flush().expect("flush provider response");
    });
    (format!("http://{addr}"), handle)
}

fn spawn_provider_response_sequence_server(
    assert_request: impl Fn(&str) + Send + Sync + 'static,
    responses: Vec<(&'static str, String)>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider response listener");
    let addr = listener.local_addr().expect("local addr");
    let assert_request = Arc::new(assert_request);
    let handle = thread::spawn(move || {
        for (response_status, response_body) in responses {
            let (mut stream, _) = listener.accept().expect("accept provider response");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(2)))
                .expect("set provider response read timeout");
            let mut buffer = [0_u8; 8192];
            let bytes_read = stream
                .read(&mut buffer)
                .expect("read provider response request");
            let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            assert_request(&request);
            let response = format!(
                "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write provider response");
            stream.flush().expect("flush provider response");
        }
    });
    (format!("http://{addr}"), handle)
}

#[test]
fn external_proxy_connector_uses_external_provider_and_trusted_header() {
    let now = Utc::now();
    let connector = ExternalProxyIdentityConnector::new(
        "corp-proxy",
        "x-auth-principal",
        Duration::minutes(10),
        BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::External {
                        authority: "corp-proxy".into(),
                    },
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::new(),
                    group_memberships: BTreeSet::from(["operators".into()]),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::new(),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]),
    );

    assert_eq!(connector.trusted_principal_header(), "x-auth-principal");
    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("session");
    assert_eq!(
        session.claims.provider,
        AuthProvider::External {
            authority: "corp-proxy".into()
        }
    );
}

#[test]
fn external_proxy_connector_prunes_and_bounds_pending_logins() {
    let now = Utc::now();
    let connector = ExternalProxyIdentityConnector::new(
        "corp-proxy",
        "x-auth-principal",
        Duration::minutes(10),
        BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::External {
                        authority: "corp-proxy".into(),
                    },
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::new(),
                    group_memberships: BTreeSet::new(),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::new(),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]),
    );
    let seeded = ProxyConnectorState {
        pending: (0..300)
            .map(|index| {
                (
                    burn_p2p_core::ContentId::new(format!("login-{index:03}")),
                    PendingLogin {
                        login_id: burn_p2p_core::ContentId::new(format!("login-{index:03}")),
                        state: format!("state-{index:03}"),
                        network_id: NetworkId::new("network-a"),
                        requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        expires_at: if index < 32 {
                            now - Duration::minutes(1)
                        } else {
                            now + Duration::minutes(index as i64)
                        },
                        oidc_nonce: None,
                        pkce_verifier: None,
                    },
                )
            })
            .collect(),
    };
    let bytes = burn_p2p_core::deterministic_cbor(&seeded).expect("encode proxy state");
    connector
        .import_persistent_state(Some(&bytes))
        .expect("import proxy state");

    let exported = connector
        .export_persistent_state()
        .expect("export proxy state")
        .expect("proxy state bytes");
    let persisted: ProxyConnectorState =
        burn_p2p_core::from_cbor_slice(&exported).expect("decode proxy state");

    assert!(persisted.pending.len() <= 256);
    assert!(
        persisted
            .pending
            .values()
            .all(|login| login.expires_at >= now)
    );
}

#[test]
fn provider_mapped_connector_refreshes_and_revokes_remote_sessions() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::from(["burn-core".into()]),
                group_memberships: BTreeSet::from(["contributors".into()]),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([("profile".into(), "static".into())]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-123\""));
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
    let (refresh_url, refresh_server) = spawn_provider_response_server(
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
    let (userinfo_url, userinfo_server) = spawn_provider_response_sequence_server(
        |request| {
            assert!(
                request.contains("\"access_token\":\"access-token-1\"")
                    || request.contains("\"access_token\":\"access-token-2\"")
            );
            assert!(
                request.contains("\"session_handle\":\"session-handle-1\"")
                    || request.contains("\"session_handle\":\"session-handle-2\"")
            );
        },
        vec![
            (
                "200 OK",
                serde_json::json!({
                    "display_name": "Alice GitHub",
                    "org_memberships": ["oss"],
                    "group_memberships": ["maintainers"],
                    "custom_claims": {
                        "avatar_url": "https://avatars.example/alice.png"
                    }
                })
                .to_string(),
            ),
            (
                "200 OK",
                serde_json::json!({
                    "display_name": "Alice Refreshed",
                    "group_memberships": ["operators"],
                    "custom_claims": {
                        "avatar_url": "https://avatars.example/alice-2.png"
                    }
                })
                .to_string(),
            ),
        ],
    );
    let (revoke_url, revoke_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-2\""));
            assert!(request.contains("\"session_handle\":\"session-handle-2\""));
        },
        "200 OK",
        "{}".into(),
    );

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
    .with_userinfo_url(Some(userinfo_url))
    .with_refresh_url(Some(refresh_url))
    .with_revoke_url(Some(revoke_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin provider login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-code-123".into()),
        })
        .expect("complete provider login");
    assert_eq!(session.claims.display_name, "Alice GitHub");
    assert!(session.claims.org_memberships.contains("burn-core"));
    assert!(session.claims.org_memberships.contains("oss"));
    assert!(session.claims.group_memberships.contains("maintainers"));
    assert_eq!(
        session.claims.custom_claims.get("avatar_url"),
        Some(&"https://avatars.example/alice.png".to_owned())
    );

    let refreshed = connector
        .refresh(&session)
        .expect("refresh provider session");
    assert_eq!(refreshed.claims.display_name, "Alice Refreshed");
    assert!(refreshed.claims.group_memberships.contains("operators"));
    assert_eq!(
        refreshed.claims.custom_claims.get("avatar_url"),
        Some(&"https://avatars.example/alice-2.png".to_owned())
    );

    connector
        .revoke(&refreshed)
        .expect("revoke provider session");

    exchange_server.join().expect("join exchange server");
    userinfo_server.join().expect("join userinfo server");
    refresh_server.join().expect("join refresh server");
    revoke_server.join().expect("join revoke server");
}

#[test]
fn provider_mapped_connector_hydrates_claims_via_userinfo_endpoint() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::from(["burn-core".into()]),
                group_memberships: BTreeSet::from(["contributors".into()]),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([("profile".into(), "static".into())]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-code-userinfo\""));
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
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice Profile",
            "org_memberships": ["oss"],
            "group_memberships": ["maintainers"],
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            }
        })
        .to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
    .with_userinfo_url(Some(userinfo_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin provider login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-code-userinfo".into()),
        })
        .expect("complete provider login");
    assert_eq!(session.claims.display_name, "Alice Profile");
    assert!(session.claims.org_memberships.contains("burn-core"));
    assert!(session.claims.org_memberships.contains("oss"));
    assert!(session.claims.group_memberships.contains("maintainers"));
    assert_eq!(
        session.claims.custom_claims.get("avatar_url"),
        Some(&"https://avatars.example/alice.png".to_owned())
    );

    exchange_server.join().expect("join exchange server");
    userinfo_server.join().expect("join userinfo server");
}

#[test]
fn provider_mapped_connector_supports_standard_token_exchange_and_userinfo_mapping() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::from(["burn-core".into()]),
                group_memberships: BTreeSet::from(["contributors".into()]),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([
                    ("provider_login".into(), "alice-gh".into()),
                    ("provider_email".into(), "alice@example.com".into()),
                ]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (token_url, token_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("grant_type=authorization_code"));
            assert!(request.contains("code=github-standard-code"));
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
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer access-token-1"));
        },
        "200 OK",
        serde_json::json!({
            "id": 42,
            "login": "alice-gh",
            "email": "alice@example.com",
            "name": "Alice Upstream",
            "organizations": ["oss"],
            "groups": ["maintainers"],
            "avatar_url": "https://avatars.example/alice-upstream.png"
        })
        .to_string(),
    );
    let (orgs_url, orgs_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer access-token-1"));
        },
        "200 OK",
        serde_json::json!([
            {
                "login": "oss"
            }
        ])
        .to_string(),
    );
    let (teams_url, teams_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer access-token-1"));
        },
        "200 OK",
        serde_json::json!([
            {
                "slug": "maintainers",
                "organization": {
                    "login": "oss"
                }
            }
        ])
        .to_string(),
    );
    let (repo_access_url, repo_access_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer access-token-1"));
        },
        "200 OK",
        serde_json::json!([
            {
                "full_name": "aberration-technology/burn_p2p",
                "permissions": {
                    "admin": true,
                    "push": true,
                    "pull": true
                }
            }
        ])
        .to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.com/login/oauth/authorize".into()),
    )
    .with_token_url(Some(token_url))
    .with_client_credentials(Some("github-client".into()), Some("github-secret".into()))
    .with_userinfo_url(Some(userinfo_url))
    .with_github_orgs_url(Some(orgs_url))
    .with_github_teams_url(Some(teams_url))
    .with_github_repo_access_url(Some(repo_access_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin provider login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-standard-code".into()),
        })
        .expect("complete provider login");

    assert_eq!(session.claims.principal_id.as_str(), "alice");
    assert_eq!(session.claims.display_name, "Alice Upstream");
    assert!(session.claims.org_memberships.contains("oss"));
    assert!(session.claims.group_memberships.contains("oss/maintainers"));
    assert_eq!(
        session.claims.custom_claims.get("provider_login"),
        Some(&"alice-gh".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("provider_email"),
        Some(&"alice@example.com".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("avatar_url"),
        Some(&"https://avatars.example/alice-upstream.png".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("provider_repo_access"),
        Some(&"aberration-technology/burn_p2p,aberration-technology/burn_p2p:admin".to_owned())
    );

    token_server.join().expect("join token server");
    userinfo_server.join().expect("join userinfo server");
    orgs_server.join().expect("join orgs server");
    teams_server.join().expect("join teams server");
    repo_access_server.join().expect("join repo access server");
}

#[test]
fn provider_mapped_connector_surfaces_provider_exchange_outages() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-outage-code\""));
        },
        "503 Service Unavailable",
        serde_json::json!({ "error": "provider outage" }).to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let error = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-outage-code".into()),
        })
        .expect_err("provider exchange should fail");
    assert!(matches!(error, AuthError::ProviderExchange(message) if message.contains("503")));

    exchange_server.join().expect("join exchange server");
}

#[test]
fn provider_mapped_connector_surfaces_partial_refresh_failures() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-refresh-code\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (refresh_url, refresh_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"refresh_token\":\"refresh-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "503 Service Unavailable",
        serde_json::json!({ "error": "refresh unavailable" }).to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice GitHub",
            "custom_claims": {
                "avatar_url": "https://avatars.example/alice.png"
            }
        })
        .to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
    .with_userinfo_url(Some(userinfo_url))
    .with_refresh_url(Some(refresh_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-refresh-code".into()),
        })
        .expect("complete login");
    let error = connector
        .refresh(&session)
        .expect_err("refresh should surface provider failure");
    assert!(matches!(error, AuthError::ProviderRefresh(message) if message.contains("503")));

    exchange_server.join().expect("join exchange server");
    userinfo_server.join().expect("join userinfo server");
    refresh_server.join().expect("join refresh server");
}

#[test]
fn provider_mapped_connector_recovers_from_stale_sessions_via_reauthentication() {
    let now = Utc::now();
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::milliseconds(1),
        BTreeMap::from([(
            PrincipalId::new("alice"),
            StaticPrincipalRecord {
                claims: PrincipalClaims {
                    principal_id: PrincipalId::new("alice"),
                    provider: AuthProvider::GitHub,
                    display_name: "Alice".into(),
                    org_memberships: BTreeSet::new(),
                    group_memberships: BTreeSet::new(),
                    granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    custom_claims: BTreeMap::new(),
                    issued_at: now,
                    expires_at: now + Duration::hours(1),
                },
                allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
            },
        )]),
        Some("https://github.example/login/oauth/authorize".into()),
    );

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("complete login");

    thread::sleep(StdDuration::from_millis(5));
    let error = connector
        .refresh(&session)
        .expect_err("stale session should require reauthentication");
    assert!(matches!(error, AuthError::SessionExpired(_)));

    let relogin = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin reauthentication login");
    let recovered = connector
        .complete_login(CallbackPayload {
            login_id: relogin.login_id,
            state: relogin.state,
            principal_id: Some(PrincipalId::new("alice")),
            provider_code: None,
        })
        .expect("recover with a fresh login");
    assert_eq!(recovered.claims.principal_id.as_str(), "alice");
    assert!(recovered.expires_at > recovered.issued_at);
}

#[test]
fn provider_persistent_state_redacts_remote_tokens_by_default() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-redacted-code\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice GitHub"
        })
        .to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
    .with_userinfo_url(Some(userinfo_url));
    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-redacted-code".into()),
        })
        .expect("complete login");
    let exported = connector
        .export_persistent_state()
        .expect("export state")
        .expect("state bytes");
    let persisted: ProviderConnectorState =
        burn_p2p_core::from_cbor_slice(&exported).expect("decode provider state");
    let session_state = persisted
        .provider_sessions
        .get(&session.session_id)
        .expect("persisted provider session");

    assert_eq!(
        session_state.material.provider_subject.as_deref(),
        Some("github-user-42")
    );
    assert!(session_state.material.access_token.is_none());
    assert!(session_state.material.refresh_token.is_none());
    assert!(session_state.material.session_handle.is_none());

    exchange_server.join().expect("join exchange server");
    userinfo_server.join().expect("join userinfo server");
}

#[test]
fn provider_persistent_state_can_opt_in_to_remote_token_persistence() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (exchange_url, exchange_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"provider_code\":\"github-persisted-code\""));
        },
        "200 OK",
        serde_json::json!({
            "principal_id": "alice",
            "provider_subject": "github-user-42",
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "session_handle": "session-handle-1"
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("\"access_token\":\"access-token-1\""));
            assert!(request.contains("\"session_handle\":\"session-handle-1\""));
        },
        "200 OK",
        serde_json::json!({
            "display_name": "Alice GitHub"
        })
        .to_string(),
    );
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
    .with_userinfo_url(Some(userinfo_url))
    .with_persist_remote_tokens(true);
    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: Some("alice".into()),
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("github-persisted-code".into()),
        })
        .expect("complete login");
    let exported = connector
        .export_persistent_state()
        .expect("export state")
        .expect("state bytes");
    let persisted: ProviderConnectorState =
        burn_p2p_core::from_cbor_slice(&exported).expect("decode provider state");
    let session_state = persisted
        .provider_sessions
        .get(&session.session_id)
        .expect("persisted provider session");

    assert_eq!(
        session_state.material.access_token.as_deref(),
        Some("access-token-1")
    );
    assert_eq!(
        session_state.material.refresh_token.as_deref(),
        Some("refresh-token-1")
    );
    assert_eq!(
        session_state.material.session_handle.as_deref(),
        Some("session-handle-1")
    );

    exchange_server.join().expect("join exchange server");
    userinfo_server.join().expect("join userinfo server");
}

#[test]
fn provider_connector_prunes_expired_and_bounds_persisted_sessions() {
    let now = Utc::now();
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        BTreeMap::new(),
        Some("https://github.example/login/oauth/authorize".into()),
    );
    let seeded = ProviderConnectorState {
        pending: (0..300)
            .map(|index| {
                (
                    burn_p2p_core::ContentId::new(format!("login-{index:03}")),
                    PendingLogin {
                        login_id: burn_p2p_core::ContentId::new(format!("login-{index:03}")),
                        state: format!("state-{index:03}"),
                        network_id: NetworkId::new("network-a"),
                        requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        expires_at: if index < 24 {
                            now - Duration::minutes(5)
                        } else {
                            now + Duration::minutes(index as i64)
                        },
                        oidc_nonce: None,
                        pkce_verifier: None,
                    },
                )
            })
            .collect(),
        provider_sessions: (0..300)
            .map(|index| {
                (
                    burn_p2p_core::ContentId::new(format!("session-{index:03}")),
                    StoredProviderSession {
                        material: ProviderSessionMaterial {
                            provider_subject: Some(format!("provider-user-{index:03}")),
                            access_token: Some(format!("access-{index:03}")),
                            refresh_token: None,
                            session_handle: None,
                            provider_expires_at: None,
                            profile: Default::default(),
                        },
                        local_expires_at: if index < 24 {
                            now - Duration::minutes(5)
                        } else {
                            now + Duration::minutes(index as i64)
                        },
                    },
                )
            })
            .collect(),
    };
    let bytes = burn_p2p_core::deterministic_cbor(&seeded).expect("encode provider state");
    connector
        .import_persistent_state(Some(&bytes))
        .expect("import provider state");

    let exported = connector
        .export_persistent_state()
        .expect("export provider state")
        .expect("provider state bytes");
    let persisted: ProviderConnectorState =
        burn_p2p_core::from_cbor_slice(&exported).expect("decode provider state");

    assert!(persisted.pending.len() <= 256);
    assert!(
        persisted
            .pending
            .values()
            .all(|login| login.expires_at >= now)
    );
    assert!(persisted.provider_sessions.len() <= 256);
    assert!(
        persisted
            .provider_sessions
            .values()
            .all(|session| session.local_expires_at >= now)
    );
}

#[test]
fn oidc_connector_validates_id_token_and_claim_driven_mapping() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::Oidc {
                    issuer: "https://issuer.example".into(),
                },
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([
                    ("provider_groups".into(), "trainers,ml-admins".into()),
                    ("provider_claim:department".into(), "research".into()),
                ]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let secret = b"oidc-shared-secret";
    let kid = "oidc-test-key";
    let (jwks_url, jwks_server) = spawn_provider_response_server(
        |_| {},
        "200 OK",
        serde_json::json!({
            "keys": [{
                "kty": "oct",
                "alg": "HS256",
                "use": "sig",
                "kid": kid,
                "k": URL_SAFE_NO_PAD.encode(secret),
            }]
        })
        .to_string(),
    );

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::Oidc {
            issuer: "https://issuer.example".into(),
        },
        Duration::minutes(10),
        principals,
        Some("https://issuer.example/authorize".into()),
    )
    .with_token_url(Some("http://placeholder.invalid/token".into()))
    .with_client_credentials(Some("oidc-client".into()), None)
    .with_redirect_uri(Some("https://edge.example/callback/oidc".into()))
    .with_jwks_url(Some(jwks_url));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin oidc login");
    let authorize_url = Url::parse(login.authorize_url.as_deref().expect("authorize url"))
        .expect("parse authorize url");
    let authorize_pairs = authorize_url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<BTreeMap<_, _>>();
    let nonce = authorize_pairs
        .get("nonce")
        .cloned()
        .expect("oidc nonce query param");
    assert_eq!(
        authorize_pairs.get("response_type"),
        Some(&"code".to_owned())
    );
    assert_eq!(
        authorize_pairs.get("client_id"),
        Some(&"oidc-client".to_owned())
    );
    assert_eq!(
        authorize_pairs.get("redirect_uri"),
        Some(&"https://edge.example/callback/oidc".to_owned())
    );
    assert_eq!(
        authorize_pairs.get("scope"),
        Some(&"openid profile email".to_owned())
    );
    assert_eq!(authorize_pairs.get("state"), Some(&login.state));
    assert!(authorize_pairs.contains_key("code_challenge"));
    assert_eq!(
        authorize_pairs.get("code_challenge_method"),
        Some(&"S256".to_owned())
    );

    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.into());
    let id_token = jsonwebtoken::encode(
        &header,
        &serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "oidc-user-42",
            "aud": "oidc-client",
            "exp": (Utc::now() + Duration::minutes(5)).timestamp(),
            "nonce": nonce,
            "name": "Alice OIDC",
            "preferred_username": "alice.oidc",
            "email": "alice@example.com",
            "groups": ["trainers", "ml-admins"],
            "department": "research",
        }),
        &EncodingKey::from_secret(secret),
    )
    .expect("encode id token");
    let (token_url, token_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("grant_type=authorization_code"));
            assert!(request.contains("code=oidc-code"));
            assert!(request.contains("client_id=oidc-client"));
            assert!(request.contains("redirect_uri=https%3A%2F%2Fedge.example%2Fcallback%2Foidc"));
            assert!(request.contains("code_verifier="));
        },
        "200 OK",
        serde_json::json!({
            "id_token": id_token,
            "expires_in": 3600,
        })
        .to_string(),
    );

    let connector = connector.with_token_url(Some(token_url));
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("oidc-code".into()),
        })
        .expect("complete oidc login");

    assert_eq!(session.claims.principal_id.as_str(), "alice");
    assert_eq!(session.claims.display_name, "Alice OIDC");
    assert!(session.claims.group_memberships.contains("trainers"));
    assert!(session.claims.group_memberships.contains("ml-admins"));
    assert_eq!(
        session.claims.custom_claims.get("provider_login"),
        Some(&"alice.oidc".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("provider_email"),
        Some(&"alice@example.com".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("department"),
        Some(&"research".to_owned())
    );

    token_server.join().expect("join token server");
    jwks_server.join().expect("join jwks server");
}

#[test]
fn oidc_connector_discovers_standard_endpoints_for_token_and_userinfo() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::Oidc {
                    issuer: "http://placeholder.invalid".into(),
                },
                display_name: "Alice".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([(
                    "provider_email".into(),
                    "alice@example.com".into(),
                )]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (token_url, token_server) = spawn_provider_response_server(
        |request| {
            assert!(request.contains("grant_type=authorization_code"));
            assert!(request.contains("code=oidc-discovery-code"));
            assert!(request.contains("client_id=oidc-client"));
            assert!(request.contains("code_verifier="));
        },
        "200 OK",
        serde_json::json!({
            "access_token": "oidc-access-token",
            "expires_in": 3600,
        })
        .to_string(),
    );
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer oidc-access-token"));
        },
        "200 OK",
        serde_json::json!({
            "sub": "corp-user-17",
            "name": "Alice Corp",
            "preferred_username": "alice.corp",
            "email": "alice@example.com",
            "organizations": ["research"],
            "groups": ["trainers"],
        })
        .to_string(),
    );
    let (discovery_base, discovery_server) = spawn_provider_response_server(
        |request| {
            assert!(request.starts_with("GET /.well-known/openid-configuration "));
        },
        "200 OK",
        serde_json::json!({
            "token_endpoint": token_url,
            "userinfo_endpoint": userinfo_url,
        })
        .to_string(),
    );

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::Oidc {
            issuer: discovery_base.clone(),
        },
        Duration::minutes(10),
        principals,
        Some("https://issuer.example/authorize".into()),
    )
    .with_client_credentials(Some("oidc-client".into()), None)
    .with_redirect_uri(Some("https://edge.example/callback/oidc".into()));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin oidc discovery login");
    let session = connector
        .complete_login(CallbackPayload {
            login_id: login.login_id,
            state: login.state,
            principal_id: None,
            provider_code: Some("oidc-discovery-code".into()),
        })
        .expect("complete oidc discovery login");

    assert_eq!(session.claims.principal_id.as_str(), "alice");
    assert_eq!(session.claims.display_name, "Alice Corp");
    assert!(session.claims.org_memberships.contains("research"));
    assert!(session.claims.group_memberships.contains("trainers"));
    assert_eq!(
        session.claims.custom_claims.get("provider_login"),
        Some(&"alice.corp".to_owned())
    );

    discovery_server.join().expect("join discovery server");
    token_server.join().expect("join token server");
    userinfo_server.join().expect("join userinfo server");
}

#[test]
fn provider_mapped_connector_hydrates_live_github_policy_claims() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice Community".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Validator]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([
                    ("provider_login".into(), "alice-gh".into()),
                    ("provider_orgs".into(), "burn-community".into()),
                    (
                        "provider_groups".into(),
                        "burn-community/maintainers".into(),
                    ),
                    (
                        "provider_repo_access".into(),
                        "aberration-technology/burn_p2p:admin".into(),
                    ),
                ]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (userinfo_url, userinfo_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        "200 OK",
        serde_json::json!({
            "id": 42,
            "login": "alice-gh",
            "email": "alice@example.com",
            "name": "Alice GitHub",
            "avatar_url": "https://avatars.example/alice.png"
        })
        .to_string(),
    );
    let (orgs_url, orgs_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        "200 OK",
        serde_json::json!([
            { "login": "burn-community" },
            { "login": "oss-friends" }
        ])
        .to_string(),
    );
    let (teams_url, teams_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        "200 OK",
        serde_json::json!([
            {
                "organization": { "login": "burn-community" },
                "slug": "maintainers"
            }
        ])
        .to_string(),
    );
    let (repos_url, repos_server) = spawn_provider_response_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        "200 OK",
        serde_json::json!([
            {
                "full_name": "aberration-technology/burn_p2p",
                "permissions": { "admin": true, "push": true, "pull": true }
            },
            {
                "full_name": "aberration-technology/website",
                "permissions": { "admin": false, "push": false, "pull": true }
            }
        ])
        .to_string(),
    );

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.com/login/oauth/authorize".into()),
    )
    .with_token_url(Some("https://github.com/login/oauth/access_token".into()))
    .with_userinfo_url(Some(format!("{userinfo_url}/user")))
    .with_github_orgs_url(Some(format!("{orgs_url}/user/orgs?per_page=100")))
    .with_github_teams_url(Some(format!("{teams_url}/user/teams?per_page=100")))
    .with_github_repo_access_url(Some(format!(
        "{repos_url}/user/repos?per_page=100&affiliation=owner,collaborator,organization_member"
    )));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin github login");
    let session = connector
        .complete_login_with_standard_token_response_for_test(
            login.login_id,
            login.state,
            StandardTokenResponse {
                access_token: Some("github-access-1".into()),
                refresh_token: Some("github-refresh-1".into()),
                token_type: Some("bearer".into()),
                expires_in: Some(3600),
                scope: None,
                id_token: None,
            },
        )
        .expect("complete github login");

    assert_eq!(session.claims.principal_id.as_str(), "alice");
    assert_eq!(session.claims.display_name, "Alice GitHub");
    assert!(session.claims.org_memberships.contains("burn-community"));
    assert!(
        session
            .claims
            .group_memberships
            .contains("burn-community/maintainers")
    );
    assert_eq!(
        session.claims.custom_claims.get("provider_login"),
        Some(&"alice-gh".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("provider_email"),
        Some(&"alice@example.com".to_owned())
    );
    assert_eq!(
        session.claims.custom_claims.get("auth_policy_source"),
        Some(&"github-live".to_owned())
    );
    assert_eq!(
        session
            .claims
            .custom_claims
            .get("auth_policy_match:provider_groups"),
        Some(&"burn-community/maintainers".to_owned())
    );
    let repo_access = session
        .claims
        .custom_claims
        .get("provider_repo_access")
        .expect("provider repo access");
    assert!(
        repo_access
            .split(',')
            .any(|entry| entry == "aberration-technology/burn_p2p:admin")
    );

    userinfo_server.join().expect("join github userinfo server");
    orgs_server.join().expect("join github orgs server");
    teams_server.join().expect("join github teams server");
    repos_server.join().expect("join github repos server");
}

#[test]
fn provider_mapped_connector_fetch_claims_rejects_revoked_github_membership() {
    let now = Utc::now();
    let principals = BTreeMap::from([(
        PrincipalId::new("alice"),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("alice"),
                provider: AuthProvider::GitHub,
                display_name: "Alice Community".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::from([
                    ("provider_login".into(), "alice-gh".into()),
                    ("provider_orgs".into(), "burn-community".into()),
                ]),
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new("network-a")]),
        },
    )]);
    let (userinfo_url, userinfo_server) = spawn_provider_response_sequence_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        vec![
            (
                "200 OK",
                serde_json::json!({
                    "id": 42,
                    "login": "alice-gh",
                    "email": "alice@example.com",
                    "name": "Alice GitHub"
                })
                .to_string(),
            ),
            (
                "200 OK",
                serde_json::json!({
                    "id": 42,
                    "login": "alice-gh",
                    "email": "alice@example.com",
                    "name": "Alice GitHub"
                })
                .to_string(),
            ),
        ],
    );
    let (orgs_url, orgs_server) = spawn_provider_response_sequence_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        vec![
            (
                "200 OK",
                serde_json::json!([{ "login": "burn-community" }]).to_string(),
            ),
            ("200 OK", serde_json::json!([]).to_string()),
        ],
    );
    let (teams_url, teams_server) = spawn_provider_response_sequence_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        vec![
            ("200 OK", serde_json::json!([]).to_string()),
            ("200 OK", serde_json::json!([]).to_string()),
        ],
    );
    let (repos_url, repos_server) = spawn_provider_response_sequence_server(
        |request| {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer github-access-1"));
        },
        vec![
            ("200 OK", serde_json::json!([]).to_string()),
            ("200 OK", serde_json::json!([]).to_string()),
        ],
    );

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.com/login/oauth/authorize".into()),
    )
    .with_token_url(Some("https://github.com/login/oauth/access_token".into()))
    .with_userinfo_url(Some(format!("{userinfo_url}/user")))
    .with_github_orgs_url(Some(format!("{orgs_url}/user/orgs?per_page=100")))
    .with_github_teams_url(Some(format!("{teams_url}/user/teams?per_page=100")))
    .with_github_repo_access_url(Some(format!(
        "{repos_url}/user/repos?per_page=100&affiliation=owner,collaborator,organization_member"
    )));

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new("network-a"),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin github login");
    let session = connector
        .complete_login_with_standard_token_response_for_test(
            login.login_id,
            login.state,
            StandardTokenResponse {
                access_token: Some("github-access-1".into()),
                refresh_token: Some("github-refresh-1".into()),
                token_type: Some("bearer".into()),
                expires_in: Some(3600),
                scope: None,
                id_token: None,
            },
        )
        .expect("complete github login");

    let error = connector
        .fetch_claims(&session)
        .expect_err("revoked github org membership should be rejected");
    assert!(matches!(
        error,
        AuthError::ProviderUserInfo(message)
            if message.contains("no longer matches the enrolled principal")
    ));

    userinfo_server.join().expect("join github userinfo server");
    orgs_server.join().expect("join github orgs server");
    teams_server.join().expect("join github teams server");
    repos_server.join().expect("join github repos server");
}

#[test]
#[ignore = "requires live oidc env vars and tokens"]
fn live_oidc_connector_validates_real_provider_identity_from_env() {
    let issuer = required_live_oidc_env("BURN_P2P_REAL_OIDC_ISSUER");
    let client_id = required_live_oidc_env("BURN_P2P_REAL_OIDC_CLIENT_ID");
    let id_token = required_live_oidc_env("BURN_P2P_REAL_OIDC_ID_TOKEN");
    let expected_subject = required_live_oidc_env("BURN_P2P_REAL_OIDC_EXPECTED_SUBJECT");
    let redirect_uri = env::var("BURN_P2P_REAL_OIDC_REDIRECT_URI")
        .unwrap_or_else(|_| "https://edge.example/callback/oidc".into());
    let network_id =
        env::var("BURN_P2P_REAL_OIDC_NETWORK_ID").unwrap_or_else(|_| "network-a".into());
    let principal_id =
        env::var("BURN_P2P_REAL_OIDC_PRINCIPAL_ID").unwrap_or_else(|_| "oidc-live".into());
    let access_token = env::var("BURN_P2P_REAL_OIDC_ACCESS_TOKEN").ok();
    let expected_email = env::var("BURN_P2P_REAL_OIDC_EXPECTED_EMAIL").ok();
    let expected_display_name = env::var("BURN_P2P_REAL_OIDC_EXPECTED_DISPLAY_NAME").ok();
    let jwks_url = env::var("BURN_P2P_REAL_OIDC_JWKS_URL").ok();
    let userinfo_url = env::var("BURN_P2P_REAL_OIDC_USERINFO_URL").ok();

    let now = Utc::now();
    let mut custom_claims = BTreeMap::from([("provider_subject".into(), expected_subject)]);
    if let Some(expected_email) = expected_email.clone() {
        custom_claims.insert("provider_email".into(), expected_email);
    }
    let principals = BTreeMap::from([(
        PrincipalId::new(principal_id.clone()),
        StaticPrincipalRecord {
            claims: PrincipalClaims {
                principal_id: PrincipalId::new(principal_id.clone()),
                provider: AuthProvider::Oidc {
                    issuer: issuer.clone(),
                },
                display_name: expected_display_name
                    .clone()
                    .unwrap_or_else(|| "live oidc".into()),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims,
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
            allowed_networks: BTreeSet::from([NetworkId::new(network_id.clone())]),
        },
    )]);

    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::Oidc {
            issuer: issuer.clone(),
        },
        Duration::minutes(10),
        principals,
        None,
    )
    .with_client_credentials(Some(client_id), None)
    .with_redirect_uri(Some(redirect_uri))
    .with_jwks_url(jwks_url)
    .with_userinfo_url(userinfo_url);

    let login = connector
        .begin_login(LoginRequest {
            network_id: NetworkId::new(network_id),
            principal_hint: None,
            requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
        })
        .expect("begin live oidc login");
    let session = connector
        .complete_login_with_standard_token_response_for_test(
            login.login_id,
            login.state,
            StandardTokenResponse {
                access_token,
                refresh_token: None,
                token_type: None,
                expires_in: Some(3600),
                scope: None,
                id_token: Some(id_token),
            },
        )
        .expect("complete live oidc login from provided token material");

    assert_eq!(session.claims.principal_id.as_str(), principal_id);
    if let Some(expected_email) = expected_email {
        assert_eq!(
            session.claims.custom_claims.get("provider_email"),
            Some(&expected_email)
        );
    }
    if let Some(expected_display_name) = expected_display_name {
        assert_eq!(session.claims.display_name, expected_display_name);
    }
}
