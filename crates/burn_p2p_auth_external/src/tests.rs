use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration as StdDuration,
};

use burn_p2p_core::{AuthProvider, ExperimentScope, NetworkId, PeerRole, PeerRoleSet, PrincipalId};
use burn_p2p_security::{
    AuthError, CallbackPayload, IdentityConnector, LoginRequest, PrincipalClaims,
    StaticPrincipalRecord,
};
use chrono::{Duration, Utc};

use crate::{ExternalProxyIdentityConnector, ProviderMappedIdentityConnector};

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
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.com/login/oauth/authorize".into()),
    )
    .with_token_url(Some(token_url))
    .with_client_credentials(Some("github-client".into()), Some("github-secret".into()))
    .with_userinfo_url(Some(userinfo_url));

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
    assert!(session.claims.group_memberships.contains("maintainers"));
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

    token_server.join().expect("join token server");
    userinfo_server.join().expect("join userinfo server");
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
    let connector = ProviderMappedIdentityConnector::new(
        AuthProvider::GitHub,
        Duration::minutes(10),
        principals,
        Some("https://github.example/login/oauth/authorize".into()),
    )
    .with_exchange_url(Some(exchange_url))
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
