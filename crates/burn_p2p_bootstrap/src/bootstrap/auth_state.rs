use super::*;
use fs2::FileExt;
use redis::Commands;
use std::io::Error as IoError;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration as StdDuration, Instant};

const REDIS_SESSION_LOCK_TTL_MS: u64 = 120_000;
const REDIS_SESSION_LOCK_WAIT_MS: u64 = 5_000;
const REDIS_SESSION_LOCK_RETRY_MS: u64 = 25;

fn default_session_state_path(authority_key_path: &std::path::Path) -> PathBuf {
    let file_name = authority_key_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("bootstrap-auth");
    authority_key_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(format!("{file_name}.auth-state.cbor"))
}

pub(super) fn auth_session_state_store(
    config: &BootstrapAuthConfig,
    network_id: &NetworkId,
) -> AuthSessionStateStore {
    match config.session_state_backend.as_ref() {
        Some(BootstrapAuthSessionBackendConfig::File { path }) => AuthSessionStateStore::File {
            lock_path: path.with_extension("lock"),
            state_path: path.clone(),
        },
        Some(BootstrapAuthSessionBackendConfig::Redis { url, key_prefix }) => {
            let base = format!(
                "{}:{}:{}",
                key_prefix,
                network_id.as_str(),
                config.authority_name.replace(':', "_"),
            );
            AuthSessionStateStore::Redis {
                url: url.clone(),
                state_key: format!("{base}:state"),
                lock_key: format!("{base}:lock"),
            }
        }
        None => {
            let state_path = config
                .session_state_path
                .clone()
                .unwrap_or_else(|| default_session_state_path(&config.authority_key_path));
            AuthSessionStateStore::File {
                lock_path: state_path.with_extension("lock"),
                state_path,
            }
        }
    }
}

fn load_persisted_auth_state_from_bytes(
    bytes: Option<Vec<u8>>,
) -> Result<Option<PersistedAuthPortalState>, Box<dyn std::error::Error>> {
    bytes
        .map(|bytes| burn_p2p_core::from_cbor_slice::<PersistedAuthPortalState>(&bytes))
        .transpose()
        .map_err(Into::into)
}

fn load_persisted_auth_state_from_file(
    state_path: &std::path::Path,
    lock_path: &std::path::Path,
) -> Result<Option<PersistedAuthPortalState>, Box<dyn std::error::Error>> {
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock_file.lock_exclusive()?;
    let state = if state_path.exists() {
        load_persisted_auth_state_from_bytes(Some(std::fs::read(state_path)?))?
    } else {
        None
    };
    fs2::FileExt::unlock(&lock_file)?;
    Ok(state)
}

fn redis_connection(url: &str) -> Result<redis::Connection, Box<dyn std::error::Error>> {
    Ok(redis::Client::open(url)?.get_connection()?)
}

fn load_persisted_auth_state_from_redis(
    url: &str,
    state_key: &str,
) -> Result<Option<PersistedAuthPortalState>, Box<dyn std::error::Error>> {
    let mut connection = redis_connection(url)?;
    let bytes = connection.get::<_, Option<Vec<u8>>>(state_key)?;
    load_persisted_auth_state_from_bytes(bytes)
}

fn acquire_redis_session_lock(
    url: &str,
    lock_key: &str,
) -> Result<(redis::Connection, String), Box<dyn std::error::Error>> {
    let mut connection = redis_connection(url)?;
    let lock_token = burn_p2p_security::random_login_state_token("auth session redis lock")
        .map_err(Box::<dyn std::error::Error>::from)?;
    let deadline = Instant::now() + StdDuration::from_millis(REDIS_SESSION_LOCK_WAIT_MS);
    loop {
        let acquired = redis::cmd("SET")
            .arg(lock_key)
            .arg(&lock_token)
            .arg("NX")
            .arg("PX")
            .arg(REDIS_SESSION_LOCK_TTL_MS)
            .query::<Option<String>>(&mut connection)?;
        if acquired.is_some() {
            return Ok((connection, lock_token));
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out acquiring redis auth session lock `{lock_key}`"),
            )
            .into());
        }
        std::thread::sleep(StdDuration::from_millis(REDIS_SESSION_LOCK_RETRY_MS));
    }
}

fn release_redis_session_lock(
    connection: &mut redis::Connection,
    lock_key: &str,
    lock_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let script = redis::Script::new(
        r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"#,
    );
    let _deleted: i32 = script.key(lock_key).arg(lock_token).invoke(connection)?;
    Ok(())
}

fn load_persisted_auth_state(
    store: &AuthSessionStateStore,
) -> Result<Option<PersistedAuthPortalState>, Box<dyn std::error::Error>> {
    match store {
        AuthSessionStateStore::File {
            state_path,
            lock_path,
        } => load_persisted_auth_state_from_file(state_path, lock_path),
        AuthSessionStateStore::Redis { url, state_key, .. } => {
            load_persisted_auth_state_from_redis(url, state_key)
        }
    }
}

fn lock_portal_state<'a, T>(
    mutex: &'a Mutex<T>,
    label: &'static str,
) -> Result<MutexGuard<'a, T>, Box<dyn std::error::Error>> {
    mutex
        .lock()
        .map_err(|_| IoError::other(format!("{label} lock poisoned")).into())
}

fn lock_sessions(
    auth: &AuthPortalState,
) -> Result<MutexGuard<'_, BTreeMap<ContentId, PrincipalSession>>, Box<dyn std::error::Error>> {
    lock_portal_state(&auth.sessions, "auth session state")
}

fn lock_directory(
    auth: &AuthPortalState,
) -> Result<MutexGuard<'_, ExperimentDirectory>, Box<dyn std::error::Error>> {
    lock_portal_state(&auth.directory, "auth directory")
}

impl AuthPortalState {
    fn apply_persisted_snapshot(
        &self,
        snapshot: Option<PersistedAuthPortalState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let (sessions, connector_state) = match snapshot {
            Some(snapshot) => (
                snapshot
                    .sessions
                    .into_iter()
                    .filter(|(_, session)| session.expires_at >= now)
                    .collect(),
                snapshot.connector_state,
            ),
            None => (BTreeMap::new(), None),
        };
        self.connector
            .import_persistent_state(connector_state.as_deref())?;
        let mut current_sessions = lock_sessions(self)?;
        *current_sessions = sessions;
        Ok(())
    }

    fn snapshot_state(&self) -> Result<PersistedAuthPortalState, Box<dyn std::error::Error>> {
        Ok(PersistedAuthPortalState {
            sessions: lock_sessions(self)?.clone(),
            connector_state: self.connector.export_persistent_state()?,
        })
    }

    fn with_shared_state<T, F>(
        &self,
        persist_after: bool,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: FnOnce(&AuthPortalState) -> Result<T, Box<dyn std::error::Error>>,
    {
        let Some(store) = self.session_state_store.as_ref() else {
            return operation(self);
        };
        match store {
            AuthSessionStateStore::File {
                state_path,
                lock_path,
            } => {
                if let Some(parent) = lock_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                if let Some(parent) = state_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(lock_path)?;
                lock_file.lock_exclusive()?;
                let snapshot = if state_path.exists() {
                    load_persisted_auth_state_from_bytes(Some(std::fs::read(state_path)?))?
                } else {
                    None
                };
                self.apply_persisted_snapshot(snapshot)?;

                let op_result = operation(self);
                let persist_result: Result<(), Box<dyn std::error::Error>> = if persist_after {
                    let snapshot = self.snapshot_state()?;
                    let bytes = burn_p2p_core::deterministic_cbor(&snapshot)?;
                    let temp_path = state_path.with_extension("tmp");
                    std::fs::write(&temp_path, &bytes)?;
                    std::fs::rename(&temp_path, state_path)?;
                    Ok(())
                } else {
                    Ok(())
                };
                let unlock_result = fs2::FileExt::unlock(&lock_file);
                unlock_result?;
                persist_result?;
                op_result
            }
            AuthSessionStateStore::Redis {
                url,
                state_key,
                lock_key,
            } => {
                let (mut connection, lock_token) = acquire_redis_session_lock(url, lock_key)?;
                let snapshot =
                    load_persisted_auth_state_from_bytes(connection.get(state_key.as_str())?)?;
                self.apply_persisted_snapshot(snapshot)?;
                let op_result = operation(self);
                let persist_result: Result<(), Box<dyn std::error::Error>> = if persist_after {
                    let snapshot = self.snapshot_state()?;
                    let bytes = burn_p2p_core::deterministic_cbor(&snapshot)?;
                    connection.set::<_, _, ()>(state_key.as_str(), bytes)?;
                    Ok(())
                } else {
                    Ok(())
                };
                let unlock_result =
                    release_redis_session_lock(&mut connection, lock_key, &lock_token);
                unlock_result?;
                persist_result?;
                op_result
            }
        }
    }

    pub(super) fn get_session(
        &self,
        session_id: &ContentId,
    ) -> Result<Option<PrincipalSession>, Box<dyn std::error::Error>> {
        self.with_shared_state(false, |auth| {
            Ok(lock_sessions(auth)?.get(session_id).cloned())
        })
    }

    pub(super) fn get_enrollment_session(
        &self,
        session_id: &ContentId,
    ) -> Result<Option<PrincipalSession>, Box<dyn std::error::Error>> {
        self.with_shared_state(true, |auth| {
            let existing = lock_sessions(auth)?.get(session_id).cloned();
            let Some(mut session) = existing else {
                return Ok(None);
            };

            match auth.connector.fetch_claims(&session) {
                Ok(claims) => {
                    session.claims = claims;
                    lock_sessions(auth)?.insert(session_id.clone(), session.clone());
                    Ok(Some(session))
                }
                Err(error) => {
                    lock_sessions(auth)?.remove(session_id);
                    Err(Box::new(error) as Box<dyn std::error::Error>)
                }
            }
        })
    }

    pub(super) fn begin_login(
        &self,
        request: LoginRequest,
    ) -> Result<burn_p2p::LoginStart, Box<dyn std::error::Error>> {
        self.with_shared_state(true, |auth| Ok(auth.connector.begin_login(request)?))
    }

    pub(super) fn complete_login(
        &self,
        callback: burn_p2p::CallbackPayload,
    ) -> Result<PrincipalSession, Box<dyn std::error::Error>> {
        self.with_shared_state(true, |auth| {
            let session = auth.connector.complete_login(callback)?;
            lock_sessions(auth)?.insert(session.session_id.clone(), session.clone());
            Ok(session)
        })
    }

    pub(super) fn refresh_session(
        &self,
        session_id: &ContentId,
    ) -> Result<PrincipalSession, Box<dyn std::error::Error>> {
        self.with_shared_state(true, |auth| {
            let session = lock_sessions(auth)?
                .get(session_id)
                .cloned()
                .ok_or_else(|| "unknown session id".to_owned())?;
            let refreshed = auth.connector.refresh(&session)?;
            let mut sessions = lock_sessions(auth)?;
            sessions.remove(session_id);
            sessions.insert(refreshed.session_id.clone(), refreshed.clone());
            Ok(refreshed)
        })
    }

    pub(super) fn revoke_session(
        &self,
        session_id: &ContentId,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.with_shared_state(true, |auth| {
            let session = lock_sessions(auth)?.get(session_id).cloned();
            if let Some(session) = session.as_ref() {
                auth.connector.revoke(session)?;
            }
            let removed = lock_sessions(auth)?.remove(session_id).is_some();
            Ok(removed)
        })
    }

    pub(super) fn restore_persisted_state(&self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(store) = self.session_state_store.as_ref() else {
            return Ok(());
        };
        let Some(snapshot) = load_persisted_auth_state(store)? else {
            return Ok(());
        };
        self.apply_persisted_snapshot(Some(snapshot))
    }
}

pub(super) fn build_auth_portal(
    config: &BootstrapAuthConfig,
    network_id: NetworkId,
    protocol_version: semver::Version,
) -> Result<AuthPortalState, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let provider = match &config.connector {
        BootstrapAuthConnectorConfig::Static => AuthProvider::Static {
            authority: config.authority_name.clone(),
        },
        BootstrapAuthConnectorConfig::GitHub { .. } => AuthProvider::GitHub,
        BootstrapAuthConnectorConfig::Oidc { issuer, .. } => AuthProvider::Oidc {
            issuer: issuer.clone(),
        },
        BootstrapAuthConnectorConfig::OAuth { provider, .. } => AuthProvider::OAuth {
            provider: provider.clone(),
        },
        BootstrapAuthConnectorConfig::External { authority, .. } => AuthProvider::External {
            authority: authority.clone(),
        },
    };
    let configured_principals = configured_auth_principals(config)?;
    let principals = configured_principals
        .iter()
        .map(|principal| {
            Ok((
                principal.principal_id.clone(),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: principal.principal_id.clone(),
                        provider: provider.clone(),
                        display_name: principal.display_name.clone(),
                        org_memberships: principal.org_memberships.clone(),
                        group_memberships: principal.group_memberships.clone(),
                        granted_roles: principal.granted_roles.clone(),
                        granted_scopes: principal.granted_scopes.clone(),
                        custom_claims: principal.custom_claims.clone(),
                        issued_at: now,
                        expires_at: now
                            + chrono::Duration::seconds(config.session_ttl_seconds.max(1)),
                    },
                    allowed_networks: principal.allowed_networks.clone(),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;

    let session_ttl = chrono::Duration::seconds(config.session_ttl_seconds.max(1));
    let github_trusted_callback = config
        .provider_policy
        .as_ref()
        .and_then(|policy| policy.github.as_ref())
        .and_then(|github| github.trusted_callback.clone());
    let connector = match &config.connector {
        BootstrapAuthConnectorConfig::Static => EdgeIdentityConnector::new(
            vec![BrowserLoginProvider {
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: None,
            }],
            Box::new(StaticIdentityConnector::new(
                config.authority_name.clone(),
                session_ttl,
                principals.clone(),
            )),
        )
        .allow_request_body_callback_principal(),
        BootstrapAuthConnectorConfig::GitHub {
            authorize_base_url,
            exchange_url,
            token_url,
            api_base_url,
            client_id,
            client_secret,
            redirect_uri,
            userinfo_url,
            refresh_url,
            revoke_url,
            jwks_url,
        } => build_github_portal_connector(
            session_ttl,
            principals.clone(),
            EdgeConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                api_base_url: api_base_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                redirect_uri: redirect_uri.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
                jwks_url: jwks_url.clone(),
                persist_remote_tokens: config.persist_provider_tokens,
                trusted_callback: github_trusted_callback,
            },
        )?,
        BootstrapAuthConnectorConfig::Oidc {
            issuer,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            redirect_uri,
            userinfo_url,
            refresh_url,
            revoke_url,
            jwks_url,
        } => build_oidc_portal_connector(
            issuer.clone(),
            session_ttl,
            principals.clone(),
            EdgeConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                api_base_url: None,
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                redirect_uri: redirect_uri.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
                jwks_url: jwks_url.clone(),
                persist_remote_tokens: config.persist_provider_tokens,
                trusted_callback: None,
            },
        )?,
        BootstrapAuthConnectorConfig::OAuth {
            provider,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            redirect_uri,
            userinfo_url,
            refresh_url,
            revoke_url,
            jwks_url,
        } => build_oauth_portal_connector(
            provider.clone(),
            session_ttl,
            principals.clone(),
            EdgeConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                api_base_url: None,
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                redirect_uri: redirect_uri.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
                jwks_url: jwks_url.clone(),
                persist_remote_tokens: config.persist_provider_tokens,
                trusted_callback: None,
            },
        )?,
        BootstrapAuthConnectorConfig::External {
            authority,
            trusted_principal_header,
            ..
        } => build_external_portal_connector(
            authority.clone(),
            trusted_principal_header.clone(),
            session_ttl,
            principals,
        )?,
    };
    let login_providers = connector.login_providers();
    let authority_keypair = load_or_create_keypair(&config.authority_key_path)?;
    let authority = NodeCertificateAuthority::new(
        network_id.clone(),
        config.project_family_id.clone(),
        config.required_release_train_hash.clone(),
        protocol_version.clone(),
        authority_keypair,
        config.issuer_key_id.clone(),
    )?;
    let active_issuer = TrustedIssuer {
        issuer_peer_id: authority.issuer_peer_id(),
        issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
    };
    let mut trusted_issuers = config
        .trusted_issuers
        .iter()
        .cloned()
        .map(|issuer| (issuer.issuer_peer_id.clone(), issuer))
        .collect::<BTreeMap<_, _>>();
    trusted_issuers.insert(active_issuer.issuer_peer_id.clone(), active_issuer);

    let auth_state = AuthPortalState {
        connector,
        login_providers,
        authority_key_path: config.authority_key_path.clone(),
        session_state_store: Some(auth_session_state_store(config, &network_id)),
        network_id: network_id.clone(),
        protocol_version,
        issuer_key_id: Mutex::new(config.issuer_key_id.clone()),
        authority: Mutex::new(authority),
        trusted_issuers: Mutex::new(trusted_issuers),
        sessions: Mutex::new(BTreeMap::new()),
        directory: Mutex::new(ExperimentDirectory {
            network_id,
            generated_at: now,
            entries: config.directory_entries.clone(),
        }),
        minimum_revocation_epoch: Mutex::new(RevocationEpoch(config.minimum_revocation_epoch)),
        reenrollment: Mutex::new(config.reenrollment.clone()),
        project_family_id: config.project_family_id.clone(),
        minimum_client_version: config.minimum_client_version.clone(),
        required_release_train_hash: config.required_release_train_hash.clone(),
        allowed_target_artifact_hashes: config.allowed_target_artifact_hashes.clone(),
    };
    auth_state.restore_persisted_state()?;
    Ok(auth_state)
}

fn configured_auth_principals(
    config: &BootstrapAuthConfig,
) -> Result<Vec<BootstrapAuthPrincipal>, Box<dyn std::error::Error>> {
    let mut principals = config.principals.clone();
    let mut provider_policy_principals = match config.provider_policy.as_ref() {
        Some(policy) => provider_policy_principals(config, policy)?,
        None => Vec::new(),
    };
    principals.append(&mut provider_policy_principals);

    let mut seen = BTreeSet::new();
    for principal in &principals {
        if !seen.insert(principal.principal_id.clone()) {
            return Err(std::io::Error::other(format!(
                "duplicate auth principal id `{}` in bootstrap auth config",
                principal.principal_id.as_str()
            ))
            .into());
        }
    }
    Ok(principals)
}

fn provider_policy_principals(
    config: &BootstrapAuthConfig,
    policy: &BootstrapAuthProviderPolicyConfig,
) -> Result<Vec<BootstrapAuthPrincipal>, Box<dyn std::error::Error>> {
    let mut principals = Vec::new();

    if let Some(github) = policy.github.as_ref() {
        if !matches!(
            config.connector,
            BootstrapAuthConnectorConfig::GitHub { .. }
        ) {
            return Err(std::io::Error::other(
                "github provider policy requires the github auth connector",
            )
            .into());
        }
        if let Some(trusted_callback) = github.trusted_callback.as_ref() {
            if trusted_callback.token_header.trim().is_empty() {
                return Err(std::io::Error::other(
                    "github trusted callback requires a non-empty token_header",
                )
                .into());
            }
            if trusted_callback.token_value.trim().is_empty() {
                return Err(std::io::Error::other(
                    "github trusted callback requires a non-empty token_value",
                )
                .into());
            }
        }
        principals.extend(
            github
                .rules
                .iter()
                .map(github_policy_rule_principal)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    Ok(principals)
}

fn github_policy_rule_principal(
    rule: &BootstrapGitHubPrincipalRule,
) -> Result<BootstrapAuthPrincipal, Box<dyn std::error::Error>> {
    let mut custom_claims = rule.custom_claims.clone();

    if let Some(login) = rule.provider_login.as_ref().map(|login| login.trim()) {
        if login.is_empty() {
            return Err(std::io::Error::other(format!(
                "github policy rule `{}` configured an empty provider_login",
                rule.principal_id.as_str()
            ))
            .into());
        }
        custom_claims.insert("provider_login".into(), login.to_owned());
    }
    if let Some(email) = rule.provider_email.as_ref().map(|email| email.trim()) {
        if email.is_empty() {
            return Err(std::io::Error::other(format!(
                "github policy rule `{}` configured an empty provider_email",
                rule.principal_id.as_str()
            ))
            .into());
        }
        custom_claims.insert("provider_email".into(), email.to_owned());
    }

    let required_orgs = join_required_provider_claims(&rule.required_orgs);
    if !required_orgs.is_empty() {
        custom_claims.insert("provider_orgs".into(), required_orgs);
    }
    let required_teams = join_required_provider_claims(&rule.required_teams);
    if !required_teams.is_empty() {
        custom_claims.insert("provider_groups".into(), required_teams);
    }

    let required_repo_access = rule
        .required_repo_access
        .iter()
        .map(|repo| {
            let repository = repo.repo.trim();
            let minimum_permission = repo.minimum_permission.trim().to_ascii_lowercase();
            if repository.is_empty() || minimum_permission.is_empty() {
                return Err(std::io::Error::other(format!(
                    "github policy rule `{}` configured an empty repo access requirement",
                    rule.principal_id.as_str()
                )));
            }
            Ok(format!("{repository}:{minimum_permission}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if !required_repo_access.is_empty() {
        custom_claims.insert(
            "provider_repo_access".into(),
            join_required_provider_claims(&required_repo_access),
        );
    }

    Ok(BootstrapAuthPrincipal {
        principal_id: rule.principal_id.clone(),
        display_name: rule.display_name.clone(),
        org_memberships: BTreeSet::new(),
        group_memberships: BTreeSet::new(),
        granted_roles: rule.granted_roles.clone(),
        granted_scopes: rule.granted_scopes.clone(),
        allowed_networks: rule.allowed_networks.clone(),
        custom_claims,
    })
}

fn join_required_provider_claims(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn auth_directory_entries(
    auth: &AuthPortalState,
    request: &HttpRequest,
) -> Result<Vec<ExperimentDirectoryEntry>, Box<dyn std::error::Error>> {
    let scopes = request
        .headers
        .get("x-session-id")
        .map(|session_id| auth.get_session(&ContentId::new(session_id.clone())))
        .transpose()?
        .flatten()
        .map(|session| session.claims.granted_scopes)
        .unwrap_or_default();

    let directory = lock_directory(auth)?;
    Ok(directory.visible_to(&scopes).into_iter().cloned().collect())
}

pub(super) fn session_allows_receipt_submission(
    session: &PrincipalSession,
    receipt: &burn_p2p::ContributionReceipt,
) -> bool {
    session
        .claims
        .granted_scopes
        .iter()
        .any(|scope| match scope {
            ExperimentScope::Connect => true,
            ExperimentScope::Train { experiment_id }
            | ExperimentScope::Validate { experiment_id } => {
                experiment_id == &receipt.experiment_id
            }
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p::{PeerRole, ProjectFamilyId};
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;

    fn github_policy_config() -> BootstrapAuthConfig {
        BootstrapAuthConfig {
            authority_name: "github-auth".into(),
            connector: BootstrapAuthConnectorConfig::GitHub {
                authorize_base_url: None,
                exchange_url: None,
                token_url: None,
                api_base_url: None,
                client_id: None,
                client_secret: None,
                redirect_uri: None,
                userinfo_url: None,
                refresh_url: None,
                revoke_url: None,
                jwks_url: None,
            },
            authority_key_path: PathBuf::from("authority.key"),
            session_state_path: None,
            session_state_backend: None,
            persist_provider_tokens: false,
            issuer_key_id: default_issuer_key_id(),
            project_family_id: ProjectFamilyId::new("demo-family"),
            minimum_client_version: semver::Version::new(0, 1, 0),
            required_release_train_hash: ContentId::new("demo-train"),
            allowed_target_artifact_hashes: BTreeSet::new(),
            session_ttl_seconds: 300,
            minimum_revocation_epoch: 1,
            principals: Vec::new(),
            provider_policy: Some(BootstrapAuthProviderPolicyConfig {
                github: Some(BootstrapGitHubAuthPolicyConfig {
                    rules: vec![BootstrapGitHubPrincipalRule {
                        principal_id: PrincipalId::new("github-community"),
                        display_name: "GitHub Community".into(),
                        provider_login: Some("mosure".into()),
                        provider_email: None,
                        required_orgs: BTreeSet::from(["burn-community".into()]),
                        required_teams: BTreeSet::from(["burn-community/maintainers".into()]),
                        required_repo_access: vec![BootstrapGitHubRepoAccessRule {
                            repo: "aberration-technology/burn_p2p".into(),
                            minimum_permission: "admin".into(),
                        }],
                        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                        granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                        allowed_networks: BTreeSet::from([NetworkId::new("community-web-demo")]),
                        custom_claims: BTreeMap::from([(
                            "deployment_profile".into(),
                            "community-web".into(),
                        )]),
                    }],
                    trusted_callback: None,
                }),
            }),
            directory_entries: Vec::new(),
            trusted_issuers: Vec::new(),
            reenrollment: None,
        }
    }

    #[test]
    fn github_provider_policy_compiles_into_dynamic_principal_rules() {
        let config = github_policy_config();
        let principals = configured_auth_principals(&config).expect("compile github policy");
        assert_eq!(principals.len(), 1);
        let principal = &principals[0];
        assert_eq!(principal.principal_id, PrincipalId::new("github-community"));
        assert_eq!(
            principal.custom_claims.get("provider_login"),
            Some(&"mosure".into())
        );
        assert_eq!(
            principal.custom_claims.get("provider_orgs"),
            Some(&"burn-community".into())
        );
        assert_eq!(
            principal.custom_claims.get("provider_groups"),
            Some(&"burn-community/maintainers".into())
        );
        assert_eq!(
            principal.custom_claims.get("provider_repo_access"),
            Some(&"aberration-technology/burn_p2p:admin".into())
        );
    }

    #[test]
    fn github_provider_policy_rejects_non_github_connector() {
        let mut config = github_policy_config();
        config.connector = BootstrapAuthConnectorConfig::Static;
        let error =
            configured_auth_principals(&config).expect_err("non-github connector should fail");
        assert!(
            error
                .to_string()
                .contains("github provider policy requires the github auth connector")
        );
    }
}
