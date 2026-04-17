use super::*;

fn auth_error_status(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(auth_error) = error.downcast_ref::<burn_p2p::AuthError>() {
        return match auth_error {
            burn_p2p::AuthError::UnknownLogin(_)
            | burn_p2p::AuthError::StateMismatch
            | burn_p2p::AuthError::LoginExpired(_)
            | burn_p2p::AuthError::SessionExpired(_)
            | burn_p2p::AuthError::MissingProviderPrincipal
            | burn_p2p::AuthError::MissingProviderCode => "401 Unauthorized",
            burn_p2p::AuthError::UnknownPrincipal(_)
            | burn_p2p::AuthError::NetworkNotGranted(_)
            | burn_p2p::AuthError::ScopeNotGranted(_) => "403 Forbidden",
            burn_p2p::AuthError::ProviderExchange(_)
            | burn_p2p::AuthError::ProviderUserInfo(_)
            | burn_p2p::AuthError::ProviderRefresh(_)
            | burn_p2p::AuthError::ProviderRevoke(_) => "502 Bad Gateway",
            _ => "400 Bad Request",
        };
    }
    "500 Internal Server Error"
}

fn write_auth_error_response(
    stream: &mut TcpStream,
    error: &(dyn std::error::Error + 'static),
) -> Result<(), Box<dyn std::error::Error>> {
    write_response(
        stream,
        auth_error_status(error),
        "text/plain; charset=utf-8",
        error.to_string().into_bytes(),
    )?;
    Ok(())
}

pub(crate) fn handle_browser_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    current_config: &BootstrapDaemonConfig,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if (request.method.as_str(), request.path.as_str()) != ("POST", "/receipts/browser") {
        return Ok(false);
    }
    if !browser_join_enabled(current_config) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"browser edge disabled".to_vec(),
        )?;
        return Ok(true);
    }
    let auth = context
        .auth_state
        .as_ref()
        .ok_or("browser-edge auth is not configured")?;
    let session = request
        .headers
        .get("x-session-id")
        .map(|session_id| auth.get_session(&ContentId::new(session_id.clone())))
        .transpose()?
        .flatten()
        .ok_or("browser receipt submission requires x-session-id")?;
    let receipts: Vec<burn_p2p::ContributionReceipt> = serde_json::from_slice(&request.body)?;
    if receipts
        .iter()
        .any(|receipt| !session_allows_receipt_submission(&session, receipt))
    {
        return Err("session is not authorized to submit one or more browser receipts".into());
    }
    let accepted_receipt_ids = context
        .state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .ingest_contribution_receipts(receipts);
    write_json(
        stream,
        &BrowserReceiptSubmissionResponse {
            pending_receipt_count: 0,
            accepted_receipt_ids,
        },
    )?;
    Ok(true)
}

pub(crate) fn handle_auth_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(auth) = context.auth_state.as_ref() else {
        return Ok(false);
    };

    match (request.method.as_str(), request.path.as_str()) {
        ("POST", path) if auth.connector.matches_login_path(path) => {
            let login_request: LoginRequest = serde_json::from_slice(&request.body)?;
            match auth.begin_login(login_request) {
                Ok(login) => write_json(stream, &login)?,
                Err(error) => write_auth_error_response(stream, error.as_ref())?,
            }
        }
        ("POST", path) if auth.connector.matches_callback_path(path) => {
            let mut callback: burn_p2p::CallbackPayload = serde_json::from_slice(&request.body)?;
            if callback.principal_id.is_none() {
                callback.principal_id = auth.connector.trusted_callback_principal(request);
            }
            match auth.complete_login(callback) {
                Ok(session) => write_json(stream, &session)?,
                Err(error) => write_auth_error_response(stream, error.as_ref())?,
            }
        }
        ("POST", "/refresh") => {
            let refresh: SessionRequest = serde_json::from_slice(&request.body)?;
            match auth.refresh_session(&refresh.session_id) {
                Ok(refreshed) => write_json(stream, &refreshed)?,
                Err(error) => write_auth_error_response(stream, error.as_ref())?,
            }
        }
        ("POST", "/logout") => {
            let logout: SessionRequest = serde_json::from_slice(&request.body)?;
            match auth.revoke_session(&logout.session_id) {
                Ok(logged_out) => write_json(stream, &LogoutResponse { logged_out })?,
                Err(error) => write_auth_error_response(stream, error.as_ref())?,
            }
        }
        ("POST", "/enroll") => {
            let effective_revocation_epoch = current_revocation_epoch(auth, &context.state);
            let enroll: BootstrapEnrollRequest = serde_json::from_slice(&request.body)?;
            if enroll.release_train_hash != auth.required_release_train_hash {
                return Err(format!(
                    "release train {} is not permitted by this authority",
                    enroll.release_train_hash.as_str(),
                )
                .into());
            }
            if !auth.allowed_target_artifact_hashes.is_empty()
                && !auth
                    .allowed_target_artifact_hashes
                    .contains(&enroll.target_artifact_hash)
            {
                return Err(format!(
                    "target artifact {} is not permitted by this authority",
                    enroll.target_artifact_hash.as_str(),
                )
                .into());
            }
            let session = auth
                .get_enrollment_session(&enroll.session_id)?
                .ok_or("unknown session id")?;
            let granted_roles = session.claims.granted_roles.clone();
            let certificate = auth
                .authority
                .lock()
                .expect("auth authority should not be poisoned")
                .issue_certificate(NodeEnrollmentRequest {
                    session,
                    project_family_id: auth.project_family_id.clone(),
                    release_train_hash: enroll.release_train_hash.clone(),
                    target_artifact_hash: enroll.target_artifact_hash.clone(),
                    peer_id: enroll.peer_id,
                    peer_public_key_hex: enroll.peer_public_key_hex,
                    granted_roles,
                    requested_scopes: enroll.requested_scopes,
                    client_policy_hash: enroll.client_policy_hash,
                    serial: enroll.serial,
                    not_before: Utc::now(),
                    not_after: Utc::now() + chrono::Duration::seconds(enroll.ttl_seconds.max(1)),
                    revocation_epoch: effective_revocation_epoch,
                })?;
            write_json(stream, &certificate)?;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

pub(crate) fn handle_admin_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if (request.method.as_str(), request.path.as_str()) != ("POST", "/admin") {
        return Ok(false);
    }
    let action: AdminAction = serde_json::from_slice(&request.body)?;
    if !token_matches(
        request,
        context.admin_token.as_deref(),
        context.allow_dev_admin_token,
    ) {
        let Some(capabilities) = request_admin_capabilities(request, context.auth_state.as_ref())
        else {
            write_response(
                stream,
                "401 Unauthorized",
                "text/plain; charset=utf-8",
                b"missing or invalid x-admin-token or x-session-id".to_vec(),
            )?;
            return Ok(true);
        };
        if !capabilities.contains(&action.capability()) {
            write_response(
                stream,
                "403 Forbidden",
                "text/plain; charset=utf-8",
                format!(
                    "session is not authorized for admin capability {:?}",
                    action.capability()
                )
                .into_bytes(),
            )?;
            return Ok(true);
        }
    }

    let result = execute_admin_action(context, action.clone())?;
    write_json(stream, &result)?;
    Ok(true)
}

pub(crate) fn execute_admin_action(
    context: &HttpServerContext,
    action: AdminAction,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    match action.clone() {
        AdminAction::RotateAuthorityMaterial {
            issuer_key_id,
            retain_previous_issuer,
            require_reenrollment,
            reenrollment_reason,
        } => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(action.capability()),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let result = rotate_authority_material(
                auth,
                &context.state,
                issuer_key_id,
                retain_previous_issuer,
                require_reenrollment,
                reenrollment_reason.clone(),
            )?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.issuer_key_id = match &result {
                    burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
                        issuer_key_id,
                        ..
                    } => issuer_key_id.clone(),
                    _ => auth_config.issuer_key_id.clone(),
                };
                auth_config.trusted_issuers = current_trust_bundle(auth, &context.state)
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
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::RolloutAuthPolicy(rollout) => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(action.capability()),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let result = rollout_auth_policy(
                &context.plan,
                auth,
                &context.state,
                rollout.clone(),
                context.control_handle.as_ref(),
            )?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
                    auth_config.minimum_revocation_epoch = auth_config
                        .minimum_revocation_epoch
                        .max(minimum_revocation_epoch.0);
                }
                if let Some(directory_entries) = rollout.directory_entries {
                    auth_config.directory_entries = directory_entries;
                }
                if let Some(trusted_issuers) = rollout.trusted_issuers {
                    auth_config.trusted_issuers = trusted_issuers;
                }
                if let Some(reenrollment) = rollout.reenrollment {
                    auth_config.reenrollment = Some(BootstrapReenrollmentConfig {
                        reason: reenrollment.reason,
                        rotated_at: reenrollment.rotated_at,
                        retired_issuer_peer_ids: reenrollment.retired_issuer_peer_ids,
                    });
                }
            }
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::RetireTrustedIssuers { issuer_peer_ids } => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(action.capability()),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let result = retire_trusted_issuers(auth, &context.state, &issuer_peer_ids)?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.trusted_issuers = current_trust_bundle(auth, &context.state)
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
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::ExportTrustBundle => {
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            Ok(burn_p2p_bootstrap::AdminResult::TrustBundle(Some(
                current_trust_bundle(auth, &context.state),
            )))
        }
        _ => {
            let signer = Some(SignatureMetadata {
                signer: context.admin_signer_peer_id.clone(),
                key_id: "bootstrap-admin".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "bootstrap-local-admin".into(),
            });
            let result = context.plan.execute_admin_action(
                action,
                &mut context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned"),
                signer,
                Utc::now(),
                context.remaining_work_units,
            )?;
            publish_admin_result(&context.plan, context.control_handle.as_ref(), &result)?;
            Ok(result)
        }
    }
}
