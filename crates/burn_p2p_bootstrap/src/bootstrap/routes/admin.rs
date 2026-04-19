use super::*;

pub(crate) fn persist_daemon_config(
    config_path: &Path,
    config: &BootstrapDaemonConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(config_path, serde_json::to_vec_pretty(config)?)?;
    Ok(())
}

pub(crate) fn rotate_authority_material(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    issuer_key_id: Option<String>,
    retain_previous_issuer: bool,
    require_reenrollment: bool,
    reenrollment_reason: Option<String>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let issuer_key_id = issuer_key_id.unwrap_or_else(|| "bootstrap-auth".into());
    if let Some(parent) = auth.authority_key_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let previous_authority = lock_shared(&auth.authority, "auth authority")?;
    let previous_issuer = TrustedIssuer {
        issuer_peer_id: previous_authority.issuer_peer_id(),
        issuer_public_key_hex: previous_authority.issuer_public_key_hex().to_owned(),
    };
    drop(previous_authority);

    let keypair = Keypair::generate_ed25519();
    std::fs::write(&auth.authority_key_path, keypair.to_protobuf_encoding()?)?;
    let authority = NodeCertificateAuthority::new(
        auth.network_id.clone(),
        auth.project_family_id.clone(),
        auth.required_release_train_hash.clone(),
        auth.protocol_version.clone(),
        keypair,
        issuer_key_id.clone(),
    )?;
    let issuer_peer_id = authority.issuer_peer_id();
    let issuer_public_key_hex = authority.issuer_public_key_hex().to_owned();
    {
        let mut trusted_issuers = lock_shared(&auth.trusted_issuers, "trusted issuer state")?;
        if retain_previous_issuer {
            trusted_issuers.insert(
                previous_issuer.issuer_peer_id.clone(),
                previous_issuer.clone(),
            );
        } else {
            trusted_issuers.remove(&previous_issuer.issuer_peer_id);
        }
        trusted_issuers.insert(
            issuer_peer_id.clone(),
            TrustedIssuer {
                issuer_peer_id: issuer_peer_id.clone(),
                issuer_public_key_hex: issuer_public_key_hex.clone(),
            },
        );
    }
    *lock_shared(&auth.issuer_key_id, "auth issuer key id")? = issuer_key_id.clone();
    if require_reenrollment {
        *lock_shared(&auth.reenrollment, "auth reenrollment state")? =
            Some(BootstrapReenrollmentConfig {
                reason: reenrollment_reason.unwrap_or_else(|| {
                    "authority material rotated; clients should re-enroll".into()
                }),
                rotated_at: Some(Utc::now()),
                retired_issuer_peer_ids: BTreeSet::from([previous_issuer.issuer_peer_id.clone()]),
            });
    } else if !retain_previous_issuer {
        *lock_shared(&auth.reenrollment, "auth reenrollment state")? = None;
    }
    *lock_shared(&auth.authority, "auth authority")? = authority;
    let trust_bundle = sync_trust_bundle(auth, state);

    Ok(burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
        issuer_key_id,
        issuer_peer_id,
        issuer_public_key_hex,
        trusted_issuers: trust_bundle.issuers.len(),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
        rotated_at: Utc::now(),
    })
}

pub(crate) fn rollout_auth_policy(
    plan: &BootstrapPlan,
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    rollout: AuthPolicyRollout,
    control_handle: Option<&ControlHandle>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let mut effective_minimum_revocation_epoch = None;
    if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
        let mut auth_epoch = lock_shared(&auth.minimum_revocation_epoch, "auth revocation epoch")?;
        *auth_epoch = (*auth_epoch).max(minimum_revocation_epoch);
        effective_minimum_revocation_epoch = Some(*auth_epoch);
    }

    let directory_entries = if let Some(entries) = rollout.directory_entries {
        let announced_at = Utc::now();
        {
            let mut directory = lock_shared(&auth.directory, "auth directory")?;
            directory.generated_at = announced_at;
            directory.entries = entries;
        }
        if let Some(control_handle) = control_handle {
            control_handle.publish_directory(ExperimentDirectoryAnnouncement {
                network_id: plan.network_id().clone(),
                entries: lock_shared(&auth.directory, "auth directory")?
                    .entries
                    .clone(),
                announced_at,
            })?;
        }
        lock_shared(&auth.directory, "auth directory")?
            .entries
            .len()
    } else {
        lock_shared(&auth.directory, "auth directory")?
            .entries
            .len()
    };

    let trusted_issuers = if let Some(issuers) = rollout.trusted_issuers {
        let mut trusted = lock_shared(&auth.trusted_issuers, "trusted issuer state")?;
        *trusted = issuers
            .into_iter()
            .map(|issuer| (issuer.issuer_peer_id.clone(), issuer))
            .collect();
        let authority = lock_shared(&auth.authority, "auth authority")?;
        trusted.insert(
            authority.issuer_peer_id(),
            TrustedIssuer {
                issuer_peer_id: authority.issuer_peer_id(),
                issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
            },
        );
        trusted.len()
    } else {
        lock_shared(&auth.trusted_issuers, "trusted issuer state")?.len()
    };

    if let Some(reenrollment) = rollout.reenrollment {
        *lock_shared(&auth.reenrollment, "auth reenrollment state")? =
            Some(BootstrapReenrollmentConfig {
                reason: reenrollment.reason,
                rotated_at: reenrollment.rotated_at,
                retired_issuer_peer_ids: reenrollment.retired_issuer_peer_ids,
            });
    }

    if let Some(epoch) = effective_minimum_revocation_epoch {
        lock_shared(state, "bootstrap admin state")?.minimum_revocation_epoch = Some(epoch);
    }
    let trust_bundle = sync_trust_bundle(auth, state);

    Ok(burn_p2p_bootstrap::AdminResult::AuthPolicyRolledOut {
        minimum_revocation_epoch: effective_minimum_revocation_epoch,
        directory_entries,
        trusted_issuers: trusted_issuers.max(trust_bundle.issuers.len()),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
    })
}

pub(crate) fn retire_trusted_issuers(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    issuer_peer_ids: &BTreeSet<PeerId>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let active_issuer_peer_id = lock_shared(&auth.authority, "auth authority")?.issuer_peer_id();
    if issuer_peer_ids.contains(&active_issuer_peer_id) {
        return Err("cannot retire the active issuer".into());
    }

    let mut trusted_issuers = lock_shared(&auth.trusted_issuers, "trusted issuer state")?;
    let previous_len = trusted_issuers.len();
    trusted_issuers.retain(|issuer_peer_id, _| !issuer_peer_ids.contains(issuer_peer_id));
    drop(trusted_issuers);

    let mut reenrollment_state = lock_shared(&auth.reenrollment, "auth reenrollment state")?;
    if let Some(reenrollment) = reenrollment_state.as_mut() {
        reenrollment
            .retired_issuer_peer_ids
            .retain(|issuer_peer_id| !issuer_peer_ids.contains(issuer_peer_id));
        if reenrollment.retired_issuer_peer_ids.is_empty() {
            *reenrollment_state = None;
        }
    }
    drop(reenrollment_state);

    let trust_bundle = sync_trust_bundle(auth, state);
    Ok(burn_p2p_bootstrap::AdminResult::TrustedIssuersRetired {
        retired_issuers: previous_len.saturating_sub(trust_bundle.issuers.len()),
        remaining_issuers: trust_bundle.issuers.len(),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
    })
}

pub(crate) fn publish_admin_result(
    plan: &BootstrapPlan,
    control_handle: Option<&ControlHandle>,
    result: &burn_p2p_bootstrap::AdminResult,
) -> Result<(), Box<dyn std::error::Error>> {
    if let (Some(control_handle), burn_p2p_bootstrap::AdminResult::Control(certificate)) =
        (control_handle, result)
    {
        control_handle.publish_control(ControlAnnouncement {
            overlay: OverlayTopic::control(plan.network_id().clone()),
            certificate: certificate.as_ref().clone(),
            announced_at: Utc::now(),
        })?;
    } else if let (Some(control_handle), burn_p2p_bootstrap::AdminResult::Lifecycle(certificate)) =
        (control_handle, result)
    {
        control_handle.publish_lifecycle(burn_p2p::ExperimentLifecycleAnnouncement {
            overlay: OverlayTopic::control(plan.network_id().clone()),
            certificate: certificate.as_ref().clone(),
            announced_at: Utc::now(),
        })?;
    } else if let (Some(control_handle), burn_p2p_bootstrap::AdminResult::Schedule(certificate)) =
        (control_handle, result)
    {
        control_handle.publish_schedule(burn_p2p::FleetScheduleAnnouncement {
            overlay: OverlayTopic::control(plan.network_id().clone()),
            certificate: certificate.as_ref().clone(),
            announced_at: Utc::now(),
        })?;
    }

    Ok(())
}

pub(crate) fn token_matches(
    request: &HttpRequest,
    admin_token: Option<&str>,
    allow_dev_admin_token: bool,
) -> bool {
    if !allow_dev_admin_token {
        return false;
    }
    match admin_token {
        Some(expected) => request
            .headers
            .get("x-admin-token")
            .is_some_and(|value| value == expected),
        None => false,
    }
}

pub(crate) fn export_admin_capabilities() -> BTreeSet<AdminCapability> {
    BTreeSet::from([
        AdminCapability::ExportDiagnostics,
        AdminCapability::ExportDiagnosticsBundle,
        AdminCapability::ExportOperatorControlReplay,
        AdminCapability::ExportHeads,
        AdminCapability::ExportReceipts,
        AdminCapability::ExportReducerLoad,
        AdminCapability::ExportTrustBundle,
        AdminCapability::OperatorRetentionPrune,
    ])
}

pub(crate) fn operator_admin_capabilities() -> BTreeSet<AdminCapability> {
    let mut capabilities = export_admin_capabilities();
    capabilities.insert(AdminCapability::Control);
    capabilities.insert(AdminCapability::BanPeer);
    capabilities.insert(AdminCapability::RegisterLiveHead);
    capabilities
}

pub(crate) fn all_admin_capabilities() -> BTreeSet<AdminCapability> {
    BTreeSet::from([
        AdminCapability::Control,
        AdminCapability::BanPeer,
        AdminCapability::RegisterLiveHead,
        AdminCapability::ExportDiagnostics,
        AdminCapability::ExportDiagnosticsBundle,
        AdminCapability::ExportOperatorControlReplay,
        AdminCapability::ExportHeads,
        AdminCapability::ExportReceipts,
        AdminCapability::ExportReducerLoad,
        AdminCapability::ExportTrustBundle,
        AdminCapability::RolloutAuthPolicy,
        AdminCapability::RetireTrustedIssuers,
        AdminCapability::RotateAuthorityMaterial,
        AdminCapability::OperatorRetentionPrune,
    ])
}

pub(crate) fn parse_admin_capability_token(token: &str) -> Option<AdminCapability> {
    let normalized = token.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match normalized.as_str() {
        "control" => Some(AdminCapability::Control),
        "banpeer" | "ban_peer" => Some(AdminCapability::BanPeer),
        "registerlivehead" | "register_live_head" => Some(AdminCapability::RegisterLiveHead),
        "exportdiagnostics" | "export_diagnostics" => Some(AdminCapability::ExportDiagnostics),
        "exportdiagnosticsbundle" | "export_diagnostics_bundle" => {
            Some(AdminCapability::ExportDiagnosticsBundle)
        }
        "exportoperatorcontrolreplay" | "export_operator_control_replay" => {
            Some(AdminCapability::ExportOperatorControlReplay)
        }
        "exportheads" | "export_heads" => Some(AdminCapability::ExportHeads),
        "exportreceipts" | "export_receipts" => Some(AdminCapability::ExportReceipts),
        "exportreducerload" | "export_reducer_load" => Some(AdminCapability::ExportReducerLoad),
        "exporttrustbundle" | "export_trust_bundle" => Some(AdminCapability::ExportTrustBundle),
        "operatorretentionprune" | "operator_retention_prune" => {
            Some(AdminCapability::OperatorRetentionPrune)
        }
        "rolloutauthpolicy" | "rollout_auth_policy" => Some(AdminCapability::RolloutAuthPolicy),
        "retiretrustedissuers" | "retire_trusted_issuers" => {
            Some(AdminCapability::RetireTrustedIssuers)
        }
        "rotateauthoritymaterial" | "rotate_authority_material" => {
            Some(AdminCapability::RotateAuthorityMaterial)
        }
        _ => None,
    }
}

pub(crate) fn session_admin_capabilities(session: &PrincipalSession) -> BTreeSet<AdminCapability> {
    let mut capabilities = BTreeSet::new();
    let custom_claims = &session.claims.custom_claims;

    if session.claims.group_memberships.contains("admins") {
        capabilities.extend(all_admin_capabilities());
    } else if session.claims.group_memberships.contains("operators") {
        capabilities.extend(operator_admin_capabilities());
    }

    if let Some(role) = custom_claims
        .get("operator_role")
        .or_else(|| custom_claims.get("admin_role"))
    {
        match role.trim().to_ascii_lowercase().as_str() {
            "viewer" | "read_only" | "readonly" => {
                capabilities.extend(export_admin_capabilities());
            }
            "operator" => {
                capabilities.extend(operator_admin_capabilities());
            }
            "admin" | "authority_admin" => {
                capabilities.extend(all_admin_capabilities());
            }
            _ => {}
        }
    }

    if let Some(tokens) = custom_claims
        .get("admin_capabilities")
        .or_else(|| custom_claims.get("operator_capabilities"))
    {
        if tokens
            .split(',')
            .any(|token| matches!(token.trim().to_ascii_lowercase().as_str(), "*" | "all"))
        {
            return all_admin_capabilities();
        }
        for token in tokens.split(',') {
            if let Some(capability) = parse_admin_capability_token(token) {
                capabilities.insert(capability);
            }
        }
    }

    capabilities
}

pub(crate) fn request_admin_capabilities(
    request: &HttpRequest,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> Option<BTreeSet<AdminCapability>> {
    let auth = auth_state?;
    let session_id = request.headers.get("x-session-id")?;
    let session = auth
        .get_session(&ContentId::new(session_id.clone()))
        .ok()
        .flatten()?;
    Some(session_admin_capabilities(&session))
}
