use super::*;

pub(crate) fn current_browser_portal_snapshot(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    state: &Arc<Mutex<BootstrapAdminState>>,
    auth_state: Option<&Arc<AuthPortalState>>,
    request: &HttpRequest,
    remaining_work_units: Option<u64>,
) -> Result<burn_p2p_bootstrap::BrowserPortalSnapshot, Box<dyn std::error::Error>> {
    let directory = current_browser_directory_snapshot(plan, auth_state, request)?;
    let (required_release_train_hash, allowed_target_artifact_hashes) = auth_state
        .map(|auth| {
            (
                Some(auth.required_release_train_hash.clone()),
                auth.allowed_target_artifact_hashes.clone(),
            )
        })
        .unwrap_or_else(|| (None, BTreeSet::new()));

    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .browser_portal_snapshot(
            plan,
            BrowserPortalSnapshotConfig {
                captured_at: Utc::now(),
                remaining_work_units,
                directory,
                edge_mode: browser_edge_mode(plan),
                browser_mode: config.optional_services.browser_mode.clone(),
                social_mode: config.optional_services.social_mode.clone(),
                profile_mode: config.optional_services.profile_mode.clone(),
                transports: browser_transport_surface(plan, config),
                auth_enabled: auth_state.is_some(),
                login_providers: browser_login_providers(auth_state),
                required_release_train_hash,
                allowed_target_artifact_hashes,
            },
        ))
}

pub(crate) fn sign_browser_snapshot<T: serde::Serialize>(
    plan: &BootstrapPlan,
    signer: &PeerId,
    schema: &str,
    payload: T,
) -> Result<SignedPayload<SchemaEnvelope<T>>, Box<dyn std::error::Error>> {
    Ok(SignedPayload::new(
        SchemaEnvelope::new(schema, plan.genesis.protocol_version.clone(), payload),
        SignatureMetadata {
            signer: signer.clone(),
            key_id: "bootstrap-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "bootstrap-browser-edge".into(),
        },
    )?)
}
