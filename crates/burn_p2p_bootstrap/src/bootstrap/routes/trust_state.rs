use super::*;

pub(crate) fn current_revocation_epoch(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> RevocationEpoch {
    let auth_minimum_revocation_epoch = *auth
        .minimum_revocation_epoch
        .lock()
        .expect("auth revocation epoch should not be poisoned");
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .minimum_revocation_epoch
        .map(|epoch| epoch.max(auth_minimum_revocation_epoch))
        .unwrap_or(auth_minimum_revocation_epoch)
}

pub(crate) fn current_trust_bundle(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> TrustBundleExport {
    let authority = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned");
    let active_issuer_peer_id = authority.issuer_peer_id();
    let active_issuer = TrustedIssuer {
        issuer_peer_id: active_issuer_peer_id.clone(),
        issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
    };
    let mut trusted_issuers = auth
        .trusted_issuers
        .lock()
        .expect("trusted issuer state should not be poisoned")
        .clone();
    trusted_issuers.insert(active_issuer_peer_id.clone(), active_issuer);
    drop(authority);

    let mut issuers = trusted_issuers
        .into_values()
        .map(|issuer| TrustedIssuerStatus {
            active_for_new_certificates: issuer.issuer_peer_id == active_issuer_peer_id,
            accepted_for_admission: true,
            issuer_peer_id: issuer.issuer_peer_id,
            issuer_public_key_hex: issuer.issuer_public_key_hex,
        })
        .collect::<Vec<_>>();
    issuers.sort_by(|left, right| left.issuer_peer_id.cmp(&right.issuer_peer_id));

    let reenrollment = auth
        .reenrollment
        .lock()
        .expect("auth reenrollment state should not be poisoned")
        .clone()
        .map(|reenrollment| ReenrollmentStatus {
            reason: reenrollment.reason,
            rotated_at: reenrollment.rotated_at,
            legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
            login_path: "/login/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
        });

    TrustBundleExport {
        network_id: auth.network_id.clone(),
        project_family_id: auth.project_family_id.clone(),
        required_release_train_hash: auth.required_release_train_hash.clone(),
        allowed_target_artifact_hashes: auth.allowed_target_artifact_hashes.clone(),
        minimum_revocation_epoch: current_revocation_epoch(auth, state),
        active_issuer_peer_id,
        issuers,
        reenrollment,
    }
}

pub(crate) fn sync_trust_bundle(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> TrustBundleExport {
    let trust_bundle = current_trust_bundle(auth, state);
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .trust_bundle = Some(trust_bundle.clone());
    trust_bundle
}
