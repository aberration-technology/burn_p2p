use super::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub(super) struct RemoteTrustedIssuerStatus {
    issuer_peer_id: PeerId,
    issuer_public_key_hex: String,
    accepted_for_admission: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub(super) struct RemoteReenrollmentStatus {
    reason: String,
    rotated_at: Option<DateTime<Utc>>,
    legacy_issuer_peer_ids: BTreeSet<PeerId>,
    login_path: String,
    enroll_path: String,
    trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub(super) struct RemoteTrustBundleExport {
    network_id: NetworkId,
    project_family_id: ProjectFamilyId,
    required_release_train_hash: ContentId,
    #[serde(default)]
    allowed_target_artifact_hashes: BTreeSet<ContentId>,
    minimum_revocation_epoch: RevocationEpoch,
    active_issuer_peer_id: PeerId,
    issuers: Vec<RemoteTrustedIssuerStatus>,
    reenrollment: Option<RemoteReenrollmentStatus>,
}

fn trust_score_for_peer<'a>(
    snapshot: &'a NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> Option<&'a TrustScore> {
    snapshot
        .trust_scores
        .iter()
        .find(|score| &score.peer_id == peer_id)
}

pub(super) fn peer_is_reducer_eligible(snapshot: &NodeTelemetrySnapshot, peer_id: &PeerId) -> bool {
    trust_score_for_peer(snapshot, peer_id)
        .map(|score| !score.quarantined && score.reducer_eligible)
        .unwrap_or(true)
}

pub(super) fn peer_is_validator_eligible(
    snapshot: &NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> bool {
    trust_score_for_peer(snapshot, peer_id)
        .map(|score| !score.quarantined && score.validator_eligible)
        .unwrap_or(true)
}

fn latest_auth_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    peer_id: &PeerId,
) -> Option<PeerAuthEnvelope> {
    snapshot
        .auth_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| announcement.envelope.clone())
}

pub(crate) fn verify_snapshot_admission(
    policy: &AdmissionPolicy,
    peer_id: &PeerId,
    snapshot: &ControlPlaneSnapshot,
) -> anyhow::Result<PeerAdmissionReport> {
    let envelope = latest_auth_from_snapshot(snapshot, peer_id).ok_or_else(|| {
        anyhow::anyhow!(
            "peer {} did not publish an auth envelope in its control snapshot",
            peer_id.as_str()
        )
    })?;
    Ok(policy.verify_peer_auth(&envelope, peer_id, Utc::now())?)
}

pub(super) fn note_admitted_peer(
    snapshot: &mut NodeTelemetrySnapshot,
    report: PeerAdmissionReport,
) {
    let now = report.verified_at;
    let peer_id = report.peer_id.clone();
    snapshot.admitted_peers.insert(peer_id.clone(), report);
    snapshot.rejected_peers.remove(&peer_id);
    snapshot
        .peer_reputation
        .entry(peer_id.clone())
        .or_insert_with(|| ReputationState::new(peer_id, now));
}

pub(super) fn note_rejected_peer(
    snapshot: &mut NodeTelemetrySnapshot,
    peer_id: PeerId,
    reason: String,
    warning_count: u32,
    failure_count: u32,
) {
    snapshot.admitted_peers.remove(&peer_id);
    snapshot.rejected_peers.insert(peer_id.clone(), reason);
    let now = Utc::now();
    let reputation_state = snapshot
        .peer_reputation
        .entry(peer_id.clone())
        .or_insert_with(|| ReputationState::new(peer_id, now));
    let _ = ReputationEngine::default().apply(
        reputation_state,
        ReputationObservation {
            accepted_work_units: 0,
            warning_count,
            failure_count,
            last_control_cert_id: None,
            last_merge_cert_id: None,
        },
        now,
    );
}

pub(super) fn admission_rejection_reason(report: &PeerAdmissionReport) -> String {
    report
        .findings
        .first()
        .map(|finding| format!("{finding:?}"))
        .unwrap_or_else(|| "peer admission rejected".to_owned())
}

fn latest_announced_revocation_epoch(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
) -> Option<RevocationEpoch> {
    snapshot
        .control_announcements
        .iter()
        .filter(|announcement| announcement.certificate.network_id == *network_id)
        .filter_map(
            |announcement| match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::RevokePeer {
                    minimum_revocation_epoch,
                    ..
                } => Some((*minimum_revocation_epoch, announcement.announced_at)),
                _ => None,
            },
        )
        .max_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)))
        .map(|(epoch, _)| epoch)
}

fn latest_peer_revocation_reason(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
    peer_id: &PeerId,
) -> Option<String> {
    snapshot
        .control_announcements
        .iter()
        .filter(|announcement| announcement.certificate.network_id == *network_id)
        .filter_map(
            |announcement| match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::RevokePeer {
                    peer_id: revoked_peer_id,
                    minimum_revocation_epoch,
                    reason,
                } if revoked_peer_id == peer_id => Some((
                    *minimum_revocation_epoch,
                    announcement.announced_at,
                    reason.clone(),
                )),
                _ => None,
            },
        )
        .max_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then(left.1.cmp(&right.1))
                .then(left.2.cmp(&right.2))
        })
        .map(|(_, _, reason)| reason)
}

fn slot_assignment_from_slot_state(slot_state: &SlotRuntimeState) -> Option<SlotAssignmentState> {
    match slot_state {
        SlotRuntimeState::Unassigned => None,
        SlotRuntimeState::Assigned(assignment)
        | SlotRuntimeState::MaterializingBase(assignment)
        | SlotRuntimeState::FetchingShards(assignment)
        | SlotRuntimeState::Training(assignment)
        | SlotRuntimeState::Publishing(assignment)
        | SlotRuntimeState::CoolingDown(assignment)
        | SlotRuntimeState::Migrating(assignment) => Some(assignment.clone()),
        SlotRuntimeState::Blocked { assignment, .. } => assignment.clone(),
    }
}

fn current_primary_assignment(snapshot: &NodeTelemetrySnapshot) -> Option<SlotAssignmentState> {
    snapshot
        .slot_states
        .first()
        .and_then(slot_assignment_from_slot_state)
}

fn local_admission_rejection_reason(report: &PeerAdmissionReport) -> String {
    report
        .findings
        .first()
        .map(|finding| format!("local certificate rejected: {finding:?}"))
        .unwrap_or_else(|| "local certificate rejected".to_owned())
}

fn local_admission_error_reason(error: &(impl ToString + std::fmt::Display)) -> String {
    format!("local certificate rejected: {error}")
}

fn set_local_node_revoked(snapshot: &mut NodeTelemetrySnapshot, reason: String) {
    snapshot.node_state = NodeRuntimeState::Revoked;
    snapshot.set_primary_slot_state(SlotRuntimeState::Blocked {
        assignment: current_primary_assignment(snapshot),
        reason,
    });
}

fn restore_local_node_ready(snapshot: &mut NodeTelemetrySnapshot) {
    if snapshot.node_state == NodeRuntimeState::Revoked {
        snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
        snapshot.set_primary_slot_state(
            current_primary_assignment(snapshot)
                .map(SlotRuntimeState::Assigned)
                .unwrap_or(SlotRuntimeState::Unassigned),
        );
    }
}

fn refresh_local_admission_state(snapshot: &mut NodeTelemetrySnapshot, auth: &Option<AuthConfig>) {
    let Some(auth) = auth else {
        return;
    };
    let Some(admission_policy) = auth.admission_policy.as_ref() else {
        return;
    };
    let Some(local_peer_auth) = auth.local_peer_auth.as_ref() else {
        return;
    };

    let peer_id = local_peer_auth.peer_id.clone();
    match admission_policy.verify_peer_auth(local_peer_auth, &peer_id, Utc::now()) {
        Ok(report) if matches!(report.decision(), AdmissionDecision::Allow) => {
            restore_local_node_ready(snapshot);
        }
        Ok(report) => {
            let reason = report
                .findings
                .iter()
                .find_map(|finding| match finding {
                    AuditFinding::RevocationEpochStale { minimum, found } => {
                        Some((*minimum, *found))
                    }
                    _ => None,
                })
                .map(|(minimum, found)| {
                    latest_peer_revocation_reason(
                        &snapshot.control_plane,
                        &admission_policy.network_id,
                        &peer_id,
                    )
                    .unwrap_or_else(|| {
                        format!(
                            "local certificate revocation epoch {} is below required {}",
                            found.0, minimum.0
                        )
                    })
                })
                .unwrap_or_else(|| local_admission_rejection_reason(&report));
            set_local_node_revoked(snapshot, reason);
        }
        Err(error) => {
            set_local_node_revoked(snapshot, local_admission_error_reason(&error));
        }
    }
}

fn reverify_admitted_peers(
    snapshot: &mut NodeTelemetrySnapshot,
    admission_policy: &AdmissionPolicy,
) {
    let peer_ids = snapshot.admitted_peers.keys().cloned().collect::<Vec<_>>();
    for peer_id in peer_ids {
        match verify_snapshot_admission(admission_policy, &peer_id, &snapshot.control_plane) {
            Ok(report) if matches!(report.decision(), AdmissionDecision::Allow) => {
                note_admitted_peer(snapshot, report);
            }
            Ok(report) => {
                note_rejected_peer(snapshot, peer_id, admission_rejection_reason(&report), 1, 0);
            }
            Err(error) => {
                note_rejected_peer(snapshot, peer_id, error.to_string(), 0, 1);
            }
        }
    }
}

pub(super) fn reconcile_live_revocation_policy(
    auth: &mut Option<AuthConfig>,
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
) -> bool {
    let Some(network_id) = snapshot.network_id.clone() else {
        return false;
    };
    let Some(announced_epoch) =
        latest_announced_revocation_epoch(&snapshot.control_plane, &network_id)
    else {
        refresh_local_admission_state(snapshot, auth);
        return false;
    };

    let mut changed = false;
    let mut persist_updated_auth = false;
    if snapshot
        .minimum_revocation_epoch
        .is_none_or(|current| announced_epoch > current)
    {
        snapshot.minimum_revocation_epoch = Some(announced_epoch);
        changed = true;
    }

    if let Some(admission_policy) = auth
        .as_mut()
        .and_then(|auth| auth.admission_policy.as_mut())
        && announced_epoch > admission_policy.minimum_revocation_epoch
    {
        admission_policy.minimum_revocation_epoch = announced_epoch;
        changed = true;
        persist_updated_auth = true;
    }

    if persist_updated_auth
        && let Some(storage) = storage
        && let Err(error) = persist_auth_state(storage, auth.as_ref())
    {
        snapshot.last_error = Some(format!("failed to persist updated auth state: {error}"));
    }

    if let Some(admission_policy) = auth
        .as_ref()
        .and_then(|auth| auth.admission_policy.as_ref())
    {
        reverify_admitted_peers(snapshot, admission_policy);
    }
    refresh_local_admission_state(snapshot, auth);

    changed
}

fn remote_trust_bundle_to_state(
    source_url: &str,
    bundle: RemoteTrustBundleExport,
) -> TrustBundleState {
    let trusted_issuers = bundle
        .issuers
        .into_iter()
        .filter(|issuer| issuer.accepted_for_admission)
        .map(|issuer| {
            let trusted = TrustedIssuer {
                issuer_peer_id: issuer.issuer_peer_id.clone(),
                issuer_public_key_hex: issuer.issuer_public_key_hex,
            };
            (issuer.issuer_peer_id, trusted)
        })
        .collect();

    TrustBundleState {
        source_url: source_url.to_owned(),
        required_release_train_hash: bundle.required_release_train_hash,
        allowed_target_artifact_hashes: bundle.allowed_target_artifact_hashes,
        active_issuer_peer_id: bundle.active_issuer_peer_id,
        trusted_issuers,
        minimum_revocation_epoch: bundle.minimum_revocation_epoch,
        reenrollment: bundle
            .reenrollment
            .map(|reenrollment| ClientReenrollmentStatus {
                reason: reenrollment.reason,
                rotated_at: reenrollment.rotated_at,
                legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
                login_path: reenrollment.login_path,
                enroll_path: reenrollment.enroll_path,
                trust_bundle_path: reenrollment.trust_bundle_path,
            }),
        updated_at: Utc::now(),
    }
}

fn trust_bundle_changed(current: Option<&TrustBundleState>, next: &TrustBundleState) -> bool {
    let Some(current) = current else {
        return true;
    };

    current.source_url != next.source_url
        || current.active_issuer_peer_id != next.active_issuer_peer_id
        || current.trusted_issuers != next.trusted_issuers
        || current.minimum_revocation_epoch != next.minimum_revocation_epoch
        || current.reenrollment != next.reenrollment
}

#[cfg(not(target_arch = "wasm32"))]
fn fetch_trust_bundle_export(source_url: &str) -> anyhow::Result<RemoteTrustBundleExport> {
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| anyhow::anyhow!("failed to build trust bundle client: {error}"))?
        .get(source_url)
        .send()
        .map_err(|error| {
            anyhow::anyhow!("failed to fetch trust bundle from {source_url}: {error}")
        })?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("trust bundle endpoint {source_url} returned {status}");
    }

    response.json().map_err(|error| {
        anyhow::anyhow!("failed to decode trust bundle from {source_url}: {error}")
    })
}

#[cfg(target_arch = "wasm32")]
fn fetch_trust_bundle_export(source_url: &str) -> anyhow::Result<RemoteTrustBundleExport> {
    futures::executor::block_on(async {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| anyhow::anyhow!("failed to build trust bundle client: {error}"))?;
        let response = client.get(source_url).send().await.map_err(|error| {
            anyhow::anyhow!("failed to fetch trust bundle from {source_url}: {error}")
        })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("trust bundle endpoint {source_url} returned {status}");
        }

        response.json().await.map_err(|error| {
            anyhow::anyhow!("failed to decode trust bundle from {source_url}: {error}")
        })
    })
}

fn fetch_trust_bundle(
    source_url: &str,
    admission_policy: &AdmissionPolicy,
) -> anyhow::Result<TrustBundleState> {
    let bundle = fetch_trust_bundle_export(source_url)?;

    if bundle.network_id != admission_policy.network_id {
        anyhow::bail!(
            "trust bundle network {} does not match runtime network {}",
            bundle.network_id.as_str(),
            admission_policy.network_id.as_str(),
        );
    }
    if bundle.project_family_id != admission_policy.project_family_id {
        anyhow::bail!(
            "trust bundle family {} does not match runtime family {}",
            bundle.project_family_id.as_str(),
            admission_policy.project_family_id.as_str(),
        );
    }
    if bundle.required_release_train_hash != admission_policy.required_release_train_hash {
        anyhow::bail!(
            "trust bundle release train {} does not match runtime release train {}",
            bundle.required_release_train_hash.as_str(),
            admission_policy.required_release_train_hash.as_str(),
        );
    }

    let trust_bundle = remote_trust_bundle_to_state(source_url, bundle);
    if !trust_bundle
        .trusted_issuers
        .contains_key(&trust_bundle.active_issuer_peer_id)
    {
        anyhow::bail!(
            "trust bundle active issuer {} is not accepted for admission",
            trust_bundle.active_issuer_peer_id.as_str(),
        );
    }

    Ok(trust_bundle)
}

pub(super) fn reconcile_remote_trust_bundle(
    auth: &mut Option<AuthConfig>,
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
) -> bool {
    let Some(auth_config) = auth.as_mut() else {
        return false;
    };
    let Some(initial_policy) = auth_config.admission_policy.as_ref() else {
        return false;
    };
    let endpoints = auth_config
        .trust_bundle_endpoints
        .iter()
        .map(|endpoint| endpoint.trim())
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return false;
    }

    let mut last_error = None;
    for endpoint in endpoints {
        match fetch_trust_bundle(&endpoint, initial_policy) {
            Ok(trust_bundle) => {
                let bundle_changed =
                    trust_bundle_changed(snapshot.trust_bundle.as_ref(), &trust_bundle);
                let mut changed = bundle_changed;

                if trust_bundle.minimum_revocation_epoch
                    > snapshot
                        .minimum_revocation_epoch
                        .unwrap_or(RevocationEpoch(0))
                {
                    snapshot.minimum_revocation_epoch = Some(trust_bundle.minimum_revocation_epoch);
                    changed = true;
                }

                {
                    let Some(admission_policy) = auth_config.admission_policy.as_mut() else {
                        snapshot.last_error = Some(
                            "auth admission policy disappeared while reconciling trust".into(),
                        );
                        return false;
                    };
                    if admission_policy.trusted_issuers != trust_bundle.trusted_issuers {
                        admission_policy.trusted_issuers = trust_bundle.trusted_issuers.clone();
                        changed = true;
                    }
                    if !trust_bundle.allowed_target_artifact_hashes.is_empty()
                        && admission_policy.allowed_target_artifact_hashes
                            != trust_bundle.allowed_target_artifact_hashes
                    {
                        admission_policy.allowed_target_artifact_hashes =
                            trust_bundle.allowed_target_artifact_hashes.clone();
                        changed = true;
                    }
                    if trust_bundle.minimum_revocation_epoch
                        > admission_policy.minimum_revocation_epoch
                    {
                        admission_policy.minimum_revocation_epoch =
                            trust_bundle.minimum_revocation_epoch;
                        changed = true;
                    }
                }

                if bundle_changed {
                    snapshot.trust_bundle = Some(trust_bundle);
                    snapshot.updated_at = Utc::now();
                }

                if changed
                    && let Some(storage) = storage
                    && let Err(error) = persist_auth_state(storage, Some(auth_config))
                {
                    snapshot.last_error = Some(format!(
                        "failed to persist trust bundle auth state: {error}"
                    ));
                }

                if let Some(admission_policy) = auth_config.admission_policy.as_ref() {
                    reverify_admitted_peers(snapshot, admission_policy);
                }
                refresh_local_admission_state(snapshot, auth);
                return changed;
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    if let Some(error) = last_error {
        snapshot.last_error = Some(error.to_string());
    }

    false
}
