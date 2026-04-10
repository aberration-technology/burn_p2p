use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use burn_p2p_core::{
    ArtifactId, ExperimentId, HeadId, NetworkId, PeerId, RevisionId, RevocationEpoch, StudyId,
    WindowId,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FORMAL_TRACE_SCHEMA_VERSION: &str = "burn_p2p.formal_trace.v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FormalTraceScenario {
    MnistSmoke,
    ReducerAdversarial,
}

impl FormalTraceScenario {
    pub fn slug(self) -> &'static str {
        match self {
            Self::MnistSmoke => "mnist-smoke",
            Self::ReducerAdversarial => "reducer-adversarial",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalTraceScope {
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalProtocolTrace {
    pub schema_version: String,
    pub scenario: FormalTraceScenario,
    pub generated_at: DateTime<Utc>,
    pub source: String,
    pub scope: FormalTraceScope,
    pub events: Vec<FormalProtocolEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FormalProtocolAction {
    PublishUpdate {
        peer_id: PeerId,
        window_id: WindowId,
        base_head_id: HeadId,
        artifact_id: ArtifactId,
    },
    PublishReducerProposal {
        peer_id: PeerId,
        window_id: WindowId,
        base_head_id: HeadId,
        artifact_id: ArtifactId,
    },
    AcceptCandidate {
        validator_peer_id: PeerId,
        candidate_peer_id: PeerId,
        artifact_id: ArtifactId,
    },
    RejectCandidate {
        validator_peer_id: PeerId,
        candidate_peer_id: PeerId,
        artifact_id: ArtifactId,
    },
    AttestReduction {
        validator_peer_id: PeerId,
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    PromoteHead {
        window_id: WindowId,
        base_head_id: HeadId,
        head_id: HeadId,
        aggregate_artifact_id: ArtifactId,
    },
    RevokePeer {
        peer_id: PeerId,
        revocation_epoch: RevocationEpoch,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormalTraceConformanceReport {
    pub schema_version: String,
    pub scenario: FormalTraceScenario,
    pub event_count: usize,
    pub accepted_candidate_count: usize,
    pub attestation_count: usize,
    pub quorum_certificate_count: usize,
    pub canonical_promotion_count: usize,
    pub lowered_action_count: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FormalTraceConformanceError {
    #[error("unsupported formal trace schema version `{0}`")]
    UnsupportedSchemaVersion(String),
    #[error("revoked peer `{peer_id}` published an update")]
    RevokedPeerPublishedUpdate { peer_id: PeerId },
    #[error(
        "candidate decision references unknown published update for peer `{peer_id}` artifact `{artifact_id}`"
    )]
    UnknownPublishedCandidate {
        peer_id: PeerId,
        artifact_id: ArtifactId,
    },
    #[error(
        "duplicate validator attestation from `{validator_peer_id}` for window `{window_id:?}` aggregate `{aggregate_artifact_id}`"
    )]
    DuplicateAttestation {
        validator_peer_id: PeerId,
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    #[error(
        "quorum certificate for window `{window_id:?}` aggregate `{aggregate_artifact_id}` has no validator peers"
    )]
    EmptyQuorumCertificate {
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    #[error(
        "quorum certificate references validator `{validator_peer_id}` without a prior attestation"
    )]
    MissingAttestationForQuorum {
        validator_peer_id: PeerId,
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    #[error(
        "canonical promotion for window `{window_id:?}` aggregate `{aggregate_artifact_id}` has no prior quorum certificate"
    )]
    MissingQuorumForPromotion {
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    #[error(
        "canonical promotion accepted contributor `{peer_id}` without a prior accepted candidate"
    )]
    PromotionIncludesUnacceptedContributor {
        peer_id: PeerId,
        window_id: WindowId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FormalProtocolEvent {
    UpdatePublished {
        observed_at: DateTime<Utc>,
        peer_id: PeerId,
        window_id: WindowId,
        base_head_id: HeadId,
        artifact_id: ArtifactId,
    },
    ReducerProposalObserved {
        observed_at: DateTime<Utc>,
        reducer_peer_id: PeerId,
        window_id: WindowId,
        base_head_id: HeadId,
        aggregate_artifact_id: ArtifactId,
        contributor_peer_ids: Vec<PeerId>,
    },
    CandidateAccepted {
        observed_at: DateTime<Utc>,
        validator_peer_id: PeerId,
        candidate_peer_id: PeerId,
        window_id: WindowId,
        artifact_id: ArtifactId,
    },
    CandidateRejected {
        observed_at: DateTime<Utc>,
        validator_peer_id: PeerId,
        candidate_peer_id: PeerId,
        window_id: WindowId,
        artifact_id: ArtifactId,
        reason: String,
    },
    ValidationAttested {
        observed_at: DateTime<Utc>,
        validator_peer_id: PeerId,
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
    },
    QuorumCertificateEmitted {
        observed_at: DateTime<Utc>,
        window_id: WindowId,
        aggregate_artifact_id: ArtifactId,
        validator_peer_ids: Vec<PeerId>,
    },
    CanonicalHeadPromoted {
        observed_at: DateTime<Utc>,
        window_id: WindowId,
        base_head_id: HeadId,
        head_id: HeadId,
        aggregate_artifact_id: ArtifactId,
        accepted_contributor_peer_ids: Vec<PeerId>,
    },
    PeerRevoked {
        observed_at: DateTime<Utc>,
        peer_id: PeerId,
        revocation_epoch: RevocationEpoch,
    },
}

pub fn sample_protocol_trace(scenario: FormalTraceScenario) -> FormalProtocolTrace {
    match scenario {
        FormalTraceScenario::MnistSmoke => sample_mnist_smoke_trace(),
        FormalTraceScenario::ReducerAdversarial => sample_reducer_adversarial_trace(),
    }
}

pub fn write_protocol_trace_json(
    path: impl AsRef<Path>,
    trace: &FormalProtocolTrace,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", serde_json::to_string_pretty(trace)?))?;
    Ok(())
}

pub fn write_protocol_trace_conformance_json(
    path: impl AsRef<Path>,
    report: &FormalTraceConformanceReport,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", serde_json::to_string_pretty(report)?))?;
    Ok(())
}

pub fn write_protocol_actions_json(
    path: impl AsRef<Path>,
    actions: &[FormalProtocolAction],
) -> anyhow::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        format!("{}\n", serde_json::to_string_pretty(actions)?),
    )?;
    Ok(())
}

pub fn lower_protocol_event(event: &FormalProtocolEvent) -> Option<FormalProtocolAction> {
    match event {
        FormalProtocolEvent::UpdatePublished {
            peer_id,
            window_id,
            base_head_id,
            artifact_id,
            ..
        } => Some(FormalProtocolAction::PublishUpdate {
            peer_id: peer_id.clone(),
            window_id: *window_id,
            base_head_id: base_head_id.clone(),
            artifact_id: artifact_id.clone(),
        }),
        FormalProtocolEvent::ReducerProposalObserved {
            reducer_peer_id,
            window_id,
            base_head_id,
            aggregate_artifact_id,
            ..
        } => Some(FormalProtocolAction::PublishReducerProposal {
            peer_id: reducer_peer_id.clone(),
            window_id: *window_id,
            base_head_id: base_head_id.clone(),
            artifact_id: aggregate_artifact_id.clone(),
        }),
        FormalProtocolEvent::CandidateAccepted {
            validator_peer_id,
            candidate_peer_id,
            artifact_id,
            ..
        } => Some(FormalProtocolAction::AcceptCandidate {
            validator_peer_id: validator_peer_id.clone(),
            candidate_peer_id: candidate_peer_id.clone(),
            artifact_id: artifact_id.clone(),
        }),
        FormalProtocolEvent::CandidateRejected {
            validator_peer_id,
            candidate_peer_id,
            artifact_id,
            ..
        } => Some(FormalProtocolAction::RejectCandidate {
            validator_peer_id: validator_peer_id.clone(),
            candidate_peer_id: candidate_peer_id.clone(),
            artifact_id: artifact_id.clone(),
        }),
        FormalProtocolEvent::ValidationAttested {
            validator_peer_id,
            window_id,
            aggregate_artifact_id,
            ..
        } => Some(FormalProtocolAction::AttestReduction {
            validator_peer_id: validator_peer_id.clone(),
            window_id: *window_id,
            aggregate_artifact_id: aggregate_artifact_id.clone(),
        }),
        FormalProtocolEvent::QuorumCertificateEmitted { .. } => None,
        FormalProtocolEvent::CanonicalHeadPromoted {
            window_id,
            base_head_id,
            head_id,
            aggregate_artifact_id,
            ..
        } => Some(FormalProtocolAction::PromoteHead {
            window_id: *window_id,
            base_head_id: base_head_id.clone(),
            head_id: head_id.clone(),
            aggregate_artifact_id: aggregate_artifact_id.clone(),
        }),
        FormalProtocolEvent::PeerRevoked {
            peer_id,
            revocation_epoch,
            ..
        } => Some(FormalProtocolAction::RevokePeer {
            peer_id: peer_id.clone(),
            revocation_epoch: *revocation_epoch,
        }),
    }
}

pub fn lower_protocol_trace(trace: &FormalProtocolTrace) -> Vec<FormalProtocolAction> {
    trace
        .events
        .iter()
        .filter_map(lower_protocol_event)
        .collect()
}

pub fn check_protocol_trace(
    trace: &FormalProtocolTrace,
) -> Result<FormalTraceConformanceReport, FormalTraceConformanceError> {
    if trace.schema_version != FORMAL_TRACE_SCHEMA_VERSION {
        return Err(FormalTraceConformanceError::UnsupportedSchemaVersion(
            trace.schema_version.clone(),
        ));
    }

    let mut revoked_peers = BTreeSet::new();
    let mut published_updates = BTreeSet::new();
    let mut accepted_candidates: BTreeMap<WindowId, BTreeSet<PeerId>> = BTreeMap::new();
    let mut attestations: BTreeMap<(WindowId, ArtifactId), BTreeSet<PeerId>> = BTreeMap::new();
    let mut quorum_certificates = BTreeSet::new();
    let mut canonical_promotion_count = 0usize;

    for event in &trace.events {
        match event {
            FormalProtocolEvent::UpdatePublished {
                peer_id,
                window_id,
                artifact_id,
                ..
            } => {
                if revoked_peers.contains(peer_id) {
                    return Err(FormalTraceConformanceError::RevokedPeerPublishedUpdate {
                        peer_id: peer_id.clone(),
                    });
                }
                published_updates.insert(((*window_id).0, peer_id.clone(), artifact_id.clone()));
            }
            FormalProtocolEvent::CandidateAccepted {
                candidate_peer_id,
                window_id,
                artifact_id,
                ..
            }
            | FormalProtocolEvent::CandidateRejected {
                candidate_peer_id,
                window_id,
                artifact_id,
                ..
            } => {
                if !published_updates.contains(&(
                    (*window_id).0,
                    candidate_peer_id.clone(),
                    artifact_id.clone(),
                )) {
                    return Err(FormalTraceConformanceError::UnknownPublishedCandidate {
                        peer_id: candidate_peer_id.clone(),
                        artifact_id: artifact_id.clone(),
                    });
                }
                if matches!(event, FormalProtocolEvent::CandidateAccepted { .. }) {
                    accepted_candidates
                        .entry(*window_id)
                        .or_default()
                        .insert(candidate_peer_id.clone());
                }
            }
            FormalProtocolEvent::ValidationAttested {
                validator_peer_id,
                window_id,
                aggregate_artifact_id,
                ..
            } => {
                let key = (*window_id, aggregate_artifact_id.clone());
                let entry = attestations.entry(key.clone()).or_default();
                if !entry.insert(validator_peer_id.clone()) {
                    return Err(FormalTraceConformanceError::DuplicateAttestation {
                        validator_peer_id: validator_peer_id.clone(),
                        window_id: *window_id,
                        aggregate_artifact_id: aggregate_artifact_id.clone(),
                    });
                }
            }
            FormalProtocolEvent::QuorumCertificateEmitted {
                validator_peer_ids,
                window_id,
                aggregate_artifact_id,
                ..
            } => {
                if validator_peer_ids.is_empty() {
                    return Err(FormalTraceConformanceError::EmptyQuorumCertificate {
                        window_id: *window_id,
                        aggregate_artifact_id: aggregate_artifact_id.clone(),
                    });
                }
                let key = (*window_id, aggregate_artifact_id.clone());
                let known = attestations.get(&key).cloned().unwrap_or_default();
                for validator_peer_id in validator_peer_ids {
                    if !known.contains(validator_peer_id) {
                        return Err(FormalTraceConformanceError::MissingAttestationForQuorum {
                            validator_peer_id: validator_peer_id.clone(),
                            window_id: *window_id,
                            aggregate_artifact_id: aggregate_artifact_id.clone(),
                        });
                    }
                }
                quorum_certificates.insert(key);
            }
            FormalProtocolEvent::CanonicalHeadPromoted {
                window_id,
                aggregate_artifact_id,
                accepted_contributor_peer_ids,
                ..
            } => {
                let key = (*window_id, aggregate_artifact_id.clone());
                if !quorum_certificates.contains(&key) {
                    return Err(FormalTraceConformanceError::MissingQuorumForPromotion {
                        window_id: *window_id,
                        aggregate_artifact_id: aggregate_artifact_id.clone(),
                    });
                }
                let accepted = accepted_candidates
                    .get(window_id)
                    .cloned()
                    .unwrap_or_default();
                for peer_id in accepted_contributor_peer_ids {
                    if !accepted.contains(peer_id) {
                        return Err(
                            FormalTraceConformanceError::PromotionIncludesUnacceptedContributor {
                                peer_id: peer_id.clone(),
                                window_id: *window_id,
                            },
                        );
                    }
                }
                canonical_promotion_count += 1;
            }
            FormalProtocolEvent::PeerRevoked { peer_id, .. } => {
                revoked_peers.insert(peer_id.clone());
            }
            FormalProtocolEvent::ReducerProposalObserved { .. } => {}
        }
    }

    Ok(FormalTraceConformanceReport {
        schema_version: trace.schema_version.clone(),
        scenario: trace.scenario,
        event_count: trace.events.len(),
        accepted_candidate_count: accepted_candidates.values().map(BTreeSet::len).sum(),
        attestation_count: attestations.values().map(BTreeSet::len).sum(),
        quorum_certificate_count: quorum_certificates.len(),
        canonical_promotion_count,
        lowered_action_count: lower_protocol_trace(trace).len(),
    })
}

fn sample_mnist_smoke_trace() -> FormalProtocolTrace {
    let scope = sample_scope();
    let generated_at = Utc::now();
    let base_head_id = HeadId::new("head-mnist-base");
    let next_head_id = HeadId::new("head-mnist-1");
    let window_id = WindowId(1);
    let trainer_peer = PeerId::new("trainer-1");
    let reducer_peer = PeerId::new("reducer-1");
    let validators = vec![PeerId::new("validator-1"), PeerId::new("validator-2")];
    let update_artifact = ArtifactId::new("artifact-delta-mnist-1");
    let aggregate_artifact = ArtifactId::new("artifact-aggregate-mnist-1");

    FormalProtocolTrace {
        schema_version: FORMAL_TRACE_SCHEMA_VERSION.into(),
        scenario: FormalTraceScenario::MnistSmoke,
        generated_at,
        source: "burn_p2p_testkit::formal_trace::sample_mnist_smoke_trace".into(),
        scope,
        events: vec![
            FormalProtocolEvent::UpdatePublished {
                observed_at: generated_at,
                peer_id: trainer_peer.clone(),
                window_id,
                base_head_id: base_head_id.clone(),
                artifact_id: update_artifact.clone(),
            },
            FormalProtocolEvent::ReducerProposalObserved {
                observed_at: generated_at + Duration::milliseconds(50),
                reducer_peer_id: reducer_peer,
                window_id,
                base_head_id: base_head_id.clone(),
                aggregate_artifact_id: aggregate_artifact.clone(),
                contributor_peer_ids: vec![trainer_peer.clone()],
            },
            FormalProtocolEvent::CandidateAccepted {
                observed_at: generated_at + Duration::milliseconds(75),
                validator_peer_id: validators[0].clone(),
                candidate_peer_id: trainer_peer.clone(),
                window_id,
                artifact_id: update_artifact,
            },
            FormalProtocolEvent::ValidationAttested {
                observed_at: generated_at + Duration::milliseconds(120),
                validator_peer_id: validators[0].clone(),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
            },
            FormalProtocolEvent::ValidationAttested {
                observed_at: generated_at + Duration::milliseconds(130),
                validator_peer_id: validators[1].clone(),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
            },
            FormalProtocolEvent::QuorumCertificateEmitted {
                observed_at: generated_at + Duration::milliseconds(145),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
                validator_peer_ids: validators,
            },
            FormalProtocolEvent::CanonicalHeadPromoted {
                observed_at: generated_at + Duration::milliseconds(165),
                window_id,
                base_head_id,
                head_id: next_head_id,
                aggregate_artifact_id: aggregate_artifact,
                accepted_contributor_peer_ids: vec![trainer_peer],
            },
        ],
    }
}

fn sample_reducer_adversarial_trace() -> FormalProtocolTrace {
    let scope = sample_scope();
    let generated_at = Utc::now();
    let base_head_id = HeadId::new("head-reducer-base");
    let next_head_id = HeadId::new("head-reducer-1");
    let window_id = WindowId(7);
    let honest_peer = PeerId::new("trainer-honest");
    let adversarial_peer = PeerId::new("trainer-adversarial");
    let reducer_peer = PeerId::new("reducer-1");
    let validators = vec![PeerId::new("validator-1"), PeerId::new("validator-2")];
    let honest_artifact = ArtifactId::new("artifact-delta-honest");
    let adversarial_artifact = ArtifactId::new("artifact-delta-adversarial");
    let aggregate_artifact = ArtifactId::new("artifact-aggregate-reducer-1");

    FormalProtocolTrace {
        schema_version: FORMAL_TRACE_SCHEMA_VERSION.into(),
        scenario: FormalTraceScenario::ReducerAdversarial,
        generated_at,
        source: "burn_p2p_testkit::formal_trace::sample_reducer_adversarial_trace".into(),
        scope,
        events: vec![
            FormalProtocolEvent::UpdatePublished {
                observed_at: generated_at,
                peer_id: honest_peer.clone(),
                window_id,
                base_head_id: base_head_id.clone(),
                artifact_id: honest_artifact.clone(),
            },
            FormalProtocolEvent::UpdatePublished {
                observed_at: generated_at + Duration::milliseconds(5),
                peer_id: adversarial_peer.clone(),
                window_id,
                base_head_id: base_head_id.clone(),
                artifact_id: adversarial_artifact.clone(),
            },
            FormalProtocolEvent::ReducerProposalObserved {
                observed_at: generated_at + Duration::milliseconds(35),
                reducer_peer_id: reducer_peer,
                window_id,
                base_head_id: base_head_id.clone(),
                aggregate_artifact_id: aggregate_artifact.clone(),
                contributor_peer_ids: vec![honest_peer.clone(), adversarial_peer.clone()],
            },
            FormalProtocolEvent::CandidateAccepted {
                observed_at: generated_at + Duration::milliseconds(60),
                validator_peer_id: validators[0].clone(),
                candidate_peer_id: honest_peer.clone(),
                window_id,
                artifact_id: honest_artifact,
            },
            FormalProtocolEvent::CandidateRejected {
                observed_at: generated_at + Duration::milliseconds(65),
                validator_peer_id: validators[0].clone(),
                candidate_peer_id: adversarial_peer.clone(),
                window_id,
                artifact_id: adversarial_artifact,
                reason: "similarity_outlier".into(),
            },
            FormalProtocolEvent::ValidationAttested {
                observed_at: generated_at + Duration::milliseconds(110),
                validator_peer_id: validators[0].clone(),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
            },
            FormalProtocolEvent::ValidationAttested {
                observed_at: generated_at + Duration::milliseconds(120),
                validator_peer_id: validators[1].clone(),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
            },
            FormalProtocolEvent::QuorumCertificateEmitted {
                observed_at: generated_at + Duration::milliseconds(135),
                window_id,
                aggregate_artifact_id: aggregate_artifact.clone(),
                validator_peer_ids: validators,
            },
            FormalProtocolEvent::CanonicalHeadPromoted {
                observed_at: generated_at + Duration::milliseconds(150),
                window_id,
                base_head_id: base_head_id.clone(),
                head_id: next_head_id,
                aggregate_artifact_id: aggregate_artifact,
                accepted_contributor_peer_ids: vec![honest_peer],
            },
            FormalProtocolEvent::PeerRevoked {
                observed_at: generated_at + Duration::milliseconds(175),
                peer_id: adversarial_peer,
                revocation_epoch: RevocationEpoch(2),
            },
        ],
    }
}

fn sample_scope() -> FormalTraceScope {
    FormalTraceScope {
        network_id: NetworkId::new("formal-net"),
        study_id: StudyId::new("formal-study"),
        experiment_id: ExperimentId::new("mnist"),
        revision_id: RevisionId::new("pre.8"),
    }
}
