use super::*;
use burn_p2p_core::{BaseCheckpointId, GroupId, RoundCursor, RoundId};

type GradientRoundKey = (
    ExperimentId,
    RevisionId,
    RoundId,
    BaseCheckpointId,
    Option<GroupId>,
    u32,
);

const RETAINED_GRADIENT_ROUNDS_PER_REVISION: usize = 2;

fn gradient_round_key(
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
    round_cursor: &RoundCursor,
) -> GradientRoundKey {
    (
        experiment_id.clone(),
        revision_id.clone(),
        round_cursor.round_id,
        round_cursor.base_checkpoint_id.clone(),
        round_cursor.group_id.clone(),
        round_cursor.num_inner_steps,
    )
}

#[derive(Clone, Debug, Default)]
struct RoundPayloadStore {
    manifests_by_round: BTreeMap<GradientRoundKey, ContentId>,
    manifests: BTreeMap<ContentId, PseudoGradientManifest>,
    chunks: BTreeMap<(ContentId, u32), PseudoGradientChunk>,
}

impl RoundPayloadStore {
    fn publish(&mut self, manifest: PseudoGradientManifest, chunks: Vec<PseudoGradientChunk>) {
        let experiment_id = manifest.experiment_id.clone();
        let revision_id = manifest.revision_id.clone();
        let manifest_id = manifest.manifest_id.clone();
        let round_key = gradient_round_key(
            &manifest.experiment_id,
            &manifest.revision_id,
            &manifest.round_cursor,
        );
        self.manifests.insert(manifest_id.clone(), manifest);
        for chunk in chunks {
            self.chunks
                .insert((manifest_id.clone(), chunk.chunk_index), chunk);
        }
        // Publishing the round index last makes its presence the readiness
        // signal for one fully deposited manifest/chunk set.
        self.manifests_by_round.insert(round_key, manifest_id);
        self.prune_history(&experiment_id, &revision_id);
    }

    fn contains_round(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        round_cursor: &RoundCursor,
    ) -> bool {
        let round_key = gradient_round_key(experiment_id, revision_id, round_cursor);
        self.manifests_by_round
            .get(&round_key)
            .and_then(|manifest_id| self.manifests.get(manifest_id))
            .is_some_and(|manifest| manifest.round_cursor == *round_cursor)
    }

    fn slice(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        round_cursor: &RoundCursor,
        chunk_index: u32,
    ) -> Option<DiLoCoGradientSlice> {
        let round_key = gradient_round_key(experiment_id, revision_id, round_cursor);
        self.manifests_by_round
            .get(&round_key)
            .and_then(|manifest_id| self.manifests.get(manifest_id))
            .filter(|manifest| {
                manifest.experiment_id == *experiment_id
                    && manifest.revision_id == *revision_id
                    && manifest.round_cursor == *round_cursor
            })
            .and_then(|manifest| {
                self.chunks
                    .get(&(manifest.manifest_id.clone(), chunk_index))
                    .cloned()
                    .map(|chunk| DiLoCoGradientSlice {
                        manifest: manifest.clone(),
                        chunk,
                    })
            })
    }

    fn prune_history(&mut self, experiment_id: &ExperimentId, revision_id: &RevisionId) {
        let retained_rounds = self
            .manifests_by_round
            .keys()
            .filter(|(experiment, revision, ..)| {
                experiment == experiment_id && revision == revision_id
            })
            .map(|(_, _, round_id, ..)| *round_id)
            .collect::<BTreeSet<_>>();
        let expired_round_count = retained_rounds
            .len()
            .saturating_sub(RETAINED_GRADIENT_ROUNDS_PER_REVISION);
        let expired_rounds = retained_rounds
            .into_iter()
            .take(expired_round_count)
            .collect::<BTreeSet<_>>();
        if expired_rounds.is_empty() {
            return;
        }

        let expired_keys = self
            .manifests_by_round
            .keys()
            .filter(|(experiment, revision, round_id, ..)| {
                experiment == experiment_id
                    && revision == revision_id
                    && expired_rounds.contains(round_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for key in expired_keys {
            let Some(manifest_id) = self.manifests_by_round.remove(&key) else {
                continue;
            };
            if self
                .manifests_by_round
                .values()
                .any(|retained| retained == &manifest_id)
            {
                continue;
            }
            if let Some(manifest) = self.manifests.remove(&manifest_id) {
                for chunk_index in 0..manifest.chunk_count {
                    self.chunks.remove(&(manifest_id.clone(), chunk_index));
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DiLoCoControlStore {
    state_snapshots: BTreeMap<(ExperimentId, RevisionId), DiLoCoStateSnapshot>,
    outer_optimizer_states: BTreeMap<(ExperimentId, RevisionId), StateBlob>,
    current_parameters: BTreeMap<(ExperimentId, RevisionId), FlattenedTensorPack>,
    latest_offers: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundOffer>,
    latest_heartbeats: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundHeartbeat>,
    latest_finalizations: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundFinalize>,
    aggregate_readiness: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoAggregateReady>,
    gradients: RoundPayloadStore,
    aggregates: RoundPayloadStore,
    aggregate_cohorts: BTreeMap<ContentId, (Vec<PeerId>, Vec<ContentId>)>,
}

impl DiLoCoControlStore {
    pub(crate) fn publish_state(
        &mut self,
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
    ) {
        let key = (snapshot.experiment_id.clone(), snapshot.revision_id.clone());
        if let Some(state) =
            outer_optimizer_state.or_else(|| snapshot.outer_optimizer_state.clone())
        {
            self.outer_optimizer_states.insert(key.clone(), state);
        }
        if let Some(pack) = current_parameters {
            self.current_parameters.insert(key.clone(), pack);
        }
        self.state_snapshots.insert(key, snapshot);
    }

    pub(crate) fn publish_gradient(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) {
        self.gradients.publish(manifest, chunks);
    }

    pub(crate) fn publish_aggregate(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        participant_peer_ids: Vec<PeerId>,
        contribution_manifest_ids: Vec<ContentId>,
    ) {
        let manifest_id = manifest.manifest_id.clone();
        self.aggregates.publish(manifest, chunks);
        self.aggregate_cohorts.insert(
            manifest_id,
            (participant_peer_ids, contribution_manifest_ids),
        );
        self.aggregate_cohorts
            .retain(|manifest_id, _| self.aggregates.manifests.contains_key(manifest_id));
    }

    pub(crate) fn aggregate_ready(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        reducer_peer_id: &PeerId,
        round_cursor: &RoundCursor,
    ) -> Option<DiLoCoAggregateReady> {
        self.aggregate_readiness
            .get(&(
                experiment_id.clone(),
                revision_id.clone(),
                reducer_peer_id.clone(),
            ))
            .filter(|ready| ready.round_cursor == *round_cursor)
            .cloned()
    }

    pub(crate) fn respond(&mut self, request: DiLoCoRequest) -> DiLoCoResponse {
        match request {
            DiLoCoRequest::RoundOffer(offer) => {
                let key = (offer.experiment_id.clone(), offer.revision_id.clone());
                let local_cursor = self
                    .state_snapshots
                    .get(&key)
                    .map(|snapshot| snapshot.round_cursor.clone());
                let accepted = local_cursor.as_ref().is_some_and(|cursor| {
                    cursor.round_id == offer.round_cursor.round_id
                        && cursor.base_checkpoint_id == offer.round_cursor.base_checkpoint_id
                });
                let message = if accepted {
                    self.latest_offers.insert(
                        (
                            offer.experiment_id.clone(),
                            offer.revision_id.clone(),
                            offer.peer_id.clone(),
                        ),
                        (*offer).clone(),
                    );
                    format!(
                        "accepted DiLoCo round offer {} for {}",
                        offer.round_cursor.round_id,
                        offer.peer_id.as_str()
                    )
                } else if let Some(cursor) = local_cursor.as_ref() {
                    format!(
                        "DiLoCo offer mismatch: local round {} base {}, remote round {} base {}",
                        cursor.round_id,
                        cursor.base_checkpoint_id.as_str(),
                        offer.round_cursor.round_id,
                        offer.round_cursor.base_checkpoint_id.as_str()
                    )
                } else {
                    "DiLoCo state snapshot is not published for this revision".into()
                };
                DiLoCoResponse::Ack {
                    accepted,
                    cursor: local_cursor,
                    message,
                }
            }
            DiLoCoRequest::RoundHeartbeat(heartbeat) => {
                let key = (
                    heartbeat.experiment_id.clone(),
                    heartbeat.revision_id.clone(),
                );
                let local_cursor = self
                    .state_snapshots
                    .get(&key)
                    .map(|snapshot| snapshot.round_cursor.clone());
                let accepted = self.gradients.contains_round(
                    &heartbeat.experiment_id,
                    &heartbeat.revision_id,
                    &heartbeat.round_cursor,
                );
                if accepted {
                    self.latest_heartbeats.insert(
                        (
                            heartbeat.experiment_id.clone(),
                            heartbeat.revision_id.clone(),
                            heartbeat.peer_id.clone(),
                        ),
                        (*heartbeat).clone(),
                    );
                }
                let message = if accepted {
                    format!(
                        "recorded retained-gradient DiLoCo heartbeat from {}",
                        heartbeat.peer_id.as_str()
                    )
                } else if let Some(cursor) = local_cursor.as_ref() {
                    format!(
                        "DiLoCo heartbeat not ready: local cursor {:?}, remote cursor {:?}",
                        cursor, heartbeat.round_cursor
                    )
                } else {
                    "DiLoCo state snapshot is not published for this revision".into()
                };
                DiLoCoResponse::Ack {
                    accepted,
                    cursor: accepted
                        .then(|| heartbeat.round_cursor.clone())
                        .or(local_cursor),
                    message,
                }
            }
            DiLoCoRequest::RoundFinalize(finalize) => {
                self.latest_finalizations.insert(
                    (
                        finalize.experiment_id.clone(),
                        finalize.revision_id.clone(),
                        finalize.peer_id.clone(),
                    ),
                    (*finalize).clone(),
                );
                DiLoCoResponse::Ack {
                    accepted: true,
                    cursor: Some(finalize.round_cursor.clone()),
                    message: format!(
                        "recorded DiLoCo finalization from {}",
                        finalize.peer_id.as_str()
                    ),
                }
            }
            DiLoCoRequest::StateSnapshot {
                experiment_id,
                revision_id,
            } => DiLoCoResponse::StateSnapshot(
                self.state_snapshots
                    .get(&(experiment_id, revision_id))
                    .cloned(),
            ),
            DiLoCoRequest::StateBundle {
                experiment_id,
                revision_id,
            } => {
                let key = (experiment_id, revision_id);
                let bundle = self
                    .state_snapshots
                    .get(&key)
                    .cloned()
                    .zip(self.current_parameters.get(&key).cloned())
                    .zip(self.outer_optimizer_states.get(&key).cloned())
                    .map(|((snapshot, current_parameters), outer_optimizer_state)| {
                        Box::new(DiLoCoStateBundle {
                            snapshot,
                            current_parameters,
                            outer_optimizer_state,
                        })
                    });
                DiLoCoResponse::StateBundle(bundle)
            }
            DiLoCoRequest::OuterOptimizerState {
                experiment_id,
                revision_id,
            } => DiLoCoResponse::OuterOptimizerState(
                self.outer_optimizer_states
                    .get(&(experiment_id, revision_id))
                    .cloned(),
            ),
            DiLoCoRequest::GradientManifest { manifest_id } => DiLoCoResponse::GradientManifest(
                self.gradients.manifests.get(&manifest_id).cloned(),
            ),
            DiLoCoRequest::CurrentParameters {
                experiment_id,
                revision_id,
            } => DiLoCoResponse::CurrentParameters(
                self.current_parameters
                    .get(&(experiment_id, revision_id))
                    .cloned(),
            ),
            DiLoCoRequest::GradientChunk {
                manifest_id,
                chunk_index,
            } => DiLoCoResponse::GradientChunk(
                self.gradients
                    .chunks
                    .get(&(manifest_id, chunk_index))
                    .cloned(),
            ),
            DiLoCoRequest::GradientSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index,
            } => {
                let slice =
                    self.gradients
                        .slice(&experiment_id, &revision_id, &round_cursor, chunk_index);
                DiLoCoResponse::GradientSlice(slice.map(Box::new))
            }
            DiLoCoRequest::AggregateSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index,
            } => {
                let slice = self
                    .aggregates
                    .slice(&experiment_id, &revision_id, &round_cursor, chunk_index)
                    .and_then(|slice| {
                        self.aggregate_cohorts
                            .get(&slice.manifest.manifest_id)
                            .cloned()
                            .map(|(participant_peer_ids, contribution_manifest_ids)| {
                                Box::new(DiLoCoAggregateSlice {
                                    manifest: slice.manifest,
                                    chunk: slice.chunk,
                                    participant_peer_ids,
                                    contribution_manifest_ids,
                                })
                            })
                    });
                DiLoCoResponse::AggregateSlice(slice)
            }
            DiLoCoRequest::AggregateReady(ready) => {
                self.aggregate_readiness.insert(
                    (
                        ready.experiment_id.clone(),
                        ready.revision_id.clone(),
                        ready.reducer_peer_id.clone(),
                    ),
                    (*ready).clone(),
                );
                DiLoCoResponse::Ack {
                    accepted: true,
                    cursor: Some(ready.round_cursor.clone()),
                    message: format!(
                        "recorded reduced aggregate {} from {}",
                        ready.aggregate_manifest_id.as_str(),
                        ready.reducer_peer_id.as_str()
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{
        DiLoCoPolicy, DiLoCoRoundHeartbeat, GradientCodec, PeerId, PseudoGradientManifestInput,
        RoundPhase, TrainingProtocol,
    };

    use super::*;

    #[test]
    fn optimizer_state_remains_fetchable_when_live_snapshot_omits_it() {
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let optimizer =
            StateBlob::try_new("test/opaque", vec![5_u8; 1024]).expect("optimizer blob");
        let parameters = FlattenedTensorPack::new(
            ContentId::new("model"),
            ContentId::new("layout"),
            vec![1.0, 2.0],
        );
        let snapshot = DiLoCoStateSnapshot {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            training_protocol: TrainingProtocol::DiLoCo(DiLoCoPolicy::default()),
            round_cursor: RoundCursor::new(BaseCheckpointId::new("base"), 1),
            checkpoint_head_id: None,
            latest_gradient_manifest_id: None,
            current_parameter_checksum: Some(parameters.checksum().expect("parameter checksum")),
            outer_optimizer_state: None,
            signature_bundle: Vec::new(),
            updated_at: Utc::now(),
        };
        let mut store = DiLoCoControlStore::default();

        store.publish_state(
            snapshot.clone(),
            Some(optimizer.clone()),
            Some(parameters.clone()),
        );

        assert_eq!(
            store.respond(DiLoCoRequest::StateSnapshot {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
            }),
            DiLoCoResponse::StateSnapshot(Some(snapshot))
        );
        assert_eq!(
            store.respond(DiLoCoRequest::StateBundle {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
            }),
            DiLoCoResponse::StateBundle(Some(Box::new(DiLoCoStateBundle {
                snapshot: store
                    .state_snapshots
                    .get(&(experiment_id.clone(), revision_id.clone()))
                    .expect("snapshot")
                    .clone(),
                current_parameters: parameters,
                outer_optimizer_state: optimizer.clone(),
            })))
        );
        assert_eq!(
            store.respond(DiLoCoRequest::OuterOptimizerState {
                experiment_id,
                revision_id,
            }),
            DiLoCoResponse::OuterOptimizerState(Some(optimizer))
        );
    }

    #[test]
    fn heartbeat_acknowledges_matching_ready_round_identity() {
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let mut cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        cursor.group_id = Some(GroupId::new("group"));
        cursor.phase = RoundPhase::BuildPseudoGradient;
        let snapshot = DiLoCoStateSnapshot {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            training_protocol: TrainingProtocol::DiLoCo(DiLoCoPolicy::default()),
            round_cursor: cursor.clone(),
            checkpoint_head_id: None,
            latest_gradient_manifest_id: None,
            current_parameter_checksum: None,
            outer_optimizer_state: None,
            signature_bundle: Vec::new(),
            updated_at: Utc::now(),
        };
        let mut store = DiLoCoControlStore::default();
        store.publish_state(snapshot, None, None);

        let mut stale_cursor = cursor.clone();
        stale_cursor.group_id = None;
        let stale = store.respond(DiLoCoRequest::RoundHeartbeat(Box::new(
            DiLoCoRoundHeartbeat {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                peer_id: PeerId::new("peer"),
                round_cursor: stale_cursor,
                observed_participants: 3,
                emitted_at: Utc::now(),
            },
        )));
        assert!(matches!(
            stale,
            DiLoCoResponse::Ack {
                accepted: false,
                cursor: Some(ref local),
                ..
            } if local == &cursor
        ));
        assert!(store.latest_heartbeats.is_empty());

        let not_published = store.respond(DiLoCoRequest::RoundHeartbeat(Box::new(
            DiLoCoRoundHeartbeat {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                peer_id: PeerId::new("peer"),
                round_cursor: cursor.clone(),
                observed_participants: 3,
                emitted_at: Utc::now(),
            },
        )));
        assert!(matches!(
            not_published,
            DiLoCoResponse::Ack {
                accepted: false,
                ..
            }
        ));

        let pack = FlattenedTensorPack::new(
            ContentId::new("model"),
            ContentId::new("layout"),
            vec![1.0, 2.0],
        );
        let created_at = Utc::now();
        let manifest = PseudoGradientManifest::try_new(PseudoGradientManifestInput {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            peer_id: PeerId::new("local"),
            round_cursor: cursor.clone(),
            codec: GradientCodec::Fp32,
            pack: &pack,
            chunk_count: 1,
            total_encoded_bytes: 8,
            created_at,
        })
        .expect("manifest");
        store.publish_gradient(
            manifest.clone(),
            vec![PseudoGradientChunk {
                manifest_id: manifest.manifest_id,
                chunk_index: 0,
                bytes: vec![0; 8],
                generated_at: created_at,
            }],
        );

        let ready = store.respond(DiLoCoRequest::RoundHeartbeat(Box::new(
            DiLoCoRoundHeartbeat {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                peer_id: PeerId::new("peer"),
                round_cursor: cursor.clone(),
                observed_participants: 3,
                emitted_at: Utc::now(),
            },
        )));
        assert!(matches!(
            ready,
            DiLoCoResponse::Ack {
                accepted: true,
                cursor: Some(ref local),
                ..
            } if local == &cursor
        ));
        assert!(store.latest_heartbeats.contains_key(&(
            experiment_id,
            revision_id,
            PeerId::new("peer")
        )));

        store
            .state_snapshots
            .values_mut()
            .next()
            .expect("snapshot")
            .round_cursor
            .phase = RoundPhase::Aggregate;
        let already_aggregating = store.respond(DiLoCoRequest::RoundHeartbeat(Box::new(
            DiLoCoRoundHeartbeat {
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
                peer_id: PeerId::new("peer"),
                round_cursor: cursor.clone(),
                observed_participants: 3,
                emitted_at: Utc::now(),
            },
        )));
        assert!(matches!(
            already_aggregating,
            DiLoCoResponse::Ack { accepted: true, .. }
        ));

        let mut completed = cursor.clone();
        completed.round_id = RoundId::new(cursor.round_id.as_u64() + 1);
        completed.group_id = None;
        completed.phase = RoundPhase::Completed;
        completed.base_checkpoint_id = BaseCheckpointId::new("next-base");
        store
            .state_snapshots
            .values_mut()
            .next()
            .expect("snapshot")
            .round_cursor = completed;
        let retained_after_completion = store.respond(DiLoCoRequest::RoundHeartbeat(Box::new(
            DiLoCoRoundHeartbeat {
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
                peer_id: PeerId::new("late-peer"),
                round_cursor: cursor.clone(),
                observed_participants: 3,
                emitted_at: Utc::now(),
            },
        )));
        assert!(matches!(
            retained_after_completion,
            DiLoCoResponse::Ack {
                accepted: true,
                cursor: Some(ref acknowledged),
                ..
            } if acknowledged == &cursor
        ));
    }

    #[test]
    fn gradient_slice_returns_one_round_scoped_manifest_and_chunk() {
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let peer_id = PeerId::new("peer");
        let mut round_cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        round_cursor.group_id = Some(GroupId::new("group"));
        round_cursor.phase = RoundPhase::BuildPseudoGradient;
        let pack = FlattenedTensorPack::new(
            ContentId::new("model"),
            ContentId::new("layout"),
            vec![1.0, 2.0],
        );
        let created_at = Utc::now();
        let manifest = PseudoGradientManifest::try_new(PseudoGradientManifestInput {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            peer_id,
            round_cursor: round_cursor.clone(),
            codec: GradientCodec::Fp32,
            pack: &pack,
            chunk_count: 2,
            total_encoded_bytes: 8,
            created_at,
        })
        .expect("manifest");
        let chunks = (0..2)
            .map(|chunk_index| PseudoGradientChunk {
                manifest_id: manifest.manifest_id.clone(),
                chunk_index,
                bytes: vec![chunk_index as u8; 4],
                generated_at: created_at,
            })
            .collect::<Vec<_>>();
        let expected = DiLoCoGradientSlice {
            manifest: manifest.clone(),
            chunk: chunks[1].clone(),
        };
        let mut store = DiLoCoControlStore::default();
        store.publish_gradient(manifest, chunks);

        assert_eq!(
            store.respond(DiLoCoRequest::GradientSlice {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                round_cursor: round_cursor.clone(),
                chunk_index: 1,
            }),
            DiLoCoResponse::GradientSlice(Some(Box::new(expected)))
        );
        assert!(matches!(
            store.respond(DiLoCoRequest::GradientSlice {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                round_cursor: round_cursor.clone(),
                chunk_index: 0,
            }),
            DiLoCoResponse::GradientSlice(Some(_))
        ));

        round_cursor.num_inner_steps += 1;
        assert_eq!(
            store.respond(DiLoCoRequest::GradientSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index: 1,
            }),
            DiLoCoResponse::GradientSlice(None)
        );
    }

    #[test]
    fn aggregate_slice_returns_payload_and_aligned_cohort_commitment() {
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let reducer_peer_id = PeerId::new("peer-a");
        let participant_peer_ids = vec![
            reducer_peer_id.clone(),
            PeerId::new("peer-b"),
            PeerId::new("peer-c"),
        ];
        let contribution_manifest_ids = vec![
            ContentId::new("gradient-a"),
            ContentId::new("gradient-b"),
            ContentId::new("gradient-c"),
        ];
        let mut round_cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);
        round_cursor.group_id = Some(GroupId::new("group"));
        round_cursor.phase = RoundPhase::BuildPseudoGradient;
        let pack = FlattenedTensorPack::new(
            ContentId::new("model"),
            ContentId::new("layout"),
            vec![1.0, 2.0],
        );
        let created_at = Utc::now();
        let manifest = PseudoGradientManifest::try_new(PseudoGradientManifestInput {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            peer_id: reducer_peer_id,
            round_cursor: round_cursor.clone(),
            codec: GradientCodec::Fp32,
            pack: &pack,
            chunk_count: 1,
            total_encoded_bytes: 8,
            created_at,
        })
        .expect("aggregate manifest");
        let chunk = PseudoGradientChunk {
            manifest_id: manifest.manifest_id.clone(),
            chunk_index: 0,
            bytes: vec![0; 8],
            generated_at: created_at,
        };
        let expected = DiLoCoAggregateSlice {
            manifest: manifest.clone(),
            chunk: chunk.clone(),
            participant_peer_ids: participant_peer_ids.clone(),
            contribution_manifest_ids: contribution_manifest_ids.clone(),
        };
        let mut store = DiLoCoControlStore::default();
        store.publish_aggregate(
            manifest,
            vec![chunk],
            participant_peer_ids,
            contribution_manifest_ids,
        );
        let ready = DiLoCoAggregateReady {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            reducer_peer_id: PeerId::new("peer-a"),
            round_cursor: round_cursor.clone(),
            aggregate_manifest_id: expected.manifest.manifest_id.clone(),
            participant_peer_ids: expected.participant_peer_ids.clone(),
            contribution_manifest_ids: expected.contribution_manifest_ids.clone(),
            emitted_at: Utc::now(),
        };
        assert_eq!(
            store.respond(DiLoCoRequest::AggregateReady(Box::new(ready.clone()))),
            DiLoCoResponse::Ack {
                accepted: true,
                cursor: Some(round_cursor.clone()),
                message: format!(
                    "recorded reduced aggregate {} from {}",
                    ready.aggregate_manifest_id.as_str(),
                    ready.reducer_peer_id.as_str()
                ),
            }
        );
        assert_eq!(
            store.aggregate_ready(
                &experiment_id,
                &revision_id,
                &ready.reducer_peer_id,
                &round_cursor,
            ),
            Some(ready)
        );

        assert_eq!(
            store.respond(DiLoCoRequest::AggregateSlice {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                round_cursor: round_cursor.clone(),
                chunk_index: 0,
            }),
            DiLoCoResponse::AggregateSlice(Some(Box::new(expected)))
        );

        round_cursor.round_id = RoundId::new(1);
        assert_eq!(
            store.respond(DiLoCoRequest::AggregateSlice {
                experiment_id,
                revision_id,
                round_cursor,
                chunk_index: 0,
            }),
            DiLoCoResponse::AggregateSlice(None)
        );
    }

    #[test]
    fn gradient_history_retains_current_and_previous_round_only() {
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let pack = FlattenedTensorPack::new(
            ContentId::new("model"),
            ContentId::new("layout"),
            vec![1.0, 2.0],
        );
        let created_at = Utc::now();
        let mut cursors = Vec::new();
        let mut store = DiLoCoControlStore::default();

        for round in 0..3 {
            let mut cursor = RoundCursor::new(BaseCheckpointId::new("base"), 4);
            cursor.round_id = RoundId::new(round);
            cursor.group_id = Some(GroupId::new("group"));
            cursor.phase = RoundPhase::BuildPseudoGradient;
            let manifest = PseudoGradientManifest::try_new(PseudoGradientManifestInput {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                peer_id: PeerId::new("peer"),
                round_cursor: cursor.clone(),
                codec: GradientCodec::Fp32,
                pack: &pack,
                chunk_count: 1,
                total_encoded_bytes: 8,
                created_at,
            })
            .expect("manifest");
            store.publish_gradient(
                manifest.clone(),
                vec![PseudoGradientChunk {
                    manifest_id: manifest.manifest_id,
                    chunk_index: 0,
                    bytes: vec![round as u8; 8],
                    generated_at: created_at,
                }],
            );
            cursors.push(cursor);
        }

        assert_eq!(store.gradients.manifests_by_round.len(), 2);
        assert_eq!(store.gradients.manifests.len(), 2);
        assert_eq!(store.gradients.chunks.len(), 2);
        assert_eq!(
            store.respond(DiLoCoRequest::GradientSlice {
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                round_cursor: cursors[0].clone(),
                chunk_index: 0,
            }),
            DiLoCoResponse::GradientSlice(None)
        );
        for cursor in &cursors[1..] {
            assert!(matches!(
                store.respond(DiLoCoRequest::GradientSlice {
                    experiment_id: experiment_id.clone(),
                    revision_id: revision_id.clone(),
                    round_cursor: cursor.clone(),
                    chunk_index: 0,
                }),
                DiLoCoResponse::GradientSlice(Some(_))
            ));
        }
    }
}
