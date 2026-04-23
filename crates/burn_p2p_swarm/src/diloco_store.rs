use super::*;

#[derive(Clone, Debug, Default)]
pub(crate) struct DiLoCoControlStore {
    state_snapshots: BTreeMap<(ExperimentId, RevisionId), DiLoCoStateSnapshot>,
    outer_optimizer_states: BTreeMap<(ExperimentId, RevisionId), StateBlob>,
    current_parameters: BTreeMap<(ExperimentId, RevisionId), FlattenedTensorPack>,
    latest_offers: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundOffer>,
    latest_heartbeats: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundHeartbeat>,
    latest_finalizations: BTreeMap<(ExperimentId, RevisionId, PeerId), DiLoCoRoundFinalize>,
    gradient_manifests: BTreeMap<ContentId, PseudoGradientManifest>,
    gradient_chunks: BTreeMap<(ContentId, u32), PseudoGradientChunk>,
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
        let manifest_id = manifest.manifest_id.clone();
        self.gradient_manifests
            .insert(manifest_id.clone(), manifest);
        for chunk in chunks {
            self.gradient_chunks
                .insert((manifest_id.clone(), chunk.chunk_index), chunk);
        }
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
                self.latest_heartbeats.insert(
                    (
                        heartbeat.experiment_id.clone(),
                        heartbeat.revision_id.clone(),
                        heartbeat.peer_id.clone(),
                    ),
                    (*heartbeat).clone(),
                );
                DiLoCoResponse::Ack {
                    accepted: true,
                    cursor: Some(heartbeat.round_cursor.clone()),
                    message: format!(
                        "recorded DiLoCo heartbeat from {}",
                        heartbeat.peer_id.as_str()
                    ),
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
            DiLoCoRequest::OuterOptimizerState {
                experiment_id,
                revision_id,
            } => DiLoCoResponse::OuterOptimizerState(
                self.outer_optimizer_states
                    .get(&(experiment_id, revision_id))
                    .cloned(),
            ),
            DiLoCoRequest::GradientManifest { manifest_id } => {
                DiLoCoResponse::GradientManifest(self.gradient_manifests.get(&manifest_id).cloned())
            }
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
                self.gradient_chunks
                    .get(&(manifest_id, chunk_index))
                    .cloned(),
            ),
        }
    }
}
