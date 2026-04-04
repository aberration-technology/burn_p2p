use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
/// Immutable network handle derived from the selected genesis and local roles.
///
/// This type is intentionally lightweight. It gives callers stable helpers for
/// overlay names, protocol derivation, and experiment handle construction
/// without exposing the mutable runtime internals.
pub struct MainnetHandle {
    /// The genesis.
    pub genesis: GenesisSpec,
    /// The roles.
    pub roles: PeerRoleSet,
}

impl MainnetHandle {
    /// Returns the bound network identifier.
    pub fn network_id(&self) -> &NetworkId {
        &self.genesis.network_id
    }

    /// Returns the control overlay topic for this network.
    pub fn control_overlay(&self) -> OverlayTopic {
        OverlayTopic::control(self.network_id().clone())
    }

    /// Returns the control-topic path string.
    pub fn control_topic(&self) -> String {
        self.control_overlay().path
    }

    /// Derives the protocol set for this network.
    pub fn protocol_set(&self) -> Result<ProtocolSet, SwarmError> {
        ProtocolSet::for_network(self.network_id())
    }

    /// Creates an experiment handle scoped to this network.
    pub fn experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> ExperimentHandle {
        ExperimentHandle {
            network_id: self.genesis.network_id.clone(),
            study_id,
            experiment_id,
            revision_id,
        }
    }

    /// Creates a checkpoint-sync handle for the requested target head.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        CheckpointSyncHandle::new(target_head_id)
    }
}

pub(crate) fn dedupe_peer_ids(peers: impl IntoIterator<Item = PeerId>) -> Vec<PeerId> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for peer_id in peers {
        if seen.insert(peer_id.clone()) {
            deduped.push(peer_id);
        }
    }
    deduped
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Addressable handle for one study / experiment / revision tuple on a network.
///
/// Downstream callers use this to derive overlay topics and checkpoint-sync
/// scope for a specific revision without manually reconstructing topic strings.
pub struct ExperimentHandle {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
}

impl ExperimentHandle {
    /// Returns the derived overlay set for this experiment.
    pub fn overlay_set(&self) -> Result<ExperimentOverlaySet, SwarmError> {
        ExperimentOverlaySet::new(
            self.network_id.clone(),
            self.study_id.clone(),
            self.experiment_id.clone(),
        )
    }

    /// Returns the derived overlay topic strings.
    pub fn overlay_topics(&self) -> ExperimentOverlayTopics {
        let overlays = self
            .overlay_set()
            .expect("experiment overlay topic derivation should always be valid");

        ExperimentOverlayTopics {
            heads: overlays.heads.path,
            leases: overlays.leases.path,
            telemetry: overlays.telemetry.path,
            alerts: overlays.alerts.path,
        }
    }

    /// Creates a checkpoint-sync handle scoped to this experiment revision.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        CheckpointSyncHandle::new(target_head_id)
    }

    /// Plans the overlay migration from this experiment to another one.
    pub fn migration_plan(
        &self,
        next: &ExperimentHandle,
        activation: WindowActivation,
        required_client_capabilities: BTreeSet<String>,
        fetch_base_head_id: Option<HeadId>,
    ) -> Result<MigrationPlan, SwarmError> {
        let current = self.overlay_set()?;
        let next = next.overlay_set()?;

        Ok(MigrationCoordinator::plan_overlay_transition(
            &current,
            &next,
            &burn_p2p_experiment::ActivationTarget {
                activation,
                required_client_capabilities,
            },
            fetch_base_head_id,
        ))
    }

    /// Returns whether the receipt belongs to this experiment revision.
    pub fn matches_receipt(&self, receipt: &ContributionReceipt) -> bool {
        receipt.study_id == self.study_id
            && receipt.experiment_id == self.experiment_id
            && receipt.revision_id == self.revision_id
    }

    /// Returns whether the merge certificate belongs to this experiment revision.
    pub fn matches_merge_certificate(&self, cert: &MergeCertificate) -> bool {
        cert.study_id == self.study_id
            && cert.experiment_id == self.experiment_id
            && cert.revision_id == self.revision_id
    }

    /// Returns whether the telemetry summary belongs to this experiment revision.
    pub fn matches_telemetry(&self, telemetry: &TelemetrySummary) -> bool {
        telemetry.network_id == self.network_id
            && telemetry.study_id == self.study_id
            && telemetry.experiment_id == self.experiment_id
            && telemetry.revision_id == self.revision_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Serializable overlay-topic view for one experiment revision.
pub struct ExperimentOverlayTopics {
    /// The heads topic path.
    pub heads: String,
    /// The leases topic path.
    pub leases: String,
    /// The telemetry topic path.
    pub telemetry: String,
    /// The alerts topic path.
    pub alerts: String,
}

/// Alias for the peer-role set used by the runtime facade.
pub type RoleSet = PeerRoleSet;

#[derive(Clone, Debug, PartialEq, Eq)]
/// Handle for planning checkpoint synchronization toward one target head.
pub struct CheckpointSyncHandle {
    /// The target head ID.
    pub target_head_id: HeadId,
}

impl CheckpointSyncHandle {
    /// Creates a new checkpoint-sync handle.
    pub fn new(target_head_id: HeadId) -> Self {
        Self { target_head_id }
    }

    /// Computes the manifest/chunk sync plan against the local checkpoint state.
    pub fn plan(
        &self,
        catalog: &CheckpointCatalog,
        local_heads: BTreeSet<HeadId>,
        local_artifacts: BTreeSet<ArtifactId>,
        local_chunks: BTreeSet<ChunkId>,
    ) -> Result<SyncPlan, CheckpointError> {
        catalog.plan_sync(SyncRequest {
            target_head_id: self.target_head_id.clone(),
            local_heads,
            local_artifacts,
            local_chunks,
        })
    }
}
