use super::*;

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    SubscribeTopic(OverlayTopic),
    UpdateRoles {
        roles: PeerRoleSet,
        reply: mpsc::Sender<Result<(), String>>,
    },
    AcknowledgeRuntimeError {
        expected: String,
        reply: mpsc::Sender<Result<(), String>>,
    },
    PublishControl(Box<ControlAnnouncement>),
    PublishLifecycle(Box<ExperimentLifecycleAnnouncement>),
    PublishSchedule(Box<FleetScheduleAnnouncement>),
    PublishHead(HeadAnnouncement),
    PublishLease(LeaseAnnouncement),
    PublishMerge(MergeAnnouncement),
    PublishMergeWindow(MergeWindowAnnouncement),
    PublishReducerAssignment(ReducerAssignmentAnnouncement),
    PublishUpdate(Box<UpdateEnvelopeAnnouncement>),
    PublishTrainerPromotionAttestation(TrainerPromotionAttestationAnnouncement),
    PublishDiffusionPromotionCertificate(DiffusionPromotionCertificateAnnouncement),
    PublishAggregateProposal(AggregateProposalAnnouncement),
    PublishReductionCertificate(ReductionCertificateAnnouncement),
    PublishValidationQuorum(ValidationQuorumAnnouncement),
    PublishReducerLoad(ReducerLoadAnnouncement),
    PublishAuth(Box<PeerAuthAnnouncement>),
    PublishDirectory(ExperimentDirectoryAnnouncement),
    PublishMetrics(MetricsAnnouncement),
    PublishDiLoCoState {
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
        reply: mpsc::Sender<Result<(), String>>,
    },
    PublishDiLoCoGradient {
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        reply: mpsc::Sender<Result<(), String>>,
    },
    WaitDiLoCoAggregateReady {
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        reducer_peer_id: PeerId,
        round_cursor: RoundCursor,
        timeout: Duration,
        reply: mpsc::Sender<Result<DiLoCoAggregateReady, String>>,
    },
    PublishDiLoCoAggregate {
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        participant_peer_ids: Vec<PeerId>,
        contribution_manifest_ids: Vec<ContentId>,
        reply: mpsc::Sender<Result<(), String>>,
    },
    PublishArtifact {
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
        reply: mpsc::Sender<Result<(), String>>,
    },
    FetchSnapshot {
        peer_id: String,
        timeout: Duration,
        reply: mpsc::Sender<Result<ControlPlaneSnapshot, String>>,
    },
    FetchArtifactManifest {
        peer_id: String,
        artifact_id: ArtifactId,
        timeout: Duration,
        reply: mpsc::Sender<Result<Option<ArtifactDescriptor>, String>>,
    },
    FetchArtifactChunk {
        peer_id: String,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
        reply: mpsc::Sender<Result<Option<ArtifactChunkPayload>, String>>,
    },
    FetchDiLoCo {
        peer_id: String,
        request: DiLoCoRequest,
        timeout: Duration,
        reply: mpsc::Sender<Result<DiLoCoResponse, String>>,
    },
    DialAddress {
        address: SwarmAddress,
    },
    RequestSnapshot {
        peer_id: String,
    },
    Shutdown,
}
