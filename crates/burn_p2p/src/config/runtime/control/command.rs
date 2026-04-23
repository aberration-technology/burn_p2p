use super::*;

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    SubscribeTopic(OverlayTopic),
    PublishControl(ControlAnnouncement),
    PublishLifecycle(Box<ExperimentLifecycleAnnouncement>),
    PublishSchedule(Box<FleetScheduleAnnouncement>),
    PublishHead(HeadAnnouncement),
    PublishLease(LeaseAnnouncement),
    PublishMerge(MergeAnnouncement),
    PublishMergeWindow(MergeWindowAnnouncement),
    PublishReducerAssignment(ReducerAssignmentAnnouncement),
    PublishUpdate(UpdateEnvelopeAnnouncement),
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
    },
    PublishDiLoCoGradient {
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
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
