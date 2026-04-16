use burn_p2p::{
    ContentId, ContributionReceipt, ContributionReceiptId, ExperimentId, HeadDescriptor, HeadId,
    NetworkId, RevisionId,
};
use burn_p2p_core::{BrowserSwarmStatus, MetricsLiveEvent};
use serde::{Deserialize, Serialize};

use crate::{
    BrowserCapabilityReport, BrowserRuntimeConfig, BrowserRuntimeState, BrowserSessionState,
    BrowserStorageSnapshot, BrowserTrainingPlan, BrowserTrainingResult, BrowserTransportStatus,
    BrowserValidationPlan, BrowserValidationResult,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported browser worker command values.
pub enum BrowserWorkerCommand {
    /// Uses the start variant.
    Start(BrowserRuntimeConfig),
    /// Uses the stop variant.
    Stop,
    /// Uses the suspend variant.
    Suspend,
    /// Uses the resume variant.
    Resume,
    /// Uses the select experiment variant.
    SelectExperiment {
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The revision ID.
        revision_id: Option<RevisionId>,
    },
    /// Uses the verify variant.
    Verify(BrowserValidationPlan),
    /// Uses the train variant.
    Train(BrowserTrainingPlan),
    /// Uses the flush receipt outbox variant.
    FlushReceiptOutbox,
    /// Uses the acknowledge submitted receipts variant.
    AcknowledgeSubmittedReceipts {
        /// The receipt IDs.
        receipt_ids: Vec<ContributionReceiptId>,
    },
    /// Uses the apply metrics live event variant.
    ApplyMetricsLiveEvent(Box<MetricsLiveEvent>),
    /// Uses the apply swarm directory snapshot variant.
    ApplySwarmDirectory(Box<crate::BrowserDirectorySnapshot>),
    /// Uses the apply swarm head snapshot variant.
    ApplySwarmHeads(Vec<HeadDescriptor>),
    /// Uses the apply swarm metrics sync variant.
    ApplySwarmMetricsSync(Box<crate::BrowserMetricsSyncState>),
    /// Uses the apply swarm status variant.
    ApplySwarmStatus(Box<BrowserSwarmStatus>),
    /// Uses the clear caches variant.
    ClearCaches,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported browser worker event values.
pub enum BrowserWorkerEvent {
    /// Uses the ready variant.
    Ready(BrowserCapabilityReport),
    /// Uses the runtime state changed variant.
    RuntimeStateChanged(BrowserRuntimeState),
    /// Uses the transport changed variant.
    TransportChanged(BrowserTransportStatus),
    /// Uses the swarm status changed variant.
    SwarmStatusChanged(Box<BrowserSwarmStatus>),
    /// Uses the session updated variant.
    SessionUpdated(Box<BrowserSessionState>),
    /// Uses the directory updated variant.
    DirectoryUpdated {
        /// The network ID.
        network_id: NetworkId,
        /// The visible entries.
        visible_entries: usize,
    },
    /// Uses the head updated variant.
    HeadUpdated {
        /// The head ID.
        head_id: HeadId,
    },
    /// Uses the validation completed variant.
    ValidationCompleted(BrowserValidationResult),
    /// Uses the training completed variant.
    TrainingCompleted(BrowserTrainingResult),
    /// Uses the receipt outbox ready variant.
    ReceiptOutboxReady {
        /// The session ID.
        session_id: ContentId,
        /// The submit path.
        submit_path: String,
        /// The receipts.
        receipts: Vec<ContributionReceipt>,
    },
    /// Uses the receipts acknowledged variant.
    ReceiptsAcknowledged {
        /// The receipt IDs.
        receipt_ids: Vec<ContributionReceiptId>,
        /// The pending receipts.
        pending_receipts: usize,
    },
    /// Uses the storage updated variant.
    StorageUpdated(Box<BrowserStorageSnapshot>),
    /// Uses the metrics updated variant.
    MetricsUpdated(Box<MetricsLiveEvent>),
    /// Uses the error variant.
    Error {
        /// The message.
        message: String,
    },
}
