use burn_p2p::{
    ContentId, ContributionReceipt, ContributionReceiptId, ExperimentId, HeadId, NetworkId,
    RevisionId,
};
use serde::{Deserialize, Serialize};

use crate::{
    BrowserCapabilityReport, BrowserRuntimeConfig, BrowserRuntimeState, BrowserSessionState,
    BrowserStorageSnapshot, BrowserTrainingPlan, BrowserTrainingResult, BrowserTransportStatus,
    BrowserValidationPlan, BrowserValidationResult,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BrowserWorkerCommand {
    Start(BrowserRuntimeConfig),
    Stop,
    Suspend,
    Resume,
    SelectExperiment {
        experiment_id: ExperimentId,
        revision_id: Option<RevisionId>,
    },
    Verify(BrowserValidationPlan),
    Train(BrowserTrainingPlan),
    FlushReceiptOutbox,
    AcknowledgeSubmittedReceipts {
        receipt_ids: Vec<ContributionReceiptId>,
    },
    ClearCaches,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BrowserWorkerEvent {
    Ready(BrowserCapabilityReport),
    RuntimeStateChanged(BrowserRuntimeState),
    TransportChanged(BrowserTransportStatus),
    SessionUpdated(Box<BrowserSessionState>),
    DirectoryUpdated {
        network_id: NetworkId,
        visible_entries: usize,
    },
    HeadUpdated {
        head_id: HeadId,
    },
    ValidationCompleted(BrowserValidationResult),
    TrainingCompleted(BrowserTrainingResult),
    ReceiptOutboxReady {
        session_id: ContentId,
        submit_path: String,
        receipts: Vec<ContributionReceipt>,
    },
    ReceiptsAcknowledged {
        receipt_ids: Vec<ContributionReceiptId>,
        pending_receipts: usize,
    },
    StorageUpdated(Box<BrowserStorageSnapshot>),
    Error {
        message: String,
    },
}
