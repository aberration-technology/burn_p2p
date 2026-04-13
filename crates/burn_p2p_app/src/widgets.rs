use burn_p2p_views::{
    AdminSessionSummaryView, BrowserAppNetworkView, ContributionIdentityPanel,
    DirectoryEntryDraftView, DirectoryMutationResultView, ExperimentDirectoryListView,
    ExperimentPickerView, LifecycleAssignmentStatusView, ParticipantAppView, RolloutPreviewView,
    RuntimeCapabilitySummaryView, TrainingResultSummaryView,
};
use dioxus::prelude::*;

#[component]
/// Renders a compact session and trust card for downstream app shells.
pub fn AuthSessionCard(session: Option<ContributionIdentityPanel>) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-auth-session-card",
            h3 { class: "burn-p2p-widget-title", "session" }
            if let Some(session) = session {
                p { class: "burn-p2p-widget-value", "{session.principal_id}" }
                p { class: "burn-p2p-widget-detail", "provider: {session.provider_label}" }
                if session.trust_badges.is_empty() {
                    p { class: "burn-p2p-widget-detail", "trust: none" }
                } else {
                    ul { class: "burn-p2p-inline-list",
                        for badge in session.trust_badges {
                            li { "{badge.label}" }
                        }
                    }
                }
                if session.scoped_experiments.is_empty() {
                    p { class: "burn-p2p-widget-detail", "scopes: none" }
                } else {
                    p { class: "burn-p2p-widget-detail",
                        "scopes: {session.scoped_experiments.len()} experiment(s)"
                    }
                }
            } else {
                p { class: "burn-p2p-widget-empty", "no active session" }
            }
        }
    }
}

#[component]
/// Renders one compact admin/session summary card.
pub fn AdminSessionCard(session: AdminSessionSummaryView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-admin-session-card",
            h3 { class: "burn-p2p-widget-title", "operator session" }
            p { class: "burn-p2p-widget-value", "{session.session_label}" }
            if let Some(principal_label) = session.principal_label {
                p { class: "burn-p2p-widget-detail", "principal: {principal_label}" }
            }
            if let Some(provider_label) = session.provider_label {
                p { class: "burn-p2p-widget-detail", "provider: {provider_label}" }
            }
            if let Some(session_id) = session.session_id {
                p { class: "burn-p2p-widget-detail", "session: {session_id}" }
            }
            p { class: "burn-p2p-widget-detail",
                if session.rollout_enabled { "rollout: enabled" } else { "rollout: disabled" }
            }
        }
    }
}

#[component]
/// Renders a compact runtime capability summary card.
pub fn RuntimeCapabilityCard(summary: RuntimeCapabilitySummaryView) -> Element {
    let training_status = if summary.can_train {
        "available"
    } else {
        "unavailable"
    };
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-runtime-capability-card",
            h3 { class: "burn-p2p-widget-title", "runtime capability" }
            p { class: "burn-p2p-widget-value", "{summary.preferred_role}" }
            p { class: "burn-p2p-widget-detail", "{summary.backend_summary}" }
            p { class: "burn-p2p-widget-detail", "training: {training_status}" }
        }
    }
}

#[component]
/// Renders one compact training-result panel.
pub fn TrainingResultPanel(result: Option<TrainingResultSummaryView>) -> Element {
    let result_row = result.map(|result| {
        let receipt_label = result
            .receipt_id
            .clone()
            .unwrap_or_else(|| "not emitted".into());
        (result.artifact_id, result.window_secs, receipt_label)
    });
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-training-result-panel",
            h3 { class: "burn-p2p-widget-title", "training result" }
            if let Some((artifact_id, window_secs, receipt_label)) = result_row {
                p { class: "burn-p2p-widget-value", "{artifact_id}" }
                p { class: "burn-p2p-widget-detail", "window: {window_secs}s" }
                p { class: "burn-p2p-widget-detail", "receipt: {receipt_label}" }
            } else {
                p { class: "burn-p2p-widget-empty", "no training result yet" }
            }
        }
    }
}

#[component]
/// Renders one transport and network-health panel.
pub fn TransportHealthPanel(network: BrowserAppNetworkView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-transport-health-panel",
            h3 { class: "burn-p2p-widget-title", "network health" }
            p { class: "burn-p2p-widget-value", "{network.transport}" }
            p { class: "burn-p2p-widget-detail", "{network.edge_base_url}" }
            ul { class: "burn-p2p-inline-list",
                li { "direct {network.direct_peers}" }
                li { "observed {network.observed_peers}" }
                li { "receipts {network.accepted_receipts}" }
                li { "merges {network.certified_merges}" }
            }
            if let Some(error) = network.last_error {
                p { class: "burn-p2p-widget-warning", "{error}" }
            }
        }
    }
}

#[component]
/// Renders a compact experiment and revision selector list.
pub fn ExperimentRevisionSelector(
    picker: ExperimentPickerView,
    selected_experiment_id: Option<String>,
    selected_revision_id: Option<String>,
    select_base_path: Option<String>,
) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-experiment-selector",
            h3 { class: "burn-p2p-widget-title", "experiments" }
            if picker.entries.is_empty() {
                p { class: "burn-p2p-widget-empty", "no visible experiments" }
            } else {
                ul { class: "burn-p2p-selector-list",
                    for entry in picker.entries {
                        {
                            let selected = selected_experiment_id.as_deref() == Some(entry.experiment_id.as_str())
                                && selected_revision_id.as_deref() == Some(entry.current_revision_id.as_str());
                            let row_class = if selected {
                                "burn-p2p-selector-row selected"
                            } else {
                                "burn-p2p-selector-row"
                            };
                            let href = select_base_path.as_ref().map(|base| {
                                format!(
                                    "{base}?experiment_id={}&revision_id={}",
                                    entry.experiment_id.as_str(),
                                    entry.current_revision_id.as_str()
                                )
                            });
                            rsx! {
                                li {
                                    class: row_class,
                                    if let Some(href) = href {
                                        a { href: href, "{entry.display_name}" }
                                    } else {
                                        span { "{entry.display_name}" }
                                    }
                                    span { class: "burn-p2p-selector-meta",
                                        "{entry.experiment_id.as_str()} / {entry.current_revision_id.as_str()}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
/// Renders a compact accepted-receipt summary for one participant-facing snapshot.
pub fn ContributionReceiptSummaryPanel(participant: ParticipantAppView) -> Element {
    let latest = participant.accepted_receipts.first().cloned();
    let latest_row = latest.map(|receipt| {
        (
            receipt.receipt_id.as_str().to_owned(),
            format!("{:.2}", receipt.accepted_weight),
        )
    });
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-receipt-summary-panel",
            h3 { class: "burn-p2p-widget-title", "receipts" }
            p { class: "burn-p2p-widget-value",
                "{participant.accepted_receipts.len()} accepted"
            }
            if let Some((receipt_id, weight_label)) = latest_row {
                p { class: "burn-p2p-widget-detail", "{receipt_id}" }
                p { class: "burn-p2p-widget-detail", "weight: {weight_label}" }
            } else {
                p { class: "burn-p2p-widget-empty", "no accepted receipts yet" }
            }
        }
    }
}

#[component]
/// Renders a lifecycle and assignment status card.
pub fn LifecycleAssignmentStatusCard(status: LifecycleAssignmentStatusView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-lifecycle-status-card",
            h3 { class: "burn-p2p-widget-title", "assignment" }
            p { class: "burn-p2p-widget-value", "{status.experiment_label}" }
            p { class: "burn-p2p-widget-detail", "revision: {status.revision_label}" }
            p { class: "burn-p2p-widget-detail", "phase: {status.lifecycle_phase}" }
            p { class: "burn-p2p-widget-detail", "status: {status.assignment_status}" }
        }
    }
}

#[component]
/// Renders a compact operator-facing directory list.
pub fn ExperimentDirectoryListPanel(view: ExperimentDirectoryListView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-directory-list-panel",
            h3 { class: "burn-p2p-widget-title", "directory" }
            p { class: "burn-p2p-widget-detail",
                "paths: {view.directory_path} | {view.signed_directory_path}"
            }
            if view.entries.is_empty() {
                p { class: "burn-p2p-widget-empty", "no directory entries" }
            } else {
                ul { class: "burn-p2p-selector-list",
                    for entry in view.entries {
                        li { class: "burn-p2p-selector-row",
                            strong { "{entry.display_name}" }
                            span { class: "burn-p2p-selector-meta",
                                "{entry.experiment_id} / {entry.revision_id} / {entry.workload_id}"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
/// Renders an editable directory-entry draft preview.
pub fn DirectoryEntryDraftPanel(draft: DirectoryEntryDraftView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-directory-draft-panel",
            h3 { class: "burn-p2p-widget-title", "directory draft" }
            p { class: "burn-p2p-widget-value", "{draft.display_name}" }
            p { class: "burn-p2p-widget-detail",
                "{draft.study_id} / {draft.experiment_id} / {draft.revision_id}"
            }
            p { class: "burn-p2p-widget-detail", "workload: {draft.workload_id}" }
            p { class: "burn-p2p-widget-detail", "visibility: {draft.visibility}" }
            p { class: "burn-p2p-widget-detail",
                "roles: {draft.allowed_roles.join(\", \")}"
            }
            p { class: "burn-p2p-widget-detail",
                "scopes: {draft.allowed_scopes.join(\", \")}"
            }
            pre { class: "burn-p2p-widget-code", "{draft.metadata_json}" }
        }
    }
}

#[component]
/// Renders a rollout preview summary.
pub fn RolloutPreviewPanel(view: RolloutPreviewView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-rollout-preview-panel",
            h3 { class: "burn-p2p-widget-title", "rollout preview" }
            p { class: "burn-p2p-widget-value", "{view.summary_label}" }
            p { class: "burn-p2p-widget-detail", "submit: {view.submit_path}" }
            p { class: "burn-p2p-widget-detail",
                if view.requires_session { "session required" } else { "session optional" }
            }
            ul { class: "burn-p2p-inline-list",
                for entry in view.entries {
                    li { "{entry.experiment_id}:{entry.revision_id}" }
                }
            }
        }
    }
}

#[component]
/// Renders the last rollout submission result.
pub fn RolloutSubmissionStatusPanel(view: DirectoryMutationResultView) -> Element {
    rsx! {
        article { class: "burn-p2p-widget burn-p2p-rollout-status-panel",
            h3 { class: "burn-p2p-widget-title", "rollout status" }
            p { class: "burn-p2p-widget-value", "{view.status_label}" }
            p { class: "burn-p2p-widget-detail",
                "directory entries: {view.directory_entries}"
            }
            p { class: "burn-p2p-widget-detail",
                "trusted issuers: {view.trusted_issuers}"
            }
            p { class: "burn-p2p-widget-detail",
                if view.reenrollment_required {
                    "re-enrollment required"
                } else {
                    "re-enrollment not required"
                }
            }
        }
    }
}
