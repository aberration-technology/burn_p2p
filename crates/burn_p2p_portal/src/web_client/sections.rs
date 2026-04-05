use burn_p2p_ui::{
    BrowserAppClientView, BrowserAppExperimentSummary, BrowserAppLeaderboardPreview,
};
use dioxus::prelude::*;

#[component]
pub(crate) fn ViewerSections(
    view: BrowserAppClientView,
    on_select_experiment: EventHandler<(String, String, bool, bool)>,
    on_enable_validate: EventHandler<MouseEvent>,
    on_enable_train: EventHandler<MouseEvent>,
) -> Element {
    let selected_key = view
        .selected_experiment
        .as_ref()
        .map(|experiment| {
            format!(
                "{}/{}",
                experiment.experiment_id.as_str(),
                experiment.revision_id.as_str()
            )
        })
        .unwrap_or_default();

    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "viewer",
                    title: "Overview",
                    detail: "Directory, heads, and standings.",
                }
                if let Some(selected) = view.selected_experiment.clone() {
                    ExperimentHero {
                        experiment: selected,
                        validation_available: view.validation.validate_available,
                        training_available: view.training.train_available,
                        can_validate: view.validation.can_validate,
                        can_train: view.training.can_train,
                        on_enable_validate: on_enable_validate,
                        on_enable_train: on_enable_train,
                    }
                } else {
                    EmptyState {
                        title: "Waiting for signed state",
                        detail: "The first directory snapshot is still syncing.",
                    }
                }
                div { class: "browser-metric-band",
                    StatTile { label: "experiments", value: view.viewer.visible_experiments.to_string(), detail: Some("visible".into()) }
                    StatTile { label: "heads", value: view.viewer.visible_heads.to_string(), detail: Some("current".into()) }
                    StatTile { label: "leaderboard", value: view.viewer.leaderboard_entries.to_string(), detail: Some("ranked".into()) }
                    StatTile { label: "receipts", value: view.network.accepted_receipts.to_string(), detail: Some("accepted".into()) }
                }
                if !view.viewer.experiments_preview.is_empty() {
                    div { class: "directory-list",
                        for experiment in view.viewer.experiments_preview.clone() {
                            DirectoryRow {
                                selected: format!(
                                    "{}/{}",
                                    experiment.experiment_id.as_str(),
                                    experiment.revision_id.as_str()
                                ) == selected_key,
                                experiment: experiment.clone(),
                                onclick: move |_| {
                                    on_select_experiment.call((
                                        experiment.experiment_id.clone(),
                                        experiment.revision_id.clone(),
                                        experiment.validate_available,
                                        experiment.train_available,
                                    ))
                                },
                            }
                        }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "leaderboard",
                        title: "Top participants",
                        detail: "Latest signed scores.",
                    }
                    if view.viewer.leaderboard_preview.is_empty() {
                        EmptyState {
                            title: "Leaderboard pending",
                            detail: "No signed leaderboard snapshot yet.",
                        }
                    } else {
                        div { class: "leaderboard-list",
                            for row in view.viewer.leaderboard_preview.clone() {
                                LeaderboardRow { row }
                            }
                        }
                    }
                }
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "focus",
                        title: "In focus",
                        detail: "Selected revision and local mode.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "No revision selected",
                            detail: "Pick a revision from the directory.",
                        }
                    }
                    KeyValueList {
                        KeyValueRow { label: "mode", value: view.runtime_label.clone() }
                        KeyValueRow { label: "capability", value: view.capability_summary.clone() }
                        KeyValueRow { label: "transport", value: view.network.transport.clone() }
                        KeyValueRow {
                            label: "sync",
                            value: view.network.last_directory_sync_at.clone().unwrap_or_else(|| "pending".into()),
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn ValidateSections(
    view: BrowserAppClientView,
    on_enable_validate: EventHandler<MouseEvent>,
) -> Element {
    let head = view
        .validation
        .current_head_id
        .clone()
        .unwrap_or_else(|| "No checkpoint yet".into());
    let status = view
        .validation
        .validation_status
        .clone()
        .unwrap_or_else(|| "idle".into());
    let summary = view
        .validation
        .evaluation_summary
        .clone()
        .unwrap_or_else(|| "Waiting for evaluation input.".into());

    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "validate",
                    title: "Validation",
                    detail: "Checkpoint checks and receipts.",
                }
                article { class: "browser-spotlight",
                    div { class: "browser-spotlight-top",
                        div { class: "browser-spotlight-copy",
                            div { class: "eyebrow", "checkpoint" }
                            h2 { class: "browser-focus-title", "{head}" }
                            p { class: "section-detail", "{summary}" }
                        }
                        span {
                            class: if view.validation.can_validate {
                                "status-pill status-pill-accent"
                            } else {
                                "status-pill status-pill-neutral"
                            },
                            "{status}"
                        }
                    }
                    div { class: "browser-metric-band",
                        StatTile {
                            label: "status",
                            value: if view.validation.can_validate {
                                "active".to_owned()
                            } else if view.validation.validate_available {
                                "available".to_owned()
                            } else {
                                "unavailable".to_owned()
                            },
                            detail: Some(view.runtime_label.clone()),
                        }
                        StatTile {
                            label: "checked",
                            value: view.validation.checked_chunks.map(|value| value.to_string()).unwrap_or_else(|| "—".into()),
                            detail: Some("chunks".into()),
                        }
                        StatTile {
                            label: "receipts",
                            value: view.validation.pending_receipts.to_string(),
                            detail: Some("pending".into()),
                        }
                        StatTile {
                            label: "sync",
                            value: view.validation.metrics_sync_at.clone().unwrap_or_else(|| "pending".into()),
                            detail: Some("metrics".into()),
                        }
                    }
                    if !view.validation.can_validate {
                        if view.validation.validate_available {
                            div { class: "browser-action-row",
                                ActionButton {
                                    label: "Enable validation",
                                    tone: "primary",
                                    onclick: move |event| on_enable_validate.call(event),
                                }
                            }
                        } else {
                            p { class: "muted", "Validation is not available for this revision on this device." }
                        }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "selection",
                        title: "Current revision",
                        detail: "The revision currently in scope.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "No revision selected",
                            detail: "Choose a revision before enabling validation.",
                        }
                    }
                }
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "source",
                        title: "Network",
                        detail: "Current transport and receipt source.",
                    }
                    KeyValueList {
                        KeyValueRow { label: "edge", value: short_edge_label(&view.network.edge_base_url) }
                        KeyValueRow { label: "transport", value: view.network.transport.clone() }
                        KeyValueRow {
                            label: "receipt",
                            value: view.validation.emitted_receipt_id.clone().unwrap_or_else(|| "not emitted".into()),
                        }
                        KeyValueRow {
                            label: "errors",
                            value: view.network.last_error.clone().unwrap_or_else(|| "none".into()),
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn TrainSections(
    view: BrowserAppClientView,
    on_enable_train: EventHandler<MouseEvent>,
) -> Element {
    let throughput = view
        .training
        .throughput_summary
        .clone()
        .unwrap_or_else(|| "Waiting for the first window".into());
    let assignment = view
        .training
        .active_assignment
        .clone()
        .unwrap_or_else(|| "No active assignment".into());
    let window_value = match (
        view.training.last_window_secs,
        view.training.max_window_secs,
    ) {
        (Some(window), Some(max)) => format!("{window}/{max}s"),
        (Some(window), None) => format!("{window}s"),
        (None, Some(max)) => format!("{max}s target"),
        (None, None) => "pending".into(),
    };

    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "train",
                    title: "Training",
                    detail: "Throughput and the current work slice.",
                }
                article { class: "browser-spotlight",
                    div { class: "browser-spotlight-top",
                        div { class: "browser-spotlight-copy",
                            div { class: "eyebrow", "throughput" }
                            h2 { class: "browser-focus-title browser-focus-metric", "{throughput}" }
                            p { class: "section-detail", "{assignment}" }
                        }
                        span {
                            class: if view.training.can_train {
                                "status-pill status-pill-accent"
                            } else {
                                "status-pill status-pill-neutral"
                            },
                            if view.training.can_train {
                                "active"
                            } else if view.training.train_available {
                                "available"
                            } else {
                                "unavailable"
                            }
                        }
                    }
                    div { class: "browser-metric-band",
                        StatTile {
                            label: "checkpoint",
                            value: view.training.latest_head_id.clone().unwrap_or_else(|| "pending".into()),
                            detail: Some("latest".into()),
                        }
                        StatTile {
                            label: "steps",
                            value: view.training.optimizer_steps.map(|value| value.to_string()).unwrap_or_else(|| "—".into()),
                            detail: Some("optimizer".into()),
                        }
                        StatTile {
                            label: "samples",
                            value: view.training.accepted_samples.map(|value| value.to_string()).unwrap_or_else(|| "—".into()),
                            detail: Some("accepted".into()),
                        }
                        StatTile {
                            label: "loss",
                            value: view.training.last_loss.clone().unwrap_or_else(|| "—".into()),
                            detail: Some("latest".into()),
                        }
                    }
                    div { class: "browser-inline-list",
                        KeyValueRow {
                            label: "window",
                            value: window_value,
                        }
                        KeyValueRow {
                            label: "publish",
                            value: view.training.publish_latency_ms.map(|value| format!("{value} ms")).unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "receipts",
                            value: view.training.pending_receipts.to_string(),
                        }
                        KeyValueRow {
                            label: "transport",
                            value: view.network.transport.clone(),
                        }
                    }
                    if !view.training.can_train {
                        if view.training.train_available {
                            div { class: "browser-action-row",
                                ActionButton {
                                    label: "Enable training",
                                    tone: "primary",
                                    onclick: move |event| on_enable_train.call(event),
                                }
                            }
                        } else {
                            p { class: "muted", "Training is not available for this revision on this device." }
                        }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "selection",
                        title: "Current revision",
                        detail: "The revision this browser is following.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "No revision selected",
                            detail: "Choose a revision before enabling training.",
                        }
                    }
                }
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "latest output",
                        title: "Publish",
                        detail: "Most recent local output.",
                    }
                    KeyValueList {
                        KeyValueRow {
                            label: "artifact",
                            value: view.training.last_artifact_id.clone().unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "receipt",
                            value: view.training.last_receipt_id.clone().unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow { label: "artifacts", value: view.training.cached_chunk_artifacts.to_string() }
                        KeyValueRow { label: "microshards", value: view.training.cached_microshards.to_string() }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn NetworkSections(view: BrowserAppClientView) -> Element {
    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "network",
                    title: "Connection",
                    detail: "Transport, peers, and sync.",
                }
                article { class: "browser-spotlight",
                    div { class: "browser-spotlight-top",
                        div { class: "browser-spotlight-copy",
                            div { class: "eyebrow", "transport" }
                            h2 { class: "browser-focus-title", "{view.network.transport}" }
                            p { class: "section-detail", "{view.network.network_note}" }
                        }
                        span { class: "status-pill status-pill-neutral", "{view.network.node_state}" }
                    }
                    div { class: "browser-metric-band",
                        StatTile { label: "direct", value: view.network.direct_peers.to_string(), detail: Some("connected".into()) }
                        StatTile { label: "visible", value: view.network.observed_peers.to_string(), detail: Some("edge scope".into()) }
                        StatTile { label: "network", value: view.network.estimated_network_size.to_string(), detail: Some("estimated".into()) }
                        StatTile { label: "merges", value: view.network.certified_merges.to_string(), detail: Some("signed".into()) }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "sync",
                        title: "Sync",
                        detail: "What is fresh on this browser right now.",
                    }
                    KeyValueList {
                        KeyValueRow {
                            label: "directory",
                            value: view.network.last_directory_sync_at.clone().unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "metrics",
                            value: if view.network.metrics_live_ready {
                                "ready".to_owned()
                            } else {
                                "pending".to_owned()
                            },
                        }
                        KeyValueRow {
                            label: "receipts",
                            value: view.network.accepted_receipts.to_string(),
                        }
                        KeyValueRow {
                            label: "error",
                            value: view.network.last_error.clone().unwrap_or_else(|| "none".into()),
                        }
                    }
                }
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "selection",
                        title: "Current revision",
                        detail: "The experiment currently in scope.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "No revision selected",
                            detail: "Choose a revision to scope the browser client.",
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn SectionHeader(eyebrow: &'static str, title: &'static str, detail: &'static str) -> Element {
    rsx! {
        header { class: "section-header",
            div { class: "eyebrow", "{eyebrow}" }
            h2 { "{title}" }
            p { class: "section-detail", "{detail}" }
        }
    }
}

#[component]
fn StatTile(label: &'static str, value: String, detail: Option<String>) -> Element {
    rsx! {
        article { class: "stat-tile",
            span { "{label}" }
            strong { "{value}" }
            if let Some(detail) = detail {
                p { class: "muted stat-detail", "{detail}" }
            }
        }
    }
}

#[component]
fn ExperimentHero(
    experiment: BrowserAppExperimentSummary,
    validation_available: bool,
    training_available: bool,
    can_validate: bool,
    can_train: bool,
    on_enable_validate: EventHandler<MouseEvent>,
    on_enable_train: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        article { class: "experiment-hero",
            div { class: "experiment-hero-copy",
                div { class: "eyebrow", "{experiment.workload_id}" }
                h3 { "{experiment.display_name}" }
                p { class: "section-detail", "{experiment.experiment_id} · {experiment.revision_id}" }
                CapabilityStrip { experiment: experiment.clone() }
                if (validation_available && !can_validate) || (training_available && !can_train) {
                    div { class: "browser-action-row experiment-hero-actions",
                        if validation_available && !can_validate {
                            ActionButton {
                                label: "Validate",
                                tone: "primary",
                                onclick: move |event| on_enable_validate.call(event),
                            }
                        }
                        if training_available && !can_train {
                            ActionButton {
                                label: "Train",
                                tone: "primary",
                                onclick: move |event| on_enable_train.call(event),
                            }
                        }
                    }
                }
            }
            div { class: "experiment-hero-meta",
                KeyValueRow {
                    label: "head",
                    value: experiment.current_head_id.unwrap_or_else(|| "pending".into()),
                }
                KeyValueRow { label: "access", value: experiment.availability }
            }
        }
    }
}

#[component]
fn ExperimentCard(experiment: BrowserAppExperimentSummary) -> Element {
    rsx! {
        article { class: "experiment-card",
            div { class: "experiment-title-row",
                strong { "{experiment.display_name}" }
                span { class: "pill subtle-pill", "{experiment.workload_id}" }
            }
            p { class: "muted experiment-meta", "{experiment.experiment_id} · {experiment.revision_id}" }
            CapabilityStrip { experiment: experiment.clone() }
            div { class: "experiment-kv",
                KeyValueRow {
                    label: "head",
                    value: experiment.current_head_id.unwrap_or_else(|| "pending".into()),
                }
                KeyValueRow { label: "access", value: experiment.availability }
            }
        }
    }
}

#[component]
fn DirectoryRow(
    experiment: BrowserAppExperimentSummary,
    selected: bool,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    let head = experiment
        .current_head_id
        .clone()
        .unwrap_or_else(|| "pending".into());

    rsx! {
        button {
            r#type: "button",
            class: if selected {
                "directory-row is-selected"
            } else {
                "directory-row"
            },
            onclick: move |event| onclick.call(event),
            div { class: "directory-row-main",
                div { class: "directory-row-title",
                    strong { "{experiment.display_name}" }
                    span { class: "pill subtle-pill", "{experiment.workload_id}" }
                }
                p { class: "muted experiment-meta", "{experiment.experiment_id} · {experiment.revision_id}" }
                CapabilityStrip { experiment: experiment.clone() }
            }
            div { class: "directory-row-side",
                div { class: "directory-row-value",
                    span { "head" }
                    strong { "{head}" }
                }
                div { class: "directory-row-value directory-row-access",
                    span { "access" }
                    strong { "{experiment.availability}" }
                }
            }
        }
    }
}

#[component]
fn CapabilityStrip(experiment: BrowserAppExperimentSummary) -> Element {
    rsx! {
        div { class: "capability-strip",
            span { class: "status-pill status-pill-neutral", "view" }
            if experiment.validate_available {
                span { class: "status-pill status-pill-neutral", "validate" }
            }
            if experiment.train_available {
                span { class: "status-pill status-pill-neutral", "train" }
            }
        }
    }
}

#[component]
fn LeaderboardRow(row: BrowserAppLeaderboardPreview) -> Element {
    rsx! {
        article { class: if row.is_local { "leaderboard-row is-local" } else { "leaderboard-row" },
            div {
                strong { "{row.label}" }
                if row.is_local {
                    span { class: "pill subtle-pill", "you" }
                }
            }
            div { class: "leaderboard-meta",
                span { "{row.score}" }
                span { "{row.receipts} receipt(s)" }
            }
        }
    }
}

#[component]
fn KeyValueList(children: Element) -> Element {
    rsx! {
        div { class: "keyvalue-list", {children} }
    }
}

#[component]
fn KeyValueRow(label: &'static str, value: String) -> Element {
    rsx! {
        div { class: "keyvalue-row",
            span { "{label}" }
            strong { "{value}" }
        }
    }
}

#[component]
fn ActionButton(
    label: &'static str,
    tone: &'static str,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        button {
            r#type: "button",
            class: "action-button action-button-{tone}",
            onclick: move |event| onclick.call(event),
            "{label}"
        }
    }
}

#[component]
fn EmptyState(title: &'static str, detail: &'static str) -> Element {
    rsx! {
        div { class: "empty-state",
            strong { "{title}" }
            p { class: "muted", "{detail}" }
        }
    }
}

fn short_edge_label(edge_base_url: &str) -> String {
    let value = edge_base_url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    value.split('/').next().unwrap_or(value).to_owned()
}
