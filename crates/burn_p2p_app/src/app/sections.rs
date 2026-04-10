use burn_p2p_views::{
    NodeAppClientView, NodeAppDiffusionView, NodeAppExperimentSummary, NodeAppLeaderboardPreview,
    NodeAppPerformanceView,
};
use dioxus::prelude::*;

#[component]
pub(crate) fn ViewerSections(
    view: NodeAppClientView,
    on_select_experiment: EventHandler<(String, String, bool, bool)>,
    on_enable_validate: Option<EventHandler<MouseEvent>>,
    on_enable_train: Option<EventHandler<MouseEvent>>,
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
                    eyebrow: "overview",
                    title: "overview",
                    detail: "directory, heads, and standings.",
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
                        title: "waiting for signed state",
                        detail: "the first directory snapshot is still syncing.",
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
                        title: "top participants",
                        detail: "latest signed scores.",
                    }
                    if view.viewer.leaderboard_preview.is_empty() {
                        EmptyState {
                            title: "leaderboard pending",
                            detail: "no signed leaderboard snapshot yet.",
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
                        title: "in focus",
                        detail: "selected revision and local mode.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "no revision selected",
                            detail: "pick a revision from the directory.",
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
    view: NodeAppClientView,
    on_enable_validate: Option<EventHandler<MouseEvent>>,
) -> Element {
    let head = view
        .validation
        .current_head_id
        .clone()
        .unwrap_or_else(|| "no checkpoint yet".into());
    let status = view
        .validation
        .validation_status
        .clone()
        .unwrap_or_else(|| "idle".into());
    let summary = view
        .validation
        .evaluation_summary
        .clone()
        .unwrap_or_else(|| "waiting for evaluation input.".into());
    let validation_notice = validation_notice(&view);

    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "validate",
                    title: "validation",
                    detail: "checkpoint checks and receipts.",
                }
                if let Some((label, detail, tone)) = validation_notice {
                    InlineNotice { label, detail, tone }
                }
                article { class: "browser-spotlight",
                    div { class: "browser-spotlight-top",
                        div { class: "browser-spotlight-copy",
                            div { class: "eyebrow", "checkpoint" }
                            h2 { class: "browser-focus-title", "{head}" }
                            p { class: "section-detail", "{summary}" }
                        }
                        span {
                            class: match view.validation.can_validate {
                                true => "status-pill status-pill-accent",
                                false => "status-pill status-pill-neutral",
                            },
                            "{status}"
                        }
                    }
                    div { class: "browser-metric-band",
                        StatTile {
                            label: "status",
                            value: match (
                                view.validation.can_validate,
                                view.validation.validate_available,
                            ) {
                                (true, _) => "active".to_owned(),
                                (false, true) => "available".to_owned(),
                                (false, false) => "unavailable".to_owned(),
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
                        if view.validation.validate_available && on_enable_validate.is_some() {
                            div { class: "browser-action-row",
                                ActionButton {
                                    label: "enable validation",
                                    tone: "primary",
                                    onclick: move |event| {
                                        if let Some(handler) = on_enable_validate.as_ref() {
                                            handler.call(event);
                                        }
                                    },
                                }
                            }
                        } else {
                            p { class: "muted", "validation is not available for this revision on this device." }
                        }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    CompactSectionHeader {
                        eyebrow: "selection",
                        detail: "current experiment and revision.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "no revision selected",
                            detail: "choose a revision before enabling validation.",
                        }
                    }
                }
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "source",
                        title: "network",
                        detail: "current transport and receipt source.",
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
                if !view.validation.metric_preview.is_empty() {
                    section { class: "panel compact-panel",
                        SectionHeader {
                            eyebrow: "metrics",
                            title: "latest metrics",
                            detail: "most recent validation metrics for the selected revision.",
                        }
                        div { class: "kv-list",
                            for metric in view.validation.metric_preview.iter() {
                                div { class: "kv-row",
                                    span { class: "kv-label", "{metric.label}" }
                                    span { class: "kv-value", "{metric.value}" }
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
pub(crate) fn TrainSections(
    view: NodeAppClientView,
    on_enable_train: Option<EventHandler<MouseEvent>>,
) -> Element {
    let performance = view.network.performance.clone();
    let throughput = view
        .training
        .throughput_summary
        .clone()
        .unwrap_or_else(|| "waiting for the first window".into());
    let assignment = view
        .training
        .active_assignment
        .clone()
        .unwrap_or_else(|| "no active assignment".into());
    let window_value = match (
        view.training.last_window_secs,
        view.training.max_window_secs,
    ) {
        (Some(window), Some(max)) => format!("{window}/{max}s"),
        (Some(window), None) => format!("{window}s"),
        (None, Some(max)) => format!("{max}s target"),
        (None, None) => "pending".into(),
    };
    let slice_target = view
        .training
        .slice_target_samples
        .map(|value| value.to_string())
        .unwrap_or_else(|| "pending".into());
    let slice_remaining = view
        .training
        .slice_remaining_samples
        .map(|value| value.to_string())
        .unwrap_or_else(|| "pending".into());
    let slice_completed = view
        .training
        .accepted_samples
        .map(|value| value.to_string())
        .unwrap_or_else(|| "pending".into());
    let train_notice = train_notice(&view);

    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                if let Some((label, detail, tone)) = train_notice {
                    InlineNotice { label, detail, tone }
                }
                article { class: "browser-spotlight",
                    div { class: "browser-spotlight-top",
                        div { class: "browser-spotlight-copy",
                            div { class: "eyebrow", "work" }
                            h2 { class: "browser-focus-title browser-focus-metric", "{throughput}" }
                            p { class: "section-detail", "{view.training.slice_status}" }
                        }
                        span {
                            class: match view.training.can_train {
                                true => "status-pill status-pill-accent",
                                false => "status-pill status-pill-neutral",
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
                            label: "samples done",
                            value: slice_completed,
                            detail: Some("accepted".into()),
                        }
                        StatTile {
                            label: "slice left",
                            value: slice_remaining,
                            detail: Some("remaining".into()),
                        }
                        StatTile {
                            label: "microshards",
                            value: view.training.cached_microshards.to_string(),
                            detail: Some("cached".into()),
                        }
                        StatTile {
                            label: "loss",
                            value: view.training.last_loss.clone().unwrap_or_else(|| "—".into()),
                            detail: Some("latest".into()),
                        }
                    }
                    if let Some(performance) = performance.clone() {
                        PerformanceMetricBand { performance: performance.clone() }
                        p { class: "muted", "{performance.scope_summary}" }
                    }
                    div { class: "browser-inline-list",
                        KeyValueRow {
                            label: "assignment",
                            value: assignment,
                        }
                        KeyValueRow {
                            label: "slice target",
                            value: slice_target,
                        }
                        KeyValueRow {
                            label: "window",
                            value: window_value,
                        }
                        KeyValueRow {
                            label: "publish",
                            value: view.training.publish_latency_ms.map(|value| format!("{value} ms")).unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "checkpoint",
                            value: view.training.latest_head_id.clone().unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "receipts",
                            value: view.training.pending_receipts.to_string(),
                        }
                    }
                    if !view.training.can_train {
                        if view.training.train_available && on_enable_train.is_some() {
                            div { class: "browser-action-row",
                                ActionButton {
                                    label: "enable training",
                                    tone: "primary",
                                    onclick: move |event| {
                                        if let Some(handler) = on_enable_train.as_ref() {
                                            handler.call(event);
                                        }
                                    },
                                }
                            }
                        } else {
                            p { class: "muted", "training is not available for this revision on this device." }
                        }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    CompactSectionHeader {
                        eyebrow: "selection",
                        detail: "current experiment and revision.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "no revision selected",
                            detail: "choose a revision before enabling training.",
                        }
                    }
                }
                section { class: "panel compact-panel",
                    CompactSectionHeader {
                        eyebrow: "latest output",
                        detail: "most recent local output.",
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
pub(crate) fn NetworkSections(view: NodeAppClientView) -> Element {
    let performance = view.network.performance.clone();
    let diffusion = view.network.diffusion.clone();
    rsx! {
        div { class: "surface-layout browser-surface-layout",
            section { class: "panel primary-panel browser-focus-panel",
                SectionHeader {
                    eyebrow: "network",
                    title: "connection",
                    detail: "transport, peers, and sync.",
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
                    if let Some(performance) = performance.clone() {
                        PerformanceMetricBand { performance }
                    }
                    if let Some(diffusion) = diffusion.clone() {
                        DiffusionMetricBand { diffusion }
                    }
                }
            }
            aside { class: "support-stack",
                section { class: "panel compact-panel",
                    SectionHeader {
                        eyebrow: "sync",
                        title: "sync",
                        detail: "what is fresh on this node right now.",
                    }
                    KeyValueList {
                        KeyValueRow {
                            label: "directory",
                            value: view.network.last_directory_sync_at.clone().unwrap_or_else(|| "pending".into()),
                        }
                        KeyValueRow {
                            label: "metrics",
                            value: match view.network.metrics_live_ready {
                                true => "ready".to_owned(),
                                false => "pending".to_owned(),
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
                if let Some(performance) = performance {
                    section { class: "panel compact-panel",
                        SectionHeader {
                            eyebrow: "performance",
                            title: "aggregate pace",
                            detail: "selected revision train, validation, and stall timing.",
                        }
                        KeyValueList {
                            KeyValueRow { label: "scope", value: performance.scope_summary }
                            KeyValueRow { label: "captured", value: performance.captured_at }
                            KeyValueRow { label: "wait", value: performance.wait_time }
                            KeyValueRow { label: "idle", value: performance.idle_time }
                        }
                    }
                }
                if let Some(diffusion) = diffusion {
                    section { class: "panel compact-panel",
                        SectionHeader {
                            eyebrow: "diffusion",
                            title: "checkpoint diffusion",
                            detail: "latest canonical head adoption and active fragmentation.",
                        }
                        KeyValueList {
                            KeyValueRow { label: "head", value: diffusion.canonical_head_id }
                            KeyValueRow { label: "captured", value: diffusion.captured_at }
                            KeyValueRow { label: "fragmentation", value: diffusion.fragmentation }
                            KeyValueRow { label: "timeline", value: diffusion.timeline }
                        }
                    }
                }
                section { class: "panel compact-panel",
                    CompactSectionHeader {
                        eyebrow: "selection",
                        detail: "current experiment and revision.",
                    }
                    if let Some(selected) = view.selected_experiment.clone() {
                        ExperimentCard { experiment: selected }
                    } else {
                        EmptyState {
                            title: "no revision selected",
                            detail: "choose a revision to scope the local client.",
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
fn CompactSectionHeader(eyebrow: &'static str, detail: &'static str) -> Element {
    rsx! {
        header { class: "section-header",
            div { class: "eyebrow", "{eyebrow}" }
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
fn PerformanceMetricBand(performance: NodeAppPerformanceView) -> Element {
    rsx! {
        div { class: "browser-metric-band",
            StatTile {
                label: "global train",
                value: performance.training_throughput,
                detail: Some("aggregate".into()),
            }
            StatTile {
                label: "global val",
                value: performance.validation_throughput,
                detail: Some("aggregate".into()),
            }
            StatTile {
                label: "wait",
                value: performance.wait_time,
                detail: Some("residual".into()),
            }
            StatTile {
                label: "idle",
                value: performance.idle_time,
                detail: Some("between windows".into()),
            }
        }
    }
}

#[component]
fn DiffusionMetricBand(diffusion: NodeAppDiffusionView) -> Element {
    rsx! {
        div { class: "browser-metric-band",
            StatTile {
                label: "latest head",
                value: diffusion.canonical_head_id,
                detail: Some("canonical".into()),
            }
            StatTile {
                label: "peer adoption",
                value: diffusion.peer_adoption,
                detail: Some("latest visible".into()),
            }
            StatTile {
                label: "window adoption",
                value: diffusion.recent_window_adoption,
                detail: Some("recent".into()),
            }
            StatTile {
                label: "fragmentation",
                value: diffusion.fragmentation,
                detail: Some("visible".into()),
            }
        }
    }
}

#[component]
fn ExperimentHero(
    experiment: NodeAppExperimentSummary,
    validation_available: bool,
    training_available: bool,
    can_validate: bool,
    can_train: bool,
    on_enable_validate: Option<EventHandler<MouseEvent>>,
    on_enable_train: Option<EventHandler<MouseEvent>>,
) -> Element {
    rsx! {
        article { class: "experiment-hero",
            div { class: "experiment-hero-copy",
                div { class: "eyebrow", "{experiment.workload_id}" }
                h3 { "{experiment.display_name}" }
                p { class: "section-detail", "{experiment.experiment_id} · {experiment.revision_id}" }
                CapabilityStrip { experiment: experiment.clone() }
                if (validation_available && !can_validate && on_enable_validate.is_some())
                    || (training_available && !can_train && on_enable_train.is_some())
                {
                    div { class: "browser-action-row experiment-hero-actions",
                        if validation_available && !can_validate && on_enable_validate.is_some() {
                            ActionButton {
                                label: "validate",
                                tone: "primary",
                                onclick: move |event| {
                                    if let Some(handler) = on_enable_validate.as_ref() {
                                        handler.call(event);
                                    }
                                },
                            }
                        }
                        if training_available && !can_train && on_enable_train.is_some() {
                            ActionButton {
                                label: "train",
                                tone: "primary",
                                onclick: move |event| {
                                    if let Some(handler) = on_enable_train.as_ref() {
                                        handler.call(event);
                                    }
                                },
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
fn ExperimentCard(experiment: NodeAppExperimentSummary) -> Element {
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
    experiment: NodeAppExperimentSummary,
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
            class: match selected {
                true => "directory-row is-selected",
                false => "directory-row",
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
fn CapabilityStrip(experiment: NodeAppExperimentSummary) -> Element {
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
fn LeaderboardRow(row: NodeAppLeaderboardPreview) -> Element {
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

fn short_edge_label(edge_base_url: &str) -> String {
    let value = edge_base_url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    value.split('/').next().unwrap_or(value).to_owned()
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

fn validation_notice(view: &NodeAppClientView) -> Option<(String, String, &'static str)> {
    if view.runtime_label.starts_with("joining ")
        || view.runtime_label.starts_with("catchup ")
        || view.runtime_label == "blocked"
    {
        return Some(("status".into(), view.runtime_detail.clone(), "neutral"));
    }
    if view.validation.can_validate && view.validation.current_head_id.is_none() {
        return Some((
            "waiting".into(),
            "waiting for a checkpoint to review".into(),
            "neutral",
        ));
    }
    None
}

fn train_notice(view: &NodeAppClientView) -> Option<(String, String, &'static str)> {
    if view.runtime_label.starts_with("joining ")
        || view.runtime_label.starts_with("catchup ")
        || view.runtime_label == "blocked"
    {
        return Some(("status".into(), view.runtime_detail.clone(), "neutral"));
    }
    match (
        view.training.can_train,
        view.training.active_assignment.as_ref(),
        view.training.latest_head_id.as_ref(),
        view.training.cached_microshards,
        view.training.throughput_summary.as_ref(),
    ) {
        (true, None, _, _, _) => Some(("waiting".into(), "waiting for work".into(), "neutral")),
        (true, Some(_), None, _, _) => Some((
            "waiting".into(),
            "waiting for checkpoint sync".into(),
            "neutral",
        )),
        (true, Some(_), Some(_), 0, _) => Some((
            "waiting".into(),
            "downloading assigned slice".into(),
            "accent",
        )),
        (true, Some(_), Some(_), _, None) => Some((
            "waiting".into(),
            "waiting for the first training window".into(),
            "neutral",
        )),
        _ => None,
    }
}

#[component]
fn InlineNotice(label: String, detail: String, tone: &'static str) -> Element {
    rsx! {
        div { class: "activity-notice activity-notice-{tone}",
            span { class: "activity-notice-label", "{label}" }
            p { class: "activity-notice-detail", "{detail}" }
        }
    }
}
