use burn_p2p_views::{NodeAppClientView, NodeAppSurface};
use dioxus::prelude::*;

use super::{NetworkSections, NodeAppUiState, TrainSections, ValidateSections, ViewerSections};

#[component]
pub(crate) fn NodeAppShell(
    state: NodeAppUiState,
    on_surface_requested: EventHandler<NodeAppSurface>,
    on_select_experiment: EventHandler<(String, String, bool, bool)>,
    on_enable_validate: Option<EventHandler<MouseEvent>>,
    on_enable_train: Option<EventHandler<MouseEvent>>,
) -> Element {
    let view = state.view_or_placeholder();
    let surface_label = surface_label(state.active_surface);
    let selected_experiment = view.selected_experiment.clone();
    let validation_available = selected_experiment
        .as_ref()
        .is_some_and(|experiment| experiment.validate_available);
    let training_available = selected_experiment
        .as_ref()
        .is_some_and(|experiment| experiment.train_available);
    let active_head = selected_experiment
        .as_ref()
        .and_then(|experiment| experiment.current_head_id.clone())
        .or_else(|| view.training.latest_head_id.clone())
        .or_else(|| view.validation.current_head_id.clone())
        .unwrap_or_else(|| "pending".into());
    let peer_summary = if view.network.estimated_network_size == 0 {
        "awaiting sync".to_owned()
    } else if view.network.direct_peers == 0 {
        format!("~{} visible", view.network.estimated_network_size)
    } else {
        format!(
            "{} direct · ~{} visible",
            view.network.direct_peers, view.network.estimated_network_size
        )
    };
    let sync_summary = format!(
        "{} · {}",
        state.refresh_status,
        short_edge_label(&view.network.edge_base_url)
    );
    let edge_summary = short_edge_label(&view.network.edge_base_url);
    let hero_subtitle = hero_subtitle(&view, state.active_surface);
    let hero_notice = hero_notice(&view, state.active_surface);

    let viewer_surface = move |_| on_surface_requested.call(NodeAppSurface::Viewer);
    let validate_surface = move |_| on_surface_requested.call(NodeAppSurface::Validate);
    let train_surface = move |_| on_surface_requested.call(NodeAppSurface::Train);
    let network_surface = move |_| on_surface_requested.call(NodeAppSurface::Network);

    rsx! {
        main { class: "browser-app-shell",
            section { class: "panel hero browser-hero",
                div { class: "browser-hero-grid",
                    div { class: "browser-hero-copy",
                        div { class: "eyebrow", "burn_p2p" }
                        h1 { class: "app-title",
                            if let Some(selected) = selected_experiment.as_ref() {
                                "{selected.display_name}"
                            } else {
                                "network overview"
                            }
                        }
                        p { class: "app-subtitle", "{hero_subtitle}" }
                        div { class: "badge-row",
                            if view.runtime_label != surface_label {
                                StatusPill { label: view.runtime_label.clone(), tone: "accent" }
                            }
                            StatusPill { label: view.capability_summary.clone(), tone: "neutral" }
                            if !matches!(view.session_label.as_str(), "guest" | "local node")
                                && view.session_label != view.runtime_label
                            {
                                StatusPill { label: view.session_label.clone(), tone: "neutral" }
                            }
                        }
                    }
                    div { class: "browser-quick-grid",
                        QuickCard { label: "head", value: active_head }
                        QuickCard { label: "peers", value: peer_summary }
                        QuickCard {
                            label: "receipts",
                            value: view.network.accepted_receipts.to_string(),
                        }
                    }
                }
                if let Some((label, detail, tone)) = hero_notice {
                    ActivityNotice { label, detail, tone }
                }
                div { class: "browser-hero-bar",
                    nav { class: "surface-nav surface-nav-inline browser-surface-nav",
                        SurfaceTab {
                            label: "overview",
                            active: state.active_surface == NodeAppSurface::Viewer,
                            available: true,
                            onclick: viewer_surface,
                        }
                        SurfaceTab {
                            label: "validate",
                            active: state.active_surface == NodeAppSurface::Validate,
                            available: validation_available,
                            onclick: validate_surface,
                        }
                        SurfaceTab {
                            label: "train",
                            active: state.active_surface == NodeAppSurface::Train,
                            available: training_available,
                            onclick: train_surface,
                        }
                        SurfaceTab {
                            label: "network",
                            active: state.active_surface == NodeAppSurface::Network,
                            available: true,
                            onclick: network_surface,
                        }
                    }
                    div { class: "browser-hero-actions",
                        div { class: "edge-summary",
                            span { class: "toolbar-meta-label", "edge" }
                            strong { class: "edge-summary-pill", "{edge_summary}" }
                        }
                        div { class: "browser-hero-meta",
                            span { class: "toolbar-meta-label", "sync" }
                            strong { "{sync_summary}" }
                        }
                    }
                }
            }
            match state.active_surface {
                NodeAppSurface::Viewer => rsx! {
                    ViewerSections {
                        view: view.clone(),
                        on_select_experiment: on_select_experiment,
                        on_enable_validate: on_enable_validate,
                        on_enable_train: on_enable_train,
                    }
                },
                NodeAppSurface::Validate => rsx! {
                    ValidateSections {
                        view: view.clone(),
                        on_enable_validate: on_enable_validate,
                    }
                },
                NodeAppSurface::Train => rsx! {
                    TrainSections {
                        view: view.clone(),
                        on_enable_train: on_enable_train,
                    }
                },
                NodeAppSurface::Network => rsx! { NetworkSections { view } },
            }
        }
    }
}

#[component]
fn StatusPill(label: String, tone: &'static str) -> Element {
    rsx! {
        span { class: "status-pill status-pill-{tone}", "{label}" }
    }
}

#[component]
fn QuickCard(label: &'static str, value: String) -> Element {
    rsx! {
        div { class: "browser-quick-card",
            span { "{label}" }
            strong { "{value}" }
        }
    }
}

#[component]
fn SurfaceTab(
    label: &'static str,
    active: bool,
    available: bool,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    let class = match (active, available) {
        (true, _) => "surface-tab is-active",
        (false, false) => "surface-tab is-muted",
        (false, true) => "surface-tab",
    };

    rsx! {
        button {
            r#type: "button",
            class: class,
            "data-surface-target": "{label}",
            "aria-pressed": if active { "true" } else { "false" },
            disabled: !available,
            onclick: move |event| {
                if available {
                    onclick.call(event)
                }
            },
            span { class: "surface-tab-label", "{label}" }
        }
    }
}

fn short_edge_label(edge_base_url: &str) -> String {
    if edge_base_url.is_empty() {
        return "unconfigured".into();
    }
    let value = edge_base_url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    value.split('/').next().unwrap_or(value).to_owned()
}

fn hero_subtitle(view: &NodeAppClientView, surface: NodeAppSurface) -> String {
    match surface {
        NodeAppSurface::Viewer => {
            if let Some(selected) = view.selected_experiment.as_ref() {
                format!(
                    "{} · {}",
                    selected.workload_id.as_str(),
                    selected.revision_id.as_str()
                )
            } else {
                "revisions, heads, and standings".into()
            }
        }
        NodeAppSurface::Validate => "checkpoint review and receipt flow".into(),
        NodeAppSurface::Train => "throughput, slice state, and publish flow".into(),
        NodeAppSurface::Network => "transport, peers, and sync".into(),
    }
}

fn surface_label(surface: NodeAppSurface) -> &'static str {
    match surface {
        NodeAppSurface::Viewer => "overview",
        NodeAppSurface::Validate => "validate",
        NodeAppSurface::Train => "train",
        NodeAppSurface::Network => "network",
    }
}

fn hero_notice(
    view: &NodeAppClientView,
    surface: NodeAppSurface,
) -> Option<(String, String, &'static str)> {
    if view.runtime_label.starts_with("joining ") {
        return Some(("joining".into(), view.runtime_detail.clone(), "accent"));
    }
    if view.runtime_label.starts_with("catchup ") || view.runtime_label == "blocked" {
        return Some(("status".into(), view.runtime_detail.clone(), "neutral"));
    }

    match surface {
        NodeAppSurface::Validate
            if view.validation.can_validate && view.validation.current_head_id.is_none() =>
        {
            Some((
                "waiting".into(),
                "waiting for a checkpoint to validate".into(),
                "neutral",
            ))
        }
        NodeAppSurface::Train if view.training.can_train => {
            let detail = match (
                view.training.active_assignment.as_ref(),
                view.training.latest_head_id.as_ref(),
                view.training.cached_microshards,
                view.training.throughput_summary.as_ref(),
            ) {
                (None, _, _, _) => Some("waiting for work".into()),
                (Some(_), None, _, _) => Some("waiting for checkpoint sync".into()),
                (Some(_), Some(_), 0, _) => Some("downloading assigned slice".into()),
                (Some(_), Some(_), _, None) => Some("waiting for the first training window".into()),
                _ => None,
            }?;
            Some(("waiting".into(), detail, "neutral"))
        }
        _ => None,
    }
}

#[component]
fn ActivityNotice(label: String, detail: String, tone: &'static str) -> Element {
    rsx! {
        div { class: "activity-notice activity-notice-{tone}",
            span { class: "activity-notice-label", "{label}" }
            p { class: "activity-notice-detail", "{detail}" }
        }
    }
}
