use burn_p2p_ui::{NodeAppClientView, NodeAppSurface};
use dioxus::prelude::*;

use super::{NetworkSections, NodeAppUiState, TrainSections, ValidateSections, ViewerSections};

#[component]
pub(crate) fn NodeAppShell(
    state: NodeAppUiState,
    edge_input: String,
    edge_editor_open: bool,
    edge_editable: bool,
    on_surface_requested: EventHandler<NodeAppSurface>,
    on_select_experiment: EventHandler<(String, String, bool, bool)>,
    on_enable_validate: Option<EventHandler<MouseEvent>>,
    on_enable_train: Option<EventHandler<MouseEvent>>,
    on_edge_input: Option<EventHandler<String>>,
    on_apply_edge: Option<EventHandler<()>>,
    on_toggle_edge_editor: Option<EventHandler<()>>,
) -> Element {
    let view = state.view_or_placeholder();
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
                                "Training network"
                            }
                        }
                        p { class: "app-subtitle", "{hero_subtitle}" }
                        div { class: "badge-row",
                            StatusPill { label: view.runtime_label.clone(), tone: "accent" }
                            StatusPill { label: view.capability_summary.clone(), tone: "neutral" }
                            if view.session_label != "viewer" {
                                StatusPill { label: view.session_label.clone(), tone: "neutral" }
                            }
                        }
                    }
                    div { class: "browser-quick-grid",
                        QuickCard { label: "mode", value: view.runtime_label.clone() }
                        QuickCard { label: "head", value: active_head }
                        QuickCard { label: "peers", value: peer_summary }
                    }
                }
                div { class: "browser-hero-bar",
                    nav { class: "surface-nav surface-nav-inline browser-surface-nav",
                        SurfaceTab {
                            label: "viewer",
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
                        if edge_editable && edge_editor_open {
                            div { class: "edge-editor",
                                label { class: "edge-connect",
                                    span { class: "toolbar-meta-label", "edge" }
                                    input {
                                        r#type: "text",
                                        value: "{edge_input}",
                                        placeholder: "https://edge.example",
                                        oninput: move |event| {
                                            if let Some(handler) = on_edge_input.as_ref() {
                                                handler.call(event.value());
                                            }
                                        },
                                        onkeydown: move |event| {
                                            if event.key() == Key::Enter
                                                && let Some(handler) = on_apply_edge.as_ref()
                                            {
                                                handler.call(());
                                            }
                                        },
                                    }
                                }
                                button {
                                    r#type: "button",
                                    class: "action-button action-button-primary edge-connect-button",
                                    onclick: move |event| {
                                        let _ = event;
                                        if let Some(handler) = on_apply_edge.as_ref() {
                                            handler.call(());
                                        }
                                    },
                                    "Apply"
                                }
                            }
                        } else {
                            div { class: "edge-summary",
                                span { class: "toolbar-meta-label", "edge" }
                                strong { class: "edge-summary-pill", "{edge_summary}" }
                            }
                            if edge_editable {
                                button {
                                    r#type: "button",
                                    class: "action-button action-button-secondary edge-toggle-button",
                                    onclick: move |event| {
                                        let _ = event;
                                        if let Some(handler) = on_toggle_edge_editor.as_ref() {
                                            handler.call(());
                                        }
                                    },
                                    if edge_editor_open { "Close" } else { "Change" }
                                }
                            }
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
                "Follow revisions, heads, and standings.".into()
            }
        }
        NodeAppSurface::Validate => "Review checkpoints and emit receipts.".into(),
        NodeAppSurface::Train => "Watch throughput before opting into work.".into(),
        NodeAppSurface::Network => "Track transport, peers, and sync.".into(),
    }
}
