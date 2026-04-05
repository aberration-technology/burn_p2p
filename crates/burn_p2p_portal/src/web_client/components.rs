use std::time::Duration;

use burn_p2p_browser::BrowserRuntimeRole;
use burn_p2p_ui::{BrowserAppClientView, BrowserAppSurface};
use dioxus::prelude::*;
use gloo_timers::future::sleep;

use super::{
    bridge,
    sections::{NetworkSections, TrainSections, ValidateSections, ViewerSections},
    state::BrowserAppUiState,
};

#[component]
pub(crate) fn BrowserAppRoot() -> Element {
    let bootstrap = bridge::load_bootstrap_from_document();
    let initial_state = match bootstrap {
        Ok(bootstrap) => {
            let edge_base_url = bridge::load_saved_edge_base_url()
                .unwrap_or_else(|| bridge::resolve_edge_base_url(&bootstrap));
            let mut state = BrowserAppUiState::new(bootstrap, edge_base_url);
            if let Some(surface) = bridge::load_saved_surface() {
                state.activate_surface(surface);
            }
            state
        }
        Err(error) => BrowserAppUiState::boot_error(error),
    };
    let ui_state = use_signal(|| initial_state.clone());
    let edge_base_url = use_signal(|| initial_state.edge_base_url.clone());
    let mut edge_input = use_signal(|| initial_state.edge_base_url.clone());
    let mut edge_editor_open = use_signal(|| false);
    let requested_role = use_signal(|| BrowserRuntimeRole::BrowserObserver);
    let selected_target = use_signal(bridge::load_saved_selection);
    let connection_epoch = use_signal(|| 0_u64);

    use_effect(move || {
        let epoch = connection_epoch();
        let edge_base_url = edge_base_url();
        let requested_role = requested_role();
        let selected_target = selected_target();
        let bootstrap = ui_state.peek().bootstrap.clone();
        if bootstrap.module_entry_path.is_empty() {
            return;
        }

        let mut ui_state = ui_state;
        let connection_epoch = connection_epoch;
        let mut edge_input = edge_input;
        spawn(async move {
            let mut next = ui_state.peek().clone();
            next.start_refresh();
            ui_state.set(next);

            match bridge::connect_controller(&edge_base_url, requested_role, selected_target).await
            {
                Ok(mut controller) => {
                    let mut next = ui_state.peek().clone();
                    next.apply_view(controller.view());
                    edge_input.set(next.edge_base_url.clone());
                    ui_state.set(next);

                    loop {
                        if *connection_epoch.peek() != epoch {
                            break;
                        }

                        sleep(Duration::from_millis(bootstrap.refresh_interval_ms)).await;

                        if *connection_epoch.peek() != epoch {
                            break;
                        }

                        let mut next = ui_state.peek().clone();
                        next.start_refresh();
                        ui_state.set(next);

                        match bridge::refresh_controller(&mut controller).await {
                            Ok(view) => {
                                let mut next = ui_state.peek().clone();
                                next.apply_view(view);
                                edge_input.set(next.edge_base_url.clone());
                                ui_state.set(next);
                            }
                            Err(error) => {
                                bridge::log_error(&error);
                                let mut next = ui_state.peek().clone();
                                next.apply_error(error);
                                ui_state.set(next);
                            }
                        }
                    }
                }
                Err(error) => {
                    bridge::log_error(&error);
                    let mut next = ui_state.peek().clone();
                    next.apply_error(error);
                    ui_state.set(next);
                }
            }
        });
    });

    let state = ui_state();
    let view = state.view_or_placeholder();
    let bootstrap = state.bootstrap.clone();
    let selected_experiment = view.selected_experiment.clone();
    let selected_target_from_view = selection_from_view(&view);
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
    let bootstrap_for_button = bootstrap.clone();
    let connect_edge = move |_| {
        edge_editor_open.set(false);
        reconnect_edge(
            ui_state,
            edge_base_url,
            edge_input,
            connection_epoch,
            &bootstrap_for_button,
        );
    };
    let bootstrap_for_enter = bootstrap.clone();
    let connect_edge_on_enter = move |event: KeyboardEvent| {
        if event.key() == Key::Enter {
            edge_editor_open.set(false);
            reconnect_edge(
                ui_state,
                edge_base_url,
                edge_input,
                connection_epoch,
                &bootstrap_for_enter,
            );
        }
    };
    let hero_subtitle = hero_subtitle(&view, state.active_surface);
    let toggle_edge_editor = move |_| {
        let next = !edge_editor_open();
        edge_editor_open.set(next);
    };

    let validate_selection = selected_target_from_view.clone();
    let enable_validate = move |_| {
        if validation_available {
            reconnect_browser_runtime(
                ui_state,
                requested_role,
                selected_target,
                connection_epoch,
                BrowserRuntimeRole::BrowserVerifier,
                selected_target
                    .peek()
                    .clone()
                    .or_else(|| validate_selection.clone()),
                Some(BrowserAppSurface::Validate),
            );
        }
    };

    let train_selection = selected_target_from_view.clone();
    let enable_train = move |_| {
        if training_available {
            reconnect_browser_runtime(
                ui_state,
                requested_role,
                selected_target,
                connection_epoch,
                BrowserRuntimeRole::BrowserTrainerWgpu,
                selected_target
                    .peek()
                    .clone()
                    .or_else(|| train_selection.clone()),
                Some(BrowserAppSurface::Train),
            );
        }
    };

    let on_select_experiment = move |selection: (String, String, bool, bool)| {
        let (experiment_id, revision_id, validate_available, train_available) = selection;
        let next_selection = Some((experiment_id, Some(revision_id)));
        let next_role = match requested_role.peek().clone() {
            BrowserRuntimeRole::BrowserTrainerWgpu if !train_available => {
                BrowserRuntimeRole::BrowserObserver
            }
            BrowserRuntimeRole::BrowserVerifier if !validate_available => {
                BrowserRuntimeRole::BrowserObserver
            }
            current => current,
        };
        let next_surface = match (state.active_surface, &next_role) {
            (BrowserAppSurface::Train, BrowserRuntimeRole::BrowserObserver) => {
                Some(BrowserAppSurface::Viewer)
            }
            (BrowserAppSurface::Validate, BrowserRuntimeRole::BrowserObserver) => {
                Some(BrowserAppSurface::Viewer)
            }
            _ => Some(state.active_surface),
        };
        reconnect_browser_runtime(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            next_role,
            next_selection,
            next_surface,
        );
    };

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
                            active: state.active_surface == BrowserAppSurface::Viewer,
                            available: true,
                            onclick: move |_| switch_surface_requested(
                                ui_state,
                                requested_role,
                                selected_target,
                                connection_epoch,
                                BrowserAppSurface::Viewer,
                                selected_target_from_view.clone(),
                            ),
                        }
                        SurfaceTab {
                            label: "validate",
                            active: state.active_surface == BrowserAppSurface::Validate,
                            available: validation_available,
                            onclick: move |_| switch_surface_requested(
                                ui_state,
                                requested_role,
                                selected_target,
                                connection_epoch,
                                BrowserAppSurface::Validate,
                                None,
                            ),
                        }
                        SurfaceTab {
                            label: "train",
                            active: state.active_surface == BrowserAppSurface::Train,
                            available: training_available,
                            onclick: move |_| switch_surface_requested(
                                ui_state,
                                requested_role,
                                selected_target,
                                connection_epoch,
                                BrowserAppSurface::Train,
                                None,
                            ),
                        }
                        SurfaceTab {
                            label: "network",
                            active: state.active_surface == BrowserAppSurface::Network,
                            available: true,
                            onclick: move |_| switch_surface_requested(
                                ui_state,
                                requested_role,
                                selected_target,
                                connection_epoch,
                                BrowserAppSurface::Network,
                                None,
                            ),
                        }
                    }
                    div { class: "browser-hero-actions",
                        if edge_editor_open() {
                            div { class: "edge-editor",
                                label { class: "edge-connect",
                                    span { class: "toolbar-meta-label", "edge" }
                                    input {
                                        r#type: "text",
                                        value: "{edge_input()}",
                                        placeholder: "https://edge.example",
                                        oninput: move |event| edge_input.set(event.value()),
                                        onkeydown: connect_edge_on_enter,
                                    }
                                }
                                button {
                                    r#type: "button",
                                    class: "action-button action-button-primary edge-connect-button",
                                    onclick: connect_edge,
                                    "Apply"
                                }
                            }
                        } else {
                            div { class: "edge-summary",
                                span { class: "toolbar-meta-label", "edge" }
                                strong { class: "edge-summary-pill", "{edge_summary}" }
                            }
                            button {
                                r#type: "button",
                                class: "action-button action-button-secondary edge-toggle-button",
                                onclick: toggle_edge_editor,
                                "Change"
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
                BrowserAppSurface::Viewer => rsx! {
                    ViewerSections {
                        view: view.clone(),
                        on_select_experiment: on_select_experiment,
                        on_enable_validate: enable_validate,
                        on_enable_train: enable_train,
                    }
                },
                BrowserAppSurface::Validate => rsx! {
                    ValidateSections {
                        view: view.clone(),
                        on_enable_validate: enable_validate,
                    }
                },
                BrowserAppSurface::Train => rsx! {
                    TrainSections {
                        view: view.clone(),
                        on_enable_train: enable_train,
                    }
                },
                BrowserAppSurface::Network => rsx! { NetworkSections { view } },
            }
        }
    }
}

fn activate_surface(mut ui_state: Signal<BrowserAppUiState>, surface: BrowserAppSurface) {
    let mut next = ui_state.peek().clone();
    next.activate_surface(surface);
    ui_state.set(next);
    bridge::save_surface(surface);
}

fn switch_surface_requested(
    ui_state: Signal<BrowserAppUiState>,
    requested_role: Signal<BrowserRuntimeRole>,
    selected_target: Signal<Option<(String, Option<String>)>>,
    connection_epoch: Signal<u64>,
    surface: BrowserAppSurface,
    fallback_selection: Option<(String, Option<String>)>,
) {
    if matches!(surface, BrowserAppSurface::Viewer)
        && requested_role.peek().clone() != BrowserRuntimeRole::BrowserObserver
    {
        reconnect_browser_runtime(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            BrowserRuntimeRole::BrowserObserver,
            selected_target.peek().clone().or(fallback_selection),
            Some(BrowserAppSurface::Viewer),
        );
        return;
    }

    activate_surface(ui_state, surface);
}

fn reconnect_browser_runtime(
    mut ui_state: Signal<BrowserAppUiState>,
    mut requested_role: Signal<BrowserRuntimeRole>,
    mut selected_target: Signal<Option<(String, Option<String>)>>,
    mut connection_epoch: Signal<u64>,
    role: BrowserRuntimeRole,
    selection: Option<(String, Option<String>)>,
    surface: Option<BrowserAppSurface>,
) {
    let current_role = requested_role.peek().clone();
    let current_selection = selected_target.peek().clone();
    if current_role == role && current_selection == selection {
        if let Some(surface) = surface {
            activate_surface(ui_state, surface);
        }
        return;
    }

    requested_role.set(role);
    selected_target.set(selection.clone());
    bridge::save_selection(&selection);
    let mut next = ui_state.peek().clone();
    if let Some(surface) = surface {
        next.activate_surface(surface);
        bridge::save_surface(surface);
    }
    next.start_refresh();
    ui_state.set(next);
    let next_epoch = *connection_epoch.peek() + 1;
    connection_epoch.set(next_epoch);
}

fn reconnect_edge(
    mut ui_state: Signal<BrowserAppUiState>,
    mut edge_base_url: Signal<String>,
    edge_input: Signal<String>,
    mut connection_epoch: Signal<u64>,
    bootstrap: &burn_p2p_ui::BrowserAppStaticBootstrap,
) {
    let next_edge = bridge::normalize_edge_base_url(edge_input.peek().as_str(), bootstrap);
    if next_edge == edge_base_url.peek().as_str() {
        return;
    }
    bridge::save_edge_base_url(&next_edge);
    edge_base_url.set(next_edge.clone());
    let mut next = ui_state.peek().clone();
    next.edge_base_url = next_edge;
    next.start_refresh();
    ui_state.set(next);
    let next_epoch = *connection_epoch.peek() + 1;
    connection_epoch.set(next_epoch);
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

fn selection_from_view(view: &BrowserAppClientView) -> Option<(String, Option<String>)> {
    view.selected_experiment.as_ref().map(|experiment| {
        (
            experiment.experiment_id.clone(),
            Some(experiment.revision_id.clone()),
        )
    })
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

fn hero_subtitle(view: &BrowserAppClientView, surface: BrowserAppSurface) -> String {
    match surface {
        BrowserAppSurface::Viewer => {
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
        BrowserAppSurface::Validate => "Review checkpoints and emit receipts.".into(),
        BrowserAppSurface::Train => "Watch throughput before opting into work.".into(),
        BrowserAppSurface::Network => "Track transport, peers, and sync.".into(),
    }
}
