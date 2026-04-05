use std::time::Duration;

use burn_p2p_browser::BrowserRuntimeRole;
use burn_p2p_views::{NodeAppClientView, NodeAppSurface};
use dioxus::prelude::*;
use gloo_timers::future::sleep;

use crate::app::{NodeAppShell, NodeAppUiState};

use super::bridge;

#[component]
pub(crate) fn BrowserAppRoot() -> Element {
    let bootstrap = bridge::load_bootstrap_from_document();
    let initial_state = match bootstrap {
        Ok(bootstrap) => {
            let edge_base_url = bridge::resolve_edge_base_url(&bootstrap);
            let mut state = NodeAppUiState::new(bootstrap, edge_base_url);
            if let Some(surface) = bridge::load_saved_surface() {
                state.activate_surface(surface);
            }
            state
        }
        Err(error) => NodeAppUiState::boot_error(error),
    };
    let ui_state = use_signal(|| initial_state.clone());
    let edge_base_url = use_signal(|| initial_state.edge_base_url.clone());
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
        spawn(async move {
            let mut next = ui_state.peek().clone();
            next.start_refresh();
            ui_state.set(next);

            match bridge::connect_controller(&edge_base_url, requested_role, selected_target).await
            {
                Ok(mut controller) => {
                    let mut next = ui_state.peek().clone();
                    next.apply_view(controller.view());
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
    let selected_target_from_view = selection_from_view(&state.view_or_placeholder());
    let surface_fallback_selection = selected_target_from_view.clone();
    let validate_fallback_selection = selected_target_from_view.clone();
    let train_fallback_selection = selected_target_from_view.clone();

    let on_surface_requested = move |surface| {
        switch_surface_requested(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            surface,
            surface_fallback_selection.clone(),
        );
    };

    let on_enable_validate = move |event: MouseEvent| {
        let _ = event;
        reconnect_browser_runtime(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            BrowserRuntimeRole::BrowserVerifier,
            selected_target
                .peek()
                .clone()
                .or_else(|| validate_fallback_selection.clone()),
            Some(NodeAppSurface::Validate),
        );
    };

    let on_enable_train = move |event: MouseEvent| {
        let _ = event;
        reconnect_browser_runtime(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            BrowserRuntimeRole::BrowserTrainerWgpu,
            selected_target
                .peek()
                .clone()
                .or_else(|| train_fallback_selection.clone()),
            Some(NodeAppSurface::Train),
        );
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
        let current_surface = ui_state.peek().active_surface;
        let next_surface = match (current_surface, &next_role) {
            (NodeAppSurface::Train, BrowserRuntimeRole::BrowserObserver) => {
                Some(NodeAppSurface::Viewer)
            }
            (NodeAppSurface::Validate, BrowserRuntimeRole::BrowserObserver) => {
                Some(NodeAppSurface::Viewer)
            }
            _ => Some(current_surface),
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
        NodeAppShell {
            state: state.clone(),
            on_surface_requested: on_surface_requested,
            on_select_experiment: on_select_experiment,
            on_enable_validate: on_enable_validate,
            on_enable_train: on_enable_train,
        }
    }
}

fn activate_surface(mut ui_state: Signal<NodeAppUiState>, surface: NodeAppSurface) {
    let mut next = ui_state.peek().clone();
    next.activate_surface(surface);
    ui_state.set(next);
    bridge::save_surface(surface);
}

fn switch_surface_requested(
    ui_state: Signal<NodeAppUiState>,
    requested_role: Signal<BrowserRuntimeRole>,
    selected_target: Signal<Option<(String, Option<String>)>>,
    connection_epoch: Signal<u64>,
    surface: NodeAppSurface,
    fallback_selection: Option<(String, Option<String>)>,
) {
    if matches!(surface, NodeAppSurface::Viewer)
        && requested_role.peek().clone() != BrowserRuntimeRole::BrowserObserver
    {
        reconnect_browser_runtime(
            ui_state,
            requested_role,
            selected_target,
            connection_epoch,
            BrowserRuntimeRole::BrowserObserver,
            selected_target.peek().clone().or(fallback_selection),
            Some(NodeAppSurface::Viewer),
        );
        return;
    }

    activate_surface(ui_state, surface);
}

fn reconnect_browser_runtime(
    mut ui_state: Signal<NodeAppUiState>,
    mut requested_role: Signal<BrowserRuntimeRole>,
    mut selected_target: Signal<Option<(String, Option<String>)>>,
    mut connection_epoch: Signal<u64>,
    role: BrowserRuntimeRole,
    selection: Option<(String, Option<String>)>,
    surface: Option<NodeAppSurface>,
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

fn selection_from_view(view: &NodeAppClientView) -> Option<(String, Option<String>)> {
    view.selected_experiment.as_ref().map(|experiment| {
        (
            experiment.experiment_id.clone(),
            Some(experiment.revision_id.clone()),
        )
    })
}
