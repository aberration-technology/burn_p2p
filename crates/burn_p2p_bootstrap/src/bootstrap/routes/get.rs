use super::*;

pub(crate) fn handle_get_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    current_config: &BootstrapDaemonConfig,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    let HttpServerContext {
        plan,
        state,
        remaining_work_units,
        auth_state,
        admin_signer_peer_id,
        ..
    } = context;

    if request.method == "GET"
        && let Some(page) = page_request_from_path(&request.path, "/receipts/page")?
    {
        let query = ReceiptQuery {
            study_id: None,
            experiment_id: None,
            revision_id: None,
            peer_id: None,
        };
        let receipts = state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_receipts_page(&query, page)?;
        write_json(stream, &receipts)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(page) = page_request_from_path(&request.path, "/heads/page")?
    {
        let heads = state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_heads_page(
                &burn_p2p_bootstrap::HeadQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    head_id: None,
                },
                page,
            )?;
        write_json(stream, &heads)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(page) = page_request_from_path(&request.path, "/merges/page")?
    {
        let merges = state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_merges_page(page)?;
        write_json(stream, &merges)?;
        return Ok(true);
    }

    if handle_artifact_publish_get_route(stream, context, request)? {
        return Ok(true);
    }

    if handle_metrics_indexer_route(stream, context, request)? {
        return Ok(true);
    }

    if handle_portal_artifact_route(stream, context, current_config, request)? {
        return Ok(true);
    }

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_dashboard_html(plan.network_id()).into_bytes(),
        )?,
        ("GET", "/capabilities") | ("GET", "/.well-known/burn-p2p-capabilities") => {
            let signed = sign_browser_snapshot(
                plan,
                admin_signer_peer_id,
                "burn_p2p.edge_service_manifest",
                edge_service_manifest(
                    plan,
                    current_config,
                    auth_state.as_ref(),
                    admin_signer_peer_id,
                ),
            )?;
            write_json(stream, &signed)?;
        }
        ("GET", "/healthz") => write_response(
            stream,
            "200 OK",
            "text/plain; charset=utf-8",
            b"ok".to_vec(),
        )?,
        ("GET", "/status") => {
            let diagnostics = current_diagnostics(plan, state, *remaining_work_units);
            write_json(stream, &diagnostics)?;
        }
        ("GET", "/diagnostics/bundle") => {
            let bundle = current_diagnostics_bundle(plan, state, *remaining_work_units);
            write_json(stream, &bundle)?;
        }
        ("GET", "/portal/snapshot") => {
            if !browser_edge_route_enabled(current_config) && !browser_join_enabled(current_config)
            {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser edge disabled".to_vec(),
                )?;
                return Ok(true);
            }
            let snapshot = current_browser_portal_snapshot(
                plan,
                current_config,
                state,
                auth_state.as_ref(),
                request,
                *remaining_work_units,
            )?;
            write_json(stream, &snapshot)?;
        }
        ("GET", "/leaderboard") => {
            if !social_enabled(current_config) {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"social disabled".to_vec(),
                )?;
                return Ok(true);
            }
            let leaderboard = current_browser_leaderboard(plan, state);
            write_json(stream, &leaderboard)?;
        }
        ("GET", "/directory/signed") => {
            if !browser_edge_route_enabled(current_config) && !browser_join_enabled(current_config)
            {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser directory disabled".to_vec(),
                )?;
                return Ok(true);
            }
            let snapshot = current_browser_directory_snapshot(plan, auth_state.as_ref(), request)?;
            let signed = sign_browser_snapshot(
                plan,
                admin_signer_peer_id,
                "burn_p2p.browser_directory_snapshot",
                snapshot,
            )?;
            write_json(stream, &signed)?;
        }
        ("GET", "/leaderboard/signed") => {
            if !social_enabled(current_config) {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"social disabled".to_vec(),
                )?;
                return Ok(true);
            }
            let signed = sign_browser_snapshot(
                plan,
                admin_signer_peer_id,
                "burn_p2p.browser_leaderboard_snapshot",
                current_browser_leaderboard(plan, state),
            )?;
            write_json(stream, &signed)?;
        }
        ("GET", "/metrics") => {
            let diagnostics = current_diagnostics(plan, state, *remaining_work_units);
            write_response(
                stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                render_openmetrics(&diagnostics).into_bytes(),
            )?;
        }
        ("GET", "/events") => write_event_stream(stream, plan, state, *remaining_work_units)?,
        ("GET", "/receipts") => {
            let query = ReceiptQuery {
                study_id: None,
                experiment_id: None,
                revision_id: None,
                peer_id: None,
            };
            let receipts = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_receipts(&query);
            write_json(stream, &receipts)?;
        }
        ("GET", "/heads") => {
            let heads = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_heads(&burn_p2p_bootstrap::HeadQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    head_id: None,
                });
            write_json(stream, &heads)?;
        }
        ("GET", "/reducers/load") => {
            let reducer_load = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_reducer_load(&burn_p2p_bootstrap::ReducerLoadQuery {
                    study_id: None,
                    experiment_id: None,
                    peer_id: None,
                });
            write_json(stream, &reducer_load)?;
        }
        ("GET", "/revocations") => {
            let auth = auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let response = RevocationResponse {
                network_id: plan.network_id().clone(),
                minimum_revocation_epoch: current_revocation_epoch(auth, state),
                quarantined_peers: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .quarantined_peers
                    .clone(),
                banned_peers: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .banned_peers
                    .clone(),
            };
            write_json(stream, &response)?;
        }
        ("GET", "/directory") => {
            let entries = auth_state
                .as_ref()
                .map(|auth| auth_directory_entries(auth, request))
                .transpose()?
                .unwrap_or_default();
            write_json(stream, &entries)?;
        }
        ("GET", "/trust") => {
            let auth = auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let trust_bundle = current_trust_bundle(auth, state);
            write_json(stream, &trust_bundle)?;
        }
        ("GET", "/reenrollment") => {
            let auth = auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let trust_bundle = current_trust_bundle(auth, state);
            write_json(stream, &trust_bundle.reenrollment)?;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn page_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<burn_p2p_core::PageRequest>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some(burn_p2p_core::PageRequest::default()));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut page = burn_p2p_core::PageRequest::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "offset" => {
                page.offset = value.parse::<usize>()?;
            }
            "limit" => {
                page.limit = value.parse::<usize>()?;
            }
            _ => {}
        }
    }

    Ok(Some(page.normalized()))
}

pub(crate) fn current_diagnostics(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnostics {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics(plan, Utc::now(), remaining_work_units)
}

pub(crate) fn current_diagnostics_bundle(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnosticsBundle {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics_bundle(plan, Utc::now(), remaining_work_units)
}
