fn handle_get_route(
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
        ("GET", "/portal") => {
            if !portal_route_enabled(current_config) {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"portal disabled".to_vec(),
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
            #[cfg(feature = "metrics-indexer")]
            let metrics_snapshots = current_metrics_snapshots(state).unwrap_or_default();
            #[cfg(feature = "artifact-publish")]
            let artifact_aliases = current_artifact_alias_statuses(state).unwrap_or_default();
            write_response(
                stream,
                "200 OK",
                "text/html; charset=utf-8",
                {
                    #[cfg(all(feature = "metrics-indexer", feature = "artifact-publish"))]
                    {
                        render_browser_portal_html_with_metrics_and_artifacts(
                            &snapshot,
                            &metrics_snapshots,
                            &artifact_aliases,
                        )
                        .into_bytes()
                    }
                    #[cfg(all(feature = "metrics-indexer", not(feature = "artifact-publish")))]
                    {
                        render_browser_portal_html_with_metrics(&snapshot, &metrics_snapshots)
                            .into_bytes()
                    }
                    #[cfg(all(not(feature = "metrics-indexer"), feature = "artifact-publish"))]
                    {
                        render_browser_portal_html_with_artifacts(&snapshot, &artifact_aliases)
                            .into_bytes()
                    }
                    #[cfg(all(not(feature = "metrics-indexer"), not(feature = "artifact-publish")))]
                    {
                        render_browser_portal_html(&snapshot).into_bytes()
                    }
                },
            )?;
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
            if !portal_route_enabled(current_config) && !browser_edge_enabled(current_config) {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser portal disabled".to_vec(),
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
            if !portal_route_enabled(current_config) && !browser_edge_enabled(current_config) {
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
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
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
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let trust_bundle = current_trust_bundle(auth, state);
            write_json(stream, &trust_bundle)?;
        }
        ("GET", "/reenrollment") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let trust_bundle = current_trust_bundle(auth, state);
            write_json(stream, &trust_bundle.reenrollment)?;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn current_diagnostics(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnostics {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics(plan, Utc::now(), remaining_work_units)
}

fn current_diagnostics_bundle(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnosticsBundle {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics_bundle(plan, Utc::now(), remaining_work_units)
}
