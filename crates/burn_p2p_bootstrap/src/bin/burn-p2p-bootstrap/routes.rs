fn handle_connection(
    mut stream: TcpStream,
    context: HttpServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(request) = read_request(&stream)? else {
        return Ok(());
    };
    let current_config = context
        .config
        .lock()
        .expect("daemon config should not be poisoned")
        .clone();

    if handle_get_route(&mut stream, &context, &current_config, &request)? {
        return Ok(());
    }
    if handle_artifact_publish_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }
    if handle_browser_post_route(&mut stream, &context, &current_config, &request)? {
        return Ok(());
    }
    if handle_auth_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }
    if handle_admin_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }

    write_response(
        &mut stream,
        "404 Not Found",
        "text/plain; charset=utf-8",
        b"not found".to_vec(),
    )?;

    Ok(())
}

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

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn handle_portal_artifact_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    current_config: &BootstrapDaemonConfig,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.method != "GET" || !request.path.starts_with("/portal/artifacts/") {
        return Ok(false);
    }
    if !portal_route_enabled(current_config) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"portal disabled".to_vec(),
        )?;
        return Ok(true);
    }
    if !artifact_publication_enabled(&context.state) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"artifact publication disabled".to_vec(),
        )?;
        return Ok(true);
    }

    if let Some(path) = request.path.strip_prefix("/portal/artifacts/runs/") {
        let mut segments = path.split('/').filter(|segment| !segment.is_empty());
        let experiment_id = burn_p2p::ExperimentId::new(
            segments
                .next()
                .ok_or("portal artifact run route requires experiment id")?,
        );
        match segments.next() {
            None => {
                let runs = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_artifact_run_summaries(&experiment_id)?;
                write_response(
                    stream,
                    "200 OK",
                    "text/html; charset=utf-8",
                    render_artifact_run_summaries_html(&experiment_id, &runs).into_bytes(),
                )?;
            }
            Some(run_id) => {
                let run_view = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_artifact_run_view(
                        &experiment_id,
                        &burn_p2p_core::RunId::new(run_id),
                    )?;
                match run_view {
                    Some(run_view) => write_response(
                        stream,
                        "200 OK",
                        "text/html; charset=utf-8",
                        render_artifact_run_view_html(&run_view).into_bytes(),
                    )?,
                    None => {
                        write_response(
                            stream,
                            "404 Not Found",
                            "text/plain; charset=utf-8",
                            b"artifact run not found".to_vec(),
                        )?;
                    }
                }
            }
        }
        return Ok(true);
    }

    if let Some(head_id) = request.path.strip_prefix("/portal/artifacts/heads/") {
        let head_view = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_head_artifact_view(&burn_p2p::HeadId::new(head_id))?;
        match head_view {
            Some(head_view) => write_response(
                stream,
                "200 OK",
                "text/html; charset=utf-8",
                render_head_artifact_view_html(&head_view).into_bytes(),
            )?,
            None => {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"artifact head not found".to_vec(),
                )?;
            }
        }
        return Ok(true);
    }

    Ok(false)
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
fn handle_portal_artifact_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _current_config: &BootstrapDaemonConfig,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

fn handle_browser_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    current_config: &BootstrapDaemonConfig,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if (request.method.as_str(), request.path.as_str()) != ("POST", "/receipts/browser") {
        return Ok(false);
    }
    if !browser_edge_enabled(current_config) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"browser edge disabled".to_vec(),
        )?;
        return Ok(true);
    }
    let auth = context
        .auth_state
        .as_ref()
        .ok_or("auth portal is not configured")?;
    let session = request
        .headers
        .get("x-session-id")
        .and_then(|session_id| {
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&ContentId::new(session_id.clone()))
                .cloned()
        })
        .ok_or("browser receipt submission requires x-session-id")?;
    let receipts: Vec<burn_p2p::ContributionReceipt> = serde_json::from_slice(&request.body)?;
    if receipts
        .iter()
        .any(|receipt| !session_allows_receipt_submission(&session, receipt))
    {
        return Err("session is not authorized to submit one or more browser receipts".into());
    }
    let accepted_receipt_ids = context
        .state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .ingest_contribution_receipts(receipts);
    write_json(
        stream,
        &BrowserReceiptSubmissionResponse {
            pending_receipt_count: 0,
            accepted_receipt_ids,
        },
    )?;
    Ok(true)
}

#[cfg(feature = "artifact-publish")]
fn handle_artifact_publish_get_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.method != "GET" {
        return Ok(false);
    }
    if request.path.starts_with("/admin/artifacts/") {
        if !artifact_publication_enabled(&context.state) {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"artifact publication disabled".to_vec(),
            )?;
            return Ok(true);
        }
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportHeads,
        )? {
            return Ok(true);
        }
        match request.path.as_str() {
            "/admin/artifacts/publications" => {
                let publications = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_published_artifacts()?;
                write_json(stream, &publications)?;
                return Ok(true);
            }
            "/admin/artifacts/jobs" => {
                let jobs = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_artifact_jobs()?;
                write_json(stream, &jobs)?;
                return Ok(true);
            }
            _ => return Ok(false),
        }
    }

    if !request.path.starts_with("/artifacts/") {
        return Ok(false);
    }
    if !artifact_publication_enabled(&context.state) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"artifact publication disabled".to_vec(),
        )?;
        return Ok(true);
    }

    match request.path.as_str() {
        "/artifacts/live" => {
            write_artifact_event_stream(stream, &context.state)?;
            return Ok(true);
        }
        "/artifacts/live/latest" => {
            match current_artifact_live_event(&context.state)? {
                Some(event) => write_json(stream, &event)?,
                None => {
                    write_response(
                        stream,
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        b"artifact live event not found".to_vec(),
                    )?;
                }
            }
            return Ok(true);
        }
        _ => {}
    }

    if request.path == "/artifacts/aliases" {
        let aliases = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_artifact_alias_statuses()?;
        write_json(stream, &aliases)?;
        return Ok(true);
    }

    if let Some(experiment_id) = request.path.strip_prefix("/artifacts/aliases/") {
        let aliases = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_artifact_alias_statuses_for_experiment(&burn_p2p::ExperimentId::new(
                experiment_id,
            ))?;
        write_json(stream, &aliases)?;
        return Ok(true);
    }
    if let Some(path) = request.path.strip_prefix("/artifacts/runs/") {
        let mut segments = path.split('/').filter(|segment| !segment.is_empty());
        let experiment_id = burn_p2p::ExperimentId::new(
            segments
                .next()
                .ok_or("artifact run route requires experiment id")?,
        );
        match segments.next() {
            None => {
                let runs = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_artifact_run_summaries(&experiment_id)?;
                write_json(stream, &runs)?;
            }
            Some(run_id) => {
                let run_view = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .export_artifact_run_view(
                        &experiment_id,
                        &burn_p2p_core::RunId::new(run_id),
                    )?;
                match run_view {
                    Some(run_view) => write_json(stream, &run_view)?,
                    None => {
                        write_response(
                            stream,
                            "404 Not Found",
                            "text/plain; charset=utf-8",
                            b"artifact run not found".to_vec(),
                        )?;
                    }
                }
            }
        }
        return Ok(true);
    }
    if let Some(head_id) = request.path.strip_prefix("/artifacts/heads/") {
        let head = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_head_artifact_view(&burn_p2p::HeadId::new(head_id))?;
        match head {
            Some(head) => write_json(stream, &head)?,
            None => {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"artifact head not found".to_vec(),
                )?;
            }
        }
        return Ok(true);
    }
    if let Some(export_job_id) = request.path.strip_prefix("/artifacts/export/") {
        let job = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .export_artifact_job(&burn_p2p_core::ExportJobId::new(export_job_id))?;
        match job {
            Some(job) => write_json(stream, &job)?,
            None => {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"artifact export job not found".to_vec(),
                )?;
            }
        }
        return Ok(true);
    }
    if let Some(ticket_id) = request.path.strip_prefix("/artifacts/download/") {
        let artifact = context
            .state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .resolve_artifact_download(&burn_p2p_core::DownloadTicketId::new(ticket_id))?;
        match artifact {
            Some(artifact) => {
                if let Some(redirect_url) = artifact.redirect_url {
                    write_response_with_headers(
                        stream,
                        "302 Found",
                        "text/plain; charset=utf-8",
                        &[("Location", redirect_url)],
                        Vec::new(),
                    )?;
                } else {
                    let headers = [(
                        "Content-Disposition",
                        format!("attachment; filename=\"{}\"", artifact.file_name),
                    )];
                    match artifact.body {
                        burn_p2p_publish::DownloadArtifactBody::Empty => {
                            write_response_with_headers(
                                stream,
                                "204 No Content",
                                &artifact.content_type,
                                &headers,
                                Vec::new(),
                            )?;
                        }
                        burn_p2p_publish::DownloadArtifactBody::Bytes(bytes) => {
                            write_response_with_headers(
                                stream,
                                "200 OK",
                                &artifact.content_type,
                                &headers,
                                bytes,
                            )?;
                        }
                        burn_p2p_publish::DownloadArtifactBody::LocalFile {
                            path,
                            content_length,
                        } => {
                            write_file_response_with_headers(
                                stream,
                                "200 OK",
                                &artifact.content_type,
                                &headers,
                                &path,
                                content_length,
                            )?;
                        }
                        burn_p2p_publish::DownloadArtifactBody::RemoteProxy {
                            url,
                            content_length,
                        } => {
                            #[cfg(feature = "artifact-s3")]
                            {
                                let response = reqwest::blocking::Client::new()
                                    .get(url)
                                    .send()?
                                    .error_for_status()?;
                                write_stream_response_with_headers(
                                    stream,
                                    "200 OK",
                                    &artifact.content_type,
                                    &headers,
                                    response.content_length().unwrap_or(content_length),
                                    response,
                                )?;
                            }
                            #[cfg(not(feature = "artifact-s3"))]
                            {
                                let _ = url;
                                let _ = content_length;
                                write_response(
                                    stream,
                                    "502 Bad Gateway",
                                    "text/plain; charset=utf-8",
                                    b"remote artifact proxy unsupported".to_vec(),
                                )?;
                            }
                        }
                    }
                }
            }
            None => {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"artifact download not found".to_vec(),
                )?;
            }
        }
        return Ok(true);
    }

    Ok(false)
}

#[cfg(not(feature = "artifact-publish"))]
fn handle_artifact_publish_get_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

#[cfg(feature = "artifact-publish")]
fn handle_artifact_publish_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.path.starts_with("/admin/artifacts/") {
        if !artifact_publication_enabled(&context.state) {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"artifact publication disabled".to_vec(),
            )?;
            return Ok(true);
        }
        if !authorize_admin_route(stream, context, request, AdminCapability::Control)? {
            return Ok(true);
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/admin/artifacts/backfill-aliases") => {
                let request: burn_p2p_publish::ArtifactBackfillRequest =
                    if request.body.is_empty() {
                        burn_p2p_publish::ArtifactBackfillRequest::default()
                    } else {
                        serde_json::from_slice(&request.body)?
                    };
                let result = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .backfill_artifact_aliases(request)?;
                write_json(stream, &result)?;
                return Ok(true);
            }
            ("POST", "/admin/artifacts/republish") => {
                let export: ArtifactExportHttpRequest = serde_json::from_slice(&request.body)?;
                let job = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .request_artifact_export(ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: export.experiment_id,
                        run_id: export.run_id,
                        head_id: export.head_id,
                        artifact_profile: export.artifact_profile,
                        publication_target_id: export.publication_target_id,
                        artifact_alias_id: export.artifact_alias_id,
                    })?;
                write_json(stream, &job)?;
                return Ok(true);
            }
            ("POST", "/admin/artifacts/prune") => {
                let request: burn_p2p_publish::ArtifactPruneRequest =
                    if request.body.is_empty() {
                        burn_p2p_publish::ArtifactPruneRequest::default()
                    } else {
                        serde_json::from_slice(&request.body)?
                    };
                let result = context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .prune_artifact_publications(request)?;
                write_json(stream, &result)?;
                return Ok(true);
            }
            _ => return Ok(false),
        }
    }

    if request.method != "POST" || !request.path.starts_with("/artifacts/") {
        return Ok(false);
    }
    if !artifact_publication_enabled(&context.state) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"artifact publication disabled".to_vec(),
        )?;
        return Ok(true);
    }

    match request.path.as_str() {
        "/artifacts/export" => {
            let export: ArtifactExportHttpRequest = serde_json::from_slice(&request.body)?;
            let principal_id = resolve_artifact_request_principal(
                request,
                context.auth_state.as_ref(),
                &export.experiment_id,
            )?;
            let job = context
                .state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .request_artifact_export(ExportRequest {
                    requested_by_principal_id: Some(principal_id),
                    experiment_id: export.experiment_id,
                    run_id: export.run_id,
                    head_id: export.head_id,
                    artifact_profile: export.artifact_profile,
                    publication_target_id: export.publication_target_id,
                    artifact_alias_id: export.artifact_alias_id,
                })?;
            write_json(stream, &job)?;
            return Ok(true);
        }
        "/artifacts/download-ticket" => {
            let download: ArtifactDownloadTicketHttpRequest =
                serde_json::from_slice(&request.body)?;
            let principal_id = resolve_artifact_request_principal(
                request,
                context.auth_state.as_ref(),
                &download.experiment_id,
            )?;
            let ticket = context
                .state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .request_artifact_download_ticket(DownloadTicketRequest {
                    principal_id,
                    experiment_id: download.experiment_id,
                    run_id: download.run_id,
                    head_id: download.head_id,
                    artifact_profile: download.artifact_profile,
                    publication_target_id: download.publication_target_id,
                    artifact_alias_id: download.artifact_alias_id,
                })?;
            write_json(stream, &ticket)?;
            return Ok(true);
        }
        _ => {}
    }

    Ok(false)
}

#[cfg(not(feature = "artifact-publish"))]
fn handle_artifact_publish_post_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

#[cfg(feature = "artifact-publish")]
fn artifact_publication_enabled(state: &Arc<Mutex<BootstrapAdminState>>) -> bool {
    let state = state
        .lock()
        .expect("bootstrap admin state should not be poisoned");
    state.publication_store_root.is_some() && state.artifact_store_root.is_some()
}

#[cfg(feature = "artifact-publish")]
fn authorize_admin_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
    required_capability: AdminCapability,
) -> Result<bool, Box<dyn std::error::Error>> {
    if token_matches(
        request,
        context.admin_token.as_deref(),
        context.allow_dev_admin_token,
    ) {
        return Ok(true);
    }
    let Some(capabilities) = request_admin_capabilities(request, context.auth_state.as_ref()) else {
        write_response(
            stream,
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            b"missing or invalid x-admin-token or x-session-id".to_vec(),
        )?;
        return Ok(false);
    };
    if !capabilities.contains(&required_capability) {
        write_response(
            stream,
            "403 Forbidden",
            "text/plain; charset=utf-8",
            format!("session is not authorized for {:?}", required_capability).into_bytes(),
        )?;
        return Ok(false);
    }
    Ok(true)
}

#[cfg(feature = "artifact-publish")]
fn resolve_artifact_request_principal(
    request: &HttpRequest,
    auth_state: Option<&Arc<AuthPortalState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<PrincipalId, Box<dyn std::error::Error>> {
    let Some(auth) = auth_state else {
        return Ok(PrincipalId::new("anonymous"));
    };
    let session_id = request
        .headers
        .get("x-session-id")
        .ok_or("artifact export requires x-session-id")?;
    let session = auth
        .sessions
        .lock()
        .expect("auth session state should not be poisoned")
        .get(&ContentId::new(session_id.clone()))
        .cloned()
        .ok_or("unknown session id")?;
    if !session_allows_artifact_access(&session, experiment_id) {
        return Err(format!(
            "session is not authorized for experiment {}",
            experiment_id.as_str()
        )
        .into());
    }
    Ok(session.claims.principal_id)
}

#[cfg(feature = "artifact-publish")]
fn session_allows_artifact_access(
    session: &PrincipalSession,
    experiment_id: &burn_p2p::ExperimentId,
) -> bool {
    session.claims.granted_scopes.iter().any(|scope| match scope {
        ExperimentScope::Connect | ExperimentScope::Discover => true,
        ExperimentScope::Train {
            experiment_id: scoped_experiment_id,
        }
        | ExperimentScope::Validate {
            experiment_id: scoped_experiment_id,
        } => scoped_experiment_id == experiment_id,
        _ => false,
    })
}

fn handle_auth_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(auth) = context.auth_state.as_ref() else {
        return Ok(false);
    };

    match (request.method.as_str(), request.path.as_str()) {
        ("POST", path) if auth.connector.matches_login_path(path) => {
            let login_request: LoginRequest = serde_json::from_slice(&request.body)?;
            let login = auth.connector.begin_login(login_request)?;
            write_json(stream, &login)?;
        }
        ("POST", path) if auth.connector.matches_callback_path(path) => {
            let mut callback: burn_p2p::CallbackPayload = serde_json::from_slice(&request.body)?;
            if callback.principal_id.is_none() {
                callback.principal_id = auth.connector.trusted_callback_principal(request);
            }
            let session = auth.connector.complete_login(callback)?;
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .insert(session.session_id.clone(), session.clone());
            write_json(stream, &session)?;
        }
        ("POST", "/refresh") => {
            let refresh: SessionRequest = serde_json::from_slice(&request.body)?;
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&refresh.session_id)
                .cloned()
                .ok_or("unknown session id")?;
            let refreshed = auth.connector.refresh(&session)?;
            let mut sessions = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned");
            sessions.remove(&refresh.session_id);
            sessions.insert(refreshed.session_id.clone(), refreshed.clone());
            write_json(stream, &refreshed)?;
        }
        ("POST", "/logout") => {
            let logout: SessionRequest = serde_json::from_slice(&request.body)?;
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&logout.session_id)
                .cloned();
            if let Some(session) = session.as_ref() {
                auth.connector.revoke(session)?;
            }
            let logged_out = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .remove(&logout.session_id)
                .is_some();
            write_json(stream, &LogoutResponse { logged_out })?;
        }
        ("POST", "/enroll") => {
            let effective_revocation_epoch = current_revocation_epoch(auth, &context.state);
            let enroll: BootstrapEnrollRequest = serde_json::from_slice(&request.body)?;
            if enroll.release_train_hash != auth.required_release_train_hash {
                return Err(format!(
                    "release train {} is not permitted by this authority",
                    enroll.release_train_hash.as_str(),
                )
                .into());
            }
            if !auth.allowed_target_artifact_hashes.is_empty()
                && !auth
                    .allowed_target_artifact_hashes
                    .contains(&enroll.target_artifact_hash)
            {
                return Err(format!(
                    "target artifact {} is not permitted by this authority",
                    enroll.target_artifact_hash.as_str(),
                )
                .into());
            }
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&enroll.session_id)
                .cloned()
                .ok_or("unknown session id")?;
            let certificate = auth
                .authority
                .lock()
                .expect("auth authority should not be poisoned")
                .issue_certificate(NodeEnrollmentRequest {
                    session,
                    project_family_id: auth.project_family_id.clone(),
                    release_train_hash: enroll.release_train_hash.clone(),
                    target_artifact_hash: enroll.target_artifact_hash.clone(),
                    peer_id: enroll.peer_id,
                    peer_public_key_hex: enroll.peer_public_key_hex,
                    granted_roles: auth
                        .sessions
                        .lock()
                        .expect("auth session state should not be poisoned")
                        .get(&enroll.session_id)
                        .map(|session| session.claims.granted_roles.clone())
                        .ok_or("unknown session id")?,
                    requested_scopes: enroll.requested_scopes,
                    client_policy_hash: enroll.client_policy_hash,
                    serial: enroll.serial,
                    not_before: Utc::now(),
                    not_after: Utc::now() + chrono::Duration::seconds(enroll.ttl_seconds.max(1)),
                    revocation_epoch: effective_revocation_epoch,
                })?;
            write_json(stream, &certificate)?;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn handle_admin_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if (request.method.as_str(), request.path.as_str()) != ("POST", "/admin") {
        return Ok(false);
    }
    let action: AdminAction = serde_json::from_slice(&request.body)?;
    if !token_matches(
        request,
        context.admin_token.as_deref(),
        context.allow_dev_admin_token,
    ) {
        let Some(capabilities) = request_admin_capabilities(request, context.auth_state.as_ref())
        else {
            write_response(
                stream,
                "401 Unauthorized",
                "text/plain; charset=utf-8",
                b"missing or invalid x-admin-token or x-session-id".to_vec(),
            )?;
            return Ok(true);
        };
        if !capabilities.contains(&action.capability()) {
            write_response(
                stream,
                "403 Forbidden",
                "text/plain; charset=utf-8",
                format!(
                    "session is not authorized for admin capability {:?}",
                    action.capability()
                )
                .into_bytes(),
            )?;
            return Ok(true);
        }
    }

    let result = execute_admin_action(context, action.clone())?;
    write_json(stream, &result)?;
    Ok(true)
}

fn execute_admin_action(
    context: &HttpServerContext,
    action: AdminAction,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    match action.clone() {
        AdminAction::RotateAuthorityMaterial {
            issuer_key_id,
            retain_previous_issuer,
            require_reenrollment,
            reenrollment_reason,
        } => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                        action.capability(),
                    ),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("auth portal is not configured")?;
            let result = rotate_authority_material(
                auth,
                &context.state,
                issuer_key_id,
                retain_previous_issuer,
                require_reenrollment,
                reenrollment_reason.clone(),
            )?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.issuer_key_id = match &result {
                    burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
                        issuer_key_id,
                        ..
                    } => issuer_key_id.clone(),
                    _ => auth_config.issuer_key_id.clone(),
                };
                auth_config.trusted_issuers = current_trust_bundle(auth, &context.state)
                    .issuers
                    .iter()
                    .map(|issuer| TrustedIssuer {
                        issuer_peer_id: issuer.issuer_peer_id.clone(),
                        issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                    })
                    .collect();
                auth_config.reenrollment = auth
                    .reenrollment
                    .lock()
                    .expect("auth reenrollment state should not be poisoned")
                    .clone();
            }
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::RolloutAuthPolicy(rollout) => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                        action.capability(),
                    ),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("auth portal is not configured")?;
            let result = rollout_auth_policy(
                &context.plan,
                auth,
                &context.state,
                rollout.clone(),
                context.control_handle.as_ref(),
            )?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
                    auth_config.minimum_revocation_epoch = auth_config
                        .minimum_revocation_epoch
                        .max(minimum_revocation_epoch.0);
                }
                if let Some(directory_entries) = rollout.directory_entries {
                    auth_config.directory_entries = directory_entries;
                }
                if let Some(trusted_issuers) = rollout.trusted_issuers {
                    auth_config.trusted_issuers = trusted_issuers;
                }
                if let Some(reenrollment) = rollout.reenrollment {
                    auth_config.reenrollment = Some(BootstrapReenrollmentConfig {
                        reason: reenrollment.reason,
                        rotated_at: reenrollment.rotated_at,
                        legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
                    });
                }
            }
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::RetireTrustedIssuers { issuer_peer_ids } => {
            if !context.plan.supports_admin_action(&action) {
                return Err(Box::new(
                    burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                        action.capability(),
                    ),
                ));
            }
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("auth portal is not configured")?;
            let result = retire_trusted_issuers(auth, &context.state, &issuer_peer_ids)?;
            let mut config_guard = context
                .config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.trusted_issuers = current_trust_bundle(auth, &context.state)
                    .issuers
                    .iter()
                    .map(|issuer| TrustedIssuer {
                        issuer_peer_id: issuer.issuer_peer_id.clone(),
                        issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                    })
                    .collect();
                auth_config.reenrollment = auth
                    .reenrollment
                    .lock()
                    .expect("auth reenrollment state should not be poisoned")
                    .clone();
            }
            persist_daemon_config(&context.config_path, &config_guard)?;
            Ok(result)
        }
        AdminAction::ExportTrustBundle => {
            let auth = context
                .auth_state
                .as_ref()
                .ok_or("auth portal is not configured")?;
            Ok(burn_p2p_bootstrap::AdminResult::TrustBundle(Some(
                current_trust_bundle(auth, &context.state),
            )))
        }
        _ => {
            let signer = Some(SignatureMetadata {
                signer: context.admin_signer_peer_id.clone(),
                key_id: "bootstrap-admin".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "bootstrap-local-admin".into(),
            });
            let result = context.plan.execute_admin_action(
                action,
                &mut context
                    .state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned"),
                signer,
                Utc::now(),
                context.remaining_work_units,
            )?;
            publish_admin_result(&context.plan, context.control_handle.as_ref(), &result)?;
            Ok(result)
        }
    }
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

fn compiled_feature_set() -> CompiledFeatureSet {
    let mut features = BTreeSet::new();
    if cfg!(feature = "admin-http") {
        features.insert(EdgeFeature::AdminHttp);
    }
    if cfg!(feature = "metrics") {
        features.insert(EdgeFeature::Metrics);
    }
    if cfg!(feature = "portal") {
        features.insert(EdgeFeature::Portal);
    }
    if cfg!(feature = "browser-edge") {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if cfg!(feature = "rbac") {
        features.insert(EdgeFeature::Rbac);
    }
    if cfg!(feature = "auth-static") {
        features.insert(EdgeFeature::AuthStatic);
    }
    if cfg!(feature = "auth-github") {
        features.insert(EdgeFeature::AuthGitHub);
    }
    if cfg!(feature = "auth-oidc") {
        features.insert(EdgeFeature::AuthOidc);
    }
    if cfg!(feature = "auth-oauth") {
        features.insert(EdgeFeature::AuthOAuth);
    }
    if cfg!(feature = "auth-external") {
        features.insert(EdgeFeature::AuthExternal);
    }
    if cfg!(feature = "social") {
        features.insert(EdgeFeature::Social);
        features.insert(EdgeFeature::Profiles);
    }
    CompiledFeatureSet { features }
}

fn configured_auth_providers(config: &BootstrapDaemonConfig) -> BTreeSet<EdgeAuthProvider> {
    match config.auth.as_ref().map(|auth| &auth.connector) {
        Some(BootstrapAuthConnectorConfig::Static) => BTreeSet::from([EdgeAuthProvider::Static]),
        Some(BootstrapAuthConnectorConfig::GitHub { .. }) => {
            BTreeSet::from([EdgeAuthProvider::GitHub])
        }
        Some(BootstrapAuthConnectorConfig::Oidc { .. }) => BTreeSet::from([EdgeAuthProvider::Oidc]),
        Some(BootstrapAuthConnectorConfig::OAuth { .. }) => {
            BTreeSet::from([EdgeAuthProvider::OAuth])
        }
        Some(BootstrapAuthConnectorConfig::External { .. }) => {
            BTreeSet::from([EdgeAuthProvider::External])
        }
        None => BTreeSet::new(),
    }
}

fn configured_service_set(config: &BootstrapDaemonConfig) -> ConfiguredServiceSet {
    let mut features = BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]);
    if config.optional_services.portal_enabled {
        features.insert(EdgeFeature::Portal);
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if config.optional_services.social_mode != SocialMode::Disabled {
        features.insert(EdgeFeature::Social);
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled {
        features.insert(EdgeFeature::Profiles);
    }
    for provider in configured_auth_providers(config) {
        match provider {
            EdgeAuthProvider::Static => {
                features.insert(EdgeFeature::AuthStatic);
            }
            EdgeAuthProvider::GitHub => {
                features.insert(EdgeFeature::AuthGitHub);
            }
            EdgeAuthProvider::Oidc => {
                features.insert(EdgeFeature::AuthOidc);
            }
            EdgeAuthProvider::OAuth => {
                features.insert(EdgeFeature::AuthOAuth);
            }
            EdgeAuthProvider::External => {
                features.insert(EdgeFeature::AuthExternal);
            }
        }
    }
    if config.auth.is_some() {
        features.insert(EdgeFeature::Rbac);
    }
    ConfiguredServiceSet { features }
}

fn active_service_set(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> ActiveServiceSet {
    let compiled = compiled_feature_set();
    let configured = configured_service_set(config);
    let features = configured
        .features
        .into_iter()
        .filter(|feature| compiled.features.contains(feature))
        .filter(|feature| match feature {
            EdgeFeature::Rbac => auth_state.is_some(),
            _ => true,
        })
        .collect();
    ActiveServiceSet { features }
}

fn validate_compiled_feature_support(
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    validate_compiled_feature_support_with(&compiled_feature_set(), config)
}

fn validate_compiled_feature_support_with(
    compiled: &CompiledFeatureSet,
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    if config.optional_services.portal_enabled && !compiled.features.contains(&EdgeFeature::Portal)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "portal",
            feature: "portal",
        });
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled
        && !compiled.features.contains(&EdgeFeature::BrowserEdge)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "browser edge",
            feature: "browser-edge",
        });
    }
    if config.optional_services.social_mode != SocialMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Social)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "social",
            feature: "social",
        });
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && config.optional_services.social_mode == SocialMode::Disabled
    {
        return Err(BootstrapCompositionError::InvalidServiceConfig(
            "profile_mode requires social_mode to be enabled",
        ));
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Profiles)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "profiles",
            feature: "social",
        });
    }
    if let Some(auth) = config.auth.as_ref() {
        let required = match &auth.connector {
            BootstrapAuthConnectorConfig::Static => EdgeFeature::AuthStatic,
            BootstrapAuthConnectorConfig::GitHub { .. } => EdgeFeature::AuthGitHub,
            BootstrapAuthConnectorConfig::Oidc { .. } => EdgeFeature::AuthOidc,
            BootstrapAuthConnectorConfig::OAuth { .. } => EdgeFeature::AuthOAuth,
            BootstrapAuthConnectorConfig::External { .. } => EdgeFeature::AuthExternal,
        };
        let feature = match required {
            EdgeFeature::AuthStatic => "auth-static",
            EdgeFeature::AuthGitHub => "auth-github",
            EdgeFeature::AuthOidc => "auth-oidc",
            EdgeFeature::AuthOAuth => "auth-oauth",
            EdgeFeature::AuthExternal => "auth-external",
            _ => unreachable!("auth feature mapping should stay exhaustive"),
        };
        if !compiled.features.contains(&required) {
            return Err(BootstrapCompositionError::MissingCompiledFeature {
                service: "auth connector",
                feature,
            });
        }
        if let BootstrapAuthConnectorConfig::External {
            trusted_principal_header,
            trusted_internal_only,
            ..
        } = &auth.connector
        {
            if !trusted_internal_only {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires trusted_internal_only = true",
                ));
            }
            if trusted_principal_header.trim().is_empty() {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires a non-empty trusted_principal_header",
                ));
            }
        }
    }
    Ok(())
}

fn portal_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> PortalMode {
    if !config.optional_services.portal_enabled {
        PortalMode::Disabled
    } else if auth_state.is_some() {
        PortalMode::Interactive
    } else {
        PortalMode::Readonly
    }
}

fn profile_mode(config: &BootstrapDaemonConfig) -> ProfileMode {
    if config.optional_services.social_mode == SocialMode::Disabled {
        ProfileMode::Disabled
    } else {
        config.optional_services.profile_mode.clone()
    }
}

fn admin_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> AdminMode {
    if auth_state.is_some() && cfg!(feature = "rbac") {
        AdminMode::Rbac
    } else if config.allow_dev_admin_token {
        AdminMode::Token
    } else {
        AdminMode::Disabled
    }
}

fn metrics_mode() -> MetricsMode {
    if cfg!(feature = "metrics") {
        MetricsMode::OpenMetrics
    } else {
        MetricsMode::Disabled
    }
}

fn edge_service_manifest(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
    edge_id: &PeerId,
) -> EdgeServiceManifest {
    let compiled_feature_set = compiled_feature_set();
    let configured_service_set = configured_service_set(config);
    let active_feature_set = active_service_set(config, auth_state);
    EdgeServiceManifest {
        edge_id: edge_id.clone(),
        network_id: plan.network_id().clone(),
        portal_mode: portal_mode(config, auth_state),
        browser_mode: config.optional_services.browser_mode.clone(),
        available_auth_providers: configured_auth_providers(config),
        social_mode: config.optional_services.social_mode.clone(),
        profile_mode: profile_mode(config),
        admin_mode: admin_mode(config, auth_state),
        metrics_mode: metrics_mode(),
        compiled_feature_set,
        configured_service_set,
        active_feature_set,
        generated_at: Utc::now(),
    }
}

fn handle_metrics_indexer_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.method != "GET" {
        return Ok(false);
    }

    match request.path.as_str() {
        "/metrics/snapshot" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_snapshots(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/ledger" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_ledger(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/catchup" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_catchup(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/candidates" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_candidates(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/disagreements" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_disagreements(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/peer-windows" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_peer_windows(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/live" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_metrics_event_stream(stream, &context.plan, &context.state)?;
            return Ok(true);
        }
        "/metrics/live/latest" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_live_event(&context.plan, &context.state)?)?;
            return Ok(true);
        }
        _ => {}
    }

    if let Some(head_id) = request.path.strip_prefix("/metrics/heads/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_head_eval_reports(&context.state, &burn_p2p::HeadId::new(head_id)),
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/experiments/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_snapshots_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/catchup/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_catchup_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/candidates/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_candidates_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/disagreements/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_disagreements_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/peer-windows/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        if let Some((experiment_id, remainder)) = experiment_id.split_once('/')
            && let Some((revision_id, base_head_id)) = remainder.split_once('/')
        {
            match current_metrics_peer_window_detail(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
                &burn_p2p::RevisionId::new(revision_id),
                &burn_p2p::HeadId::new(base_head_id),
            )? {
                Some(detail) => write_json(stream, &detail)?,
                None => {
                    write_response(
                        stream,
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        b"peer-window detail not found".to_vec(),
                    )?;
                }
            }
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_peer_windows_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }

    Ok(false)
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_snapshots(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::MetricsSnapshot>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_snapshots()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_snapshots(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_snapshots_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::MetricsSnapshot>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_snapshots_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_snapshots_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_ledger(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::MetricsLedgerSegment>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_ledger_segments()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_ledger(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_catchup(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::MetricsCatchupBundle>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_catchup_bundles()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_catchup(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_catchup_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::MetricsCatchupBundle>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_catchup_bundles_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_catchup_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_candidates(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_candidates()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_candidates(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_candidates_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_candidates_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_candidates_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_disagreements(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_disagreements()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_disagreements(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_disagreements_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_disagreements_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_disagreements_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_peer_windows(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::PeerWindowDistributionSummary>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distributions()?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_peer_windows(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_peer_windows_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::PeerWindowDistributionSummary>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distributions_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_peer_windows_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_peer_window_detail(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
    revision_id: &burn_p2p::RevisionId,
    base_head_id: &burn_p2p::HeadId,
) -> Result<Option<burn_p2p_metrics::PeerWindowDistributionDetail>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distribution_detail(experiment_id, revision_id, base_head_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_peer_window_detail(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
    _revision_id: &burn_p2p::RevisionId,
    _base_head_id: &burn_p2p::HeadId,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
fn current_metrics_live_event(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<burn_p2p_core::MetricsLiveEvent, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_live_event(plan.network_id())?)
}

#[cfg(not(feature = "metrics-indexer"))]
fn current_metrics_live_event(
    _plan: &BootstrapPlan,
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "artifact-publish")]
fn current_artifact_alias_statuses(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> anyhow::Result<Vec<burn_p2p_bootstrap::ArtifactAliasStatus>> {
    let state = state
        .lock()
        .expect("bootstrap admin state should not be poisoned");
    state.export_artifact_alias_statuses()
}

#[cfg(not(feature = "artifact-publish"))]
fn current_artifact_alias_statuses(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> anyhow::Result<Vec<()>> {
    Ok(Vec::new())
}

#[cfg(feature = "artifact-publish")]
fn current_artifact_live_event(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Option<burn_p2p_core::ArtifactLiveEvent>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_latest_artifact_live_event()?)
}

#[cfg(not(feature = "artifact-publish"))]
fn current_artifact_live_event(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    Ok(None)
}

fn current_head_eval_reports(
    state: &Arc<Mutex<BootstrapAdminState>>,
    head_id: &burn_p2p::HeadId,
) -> Vec<burn_p2p::HeadEvalReport> {
    state.lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_head_eval_reports(head_id)
}

fn portal_route_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.portal_enabled
}

fn browser_edge_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.browser_mode != BrowserMode::Disabled
}

fn social_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.social_mode != SocialMode::Disabled
}

fn browser_edge_mode(plan: &BootstrapPlan) -> BrowserEdgeMode {
    if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Relay)
        || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Kademlia)
    {
        if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Validator)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Archive)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Authority)
        {
            BrowserEdgeMode::Full
        } else {
            BrowserEdgeMode::Peer
        }
    } else {
        BrowserEdgeMode::Minimal
    }
}

fn browser_transport_surface(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
) -> BrowserTransportSurface {
    if !browser_edge_enabled(config) {
        return BrowserTransportSurface {
            webrtc_direct: false,
            webtransport_gateway: false,
            wss_fallback: false,
        };
    }
    let edge_mode = browser_edge_mode(plan);
    BrowserTransportSurface {
        webrtc_direct: matches!(edge_mode, BrowserEdgeMode::Peer | BrowserEdgeMode::Full),
        webtransport_gateway: matches!(edge_mode, BrowserEdgeMode::Full),
        wss_fallback: true,
    }
}

fn browser_login_providers(auth_state: Option<&Arc<AuthPortalState>>) -> Vec<BrowserLoginProvider> {
    auth_state
        .map(|auth| auth.login_providers.clone())
        .unwrap_or_default()
}

fn current_browser_directory_snapshot(
    plan: &BootstrapPlan,
    auth_state: Option<&Arc<AuthPortalState>>,
    request: &HttpRequest,
) -> Result<BrowserDirectorySnapshot, Box<dyn std::error::Error>> {
    let entries = auth_state
        .map(|auth| auth_directory_entries(auth, request))
        .transpose()?
        .unwrap_or_default();
    Ok(BrowserDirectorySnapshot {
        network_id: plan.network_id().clone(),
        generated_at: Utc::now(),
        entries,
    })
}

fn current_browser_leaderboard(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> burn_p2p_bootstrap::BrowserLeaderboardSnapshot {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .leaderboard_snapshot(plan, Utc::now())
}

include!("service_state.rs");
include!("auth_portal.rs");
include!("admin_helpers.rs");
include!("http_io.rs");
