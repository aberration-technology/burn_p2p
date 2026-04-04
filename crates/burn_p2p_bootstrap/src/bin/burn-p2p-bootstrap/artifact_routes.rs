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
