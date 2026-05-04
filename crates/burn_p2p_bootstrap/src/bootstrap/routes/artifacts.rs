use super::*;

#[cfg(feature = "artifact-publish")]
use burn_p2p_bootstrap::artifact_mirror::{
    mirror_peer_artifact_into_store, peer_artifact_mirror_error_status,
};

#[cfg(feature = "artifact-publish")]
use burn_p2p::FsArtifactStore;

#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PeerArtifactMirrorRequest;

#[cfg(feature = "artifact-publish")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestedByteRange {
    start: Option<u64>,
    end_inclusive: Option<u64>,
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
pub(crate) fn handle_portal_artifact_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    current_config: &BootstrapDaemonConfig,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if !matches!(request.method.as_str(), "GET" | "HEAD")
        || !request.path.starts_with("/portal/artifacts/")
    {
        return Ok(false);
    }
    if !browser_edge_route_enabled(current_config) {
        write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"browser edge disabled".to_vec(),
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
                let runs = with_admin_state(&context.state, |state| {
                    state.export_artifact_run_summaries(&experiment_id)
                })?;
                write_response_for_method(
                    stream,
                    &request.method,
                    "200 OK",
                    "text/html; charset=utf-8",
                    render_artifact_run_summaries_html(&experiment_id, &runs).into_bytes(),
                )?;
            }
            Some(run_id) => {
                let run_view = with_admin_state(&context.state, |state| {
                    state.export_artifact_run_view(
                        &experiment_id,
                        &burn_p2p_core::RunId::new(run_id),
                    )
                })?;
                match run_view {
                    Some(run_view) => write_response_for_method(
                        stream,
                        &request.method,
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
        let head_view = with_admin_state(&context.state, |state| {
            state.export_head_artifact_view(&burn_p2p::HeadId::new(head_id))
        })?;
        match head_view {
            Some(head_view) => write_response_for_method(
                stream,
                &request.method,
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

#[cfg(any(not(feature = "browser-edge"), not(feature = "artifact-publish")))]
pub(crate) fn handle_portal_artifact_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _current_config: &BootstrapDaemonConfig,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn handle_artifact_publish_get_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if !matches!(request.method.as_str(), "GET" | "HEAD") {
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
        if !authorize_admin_route(stream, context, request, AdminCapability::ExportHeads)? {
            return Ok(true);
        }
        match request.path.as_str() {
            "/admin/artifacts/publications" => {
                let publications =
                    with_admin_state(&context.state, |state| state.export_published_artifacts())?;
                write_json_for_method(stream, &request.method, &publications)?;
                return Ok(true);
            }
            "/admin/artifacts/jobs" => {
                let jobs = with_admin_state(&context.state, |state| state.export_artifact_jobs())?;
                write_json_for_method(stream, &request.method, &jobs)?;
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
            if request.method == "HEAD" {
                write_response_for_method(
                    stream,
                    &request.method,
                    "405 Method Not Allowed",
                    "text/plain; charset=utf-8",
                    b"artifact live stream requires GET".to_vec(),
                )?;
                return Ok(true);
            }
            write_artifact_event_stream(stream, &context.state)?;
            return Ok(true);
        }
        "/artifacts/live/latest" => {
            match current_artifact_live_event(&context.state)? {
                Some(event) => write_json_for_method(stream, &request.method, &event)?,
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
        let aliases = with_admin_state(&context.state, |state| {
            state.export_artifact_alias_statuses()
        })?;
        write_json_for_method(stream, &request.method, &aliases)?;
        return Ok(true);
    }

    if let Some(experiment_id) = request.path.strip_prefix("/artifacts/aliases/") {
        let aliases = with_admin_state(&context.state, |state| {
            state.export_artifact_alias_statuses_for_experiment(&burn_p2p::ExperimentId::new(
                experiment_id,
            ))
        })?;
        write_json_for_method(stream, &request.method, &aliases)?;
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
                let runs = with_admin_state(&context.state, |state| {
                    state.export_artifact_run_summaries(&experiment_id)
                })?;
                write_json_for_method(stream, &request.method, &runs)?;
            }
            Some(run_id) => {
                let run_view = with_admin_state(&context.state, |state| {
                    state.export_artifact_run_view(
                        &experiment_id,
                        &burn_p2p_core::RunId::new(run_id),
                    )
                })?;
                match run_view {
                    Some(run_view) => write_json_for_method(stream, &request.method, &run_view)?,
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
        let head = with_admin_state(&context.state, |state| {
            state.export_head_artifact_view(&burn_p2p::HeadId::new(head_id))
        })?;
        match head {
            Some(head) => write_json_for_method(stream, &request.method, &head)?,
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
        let job = with_admin_state(&context.state, |state| {
            state.export_artifact_job(&burn_p2p_core::ExportJobId::new(export_job_id))
        })?;
        match job {
            Some(job) => write_json_for_method(stream, &request.method, &job)?,
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
        let artifact = with_admin_state(&context.state, |state| {
            state.resolve_artifact_download(&burn_p2p_core::DownloadTicketId::new(ticket_id))
        })?;
        match artifact {
            Some(artifact) => {
                let requested_range =
                    parse_requested_byte_range(request.headers.get("range").map(String::as_str))?;
                if let Some(redirect_url) = artifact.redirect_url {
                    write_response_with_headers_for_method(
                        stream,
                        &request.method,
                        "302 Found",
                        "text/plain; charset=utf-8",
                        &[("Location", redirect_url)],
                        Vec::new(),
                    )?;
                } else {
                    let mut headers = vec![(
                        "Content-Disposition",
                        format!("attachment; filename=\"{}\"", artifact.file_name),
                    )];
                    headers.push(("Accept-Ranges", "bytes".into()));
                    match artifact.body {
                        burn_p2p_publish::DownloadArtifactBody::Empty => {
                            write_response_with_headers_for_method(
                                stream,
                                &request.method,
                                "204 No Content",
                                &artifact.content_type,
                                &headers,
                                Vec::new(),
                            )?;
                        }
                        burn_p2p_publish::DownloadArtifactBody::Bytes(bytes) => {
                            if let Some((start, end_inclusive)) =
                                normalize_requested_byte_range(requested_range, bytes.len() as u64)?
                            {
                                headers.push((
                                    "Content-Range",
                                    format!("bytes {}-{}/{}", start, end_inclusive, bytes.len()),
                                ));
                                write_response_with_headers_for_method(
                                    stream,
                                    &request.method,
                                    "206 Partial Content",
                                    &artifact.content_type,
                                    &headers,
                                    bytes[start as usize..=end_inclusive as usize].to_vec(),
                                )?;
                            } else {
                                write_response_with_headers_for_method(
                                    stream,
                                    &request.method,
                                    "200 OK",
                                    &artifact.content_type,
                                    &headers,
                                    bytes,
                                )?;
                            }
                        }
                        burn_p2p_publish::DownloadArtifactBody::LocalFile {
                            path,
                            content_length,
                        } => {
                            if let Some((start, end_inclusive)) =
                                normalize_requested_byte_range(requested_range, content_length)?
                            {
                                headers.push((
                                    "Content-Range",
                                    format!("bytes {}-{}/{}", start, end_inclusive, content_length),
                                ));
                                let mut file = std::fs::File::open(&path)?;
                                use std::io::Seek;
                                file.seek(std::io::SeekFrom::Start(start))?;
                                let reader =
                                    std::io::BufReader::new(file.take(end_inclusive - start + 1));
                                write_stream_response_with_headers_for_method(
                                    stream,
                                    &request.method,
                                    "206 Partial Content",
                                    &artifact.content_type,
                                    &headers,
                                    end_inclusive - start + 1,
                                    reader,
                                )?;
                            } else {
                                write_file_response_with_headers_for_method(
                                    stream,
                                    &request.method,
                                    "200 OK",
                                    &artifact.content_type,
                                    &headers,
                                    &path,
                                    content_length,
                                )?;
                            }
                        }
                        burn_p2p_publish::DownloadArtifactBody::RemoteProxy {
                            url,
                            content_length,
                        } => {
                            #[cfg(feature = "artifact-s3")]
                            {
                                let mut proxy_request = reqwest::blocking::Client::new().get(url);
                                if let Some((start, end_inclusive)) =
                                    normalize_requested_byte_range(requested_range, content_length)?
                                {
                                    proxy_request = proxy_request.header(
                                        reqwest::header::RANGE,
                                        format!("bytes={start}-{end_inclusive}"),
                                    );
                                }
                                let response = proxy_request.send()?.error_for_status()?;
                                if requested_range.is_some()
                                    && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
                                {
                                    write_response(
                                        stream,
                                        "502 Bad Gateway",
                                        "text/plain; charset=utf-8",
                                        b"upstream artifact proxy did not honor byte range"
                                            .to_vec(),
                                    )?;
                                    return Ok(true);
                                }
                                if let Some(content_range) = response
                                    .headers()
                                    .get(reqwest::header::CONTENT_RANGE)
                                    .and_then(|value| value.to_str().ok())
                                {
                                    headers.push(("Content-Range", content_range.to_owned()));
                                }
                                write_stream_response_with_headers_for_method(
                                    stream,
                                    &request.method,
                                    if requested_range.is_some() {
                                        "206 Partial Content"
                                    } else {
                                        "200 OK"
                                    },
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

#[cfg(feature = "artifact-publish")]
fn parse_requested_byte_range(
    header: Option<&str>,
) -> Result<Option<RequestedByteRange>, Box<dyn std::error::Error>> {
    let Some(header) = header.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let Some(ranges) = header.strip_prefix("bytes=") else {
        return Err("unsupported range unit".into());
    };
    if ranges.contains(',') {
        return Err("multipart ranges are unsupported".into());
    }
    let (start, end_inclusive) = ranges.split_once('-').unwrap_or((ranges, ""));
    if start.is_empty() && end_inclusive.is_empty() {
        return Err("empty byte range".into());
    }
    Ok(Some(RequestedByteRange {
        start: (!start.is_empty())
            .then(|| start.parse::<u64>())
            .transpose()?,
        end_inclusive: (!end_inclusive.is_empty())
            .then(|| end_inclusive.parse::<u64>())
            .transpose()?,
    }))
}

#[cfg(feature = "artifact-publish")]
fn normalize_requested_byte_range(
    requested: Option<RequestedByteRange>,
    content_length: u64,
) -> Result<Option<(u64, u64)>, Box<dyn std::error::Error>> {
    let Some(requested) = requested else {
        return Ok(None);
    };
    if content_length == 0 {
        return Err("requested a byte range for an empty artifact".into());
    }
    let (start, end_inclusive) = match (requested.start, requested.end_inclusive) {
        (Some(start), Some(end_inclusive)) => (start, end_inclusive.min(content_length - 1)),
        (Some(start), None) => (start, content_length - 1),
        (None, Some(suffix_len)) => {
            let bounded = suffix_len.min(content_length);
            (content_length - bounded, content_length - 1)
        }
        (None, None) => return Err("invalid byte range".into()),
    };
    if start >= content_length || end_inclusive < start {
        return Err(
            format!("requested byte range {start}-{end_inclusive} is out of bounds").into(),
        );
    }
    Ok(Some((start, end_inclusive)))
}

#[cfg(not(feature = "artifact-publish"))]
pub(crate) fn handle_artifact_publish_get_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn handle_artifact_publish_post_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.path.starts_with("/admin/artifacts/") {
        if request.method == "POST" && request.path == "/admin/artifacts/mirror-peer" {
            if !authorize_admin_route(stream, context, request, AdminCapability::RegisterLiveHead)?
            {
                return Ok(true);
            }
            let mirror: PeerArtifactMirrorRequest = serde_json::from_slice(&request.body)?;
            let Some(control) = context.control_handle.as_ref() else {
                write_response(
                    stream,
                    "503 Service Unavailable",
                    "text/plain; charset=utf-8",
                    b"edge peer control handle is unavailable".to_vec(),
                )?;
                return Ok(true);
            };
            let artifact_store = with_admin_state(&context.state, |state| {
                Ok::<_, std::convert::Infallible>(
                    state.artifact_store_root.clone().map(FsArtifactStore::new),
                )
            })?;
            match mirror_peer_artifact_into_store(control, mirror, artifact_store.as_ref()) {
                Ok(response) => write_json(stream, &response)?,
                Err(error) => {
                    let body = format!("peer artifact mirror failed: {error}");
                    write_response(
                        stream,
                        peer_artifact_mirror_error_status(&body),
                        "text/plain; charset=utf-8",
                        body.into_bytes(),
                    )?;
                }
            }
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
        if !authorize_admin_route(stream, context, request, AdminCapability::Control)? {
            return Ok(true);
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/admin/artifacts/backfill-aliases") => {
                let request: burn_p2p_publish::ArtifactBackfillRequest = if request.body.is_empty()
                {
                    burn_p2p_publish::ArtifactBackfillRequest::default()
                } else {
                    serde_json::from_slice(&request.body)?
                };
                let result = with_admin_state(&context.state, |state| {
                    state.backfill_artifact_aliases(request)
                })?;
                write_json(stream, &result)?;
                return Ok(true);
            }
            ("POST", "/admin/artifacts/republish") => {
                let export: ArtifactExportHttpRequest = serde_json::from_slice(&request.body)?;
                let job = with_admin_state(&context.state, |state| {
                    state.request_artifact_export(ExportRequest {
                        requested_by_principal_id: None,
                        experiment_id: export.experiment_id,
                        run_id: export.run_id,
                        head_id: export.head_id,
                        artifact_profile: export.artifact_profile,
                        publication_target_id: export.publication_target_id,
                        artifact_alias_id: export.artifact_alias_id,
                    })
                })?;
                write_json(stream, &job)?;
                return Ok(true);
            }
            ("POST", "/admin/artifacts/prune") => {
                let request: burn_p2p_publish::ArtifactPruneRequest = if request.body.is_empty() {
                    burn_p2p_publish::ArtifactPruneRequest::default()
                } else {
                    serde_json::from_slice(&request.body)?
                };
                let result = with_admin_state(&context.state, |state| {
                    state.prune_artifact_publications(request)
                })?;
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
            let job = with_admin_state(&context.state, |state| {
                state.request_artifact_export(ExportRequest {
                    requested_by_principal_id: Some(principal_id),
                    experiment_id: export.experiment_id,
                    run_id: export.run_id,
                    head_id: export.head_id,
                    artifact_profile: export.artifact_profile,
                    publication_target_id: export.publication_target_id,
                    artifact_alias_id: export.artifact_alias_id,
                })
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
            let ticket = with_admin_state(&context.state, |state| {
                state.request_artifact_download_ticket(DownloadTicketRequest {
                    principal_id,
                    experiment_id: download.experiment_id,
                    run_id: download.run_id,
                    head_id: download.head_id,
                    artifact_profile: download.artifact_profile,
                    publication_target_id: download.publication_target_id,
                    artifact_alias_id: download.artifact_alias_id,
                })
            })?;
            write_json(stream, &ticket)?;
            return Ok(true);
        }
        _ => {}
    }

    Ok(false)
}

#[cfg(not(feature = "artifact-publish"))]
pub(crate) fn handle_artifact_publish_post_route(
    _stream: &mut TcpStream,
    _context: &HttpServerContext,
    _request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn artifact_publication_enabled(state: &Arc<Mutex<BootstrapAdminState>>) -> bool {
    with_admin_state(state, |state| {
        Ok::<_, std::convert::Infallible>(
            state.publication_store_root.is_some() && state.artifact_store_root.is_some(),
        )
    })
    .expect("artifact publication check should tolerate bootstrap admin state access")
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn resolve_artifact_request_principal(
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
        .get_session(&ContentId::new(session_id.clone()))?
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
pub(crate) fn session_allows_artifact_access(
    session: &PrincipalSession,
    experiment_id: &burn_p2p::ExperimentId,
) -> bool {
    session
        .claims
        .granted_scopes
        .iter()
        .any(|scope| match scope {
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
pub(crate) fn current_artifact_live_event(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Option<burn_p2p_core::ArtifactLiveEvent>, Box<dyn std::error::Error>> {
    with_admin_state(state, |state| state.export_latest_artifact_live_event())
}
