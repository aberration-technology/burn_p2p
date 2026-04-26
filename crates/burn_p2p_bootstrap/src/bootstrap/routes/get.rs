use super::*;
use burn_p2p_bootstrap::{
    OperatorAuditKind, OperatorAuditQuery, OperatorControlReplayKind, OperatorControlReplayQuery,
    OperatorReplayQuery, render_operator_audit_html, render_operator_replay_html,
    render_operator_replay_snapshot_html, render_operator_retention_html,
};
use burn_p2p_core::{ExperimentId, HeadId, NetworkId, RevisionId, StudyId, WindowId};

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
        if !authorize_admin_route(stream, context, request, AdminCapability::ExportReceipts)? {
            return Ok(true);
        }
        let query = ReceiptQuery {
            study_id: None,
            experiment_id: None,
            revision_id: None,
            peer_id: None,
        };
        let receipts = with_admin_state(state, |state| state.export_receipts_page(&query, page))?;
        write_json(stream, &receipts)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(page) = page_request_from_path(&request.path, "/heads/page")?
    {
        if !authorize_admin_route(stream, context, request, AdminCapability::ExportHeads)? {
            return Ok(true);
        }
        let heads = with_admin_state(state, |state| {
            state.export_heads_page(
                &burn_p2p_bootstrap::HeadQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    head_id: None,
                },
                page,
            )
        })?;
        write_json(stream, &heads)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(page) = page_request_from_path(&request.path, "/merges/page")?
    {
        if !authorize_admin_route(stream, context, request, AdminCapability::ExportHeads)? {
            return Ok(true);
        }
        let merges = with_admin_state(state, |state| state.export_merges_page(page))?;
        write_json(stream, &merges)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_audit_request_from_path(&request.path, "/operator/audit")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let (audit_summary, audit_facets, audit_page) = with_admin_state(state, |state| {
            Ok::<_, Box<dyn std::error::Error>>((
                state.export_operator_audit_summary(&query)?,
                state.export_operator_audit_facets(&query, 8)?,
                state.export_operator_audit_page(&query, page)?,
            ))
        })?;
        write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_operator_audit_html(&query, &audit_page, &audit_summary, &audit_facets)
                .into_bytes(),
        )?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_audit_request_from_path(&request.path, "/operator/audit/page")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let audit_page = with_admin_state(state, |state| {
            state.export_operator_audit_page(&query, page)
        })?;
        write_json(stream, &audit_page)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(query) =
            operator_audit_summary_request_from_path(&request.path, "/operator/audit/summary")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let audit_summary =
            with_admin_state(state, |state| state.export_operator_audit_summary(&query))?;
        write_json(stream, &audit_summary)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((limit, query)) =
            operator_audit_facets_request_from_path(&request.path, "/operator/audit/facets")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let audit_facets = with_admin_state(state, |state| {
            state.export_operator_audit_facets(&query, limit)
        })?;
        write_json(stream, &audit_facets)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(captured_at) =
            operator_replay_request_from_path(&request.path, "/operator/replay/snapshot/view")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let replay_snapshot = with_admin_state(state, |state| {
            state.export_operator_replay_snapshot(captured_at)
        })?;
        if let Some(snapshot) = replay_snapshot {
            write_response(
                stream,
                "200 OK",
                "text/html; charset=utf-8",
                render_operator_replay_snapshot_html(captured_at, &snapshot).into_bytes(),
            )?;
        } else {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"operator replay snapshot not found".to_vec(),
            )?;
        }
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(captured_at) =
            operator_replay_request_from_path(&request.path, "/operator/replay/snapshot")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let replay_snapshot = with_admin_state(state, |state| {
            state.export_operator_replay_snapshot(captured_at)
        })?;
        write_json(stream, &replay_snapshot)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_replay_page_request_from_path(&request.path, "/operator/replay")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let replay_page = with_admin_state(state, |state| {
            state.export_operator_replay_page(&query, page)
        })?;
        write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_operator_replay_html(&query, &replay_page).into_bytes(),
        )?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_replay_page_request_from_path(&request.path, "/operator/replay/page")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let replay_page = with_admin_state(state, |state| {
            state.export_operator_replay_page(&query, page)
        })?;
        write_json(stream, &replay_page)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_control_replay_page_request_from_path(&request.path, "/operator/control")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let (control_summary, control_page) = with_admin_state(state, |state| {
            Ok::<_, Box<dyn std::error::Error>>((
                state.export_operator_control_replay_summary(&query)?,
                state.export_operator_control_replay_page(&query, page)?,
            ))
        })?;
        write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_operator_control_replay_html(&query, &control_page, &control_summary)
                .into_bytes(),
        )?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some((page, query)) =
            operator_control_replay_page_request_from_path(&request.path, "/operator/control/page")?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let control_page = with_admin_state(state, |state| {
            state.export_operator_control_replay_page(&query, page)
        })?;
        write_json(stream, &control_page)?;
        return Ok(true);
    }

    if request.method == "GET"
        && let Some(query) = operator_control_replay_summary_request_from_path(
            &request.path,
            "/operator/control/summary",
        )?
    {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let control_summary = with_admin_state(state, |state| {
            state.export_operator_control_replay_summary(&query)
        })?;
        write_json(stream, &control_summary)?;
        return Ok(true);
    }

    if request.method == "GET" && request.path == "/operator/retention" {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let retention = with_admin_state(state, |state| state.export_operator_retention_summary())?;
        write_json(stream, &retention)?;
        return Ok(true);
    }

    if request.method == "GET" && request.path == "/operator/retention/view" {
        if !authorize_admin_route(
            stream,
            context,
            request,
            AdminCapability::ExportOperatorControlReplay,
        )? {
            return Ok(true);
        }
        let retention = with_admin_state(state, |state| state.export_operator_retention_summary())?;
        write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_operator_retention_html(&retention).into_bytes(),
        )?;
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
            if !authorize_admin_route(
                stream,
                context,
                request,
                AdminCapability::ExportDiagnosticsBundle,
            )? {
                return Ok(true);
            }
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
        ("GET", "/browser/seeds/signed") => {
            if !browser_join_enabled(current_config) {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser join disabled".to_vec(),
                )?;
                return Ok(true);
            }
            let Some(advertisement) = with_admin_state(state, |state| {
                Ok::<_, std::convert::Infallible>(current_browser_seed_advertisement(
                    plan,
                    current_config,
                    request,
                    state.runtime_snapshot.as_ref(),
                ))
            })?
            else {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser seed advertisement unavailable".to_vec(),
                )?;
                return Ok(true);
            };
            let signed = sign_browser_snapshot(
                plan,
                admin_signer_peer_id,
                "burn_p2p.browser_seed_advertisement",
                advertisement,
            )?;
            write_json(stream, &signed)?;
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
            let leaderboard = current_browser_leaderboard(plan, state)?;
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
                current_browser_leaderboard(plan, state)?,
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
        ("GET", "/events") => {
            if !authorize_admin_route(stream, context, request, AdminCapability::ExportDiagnostics)?
            {
                return Ok(true);
            }
            write_event_stream(stream, plan, state, *remaining_work_units)?
        }
        ("GET", "/receipts") => {
            if !authorize_admin_route(stream, context, request, AdminCapability::ExportReceipts)? {
                return Ok(true);
            }
            let query = ReceiptQuery {
                study_id: None,
                experiment_id: None,
                revision_id: None,
                peer_id: None,
            };
            let receipts = with_admin_state(state, |state| {
                Ok::<_, std::convert::Infallible>(state.export_receipts(&query))
            })?;
            write_json(stream, &receipts)?;
        }
        ("GET", "/heads") => {
            if !authorize_admin_route(stream, context, request, AdminCapability::ExportHeads)? {
                return Ok(true);
            }
            let heads = with_admin_state(state, |state| {
                Ok::<_, std::convert::Infallible>(state.export_heads(
                    &burn_p2p_bootstrap::HeadQuery {
                        study_id: None,
                        experiment_id: None,
                        revision_id: None,
                        head_id: None,
                    },
                ))
            })?;
            write_json(stream, &heads)?;
        }
        ("GET", "/reducers/load") => {
            if !authorize_admin_route(stream, context, request, AdminCapability::ExportReducerLoad)?
            {
                return Ok(true);
            }
            let reducer_load = with_admin_state(state, |state| {
                Ok::<_, std::convert::Infallible>(state.export_reducer_load(
                    &burn_p2p_bootstrap::ReducerLoadQuery {
                        study_id: None,
                        experiment_id: None,
                        peer_id: None,
                    },
                ))
            })?;
            write_json(stream, &reducer_load)?;
        }
        ("GET", "/revocations") => {
            let auth = auth_state
                .as_ref()
                .ok_or("browser-edge auth is not configured")?;
            let (quarantined_peers, banned_peers) = with_admin_state(state, |state| {
                Ok::<_, std::convert::Infallible>((
                    state.quarantined_peers.clone(),
                    state.banned_peers.clone(),
                ))
            })?;
            let response = RevocationResponse {
                network_id: plan.network_id().clone(),
                minimum_revocation_epoch: current_revocation_epoch(auth, state),
                quarantined_peers,
                banned_peers,
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

fn operator_audit_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<(burn_p2p_core::PageRequest, OperatorAuditQuery)>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some((
            burn_p2p_core::PageRequest::default(),
            OperatorAuditQuery::default(),
        )));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut page = burn_p2p_core::PageRequest::default();
    let mut audit = OperatorAuditQuery::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "offset" => {
                page.offset = value.parse::<usize>()?;
            }
            "limit" => {
                page.limit = value.parse::<usize>()?;
            }
            "kind" => {
                audit.kind = Some(if let Some(kind) = OperatorAuditKind::from_slug(value) {
                    kind
                } else {
                    return Err(format!("unsupported operator audit kind `{value}`").into());
                });
            }
            "study_id" => {
                audit.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                audit.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                audit.revision_id = Some(RevisionId::new(value));
            }
            "peer_id" => {
                audit.peer_id = Some(PeerId::new(value));
            }
            "head_id" => {
                audit.head_id = Some(HeadId::new(value));
            }
            "since" => {
                audit.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                audit.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                audit.text = Some(value.replace('+', " "));
            }
            _ => {}
        }
    }

    Ok(Some((page.normalized(), audit)))
}

fn operator_replay_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<Option<DateTime<Utc>>>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some(None));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut captured_at = None;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "captured_at" && !value.is_empty() {
            captured_at = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
        }
    }
    Ok(Some(captured_at))
}

fn operator_audit_summary_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<OperatorAuditQuery>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some(OperatorAuditQuery::default()));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut audit = OperatorAuditQuery::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "kind" => {
                audit.kind = Some(if let Some(kind) = OperatorAuditKind::from_slug(value) {
                    kind
                } else {
                    return Err(format!("unsupported operator audit kind `{value}`").into());
                });
            }
            "study_id" => {
                audit.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                audit.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                audit.revision_id = Some(RevisionId::new(value));
            }
            "peer_id" => {
                audit.peer_id = Some(PeerId::new(value));
            }
            "head_id" => {
                audit.head_id = Some(HeadId::new(value));
            }
            "since" => {
                audit.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                audit.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                audit.text = Some(value.replace('+', " "));
            }
            _ => {}
        }
    }

    Ok(Some(audit))
}

fn operator_audit_facets_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<(usize, OperatorAuditQuery)>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some((8, OperatorAuditQuery::default())));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut audit = OperatorAuditQuery::default();
    let mut limit = 8usize;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "kind" => {
                audit.kind = Some(if let Some(kind) = OperatorAuditKind::from_slug(value) {
                    kind
                } else {
                    return Err(format!("unsupported operator audit kind `{value}`").into());
                });
            }
            "study_id" => {
                audit.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                audit.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                audit.revision_id = Some(RevisionId::new(value));
            }
            "peer_id" => {
                audit.peer_id = Some(PeerId::new(value));
            }
            "head_id" => {
                audit.head_id = Some(HeadId::new(value));
            }
            "since" => {
                audit.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                audit.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                audit.text = Some(value.replace('+', " "));
            }
            "limit" => {
                limit = value.parse::<usize>()?;
            }
            _ => {}
        }
    }

    Ok(Some((limit, audit)))
}

fn operator_replay_page_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<(burn_p2p_core::PageRequest, OperatorReplayQuery)>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some((
            burn_p2p_core::PageRequest::default(),
            OperatorReplayQuery::default(),
        )));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut page = burn_p2p_core::PageRequest::default();
    let mut replay = OperatorReplayQuery::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "offset" => {
                page.offset = value.parse::<usize>()?;
            }
            "limit" => {
                page.limit = value.parse::<usize>()?;
            }
            "study_id" => {
                replay.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                replay.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                replay.revision_id = Some(RevisionId::new(value));
            }
            "head_id" => {
                replay.head_id = Some(HeadId::new(value));
            }
            "since" => {
                replay.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                replay.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                replay.text = Some(value.replace('+', " "));
            }
            _ => {}
        }
    }

    Ok(Some((page.normalized(), replay)))
}

fn operator_control_replay_page_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<
    Option<(burn_p2p_core::PageRequest, OperatorControlReplayQuery)>,
    Box<dyn std::error::Error>,
> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some((
            burn_p2p_core::PageRequest::default(),
            OperatorControlReplayQuery::default(),
        )));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut page = burn_p2p_core::PageRequest::default();
    let mut control = OperatorControlReplayQuery::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "offset" => {
                page.offset = value.parse::<usize>()?;
            }
            "limit" => {
                page.limit = value.parse::<usize>()?;
            }
            "kind" => {
                control.kind = Some(
                    if let Some(kind) = OperatorControlReplayKind::from_slug(value) {
                        kind
                    } else {
                        return Err(format!("unsupported operator control kind `{value}`").into());
                    },
                );
            }
            "network_id" => {
                control.network_id = Some(NetworkId::new(value));
            }
            "study_id" => {
                control.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                control.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                control.revision_id = Some(RevisionId::new(value));
            }
            "peer_id" => {
                control.peer_id = Some(PeerId::new(value));
            }
            "window_id" => {
                control.window_id = Some(WindowId(value.parse::<u64>()?));
            }
            "since" => {
                control.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                control.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                control.text = Some(value.replace('+', " "));
            }
            _ => {}
        }
    }

    Ok(Some((page.normalized(), control)))
}

fn operator_control_replay_summary_request_from_path(
    request_path: &str,
    base_path: &str,
) -> Result<Option<OperatorControlReplayQuery>, Box<dyn std::error::Error>> {
    let Some(path) = request_path.strip_prefix(base_path) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(Some(OperatorControlReplayQuery::default()));
    }
    let Some(query) = path.strip_prefix('?') else {
        return Ok(None);
    };

    let mut control = OperatorControlReplayQuery::default();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if value.is_empty() {
            continue;
        }
        match key {
            "kind" => {
                control.kind = Some(
                    if let Some(kind) = OperatorControlReplayKind::from_slug(value) {
                        kind
                    } else {
                        return Err(format!("unsupported operator control kind `{value}`").into());
                    },
                );
            }
            "network_id" => {
                control.network_id = Some(NetworkId::new(value));
            }
            "study_id" => {
                control.study_id = Some(StudyId::new(value));
            }
            "experiment_id" => {
                control.experiment_id = Some(ExperimentId::new(value));
            }
            "revision_id" => {
                control.revision_id = Some(RevisionId::new(value));
            }
            "peer_id" => {
                control.peer_id = Some(PeerId::new(value));
            }
            "window_id" => {
                control.window_id = Some(WindowId(value.parse::<u64>()?));
            }
            "since" => {
                control.since = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "until" => {
                control.until = Some(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc));
            }
            "text" => {
                control.text = Some(value.replace('+', " "));
            }
            _ => {}
        }
    }

    Ok(Some(control))
}

pub(crate) fn current_diagnostics(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnostics {
    with_admin_state(state, |state| {
        Ok::<_, std::convert::Infallible>(state.diagnostics(plan, Utc::now(), remaining_work_units))
    })
    .expect("diagnostics should tolerate bootstrap admin state access")
}

pub(crate) fn current_diagnostics_bundle(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnosticsBundle {
    with_admin_state(state, |state| {
        Ok::<_, std::convert::Infallible>(state.diagnostics_bundle(
            plan,
            Utc::now(),
            remaining_work_units,
        ))
    })
    .expect("diagnostics bundle should tolerate bootstrap admin state access")
}
