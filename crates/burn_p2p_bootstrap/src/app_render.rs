#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
use burn_p2p_core::HeadEvalReport;
use burn_p2p_core::{ExperimentId, NetworkId};

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn format_artifact_metric_value(value: &burn_p2p_core::MetricValue) -> String {
    match value {
        burn_p2p_core::MetricValue::Integer(value) => value.to_string(),
        burn_p2p_core::MetricValue::Float(value) => format!("{value:.4}"),
        burn_p2p_core::MetricValue::Bool(value) => value.to_string(),
        burn_p2p_core::MetricValue::Text(value) => value.clone(),
    }
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_alias_history_rows(
    alias_history: &[burn_p2p_core::ArtifactAlias],
) -> Vec<burn_p2p_app::AppArtifactAliasHistoryRow> {
    alias_history
        .iter()
        .map(|alias| burn_p2p_app::AppArtifactAliasHistoryRow {
            alias_name: alias.alias_name.clone(),
            scope: format!("{:?}", alias.scope),
            artifact_profile: format!("{:?}", alias.artifact_profile),
            head_id: alias.head_id.as_str().to_owned(),
            resolved_at: alias.resolved_at.to_rfc3339(),
            source_reason: format!("{:?}", alias.source_reason),
        })
        .collect()
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_published_artifact_rows(
    publications: &[burn_p2p_core::PublishedArtifactRecord],
) -> Vec<burn_p2p_app::AppPublishedArtifactRow> {
    publications
        .iter()
        .map(|record| burn_p2p_app::AppPublishedArtifactRow {
            head_id: record.head_id.as_str().to_owned(),
            artifact_profile: format!("{:?}", record.artifact_profile),
            publication_target_id: record.publication_target_id.as_str().to_owned(),
            status: format!("{:?}", record.status),
            object_key: record.object_key.clone(),
            content_length: record.content_length,
            created_at: record.created_at.to_rfc3339(),
            expires_at: record.expires_at.map(|timestamp| timestamp.to_rfc3339()),
        })
        .collect()
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_eval_summary_rows(
    eval_reports: &[HeadEvalReport],
) -> Vec<burn_p2p_app::AppHeadEvalSummaryRow> {
    eval_reports
        .iter()
        .map(|report| burn_p2p_app::AppHeadEvalSummaryRow {
            head_id: report.head_id.as_str().to_owned(),
            eval_protocol_id: report.eval_protocol_id.as_str().to_owned(),
            status: format!("{:?}", report.status),
            dataset_view_id: report.dataset_view_id.as_str().to_owned(),
            sample_count: report.sample_count,
            metric_summary: if report.metric_values.is_empty() {
                "n/a".into()
            } else {
                report
                    .metric_values
                    .iter()
                    .map(|(label, value)| {
                        format!("{label}={}", format_artifact_metric_value(value))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            finished_at: report.finished_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_artifact_run_summary_rows(
    summaries: &[crate::ArtifactRunSummary],
) -> Vec<burn_p2p_app::AppArtifactRunSummaryRow> {
    summaries
        .iter()
        .map(|summary| burn_p2p_app::AppArtifactRunSummaryRow {
            experiment_id: summary.experiment_id.as_str().to_owned(),
            run_id: summary.run_id.as_str().to_owned(),
            latest_head_id: summary.latest_head_id.as_str().to_owned(),
            alias_count: summary.alias_count,
            alias_history_count: summary.alias_history_count,
            published_artifact_count: summary.published_artifact_count,
            run_view_path: format!(
                "/portal/artifacts/runs/{}/{}",
                summary.experiment_id.as_str(),
                summary.run_id.as_str()
            ),
            json_view_path: format!(
                "/artifacts/runs/{}/{}",
                summary.experiment_id.as_str(),
                summary.run_id.as_str()
            ),
        })
        .collect()
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_artifact_run_view(view: &crate::ArtifactRunView) -> burn_p2p_app::AppArtifactRunView {
    let aliases = view
        .aliases
        .iter()
        .map(|row| {
            let status = row
                .published_artifact
                .as_ref()
                .map(|record| format!("{:?}", record.status))
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| format!("{:?}", job.status))
                })
                .unwrap_or_else(|| "NotPublished".into());
            let publication_target_id = row
                .published_artifact
                .as_ref()
                .map(|record| record.publication_target_id.as_str().to_owned())
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| job.publication_target_id.as_str().to_owned())
                })
                .unwrap_or_else(|| crate::DEFAULT_PUBLICATION_TARGET_ID.to_owned());
            burn_p2p_app::AppArtifactRow {
                alias_name: row.alias.alias_name.clone(),
                scope: format!("{:?}", row.alias.scope),
                artifact_profile: format!("{:?}", row.alias.artifact_profile),
                experiment_id: row.alias.experiment_id.as_str().to_owned(),
                run_id: row
                    .alias
                    .run_id
                    .as_ref()
                    .map(|run_id| run_id.as_str().to_owned()),
                head_id: row.alias.head_id.as_str().to_owned(),
                publication_target_id,
                artifact_alias_id: Some(row.alias.artifact_alias_id.as_str().to_owned()),
                status,
                last_published_at: row
                    .published_artifact
                    .as_ref()
                    .map(|record| record.created_at.to_rfc3339()),
                history_count: row.history_count,
                previous_head_id: row
                    .previous_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
                head_view_path: format!("/portal/artifacts/heads/{}", row.alias.head_id.as_str()),
                run_view_path: row.alias.run_id.as_ref().map(|run_id| {
                    format!(
                        "/portal/artifacts/runs/{}/{}",
                        row.alias.experiment_id.as_str(),
                        run_id.as_str()
                    )
                }),
                export_path: "/artifacts/export".into(),
                download_ticket_path: "/artifacts/download-ticket".into(),
            }
        })
        .collect();

    burn_p2p_app::AppArtifactRunView {
        experiment_id: view.experiment_id.as_str().to_owned(),
        run_id: view.run_id.as_str().to_owned(),
        latest_head_id: view
            .heads
            .iter()
            .max_by_key(|head| head.created_at)
            .map(|head| head.head_id.as_str().to_owned()),
        heads: view
            .heads
            .iter()
            .map(|head| burn_p2p_app::AppHeadRow {
                experiment_id: head.experiment_id.as_str().to_owned(),
                revision_id: head.revision_id.as_str().to_owned(),
                head_id: head.head_id.as_str().to_owned(),
                global_step: head.global_step,
                created_at: head.created_at.to_rfc3339(),
            })
            .collect(),
        aliases,
        alias_history: app_alias_history_rows(&view.alias_history),
        eval_reports: app_eval_summary_rows(&view.eval_reports),
        publications: app_published_artifact_rows(&view.published_artifacts),
        app_path: "/".into(),
        json_view_path: format!(
            "/artifacts/runs/{}/{}",
            view.experiment_id.as_str(),
            view.run_id.as_str()
        ),
    }
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
fn app_head_artifact_view(view: &crate::HeadArtifactView) -> burn_p2p_app::AppHeadArtifactView {
    let aliases = view
        .aliases
        .iter()
        .map(|alias| burn_p2p_app::AppArtifactRow {
            alias_name: alias.alias_name.clone(),
            scope: format!("{:?}", alias.scope),
            artifact_profile: format!("{:?}", alias.artifact_profile),
            experiment_id: alias.experiment_id.as_str().to_owned(),
            run_id: alias
                .run_id
                .as_ref()
                .map(|run_id| run_id.as_str().to_owned()),
            head_id: alias.head_id.as_str().to_owned(),
            publication_target_id: crate::DEFAULT_PUBLICATION_TARGET_ID.to_owned(),
            artifact_alias_id: Some(alias.artifact_alias_id.as_str().to_owned()),
            status: "Resolved".into(),
            last_published_at: None,
            history_count: 0,
            previous_head_id: None,
            head_view_path: format!("/portal/artifacts/heads/{}", alias.head_id.as_str()),
            run_view_path: alias.run_id.as_ref().map(|run_id| {
                format!(
                    "/portal/artifacts/runs/{}/{}",
                    alias.experiment_id.as_str(),
                    run_id.as_str()
                )
            }),
            export_path: "/artifacts/export".into(),
            download_ticket_path: "/artifacts/download-ticket".into(),
        })
        .collect();

    burn_p2p_app::AppHeadArtifactView {
        head: burn_p2p_app::AppHeadRow {
            experiment_id: view.head.experiment_id.as_str().to_owned(),
            revision_id: view.head.revision_id.as_str().to_owned(),
            head_id: view.head.head_id.as_str().to_owned(),
            global_step: view.head.global_step,
            created_at: view.head.created_at.to_rfc3339(),
        },
        experiment_id: view.head.experiment_id.as_str().to_owned(),
        run_id: view.run_id.as_str().to_owned(),
        aliases,
        alias_history: app_alias_history_rows(&view.alias_history),
        eval_reports: app_eval_summary_rows(&view.eval_reports),
        publications: app_published_artifact_rows(&view.published_artifacts),
        available_profiles: view
            .available_profiles
            .iter()
            .map(|profile| format!("{profile:?}"))
            .collect(),
        app_path: "/".into(),
        run_view_path: format!(
            "/portal/artifacts/runs/{}/{}",
            view.head.experiment_id.as_str(),
            view.run_id.as_str()
        ),
        json_view_path: format!("/artifacts/heads/{}", view.head.head_id.as_str()),
    }
}

#[cfg(feature = "browser-edge")]
#[allow(dead_code)]
fn control_summary_count_rows(summary: &crate::OperatorControlReplaySummary) -> Vec<String> {
    summary
        .counts_by_kind
        .iter()
        .map(|(kind, count)| format!("{kind}: {count}"))
        .collect()
}

fn control_summary_text(summary: &std::collections::BTreeMap<String, String>) -> String {
    let parts = summary
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "n/a".into()
    } else {
        parts.join(" · ")
    }
}

fn control_query_pairs(query: &crate::OperatorControlReplayQuery) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if let Some(kind) = query.kind.as_ref() {
        pairs.push(("kind".into(), kind.as_slug().into()));
    }
    if let Some(network_id) = query.network_id.as_ref() {
        pairs.push(("network_id".into(), network_id.as_str().to_owned()));
    }
    if let Some(study_id) = query.study_id.as_ref() {
        pairs.push(("study_id".into(), study_id.as_str().to_owned()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        pairs.push(("experiment_id".into(), experiment_id.as_str().to_owned()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        pairs.push(("revision_id".into(), revision_id.as_str().to_owned()));
    }
    if let Some(peer_id) = query.peer_id.as_ref() {
        pairs.push(("peer_id".into(), peer_id.as_str().to_owned()));
    }
    if let Some(window_id) = query.window_id {
        pairs.push(("window_id".into(), window_id.0.to_string()));
    }
    if let Some(since) = query.since {
        pairs.push(("since".into(), since.to_rfc3339()));
    }
    if let Some(until) = query.until {
        pairs.push(("until".into(), until.to_rfc3339()));
    }
    if let Some(text) = query.text.as_ref() {
        pairs.push(("text".into(), text.clone()));
    }
    pairs
}

fn control_query_tags(query: &crate::OperatorControlReplayQuery) -> Vec<String> {
    control_query_pairs(query)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

fn page_path_with_query(
    base_path: &str,
    query: &crate::OperatorControlReplayQuery,
    page: Option<burn_p2p_core::PageRequest>,
) -> String {
    let mut pairs = control_query_pairs(query);
    if let Some(page) = page {
        pairs.push(("offset".into(), page.offset.to_string()));
        pairs.push(("limit".into(), page.limit.to_string()));
    }
    if pairs.is_empty() {
        return base_path.to_owned();
    }
    let query = pairs
        .into_iter()
        .map(|(key, value)| format!("{}={}", key, value.replace(' ', "+")))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base_path}?{query}")
}

#[cfg(feature = "browser-edge")]
fn app_operator_control_replay_view(
    query: &crate::OperatorControlReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorControlReplayRecord>,
    summary: &crate::OperatorControlReplaySummary,
) -> burn_p2p_app::AppOperatorControlReplayPageView {
    let normalized = burn_p2p_core::PageRequest::new(page.offset, page.limit).normalized();
    let next_offset = normalized.offset + normalized.limit;
    let prev_offset = normalized.offset.saturating_sub(normalized.limit);
    burn_p2p_app::AppOperatorControlReplayPageView {
        summary: burn_p2p_app::AppOperatorControlReplaySummaryView {
            backend: summary.backend.clone(),
            record_count: summary.record_count,
            kind_counts: control_summary_count_rows(summary),
            distinct_network_count: summary.distinct_network_count,
            distinct_study_count: summary.distinct_study_count,
            distinct_experiment_count: summary.distinct_experiment_count,
            distinct_revision_count: summary.distinct_revision_count,
            distinct_peer_count: summary.distinct_peer_count,
            distinct_window_count: summary.distinct_window_count,
            earliest_captured_at: summary
                .earliest_captured_at
                .map(|timestamp| timestamp.to_rfc3339()),
            latest_captured_at: summary
                .latest_captured_at
                .map(|timestamp| timestamp.to_rfc3339()),
        },
        rows: page
            .items
            .iter()
            .map(|row| burn_p2p_app::AppOperatorControlReplayRow {
                record_id: row.record_id.clone(),
                kind: row.kind.as_slug().to_owned(),
                network_id: row.network_id.as_str().to_owned(),
                study_id: row.study_id.as_str().to_owned(),
                experiment_id: row.experiment_id.as_str().to_owned(),
                revision_id: row.revision_id.as_str().to_owned(),
                source_experiment_id: row
                    .source_experiment_id
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                source_revision_id: row
                    .source_revision_id
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                peer_id: row.peer_id.as_ref().map(|value| value.as_str().to_owned()),
                window_id: row.window_id.0,
                ends_before_window: row.ends_before_window.map(|value| value.0),
                slot_index: row.slot_index,
                plan_epoch: row.plan_epoch,
                captured_at: row.captured_at.to_rfc3339(),
                summary: control_summary_text(&row.summary),
            })
            .collect(),
        filter_tags: control_query_tags(query),
        offset: page.offset,
        limit: page.limit,
        total: page.total,
        json_page_path: page_path_with_query(
            "/operator/control/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
        ),
        json_summary_path: page_path_with_query("/operator/control/summary", query, None),
        prev_page_path: (page.offset > 0).then(|| {
            page_path_with_query(
                "/operator/control",
                query,
                Some(burn_p2p_core::PageRequest::new(prev_offset, page.limit)),
            )
        }),
        next_page_path: (next_offset < page.total).then(|| {
            page_path_with_query(
                "/operator/control",
                query,
                Some(burn_p2p_core::PageRequest::new(next_offset, page.limit)),
            )
        }),
    }
}

#[cfg(feature = "browser-edge")]
/// Performs the render dashboard html operation.
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    burn_p2p_app::render_dashboard_html(network_id.as_str())
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><body><main><h1>burn_p2p bootstrap</h1><p>Network <strong>{}</strong>. Portal support was not compiled into this build.</p></main></body></html>",
        network_id.as_str()
    )
}

#[cfg(feature = "browser-edge")]
/// Performs the render operator control replay html operation.
pub fn render_operator_control_replay_html(
    query: &crate::OperatorControlReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorControlReplayRecord>,
    summary: &crate::OperatorControlReplaySummary,
) -> String {
    burn_p2p_app::render_operator_control_replay_html(&app_operator_control_replay_view(
        query, page, summary,
    ))
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_operator_control_replay_html(
    query: &crate::OperatorControlReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorControlReplayRecord>,
    summary: &crate::OperatorControlReplaySummary,
) -> String {
    let filters = control_query_tags(query);
    let filter_html = if filters.is_empty() {
        "<p>No filters are active.</p>".to_owned()
    } else {
        format!("<p>{}</p>", filters.join(" | "))
    };
    let rows_html = if page.items.is_empty() {
        "<p>No lifecycle or schedule rows matched the current query.</p>".to_owned()
    } else {
        let rows = page
            .items
            .iter()
            .map(|row| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    row.kind.as_slug(),
                    row.record_id,
                    row.peer_id
                        .as_ref()
                        .map(|peer_id| peer_id.as_str())
                        .unwrap_or("n/a"),
                    row.window_id.0,
                    row.captured_at.to_rfc3339(),
                    control_summary_text(&row.summary),
                )
            })
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table><thead><tr><th>Kind</th><th>Record</th><th>Peer</th><th>Window</th><th>Captured</th><th>Summary</th></tr></thead><tbody>{rows}</tbody></table>"
        )
    };
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><body><main>",
            "<h1>Lifecycle and schedule history</h1>",
            "<p>Backend: <strong>{backend}</strong> | records: <strong>{record_count}</strong></p>",
            "<p><a href=\"{json_page}\">json page</a> | <a href=\"{json_summary}\">json summary</a></p>",
            "{filters}",
            "{rows}",
            "</main></body></html>"
        ),
        backend = summary.backend,
        record_count = summary.record_count,
        json_page = page_path_with_query(
            "/operator/control/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
        ),
        json_summary = page_path_with_query("/operator/control/summary", query, None),
        filters = filter_html,
        rows = rows_html,
    )
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
/// Performs the render artifact run summaries html operation.
pub fn render_artifact_run_summaries_html(
    experiment_id: &ExperimentId,
    summaries: &[crate::ArtifactRunSummary],
) -> String {
    burn_p2p_app::render_artifact_run_summaries_html(
        experiment_id.as_str(),
        &app_artifact_run_summary_rows(summaries),
        "/",
    )
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
/// Performs the render artifact run view html operation.
pub fn render_artifact_run_view_html(view: &crate::ArtifactRunView) -> String {
    burn_p2p_app::render_artifact_run_view_html(&app_artifact_run_view(view))
}

#[cfg(all(feature = "browser-edge", feature = "artifact-publish"))]
/// Performs the render head artifact view html operation.
pub fn render_head_artifact_view_html(view: &crate::HeadArtifactView) -> String {
    burn_p2p_app::render_head_artifact_view_html(&app_head_artifact_view(view))
}

#[cfg(any(not(feature = "browser-edge"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_summaries_html<T>(
    _experiment_id: &ExperimentId,
    _summaries: &[T],
) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "browser-edge"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "browser-edge"), not(feature = "artifact-publish")))]
pub fn render_head_artifact_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}
