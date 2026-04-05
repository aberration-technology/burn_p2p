#[cfg(all(feature = "portal", feature = "artifact-publish"))]
use burn_p2p_core::HeadEvalReport;
use burn_p2p_core::{ExperimentId, NetworkId};

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn format_artifact_metric_value(value: &burn_p2p_core::MetricValue) -> String {
    match value {
        burn_p2p_core::MetricValue::Integer(value) => value.to_string(),
        burn_p2p_core::MetricValue::Float(value) => format!("{value:.4}"),
        burn_p2p_core::MetricValue::Bool(value) => value.to_string(),
        burn_p2p_core::MetricValue::Text(value) => value.clone(),
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_alias_history_rows(
    alias_history: &[burn_p2p_core::ArtifactAlias],
) -> Vec<burn_p2p_portal::PortalArtifactAliasHistoryRow> {
    alias_history
        .iter()
        .map(|alias| burn_p2p_portal::PortalArtifactAliasHistoryRow {
            alias_name: alias.alias_name.clone(),
            scope: format!("{:?}", alias.scope),
            artifact_profile: format!("{:?}", alias.artifact_profile),
            head_id: alias.head_id.as_str().to_owned(),
            resolved_at: alias.resolved_at.to_rfc3339(),
            source_reason: format!("{:?}", alias.source_reason),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_published_artifact_rows(
    publications: &[burn_p2p_core::PublishedArtifactRecord],
) -> Vec<burn_p2p_portal::PortalPublishedArtifactRow> {
    publications
        .iter()
        .map(|record| burn_p2p_portal::PortalPublishedArtifactRow {
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

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_eval_summary_rows(
    eval_reports: &[HeadEvalReport],
) -> Vec<burn_p2p_portal::PortalHeadEvalSummaryRow> {
    eval_reports
        .iter()
        .map(|report| burn_p2p_portal::PortalHeadEvalSummaryRow {
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

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_run_summary_rows(
    summaries: &[crate::ArtifactRunSummary],
) -> Vec<burn_p2p_portal::PortalArtifactRunSummaryRow> {
    summaries
        .iter()
        .map(|summary| burn_p2p_portal::PortalArtifactRunSummaryRow {
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

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_run_view(
    view: &crate::ArtifactRunView,
) -> burn_p2p_portal::PortalArtifactRunView {
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
            burn_p2p_portal::PortalArtifactRow {
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

    burn_p2p_portal::PortalArtifactRunView {
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
            .map(|head| burn_p2p_portal::PortalHeadRow {
                experiment_id: head.experiment_id.as_str().to_owned(),
                revision_id: head.revision_id.as_str().to_owned(),
                head_id: head.head_id.as_str().to_owned(),
                global_step: head.global_step,
                created_at: head.created_at.to_rfc3339(),
            })
            .collect(),
        aliases,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        portal_path: "/".into(),
        json_view_path: format!(
            "/artifacts/runs/{}/{}",
            view.experiment_id.as_str(),
            view.run_id.as_str()
        ),
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_head_artifact_view(
    view: &crate::HeadArtifactView,
) -> burn_p2p_portal::PortalHeadArtifactView {
    let aliases = view
        .aliases
        .iter()
        .map(|alias| burn_p2p_portal::PortalArtifactRow {
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

    burn_p2p_portal::PortalHeadArtifactView {
        head: burn_p2p_portal::PortalHeadRow {
            experiment_id: view.head.experiment_id.as_str().to_owned(),
            revision_id: view.head.revision_id.as_str().to_owned(),
            head_id: view.head.head_id.as_str().to_owned(),
            global_step: view.head.global_step,
            created_at: view.head.created_at.to_rfc3339(),
        },
        experiment_id: view.head.experiment_id.as_str().to_owned(),
        run_id: view.run_id.as_str().to_owned(),
        aliases,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        available_profiles: view
            .available_profiles
            .iter()
            .map(|profile| format!("{profile:?}"))
            .collect(),
        portal_path: "/".into(),
        run_view_path: format!(
            "/portal/artifacts/runs/{}/{}",
            view.head.experiment_id.as_str(),
            view.run_id.as_str()
        ),
        json_view_path: format!("/artifacts/heads/{}", view.head.head_id.as_str()),
    }
}

#[cfg(feature = "portal")]
/// Performs the render dashboard html operation.
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    burn_p2p_portal::render_dashboard_html(network_id.as_str())
}

#[cfg(not(feature = "portal"))]
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><body><main><h1>burn_p2p bootstrap</h1><p>Network <strong>{}</strong>. Portal support was not compiled into this build.</p></main></body></html>",
        network_id.as_str()
    )
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render artifact run summaries html operation.
pub fn render_artifact_run_summaries_html(
    experiment_id: &ExperimentId,
    summaries: &[crate::ArtifactRunSummary],
) -> String {
    burn_p2p_portal::render_artifact_run_summaries_html(
        experiment_id.as_str(),
        &portal_artifact_run_summary_rows(summaries),
        "/",
    )
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render artifact run view html operation.
pub fn render_artifact_run_view_html(view: &crate::ArtifactRunView) -> String {
    burn_p2p_portal::render_artifact_run_view_html(&portal_artifact_run_view(view))
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render head artifact view html operation.
pub fn render_head_artifact_view_html(view: &crate::HeadArtifactView) -> String {
    burn_p2p_portal::render_head_artifact_view_html(&portal_head_artifact_view(view))
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_summaries_html<T>(
    _experiment_id: &ExperimentId,
    _summaries: &[T],
) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_head_artifact_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}
