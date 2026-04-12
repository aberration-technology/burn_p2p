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
                audit_path: audit_page_path_with_query(
                    "/operator/audit",
                    &crate::OperatorAuditQuery {
                        kind: None,
                        study_id: Some(row.study_id.clone()),
                        experiment_id: Some(row.experiment_id.clone()),
                        revision_id: Some(row.revision_id.clone()),
                        peer_id: row.peer_id.clone(),
                        head_id: None,
                        since: None,
                        until: None,
                        text: None,
                    },
                    None,
                    None,
                ),
                replay_path: replay_page_path_with_query(
                    "/operator/replay",
                    &crate::OperatorReplayQuery {
                        study_id: Some(row.study_id.clone()),
                        experiment_id: Some(row.experiment_id.clone()),
                        revision_id: Some(row.revision_id.clone()),
                        head_id: None,
                        since: None,
                        until: None,
                        text: None,
                    },
                    None,
                ),
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
        clear_filters_path: (!control_query_tags(query).is_empty() || page.offset > 0)
            .then(|| "/operator/control".to_owned()),
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
fn audit_summary_count_rows(summary: &crate::OperatorAuditSummary) -> Vec<String> {
    summary
        .counts_by_kind
        .iter()
        .map(|(kind, count)| format!("{kind}: {count}"))
        .collect()
}

#[cfg(feature = "browser-edge")]
fn facet_bucket_views(
    buckets: &[crate::OperatorFacetBucket],
) -> Vec<burn_p2p_app::AppOperatorFacetBucketView> {
    buckets
        .iter()
        .map(|bucket| burn_p2p_app::AppOperatorFacetBucketView {
            value: bucket.value.clone(),
            count: bucket.count,
        })
        .collect()
}

fn audit_query_pairs(query: &crate::OperatorAuditQuery) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if let Some(kind) = query.kind.as_ref() {
        pairs.push(("kind".into(), kind.as_slug().into()));
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
    if let Some(head_id) = query.head_id.as_ref() {
        pairs.push(("head_id".into(), head_id.as_str().to_owned()));
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

fn audit_query_tags(query: &crate::OperatorAuditQuery) -> Vec<String> {
    audit_query_pairs(query)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

fn audit_page_path_with_query(
    base_path: &str,
    query: &crate::OperatorAuditQuery,
    page: Option<burn_p2p_core::PageRequest>,
    limit_override: Option<usize>,
) -> String {
    let mut pairs = audit_query_pairs(query);
    if let Some(page) = page {
        pairs.push(("offset".into(), page.offset.to_string()));
        pairs.push(("limit".into(), page.limit.to_string()));
    }
    if let Some(limit) = limit_override {
        pairs.push(("limit".into(), limit.to_string()));
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
fn app_operator_audit_view(
    query: &crate::OperatorAuditQuery,
    page: &burn_p2p_core::Page<crate::OperatorAuditRecord>,
    summary: &crate::OperatorAuditSummary,
    facets: &crate::OperatorAuditFacetSummary,
) -> burn_p2p_app::AppOperatorAuditPageView {
    let normalized = burn_p2p_core::PageRequest::new(page.offset, page.limit).normalized();
    let next_offset = normalized.offset + normalized.limit;
    let prev_offset = normalized.offset.saturating_sub(normalized.limit);
    burn_p2p_app::AppOperatorAuditPageView {
        summary: burn_p2p_app::AppOperatorAuditSummaryView {
            backend: summary.backend.clone(),
            record_count: summary.record_count,
            kind_counts: audit_summary_count_rows(summary),
            distinct_study_count: summary.distinct_study_count,
            distinct_experiment_count: summary.distinct_experiment_count,
            distinct_revision_count: summary.distinct_revision_count,
            distinct_peer_count: summary.distinct_peer_count,
            distinct_head_count: summary.distinct_head_count,
            earliest_captured_at: summary
                .earliest_captured_at
                .map(|timestamp| timestamp.to_rfc3339()),
            latest_captured_at: summary
                .latest_captured_at
                .map(|timestamp| timestamp.to_rfc3339()),
        },
        facets: burn_p2p_app::AppOperatorAuditFacetSummaryView {
            limit: facets.limit,
            kinds: facet_bucket_views(&facets.kinds),
            studies: facet_bucket_views(&facets.studies),
            experiments: facet_bucket_views(&facets.experiments),
            revisions: facet_bucket_views(&facets.revisions),
            peers: facet_bucket_views(&facets.peers),
            heads: facet_bucket_views(&facets.heads),
        },
        rows: page
            .items
            .iter()
            .map(|row| burn_p2p_app::AppOperatorAuditRow {
                record_id: row.record_id.clone(),
                kind: row.kind.as_slug().to_owned(),
                study_id: row.study_id.as_ref().map(|value| value.as_str().to_owned()),
                experiment_id: row
                    .experiment_id
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                revision_id: row
                    .revision_id
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                peer_id: row.peer_id.as_ref().map(|value| value.as_str().to_owned()),
                head_id: row.head_id.as_ref().map(|value| value.as_str().to_owned()),
                captured_at: row.captured_at.to_rfc3339(),
                summary: control_summary_text(&row.summary),
                control_path: row.experiment_id.as_ref().map(|experiment_id| {
                    page_path_with_query(
                        "/operator/control",
                        &crate::OperatorControlReplayQuery {
                            kind: None,
                            network_id: None,
                            study_id: row.study_id.clone(),
                            experiment_id: Some(experiment_id.clone()),
                            revision_id: row.revision_id.clone(),
                            peer_id: row.peer_id.clone(),
                            window_id: None,
                            since: None,
                            until: None,
                            text: None,
                        },
                        None,
                    )
                }),
                replay_path: row.experiment_id.as_ref().map(|experiment_id| {
                    replay_page_path_with_query(
                        "/operator/replay",
                        &crate::OperatorReplayQuery {
                            study_id: row.study_id.clone(),
                            experiment_id: Some(experiment_id.clone()),
                            revision_id: row.revision_id.clone(),
                            head_id: row.head_id.clone(),
                            since: None,
                            until: None,
                            text: None,
                        },
                        None,
                    )
                }),
            })
            .collect(),
        filter_tags: audit_query_tags(query),
        offset: page.offset,
        limit: page.limit,
        total: page.total,
        json_page_path: audit_page_path_with_query(
            "/operator/audit/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
            None,
        ),
        json_summary_path: audit_page_path_with_query("/operator/audit/summary", query, None, None),
        json_facets_path: audit_page_path_with_query(
            "/operator/audit/facets",
            query,
            None,
            Some(facets.limit),
        ),
        clear_filters_path: (!audit_query_tags(query).is_empty() || page.offset > 0)
            .then(|| "/operator/audit".to_owned()),
        prev_page_path: (page.offset > 0).then(|| {
            audit_page_path_with_query(
                "/operator/audit",
                query,
                Some(burn_p2p_core::PageRequest::new(prev_offset, page.limit)),
                None,
            )
        }),
        next_page_path: (next_offset < page.total).then(|| {
            audit_page_path_with_query(
                "/operator/audit",
                query,
                Some(burn_p2p_core::PageRequest::new(next_offset, page.limit)),
                None,
            )
        }),
    }
}

fn replay_query_pairs(query: &crate::OperatorReplayQuery) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if let Some(study_id) = query.study_id.as_ref() {
        pairs.push(("study_id".into(), study_id.as_str().to_owned()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        pairs.push(("experiment_id".into(), experiment_id.as_str().to_owned()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        pairs.push(("revision_id".into(), revision_id.as_str().to_owned()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        pairs.push(("head_id".into(), head_id.as_str().to_owned()));
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

fn replay_query_tags(query: &crate::OperatorReplayQuery) -> Vec<String> {
    replay_query_pairs(query)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

fn replay_page_path_with_query(
    base_path: &str,
    query: &crate::OperatorReplayQuery,
    page: Option<burn_p2p_core::PageRequest>,
) -> String {
    let mut pairs = replay_query_pairs(query);
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

fn replay_snapshot_summary_text(summary: &crate::OperatorReplaySnapshotSummary) -> String {
    let mut text = summary
        .study_ids
        .iter()
        .map(burn_p2p_core::StudyId::as_str)
        .chain(
            summary
                .experiment_ids
                .iter()
                .map(burn_p2p_core::ExperimentId::as_str),
        )
        .chain(
            summary
                .revision_ids
                .iter()
                .map(burn_p2p_core::RevisionId::as_str),
        )
        .chain(summary.head_ids.iter().map(burn_p2p_core::HeadId::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    for (key, value) in &summary.summary {
        text.push(' ');
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text.trim().to_owned()
}

fn replay_scope_history_paths(
    study_ids: &[burn_p2p_core::StudyId],
    experiment_ids: &[burn_p2p_core::ExperimentId],
    revision_ids: &[burn_p2p_core::RevisionId],
    head_ids: &[burn_p2p_core::HeadId],
) -> (Option<String>, Option<String>) {
    if study_ids.len() != 1 || experiment_ids.len() != 1 || revision_ids.len() != 1 {
        return (None, None);
    }

    let audit_path = audit_page_path_with_query(
        "/operator/audit",
        &crate::OperatorAuditQuery {
            kind: None,
            study_id: Some(study_ids[0].clone()),
            experiment_id: Some(experiment_ids[0].clone()),
            revision_id: Some(revision_ids[0].clone()),
            peer_id: None,
            head_id: (head_ids.len() == 1).then(|| head_ids[0].clone()),
            since: None,
            until: None,
            text: None,
        },
        None,
        None,
    );
    let control_path = page_path_with_query(
        "/operator/control",
        &crate::OperatorControlReplayQuery {
            kind: None,
            network_id: None,
            study_id: Some(study_ids[0].clone()),
            experiment_id: Some(experiment_ids[0].clone()),
            revision_id: Some(revision_ids[0].clone()),
            peer_id: None,
            window_id: None,
            since: None,
            until: None,
            text: None,
        },
        None,
    );

    (Some(audit_path), Some(control_path))
}

#[cfg(feature = "browser-edge")]
fn app_operator_replay_view(
    query: &crate::OperatorReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorReplaySnapshotSummary>,
) -> burn_p2p_app::AppOperatorReplayPageView {
    let normalized = burn_p2p_core::PageRequest::new(page.offset, page.limit).normalized();
    let next_offset = normalized.offset + normalized.limit;
    let prev_offset = normalized.offset.saturating_sub(normalized.limit);
    burn_p2p_app::AppOperatorReplayPageView {
        rows: page
            .items
            .iter()
            .map(|row| {
                let (audit_scope_path, control_scope_path) = replay_scope_history_paths(
                    &row.study_ids,
                    &row.experiment_ids,
                    &row.revision_ids,
                    &row.head_ids,
                );
                burn_p2p_app::AppOperatorReplaySnapshotRow {
                    captured_at: row.captured_at.to_rfc3339(),
                    study_ids: row
                        .study_ids
                        .iter()
                        .map(|value| value.as_str().to_owned())
                        .collect(),
                    experiment_ids: row
                        .experiment_ids
                        .iter()
                        .map(|value| value.as_str().to_owned())
                        .collect(),
                    revision_ids: row
                        .revision_ids
                        .iter()
                        .map(|value| value.as_str().to_owned())
                        .collect(),
                    head_ids: row
                        .head_ids
                        .iter()
                        .map(|value| value.as_str().to_owned())
                        .collect(),
                    receipt_count: row.receipt_count,
                    head_count: row.head_count,
                    merge_count: row.merge_count,
                    lifecycle_plan_count: row.lifecycle_plan_count,
                    schedule_epoch_count: row.schedule_epoch_count,
                    peer_window_metric_count: row.peer_window_metric_count,
                    reducer_cohort_metric_count: row.reducer_cohort_metric_count,
                    head_eval_report_count: row.head_eval_report_count,
                    summary: replay_snapshot_summary_text(row),
                    snapshot_view_path: format!(
                        "/operator/replay/snapshot/view?captured_at={}",
                        row.captured_at.to_rfc3339()
                    ),
                    json_snapshot_path: format!(
                        "/operator/replay/snapshot?captured_at={}",
                        row.captured_at.to_rfc3339()
                    ),
                    audit_scope_path,
                    control_scope_path,
                }
            })
            .collect(),
        filter_tags: replay_query_tags(query),
        offset: page.offset,
        limit: page.limit,
        total: page.total,
        visible_receipt_count: page.items.iter().map(|row| row.receipt_count).sum(),
        visible_lifecycle_plan_count: page.items.iter().map(|row| row.lifecycle_plan_count).sum(),
        visible_schedule_epoch_count: page.items.iter().map(|row| row.schedule_epoch_count).sum(),
        json_page_path: replay_page_path_with_query(
            "/operator/replay/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
        ),
        clear_filters_path: (!replay_query_tags(query).is_empty() || page.offset > 0)
            .then(|| "/operator/replay".to_owned()),
        prev_page_path: (page.offset > 0).then(|| {
            replay_page_path_with_query(
                "/operator/replay",
                query,
                Some(burn_p2p_core::PageRequest::new(prev_offset, page.limit)),
            )
        }),
        next_page_path: (next_offset < page.total).then(|| {
            replay_page_path_with_query(
                "/operator/replay",
                query,
                Some(burn_p2p_core::PageRequest::new(next_offset, page.limit)),
            )
        }),
    }
}

fn snapshot_scope_ids<T, F>(items: &[T], project: F) -> Vec<String>
where
    F: Fn(&T) -> Option<&str>,
{
    let mut values = std::collections::BTreeSet::new();
    for item in items {
        if let Some(value) = project(item) {
            values.insert(value.to_owned());
        }
    }
    values.into_iter().collect()
}

#[cfg(feature = "browser-edge")]
fn app_operator_replay_snapshot_detail_view(
    captured_at: Option<chrono::DateTime<chrono::Utc>>,
    snapshot: &crate::OperatorReplaySnapshot,
) -> burn_p2p_app::AppOperatorReplaySnapshotDetailView {
    let captured_at_value = captured_at.unwrap_or(snapshot.captured_at);
    let mut study_ids = std::collections::BTreeSet::new();
    let mut experiment_ids = std::collections::BTreeSet::new();
    let mut revision_ids = std::collections::BTreeSet::new();
    for receipt in &snapshot.receipts {
        study_ids.insert(receipt.study_id.as_str().to_owned());
        experiment_ids.insert(receipt.experiment_id.as_str().to_owned());
        revision_ids.insert(receipt.revision_id.as_str().to_owned());
    }
    for head in &snapshot.heads {
        study_ids.insert(head.study_id.as_str().to_owned());
        experiment_ids.insert(head.experiment_id.as_str().to_owned());
        revision_ids.insert(head.revision_id.as_str().to_owned());
    }
    let typed_study_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.study_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.study_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::StudyId::new)
            .collect::<Vec<_>>();
    let typed_experiment_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.experiment_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.experiment_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::ExperimentId::new)
            .collect::<Vec<_>>();
    let typed_revision_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.revision_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.revision_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::RevisionId::new)
            .collect::<Vec<_>>();
    let typed_head_ids = snapshot
        .heads
        .iter()
        .map(|head| head.head_id.clone())
        .collect::<Vec<_>>();
    let (audit_scope_path, control_scope_path) = replay_scope_history_paths(
        &typed_study_ids,
        &typed_experiment_ids,
        &typed_revision_ids,
        &typed_head_ids,
    );
    burn_p2p_app::AppOperatorReplaySnapshotDetailView {
        captured_at: snapshot.captured_at.to_rfc3339(),
        study_ids: study_ids.into_iter().collect(),
        experiment_ids: experiment_ids.into_iter().collect(),
        revision_ids: revision_ids.into_iter().collect(),
        head_ids: snapshot_scope_ids(&snapshot.heads, |item| Some(item.head_id.as_str())),
        receipt_count: snapshot.receipts.len(),
        head_count: snapshot.heads.len(),
        merge_count: snapshot.merges.len(),
        lifecycle_plan_count: snapshot.lifecycle_announcements.len(),
        schedule_epoch_count: snapshot.schedule_announcements.len(),
        peer_window_metric_count: snapshot.peer_window_metrics.len(),
        reducer_cohort_metric_count: snapshot.reducer_cohort_metrics.len(),
        head_eval_report_count: snapshot.head_eval_reports.len(),
        eval_protocol_manifest_count: snapshot.eval_protocol_manifests.len(),
        replay_page_path: "/operator/replay".into(),
        json_snapshot_path: format!(
            "/operator/replay/snapshot?captured_at={}",
            captured_at_value.to_rfc3339()
        ),
        audit_scope_path,
        control_scope_path,
    }
}

#[cfg(feature = "browser-edge")]
fn app_operator_retention_view(
    summary: &crate::OperatorRetentionSummary,
) -> burn_p2p_app::AppOperatorRetentionView {
    burn_p2p_app::AppOperatorRetentionView {
        backend: summary.backend.clone(),
        snapshot_retention_limit: summary.snapshot_retention_limit,
        audit_retention_limit: summary.audit_retention_limit,
        persisted_snapshot_count: summary.persisted_snapshot_count,
        persisted_audit_record_count: summary.persisted_audit_record_count,
        latest_snapshot_at: summary
            .latest_snapshot_at
            .map(|timestamp| timestamp.to_rfc3339()),
        max_peer_window_entries_per_revision: summary
            .metrics_retention
            .max_peer_window_entries_per_revision,
        max_reducer_cohort_entries_per_revision: summary
            .metrics_retention
            .max_reducer_cohort_entries_per_revision,
        max_head_eval_reports_per_revision: summary
            .metrics_retention
            .max_head_eval_reports_per_revision,
        max_metric_revisions_per_experiment: summary
            .metrics_retention
            .max_metric_revisions_per_experiment,
        max_peer_window_detail_windows: summary.metrics_retention.max_peer_window_detail_windows,
        json_summary_path: "/operator/retention".into(),
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

#[cfg(feature = "browser-edge")]
pub fn render_operator_audit_html(
    query: &crate::OperatorAuditQuery,
    page: &burn_p2p_core::Page<crate::OperatorAuditRecord>,
    summary: &crate::OperatorAuditSummary,
    facets: &crate::OperatorAuditFacetSummary,
) -> String {
    burn_p2p_app::render_operator_audit_html(&app_operator_audit_view(query, page, summary, facets))
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_operator_audit_html(
    query: &crate::OperatorAuditQuery,
    page: &burn_p2p_core::Page<crate::OperatorAuditRecord>,
    summary: &crate::OperatorAuditSummary,
    facets: &crate::OperatorAuditFacetSummary,
) -> String {
    let filters = audit_query_tags(query);
    let filter_html = if filters.is_empty() {
        "<p>No filters are active.</p>".to_owned()
    } else {
        format!("<p>{}</p>", filters.join(" | "))
    };
    let reset_html = if !filters.is_empty() || page.offset > 0 {
        " | <a href=\"/operator/audit\">Reset query</a>"
    } else {
        ""
    };
    let facet_html = [
        ("kinds", &facets.kinds),
        ("studies", &facets.studies),
        ("experiments", &facets.experiments),
        ("revisions", &facets.revisions),
        ("peers", &facets.peers),
        ("heads", &facets.heads),
    ]
    .into_iter()
    .map(|(label, buckets)| {
        let values = if buckets.is_empty() {
            "n/a".to_owned()
        } else {
            buckets
                .iter()
                .map(|bucket| format!("{} ({})", bucket.value, bucket.count))
                .collect::<Vec<_>>()
                .join(" | ")
        };
        format!("<p><strong>{label}</strong>: {values}</p>")
    })
    .collect::<Vec<_>>()
    .join("");
    let rows_html = if page.items.is_empty() {
        "<p>No audit rows matched the current query.</p>".to_owned()
    } else {
        let rows = page
            .items
            .iter()
            .map(|row| {
                let control_path = row.experiment_id.as_ref().map(|experiment_id| {
                    page_path_with_query(
                        "/operator/control",
                        &crate::OperatorControlReplayQuery {
                            kind: None,
                            network_id: None,
                            study_id: row.study_id.clone(),
                            experiment_id: Some(experiment_id.clone()),
                            revision_id: row.revision_id.clone(),
                            peer_id: row.peer_id.clone(),
                            window_id: None,
                            since: None,
                            until: None,
                            text: None,
                        },
                        None,
                    )
                });
                let replay_path = row.experiment_id.as_ref().map(|experiment_id| {
                    replay_page_path_with_query(
                        "/operator/replay",
                        &crate::OperatorReplayQuery {
                            study_id: row.study_id.clone(),
                            experiment_id: Some(experiment_id.clone()),
                            revision_id: row.revision_id.clone(),
                            head_id: row.head_id.clone(),
                            since: None,
                            until: None,
                            text: None,
                        },
                        None,
                    )
                });
                let links_html = match (control_path, replay_path) {
                    (Some(control_path), Some(replay_path)) => {
                        format!("<div><a href=\"{control_path}\">control</a> | <a href=\"{replay_path}\">replay</a></div>")
                    }
                    (Some(control_path), None) => {
                        format!("<div><a href=\"{control_path}\">control</a></div>")
                    }
                    (None, Some(replay_path)) => {
                        format!("<div><a href=\"{replay_path}\">replay</a></div>")
                    }
                    (None, None) => String::new(),
                };
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><div>{}</div>{}</td></tr>",
                    row.kind.as_slug(),
                    row.record_id,
                    row.peer_id
                        .as_ref()
                        .map(|peer_id| peer_id.as_str())
                        .unwrap_or("n/a"),
                    row.captured_at.to_rfc3339(),
                    control_summary_text(&row.summary),
                    links_html,
                )
            })
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table><thead><tr><th>Kind</th><th>Record</th><th>Peer</th><th>Captured</th><th>Summary</th></tr></thead><tbody>{rows}</tbody></table>"
        )
    };
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><body><main>",
            "<h1>Operator audit history</h1>",
            "<p>Backend: <strong>{backend}</strong> | records: <strong>{record_count}</strong></p>",
            "<p><a href=\"{json_page}\">json page</a> | <a href=\"{json_summary}\">json summary</a> | <a href=\"{json_facets}\">json facets</a>{reset}</p>",
            "{filters}",
            "{facets}",
            "{rows}",
            "</main></body></html>"
        ),
        backend = summary.backend,
        record_count = summary.record_count,
        json_page = audit_page_path_with_query(
            "/operator/audit/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
            None,
        ),
        json_summary = audit_page_path_with_query("/operator/audit/summary", query, None, None),
        json_facets =
            audit_page_path_with_query("/operator/audit/facets", query, None, Some(facets.limit),),
        reset = reset_html,
        filters = filter_html,
        facets = facet_html,
        rows = rows_html,
    )
}

#[cfg(feature = "browser-edge")]
pub fn render_operator_replay_html(
    query: &crate::OperatorReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorReplaySnapshotSummary>,
) -> String {
    burn_p2p_app::render_operator_replay_html(&app_operator_replay_view(query, page))
}

#[cfg(feature = "browser-edge")]
pub fn render_operator_replay_snapshot_html(
    captured_at: Option<chrono::DateTime<chrono::Utc>>,
    snapshot: &crate::OperatorReplaySnapshot,
) -> String {
    burn_p2p_app::render_operator_replay_snapshot_html(&app_operator_replay_snapshot_detail_view(
        captured_at,
        snapshot,
    ))
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_operator_replay_html(
    query: &crate::OperatorReplayQuery,
    page: &burn_p2p_core::Page<crate::OperatorReplaySnapshotSummary>,
) -> String {
    let filters = replay_query_tags(query);
    let filter_html = if filters.is_empty() {
        "<p>No filters are active.</p>".to_owned()
    } else {
        format!("<p>{}</p>", filters.join(" | "))
    };
    let reset_html = if !filters.is_empty() || page.offset > 0 {
        " | <a href=\"/operator/replay\">Reset query</a>"
    } else {
        ""
    };
    let rows_html = if page.items.is_empty() {
        "<p>No replay snapshots matched the current query.</p>".to_owned()
    } else {
        let rows = page
            .items
            .iter()
            .map(|row| {
                let (audit_scope_path, control_scope_path) = replay_scope_history_paths(
                    &row.study_ids,
                    &row.experiment_ids,
                    &row.revision_ids,
                    &row.head_ids,
                );
                let mut links = vec![
                    format!(
                        "<a href=\"/operator/replay/snapshot/view?captured_at={}\">view</a>",
                        row.captured_at.to_rfc3339()
                    ),
                    format!(
                        "<a href=\"/operator/replay/snapshot?captured_at={}\">json</a>",
                        row.captured_at.to_rfc3339()
                    ),
                ];
                if let Some(audit_scope_path) = audit_scope_path {
                    links.push(format!("<a href=\"{audit_scope_path}\">audit</a>"));
                }
                if let Some(control_scope_path) = control_scope_path {
                    links.push(format!("<a href=\"{control_scope_path}\">control</a>"));
                }
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    row.captured_at.to_rfc3339(),
                    row.receipt_count,
                    replay_snapshot_summary_text(row),
                    links.join(" | "),
                )
            })
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table><thead><tr><th>Captured</th><th>Receipts</th><th>Summary</th><th>Links</th></tr></thead><tbody>{rows}</tbody></table>"
        )
    };
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><body><main>",
            "<h1>Retained replay snapshots</h1>",
            "<p>Snapshots: <strong>{total}</strong></p>",
            "<p><a href=\"{json_page}\">json page</a>{reset}</p>",
            "{filters}",
            "{rows}",
            "</main></body></html>"
        ),
        total = page.total,
        json_page = replay_page_path_with_query(
            "/operator/replay/page",
            query,
            Some(burn_p2p_core::PageRequest::new(page.offset, page.limit)),
        ),
        reset = reset_html,
        filters = filter_html,
        rows = rows_html,
    )
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_operator_replay_snapshot_html(
    captured_at: Option<chrono::DateTime<chrono::Utc>>,
    snapshot: &crate::OperatorReplaySnapshot,
) -> String {
    let captured_at = captured_at.unwrap_or(snapshot.captured_at).to_rfc3339();
    let typed_study_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.study_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.study_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::StudyId::new)
            .collect::<Vec<_>>();
    let typed_experiment_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.experiment_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.experiment_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::ExperimentId::new)
            .collect::<Vec<_>>();
    let typed_revision_ids =
        snapshot_scope_ids(&snapshot.receipts, |item| Some(item.revision_id.as_str()))
            .into_iter()
            .chain(snapshot_scope_ids(&snapshot.heads, |item| {
                Some(item.revision_id.as_str())
            }))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(burn_p2p_core::RevisionId::new)
            .collect::<Vec<_>>();
    let typed_head_ids = snapshot
        .heads
        .iter()
        .map(|head| head.head_id.clone())
        .collect::<Vec<_>>();
    let (audit_scope_path, control_scope_path) = replay_scope_history_paths(
        &typed_study_ids,
        &typed_experiment_ids,
        &typed_revision_ids,
        &typed_head_ids,
    );
    let scope_links = match (audit_scope_path, control_scope_path) {
        (Some(audit_scope_path), Some(control_scope_path)) => {
            format!(
                " | <a href=\"{audit_scope_path}\">Audit scope</a> | <a href=\"{control_scope_path}\">Control scope</a>"
            )
        }
        (Some(audit_scope_path), None) => {
            format!(" | <a href=\"{audit_scope_path}\">Audit scope</a>")
        }
        (None, Some(control_scope_path)) => {
            format!(" | <a href=\"{control_scope_path}\">Control scope</a>")
        }
        (None, None) => String::new(),
    };
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><body><main>",
            "<h1>Retained replay snapshot</h1>",
            "<p>Captured: <strong>{captured_at}</strong></p>",
            "<p>receipts={receipts} heads={heads} merges={merges} lifecycle={lifecycle} schedule={schedule}</p>",
            "<p><a href=\"/operator/replay\">replay history</a> | <a href=\"/operator/replay/snapshot?captured_at={captured_at}\">json snapshot</a>{scope_links}</p>",
            "</main></body></html>"
        ),
        captured_at = captured_at,
        receipts = snapshot.receipts.len(),
        heads = snapshot.heads.len(),
        merges = snapshot.merges.len(),
        lifecycle = snapshot.lifecycle_announcements.len(),
        schedule = snapshot.schedule_announcements.len(),
        scope_links = scope_links,
    )
}

#[cfg(feature = "browser-edge")]
pub fn render_operator_retention_html(summary: &crate::OperatorRetentionSummary) -> String {
    burn_p2p_app::render_operator_retention_html(&app_operator_retention_view(summary))
}

#[cfg(not(feature = "browser-edge"))]
pub fn render_operator_retention_html(summary: &crate::OperatorRetentionSummary) -> String {
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><body><main>",
            "<h1>Retention policy</h1>",
            "<p>Backend: <strong>{backend}</strong></p>",
            "<p>snapshot_limit={snapshot_limit} audit_limit={audit_limit} retained_snapshots={retained_snapshots} retained_audit={retained_audit}</p>",
            "<p><a href=\"/operator/retention\">json summary</a></p>",
            "</main></body></html>"
        ),
        backend = summary.backend,
        snapshot_limit = summary.snapshot_retention_limit,
        audit_limit = summary.audit_retention_limit,
        retained_snapshots = summary.persisted_snapshot_count,
        retained_audit = summary.persisted_audit_record_count,
    )
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
    let reset_html = if !filters.is_empty() || page.offset > 0 {
        " | <a href=\"/operator/control\">Reset query</a>"
    } else {
        ""
    };
    let rows_html = if page.items.is_empty() {
        "<p>No lifecycle or schedule rows matched the current query.</p>".to_owned()
    } else {
        let rows = page
            .items
            .iter()
            .map(|row| {
                let audit_path = audit_page_path_with_query(
                    "/operator/audit",
                    &crate::OperatorAuditQuery {
                        kind: None,
                        study_id: Some(row.study_id.clone()),
                        experiment_id: Some(row.experiment_id.clone()),
                        revision_id: Some(row.revision_id.clone()),
                        peer_id: row.peer_id.clone(),
                        head_id: None,
                        since: None,
                        until: None,
                        text: None,
                    },
                    None,
                    None,
                );
                let replay_path = replay_page_path_with_query(
                    "/operator/replay",
                    &crate::OperatorReplayQuery {
                        study_id: Some(row.study_id.clone()),
                        experiment_id: Some(row.experiment_id.clone()),
                        revision_id: Some(row.revision_id.clone()),
                        head_id: None,
                        since: None,
                        until: None,
                        text: None,
                    },
                    None,
                );
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><div>{}</div><div><a href=\"{audit_path}\">audit</a> | <a href=\"{replay_path}\">replay</a></div></td></tr>",
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
            "<p><a href=\"{json_page}\">json page</a> | <a href=\"{json_summary}\">json summary</a>{reset}</p>",
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
        reset = reset_html,
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
