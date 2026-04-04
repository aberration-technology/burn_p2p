#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
use std::collections::BTreeMap;

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
use burn_p2p_core::HeadEvalReport;
use burn_p2p_core::{ExperimentId, NetworkId};
#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
use burn_p2p_core::{HeadId, ReducerCohortMetrics};
#[cfg(feature = "portal")]
use burn_p2p_core::{PrincipalId, ProfileMode, SocialMode};
#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
use burn_p2p_metrics::{
    DerivedMetricKind, MetricsSnapshot, PeerWindowDistributionSummary,
    derive_peer_window_distribution_summaries,
};

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_quality(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let Some(report) = snapshot
        .head_eval_reports
        .iter()
        .filter(|report| report.status == burn_p2p_core::HeadEvalStatus::Completed)
        .max_by_key(|report| report.finished_at)
    else {
        return Vec::new();
    };

    report
        .metric_values
        .iter()
        .map(|(label, value)| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} {}",
                report.experiment_id.as_str(),
                label.replace('_', " ")
            ),
            value: format_metric_value(value),
            scope: "head".into(),
            trust: format!("{:?}", report.trust_class).to_ascii_lowercase(),
            key: report.head_id.as_str().to_owned(),
            protocol: Some(report.eval_protocol_id.as_str().to_owned()),
            operator_hint: None,
            detail_path: None,
            freshness: report.finished_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_derived_kinds(
    snapshots: &[MetricsSnapshot],
    kinds: &[DerivedMetricKind],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut rows = Vec::new();
    for snapshot in snapshots {
        for kind in kinds {
            if let Some(point) = snapshot
                .derived_metrics
                .iter()
                .find(|point| &point.metric == kind)
            {
                rows.push(burn_p2p_portal::PortalMetricRow {
                    label: format!(
                        "{} {}",
                        point.experiment_id.as_str(),
                        derived_metric_label(kind)
                    ),
                    value: format!("{:.4}", point.value),
                    scope: format!("{:?}", point.scope).to_ascii_lowercase(),
                    trust: format!("{:?}", point.trust).to_ascii_lowercase(),
                    key: point
                        .canonical_head_id
                        .as_ref()
                        .or(point.candidate_head_id.as_ref())
                        .or(point.base_head_id.as_ref())
                        .map(|id| id.as_str().to_owned())
                        .unwrap_or_else(|| point.experiment_id.as_str().to_owned()),
                    protocol: None,
                    operator_hint: None,
                    detail_path: None,
                    freshness: point.captured_at.to_rfc3339(),
                });
            }
        }
    }
    rows
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_peer_window_distributions(
    summaries: &[PeerWindowDistributionSummary],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    summaries
        .iter()
        .map(|summary| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} peer windows @ {}",
                summary.experiment_id.as_str(),
                summary.base_head_id.as_str()
            ),
            value: format!(
                "loss p10/p50/p90 {} · compute ms {:.0}/{:.0}/{:.0} · fetch ms {:.0}/{:.0}/{:.0} · publish ms {:.0}/{:.0}/{:.0} · lag {:.0}/{:.0}/{:.0} · accepted {}/{} ({:.2}) · roles {} · backends {}",
                format_optional_triplet(
                    summary.local_train_loss_mean_p10,
                    summary.local_train_loss_mean_p50,
                    summary.local_train_loss_mean_p90,
                ),
                summary.compute_time_ms_p10,
                summary.compute_time_ms_p50,
                summary.compute_time_ms_p90,
                summary.data_fetch_time_ms_p10,
                summary.data_fetch_time_ms_p50,
                summary.data_fetch_time_ms_p90,
                summary.publish_latency_ms_p10,
                summary.publish_latency_ms_p50,
                summary.publish_latency_ms_p90,
                summary.head_lag_at_finish_p10,
                summary.head_lag_at_finish_p50,
                summary.head_lag_at_finish_p90,
                summary.accepted_windows,
                summary.sample_count,
                summary.acceptance_ratio,
                format_breakdown(&summary.role_counts),
                format_breakdown(&summary.backend_counts),
            ),
            scope: "peer".into(),
            trust: "derived".into(),
            key: summary.base_head_id.as_str().to_owned(),
            protocol: None,
            operator_hint: peer_window_distribution_operator_hint(summary),
            detail_path: Some(format!(
                "/metrics/peer-windows/{}/{}/{}",
                summary.experiment_id.as_str(),
                summary.revision_id.as_str(),
                summary.base_head_id.as_str(),
            )),
            freshness: summary.captured_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_health(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut rows = vec![
        burn_p2p_portal::PortalMetricRow {
            label: "Connected peers".into(),
            value: snapshot.diagnostics.swarm.connected_peers.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
        burn_p2p_portal::PortalMetricRow {
            label: "Accepted receipts".into(),
            value: snapshot.diagnostics.accepted_receipts.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
        burn_p2p_portal::PortalMetricRow {
            label: "Certified merges".into(),
            value: snapshot.diagnostics.certified_merges.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
    ];
    rows.extend(portal_metric_rows_for_derived_kinds(
        metrics_snapshots,
        &[
            DerivedMetricKind::ValidationServiceRate,
            DerivedMetricKind::BytesPerAcceptedToken,
        ],
    ));
    rows
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_candidates(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let eval_by_head = snapshot
        .head_eval_reports
        .iter()
        .map(|report| (report.head_id.clone(), report))
        .collect::<BTreeMap<_, _>>();
    let mut cohorts = snapshot.reducer_cohort_metrics.iter().collect::<Vec<_>>();
    cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));

    cohorts
        .into_iter()
        .filter(|metrics| reducer_cohort_is_candidate_view(metrics))
        .map(|metrics| {
            let eval_report = metrics
                .candidate_head_id
                .as_ref()
                .and_then(|head_id| eval_by_head.get(head_id))
                .copied();
            let subject_key = metrics
                .candidate_head_id
                .as_ref()
                .map(HeadId::as_str)
                .unwrap_or_else(|| metrics.merge_window_id.as_str());
            let eval_suffix = eval_report
                .map(|report| format!(" · eval {:?}", report.status).to_ascii_lowercase())
                .unwrap_or_else(|| " · eval pending".into());
            let agreement_suffix = metrics
                .replica_agreement
                .map(|value| format!(" · agreement {:.2}", value))
                .unwrap_or_default();
            let late_suffix = metrics
                .late_arrival_count
                .map(|count| format!(" · late {}", count))
                .unwrap_or_default();
            let missing_suffix = metrics
                .missing_peer_count
                .map(|count| format!(" · missing {}", count))
                .unwrap_or_default();
            burn_p2p_portal::PortalMetricRow {
                label: metrics
                    .candidate_head_id
                    .as_ref()
                    .map(|head_id| {
                        format!(
                            "{} candidate {}",
                            snapshot.manifest.experiment_id.as_str(),
                            head_id.as_str()
                        )
                    })
                    .unwrap_or_else(|| {
                        format!(
                            "{} cohort {}",
                            snapshot.manifest.experiment_id.as_str(),
                            metrics.merge_window_id.as_str()
                        )
                    }),
                value: format!(
                    "status {} · base {} · accepted {}/{} · rejected {} · tokens {} · stale {:.2}/{:.2} · load {:.2}{}{}{}{}",
                    format!("{:?}", metrics.status).to_ascii_lowercase(),
                    metrics.base_head_id.as_str(),
                    metrics.accepted_updates,
                    metrics.received_updates,
                    metrics.rejected_updates,
                    metrics.accepted_tokens_or_samples,
                    metrics.staleness_mean,
                    metrics.staleness_max,
                    metrics.reducer_load,
                    agreement_suffix,
                    late_suffix,
                    missing_suffix,
                    eval_suffix,
                ),
                scope: "cohort".into(),
                trust: format!("{:?}", metrics.trust_class()).to_ascii_lowercase(),
                key: subject_key.to_owned(),
                protocol: eval_report.map(|report| report.eval_protocol_id.as_str().to_owned()),
                operator_hint: Some(candidate_operator_hint(metrics)),
                detail_path: Some(format!(
                    "/metrics/candidates/{}",
                    snapshot.manifest.experiment_id.as_str()
                )),
                freshness: metrics.captured_at.to_rfc3339(),
            }
        })
        .collect()
}

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_candidate_view(metrics: &ReducerCohortMetrics) -> bool {
    metrics.candidate_head_id.is_some()
        || metrics.status != burn_p2p_core::ReducerCohortStatus::Closed
}

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_disagreement(metrics: &ReducerCohortMetrics) -> bool {
    metrics.status == burn_p2p_core::ReducerCohortStatus::Inconsistent
        || metrics
            .replica_agreement
            .map(|agreement| agreement < 0.999)
            .unwrap_or(false)
        || metrics.late_arrival_count.unwrap_or(0) > 0
        || metrics.missing_peer_count.unwrap_or(0) > 0
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn candidate_operator_hint(metrics: &ReducerCohortMetrics) -> String {
    match metrics.status {
        burn_p2p_core::ReducerCohortStatus::Open => {
            "Window still open; watch late arrivals before judging the branch.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Closed => {
            "Closed cohort; wait for validator evaluation or supersession before treating it as final.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Inconsistent => {
            "Hold promotion and inspect reducer disagreement before trusting this candidate.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Promoted => {
            "Candidate already won promotion; compare adoption lag against peers still training on the base head.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Superseded => {
            "Superseded branch; compare against the newer winning candidate before retrying the same merge path.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Rejected => {
            let primary_reason = metrics
                .rejection_reasons
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(reason, count)| format!("{reason} ({count})"))
                .unwrap_or_else(|| "unspecified".into());
            format!("Rejected branch; inspect the dominant rejection reason: {primary_reason}.")
        }
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn disagreement_operator_hint(metrics: &ReducerCohortMetrics) -> String {
    if metrics
        .replica_agreement
        .map(|value| value < 0.999)
        .unwrap_or(false)
    {
        "Reducer replicas diverged; compare reducer inputs before trusting any single aggregate."
            .into()
    } else if metrics.missing_peer_count.unwrap_or(0) > 0 {
        "Expected peers were missing; check reducer reachability and peer liveness before promotion.".into()
    } else if metrics.late_arrival_count.unwrap_or(0) > 0 {
        "Late arrivals exceeded the clean-close window; inspect network lag or increase the close delay.".into()
    } else {
        "Reducer disagreement needs operator review before these numbers are treated as settled truth.".into()
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn peer_window_distribution_operator_hint(
    summary: &PeerWindowDistributionSummary,
) -> Option<String> {
    if summary.sample_count < 3 {
        Some(
            "Small peer-window sample; treat the spread as directional rather than settled.".into(),
        )
    } else if summary.head_lag_at_finish_p90 > 4.0 {
        Some(
            "Peers were finishing far behind the frontier; compare this row against adoption lag and branch disagreement before widening rollout."
                .into(),
        )
    } else if summary.acceptance_ratio < 0.5 {
        Some(
            "Less than half of attempted work was accepted; inspect rejection reasons and reducer disagreement before trusting the apparent loss spread."
                .into(),
        )
    } else {
        None
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_disagreements(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut cohorts = snapshot
        .reducer_cohort_metrics
        .iter()
        .filter(|metrics| reducer_cohort_is_disagreement(metrics))
        .collect::<Vec<_>>();
    cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));

    cohorts
        .into_iter()
        .map(|metrics| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} reducer {}",
                snapshot.manifest.experiment_id.as_str(),
                metrics.reducer_group_id.as_str()
            ),
            value: format!(
                "status {} · candidate {} · agreement {} · late {} · missing {} · rejected {} · bytes {}/{}",
                format!("{:?}", metrics.status).to_ascii_lowercase(),
                metrics
                    .candidate_head_id
                    .as_ref()
                    .map(HeadId::as_str)
                    .unwrap_or("-"),
                metrics
                    .replica_agreement
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "n/a".into()),
                metrics.late_arrival_count.unwrap_or(0),
                metrics.missing_peer_count.unwrap_or(0),
                metrics.rejected_updates,
                metrics.ingress_bytes,
                metrics.egress_bytes,
            ),
            scope: "cohort".into(),
            trust: format!("{:?}", metrics.trust_class()).to_ascii_lowercase(),
            key: metrics.merge_window_id.as_str().to_owned(),
            protocol: None,
            operator_hint: Some(disagreement_operator_hint(metrics)),
            detail_path: Some(format!(
                "/metrics/disagreements/{}",
                snapshot.manifest.experiment_id.as_str()
            )),
            freshness: metrics.captured_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metrics_panels(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> Vec<burn_p2p_portal::PortalMetricsPanel> {
    let peer_window_distributions = derive_peer_window_distribution_summaries(
        &metrics_snapshots
            .iter()
            .flat_map(|snapshot| snapshot.peer_window_metrics.iter().cloned())
            .collect::<Vec<_>>(),
    );
    vec![
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "quality".into(),
            title: "Canonical quality".into(),
            description:
                "Validator-backed head metrics. These are the comparable quality signals for decentralized training."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_quality)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "progress".into(),
            title: "Global progress".into(),
            description:
                "Accepted work and effective progress. These summarize network output rather than pretending there is one global train loss line."
                    .into(),
            rows: portal_metric_rows_for_derived_kinds(
                metrics_snapshots,
                &[
                    DerivedMetricKind::AcceptedTokensPerSec,
                    DerivedMetricKind::AcceptedUpdatesPerHour,
                    DerivedMetricKind::EffectiveEpochProgress,
                    DerivedMetricKind::UniqueSampleCoverageEstimate,
                    DerivedMetricKind::AcceptanceRatio,
                ],
            ),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "dynamics".into(),
            title: "Decentralized dynamics".into(),
            description:
                "Lag, staleness, reducer agreement, and branch signals for the asynchronous training regime."
                    .into(),
            rows: portal_metric_rows_for_derived_kinds(
                metrics_snapshots,
                &[
                    DerivedMetricKind::CanonicalHeadCadence,
                    DerivedMetricKind::StaleWorkFraction,
                    DerivedMetricKind::MeanHeadLag,
                    DerivedMetricKind::MaxHeadLag,
                    DerivedMetricKind::HeadAdoptionLagP50,
                    DerivedMetricKind::HeadAdoptionLagP90,
                    DerivedMetricKind::ReducerReplicaAgreement,
                    DerivedMetricKind::MergeWindowSkew,
                    DerivedMetricKind::CandidateBranchFactor,
                ],
            ),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "peer-windows".into(),
            title: "Peer-window distributions".into(),
            description:
                "Local train-loss, step-time, fetch-latency, publish-latency, and lag spreads grouped by base head instead of being flattened into a fake global train-loss line."
                    .into(),
            rows: portal_metric_rows_for_peer_window_distributions(&peer_window_distributions),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "branches".into(),
            title: "Branch candidates".into(),
            description:
                "Provisional reducer cohorts and candidate heads. These rows keep branch provenance, reducer disagreement, and candidate status visible instead of flattening them into one global number."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_candidates)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "disagreement".into(),
            title: "Reducer disagreement".into(),
            description:
                "Reducer replicas, late arrivals, and missing-peer signals that need operator attention before they get mistaken for settled global truth."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_disagreements)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "health".into(),
            title: "Network health".into(),
            description:
                "Operational state for the current edge, augmented with validation service rate from the metrics plane."
                    .into(),
            rows: portal_metric_rows_for_health(snapshot, metrics_snapshots),
        },
    ]
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn derived_metric_label(kind: &DerivedMetricKind) -> &'static str {
    match kind {
        DerivedMetricKind::AcceptedTokensPerSec => "accepted tokens per sec",
        DerivedMetricKind::AcceptedUpdatesPerHour => "accepted updates per hour",
        DerivedMetricKind::EffectiveEpochProgress => "effective epoch progress",
        DerivedMetricKind::UniqueSampleCoverageEstimate => "unique sample coverage",
        DerivedMetricKind::CanonicalHeadCadence => "canonical head cadence",
        DerivedMetricKind::StaleWorkFraction => "stale work fraction",
        DerivedMetricKind::MeanHeadLag => "mean head lag",
        DerivedMetricKind::MaxHeadLag => "max head lag",
        DerivedMetricKind::ReducerReplicaAgreement => "reducer replica agreement",
        DerivedMetricKind::MergeWindowSkew => "merge window skew",
        DerivedMetricKind::CandidateBranchFactor => "candidate branch factor",
        DerivedMetricKind::HeadAdoptionLagP50 => "head adoption lag p50",
        DerivedMetricKind::HeadAdoptionLagP90 => "head adoption lag p90",
        DerivedMetricKind::AcceptanceRatio => "acceptance ratio",
        DerivedMetricKind::RejectionRatioByReason => "rejection ratio by reason",
        DerivedMetricKind::BytesPerAcceptedToken => "bytes per accepted token",
        DerivedMetricKind::ValidationServiceRate => "validation service rate",
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_optional_triplet(p10: Option<f64>, p50: Option<f64>, p90: Option<f64>) -> String {
    match (p10, p50, p90) {
        (Some(p10), Some(p50), Some(p90)) => format!("{p10:.4}/{p50:.4}/{p90:.4}"),
        _ => "n/a".into(),
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_breakdown(values: &BTreeMap<String, usize>) -> String {
    if values.is_empty() {
        return "n/a".into();
    }
    values
        .iter()
        .map(|(label, count)| format!("{label}:{count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_metric_value(value: &burn_p2p_core::MetricValue) -> String {
    match value {
        burn_p2p_core::MetricValue::Integer(value) => value.to_string(),
        burn_p2p_core::MetricValue::Float(value) => format!("{value:.4}"),
        burn_p2p_core::MetricValue::Bool(value) => value.to_string(),
        burn_p2p_core::MetricValue::Text(value) => value.clone(),
    }
}

#[cfg(feature = "portal")]
fn portal_snapshot_view(
    snapshot: &crate::BrowserPortalSnapshot,
) -> burn_p2p_portal::PortalSnapshotView {
    portal_snapshot_view_with_metrics_and_artifacts(snapshot, Vec::new(), Vec::new())
}

#[cfg(feature = "portal")]
fn portal_snapshot_view_with_metrics(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_panels: Vec<burn_p2p_portal::PortalMetricsPanel>,
) -> burn_p2p_portal::PortalSnapshotView {
    portal_snapshot_view_with_metrics_and_artifacts(snapshot, metrics_panels, Vec::new())
}

#[cfg(feature = "portal")]
fn portal_snapshot_view_with_metrics_and_artifacts(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_panels: Vec<burn_p2p_portal::PortalMetricsPanel>,
    artifact_rows: Vec<burn_p2p_portal::PortalArtifactRow>,
) -> burn_p2p_portal::PortalSnapshotView {
    burn_p2p_portal::PortalSnapshotView {
        network_id: snapshot.network_id.as_str().to_owned(),
        auth_enabled: snapshot.auth_enabled,
        edge_mode: format!("{:?}", snapshot.edge_mode),
        browser_mode: format!("{:?}", snapshot.browser_mode),
        social_enabled: snapshot.social_mode != SocialMode::Disabled,
        profile_enabled: snapshot.profile_mode != ProfileMode::Disabled,
        login_providers: snapshot
            .login_providers
            .iter()
            .map(|provider| burn_p2p_portal::PortalLoginProvider {
                label: provider.label.clone(),
                login_path: provider.login_path.clone(),
                callback_path: provider.callback_path.clone(),
                device_path: provider.device_path.clone(),
            })
            .collect(),
        transports: burn_p2p_portal::PortalTransportSurface {
            webrtc_direct: snapshot.transports.webrtc_direct,
            webtransport_gateway: snapshot.transports.webtransport_gateway,
            wss_fallback: snapshot.transports.wss_fallback,
        },
        paths: burn_p2p_portal::PortalPaths {
            portal_snapshot_path: snapshot.paths.portal_snapshot_path.clone(),
            signed_directory_path: snapshot.paths.signed_directory_path.clone(),
            signed_leaderboard_path: snapshot.paths.signed_leaderboard_path.clone(),
            artifacts_aliases_path: snapshot.paths.artifacts_aliases_path.clone(),
            artifacts_export_path: snapshot.paths.artifacts_export_path.clone(),
            artifacts_download_ticket_path: snapshot.paths.artifacts_download_ticket_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
        },
        diagnostics: burn_p2p_portal::PortalDiagnosticsView {
            connected_peers: snapshot.diagnostics.swarm.connected_peers as usize,
            admitted_peers: snapshot.diagnostics.admitted_peers.len(),
            rejected_peers: snapshot.diagnostics.rejected_peers.len(),
            quarantined_peers: snapshot.diagnostics.quarantined_peers.len(),
            accepted_receipts: snapshot.diagnostics.accepted_receipts,
            certified_merges: snapshot.diagnostics.certified_merges,
            active_services: snapshot
                .diagnostics
                .services
                .iter()
                .map(|service| format!("{service:?}"))
                .collect(),
        },
        trust: burn_p2p_portal::PortalTrustView {
            required_release_train_hash: snapshot
                .required_release_train_hash
                .as_ref()
                .map(|hash| hash.as_str())
                .map(ToOwned::to_owned),
            approved_target_artifact_count: snapshot.allowed_target_artifact_hashes.len(),
            active_issuer_peer_id: snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.active_issuer_peer_id.as_str().to_owned()),
            minimum_revocation_epoch: snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.minimum_revocation_epoch.0),
            reenrollment_required: snapshot
                .trust_bundle
                .as_ref()
                .and_then(|bundle| bundle.reenrollment.as_ref())
                .is_some(),
        },
        experiments: snapshot
            .directory
            .entries
            .iter()
            .map(|entry| burn_p2p_portal::PortalExperimentRow {
                display_name: entry.display_name.clone(),
                experiment_id: entry.experiment_id.as_str().to_owned(),
                revision_id: entry.current_revision_id.as_str().to_owned(),
                has_head: entry.current_head_id.is_some(),
                estimated_window_seconds: entry.resource_requirements.estimated_window_seconds,
            })
            .collect(),
        heads: snapshot
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
        leaderboard: snapshot
            .leaderboard
            .entries
            .iter()
            .map(|entry| burn_p2p_portal::PortalLeaderboardRow {
                principal_label: entry
                    .identity
                    .principal_id
                    .as_ref()
                    .map(PrincipalId::as_str)
                    .unwrap_or("anonymous")
                    .to_owned(),
                leaderboard_score_v1: entry.leaderboard_score_v1,
                accepted_receipt_count: entry.accepted_receipt_count as usize,
            })
            .collect(),
        artifact_rows,
        metrics_panels,
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_rows(
    snapshot: &crate::BrowserPortalSnapshot,
    artifact_aliases: &[crate::ArtifactAliasStatus],
) -> Vec<burn_p2p_portal::PortalArtifactRow> {
    artifact_aliases
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
                export_path: snapshot.paths.artifacts_export_path.clone(),
                download_ticket_path: snapshot.paths.artifacts_download_ticket_path.clone(),
            }
        })
        .collect()
}

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
    let alias_rows = view
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
        aliases: alias_rows,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        portal_path: "/portal".into(),
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
    let artifact_rows = view
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
        aliases: artifact_rows,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        available_profiles: view
            .available_profiles
            .iter()
            .map(|profile| format!("{profile:?}"))
            .collect(),
        portal_path: "/portal".into(),
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

#[cfg(feature = "portal")]
/// Performs the render browser portal html operation.
pub fn render_browser_portal_html(snapshot: &crate::BrowserPortalSnapshot) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view(snapshot))
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
/// Performs the render browser portal html operation with metrics panels.
pub fn render_browser_portal_html_with_metrics(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics(
        snapshot,
        portal_metrics_panels(snapshot, metrics_snapshots),
    ))
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render browser portal html operation with artifact publication rows.
pub fn render_browser_portal_html_with_artifacts(
    snapshot: &crate::BrowserPortalSnapshot,
    artifact_aliases: &[crate::ArtifactAliasStatus],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics_and_artifacts(
        snapshot,
        Vec::new(),
        portal_artifact_rows(snapshot, artifact_aliases),
    ))
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
        "/portal",
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
pub fn render_browser_portal_html_with_artifacts<T>(
    snapshot: &crate::BrowserPortalSnapshot,
    _artifact_aliases: &[T],
) -> String {
    render_browser_portal_html(snapshot)
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

#[cfg(all(
    feature = "portal",
    feature = "metrics-indexer",
    feature = "artifact-publish"
))]
/// Performs the render browser portal html operation with both metrics panels and artifact rows.
pub fn render_browser_portal_html_with_metrics_and_artifacts(
    snapshot: &crate::BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
    artifact_aliases: &[crate::ArtifactAliasStatus],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics_and_artifacts(
        snapshot,
        portal_metrics_panels(snapshot, metrics_snapshots),
        portal_artifact_rows(snapshot, artifact_aliases),
    ))
}

#[cfg(any(
    not(feature = "portal"),
    not(feature = "metrics-indexer"),
    not(feature = "artifact-publish")
))]
pub fn render_browser_portal_html_with_metrics_and_artifacts<M, T>(
    snapshot: &crate::BrowserPortalSnapshot,
    _metrics_snapshots: &[M],
    _artifact_aliases: &[T],
) -> String {
    render_browser_portal_html(snapshot)
}

#[cfg(any(not(feature = "portal"), not(feature = "metrics-indexer")))]
pub fn render_browser_portal_html_with_metrics<T>(
    snapshot: &crate::BrowserPortalSnapshot,
    _metrics_snapshots: &[T],
) -> String {
    render_browser_portal_html(snapshot)
}

#[cfg(not(feature = "portal"))]
pub fn render_browser_portal_html(snapshot: &crate::BrowserPortalSnapshot) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><body><main><h1>burn_p2p browser portal</h1><p>Network <strong>{}</strong>. Portal support was not compiled into this build.</p></main></body></html>",
        snapshot.network_id.as_str()
    )
}
