use crate::models::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalHeadArtifactView, PortalHeadEvalSummaryRow, PortalHeadRow,
    PortalPublishedArtifactRow,
};
use dioxus::prelude::*;
use dioxus::ssr::render_element;

use super::browser_app;

const DASHBOARD_SCRIPT: &str = r#"
const raw = document.getElementById("raw");
const robustnessReasons = document.getElementById("robustness-reasons");
const robustnessAlerts = document.getElementById("robustness-alerts");
const robustnessPeers = document.getElementById("robustness-peers");
const robustnessCanary = document.getElementById("robustness-canary");
const update = (payload) => {
  document.getElementById("connected").textContent = payload.swarm.connected_peers;
  document.getElementById("observed").textContent = payload.swarm.observed_peers.length;
  document.getElementById("admitted").textContent = payload.admitted_peers.length;
  document.getElementById("rejected").textContent = Object.keys(payload.rejected_peers).length;
  document.getElementById("receipts").textContent = payload.accepted_receipts;
  document.getElementById("merges").textContent = payload.certified_merges;
  document.getElementById("services").textContent = payload.services.join(", ");
  document.getElementById("transfers").textContent = payload.in_flight_transfers.length;
  document.getElementById("quarantined").textContent = payload.quarantined_peers.length;
  document.getElementById("banned").textContent = payload.banned_peers.length;
  document.getElementById("revocation").textContent = payload.minimum_revocation_epoch == null ? "n/a" : payload.minimum_revocation_epoch;
  const lower = payload.swarm.network_estimate.eta_lower_seconds;
  const upper = payload.swarm.network_estimate.eta_upper_seconds;
  document.getElementById("eta").textContent = lower == null && upper == null ? "n/a" : `${lower ?? "?"}s - ${upper ?? "?"}s`;
  document.getElementById("error").textContent = payload.last_error ?? "none";
  const panel = payload.robustness_panel;
  const rollup = payload.robustness_rollup;
  const rejectedUpdates = rollup?.rejected_updates ?? (panel == null ? 0 : panel.rejection_reasons.reduce((count, reason) => count + reason.count, 0));
  const meanTrust = rollup?.mean_trust_score != null
    ? rollup.mean_trust_score.toFixed(3)
    : panel == null || panel.trust_scores.length === 0
      ? "n/a"
      : (panel.trust_scores.reduce((sum, point) => sum + point.score, 0) / panel.trust_scores.length).toFixed(3);
  const quarantinedPeers = rollup?.quarantined_peer_count ?? (panel?.quarantined_peers.length ?? 0);
  const banRecommendations = rollup?.ban_recommended_peer_count ?? (panel?.quarantined_peers.filter((peer) => peer.ban_recommended).length ?? 0);
  const canaryRegressions = rollup?.canary_regression_count ?? (panel?.canary_regressions.length ?? 0);
  document.getElementById("robustness-preset").textContent = panel?.preset ?? "n/a";
  document.getElementById("robustness-rejections").textContent = `${rejectedUpdates}`;
  document.getElementById("robustness-quarantined").textContent = `${quarantinedPeers}`;
  document.getElementById("robustness-ban").textContent = `${banRecommendations}`;
  document.getElementById("robustness-canary-count").textContent = `${canaryRegressions}`;
  document.getElementById("robustness-trust").textContent = meanTrust;
  robustnessReasons.textContent = panel == null || panel.rejection_reasons.length === 0
    ? "No rejection reasons recorded yet."
    : panel.rejection_reasons.map((reason) => `${reason.reason}: ${reason.count}`).join("\n");
  robustnessAlerts.textContent = panel == null || panel.alerts.length === 0
    ? "No active robustness alerts."
    : panel.alerts.map((alert) => `${alert.severity}: ${alert.message}`).join("\n");
  robustnessPeers.textContent = panel == null || panel.quarantined_peers.length === 0
    ? "No quarantined peers."
    : panel.quarantined_peers.map((peer) => `${peer.peer_id} | reducer=${peer.reducer_eligible} | validator=${peer.validator_eligible} | ban=${peer.ban_recommended}`).join("\n");
  robustnessCanary.textContent = panel == null || panel.canary_regressions.length === 0
    ? "No canary regressions."
    : panel.canary_regressions.map((report) => `${report.candidate_head_id} | margin=${report.regression_margin.toFixed(4)} | backdoor=${report.detected_backdoor_trigger}`).join("\n");
  raw.textContent = JSON.stringify(payload, null, 2);
};

fetch("/status").then((response) => response.json()).then(update).catch((error) => {
  raw.textContent = `failed to load /status: ${error}`;
});

const events = new EventSource("/events");
events.onmessage = (event) => {
  update(JSON.parse(event.data));
};
events.onerror = () => {
  document.body.classList.add("danger");
};
"#;

pub(super) fn render_dashboard_html(network_id: &str) -> String {
    render_document(
        format!("burn_p2p bootstrap {network_id}"),
        rsx! { DashboardPage { network_id: network_id.to_owned() } },
        Some(DASHBOARD_SCRIPT),
    )
}

pub(super) fn render_artifact_run_summaries_html(
    experiment_id: &str,
    rows: &[PortalArtifactRunSummaryRow],
    portal_path: &str,
) -> String {
    render_document(
        format!("Artifact runs for {experiment_id}"),
        rsx! {
            ArtifactRunSummariesPage {
                experiment_id: experiment_id.to_owned(),
                rows: rows.to_vec(),
                back_href: portal_path.to_owned(),
            }
        },
        None,
    )
}

pub(super) fn render_artifact_run_view_html(view: &PortalArtifactRunView) -> String {
    render_document(
        format!("Artifact run {}", view.run_id),
        rsx! { ArtifactRunPage { view: view.clone() } },
        None,
    )
}

pub(super) fn render_head_artifact_view_html(view: &PortalHeadArtifactView) -> String {
    render_document(
        format!("Artifact head {}", view.head.head_id),
        rsx! { ArtifactHeadPage { view: view.clone() } },
        None,
    )
}

fn render_document(title: String, page: Element, script: Option<&str>) -> String {
    let title = escape_html_text(&title);
    let body = render_element(rsx! {
        {page}
        if let Some(script) = script {
            script { dangerous_inner_html: "{script}" }
        }
    });
    format!(
        concat!(
            "<!doctype html>",
            "<html lang=\"en\">",
            "<head>",
            "<meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "<meta name=\"theme-color\" content=\"#05070c\">",
            "<title>{title}</title>",
            "<style>{style}</style>",
            "</head>",
            "<body>{body}</body>",
            "</html>"
        ),
        title = title,
        style = browser_app::browser_app_stylesheet(),
        body = body,
    )
}

fn escape_html_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[component]
fn DashboardPage(network_id: String) -> Element {
    rsx! {
        main { class: "operator-main",
            section { class: "panel hero-shell hero",
                div { class: "hero-top",
                    div { class: "hero-copy",
                        div { class: "eyebrow", "bootstrap edge" }
                        h1 { "burn_p2p" }
                        p { class: "hero-lead",
                            "Signed snapshots, diagnostics, and artifact history for ",
                            strong { "{network_id}" },
                            "."
                        }
                        div { class: "service-pill-stack",
                            span { class: "pill", "network {network_id}" }
                            span { class: "pill", "live edge" }
                        }
                    }
                    div { class: "hero-meta-grid",
                        MetaCard { label: "events", value: "/events", detail: "live diagnostics" }
                        MetaCard { label: "bundle", value: "/diagnostics/bundle", detail: "support export" }
                        MetaCard { label: "snapshot", value: "/portal/snapshot", detail: "browser edge state" }
                        MetaCard { label: "trust", value: "/trust", detail: "rollout and re-enrollment" }
                    }
                }
                div { class: "summary-grid hero-summary-grid",
                    LiveSummaryCard { label: "connected", value: "0", element_id: Some("connected") }
                    LiveSummaryCard { label: "observed", value: "0", element_id: Some("observed") }
                    LiveSummaryCard { label: "admitted", value: "0", element_id: Some("admitted") }
                    LiveSummaryCard { label: "rejected", value: "0", element_id: Some("rejected") }
                    LiveSummaryCard { label: "receipts", value: "0", element_id: Some("receipts") }
                    LiveSummaryCard { label: "merges", value: "0", element_id: Some("merges") }
                }
                div { class: "operator-link-row",
                    LinkPill { href: "/portal/snapshot", label: "/portal/snapshot" }
                    LinkPill { href: "/directory/signed", label: "/directory/signed" }
                    LinkPill { href: "/leaderboard", label: "/leaderboard" }
                    LinkPill { href: "/heads", label: "/heads" }
                    LinkPill { href: "/receipts", label: "/receipts" }
                    LinkPill { href: "/reducers/load", label: "/reducers/load" }
                }
            }
            section { class: "panel",
                div { class: "eyebrow", "services" }
                h2 { "Live edge state" }
                p { class: "muted section-copy", "Runtime posture, queue health, and trust state from the active bootstrap edge." }
                div { id: "services", class: "status-strip muted", "loading..." }
            }
            section { class: "panel",
                div { class: "eyebrow", "policy" }
                h2 { "Peers and policy" }
                div { class: "info-list",
                    InfoMetric { label: "in-flight transfers", value: "0", element_id: Some("transfers") }
                    InfoMetric { label: "quarantined", value: "0", element_id: Some("quarantined") }
                    InfoMetric { label: "banned", value: "0", element_id: Some("banned") }
                    InfoMetric { label: "min revocation epoch", value: "n/a", element_id: Some("revocation") }
                    InfoMetric { label: "eta", value: "n/a", element_id: Some("eta") }
                    InfoMetric { label: "last error", value: "none", element_id: Some("error") }
                }
            }
            section { class: "panel",
                div { class: "eyebrow", "robustness" }
                h2 { "Robustness" }
                p { class: "muted section-copy", "Current screening posture, quarantine state, and canary outcomes." }
                div { class: "info-list",
                    InfoMetric { label: "preset", value: "n/a", element_id: Some("robustness-preset") }
                    InfoMetric { label: "rejected updates", value: "0", element_id: Some("robustness-rejections") }
                    InfoMetric { label: "quarantined peers", value: "0", element_id: Some("robustness-quarantined") }
                    InfoMetric { label: "ban recommendations", value: "0", element_id: Some("robustness-ban") }
                    InfoMetric { label: "canary regressions", value: "0", element_id: Some("robustness-canary-count") }
                    InfoMetric { label: "mean trust", value: "n/a", element_id: Some("robustness-trust") }
                }
                div { class: "scroll-table",
                    div { class: "operator-raw-block",
                        h3 { "Rejection reasons" }
                        pre { id: "robustness-reasons", class: "operator-raw", "waiting for snapshot..." }
                    }
                    div { class: "operator-raw-block",
                        h3 { "Alerts" }
                        pre { id: "robustness-alerts", class: "operator-raw", "waiting for snapshot..." }
                    }
                    div { class: "operator-raw-block",
                        h3 { "Quarantined peers" }
                        pre { id: "robustness-peers", class: "operator-raw", "waiting for snapshot..." }
                    }
                    div { class: "operator-raw-block",
                        h3 { "Canary regressions" }
                        pre { id: "robustness-canary", class: "operator-raw", "waiting for snapshot..." }
                    }
                }
            }
            section { class: "panel",
                div { class: "eyebrow", "raw" }
                h2 { "Diagnostics" }
                p { class: "muted section-copy", "Live /status payload for quick inspection." }
                pre { id: "raw", class: "operator-raw", "waiting for snapshot..." }
            }
        }
    }
}

#[component]
fn ArtifactRunSummariesPage(
    experiment_id: String,
    rows: Vec<PortalArtifactRunSummaryRow>,
    back_href: String,
) -> Element {
    let run_count = rows.len().to_string();
    rsx! {
        main { class: "operator-main",
            section { class: "panel hero-shell hero",
                div { class: "hero-top",
                    div { class: "hero-copy",
                        div { class: "eyebrow", "artifact history" }
                        h1 { "Artifact runs for {experiment_id}" }
                        p { class: "hero-lead", "Run-scoped alias history and publication summaries." }
                        div { class: "service-pill-stack",
                            span { class: "pill", "experiment {experiment_id}" }
                            span { class: "pill", "{rows.len()} run(s)" }
                        }
                    }
                    div { class: "hero-meta-grid",
                        MetaCard { label: "view", value: "runs", detail: "current aliases and publications" }
                        MetaCard { label: "api", value: "/artifacts/runs", detail: "json detail paths" }
                    }
                }
                div { class: "summary-grid hero-summary-grid",
                    LiveSummaryCard { label: "experiment", value: experiment_id.clone(), element_id: None }
                    LiveSummaryCard { label: "visible runs", value: run_count, element_id: None }
                }
                div { class: "operator-link-row",
                    LinkPill { href: back_href, label: "Back" }
                }
            }
            section { class: "panel",
                div { class: "eyebrow", "runs" }
                h2 { "Runs" }
                if rows.is_empty() {
                    p { class: "muted", "No run-scoped artifact history is currently available for this experiment." }
                } else {
                    div { class: "scroll-table",
                        table {
                            thead {
                                tr {
                                    th { "Run" }
                                    th { "Latest head" }
                                    th { "Aliases" }
                                    th { "Alias history" }
                                    th { "Published" }
                                    th { "API" }
                                }
                            }
                            tbody {
                                for row in rows.iter() {
                                    tr {
                                        td { a { href: row.run_view_path.clone(), code { "{row.run_id}" } } }
                                        td { code { "{row.latest_head_id}" } }
                                        td { "{row.alias_count}" }
                                        td { "{row.alias_history_count}" }
                                        td { "{row.published_artifact_count}" }
                                        td { a { class: "table-link", href: row.json_view_path.clone(), "json" } }
                                    }
                                }
                            }
                        }
                    }
                    div { class: "mobile-card-list",
                        for row in rows {
                            article { class: "mobile-card",
                                div { strong { "{row.run_id}" } }
                                div { class: "mobile-meta", "Latest head: {row.latest_head_id}" }
                                div { class: "mobile-meta", "Aliases: {row.alias_count}" }
                                div { class: "mobile-meta", "Alias history: {row.alias_history_count}" }
                                div { class: "mobile-meta", "Published: {row.published_artifact_count}" }
                                div { class: "operator-link-row", style: "margin-top: 10px;",
                                    a { href: row.run_view_path.clone(), "Open run" }
                                    a { href: row.json_view_path.clone(), "JSON" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ArtifactRunPage(view: PortalArtifactRunView) -> Element {
    let latest_head = view
        .latest_head_id
        .clone()
        .unwrap_or_else(|| "n/a".to_owned());
    rsx! {
        main { class: "operator-main",
            section { class: "panel hero-shell hero",
                div { class: "hero-top",
                    div { class: "hero-copy",
                        div { class: "eyebrow", "artifact run" }
                        h1 { "Artifact run {view.run_id}" }
                        p { class: "hero-lead", "Current aliases, head history, evaluations, and publication state." }
                        div { class: "service-pill-stack",
                            span { class: "pill", "experiment {view.experiment_id}" }
                            span { class: "pill", "run {view.run_id}" }
                        }
                    }
                    div { class: "hero-meta-grid",
                        MetaCard { label: "latest head", value: latest_head.clone(), detail: "current frontier" }
                        MetaCard { label: "json", value: view.json_view_path.clone(), detail: "run payload" }
                    }
                }
                div { class: "summary-grid hero-summary-grid",
                    LiveSummaryCard { label: "heads", value: view.heads.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "aliases", value: view.aliases.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "history", value: view.alias_history.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "publications", value: view.publications.len().to_string(), element_id: None }
                }
                div { class: "operator-link-row",
                    LinkPill { href: view.portal_path.clone(), label: "Back" }
                    LinkPill { href: view.json_view_path.clone(), label: "View JSON" }
                }
            }
            HeadSection { heads: view.heads.clone(), aliases: view.aliases.clone() }
            AliasSection { aliases: view.aliases.clone(), empty: "No current artifact aliases are visible for this run." }
            AliasHistorySection { rows: view.alias_history.clone(), empty: "No historical alias transitions have been recorded for this run." }
            EvalSection { rows: view.eval_reports.clone(), empty: "No evaluation reports are currently attached to this run." }
            PublicationSection { rows: view.publications.clone(), empty: "No published artifacts are currently attached to this run." }
        }
    }
}

#[component]
fn ArtifactHeadPage(view: PortalHeadArtifactView) -> Element {
    rsx! {
        main { class: "operator-main",
            section { class: "panel hero-shell hero",
                div { class: "hero-top",
                    div { class: "hero-copy",
                        div { class: "eyebrow", "artifact head" }
                        h1 { "Artifact head {view.head.head_id}" }
                        p { class: "hero-lead", "Publication state, available profiles, and attached evaluations for this checkpoint." }
                        div { class: "service-pill-stack",
                            span { class: "pill", "experiment {view.experiment_id}" }
                            span { class: "pill", "run {view.run_id}" }
                        }
                    }
                    div { class: "hero-meta-grid",
                        MetaCard { label: "step", value: view.head.global_step.to_string(), detail: view.head.created_at.clone() }
                        MetaCard { label: "json", value: view.json_view_path.clone(), detail: "head payload" }
                    }
                }
                div { class: "summary-grid hero-summary-grid",
                    LiveSummaryCard { label: "aliases", value: view.aliases.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "history", value: view.alias_history.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "evaluations", value: view.eval_reports.len().to_string(), element_id: None }
                    LiveSummaryCard { label: "publications", value: view.publications.len().to_string(), element_id: None }
                }
                div { class: "operator-link-row",
                    LinkPill { href: view.portal_path.clone(), label: "Back" }
                    LinkPill { href: view.run_view_path.clone(), label: "Run history" }
                    LinkPill { href: view.json_view_path.clone(), label: "View JSON" }
                }
            }
            section { class: "panel",
                div { class: "eyebrow", "profiles" }
                h2 { "Available profiles" }
                if view.available_profiles.is_empty() {
                    p { class: "muted", "No publication profiles are currently available." }
                } else {
                    div { class: "service-pill-stack",
                        for profile in view.available_profiles.iter() {
                            span { class: "pill", "{profile}" }
                        }
                    }
                }
            }
            AliasSection { aliases: view.aliases.clone(), empty: "No current aliases resolve to this head." }
            AliasHistorySection { rows: view.alias_history.clone(), empty: "No alias transition history is currently visible for this head." }
            EvalSection { rows: view.eval_reports.clone(), empty: "No evaluation reports are currently attached to this head." }
            PublicationSection { rows: view.publications.clone(), empty: "No published artifacts are currently attached to this head." }
        }
    }
}

#[component]
fn HeadSection(heads: Vec<PortalHeadRow>, aliases: Vec<PortalArtifactRow>) -> Element {
    rsx! {
        section { class: "panel",
            div { class: "eyebrow", "heads" }
            h2 { "Heads" }
            if heads.is_empty() {
                p { class: "muted", "No heads are currently recorded for this run." }
            } else {
                div { class: "scroll-table",
                    table {
                        thead { tr { th { "Head" } th { "Revision" } th { "Step" } th { "Created" } } }
                        tbody {
                            for head in heads.iter() {
                                tr {
                                    td {
                                        a {
                                            href: aliases.iter()
                                                .find(|row| row.head_id == head.head_id)
                                                .map(|row| row.head_view_path.clone())
                                                .unwrap_or_else(|| format!("#head-{}", head.head_id)),
                                            code { "{head.head_id}" }
                                        }
                                    }
                                    td { code { "{head.revision_id}" } }
                                    td { "{head.global_step}" }
                                    td { "{head.created_at}" }
                                }
                            }
                        }
                    }
                }
                div { class: "mobile-card-list",
                    for head in heads {
                        article { class: "mobile-card",
                            div { strong { "{head.head_id}" } }
                            div { class: "mobile-meta", "Revision: {head.revision_id}" }
                            div { class: "mobile-meta", "Step: {head.global_step}" }
                            div { class: "mobile-meta", "{head.created_at}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn AliasSection(aliases: Vec<PortalArtifactRow>, empty: &'static str) -> Element {
    rsx! {
        section { class: "panel",
            div { class: "eyebrow", "aliases" }
            h2 { "Current aliases" }
            if aliases.is_empty() {
                p { class: "muted", "{empty}" }
            } else {
                div { class: "scroll-table",
                    table {
                        thead {
                            tr {
                                th { "Alias" }
                                th { "Scope" }
                                th { "Profile" }
                                th { "Head" }
                                th { "Status" }
                                th { "Published" }
                                th { "Actions" }
                            }
                        }
                        tbody {
                            for row in aliases.iter() {
                                AliasRow { row: row.clone() }
                            }
                        }
                    }
                }
                div { class: "mobile-card-list",
                    for row in aliases {
                        AliasCard { row }
                    }
                }
            }
        }
    }
}

#[component]
fn AliasHistorySection(rows: Vec<PortalArtifactAliasHistoryRow>, empty: &'static str) -> Element {
    rsx! {
        section { class: "panel",
            div { class: "eyebrow", "history" }
            h2 { "Alias transition history" }
            if rows.is_empty() {
                p { class: "muted", "{empty}" }
            } else {
                div { class: "scroll-table",
                    table {
                        thead { tr { th { "Alias" } th { "Scope" } th { "Profile" } th { "Head" } th { "Resolved" } th { "Reason" } } }
                        tbody {
                            for row in rows.iter() {
                                tr {
                                    td { code { "{row.alias_name}" } }
                                    td { "{row.scope}" }
                                    td { "{row.artifact_profile}" }
                                    td { code { "{row.head_id}" } }
                                    td { "{row.resolved_at}" }
                                    td { "{row.source_reason}" }
                                }
                            }
                        }
                    }
                }
                div { class: "mobile-card-list",
                    for row in rows {
                        article { class: "mobile-card",
                            div { strong { "{row.alias_name}" } }
                            div { class: "mobile-meta", "{row.scope} · {row.artifact_profile}" }
                            div { class: "mobile-meta", "Head: {row.head_id}" }
                            div { class: "mobile-meta", "{row.resolved_at}" }
                            div { class: "muted", "{row.source_reason}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn EvalSection(rows: Vec<PortalHeadEvalSummaryRow>, empty: &'static str) -> Element {
    rsx! {
        section { class: "panel",
            div { class: "eyebrow", "evaluation" }
            h2 { "Evaluation reports" }
            if rows.is_empty() {
                p { class: "muted", "{empty}" }
            } else {
                div { class: "scroll-table",
                    table {
                        thead { tr { th { "Head" } th { "Protocol" } th { "Status" } th { "Dataset" } th { "Samples" } th { "Metrics" } th { "Finished" } } }
                        tbody {
                            for row in rows.iter() {
                                tr {
                                    td { code { "{row.head_id}" } }
                                    td { code { "{row.eval_protocol_id}" } }
                                    td { "{row.status}" }
                                    td { code { "{row.dataset_view_id}" } }
                                    td { "{row.sample_count}" }
                                    td { "{row.metric_summary}" }
                                    td { "{row.finished_at}" }
                                }
                            }
                        }
                    }
                }
                div { class: "mobile-card-list",
                    for row in rows {
                        article { class: "mobile-card",
                            div { strong { "{row.head_id}" } }
                            div { class: "mobile-meta", "Protocol: {row.eval_protocol_id}" }
                            div { class: "mobile-meta", "Status: {row.status}" }
                            div { class: "mobile-meta", "Dataset: {row.dataset_view_id}" }
                            div { class: "mobile-meta", "Samples: {row.sample_count}" }
                            div { class: "muted", "{row.metric_summary}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn PublicationSection(rows: Vec<PortalPublishedArtifactRow>, empty: &'static str) -> Element {
    rsx! {
        section { class: "panel",
            div { class: "eyebrow", "publication" }
            h2 { "Published artifacts" }
            if rows.is_empty() {
                p { class: "muted", "{empty}" }
            } else {
                div { class: "scroll-table",
                    table {
                        thead { tr { th { "Head" } th { "Profile" } th { "Target" } th { "Status" } th { "Object key" } th { "Bytes" } th { "Expires / created" } } }
                        tbody {
                            for row in rows.iter() {
                                tr {
                                    td { code { "{row.head_id}" } }
                                    td { "{row.artifact_profile}" }
                                    td { code { "{row.publication_target_id}" } }
                                    td { "{row.status}" }
                                    td { code { "{row.object_key}" } }
                                    td { "{row.content_length}" }
                                    td { "{row.expires_at.clone().unwrap_or_else(|| row.created_at.clone())}" }
                                }
                            }
                        }
                    }
                }
                div { class: "mobile-card-list",
                    for row in rows {
                        article { class: "mobile-card",
                            div { strong { "{row.head_id}" } }
                            div { class: "mobile-meta", "{row.artifact_profile} · {row.status}" }
                            div { class: "mobile-meta", "Target: {row.publication_target_id}" }
                            div { class: "mobile-meta", "{row.object_key}" }
                            div { class: "mobile-meta", "Bytes: {row.content_length}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn AliasRow(row: PortalArtifactRow) -> Element {
    let published = row
        .last_published_at
        .clone()
        .unwrap_or_else(|| "not yet".to_owned());
    let history = alias_history_summary(&row);
    rsx! {
        tr {
            td {
                div { class: "table-cell-stack",
                    code { "{row.alias_name}" }
                    if !history.is_empty() {
                        span { class: "muted", "{history}" }
                    }
                }
            }
            td { "{row.scope}" }
            td { "{row.artifact_profile}" }
            td { a { href: row.head_view_path.clone(), code { "{row.head_id}" } } }
            td { "{row.status}" }
            td { "{published}" }
            td { class: "table-cell-stack",
                button { r#type: "button", class: "action-button", "Download" }
                button { r#type: "button", class: "action-button secondary", "Prepare export" }
                if let Some(run_view_path) = row.run_view_path.clone() {
                    a { class: "table-link", href: run_view_path, "Run history" }
                }
            }
        }
    }
}

#[component]
fn AliasCard(row: PortalArtifactRow) -> Element {
    let published = row
        .last_published_at
        .clone()
        .unwrap_or_else(|| "not yet".to_owned());
    let history = alias_history_summary(&row);
    rsx! {
        article { class: "mobile-card",
            div { strong { "{row.alias_name}" } }
            div { class: "mobile-meta", "{row.scope} · {row.artifact_profile}" }
            div { class: "mobile-meta", "Head: {row.head_id}" }
            div { class: "mobile-meta", "Status: {row.status}" }
            div { class: "mobile-meta", "Published: {published}" }
            if !history.is_empty() {
                div { class: "muted", "{history}" }
            }
            div { class: "operator-link-row", style: "margin-top: 10px;",
                a { href: row.head_view_path.clone(), "Head" }
                if let Some(run_view_path) = row.run_view_path.clone() {
                    a { href: run_view_path, "Run history" }
                }
            }
        }
    }
}

fn alias_history_summary(row: &PortalArtifactRow) -> String {
    if row.history_count <= 1 && row.previous_head_id.is_none() {
        return String::new();
    }
    let mut summary = format!("{} resolution", row.history_count);
    if row.history_count != 1 {
        summary.push('s');
    }
    if let Some(previous_head_id) = row.previous_head_id.as_deref() {
        summary.push_str(" · previous ");
        summary.push_str(previous_head_id);
    }
    summary
}

#[component]
fn MetaCard(label: &'static str, value: String, detail: String) -> Element {
    rsx! {
        div { class: "hero-meta-card",
            span { "{label}" }
            strong { "{value}" }
            p { "{detail}" }
        }
    }
}

#[component]
fn LiveSummaryCard(
    label: &'static str,
    value: String,
    element_id: Option<&'static str>,
) -> Element {
    rsx! {
        div { class: "summary-card",
            div { class: "muted", "{label}" }
            if let Some(element_id) = element_id {
                strong { id: element_id, "{value}" }
            } else {
                strong { "{value}" }
            }
        }
    }
}

#[component]
fn InfoMetric(label: &'static str, value: String, element_id: Option<&'static str>) -> Element {
    rsx! {
        div {
            span { "{label}" }
            if let Some(element_id) = element_id {
                strong { id: element_id, "{value}" }
            } else {
                strong { "{value}" }
            }
        }
    }
}

#[component]
fn LinkPill(href: String, label: &'static str) -> Element {
    rsx! {
        a { class: "pill", href: href, "{label}" }
    }
}
