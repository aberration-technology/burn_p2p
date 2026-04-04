use crate::models::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalHeadArtifactView, PortalHeadEvalSummaryRow, PortalMetricRow,
    PortalMetricsPanel, PortalPublishedArtifactRow, PortalSnapshotView,
};

/// Renders the operator-facing bootstrap dashboard HTML.
pub fn render_dashboard_html(network_id: &str) -> String {
    format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>burn_p2p bootstrap {network_id}</title>
  <style>
    :root {{
      --bg: #f6f5ef;
      --panel: #fffdf7;
      --ink: #1d241f;
      --accent: #0d6b4d;
      --muted: #5f665f;
      --line: #d6d2c4;
      --danger: #8b2e24;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #ede8da, #f8f6ef); color: var(--ink); }}
    main {{ max-width: 1040px; margin: 0 auto; padding: 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 16px; box-shadow: 0 8px 24px rgba(29,36,31,0.06); }}
    .metric {{ font-size: 1.6rem; font-weight: 700; color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    pre {{ white-space: pre-wrap; word-break: break-word; font-size: 0.85rem; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid var(--line); font-size: 0.9rem; }}
    .danger {{ color: var(--danger); }}
  </style>
</head>
<body>
  <main>
    <h1>burn_p2p bootstrap</h1>
    <p class="muted">Network <strong>{network_id}</strong>. Live diagnostics stream over <code>/events</code>; bundle export lives at <code>/diagnostics/bundle</code>; operator history is available from <code>/heads</code>, <code>/receipts</code>, and <code>/reducers/load</code>; browser-edge snapshots are exposed via <code>/portal/snapshot</code>, <code>/directory/signed</code>, and <code>/leaderboard</code>; the reference browser portal lives at <code>/portal</code>; trust rollout and re-enrollment status are exposed via <code>/trust</code> and <code>/reenrollment</code>.</p>
    <section class="grid">
      <article class="panel"><div class="muted">Connected peers</div><div id="connected" class="metric">0</div></article>
      <article class="panel"><div class="muted">Observed peers</div><div id="observed" class="metric">0</div></article>
      <article class="panel"><div class="muted">Admitted peers</div><div id="admitted" class="metric">0</div></article>
      <article class="panel"><div class="muted">Rejected peers</div><div id="rejected" class="metric">0</div></article>
      <article class="panel"><div class="muted">Accepted receipts</div><div id="receipts" class="metric">0</div></article>
      <article class="panel"><div class="muted">Certified merges</div><div id="merges" class="metric">0</div></article>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Services</h2>
      <div id="services" class="muted">loading...</div>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Peers / policy</h2>
      <table>
        <tbody>
          <tr><th>In-flight transfers</th><td id="transfers">0</td></tr>
          <tr><th>Quarantined peers</th><td id="quarantined">0</td></tr>
          <tr><th>Banned peers</th><td id="banned">0</td></tr>
          <tr><th>Min revocation epoch</th><td id="revocation">n/a</td></tr>
          <tr><th>ETA range</th><td id="eta">n/a</td></tr>
          <tr><th>Last error</th><td id="error">none</td></tr>
        </tbody>
      </table>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Raw diagnostics</h2>
      <pre id="raw">waiting for snapshot...</pre>
    </section>
  </main>
  <script>
    const raw = document.getElementById("raw");
    const update = (payload) => {{
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
      document.getElementById("eta").textContent = lower == null && upper == null ? "n/a" : `${{lower ?? "?"}}s - ${{upper ?? "?"}}s`;
      document.getElementById("error").textContent = payload.last_error ?? "none";
      raw.textContent = JSON.stringify(payload, null, 2);
    }};

    fetch("/status").then((response) => response.json()).then(update).catch((error) => {{
      raw.textContent = `failed to load /status: ${{error}}`;
    }});

    const events = new EventSource("/events");
    events.onmessage = (event) => {{
      update(JSON.parse(event.data));
    }};
    events.onerror = () => {{
      document.body.classList.add("danger");
    }};
  </script>
</body>
</html>"##
    )
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn render_operator_shell_html(title: &str, subtitle: &str, body: &str) -> String {
    format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f4efe4;
      --panel: #fffaf0;
      --ink: #1a201c;
      --accent: #135d66;
      --muted: #5d645f;
      --line: #d8d0bd;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #efe7d5, #f8f4ea); color: var(--ink); }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 2.1rem; }}
    h2 {{ margin: 0 0 12px; font-size: 1.15rem; }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 18px; box-shadow: 0 10px 28px rgba(26,32,28,0.06); }}
    .stack {{ display: grid; gap: 16px; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }}
    .muted {{ color: var(--muted); }}
    .metric {{ font-size: 1.5rem; font-weight: 700; color: var(--accent); }}
    .pill {{ display: inline-flex; align-items: center; border-radius: 999px; padding: 4px 10px; background: #e8f0ee; color: var(--accent); font-size: 0.85rem; margin-right: 8px; margin-bottom: 8px; }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); font-size: 0.92rem; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    code {{ font-size: 0.9em; }}
    @media (max-width: 720px) {{
      main {{ padding: 16px; }}
      th, td {{ font-size: 0.88rem; }}
    }}
  </style>
</head>
<body>
  <main class="stack">
    <section class="panel">
      <h1>{title}</h1>
      <p class="muted">{subtitle}</p>
    </section>
    {body}
  </main>
</body>
</html>"##,
        title = escape(title),
        subtitle = escape(subtitle),
        body = body,
    )
}

fn render_table_or_empty(headers: &[&str], rows: &[String], empty: &str) -> String {
    if rows.is_empty() {
        return format!(r#"<p class="muted">{}</p>"#, escape(empty));
    }
    let header_html = headers
        .iter()
        .map(|header| format!("<th>{}</th>", escape(header)))
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"<table><thead><tr>{header_html}</tr></thead><tbody>{rows}</tbody></table>"#,
        header_html = header_html,
        rows = rows.join(""),
    )
}

struct BrowserPortalSections {
    interactive_auth_enabled: bool,
    login_providers: String,
    experiments: String,
    experiment_cards: String,
    heads: String,
    head_cards: String,
    leaderboard: String,
    leaderboard_cards: String,
    transports: String,
}

fn render_login_providers(snapshot: &PortalSnapshotView) -> (bool, String) {
    let interactive_auth_enabled = snapshot.auth_enabled && !snapshot.login_providers.is_empty();
    let login_providers = if interactive_auth_enabled {
        snapshot
            .login_providers
            .iter()
            .map(|provider| {
                format!(
                    "<li><strong>{}</strong> <code>{}</code>{}</li>",
                    escape(&provider.label),
                    escape(&provider.login_path),
                    provider
                        .callback_path
                        .as_ref()
                        .map(|path| format!(
                            " <span class=\"muted\">callback {}</span>",
                            escape(path)
                        ))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("")
    } else {
        String::new()
    };

    (interactive_auth_enabled, login_providers)
}

fn render_experiment_sections(snapshot: &PortalSnapshotView) -> (String, String) {
    let experiments = if snapshot.experiments.is_empty() {
        "<tr data-experiment-row><td colspan=\"5\">No browser-visible experiments.</td></tr>"
            .to_owned()
    } else {
        snapshot
            .experiments
            .iter()
            .map(|entry| {
                format!(
                    "<tr data-experiment-row data-search=\"{} {} {}\"><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                    escape(&entry.display_name),
                    escape(&entry.experiment_id),
                    escape(&entry.revision_id),
                    escape(&entry.display_name),
                    escape(&entry.experiment_id),
                    escape(&entry.revision_id),
                    if entry.has_head { "yes" } else { "no" },
                    entry.estimated_window_seconds
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    let experiment_cards = if snapshot.experiments.is_empty() {
        "<article class=\"mobile-card\"><strong>No browser-visible experiments.</strong></article>"
            .to_owned()
    } else {
        snapshot
            .experiments
            .iter()
            .map(|entry| {
                format!(
                    "<article class=\"mobile-card\" data-experiment-row data-search=\"{} {} {}\"><strong>{}</strong><div class=\"muted\"><code>{}</code> / <code>{}</code></div><div class=\"mobile-meta\">Head: {} · Window: {}s</div></article>",
                    escape(&entry.display_name),
                    escape(&entry.experiment_id),
                    escape(&entry.revision_id),
                    escape(&entry.display_name),
                    escape(&entry.experiment_id),
                    escape(&entry.revision_id),
                    if entry.has_head { "yes" } else { "no" },
                    entry.estimated_window_seconds,
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    (experiments, experiment_cards)
}

fn render_head_sections(snapshot: &PortalSnapshotView) -> (String, String) {
    let heads = if snapshot.heads.is_empty() {
        "<tr data-head-row><td colspan=\"5\">No certified heads are currently exposed by this edge.</td></tr>"
            .to_owned()
    } else {
        snapshot
            .heads
            .iter()
            .map(|head| {
                format!(
                    "<tr data-head-row data-search=\"{} {} {}\"><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                    escape(&head.experiment_id),
                    escape(&head.revision_id),
                    escape(&head.head_id),
                    escape(&head.experiment_id),
                    escape(&head.revision_id),
                    escape(&head.head_id),
                    head.global_step,
                    escape(&head.created_at)
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    let head_cards = if snapshot.heads.is_empty() {
        "<article class=\"mobile-card\"><strong>No certified heads are currently exposed by this edge.</strong></article>"
            .to_owned()
    } else {
        snapshot
            .heads
            .iter()
            .map(|head| {
                format!(
                    "<article class=\"mobile-card\" data-head-row data-search=\"{} {} {}\"><strong><code>{}</code></strong><div class=\"muted\"><code>{}</code> / <code>{}</code></div><div class=\"mobile-meta\">Step {} · {}</div></article>",
                    escape(&head.experiment_id),
                    escape(&head.revision_id),
                    escape(&head.head_id),
                    escape(&head.head_id),
                    escape(&head.experiment_id),
                    escape(&head.revision_id),
                    head.global_step,
                    escape(&head.created_at),
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    (heads, head_cards)
}

fn render_leaderboard_sections(snapshot: &PortalSnapshotView) -> (String, String) {
    let leaderboard = if snapshot.leaderboard.is_empty() {
        "<tr><td colspan=\"4\">No accepted receipts yet.</td></tr>".to_owned()
    } else {
        snapshot
            .leaderboard
            .iter()
            .take(10)
            .enumerate()
            .map(|(index, entry)| {
                format!(
                    "<tr><td>{}</td><td><code>{}</code></td><td>{:.2}</td><td>{}</td></tr>",
                    index + 1,
                    escape(&entry.principal_label),
                    entry.leaderboard_score_v1,
                    entry.accepted_receipt_count
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    let leaderboard_cards = if snapshot.leaderboard.is_empty() {
        "<article class=\"mobile-card\"><strong>No accepted receipts yet.</strong></article>"
            .to_owned()
    } else {
        snapshot
            .leaderboard
            .iter()
            .take(10)
            .enumerate()
            .map(|(index, entry)| {
                format!(
                    "<article class=\"mobile-card\"><strong>#{} <code>{}</code></strong><div class=\"mobile-meta\">Score {:.2} · Accepted receipts {}</div></article>",
                    index + 1,
                    escape(&entry.principal_label),
                    entry.leaderboard_score_v1,
                    entry.accepted_receipt_count,
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    (leaderboard, leaderboard_cards)
}

fn render_transport_list(snapshot: &PortalSnapshotView) -> String {
    [
        ("WebRTC direct", snapshot.transports.webrtc_direct),
        (
            "WebTransport gateway",
            snapshot.transports.webtransport_gateway,
        ),
        ("WSS fallback", snapshot.transports.wss_fallback),
    ]
    .into_iter()
    .map(|(label, enabled)| {
        format!(
            "<li>{}: <strong>{}</strong></li>",
            label,
            if enabled { "enabled" } else { "disabled" }
        )
    })
    .collect::<Vec<_>>()
    .join("")
}

fn build_browser_portal_sections(snapshot: &PortalSnapshotView) -> BrowserPortalSections {
    let (interactive_auth_enabled, login_providers) = render_login_providers(snapshot);
    let (experiments, experiment_cards) = render_experiment_sections(snapshot);
    let (heads, head_cards) = render_head_sections(snapshot);
    let (leaderboard, leaderboard_cards) = render_leaderboard_sections(snapshot);
    let transports = render_transport_list(snapshot);

    BrowserPortalSections {
        interactive_auth_enabled,
        login_providers,
        experiments,
        experiment_cards,
        heads,
        head_cards,
        leaderboard,
        leaderboard_cards,
        transports,
    }
}

fn browser_join_enabled(snapshot: &PortalSnapshotView) -> bool {
    snapshot.browser_mode != "Disabled"
        && (snapshot.transports.webrtc_direct
            || snapshot.transports.webtransport_gateway
            || snapshot.transports.wss_fallback)
        && !snapshot.experiments.is_empty()
}

fn render_login_panel(
    interactive_auth_enabled: bool,
    auth_message: &str,
    login_providers: &str,
) -> String {
    if interactive_auth_enabled {
        format!(
            r#"<article class="panel">
        <h2>Login providers</h2>
        <p class="muted">{auth_message}</p>
        <ul>{login_providers}</ul>
      </article>"#,
            auth_message = escape(auth_message),
            login_providers = login_providers
        )
    } else {
        format!(
            r#"<article class="panel">
        <h2>Enrollment</h2>
        <p class="muted">{auth_message}</p>
      </article>"#,
            auth_message = escape(auth_message)
        )
    }
}

fn render_transport_panel(
    browser_join_enabled: bool,
    browser_message: &str,
    transports: &str,
) -> String {
    format!(
        r#"<article class="panel">
        <h2>{transport_title}</h2>
        <p class="muted">{browser_message}</p>
        {transports_block}
      </article>"#,
        transport_title = if browser_join_enabled {
            "Transport surface"
        } else {
            "Native client path"
        },
        browser_message = escape(browser_message),
        transports_block = if browser_join_enabled {
            format!("<ul>{transports}</ul>")
        } else {
            "<p class=\"muted\">Browser peer join is disabled on this edge. Use the native client path for trainer and validator participation.</p>".into()
        }
    )
}

fn render_leaderboard_path_pill(snapshot: &PortalSnapshotView) -> String {
    if snapshot.social_enabled {
        format!(
            "<div class=\"pill\"><code>{}</code></div>",
            escape(&snapshot.paths.signed_leaderboard_path)
        )
    } else {
        String::new()
    }
}

fn render_leaderboard_row(snapshot: &PortalSnapshotView) -> String {
    if snapshot.social_enabled {
        format!(
            "<tr><th>Leaderboard snapshot</th><td><code>{}</code></td></tr>",
            escape(&snapshot.paths.signed_leaderboard_path)
        )
    } else {
        String::new()
    }
}

fn render_leaderboard_panel(snapshot: &PortalSnapshotView, leaderboard: &str) -> String {
    if snapshot.social_enabled {
        format!(
            r#"<section class="panel" style="margin-top:16px;">
      <h2>Leaderboard</h2>
      <table>
        <thead>
          <tr><th>#</th><th>Principal</th><th>Score</th><th>Accepted receipts</th></tr>
        </thead>
        <tbody>{leaderboard}</tbody>
      </table>
    </section>"#,
            leaderboard = leaderboard
        )
    } else {
        String::new()
    }
}

fn render_leaderboard_mobile_section(
    snapshot: &PortalSnapshotView,
    leaderboard_cards: &str,
) -> String {
    if snapshot.social_enabled {
        format!(
            "<section class=\"mobile-card-list\" style=\"margin-top:16px;\">{leaderboard_cards}</section>",
            leaderboard_cards = leaderboard_cards
        )
    } else {
        String::new()
    }
}

fn render_service_pills(snapshot: &PortalSnapshotView) -> String {
    if snapshot.diagnostics.active_services.is_empty() {
        "<span class=\"pill\">core-runtime</span>".to_owned()
    } else {
        snapshot
            .diagnostics
            .active_services
            .iter()
            .map(|service| format!("<span class=\"pill\">{}</span>", escape(service)))
            .collect::<Vec<_>>()
            .join("")
    }
}

fn render_metrics_row(row: &PortalMetricRow) -> String {
    let operator_hint = row
        .operator_hint
        .as_deref()
        .map(|hint| format!("<div class=\"muted\">Operator hint: {}</div>", escape(hint)))
        .unwrap_or_default();
    let detail_link = row
        .detail_path
        .as_deref()
        .map(|path| format!("<div><a href=\"{}\">View drilldown</a></div>", escape(path)))
        .unwrap_or_default();
    format!(
        "<tr><td>{}</td><td>{}<div style=\"margin-top:6px;\">{}{}</div></td><td><span class=\"pill\">{}</span></td><td><span class=\"pill\">{}</span></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
        escape(&row.label),
        escape(&row.value),
        operator_hint,
        detail_link,
        escape(&row.scope),
        escape(&row.trust),
        escape(&row.key),
        row.protocol
            .as_deref()
            .map(escape)
            .unwrap_or_else(|| "n/a".into()),
        escape(&row.freshness)
    )
}

fn render_metrics_card(row: &PortalMetricRow) -> String {
    let protocol = row
        .protocol
        .as_deref()
        .map(|protocol| format!("protocol {}", escape(protocol)))
        .unwrap_or_else(|| "no protocol".into());
    let operator_hint = row
        .operator_hint
        .as_deref()
        .map(|hint| format!("<div class=\"muted\">Operator hint: {}</div>", escape(hint)))
        .unwrap_or_default();
    let detail_link = row
        .detail_path
        .as_deref()
        .map(|path| {
            format!(
                "<div style=\"margin-top:6px;\"><a href=\"{}\">View drilldown</a></div>",
                escape(path)
            )
        })
        .unwrap_or_default();
    format!(
        "<article class=\"mobile-card\"><strong>{}</strong><div class=\"mobile-meta\">{} · scope {} · trust {} · key <code>{}</code></div><div class=\"metric\" style=\"margin-top:8px;\">{}</div>{}<div class=\"muted\">Freshness {}</div>{}</article>",
        escape(&row.label),
        protocol,
        escape(&row.scope),
        escape(&row.trust),
        escape(&row.key),
        escape(&row.value),
        operator_hint,
        escape(&row.freshness),
        detail_link,
    )
}

fn render_metrics_panel(panel: &PortalMetricsPanel) -> String {
    let rows = if panel.rows.is_empty() {
        "<tr><td colspan=\"7\">No metrics currently available for this panel.</td></tr>".to_owned()
    } else {
        panel
            .rows
            .iter()
            .map(render_metrics_row)
            .collect::<Vec<_>>()
            .join("")
    };
    let cards = if panel.rows.is_empty() {
        "<article class=\"mobile-card\"><strong>No metrics currently available for this panel.</strong></article>".to_owned()
    } else {
        panel
            .rows
            .iter()
            .map(render_metrics_card)
            .collect::<Vec<_>>()
            .join("")
    };
    format!(
        r#"<section id="{panel_id}" class="panel" style="margin-top:16px;">
      <h2>{title}</h2>
      <p class="muted">{description}</p>
      <table>
        <thead>
          <tr><th>Metric</th><th>Value</th><th>Scope</th><th>Trust</th><th>Key</th><th>Protocol</th><th>Freshness</th></tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="mobile-card-list">{cards}</div>
    </section>"#,
        panel_id = escape(&panel.panel_id),
        title = escape(&panel.title),
        description = escape(&panel.description),
        rows = rows,
        cards = cards,
    )
}

fn render_metrics_sections(snapshot: &PortalSnapshotView) -> String {
    if snapshot.metrics_panels.is_empty() {
        return String::new();
    }
    snapshot
        .metrics_panels
        .iter()
        .map(render_metrics_panel)
        .collect::<Vec<_>>()
        .join("")
}

fn render_artifact_sections(snapshot: &PortalSnapshotView) -> String {
    if snapshot.artifact_rows.is_empty() {
        return String::new();
    }

    let rows = snapshot
        .artifact_rows
        .iter()
        .map(render_artifact_row)
        .collect::<Vec<_>>()
        .join("");
    let cards = snapshot
        .artifact_rows
        .iter()
        .map(render_artifact_card)
        .collect::<Vec<_>>()
        .join("");

    format!(
        r#"<section id="artifacts" class="panel" style="margin-top:16px;">
      <h2>Artifacts</h2>
      <p class="muted">Checkpoint publication stays optional and user-triggered. Use these rows to inspect latest/best aliases, prepare exports, and request short-lived download tickets without exposing every checkpoint by default.</p>
      <table>
        <thead>
          <tr><th>Alias</th><th>Scope</th><th>Profile</th><th>Head</th><th>Status</th><th>Published</th><th>Actions</th></tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="mobile-card-list">{cards}</div>
      <div id="artifact-feedback" class="muted" style="margin-top:10px;"></div>
    </section>"#,
        rows = rows,
        cards = cards,
    )
}

fn render_artifact_row(row: &PortalArtifactRow) -> String {
    let published = row
        .last_published_at
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "not yet".into());
    let history = render_artifact_history(row);
    format!(
        r#"<tr>
      <td><code>{alias}</code>{history}</td>
      <td>{scope}</td>
      <td>{profile}</td>
      <td><a href="{head_view_path}"><code>{head_id}</code></a></td>
      <td>{status}</td>
      <td>{published}</td>
      <td>{actions}</td>
    </tr>"#,
        alias = escape(&row.alias_name),
        history = history,
        scope = escape(&row.scope),
        profile = escape(&row.artifact_profile),
        head_view_path = escape(&row.head_view_path),
        head_id = escape(&row.head_id),
        status = escape(&row.status),
        published = published,
        actions = render_artifact_actions(row),
    )
}

fn render_artifact_card(row: &PortalArtifactRow) -> String {
    let published = row
        .last_published_at
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "not yet".into());
    let history = render_artifact_history(row);
    format!(
        r#"<article class="mobile-card">
      <div><strong>{alias}</strong></div>
      <div class="mobile-meta">Scope: {scope}</div>
      <div class="mobile-meta">Profile: {profile}</div>
      <div class="mobile-meta">Head: <a href="{head_view_path}"><code>{head_id}</code></a></div>
      <div class="mobile-meta">Status: {status}</div>
      <div class="mobile-meta">Published: {published}</div>
      {history}
      <div style="margin-top:10px;">{actions}</div>
    </article>"#,
        alias = escape(&row.alias_name),
        scope = escape(&row.scope),
        profile = escape(&row.artifact_profile),
        head_view_path = escape(&row.head_view_path),
        head_id = escape(&row.head_id),
        status = escape(&row.status),
        published = published,
        history = history,
        actions = render_artifact_actions(row),
    )
}

fn render_artifact_history(row: &PortalArtifactRow) -> String {
    if row.history_count <= 1 && row.previous_head_id.is_none() {
        return String::new();
    }
    let mut summary = format!("History: {} resolution", row.history_count);
    if row.history_count != 1 {
        summary.push('s');
    }
    if let Some(previous_head_id) = row.previous_head_id.as_deref() {
        summary.push_str(", previous head ");
        summary.push_str(previous_head_id);
    }
    format!(
        r#"<div class="muted" style="margin-top:4px;">{}</div>"#,
        escape(&summary)
    )
}

fn render_artifact_actions(row: &PortalArtifactRow) -> String {
    let run_id = row.run_id.as_deref().map(escape).unwrap_or_default();
    let alias_id = row
        .artifact_alias_id
        .as_deref()
        .map(escape)
        .unwrap_or_default();
    let history_link = row
        .run_view_path
        .as_deref()
        .map(|path| {
            format!(
                r#"<a class="artifact-history" href="{}">Run history</a>"#,
                escape(path)
            )
        })
        .unwrap_or_default();
    format!(
        r#"<div class="artifact-actions" data-experiment-id="{experiment_id}" data-run-id="{run_id}" data-head-id="{head_id}" data-artifact-profile="{artifact_profile}" data-publication-target-id="{publication_target_id}" data-artifact-alias-id="{artifact_alias_id}" data-export-path="{export_path}" data-download-ticket-path="{download_ticket_path}">
      <button type="button" class="artifact-download">Download</button>
      <button type="button" class="artifact-export">Prepare export</button>
      {history_link}
    </div>"#,
        experiment_id = escape(&row.experiment_id),
        run_id = run_id,
        head_id = escape(&row.head_id),
        artifact_profile = escape(&row.artifact_profile),
        publication_target_id = escape(&row.publication_target_id),
        artifact_alias_id = alias_id,
        export_path = escape(&row.export_path),
        download_ticket_path = escape(&row.download_ticket_path),
        history_link = history_link,
    )
}

fn render_alias_history_rows(rows: &[PortalArtifactAliasHistoryRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                escape(&row.alias_name),
                escape(&row.scope),
                escape(&row.artifact_profile),
                escape(&row.head_id),
                escape(&row.resolved_at),
                escape(&row.source_reason),
            )
        })
        .collect()
}

fn render_publication_rows(rows: &[PortalPublishedArtifactRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td><code>{}</code></td><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                escape(&row.head_id),
                escape(&row.artifact_profile),
                escape(&row.publication_target_id),
                escape(&row.status),
                escape(&row.object_key),
                row.content_length,
                row.expires_at
                    .as_deref()
                    .map(escape)
                    .unwrap_or_else(|| escape(&row.created_at)),
            )
        })
        .collect()
}

fn render_eval_rows(rows: &[PortalHeadEvalSummaryRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            format!(
                "<tr><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape(&row.head_id),
                escape(&row.eval_protocol_id),
                escape(&row.status),
                escape(&row.dataset_view_id),
                row.sample_count,
                escape(&row.metric_summary),
                escape(&row.finished_at),
            )
        })
        .collect()
}

/// Renders a run-summary page for one experiment's artifact history.
pub fn render_artifact_run_summaries_html(
    experiment_id: &str,
    rows: &[PortalArtifactRunSummaryRow],
    portal_path: &str,
) -> String {
    let summary_rows = rows
        .iter()
        .map(|row| {
            format!(
                "<tr><td><a href=\"{}\"><code>{}</code></a></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td><a href=\"{}\">json</a></td></tr>",
                escape(&row.run_view_path),
                escape(&row.run_id),
                escape(&row.latest_head_id),
                row.alias_count,
                row.alias_history_count,
                row.published_artifact_count,
                escape(&row.json_view_path),
            )
        })
        .collect::<Vec<_>>();
    let body = format!(
        r#"<section class="panel">
      <div class="actions">
        <a href="{portal_path}">Back to portal</a>
      </div>
      <div class="grid" style="margin-top:16px;">
        <article><div class="muted">Experiment</div><div class="metric"><code>{experiment_id}</code></div></article>
        <article><div class="muted">Visible runs</div><div class="metric">{run_count}</div></article>
      </div>
    </section>
    <section class="panel">
      <h2>Runs</h2>
      {table}
    </section>"#,
        portal_path = escape(portal_path),
        experiment_id = escape(experiment_id),
        run_count = rows.len(),
        table = render_table_or_empty(
            &[
                "Run",
                "Latest head",
                "Aliases",
                "Alias history",
                "Published artifacts",
                "API",
            ],
            &summary_rows,
            "No run-scoped artifact history is currently available for this experiment.",
        ),
    );
    render_operator_shell_html(
        &format!("Artifact runs for {}", experiment_id),
        "Run-scoped alias history and publication summaries backed by the artifact publication registry.",
        &body,
    )
}

/// Renders one run-scoped artifact history/detail page.
pub fn render_artifact_run_view_html(view: &PortalArtifactRunView) -> String {
    let alias_rows = view
        .aliases
        .iter()
        .map(render_artifact_row)
        .collect::<Vec<_>>();
    let head_rows = view
        .heads
        .iter()
        .map(|head| {
            format!(
                "<tr><td><a href=\"{}\"><code>{}</code></a></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                escape(
                    &view
                        .aliases
                        .iter()
                        .find(|row| row.head_id == head.head_id)
                        .map(|row| row.head_view_path.clone())
                        .unwrap_or_else(|| format!("#head-{}", head.head_id))
                ),
                escape(&head.head_id),
                escape(&head.revision_id),
                head.global_step,
                escape(&head.created_at),
            )
        })
        .collect::<Vec<_>>();
    let body = format!(
        r#"<section class="panel">
      <div class="actions">
        <a href="{portal_path}">Back to portal</a>
        <a href="{json_view_path}">View JSON</a>
      </div>
      <div class="grid" style="margin-top:16px;">
        <article><div class="muted">Experiment</div><div class="metric"><code>{experiment_id}</code></div></article>
        <article><div class="muted">Run</div><div class="metric"><code>{run_id}</code></div></article>
        <article><div class="muted">Latest head</div><div class="metric"><code>{latest_head}</code></div></article>
        <article><div class="muted">Alias history entries</div><div class="metric">{alias_history_count}</div></article>
      </div>
    </section>
    <section class="panel">
      <h2>Heads</h2>
      {heads_table}
    </section>
    <section class="panel">
      <h2>Current aliases</h2>
      {aliases_table}
    </section>
    <section class="panel">
      <h2>Alias transition history</h2>
      {history_table}
    </section>
    <section class="panel">
      <h2>Evaluation reports</h2>
      {eval_table}
    </section>
    <section class="panel">
      <h2>Published artifacts</h2>
      {publication_table}
    </section>"#,
        portal_path = escape(&view.portal_path),
        json_view_path = escape(&view.json_view_path),
        experiment_id = escape(&view.experiment_id),
        run_id = escape(&view.run_id),
        latest_head = view
            .latest_head_id
            .as_deref()
            .map(escape)
            .unwrap_or_else(|| "n/a".into()),
        alias_history_count = view.alias_history.len(),
        heads_table = render_table_or_empty(
            &["Head", "Revision", "Step", "Created"],
            &head_rows,
            "No heads are currently recorded for this run.",
        ),
        aliases_table = render_table_or_empty(
            &[
                "Alias",
                "Scope",
                "Profile",
                "Head",
                "Status",
                "Published",
                "Actions"
            ],
            &alias_rows,
            "No current artifact aliases are visible for this run.",
        ),
        history_table = render_table_or_empty(
            &["Alias", "Scope", "Profile", "Head", "Resolved at", "Reason"],
            &render_alias_history_rows(&view.alias_history),
            "No historical alias transitions have been recorded for this run.",
        ),
        eval_table = render_table_or_empty(
            &[
                "Head",
                "Protocol",
                "Status",
                "Dataset view",
                "Samples",
                "Metrics",
                "Finished"
            ],
            &render_eval_rows(&view.eval_reports),
            "No evaluation reports are currently attached to this run.",
        ),
        publication_table = render_table_or_empty(
            &[
                "Head",
                "Profile",
                "Target",
                "Status",
                "Object key",
                "Bytes",
                "Expires/created"
            ],
            &render_publication_rows(&view.publications),
            "No published artifacts are currently attached to this run.",
        ),
    );
    render_operator_shell_html(
        &format!("Artifact run {}", view.run_id),
        "Dedicated run-scoped artifact history backed by alias resolution history, evaluation reports, and publication records.",
        &body,
    )
}

/// Renders one head-scoped artifact detail page.
pub fn render_head_artifact_view_html(view: &PortalHeadArtifactView) -> String {
    let alias_rows = view
        .aliases
        .iter()
        .map(render_artifact_row)
        .collect::<Vec<_>>();
    let available_profiles = if view.available_profiles.is_empty() {
        "<span class=\"muted\">No publication profiles are currently available.</span>".to_owned()
    } else {
        view.available_profiles
            .iter()
            .map(|profile| format!(r#"<span class="pill">{}</span>"#, escape(profile)))
            .collect::<Vec<_>>()
            .join("")
    };
    let body = format!(
        r#"<section class="panel">
      <div class="actions">
        <a href="{portal_path}">Back to portal</a>
        <a href="{run_view_path}">Run history</a>
        <a href="{json_view_path}">View JSON</a>
      </div>
      <div class="grid" style="margin-top:16px;">
        <article><div class="muted">Head</div><div class="metric"><code>{head_id}</code></div></article>
        <article><div class="muted">Experiment</div><div class="metric"><code>{experiment_id}</code></div></article>
        <article><div class="muted">Run</div><div class="metric"><code>{run_id}</code></div></article>
        <article><div class="muted">Step</div><div class="metric">{global_step}</div></article>
      </div>
      <div style="margin-top:16px;">
        <div class="muted">Available profiles</div>
        <div style="margin-top:8px;">{available_profiles}</div>
      </div>
    </section>
    <section class="panel">
      <h2>Current aliases</h2>
      {aliases_table}
    </section>
    <section class="panel">
      <h2>Alias transition history</h2>
      {history_table}
    </section>
    <section class="panel">
      <h2>Evaluation reports</h2>
      {eval_table}
    </section>
    <section class="panel">
      <h2>Published artifacts</h2>
      {publication_table}
    </section>"#,
        portal_path = escape(&view.portal_path),
        run_view_path = escape(&view.run_view_path),
        json_view_path = escape(&view.json_view_path),
        head_id = escape(&view.head.head_id),
        experiment_id = escape(&view.experiment_id),
        run_id = escape(&view.run_id),
        global_step = view.head.global_step,
        available_profiles = available_profiles,
        aliases_table = render_table_or_empty(
            &[
                "Alias",
                "Scope",
                "Profile",
                "Head",
                "Status",
                "Published",
                "Actions"
            ],
            &alias_rows,
            "No current aliases resolve to this head.",
        ),
        history_table = render_table_or_empty(
            &["Alias", "Scope", "Profile", "Head", "Resolved at", "Reason"],
            &render_alias_history_rows(&view.alias_history),
            "No alias transition history is currently visible for this head.",
        ),
        eval_table = render_table_or_empty(
            &[
                "Head",
                "Protocol",
                "Status",
                "Dataset view",
                "Samples",
                "Metrics",
                "Finished"
            ],
            &render_eval_rows(&view.eval_reports),
            "No evaluation reports are currently attached to this head.",
        ),
        publication_table = render_table_or_empty(
            &[
                "Head",
                "Profile",
                "Target",
                "Status",
                "Object key",
                "Bytes",
                "Expires/created"
            ],
            &render_publication_rows(&view.publications),
            "No published artifacts are currently attached to this head.",
        ),
    );
    render_operator_shell_html(
        &format!("Artifact head {}", view.head.head_id),
        "Dedicated head-scoped artifact detail for publication state, validation evidence, and alias history.",
        &body,
    )
}

/// Renders the browser-facing reference portal HTML from a snapshot view.
pub fn render_browser_portal_html(snapshot: &PortalSnapshotView) -> String {
    let sections = build_browser_portal_sections(snapshot);
    let browser_join_enabled = browser_join_enabled(snapshot);
    let auth_message = if sections.interactive_auth_enabled {
        "Interactive enrollment is available from this edge."
    } else {
        "Join currently requires pre-provisioned credentials or a trusted upstream auth layer."
    };
    let browser_message = if browser_join_enabled {
        "Browser peer join is available for at least one visible revision on this edge."
    } else {
        "Browser peer join is not currently available. Use the native client path or wait for an eligible browser revision."
    };
    let social_message = if !snapshot.social_enabled {
        "Social features are disabled for this deployment."
    } else if snapshot.leaderboard.is_empty() {
        "No public leaderboard data is currently exposed from this edge."
    } else {
        "Leaderboard entries are driven from accepted receipts only."
    };
    let profile_message = if snapshot.profile_enabled {
        "Profile pages are available for this deployment."
    } else {
        "Profile pages are disabled for this deployment."
    };
    let login_panel = render_login_panel(
        sections.interactive_auth_enabled,
        auth_message,
        &sections.login_providers,
    );
    let transport_panel =
        render_transport_panel(browser_join_enabled, browser_message, &sections.transports);
    let leaderboard_path_pill = render_leaderboard_path_pill(snapshot);
    let leaderboard_row = render_leaderboard_row(snapshot);
    let leaderboard_panel = render_leaderboard_panel(snapshot, &sections.leaderboard);
    let leaderboard_mobile_section =
        render_leaderboard_mobile_section(snapshot, &sections.leaderboard_cards);
    let edge_mode_label = format!(
        "{} / {}",
        escape(&snapshot.edge_mode),
        escape(&snapshot.browser_mode)
    );
    let service_pills = render_service_pills(snapshot);
    let artifact_sections = render_artifact_sections(snapshot);
    let metrics_sections = render_metrics_sections(snapshot);
    let trust_release = snapshot
        .trust
        .required_release_train_hash
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "unpublished".into());
    let trust_issuer = snapshot
        .trust
        .active_issuer_peer_id
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "unavailable".into());
    let trust_revocation = snapshot
        .trust
        .minimum_revocation_epoch
        .map(|epoch| epoch.to_string())
        .unwrap_or_else(|| "n/a".into());

    format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>burn_p2p portal {network}</title>
  <style>
    :root {{
      --bg: #f4efe4;
      --panel: #fffaf0;
      --ink: #1a201c;
      --accent: #135d66;
      --accent-strong: #0f4854;
      --muted: #5d645f;
      --line: #d8d0bd;
      --soft: #eef4f2;
      --soft-ink: #234547;
      --hero: linear-gradient(135deg, rgba(19,93,102,0.94), rgba(16,60,68,0.96));
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #efe7d3, #f8f4ea); color: var(--ink); }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ line-height: 1.5; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 12px 28px rgba(26,32,28,0.06); }}
    .hero {{ background: var(--hero); color: #f4fbfc; border: none; }}
    .hero .muted {{ color: rgba(244,251,252,0.78); }}
    .metric {{ font-size: 1.6rem; font-weight: 700; color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    ul {{ margin: 0; padding-left: 18px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid var(--line); font-size: 0.92rem; vertical-align: top; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: var(--soft); color: var(--accent); font-size: 0.8rem; margin-right: 6px; }}
    .hero .pill {{ background: rgba(255,255,255,0.12); color: #f4fbfc; }}
    .section-nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 16px; }}
    .section-nav a {{ text-decoration: none; color: var(--soft-ink); background: var(--soft); padding: 8px 12px; border-radius: 999px; font-size: 0.9rem; }}
    .hero-stats {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); margin-top: 18px; }}
    .hero-stat {{ background: rgba(255,255,255,0.12); border-radius: 14px; padding: 12px; }}
    .hero-stat strong {{ display: block; font-size: 1.5rem; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 16px 0 8px; }}
    .toolbar input {{ flex: 1 1 260px; border: 1px solid var(--line); border-radius: 999px; padding: 10px 14px; font: inherit; background: #fffef9; }}
    .mobile-card-list {{ display: none; }}
    .mobile-card {{ background: #fffdf7; border: 1px solid var(--line); border-radius: 14px; padding: 14px; box-shadow: 0 6px 18px rgba(26,32,28,0.05); }}
    .mobile-meta {{ margin-top: 6px; color: var(--muted); font-size: 0.92rem; }}
    code {{ font-size: 0.9em; }}
    @media (max-width: 720px) {{
      main {{ padding: 16px; }}
      .panel {{ padding: 14px; }}
      .toolbar {{ flex-direction: column; align-items: stretch; }}
      table {{ display: none; }}
      .mobile-card-list {{ display: grid; gap: 10px; }}
      .section-nav a {{ width: fit-content; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="panel hero">
      <h1>burn_p2p browser portal</h1>
      <p class="muted">Network <strong>{network}</strong>. This reference product surface is rendered from the live browser-edge snapshot and now includes join guidance, transport posture, trust state, current heads, and contribution leaderboards without extra tooling.</p>
      <div>{service_pills}</div>
      <div class="hero-stats">
        <div class="hero-stat"><div class="muted">Connected peers</div><strong>{connected_peers}</strong></div>
        <div class="hero-stat"><div class="muted">Accepted receipts</div><strong>{accepted_receipts}</strong></div>
        <div class="hero-stat"><div class="muted">Certified merges</div><strong>{certified_merges}</strong></div>
        <div class="hero-stat"><div class="muted">Visible experiments</div><strong>{experiments_count}</strong></div>
      </div>
      <nav class="section-nav">
        <a href="#join">Join</a>
        <a href="#directory">Directory</a>
        <a href="#heads">Heads</a>
        <a href="#artifacts">Artifacts</a>
        <a href="#trust">Trust</a>
        <a href="#quality">Quality</a>
        <a href="#progress">Progress</a>
        <a href="#dynamics">Dynamics</a>
        <a href="#peer-windows">Peer windows</a>
        <a href="#branches">Branches</a>
        <a href="#disagreement">Disagreement</a>
        <a href="#health">Health</a>
        <a href="#leaderboard">Leaderboard</a>
      </nav>
    </section>
    <section class="grid">
      <article class="panel"><div class="muted">Auth</div><div class="metric">{auth}</div></article>
      <article class="panel"><div class="muted">Visible experiments</div><div class="metric">{experiments_count}</div></article>
      <article class="panel"><div class="muted">Leaderboard entries</div><div class="metric">{leaders_count}</div></article>
      <article class="panel"><div class="muted">Edge mode</div><div class="metric">{edge_mode}</div></article>
    </section>
    <section id="join" class="grid" style="margin-top:16px;">
      {login_panel}
      {transport_panel}
      <article class="panel">
        <h2>Snapshot paths</h2>
        <div class="pill"><code>{portal_path}</code></div>
        <div class="pill"><code>{directory_path}</code></div>
        {leaderboard_path_pill}
        <div class="pill"><code>{trust_path}</code></div>
      </article>
    </section>
    <section class="grid" style="margin-top:16px;">
      <article class="panel">
        <h2>What You Can Do</h2>
        <p class="muted">{auth_message}</p>
        <p class="muted">{browser_message}</p>
        <p class="muted">{social_message}</p>
        <p class="muted">{profile_message}</p>
      </article>
      <article class="panel">
        <h2>Operational posture</h2>
        <table>
          <tbody>
            <tr><th>Connected peers</th><td>{connected_peers}</td></tr>
            <tr><th>Admitted peers</th><td>{admitted_peers}</td></tr>
            <tr><th>Rejected peers</th><td>{rejected_peers}</td></tr>
            <tr><th>Quarantined peers</th><td>{quarantined_peers}</td></tr>
            <tr><th>Portal snapshot</th><td><code>{portal_path}</code></td></tr>
            <tr><th>Directory snapshot</th><td><code>{directory_path}</code></td></tr>
            {leaderboard_row}
            <tr><th>Trust bundle</th><td><code>{trust_path}</code></td></tr>
          </tbody>
        </table>
      </article>
    </section>
    <section id="directory" class="panel" style="margin-top:16px;">
      <div class="toolbar">
        <h2>Experiments</h2>
        <input id="portal-filter" type="search" placeholder="Filter experiments and heads by name, experiment, revision, or head id" />
      </div>
      <p class="muted">The directory reflects browser-visible revisions after auth scope filtering and release-train compatibility checks.</p>
      <table>
        <thead>
          <tr><th>Name</th><th>Experiment</th><th>Revision</th><th>Head</th><th>Window secs</th></tr>
        </thead>
        <tbody>{experiments}</tbody>
      </table>
      <div class="mobile-card-list">{experiment_cards}</div>
    </section>
    <section id="heads" class="panel" style="margin-top:16px;">
      <h2>Current heads</h2>
      <p class="muted">These heads drive browser catch-up, verifier checks, and trainer base selection on this edge.</p>
      <table>
        <thead>
          <tr><th>Experiment</th><th>Revision</th><th>Head</th><th>Step</th><th>Created</th></tr>
        </thead>
        <tbody>{heads}</tbody>
      </table>
      <div class="mobile-card-list">{head_cards}</div>
    </section>
    <section id="trust" class="grid" style="margin-top:16px;">
      <article class="panel">
        <h2>Trust and release</h2>
        <table>
          <tbody>
            <tr><th>Release train</th><td><code>{trust_release}</code></td></tr>
            <tr><th>Approved targets</th><td>{approved_target_artifact_count}</td></tr>
            <tr><th>Active issuer</th><td><code>{trust_issuer}</code></td></tr>
            <tr><th>Minimum revocation epoch</th><td>{trust_revocation}</td></tr>
            <tr><th>Re-enrollment required</th><td>{reenrollment_required}</td></tr>
          </tbody>
        </table>
      </article>
      <article class="panel">
        <h2>Join checklist</h2>
        <ul>
          <li>Confirm your target artifact matches the approved release train.</li>
          <li>Use the login or trusted-enrollment path exposed by this edge.</li>
          <li>Wait for a certified head before attempting verifier or trainer work.</li>
          <li>Receipt-driven leaderboards reflect accepted work only.</li>
        </ul>
      </article>
    </section>
    <section id="leaderboard" class="panel" style="margin-top:16px;">
      <h2>Leaderboard posture</h2>
      <p class="muted">Use the sections above for transport and trust posture. The board below is public only when this edge enables social snapshots.</p>
    </section>
    {artifact_sections}
    {metrics_sections}
    {leaderboard_panel}
    {leaderboard_mobile_section}
  </main>
  <script>
    const filter = document.getElementById("portal-filter");
    if (filter) {{
      filter.addEventListener("input", () => {{
        const query = filter.value.trim().toLowerCase();
        for (const row of document.querySelectorAll("[data-experiment-row], [data-head-row]")) {{
          const haystack = (row.getAttribute("data-search") || "").toLowerCase();
          row.style.display = !query || haystack.includes(query) ? "" : "none";
        }}
      }});
    }}
    const artifactFeedback = document.getElementById("artifact-feedback");
    const artifactPayload = (container) => {{
      const runId = container.dataset.runId;
      const aliasId = container.dataset.artifactAliasId;
      return {{
        experiment_id: container.dataset.experimentId,
        run_id: runId ? runId : null,
        head_id: container.dataset.headId,
        artifact_profile: container.dataset.artifactProfile,
        publication_target_id: container.dataset.publicationTargetId,
        artifact_alias_id: aliasId ? aliasId : null
      }};
    }};
    const artifactAction = async (container, action) => {{
      const endpoint = action === "download" ? container.dataset.downloadTicketPath : container.dataset.exportPath;
      const response = await fetch(endpoint, {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify(artifactPayload(container))
      }});
      if (!response.ok) {{
        throw new Error(await response.text());
      }}
      return response.json();
    }};
    document.querySelectorAll(".artifact-download").forEach((button) => {{
      button.addEventListener("click", async () => {{
        const container = button.closest(".artifact-actions");
        if (!artifactFeedback || !container) {{
          return;
        }}
        artifactFeedback.textContent = "Preparing artifact download...";
        try {{
          const payload = await artifactAction(container, "download");
          artifactFeedback.textContent = `Download ready for ${{container.dataset.headId}}`;
          window.location.href = payload.download_path;
        }} catch (error) {{
          artifactFeedback.textContent = `Download failed: ${{error}}`;
        }}
      }});
    }});
    document.querySelectorAll(".artifact-export").forEach((button) => {{
      button.addEventListener("click", async () => {{
        const container = button.closest(".artifact-actions");
        if (!artifactFeedback || !container) {{
          return;
        }}
        artifactFeedback.textContent = "Queueing artifact export...";
        try {{
          const payload = await artifactAction(container, "export");
          artifactFeedback.textContent = `Export job ${{payload.export_job_id}} is ${{payload.status}}`;
        }} catch (error) {{
          artifactFeedback.textContent = `Export failed: ${{error}}`;
        }}
      }});
    }});
  </script>
</body>
</html>"##,
        network = escape(&snapshot.network_id),
        auth = if snapshot.auth_enabled {
            "enabled"
        } else {
            "disabled"
        },
        experiments_count = snapshot.experiments.len(),
        leaders_count = if snapshot.social_enabled {
            snapshot.leaderboard.len()
        } else {
            0
        },
        connected_peers = snapshot.diagnostics.connected_peers,
        admitted_peers = snapshot.diagnostics.admitted_peers,
        rejected_peers = snapshot.diagnostics.rejected_peers,
        quarantined_peers = snapshot.diagnostics.quarantined_peers,
        accepted_receipts = snapshot.diagnostics.accepted_receipts,
        certified_merges = snapshot.diagnostics.certified_merges,
        edge_mode = edge_mode_label,
        service_pills = service_pills,
        artifact_sections = artifact_sections,
        auth_message = escape(auth_message),
        browser_message = escape(browser_message),
        social_message = escape(social_message),
        profile_message = escape(profile_message),
        login_panel = login_panel,
        transport_panel = transport_panel,
        leaderboard_path_pill = leaderboard_path_pill,
        leaderboard_row = leaderboard_row,
        portal_path = escape(&snapshot.paths.portal_snapshot_path),
        directory_path = escape(&snapshot.paths.signed_directory_path),
        trust_path = escape(&snapshot.paths.trust_bundle_path),
        experiments = sections.experiments,
        experiment_cards = sections.experiment_cards,
        heads = sections.heads,
        head_cards = sections.head_cards,
        trust_release = trust_release,
        approved_target_artifact_count = snapshot.trust.approved_target_artifact_count,
        trust_issuer = trust_issuer,
        trust_revocation = trust_revocation,
        reenrollment_required = if snapshot.trust.reenrollment_required {
            "yes"
        } else {
            "no"
        },
        metrics_sections = metrics_sections,
        leaderboard_panel = leaderboard_panel,
        leaderboard_mobile_section = leaderboard_mobile_section,
    )
}
