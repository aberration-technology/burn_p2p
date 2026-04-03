#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortalTransportSurface {
    pub webrtc_direct: bool,
    pub webtransport_gateway: bool,
    pub wss_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortalLoginProvider {
    pub label: String,
    pub login_path: String,
    pub callback_path: Option<String>,
    pub device_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortalPaths {
    pub portal_snapshot_path: String,
    pub signed_directory_path: String,
    pub signed_leaderboard_path: String,
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortalExperimentRow {
    pub display_name: String,
    pub experiment_id: String,
    pub revision_id: String,
    pub has_head: bool,
    pub estimated_window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortalLeaderboardRow {
    pub principal_label: String,
    pub leaderboard_score_v1: f64,
    pub accepted_receipt_count: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortalSnapshotView {
    pub network_id: String,
    pub auth_enabled: bool,
    pub edge_mode: String,
    pub browser_mode: String,
    pub social_enabled: bool,
    pub profile_enabled: bool,
    pub login_providers: Vec<PortalLoginProvider>,
    pub transports: PortalTransportSurface,
    pub paths: PortalPaths,
    pub experiments: Vec<PortalExperimentRow>,
    pub leaderboard: Vec<PortalLeaderboardRow>,
}

pub fn render_dashboard_html(network_id: &str) -> String {
    format!(
        r#"<!doctype html>
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
</html>"#
    )
}

pub fn render_browser_portal_html(snapshot: &PortalSnapshotView) -> String {
    fn escape(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }

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

    let experiments = if snapshot.experiments.is_empty() {
        "<tr><td colspan=\"5\">No browser-visible experiments.</td></tr>".to_owned()
    } else {
        snapshot
            .experiments
            .iter()
            .map(|entry| {
                format!(
                    "<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
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

    let transports = [
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
    .join("");

    let browser_join_enabled = snapshot.browser_mode != "Disabled"
        && (snapshot.transports.webrtc_direct
            || snapshot.transports.webtransport_gateway
            || snapshot.transports.wss_fallback)
        && !snapshot.experiments.is_empty();
    let auth_message = if interactive_auth_enabled {
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
    let login_panel = if interactive_auth_enabled {
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
    };
    let transport_panel = format!(
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
    );
    let leaderboard_path_pill = if snapshot.social_enabled {
        format!(
            "<div class=\"pill\"><code>{}</code></div>",
            escape(&snapshot.paths.signed_leaderboard_path)
        )
    } else {
        String::new()
    };
    let leaderboard_row = if snapshot.social_enabled {
        format!(
            "<tr><th>Leaderboard snapshot</th><td><code>{}</code></td></tr>",
            escape(&snapshot.paths.signed_leaderboard_path)
        )
    } else {
        String::new()
    };
    let leaderboard_panel = if snapshot.social_enabled {
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
    };
    let edge_mode_label = format!(
        "{} / {}",
        escape(&snapshot.edge_mode),
        escape(&snapshot.browser_mode)
    );

    format!(
        r#"<!doctype html>
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
      --muted: #5d645f;
      --line: #d8d0bd;
      --soft: #eef4f2;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #efe7d3, #f8f4ea); color: var(--ink); }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ line-height: 1.5; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 12px 28px rgba(26,32,28,0.06); }}
    .metric {{ font-size: 1.6rem; font-weight: 700; color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    ul {{ margin: 0; padding-left: 18px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid var(--line); font-size: 0.92rem; vertical-align: top; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: var(--soft); color: var(--accent); font-size: 0.8rem; margin-right: 6px; }}
    code {{ font-size: 0.9em; }}
  </style>
</head>
<body>
  <main>
    <h1>burn_p2p browser portal</h1>
    <p class="muted">Network <strong>{network}</strong>. This page is rendered from the live browser-edge snapshot and mirrors the current directory, auth surface, transport hints, and leaderboard without custom tooling.</p>
    <section class="grid">
      <article class="panel"><div class="muted">Auth</div><div class="metric">{auth}</div></article>
      <article class="panel"><div class="muted">Visible experiments</div><div class="metric">{experiments_count}</div></article>
      <article class="panel"><div class="muted">Leaderboard entries</div><div class="metric">{leaders_count}</div></article>
      <article class="panel"><div class="muted">Edge mode</div><div class="metric">{edge_mode}</div></article>
    </section>
    <section class="grid" style="margin-top:16px;">
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
        <h2>Edge snapshot</h2>
        <table>
          <tbody>
            <tr><th>Portal snapshot</th><td><code>{portal_path}</code></td></tr>
            <tr><th>Directory snapshot</th><td><code>{directory_path}</code></td></tr>
            {leaderboard_row}
            <tr><th>Trust bundle</th><td><code>{trust_path}</code></td></tr>
          </tbody>
        </table>
      </article>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Experiments</h2>
      <table>
        <thead>
          <tr><th>Name</th><th>Experiment</th><th>Revision</th><th>Head</th><th>Window secs</th></tr>
        </thead>
        <tbody>{experiments}</tbody>
      </table>
    </section>
    {leaderboard_panel}
  </main>
</body>
</html>"#,
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
        edge_mode = edge_mode_label,
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
        experiments = experiments,
        leaderboard_panel = leaderboard_panel,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        PortalExperimentRow, PortalLeaderboardRow, PortalLoginProvider, PortalPaths,
        PortalSnapshotView, PortalTransportSurface, render_browser_portal_html,
        render_dashboard_html,
    };

    #[test]
    fn dashboard_html_mentions_bootstrap_routes() {
        let html = render_dashboard_html("mainnet");
        assert!(html.contains("/portal"));
        assert!(html.contains("/diagnostics/bundle"));
    }

    #[test]
    fn browser_portal_html_renders_snapshot_content() {
        let html = render_browser_portal_html(&PortalSnapshotView {
            network_id: "mainnet".into(),
            auth_enabled: true,
            edge_mode: "Trainer".into(),
            browser_mode: "Trainer".into(),
            social_enabled: true,
            profile_enabled: true,
            login_providers: vec![PortalLoginProvider {
                label: "GitHub".into(),
                login_path: "/login/github".into(),
                callback_path: Some("/callback/github".into()),
                device_path: None,
            }],
            transports: PortalTransportSurface {
                webrtc_direct: true,
                webtransport_gateway: false,
                wss_fallback: true,
            },
            paths: PortalPaths {
                portal_snapshot_path: "/portal/snapshot".into(),
                signed_directory_path: "/directory/signed".into(),
                signed_leaderboard_path: "/leaderboard/signed".into(),
                trust_bundle_path: "/trust".into(),
            },
            experiments: vec![PortalExperimentRow {
                display_name: "Auth Demo".into(),
                experiment_id: "exp-auth".into(),
                revision_id: "rev-auth".into(),
                has_head: true,
                estimated_window_seconds: 45,
            }],
            leaderboard: vec![PortalLeaderboardRow {
                principal_label: "alice".into(),
                leaderboard_score_v1: 3.5,
                accepted_receipt_count: 2,
            }],
        });
        assert!(html.contains("burn_p2p browser portal"));
        assert!(html.contains("/login/github"));
        assert!(html.contains("alice"));
    }

    #[test]
    fn browser_portal_html_hides_disabled_auth_and_social_flows() {
        let html = render_browser_portal_html(&PortalSnapshotView {
            network_id: "mainnet".into(),
            auth_enabled: false,
            edge_mode: "Minimal".into(),
            browser_mode: "Disabled".into(),
            social_enabled: false,
            profile_enabled: false,
            login_providers: Vec::new(),
            transports: PortalTransportSurface {
                webrtc_direct: false,
                webtransport_gateway: false,
                wss_fallback: false,
            },
            paths: PortalPaths {
                portal_snapshot_path: "/portal/snapshot".into(),
                signed_directory_path: "/directory/signed".into(),
                signed_leaderboard_path: "/leaderboard/signed".into(),
                trust_bundle_path: "/trust".into(),
            },
            experiments: vec![PortalExperimentRow {
                display_name: "Native Only".into(),
                experiment_id: "exp-native".into(),
                revision_id: "rev-native".into(),
                has_head: false,
                estimated_window_seconds: 60,
            }],
            leaderboard: Vec::new(),
        });
        assert!(html.contains("Join currently requires pre-provisioned credentials"));
        assert!(html.contains("Browser peer join is not currently available"));
        assert!(html.contains("Social features are disabled for this deployment."));
        assert!(!html.contains("No login providers configured."));
        assert!(!html.contains("<h2>Leaderboard</h2>"));
        assert!(!html.contains("/leaderboard/signed"));
    }
}
