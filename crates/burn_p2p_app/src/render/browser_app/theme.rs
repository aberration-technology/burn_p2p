pub(super) const BROWSER_PORTAL_CSS: &str = r#"
:root {
  --bg: #05070c;
  --bg-elevated: #0b1119;
  --panel: rgba(15, 19, 28, 0.9);
  --panel-strong: rgba(20, 25, 36, 0.96);
  --panel-soft: rgba(22, 28, 40, 0.78);
  --ink: #f7f3eb;
  --muted: #b0a89b;
  --line: rgba(255, 255, 255, 0.08);
  --line-strong: rgba(255, 184, 92, 0.2);
  --accent: #ffb85c;
  --accent-strong: #ff8c42;
  --accent-soft: rgba(255, 184, 92, 0.14);
  --accent-cool: #8eb9ff;
  --success: #6fd3a4;
  --warning: #ffd166;
  --danger: #ff7f7f;
  --shadow: 0 28px 80px rgba(0, 0, 0, 0.34);
  color-scheme: dark;
  font-family: "Sora", "IBM Plex Sans", "Avenir Next", sans-serif;
}

* { box-sizing: border-box; }

html { background: var(--bg); }

body {
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at top center, rgba(255, 184, 92, 0.12), transparent 28%),
    radial-gradient(circle at 85% 10%, rgba(142, 185, 255, 0.12), transparent 24%),
    linear-gradient(180deg, #06080d, #090d14 35%, #05070c 100%);
}

a {
  color: var(--accent);
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}

main {
  max-width: 1180px;
  margin: 0 auto;
  padding: 24px 24px 52px;
}

.browser-app-shell {
  display: grid;
  gap: 18px;
}

h1,
h2 {
  margin: 0;
  letter-spacing: -0.03em;
}

h3 {
  margin: 0;
  font-size: 1.02rem;
  letter-spacing: -0.02em;
}

h1 {
  font-size: clamp(2.6rem, 4vw, 4.7rem);
  line-height: 0.98;
}

h2 {
  font-size: clamp(1.25rem, 2vw, 1.8rem);
  line-height: 1.06;
}

p {
  line-height: 1.65;
  margin: 0;
}

code {
  font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
  font-size: 0.9em;
}

.app-main {
  display: grid;
  gap: 22px;
  margin-top: 22px;
  min-width: 0;
}

.app-header {
  display: grid;
  gap: 20px;
  grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
  align-items: start;
}

.app-header-copy {
  display: grid;
  gap: 12px;
}

.app-title {
  font-size: clamp(2.05rem, 3vw, 3.25rem);
  line-height: 1.02;
}

.app-subtitle,
.toolbar-detail,
.section-detail {
  color: #e0d7ca;
  font-size: 0.98rem;
  line-height: 1.6;
}

.badge-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.status-pill {
  display: inline-flex;
  align-items: center;
  min-height: 34px;
  padding: 7px 12px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
  color: var(--ink);
  font-size: 0.84rem;
}

.status-pill-accent {
  border-color: rgba(255, 184, 92, 0.3);
  background: rgba(255, 184, 92, 0.12);
  color: var(--accent);
}

.status-pill-neutral {
  color: var(--muted);
}

.app-summary-grid {
  align-self: stretch;
}

.app-summary-card {
  min-height: 118px;
}

.app-toolbar {
  display: grid;
  gap: 16px;
  align-items: center;
  grid-template-columns: minmax(0, 1fr) auto;
}

.toolbar-copy {
  display: grid;
  gap: 6px;
}

.toolbar-meta {
  display: grid;
  justify-items: end;
  gap: 4px;
}

.toolbar-meta-label {
  color: var(--muted);
  font-size: 0.76rem;
  letter-spacing: 0.04em;
}

.surface-layout {
  display: grid;
  gap: 18px;
  grid-template-columns: minmax(0, 1.28fr) minmax(280px, 0.72fr);
  align-items: start;
}

.support-stack {
  display: grid;
  gap: 18px;
}

.primary-panel {
  display: grid;
  gap: 18px;
}

.section-header {
  display: grid;
  gap: 6px;
}

.primary-value-block {
  display: grid;
  gap: 10px;
  padding: 4px 0 2px;
}

.primary-value {
  font-size: clamp(1.55rem, 2.6vw, 2.45rem);
  line-height: 1.08;
  letter-spacing: -0.04em;
  color: var(--ink);
}

.stat-grid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
}

.stat-tile {
  padding: 14px 15px 13px;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.025);
}

.stat-tile span {
  display: block;
  margin-bottom: 8px;
  color: var(--muted);
  font-size: 0.76rem;
  letter-spacing: 0.03em;
}

.stat-tile strong {
  display: block;
  font-size: 1.35rem;
  letter-spacing: -0.03em;
}

.stat-detail {
  margin-top: 8px;
  font-size: 0.88rem;
}

.subsection {
  display: grid;
  gap: 12px;
}

.experiment-hero,
.experiment-card,
.leaderboard-row,
.empty-state {
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.025);
}

.experiment-hero {
  display: grid;
  gap: 14px;
  grid-template-columns: minmax(0, 1fr) minmax(220px, 0.72fr);
  padding: 20px;
  background:
    radial-gradient(circle at top right, rgba(255, 184, 92, 0.09), transparent 24%),
    rgba(255, 255, 255, 0.025);
}

.experiment-hero-copy,
.experiment-hero-meta {
  display: grid;
  gap: 10px;
}

.experiment-list,
.leaderboard-list {
  display: grid;
  gap: 12px;
}

.experiment-card {
  display: grid;
  gap: 10px;
  padding: 16px;
}

.experiment-title-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  justify-content: space-between;
}

.subtle-pill {
  background: rgba(255, 255, 255, 0.04);
  border-color: var(--line);
  color: var(--muted);
}

.experiment-meta {
  font-size: 0.92rem;
}

.experiment-kv,
.keyvalue-list {
  display: grid;
  gap: 10px;
}

.keyvalue-row {
  display: flex;
  gap: 14px;
  align-items: baseline;
  justify-content: space-between;
  padding-top: 8px;
  border-top: 1px solid rgba(255, 255, 255, 0.05);
}

.keyvalue-row:first-child {
  padding-top: 0;
  border-top: 0;
}

.keyvalue-row span {
  color: var(--muted);
  font-size: 0.84rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.keyvalue-row strong {
  text-align: right;
  font-size: 0.96rem;
}

.leaderboard-row {
  display: flex;
  gap: 12px;
  align-items: center;
  justify-content: space-between;
  padding: 12px 14px;
}

.leaderboard-row.is-local {
  border-color: rgba(255, 184, 92, 0.32);
}

.leaderboard-meta {
  display: grid;
  gap: 2px;
  justify-items: end;
  color: var(--muted);
  font-size: 0.9rem;
}

.empty-state {
  display: grid;
  gap: 8px;
  padding: 16px;
}

.panel {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.01), rgba(255, 255, 255, 0)),
    var(--panel);
  border: 1px solid var(--line);
  border-radius: 24px;
  padding: 22px;
  box-shadow: 0 20px 54px rgba(0, 0, 0, 0.28);
  backdrop-filter: blur(14px);
}

.hero {
  background:
    radial-gradient(circle at top left, rgba(255, 184, 92, 0.16), transparent 26%),
    radial-gradient(circle at 88% 0%, rgba(142, 185, 255, 0.14), transparent 24%),
    linear-gradient(135deg, rgba(18, 22, 31, 0.98), rgba(12, 16, 24, 0.98));
  border-color: var(--line-strong);
}

.hero-shell {
  display: grid;
  gap: 22px;
  overflow: hidden;
}

.hero-top {
  display: grid;
  gap: 24px;
  grid-template-columns: minmax(0, 1.25fr) minmax(320px, 0.9fr);
  align-items: start;
}

.hero-copy {
  display: grid;
  gap: 14px;
}

.hero-lead {
  max-width: 64ch;
  color: #ece5d8;
  font-size: 1.03rem;
}

.hero-meta-grid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.hero-meta-card,
.summary-card,
.compact-panel,
.runtime-card,
.mobile-card,
.path-grid div,
.info-list div {
  background: var(--panel-soft);
  border: 1px solid var(--line);
  border-radius: 20px;
}

.hero-meta-card {
  padding: 15px 16px;
}

.hero-meta-card span,
.info-list span,
.path-grid span {
  display: block;
  margin-bottom: 6px;
  color: var(--muted);
  font-size: 0.78rem;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.hero-meta-card strong {
  display: block;
  font-size: 1.15rem;
  color: var(--ink);
}

.hero-meta-card p {
  margin-top: 8px;
  color: var(--muted);
  font-size: 0.92rem;
}

.hero-summary-grid,
.summary-grid,
.grid {
  display: grid;
  gap: 14px;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}

.summary-card {
  padding: 16px 18px;
}

.summary-card strong {
  display: block;
  margin-top: 10px;
  font-size: 1.65rem;
  letter-spacing: -0.03em;
}

.hero-bottom {
  display: grid;
  gap: 18px;
  grid-template-columns: minmax(0, 1fr) minmax(280px, 0.45fr);
  align-items: end;
}

.surface-nav {
  display: grid;
  gap: 10px;
}

.surface-nav-inline {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.surface-tab {
  width: 100%;
  text-align: left;
  padding: 14px 16px;
  border-radius: 18px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.02);
  color: var(--ink);
  cursor: pointer;
  transition:
    border-color 180ms ease,
    transform 180ms ease,
    background 180ms ease,
    box-shadow 180ms ease;
}

.surface-tab:hover {
  transform: translateY(-1px);
  border-color: rgba(255, 184, 92, 0.3);
  background: rgba(255, 184, 92, 0.08);
}

.surface-tab.is-active {
  border-color: rgba(255, 184, 92, 0.54);
  background:
    linear-gradient(180deg, rgba(255, 184, 92, 0.14), rgba(255, 140, 66, 0.08)),
    rgba(255, 255, 255, 0.02);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}

.surface-tab-label {
  display: block;
  font-weight: 700;
  font-size: 0.98rem;
}

.surface-tab-copy {
  display: block;
  margin-top: 7px;
  color: var(--muted);
  font-size: 0.88rem;
  line-height: 1.4;
}

.live-rail {
  display: grid;
  gap: 10px;
  padding: 16px 18px;
  border: 1px solid var(--line);
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.02);
}

.workspace {
  display: none;
}

.workspace.is-active {
  display: grid;
  gap: 18px;
}

.workspace-grid {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(12, minmax(0, 1fr));
}

.support-grid > .spotlight {
  grid-column: span 6;
}

.support-grid > .compact-panel {
  grid-column: span 2;
}

.spotlight {
  background:
    radial-gradient(circle at top right, rgba(255, 184, 92, 0.16), transparent 28%),
    linear-gradient(145deg, rgba(22, 26, 38, 0.98), rgba(14, 18, 27, 0.98));
  border-color: rgba(255, 184, 92, 0.22);
}

.metric {
  color: var(--ink);
  font-size: 1.45rem;
  font-weight: 700;
  letter-spacing: -0.03em;
}

.spotlight-metric {
  color: var(--accent);
  font-size: clamp(2rem, 3vw, 2.9rem);
}

.muted,
.sidebar-copy,
.mobile-meta,
.focus-note,
.action-feedback {
  color: var(--muted);
}

.eyebrow {
  color: var(--accent);
  font-size: 0.76rem;
  letter-spacing: 0.04em;
  margin-bottom: 10px;
}

.focus-note {
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid var(--line);
}

.status-strip,
.status-stack,
.service-pill-stack,
.artifact-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
}

.status-chip,
.pill,
.toolbar-status {
  display: inline-flex;
  align-items: center;
  min-height: 34px;
  padding: 7px 12px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
  color: var(--ink);
  font-size: 0.86rem;
}

.pill {
  background: var(--accent-soft);
  border-color: rgba(255, 184, 92, 0.22);
  color: var(--accent);
}

.status-chip.accent {
  background: rgba(255, 184, 92, 0.14);
  border-color: rgba(255, 184, 92, 0.34);
  color: var(--accent);
}

.status-chip.secondary {
  color: var(--muted);
}

.status-chip[data-status-tone="healthy"] {
  border-color: rgba(111, 211, 164, 0.34);
  color: var(--success);
}

.status-chip[data-status-tone="refreshing"],
.status-chip[data-status-tone="working"] {
  border-color: rgba(255, 209, 102, 0.34);
  color: var(--warning);
}

.status-chip[data-status-tone="error"] {
  border-color: rgba(255, 127, 127, 0.34);
  color: var(--danger);
}

.toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.toolbar input {
  flex: 1 1 280px;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 12px 16px;
  font: inherit;
  color: var(--ink);
  background: rgba(255, 255, 255, 0.03);
}

.paired-sections {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.section-block {
  margin-top: 0;
}

.scroll-table {
  overflow: auto;
  margin-top: 14px;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.02);
}

.scroll-table table {
  min-width: 720px;
}

table {
  width: 100%;
  border-collapse: collapse;
}

th,
td {
  text-align: left;
  padding: 12px 12px;
  border-bottom: 1px solid var(--line);
  font-size: 0.92rem;
  vertical-align: top;
}

th {
  color: var(--muted);
  font-size: 0.8rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

tbody tr:hover {
  background: rgba(255, 255, 255, 0.02);
}

ul {
  margin: 0;
  padding-left: 18px;
}

.runtime-card {
  box-shadow: none;
}

.inset-card {
  background: rgba(255, 255, 255, 0.025);
}

.path-grid,
.info-list {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  margin-top: 14px;
}

.path-grid div,
.info-list div {
  padding: 14px 15px;
}

.advanced-panel summary {
  cursor: pointer;
  font-weight: 700;
  color: var(--accent);
  list-style: none;
}

.advanced-panel summary::-webkit-details-marker {
  display: none;
}

.mobile-card-list {
  display: none;
}

.mobile-card {
  padding: 14px 15px;
}

.action-button {
  border: 1px solid rgba(255, 184, 92, 0.34);
  border-radius: 999px;
  background: rgba(255, 184, 92, 0.12);
  color: var(--ink);
  padding: 9px 13px;
  cursor: pointer;
}

.action-button.secondary {
  border-color: var(--line);
  background: rgba(255, 255, 255, 0.03);
}

.action-button:disabled {
  cursor: wait;
  opacity: 0.7;
}

.action-feedback[data-status-tone="healthy"] { color: var(--success); }
.action-feedback[data-status-tone="working"] { color: var(--warning); }
.action-feedback[data-status-tone="error"] { color: var(--danger); }

.operator-main {
  display: grid;
  gap: 22px;
}

.operator-link-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
}

.operator-raw {
  margin: 0;
  padding: 18px;
  border-radius: 20px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
  color: var(--muted);
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  font: 0.88rem/1.6 "IBM Plex Mono", "SFMono-Regular", monospace;
}

.section-copy {
  max-width: 70ch;
}

.table-cell-stack {
  display: grid;
  gap: 4px;
}

.table-link {
  font-size: 0.88rem;
}

.progress-track {
  margin-top: 10px;
  width: 100%;
  height: 8px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.08);
  overflow: hidden;
}

.progress-bar {
  height: 100%;
  border-radius: 999px;
  background: linear-gradient(90deg, var(--accent), var(--accent-strong));
}

.browser-app-shell {
  gap: 18px;
}

.browser-hero {
  display: grid;
  gap: 16px;
  padding: 24px;
}

.browser-hero-grid {
  display: grid;
  gap: 18px;
  grid-template-columns: minmax(0, 1fr) minmax(280px, 0.78fr);
  align-items: end;
}

.browser-hero-copy {
  display: grid;
  gap: 10px;
  max-width: 44rem;
}

.browser-quick-grid {
  display: grid;
  gap: 10px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.browser-quick-card {
  padding: 14px 14px 13px;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.02);
}

.browser-quick-card span {
  display: block;
  margin-bottom: 10px;
  color: var(--muted);
  font-size: 0.78rem;
  letter-spacing: 0.03em;
}

.browser-quick-card strong {
  display: block;
  font-size: 1.05rem;
  line-height: 1.2;
  letter-spacing: -0.03em;
}

.browser-hero-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  align-items: center;
  justify-content: space-between;
  padding-top: 10px;
  border-top: 1px solid rgba(255, 255, 255, 0.06);
}

.activity-notice {
  display: grid;
  gap: 6px;
  padding: 14px 16px;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.025);
}

.activity-notice-accent {
  border-color: rgba(255, 184, 92, 0.28);
  background: rgba(255, 184, 92, 0.08);
}

.activity-notice-label {
  color: var(--accent);
  font-size: 0.78rem;
  letter-spacing: 0.03em;
}

.activity-notice-detail {
  margin: 0;
  color: #ddd5c7;
  font-size: 0.94rem;
}

.browser-hero-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: end;
  justify-content: flex-end;
}

.edge-summary {
  display: grid;
  gap: 6px;
}

.edge-summary-pill {
  display: inline-flex;
  align-items: center;
  min-height: 40px;
  padding: 0 14px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.03);
  font-size: 0.92rem;
}

.edge-editor {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: end;
}

.browser-hero-meta {
  display: grid;
  gap: 4px;
  justify-items: end;
}

.edge-connect {
  display: grid;
  gap: 6px;
  min-width: min(320px, 100%);
}

.edge-connect input {
  min-width: 280px;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 11px 14px;
  font: inherit;
  color: var(--ink);
  background: rgba(255, 255, 255, 0.03);
}

.edge-connect input::placeholder {
  color: rgba(221, 213, 199, 0.55);
}

.edge-connect-button {
  min-height: 44px;
}

.browser-surface-nav {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.browser-surface-nav .surface-tab {
  width: auto;
  min-width: 92px;
  padding: 10px 14px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.025);
  transform: none;
}

.browser-surface-nav .surface-tab:hover {
  transform: none;
}

.browser-surface-nav .surface-tab.is-active {
  box-shadow: none;
}

.browser-surface-nav .surface-tab.is-muted {
  opacity: 0.58;
}

.browser-surface-nav .surface-tab:disabled {
  cursor: default;
  pointer-events: none;
}

.surface-tab-label {
  font-size: 0.88rem;
}

.surface-tab-copy {
  display: none;
}

.browser-surface-layout {
  gap: 18px;
  grid-template-columns: minmax(0, 1.45fr) minmax(270px, 0.55fr);
}

.browser-focus-panel {
  gap: 18px;
}

.browser-spotlight {
  display: grid;
  gap: 16px;
  padding: 20px;
  border: 1px solid rgba(255, 184, 92, 0.18);
  border-radius: 20px;
  background:
    radial-gradient(circle at top right, rgba(255, 184, 92, 0.08), transparent 24%),
    linear-gradient(145deg, rgba(18, 22, 31, 0.98), rgba(13, 17, 26, 0.98));
}

.browser-spotlight-top {
  display: flex;
  gap: 16px;
  align-items: start;
  justify-content: space-between;
}

.browser-spotlight-copy {
  display: grid;
  gap: 10px;
}

.browser-focus-title {
  margin: 0;
  font-size: clamp(1.55rem, 2.4vw, 2.3rem);
  line-height: 1.02;
  letter-spacing: -0.04em;
}

.browser-focus-metric {
  color: var(--accent);
}

.browser-metric-band {
  display: grid;
  gap: 10px;
  grid-template-columns: repeat(auto-fit, minmax(128px, 1fr));
}

.browser-inline-list {
  display: grid;
  gap: 10px;
}

.browser-action-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.action-button {
  border: 1px solid rgba(255, 184, 92, 0.3);
  border-radius: 999px;
  padding: 11px 16px;
  font: inherit;
  font-weight: 600;
  cursor: pointer;
  transition: background 180ms ease, border-color 180ms ease, transform 180ms ease;
}

.action-button:hover {
  transform: translateY(-1px);
}

.action-button-primary {
  background: linear-gradient(180deg, rgba(255, 184, 92, 0.18), rgba(255, 140, 66, 0.12));
  color: var(--ink);
}

.action-button-secondary {
  border-color: var(--line);
  background: rgba(255, 255, 255, 0.03);
  color: var(--ink);
}

.experiment-card-button {
  width: 100%;
  text-align: left;
  cursor: pointer;
}

.experiment-card-button.is-selected {
  border-color: rgba(255, 184, 92, 0.36);
  background: rgba(255, 184, 92, 0.08);
}

.capability-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.experiment-hero-actions {
  padding-top: 2px;
}

.app-title {
  max-width: 16ch;
  font-size: clamp(2rem, 3.8vw, 3.7rem);
}

.app-subtitle,
.toolbar-detail,
.section-detail {
  max-width: 48ch;
  color: #ddd5c7;
  font-size: 0.94rem;
}

.panel {
  padding: 24px;
}

.directory-list {
  display: grid;
  gap: 10px;
}

.directory-row {
  width: 100%;
  display: grid;
  gap: 14px;
  grid-template-columns: minmax(0, 1fr) minmax(180px, 0.42fr);
  align-items: center;
  padding: 16px 18px;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.02);
  text-align: left;
  cursor: pointer;
  transition:
    border-color 180ms ease,
    background 180ms ease,
    transform 180ms ease;
}

.directory-row:hover {
  transform: translateY(-1px);
  border-color: rgba(255, 184, 92, 0.26);
  background: rgba(255, 255, 255, 0.03);
}

.directory-row.is-selected {
  border-color: rgba(255, 184, 92, 0.36);
  background:
    linear-gradient(180deg, rgba(255, 184, 92, 0.06), rgba(255, 255, 255, 0.02)),
    rgba(255, 255, 255, 0.03);
}

.directory-row-main,
.directory-row-side {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.directory-row-title {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
}

.directory-row-side {
  justify-items: end;
}

.directory-row-value {
  display: grid;
  gap: 2px;
  justify-items: end;
}

.directory-row-value span {
  color: var(--muted);
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.directory-row-value strong {
  font-size: 0.94rem;
  text-align: right;
}

.directory-row-access strong {
  color: #ddd5c7;
}

.leaderboard-list {
  gap: 10px;
}

@media (max-width: 1100px) {
  .hero-top,
  .hero-bottom,
  .paired-sections,
  .app-header,
  .surface-layout,
  .browser-hero-grid,
  .browser-surface-layout {
    grid-template-columns: 1fr;
  }

  .surface-nav-inline {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .app-toolbar {
    grid-template-columns: 1fr;
  }

  .toolbar-meta {
    justify-items: start;
  }

  .browser-hero-meta {
    justify-items: start;
  }

  .browser-hero-actions {
    justify-content: start;
  }

  .support-grid > .spotlight,
  .support-grid > .compact-panel {
    grid-column: span 12;
  }

  .directory-row {
    grid-template-columns: 1fr;
  }

  .directory-row-side {
    justify-items: start;
  }

  .directory-row-value,
  .directory-row-value strong {
    text-align: left;
    justify-items: start;
  }
}

@media (max-width: 720px) {
  main {
    padding: 16px 16px 32px;
  }

  .panel {
    padding: 16px;
    border-radius: 20px;
  }

  .summary-grid,
  .grid,
  .hero-meta-grid,
  .stat-grid,
  .app-summary-grid,
  .experiment-hero,
  .browser-quick-grid,
  .browser-metric-band,
  .directory-row {
    grid-template-columns: 1fr;
  }

  .toolbar {
    flex-direction: column;
    align-items: stretch;
  }

  .toolbar-status {
    width: 100%;
  }

  .surface-nav-inline {
    grid-template-columns: 1fr 1fr;
  }

  .browser-surface-nav {
    width: 100%;
  }

  .browser-hero-actions,
  .edge-connect,
  .edge-editor,
  .edge-summary {
    width: 100%;
  }

  .edge-connect input {
    min-width: 0;
    width: 100%;
  }

  .browser-surface-nav .surface-tab {
    flex: 1 1 calc(50% - 10px);
    min-width: 0;
  }

  .keyvalue-row,
  .leaderboard-row,
  .experiment-title-row,
  .browser-spotlight-top,
  .browser-hero-bar {
    flex-direction: column;
    align-items: start;
  }

  .keyvalue-row strong,
  .leaderboard-meta {
    text-align: left;
    justify-items: start;
  }

  table {
    display: none;
  }

  .scroll-table {
    display: none;
  }

  .mobile-card-list {
    display: grid;
    gap: 10px;
    margin-top: 14px;
  }
}
"#;
