# memory retention

this note is about long-running node memory, not just rust leaks.

the main distinction is:

- **resident state**: data that stays in process memory for the full node lifetime
- **request-time state**: data loaded on demand for one admin/export request

the goal is:

- training and validation loops should keep flat resident memory over many windows
- operator/admin history should live in durable storage, not in peer memory
- when a full export is needed, it should be an explicit request-time cost

## current shape

### low risk

- `burn_p2p`
  - training, validation, and canonical promotion do not retain per-window model state forever
  - runtime security telemetry is pruned to active and recent peers
  - raw metrics artifact pruning already runs against configured budgets

- `burn_p2p_engine`
  - merge math is ephemeral
  - no long-lived append-only resident collections

- `burn_p2p_workload`
  - workload/data-pipeline types are mostly config and batch wiring
  - no durable resident history

- `burn_p2p_checkpoint`
  - artifacts are disk-backed
  - memory use is tied to the current artifact/materialization call, not node lifetime

- `burn_p2p_dataloader`
  - active lease caches are pruned by window
  - no unbounded in-memory lease history

- `burn_p2p_experiment`
  - topology and revision planning are structural/config objects

- `burn_p2p_security`
  - policy engines and reports are scoped to current trust state, not append-only history

- `burn_p2p_limits`
  - capability heuristics only

- `burn_p2p_social`
  - leaderboard computation is a pure transform over provided receipts/merges
  - it does not keep global resident history by itself

### low after mitigation

- `burn_p2p_swarm`
  - live control-plane announcement tails are bounded
  - disconnected peer observations are now age-pruned and count-pruned
  - per-peer address sets are capped

- `burn_p2p_metrics`
  - raw metric envelopes are retained by per-revision and per-experiment budgets
  - reopening the durable store compacts loaded history back to that budget

- `burn_p2p_bootstrap`
  - operator state keeps bounded in-memory tails only
  - receipts, merges, heads, and protocol manifests are loaded from durable history on demand
  - leaderboard and export surfaces read from durable history instead of growing preview vectors

- `burn_p2p_auth_external`
  - pending logins and provider sessions are pruned by expiry and capped by count

- `burn_p2p_publish`
  - current aliases and active download tickets stay in the registry
  - alias history, export jobs, and published artifacts are durable sidecar histories
  - the resident registry now keeps bounded previews instead of the full history

### medium residual

- `burn_p2p_browser`
  - receipt membership history is bounded
  - metrics catchup cache is bounded
  - receipt flushes now send bounded batches instead of cloning the full outbox every time
  - **residual**: the pending receipt outbox itself is still intentionally lossless and can grow while the browser stays offline

- `burn_p2p_publish`
  - resident memory is now bounded
  - **residual**: admin endpoints that ask for the full export/publication history still allocate a full response on demand

### low / no long-lived resident risk in practice

- `burn_p2p_python`
  - worker control state is session-oriented
  - model weights live in the python worker, not in long-lived rust-side append-only stores

- `burn_p2p_browser` example and test scaffolding
  - test/runtime state is short-lived by construction

- `burn_p2p_testkit`
  - stress fixtures are process-scoped and expected to exit
  - not a production resident service

## what changed in this pass

- `burn_p2p_swarm`
  - stale disconnected peers are pruned
  - disconnected-peer history is capped
  - per-peer address sets are capped

- `burn_p2p_publish`
  - `registry.json` now keeps current aliases, active tickets, and bounded previews only
  - export jobs, published artifacts, and alias history moved to durable jsonl sidecars
  - admin/export views rehydrate from durable history on demand

- `burn_p2p_browser`
  - receipt outbox flushes are batched so one worker event does not clone an arbitrarily large backlog

- `burn_p2p_metrics`
  - explicit reopen test now verifies that old `entries.jsonl` history is compacted back to the configured retention budget

## remaining boundaries

### browser receipt outbox

the browser outbox is no longer purely in-memory. the browser controller now
persists the queued receipt outbox through a local-storage-backed durable path.

that improves reload/restart behavior, but it does not remove the larger
scaling tradeoff:

- the outbox is still intentionally lossless while receipts remain unsent
- very large offline backlogs still make snapshot-sized durable state grow

the next clean step is still:

- an indexeddb-backed queue for wasm/browser deployments
- with `BrowserStorageSnapshot` holding only counters and recent preview ids

### large operator exports

bootstrap/publication/metrics export paths can still allocate large response bodies when an operator asks for full history.

that is deliberate request-time cost, not background resident growth.

for internet-scale or long-lived corporate deployments, the better shape is:

- keep peer memory bounded and local
- keep training-critical state in the p2p runtime
- move operator history, leaderboard materializations, and long-term metrics into a durable store or external db

the current file-backed stores are fine as the default reference implementation. a centralized db is the next scaling step for operator surfaces, not for training correctness.

## verification

the important retention regressions are covered by tests:

- `control_plane_snapshot_caps_live_announcement_histories`
- `peer_store_prunes_stale_and_excess_disconnected_observations`
- `metrics_indexer_prunes_raw_history_and_caps_detail_payloads`
- `metrics_indexer_prunes_older_revisions_per_experiment`
- `metrics_store_reopen_compacts_loaded_entries_to_retention_budget`
- `operator_history_preview_stays_bounded_while_store_backed_exports_remain_complete`
- `contribution_receipt_ingest_keeps_a_bounded_preview_tail`
- `publication_store_keeps_bounded_recent_previews_while_full_history_remains_durable`
- `browser_storage_bounds_receipt_membership_history`
- `worker_runtime_flushes_receipt_outbox_in_bounded_batches`
- `external_proxy_connector_prunes_and_bounds_pending_logins`
- `provider_connector_prunes_expired_and_bounds_persisted_sessions`
- `tracked_peer_security_state_prunes_inactive_history_but_keeps_active_peers`
