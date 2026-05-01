# production roadmap

this is the current state of the longer-term production hardening work.

the short version:

- the repo is coherent and healthy now
- several production seams are now explicit in code
- the full internet-scale roadmap is still not "done"

## implemented now

### provider-backed github policy

github auth is no longer only a branded callback surface.

the connector now:

- hydrates live provider identity from `github.com` / `api.github.com`
- rehydrates org membership from `/user/orgs`
- rehydrates team membership from `/user/teams`
- rehydrates collaborator/ownership access from `/user/repos`

principal records can now match:

- `provider_subject`
- `provider_login`
- `provider_email`
- `provider_orgs`
- `provider_groups`
- `provider_repo_access`
- `provider_claim:<name>`

bootstrap enrollment now revalidates provider-backed sessions before issuing
node certificates, and issued certificates now capture an `AuthPolicySnapshot`
inside the signed claims so the admission decision is auditable after the fact.

deploy profiles can now also express first-class `provider_policy.github.rules`
instead of encoding github-governed admission only as static principal records.

### authority config surface

the repo now has first-class authority governance manifest types:

- `ValidatorSetManifest`
- `ValidatorSetMember`
- `AuthorityEpochManifest`
- `AuthorityEvidenceRecord`

these live in `burn_p2p_core` and are wired into bootstrap `AuthorityPlan`.
bootstrap planning now rejects:

- empty validator sets
- impossible quorum weights
- mismatched authority epoch / validator-set references
- network-id mismatches

this gives validator governance a real schema home instead of leaving it as
only chat/docs intent.

### operator store seam

bootstrap now reads operator history, metrics-backed eval reports, publication
state, and leaderboard snapshots through `operator_store`.

the important boundary is now explicit:

- bootstrap admin/export paths no longer need to read directly from ad hoc
  preview vectors
- shared operator state can now be mirrored through postgres-backed snapshots
  for multi-edge reads without rewriting the route layer

### browser browser-state durability seam

browser storage is no longer only an in-memory snapshot plus a best-effort
receipt queue.

the browser storage model now has:

- an explicit `BrowserReceiptOutbox` backend with `IndexedDb` and `LocalStorage`
- a durable `BrowserStorageSnapshot` path that prefers indexeddb and falls back
  to local storage

the wasm/browser runtime now prefers indexeddb-backed durability, falls back to
local-storage when indexeddb is unavailable, and migrates legacy local-storage
payloads forward when possible.

## partially implemented

### authority plane

partially present:

- release policy
- validator evidence policy
- revocation epochs
- trusted issuer rotation / retirement
- validator-set and authority-epoch manifests

still missing for a stronger internet-scale authority story:

- signed validator-set rollout and rotation workflow
- evidence publication and audit pipeline
- authority epoch transition certificates
- validator-set governance as a first-class control-plane operation

### operator plane

partially present:

- file-backed durable history
- bounded in-memory previews
- paged history reads in bootstrap
- publication/leaderboard/export paths reading through a shared `OperatorStore`
  seam
- optional redis-mirrored operator snapshots for multi-edge read coherence
  across heads, receipts, merges, peer-window metrics, reducer-cohort metrics,
  and head-eval reports
- postgres-backed shared operator snapshots for multi-edge read coherence
- packaged redis/postgres reference modules in `deploy/compose/`

still missing:

- stronger search, retention, audit, and replay semantics than the current
  point-in-time snapshot backends
- streaming export paths for very large operator histories

### browser durability

partially present:

- bounded membership history
- batched receipt flush
- explicit outbox backend seam
- compatibility with legacy snapshot payloads
- local-storage-backed durable receipt outbox wired into the browser controller
- indexeddb-backed durable receipt outbox wired into the browser controller with
  local-storage migration fallback
- durable browser storage snapshots wired into the browser controller with
  indexeddb/local-storage fallback

still missing:

- resumable outbox sync and replay after long offline periods
- durable artifact/receipt sync checkpoints for browser peers

### placement and scheduling

partially present:

- native and browser capability probes
- role/budget heuristics
- damped observed-throughput rebudgeting
- recent peer-window placement hints propagated through the metrics plane and used to avoid
  immediately reassigning freshly failed or badly lagging trainers
- bounded decayed multi-window trainer history used to rank stable healthy peers above
  bursty or repeatedly failing peers during assignment

still missing:

- decayed long-horizon performance model
- stronger placement feedback loop between limits, topology, and lease planning
- attested or signed placement inputs for semi-trusted/public fleets

### artifact publication and storage

partially present:

- explicit `artifact_publication.targets` in the reference/browser deployment configs
- hot local filesystem publication target for low-latency edge streaming
- warm s3-compatible publication target wired through env-placeholder deployment config
- packaged minio reference module for local/reference s3-compatible publication

still missing:

- a true cold-archive lifecycle manager
- operator-grade large-export orchestration and retention enforcement across tiers
- managed-cloud reference modules for artifact publication beyond the local/reference minio stack

## not complete yet

these roadmap items are still open work:

- stronger local-sgd drift correction and adaptive micro-epoch sizing beyond
  the new default root-ema smoothing
- long-run browser durability beyond storage-snapshot persistence, especially
  resumable artifact sync and long-offline replay

## current claim

the honest claim today is:

- good `0.21.0-pre.54` decentralized training runtime
- coherent validator-quorum authority boundary
- sane deployment references
- bounded long-run memory behavior in the main live runtime paths
- provider-backed github policy, shared operator-state backends, and browser
  storage durability represented directly in code/config rather than only as
  future seams
- production seams made explicit in code for the next stage

the repo should not yet claim:

- byzantine-grade validator governance
- fully externalized operator plane
- durable public-browser training at long offline horizons
- finished internet-scale production packaging
