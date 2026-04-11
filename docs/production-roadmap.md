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

today the default implementation is still file-backed, but the important
boundary is now explicit:

- bootstrap admin/export paths no longer need to read directly from ad hoc
  preview vectors
- a shared operator store backend can be swapped in later without rewriting the
  route layer

### browser receipt outbox seam

browser receipt submission is no longer modeled as "just a vec in storage".

the browser storage model now has an explicit `BrowserReceiptOutbox` with a
declared backend:

- `Snapshot`
- `IndexedDb`

the wasm/browser runtime still defaults to snapshot-backed persistence today,
but the storage schema and worker flow now have a durable-outbox seam and
backward-compatible snapshot decoding.

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
  across heads, receipts, merges, and head-eval reports

still missing:

- a real external backend, with postgres first
- stronger search, retention, audit, and replay semantics than point-in-time
  mirrored snapshots
- streaming export paths for very large operator histories

### browser durability

partially present:

- bounded membership history
- batched receipt flush
- explicit outbox backend seam
- compatibility with legacy snapshot payloads
- local-storage-backed durable receipt outbox wired into the browser controller

still missing:

- indexeddb-backed durable outbox implementation
- resumable outbox sync and replay after long offline periods
- durable artifact/receipt sync checkpoints for browser peers

### placement and scheduling

partially present:

- native and browser capability probes
- role/budget heuristics
- observed-throughput rebudgeting

still missing:

- decayed long-horizon performance model
- stronger placement feedback loop between limits, topology, and lease planning
- attested or signed placement inputs for semi-trusted/public fleets

## not complete yet

these roadmap items are still open work:

- hot / warm / cold artifact storage classes and clearer large-artifact
  placement policy
- first-class deploy secret injection and packaged redis/postgres reference
  modules
- stronger local-sgd drift correction and adaptive micro-epoch sizing
- long-run browser durability beyond bounded in-memory state

## current claim

the honest claim today is:

- good `pre.8` decentralized training runtime
- coherent validator-quorum authority boundary
- sane deployment references
- bounded long-run memory behavior in the main live runtime paths
- production seams made explicit in code for the next stage

the repo should not yet claim:

- byzantine-grade validator governance
- fully externalized operator plane
- durable public-browser training at long offline horizons
- finished internet-scale production packaging
