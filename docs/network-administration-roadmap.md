# network administration roadmap

this document defines the target administration model for `burn_p2p`.

it exists to close two concrete problems in the current runtime:

- experiment administration is split between directory-driven reassignment and
  control-announcement windowing
- the control schema is ahead of the runtime orchestration, so some signed
  operations exist in schema form but are not yet the authoritative runtime path

the goal is to make the network administration story explicit, uniform, and
operationally safe for long-running multi-experiment fleets.

## status on main

the core roadmap has now landed on `main`.

as of `2026-04-12`, the repo now has:

- signed lifecycle plans and signed fleet schedule epochs carried end to end
  through the control plane
- runtime execution for pause, resume, patch, head promotion, rollback, and
  cohort reassignment through one authoritative control path
- slot-aware runtime persistence and assignment handling instead of a
  single-primary-only control model
- bootstrap/admin issuance for lifecycle and schedule actions
- retained operator lifecycle/schedule history in replay snapshots and typed
  control replay records
- typed json and human-facing html views for operator audit, retained replay,
  and control replay history
- browser replay invalidation and stale-revision handling tied to assignment and
  publication state

the remaining work is now mostly incremental hardening, scale tuning, and
downstream adoption pressure rather than missing administration primitives.

## current state

the runtime now behaves like this:

- one `network_id` hosts a shared control plane
- that control plane can advertise multiple experiments through directory
  entries
- lifecycle plans and schedule epochs are the authoritative transition and
  placement path when present
- nodes can persist and restore slot-local assignments
- operator retention and replay include lifecycle and schedule history
- browser clients invalidate stale replay state when assignment or publication
  bindings no longer match

that is now a coherent administration model rather than only a set of seams.

the main remaining weak points are narrower:

- the browser replay path still benefits from more scale-oriented durability
  work and longer-horizon soak coverage
- operator search and replay surfaces can still grow more task-oriented export
  flows on top of the typed records now available
- larger real-world fleets may still expose planner and retention ergonomics
  that only appear under heavier production pressure

## target claim

the intended model is:

- one network is a long-lived tenancy boundary
- one network can host many concurrent studies and experiments
- revisions are explicit lifecycle transitions within an experiment
- directory entries are discovery and eligibility hints, not the final
  authoritative switch trigger
- all runtime-effective experiment transitions happen through signed, replayable,
  window-bound control objects
- nodes may carry multiple assignment slots when their capability budget allows
  it
- scheduler inputs and experiment transitions are durable, queryable, and
  operator-auditable

## administrative hierarchy

the control hierarchy should be treated as:

### network

the network is the transport and governance boundary.

it pins:

- network identity
- authority epoch
- compatible project family
- release train and protocol major
- planner set and scheduler trust boundary
- storage, retention, and publication policy

### study

a study is the operator-facing container for related experiments.

it groups:

- shared dataset lineage
- shared governance and access policy
- shared reporting and audit views

### experiment

an experiment is the unit of live assignment and lifecycle control.

it owns:

- merge policy
- eligibility policy
- target workload family
- active revision
- active scheduler policy
- active pause or drain status

### revision

a revision is an activation-managed change to one experiment.

it should capture:

- workload identity
- model schema hash
- dataset view
- patch set
- activation target
- migration prerequisites
- fallback or rollback target

### assignment slot

an assignment slot is the unit of node participation.

the long-term model should allow:

- zero or more active slots per node
- per-slot role assignment
- per-slot experiment binding
- per-slot drain, block, and migration state

## target control-plane model

the control plane should be split into three layers with clear semantics.

### 1. directory layer

the directory is for discovery, visibility, and eligibility.

it should answer:

- what experiments exist on this network
- which revisions are current or staged
- who is eligible to discover or opt in
- what capability class is required

it should not be the final authority for immediate runtime cutover.

directory entries should become eventually consistent read models derived from
the authoritative lifecycle state.

### 2. lifecycle layer

the lifecycle layer is the authoritative experiment state machine.

introduce a signed `ExperimentLifecyclePlan` with:

- `network_id`
- `study_id`
- `experiment_id`
- `plan_epoch`
- `target_revision_id`
- `base_revision_id`
- `activation_window`
- `grace_windows`
- `required_client_capabilities`
- `required_authority_epoch`
- `required_project_family_id`
- `required_release_train_hash`
- `migration_policy`
- `rollback_policy`
- `planner_epoch`

the lifecycle plan is what nodes actually execute.

### 3. scheduler layer

the scheduler layer is the authoritative assignment plan for a bounded window
range.

introduce a signed `FleetScheduleEpoch` with:

- `network_id`
- `schedule_epoch`
- `window_range`
- `planner_set_epoch`
- per-peer capability summary
- per-peer trust or confidence weight
- per-peer experiment slot assignments
- per-peer budget scale
- per-peer microshard scale
- per-peer preferred reducer or validator placement hints

the schedule epoch should be logically separate from the directory and the
lifecycle plan, but cross-linked to both.

## unified experiment lifecycle

every experiment revision transition should move through one explicit state
machine.

recommended states:

- `draft`
- `announced`
- `staged`
- `prewarming`
- `ready`
- `activating`
- `active`
- `draining`
- `paused`
- `rolled_back`
- `archived`

state meaning:

- `draft`: operator-private intent, not visible to workers
- `announced`: visible in directory, not yet eligible for assignment
- `staged`: valid plan exists, clients can prefetch metadata and artifacts
- `prewarming`: selected peers are fetching base artifacts and verifying
  compatibility
- `ready`: activation barrier may proceed if readiness quorum is satisfied
- `activating`: switch is in progress at the activation window
- `active`: revision accepts new leases and publishes new training output
- `draining`: old revision still serves artifacts and receipts, but receives no
  new leases
- `paused`: experiment is administratively blocked without losing its lifecycle
  context
- `rolled_back`: transition reverted to a prior known-good revision
- `archived`: no further leases or control changes, read-only history remains

## authoritative change-over protocol

experiment change-over should no longer be a best-effort local reconciliation.

the target rollout protocol is:

### phase a: publish staged revision

operators publish:

- the new revision manifest
- the signed lifecycle plan
- the target activation window
- the compatibility and capability requirements

nodes do not switch yet.

### phase b: prewarm and acknowledge

eligible nodes:

- verify the signed lifecycle plan
- verify project family and release compatibility
- prefetch required artifacts
- validate base head lineage
- publish signed readiness acknowledgements

bootstrap and operators aggregate readiness by role and cohort.

### phase c: activate at a window barrier

at `activation_window`:

- only nodes with valid readiness acknowledgements participate in the new
  revision
- old revision stops receiving new leases
- scheduler assignment moves to the new revision
- control plane marks the old revision as `draining`

the barrier should require role-specific minimum readiness, not only a single
global count.

### phase d: drain and retire

after activation:

- old revision continues serving head and artifact history for a bounded drain
  window
- validators finish any in-flight attestations against the old revision
- nodes unsubscribe from old experiment topics after the drain window closes

### phase e: finalize or roll back

if health checks stay green, finalize the new revision as `active`.

if readiness or health fails, publish a signed rollback plan that returns the
assignment and head target to the previous revision at a declared rollback
window.

## runtime alignment work

the runtime should fully consume the control schema rather than leaving commands
as partially wired types.

the required alignment is:

- `PatchExperiment` updates staged revision state and directory projections
- `PromoteHead` changes the authoritative serve or canonical target for the
  current lifecycle plan
- `RollbackHead` triggers a signed head rollback within the current revision or
  lifecycle plan
- `ReassignCohort` updates the scheduler view and slot assignment plan
- `PauseExperiment` and `ResumeExperiment` continue to act as lifecycle state
  changes, not special one-off logic

the node should stop treating directory diffs as sufficient reason to switch
revisions immediately. directory change should point the node toward the latest
lifecycle plan, and the lifecycle plan should decide when the switch happens.

## network reuse for more than one experiment

the intended network-tenancy model should support:

- multiple experiments active at the same time on one network
- multiple revisions staged while one revision is active
- multiple assignment slots on one capable node
- draining one experiment while another continues uninterrupted

that implies three concrete changes:

- replace the implicit single-primary-assignment assumption with explicit slot
  capacity and slot leases
- make experiment overlays and subscription state per slot, not only per node
- add explicit topic retirement so a node does not accumulate stale
  subscriptions across many experiment transitions

reusing the same network should mean reusing:

- identity and governance
- discovery and peer directory
- artifact routing surface
- scheduler and planner trust boundary

it should not mean overloading one node-global assignment state forever.

## long-horizon signed fleet scheduling

the current short-horizon placement hints should become one input to a stronger
planner epoch model.

the target scheduler design is:

- collect peer capability and performance summaries over many windows
- aggregate those into a decayed long-horizon history
- let one planner or a planner quorum publish a signed `FleetScheduleEpoch`
- require distinct-planner agreement before strong negative actions such as
  pruning or hard demotion
- separate scheduler confidence from hard admission or revocation

minimum scheduler features:

- per-peer long-horizon throughput and failure history
- per-peer lag and stale-head history
- per-experiment fairness budget
- anti-fragmentation placement for reducers, validators, and trainers
- explicit slot packing limits for multi-slot peers
- signed budget and microshard scales
- explicit validity window and supersession rules

## operator data model

the operator plane should model lifecycle and scheduling as first-class audited
objects in postgres rather than only snapshots and flattened rows.

the minimum tables or logical entities should be:

- `network_epochs`
- `study_records`
- `experiment_records`
- `revision_records`
- `lifecycle_plans`
- `lifecycle_events`
- `readiness_acknowledgements`
- `schedule_epochs`
- `schedule_assignments`
- `head_decisions`
- `retention_policies`
- `artifact_drain_records`
- `replay_snapshots`

minimum operator capabilities:

- search by network, study, experiment, revision, window, peer, head, or epoch
- replay lifecycle and scheduler state at any retained point in time
- retention reporting by entity family
- bounded streaming export for large histories
- explicit prune actions with audit evidence

## browser and long-offline semantics

browser support should be treated as a first-class but capacity-limited
participant class.

for long-offline operation, the model should be:

- artifact manifests are durable and content-addressed
- checkpoint metadata stores completed chunk ranges, not only coarse prefixes
- resumed downloads use `Range` fetch and provider fallback
- old partial chunks remain reusable until retention policy or manifest mismatch
  invalidates them
- receipt and control outboxes replay independently from artifact replay

the browser runtime should also understand lifecycle state:

- do not start a large resume on a revision that is already draining
- prefer the active revision over a staged one unless the node explicitly opted
  into prewarm
- surface lifecycle state in the browser app, not only generic assignment state

## operator workflow

the operator-facing workflow should become:

1. create or patch a study
2. create or patch an experiment
3. publish a staged revision and lifecycle plan
4. observe readiness acknowledgements and prewarm completion
5. publish or confirm the schedule epoch for the activation window
6. activate at the barrier
7. monitor drain and health
8. finalize or roll back
9. archive old revisions under explicit retention policy

the important change is that operators should reason in terms of staged plans
and activation barriers, not raw directory mutations.

## phased implementation roadmap

### phase 1: make lifecycle authoritative

deliverables:

- add `ExperimentLifecyclePlan`
- change directory to derived read model semantics
- teach running nodes to execute lifecycle plans at window barriers
- wire all existing control commands into runtime application

success criteria:

- revision rollout no longer depends on direct directory diff switching
- pause, resume, patch, head promotion, and cohort reassignment all execute
  through one runtime path

### phase 2: introduce multi-slot assignment

deliverables:

- replace single-primary-assignment assumptions with slot leases
- store slot-local experiment bindings
- add explicit subscription retirement on slot drain or archive

success criteria:

- one node can host multiple experiment assignments safely
- experiment reuse no longer implies node-global state churn

### phase 3: ship signed scheduler epochs

deliverables:

- add `FleetScheduleEpoch`
- require verified planner signatures
- incorporate long-horizon decayed history
- add planner agreement rules for hard negative placement decisions

success criteria:

- assignment plans are replayable and operator-auditable
- placement no longer depends only on local short-horizon hints

### phase 4: formalize operator postgres history

deliverables:

- lifecycle, assignment, and replay entities in postgres
- search endpoints and replay views
- retention and prune semantics with audit evidence

success criteria:

- operators can reconstruct exactly why an experiment changed state
- operators can replay scheduler and lifecycle decisions after the fact

### phase 5: finish long-offline browser replay

deliverables:

- durable chunk-range index
- manifest-aware replay repair
- explicit stale partial invalidation rules
- lifecycle-aware resume policy

success criteria:

- long-offline browser peers can resume large artifact downloads without
  restarting from scratch
- operator retention changes do not leave the browser in undefined replay state

## invariants to preserve

the following invariants should hold throughout the rollout:

- no node trains a new window on a revision that is not `active`
- no revision becomes `active` without a signed lifecycle plan
- no hard placement exclusion is accepted from an unverified planner
- directory state never outruns lifecycle authority
- rollback is explicit, signed, and replayable
- old artifacts remain fetchable through the declared drain window

## honest claim after this roadmap

after this roadmap, the admin story is:

- one long-lived network can safely host many experiments and revisions
- experiment transitions are scheduled, signed, replayable, and observable
- scheduler plans are durable and auditable
- browser peers can survive long offline periods without undefined replay state

the remaining claim should now be narrower:

- the core administration model is in place
- further work is mostly hardening, scale validation, and operator ux polish
