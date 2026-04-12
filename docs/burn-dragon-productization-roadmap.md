# burn_dragon productization roadmap

this document captures the remaining `burn_p2p` work that would reduce
downstream glue in `burn_dragon_p2p` and make a real multi-workload burn app
feel like a first-class downstream target instead of a custom integration.

it is intentionally narrower than the repo-wide production or network
administration roadmaps:

- [production-roadmap.md](production-roadmap.md) covers internet-scale operator,
  deployment, durability, and authority hardening
- [network-administration-roadmap.md](network-administration-roadmap.md) covers
  the long-lived multi-experiment control model for the network itself
- this document covers the downstream ergonomics needed by one concrete custom
  burn product family, `burn_dragon_p2p`

the goal is not to move dragon-specific model code upstream. the goal is to
move reusable p2p/runtime/product seams upstream so downstream crates only own
experiment-specific logic.

## current alignment with the repo

parts of the original burn_dragon roadmap are still right, but several adjacent
productization gaps have already been closed upstream and should no longer be
treated as missing capability.

### already upstream now

- github-governed admission is no longer only downstream composition
  - provider-backed github policy is implemented
  - deploy configs can use `provider_policy.github.rules`
  - issued certificates carry an auditable `AuthPolicySnapshot`
- network administration is no longer only directory-driven
  - signed `ExperimentLifecyclePlan` exists
  - lifecycle plans are persisted and applied during normal training-window
    progression
  - bootstrap admin can issue lifecycle actions
- operator replay and retention are no longer only preview-vector state
  - shared operator-store backends exist
  - postgres-backed replay/search/retention support exists
- browser durability is no longer only a best-effort receipt queue
  - `BrowserStorageSnapshot` exists
  - indexeddb/local-storage durability exists
  - resumable replay checkpoints exist
- the common data-path seam already exists as `LeaseDataPipeline`
  - shard-backed, indexed, generated, and custom pipeline kinds already exist
  - downstream burn apps already have a reasonable native path through
    `burn_p2p::burn::from_loaders(...)` and `.with_data_pipeline(...)`

### still real downstream gaps

the remaining burn_dragon-specific gaps are more focused:

- browser training execution payloads are still owned by `burn_p2p_browser`
  instead of a host-neutral workload seam
- native and browser training paths still do not share enough of the same
  upstream execution/result language
- browser/local workload input descriptions are still too implicit in closures
  and runtime hooks for a custom multi-workload product
- downstream apps still need cleaner builders for experiment-family and
  lifecycle-oriented administration
- `burn_p2p_app` is still more reusable as a full reference portal than as a
  set of smaller product components
- repo-internal `burn_p2p_testkit` coverage is strong, but there is not yet a
  small public downstream conformance surface for custom browser burn products

## problem statement

`burn_dragon_p2p` works today, but it still owns too much code that is
conceptually part of the `burn_p2p*` product surface:

- browser-local burn training loop and stats reporting
- browser-local train/eval command/result contracts
- browser-local workload input modeling for token windows and synthetic corpora
- custom experiment-family and lifecycle configuration glue
- experiment/product ui that is more specific than the reference portal but
  still overlaps with `burn_p2p_app`

the result is functional, but not ideal:

- native burn integration is clean through `burn_p2p::burn`
- browser burn integration is still too downstream-owned
- native and wasm training paths still diverge more than they should
- workload input/data-pipeline seams are not yet rich enough for a custom
  multi-workload browser product
- shared app/view crates do not yet expose the right reusable seams for a
  custom experiment product

## design constraints

the roadmap should preserve the current project direction rather than pulling
the repo back toward an older design.

### 1. keep network administration aligned with lifecycle plans

downstream ergonomics should build on the current administration model:

- lifecycle plans are authoritative for revision change-over
- directory entries are discovery/read models, not the final switch trigger
- a long-lived network may host many experiments and revisions over time

burn_dragon helpers should therefore target:

- lifecycle-plan creation
- staged/prewarm/activate workflows
- experiment-family selection
- multi-experiment reassignment

they should not re-center the downstream story around hand-built directory
updates alone.

### 2. build on `LeaseDataPipeline`, not beside it

the repo already has a backend-neutral data-path seam:

- `LeaseDataPipeline`
- `LeaseDataPipelineDescriptor`
- `LeaseDataPipelineKind::{ShardedStatic, IndexedDataset, GeneratedDataset, Custom}`

the burn_dragon roadmap should extend that seam with better descriptors,
serialization, and helpers where needed. it should not invent a parallel input
model unless there is a concrete capability that `LeaseDataPipeline` cannot
support.

### 3. browser training should stay WebGPU-first

browser training remains a WebGPU-oriented product capability.

non-goals:

- do not productize browser cpu training as a first-class runtime role
- do not bend shared execution contracts around smoke-test-only cpu fallback

### 4. do not expose repo-internal testkit structure as the public downstream api

`burn_p2p_testkit` is currently the repo's qa/simulation harness, not a public
downstream dependency contract.

if downstream conformance support is needed, the correct shape is:

- a small public wrapper surface
- example-driven fixtures
- xtask- or harness-level entrypoints

not "tell downstream apps to depend directly on internal testkit guts."

## observed downstream pain points

### 1. native burn integration is upstreamed, browser burn integration is not

today:

- `burn_p2p::burn` provides a strong native integration seam
- `burn_p2p_browser` provides browser runtime/auth/session/control surfaces
- there is still no equivalent host-neutral upstream browser burn execution seam

downstream consequence:

- `burn_dragon_p2p` still owns browser-local training logic
- browser-local train/eval stats are downstream-defined
- native and wasm paths do not share enough of the same execution contract

this remains the biggest structural gap.

### 2. browser training contracts are still too host-owned

today, the browser crate still owns:

- `BrowserTrainingBudget`
- `BrowserTrainingPlan`
- `BrowserTrainingResult`
- `BrowserValidationPlan`
- `BrowserValidationResult`

downstream consequence:

- downstream browser-burn apps have to speak a browser-owned contract
- native and browser execution paths cannot converge cleanly on one shared
  upstream language

### 3. workload input description is still too closure- and app-owned

today, `LeaseDataPipeline` is the right base seam, but it is still too implicit
for richer downstream products.

what is missing is not a second unrelated pipeline system. what is missing is:

- richer serializable descriptors for browser/local input sources
- reusable helpers for common inline/http/generated browser input shapes
- clearer upstream language for generated/synthetic input providers

### 4. downstream experiment administration is still too manual

today the repo has the right runtime control objects, but downstream custom
products still lack a small ergonomic builder layer for:

- one project family with a small number of workloads
- one network with several experiments over time
- lifecycle-plan publication for staged revision rollout
- schedule/assignment helpers for common hosted topologies

the gap is now more about operator ergonomics than raw control-plane
capability.

### 5. `burn_p2p_app` is still too portal-oriented for custom products

`burn_p2p_app` is valuable as:

- the reference dioxus app
- static/ssr render surface
- shared browser/native component tree

but for burn_dragon the missing seams are still:

- reusable training/network panels without adopting the whole portal shell
- reusable auth/session/join widgets
- reusable typed models for local training results and browser trainer controls

### 6. downstream verification still needs a public conformance layer

the repo now has strong internal administration, browser, and mixed-fleet
coverage, but a custom downstream product still does not get a narrow public
surface for validating:

- auth/session restore
- local browser training execution
- network submission
- lifecycle-plan-driven experiment switching
- app snapshot rendering

that should be solved without making internal `burn_p2p_testkit` structure the
public downstream API.

## what should move upstream

### a. shared training and validation execution payloads

recommended ownership:

- `burn_p2p_workload`
  - host-neutral training/validation budget, plan, progress, and result
    contracts
- `burn_p2p::burn`
  - native burn host adapter for those contracts
- `burn_p2p_browser`
  - browser host/runtime/session/worker adapter for the same contracts
- `burn_p2p_views`
  - typed presentation models for those shared execution results

the reusable logic should be shared by native and browser paths, not copied
into a browser-only burn surface.

candidate shared types:

- `WorkloadTrainingBudget`
- `WorkloadTrainingPlan`
- `WorkloadTrainingProgress`
- `WorkloadTrainingResult`
- `WorkloadValidationPlan`
- `WorkloadValidationResult`

important boundary:

- these shared types should stay minimal
- they should cover training/validation plan, result, and progress semantics
- they should not absorb browser worker command routing, session state,
  transport state, runtime orchestration, or storage concerns
- host-specific browser orchestration should remain in `burn_p2p_browser`
  modules such as the bridge, worker, runtime, and session/storage surfaces

initial migration shape:

- keep the current browser types as compatibility aliases or wrappers for one
  release
- move host-neutral semantics into `burn_p2p_workload`
- leave browser worker/runtime/auth transport logic in `burn_p2p_browser`
- let browser worker commands and events continue to wrap or carry the shared
  execution payloads rather than moving the worker protocol itself upstream

### b. extend the current pipeline seam with richer input descriptors

recommended ownership:

- `burn_p2p_workload`
  - richer serializable pipeline/input descriptors
- `burn_p2p_dataloader`
  - reusable fetch/planning helpers
- `burn_p2p_browser`
  - browser-specific consumption of those descriptors where needed

recommended direction:

- keep `LeaseDataPipeline` as the main execution seam
- extend `LeaseDataPipelineDescriptor` or adjacent types for richer source
  description
- expose common helper constructors for:
  - inline records
  - http json records
  - prepared shard manifests over http
  - generated/synthetic recipes

better naming direction:

- `GeneratedWorkloadInputProvider`

not:

- a browser-locked trait name
- a second parallel pipeline system detached from `LeaseDataPipeline`

### c. add downstream-facing administration builders aligned with lifecycle plans

recommended ownership:

- `burn_p2p`
- `burn_p2p_experiment`
- `burn_p2p_bootstrap`

recommended scope:

- helper builders for the common "one family, few workloads, few experiments"
  case
- typed builders for:
  - `SupportedWorkload`
  - release/family manifests
  - experiment descriptors
  - lifecycle-plan publication
  - common staged revision rollout flows
- light wrappers for common assignment or schedule inputs used by custom hosted
  experiment products

important constraint:

- these helpers should target the current lifecycle-plan model
- they should not encourage downstream apps to treat directory entries as the
  authoritative cutover mechanism

### d. make `burn_p2p_app` reusable below the full portal shell

recommended work:

- extract reusable dioxus sections/widgets from the current portal shell
- move typed non-portal contracts into `burn_p2p_views`
- keep `burn_p2p_app` as the assembled reference product

candidate reusable surfaces:

- auth/session card
- browser capability/runtime card
- training result panel
- transport/network health panel
- experiment/revision selector
- contribution receipt summary
- lifecycle/assignment status card

### e. add a public downstream conformance surface

recommended ownership:

- a small public surface in `burn_p2p_testkit` only if it is intentionally made
  downstream-stable, or
- a new thin downstream-facing qa crate/harness if keeping `burn_p2p_testkit`
  internal remains the preferred policy

recommended scope:

- fixture helpers for authenticated browser session bootstrap
- workload training/validation command execution
- lifecycle-plan-driven experiment rollover
- reusable app snapshot assertions
- example-backed smoke harnesses for custom browser burn products

## what should stay downstream

the following should remain in `burn_dragon_p2p` or other downstream crates:

- dragon model config and BDH-specific logic
- language tokenization specifics
- cbp and other experiment/model-specific optimizer/runtime seams
- nca corpus generation logic
- climbmix-specific dataset semantics
- experiment-specific metrics and presentation language

upstream should provide seams, not absorb domain-specific training code.

## phased roadmap

### phase 1: shared execution payloads across native and browser

target crates:

- `burn_p2p_workload`
- `burn_p2p::burn`
- `burn_p2p_browser`
- `burn_p2p_views`

deliverables:

- move host-neutral training/validation budget/plan/result/progress types into
  `burn_p2p_workload`
- make `burn_p2p_browser` consume those shared contracts
- make `burn_p2p::burn` expose native execution against the same contracts
- keep browser trainer semantics WebGPU-first and product-oriented
- keep browser worker/bridge/runtime/session orchestration in
  `burn_p2p_browser`

success criteria:

- `burn_dragon_p2p` can delete most of its host-specific browser training-loop
  glue
- native and wasm code paths share the same training/result contract
- browser training remains explicitly WebGPU-oriented in the public surface
- shared execution payloads remain small enough that they do not become a
  second browser-runtime abstraction layer

### phase 2: richer pipeline descriptors and generated-input hooks

target crates:

- `burn_p2p_workload`
- `burn_p2p_dataloader`
- `burn_p2p_browser`
- `burn_p2p::burn`

deliverables:

- richer descriptors layered on the existing `LeaseDataPipeline` seam
- inline/http/generated input helper constructors
- generic generated-input trait or hook
- prepared-shard http fetch helper usable by browser and other local clients

success criteria:

- downstream no longer needs a custom token-source enum for the common cases
- nca generation stays downstream but plugs into an upstream trait
- the roadmap extends the existing data-pipeline seam rather than duplicating it

### phase 3: downstream administration builders

target crates:

- `burn_p2p`
- `burn_p2p_experiment`
- `burn_p2p_bootstrap`

deliverables:

- single-family and few-workload manifest helpers
- lifecycle-plan builders for staged revision rollouts
- common helpers for multi-experiment network reuse
- downstream-friendly wrappers for common hosted control flows

success criteria:

- downstream experiment administration becomes mostly configuration and builder
  calls, not hand-built control objects
- custom products align naturally with lifecycle-plan-driven change-over

### phase 4: reusable app/product surface

target crates:

- `burn_p2p_app`
- `burn_p2p_views`

deliverables:

- reusable dioxus sections/widgets
- typed training/runtime/lifecycle view models
- a cleaner split between the reference portal shell and reusable app pieces

success criteria:

- downstream apps can reuse upstream UI pieces without adopting the whole portal

### phase 5: public downstream verification surface

target crates:

- downstream-facing qa harness surface
- example-backed smoke tooling

deliverables:

- a narrow public conformance harness for browser burn apps
- auth/session, local training, network submission, and lifecycle rollover
  fixtures
- reusable snapshot assertions

success criteria:

- a custom browser burn app can validate integration without copying the mnist
  demo structure or depending directly on repo-internal testkit internals

## acceptance criteria for "dragon-ready" upstream surfaces

`burn_p2p` should be considered dragon-ready when:

- a downstream crate can support native burn training and browser burn training
  without owning a custom browser-local learner loop
- native and wasm training paths share upstream execution/result contracts
  rather than diverging into parallel downstream implementations
- browser training stays explicitly WebGPU-oriented in the product/runtime
  model instead of bending the public API around cpu fallback behavior
- shard-backed, inline/http, and generated browser training paths build on the
  shared pipeline/input seam
- github-required deployments are mostly config/builders rather than downstream
  auth-flow assembly
- downstream apps can publish staged lifecycle transitions without owning their
  own control-plane glue
- downstream apps can reuse auth/runtime/training panels from `burn_p2p_app`
  without adopting the full reference portal
- a downstream browser burn app has a supported smoke/conformance path for
  auth, local training, network submission, and experiment rollover

## recommended first implementation step

the first upstream step should still be:

1. move shared training/validation budget/plan/result/progress contracts into
   `burn_p2p_workload`
2. make `burn_p2p_browser` use those contracts instead of browser-owned-only
   variants
3. make `burn_p2p::burn` expose native execution against the same contracts
4. keep browser trainer semantics WebGPU-first and do not productize browser
   cpu training

that remains the highest-leverage change because it removes the biggest
structural duplication in `burn_dragon_p2p`, reduces the native/wasm split, and
improves the existing `burn_p2p*` surface instead of fragmenting it further.
