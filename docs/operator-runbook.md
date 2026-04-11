# burn_p2p operator runbook

## scope

this runbook covers the bootstrap daemon and the deployment profiles currently
represented in the repo.

current deployment model:

- bootstrap nodes are cheap coherence seeds
- validator / authority nodes are separate workload-capable nodes
- trainer nodes are separate again and can scale to different hardware classes
- browser edge is an explicit deployment role, not the default bootstrap role

bootstrap nodes are cpu-and-network boxes, not gpu boxes. their job is to keep
the control plane coherent, surface browser-edge/http state, and help peers
discover one another.

they now act as relay-capable, rendezvous-capable, and kademlia-capable
coherence seeds for native peers. browser peers still join through the browser
edge rather than as direct libp2p peers.

for multi-edge admin/browser surfaces, bootstrap can also mirror operator read
state into redis by setting:

- `BURN_P2P_OPERATOR_STATE_REDIS_URL`
- optional `BURN_P2P_OPERATOR_STATE_KEY_PREFIX`

that mirror is intentionally a read-coherence aid for heads, receipts, merges,
peer-window metrics, reducer-cohort metrics, and head-eval reports. it is not
yet the final audited operator backend.

profiles:

- `reference split fleet`
- `trusted-minimal`
- `trusted-browser`
- `enterprise-sso`
- `community-web`

deployment assets in this repo:

- local containers and split compose stacks: `deploy/containers/` and
  `deploy/compose/`
- cloud reference stacks: `deploy/terraform/aws/` and `deploy/terraform/gcp/`
- one-command wrappers: `cargo xtask deploy compose ...`,
  `cargo xtask deploy aws ...`, and `cargo xtask deploy gcp ...`

## preflight

before starting a deployment:

1. confirm the network trust domain, project family, release train, and authority keys.
2. confirm the binary was compiled with the features the deployment expects.
3. check the `EdgeServiceManifest` output from `/capabilities` after boot.
4. verify that requested services are both compiled and enabled in config.
5. verify storage paths exist and have enough space for heads, receipts, and artifact chunks.
6. keep admin mutation routes private; the reference deploy assets no longer ship with inline admin secrets.
7. if more than one edge/admin surface should show the same operator state, point
   them at the same `BURN_P2P_OPERATOR_STATE_REDIS_URL`.

for bootstrap-only deployments also confirm:

1. the preset resolves to `CoherenceSeed`
2. no embedded runtime is configured
3. retention is lean and does not keep validator/archive-heavy state unless explicitly needed

## profile guidance

### reference split fleet

use this when:

- the goal is real native p2p training
- bootstrap, reducer, and validator roles should stay separate
- public ingress should terminate only on cheap bootstrap seeds
- validator and reducer nodes should stay private by default

this is the default shape represented by:

- `deploy/config/reference-bootstrap.json`
- `deploy/config/reference-validator.json`
- `deploy/config/reference-reducer.json`
- `deploy/compose/split-fleet.compose.yaml`
- `deploy/terraform/aws`
- `deploy/terraform/gcp`

### trusted-minimal

use this when:

- the environment is internal
- node certificates are pre-provisioned or issued through static/external trusted mode
- no portal enrollment or public browser ingress is desired
- the node should act as a cheap coherence seed, not a validator

this is a local/simple deployment shape, not the main internet-scale reference.

compile/features:

- `admin-http`
- `metrics`
- `auth-static` or `auth-external`

should not require:

- `burn_p2p_app`
- `burn_p2p_social`
- `burn_p2p_browser`
- provider crates for github, oidc, or oauth

### trusted-browser

use this when:

- the environment is still internal
- the operator wants internal dashboards or controlled browser peers
- bootstrap/coherence and browser-edge http should be colocated

compile/features:

- `admin-http`
- `metrics`
- `browser-edge`
- `browser-join`
- one of `auth-static`, `auth-external`, or internal `auth-oidc`

### enterprise-sso

use this when:

- the deployment is private and organization-backed
- oidc and rbac are required
- social may be private or disabled
- validation and authority duties are intentionally separate from cheap bootstrap seeds

compile/features:

- `admin-http`
- `metrics`
- `rbac`
- `auth-oidc`

optional:

- `browser-edge`
- `browser-join`
- `social`

oidc guidance:

- prefer the standard token flow over custom exchange shims when your IdP supports it
- set `client_id`, `redirect_uri`, and either `jwks_url` or a discoverable issuer
- prefer `session_state_backend.kind = "redis"` if more than one browser-edge
  process may serve the same auth surface; `session_state_path` remains as the
  file-backed compatibility path for single-edge or shared-volume deployments
- leave `persist_provider_tokens = false` unless you intentionally accept
  upstream bearer/session material at rest in the shared auth backend
- the browser-edge auth flow now binds login state with random state + oidc nonce and
  uses pkce for the standard authorization-code path
- `id_token` validation is jwks-backed rather than being treated as an opaque blob
- principal records can still match explicit identities with `provider_subject`,
  `provider_login`, or `provider_email`, and can now also require
  `provider_groups`, `provider_orgs`, or `provider_claim:<name>` values in
  `custom_claims`
- the bootstrap edge persists both issued sessions and pending login state, so
  callback completion and refresh/logout can survive restart and can be shared
  across multiple edges that point at the same session backend

github community guidance:

- prefer the standard github oauth token flow with `token_url =
  "https://github.com/login/oauth/access_token"` and `api_base_url =
  "https://api.github.com"`
- use dynamic principal rules such as `provider_orgs`,
  `provider_groups`, and `provider_repo_access` instead of hand-maintained
  principal allowlists
- enrollment now rehydrates upstream github claims before certificate issuance,
  so org/team/collaborator changes can revoke browser admission on the next
  enroll/refresh cycle
- issued node certificates now carry an `AuthPolicySnapshot`, which preserves
  the matched github claims, granted roles/scopes, and observed provider
  identity inside the signed certificate claims

external trusted-ingress guidance:

- use `auth-external` when corporate sso is already terminated by a trusted internal gateway
- keep `trusted_internal_only = true`
- treat the upstream proxy/header boundary as the security boundary for that mode
- bind bootstrap http to loopback or a private interface and put the trusted gateway in front
- do not expose raw browser-edge http publicly when `auth-external` is active

browser session guidance:

- browser-edge currently uses `x-session-id` as a bearer-style session header
- that means tls and origin hardening belong at the http ingress layer
- do not treat the reference bootstrap http listener as a public-internet auth frontend by itself

### community-web

use this when:

- the network is public or community-facing
- browser onboarding and public portal flows are required
- public social surfaces are enabled
- multiple bootstrap/coherence seeds should be deployed for ingress resilience

## recommended auth models

use these as the default starting points for real deployments.

### 1. native-only private cluster

preferred model:

- use `trusted-minimal`
- prefer pre-provisioned node certificates, `auth-static`, or `auth-external`
- keep browser-edge and browser-join disabled unless the cluster truly needs an http portal
- treat bootstrap as trust/discovery/certificate infrastructure, not as a full interactive login app

why:

- native peers usually do not need browser-style pending-login and session state
- static or trusted-ingress enrollment keeps the deployment simpler and easier to harden
- this is the cleanest fit for internal gpu fleets, fixed trainer pools, and validator/reducer nodes

operational note:

- if certificates are issued out of band or through trusted internal enrollment, a shared auth-session backend is not required

### 2. mixed native + browser corporate deployment

preferred model:

- use `enterprise-sso` when bootstrap is the browser-facing relying party
- use `trusted-browser` plus `auth-external` when corporate sso is already terminated by a trusted internal gateway
- native peers should still prefer pre-provisioned or tightly controlled certificate enrollment
- browser peers should use edge-backed login plus enrollment

why:

- native peers do not benefit much from a browser-style callback/session flow
- browser peers do need an interactive auth surface and a local burn session before enrollment
- separating those concerns keeps native fleet operations simpler while still allowing controlled browser participation

operational note:

- use `session_state_path` for a single browser-edge or tightly controlled shared-volume setup
- prefer `session_state_backend.kind = "redis"` when more than one edge instance may serve login, callback, refresh, or logout for the same auth surface

### 3. public/community browser network

preferred model:

- use `community-web`
- require explicit provider-backed login and edge-issued node certificates for contributing browser peers
- allow public read/download surfaces only where the publication policy is intentionally public
- deploy multiple bootstrap/coherence seeds for ingress resilience

why:

- public browser participation still needs identity, authorization, and revocation boundaries
- anonymous artifact viewing or download can be acceptable, but anonymous contribution is a very different and much riskier model
- the current browser path is designed around authenticated browser enrollment, not around arbitrary anonymous contribution

operational note:

- public/community should not be interpreted as unauthenticated peer contribution by default
- contributor browsers should still authenticate, receive a local burn session, and enroll for a node certificate before they submit work

## open admission and unauthenticated networks

the runtime can be configured without an admission policy, which means native
peers are not forced through certificate-backed peer admission.

this is acceptable for:

- lab networks
- local/dev clusters
- trusted hobby or research meshes where open membership is the point

this is not the recommended model for serious public or enterprise deployments
because it removes the strongest join and revocation boundary. in practice that
means:

- no meaningful sybil resistance
- weaker abuse handling
- weaker accountability for bad updates
- more pressure on downstream validator robustness and reputation logic

current recommendation:

- open native membership is possible
- authenticated browser contribution is the first-class browser path
- fully anonymous browser contribution should be treated as future or experimental work, not as the production default

## bootstrap topology guidance

recommended shape:

- 2-3 cheap bootstrap/coherence seed nodes per network
- separate reducer nodes
- separate authority/validator nodes
- separate trainer pools
- optional archive nodes when long-lived artifact storage matters

expected connection dynamics:

- many peers may contact a bootstrap seed first
- peers then exchange peer-directory information over the control plane
- peers preferentially fan out toward non-bootstrap peers
- once a mesh exists, bootstrap nodes should stop being the permanent hub for every connection
- public internet exposure should usually stop at the bootstrap seed layer unless the deployment explicitly serves browser edge

current discovery model:

- bootstrap helps peers discover each other via libp2p rendezvous, network-scoped Kademlia, and
  control-plane peer-directory announcements
- each peer republishes its own peer-directory advertisement on a short heartbeat, not only at first listen
- stale peer-directory advertisements are ignored for mesh repair after a bounded freshness window
- peers remember known addresses and redial to maintain a small mesh
- when a node is under-connected and has no fresh non-bootstrap repair targets, it falls back to bootstrap seeds to rediscover the mesh
- native peers register their currently reachable addresses with rendezvous-capable seeds under the network namespace
- other native peers discover those registrations through the same seeds, then feed the resulting addresses into the normal repair mesh
- native peers also seed Kademlia with identified or rendezvous-learned addresses, so later native lookups can rediscover peers through the DHT instead of relying only on the original seed session
- relay-capable bootstrap or relayhelper nodes accept libp2p relay reservations from native peers
- private/native peers can register relay-backed reachable addresses through rendezvous and advertise them through the peer-directory path
- once two native peers meet through a relay path, DCUtR attempts a direct upgrade so later repair dials can prefer the direct route
- autonat observations feed reachable-address confirmation and expiry, so stale external addresses stop being treated as good repair targets
- once a peer has reached its non-bootstrap mesh target, it proactively disconnects steady-state bootstrap seed sessions instead of pinning every node to a seed forever
- if that mesh later thins out, connectivity repair falls back to bootstrap seeds again for rediscovery
- reducers publish aggregate proposals for validators to attest against
- validators can redial artifact providers from that learned directory

current non-goals:

- no browser-native libp2p relay client path yet

so:

- bootstrap nodes should stay relatively cheap
- relay/rendezvous/Kademlia-capable seeds are still most important for ingress, discovery, and fallback connectivity, not as a permanent traffic hub
- once peers learn direct routes, repair dialing prefers those direct addresses before relay-backed addresses, and healthy peers now drop steady-state seed sessions after the non-bootstrap mesh target is met
- very large or hostile deployments should still use multiple seeds
- future large-scale work would still benefit from broader browser-native peer transport and stronger scheduling/placement controls

## shard assignment coordination

current training coordination model:

- each trainer plans a lease for the active experiment window against the same shared lease overlay
- before planning a new lease, the trainer merges lease announcements learned from connected peers
- active same-window leases from other peers are treated as reserved, so already-announced microshards are avoided when alternatives exist
- if every microshard is already leased, the runtime falls back to the full set rather than stalling forever
- deterministic peer ordering then biases each peer toward a different preferred subset, which reduces collisions even before a lease is published
- once a lease is chosen, the trainer publishes the lease announcement immediately and persists it locally before training starts

operationally this means:

- shard coordination is optimistic and control-plane driven, not centrally scheduled
- collisions are reduced by visibility plus deterministic peer preference, not by a locking service
- late or partitioned peers can still collide in degraded conditions, but same-window healthy peers should spread out across the available microshards

## containers and one-command entrypoints

the deployment directory now splits images by role:

- `deploy/containers/bootstrap.Dockerfile`
  builds the publishable `burn-p2p-bootstrap` binary for cheap bootstrap or
  reducer/validator service containers
- `deploy/containers/node-native.Dockerfile`
  builds a downstream native app binary for CPU trainer pools
- `deploy/containers/node-cuda.Dockerfile`
  builds a downstream app binary into an NVIDIA CUDA runtime image for GPU
  trainer pools

local entrypoints:

```bash
cargo xtask deploy compose --stack bootstrap-edge --action up
cargo xtask deploy compose --stack split-fleet --env-file deploy/compose/split-fleet.env --profile-name trainers --action up
```

cloud entrypoints:

```bash
cargo xtask deploy aws --action plan --var-file deploy/terraform/aws/reference.tfvars.example --bootstrap-image <image> --reducer-image <image> --validator-image <image> --trainer-image <image>
cargo xtask deploy gcp --action plan --var-file deploy/terraform/gcp/reference.tfvars.example --bootstrap-image <image> --reducer-image <image> --validator-image <image> --trainer-image <image>
```

the cloud wrappers inject the repo-owned bootstrap, reducer, and validator json
configs by default. the bootstrap default is now the pure
`reference-bootstrap.json` seed profile. trainer images remain downstream-app
specific because workload code does not live in this crate graph.

the reference cloud stacks also keep validator, reducer, and trainer roles
private by default. only bootstrap swarm ingress is public unless the operator
explicitly widens the bootstrap http surface for browser edge.

compile/features:

- `admin-http`
- `metrics`
- `browser-edge`
- `browser-join`
- `rbac`
- `auth-github`
- optional `auth-oidc`
- `social`

## startup checklist

1. start the daemon with the intended config profile.
2. query `/capabilities`.
3. confirm:
   - `app_mode`
   - `browser_mode`
   - `available_auth_providers`
   - `social_mode`
   - `compiled_feature_set`
   - `active_feature_set`
4. query `/status` and `/portal/snapshot`.
5. confirm the release-train and project-family invariants match the intended deployment.

## operator checks during runtime

monitor:

- connected peers
- admitted vs rejected peers
- minimum revocation epoch
- receipt counts
- certified merge counts
- reducer load
- diagnostic bundle freshness

routes worth checking:

- `/status`
- `/events`
- `/heads`
- `/receipts`
- `/reducers/load`
- `/trust`
- `/reenrollment`
- `/capabilities`

## rollout and rollback

### release-train rollout

1. publish the new release-train manifest and target artifact set.
2. confirm authority policy matches the new train.
3. roll out bootstrap/edge nodes first.
4. roll out native nodes and browser clients for approved targets.
5. watch `/capabilities`, admission logs, and revocation/trust bundle state.

### rollback

1. stop issuing new certificates for the bad train.
2. roll back bootstrap/edge binaries and config.
3. restore the previous release-train manifest.
4. raise the minimum revocation epoch or trusted issuer set only if required by the incident.
5. re-check head sync, receipt submission, and portal snapshot correctness.

## incident handling

### auth outage

- disable interactive enrollment in config if the provider is unavailable.
- keep pre-provisioned and already-admitted nodes running.
- expose status in operator communications and portal text if public-facing.

### revocation event

- raise `minimum_revocation_epoch`.
- verify `/trust` and `/reenrollment` reflect the new epoch.
- expect affected sessions and certificates to require reenrollment.

### browser ingress overload

- temporarily reduce browser mode from `trainer` to `verifier` or `observer`.
- lower portal event/sample rates.
- increase reducer and upload budgets only if the storage/network path can absorb it.

### head or merge instability

- inspect `/heads`, `/receipts`, and reducer load.
- check lag state and catch-up policy.
- if required, use the admin surface to freeze rollout changes before widening the incident.
