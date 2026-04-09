# burn_p2p Operator Runbook

## Scope

This runbook covers the bootstrap daemon and the named deployment profiles currently represented in
the repository.

Current deployment model:

- bootstrap nodes are cheap coherence seeds
- validator / authority nodes are separate workload-capable nodes
- trainer nodes are separate again and can scale to different hardware classes

Bootstrap nodes are intended to be CPU-and-network boxes, not GPU boxes. Their job is to keep the
control plane coherent, surface browser-edge/http state, and help peers discover one another.
They now act as relay-capable, rendezvous-capable, Kademlia-capable coherence seeds for native
peers, while browser peers still join through the browser edge rather than as direct libp2p peers.

Profiles:

- `trusted-minimal`
- `trusted-browser`
- `enterprise-sso`
- `community-web`

Deployment assets in this repo:

- local containers and split compose stacks: `deploy/containers/` and
  `deploy/compose/`
- cloud reference stacks: `deploy/terraform/aws/` and `deploy/terraform/gcp/`
- one-command wrappers: `cargo xtask deploy compose ...`,
  `cargo xtask deploy aws ...`, and `cargo xtask deploy gcp ...`

## Preflight

Before starting a deployment:

1. Confirm the network trust domain, project family, release train, and authority keys.
2. Confirm the binary was compiled with the features the deployment expects.
3. Check the `EdgeServiceManifest` output from `/capabilities` after boot.
4. Verify that requested services are both compiled and enabled in config.
5. Verify storage paths exist and have enough space for heads, receipts, and artifact chunks.

For bootstrap-only deployments also confirm:

1. the preset resolves to `CoherenceSeed`
2. no embedded runtime is configured
3. retention is lean and does not keep validator/archive-heavy state unless explicitly needed

## Profile guidance

### trusted-minimal

Use this when:

- the environment is internal
- node certificates are pre-provisioned or issued through static/external trusted mode
- no portal enrollment or public browser ingress is desired
- the node should act as a cheap coherence seed, not a validator

Compile/features:

- `admin-http`
- `metrics`
- `auth-static` or `auth-external`

Should not require:

- `burn_p2p_app`
- `burn_p2p_social`
- `burn_p2p_browser`
- GitHub/OIDC/OAuth provider crates

### trusted-browser

Use this when:

- the environment is still internal
- the operator wants internal dashboards or controlled browser peers
- bootstrap/coherence and browser-edge http should be colocated

Compile/features:

- `admin-http`
- `metrics`
- `browser-edge`
- `browser-join`
- one of `auth-static`, `auth-external`, or internal `auth-oidc`

### enterprise-sso

Use this when:

- the deployment is private and organization-backed
- OIDC and RBAC are required
- social may be private or disabled
- validation and authority duties are intentionally separate from cheap bootstrap seeds

Compile/features:

- `admin-http`
- `metrics`
- `rbac`
- `auth-oidc`

Optional:

- `browser-edge`
- `browser-join`
- `social`

OIDC guidance:

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

External trusted-ingress guidance:

- use `auth-external` when corporate sso is already terminated by a trusted internal gateway
- keep `trusted_internal_only = true`
- treat the upstream proxy/header boundary as the security boundary for that mode

### community-web

Use this when:

- the network is public or community-facing
- browser onboarding and public portal flows are required
- public social surfaces are enabled
- multiple bootstrap/coherence seeds should be deployed for ingress resilience

## Recommended auth models

Use these as the default starting points for real deployments.

### 1. native-only private cluster

Preferred model:

- use `trusted-minimal`
- prefer pre-provisioned node certificates, `auth-static`, or `auth-external`
- keep browser-edge and browser-join disabled unless the cluster truly needs an http portal
- treat bootstrap as trust/discovery/certificate infrastructure, not as a full interactive login app

Why:

- native peers usually do not need browser-style pending-login and session state
- static or trusted-ingress enrollment keeps the deployment simpler and easier to harden
- this is the cleanest fit for internal gpu fleets, fixed trainer pools, and validator/reducer nodes

Operational note:

- if certificates are issued out of band or through trusted internal enrollment, a shared auth-session backend is not required

### 2. mixed native + browser corporate deployment

Preferred model:

- use `enterprise-sso` when bootstrap is the browser-facing relying party
- use `trusted-browser` plus `auth-external` when corporate sso is already terminated by a trusted internal gateway
- native peers should still prefer pre-provisioned or tightly controlled certificate enrollment
- browser peers should use edge-backed login plus enrollment

Why:

- native peers do not benefit much from a browser-style callback/session flow
- browser peers do need an interactive auth surface and a local burn session before enrollment
- separating those concerns keeps native fleet operations simpler while still allowing controlled browser participation

Operational note:

- use `session_state_path` for a single browser-edge or tightly controlled shared-volume setup
- prefer `session_state_backend.kind = "redis"` when more than one edge instance may serve login, callback, refresh, or logout for the same auth surface

### 3. public/community browser network

Preferred model:

- use `community-web`
- require explicit provider-backed login and edge-issued node certificates for contributing browser peers
- allow public read/download surfaces only where the publication policy is intentionally public
- deploy multiple bootstrap/coherence seeds for ingress resilience

Why:

- public browser participation still needs identity, authorization, and revocation boundaries
- anonymous artifact viewing or download can be acceptable, but anonymous contribution is a very different and much riskier model
- the current browser path is designed around authenticated browser enrollment, not around arbitrary anonymous contribution

Operational note:

- public/community should not be interpreted as unauthenticated peer contribution by default
- contributor browsers should still authenticate, receive a local burn session, and enroll for a node certificate before they submit work

## Open admission and unauthenticated networks

The runtime can be configured without an admission policy, which means native peers are not forced through certificate-backed peer admission.

This is acceptable for:

- lab networks
- local/dev clusters
- trusted hobby or research meshes where open membership is the point

This is not the recommended model for serious public or enterprise deployments because it removes the strongest join and revocation boundary. In practice that means:

- no meaningful sybil resistance
- weaker abuse handling
- weaker accountability for bad updates
- more pressure on downstream validator robustness and reputation logic

Current recommendation:

- open native membership is possible
- authenticated browser contribution is the first-class browser path
- fully anonymous browser contribution should be treated as future or experimental work, not as the production default

## Bootstrap topology guidance

Recommended shape:

- 2-3 cheap bootstrap/coherence seed nodes per network
- separate reducer nodes
- separate authority/validator nodes
- separate trainer pools
- optional archive nodes when long-lived artifact storage matters

Expected connection dynamics:

- many peers may contact a bootstrap seed first
- peers then exchange peer-directory information over the control plane
- peers preferentially fan out toward non-bootstrap peers
- once a mesh exists, bootstrap nodes should stop being the permanent hub for every connection

Current discovery model:

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
- AutoNAT observations feed reachable-address confirmation and expiry, so stale external addresses stop being treated as good repair targets
- once a peer has reached its non-bootstrap mesh target, it proactively disconnects steady-state bootstrap seed sessions instead of pinning every node to a seed forever
- if that mesh later thins out, connectivity repair falls back to bootstrap seeds again for rediscovery
- reducers publish aggregate proposals for validators to attest against
- validators can redial artifact providers from that learned directory

Current non-goals:

- no browser-native libp2p relay client path yet

So:

- bootstrap nodes should stay relatively cheap
- relay/rendezvous/Kademlia-capable seeds are still most important for ingress, discovery, and fallback connectivity, not as a permanent traffic hub
- once peers learn direct routes, repair dialing prefers those direct addresses before relay-backed addresses, and healthy peers now drop steady-state seed sessions after the non-bootstrap mesh target is met
- very large or hostile deployments should still use multiple seeds
- future large-scale work would still benefit from broader browser-native peer transport and stronger scheduling/placement controls

## Shard assignment coordination

Current training coordination model:

- each trainer plans a lease for the active experiment window against the same shared lease overlay
- before planning a new lease, the trainer merges lease announcements learned from connected peers
- active same-window leases from other peers are treated as reserved, so already-announced microshards are avoided when alternatives exist
- if every microshard is already leased, the runtime falls back to the full set rather than stalling forever
- deterministic peer ordering then biases each peer toward a different preferred subset, which reduces collisions even before a lease is published
- once a lease is chosen, the trainer publishes the lease announcement immediately and persists it locally before training starts

Operationally this means:

- shard coordination is optimistic and control-plane driven, not centrally scheduled
- collisions are reduced by visibility plus deterministic peer preference, not by a locking service
- late or partitioned peers can still collide in degraded conditions, but same-window healthy peers should spread out across the available microshards

## Containers and one-command entrypoints

The deployment directory now splits images by role:

- `deploy/containers/bootstrap.Dockerfile`
  builds the publishable `burn-p2p-bootstrap` binary for cheap bootstrap or
  reducer/validator service containers
- `deploy/containers/node-native.Dockerfile`
  builds a downstream native app binary for CPU trainer pools
- `deploy/containers/node-cuda.Dockerfile`
  builds a downstream app binary into an NVIDIA CUDA runtime image for GPU
  trainer pools

Local entrypoints:

```bash
cargo xtask deploy compose --stack bootstrap-edge --action up
cargo xtask deploy compose --stack split-fleet --env-file deploy/compose/split-fleet.env --profile-name trainers --action up
```

Cloud entrypoints:

```bash
cargo xtask deploy aws --action plan --var-file deploy/terraform/aws/reference.tfvars.example --bootstrap-image <image> --reducer-image <image> --validator-image <image> --trainer-image <image>
cargo xtask deploy gcp --action plan --var-file deploy/terraform/gcp/reference.tfvars.example --bootstrap-image <image> --reducer-image <image> --validator-image <image> --trainer-image <image>
```

The cloud wrappers inject the repo-owned bootstrap, reducer, and validator JSON
configs by default. Trainer images remain downstream-app specific because
workload code does not live in this crate graph.

Compile/features:

- `admin-http`
- `metrics`
- `browser-edge`
- `browser-join`
- `rbac`
- `auth-github`
- optional `auth-oidc`
- `social`

## Startup checklist

1. Start the daemon with the intended config profile.
2. Query `/capabilities`.
3. Confirm:
   - `app_mode`
   - `browser_mode`
   - `available_auth_providers`
   - `social_mode`
   - `compiled_feature_set`
   - `active_feature_set`
4. Query `/status` and `/portal/snapshot`.
5. Confirm the release-train and project-family invariants match the intended deployment.

## Operator checks during runtime

Monitor:

- connected peers
- admitted vs rejected peers
- minimum revocation epoch
- receipt counts
- certified merge counts
- reducer load
- diagnostic bundle freshness

Routes worth checking:

- `/status`
- `/events`
- `/heads`
- `/receipts`
- `/reducers/load`
- `/trust`
- `/reenrollment`
- `/capabilities`

## Rollout and rollback

### Release-train rollout

1. Publish the new release-train manifest and target artifact set.
2. Confirm authority policy matches the new train.
3. Roll out bootstrap/edge nodes first.
4. Roll out native nodes and browser clients for approved targets.
5. Watch `/capabilities`, admission logs, and revocation/trust bundle state.

### Rollback

1. Stop issuing new certificates for the bad train.
2. Roll back bootstrap/edge binaries and config.
3. Restore the previous release-train manifest.
4. Raise the minimum revocation epoch or trusted issuer set only if required by the incident.
5. Re-check head sync, receipt submission, and portal snapshot correctness.

## Incident handling

### Auth outage

- Disable interactive enrollment in config if the provider is unavailable.
- Keep pre-provisioned and already-admitted nodes running.
- Expose status in operator communications and portal text if public-facing.

### Revocation event

- Raise `minimum_revocation_epoch`.
- Verify `/trust` and `/reenrollment` reflect the new epoch.
- Expect affected sessions and certificates to require reenrollment.

### Browser ingress overload

- Temporarily reduce browser mode from `trainer` to `verifier` or `observer`.
- Lower portal event/sample rates.
- Increase reducer and upload budgets only if the storage/network path can absorb it.

### Head or merge instability

- Inspect `/heads`, `/receipts`, and reducer load.
- Check lag state and catch-up policy.
- If required, use the admin surface to freeze rollout changes before widening the incident.
