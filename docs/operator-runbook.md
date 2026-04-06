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
They are not currently first-class libp2p relay, rendezvous, or kademlia servers.

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

### community-web

Use this when:

- the network is public or community-facing
- browser onboarding and public portal flows are required
- public social surfaces are enabled
- multiple bootstrap/coherence seeds should be deployed for ingress resilience

## Bootstrap topology guidance

Recommended shape:

- 2-3 cheap bootstrap/coherence seed nodes per network
- separate authority/validator nodes
- separate trainer pools
- optional archive nodes when long-lived artifact storage matters

Expected connection dynamics:

- many peers may contact a bootstrap seed first
- peers then exchange peer-directory information over the control plane
- peers preferentially fan out toward non-bootstrap peers
- once a mesh exists, bootstrap nodes should stop being the permanent hub for every connection

Current discovery model:

- bootstrap helps peers discover each other via control-plane peer-directory announcements
- peers remember known addresses and redial to maintain a small mesh
- validators can redial artifact providers from that learned directory

Current non-goals:

- no first-class libp2p kademlia dht
- no first-class libp2p rendezvous service
- no first-class libp2p relay reservation layer

So:

- bootstrap nodes should stay relatively cheap
- very large or hostile deployments should use multiple seeds
- future large-scale work would still benefit from a dedicated discovery / nat-traversal tier

## Containers and one-command entrypoints

The deployment directory now splits images by role:

- `deploy/containers/bootstrap.Dockerfile`
  builds the publishable `burn-p2p-bootstrap` binary for cheap bootstrap or
  validator service containers
- `deploy/containers/node-native.Dockerfile`
  builds a downstream native app binary for CPU validator/trainer pools
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
cargo xtask deploy aws --action plan --var-file deploy/terraform/aws/reference.tfvars.example --bootstrap-image <image> --validator-image <image> --trainer-image <image>
cargo xtask deploy gcp --action plan --var-file deploy/terraform/gcp/reference.tfvars.example --bootstrap-image <image> --validator-image <image> --trainer-image <image>
```

The cloud wrappers inject the repo-owned bootstrap and validator JSON configs by
default. Trainer images remain downstream-app specific because workload code
does not live in this crate graph.

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
