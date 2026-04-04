# burn_p2p Operator Runbook

## Scope

This runbook covers the bootstrap daemon and the named deployment profiles currently represented in
the repository.

Profiles:

- `trusted-minimal`
- `trusted-browser`
- `enterprise-sso`
- `community-web`

## Preflight

Before starting a deployment:

1. Confirm the network trust domain, project family, release train, and authority keys.
2. Confirm the binary was compiled with the features the deployment expects.
3. Check the `EdgeServiceManifest` output from `/capabilities` after boot.
4. Verify that requested services are both compiled and enabled in config.
5. Verify storage paths exist and have enough space for heads, receipts, and artifact chunks.

## Profile guidance

### trusted-minimal

Use this when:

- the environment is internal
- node certificates are pre-provisioned or issued through static/external trusted mode
- no portal enrollment or public browser ingress is desired

Compile/features:

- `admin-http`
- `metrics`
- `auth-static` or `auth-external`

Should not require:

- `burn_p2p_portal`
- `burn_p2p_social`
- `burn_p2p_browser`
- GitHub/OIDC/OAuth provider crates

### trusted-browser

Use this when:

- the environment is still internal
- the operator wants internal dashboards or controlled browser peers

Compile/features:

- `admin-http`
- `metrics`
- `portal`
- `browser-edge`
- one of `auth-static`, `auth-external`, or internal `auth-oidc`

### enterprise-sso

Use this when:

- the deployment is private and organization-backed
- OIDC and RBAC are required
- social may be private or disabled

Compile/features:

- `admin-http`
- `metrics`
- `portal`
- `rbac`
- `auth-oidc`

Optional:

- `browser-edge`
- `social`

### community-web

Use this when:

- the network is public or community-facing
- browser onboarding and public portal flows are required
- public social surfaces are enabled

Compile/features:

- `admin-http`
- `metrics`
- `portal`
- `browser-edge`
- `rbac`
- `auth-github`
- optional `auth-oidc`
- `social`

## Startup checklist

1. Start the daemon with the intended config profile.
2. Query `/capabilities`.
3. Confirm:
   - `portal_mode`
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
