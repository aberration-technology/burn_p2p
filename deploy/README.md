# deployment

this directory is the deployment source of truth:

- `containers/`: role-specific container build recipes
- `config/`: bootstrap, reducer, and validator daemon configs
- `compose/`: local and single-host reference stacks
- `terraform/`: cloud reference stacks for aws and gcp

the deployment model is intentionally split:

- bootstrap nodes are cheap coherence seeds
- reducers are separate proposal builders
- validators are separate authority nodes
- trainers are separate workload-specific nodes
- browser edge is optional and should only exist on stacks that actually need it

bootstrap configs support `${ENV_VAR}` and `${ENV_VAR:-default}` placeholders.
use that for provider client secrets, redis urls, redirect uris, and other
deployment-specific values instead of checking secrets into json.

## reference regimes

use these as the main starting points:

- `reference-bootstrap.json`: pure seed node, no browser edge, no inline admin secret
- `reference-validator.json`: validator/authority node for the reference split fleet
- `reference-reducer.json`: reducer node for the reference split fleet
- `trusted-browser.json`: private browser edge behind trusted ingress
- `enterprise-sso.json`: oidc-backed browser edge
- `community-web.json`: public/community browser edge

`community-web.json` now models live github-governed admission rather than a
hand-authored user allowlist:

- github oauth authorize/token endpoints point at the public github surface
- `api_base_url` points at `https://api.github.com`
- principal admission is expressed with `provider_policy.github.rules`
  requiring org, team, and repo-collaborator access
- bootstrap rehydrates provider claims before certificate issuance, so github
  membership changes can revoke future enrollments

`trusted-minimal.json` is the smallest local native bootstrap profile. it is useful
for local or tightly controlled deployments, but it is not the internet-scale
reference shape.

## secure defaults

the reference assets now bias toward the safer shape:

- split-fleet bootstrap is a pure seed, not a browser-edge node
- validator and reducer configs do not ship with inline admin tokens
- cloud reference stacks expose only bootstrap swarm ingress publicly by default
- validator, reducer, and trainer nodes stay private by default in cloud stacks
- browser/http ingress is only part of the browser-oriented profiles

that means:

- use the split fleet when you want a clean native p2p training baseline
- use `trusted-browser`, `enterprise-sso`, or `community-web` only when you actually need browser ingress
- treat admin mutation routes as private operator surfaces, not public internet surfaces

## local compose

bootstrap/browser-edge profiles:

```bash
docker compose -f deploy/compose/trusted-browser.compose.yaml up --build
docker compose -f deploy/compose/enterprise-sso.compose.yaml up --build
docker compose -f deploy/compose/community-web.compose.yaml up --build
```

shared operator-state and browser-edge backing services:

```bash
docker compose -f deploy/compose/operator-state.compose.yaml up -d
```

that module packages:

- redis for browser-edge auth session state
- postgres as the reference shared operator-state backend

reference artifact-publication backing service:

```bash
docker compose -f deploy/compose/artifact-storage.compose.yaml up -d
```

that module packages:

- minio as the reference s3-compatible warm artifact store
- one bucket-init sidecar so the reference bucket exists before export jobs run

pure split fleet:

```bash
cp deploy/compose/split-fleet.env.example deploy/compose/split-fleet.env
docker compose \
  --env-file deploy/compose/split-fleet.env \
  -f deploy/compose/split-fleet.compose.yaml \
  up --build
```

the split fleet starts:

- one local postgres operator-state backend
- one local redis backing service for optional browser-edge/session flows
- one bootstrap/coherence seed
- one separate reducer
- one separate validator/authority
- optional trainer containers under the `trainers` profile

the single-bootstrap compose files share one base service definition in
`deploy/compose/bootstrap-base.compose.yaml` so the hardening and container
shape stay aligned across profiles.

the split-fleet env example now includes:

- shared operator-state postgres wiring
- browser-edge auth session redis wiring
- github / oidc client secret placeholders
- packaged postgres module defaults for the shared operator backend
- packaged minio / s3-compatible artifact-publication defaults

the compose browser-edge stacks bind `8787` to `127.0.0.1` on the host by
default. if you want internet or corp-lan browser ingress, put a real reverse
proxy / tls terminator in front instead of publishing raw bootstrap http
directly.

## cloud

`terraform/` contains split-fleet reference stacks for:

- `terraform/aws`
- `terraform/gcp`

the default cloud posture is:

- bootstrap nodes can be public
- validators, reducers, and trainers are private by default
- internal fleet ports are allowed only inside the fleet
- public bootstrap ingress is limited to swarm ports unless you opt into more

if you want browser edge on the public internet, that is an explicit profile
change. add the browser-edge config and widen only the bootstrap http ports you
actually intend to expose.

for `auth-external`, do not expose bootstrap http directly. that mode trusts an
upstream identity header, so the proxy / ingress layer is part of the security
boundary.

the cloud stacks do not provision managed redis for browser-edge auth session
state. for multi-edge oidc/external auth deployments, point
`session_state_backend.url` at an external redis service such as elasticache or
memorystore.

for production object-store publication, replace the reference minio module with
managed s3 / gcs-compatible storage and set the `BURN_P2P_ARTIFACT_S3_*`
variables accordingly.

## artifact storage policy

the reference configs now carry an explicit two-tier artifact-publication policy
through `artifact_publication.targets`:

- `local-default` is the hot local cache used for low-latency edge streaming and
  authenticated on-demand export
- `warm-s3` is the warm object-store mirror used for larger or longer-lived
  publication with signed-url delivery

recommended interpretation:

- hot: local filesystem under the bootstrap publication root for short-lived
  active heads and manifest bundles
- warm: s3-compatible object store for canonical checkpoints, browser snapshots,
  and operator-requested exports
- cold: long-term archival, compliance, and retention workflows outside the live
  bootstrap process

the repo does not yet implement a full cold-archive lifecycle manager. the
important improvement here is that the deployment/config surface now declares
the hot and warm tiers directly instead of leaving them implicit.

## authority governance workflow

the repo now has first-class authority manifest types, and the deployment
workflow should treat them as the control-plane source of truth:

1. build the next validator set and authority epoch manifests offline
2. review quorum weights, network ids, and release-train compatibility
3. distribute the new manifests with the bootstrap/reducer/validator rollout
4. rotate issuer material and trusted-issuer lists together with the new epoch
5. advance revocation policy when retiring validators or browser issuers
6. keep bootstrap admin/export surfaces private while the rollout is in flight

what is still not finished is a fully automated byzantine-grade governance
pipeline. the important part for the current repo state is that the workflow is
now explicit and grounded in manifest-backed control-plane types rather than
informal operator convention.
