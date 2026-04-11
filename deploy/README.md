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
- principal admission is expressed with `provider_orgs`,
  `provider_groups`, and `provider_repo_access`
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

pure split fleet:

```bash
cp deploy/compose/split-fleet.env.example deploy/compose/split-fleet.env
docker compose \
  --env-file deploy/compose/split-fleet.env \
  -f deploy/compose/split-fleet.compose.yaml \
  up --build
```

the split fleet starts:

- one bootstrap/coherence seed
- one separate reducer
- one separate validator/authority
- optional trainer containers under the `trainers` profile

the single-bootstrap compose files share one base service definition in
`deploy/compose/bootstrap-base.compose.yaml` so the hardening and container
shape stay aligned across profiles.

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
