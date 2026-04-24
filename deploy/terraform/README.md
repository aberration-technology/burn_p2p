# terraform

these stacks are intentionally simple reference deployments:

- `aws/`: ec2 split fleet
- `gcp/`: compute engine split fleet

the reference topology is:

- public bootstrap/coherence seed nodes
- private validator nodes
- private reducer nodes
- optional private trainer nodes

that is the default because it maps better to real p2p training:

- bootstrap nodes are the ingress/discovery surface
- validators and reducers are control-plane authorities, not public internet services
- trainers usually belong on private worker subnets or internal corp networks

## what the stacks do

they:

1. create separate bootstrap, validator, reducer, and optional trainer hosts
2. write the selected config json onto each host
3. install docker and run the requested container image

they do not:

- build images in the cloud
- provision a managed redis session backend
- provision a managed postgres operator-state backend
- provision managed object storage for artifact publication

## default network posture

the defaults are intentionally narrow:

- bootstrap swarm tcp/udp ports are public
- validator/reducer/trainer hosts do not get public ips by default
- fleet tcp/udp ports are opened only between members of the fleet
- ssh remains opt-in through explicit source ranges

if you want public browser edge or public http metrics, widen only the bootstrap
public tcp ports you actually need.

## expected flow

1. build and push the bootstrap image
2. build and push the reducer and validator images, or reuse the bootstrap image
3. build and push the downstream trainer image
4. point terraform at those image uris
5. let `cargo xtask deploy aws ...` or `cargo xtask deploy gcp ...` inject the
   selected config json from `deploy/config/`

example:

```bash
cargo xtask deploy aws \
  --action plan \
  --var-file deploy/terraform/aws/reference.tfvars.example \
  --bootstrap-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.37 \
  --reducer-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.37 \
  --validator-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.37 \
  --trainer-image ghcr.io/acme/my-burn-node:latest
```

for browser-edge oidc/external auth in ha mode, point
`session_state_backend.url` at a managed redis service such as elasticache or
memorystore.

the current deploy configs also expect container environment for:

- `BURN_P2P_OPERATOR_STATE_POSTGRES_URL` and optional
  `BURN_P2P_OPERATOR_STATE_POSTGRES_TABLE`
- browser-edge auth secrets such as `BURN_P2P_GITHUB_CLIENT_SECRET` or
  `BURN_P2P_OIDC_CLIENT_SECRET`
- artifact publication variables such as `BURN_P2P_ARTIFACT_S3_*`

the terraform stacks now pass `container_environment` into every node container
through an env file, so the config json placeholders used in `deploy/config/`
can resolve correctly at runtime.
