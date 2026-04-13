# reference downstream assets

these files are the repo's copy-edit starting points for downstream product
repos.

they are intentionally **not** the same thing as the stable downstream-facing
crate api.

## stable downstream-facing api

prefer treating these as the reusable upstream surfaces:

- `burn_p2p_admin`
- `burn_p2p_workload` directory-metadata helpers
- `burn_p2p_views`
- `burn_p2p_app`
- `burn_p2p_browser::BrowserSiteBootstrapConfig`
- `burn_p2p_e2e`

those are the mechanism-oriented seams downstreams should code against.

## reference infra and workflow assets

the files in this folder are different:

- they are examples and templates
- they are meant to be copied into downstream repos and edited
- they may change as the repo's own operational defaults change
- they are not a semver-stable compatibility contract

use them as a starting point when wiring:

- static browser-site deployment
- dataset publication
- staged validation
- bootstrap/cloud deployment

## included templates

- [github-pages-browser-site.yml](github-pages-browser-site.yml)
  - reference github pages workflow for a downstream wasm browser shell built
    with `cargo xtask browser site`
- [dataset-publish.yml](dataset-publish.yml)
  - reference workflow pattern for building a downstream dataset bundle,
    uploading it to object storage, and keeping artifact smoke checks in the
    loop
- [staged-validation.yml](staged-validation.yml)
  - reference multi-stage validation workflow built around the repo's grouped
    `cargo xtask ci ...` lanes
- [bootstrap-deploy.md](bootstrap-deploy.md)
  - reference bootstrap/cloud deployment pattern that points at the generic
    compose and terraform assets already in `deploy/`

## what to keep downstream

do not cargo-cult these templates without editing:

- package names
- binary names
- domains and edge urls
- dataset build commands
- object-store paths
- cloud account wiring
- product-specific browser selection or auth defaults

the templates intentionally use placeholders so another downstream can start
from them without inheriting dragon-specific naming or assumptions.
