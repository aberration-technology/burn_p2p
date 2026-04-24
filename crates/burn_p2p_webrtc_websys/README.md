# `burn_p2p_webrtc_websys`

Patched browser WebRTC-direct transport for `burn_p2p`, exposed through
`web-sys` bindings.

## Usage

This crate is an internal transport dependency of `burn_p2p_swarm`'s wasm
browser runtime. Downstream browser apps normally use `burn_p2p_browser` or the
`burn_p2p_swarm::WasmBrowserSwarmRuntime` boundary instead of constructing this
transport directly.

The operational contract for browser WebRTC/WebTransport support lives in
[../../docs/browser-transport-backend.md](../../docs/browser-transport-backend.md).
