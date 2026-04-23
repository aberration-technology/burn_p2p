# Browser Transport Backend

`burn_p2p` uses the Rust libp2p wasm stack as the first browser transport
backend. Browser code should not synthesize peer state from edge snapshots:
`burn_p2p_swarm::WasmBrowserSwarmRuntime` owns the live browser swarm, reports
the selected transport separately from the connected transport, and only marks a
browser transport connected after libp2p reports a real peer connection.

## Backend Choice

The current backend is:

- `burn_p2p_webrtc_websys` for WebRTC-direct seed dials
- `libp2p-webtransport-websys` for WebTransport when a deployment enables it
- `libp2p-websocket-websys` as an explicit WSS fallback, not as proof of direct
  browser transport health

The runtime is intentionally Rust-owned. JavaScript is only used by browser API
bindings and Playwright/canary harnesses; it must not duplicate transport
selection, authorization, or peer-state semantics.

## Runtime Contract

`BrowserSwarmStatus` is the diagnostic source of truth:

- `desired_transport` means a signed seed policy selected a transport family.
- `connected_transport` means a live libp2p connection exists for that family.
- `connected_peer_ids` and `connected_peer_count` are real swarm peers.
- `last_error` preserves the exact failed seed candidate chain until a later
  successful connection clears it.

Consumers should treat `Selected` as "dialing/planned" and `Connected` as the
first usable peer path. A browser can train only when the downstream workload has
an assignment/head and the runtime has a connected browser-capable peer.

## Firefox/WebRTC Notes

Firefox WebRTC-direct dials can take longer than Chromium to finish ICE, DTLS,
data-channel open, and Noise setup. The wasm swarm connection timeout is
therefore 15 seconds. Shorter timeouts can tear down the data channel during
Noise negotiation and surface as native-side broken-pipe authentication errors.

The patched WebRTC transport also accepts LF-only SDP fingerprints and logs RTC
state transitions, data-channel lifecycle events, SDP offer/answer progress, and
Noise handshake progress to the browser console. These logs are part of the
operational contract for production transport triage.

## Required Gates

Before a deployment claims browser swarm support, CI/deploy validation should
prove:

- the edge advertises a signed browser seed advertisement with runtime certhash
  material
- Chromium can authenticate, connect by WebRTC-direct, and reach training-ready
  state
- Firefox can authenticate and connect by WebRTC-direct
- WSS fallback lanes remain non-blocking diagnostics unless explicitly promoted
- a canary fails if the browser reports `connected_transport = null`, retains a
  direct transport error after connection, or only observes edge polling

This is intentionally stricter than "the page loaded." Browser peer support is
only healthy when the live browser runtime reports real connected peer state.
