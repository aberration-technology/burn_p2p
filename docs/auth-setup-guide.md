# burn_p2p Auth Setup Guide

## Purpose

This guide covers the provider-backed auth crates and the trusted external-proxy mode.

Available provider crates:

- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_auth_external`

## Shared model

All provider flows ultimately produce:

- a `PrincipalSession`
- `PrincipalClaims`
- certificate issuance through `burn_p2p_security`

The network trust model does not depend on the provider itself. It depends on the certificate
issued after authenticated enrollment.

## GitHub

Use `burn_p2p_auth_github` when:

- the deployment is public/community-facing
- GitHub identity is the primary contributor identity

Recommended setup:

1. configure authorize URL
2. configure token URL or exchange URL
3. configure userinfo URL
4. configure refresh/revoke URLs when upstream behavior supports them
5. map stable principal records using provider subject, login, or email claims

Operational note:

- keep org/team mapping provider-specific where it truly depends on GitHub APIs
- keep generic principal/session logic in `burn_p2p_auth_external`
- use GitHub-specific mapping only for semantics that depend on repository, org, or team APIs
  outside the standard token or userinfo payload

## OIDC

Use `burn_p2p_auth_oidc` when:

- the deployment is enterprise/private
- issuer-backed group or role claims are available

Recommended setup:

1. configure issuer-specific authorize and token endpoints
2. configure client ID and secret
3. configure userinfo if claims are not fully present in tokens
4. define principal mapping based on stable subject plus group claims

Operational note:

- treat issuer-specific `groups`, `roles`, and SCIM-style org claims as OIDC-local policy
- keep only normalized principal/session state in the generic external connector

## OAuth

Use `burn_p2p_auth_oauth` when:

- the provider is not GitHub-specific
- OIDC discovery is not available or not desired

Recommended setup:

1. configure token and userinfo endpoints explicitly
2. define principal mapping from stable provider claims
3. treat org/team semantics as provider-local policy, not generic runtime truth

## Team and org mapping boundary

The workspace now treats provider-to-principal mapping as:

- `burn_p2p_auth_external`: generic subject, display-name, email, avatar, and normalized
  organization membership extraction from token or userinfo payloads
- `burn_p2p_auth_github`: GitHub-specific org/team semantics that require GitHub API knowledge,
  naming conventions, or installation-scoped policy
- `burn_p2p_auth_oidc`: issuer-specific group, role, and enterprise claim normalization
- `burn_p2p_auth_oauth`: provider-local claim mapping when neither GitHub nor OIDC conventions
  apply cleanly

Rule of thumb:

- if the mapping can be derived from standard token or userinfo claims, keep it generic
- if the mapping requires provider-specific APIs, semantics, or rollout policy, keep it in the
  provider crate

## Trusted external mode

Use `burn_p2p_auth_external` trusted-proxy mode only when:

- the deployment sits behind a VPN, reverse proxy, service mesh, or internal SSO gateway
- the upstream identity assertion is already trusted

Requirements:

- `trusted_internal_only = true`
- a configured trusted principal header
- no direct public exposure of the upstream assertion path

This mode is not safe for open Internet direct exposure.

## Revocation and reenrollment

Revocation is enforced by epoch. When the minimum revocation epoch advances:

1. previously issued certificates below that epoch become stale
2. session/certificate refresh may fail closed
3. the client must reenroll and receive a new certificate

Recommended incident procedure:

1. raise the epoch
2. verify `/trust` and `/reenrollment`
3. communicate reenrollment scope to operators and users
4. monitor for stale-session or stale-certificate errors

## Failure handling

The repository now includes explicit tests for:

- provider exchange outage
- partial refresh failure
- stale-session recovery through fresh login

Operational expectations:

- provider outage should not corrupt local trust state
- refresh failure should fail closed and require reauthentication
- stale local sessions should never be silently reused past expiry
