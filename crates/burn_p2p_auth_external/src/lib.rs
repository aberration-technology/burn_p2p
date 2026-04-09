//! Provider-backed external authentication connectors for burn_p2p enrollment flows.
#![forbid(unsafe_code)]

mod oidc;
mod provider;
mod proxy;
mod shared;

#[cfg(test)]
mod tests;

pub use provider::ProviderMappedIdentityConnector;
pub use proxy::ExternalProxyIdentityConnector;
