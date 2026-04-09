use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use burn_p2p_security::{AuthError, random_login_state_token};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct OidcDiscoveryDocument {
    #[serde(default)]
    pub(crate) authorization_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) token_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) userinfo_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) revocation_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) jwks_uri: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OidcIdTokenClaims {
    iss: String,
    sub: String,
    exp: usize,
    #[serde(default)]
    aud: Value,
    #[serde(default)]
    nonce: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl OidcIdTokenClaims {
    fn as_value(&self) -> Value {
        let mut object = serde_json::Map::new();
        object.insert("iss".into(), Value::String(self.iss.clone()));
        object.insert("sub".into(), Value::String(self.sub.clone()));
        object.insert(
            "exp".into(),
            Value::Number(serde_json::Number::from(self.exp as u64)),
        );
        object.insert("aud".into(), self.aud.clone());
        if let Some(nonce) = self.nonce.as_ref() {
            object.insert("nonce".into(), Value::String(nonce.clone()));
        }
        object.extend(self.extra.clone());
        Value::Object(object)
    }
}

pub(crate) fn generate_pkce_pair() -> Result<(String, String), AuthError> {
    let verifier = random_login_state_token("oidc pkce verifier")?;
    let challenge = pkce_challenge_for_verifier(&verifier);
    Ok((verifier, challenge))
}

pub(crate) fn pkce_challenge_for_verifier(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

pub(crate) fn validate_and_decode_id_token(
    id_token: &str,
    issuer: &str,
    client_id: &str,
    jwks: &jsonwebtoken::jwk::JwkSet,
    expected_nonce: Option<&str>,
) -> Result<Value, AuthError> {
    let header = decode_header(id_token)
        .map_err(|error| AuthError::ProviderExchange(format!("invalid oidc id_token: {error}")))?;
    let algorithm = header.alg;
    let jwk = select_jwk(jwks, header.kid.as_deref(), algorithm)?;
    let decoding_key = DecodingKey::from_jwk(jwk).map_err(|error| {
        AuthError::ProviderExchange(format!("invalid oidc jwk for id_token validation: {error}"))
    })?;

    let mut validation = Validation::new(algorithm);
    validation.validate_nbf = true;
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[client_id]);
    validation.algorithms = vec![algorithm];

    let claims = decode::<OidcIdTokenClaims>(id_token, &decoding_key, &validation)
        .map_err(|error| {
            AuthError::ProviderExchange(format!("oidc id_token validation failed: {error}"))
        })?
        .claims;

    if let Some(expected_nonce) = expected_nonce
        && claims.nonce.as_deref() != Some(expected_nonce)
    {
        return Err(AuthError::ProviderExchange(
            "oidc id_token nonce mismatch".into(),
        ));
    }

    Ok(claims.as_value())
}

fn select_jwk<'a>(
    jwks: &'a jsonwebtoken::jwk::JwkSet,
    kid: Option<&str>,
    _algorithm: Algorithm,
) -> Result<&'a jsonwebtoken::jwk::Jwk, AuthError> {
    if let Some(kid) = kid {
        return jwks
            .keys
            .iter()
            .find(|jwk| jwk.common.key_id.as_deref() == Some(kid))
            .ok_or_else(|| {
                AuthError::ProviderExchange(format!(
                    "oidc jwks did not contain a key for kid `{kid}`"
                ))
            });
    }

    let mut keys = jwks.keys.iter();
    let Some(jwk) = keys.next() else {
        return Err(AuthError::ProviderExchange(
            "oidc jwks did not contain a compatible signing key".into(),
        ));
    };
    if keys.next().is_some() {
        return Err(AuthError::ProviderExchange(
            "oidc id_token omitted kid and jwks matched multiple keys".into(),
        ));
    }
    Ok(jwk)
}
