use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    ArtifactAlias, ArtifactProfile, DownloadDeliveryMode, HeadId, PublicationAccessMode,
    PublicationMode, PublicationTarget, PublicationTargetId, PublicationTargetKind, RevisionId,
    RunId,
};
#[cfg(feature = "s3")]
use {
    hmac::{Hmac, Mac},
    percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode},
    reqwest::blocking::Client,
    serde::Deserialize,
    sha2::{Digest, Sha256},
    std::time::Duration,
    url::Url,
};

use crate::{DEFAULT_PUBLICATION_TARGET_ID, DEFAULT_TARGET_RETENTION_TTL_SECS, PublishError};

#[cfg(feature = "s3")]
const DEFAULT_SIGNED_URL_TTL_SECS: u64 = 300;
#[cfg(feature = "s3")]
const QUERY_PERCENT_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub(crate) fn default_filesystem_target(root_dir: &Path) -> PublicationTarget {
    PublicationTarget {
        publication_target_id: PublicationTargetId::new(DEFAULT_PUBLICATION_TARGET_ID),
        label: "local mirror".into(),
        kind: PublicationTargetKind::LocalFilesystem,
        publication_mode: PublicationMode::LazyOnDemand,
        access_mode: PublicationAccessMode::Authenticated,
        allow_public_reads: false,
        supports_signed_urls: false,
        edge_proxy_required: true,
        max_artifact_size_bytes: None,
        retention_ttl_secs: Some(DEFAULT_TARGET_RETENTION_TTL_SECS),
        allowed_artifact_profiles: BTreeSet::from([
            ArtifactProfile::FullTrainingCheckpoint,
            ArtifactProfile::ServeCheckpoint,
            ArtifactProfile::ManifestOnly,
        ]),
        eager_alias_names: BTreeSet::new(),
        local_root: Some(root_dir.join("mirror").display().to_string()),
        bucket: None,
        endpoint: None,
        region: None,
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        path_prefix: Some("artifacts".into()),
        multipart_threshold_bytes: None,
        server_side_encryption: None,
        signed_url_ttl_secs: None,
    }
}

pub(crate) fn normalize_target(
    root_dir: &Path,
    mut target: PublicationTarget,
) -> PublicationTarget {
    if target.kind == PublicationTargetKind::LocalFilesystem && target.local_root.is_none() {
        target.local_root = Some(root_dir.join("mirror").display().to_string());
    }
    target
}

pub(crate) fn ensure_profile_allowed(
    target: &PublicationTarget,
    artifact_profile: &ArtifactProfile,
) -> Result<(), PublishError> {
    if target.allowed_artifact_profiles.contains(artifact_profile) {
        Ok(())
    } else {
        Err(PublishError::DisallowedArtifactProfile {
            target_id: target.publication_target_id.clone(),
            profile: artifact_profile.clone(),
        })
    }
}

pub(crate) fn should_eager_publish(alias: &ArtifactAlias, target: &PublicationTarget) -> bool {
    match target.publication_mode {
        PublicationMode::Disabled | PublicationMode::LazyOnDemand => false,
        PublicationMode::Eager => true,
        PublicationMode::Hybrid => {
            if !target.eager_alias_names.is_empty() {
                target.eager_alias_names.contains(&alias.alias_name)
            } else {
                alias.artifact_profile == ArtifactProfile::ServeCheckpoint
            }
        }
    }
}

pub(crate) fn write_local_object(
    target: &PublicationTarget,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), PublishError> {
    let target_root = target
        .local_root
        .as_ref()
        .ok_or(PublishError::MissingFilesystemTarget)?;
    let path = PathBuf::from(target_root).join(object_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn publication_object_key(
    target: &PublicationTarget,
    experiment_id: &str,
    run_id: Option<&RunId>,
    revision_id: &RevisionId,
    head_id: &HeadId,
    artifact_profile: &ArtifactProfile,
    extension: &str,
) -> String {
    let run_segment = run_id
        .map(|run_id| format!("run/{}", run_id.as_str()))
        .unwrap_or_else(|| format!("revision/{}", revision_id.as_str()));
    let profile = match artifact_profile {
        ArtifactProfile::FullTrainingCheckpoint => "full",
        ArtifactProfile::ServeCheckpoint => "serve",
        ArtifactProfile::BrowserSnapshot => "browser",
        ArtifactProfile::ManifestOnly => "manifest",
    };
    let base_key = format!(
        "exp/{experiment_id}/{run_segment}/{profile}/{}.{}",
        head_id.as_str(),
        extension
    );
    target
        .path_prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| format!("{}/{}", prefix.trim_matches('/'), base_key))
        .unwrap_or(base_key)
}

pub(crate) fn delivery_mode_for_target(target: &PublicationTarget) -> DownloadDeliveryMode {
    match target.kind {
        PublicationTargetKind::S3Compatible
            if target.supports_signed_urls && !target.edge_proxy_required =>
        {
            DownloadDeliveryMode::RedirectToObjectStore
        }
        PublicationTargetKind::None
        | PublicationTargetKind::LocalFilesystem
        | PublicationTargetKind::S3Compatible => DownloadDeliveryMode::EdgeStream,
    }
}

pub(crate) fn download_redirect_url(
    target: &PublicationTarget,
    object_key: &str,
) -> Result<String, PublishError> {
    match target.kind {
        PublicationTargetKind::S3Compatible if target.supports_signed_urls => {
            #[cfg(feature = "s3")]
            {
                presign_s3_get_url(
                    target,
                    object_key,
                    target
                        .signed_url_ttl_secs
                        .unwrap_or(DEFAULT_SIGNED_URL_TTL_SECS),
                )
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = object_key;
                Err(PublishError::S3FeatureDisabled(
                    target.publication_target_id.clone(),
                ))
            }
        }
        _ => Err(PublishError::DisabledPublicationTarget(
            target.publication_target_id.clone(),
        )),
    }
}

#[cfg(feature = "s3")]
pub(crate) fn proxy_download_url(
    target: &PublicationTarget,
    object_key: &str,
) -> Result<String, PublishError> {
    match target.kind {
        PublicationTargetKind::S3Compatible => presign_s3_get_url(
            target,
            object_key,
            target
                .signed_url_ttl_secs
                .unwrap_or(DEFAULT_SIGNED_URL_TTL_SECS),
        ),
        _ => Err(PublishError::DisabledPublicationTarget(
            target.publication_target_id.clone(),
        )),
    }
}

#[cfg(feature = "s3")]
type HmacSha256 = Hmac<Sha256>;

#[cfg(feature = "s3")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct S3Credentials {
    endpoint: Url,
    bucket: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[cfg(feature = "s3")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct AmbientS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[cfg(feature = "s3")]
#[derive(Clone, Debug, Deserialize)]
struct ImdsRoleCredentialsResponse {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "Token")]
    session_token: Option<String>,
}

#[cfg(feature = "s3")]
fn ambient_s3_credentials_from_env<F>(get_var: F) -> Option<AmbientS3Credentials>
where
    F: Fn(&str) -> Option<String>,
{
    let access_key_id = get_var("AWS_ACCESS_KEY_ID")?.trim().to_owned();
    let secret_access_key = get_var("AWS_SECRET_ACCESS_KEY")?.trim().to_owned();
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }
    let session_token = get_var("AWS_SESSION_TOKEN")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Some(AmbientS3Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

#[cfg(feature = "s3")]
fn ambient_s3_credentials_from_imds() -> Option<AmbientS3Credentials> {
    let imds_disabled = std::env::var("AWS_EC2_METADATA_DISABLED")
        .ok()
        .map(|value| value.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if imds_disabled {
        return None;
    }
    let endpoint = std::env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "http://169.254.169.254".into());
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .ok()?;
    let token = client
        .put(format!("{endpoint}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .ok()?;
    let role_name = client
        .get(format!(
            "{endpoint}/latest/meta-data/iam/security-credentials/"
        ))
        .header("X-aws-ec2-metadata-token", token.trim())
        .send()
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .ok()?
        .lines()
        .find_map(|line| {
            let line = line.trim();
            (!line.is_empty()).then(|| line.to_owned())
        })?;
    let response = client
        .get(format!(
            "{endpoint}/latest/meta-data/iam/security-credentials/{role_name}"
        ))
        .header("X-aws-ec2-metadata-token", token.trim())
        .send()
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .ok()
        .and_then(|body| serde_json::from_str::<ImdsRoleCredentialsResponse>(&body).ok())?;
    let access_key_id = response.access_key_id.trim().to_owned();
    let secret_access_key = response.secret_access_key.trim().to_owned();
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }
    let session_token = response
        .session_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Some(AmbientS3Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

#[cfg(feature = "s3")]
fn ambient_s3_credentials() -> Option<AmbientS3Credentials> {
    ambient_s3_credentials_from_env(|name| std::env::var(name).ok())
        .or_else(ambient_s3_credentials_from_imds)
}

#[cfg(feature = "s3")]
fn s3_credentials(target: &PublicationTarget) -> Result<S3Credentials, PublishError> {
    let endpoint_raw = target
        .endpoint
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "endpoint",
        })?;
    let endpoint = Url::parse(endpoint_raw).map_err(|_| PublishError::InvalidS3Endpoint {
        target_id: target.publication_target_id.clone(),
        endpoint: endpoint_raw.to_owned(),
    })?;
    let bucket = target
        .bucket
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "bucket",
        })?
        .to_owned();
    let region = target
        .region
        .as_deref()
        .ok_or_else(|| PublishError::MissingS3Config {
            target_id: target.publication_target_id.clone(),
            field: "region",
        })?
        .to_owned();
    let (access_key_id, secret_access_key, session_token) = match (
        target.access_key_id.as_deref(),
        target.secret_access_key.as_deref(),
    ) {
        (Some(access_key_id), Some(secret_access_key)) => (
            access_key_id.to_owned(),
            secret_access_key.to_owned(),
            target.session_token.clone(),
        ),
        (Some(_), None) => {
            return Err(PublishError::MissingS3Config {
                target_id: target.publication_target_id.clone(),
                field: "secret_access_key",
            });
        }
        (None, Some(_)) => {
            return Err(PublishError::MissingS3Config {
                target_id: target.publication_target_id.clone(),
                field: "access_key_id",
            });
        }
        (None, None) => {
            let ambient =
                ambient_s3_credentials().ok_or_else(|| PublishError::MissingS3Config {
                    target_id: target.publication_target_id.clone(),
                    field: "access_key_id",
                })?;
            (
                ambient.access_key_id,
                ambient.secret_access_key,
                ambient.session_token,
            )
        }
    };
    Ok(S3Credentials {
        endpoint,
        bucket,
        region,
        access_key_id,
        secret_access_key,
        session_token,
    })
}

#[cfg(feature = "s3")]
pub(crate) fn upload_s3_object(
    target: &PublicationTarget,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), PublishError> {
    let url = s3_object_url(target, object_key)?;
    let payload_hash = sha256_hex(bytes);
    let mut additional_headers = Vec::<(String, String)>::new();
    if let Some(sse) = target.server_side_encryption.clone() {
        additional_headers.push(("x-amz-server-side-encryption".into(), sse));
    }
    let headers = s3_authorized_headers(target, "PUT", &url, &payload_hash, &additional_headers)?;
    let client = Client::new();
    let mut request = client.put(url).body(bytes.to_vec());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send()?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    Err(PublishError::S3RequestFailed {
        target_id: target.publication_target_id.clone(),
        status,
        body,
    })
}

#[cfg(feature = "s3")]
pub(crate) fn delete_s3_object(
    target: &PublicationTarget,
    object_key: &str,
) -> Result<(), PublishError> {
    let url = s3_object_url(target, object_key)?;
    let headers = s3_authorized_headers(
        target,
        "DELETE",
        &url,
        &sha256_hex(&[]),
        &Vec::<(String, String)>::new(),
    )?;
    let client = Client::new();
    let mut request = client.delete(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send()?;
    if response.status().is_success() || response.status().as_u16() == 404 {
        return Ok(());
    }
    let status = response.status().as_u16();
    let body = response.text().unwrap_or_default();
    Err(PublishError::S3RequestFailed {
        target_id: target.publication_target_id.clone(),
        status,
        body,
    })
}

#[cfg(feature = "s3")]
fn presign_s3_get_url(
    target: &PublicationTarget,
    object_key: &str,
    ttl_secs: u64,
) -> Result<String, PublishError> {
    let creds = s3_credentials(target)?;
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let mut url = s3_object_url(target, object_key)?;
    let credential_scope = format!("{}/{}/s3/aws4_request", datestamp, creds.region);
    let credential = format!("{}/{}", creds.access_key_id, credential_scope);
    let mut query_params = vec![
        ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        ("X-Amz-Credential".to_owned(), credential),
        ("X-Amz-Date".to_owned(), amz_date.clone()),
        ("X-Amz-Expires".to_owned(), ttl_secs.to_string()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ];
    if let Some(token) = creds.session_token {
        query_params.push(("X-Amz-Security-Token".to_owned(), token.to_owned()));
    }
    let canonical_query = canonical_query_string(&query_params);
    url.set_query(Some(&canonical_query));
    let host = url_host_header(&url);
    let canonical_request = format!(
        "GET\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        url.path(),
        canonical_query,
        host
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hmac_signature(
        &creds.secret_access_key,
        &datestamp,
        &creds.region,
        "s3",
        &string_to_sign,
    );
    let signed_query = if canonical_query.is_empty() {
        format!("X-Amz-Signature={signature}")
    } else {
        format!("{canonical_query}&X-Amz-Signature={signature}")
    };
    url.set_query(Some(&signed_query));
    Ok(url.to_string())
}

#[cfg(feature = "s3")]
fn s3_authorized_headers(
    target: &PublicationTarget,
    method: &str,
    url: &Url,
    payload_hash: &str,
    additional_headers: &[(String, String)],
) -> Result<Vec<(String, String)>, PublishError> {
    let creds = s3_credentials(target)?;
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();
    let mut headers = std::collections::BTreeMap::<String, String>::from([
        ("host".into(), url_host_header(url)),
        ("x-amz-content-sha256".into(), payload_hash.to_owned()),
        ("x-amz-date".into(), amz_date.clone()),
    ]);
    if let Some(token) = creds.session_token {
        headers.insert("x-amz-security-token".into(), token.to_owned());
    }
    for (name, value) in additional_headers {
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let credential_scope = format!("{}/{}/s3/aws4_request", datestamp, creds.region);
    let canonical_request = format!(
        "{method}\n{}\n{}\n{}\n{}\n{}",
        url.path(),
        canonical_query_from_url(url),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hmac_signature(
        &creds.secret_access_key,
        &datestamp,
        &creds.region,
        "s3",
        &string_to_sign,
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        creds.access_key_id, credential_scope, signed_headers, signature
    );
    headers.insert("authorization".into(), authorization);
    Ok(headers.into_iter().collect())
}

#[cfg(feature = "s3")]
fn s3_object_url(target: &PublicationTarget, object_key: &str) -> Result<Url, PublishError> {
    let creds = s3_credentials(target)?;
    let mut url = creds.endpoint;
    {
        let mut segments =
            url.path_segments_mut()
                .map_err(|_| PublishError::InvalidS3Endpoint {
                    target_id: target.publication_target_id.clone(),
                    endpoint: target.endpoint.clone().unwrap_or_default(),
                })?;
        segments.pop_if_empty();
        segments.push(&creds.bucket);
        for segment in object_key.split('/') {
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
    }
    Ok(url)
}

#[cfg(feature = "s3")]
fn canonical_query_from_url(url: &Url) -> String {
    let params = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    canonical_query_string(&params)
}

#[cfg(feature = "s3")]
fn canonical_query_string(params: &[(String, String)]) -> String {
    let mut encoded = params
        .iter()
        .map(|(key, value)| {
            (
                utf8_percent_encode(key, QUERY_PERCENT_ENCODE_SET).to_string(),
                utf8_percent_encode(value, QUERY_PERCENT_ENCODE_SET).to_string(),
            )
        })
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(feature = "s3")]
fn url_host_header(url: &Url) -> String {
    match url.port() {
        Some(port) => format!(
            "{}:{}",
            url.host_str().expect("url should always include host"),
            port
        ),
        None => url
            .host_str()
            .expect("url should always include host")
            .to_owned(),
    }
}

#[cfg(feature = "s3")]
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(feature = "s3")]
fn hmac_signature(
    secret_access_key: &str,
    datestamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let k_date = hmac_bytes(format!("AWS4{secret_access_key}").as_bytes(), datestamp);
    let k_region = hmac_bytes(&k_date, region);
    let k_service = hmac_bytes(&k_region, service);
    let k_signing = hmac_bytes(&k_service, "aws4_request");
    hex::encode(hmac_bytes(&k_signing, string_to_sign))
}

#[cfg(feature = "s3")]
fn hmac_bytes(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key should be valid");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[cfg(all(test, feature = "s3"))]
mod tests {
    use super::{AmbientS3Credentials, ambient_s3_credentials_from_env};
    use std::collections::BTreeMap;

    #[test]
    fn ambient_s3_credentials_from_env_requires_access_and_secret_keys() {
        let values = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID", "access"),
            ("AWS_SESSION_TOKEN", "token"),
        ]);
        let creds =
            ambient_s3_credentials_from_env(|name| values.get(name).map(|value| value.to_string()));
        assert_eq!(creds, None);
    }

    #[test]
    fn ambient_s3_credentials_from_env_uses_standard_aws_env_vars() {
        let values = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID", "access-key"),
            ("AWS_SECRET_ACCESS_KEY", "secret-key"),
            ("AWS_SESSION_TOKEN", "session-token"),
        ]);
        let creds =
            ambient_s3_credentials_from_env(|name| values.get(name).map(|value| value.to_string()));
        assert_eq!(
            creds,
            Some(AmbientS3Credentials {
                access_key_id: "access-key".into(),
                secret_access_key: "secret-key".into(),
                session_token: Some("session-token".into()),
            })
        );
    }
}
