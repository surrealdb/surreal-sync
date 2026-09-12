//! Google service-account authentication for the BigQuery REST API.
//!
//! Snowflake signs a JWT and sends it straight to the API; Google requires one
//! extra hop. We build a short-lived RS256 JWT asserting the service account's
//! identity, exchange it at the token endpoint for an OAuth2 access token, and
//! cache that token until shortly before it expires.
//!
//! The emulator ignores authentication entirely, so [`Credentials::Anonymous`]
//! makes [`TokenProvider::access_token`] return `None` and the client omits the
//! `Authorization` header altogether.
//!
//! # References
//! - <https://developers.google.com/identity/protocols/oauth2/service-account#authorizingrequests>

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use tokio::sync::Mutex;

/// Read-only BigQuery scope. Ingestion never writes to the source.
const SCOPE: &str = "https://www.googleapis.com/auth/bigquery.readonly";

/// Lifetime requested for the assertion JWT. Google caps this at one hour.
const JWT_LIFETIME_SECS: u64 = 3600;

/// Refresh this long before the access token actually expires, so a long-running
/// sync never sends a token that lapses mid-flight.
const REFRESH_MARGIN_SECS: u64 = 60;

/// The subset of a service-account JSON key file that we need.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceAccountKey {
    /// The service account's email, used as the JWT issuer and subject.
    pub client_email: String,
    /// PKCS#8 private key PEM (`-----BEGIN PRIVATE KEY-----`).
    pub private_key: String,
    /// Token exchange endpoint. Google's keys always carry this.
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Project the key belongs to. Used as a fallback for `--project-id`.
    #[serde(default)]
    pub project_id: Option<String>,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

impl ServiceAccountKey {
    /// Parse a service-account key from JSON, rejecting the other credential
    /// shapes Google ships (authorized-user and external-account files) with a
    /// message that says what to do instead.
    pub fn from_json(json: &str) -> Result<Self> {
        let probe: serde_json::Value =
            serde_json::from_str(json).context("credentials file is not valid JSON")?;
        match probe.get("type").and_then(serde_json::Value::as_str) {
            Some("service_account") | None => {}
            Some(other) => bail!(
                "unsupported credentials type '{other}'; surreal-sync needs a \
                 service-account key (create one with \
                 `gcloud iam service-accounts keys create key.json --iam-account=…`)"
            ),
        }
        serde_json::from_value(probe).context("credentials file is not a service-account key")
    }
}

/// How the client authenticates against the API.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Sign and exchange a JWT for an access token.
    ServiceAccount(Box<ServiceAccountKey>),
    /// Send no `Authorization` header at all (emulator only).
    Anonymous,
}

/// JWT claim set for the `jwt-bearer` grant.
#[derive(Debug, serde::Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

/// The token endpoint's success response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// The token endpoint's error response.
#[derive(Debug, Deserialize)]
struct TokenError {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

#[derive(Debug, Clone)]
struct CachedToken {
    value: String,
    /// Unix seconds after which the token must be re-fetched.
    refresh_after: u64,
}

/// Mints and caches OAuth2 access tokens for one service account.
pub struct TokenProvider {
    credentials: Credentials,
    http: reqwest::Client,
    cached: Mutex<Option<CachedToken>>,
}

impl TokenProvider {
    /// Build a provider over an existing HTTP client (shared with the API client
    /// so both reuse the same connection pool).
    pub fn new(credentials: Credentials, http: reqwest::Client) -> Self {
        Self {
            credentials,
            http,
            cached: Mutex::new(None),
        }
    }

    /// Whether this provider will actually send credentials.
    pub fn is_anonymous(&self) -> bool {
        matches!(self.credentials, Credentials::Anonymous)
    }

    /// Return a valid access token, refreshing it if the cached one is close to
    /// expiry. Returns `Ok(None)` for [`Credentials::Anonymous`].
    pub async fn access_token(&self) -> Result<Option<String>> {
        let key = match &self.credentials {
            Credentials::Anonymous => return Ok(None),
            Credentials::ServiceAccount(key) => key,
        };

        let mut cached = self.cached.lock().await;
        let now = unix_now()?;
        if let Some(token) = cached.as_ref() {
            if now < token.refresh_after {
                return Ok(Some(token.value.clone()));
            }
        }

        let fetched = self.fetch_token(key).await?;
        let token = CachedToken {
            value: fetched.access_token,
            refresh_after: now
                + fetched
                    .expires_in
                    .unwrap_or(JWT_LIFETIME_SECS)
                    .saturating_sub(REFRESH_MARGIN_SECS)
                    .max(1),
        };
        let value = token.value.clone();
        *cached = Some(token);
        tracing::debug!("Obtained a new BigQuery access token");
        Ok(Some(value))
    }

    async fn fetch_token(&self, key: &ServiceAccountKey) -> Result<TokenResponse> {
        let assertion = sign_assertion(key, unix_now()?)?;

        let response = self
            .http
            .post(&key.token_uri)
            .form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:jwt-bearer".to_string(),
                ),
                ("assertion", assertion),
            ])
            .send()
            .await
            .context("Google token exchange request failed")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read the token response body")?;

        if !status.is_success() {
            // Surface Google's own diagnosis; its messages name the actual problem
            // (clock skew, revoked key, missing scope) far better than the status.
            let detail = serde_json::from_str::<TokenError>(&body)
                .ok()
                .and_then(|e| e.error_description.or(e.error))
                .unwrap_or(body);
            bail!("Google token exchange failed ({status}): {detail}");
        }

        serde_json::from_str(&body).context("failed to parse the token response")
    }
}

/// Build and RS256-sign the assertion JWT for `key`, valid from `issued_at`.
///
/// Split out from the network path so the claim set can be unit-tested.
fn sign_assertion(key: &ServiceAccountKey, issued_at: u64) -> Result<String> {
    let claims = Claims {
        iss: &key.client_email,
        sub: &key.client_email,
        scope: SCOPE,
        aud: &key.token_uri,
        iat: issued_at,
        exp: issued_at + JWT_LIFETIME_SECS,
    };

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(key.private_key.as_bytes())
        .context(
            "failed to read the service-account private key; it must be an \
             unencrypted PKCS#8 RSA key (the `private_key` field of the JSON key file)",
        )?;

    jsonwebtoken::encode(&header, &claims, &encoding_key)
        .map_err(|e| anyhow!("failed to sign the Google assertion JWT: {e}"))
}

fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key_json() -> String {
        json!({
            "type": "service_account",
            "project_id": "demo-project",
            "client_email": "sync@demo-project.iam.gserviceaccount.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
            "token_uri": "https://oauth2.googleapis.com/token"
        })
        .to_string()
    }

    #[test]
    fn parses_a_service_account_key() {
        let key = ServiceAccountKey::from_json(&key_json()).unwrap();
        assert_eq!(
            key.client_email,
            "sync@demo-project.iam.gserviceaccount.com"
        );
        assert_eq!(key.token_uri, "https://oauth2.googleapis.com/token");
        assert_eq!(key.project_id.as_deref(), Some("demo-project"));
    }

    #[test]
    fn token_uri_defaults_when_absent() {
        let json = json!({
            "type": "service_account",
            "client_email": "a@b.iam.gserviceaccount.com",
            "private_key": "pem"
        })
        .to_string();
        let key = ServiceAccountKey::from_json(&json).unwrap();
        assert_eq!(key.token_uri, "https://oauth2.googleapis.com/token");
    }

    #[test]
    fn rejects_authorized_user_credentials_with_actionable_message() {
        let json = json!({
            "type": "authorized_user",
            "client_id": "x",
            "refresh_token": "y"
        })
        .to_string();
        let err = ServiceAccountKey::from_json(&json).unwrap_err().to_string();
        assert!(err.contains("authorized_user"), "got: {err}");
        assert!(err.contains("service-account key"), "got: {err}");
    }

    #[test]
    fn rejects_non_json() {
        assert!(ServiceAccountKey::from_json("{not json").is_err());
    }

    #[test]
    fn claims_cover_the_requested_hour_and_read_only_scope() {
        let key = ServiceAccountKey::from_json(&key_json()).unwrap();
        let claims = Claims {
            iss: &key.client_email,
            sub: &key.client_email,
            scope: SCOPE,
            aud: &key.token_uri,
            iat: 1_700_000_000,
            exp: 1_700_000_000 + JWT_LIFETIME_SECS,
        };
        let encoded = serde_json::to_value(&claims).unwrap();
        assert_eq!(encoded["iss"], key.client_email);
        assert_eq!(encoded["sub"], key.client_email);
        assert_eq!(encoded["aud"], key.token_uri);
        assert_eq!(
            encoded["exp"].as_u64().unwrap() - encoded["iat"].as_u64().unwrap(),
            3600
        );
        assert_eq!(
            encoded["scope"],
            "https://www.googleapis.com/auth/bigquery.readonly"
        );
    }

    #[test]
    fn signing_a_bogus_pem_fails_with_a_helpful_message() {
        let key = ServiceAccountKey::from_json(&key_json()).unwrap();
        let err = sign_assertion(&key, 1_700_000_000).unwrap_err().to_string();
        assert!(err.contains("private key"), "got: {err}");
    }

    #[tokio::test]
    async fn anonymous_provider_returns_no_token() {
        let provider = TokenProvider::new(Credentials::Anonymous, reqwest::Client::new());
        assert!(provider.is_anonymous());
        assert_eq!(provider.access_token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_fresh_cached_token_is_reused_without_a_network_call() {
        // The provider holds an unusable key: if the cache is consulted correctly,
        // no token exchange is attempted and this cannot fail.
        let key = ServiceAccountKey::from_json(&key_json()).unwrap();
        let provider = TokenProvider::new(
            Credentials::ServiceAccount(Box::new(key)),
            reqwest::Client::new(),
        );
        *provider.cached.lock().await = Some(CachedToken {
            value: "cached-token".to_string(),
            refresh_after: unix_now().unwrap() + 600,
        });

        assert_eq!(
            provider.access_token().await.unwrap().as_deref(),
            Some("cached-token")
        );
    }

    #[tokio::test]
    async fn an_expired_cached_token_triggers_a_refresh() {
        // Same unusable key, but the cache is stale, so a refresh is attempted and
        // fails at the signing step — proving expiry is honoured.
        let key = ServiceAccountKey::from_json(&key_json()).unwrap();
        let provider = TokenProvider::new(
            Credentials::ServiceAccount(Box::new(key)),
            reqwest::Client::new(),
        );
        *provider.cached.lock().await = Some(CachedToken {
            value: "stale-token".to_string(),
            refresh_after: unix_now().unwrap().saturating_sub(1),
        });

        assert!(provider.access_token().await.is_err());
    }
}
