//! Snowflake SQL REST API v2 client (key-pair JWT auth).
//!
//! Handles JWT generation, statement submission, asynchronous (`202`) polling,
//! and result-partition pagination. Result decoding into `Value`s is
//! the job of [`crate::types`]; this module only produces the raw
//! `(rowType, data)` pair.
//!
//! Large results are exposed via [`QueryStream`], which keeps **one partition**
//! in memory at a time and yields bounded [`QueryStream::next_batch`] slices for
//! the apply path.
//!
//! Private keys may be plain or passphrase-encrypted PKCS#8; see
//! [`resolve_private_key_pem`].

use std::time::{Duration, Instant};

use crate::types::ColumnType;
use anyhow::{anyhow, bail, Context, Result};
use pkcs8::der::Decode;
use pkcs8::pkcs5::EncryptionScheme;
use pkcs8::{der::SecretDocument, EncryptedPrivateKeyInfo, LineEnding};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use zeroize::Zeroizing;

use super::SourceOpts;

/// PEM label for a passphrase-encrypted PKCS#8 key.
const LABEL_ENCRYPTED: &str = "ENCRYPTED PRIVATE KEY";
/// PEM label for an unencrypted PKCS#8 key -- what `snowflake-jwt` requires.
const LABEL_PLAIN: &str = "PRIVATE KEY";
/// PEM label for a PKCS#1 ("traditional") RSA key. Snowflake does not accept
/// these for key-pair auth, and neither does `snowflake-jwt`.
const LABEL_PKCS1: &str = "RSA PRIVATE KEY";

/// Suggested fix appended to every "this key won't work" error.
const REWRAP_HINT: &str = "re-wrap the key as PKCS#8 with a supported cipher:\n  \
     openssl pkcs8 -topk8 -in old_key.pem -out rsa_key.p8 -v2 aes-256-cbc";

/// Normalize a private key into the unencrypted PKCS#8 PEM that
/// [`snowflake_jwt::generate_jwt_token`] expects.
///
/// `snowflake-jwt` calls `RsaPrivateKey::from_pkcs8_pem`, which reads plaintext
/// PKCS#8 only. Encrypted keys are decrypted here so that constraint stays an
/// implementation detail rather than a user-visible limitation.
///
/// Supported: PBES2 with AES-CBC/AES-GCM or 3DES-CBC (`openssl pkcs8 -topk8 -v2
/// aes-256-cbc` / `-v2 des3`). Not supported: legacy PBES1/SHA-1, and OpenSSL's
/// pre-PKCS#8 "traditional" encrypted PEM. Both are reported explicitly rather
/// than surfacing as an opaque DER parse failure.
///
/// The result is [`Zeroizing`] so the decrypted key is wiped on drop.
fn resolve_private_key_pem(pem: &str, passphrase: Option<&str>) -> Result<Zeroizing<String>> {
    // Traditional OpenSSL encryption is signalled by RFC 1421 headers inside an
    // otherwise-PKCS#1 body, so it has to be caught before the DER parse.
    if pem.contains("Proc-Type:") && pem.contains("DEK-Info:") {
        bail!(
            "the private key uses OpenSSL's legacy \"traditional\" PEM encryption \
             (Proc-Type/DEK-Info headers), which is not PKCS#8 and is not accepted by \
             Snowflake for key-pair auth. {REWRAP_HINT}"
        );
    }

    let (label, document) = SecretDocument::from_pem(pem).map_err(|e| {
        anyhow!(
            "could not parse the private key as PEM: {e}. Expected a PKCS#8 key \
             beginning with \"-----BEGIN {LABEL_PLAIN}-----\" or \
             \"-----BEGIN {LABEL_ENCRYPTED}-----\""
        )
    })?;

    match label {
        LABEL_ENCRYPTED => {
            let Some(passphrase) = passphrase else {
                bail!(
                    "the private key is encrypted but no passphrase was supplied. \
                     Pass --private-key-passphrase (or set \
                     SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)"
                );
            };

            let info = EncryptedPrivateKeyInfo::from_der(document.as_bytes())
                .map_err(|e| anyhow!("could not parse the encrypted private key structure: {e}"))?;

            let decrypted = info
                .decrypt(passphrase)
                .map_err(|e| decrypt_failure(&info.encryption_algorithm, e))?;

            let pem = decrypted
                .to_pem(LABEL_PLAIN, LineEnding::LF)
                .map_err(|e| anyhow!("could not re-encode the decrypted private key: {e}"))?;

            // `to_pem` already returns Zeroizing<String>; keep that guarantee.
            Ok(pem)
        }

        LABEL_PLAIN => {
            if passphrase.is_some() {
                tracing::warn!(
                    "--private-key-passphrase was supplied but the private key is not \
                     encrypted; ignoring the passphrase"
                );
            }
            Ok(Zeroizing::new(pem.to_string()))
        }

        LABEL_PKCS1 => bail!(
            "the private key is in PKCS#1 format (\"-----BEGIN {LABEL_PKCS1}-----\"). \
             Snowflake key-pair auth requires PKCS#8. Convert it:\n  \
             openssl pkcs8 -topk8 -in old_key.pem -out rsa_key.p8 -nocrypt\n\
             (omit -nocrypt to keep it passphrase-encrypted)"
        ),

        other => bail!(
            "unsupported private key PEM label \"{other}\". Expected \"{LABEL_PLAIN}\" \
             or \"{LABEL_ENCRYPTED}\""
        ),
    }
}

/// Turn a decryption failure into something the operator can act on.
///
/// A wrong passphrase and an unsupported cipher are indistinguishable at the
/// `pkcs5` error level, so disambiguate on the scheme instead: PBES1 is never
/// supported, whereas PBES2 is, which makes the passphrase the likely culprit.
fn decrypt_failure(scheme: &EncryptionScheme<'_>, err: pkcs8::Error) -> anyhow::Error {
    match scheme {
        EncryptionScheme::Pbes1(_) => anyhow!(
            "the private key uses legacy PBES1 (PKCS#5 v1.5) encryption, which is not \
             supported. {REWRAP_HINT}"
        ),
        // PBES2 is supported, so a failure here points at the passphrase first.
        // `EncryptionScheme` is #[non_exhaustive]; any future variant lands here
        // too, where the wording stays accurate.
        _ => anyhow!(
            "could not decrypt the private key ({err}). The passphrase is most likely \
             wrong. If it is correct, the key may use a cipher that is not supported \
             (PBES2 with AES-CBC, AES-GCM or 3DES-CBC is); in that case {REWRAP_HINT}"
        ),
    }
}

/// A decoded (but not yet type-converted) result set: column metadata plus every
/// data row across all partitions.
///
/// Prefer [`SnowflakeClient::execute_query_stream`] for large tables — this type
/// materializes the full result and is mainly for small queries (DDL, discovery).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Per-column metadata (`resultSetMetaData.rowType`).
    pub columns: Vec<ColumnType>,
    /// Rows, each a vector of raw JSON cells aligned with `columns`.
    pub rows: Vec<Vec<JsonValue>>,
}

/// Streaming view of a statement result: one Snowflake partition buffered at a
/// time, sliced into caller-sized batches.
pub struct QueryStream<'a> {
    client: &'a SnowflakeClient,
    columns: Vec<ColumnType>,
    /// Statement handle used to fetch partitions `1..partition_count`.
    handle: Option<String>,
    /// Total partitions reported by Snowflake (or `1` when data arrived without
    /// `partitionInfo`).
    partition_count: usize,
    /// Next partition index to fetch after the current buffer is exhausted.
    next_partition: usize,
    /// Rows for the partition currently being drained.
    current: Vec<Vec<JsonValue>>,
    /// Offset within [`Self::current`].
    current_offset: usize,
}

impl<'a> QueryStream<'a> {
    /// Column metadata for the statement (stable for the life of the stream).
    pub fn columns(&self) -> &[ColumnType] {
        &self.columns
    }

    /// Yield up to `max_rows` raw cells from the current partition, fetching the
    /// next partition only when the buffer is empty.
    ///
    /// Returns `Ok(None)` when the result is fully consumed. A returned batch may
    /// be smaller than `max_rows` when a partition ends mid-batch (callers should
    /// treat that as a normal, final partial batch for that partition).
    pub async fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<JsonValue>>>> {
        let max_rows = max_rows.max(1);
        loop {
            if self.current_offset >= self.current.len() {
                self.current.clear();
                self.current_offset = 0;
                if !self.fetch_next_partition_if_needed().await? {
                    return Ok(None);
                }
                // Empty partitions are skipped by continuing the loop.
                continue;
            }

            let end = (self.current_offset + max_rows).min(self.current.len());
            let batch = self.current[self.current_offset..end].to_vec();
            self.current_offset = end;

            // Drop the partition buffer once fully drained so peak memory stays
            // near one partition (+ the in-flight apply window).
            if self.current_offset >= self.current.len() {
                self.current.clear();
                self.current.shrink_to_fit();
                self.current_offset = 0;
            }

            if batch.is_empty() {
                continue;
            }
            return Ok(Some(batch));
        }
    }

    async fn fetch_next_partition_if_needed(&mut self) -> Result<bool> {
        if self.next_partition >= self.partition_count {
            return Ok(false);
        }

        // Partition 0 is always loaded into `current` at stream open. Subsequent
        // partitions are fetched by index (`next_partition` starts at 1).
        let handle = self
            .handle
            .as_deref()
            .ok_or_else(|| anyhow!("multi-partition result missing statementHandle"))?;
        let partition = self.next_partition;
        tracing::debug!(
            partition,
            partition_count = self.partition_count,
            "Fetching Snowflake result partition"
        );
        self.current = self.client.fetch_partition(handle, partition).await?;
        self.next_partition += 1;
        self.current_offset = 0;
        Ok(true)
    }
}

/// Client for a single Snowflake account, bound to one warehouse/database/schema.
pub struct SnowflakeClient {
    http: reqwest::Client,
    base_url: String,
    /// Uppercased `ACCOUNT.USER` identity used as the JWT subject.
    jwt_identity: String,
    /// Unencrypted PKCS#8 PEM, decrypted at construction if it arrived encrypted.
    /// Zeroized on drop.
    private_key_pem: Zeroizing<String>,
    warehouse: String,
    database: String,
    schema: String,
    role: Option<String>,
    /// Server-side statement timeout (seconds) sent with each request.
    statement_timeout_secs: u64,
    /// Wall-clock budget for polling a single asynchronous statement.
    poll_timeout: Duration,
}

/// Top-level shape of a `/api/v2/statements` response (POST or GET status).
#[derive(Debug, Deserialize)]
struct StatementResponse {
    #[serde(rename = "resultSetMetaData")]
    result_set_meta_data: Option<ResultSetMetaData>,
    data: Option<Vec<Vec<JsonValue>>>,
    #[serde(rename = "statementHandle")]
    statement_handle: Option<String>,
    #[serde(rename = "statementStatusUrl")]
    statement_status_url: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResultSetMetaData {
    #[serde(rename = "rowType", default)]
    row_type: Vec<ColumnType>,
    #[serde(rename = "partitionInfo", default)]
    partition_info: Vec<PartitionInfo>,
}

#[derive(Debug, Deserialize)]
struct PartitionInfo {
    #[serde(rename = "rowCount")]
    #[allow(dead_code)]
    row_count: Option<i64>,
}

/// Body of a partition fetch (`?partition=N`) — data only, no metadata.
#[derive(Debug, Deserialize)]
struct PartitionResponse {
    data: Option<Vec<Vec<JsonValue>>>,
}

impl SnowflakeClient {
    /// Build a client from source options.
    ///
    /// Decrypts the private key up front when `private_key_passphrase` is set, so
    /// a bad passphrase or unusable key format fails here with a specific message
    /// rather than on the first request. Also fails on an HTTP client
    /// construction error.
    pub fn new(opts: &SourceOpts) -> Result<Self> {
        let private_key_pem = resolve_private_key_pem(
            &opts.private_key_pem,
            opts.private_key_passphrase.as_deref(),
        )
        .context("failed to load the Snowflake private key")?;

        let http = reqwest::Client::builder()
            .build()
            .context("failed to build HTTP client")?;

        let base_url = format!("https://{}.snowflakecomputing.com", opts.account);
        // Snowflake's key-pair JWT subject is the uppercased ACCOUNT.USER.
        let jwt_identity = format!(
            "{}.{}",
            opts.account.to_ascii_uppercase(),
            opts.user.to_ascii_uppercase()
        );

        Ok(Self {
            http,
            base_url,
            jwt_identity,
            private_key_pem,
            warehouse: opts.warehouse.clone(),
            database: opts.database.clone(),
            schema: opts.schema.clone(),
            role: opts.role.clone(),
            statement_timeout_secs: 300,
            poll_timeout: Duration::from_secs(600),
        })
    }

    /// Generate a fresh short-lived JWT. Regenerated per request so long syncs do
    /// not outlive a single token.
    fn jwt(&self) -> Result<String> {
        snowflake_jwt::generate_jwt_token(&self.private_key_pem, &self.jwt_identity)
            .map_err(|e| anyhow!("failed to generate Snowflake JWT: {e}"))
    }

    /// Execute a SQL statement and stream partitions one at a time.
    ///
    /// Peak source-side memory is roughly one Snowflake result partition (plus
    /// whatever the caller retains from [`QueryStream::next_batch`]).
    pub async fn execute_query_stream(&self, sql: &str) -> Result<QueryStream<'_>> {
        tracing::debug!("Snowflake execute (stream): {sql}");

        let mut body = serde_json::Map::new();
        body.insert("statement".into(), JsonValue::String(sql.to_string()));
        body.insert(
            "timeout".into(),
            JsonValue::Number(self.statement_timeout_secs.into()),
        );
        body.insert(
            "warehouse".into(),
            JsonValue::String(self.warehouse.clone()),
        );
        body.insert("database".into(), JsonValue::String(self.database.clone()));
        body.insert("schema".into(), JsonValue::String(self.schema.clone()));
        if let Some(role) = &self.role {
            body.insert("role".into(), JsonValue::String(role.clone()));
        }
        let body = JsonValue::Object(body);

        let url = format!("{}/api/v2/statements", self.base_url);
        let (status, resp) = self.send_post(&url, &body).await?;
        let resp = self.await_completion(status, resp).await?;

        let meta = resp
            .result_set_meta_data
            .ok_or_else(|| anyhow!("Snowflake response missing resultSetMetaData"))?;
        let columns = meta.row_type;
        let current = resp.data.unwrap_or_default();

        // Snowflake normally reports partitionInfo; when it is absent but rows
        // arrived inline, treat that as a single already-buffered partition.
        let partition_count = if meta.partition_info.is_empty() {
            if current.is_empty() {
                0
            } else {
                1
            }
        } else {
            meta.partition_info.len()
        };

        Ok(QueryStream {
            client: self,
            columns,
            handle: resp.statement_handle,
            partition_count,
            // Partition 0 is already in `current`; the next fetch (if any) is 1.
            next_partition: 1,
            current,
            current_offset: 0,
        })
    }

    /// Execute a SQL statement and return the fully-paginated result set.
    ///
    /// Convenience for small results (DDL, `INFORMATION_SCHEMA`, tests). Prefer
    /// [`Self::execute_query_stream`] for table ingestion.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let mut stream = self.execute_query_stream(sql).await?;
        let columns = stream.columns().to_vec();
        let mut rows = Vec::new();
        // Drain with a large batch size; partitions still arrive one at a time,
        // then are appended here (intentional full materialization).
        while let Some(mut batch) = stream.next_batch(10_000).await? {
            rows.append(&mut batch);
        }
        Ok(QueryResult { columns, rows })
    }

    async fn send_post(
        &self,
        url: &str,
        body: &JsonValue,
    ) -> Result<(reqwest::StatusCode, StatementResponse)> {
        let token = self.jwt()?;
        let resp = self
            .http
            .post(url)
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "surreal-sync")
            .json(body)
            .send()
            .await
            .context("Snowflake statement request failed")?;
        Self::parse_response(resp).await
    }

    async fn send_get(&self, url: &str) -> Result<(reqwest::StatusCode, StatementResponse)> {
        let token = self.jwt()?;
        let resp = self
            .http
            .get(url)
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "surreal-sync")
            .send()
            .await
            .context("Snowflake status request failed")?;
        Self::parse_response(resp).await
    }

    async fn parse_response(
        resp: reqwest::Response,
    ) -> Result<(reqwest::StatusCode, StatementResponse)> {
        let status = resp.status();
        let text = resp
            .text()
            .await
            .context("failed to read Snowflake response body")?;

        if status != reqwest::StatusCode::OK && status != reqwest::StatusCode::ACCEPTED {
            // Surface Snowflake's error message when present.
            let detail = serde_json::from_str::<StatementResponse>(&text)
                .ok()
                .and_then(|r| r.message)
                .unwrap_or_else(|| text.clone());
            bail!("Snowflake API error ({status}): {detail}");
        }

        let parsed: StatementResponse = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse Snowflake response ({status})"))?;
        Ok((status, parsed))
    }

    /// Poll a `202 Accepted` statement until it reports `200 OK` or the poll
    /// budget is exhausted.
    async fn await_completion(
        &self,
        mut status: reqwest::StatusCode,
        mut resp: StatementResponse,
    ) -> Result<StatementResponse> {
        if status == reqwest::StatusCode::OK {
            return Ok(resp);
        }

        let handle = resp
            .statement_handle
            .clone()
            .ok_or_else(|| anyhow!("async statement (202) missing statementHandle"))?;
        // Prefer the server-provided status path; fall back to the canonical one.
        let status_path = resp
            .statement_status_url
            .clone()
            .unwrap_or_else(|| format!("/api/v2/statements/{handle}"));
        let status_url = format!("{}{}", self.base_url, status_path);

        let started = Instant::now();
        let mut backoff = Duration::from_millis(500);
        while status == reqwest::StatusCode::ACCEPTED {
            if started.elapsed() > self.poll_timeout {
                bail!(
                    "timed out after {:?} waiting for async Snowflake statement {handle}",
                    self.poll_timeout
                );
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));

            let (s, r) = self.send_get(&status_url).await?;
            status = s;
            resp = r;
        }
        Ok(resp)
    }

    async fn fetch_partition(&self, handle: &str, partition: usize) -> Result<Vec<Vec<JsonValue>>> {
        let url = format!(
            "{}/api/v2/statements/{handle}?partition={partition}",
            self.base_url
        );
        let token = self.jwt()?;
        let resp = self
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "surreal-sync")
            .send()
            .await
            .with_context(|| format!("failed to fetch result partition {partition}"))?;

        let status = resp.status();
        let text = resp.text().await.context("failed to read partition body")?;
        if status != reqwest::StatusCode::OK {
            bail!("Snowflake partition {partition} error ({status}): {text}");
        }
        let parsed: PartitionResponse = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse result partition {partition}"))?;
        Ok(parsed.data.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    const PASSPHRASE: &str = "correct horse battery staple";

    /// Run `openssl` with optional stdin, returning stdout on success.
    ///
    /// Returns `None` when the `openssl` binary is missing or the command fails,
    /// so tests degrade to a skip rather than a false failure on a machine
    /// without it (or with an OpenSSL build lacking a given cipher).
    fn openssl(args: &[&str], stdin: Option<&[u8]>) -> Option<Vec<u8>> {
        let mut child = Command::new("openssl")
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        if let Some(bytes) = stdin {
            child.stdin.take()?.write_all(bytes).ok()?;
        }

        let out = child.wait_with_output().ok()?;
        out.status.success().then_some(out.stdout)
    }

    /// A 2048-bit RSA key as PKCS#1 PEM, generated once per test binary.
    ///
    /// Key generation is the slow part, so every case derives from this one key.
    fn pkcs1_key() -> Option<&'static [u8]> {
        use std::sync::OnceLock;
        static KEY: OnceLock<Option<Vec<u8>>> = OnceLock::new();
        KEY.get_or_init(|| openssl(&["genrsa", "2048"], None))
            .as_deref()
    }

    fn plain_pkcs8() -> Option<String> {
        let der = pkcs1_key()?;
        let out = openssl(
            &["pkcs8", "-topk8", "-inform", "PEM", "-nocrypt"],
            Some(der),
        )?;
        String::from_utf8(out).ok()
    }

    /// Encrypt the shared key with an explicit PBES2 cipher, exactly as the docs
    /// tell customers to (`-v2 aes-256-cbc` / `-v2 des3`).
    fn encrypted_pkcs8(cipher: &str) -> Option<String> {
        let der = pkcs1_key()?;
        let out = openssl(
            &[
                "pkcs8",
                "-topk8",
                "-inform",
                "PEM",
                "-v2",
                cipher,
                "-passout",
                &format!("pass:{PASSPHRASE}"),
            ],
            Some(der),
        )?;
        String::from_utf8(out).ok()
    }

    /// Skip with a message instead of failing when openssl can't produce a fixture.
    macro_rules! require {
        ($e:expr, $what:expr) => {
            match $e {
                Some(v) => v,
                None => {
                    eprintln!("skipping: openssl could not produce {}", $what);
                    return;
                }
            }
        };
    }

    /// An encrypted key must yield the same plaintext PKCS#8 as `-nocrypt` does.
    /// This is the property that makes the decrypted PEM usable by snowflake-jwt.
    #[test]
    fn decrypts_aes_256_cbc_to_the_same_key() {
        let expected = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");

        assert!(encrypted.contains(LABEL_ENCRYPTED), "fixture not encrypted");

        let resolved = resolve_private_key_pem(&encrypted, Some(PASSPHRASE)).unwrap();
        assert_eq!(normalize(&resolved), normalize(&expected));
    }

    /// Snowflake's own docs have historically used `-v2 des3`; the `3des` feature
    /// exists for exactly these keys.
    #[test]
    fn decrypts_3des() {
        let expected = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        let encrypted = require!(encrypted_pkcs8("des3"), "a 3DES key");

        let resolved = resolve_private_key_pem(&encrypted, Some(PASSPHRASE)).unwrap();
        assert_eq!(normalize(&resolved), normalize(&expected));
    }

    /// The decrypted PEM must actually drive JWT generation end to end -- that is
    /// the whole point, and it is what `snowflake-jwt` was rejecting before.
    #[test]
    fn decrypted_key_generates_the_same_jwt_as_the_plaintext_key() {
        let plain = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");

        let from_encrypted = resolve_private_key_pem(&encrypted, Some(PASSPHRASE)).unwrap();

        let a = snowflake_jwt::generate_jwt_token(&from_encrypted, "ACME.SYNC_USER")
            .expect("JWT from the decrypted key");
        let b = snowflake_jwt::generate_jwt_token(&plain, "ACME.SYNC_USER")
            .expect("JWT from the plaintext key");
        assert_eq!(a, b, "decrypted key produced a different signature");
    }

    #[test]
    fn plain_key_passes_through_unchanged() {
        let plain = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        let resolved = resolve_private_key_pem(&plain, None).unwrap();
        assert_eq!(*resolved, plain);
    }

    /// A stray passphrase on an unencrypted key warns but must not fail -- an
    /// operator with SNOWFLAKE_PRIVATE_KEY_PASSPHRASE exported globally should
    /// still be able to use a plaintext key.
    #[test]
    fn plain_key_with_passphrase_is_accepted() {
        let plain = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        assert!(resolve_private_key_pem(&plain, Some(PASSPHRASE)).is_ok());
    }

    #[test]
    fn wrong_passphrase_is_reported_as_such() {
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");
        let err = resolve_private_key_pem(&encrypted, Some("not the passphrase"))
            .expect_err("wrong passphrase must fail")
            .to_string();
        assert!(
            err.contains("passphrase is most likely wrong"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn encrypted_key_without_passphrase_says_to_supply_one() {
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");
        let err = resolve_private_key_pem(&encrypted, None)
            .expect_err("missing passphrase must fail")
            .to_string();
        assert!(
            err.contains("--private-key-passphrase"),
            "error should name the flag: {err}"
        );
    }

    #[test]
    fn pkcs1_key_is_told_to_convert() {
        // OpenSSL 3 emits PKCS#8 from `genrsa`, so ask for PKCS#1 explicitly --
        // customers with older keys really do have this format.
        let key = require!(pkcs1_key(), "an RSA key");
        let out = require!(
            openssl(&["rsa", "-traditional"], Some(key)),
            "a PKCS#1 key (openssl rsa -traditional)"
        );
        let pem = String::from_utf8(out).unwrap();
        assert!(
            pem.contains(LABEL_PKCS1),
            "fixture is not PKCS#1: {pem:.40}"
        );

        let err = resolve_private_key_pem(&pem, None)
            .expect_err("PKCS#1 must be rejected")
            .to_string();
        assert!(err.contains("PKCS#8"), "error should name PKCS#8: {err}");
        assert!(
            err.contains("openssl pkcs8"),
            "error should give a fix: {err}"
        );
    }

    #[test]
    fn legacy_traditional_encryption_is_named_explicitly() {
        // Synthesized: OpenSSL 3 will not emit this format any more, but keys
        // created years ago still look like this.
        let pem = "-----BEGIN RSA PRIVATE KEY-----\n\
                   Proc-Type: 4,ENCRYPTED\n\
                   DEK-Info: DES-EDE3-CBC,0123456789ABCDEF\n\n\
                   AAAA\n\
                   -----END RSA PRIVATE KEY-----\n";
        let err = resolve_private_key_pem(pem, Some(PASSPHRASE))
            .expect_err("traditional encryption must be rejected")
            .to_string();
        assert!(err.contains("traditional"), "unhelpful error: {err}");
        assert!(
            err.contains("openssl pkcs8"),
            "error should give a fix: {err}"
        );
    }

    #[test]
    fn garbage_input_is_rejected_clearly() {
        let err = resolve_private_key_pem("not a pem at all", None)
            .expect_err("garbage must fail")
            .to_string();
        assert!(err.contains("PEM"), "unhelpful error: {err}");
    }

    /// Build a `SourceOpts` that differs only in key material / passphrase.
    fn opts(private_key_pem: String, passphrase: Option<&str>) -> SourceOpts {
        SourceOpts {
            account: "demo-acct".into(),
            user: "SYNC_USER".into(),
            private_key_pem,
            private_key_passphrase: passphrase.map(str::to_string),
            warehouse: "WH".into(),
            database: "DB".into(),
            schema: "PUBLIC".into(),
            role: None,
            tables: vec!["T".into()],
            id_columns: vec![],
        }
    }

    /// The whole point, at the public boundary: constructing a client from an
    /// encrypted key now succeeds, and the JWT it mints is usable.
    #[test]
    fn client_accepts_an_encrypted_key() {
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");
        let client =
            SnowflakeClient::new(&opts(encrypted, Some(PASSPHRASE))).expect("client should build");
        client.jwt().expect("client should mint a JWT");
    }

    /// A bad passphrase must fail at construction, not on the first query.
    #[test]
    fn client_rejects_a_wrong_passphrase_up_front() {
        let encrypted = require!(encrypted_pkcs8("aes-256-cbc"), "an AES-256-CBC key");
        // Not `expect_err`: that needs `T: Debug`, and `SnowflakeClient` deliberately
        // does not derive Debug because it holds the decrypted private key.
        let err = match SnowflakeClient::new(&opts(encrypted, Some("wrong"))) {
            Ok(_) => panic!("wrong passphrase must fail"),
            Err(e) => format!("{e:#}"),
        };
        assert!(
            err.contains("failed to load the Snowflake private key"),
            "error should be contextualised: {err}"
        );
    }

    /// Regression: plaintext keys, the only thing that worked before, still do.
    #[test]
    fn client_still_accepts_a_plaintext_key() {
        let plain = require!(plain_pkcs8(), "a plaintext PKCS#8 key");
        let client = SnowflakeClient::new(&opts(plain, None)).expect("client should build");
        client.jwt().expect("client should mint a JWT");
    }

    /// Compare PEM bodies ignoring line-ending and trailing-newline differences
    /// between OpenSSL's output and `der`'s re-encoding.
    fn normalize(pem: &str) -> String {
        pem.replace("\r\n", "\n").trim().to_string()
    }
}
