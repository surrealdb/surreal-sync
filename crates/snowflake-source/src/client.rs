//! Snowflake SQL REST API v2 client (key-pair JWT auth).
//!
//! Handles JWT generation, statement submission, asynchronous (`202`) polling,
//! and result-partition pagination. Result decoding into `UniversalValue`s is
//! the job of the `snowflake-types` crate; this module only produces the raw
//! `(rowType, data)` pair.
//!
//! Large results are exposed via [`QueryStream`], which keeps **one partition**
//! in memory at a time and yields bounded [`QueryStream::next_batch`] slices for
//! the apply path.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use snowflake_types::ColumnType;

use crate::SourceOpts;

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
    private_key_pem: String,
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
    /// Build a client from source options. Fails fast on an encrypted private key
    /// (not yet supported) or an HTTP client construction error.
    pub fn new(opts: &SourceOpts) -> Result<Self> {
        if opts.private_key_passphrase.is_some() {
            bail!(
                "encrypted private keys are not supported yet; \
                 provide an unencrypted PKCS#8 private key via --private-key-path"
            );
        }

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
            private_key_pem: opts.private_key_pem.clone(),
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
