//! BigQuery REST API v2 client (service-account OAuth2 auth).
//!
//! Handles statement submission, incomplete-job polling, and result-page
//! pagination. Decoding result cells into `Value`s is the job of
//! [`crate::types`]; this module only produces the raw `(schema, rows)` pair.
//!
//! Large results are exposed via [`QueryStream`], which keeps **one result page**
//! in memory at a time and yields bounded [`QueryStream::next_batch`] slices for
//! the apply path.
//!
//! # References
//! - <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query>
//! - <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults>

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::auth::{Credentials, ServiceAccountKey, TokenProvider};
use super::SourceOpts;
use crate::types::FieldSchema;

/// How long the server may block waiting for a query before replying `jobComplete:
/// false` and handing us a job to poll.
const QUERY_TIMEOUT_MS: u64 = 30_000;

/// A decoded (but not yet type-converted) result set: column metadata plus every
/// data row across all pages.
///
/// Prefer [`BigQueryClient::execute_query_stream`] for large tables — this type
/// materializes the full result and is mainly for small queries (discovery, tests).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Per-column metadata (`schema.fields`).
    pub schema: Vec<FieldSchema>,
    /// Rows, each a vector of raw JSON cells aligned with `schema`.
    pub rows: Vec<Vec<JsonValue>>,
}

/// Streaming view of a query result: one page buffered at a time, sliced into
/// caller-sized batches.
pub struct QueryStream<'a> {
    client: &'a BigQueryClient,
    schema: Vec<FieldSchema>,
    /// Job handle used to fetch subsequent pages.
    job_id: Option<String>,
    /// Job location, echoed back on every `getQueryResults` call.
    location: Option<String>,
    /// Token for the next page, or `None` when the result is exhausted.
    page_token: Option<String>,
    /// Rows requested per page.
    page_size: usize,
    /// Rows for the page currently being drained.
    current: Vec<Vec<JsonValue>>,
    /// Offset within [`Self::current`].
    current_offset: usize,
}

impl QueryStream<'_> {
    /// Column metadata for the query (stable for the life of the stream).
    pub fn schema(&self) -> &[FieldSchema] {
        &self.schema
    }

    /// Yield up to `max_rows` raw cells from the current page, fetching the next
    /// page only when the buffer is empty.
    ///
    /// Returns `Ok(None)` when the result is fully consumed. A returned batch may
    /// be smaller than `max_rows` when a page ends mid-batch (callers should treat
    /// that as a normal, final partial batch for that page).
    pub async fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<JsonValue>>>> {
        let max_rows = max_rows.max(1);
        loop {
            if self.current_offset >= self.current.len() {
                self.current.clear();
                self.current_offset = 0;
                if !self.fetch_next_page_if_needed().await? {
                    return Ok(None);
                }
                // Empty pages are skipped by continuing the loop.
                continue;
            }

            let end = (self.current_offset + max_rows).min(self.current.len());
            let batch = self.current[self.current_offset..end].to_vec();
            self.current_offset = end;

            // Drop the page buffer once fully drained so peak memory stays near one
            // page (+ the in-flight apply window).
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

    async fn fetch_next_page_if_needed(&mut self) -> Result<bool> {
        let Some(page_token) = self.page_token.clone() else {
            return Ok(false);
        };
        let job_id = self
            .job_id
            .as_deref()
            .ok_or_else(|| anyhow!("paginated result is missing jobReference.jobId"))?;

        tracing::debug!("Fetching the next BigQuery result page");
        let response = self
            .client
            .get_query_results(
                job_id,
                self.location.as_deref(),
                Some(&page_token),
                self.page_size,
            )
            .await?;

        self.current = decode_rows(response.rows.unwrap_or_default());
        self.page_token = response.page_token.filter(|t| !t.is_empty());
        self.current_offset = 0;
        Ok(true)
    }
}

/// Client for a single BigQuery project, bound to one dataset.
pub struct BigQueryClient {
    http: reqwest::Client,
    token: TokenProvider,
    /// API root without a trailing slash.
    api_endpoint: String,
    /// Project billed for query jobs (the one in the request path).
    billing_project: String,
    /// Project that owns the dataset being read.
    project_id: String,
    dataset: String,
    location: Option<String>,
    /// Rows requested per result page.
    page_size: usize,
    /// Wall-clock budget for polling a single incomplete query job.
    poll_timeout: Duration,
}

/// Shape of a `jobs.query` / `jobs.getQueryResults` response.
#[derive(Debug, Deserialize)]
struct QueryResponse {
    #[serde(default)]
    schema: Option<TableSchema>,
    #[serde(default)]
    rows: Option<Vec<ResultRow>>,
    #[serde(rename = "jobComplete", default)]
    job_complete: Option<bool>,
    #[serde(rename = "jobReference", default)]
    job_reference: Option<JobReference>,
    #[serde(rename = "pageToken", default)]
    page_token: Option<String>,
    #[serde(default)]
    errors: Option<Vec<ErrorProto>>,
}

#[derive(Debug, Deserialize)]
struct TableSchema {
    #[serde(default)]
    fields: Vec<FieldSchema>,
}

#[derive(Debug, Deserialize)]
struct ResultRow {
    #[serde(default)]
    f: Vec<Cell>,
}

#[derive(Debug, Deserialize)]
struct Cell {
    #[serde(default)]
    v: JsonValue,
}

#[derive(Debug, Deserialize)]
struct JobReference {
    #[serde(rename = "jobId", default)]
    job_id: Option<String>,
    #[serde(default)]
    location: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorProto {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

/// Top-level `{"error": {...}}` envelope Google returns on HTTP failures.
#[derive(Debug, Deserialize)]
struct ApiErrorEnvelope {
    #[serde(default)]
    error: Option<ApiError>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(default)]
    message: Option<String>,
}

/// Flatten `[{"f": [{"v": …}, …]}]` into `[[…]]`, dropping the envelopes the
/// conversion layer does not need.
fn decode_rows(rows: Vec<ResultRow>) -> Vec<Vec<JsonValue>> {
    rows.into_iter()
        .map(|row| row.f.into_iter().map(|cell| cell.v).collect())
        .collect()
}

impl BigQueryClient {
    /// Build a client from source options.
    ///
    /// Fails fast when credentials are missing for the public API — reading zero
    /// rows because a key was not configured is a worse outcome than an error.
    pub fn new(opts: &SourceOpts) -> Result<Self> {
        let credentials = match opts.credentials_json.as_deref() {
            Some(json) => {
                Credentials::ServiceAccount(Box::new(ServiceAccountKey::from_json(json)?))
            }
            None if opts.is_public_endpoint() => bail!(
                "no BigQuery credentials configured; pass --credentials-path or set \
                 GOOGLE_APPLICATION_CREDENTIALS (anonymous access is only allowed \
                 against a custom --api-endpoint such as an emulator)"
            ),
            None => {
                tracing::warn!(
                    endpoint = %opts.api_endpoint,
                    "Connecting to BigQuery without credentials (non-public endpoint)"
                );
                Credentials::Anonymous
            }
        };

        if !matches!(credentials, Credentials::Anonymous) {
            ensure_secure_url(&opts.api_endpoint)?;
        }

        let http = reqwest::Client::builder()
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            token: TokenProvider::new(credentials, http.clone()),
            http,
            api_endpoint: opts.api_endpoint.trim_end_matches('/').to_string(),
            billing_project: opts.billing_project().to_string(),
            project_id: opts.project_id.clone(),
            dataset: opts.dataset.clone(),
            location: opts.location.clone(),
            page_size: opts.page_size.max(1),
            poll_timeout: Duration::from_secs(600),
        })
    }

    /// Project that owns the dataset being read.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Dataset being read.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// Execute a SQL statement and stream result pages one at a time.
    ///
    /// `jobs.query` only starts the job here (`maxResults: 0`); every row is then
    /// read through `jobs.getQueryResults`. That extra round trip is deliberate:
    /// the BigQuery emulator used in CI ignores `maxResults` on `jobs.query` and
    /// returns the whole result inline with no `pageToken`, so routing all rows
    /// through `getQueryResults` is what makes CI actually exercise the pagination
    /// loop instead of silently taking a single-page path. It is also the sequence
    /// Google's own client libraries use.
    ///
    /// Peak source-side memory is roughly one result page (plus whatever the
    /// caller retains from [`QueryStream::next_batch`]).
    pub async fn execute_query_stream(&self, sql: &str) -> Result<QueryStream<'_>> {
        tracing::debug!("BigQuery execute (stream): {sql}");

        let mut body = serde_json::Map::new();
        body.insert("query".into(), JsonValue::String(sql.to_string()));
        body.insert("useLegacySql".into(), JsonValue::Bool(false));
        body.insert("maxResults".into(), JsonValue::Number(0.into()));
        body.insert(
            "timeoutMs".into(),
            JsonValue::Number(QUERY_TIMEOUT_MS.into()),
        );
        body.insert(
            "formatOptions".into(),
            serde_json::json!({ "useInt64Timestamp": true }),
        );
        if let Some(location) = &self.location {
            body.insert("location".into(), JsonValue::String(location.clone()));
        }

        let url = format!(
            "{}/bigquery/v2/projects/{}/queries",
            self.api_endpoint, self.billing_project
        );
        let started = self.send_post(&url, &JsonValue::Object(body)).await?;
        check_job_errors(&started)?;

        let job_reference = started
            .job_reference
            .ok_or_else(|| anyhow!("BigQuery query response is missing jobReference"))?;
        let job_id = job_reference
            .job_id
            .ok_or_else(|| anyhow!("BigQuery query response is missing jobReference.jobId"))?;
        // Prefer the job's own location so `getQueryResults` addresses the right
        // region even when the caller left --location unset.
        let location = job_reference.location.or_else(|| self.location.clone());

        let first = self.await_completion(&job_id, location.as_deref()).await?;

        let schema = first
            .schema
            .map(|s| s.fields)
            .ok_or_else(|| anyhow!("BigQuery response is missing the result schema"))?;

        Ok(QueryStream {
            client: self,
            schema,
            job_id: Some(job_id),
            location,
            page_token: first.page_token.filter(|t| !t.is_empty()),
            page_size: self.page_size,
            current: decode_rows(first.rows.unwrap_or_default()),
            current_offset: 0,
        })
    }

    /// Execute a SQL statement and return the fully-paginated result set.
    ///
    /// Convenience for small results (discovery, tests). Prefer
    /// [`Self::execute_query_stream`] for table ingestion.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let page_size = self.page_size;
        let mut stream = self.execute_query_stream(sql).await?;
        let schema = stream.schema().to_vec();
        let mut rows = Vec::new();
        // Drain with a large batch size; pages still arrive one at a time, then are
        // appended here (intentional full materialization).
        while let Some(mut batch) = stream.next_batch(page_size).await? {
            rows.append(&mut batch);
        }
        Ok(QueryResult { schema, rows })
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/tables` — the discovery
    /// fallback used when `INFORMATION_SCHEMA` is unavailable.
    pub async fn list_dataset_tables(&self) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct TableList {
            #[serde(default)]
            tables: Vec<TableListEntry>,
            #[serde(rename = "nextPageToken", default)]
            next_page_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct TableListEntry {
            #[serde(rename = "tableReference", default)]
            table_reference: Option<TableReference>,
            #[serde(rename = "type", default)]
            table_type: Option<String>,
        }
        #[derive(Deserialize)]
        struct TableReference {
            #[serde(rename = "tableId", default)]
            table_id: Option<String>,
        }

        let base = format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables",
            self.api_endpoint, self.project_id, self.dataset
        );

        let mut names = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = base.clone();
            if let Some(token) = &page_token {
                url.push_str(&format!("?pageToken={token}"));
            }

            let body = self.send_get(&url).await?;
            let page: TableList =
                serde_json::from_str(&body).context("failed to parse the table list")?;

            for entry in page.tables {
                // Views and external tables are not snapshot sources.
                if entry
                    .table_type
                    .as_deref()
                    .is_some_and(|t| !t.eq_ignore_ascii_case("TABLE"))
                {
                    continue;
                }
                if let Some(id) = entry.table_reference.and_then(|r| r.table_id) {
                    names.push(id);
                }
            }

            page_token = page.next_page_token.filter(|t| !t.is_empty());
            if page_token.is_none() {
                break;
            }
        }

        names.sort();
        Ok(names)
    }

    /// Whether this request may travel over a cleartext connection.
    ///
    /// An emulator is reached over plain HTTP, which is safe precisely because
    /// nothing sensitive is sent to it: an anonymous client carries no
    /// `Authorization` header and no credential of any kind. As soon as there is a
    /// token to attach, the transport has to be encrypted.
    fn cleartext_is_allowed(&self) -> bool {
        self.token.is_anonymous()
    }

    /// Attach credentials to a request whose transport has already been checked by
    /// the caller.
    async fn authorize(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        let builder = builder
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "surreal-sync");

        match self.token.access_token().await? {
            Some(token) => Ok(builder.header("Authorization", format!("Bearer {token}"))),
            None => Ok(builder),
        }
    }

    async fn send_post(&self, url: &str, body: &JsonValue) -> Result<QueryResponse> {
        // Guarded here, next to the request itself, so the rule holds no matter how
        // the URL was built.
        if !self.cleartext_is_allowed() && !url.starts_with("https://") {
            return Err(cleartext_refusal(url));
        }

        let request = self
            .authorize(self.http.post(url))
            .await?
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(body);
        let response = request
            .send()
            .await
            .context("BigQuery query request failed")?;
        let text = Self::read_body(response).await?;
        serde_json::from_str(&text).context("failed to parse the BigQuery query response")
    }

    async fn send_get(&self, url: &str) -> Result<String> {
        if !self.cleartext_is_allowed() && !url.starts_with("https://") {
            return Err(cleartext_refusal(url));
        }

        let response = self
            .authorize(self.http.get(url))
            .await?
            .send()
            .await
            .with_context(|| format!("BigQuery GET {url} failed"))?;
        Self::read_body(response).await
    }

    async fn get_query_results(
        &self,
        job_id: &str,
        location: Option<&str>,
        page_token: Option<&str>,
        max_results: usize,
    ) -> Result<QueryResponse> {
        let mut url = format!(
            "{}/bigquery/v2/projects/{}/queries/{}?maxResults={}&timeoutMs={}&formatOptions.useInt64Timestamp=true",
            self.api_endpoint, self.billing_project, job_id, max_results, QUERY_TIMEOUT_MS
        );
        if let Some(location) = location {
            url.push_str(&format!("&location={location}"));
        }
        if let Some(token) = page_token {
            url.push_str(&format!("&pageToken={token}"));
        }

        let text = self.send_get(&url).await?;
        let parsed: QueryResponse =
            serde_json::from_str(&text).context("failed to parse the BigQuery results response")?;
        check_job_errors(&parsed)?;
        Ok(parsed)
    }

    /// Read a response body, converting a non-2xx status into an error carrying
    /// Google's own message rather than the raw payload.
    async fn read_body(response: reqwest::Response) -> Result<String> {
        let status = response.status();
        let text = response
            .text()
            .await
            .context("failed to read the BigQuery response body")?;

        if !status.is_success() {
            let detail = serde_json::from_str::<ApiErrorEnvelope>(&text)
                .ok()
                .and_then(|e| e.error)
                .and_then(|e| e.message)
                .unwrap_or_else(|| text.clone());
            bail!("BigQuery API error ({status}): {detail}");
        }

        Ok(text)
    }

    /// Fetch the first result page, polling while the job is still running.
    ///
    /// `jobs.query` may return before the job finishes, and it returns no rows at
    /// all here because we submit it with `maxResults: 0`, so the first page always
    /// comes from `getQueryResults`.
    async fn await_completion(
        &self,
        job_id: &str,
        location: Option<&str>,
    ) -> Result<QueryResponse> {
        let mut first = self
            .get_query_results(job_id, location, None, self.page_size)
            .await?;
        if first.job_complete.unwrap_or(true) {
            return Ok(first);
        }

        let started = Instant::now();
        let mut backoff = Duration::from_millis(500);
        loop {
            if started.elapsed() > self.poll_timeout {
                bail!(
                    "timed out after {:?} waiting for BigQuery job {job_id}",
                    self.poll_timeout
                );
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));

            first = self
                .get_query_results(job_id, location, None, self.page_size)
                .await?;
            if first.job_complete.unwrap_or(true) {
                return Ok(first);
            }
        }
    }
}

/// Reject a URL that would carry credentials in cleartext.
///
/// Used by [`BigQueryClient::new`] so the CLI preflight reports the mistake before
/// it dials SurrealDB. The per-request paths repeat the scheme test inline rather
/// than calling this, so the guard sits in the same function as the request it
/// protects.
fn ensure_secure_url(url: &str) -> Result<()> {
    if !url.starts_with("https://") {
        return Err(cleartext_refusal(url));
    }
    Ok(())
}

/// The single wording for every refusal, so the three call sites stay consistent.
fn cleartext_refusal(url: &str) -> anyhow::Error {
    anyhow!(
        "refusing to send BigQuery credentials over a cleartext connection ({url}). \
         Use an https:// --api-endpoint, or omit credentials entirely when \
         targeting a local emulator."
    )
}

/// Surface per-job errors, which BigQuery reports in a 200 response body.
fn check_job_errors(response: &QueryResponse) -> Result<()> {
    let Some(errors) = response.errors.as_ref().filter(|e| !e.is_empty()) else {
        return Ok(());
    };
    let detail = errors
        .iter()
        .map(|e| match (&e.message, &e.reason) {
            (Some(m), Some(r)) => format!("{m} ({r})"),
            (Some(m), None) => m.clone(),
            (None, Some(r)) => r.clone(),
            (None, None) => "unknown error".to_string(),
        })
        .collect::<Vec<_>>()
        .join("; ");
    bail!("BigQuery job failed: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opts() -> SourceOpts {
        SourceOpts {
            project_id: "demo".into(),
            dataset: "app".into(),
            billing_project_id: None,
            credentials_json: None,
            location: None,
            api_endpoint: "http://127.0.0.1:9050".into(),
            tables: Vec::new(),
            id_columns: Vec::new(),
            page_size: crate::from_bigquery::DEFAULT_PAGE_SIZE,
        }
    }

    #[test]
    fn anonymous_is_allowed_against_a_custom_endpoint() {
        let client = BigQueryClient::new(&opts()).expect("emulator client");
        assert_eq!(client.project_id(), "demo");
        assert_eq!(client.dataset(), "app");
    }

    #[test]
    fn anonymous_is_refused_against_the_public_api() {
        let mut o = opts();
        o.api_endpoint = crate::from_bigquery::DEFAULT_API_ENDPOINT.into();
        let err = match BigQueryClient::new(&o) {
            Ok(_) => panic!("anonymous access to the public API should be refused"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("--credentials-path"), "got: {err}");
    }

    fn service_account_json() -> String {
        json!({
            "type": "service_account",
            "client_email": "sync@demo.iam.gserviceaccount.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
            "token_uri": "https://oauth2.googleapis.com/token"
        })
        .to_string()
    }

    #[test]
    fn credentials_are_refused_over_a_cleartext_endpoint() {
        // The emulator escape hatch must not become a way to leak a real token:
        // http:// plus credentials is rejected outright.
        let mut o = opts();
        o.credentials_json = Some(service_account_json());
        let err = match BigQueryClient::new(&o) {
            Ok(_) => panic!("credentials over http should be refused"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("cleartext"), "got: {err}");
    }

    #[test]
    fn credentials_are_allowed_over_a_custom_https_endpoint() {
        let mut o = opts();
        o.credentials_json = Some(service_account_json());
        o.api_endpoint = "https://bigquery.example.internal".into();
        assert!(BigQueryClient::new(&o).is_ok());
    }

    #[test]
    fn anonymous_clients_may_use_a_cleartext_emulator_endpoint() {
        // Nothing sensitive is on the wire without credentials, so plain HTTP is
        // fine here. This is what keeps the emulator usable.
        let client = BigQueryClient::new(&opts()).expect("emulator client");
        assert!(client.cleartext_is_allowed());
    }

    #[test]
    fn ensure_secure_url_only_accepts_https() {
        assert!(ensure_secure_url("https://bigquery.googleapis.com").is_ok());
        assert!(ensure_secure_url("http://bigquery.googleapis.com").is_err());
        assert!(ensure_secure_url("http://127.0.0.1:9050").is_err());
        assert!(ensure_secure_url("bigquery.googleapis.com").is_err());
    }

    #[test]
    fn trailing_slash_is_trimmed_from_the_endpoint() {
        let mut o = opts();
        o.api_endpoint = "http://127.0.0.1:9050/".into();
        let client = BigQueryClient::new(&o).unwrap();
        assert_eq!(client.api_endpoint, "http://127.0.0.1:9050");
    }

    #[test]
    fn decode_rows_strips_the_f_and_v_envelopes() {
        let response: QueryResponse = serde_json::from_value(json!({
            "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "1"}]}, {"f": [{"v": "2"}]}],
            "jobComplete": true
        }))
        .unwrap();
        let rows = decode_rows(response.rows.unwrap());
        assert_eq!(rows, vec![vec![json!("1")], vec![json!("2")]]);
    }

    #[test]
    fn decode_rows_preserves_null_cells() {
        let response: QueryResponse = serde_json::from_value(json!({
            "rows": [{"f": [{"v": null}, {"v": "x"}]}],
            "jobComplete": true
        }))
        .unwrap();
        let rows = decode_rows(response.rows.unwrap());
        assert_eq!(rows, vec![vec![JsonValue::Null, json!("x")]]);
    }

    #[test]
    fn job_errors_in_a_200_body_are_surfaced() {
        let response: QueryResponse = serde_json::from_value(json!({
            "jobComplete": true,
            "errors": [{"message": "Table not found: demo.app.missing", "reason": "notFound"}]
        }))
        .unwrap();
        let err = check_job_errors(&response).unwrap_err().to_string();
        assert!(err.contains("Table not found"), "got: {err}");
        assert!(err.contains("notFound"), "got: {err}");
    }

    #[test]
    fn an_empty_errors_array_is_not_a_failure() {
        let response: QueryResponse = serde_json::from_value(json!({
            "jobComplete": true,
            "errors": []
        }))
        .unwrap();
        assert!(check_job_errors(&response).is_ok());
    }

    #[test]
    fn missing_job_complete_is_treated_as_complete() {
        let response: QueryResponse = serde_json::from_value(json!({
            "schema": {"fields": []},
            "rows": []
        }))
        .unwrap();
        assert!(response.job_complete.unwrap_or(true));
    }
}
