//! Google BigQuery ingestion source for surreal-sync.
//!
//! Performs a full, one-shot batch snapshot of selected BigQuery tables into
//! SurrealDB via the documented [REST API v2] using service-account (JWT → OAuth2)
//! auth. There is no CDC/incremental support and no durable source cursor — this is
//! an ingestion-only source. Rows still go through the shared transform/apply path
//! ([`full_sync::run_full_sync_with_transforms`]) so `--transforms-config` batching
//! and `max_in_flight` apply within each table. Source reads stream **one result
//! page at a time**, sliced into `batch_size` apply chunks.
//!
//! # Embed surface
//!
//! Public embed API is only [`run`], [`FlattenId`], [`InPlaceTransform`], and
//! [`Value`]:
//!
//! ```ignore
//! use surreal_sync_bigquery::from_bigquery::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>]).await?;
//! ```
//!
//! [REST API v2]: https://cloud.google.com/bigquery/docs/reference/rest

pub mod auth;
pub mod autoconf;
pub mod client;
pub(crate) mod embed;
pub mod full_sync;

/// The public BigQuery API root. Anything else is treated as a local emulator.
pub const DEFAULT_API_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Connection + selection options for the BigQuery source (no clap types).
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// Project that owns the dataset being read.
    pub project_id: String,
    /// Dataset within the project.
    pub dataset: String,
    /// Project the query jobs run in, which is the project BigQuery bills for
    /// them. Defaults to [`Self::project_id`]. Set from `--billing-project-id`.
    pub job_project_id: Option<String>,
    /// Service-account JSON key contents. `None` means unauthenticated, which is
    /// only allowed against a non-Google [`Self::api_endpoint`] (i.e. an emulator).
    pub credentials_json: Option<String>,
    /// Dataset location (`US`, `EU`, `europe-west2`, …). Sent with every request
    /// when set; BigQuery infers it otherwise.
    pub location: Option<String>,
    /// API root. Defaults to [`DEFAULT_API_ENDPOINT`]; override to target an emulator.
    pub api_endpoint: String,
    /// Tables to ingest. Empty means all base tables in the dataset.
    pub tables: Vec<String>,
    /// Columns forming the SurrealDB record ID. Empty means auto-generate a
    /// sequential per-table index.
    pub id_columns: Vec<String>,
    /// Rows requested per result page. BigQuery also caps pages by response size,
    /// so a page may come back smaller than this.
    pub page_size: usize,
}

/// Rows per result page when the caller does not choose.
pub const DEFAULT_PAGE_SIZE: usize = 10_000;

impl SourceOpts {
    /// The project that query jobs run in, and so the project billed for them.
    pub fn job_project(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }

    /// Whether [`Self::api_endpoint`] points at Google rather than an emulator.
    ///
    /// Used to refuse anonymous access against the real API: silently reading
    /// nothing because a credential was missing is worse than failing loudly.
    pub fn is_public_endpoint(&self) -> bool {
        self.api_endpoint
            .trim_end_matches('/')
            .eq_ignore_ascii_case(DEFAULT_API_ENDPOINT)
            || self.api_endpoint.contains("googleapis.com")
    }
}

/// Non-connection sync options.
#[derive(Clone, Debug)]
pub struct SyncOpts {
    /// Number of rows per read chunk fed into the apply window.
    pub batch_size: usize,
    /// When true, read and convert but do not write to SurrealDB.
    pub dry_run: bool,
}

/// Public embed surface: `run`, `FlattenId`, `InPlaceTransform`, `Value` only.
pub use embed::{run, FlattenId, InPlaceTransform, Value};

/// Stock CLI argv helpers (`Args`, `run_args_with_sink`). Not part of the embed API.
#[doc(hidden)]
pub mod cli {
    pub use super::embed::{preflight, run_args_with_sink, Args};
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(api_endpoint: &str) -> SourceOpts {
        SourceOpts {
            project_id: "p".into(),
            dataset: "d".into(),
            job_project_id: None,
            credentials_json: None,
            location: None,
            api_endpoint: api_endpoint.into(),
            tables: Vec::new(),
            id_columns: Vec::new(),
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    #[test]
    fn job_project_defaults_to_project_id() {
        assert_eq!(opts(DEFAULT_API_ENDPOINT).job_project(), "p");
    }

    #[test]
    fn job_project_override_wins() {
        let mut o = opts(DEFAULT_API_ENDPOINT);
        o.job_project_id = Some("other-project".into());
        assert_eq!(o.job_project(), "other-project");
    }

    #[test]
    fn google_endpoints_are_public() {
        assert!(opts(DEFAULT_API_ENDPOINT).is_public_endpoint());
        assert!(opts("https://bigquery.googleapis.com/").is_public_endpoint());
    }

    #[test]
    fn emulator_endpoints_are_not_public() {
        assert!(!opts("http://127.0.0.1:9050").is_public_endpoint());
        assert!(!opts("http://bigquery-emulator:9050").is_public_endpoint());
    }
}
