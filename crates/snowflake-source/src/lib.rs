//! Snowflake ingestion source for surreal-sync.
//!
//! Performs a full, one-shot batch snapshot of selected Snowflake tables into
//! SurrealDB via the documented [SQL REST API v2] using key-pair (JWT) auth.
//! There is no CDC/incremental support and no durable source cursor — this is an
//! ingestion-only source. Rows still go through the shared transform/apply path
//! ([`run_full_sync_with_transforms`]) so `--transforms-config` batching and
//! `max_in_flight` apply within each table. Source reads stream **one Snowflake
//! result partition at a time**, sliced into `batch_size` apply chunks.
//!
//! # Usage
//! ```ignore
//! let opts = SourceOpts { /* account, user, private_key_pem, ... */ };
//! let client = SnowflakeClient::new(&opts)?;
//! run_full_sync(&client, &sink, &opts, &SyncOpts { batch_size: 1000, dry_run: false }).await?;
//! ```
//!
//! [SQL REST API v2]: https://docs.snowflake.com/en/developer-guide/sql-api/index

pub mod autoconf;
pub mod client;
pub mod full_sync;

pub use client::{QueryResult, QueryStream, SnowflakeClient};
pub use full_sync::{
    apply_query_result_with_transforms, apply_query_stream_with_transforms, migrate_table,
    migrate_table_with_transforms, run_full_sync, run_full_sync_with_transforms,
};

/// Connection + selection options for the Snowflake source (no clap types).
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// Account identifier usable in the host `{account}.snowflakecomputing.com`
    /// (e.g. `myorg-myaccount` or `xy12345.us-east-1`).
    pub account: String,
    /// Login name of the user whose key-pair is registered with Snowflake.
    pub user: String,
    /// Unencrypted PKCS#8 private key PEM contents.
    pub private_key_pem: String,
    /// Passphrase for an encrypted key. Currently unsupported (errors if set).
    pub private_key_passphrase: Option<String>,
    /// Virtual warehouse used to run the SELECTs.
    pub warehouse: String,
    /// Database to read from.
    pub database: String,
    /// Schema within the database (typically `PUBLIC`).
    pub schema: String,
    /// Optional role to assume for the session.
    pub role: Option<String>,
    /// Tables to ingest. Empty means all base tables in the schema.
    pub tables: Vec<String>,
    /// Columns forming the SurrealDB record ID. Empty means auto-generate a
    /// sequential per-table index.
    pub id_columns: Vec<String>,
}

/// Non-connection sync options.
#[derive(Clone, Debug)]
pub struct SyncOpts {
    /// Number of rows per read chunk fed into the apply window.
    pub batch_size: usize,
    /// When true, read and convert but do not write to SurrealDB.
    pub dry_run: bool,
}
