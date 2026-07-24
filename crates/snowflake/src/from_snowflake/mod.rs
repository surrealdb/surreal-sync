//! Snowflake ingestion source for surreal-sync.
//!
//! Performs a full, one-shot batch snapshot of selected Snowflake tables into
//! SurrealDB via the documented [SQL REST API v2] using key-pair (JWT) auth.
//! There is no CDC/incremental support and no durable source cursor — this is an
//! ingestion-only source. Rows still go through the shared transform/apply path
//! ([`full_sync::run_full_sync_with_transforms`]) so `--transforms-config` batching and
//! `max_in_flight` apply within each table. Source reads stream **one Snowflake
//! result partition at a time**, sliced into `batch_size` apply chunks.
//!
//! # Embed surface
//!
//! Public embed API is only [`run`], [`FlattenId`], [`InPlaceTransform`], and
//! [`Value`]:
//!
//! ```ignore
//! use surreal_sync_snowflake::from_snowflake::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>]).await?;
//! ```
//!
//! [SQL REST API v2]: https://docs.snowflake.com/en/developer-guide/sql-api/index

pub mod autoconf;
pub mod client;
pub(crate) mod embed;
pub mod full_sync;

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

/// Public embed surface: `run`, `FlattenId`, `InPlaceTransform`, `Value` only.
pub use embed::{run, FlattenId, InPlaceTransform, Value};

/// Stock CLI argv helpers (`Args`, `run_args_with_sink`). Not part of the embed API.
#[doc(hidden)]
pub mod cli {
    pub use super::embed::{run_args_with_sink, Args};
}
