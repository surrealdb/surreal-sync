//! Clap argument types for MySQL/MariaDB binlog CDC (`sync` / `snapshot`).
//!
//! Embedders parse these via [`Commands`] (source-shaped argv). The stock
//! `surreal-sync from mysql-binlog` binary nests the same types under `from`.

use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;

/// Full-sync strategy for sources that support interleaved snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum SyncStrategy {
    /// DBLog-style watermark snapshot copied concurrently/interleaved with the
    /// change stream: resumable, bounded memory, and does not pin the source
    /// change log for the whole snapshot (bounded retention). The default;
    /// requires a primary key on every table.
    #[default]
    InterleavedSnapshot,
    /// Monolithic `SELECT *` per table, then a separate replay of the [t1,t2]
    /// change log on top. The source log is pinned for the whole snapshot
    /// (unbounded retention). Opt-out for tables without a usable primary key
    /// or when writing watermark rows to the source is not allowed.
    SequentialSnapshot,
}

/// Default chunk size for the watermark snapshot (matches Debezium's
/// incremental snapshot default).
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Engine flavor override for binlog CDC (auto-detected from server when omitted).
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum FlavorArg {
    #[value(name = "mysql")]
    MySql,
    #[value(name = "mariadb")]
    MariaDb,
}

impl From<FlavorArg> for crate::from_binlog::Flavor {
    fn from(value: FlavorArg) -> Self {
        match value {
            FlavorArg::MySql => Self::MySql,
            FlavorArg::MariaDb => Self::MariaDb,
        }
    }
}

/// MariaDB `@slave_gtid_strict_mode` behavior for GTID resume sessions.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum MariaDbGtidStrictModeArg {
    #[default]
    ServerDefault,
    On,
    Off,
}

impl From<MariaDbGtidStrictModeArg> for crate::from_binlog::MariaDbGtidStrictMode {
    fn from(value: MariaDbGtidStrictModeArg) -> Self {
        match value {
            MariaDbGtidStrictModeArg::ServerDefault => Self::ServerDefault,
            MariaDbGtidStrictModeArg::On => Self::On,
            MariaDbGtidStrictModeArg::Off => Self::Off,
        }
    }
}

/// TLS behavior for MySQL/MariaDB SQL and binlog connections.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum TlsModeArg {
    #[default]
    Disabled,
    Preferred,
    Required,
}

#[derive(Clone, Debug, Args, Default)]
pub struct TlsArgs {
    /// TLS mode for MySQL connections (`disabled`, `preferred`, or `required`)
    #[arg(long, value_enum, default_value_t = TlsModeArg::default())]
    pub tls_mode: TlsModeArg,

    /// PEM CA bundle used to verify the MySQL server certificate (ignored when `--tls-mode disabled`)
    #[arg(long, value_name = "PATH")]
    pub tls_ca: Option<String>,

    /// PEM client certificate for MySQL TLS client auth (ignored when `--tls-mode disabled`)
    #[arg(long, value_name = "PATH")]
    pub tls_cert: Option<String>,

    /// PEM private key for MySQL TLS client auth (ignored when `--tls-mode disabled`; requires `--tls-cert`)
    #[arg(long, value_name = "PATH")]
    pub tls_key: Option<String>,
}

impl TlsArgs {
    pub fn ssl_mode(&self) -> crate::from_binlog::SslMode {
        let options = crate::from_binlog::SslOptions {
            ca: self.tls_ca.clone(),
            cert: self.tls_cert.clone(),
            key: self.tls_key.clone(),
        };
        match self.tls_mode {
            TlsModeArg::Disabled => crate::from_binlog::SslMode::Disabled,
            TlsModeArg::Preferred => crate::from_binlog::SslMode::Preferred(options),
            TlsModeArg::Required => crate::from_binlog::SslMode::Required(options),
        }
    }
}

/// Controls whether `sync` runs an initial snapshot, streams only, or snapshots only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum SnapshotModeArg {
    /// Interleaved snapshot then continuous stream (default).
    #[default]
    Initial,
    /// Stream only from `--from` or the checkpoint store (no snapshot).
    Never,
    /// Snapshot only, then exit (no stream).
    Only,
}

/// Source-shaped mysql-binlog commands (`sync` | `snapshot` + flags).
///
/// Nested under stock `from mysql-binlog`, or parsed at the top level via
/// [`super::run`] (which wraps this enum in a private [`clap::Parser`]).
#[derive(Subcommand)]
pub enum Commands {
    /// Snapshot and/or stream sync from MySQL/MariaDB binlog
    Sync(Box<SyncArgs>),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(SnapshotArgs),
}

/// Combined snapshot+stream sync for MySQL/MariaDB binlog.
#[derive(Args)]
pub struct SyncArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    pub connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    pub database: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Unique replica server id for this binlog consumer (random if omitted)
    #[arg(long)]
    pub server_id: Option<u32>,

    /// Override auto-detected engine flavor (mysql or mariadb)
    #[arg(long, value_enum)]
    pub flavor: Option<FlavorArg>,

    /// MariaDB @slave_gtid_strict_mode for GTID resume sessions
    #[arg(long, value_enum, default_value_t = MariaDbGtidStrictModeArg::default())]
    pub mariadb_gtid_strict_mode: MariaDbGtidStrictModeArg,

    #[command(flatten)]
    pub tls: TlsArgs,

    /// Target SurrealDB namespace
    #[arg(long)]
    pub to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    pub to_database: String,

    /// Snapshot phase: initial (snapshot then stream), never (stream only), only (snapshot then exit)
    #[arg(long, value_enum, default_value_t = SnapshotModeArg::default())]
    pub snapshot_mode: SnapshotModeArg,

    /// Stream resume/start position (`head` = current master, or a checkpoint string).
    /// Used with `never` and when resuming `initial`. If omitted, reads from the checkpoint store
    /// (falling back to head when empty).
    #[arg(long)]
    pub from: Option<String>,

    /// Stop the stream phase after this wall-clock duration (e.g. 3600s, 30m, 300).
    #[arg(long, value_name = "DURATION", conflicts_with = "stop_at")]
    pub stop_after: Option<String>,

    /// Stop the stream phase at this binlog checkpoint
    #[arg(long, value_name = "CHECKPOINT", conflicts_with = "stop_after")]
    pub stop_at: Option<String>,

    /// Full-sync strategy for the snapshot phase (interleaved-snapshot is the default)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    pub strategy: SyncStrategy,

    /// Rows read per keyset chunk during snapshot phases
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    pub chunk_size: usize,

    /// Directory to persist snapshot and stream checkpoints
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    pub checkpoint_dir: Option<String>,

    /// SurrealDB table for persisting snapshot and stream checkpoints
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    pub checkpoints_surreal_table: Option<String>,

    /// Persist stream checkpoints at this interval in seconds
    #[arg(long, default_value = "10")]
    pub checkpoint_interval: u64,

    /// Blocking read timeout for binlog packet polls during the replication tail (milliseconds)
    #[arg(long, default_value = "500")]
    pub binlog_poll_timeout_ms: u64,

    /// Sleep when a replication tail poll returns no events (milliseconds)
    #[arg(long, default_value = "100")]
    pub idle_sleep_ms: u64,

    /// Max events requested per binlog read in the replication tail loop
    #[arg(long, default_value_t = 32)]
    pub binlog_event_batch_size: usize,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    pub transforms_config: Option<PathBuf>,

    #[command(flatten)]
    pub surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for MySQL binlog by inserting
/// an execute-snapshot signal row.
#[derive(Args)]
pub struct SnapshotArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    pub connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    pub database: Option<String>,

    /// Unique replica server id for this binlog consumer (random if omitted)
    #[arg(long)]
    pub server_id: Option<u32>,

    /// Override auto-detected engine flavor (mysql or mariadb)
    #[arg(long, value_enum)]
    pub flavor: Option<FlavorArg>,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    pub tables: Vec<String>,

    #[command(flatten)]
    pub tls: TlsArgs,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_args_default_to_disabled() {
        let args = TlsArgs::default();
        assert!(matches!(
            args.ssl_mode(),
            crate::from_binlog::SslMode::Disabled
        ));
    }

    #[test]
    fn tls_args_preserve_ca_and_client_cert_paths() {
        let args = TlsArgs {
            tls_mode: TlsModeArg::Required,
            tls_ca: Some("ca.pem".to_string()),
            tls_cert: Some("client.pem".to_string()),
            tls_key: Some("client-key.pem".to_string()),
        };
        let crate::from_binlog::SslMode::Required(options) = args.ssl_mode() else {
            panic!("expected required TLS mode");
        };
        assert_eq!(options.ca.as_deref(), Some("ca.pem"));
        assert_eq!(options.cert.as_deref(), Some("client.pem"));
        assert_eq!(options.key.as_deref(), Some("client-key.pem"));
    }

    #[test]
    fn mariadb_gtid_strict_mode_arg_maps_to_source_option() {
        assert_eq!(
            crate::from_binlog::MariaDbGtidStrictMode::from(
                MariaDbGtidStrictModeArg::ServerDefault
            ),
            crate::from_binlog::MariaDbGtidStrictMode::ServerDefault
        );
        assert_eq!(
            crate::from_binlog::MariaDbGtidStrictMode::from(MariaDbGtidStrictModeArg::On),
            crate::from_binlog::MariaDbGtidStrictMode::On
        );
        assert_eq!(
            crate::from_binlog::MariaDbGtidStrictMode::from(MariaDbGtidStrictModeArg::Off),
            crate::from_binlog::MariaDbGtidStrictMode::Off
        );
    }
}
