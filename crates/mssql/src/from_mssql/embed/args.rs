//! Clap argument types for SQL Server CDC (`sync` only).

use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;

/// Full-sync strategy. Interleaved CDC + watermarks is the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum SyncStrategy {
    /// Copy PK-ordered chunks while SQL Server CDC runs (signal-table watermarks).
    /// Requires CDC, SQL Server Agent, and a primary key on every table.
    #[default]
    InterleavedSnapshot,
    /// One SNAPSHOT-isolation read of each table, then CDC from that LSN.
    /// Writers are not locked. Requires `ALLOW_SNAPSHOT_ISOLATION`.
    SequentialSnapshot,
}

/// Default rows per keyset chunk.
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Whether `sync` runs an initial snapshot, streams only, or snapshots only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum SnapshotModeArg {
    /// Snapshot then continuous CDC (default).
    #[default]
    Initial,
    /// CDC only from the checkpoint store (no snapshot).
    Never,
    /// Snapshot only, then exit (no CDC tail).
    Only,
}

/// Source-shaped mssql commands (`sync` + flags).
#[derive(Subcommand)]
pub enum Commands {
    /// Snapshot and/or stream sync from SQL Server CDC
    Sync(Box<SyncArgs>),
}

/// Combined snapshot+stream sync for SQL Server.
#[derive(Args)]
pub struct SyncArgs {
    /// ADO.NET connection string (SQL auth, or IntegratedSecurity=true on Windows)
    #[arg(long, env = "MSSQL_CONNECTION_STRING")]
    pub connection_string: String,

    /// Tables to sync (`schema.table` or bare name for dbo). Empty means all user tables.
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Tables to treat as SurrealDB relations (comma-separated Surreal table names)
    #[arg(long, value_delimiter = ',')]
    pub relation_tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    pub to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    pub to_database: String,

    /// Snapshot phase: initial (snapshot then stream), never (stream only), only (snapshot then exit)
    #[arg(long, value_enum, default_value_t = SnapshotModeArg::default())]
    pub snapshot_mode: SnapshotModeArg,

    /// Stop the CDC tail after this duration (for example 3600s, 30m)
    #[arg(long, value_name = "DURATION")]
    pub timeout: Option<String>,

    /// Full-sync strategy (interleaved-snapshot is the default)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    pub strategy: SyncStrategy,

    /// Rows read per keyset chunk during snapshot
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    pub chunk_size: usize,

    /// Directory to persist snapshot and stream checkpoints
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    pub checkpoint_dir: Option<String>,

    /// SurrealDB table for persisting snapshot and stream checkpoints
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    pub checkpoints_surreal_table: Option<String>,

    /// TOML file describing the transform pipeline (`[[transforms]]`)
    #[arg(long, value_name = "PATH")]
    pub transforms_config: Option<PathBuf>,

    /// Emit DEFINE TABLE / FIELD / INDEX before copying (default is schemaless)
    #[arg(long)]
    pub schemafull: bool,

    #[command(flatten)]
    pub surreal: SurrealOpts,
}
