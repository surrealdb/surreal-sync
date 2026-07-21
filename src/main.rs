//! Command-line interface for surreal-sync
//!
//! # Usage Examples
//!
//! ## Full Sync
//! ```bash
//! # MongoDB full sync
//! surreal-sync from mongodb full \
//!   --connection-string mongodb://localhost:27017 \
//!   --database mydb \
//!   --to-namespace test --to-database test
//!
//! # Neo4j full sync with checkpoints
//! surreal-sync from neo4j full \
//!   --connection-string bolt://localhost:7687 \
//!   --to-namespace test --to-database test \
//!   --emit-checkpoints
//! ```
//!
//! ## Incremental Sync
//! ```bash
//! # MongoDB incremental sync from checkpoint
//! surreal-sync from mongodb incremental \
//!   --connection-string mongodb://localhost:27017 \
//!   --database mydb \
//!   --to-namespace test --to-database test \
//!   --incremental-from "mongodb::2024-01-01T00:00:00Z"
//!
//! # Neo4j incremental sync between checkpoints
//! surreal-sync from neo4j incremental \
//!   --connection-string bolt://localhost:7687 \
//!   --to-namespace test --to-database test \
//!   --incremental-from "neo4j:2024-01-01T10:00:00Z" \
//!   --incremental-to "neo4j:2024-01-01T12:00:00Z"
//! ```
//!
//! ## Load Testing
//! ```bash
//! # Populate MySQL with test data
//! surreal-sync loadtest populate mysql \
//!   --schema loadtest_schema.yaml \
//!   --row-count 1000 \
//!   --mysql-connection-string "mysql://root:root@localhost:3306/testdb"
//!
//! # Verify synced data in SurrealDB
//! surreal-sync loadtest verify \
//!   --schema loadtest_schema.yaml \
//!   --row-count 1000 \
//!   --surreal-endpoint ws://localhost:8000
//! ```
//!
//! ## Config File (PostgreSQL)
//!
//! PostgreSQL sources support `-c` / `--config-file` to replace CLI flags with a TOML file:
//!
//! ```bash
//! # All PostgreSQL settings from a config file
//! surreal-sync from postgresql-trigger full -c surreal-sync.toml
//! surreal-sync from postgresql-trigger incremental -c surreal-sync.toml
//! surreal-sync from postgresql full -c surreal-sync.toml
//! surreal-sync from postgresql incremental -c surreal-sync.toml
//! ```
//!
//! Example `surreal-sync.toml`:
//! ```toml
//! [source.postgresql]
//! connection_string = "postgresql://user:pass@host:5432/mydb"
//! tables = ["users", "orders"]
//! checkpoint_dir = "./checkpoints"
//!
//! [sink.surrealdb]
//! endpoint = "http://localhost:8000"
//! namespace = "test"
//! database = "test"
//! ```
//!
//! CLI flags take precedence over config file values when both are provided.
//!
//! ## Checkpoint Formats
//! - Neo4j: `neo4j:2024-01-01T00:00:00Z` (timestamp-based)
//! - MongoDB: `mongodb:base64token:2024-01-01T00:00:00Z` (resume token + timestamp)
//! - PostgreSQL: `postgresql:sequence:123` (trigger-based audit table)
//! - MySQL: `mysql:sequence:456` (trigger-based audit table)

use clap::{Args, Parser, Subcommand, ValueEnum};
use rustls::crypto::CryptoProvider;
use std::path::PathBuf;
use surreal_sync::SurrealOpts;

/// Full-sync strategy for sources that support interleaved snapshot
/// (PostgreSQL wal2json, PostgreSQL trigger, MySQL).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
enum SyncStrategy {
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
const DEFAULT_CHUNK_SIZE: usize = 1024;

// Load testing imports
use loadtest_populate_csv::CSVPopulateArgs;
use loadtest_populate_jsonl::JSONLPopulateArgs;
use loadtest_populate_kafka::KafkaPopulateArgs;
use loadtest_populate_mongodb::MongoDBPopulateArgs;
use loadtest_populate_mysql::MySQLPopulateArgs;
use loadtest_populate_neo4j::Neo4jPopulateArgs;
use loadtest_populate_postgresql::PostgreSQLPopulateArgs;
use loadtest_verify_surreal2::VerifyArgs;

// Load testing distributed imports
use loadtest_distributed::{AggregateServerArgs, GenerateArgs};

// Configuration utilities
mod config;

// Source-specific sync implementations
mod from;

// Loadtest command handlers
mod loadtest;

#[derive(Parser)]
#[command(name = "surreal-sync")]
#[command(version)]
#[command(about = "A tool for syncing data FROM various sources TO SurrealDB")]
#[command(long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync data FROM a source database TO SurrealDB
    From {
        #[command(subcommand)]
        source: Box<FromSource>,
    },

    /// Load testing utilities for populating and verifying test data
    Loadtest {
        #[command(subcommand)]
        command: Box<LoadtestCommand>,
    },
}

/// Available source databases for the `from` command
#[derive(Subcommand)]
enum FromSource {
    /// Sync from MongoDB (supports full and incremental)
    #[command(name = "mongodb")]
    MongoDB {
        #[command(subcommand)]
        command: MongoDBCommands,
    },

    /// Sync from Neo4j (supports full and incremental)
    #[command(name = "neo4j")]
    Neo4j {
        #[command(subcommand)]
        command: Neo4jCommands,
    },

    /// Sync from PostgreSQL using trigger-based CDC (supports full and incremental)
    #[command(name = "postgresql-trigger")]
    PostgreSQLTrigger {
        #[command(subcommand)]
        command: PostgreSQLTriggerCommands,
    },

    /// Sync from MySQL using trigger-based CDC (supports full and incremental)
    #[command(name = "mysql")]
    MySQL {
        #[command(subcommand)]
        command: MySQLCommands,
    },

    /// Sync from PostgreSQL using WAL-based logical replication (supports full and incremental)
    #[command(name = "postgresql")]
    PostgreSQL {
        #[command(subcommand)]
        command: PostgreSQLLogicalCommands,
    },

    /// Sync from MySQL/MariaDB using binlog CDC (supports full and incremental)
    #[command(name = "mysql-binlog")]
    MySQLBinlog {
        #[command(subcommand)]
        command: MySQLBinlogCommands,
    },

    /// Sync from PostgreSQL using pgoutput logical replication (WAL CDC)
    #[command(name = "postgresql-pgoutput")]
    PostgreSQLPgoutput {
        #[command(subcommand)]
        command: PostgreSQLPgoutputCommands,
    },

    /// Sync from Kafka topics (incremental-only)
    #[command(name = "kafka")]
    Kafka(KafkaArgs),

    /// Import from CSV files
    #[command(name = "csv")]
    Csv(CsvArgs),

    /// Import from JSONL files
    #[command(name = "jsonl")]
    Jsonl(JsonlArgs),
}

// =============================================================================
// MongoDB Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum MongoDBCommands {
    /// Full sync from MongoDB
    Full(MongoDBFullArgs),
    /// Incremental sync from MongoDB using change streams
    Incremental(MongoDBIncrementalArgs),
}

#[derive(Args)]
struct MongoDBFullArgs {
    /// MongoDB connection string
    #[arg(long, env = "MONGODB_URI")]
    connection_string: String,

    /// MongoDB database name
    #[arg(long, env = "MONGODB_DATABASE")]
    database: String,

    /// Collections to sync (comma-separated, empty means all collections)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Directory to write checkpoint files (mutually exclusive with --checkpoints-surreal-table)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table for storing checkpoints (mutually exclusive with --checkpoint-dir)
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct MongoDBIncrementalArgs {
    /// MongoDB connection string
    #[arg(long, env = "MONGODB_URI")]
    connection_string: String,

    /// MongoDB database name
    #[arg(long, env = "MONGODB_DATABASE")]
    database: String,

    /// Collections to sync (comma-separated, empty means all collections)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Start incremental sync from this checkpoint (e.g., "mongodb:resumetoken:<base64>")
    /// If not provided, checkpoint is read from --checkpoints-surreal-table
    #[arg(long)]
    incremental_from: Option<String>,

    /// SurrealDB table for reading checkpoints (alternative to --incremental-from)
    #[arg(long, value_name = "TABLE")]
    checkpoints_surreal_table: Option<String>,

    /// Stop incremental sync when reaching this checkpoint (optional)
    #[arg(long)]
    incremental_to: Option<String>,

    /// Maximum time to run incremental sync (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

// =============================================================================
// Neo4j Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum Neo4jCommands {
    /// Full sync from Neo4j
    Full(Neo4jFullArgs),
    /// Incremental sync from Neo4j using timestamp tracking
    Incremental(Neo4jIncrementalArgs),
}

#[derive(Args)]
struct Neo4jFullArgs {
    /// Neo4j connection string (bolt://...)
    #[arg(long, env = "NEO4J_URI")]
    connection_string: String,

    /// Neo4j database name. For composite database constituents, use
    /// "composite.constituent" (e.g., "composite.db1") to automatically
    /// route queries via USE clause.
    #[arg(long, env = "NEO4J_DATABASE")]
    database: Option<String>,

    /// Neo4j username
    #[arg(long, env = "NEO4J_USERNAME")]
    username: Option<String>,

    /// Neo4j password
    #[arg(long, env = "NEO4J_PASSWORD")]
    password: Option<String>,

    /// Labels to sync (comma-separated, empty means all labels)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Timezone for local datetime conversion
    #[arg(long, default_value = "UTC", env = "NEO4J_TIMEZONE")]
    timezone: String,

    /// Properties to parse as JSON (e.g., "User.metadata,Post.config")
    #[arg(long, value_delimiter = ',', env = "NEO4J_JSON_PROPERTIES")]
    json_properties: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Directory to write checkpoint files (filesystem checkpoint storage)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table for storing checkpoints (alternative to --checkpoint-dir)
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Property name for change tracking (default: "updated_at")
    #[arg(long, default_value = "updated_at")]
    change_tracking_property: String,

    /// Assumed start timestamp to use when no tracking property timestamps are found
    /// (RFC3339 format, e.g., "2024-01-01T00:00:00Z")
    /// Requires --allow-empty-tracking-timestamp to be set
    #[arg(long, value_name = "TIMESTAMP")]
    assumed_start_timestamp: Option<String>,

    /// Allow full sync on data without tracking property timestamps
    /// When enabled with --assumed-start-timestamp, uses the assumed timestamp for checkpoints
    /// Useful for testing and loadtest scenarios
    #[arg(long)]
    allow_empty_tracking_timestamp: bool,

    /// Node property to use as SurrealDB record ID (default: "id").
    /// If a node does not have this property, the Neo4j internal node ID is used as fallback.
    #[arg(long, default_value = "id")]
    id_property: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct Neo4jIncrementalArgs {
    /// Neo4j connection string (bolt://...)
    #[arg(long, env = "NEO4J_URI")]
    connection_string: String,

    /// Neo4j database name. For composite database constituents, use
    /// "composite.constituent" (e.g., "composite.db1") to automatically
    /// route queries via USE clause.
    #[arg(long, env = "NEO4J_DATABASE")]
    database: Option<String>,

    /// Neo4j username
    #[arg(long, env = "NEO4J_USERNAME")]
    username: Option<String>,

    /// Neo4j password
    #[arg(long, env = "NEO4J_PASSWORD")]
    password: Option<String>,

    /// Labels to sync (comma-separated, empty means all labels)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Timezone for local datetime conversion
    #[arg(long, default_value = "UTC", env = "NEO4J_TIMEZONE")]
    timezone: String,

    /// Properties to parse as JSON (e.g., "User.metadata,Post.config")
    #[arg(long, value_delimiter = ',', env = "NEO4J_JSON_PROPERTIES")]
    json_properties: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Start incremental sync from this checkpoint (e.g., "neo4j:2024-01-01T00:00:00Z")
    /// If not provided, checkpoint is read from --checkpoints-surreal-table
    #[arg(long)]
    incremental_from: Option<String>,

    /// SurrealDB table for reading checkpoints (alternative to --incremental-from)
    #[arg(long, value_name = "TABLE")]
    checkpoints_surreal_table: Option<String>,

    /// Stop incremental sync when reaching this checkpoint (optional)
    #[arg(long)]
    incremental_to: Option<String>,

    /// Maximum time to run incremental sync (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Property name for change tracking (default: "updated_at")
    #[arg(long, default_value = "updated_at")]
    change_tracking_property: String,

    /// Force use of custom change tracking instead of CDC
    #[arg(long)]
    no_cdc: bool,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Assumed start timestamp to use when no tracking property timestamps are found
    /// (RFC3339 format, e.g., "2024-01-01T00:00:00Z")
    /// Requires --allow-empty-tracking-timestamp to be set
    #[arg(long, value_name = "TIMESTAMP")]
    assumed_start_timestamp: Option<String>,

    /// Allow full sync on data without tracking property timestamps
    /// When enabled with --assumed-start-timestamp, uses the assumed timestamp for checkpoints
    /// Useful for testing and loadtest scenarios
    #[arg(long)]
    allow_empty_tracking_timestamp: bool,

    /// Node property to use as SurrealDB record ID (default: "id").
    /// If a node does not have this property, the Neo4j internal node ID is used as fallback.
    #[arg(long, default_value = "id")]
    id_property: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

// =============================================================================
// PostgreSQL Trigger Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum PostgreSQLTriggerCommands {
    /// Full sync from PostgreSQL (trigger-based)
    Full(PostgreSQLTriggerFullArgs),
    /// Incremental sync from PostgreSQL using triggers
    Incremental(PostgreSQLTriggerIncrementalArgs),
    /// Combined snapshot+stream full sync that hands off to incremental in one
    /// process (watermark strategy)
    Sync(PostgreSQLTriggerSyncArgs),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(PostgreSQLTriggerSnapshotArgs),
}

#[derive(Args)]
struct PostgreSQLTriggerFullArgs {
    /// TOML config file (provides defaults for all other flags)
    #[arg(short = 'c', long = "config-file", value_name = "PATH")]
    config_file: Option<PathBuf>,

    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: Option<String>,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: Option<String>,

    /// Directory to store checkpoint files (filesystem storage)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table name for storing checkpoints (e.g., "surreal_sync_checkpoints")
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Full-sync strategy (interleaved-snapshot is the default for this source)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    strategy: SyncStrategy,

    /// Rows read per keyset chunk when using the interleaved-snapshot strategy
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct PostgreSQLTriggerIncrementalArgs {
    /// TOML config file (provides defaults for all other flags)
    #[arg(short = 'c', long = "config-file", value_name = "PATH")]
    config_file: Option<PathBuf>,

    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: Option<String>,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: Option<String>,

    /// Start incremental sync from this checkpoint (e.g., "postgresql:sequence:123")
    /// If not specified, reads from SurrealDB using --checkpoints-surreal-table
    #[arg(long)]
    incremental_from: Option<String>,

    /// SurrealDB table name for reading t1 checkpoint (e.g., "surreal_sync_checkpoints")
    /// Used when --incremental-from is not specified
    #[arg(long, value_name = "TABLE")]
    checkpoints_surreal_table: Option<String>,

    /// Stop incremental sync when reaching this checkpoint (optional)
    #[arg(long)]
    incremental_to: Option<String>,

    /// Maximum time to run incremental sync (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

/// Combined snapshot+stream sync for the PostgreSQL trigger source.
#[derive(Args)]
struct PostgreSQLTriggerSyncArgs {
    /// PostgreSQL connection string (must include database name)
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Rows read per keyset chunk during the snapshot phase
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Maximum time to run the incremental phase (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for the PostgreSQL trigger
/// source by inserting an execute-snapshot signal row.
#[derive(Args)]
struct PostgreSQLTriggerSnapshotArgs {
    /// PostgreSQL connection string (must include database name)
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    tables: Vec<String>,
}

// =============================================================================
// MySQL Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum MySQLCommands {
    /// Full sync from MySQL
    Full(MySQLFullArgs),
    /// Incremental sync from MySQL using triggers
    Incremental(MySQLIncrementalArgs),
    /// Combined snapshot+stream full sync that hands off to incremental in one
    /// process (watermark strategy)
    Sync(MySQLSyncArgs),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(MySQLSnapshotArgs),
}

#[derive(Args)]
struct MySQLFullArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// MySQL JSON paths that contain boolean values stored as 0/1
    #[arg(long, value_delimiter = ',', env = "MYSQL_BOOLEAN_PATHS")]
    boolean_paths: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Directory to write checkpoint files (mutually exclusive with --checkpoints-surreal-table)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table for storing checkpoints (mutually exclusive with --checkpoint-dir)
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Full-sync strategy (interleaved-snapshot is the default for this source)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    strategy: SyncStrategy,

    /// Rows read per keyset chunk when using the interleaved-snapshot strategy
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct MySQLIncrementalArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// MySQL JSON paths that contain boolean values stored as 0/1
    #[arg(long, value_delimiter = ',', env = "MYSQL_BOOLEAN_PATHS")]
    boolean_paths: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Start incremental sync from this checkpoint (e.g., "mysql:sequence:456")
    /// If not provided, checkpoint is read from --checkpoints-surreal-table
    #[arg(long)]
    incremental_from: Option<String>,

    /// SurrealDB table for reading checkpoints (alternative to --incremental-from)
    #[arg(long, value_name = "TABLE")]
    checkpoints_surreal_table: Option<String>,

    /// Stop incremental sync when reaching this checkpoint (optional)
    #[arg(long)]
    incremental_to: Option<String>,

    /// Maximum time to run incremental sync (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

/// Combined snapshot+stream sync for MySQL.
#[derive(Args)]
struct MySQLSyncArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// MySQL JSON paths that contain boolean values stored as 0/1
    #[arg(long, value_delimiter = ',', env = "MYSQL_BOOLEAN_PATHS")]
    boolean_paths: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Rows read per keyset chunk during the snapshot phase
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Maximum time to run the incremental phase (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

        /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

#[command(flatten)]
    surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for MySQL by inserting an
/// execute-snapshot signal row.
#[derive(Args)]
struct MySQLSnapshotArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    tables: Vec<String>,
}

// =============================================================================
// MySQL Binlog Commands and Args
// =============================================================================

/// Engine flavor override for binlog CDC (auto-detected from server when omitted).
#[derive(Clone, Copy, Debug, ValueEnum)]
enum MySQLBinlogFlavorArg {
    #[value(name = "mysql")]
    MySql,
    #[value(name = "mariadb")]
    MariaDb,
}

impl From<MySQLBinlogFlavorArg> for surreal_sync_mysql_binlog_source::Flavor {
    fn from(value: MySQLBinlogFlavorArg) -> Self {
        match value {
            MySQLBinlogFlavorArg::MySql => Self::MySql,
            MySQLBinlogFlavorArg::MariaDb => Self::MariaDb,
        }
    }
}

/// MariaDB `@slave_gtid_strict_mode` behavior for GTID resume sessions.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum MariaDbGtidStrictModeArg {
    #[default]
    ServerDefault,
    On,
    Off,
}

impl From<MariaDbGtidStrictModeArg> for surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode {
    fn from(value: MariaDbGtidStrictModeArg) -> Self {
        match value {
            MariaDbGtidStrictModeArg::ServerDefault => Self::ServerDefault,
            MariaDbGtidStrictModeArg::On => Self::On,
            MariaDbGtidStrictModeArg::Off => Self::Off,
        }
    }
}

/// TLS behavior for the raw MySQL binlog replication connection.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum MySQLBinlogTlsModeArg {
    #[default]
    Disabled,
    Preferred,
    Required,
}

#[derive(Clone, Debug, Args, Default)]
struct MySQLBinlogTlsArgs {
    /// TLS mode for the raw binlog replication connection
    #[arg(long, value_enum, default_value_t = MySQLBinlogTlsModeArg::default())]
    tls_mode: MySQLBinlogTlsModeArg,

    /// PEM CA bundle used to verify the MySQL server certificate
    #[arg(long, value_name = "PATH")]
    tls_ca: Option<String>,

    /// PEM client certificate for MySQL TLS client auth
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<String>,

    /// PEM private key for MySQL TLS client auth
    #[arg(long, value_name = "PATH")]
    tls_key: Option<String>,
}

impl MySQLBinlogTlsArgs {
    fn ssl_mode(&self) -> surreal_sync_mysql_binlog_source::SslMode {
        let options = surreal_sync_mysql_binlog_source::SslOptions {
            ca: self.tls_ca.clone(),
            cert: self.tls_cert.clone(),
            key: self.tls_key.clone(),
        };
        match self.tls_mode {
            MySQLBinlogTlsModeArg::Disabled => surreal_sync_mysql_binlog_source::SslMode::Disabled,
            MySQLBinlogTlsModeArg::Preferred => {
                surreal_sync_mysql_binlog_source::SslMode::Preferred(options)
            }
            MySQLBinlogTlsModeArg::Required => {
                surreal_sync_mysql_binlog_source::SslMode::Required(options)
            }
        }
    }
}

#[derive(Subcommand)]
enum MySQLBinlogCommands {
    /// Snapshot and/or stream sync from MySQL/MariaDB binlog
    Sync(Box<MySQLBinlogSyncArgs>),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(MySQLBinlogSnapshotArgs),
}

/// Controls whether `sync` runs an initial snapshot, streams only, or snapshots only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum BinlogSnapshotModeArg {
    /// Interleaved snapshot then continuous stream (default).
    #[default]
    Initial,
    /// Stream only from `--from` or the checkpoint store (no snapshot).
    Never,
    /// Snapshot only, then exit (no stream).
    Only,
}

/// Combined snapshot+stream sync for MySQL/MariaDB binlog.
#[derive(Args)]
struct MySQLBinlogSyncArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Unique replica server id for this binlog consumer (random if omitted)
    #[arg(long)]
    server_id: Option<u32>,

    /// Override auto-detected engine flavor (mysql or mariadb)
    #[arg(long, value_enum)]
    flavor: Option<MySQLBinlogFlavorArg>,

    /// MariaDB @slave_gtid_strict_mode for GTID resume sessions
    #[arg(long, value_enum, default_value_t = MariaDbGtidStrictModeArg::default())]
    mariadb_gtid_strict_mode: MariaDbGtidStrictModeArg,

    #[command(flatten)]
    tls: MySQLBinlogTlsArgs,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Snapshot phase: initial (snapshot then stream), never (stream only), only (snapshot then exit)
    #[arg(long, value_enum, default_value_t = BinlogSnapshotModeArg::default())]
    snapshot_mode: BinlogSnapshotModeArg,

    /// Stream resume/start position (`head` = current master, or a checkpoint string).
    /// Used with `never` and when resuming `initial`. If omitted, reads from the checkpoint store
    /// (falling back to head when empty).
    #[arg(long)]
    from: Option<String>,

    /// Stop the stream phase after this wall-clock duration (e.g. 3600s, 30m, 300).
    #[arg(long, value_name = "DURATION", conflicts_with = "stop_at")]
    stop_after: Option<String>,

    /// Stop the stream phase at this binlog checkpoint
    #[arg(long, value_name = "CHECKPOINT", conflicts_with = "stop_after")]
    stop_at: Option<String>,

    /// Full-sync strategy for the snapshot phase (interleaved-snapshot is the default)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    strategy: SyncStrategy,

    /// Rows read per keyset chunk during snapshot phases
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Directory to persist snapshot and stream checkpoints
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table for persisting snapshot and stream checkpoints
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Persist stream checkpoints at this interval in seconds
    #[arg(long, default_value = "10")]
    checkpoint_interval: u64,

    /// Blocking read timeout for binlog packet polls during the replication tail (milliseconds)
    #[arg(long, default_value = "500")]
    binlog_poll_timeout_ms: u64,

    /// Sleep when a replication tail poll returns no events (milliseconds)
    #[arg(long, default_value = "100")]
    idle_sleep_ms: u64,

    /// Max events requested per binlog read in the replication tail loop
    #[arg(long, default_value_t = 32)]
    binlog_event_batch_size: usize,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for MySQL binlog by inserting
/// an execute-snapshot signal row.
#[derive(Args)]
struct MySQLBinlogSnapshotArgs {
    /// MySQL connection string
    #[arg(long, env = "MYSQL_URI")]
    connection_string: String,

    /// MySQL database name (extracted from connection string if not provided)
    #[arg(long, env = "MYSQL_DATABASE")]
    database: Option<String>,

    /// Unique replica server id for this binlog consumer (random if omitted)
    #[arg(long)]
    server_id: Option<u32>,

    /// Override auto-detected engine flavor (mysql or mariadb)
    #[arg(long, value_enum)]
    flavor: Option<MySQLBinlogFlavorArg>,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    tables: Vec<String>,
}

// =============================================================================
// PostgreSQL pgoutput WAL Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum PostgreSQLPgoutputCommands {
    /// Snapshot and/or stream sync from PostgreSQL pgoutput WAL
    Sync(Box<PostgreSQLPgoutputSyncArgs>),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(PostgreSQLPgoutputSnapshotArgs),
}

/// Combined snapshot+stream sync for PostgreSQL pgoutput WAL.
#[derive(Args)]
struct PostgreSQLPgoutputSyncArgs {
    /// PostgreSQL connection string
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// PostgreSQL schema (default: public)
    #[arg(long, default_value = "public")]
    schema: String,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Logical replication slot name
    #[arg(long, default_value = "surreal_sync_slot")]
    slot: String,

    /// Publication name for pgoutput
    #[arg(long, default_value = "surreal_sync_pub")]
    publication: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Snapshot phase: initial (snapshot then stream), never (stream only), only (snapshot then exit)
    #[arg(long, value_enum, default_value_t = BinlogSnapshotModeArg::default())]
    snapshot_mode: BinlogSnapshotModeArg,

    /// Stream resume/start position (`head` = current WAL head, or an LSN checkpoint string).
    #[arg(long)]
    from: Option<String>,

    /// Stop the stream phase after this wall-clock duration (e.g. 3600s, 30m, 300).
    #[arg(long, value_name = "DURATION", conflicts_with = "stop_at")]
    stop_after: Option<String>,

    /// Stop the stream phase at this WAL LSN checkpoint
    #[arg(long, value_name = "CHECKPOINT", conflicts_with = "stop_after")]
    stop_at: Option<String>,

    /// Full-sync strategy for the snapshot phase (interleaved-snapshot is the default)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    strategy: SyncStrategy,

    /// Rows read per keyset chunk during snapshot phases
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Directory to persist snapshot and stream checkpoints
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table for persisting snapshot and stream checkpoints
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Persist stream checkpoints at this interval in seconds
    #[arg(long, default_value = "10")]
    checkpoint_interval: u64,

    /// Blocking read timeout for WAL event polls during the replication tail (milliseconds)
    #[arg(long, default_value = "500")]
    wal_poll_timeout_ms: u64,

    /// Sleep when a replication tail poll returns no events (milliseconds)
    #[arg(long, default_value = "100")]
    idle_sleep_ms: u64,

    /// Max events requested per WAL read in the replication tail loop
    #[arg(long, default_value_t = 32)]
    wal_event_batch_size: usize,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for PostgreSQL WAL.
#[derive(Args)]
struct PostgreSQLPgoutputSnapshotArgs {
    /// PostgreSQL connection string
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    tables: Vec<String>,
}

// =============================================================================
// PostgreSQL WAL-based Logical Replication Commands and Args
// =============================================================================

#[derive(Subcommand)]
enum PostgreSQLLogicalCommands {
    /// Full sync from PostgreSQL (logical replication)
    Full(PostgreSQLLogicalFullArgs),
    /// Incremental sync from PostgreSQL using logical replication
    Incremental(PostgreSQLLogicalIncrementalArgs),
    /// Combined snapshot+stream full sync that hands off to incremental in one
    /// process (watermark strategy)
    Sync(PostgreSQLLogicalSyncArgs),
    /// Trigger an ad-hoc snapshot of additional tables against a running `sync`
    Snapshot(PostgreSQLLogicalSnapshotArgs),
}

#[derive(Args)]
struct PostgreSQLLogicalFullArgs {
    /// TOML config file (provides defaults for all other flags)
    #[arg(short = 'c', long = "config-file", value_name = "PATH")]
    config_file: Option<PathBuf>,

    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
    #[arg(long)]
    connection_string: Option<String>,

    /// Replication slot name
    #[arg(long, default_value = "surreal_sync_slot")]
    slot: String,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// PostgreSQL schema
    #[arg(long, default_value = "public")]
    schema: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: Option<String>,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Directory to store checkpoint files (filesystem storage)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table name for storing checkpoints (e.g., "surreal_sync_checkpoints")
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Full-sync strategy (interleaved-snapshot is the default for this source)
    #[arg(long, value_enum, default_value_t = SyncStrategy::default())]
    strategy: SyncStrategy,

    /// Rows read per keyset chunk when using the interleaved-snapshot strategy
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct PostgreSQLLogicalIncrementalArgs {
    /// TOML config file (provides defaults for all other flags)
    #[arg(short = 'c', long = "config-file", value_name = "PATH")]
    config_file: Option<PathBuf>,

    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
    #[arg(long)]
    connection_string: Option<String>,

    /// Replication slot name
    #[arg(long, default_value = "surreal_sync_slot")]
    slot: String,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// PostgreSQL schema
    #[arg(long, default_value = "public")]
    schema: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: Option<String>,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Start incremental sync from this checkpoint (LSN format, e.g., "0/1949850")
    /// If not specified, reads from SurrealDB using --checkpoints-surreal-table
    #[arg(long)]
    incremental_from: Option<String>,

    /// SurrealDB table name for reading t1 checkpoint (e.g., "surreal_sync_checkpoints")
    /// Used when --incremental-from is not specified
    #[arg(long, value_name = "TABLE")]
    checkpoints_surreal_table: Option<String>,

    /// Stop incremental sync at this checkpoint (optional)
    #[arg(long)]
    incremental_to: Option<String>,

    /// Timeout in seconds (default: 3600 = 1 hour)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

/// Combined snapshot+stream sync for the PostgreSQL wal2json source.
#[derive(Args)]
struct PostgreSQLLogicalSyncArgs {
    /// PostgreSQL connection string (must include database name)
    #[arg(long)]
    connection_string: String,

    /// Replication slot name
    #[arg(long, default_value = "surreal_sync_slot")]
    slot: String,

    /// Tables to sync (comma-separated, empty means all tables)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// PostgreSQL schema
    #[arg(long, default_value = "public")]
    schema: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Rows read per keyset chunk during the snapshot phase
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Maximum time to run the incremental phase (in seconds, default: 3600)
    #[arg(long, default_value = "3600")]
    timeout: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

/// Trigger an ad-hoc snapshot of additional tables for the PostgreSQL wal2json
/// source by inserting an execute-snapshot signal row.
#[derive(Args)]
struct PostgreSQLLogicalSnapshotArgs {
    /// PostgreSQL connection string (must include database name)
    #[arg(long)]
    connection_string: String,

    /// Replication slot name
    #[arg(long, default_value = "surreal_sync_slot")]
    slot: String,

    /// PostgreSQL schema
    #[arg(long, default_value = "public")]
    schema: String,

    /// Tables to snapshot (comma-separated)
    #[arg(long, value_delimiter = ',', required = true)]
    tables: Vec<String>,
}

// =============================================================================
// Kafka Args (single command - incremental-only)
// =============================================================================

#[derive(Args)]
struct KafkaArgs {
    /// Kafka source configuration
    #[command(flatten)]
    config: surreal_sync_kafka_source::Config,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Timeout for consuming messages (e.g. "1h", "30m", "300s")
    /// After this time, the consumer will stop and exit.
    #[arg(long, default_value = "1h")]
    timeout: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

// =============================================================================
// CSV Args (single command - import-only)
// =============================================================================

#[derive(Args)]
#[group(id = "source", required = true, multiple = false, args = ["files", "s3_uris", "http_uris"])]
struct CsvArgs {
    /// CSV file paths to import (can specify multiple)
    #[arg(long, required = true, value_name = "FILE")]
    files: Vec<PathBuf>,

    /// S3 URIs to import (can specify multiple)
    #[arg(long, value_name = "S3_URI")]
    s3_uris: Vec<String>,

    /// HTTP/HTTPS URLs to import (can specify multiple)
    #[arg(long, value_name = "HTTP_URI")]
    http_uris: Vec<String>,

    /// Target SurrealDB table name
    #[arg(long)]
    table: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Whether the CSV has headers
    #[arg(long, default_value = "true")]
    has_headers: bool,

    /// CSV delimiter character
    #[arg(long, default_value = ",")]
    delimiter: char,

    /// Field to use as record ID (optional)
    #[arg(long)]
    id_field: Option<String>,

    /// Column names when has_headers is false (comma-separated)
    #[arg(long, value_delimiter = ',')]
    column_names: Option<Vec<String>>,

    /// Emit metrics to this file during execution
    #[arg(long, value_name = "PATH")]
    emit_metrics: Option<PathBuf>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

// =============================================================================
// JSONL Args (single command - import-only)
// =============================================================================

#[derive(Args)]
struct JsonlArgs {
    /// Path to JSONL files directory or single file
    #[arg(long)]
    path: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// ID field name (default: "id")
    #[arg(long, default_value = "id")]
    id_field: String,

    /// Conversion rules (format: 'type="page_id",page_id page:page_id')
    #[arg(long = "rule", value_name = "RULE")]
    conversion_rules: Vec<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    transforms_config: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

// =============================================================================
// Loadtest Commands
// =============================================================================

/// Load testing subcommands
#[derive(Subcommand)]
enum LoadtestCommand {
    /// Populate source database with deterministic test data for load testing
    Populate {
        #[command(subcommand)]
        source: PopulateSource,
    },

    /// Verify synced data in SurrealDB matches expected values
    Verify {
        #[command(flatten)]
        args: VerifyArgs,
    },

    /// Generate Docker Compose or Kubernetes configurations for distributed load testing
    Generate(GenerateArgs),

    /// Run HTTP server to aggregate metrics from distributed workers
    AggregateServer(AggregateServerArgs),
}

/// Source database to populate with test data
#[derive(Subcommand)]
enum PopulateSource {
    /// Populate MySQL database with test data
    #[command(name = "mysql")]
    MySQL {
        #[command(flatten)]
        args: MySQLPopulateArgs,
    },
    /// Populate PostgreSQL database with test data
    #[command(name = "postgresql")]
    PostgreSQL {
        #[command(flatten)]
        args: PostgreSQLPopulateArgs,
    },
    /// Populate MongoDB database with test data
    #[command(name = "mongodb")]
    MongoDB {
        #[command(flatten)]
        args: MongoDBPopulateArgs,
    },
    /// Populate Neo4j database with test data
    #[command(name = "neo4j")]
    Neo4j {
        #[command(flatten)]
        args: Neo4jPopulateArgs,
    },
    /// Generate CSV files with test data
    #[command(name = "csv")]
    Csv {
        #[command(flatten)]
        args: CSVPopulateArgs,
    },
    /// Generate JSONL files with test data
    #[command(name = "jsonl")]
    Jsonl {
        #[command(flatten)]
        args: JSONLPopulateArgs,
    },
    /// Populate Kafka topics with test data
    #[command(name = "kafka")]
    Kafka {
        #[command(flatten)]
        args: KafkaPopulateArgs,
    },
}

// =============================================================================
// Main Entry Point
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(e) = run().await {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    // Install the crypto provider before any TLS operations occur
    if let Err(err) = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
    {
        eprintln!("Error setting up crypto provider for TLS: {err:?}");
    }

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::From { source } => handle_from_command(*source).await?,
        Commands::Loadtest { command } => handle_loadtest_command(*command).await?,
    }

    Ok(())
}

// =============================================================================
// From Command Handler
// =============================================================================

async fn handle_from_command(source: FromSource) -> anyhow::Result<()> {
    match source {
        FromSource::MongoDB { command } => match command {
            MongoDBCommands::Full(args) => from::mongodb::run_full(args).await?,
            MongoDBCommands::Incremental(args) => from::mongodb::run_incremental(args).await?,
        },
        FromSource::Neo4j { command } => match command {
            Neo4jCommands::Full(args) => from::neo4j::run_full(args).await?,
            Neo4jCommands::Incremental(args) => from::neo4j::run_incremental(args).await?,
        },
        FromSource::PostgreSQLTrigger { command } => match command {
            PostgreSQLTriggerCommands::Full(args) => {
                from::postgresql_trigger::run_full(args).await?
            }
            PostgreSQLTriggerCommands::Incremental(args) => {
                from::postgresql_trigger::run_incremental(args).await?
            }
            PostgreSQLTriggerCommands::Sync(args) => {
                from::postgresql_trigger::run_sync(args).await?
            }
            PostgreSQLTriggerCommands::Snapshot(args) => {
                from::postgresql_trigger::run_snapshot_signal(args).await?
            }
        },
        FromSource::MySQL { command } => match command {
            MySQLCommands::Full(args) => from::mysql::run_full(args).await?,
            MySQLCommands::Incremental(args) => from::mysql::run_incremental(args).await?,
            MySQLCommands::Sync(args) => from::mysql::run_sync(args).await?,
            MySQLCommands::Snapshot(args) => from::mysql::run_snapshot_signal(args).await?,
        },
        FromSource::MySQLBinlog { command } => match command {
            MySQLBinlogCommands::Sync(args) => from::mysql_binlog::run_sync(*args).await?,
            MySQLBinlogCommands::Snapshot(args) => {
                from::mysql_binlog::run_snapshot_signal(args).await?
            }
        },
        FromSource::PostgreSQLPgoutput { command } => match command {
            PostgreSQLPgoutputCommands::Sync(args) => {
                from::postgresql_pgoutput::run_sync(*args).await?
            }
            PostgreSQLPgoutputCommands::Snapshot(args) => {
                from::postgresql_pgoutput::run_snapshot_signal(args).await?
            }
        },
        FromSource::PostgreSQL { command } => match command {
            PostgreSQLLogicalCommands::Full(args) => {
                from::postgresql_wal2json::run_full(args).await?
            }
            PostgreSQLLogicalCommands::Incremental(args) => {
                from::postgresql_wal2json::run_incremental(args).await?
            }
            PostgreSQLLogicalCommands::Sync(args) => {
                from::postgresql_wal2json::run_sync(args).await?
            }
            PostgreSQLLogicalCommands::Snapshot(args) => {
                from::postgresql_wal2json::run_snapshot_signal(args).await?
            }
        },
        FromSource::Kafka(args) => from::kafka::run(args).await?,
        FromSource::Csv(args) => from::csv::run(args).await?,
        FromSource::Jsonl(args) => from::jsonl::run(args).await?,
    }
    Ok(())
}

// =============================================================================
// Loadtest Command Handler
// =============================================================================

async fn handle_loadtest_command(command: LoadtestCommand) -> anyhow::Result<()> {
    match command {
        LoadtestCommand::Populate { source } => loadtest::populate::run_populate(source).await?,
        LoadtestCommand::Verify { args } => loadtest::verify::run_verify(args).await?,
        LoadtestCommand::Generate(args) => loadtest::generate::run_loadtest_generate(args).await?,
        LoadtestCommand::AggregateServer(args) => {
            loadtest::aggregate_server::run_aggregate_server(args).await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_binlog_tls_args_default_to_disabled() {
        let args = MySQLBinlogTlsArgs::default();
        assert!(matches!(
            args.ssl_mode(),
            surreal_sync_mysql_binlog_source::SslMode::Disabled
        ));
    }

    #[test]
    fn mysql_binlog_tls_args_preserve_ca_and_client_cert_paths() {
        let args = MySQLBinlogTlsArgs {
            tls_mode: MySQLBinlogTlsModeArg::Required,
            tls_ca: Some("ca.pem".to_string()),
            tls_cert: Some("client.pem".to_string()),
            tls_key: Some("client-key.pem".to_string()),
        };
        let surreal_sync_mysql_binlog_source::SslMode::Required(options) = args.ssl_mode() else {
            panic!("expected required TLS mode");
        };
        assert_eq!(options.ca.as_deref(), Some("ca.pem"));
        assert_eq!(options.cert.as_deref(), Some("client.pem"));
        assert_eq!(options.key.as_deref(), Some("client-key.pem"));
    }

    #[test]
    fn mariadb_gtid_strict_mode_arg_maps_to_source_option() {
        assert_eq!(
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::from(
                MariaDbGtidStrictModeArg::ServerDefault
            ),
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault
        );
        assert_eq!(
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::from(
                MariaDbGtidStrictModeArg::On
            ),
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::On
        );
        assert_eq!(
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::from(
                MariaDbGtidStrictModeArg::Off
            ),
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::Off
        );
    }
}
