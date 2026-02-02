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
//! ## Checkpoint Formats
//! - Neo4j: `neo4j:2024-01-01T00:00:00Z` (timestamp-based)
//! - MongoDB: `mongodb:base64token:2024-01-01T00:00:00Z` (resume token + timestamp)
//! - PostgreSQL: `postgresql:sequence:123` (trigger-based audit table)
//! - MySQL: `mysql:sequence:456` (trigger-based audit table)

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use surreal_sync::SurrealOpts;

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

    /// Neo4j database name
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

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct Neo4jIncrementalArgs {
    /// Neo4j connection string (bolt://...)
    #[arg(long, env = "NEO4J_URI")]
    connection_string: String,

    /// Neo4j database name
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
}

#[derive(Args)]
struct PostgreSQLTriggerFullArgs {
    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
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

    /// Directory to store checkpoint files (filesystem storage)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table name for storing checkpoints (e.g., "surreal_sync_checkpoints")
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct PostgreSQLTriggerIncrementalArgs {
    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
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

    #[command(flatten)]
    surreal: SurrealOpts,
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

    #[command(flatten)]
    surreal: SurrealOpts,
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
}

#[derive(Args)]
struct PostgreSQLLogicalFullArgs {
    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
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

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

    /// Directory to store checkpoint files (filesystem storage)
    #[arg(long, value_name = "DIR", conflicts_with = "checkpoints_surreal_table")]
    checkpoint_dir: Option<String>,

    /// SurrealDB table name for storing checkpoints (e.g., "surreal_sync_checkpoints")
    #[arg(long, value_name = "TABLE", conflicts_with = "checkpoint_dir")]
    checkpoints_surreal_table: Option<String>,

    #[command(flatten)]
    surreal: SurrealOpts,
}

#[derive(Args)]
struct PostgreSQLLogicalIncrementalArgs {
    /// PostgreSQL connection string (must include database name, e.g., postgresql://user:pass@host:5432/mydb)
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

    #[command(flatten)]
    surreal: SurrealOpts,
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
    // Initialize rustls crypto provider (required for rustls 0.23+)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

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
        },
        FromSource::MySQL { command } => match command {
            MySQLCommands::Full(args) => from::mysql::run_full(args).await?,
            MySQLCommands::Incremental(args) => from::mysql::run_incremental(args).await?,
        },
        FromSource::PostgreSQL { command } => match command {
            PostgreSQLLogicalCommands::Full(args) => {
                from::postgresql_wal2json::run_full(args).await?
            }
            PostgreSQLLogicalCommands::Incremental(args) => {
                from::postgresql_wal2json::run_incremental(args).await?
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
