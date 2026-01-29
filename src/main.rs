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

use anyhow::Context;
use checkpoint::Checkpoint;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use surreal_sync::SurrealOpts;

// Database-specific sync crates (fully-qualified paths used in match arms)
#[allow(clippy::single_component_path_imports)]
use surreal_sync_mongodb_changestream_source;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_mysql_trigger_source;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_neo4j_source;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql_trigger_source;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql_wal2json_source;

// SurrealDB sink crates for v2/v3 SDK support
#[allow(clippy::single_component_path_imports)]
use checkpoint_surreal3;
#[allow(clippy::single_component_path_imports)]
use surreal3_sink;

// Load testing imports
use loadtest_populate_csv::CSVPopulateArgs;
use loadtest_populate_jsonl::JSONLPopulateArgs;
use loadtest_populate_kafka::{generate_proto_for_table, KafkaPopulateArgs};
use loadtest_populate_mongodb::MongoDBPopulateArgs;
use loadtest_populate_mysql::MySQLPopulateArgs;
use loadtest_populate_neo4j::Neo4jPopulateArgs;
use loadtest_populate_postgresql::PostgreSQLPopulateArgs;
use loadtest_verify_surreal2::VerifyArgs;
use sync_core::Schema;

// Load testing distributed imports
use loadtest_distributed::{
    build_cluster_config,
    generator::{ConfigGenerator, DockerComposeGenerator, KubernetesGenerator},
    AggregateServerArgs, GenerateArgs, Platform, SourceType,
};

// =============================================================================
// SDK Version Detection
// =============================================================================

/// SDK version to use for SurrealDB operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SdkVersion {
    V2,
    V3,
}

/// Get the SDK version to use, either from explicit user preference or auto-detection.
async fn get_sdk_version(endpoint: &str, explicit: Option<&str>) -> anyhow::Result<SdkVersion> {
    match explicit {
        Some("v2") => {
            tracing::info!("Using SurrealDB SDK v2 (explicitly specified)");
            Ok(SdkVersion::V2)
        }
        Some("v3") => {
            tracing::info!("Using SurrealDB SDK v3 (explicitly specified)");
            Ok(SdkVersion::V3)
        }
        Some(other) => {
            anyhow::bail!("Unknown SDK version: '{other}'. Valid values: v2, v3")
        }
        None => {
            tracing::debug!("Auto-detecting SurrealDB server version...");
            let detected = surreal_version::detect_server_version(endpoint).await?;
            let version = match detected {
                surreal_version::SurrealMajorVersion::V2 => SdkVersion::V2,
                surreal_version::SurrealMajorVersion::V3 => SdkVersion::V3,
            };
            tracing::info!(
                "Auto-detected SurrealDB server version: {detected}, using SDK {version:?}"
            );
            Ok(version)
        }
    }
}

#[derive(Parser)]
#[command(name = "surreal-sync")]
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

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Emit checkpoint files for coordinating with incremental sync
    #[arg(long)]
    emit_checkpoints: bool,

    /// Directory to write checkpoint files
    #[arg(long, default_value = ".surreal-sync-checkpoints")]
    checkpoint_dir: String,

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

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Start incremental sync from this checkpoint (e.g., "mongodb:resumetoken:<base64>")
    #[arg(long)]
    incremental_from: String,

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

    /// Emit checkpoint files for coordinating with incremental sync
    #[arg(long)]
    emit_checkpoints: bool,

    /// Directory to write checkpoint files
    #[arg(long, default_value = ".surreal-sync-checkpoints")]
    checkpoint_dir: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

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
    #[arg(long)]
    incremental_from: String,

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

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Emit checkpoint files for coordinating with incremental sync
    #[arg(long)]
    emit_checkpoints: bool,

    /// Directory to write checkpoint files
    #[arg(long, default_value = ".surreal-sync-checkpoints")]
    checkpoint_dir: String,

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

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Start incremental sync from this checkpoint (e.g., "postgresql:sequence:123")
    #[arg(long)]
    incremental_from: String,

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

    /// MySQL JSON paths that contain boolean values stored as 0/1
    #[arg(long, value_delimiter = ',', env = "MYSQL_BOOLEAN_PATHS")]
    boolean_paths: Option<Vec<String>>,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Emit checkpoint files for coordinating with incremental sync
    #[arg(long)]
    emit_checkpoints: bool,

    /// Directory to write checkpoint files
    #[arg(long, default_value = ".surreal-sync-checkpoints")]
    checkpoint_dir: String,

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
    #[arg(long)]
    incremental_from: String,

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
            MongoDBCommands::Full(args) => run_mongodb_full(args).await?,
            MongoDBCommands::Incremental(args) => run_mongodb_incremental(args).await?,
        },
        FromSource::Neo4j { command } => match command {
            Neo4jCommands::Full(args) => run_neo4j_full(args).await?,
            Neo4jCommands::Incremental(args) => run_neo4j_incremental(args).await?,
        },
        FromSource::PostgreSQLTrigger { command } => match command {
            PostgreSQLTriggerCommands::Full(args) => run_postgresql_trigger_full(args).await?,
            PostgreSQLTriggerCommands::Incremental(args) => {
                run_postgresql_trigger_incremental(args).await?
            }
        },
        FromSource::MySQL { command } => match command {
            MySQLCommands::Full(args) => run_mysql_full(args).await?,
            MySQLCommands::Incremental(args) => run_mysql_incremental(args).await?,
        },
        FromSource::PostgreSQL { command } => match command {
            PostgreSQLLogicalCommands::Full(args) => run_postgresql_logical_full(args).await?,
            PostgreSQLLogicalCommands::Incremental(args) => {
                run_postgresql_logical_incremental(args).await?
            }
        },
        FromSource::Kafka(args) => run_kafka(args).await?,
        FromSource::Csv(args) => run_csv(args).await?,
        FromSource::Jsonl(args) => run_jsonl(args).await?,
    }
    Ok(())
}

// =============================================================================
// MongoDB Handlers
// =============================================================================

async fn run_mongodb_full(args: MongoDBFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_mongodb_full_v2(args).await,
        SdkVersion::V3 => run_mongodb_full_v3(args).await,
    }
}

async fn run_mongodb_full_v2(args: MongoDBFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MongoDB to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mongodb_changestream_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mongodb_changestream_source::migrate_from_mongodb(
            &sink,
            source_opts,
            sync_opts,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mongodb_full_v3(args: MongoDBFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MongoDB to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mongodb_changestream_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mongodb_changestream_source::migrate_from_mongodb(
            &sink,
            source_opts,
            sync_opts,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mongodb_incremental(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_mongodb_incremental_v2(args).await,
        SdkVersion::V3 => run_mongodb_incremental_v3(args).await,
    }
}

async fn run_mongodb_incremental_v2(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MongoDB to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mongodb_from =
        surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(
            &args.incremental_from,
        )?;
    let mongodb_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb_changestream_source::run_incremental_sync(
        &sink,
        source_opts,
        mongodb_from,
        deadline,
        mongodb_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_mongodb_incremental_v3(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MongoDB to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mongodb_from =
        surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(
            &args.incremental_from,
        )?;
    let mongodb_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb_changestream_source::run_incremental_sync(
        &sink,
        source_opts,
        mongodb_from,
        deadline,
        mongodb_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// Neo4j Handlers
// =============================================================================

async fn run_neo4j_full(args: Neo4jFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_neo4j_full_v2(args).await,
        SdkVersion::V3 => run_neo4j_full_v3(args).await,
    }
}

async fn run_neo4j_full_v2(args: Neo4jFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from Neo4j to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_neo4j_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_neo4j_full_v3(args: Neo4jFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from Neo4j to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_neo4j_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_neo4j_incremental(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_neo4j_incremental_v2(args).await,
        SdkVersion::V3 => run_neo4j_incremental_v3(args).await,
    }
}

async fn run_neo4j_incremental_v2(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from Neo4j to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let neo4j_from =
        surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(&args.incremental_from)?;
    let neo4j_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    surreal_sync_neo4j_source::run_incremental_sync(
        &sink,
        source_opts,
        sync_opts,
        neo4j_from,
        deadline,
        neo4j_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_neo4j_incremental_v3(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from Neo4j to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let neo4j_from =
        surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(&args.incremental_from)?;
    let neo4j_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    surreal_sync_neo4j_source::run_incremental_sync(
        &sink,
        source_opts,
        sync_opts,
        neo4j_from,
        deadline,
        neo4j_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// PostgreSQL Trigger Handlers
// =============================================================================

async fn run_postgresql_trigger_full(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_postgresql_trigger_full_v2(args).await,
        SdkVersion::V3 => run_postgresql_trigger_full_v3(args).await,
    }
}

async fn run_postgresql_trigger_full_v2(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_postgresql_trigger_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_postgresql_trigger_full_v3(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_postgresql_trigger_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_postgresql_trigger_incremental(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_postgresql_trigger_incremental_v2(args).await,
        SdkVersion::V3 => run_postgresql_trigger_incremental_v3(args).await,
    }
}

async fn run_postgresql_trigger_incremental_v2(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let pg_from = surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        pg_from,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_postgresql_trigger_incremental_v3(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let pg_from = surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        pg_from,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// MySQL Handlers
// =============================================================================

async fn run_mysql_full(args: MySQLFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_mysql_full_v2(args).await,
        SdkVersion::V3 => run_mysql_full_v3(args).await,
    }
}

async fn run_mysql_full_v2(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mysql_trigger_source::run_full_sync(
            &sink,
            &source_opts,
            &sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            &source_opts,
            &sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mysql_full_v3(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mysql_trigger_source::run_full_sync(
            &sink,
            &source_opts,
            &sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            &source_opts,
            &sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mysql_incremental(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_mysql_incremental_v2(args).await,
        SdkVersion::V3 => run_mysql_incremental_v3(args).await,
    }
}

async fn run_mysql_incremental_v2(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mysql_from = surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let mysql_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_mysql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        mysql_from,
        deadline,
        mysql_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_mysql_incremental_v3(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mysql_from = surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let mysql_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_mysql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        mysql_from,
        deadline,
        mysql_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// PostgreSQL WAL-based Logical Replication Handlers
// =============================================================================

async fn run_postgresql_logical_full(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_postgresql_logical_full_v2(args).await,
        SdkVersion::V3 => run_postgresql_logical_full_v3(args).await,
    }
}

async fn run_postgresql_logical_full_v2(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            // SurrealDB v2 checkpoint storage
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap's conflicts_with
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

async fn run_postgresql_logical_full_v3(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            // SurrealDB v3 checkpoint storage
            let store = checkpoint_surreal3::Surreal3Store::new(surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap's conflicts_with
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

async fn run_postgresql_logical_incremental(
    args: PostgreSQLLogicalIncrementalArgs,
) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_postgresql_logical_incremental_v2(args).await,
        SdkVersion::V3 => run_postgresql_logical_incremental_v3(args).await,
    }
}

async fn run_postgresql_logical_incremental_v2(
    args: PostgreSQLLogicalIncrementalArgs,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            // Explicit checkpoint from CLI
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
        }
        (None, Some(table)) => {
            // Read from SurrealDB v2 checkpoint storage
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                &s,
            )
        })
        .transpose()?;

    // Parse timeout
    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    // Connect to SurrealDB v2 for data sink
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string.clone(),
        slot_name: args.slot.clone(),
        tables: args.tables.clone(),
        schema: args.schema.clone(),
    };

    surreal_sync_postgresql_wal2json_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        to_checkpoint,
    )
    .await?;

    Ok(())
}

async fn run_postgresql_logical_incremental_v3(
    args: PostgreSQLLogicalIncrementalArgs,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            // Explicit checkpoint from CLI
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
        }
        (None, Some(table)) => {
            // Read from SurrealDB v3 checkpoint storage
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                &s,
            )
        })
        .transpose()?;

    // Parse timeout
    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    // Connect to SurrealDB v3 for data sink
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string.clone(),
        slot_name: args.slot.clone(),
        tables: args.tables.clone(),
        schema: args.schema.clone(),
    };

    surreal_sync_postgresql_wal2json_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        to_checkpoint,
    )
    .await?;

    Ok(())
}

// =============================================================================
// Kafka Handler
// =============================================================================

async fn run_kafka(args: KafkaArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_kafka_v2(args).await,
        SdkVersion::V3 => run_kafka_v3(args).await,
    }
}

async fn run_kafka_v2(args: KafkaArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Timeout: {}", args.timeout);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Parse timeout duration
    let timeout_secs = parse_duration_to_secs(&args.timeout)
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);
    tracing::info!("Will consume until deadline: {}", deadline);

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = std::sync::Arc::new(surreal2_sink::Surreal2Sink::new(surreal));

    let table_schema = if let Some(schema_path) = args.schema_file {
        let schema = Schema::from_file(&schema_path)
            .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
        let table_name = args
            .config
            .table_name
            .as_ref()
            .unwrap_or(&args.config.topic);
        schema
            .get_table(table_name)
            .map(|t| t.to_table_definition())
    } else {
        None
    };

    surreal_sync_kafka_source::run_incremental_sync(sink, args.config, deadline, table_schema)
        .await?;

    Ok(())
}

async fn run_kafka_v3(args: KafkaArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Timeout: {}", args.timeout);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Parse timeout duration
    let timeout_secs = parse_duration_to_secs(&args.timeout)
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);
    tracing::info!("Will consume until deadline: {}", deadline);

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = std::sync::Arc::new(surreal3_sink::Surreal3Sink::new(surreal));

    let table_schema = if let Some(schema_path) = args.schema_file {
        let schema = Schema::from_file(&schema_path)
            .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
        let table_name = args
            .config
            .table_name
            .as_ref()
            .unwrap_or(&args.config.topic);
        schema
            .get_table(table_name)
            .map(|t| t.to_table_definition())
    } else {
        None
    };

    surreal_sync_kafka_source::run_incremental_sync(sink, args.config, deadline, table_schema)
        .await?;

    Ok(())
}

// =============================================================================
// CSV Handler
// =============================================================================

async fn run_csv(args: CsvArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_csv_v2(args).await,
        SdkVersion::V3 => run_csv_v3(args).await,
    }
}

async fn run_csv_v2(args: CsvArgs) -> anyhow::Result<()> {
    tracing::info!("Starting CSV import (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let config = surreal_sync::csv::Config {
        sources: vec![],
        files: args.files,
        s3_uris: args.s3_uris,
        http_uris: args.http_uris,
        table: args.table,
        batch_size: args.surreal.batch_size,
        has_headers: args.has_headers,
        delimiter: args.delimiter as u8,
        id_field: args.id_field,
        column_names: args.column_names,
        emit_metrics: args.emit_metrics,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::csv::sync(&sink, config).await?;

    tracing::info!("CSV import completed successfully");
    Ok(())
}

async fn run_csv_v3(args: CsvArgs) -> anyhow::Result<()> {
    tracing::info!("Starting CSV import (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let config = surreal_sync::csv::Config {
        sources: vec![],
        files: args.files,
        s3_uris: args.s3_uris,
        http_uris: args.http_uris,
        table: args.table,
        batch_size: args.surreal.batch_size,
        has_headers: args.has_headers,
        delimiter: args.delimiter as u8,
        id_field: args.id_field,
        column_names: args.column_names,
        emit_metrics: args.emit_metrics,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::csv::sync(&sink, config).await?;

    tracing::info!("CSV import completed successfully");
    Ok(())
}

// =============================================================================
// JSONL Handler
// =============================================================================

async fn run_jsonl(args: JsonlArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_jsonl_v2(args).await,
        SdkVersion::V3 => run_jsonl_v3(args).await,
    }
}

async fn run_jsonl_v2(args: JsonlArgs) -> anyhow::Result<()> {
    tracing::info!("Starting JSONL import (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    // Create config with file source
    let config = surreal_sync::jsonl::Config {
        sources: vec![],
        files: vec![args.path.into()],
        s3_uris: vec![],
        http_uris: vec![],
        id_field: args.id_field,
        conversion_rules: args.conversion_rules,
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema: None,
    };
    surreal_sync::jsonl::sync(&sink, config).await?;

    tracing::info!("JSONL import completed successfully");
    Ok(())
}

async fn run_jsonl_v3(args: JsonlArgs) -> anyhow::Result<()> {
    tracing::info!("Starting JSONL import (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    // Create config with file source
    let config = surreal_sync::jsonl::Config {
        sources: vec![],
        files: vec![args.path.into()],
        s3_uris: vec![],
        http_uris: vec![],
        id_field: args.id_field,
        conversion_rules: args.conversion_rules,
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema: None,
    };
    surreal_sync::jsonl::sync(&sink, config).await?;

    tracing::info!("JSONL import completed successfully");
    Ok(())
}

// =============================================================================
// Loadtest Command Handler
// =============================================================================

async fn handle_loadtest_command(command: LoadtestCommand) -> anyhow::Result<()> {
    match command {
        LoadtestCommand::Populate { source } => run_populate(source).await?,
        LoadtestCommand::Verify { args } => run_verify(args).await?,
        LoadtestCommand::Generate(args) => run_loadtest_generate(args).await?,
        LoadtestCommand::AggregateServer(args) => run_aggregate_server(args).await?,
    }
    Ok(())
}

/// Mask password in connection string for safe logging
fn mask_connection_password(conn_str: &str) -> String {
    // Pattern: protocol://user:password@host...
    // Replace password portion with ***
    if let Some(at_pos) = conn_str.find('@') {
        if let Some(colon_pos) = conn_str[..at_pos].rfind(':') {
            let protocol_end = conn_str.find("://").map(|p| p + 3).unwrap_or(0);
            if colon_pos > protocol_end {
                return format!("{}:***{}", &conn_str[..colon_pos], &conn_str[at_pos..]);
            }
        }
    }
    conn_str.to_string()
}

/// Post ContainerMetrics to the aggregator server via HTTP POST with retry logic.
///
/// This function uses std::net::TcpStream to avoid external HTTP client dependencies.
/// It sends a simple HTTP/1.1 POST request with JSON body and validates the response.
///
/// Retries up to 3 times with 2-second delays to handle transient network issues.
fn post_metrics_to_aggregator(
    url: &str,
    metrics: &loadtest_distributed::metrics::ContainerMetrics,
) -> anyhow::Result<()> {
    use std::thread;
    use std::time::Duration;

    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY: Duration = Duration::from_secs(2);

    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        match try_post_metrics(url, metrics) {
            Ok(()) => {
                if attempt > 1 {
                    tracing::info!(
                        "Successfully posted metrics to aggregator after {attempt} attempts"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < MAX_RETRIES {
                    tracing::warn!(
                        "Failed to POST metrics to aggregator (attempt {}/{}): {}. Retrying in {:?}...",
                        attempt,
                        MAX_RETRIES,
                        last_error.as_ref().unwrap(),
                        RETRY_DELAY
                    );
                    thread::sleep(RETRY_DELAY);
                }
            }
        }
    }

    Err(last_error.unwrap()).context(format!(
        "Failed to POST metrics to aggregator after {MAX_RETRIES} attempts"
    ))
}

/// Single attempt to POST metrics to aggregator.
fn try_post_metrics(
    url: &str,
    metrics: &loadtest_distributed::metrics::ContainerMetrics,
) -> anyhow::Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    // Parse URL to extract host:port and path
    let url = url.strip_prefix("http://").unwrap_or(url);
    let (host, path) = if let Some(pos) = url.find('/') {
        (&url[..pos], &url[pos..])
    } else {
        (url, "/metrics")
    };

    // Connect to aggregator
    let mut stream = TcpStream::connect(host)
        .with_context(|| format!("Failed to connect to aggregator at {host}"))?;

    // Serialize metrics to JSON
    let json =
        serde_json::to_string(metrics).context("Failed to serialize ContainerMetrics to JSON")?;

    // Build HTTP POST request
    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        path,
        host,
        json.len(),
        json
    );

    // Send request
    stream
        .write_all(request.as_bytes())
        .context("Failed to write HTTP request")?;
    stream.flush().context("Failed to flush HTTP request")?;

    // Read response
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("Failed to read HTTP response")?;

    // Validate response status
    if !response.starts_with("HTTP/1.1 200") {
        let status_line = response.lines().next().unwrap_or("(no status line)");
        anyhow::bail!("Aggregator returned non-200 response: {status_line}");
    }

    Ok(())
}

/// Run populate command to fill source database with deterministic test data
async fn run_populate(source: PopulateSource) -> anyhow::Result<()> {
    match source {
        PopulateSource::MySQL { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder to track container start/stop state
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate MySQL with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.mysql_connection_string)
                );
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // Even in dry-run mode, create and POST metrics to test aggregator integration
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating MySQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            // Accumulate metrics from all tables
            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                //
                // The DataGenerator uses an internal index counter that increments with each row.
                // If we reused the same populator across tables, the second table would start at
                // index N (after generating N rows for the first table), causing sequential IDs
                // to be offset (e.g., table2 would have IDs starting at 1001 instead of 1).
                //
                // This offset causes verification failures because:
                // 1. The verifier looks up records by generated ID (e.g., `SELECT * FROM orders:1`)
                // 2. If populate used offset IDs (orders:1001-2000), the record `orders:1` doesn't exist
                // 3. The verifier reports these as "missing" even though the data exists at different IDs
                //
                // Both populate and verify must use the same per-table generator reset strategy.
                let mut populator = loadtest_populate_mysql::MySQLPopulator::new(
                    &args.mysql_connection_string,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to MySQL")?
                .with_batch_size(args.common.batch_size);

                if let Err(e) = populator.create_table(table_name).await {
                    let error_msg = format!("Failed to create table '{table_name}': {e}");
                    tracing::error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} rows in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Finalize container metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // MySQL doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST to aggregator if URL provided
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            // Fail if there were any errors
            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::PostgreSQL { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate PostgreSQL with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.postgresql_connection_string)
                );
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating PostgreSQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = loadtest_populate_postgresql::PostgreSQLPopulator::new(
                    &args.postgresql_connection_string,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to PostgreSQL")?
                .with_batch_size(args.common.batch_size);

                // Skip table creation in data-only mode (tables must already exist)
                if !args.common.data_only {
                    if let Err(e) = populator.create_table(table_name).await {
                        let error_msg = format!("Failed to create table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                }

                // Skip data insertion in schema-only mode
                if args.common.schema_only {
                    tracing::info!(
                        "Skipping data insertion for '{}' (schema-only mode)",
                        table_name
                    );
                    continue;
                }

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} rows in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None,
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::MongoDB { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate MongoDB with {} documents per collection (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.mongodb_connection_string)
                );
                tracing::info!("[DRY-RUN] Database: {}", args.mongodb_database);
                tracing::info!("[DRY-RUN] Collections: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating MongoDB with {} documents per collection (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = loadtest_populate_mongodb::MongoDBPopulator::new(
                    &args.mongodb_connection_string,
                    &args.mongodb_database,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to MongoDB")?
                .with_batch_size(args.common.batch_size);

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} documents in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg =
                            format!("Failed to populate collection '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None,
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Csv { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate CSV files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Generating CSV files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            if let Err(e) = std::fs::create_dir_all(&args.output_dir) {
                let error_msg = format!(
                    "Failed to create output directory {:?}: {}",
                    args.output_dir, e
                );
                tracing::error!("{}", error_msg);
                let container_metrics =
                    metrics_builder.finish_populate(None, vec![error_msg.clone()], false);
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    }
                }
                anyhow::bail!(error_msg);
            }

            let mut total_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator =
                    loadtest_populate_csv::CSVPopulator::new(schema.clone(), args.common.seed);

                let output_path = args.output_dir.join(format!("{table_name}.csv"));

                match populator.populate(table_name, &output_path, args.common.row_count) {
                    Ok(metrics) => {
                        total_rows += metrics.rows_written;
                        total_bytes += metrics.file_size_bytes;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Generated {:?}: {} rows in {:?}",
                            output_path,
                            metrics.rows_written,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to generate CSV for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: 0, // CSV writes all rows at once
                    rows_per_second,
                    bytes_written: Some(total_bytes),
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Jsonl { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate JSONL files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Generating JSONL files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            if let Err(e) = std::fs::create_dir_all(&args.output_dir) {
                let error_msg = format!(
                    "Failed to create output directory {:?}: {}",
                    args.output_dir, e
                );
                tracing::error!("{}", error_msg);
                let container_metrics =
                    metrics_builder.finish_populate(None, vec![error_msg.clone()], false);
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    }
                }
                anyhow::bail!(error_msg);
            }

            let mut total_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator =
                    loadtest_populate_jsonl::JsonlPopulator::new(schema.clone(), args.common.seed);

                let output_path = args.output_dir.join(format!("{table_name}.jsonl"));

                match populator.populate(table_name, &output_path, args.common.row_count) {
                    Ok(metrics) => {
                        total_rows += metrics.rows_written;
                        total_bytes += metrics.file_size_bytes;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Generated {:?}: {} rows in {:?}",
                            output_path,
                            metrics.rows_written,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to generate JSONL for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: 0, // JSONL writes all rows at once
                    rows_per_second,
                    bytes_written: Some(total_bytes),
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Kafka { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate Kafka topics with {} messages per topic (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Brokers: {}", args.kafka_brokers);
                tracing::info!("[DRY-RUN] Topics: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Populating Kafka topics with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = match loadtest_populate_kafka::KafkaPopulator::new(
                    &args.kafka_brokers,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                {
                    Ok(p) => p.with_batch_size(args.common.batch_size),
                    Err(e) => {
                        let error_msg =
                            format!("Failed to create Kafka populator for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                };

                // Prepare table (generates .proto file)
                if let Err(e) = populator.prepare_table(table_name) {
                    let error_msg = format!("Failed to prepare proto for '{table_name}': {e}");
                    tracing::error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }

                // Create topic
                if let Err(e) = populator.create_topic(table_name).await {
                    let error_msg = format!("Failed to create topic '{table_name}': {e}");
                    tracing::error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }

                // Populate
                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.messages_published;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated '{}': {} messages in {:?} ({:.2} msg/sec)",
                            table_name,
                            metrics.messages_published,
                            metrics.total_duration,
                            metrics.messages_per_second()
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate topic '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // Kafka doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Neo4j { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate Neo4j with {} nodes per label (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.neo4j_connection_string)
                );
                tracing::info!("[DRY-RUN] Database: {}", args.neo4j_database);
                tracing::info!("[DRY-RUN] Labels: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Populating Neo4j with {} nodes per label (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = match loadtest_populate_neo4j::Neo4jPopulator::new(
                    &args.neo4j_connection_string,
                    &args.neo4j_username,
                    &args.neo4j_password,
                    &args.neo4j_database,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                {
                    Ok(p) => p.with_batch_size(args.common.batch_size),
                    Err(e) => {
                        let error_msg =
                            format!("Failed to connect to Neo4j for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                };

                // Delete existing nodes (ignore errors)
                populator.delete_nodes(table_name).await.ok();

                // Populate
                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} nodes in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate label '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // Neo4j doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
    }

    tracing::info!("Populate completed successfully");
    Ok(())
}

/// Run verify command to check synced data in SurrealDB
/// This auto-detects the server version and dispatches to the appropriate implementation.
async fn run_verify(args: VerifyArgs) -> anyhow::Result<()> {
    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let ws_endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    // Convert ws:// to http:// for version detection
    let http_endpoint = ws_endpoint
        .replace("ws://", "http://")
        .replace("wss://", "https://");

    // Auto-detect server version
    tracing::info!("Detecting SurrealDB server version at {}", http_endpoint);
    let detected = surreal_version::detect_server_version(&http_endpoint).await;

    match detected {
        Ok(version) => {
            tracing::info!("Detected SurrealDB server version: {:?}", version);
            match version {
                surreal_version::SurrealMajorVersion::V2 => {
                    tracing::info!("Using SurrealDB SDK v2 for verification");
                    run_verify_v2(args).await
                }
                surreal_version::SurrealMajorVersion::V3 => {
                    tracing::info!("Using SurrealDB SDK v3 for verification");
                    run_verify_v3(args).await
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to detect SurrealDB version: {}. Defaulting to v2 SDK.",
                e
            );
            run_verify_v2(args).await
        }
    }
}

/// Run verify command using SurrealDB SDK v2
async fn run_verify_v2(args: VerifyArgs) -> anyhow::Result<()> {
    let schema = Schema::from_file(&args.schema)
        .with_context(|| format!("Failed to load schema from {:?}", args.schema))?;

    let tables = if args.tables.is_empty() {
        schema.table_names()
    } else {
        args.tables.iter().map(|s| s.as_str()).collect()
    };

    // Create metrics builder
    let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
    let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
        loadtest_distributed::metrics::Operation::Verify,
        tables_vec.clone(),
    )?;

    if args.dry_run {
        tracing::info!(
            "[DRY-RUN] Would verify {} rows per table in SurrealDB (seed={})",
            args.row_count,
            args.seed
        );
        tracing::info!("[DRY-RUN] Endpoint: {}", args.surreal_endpoint);
        tracing::info!(
            "[DRY-RUN] Namespace/Database: {}/{}",
            args.surreal_namespace,
            args.surreal_database
        );
        tracing::info!("[DRY-RUN] Tables: {:?}", tables);
        tracing::info!("[DRY-RUN] Schema validated successfully");

        // POST dry-run metrics to aggregator
        let container_metrics = metrics_builder.finish_dry_run();
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            } else {
                tracing::info!("Successfully posted dry-run metrics to aggregator");
            }
        }
        return Ok(());
    }

    tracing::info!(
        "Verifying {} rows per table in SurrealDB v2 (seed={})",
        args.row_count,
        args.seed
    );

    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    let surreal = match surrealdb2::engine::any::connect(&endpoint).await {
        Ok(s) => s,
        Err(e) => {
            let error_msg = format!("Failed to connect to SurrealDB: {e}");
            tracing::error!("{}", error_msg);
            let container_metrics =
                metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
            if let Some(url) = &args.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                }
            }
            anyhow::bail!(error_msg);
        }
    };

    if let Err(e) = surreal
        .signin(surrealdb2::opt::auth::Root {
            username: &args.surreal_username,
            password: &args.surreal_password,
        })
        .await
    {
        let error_msg = format!("Failed to authenticate with SurrealDB: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    if let Err(e) = surreal
        .use_ns(&args.surreal_namespace)
        .use_db(&args.surreal_database)
        .await
    {
        let error_msg = format!("Failed to select namespace/database: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    let mut table_reports = Vec::new();
    let mut errors = Vec::new();

    for table_name in &tables {
        let mut verifier = match loadtest_verify_surreal2::StreamingVerifier::new(
            surreal.clone(),
            schema.clone(),
            args.seed,
            table_name,
        ) {
            Ok(v) => v,
            Err(e) => {
                let error_msg = format!("Failed to create verifier for table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
                continue;
            }
        };

        match verifier.verify_streaming(args.row_count).await {
            Ok(report) => {
                if report.is_success() {
                    tracing::info!(
                        "Table '{}': {} rows verified successfully",
                        table_name,
                        report.matched
                    );
                } else {
                    tracing::error!(
                        "Table '{}': {} matched, {} missing, {} mismatched",
                        table_name,
                        report.matched,
                        report.missing,
                        report.mismatched
                    );
                }
                table_reports.push(report);
            }
            Err(e) => {
                let error_msg = format!("Failed to verify table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
            }
        }
    }

    // Build combined verification report
    let combined_report = if !table_reports.is_empty() {
        let table_results: Vec<loadtest_distributed::metrics::VerificationResult> = tables
            .iter()
            .zip(table_reports.iter())
            .map(
                |(table_name, report)| loadtest_distributed::metrics::VerificationResult {
                    table_name: table_name.to_string(),
                    expected: report.expected,
                    found: report.found,
                    missing: report.missing,
                    mismatched: report.mismatched,
                    matched: report.matched,
                },
            )
            .collect();

        Some(loadtest_distributed::metrics::VerificationReport {
            tables: table_results,
            total_expected: table_reports.iter().map(|r| r.expected).sum(),
            total_found: table_reports.iter().map(|r| r.found).sum(),
            total_missing: table_reports.iter().map(|r| r.missing).sum(),
            total_mismatched: table_reports.iter().map(|r| r.mismatched).sum(),
            total_matched: table_reports.iter().map(|r| r.matched).sum(),
        })
    } else {
        None
    };

    let success = errors.is_empty() && table_reports.iter().all(|r| r.is_success());
    let container_metrics = metrics_builder.finish_verify(combined_report, errors.clone(), success);

    // POST metrics to aggregator
    if let Some(url) = &args.aggregator_url {
        if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
            tracing::warn!("Failed to POST metrics to aggregator: {}", e);
        } else {
            tracing::info!("Successfully posted metrics to aggregator");
        }
    }

    if success {
        tracing::info!("Verification completed successfully - all tables match expected data");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Verification failed - some tables have missing or mismatched data"
        ))
    }
}

/// Run verify command using SurrealDB SDK v3
async fn run_verify_v3(args: VerifyArgs) -> anyhow::Result<()> {
    let schema = Schema::from_file(&args.schema)
        .with_context(|| format!("Failed to load schema from {:?}", args.schema))?;

    let tables = if args.tables.is_empty() {
        schema.table_names()
    } else {
        args.tables.iter().map(|s| s.as_str()).collect()
    };

    // Create metrics builder
    let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
    let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
        loadtest_distributed::metrics::Operation::Verify,
        tables_vec.clone(),
    )?;

    if args.dry_run {
        tracing::info!(
            "[DRY-RUN] Would verify {} rows per table in SurrealDB v3 (seed={})",
            args.row_count,
            args.seed
        );
        tracing::info!("[DRY-RUN] Endpoint: {}", args.surreal_endpoint);
        tracing::info!(
            "[DRY-RUN] Namespace/Database: {}/{}",
            args.surreal_namespace,
            args.surreal_database
        );
        tracing::info!("[DRY-RUN] Tables: {:?}", tables);
        tracing::info!("[DRY-RUN] Schema validated successfully");

        // POST dry-run metrics to aggregator
        let container_metrics = metrics_builder.finish_dry_run();
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            } else {
                tracing::info!("Successfully posted dry-run metrics to aggregator");
            }
        }
        return Ok(());
    }

    tracing::info!(
        "Verifying {} rows per table in SurrealDB v3 (seed={})",
        args.row_count,
        args.seed
    );

    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    let surreal = match surrealdb3::engine::any::connect(&endpoint).await {
        Ok(s) => s,
        Err(e) => {
            let error_msg = format!("Failed to connect to SurrealDB v3: {e}");
            tracing::error!("{}", error_msg);
            let container_metrics =
                metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
            if let Some(url) = &args.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                }
            }
            anyhow::bail!(error_msg);
        }
    };

    // V3 SDK requires String for auth credentials
    if let Err(e) = surreal
        .signin(surrealdb3::opt::auth::Root {
            username: args.surreal_username.clone(),
            password: args.surreal_password.clone(),
        })
        .await
    {
        let error_msg = format!("Failed to authenticate with SurrealDB v3: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    if let Err(e) = surreal
        .use_ns(&args.surreal_namespace)
        .use_db(&args.surreal_database)
        .await
    {
        let error_msg = format!("Failed to select namespace/database: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    let mut table_reports = Vec::new();
    let mut errors = Vec::new();

    for table_name in &tables {
        let mut verifier = match loadtest_verify_surreal3::StreamingVerifier3::new(
            surreal.clone(),
            schema.clone(),
            args.seed,
            table_name,
        ) {
            Ok(v) => v,
            Err(e) => {
                let error_msg =
                    format!("Failed to create v3 verifier for table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
                continue;
            }
        };

        match verifier.verify_streaming(args.row_count).await {
            Ok(report) => {
                if report.is_success() {
                    tracing::info!(
                        "Table '{}': {} rows verified successfully",
                        table_name,
                        report.matched
                    );
                } else {
                    tracing::error!(
                        "Table '{}': {} matched, {} missing, {} mismatched",
                        table_name,
                        report.matched,
                        report.missing,
                        report.mismatched
                    );
                }
                table_reports.push(report);
            }
            Err(e) => {
                let error_msg = format!("Failed to verify table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
            }
        }
    }

    // Build combined verification report
    let combined_report = if !table_reports.is_empty() {
        let table_results: Vec<loadtest_distributed::metrics::VerificationResult> = tables
            .iter()
            .zip(table_reports.iter())
            .map(
                |(table_name, report)| loadtest_distributed::metrics::VerificationResult {
                    table_name: table_name.to_string(),
                    expected: report.expected,
                    found: report.found,
                    missing: report.missing,
                    mismatched: report.mismatched,
                    matched: report.matched,
                },
            )
            .collect();

        Some(loadtest_distributed::metrics::VerificationReport {
            tables: table_results,
            total_expected: table_reports.iter().map(|r| r.expected).sum(),
            total_found: table_reports.iter().map(|r| r.found).sum(),
            total_missing: table_reports.iter().map(|r| r.missing).sum(),
            total_mismatched: table_reports.iter().map(|r| r.mismatched).sum(),
            total_matched: table_reports.iter().map(|r| r.matched).sum(),
        })
    } else {
        None
    };

    let success = errors.is_empty() && table_reports.iter().all(|r| r.is_success());
    let container_metrics = metrics_builder.finish_verify(combined_report, errors.clone(), success);

    // POST metrics to aggregator
    if let Some(url) = &args.aggregator_url {
        if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
            tracing::warn!("Failed to POST metrics to aggregator: {}", e);
        } else {
            tracing::info!("Successfully posted metrics to aggregator");
        }
    }

    if success {
        tracing::info!("Verification completed successfully - all tables match expected data");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Verification failed - some tables have missing or mismatched data"
        ))
    }
}

// =============================================================================
// Loadtest Generate Handler
// =============================================================================

/// Run the loadtest generate command.
async fn run_loadtest_generate(args: GenerateArgs) -> anyhow::Result<()> {
    tracing::info!("Generating load test configuration...");
    tracing::info!("Platform: {:?}", args.platform);
    tracing::info!("Source: {:?}", args.source);
    tracing::info!("Preset: {:?}", args.preset);

    // Convert CLI enums to internal types
    let preset_size = args.preset.into();
    let source_type = args.source.into();
    let platforms = args.platform.to_platforms();

    // Get tables from schema file
    let schema_content = std::fs::read_to_string(&args.schema)
        .with_context(|| format!("Failed to read schema file: {:?}", args.schema))?;
    let schema: sync_core::GeneratorSchema =
        serde_yaml::from_str(&schema_content).with_context(|| "Failed to parse schema YAML")?;
    let tables: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();

    // Build cluster configuration
    let mut config = build_cluster_config(
        preset_size,
        source_type,
        platforms[0], // Use first platform for config
        args.num_containers,
        args.cpu_limit.clone(),
        args.memory_limit.clone(),
        if args.tmpfs {
            args.tmpfs_size.clone()
        } else {
            None
        },
        args.row_count,
        args.batch_size,
        Some(args.schema.clone()),
        &tables,
        args.dry_run,
        args.surrealdb_image.clone(),
    )?;

    // Set schema content for Kubernetes ConfigMap embedding
    if platforms.contains(&Platform::Kubernetes) {
        config.schema_content = Some(schema_content.clone());

        // For Kafka source, generate proto files and embed in ConfigMap
        if source_type == SourceType::Kafka {
            let mut proto_contents = std::collections::HashMap::new();
            for table in &schema.tables {
                let proto_content = generate_proto_for_table(table, "loadtest");
                proto_contents.insert(table.name.clone(), proto_content);
            }
            config.proto_contents = Some(proto_contents);
        }
    }

    // Output ClusterConfig as JSON for CI tooling (single line, easy to parse)
    // This allows run_ci.py to get the effective row_count used by the generator
    println!("{}", serde_json::to_string(&config)?);

    // Create output directory
    let output_dir = &args.output_dir;
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {output_dir:?}"))?;

    // Generate configurations
    for platform in &platforms {
        match platform {
            Platform::DockerCompose => {
                let generator = DockerComposeGenerator;
                let content = generator.generate(&config)?;
                let path = output_dir.join(generator.filename());
                std::fs::write(&path, &content)
                    .with_context(|| format!("Failed to write {}", path.display()))?;
                tracing::info!("Generated: {}", path.display());
            }
            Platform::Kubernetes => {
                let generator = KubernetesGenerator;

                // Option 1: Single file
                let content = generator.generate(&config)?;
                let path = output_dir.join(generator.filename());
                std::fs::write(&path, &content)
                    .with_context(|| format!("Failed to write {}", path.display()))?;
                tracing::info!("Generated: {}", path.display());

                // Option 2: Multiple files in subdirectory
                let k8s_dir = output_dir.join("kubernetes");
                std::fs::create_dir_all(&k8s_dir)?;
                let files = generator.generate_to_files(&config)?;
                for (filename, content) in files {
                    let path = k8s_dir.join(&filename);
                    std::fs::write(&path, &content)
                        .with_context(|| format!("Failed to write {}", path.display()))?;
                    tracing::info!("Generated: {}", path.display());
                }
            }
        }
    }

    // Copy schema file
    let schema_dest = output_dir.join("config").join("schema.yaml");
    std::fs::create_dir_all(schema_dest.parent().unwrap())?;
    std::fs::copy(&args.schema, &schema_dest)
        .with_context(|| format!("Failed to copy schema from {:?}", args.schema))?;
    tracing::info!("Copied schema to: {}", schema_dest.display());

    // Generate proto files for Kafka source
    if source_type == SourceType::Kafka {
        let proto_dir = output_dir.join("config").join("proto");
        std::fs::create_dir_all(&proto_dir)
            .with_context(|| format!("Failed to create proto directory: {proto_dir:?}"))?;

        for table in &schema.tables {
            let proto_content = generate_proto_for_table(table, "loadtest");
            let proto_path = proto_dir.join(format!("{}.proto", table.name));
            std::fs::write(&proto_path, &proto_content)
                .with_context(|| format!("Failed to write proto file: {proto_path:?}"))?;
            tracing::info!("Generated proto file: {}", proto_path.display());
        }
    }

    tracing::info!(
        "Configuration generated successfully in: {}",
        output_dir.display()
    );
    tracing::info!("");
    tracing::info!("Next steps:");
    if platforms.contains(&Platform::DockerCompose) {
        tracing::info!("  Docker Compose:");
        tracing::info!(
            "    cd {:?} && docker-compose -f docker-compose.loadtest.yml up",
            output_dir
        );
    }
    if platforms.contains(&Platform::Kubernetes) {
        tracing::info!("  Kubernetes:");
        tracing::info!("    kubectl apply -f {:?}/kubernetes/", output_dir);
    }

    Ok(())
}

// =============================================================================
// Aggregate Server Handler
// =============================================================================

/// Run the aggregate server command.
async fn run_aggregate_server(args: AggregateServerArgs) -> anyhow::Result<()> {
    loadtest_distributed::run_aggregator_server(args).await
}

// =============================================================================
// Helper Functions
// =============================================================================

fn load_schema_if_provided(schema_file: &Option<PathBuf>) -> anyhow::Result<Option<Schema>> {
    if let Some(schema_path) = schema_file {
        Ok(Some(Schema::from_file(schema_path).with_context(|| {
            format!("Failed to load sync schema from {schema_path:?}")
        })?))
    } else {
        Ok(None)
    }
}

/// Extract JSON field paths from a schema (e.g., ["users.profile_data", "products.metadata"]).
/// This is used to auto-populate Neo4j JSON properties from the schema file.
fn extract_json_fields_from_schema(schema: &Schema) -> Vec<String> {
    use sync_core::UniversalType;

    let mut json_fields = Vec::new();
    for table in &schema.tables {
        for field in &table.fields {
            if matches!(field.field_type, UniversalType::Json | UniversalType::Jsonb) {
                json_fields.push(format!("{}.{}", table.name, field.name));
            }
        }
    }
    json_fields
}

/// Parse a duration string like "1h", "30m", "300s", "300" into seconds.
/// Supports:
/// - Plain numbers (interpreted as seconds): "300"
/// - Seconds suffix: "300s"
/// - Minutes suffix: "30m"
/// - Hours suffix: "1h"
fn parse_duration_to_secs(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    // Check for suffix
    if let Some(num_str) = s.strip_suffix('h') {
        let hours: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid hours value: {num_str}"))?;
        return Ok(hours * 3600);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let minutes: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid minutes value: {num_str}"))?;
        return Ok(minutes * 60);
    }
    if let Some(num_str) = s.strip_suffix('s') {
        let secs: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid seconds value: {num_str}"))?;
        return Ok(secs);
    }

    // No suffix - treat as seconds
    s.parse::<i64>()
        .with_context(|| format!("Invalid duration value: {s}"))
}

/// Extract database name from a PostgreSQL connection string.
/// Supports formats like: postgresql://user:pass@host:port/database
fn extract_postgresql_database(connection_string: &str) -> Option<String> {
    // Try to extract from connection string
    // Format: postgresql://user:pass@host:port/database?params
    if let Some(db_start) = connection_string.rfind('/') {
        let after_slash = &connection_string[db_start + 1..];
        // Remove query params if present
        let db_name = after_slash.split('?').next().unwrap_or(after_slash);
        if !db_name.is_empty() {
            return Some(db_name.to_string());
        }
    }

    None
}
