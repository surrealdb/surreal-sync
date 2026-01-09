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
use surreal_sync_surreal::surreal_connect;

// Database-specific sync crates (fully-qualified paths used in match arms)
#[allow(clippy::single_component_path_imports)]
use surreal_sync_mongodb;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_mysql_trigger;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_neo4j;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql_logical_replication;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_postgresql_trigger;

// Load testing imports
use loadtest_populate_csv::CSVPopulateArgs;
use loadtest_populate_jsonl::JSONLPopulateArgs;
use loadtest_populate_kafka::KafkaPopulateArgs;
use loadtest_populate_mongodb::MongoDBPopulateArgs;
use loadtest_populate_mysql::MySQLPopulateArgs;
use loadtest_populate_postgresql::PostgreSQLPopulateArgs;
use loadtest_verify::VerifyArgs;
use sync_core::Schema;

// Load testing distributed imports
use loadtest_distributed::{
    build_cluster_config,
    generator::{ConfigGenerator, DockerComposeGenerator, KubernetesGenerator},
    AggregateServerArgs, GenerateArgs, Platform,
};

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
        command: LoadtestCommand,
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

    /// Sync from PostgreSQL using WAL-based logical replication (continuous sync)
    #[command(name = "postgresql")]
    PostgreSQL(PostgreSQLArgs),

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
    /// PostgreSQL connection string
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// PostgreSQL database name (extracted from connection string if not provided)
    #[arg(long, env = "POSTGRESQL_DATABASE")]
    database: Option<String>,

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
    /// PostgreSQL connection string
    #[arg(long, env = "POSTGRESQL_URI")]
    connection_string: String,

    /// PostgreSQL database name (extracted from connection string if not provided)
    #[arg(long, env = "POSTGRESQL_DATABASE")]
    database: Option<String>,

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
// PostgreSQL WAL-based Logical Replication Args (single command - continuous sync)
// =============================================================================

#[derive(Args)]
struct PostgreSQLArgs {
    /// PostgreSQL connection string
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
    config: surreal_sync_kafka::Config,

    /// Target SurrealDB namespace
    #[arg(long)]
    to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    to_database: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    schema_file: Option<PathBuf>,

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
        Commands::Loadtest { command } => handle_loadtest_command(command).await?,
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
        FromSource::PostgreSQL(args) => run_postgresql_logical(args).await?,
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
    tracing::info!("Starting full sync from MongoDB to SurrealDB");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let sync_config = if args.emit_checkpoints {
        Some(checkpoint::SyncConfig {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(args.checkpoint_dir),
        })
    } else {
        None
    };

    let source_opts = surreal_sync_mongodb::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb::run_full_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_mongodb::SurrealOpts::from(&args.surreal),
        sync_config,
    )
    .await?;

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mongodb_incremental(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MongoDB to SurrealDB");
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

    let mongodb_from =
        surreal_sync_mongodb::MongoDBCheckpoint::from_cli_string(&args.incremental_from)?;
    let mongodb_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mongodb::MongoDBCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mongodb::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb::run_incremental_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_mongodb::SurrealOpts::from(&args.surreal),
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
    tracing::info!("Starting full sync from Neo4j to SurrealDB");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let sync_config = if args.emit_checkpoints {
        Some(checkpoint::SyncConfig {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(args.checkpoint_dir),
        })
    } else {
        None
    };

    let source_opts = surreal_sync_neo4j::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: args.json_properties,
    };

    surreal_sync_neo4j::run_full_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_neo4j::SurrealOpts::from(&args.surreal),
        sync_config,
    )
    .await?;

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_neo4j_incremental(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from Neo4j to SurrealDB");
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

    let neo4j_from = surreal_sync_neo4j::Neo4jCheckpoint::from_cli_string(&args.incremental_from)?;
    let neo4j_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_neo4j::Neo4jCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_neo4j::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: args.json_properties,
    };

    surreal_sync_neo4j::run_incremental_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_neo4j::SurrealOpts::from(&args.surreal),
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
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let sync_config = if args.emit_checkpoints {
        Some(checkpoint::SyncConfig {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(args.checkpoint_dir),
        })
    } else {
        None
    };

    let source_opts = surreal_sync_postgresql_trigger::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
    };

    surreal_sync_postgresql_trigger::run_full_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_postgresql::SurrealOpts::from(&args.surreal),
        sync_config,
    )
    .await?;

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_postgresql_trigger_incremental(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB");
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

    let pg_from = surreal_sync_postgresql_trigger::PostgreSQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_postgresql_trigger::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
    };

    surreal_sync_postgresql_trigger::run_incremental_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_postgresql::SurrealOpts::from(&args.surreal),
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
    tracing::info!("Starting full sync from MySQL to SurrealDB");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_conn_opts = surreal_sync_surreal::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal_connect(&surreal_conn_opts, &args.to_namespace, &args.to_database).await?;

    let sync_config = if args.emit_checkpoints {
        Some(checkpoint::SyncConfig {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(args.checkpoint_dir),
        })
    } else {
        None
    };

    let source_opts = surreal_sync_mysql_trigger::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    surreal_sync_mysql_trigger::run_full_sync(
        &source_opts,
        &surreal_sync_mysql_trigger::SurrealOpts::from(&args.surreal),
        sync_config,
        &surreal,
    )
    .await?;

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_mysql_incremental(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL to SurrealDB");
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

    let mysql_from =
        surreal_sync_mysql_trigger::MySQLCheckpoint::from_cli_string(&args.incremental_from)?;
    let mysql_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mysql_trigger::MySQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mysql_trigger::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        mysql_boolean_paths: args.boolean_paths,
    };

    surreal_sync_mysql_trigger::run_incremental_sync(
        source_opts,
        args.to_namespace,
        args.to_database,
        surreal_sync_mysql_trigger::SurrealOpts::from(&args.surreal),
        mysql_from,
        deadline,
        mysql_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// PostgreSQL WAL-based Logical Replication Handler
// =============================================================================

async fn run_postgresql_logical(args: PostgreSQLArgs) -> anyhow::Result<()> {
    tracing::info!("Starting PostgreSQL WAL-based logical replication");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let config = surreal_sync_postgresql_logical_replication::sync::Config {
        connection_string: args.connection_string,
        slot: args.slot,
        tables: args.tables,
        schema: args.schema,
        to_namespace: args.to_namespace,
        to_database: args.to_database,
    };

    let surreal_opts =
        surreal_sync_postgresql_logical_replication::sync::SurrealOpts::from(&args.surreal);
    surreal_sync_postgresql_logical_replication::sync::sync(config, surreal_opts).await?;

    Ok(())
}

// =============================================================================
// Kafka Handler
// =============================================================================

async fn run_kafka(args: KafkaArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

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

    surreal_sync_kafka::run_incremental_sync(
        args.config,
        args.to_namespace,
        args.to_database,
        surreal_sync_kafka::SurrealOpts::from(&args.surreal),
        chrono::Utc::now() + chrono::Duration::hours(1),
        table_schema,
    )
    .await?;

    Ok(())
}

// =============================================================================
// CSV Handler
// =============================================================================

async fn run_csv(args: CsvArgs) -> anyhow::Result<()> {
    tracing::info!("Starting CSV import");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    let config = surreal_sync::csv::Config {
        sources: vec![],
        files: args.files,
        s3_uris: args.s3_uris,
        http_uris: args.http_uris,
        table: args.table,
        batch_size: args.surreal.batch_size,
        namespace: args.to_namespace,
        database: args.to_database,
        surreal_opts: surreal_sync_surreal::SurrealOpts {
            surreal_endpoint: args.surreal.surreal_endpoint,
            surreal_username: args.surreal.surreal_username,
            surreal_password: args.surreal.surreal_password,
        },
        has_headers: args.has_headers,
        delimiter: args.delimiter as u8,
        id_field: args.id_field,
        column_names: args.column_names,
        emit_metrics: args.emit_metrics,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::csv::sync(config).await?;

    tracing::info!("CSV import completed successfully");
    Ok(())
}

// =============================================================================
// JSONL Handler
// =============================================================================

async fn run_jsonl(args: JsonlArgs) -> anyhow::Result<()> {
    tracing::info!("Starting JSONL import");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let jsonl_from_opts = surreal_sync::jsonl::SourceOpts {
        source_uri: args.path,
    };
    let jsonl_to_opts = surreal_sync_surreal::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };

    surreal_sync::jsonl::migrate_from_jsonl(
        jsonl_from_opts,
        args.to_namespace,
        args.to_database,
        jsonl_to_opts,
        args.id_field,
        args.conversion_rules,
    )
    .await?;

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
                return Ok(());
            }

            tracing::info!(
                "Populating MySQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut populator = loadtest_populate_mysql::MySQLPopulator::new(
                &args.mysql_connection_string,
                schema.clone(),
                args.common.seed,
            )
            .await
            .context("Failed to connect to MySQL")?
            .with_batch_size(args.common.batch_size);

            for table_name in &tables {
                populator
                    .create_table(table_name)
                    .await
                    .with_context(|| format!("Failed to create table '{table_name}'"))?;

                let metrics = populator
                    .populate(table_name, args.common.row_count)
                    .await
                    .with_context(|| format!("Failed to populate table '{table_name}'"))?;

                tracing::info!(
                    "Populated {}: {} rows in {:?}",
                    table_name,
                    metrics.rows_inserted,
                    metrics.total_duration
                );
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
                return Ok(());
            }

            tracing::info!(
                "Populating PostgreSQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut populator = loadtest_populate_postgresql::PostgreSQLPopulator::new(
                &args.postgresql_connection_string,
                schema.clone(),
                args.common.seed,
            )
            .await
            .context("Failed to connect to PostgreSQL")?
            .with_batch_size(args.common.batch_size);

            for table_name in &tables {
                populator
                    .create_table(table_name)
                    .await
                    .with_context(|| format!("Failed to create table '{table_name}'"))?;

                let metrics = populator
                    .populate(table_name, args.common.row_count)
                    .await
                    .with_context(|| format!("Failed to populate table '{table_name}'"))?;

                tracing::info!(
                    "Populated {}: {} rows in {:?}",
                    table_name,
                    metrics.rows_inserted,
                    metrics.total_duration
                );
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
                return Ok(());
            }

            tracing::info!(
                "Populating MongoDB with {} documents per collection (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut populator = loadtest_populate_mongodb::MongoDBPopulator::new(
                &args.mongodb_connection_string,
                &args.mongodb_database,
                schema.clone(),
                args.common.seed,
            )
            .await
            .context("Failed to connect to MongoDB")?
            .with_batch_size(args.common.batch_size);

            for table_name in &tables {
                let metrics = populator
                    .populate(table_name, args.common.row_count)
                    .await
                    .with_context(|| format!("Failed to populate collection '{table_name}'"))?;

                tracing::info!(
                    "Populated {}: {} documents in {:?}",
                    table_name,
                    metrics.rows_inserted,
                    metrics.total_duration
                );
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

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate CSV files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");
                return Ok(());
            }

            tracing::info!(
                "Generating CSV files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            std::fs::create_dir_all(&args.output_dir).with_context(|| {
                format!("Failed to create output directory {:?}", args.output_dir)
            })?;

            let mut populator =
                loadtest_populate_csv::CSVPopulator::new(schema.clone(), args.common.seed);

            for table_name in &tables {
                let output_path = args.output_dir.join(format!("{table_name}.csv"));
                let metrics = populator
                    .populate(table_name, &output_path, args.common.row_count)
                    .with_context(|| format!("Failed to generate CSV for '{table_name}'"))?;

                tracing::info!(
                    "Generated {:?}: {} rows in {:?}",
                    output_path,
                    metrics.rows_written,
                    metrics.total_duration
                );
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

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate JSONL files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");
                return Ok(());
            }

            tracing::info!(
                "Generating JSONL files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            std::fs::create_dir_all(&args.output_dir).with_context(|| {
                format!("Failed to create output directory {:?}", args.output_dir)
            })?;

            let mut populator =
                loadtest_populate_jsonl::JsonlPopulator::new(schema.clone(), args.common.seed);

            for table_name in &tables {
                let output_path = args.output_dir.join(format!("{table_name}.jsonl"));
                let metrics = populator
                    .populate(table_name, &output_path, args.common.row_count)
                    .with_context(|| format!("Failed to generate JSONL for '{table_name}'"))?;

                tracing::info!(
                    "Generated {:?}: {} rows in {:?}",
                    output_path,
                    metrics.rows_written,
                    metrics.total_duration
                );
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

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate Kafka topics with {} messages per topic (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Brokers: {}", args.kafka_brokers);
                tracing::info!("[DRY-RUN] Topics: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");
                return Ok(());
            }

            tracing::info!(
                "Populating Kafka topics with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            for table_name in &tables {
                // Create a fresh populator for each table to reset the generator index
                let mut populator = loadtest_populate_kafka::KafkaPopulator::new(
                    &args.kafka_brokers,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to create Kafka populator")?
                .with_batch_size(args.common.batch_size);

                // Prepare table (generates .proto file)
                let proto_path = populator
                    .prepare_table(table_name)
                    .with_context(|| format!("Failed to prepare proto for '{table_name}'"))?;

                tracing::info!("Generated proto file: {:?}", proto_path);

                // Create topic
                populator
                    .create_topic(table_name)
                    .await
                    .with_context(|| format!("Failed to create topic '{table_name}'"))?;

                // Populate
                let metrics = populator
                    .populate(table_name, args.common.row_count)
                    .await
                    .with_context(|| format!("Failed to populate topic '{table_name}'"))?;

                tracing::info!(
                    "Populated '{}': {} messages in {:?} ({:.2} msg/sec)",
                    table_name,
                    metrics.messages_published,
                    metrics.total_duration,
                    metrics.messages_per_second()
                );
            }
        }
    }

    tracing::info!("Populate completed successfully");
    Ok(())
}

/// Run verify command to check synced data in SurrealDB
async fn run_verify(args: VerifyArgs) -> anyhow::Result<()> {
    let schema = Schema::from_file(&args.schema)
        .with_context(|| format!("Failed to load schema from {:?}", args.schema))?;

    let tables = if args.tables.is_empty() {
        schema.table_names()
    } else {
        args.tables.iter().map(|s| s.as_str()).collect()
    };

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
        return Ok(());
    }

    tracing::info!(
        "Verifying {} rows per table in SurrealDB (seed={})",
        args.row_count,
        args.seed
    );

    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    let surreal = surrealdb::engine::any::connect(&endpoint)
        .await
        .context("Failed to connect to SurrealDB")?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &args.surreal_username,
            password: &args.surreal_password,
        })
        .await
        .context("Failed to authenticate with SurrealDB")?;

    surreal
        .use_ns(&args.surreal_namespace)
        .use_db(&args.surreal_database)
        .await
        .context("Failed to select namespace/database")?;

    let mut all_passed = true;

    for table_name in &tables {
        let mut verifier = loadtest_verify::StreamingVerifier::new(
            surreal.clone(),
            schema.clone(),
            args.seed,
            table_name,
        )
        .with_context(|| format!("Failed to create verifier for table '{table_name}'"))?;

        let report = verifier
            .verify_streaming(args.row_count)
            .await
            .with_context(|| format!("Failed to verify table '{table_name}'"))?;

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
            all_passed = false;
        }
    }

    if all_passed {
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
    let config = build_cluster_config(
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
    )?;

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
    tracing::info!(
        "Starting aggregator server on {} (expecting {} containers)",
        args.listen,
        args.expected_containers
    );

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
