//! Command-line interface for surreal-sync
//!
//! # Usage Examples
//!
//! ## Full Sync
//! ```bash
//! # MongoDB full sync
//! surreal-sync sync mongodb \
//!   --source-uri mongodb://localhost:27017 \
//!   --source-database mydb \
//!   --to-namespace test --to-database test
//!
//! # Neo4j full sync with checkpoints
//! surreal-sync sync neo4j \
//!   --source-uri bolt://localhost:7687 \
//!   --to-namespace test --to-database test \
//!   --emit-checkpoints
//! ```
//!
//! ## Incremental Sync
//! ```bash
//! # MongoDB incremental sync from checkpoint
//! surreal-sync incremental mongodb \
//!   --source-uri mongodb://localhost:27017 \
//!   --source-database mydb \
//!   --to-namespace test --to-database test \
//!   --incremental-from "mongodb::2024-01-01T00:00:00Z"
//!
//! # Neo4j incremental sync between checkpoints
//! surreal-sync incremental neo4j \
//!   --source-uri bolt://localhost:7687 \
//!   --to-namespace test --to-database test \
//!   --incremental-from "neo4j:2024-01-01T10:00:00Z" \
//!   --incremental-to "neo4j:2024-01-01T12:00:00Z"
//! ```
//!
//! ```bash
//! # Run full sync with checkpoint emission
//! surreal-sync sync mongodb \
//!   --source-uri mongodb://localhost:27017 \
//!   --source-database mydb \
//!   --to-namespace test --to-database test \
//!   --emit-checkpoints
//!
//! # Run incremental sync from checkpoint
//! surreal-sync incremental mongodb \
//!   --source-uri mongodb://localhost:27017 \
//!   --source-database mydb \
//!   --to-namespace test --to-database test \
//!   --incremental-from mongodb:token:2024-01-01T00:00:00Z
//! ```
//!
//! ## Checkpoint Formats
//! - Neo4j: `neo4j:2024-01-01T00:00:00Z` (timestamp-based)
//! - MongoDB: `mongodb:base64token:2024-01-01T00:00:00Z` (resume token + timestamp)
//! - PostgreSQL: `postgresql:sequence:123` (trigger-based audit table)
//! - MySQL: `mysql:sequence:456` (trigger-based audit table)

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use surreal_sync::surreal::surreal_connect;
use surreal_sync::{
    csv, jsonl, kafka, mongodb, mysql, neo4j, postgresql,
    sync::{SyncCheckpoint, SyncConfig},
    SourceOpts, SurrealOpts,
};

#[derive(Parser)]
#[command(name = "surreal-sync")]
#[command(about = "A tool for migrating Neo4j, MongoDB, and JSONL data to SurrealDB")]
#[command(long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate data from source database to SurrealDB (full sync)
    Full {
        /// Source database type
        #[arg(value_enum)]
        from: SourceDatabase,

        /// Source database connection options
        #[command(flatten)]
        from_opts: SourceOpts,

        /// Target SurrealDB namespace
        #[arg(long)]
        to_namespace: String,

        /// Target SurrealDB database
        #[arg(long)]
        to_database: String,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,

        /// ID field name for JSONL source (default: "id")
        #[arg(long, default_value = "id")]
        id_field: String,

        /// Conversion rules for JSONL source (format: 'type="page_id",page_id page:page_id')
        #[arg(long = "rule", value_name = "RULE")]
        conversion_rules: Vec<String>,

        /// Emit checkpoint files for coordinating with incremental sync
        #[arg(long)]
        emit_checkpoints: bool,

        /// Directory to write checkpoint files (default: .surreal-sync-checkpoints)
        #[arg(long, default_value = ".surreal-sync-checkpoints")]
        checkpoint_dir: String,
    },

    /// Run incremental sync from source database to SurrealDB
    Incremental {
        /// Source database type
        #[arg(value_enum)]
        from: SourceDatabase,

        /// Source database connection options
        #[command(flatten)]
        from_opts: SourceOpts,

        /// Target SurrealDB namespace
        #[arg(long)]
        to_namespace: String,

        /// Target SurrealDB database
        #[arg(long)]
        to_database: String,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,

        /// Start incremental sync from this checkpoint
        /// Format: checkpoint_type:value (e.g., "neo4j_tx:12345", "timestamp:2024-01-01T00:00:00Z")
        #[arg(long)]
        incremental_from: String,

        /// Stop incremental sync when reaching this checkpoint (optional)
        /// Format: checkpoint_type:value
        #[arg(long)]
        incremental_to: Option<String>,

        /// Maximum time to run incremental sync (optional, default: 1 hour)
        /// Format: duration in seconds or with units like "30m", "2h", "1d"
        #[arg(long, default_value = "3600")]
        timeout: String,

        /// Property name for change tracking (Neo4j only, default: "updated_at")
        /// Only used when CDC is not available
        #[arg(long, default_value = "updated_at")]
        change_tracking_property: String,

        /// Force use of custom change tracking instead of CDC (Neo4j only)
        #[arg(long)]
        no_cdc: bool,
    },

    Kafka {
        /// Kafka source configuration
        #[command(flatten)]
        config: kafka::Config,

        /// Target SurrealDB namespace
        #[arg(long)]
        to_namespace: String,

        /// Target SurrealDB database
        #[arg(long)]
        to_database: String,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,
    },

    #[command(name = "postgresql")]
    PostgreSQL {
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: String,

        /// Replication slot name (default: "surreal_sync_slot")
        #[arg(long, default_value = "surreal_sync_slot")]
        slot: String,

        /// Tables to sync (comma-separated, empty means all tables)
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,

        /// PostgreSQL schema (default: "public")
        #[arg(long, default_value = "public")]
        schema: String,

        /// Target SurrealDB namespace
        #[arg(long)]
        to_namespace: String,

        /// Target SurrealDB database
        #[arg(long)]
        to_database: String,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,
    },

    /// Import CSV files to SurrealDB
    #[group(id = "source", required = true, multiple = false, args = ["files", "s3-uris", "http-uris"])]
    Csv {
        /// CSV file paths to import (can specify multiple)
        #[arg(long, required = true, value_name = "FILE")]
        files: Vec<std::path::PathBuf>,

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

        /// Whether the CSV has headers (default: true)
        #[arg(long, default_value = "true")]
        has_headers: bool,

        /// CSV delimiter character (default: ',')
        #[arg(long, default_value = ",")]
        delimiter: char,

        /// Field to use as record ID (optional, auto-generates if not specified)
        #[arg(long)]
        id_field: Option<String>,

        /// Column names when has_headers is false (comma-separated, e.g., "id,name,age")
        /// Must match the number of columns in the CSV file
        #[arg(long, value_delimiter = ',')]
        column_names: Option<Vec<String>>,

        /// Emit metrics to this file during execution (for load testing)
        #[arg(long, value_name = "PATH")]
        emit_metrics: Option<std::path::PathBuf>,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum SourceDatabase {
    /// MongoDB database
    #[value(name = "mongodb")]
    MongoDB,
    /// Neo4j graph database
    #[value(name = "neo4j")]
    Neo4j,
    /// PostgreSQL database
    #[value(name = "postgresql")]
    PostgreSQL,
    /// MySQL database
    #[value(name = "mysql")]
    MySQL,
    /// JSONL files directory
    #[value(name = "jsonl")]
    Jsonl,
}

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
        Commands::Full {
            from,
            from_opts,
            to_namespace,
            to_database,
            to_opts,
            id_field,
            conversion_rules,
            emit_checkpoints,
            checkpoint_dir,
        } => {
            run_full_sync(
                from,
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                id_field,
                conversion_rules,
                emit_checkpoints,
                checkpoint_dir,
            )
            .await?;
        }
        Commands::Incremental {
            from,
            from_opts,
            to_namespace,
            to_database,
            to_opts,
            incremental_from,
            incremental_to,
            timeout,
            change_tracking_property: _,
            no_cdc: _,
        } => {
            run_incremental_sync(
                from,
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                incremental_from,
                incremental_to,
                timeout,
            )
            .await?;
        }
        Commands::Kafka {
            config,
            to_namespace,
            to_database,
            to_opts,
        } => {
            kafka::run_incremental_sync(
                config,
                to_namespace,
                to_database,
                to_opts,
                chrono::Utc::now() + chrono::Duration::hours(1),
            )
            .await?;
        }
        Commands::PostgreSQL {
            connection_string,
            slot,
            tables,
            schema,
            to_namespace,
            to_database,
            to_opts,
        } => {
            let config = postgresql::Config {
                connection_string,
                slot,
                tables,
                schema,
                to_namespace,
                to_database,
                to_opts,
            };
            postgresql::sync(config).await?;
        }
        Commands::Csv {
            files,
            s3_uris,
            http_uris,
            table,
            to_namespace,
            to_database,
            has_headers,
            delimiter,
            id_field,
            column_names,
            emit_metrics,
            to_opts,
        } => {
            let config = csv::Config {
                files,
                s3_uris,
                http_uris,
                table,
                batch_size: to_opts.batch_size,
                namespace: to_namespace,
                database: to_database,
                surreal_opts: csv::surreal::SurrealOpts {
                    surreal_endpoint: to_opts.surreal_endpoint,
                    surreal_username: to_opts.surreal_username,
                    surreal_password: to_opts.surreal_password,
                },
                has_headers,
                delimiter: delimiter as u8,
                id_field,
                column_names,
                emit_metrics,
                dry_run: to_opts.dry_run,
            };
            csv::sync(config).await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_full_sync(
    from: SourceDatabase,
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    id_field: String,
    conversion_rules: Vec<String>,
    emit_checkpoints: bool,
    checkpoint_dir: String,
) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from {:?} to SurrealDB", from);
    tracing::info!("Target: {}/{}", to_namespace, to_database);

    if to_opts.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let surreal = surreal_connect(&to_opts, &to_namespace, &to_database).await?;

    match from {
        SourceDatabase::MongoDB => {
            let sync_config = if emit_checkpoints {
                Some(SyncConfig {
                    incremental: false,
                    incremental_from: None,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir.clone()),
                })
            } else {
                None
            };
            surreal_sync::mongodb::run_full_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                sync_config,
            )
            .await?;
        }
        SourceDatabase::Neo4j => {
            let sync_config = if emit_checkpoints {
                Some(SyncConfig {
                    incremental: false,
                    incremental_from: None,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir),
                })
            } else {
                None
            };
            surreal_sync::neo4j::run_full_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                sync_config,
            )
            .await?;
        }
        SourceDatabase::PostgreSQL => {
            let sync_config = if emit_checkpoints {
                Some(SyncConfig {
                    incremental: false,
                    incremental_from: None,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir.clone()),
                })
            } else {
                None
            };
            surreal_sync::postgresql::run_full_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                sync_config,
            )
            .await?;
        }
        SourceDatabase::MySQL => {
            let sync_config = if emit_checkpoints {
                Some(SyncConfig {
                    incremental: false,
                    incremental_from: None,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir),
                })
            } else {
                None
            };
            surreal_sync::mysql::run_full_sync(&from_opts, &to_opts, sync_config, &surreal).await?;
        }
        SourceDatabase::Jsonl => {
            // Note: JSONL sync is file-based and does not require checkpoints
            // Convert SourceOpts and SurrealOpts to jsonl crate types
            let jsonl_from_opts = jsonl::SourceOpts {
                source_uri: from_opts.source_uri,
            };
            let jsonl_to_opts = jsonl::SurrealOpts {
                surreal_endpoint: to_opts.surreal_endpoint,
                surreal_username: to_opts.surreal_username,
                surreal_password: to_opts.surreal_password,
                batch_size: to_opts.batch_size,
                dry_run: to_opts.dry_run,
            };
            jsonl::migrate_from_jsonl(
                jsonl_from_opts,
                to_namespace,
                to_database,
                jsonl_to_opts,
                id_field,
                conversion_rules,
            )
            .await?;
        }
    }

    tracing::info!("Full sync completed successfully");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_incremental_sync(
    from: SourceDatabase,
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    incremental_from: String,
    incremental_to: Option<String>,
    timeout: String,
) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from {:?} to SurrealDB", from);
    tracing::info!("Target: {}/{}", to_namespace, to_database);
    tracing::info!("Starting from checkpoint: {}", incremental_from);

    if let Some(ref to) = incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if to_opts.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Parse checkpoints
    let from_checkpoint = SyncCheckpoint::from_string(&incremental_from).with_context(|| {
        format!(
            "Failed to parse checkpoint: '{incremental_from}'\n\n\
                    Valid checkpoint formats:\n\
                    • Neo4j: 'neo4j:2024-01-01T00:00:00Z'\n\
                    • MongoDB: 'mongodb:base64token:2024-01-01T00:00:00Z'\n\
                    • PostgreSQL: 'postgresql:sequence:123'\n\
                    • MySQL: 'mysql:sequence:456'\n\
                    \n\
                    Example: --incremental-from 'mysql:sequence:123'"
        )
    })?;
    let to_checkpoint = incremental_to
        .as_ref()
        .map(|s| {
            SyncCheckpoint::from_string(s).with_context(|| {
                format!(
                    "Failed to parse 'to' checkpoint: '{s}'\n\n\
                        See valid checkpoint formats above."
                )
            })
        })
        .transpose()?;

    // Parse timeout from CLI parameter and compute deadline (defaults to 1 hour)
    let timeout_seconds: i64 = timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {timeout}"))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    match from {
        SourceDatabase::Neo4j => {
            neo4j::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                from_checkpoint,
                deadline,
                to_checkpoint,
            )
            .await?;
        }
        SourceDatabase::MongoDB => {
            mongodb::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                from_checkpoint,
                deadline,
                to_checkpoint,
            )
            .await?;
        }
        SourceDatabase::PostgreSQL => {
            postgresql::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                from_checkpoint,
                deadline,
                to_checkpoint,
            )
            .await?;
        }
        SourceDatabase::MySQL => {
            mysql::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                from_checkpoint,
                deadline,
                to_checkpoint,
            )
            .await?;
        }
        SourceDatabase::Jsonl => {
            return Err(anyhow::anyhow!(
                "Incremental sync for JSONL is not supported.\n\n\
                        JSONL files are static and don't support change tracking.\n\
                        \n\
                        Suggestion: Use the top-level command instead:\n\
                        surreal-sync jsonl --source-uri /path/to/files"
            ));
        }
    }

    tracing::info!("Incremental sync completed successfully");

    Ok(())
}
