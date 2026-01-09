//! Command-line interface for surreal-sync
//!
//! # Usage Examples
//!
//! ## Full Sync
//! ```bash
//! # MongoDB full sync
//! surreal-sync full mongodb \
//!   --source-uri mongodb://localhost:27017 \
//!   --source-database mydb \
//!   --to-namespace test --to-database test
//!
//! # Neo4j full sync with checkpoints
//! surreal-sync full neo4j \
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
use clap::{Parser, Subcommand, ValueEnum};
use surreal_sync::{csv, jsonl, kafka, mysql, postgresql, SourceOpts, SurrealOpts};
use surreal_sync_surreal::surreal_connect;

// Database-specific sync crates (fully-qualified paths used in match arms)
#[allow(clippy::single_component_path_imports)]
use surreal_sync_mongodb;
#[allow(clippy::single_component_path_imports)]
use surreal_sync_neo4j;

// Load testing imports
use loadtest_populate_csv::CSVPopulateArgs;
use loadtest_populate_jsonl::JSONLPopulateArgs;
use loadtest_populate_kafka::KafkaPopulateArgs;
use loadtest_populate_mongodb::MongoDBPopulateArgs;
use loadtest_populate_mysql::MySQLPopulateArgs;
use loadtest_populate_postgresql::PostgreSQLPopulateArgs;
use loadtest_verify::VerifyArgs;
use sync_core::Schema;

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

        /// Schema file for type-aware conversion (enables proper type handling)
        #[arg(long, value_name = "PATH")]
        schema_file: Option<std::path::PathBuf>,
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

        /// Schema file for type-aware conversion (enables proper type handling)
        #[arg(long, value_name = "PATH")]
        schema_file: Option<std::path::PathBuf>,
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

        /// Schema file for type-aware conversion (enables JSON field parsing and empty array handling)
        #[arg(long, value_name = "PATH")]
        schema_file: Option<std::path::PathBuf>,
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

        /// Schema file for type-aware conversion (enables proper type handling)
        #[arg(long, value_name = "PATH")]
        schema_file: Option<std::path::PathBuf>,
    },

    /// Import CSV files to SurrealDB
    #[group(id = "source", required = true, multiple = false, args = ["files", "s3_uris", "http_uris"])]
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

        /// Schema file for type-aware conversion (enables proper type parsing from strings)
        #[arg(long, value_name = "PATH")]
        schema_file: Option<std::path::PathBuf>,
    },

    /// Load testing utilities for populating and verifying test data
    Loadtest {
        #[command(subcommand)]
        command: LoadtestCommand,
    },
}

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
            schema_file,
        } => {
            // Load sync schema if provided (for future use in type-aware conversion)
            let _schema =
                if let Some(schema_path) = schema_file {
                    Some(Schema::from_file(&schema_path).with_context(|| {
                        format!("Failed to load sync schema from {schema_path:?}")
                    })?)
                } else {
                    None
                };

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
            schema_file,
        } => {
            // Load sync schema if provided (for future use in type-aware conversion)
            let _schema =
                if let Some(schema_path) = schema_file {
                    Some(Schema::from_file(&schema_path).with_context(|| {
                        format!("Failed to load sync schema from {schema_path:?}")
                    })?)
                } else {
                    None
                };

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
            schema_file,
        } => {
            // Load schema if provided for type-aware conversion
            let table_schema = if let Some(schema_path) = schema_file {
                let schema = Schema::from_file(&schema_path)
                    .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
                // Get the table schema using the configured table name or topic name
                let table_name = config.table_name.as_ref().unwrap_or(&config.topic);
                // Convert GeneratorTableDefinition to base TableDefinition for sync operations
                schema
                    .get_table(table_name)
                    .map(|t| t.to_table_definition())
            } else {
                None
            };

            kafka::run_incremental_sync(
                config,
                to_namespace,
                to_database,
                to_opts,
                chrono::Utc::now() + chrono::Duration::hours(1),
                table_schema,
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
            schema_file,
        } => {
            // Load sync schema if provided (for future use in type-aware conversion)
            let _schema = if let Some(schema_path) = schema_file {
                Some(
                    Schema::from_file(&schema_path)
                        .with_context(|| format!("Failed to load schema from {schema_path:?}"))?,
                )
            } else {
                None
            };

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
            schema_file,
        } => {
            // Load sync schema if provided for type-aware conversion
            let schema =
                if let Some(schema_path) = schema_file {
                    Some(Schema::from_file(&schema_path).with_context(|| {
                        format!("Failed to load sync schema from {schema_path:?}")
                    })?)
                } else {
                    None
                };

            let config = csv::Config {
                sources: vec![],
                files,
                s3_uris,
                http_uris,
                table,
                batch_size: to_opts.batch_size,
                namespace: to_namespace,
                database: to_database,
                surreal_opts: surreal_sync_surreal::SurrealOpts {
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
                schema,
            };
            csv::sync(config).await?;
        }
        Commands::Loadtest { command } => match command {
            LoadtestCommand::Populate { source } => {
                run_populate(source).await?;
            }
            LoadtestCommand::Verify { args } => {
                run_verify(args).await?;
            }
        },
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

    let surreal_conn_opts = surreal_sync_surreal::SurrealOpts {
        surreal_endpoint: to_opts.surreal_endpoint.clone(),
        surreal_username: to_opts.surreal_username.clone(),
        surreal_password: to_opts.surreal_password.clone(),
    };
    let surreal = surreal_connect(&surreal_conn_opts, &to_namespace, &to_database).await?;

    match from {
        SourceDatabase::MongoDB => {
            let sync_config = if emit_checkpoints {
                Some(checkpoint::SyncConfig {
                    incremental: false,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir.clone()),
                })
            } else {
                None
            };
            surreal_sync_mongodb::run_full_sync(
                surreal_sync_mongodb::SourceOpts::from(&from_opts),
                to_namespace,
                to_database,
                surreal_sync_mongodb::SurrealOpts::from(&to_opts),
                sync_config,
            )
            .await?;
        }
        SourceDatabase::Neo4j => {
            let sync_config = if emit_checkpoints {
                Some(checkpoint::SyncConfig {
                    incremental: false,
                    emit_checkpoints: true,
                    checkpoint_dir: Some(checkpoint_dir),
                })
            } else {
                None
            };
            surreal_sync_neo4j::run_full_sync(
                surreal_sync_neo4j::SourceOpts::from(&from_opts),
                to_namespace,
                to_database,
                surreal_sync_neo4j::SurrealOpts::from(&to_opts),
                sync_config,
            )
            .await?;
        }
        SourceDatabase::PostgreSQL => {
            let sync_config = if emit_checkpoints {
                Some(checkpoint::SyncConfig {
                    incremental: false,
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
                Some(checkpoint::SyncConfig {
                    incremental: false,
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
            let jsonl_to_opts = surreal_sync_surreal::SurrealOpts {
                surreal_endpoint: to_opts.surreal_endpoint,
                surreal_username: to_opts.surreal_username,
                surreal_password: to_opts.surreal_password,
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

    // Parse timeout from CLI parameter and compute deadline (defaults to 1 hour)
    let timeout_seconds: i64 = timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {timeout}"))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    match from {
        SourceDatabase::Neo4j => {
            // Parse checkpoint strings using database-specific types
            let neo4j_from =
                surreal_sync_neo4j::Neo4jCheckpoint::from_cli_string(&incremental_from)?;
            let neo4j_to = incremental_to
                .as_ref()
                .map(|s| surreal_sync_neo4j::Neo4jCheckpoint::from_cli_string(s))
                .transpose()?;
            surreal_sync_neo4j::run_incremental_sync(
                surreal_sync_neo4j::SourceOpts::from(&from_opts),
                to_namespace,
                to_database,
                surreal_sync_neo4j::SurrealOpts::from(&to_opts),
                neo4j_from,
                deadline,
                neo4j_to,
            )
            .await?;
        }
        SourceDatabase::MongoDB => {
            // Parse checkpoint strings using database-specific types
            let mongodb_from =
                surreal_sync_mongodb::MongoDBCheckpoint::from_cli_string(&incremental_from)?;
            let mongodb_to = incremental_to
                .as_ref()
                .map(|s| surreal_sync_mongodb::MongoDBCheckpoint::from_cli_string(s))
                .transpose()?;
            surreal_sync_mongodb::run_incremental_sync(
                surreal_sync_mongodb::SourceOpts::from(&from_opts),
                to_namespace,
                to_database,
                surreal_sync_mongodb::SurrealOpts::from(&to_opts),
                mongodb_from,
                deadline,
                mongodb_to,
            )
            .await?;
        }
        SourceDatabase::PostgreSQL => {
            // Parse checkpoint strings using database-specific types
            let pg_from =
                postgresql::checkpoint::PostgreSQLCheckpoint::from_cli_string(&incremental_from)?;
            let pg_to = incremental_to
                .as_ref()
                .map(|s| postgresql::checkpoint::PostgreSQLCheckpoint::from_cli_string(s))
                .transpose()?;
            postgresql::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                pg_from,
                deadline,
                pg_to,
            )
            .await?;
        }
        SourceDatabase::MySQL => {
            // Parse checkpoint strings using database-specific types
            let mysql_from =
                mysql::checkpoint::MySQLCheckpoint::from_cli_string(&incremental_from)?;
            let mysql_to = incremental_to
                .as_ref()
                .map(|s| mysql::checkpoint::MySQLCheckpoint::from_cli_string(s))
                .transpose()?;
            mysql::run_incremental_sync(
                from_opts,
                to_namespace,
                to_database,
                to_opts,
                mysql_from,
                deadline,
                mysql_to,
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

/// Run populate command to fill source database with deterministic test data
async fn run_populate(source: PopulateSource) -> anyhow::Result<()> {
    match source {
        PopulateSource::MySQL { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

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

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

            tracing::info!(
                "Populating Kafka topics with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

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

    tracing::info!(
        "Verifying {} rows per table in SurrealDB (seed={})",
        args.row_count,
        args.seed
    );

    let surreal = surrealdb::engine::any::connect(&args.surreal_endpoint)
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

    let tables = if args.tables.is_empty() {
        schema.table_names()
    } else {
        args.tables.iter().map(|s| s.as_str()).collect()
    };

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
