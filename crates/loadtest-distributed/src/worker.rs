//! Task execution for distributed load testing.
//!
//! This module implements the populate/verify tasks that run inside containers.

use crate::cli::TaskArgs;
use crate::config::SourceType;
use crate::environment::log_runtime_environment;
use crate::metrics::{ContainerMetrics, Operation, PopulateMetrics};
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info};

/// Run a task (populate or verify) with the given arguments.
pub async fn run_task(args: TaskArgs) -> Result<ContainerMetrics> {
    let started_at = Utc::now();
    let start = Instant::now();

    info!("Starting task: {}", args.container_id);
    info!("Source: {:?}", args.source_type());
    info!("Tables: {:?}", args.tables);
    info!("Row count: {}", args.row_count);
    info!("Seed: {}", args.seed);

    // Log runtime environment
    let env_info = log_runtime_environment();

    // Load schema
    let schema_content = std::fs::read_to_string(&args.schema)
        .with_context(|| format!("Failed to read schema file: {:?}", args.schema))?;
    let schema: sync_core::GeneratorSchema =
        serde_yaml::from_str(&schema_content).with_context(|| "Failed to parse schema YAML")?;

    info!("Schema loaded: {} tables defined", schema.tables.len());

    let operation = if args.verify {
        Operation::Verify
    } else {
        Operation::Populate
    };

    let mut errors = Vec::new();
    let mut metrics = None;
    let verification_report = None;
    let mut success = true;

    // Run the appropriate operation based on source type
    match operation {
        Operation::Populate => match run_populate(&args, &schema).await {
            Ok(m) => {
                metrics = Some(m);
            }
            Err(e) => {
                error!("Populate failed: {:#}", e);
                errors.push(format!("{e:#}"));
                success = false;
            }
        },
        Operation::Verify => {
            // Verification not implemented in this initial version
            // Would need to connect to SurrealDB and verify synced data
            errors.push("Verification not yet implemented".to_string());
            success = false;
        }
    }

    let completed_at = Utc::now();

    let container_metrics = ContainerMetrics {
        container_id: args.container_id.clone(),
        hostname: hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        started_at,
        completed_at,
        environment: env_info,
        tables_processed: args.tables.clone(),
        operation,
        metrics,
        verification_report,
        errors,
        success,
    };

    // Write metrics to output file
    let json = serde_json::to_string_pretty(&container_metrics)?;
    std::fs::write(&args.metrics_output, &json)
        .with_context(|| format!("Failed to write metrics to {:?}", args.metrics_output))?;

    info!(
        "Container {} completed in {:.2}s",
        args.container_id,
        start.elapsed().as_secs_f64()
    );
    info!("Metrics written to {:?}", args.metrics_output);

    Ok(container_metrics)
}

/// Run populate operation.
async fn run_populate(
    args: &TaskArgs,
    schema: &sync_core::GeneratorSchema,
) -> Result<PopulateMetrics> {
    let start = Instant::now();

    let total_rows = match args.source_type() {
        SourceType::MySQL => run_mysql_populate(args, schema).await?,
        // Both PostgreSQL variants use the same populate function (data goes to same database)
        SourceType::PostgreSQL | SourceType::PostgreSQLLogical => {
            run_postgresql_populate(args, schema).await?
        }
        SourceType::MongoDB => run_mongodb_populate(args, schema).await?,
        SourceType::Neo4j => run_neo4j_populate(args, schema).await?,
        SourceType::Csv => run_csv_populate(args, schema)?,
        SourceType::Jsonl => run_jsonl_populate(args, schema)?,
        SourceType::Kafka => run_kafka_populate(args, schema).await?,
    };

    let batch_count = total_rows.div_ceil(args.batch_size);

    let duration_ms = start.elapsed().as_millis() as u64;
    let rows_per_second = if duration_ms > 0 {
        total_rows as f64 / (duration_ms as f64 / 1000.0)
    } else {
        0.0
    };

    Ok(PopulateMetrics {
        rows_processed: total_rows,
        duration_ms,
        batch_count,
        rows_per_second,
        bytes_written: None,
    })
}

async fn run_mysql_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_mysql::MySQLPopulator;

    let mut populator = MySQLPopulator::new(&args.connection_string, schema.clone(), args.seed)
        .await
        .context("Failed to create MySQL populator")?;

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Populating table: {}", table);

        populator
            .create_table(table)
            .await
            .with_context(|| format!("Failed to create table {table}"))?;

        let metrics = populator
            .populate(table, args.row_count)
            .await
            .with_context(|| format!("Failed to populate table {table}"))?;

        total_rows += metrics.rows_inserted;
        info!(
            "Table {} populated: {} rows in {:?}",
            table, metrics.rows_inserted, metrics.total_duration
        );
    }

    Ok(total_rows)
}

async fn run_postgresql_populate(
    args: &TaskArgs,
    schema: &sync_core::GeneratorSchema,
) -> Result<u64> {
    use loadtest_populate_postgresql::PostgreSQLPopulator;

    let mut populator =
        PostgreSQLPopulator::new(&args.connection_string, schema.clone(), args.seed)
            .await
            .context("Failed to create PostgreSQL populator")?;

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Populating table: {}", table);

        populator
            .create_table(table)
            .await
            .with_context(|| format!("Failed to create table {table}"))?;

        let metrics = populator
            .populate(table, args.row_count)
            .await
            .with_context(|| format!("Failed to populate table {table}"))?;

        total_rows += metrics.rows_inserted;
        info!(
            "Table {} populated: {} rows in {:?}",
            table, metrics.rows_inserted, metrics.total_duration
        );
    }

    Ok(total_rows)
}

async fn run_mongodb_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_mongodb::MongoDBPopulator;

    // Parse database from connection string or use default
    let database =
        extract_mongodb_database(&args.connection_string).unwrap_or_else(|| "loadtest".to_string());

    let mut populator = MongoDBPopulator::new(
        &args.connection_string,
        &database,
        schema.clone(),
        args.seed,
    )
    .await
    .context("Failed to create MongoDB populator")?;

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Populating collection: {}", table);

        let metrics = populator
            .populate(table, args.row_count)
            .await
            .with_context(|| format!("Failed to populate collection {table}"))?;

        total_rows += metrics.rows_inserted;
        info!(
            "Collection {} populated: {} documents in {:?}",
            table, metrics.rows_inserted, metrics.total_duration
        );
    }

    Ok(total_rows)
}

async fn run_neo4j_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_neo4j::Neo4jPopulator;

    let (uri, user, password) = parse_neo4j_connection(&args.connection_string)?;

    let mut populator = Neo4jPopulator::new(
        &uri,
        &user,
        &password,
        "neo4j", // database name
        schema.clone(),
        args.seed,
    )
    .await
    .context("Failed to create Neo4j populator")?;

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Populating nodes: {}", table);

        let metrics = populator
            .populate(table, args.row_count)
            .await
            .with_context(|| format!("Failed to populate nodes {table}"))?;

        total_rows += metrics.rows_inserted;
        info!(
            "Nodes {} populated: {} nodes in {:?}",
            table, metrics.rows_inserted, metrics.total_duration
        );
    }

    Ok(total_rows)
}

fn run_csv_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_csv::CSVPopulator;

    // For CSV, connection_string is the output directory
    let output_dir = PathBuf::from(&args.connection_string);
    std::fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            args.connection_string
        )
    })?;

    let mut populator = CSVPopulator::new(schema.clone(), args.seed);

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Generating CSV: {}", table);

        let output_path = output_dir.join(format!("{table}.csv"));
        let metrics = populator
            .populate(table, &output_path, args.row_count)
            .with_context(|| format!("Failed to generate CSV for {table}"))?;

        total_rows += metrics.rows_written;
        info!(
            "CSV {} generated: {} rows in {:?}",
            table, metrics.rows_written, metrics.total_duration
        );
    }

    Ok(total_rows)
}

fn run_jsonl_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_jsonl::JsonlPopulator;

    // For JSONL, connection_string is the output directory
    let output_dir = PathBuf::from(&args.connection_string);
    std::fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            args.connection_string
        )
    })?;

    let mut populator = JsonlPopulator::new(schema.clone(), args.seed);

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Generating JSONL: {}", table);

        let output_path = output_dir.join(format!("{table}.jsonl"));
        let metrics = populator
            .populate(table, &output_path, args.row_count)
            .with_context(|| format!("Failed to generate JSONL for {table}"))?;

        total_rows += metrics.rows_written;
        info!(
            "JSONL {} generated: {} rows in {:?}",
            table, metrics.rows_written, metrics.total_duration
        );
    }

    Ok(total_rows)
}

async fn run_kafka_populate(args: &TaskArgs, schema: &sync_core::GeneratorSchema) -> Result<u64> {
    use loadtest_populate_kafka::KafkaPopulator;

    let mut populator = KafkaPopulator::new(&args.connection_string, schema.clone(), args.seed)
        .await
        .context("Failed to create Kafka populator")?;

    let mut total_rows = 0u64;
    for table in &args.tables {
        info!("Publishing to topic: {}", table);

        let metrics = populator
            .populate(table, args.row_count)
            .await
            .with_context(|| format!("Failed to publish to topic {table}"))?;

        total_rows += metrics.messages_published;
        info!(
            "Topic {} populated: {} messages in {:?}",
            table, metrics.messages_published, metrics.total_duration
        );
    }

    Ok(total_rows)
}

/// Extract database name from MongoDB connection string.
///
/// Supports both `mongodb://` and `mongodb+srv://` schemes.
/// Returns `None` if the connection string is empty, has invalid scheme,
/// or does not contain a database name.
fn extract_mongodb_database(conn_string: &str) -> Option<String> {
    if conn_string.is_empty() {
        tracing::warn!("MongoDB connection string is empty");
        return None;
    }

    // Support both mongodb:// and mongodb+srv://
    let after_scheme = conn_string
        .strip_prefix("mongodb://")
        .or_else(|| conn_string.strip_prefix("mongodb+srv://"));

    let after_scheme = match after_scheme {
        Some(s) => s,
        None => {
            tracing::warn!(
                "MongoDB connection string has invalid scheme (expected mongodb:// or mongodb+srv://): {}",
                conn_string
            );
            return None;
        }
    };

    // Find the path component (after host:port or host)
    // Format: user:pass@host:port/database?options or host:port/database?options
    if let Some(path_start) = after_scheme.find('/') {
        let path = &after_scheme[path_start + 1..];
        // Split off query parameters
        let db_name = path.split('?').next().unwrap_or(path);
        if !db_name.is_empty() {
            return Some(db_name.to_string());
        }
    }

    tracing::warn!(
        "MongoDB connection string missing database name: {}",
        conn_string
    );
    None
}

/// Parse Neo4j connection string into (uri, username, password).
///
/// Supports formats:
/// - `bolt://host:port` - uses default credentials (neo4j/password)
/// - `bolt://user:password@host:port` - extracts credentials
/// - `neo4j://host:port` - uses default credentials
/// - `neo4j://user:password@host:port` - extracts credentials
///
/// Returns an error if the connection string is empty or has an invalid scheme.
fn parse_neo4j_connection(conn_string: &str) -> Result<(String, String, String)> {
    if conn_string.is_empty() {
        return Err(anyhow::anyhow!("Neo4j connection string cannot be empty"));
    }

    // Support both bolt:// and neo4j:// schemes
    let (scheme, without_scheme) = if let Some(rest) = conn_string.strip_prefix("bolt://") {
        ("bolt://", rest)
    } else if let Some(rest) = conn_string.strip_prefix("neo4j://") {
        ("neo4j://", rest)
    } else {
        return Err(anyhow::anyhow!(
            "Neo4j connection string has invalid scheme (expected bolt:// or neo4j://): {conn_string}"
        ));
    };

    // Check if credentials are embedded in the URL
    if let Some(at_pos) = without_scheme.find('@') {
        let creds_part = &without_scheme[..at_pos];
        let host_part = &without_scheme[at_pos + 1..];

        if host_part.is_empty() {
            return Err(anyhow::anyhow!(
                "Neo4j connection string missing host after '@': {conn_string}"
            ));
        }

        // Parse credentials (user:password or just user)
        let (user, password) = if let Some(colon_pos) = creds_part.find(':') {
            let user = &creds_part[..colon_pos];
            let pass = &creds_part[colon_pos + 1..];
            if user.is_empty() {
                return Err(anyhow::anyhow!(
                    "Neo4j connection string has empty username: {conn_string}"
                ));
            }
            (user.to_string(), pass.to_string())
        } else {
            // No colon means just username, use default password
            if creds_part.is_empty() {
                return Err(anyhow::anyhow!(
                    "Neo4j connection string has empty credentials: {conn_string}"
                ));
            }
            (creds_part.to_string(), "password".to_string())
        };

        return Ok((format!("{scheme}{host_part}"), user, password));
    }

    // No credentials in URL, use defaults
    Ok((
        conn_string.to_string(),
        "neo4j".to_string(),
        "password".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_mongodb_database_valid() {
        assert_eq!(
            extract_mongodb_database("mongodb://localhost:27017/mydb"),
            Some("mydb".to_string())
        );
        assert_eq!(
            extract_mongodb_database("mongodb://localhost:27017/mydb?authSource=admin"),
            Some("mydb".to_string())
        );
        // mongodb+srv:// scheme
        assert_eq!(
            extract_mongodb_database("mongodb+srv://cluster.example.com/mydb"),
            Some("mydb".to_string())
        );
        // With credentials
        assert_eq!(
            extract_mongodb_database("mongodb://user:pass@localhost:27017/mydb"),
            Some("mydb".to_string())
        );
    }

    #[test]
    fn test_extract_mongodb_database_missing() {
        assert_eq!(extract_mongodb_database("mongodb://localhost:27017"), None);
        assert_eq!(extract_mongodb_database("mongodb://localhost:27017/"), None);
        // Empty string
        assert_eq!(extract_mongodb_database(""), None);
        // Invalid scheme
        assert_eq!(extract_mongodb_database("mysql://localhost/db"), None);
    }

    #[test]
    fn test_parse_neo4j_connection_valid() {
        // Simple URL without credentials
        let (uri, user, pass) = parse_neo4j_connection("bolt://localhost:7687").unwrap();
        assert_eq!(uri, "bolt://localhost:7687");
        assert_eq!(user, "neo4j");
        assert_eq!(pass, "password");

        // URL with credentials
        let (uri, user, pass) =
            parse_neo4j_connection("bolt://admin:secret@localhost:7687").unwrap();
        assert_eq!(uri, "bolt://localhost:7687");
        assert_eq!(user, "admin");
        assert_eq!(pass, "secret");

        // neo4j:// scheme
        let (uri, user, pass) = parse_neo4j_connection("neo4j://localhost:7687").unwrap();
        assert_eq!(uri, "neo4j://localhost:7687");
        assert_eq!(user, "neo4j");
        assert_eq!(pass, "password");

        // neo4j:// with credentials
        let (uri, user, pass) =
            parse_neo4j_connection("neo4j://admin:secret@localhost:7687").unwrap();
        assert_eq!(uri, "neo4j://localhost:7687");
        assert_eq!(user, "admin");
        assert_eq!(pass, "secret");

        // User without password (uses default password)
        let (uri, user, pass) = parse_neo4j_connection("bolt://admin@localhost:7687").unwrap();
        assert_eq!(uri, "bolt://localhost:7687");
        assert_eq!(user, "admin");
        assert_eq!(pass, "password");
    }

    #[test]
    fn test_parse_neo4j_connection_errors() {
        // Empty string
        assert!(parse_neo4j_connection("").is_err());

        // Invalid scheme
        assert!(parse_neo4j_connection("http://localhost:7687").is_err());

        // Empty username (empty string before colon)
        assert!(parse_neo4j_connection("bolt://:password@localhost:7687").is_err());

        // Empty credentials (just @)
        assert!(parse_neo4j_connection("bolt://@localhost:7687").is_err());

        // Missing host after @
        assert!(parse_neo4j_connection("bolt://user:pass@").is_err());
    }
}
