//! CSV and JSONL file import Docker Compose service generators.
//!
//! Source crates:
//! - crates/csv-source/
//! - crates/jsonl-source/
//!
//! SourceChoice: Csv, Jsonl
//!
//! SourceType: Csv, Jsonl
//!
//! CLI commands:
//! - CSV: `from csv --files ... --table ... --has-headers --id-field ... --to-namespace ... --to-database ...`
//! - JSONL: `from jsonl --path ... --to-namespace ... --to-database ...`
//!
//! ## Pipeline
//!
//! Per-file parallel: For each file/table, Populate -> Sync -> Verify runs independently.
//! CSV/JSONL need one sync service per file since the CLI handles one table at a time.

use super::common::{create_service_base, dry_run_flag};
use crate::config::{ClusterConfig, SourceType};
use serde_yaml::{Mapping, Value};

/// Generate file sync service configuration for a single table (CSV/JSONL).
///
/// CSV/JSONL need one sync service per file since the CLI handles one table at a time.
pub fn generate_sync_service(
    config: &ClusterConfig,
    table_name: &str,
    _row_count: u64,
    container_idx: usize,
) -> Value {
    let mut service = create_service_base(config);

    // CSV/JSONL sync command - reads from file, imports to SurrealDB
    // CSV and JSONL have different CLI syntax
    let command = match config.source_type {
        SourceType::Csv => {
            // CSV: --files, --table, --has-headers, --id-field
            format!(
                "from csv \
                --files '/data/csv/{table_name}.csv' \
                --table '{table_name}' \
                --has-headers \
                --id-field id \
                --to-namespace {} \
                --to-database {} \
                --surreal-endpoint 'http://surrealdb:8000' \
                --surreal-username root \
                --surreal-password root \
                --schema-file /config/schema.yaml{}",
                config.surrealdb.namespace,
                config.surrealdb.database,
                dry_run_flag(config)
            )
        }
        SourceType::Jsonl => {
            // JSONL: --path (directory or file), --id-field (defaults to id)
            // JSONL infers table name from filename
            format!(
                "from jsonl \
                --path '/data/jsonl/{table_name}.jsonl' \
                --to-namespace {} \
                --to-database {} \
                --surreal-endpoint 'http://surrealdb:8000' \
                --surreal-username root \
                --surreal-password root \
                --schema-file /config/schema.yaml{}",
                config.surrealdb.namespace,
                config.surrealdb.database,
                dry_run_flag(config)
            )
        }
        _ => unreachable!("generate_file_sync_service only called for CSV/JSONL"),
    };
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes - mount config and shared data volume
    let volumes = vec![
        Value::String("./config:/config:ro".to_string()),
        Value::String("loadtest-data:/data".to_string()),
    ];
    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volumes),
    );

    // Dependencies - wait for populate container to complete
    let mut depends_on = Mapping::new();

    // Wait for the populate container that handles this table
    let mut populate_dep = Mapping::new();
    populate_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_completed_successfully".to_string()),
    );
    depends_on.insert(
        Value::String(format!("populate-{container_idx}")),
        Value::Mapping(populate_dep),
    );

    // Wait for SurrealDB to be healthy
    let mut surreal_dep = Mapping::new();
    surreal_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(
        Value::String("surrealdb".to_string()),
        Value::Mapping(surreal_dep),
    );

    // Wait for file-generator to be healthy (ensures shared volume is ready)
    let mut file_gen_dep = Mapping::new();
    file_gen_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(
        Value::String("file-generator".to_string()),
        Value::Mapping(file_gen_dep),
    );

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}
