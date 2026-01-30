//! Kafka streaming sync Docker Compose service generator.
//!
//! Source crate: crates/kafka-source/
//! SourceChoice: Kafka
//! SourceType: Kafka
//! CLI commands:
//!   - Sync: `from kafka --proto-path ... --brokers ... --topic ... --message-type ... --to-namespace ... --to-database ...`
//!
//! ## Pipeline
//!
//! Per-topic parallel: For each topic, Populate -> Sync -> Verify runs independently.
//! Kafka needs one sync service per topic since the CLI handles one topic at a time.

use super::common::{create_service_base, dry_run_flag, to_pascal_case};
use crate::config::ClusterConfig;
use serde_yaml::{Mapping, Value};

/// Generate Kafka sync service configuration for a single topic.
///
/// Kafka needs one sync service per topic because the CLI handles one topic at a time.
/// Each sync service reads from a shared proto volume and consumes from its assigned topic.
/// The `row_count` parameter enables early exit when all expected messages are consumed.
pub fn generate_sync_service(
    config: &ClusterConfig,
    table_name: &str,
    row_count: u64,
    container_idx: usize,
) -> Value {
    let mut service = create_service_base(config);

    // Kafka sync command - reads from topic, uses proto file for schema
    // Message type is pascal case of table name (e.g., "users" -> "Users")
    // Timeout is set to 1 minute (60s) as a safety limit
    // --max-messages enables early exit once all expected messages are consumed
    let message_type = to_pascal_case(table_name);
    let command = format!(
        "from kafka \
        --proto-path '/proto/{table_name}.proto' \
        --brokers 'kafka:9092' \
        --group-id 'loadtest-sync-{table_name}' \
        --topic '{table_name}' \
        --message-type '{message_type}' \
        --buffer-size 1000 \
        --session-timeout-ms 30000 \
        --kafka-batch-size 100 \
        --max-messages {row_count} \
        --timeout '1m' \
        --to-namespace {} \
        --to-database {} \
        --surreal-endpoint 'http://surrealdb:8000' \
        --surreal-username root \
        --surreal-password root \
        --schema-file /config/schema.yaml{}",
        config.surrealdb.namespace,
        config.surrealdb.database,
        dry_run_flag(config)
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes - mount config and proto files (proto files generated at config time)
    let volumes = vec![
        Value::String("./config:/config:ro".to_string()),
        Value::String("./config/proto:/proto:ro".to_string()),
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

    // Wait for Kafka to be healthy
    let mut kafka_dep = Mapping::new();
    kafka_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(
        Value::String("kafka".to_string()),
        Value::Mapping(kafka_dep),
    );

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}
