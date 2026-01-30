//! Common helper functions for Docker Compose service generation.
//!
//! These functions are shared across multiple source-specific generators.

use crate::config::{ClusterConfig, SourceType};
use serde_yaml::{Mapping, Value};

/// Normalize memory unit from Kubernetes format (Mi, Gi) to Docker format (m, g).
/// Docker Compose expects lowercase suffixes without 'i' (e.g., "512m" not "512Mi").
pub fn normalize_memory_unit(memory: &str) -> String {
    memory
        .replace("Gi", "g")
        .replace("Mi", "m")
        .replace("Ki", "k")
}

/// Get the Docker service name for the source database.
pub fn database_service_name(source_type: SourceType) -> String {
    match source_type {
        SourceType::MySQL | SourceType::MySQLIncremental => "mysql".to_string(),
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => "postgresql".to_string(),
        SourceType::MongoDB | SourceType::MongoDBIncremental => "mongodb".to_string(),
        SourceType::Neo4j | SourceType::Neo4jIncremental => "neo4j".to_string(),
        SourceType::Kafka => "kafka".to_string(),
        SourceType::Csv | SourceType::Jsonl => "generator".to_string(),
    }
}

/// Convert a snake_case string to PascalCase.
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Create a standard service base with image, labels, and network.
pub fn create_service_base(config: &ClusterConfig) -> Mapping {
    let mut service = Mapping::new();

    service.insert(
        Value::String("image".to_string()),
        Value::String("surreal-sync:latest".to_string()),
    );

    // Labels for cleanup
    let mut labels = Mapping::new();
    labels.insert(
        Value::String("com.surreal-loadtest".to_string()),
        Value::String("true".to_string()),
    );
    service.insert(Value::String("labels".to_string()), Value::Mapping(labels));

    // Environment
    let environment = vec![Value::String("RUST_LOG=info".to_string())];
    service.insert(
        Value::String("environment".to_string()),
        Value::Sequence(environment),
    );

    // Networks
    let networks = vec![Value::String(config.network_name.clone())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    service
}

/// Add standard volumes (config mount).
pub fn add_config_volume(service: &mut Mapping) {
    let volumes = vec![Value::String("./config:/config:ro".to_string())];
    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volumes),
    );
}

/// Add dependency on populate containers completing successfully.
pub fn add_populate_dependencies(service: &mut Mapping, config: &ClusterConfig) {
    let mut depends_on = get_or_create_depends_on(service);

    for container in &config.containers {
        let mut container_dep = Mapping::new();
        container_dep.insert(
            Value::String("condition".to_string()),
            Value::String("service_completed_successfully".to_string()),
        );
        depends_on.insert(
            Value::String(container.id.clone()),
            Value::Mapping(container_dep),
        );
    }

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );
}

/// Add dependency on SurrealDB being healthy.
pub fn add_surrealdb_dependency(service: &mut Mapping) {
    let mut depends_on = get_or_create_depends_on(service);

    let mut surreal_dep = Mapping::new();
    surreal_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(
        Value::String("surrealdb".to_string()),
        Value::Mapping(surreal_dep),
    );

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );
}

/// Add dependency on source database being healthy.
pub fn add_source_db_dependency(service: &mut Mapping, db_name: &str) {
    let mut depends_on = get_or_create_depends_on(service);

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String(db_name.to_string()), Value::Mapping(db_dep));

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );
}

/// Get existing depends_on mapping or create a new one.
fn get_or_create_depends_on(service: &mut Mapping) -> Mapping {
    if let Some(Value::Mapping(existing)) = service.remove(Value::String("depends_on".to_string()))
    {
        existing
    } else {
        Mapping::new()
    }
}

/// Build the dry-run flag suffix.
pub fn dry_run_flag(config: &ClusterConfig) -> &'static str {
    if config.dry_run {
        " --dry-run"
    } else {
        ""
    }
}

/// Get all table names from cluster config containers.
pub fn get_all_tables(config: &ClusterConfig) -> Vec<String> {
    config
        .containers
        .iter()
        .flat_map(|c| c.tables.clone())
        .collect()
}
