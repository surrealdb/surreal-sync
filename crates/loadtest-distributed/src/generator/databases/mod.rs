//! Database-specific Docker and Kubernetes configurations.

mod csv_jsonl;
mod kafka;
mod mongodb;
mod mysql;
mod neo4j;
mod postgresql;
mod postgresql_logical;

use crate::config::{DatabaseConfig, SourceType};
use serde_yaml::{Mapping, Value};

pub use csv_jsonl::*;
pub use kafka::*;
pub use mongodb::*;
pub use mysql::*;
pub use neo4j::*;
pub use postgresql::*;
pub use postgresql_logical::*;

/// Normalize memory unit from Kubernetes format (Mi, Gi) to Docker format (m, g).
fn normalize_memory_unit(memory: &str) -> String {
    memory
        .replace("Gi", "g")
        .replace("Mi", "m")
        .replace("Ki", "k")
}

/// Generate Docker service configuration for a database.
pub fn generate_docker_service(config: &DatabaseConfig) -> Value {
    match config.source_type {
        SourceType::MySQL => generate_mysql_docker_service(config),
        SourceType::PostgreSQL => generate_postgresql_docker_service(config),
        SourceType::PostgreSQLWal2JsonIncremental => {
            generate_postgresql_logical_docker_service(config)
        }
        SourceType::MongoDB => generate_mongodb_docker_service(config),
        SourceType::Neo4j => generate_neo4j_docker_service(config),
        SourceType::Kafka => generate_kafka_docker_service(config),
        SourceType::Csv | SourceType::Jsonl => generate_file_generator_docker_service(config),
    }
}

/// Generate MongoDB init service for replica set initialization.
pub fn generate_mongodb_init_service() -> Value {
    mongodb::generate_mongodb_init_service_internal()
}

/// Generate Kubernetes StatefulSet for a database.
pub fn generate_k8s_statefulset(config: &DatabaseConfig, namespace: &str) -> String {
    match config.source_type {
        SourceType::MySQL => generate_mysql_k8s_statefulset(config, namespace),
        SourceType::PostgreSQL => generate_postgresql_k8s_statefulset(config, namespace),
        SourceType::PostgreSQLWal2JsonIncremental => {
            generate_postgresql_logical_k8s_statefulset(config, namespace)
        }
        SourceType::MongoDB => generate_mongodb_k8s_statefulset(config, namespace),
        SourceType::Neo4j => generate_neo4j_k8s_statefulset(config, namespace),
        SourceType::Kafka => generate_kafka_k8s_statefulset(config, namespace),
        SourceType::Csv | SourceType::Jsonl => generate_file_generator_k8s_job(config, namespace),
    }
}

/// Generate Kubernetes Service for a database.
pub fn generate_k8s_service(config: &DatabaseConfig, namespace: &str) -> String {
    match config.source_type {
        SourceType::MySQL => generate_mysql_k8s_service(namespace),
        SourceType::PostgreSQL => generate_postgresql_k8s_service(namespace),
        SourceType::PostgreSQLWal2JsonIncremental => {
            generate_postgresql_logical_k8s_service(namespace)
        }
        SourceType::MongoDB => generate_mongodb_k8s_service(namespace),
        SourceType::Neo4j => generate_neo4j_k8s_service(namespace),
        SourceType::Kafka => generate_kafka_k8s_service(namespace),
        SourceType::Csv | SourceType::Jsonl => String::new(), // No service needed
    }
}

/// Helper to create base Docker service with common fields.
pub(crate) fn create_base_docker_service(
    image: &str,
    resources: &crate::config::ResourceLimits,
    network_name: &str,
) -> Mapping {
    let mut service = Mapping::new();

    service.insert(
        Value::String("image".to_string()),
        Value::String(image.to_string()),
    );

    // Resources
    let mut deploy = Mapping::new();
    let mut resources_map = Mapping::new();
    let mut limits = Mapping::new();
    limits.insert(
        Value::String("cpus".to_string()),
        Value::String(resources.cpu_limit.clone()),
    );
    limits.insert(
        Value::String("memory".to_string()),
        Value::String(normalize_memory_unit(&resources.memory_limit)),
    );
    resources_map.insert(Value::String("limits".to_string()), Value::Mapping(limits));
    deploy.insert(
        Value::String("resources".to_string()),
        Value::Mapping(resources_map),
    );
    service.insert(Value::String("deploy".to_string()), Value::Mapping(deploy));

    // Networks
    let networks = vec![Value::String(network_name.to_string())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    service
}

/// Helper to add healthcheck to a Docker service.
pub(crate) fn add_healthcheck(
    service: &mut Mapping,
    test: Vec<&str>,
    interval: &str,
    timeout: &str,
    retries: u32,
) {
    let mut healthcheck = Mapping::new();
    healthcheck.insert(
        Value::String("test".to_string()),
        Value::Sequence(
            test.into_iter()
                .map(|s| Value::String(s.to_string()))
                .collect(),
        ),
    );
    healthcheck.insert(
        Value::String("interval".to_string()),
        Value::String(interval.to_string()),
    );
    healthcheck.insert(
        Value::String("timeout".to_string()),
        Value::String(timeout.to_string()),
    );
    healthcheck.insert(
        Value::String("retries".to_string()),
        Value::Number(retries.into()),
    );
    service.insert(
        Value::String("healthcheck".to_string()),
        Value::Mapping(healthcheck),
    );
}

/// Helper to add environment variables to a Docker service.
pub(crate) fn add_environment(service: &mut Mapping, env_vars: Vec<(&str, &str)>) {
    let environment: Vec<Value> = env_vars
        .into_iter()
        .map(|(k, v)| Value::String(format!("{k}={v}")))
        .collect();
    service.insert(
        Value::String("environment".to_string()),
        Value::Sequence(environment),
    );
}

/// Helper to add tmpfs mount to a Docker service.
pub(crate) fn add_tmpfs(service: &mut Mapping, path: &str, size: &str) {
    let normalized_size = normalize_memory_unit(size);
    let tmpfs = vec![Value::String(format!("{path}:size={normalized_size}"))];
    service.insert(Value::String("tmpfs".to_string()), Value::Sequence(tmpfs));
}

/// Helper to add volume mount to a Docker service.
pub(crate) fn add_volume(service: &mut Mapping, volumes: Vec<String>) {
    let volume_values: Vec<Value> = volumes.into_iter().map(Value::String).collect();
    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volume_values),
    );
}
