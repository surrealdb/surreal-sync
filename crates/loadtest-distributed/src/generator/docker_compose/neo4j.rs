//! Neo4j full sync Docker Compose service generator.
//!
//! Source crate: crates/neo4j-source/
//! SourceChoice: Neo4j
//! SourceType: Neo4j
//! CLI commands:
//!   - Sync: `from neo4j full --connection-string ... --username ... --password ... --to-namespace ... --to-database ...`
//!
//! ## Pipeline
//!
//! 3-stage (Full Sync): Populate -> Sync -> Verify

use super::common::{
    add_config_volume, add_populate_dependencies, add_source_db_dependency,
    add_surrealdb_dependency, create_service_base, dry_run_flag,
};
use crate::config::ClusterConfig;
use serde_yaml::Value;

/// Generate Neo4j full sync service.
///
/// This generates a single sync container that runs `from neo4j full` to sync
/// all nodes from Neo4j to SurrealDB.
pub fn generate_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command
    let command = format!(
        "from neo4j full --connection-string 'bolt://neo4j:7687' --username neo4j --password password --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root --schema-file /config/schema.yaml{}",
        config.surrealdb.namespace,
        config.surrealdb.database,
        dry_run_flag(config)
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes
    add_config_volume(&mut service);

    // Dependencies
    add_populate_dependencies(&mut service, config);
    add_surrealdb_dependency(&mut service);
    add_source_db_dependency(&mut service, "neo4j");

    Value::Mapping(service)
}
