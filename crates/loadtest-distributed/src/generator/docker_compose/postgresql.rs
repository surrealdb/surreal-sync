//! PostgreSQL trigger-based full sync Docker Compose service generator.
//!
//! Source crate: crates/postgresql-trigger-source/
//! SourceChoice: Postgresql
//! SourceType: PostgreSQL
//! CLI commands:
//!   - Sync: `from postgresql-trigger full --connection-string ... --to-namespace ... --to-database ...`
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

/// Generate PostgreSQL trigger-based full sync service.
///
/// This generates a single sync container that runs `from postgresql-trigger full` to sync
/// all tables from PostgreSQL to SurrealDB using trigger-based change tracking.
pub fn generate_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command
    let command = format!(
        "from postgresql-trigger full --connection-string 'postgresql://postgres:postgres@postgresql:5432/{}' --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root --schema-file /config/schema.yaml{}",
        config.database.database_name,
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
    add_source_db_dependency(&mut service, "postgresql");

    Value::Mapping(service)
}
