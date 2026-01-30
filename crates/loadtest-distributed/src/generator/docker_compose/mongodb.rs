//! MongoDB full sync Docker Compose service generator.
//!
//! Source crate: crates/mongodb-changestream-source/
//! SourceChoice: Mongodb
//! SourceType: MongoDB
//! CLI commands:
//!   - Sync: `from mongodb full --connection-string ... --database ... --to-namespace ... --to-database ...`
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

/// Generate MongoDB full sync service.
///
/// This generates a single sync container that runs `from mongodb full` to sync
/// all collections from MongoDB to SurrealDB.
pub fn generate_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command
    let command = format!(
        "from mongodb full --connection-string 'mongodb://root:root@mongodb:27017' --database {} --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root --schema-file /config/schema.yaml{}",
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
    add_source_db_dependency(&mut service, "mongodb");

    Value::Mapping(service)
}
