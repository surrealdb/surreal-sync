//! MySQL full sync Docker Compose service generator.
//!
//! Source crate: crates/mysql-trigger-source/
//! SourceChoice: Mysql
//! SourceType: MySQL
//! CLI commands:
//!   - Sync: `from mysql full --connection-string ... --to-namespace ... --to-database ...`
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

/// Generate MySQL full sync service.
///
/// This generates a single sync container that runs `from mysql full` to sync
/// all tables from MySQL to SurrealDB.
pub fn generate_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command
    let command = format!(
        "from mysql full --connection-string 'mysql://root:root@mysql:3306/{}' --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root --schema-file /config/schema.yaml{}",
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
    add_source_db_dependency(&mut service, "mysql");

    Value::Mapping(service)
}
