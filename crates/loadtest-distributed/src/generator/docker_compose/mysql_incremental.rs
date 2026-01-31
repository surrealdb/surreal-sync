//! MySQL trigger-based incremental sync Docker Compose service generators.
//!
//! Source crate: crates/mysql-trigger-source/
//! SourceChoice: MysqlTriggerIncremental
//! SourceType: MySQLTriggerIncremental
//! CLI commands:
//!   - Schema init: `loadtest populate mysql --schema-only ...`
//!   - Full sync setup: `from mysql full --checkpoints-surreal-table ...`
//!   - Incremental sync: `from mysql incremental --checkpoints-surreal-table ...`
//!
//! ## Pipeline
//!
//! 5-stage (Incremental Sync):
//! 1. Schema-Init: Create empty tables using `--schema-only` flag
//! 2. Full-Sync-Setup: Set up triggers/audit table via full sync on empty DB, store checkpoint
//! 3. Populate-N: Insert test data (changes captured in audit table via triggers)
//! 4. Incremental-Sync: Read checkpoint from SurrealDB, sync changes from audit table
//! 5. Verify-N: Validate synced data

use super::common::{
    add_config_volume, add_populate_dependencies, add_surrealdb_dependency, create_service_base,
    get_all_tables,
};
use crate::config::ClusterConfig;
use serde_yaml::{Mapping, Value};

/// Generate schema-init service for MySQL Incremental (creates empty tables).
///
/// This container runs `loadtest populate mysql --schema-only` to create empty tables
/// before the full-sync-setup runs. This ensures triggers can be created on existing tables.
pub fn generate_schema_init_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - populate with --schema-only flag
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "loadtest populate mysql --schema /config/schema.yaml --tables {} --row-count 0 --schema-only --mysql-connection-string 'mysql://root:root@mysql:3306/{}' --aggregator-url http://aggregator:9090",
        tables_arg, config.database.database_name
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes
    add_config_volume(&mut service);

    // Dependencies - wait for MySQL and aggregator
    let mut depends_on = Mapping::new();

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String("mysql".to_string()), Value::Mapping(db_dep));

    let mut agg_dep = Mapping::new();
    agg_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_started".to_string()),
    );
    depends_on.insert(
        Value::String("aggregator".to_string()),
        Value::Mapping(agg_dep),
    );

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}

/// Generate full-sync-setup service for MySQL Incremental.
///
/// This container creates the audit table and triggers, runs full sync on empty tables,
/// and stores the checkpoint to SurrealDB for the incremental sync to use later.
pub fn generate_full_sync_setup_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - full sync with SurrealDB checkpoint storage
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "from mysql full --connection-string 'mysql://root:root@mysql:3306/{}' --tables '{}' --checkpoints-surreal-table surreal_sync_checkpoints --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root",
        config.database.database_name,
        tables_arg,
        config.surrealdb.namespace,
        config.surrealdb.database
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Dependencies - wait for schema-init and SurrealDB
    let mut depends_on = Mapping::new();

    let mut schema_dep = Mapping::new();
    schema_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_completed_successfully".to_string()),
    );
    depends_on.insert(
        Value::String("schema-init".to_string()),
        Value::Mapping(schema_dep),
    );

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

    Value::Mapping(service)
}

/// Generate incremental-sync service for MySQL Incremental.
///
/// This container reads the checkpoint from SurrealDB and processes changes from the
/// audit table that occurred after the full-sync-setup completed (i.e., changes from
/// populate containers captured by triggers).
pub fn generate_incremental_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - incremental sync reading checkpoint from SurrealDB
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "from mysql incremental --connection-string 'mysql://root:root@mysql:3306/{}' --tables '{}' --checkpoints-surreal-table surreal_sync_checkpoints --timeout 60 --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root",
        config.database.database_name,
        tables_arg,
        config.surrealdb.namespace,
        config.surrealdb.database
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Dependencies - wait for ALL populate containers and databases
    add_populate_dependencies(&mut service, config);
    add_surrealdb_dependency(&mut service);

    // Also wait for mysql to be healthy
    let mut depends_on = if let Some(Value::Mapping(existing)) =
        service.remove(Value::String("depends_on".to_string()))
    {
        existing
    } else {
        Mapping::new()
    };

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String("mysql".to_string()), Value::Mapping(db_dep));

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}
