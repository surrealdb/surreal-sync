//! MongoDB change stream incremental sync Docker Compose service generators.
//!
//! Source crate: crates/mongodb-changestream-source/
//! SourceChoice: MongodbIncremental
//! SourceType: MongoDBIncremental
//! CLI commands:
//!   - Schema init: `loadtest populate mongodb --schema-only ...`
//!   - Full sync setup: `from mongodb full --checkpoints-surreal-table ...`
//!   - Incremental sync: `from mongodb incremental --checkpoints-surreal-table ...`
//!
//! ## Pipeline
//!
//! 5-stage (Incremental Sync):
//! 1. Schema-Init: Create empty collections using `--schema-only` flag
//! 2. Full-Sync-Setup: Run full sync on empty DB, store change stream resume token
//! 3. Populate-N: Insert test data (captured by change stream)
//! 4. Incremental-Sync: Read resume token from SurrealDB, sync changes from change stream
//! 5. Verify-N: Validate synced data

use super::common::{
    add_config_volume, add_populate_dependencies, add_surrealdb_dependency, create_service_base,
    get_all_tables,
};
use crate::config::ClusterConfig;
use serde_yaml::{Mapping, Value};

/// Generate schema-init service for MongoDB Incremental (creates empty collections).
///
/// This container runs `loadtest populate mongodb --schema-only` to create empty collections
/// before the full-sync-setup runs. This ensures change streams can be started on existing collections.
pub fn generate_schema_init_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - populate with --schema-only flag
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "loadtest populate mongodb --schema /config/schema.yaml --tables {} --row-count 0 --schema-only --mongodb-connection-string 'mongodb://root:root@mongodb:27017' --mongodb-database {} --aggregator-url http://aggregator:9090",
        tables_arg, config.database.database_name
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes
    add_config_volume(&mut service);

    // Dependencies - wait for mongodb-init (replica set), mongodb, and aggregator
    let mut depends_on = Mapping::new();

    // Wait for mongodb-init to complete (replica set initialization)
    let mut init_dep = Mapping::new();
    init_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_completed_successfully".to_string()),
    );
    depends_on.insert(
        Value::String("mongodb-init".to_string()),
        Value::Mapping(init_dep),
    );

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String("mongodb".to_string()), Value::Mapping(db_dep));

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

/// Generate full-sync-setup service for MongoDB Incremental.
///
/// This container runs full sync on empty collections and stores the change stream
/// resume token to SurrealDB for the incremental sync to use later.
pub fn generate_full_sync_setup_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - full sync with SurrealDB checkpoint storage
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "from mongodb full --connection-string 'mongodb://root:root@mongodb:27017' --database {} --tables '{}' --checkpoints-surreal-table surreal_sync_checkpoints --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root",
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

/// Generate incremental-sync service for MongoDB Incremental.
///
/// This container reads the change stream resume token from SurrealDB and processes
/// changes that occurred after the full-sync-setup completed (i.e., changes from
/// populate containers captured by change stream).
pub fn generate_incremental_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - incremental sync reading checkpoint from SurrealDB
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "from mongodb incremental --connection-string 'mongodb://root:root@mongodb:27017' --database {} --tables '{}' --checkpoints-surreal-table surreal_sync_checkpoints --timeout 60 --to-namespace {} --to-database {} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root",
        config.database.database_name,
        tables_arg,
        config.surrealdb.namespace,
        config.surrealdb.database
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Dependencies - wait for ALL populate containers and databases
    add_populate_dependencies(&mut service, config);
    add_surrealdb_dependency(&mut service);

    // Also wait for mongodb to be healthy
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
    depends_on.insert(Value::String("mongodb".to_string()), Value::Mapping(db_dep));

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}
