//! Neo4j timestamp-based incremental sync Docker Compose service generators.
//!
//! Source crate: crates/neo4j-source/
//! SourceChoice: Neo4jIncremental
//! SourceType: Neo4jIncremental
//! CLI commands:
//!   - Schema init: `loadtest populate neo4j --schema-only ...`
//!   - Full sync setup: `from neo4j full --checkpoints-surreal-table ...`
//!   - Incremental sync: `from neo4j incremental --checkpoints-surreal-table ...`
//!
//! ## Pipeline
//!
//! 5-stage (Incremental Sync):
//! 1. Schema-Init: Create empty nodes using `--schema-only` flag
//! 2. Full-Sync-Setup: Run full sync on empty DB, store timestamp checkpoint
//! 3. Populate-N: Insert test data with `updated_at` timestamps (tracked by timestamp)
//! 4. Incremental-Sync: Read timestamp from SurrealDB, sync nodes with newer timestamps
//! 5. Verify-N: Validate synced data
//!
//! **Limitation:** Neo4j incremental sync cannot detect deletions (timestamp-based tracking only).

use super::common::{
    add_config_volume, add_populate_dependencies, add_surrealdb_dependency, create_service_base,
    get_all_tables,
};
use crate::config::ClusterConfig;
use serde_yaml::{Mapping, Value};

/// Generate schema-init service for Neo4j Incremental (creates empty nodes).
///
/// This container runs `loadtest populate neo4j --schema-only` to create empty node labels
/// before the full-sync-setup runs.
pub fn generate_schema_init_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - populate with --schema-only flag
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let command = format!(
        "loadtest populate neo4j --schema /config/schema.yaml --tables {tables_arg} --row-count 0 --schema-only --neo4j-connection-string 'bolt://neo4j:7687' --neo4j-username neo4j --neo4j-password password --aggregator-url http://aggregator:9090"
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Volumes
    add_config_volume(&mut service);

    // Dependencies - wait for neo4j and aggregator
    let mut depends_on = Mapping::new();

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String("neo4j".to_string()), Value::Mapping(db_dep));

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

/// Generate full-sync-setup service for Neo4j Incremental.
///
/// This container runs full sync on empty nodes and stores the timestamp
/// checkpoint to SurrealDB for the incremental sync to use later.
pub fn generate_full_sync_setup_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - full sync with SurrealDB checkpoint storage
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let namespace = &config.surrealdb.namespace;
    let database = &config.surrealdb.database;
    let command = format!(
        "from neo4j full --connection-string 'bolt://neo4j:7687' --username neo4j --password password --tables '{tables_arg}' --checkpoints-surreal-table surreal_sync_checkpoints --to-namespace {namespace} --to-database {database} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root"
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

/// Generate incremental-sync service for Neo4j Incremental.
///
/// This container reads the timestamp checkpoint from SurrealDB and processes
/// nodes that have been updated after the full-sync-setup completed (i.e., nodes from
/// populate containers with newer `updated_at` timestamps).
pub fn generate_incremental_sync_service(config: &ClusterConfig) -> Value {
    let mut service = create_service_base(config);

    // Command - incremental sync reading checkpoint from SurrealDB
    let tables = get_all_tables(config);
    let tables_arg = tables.join(",");

    let namespace = &config.surrealdb.namespace;
    let database = &config.surrealdb.database;
    let command = format!(
        "from neo4j incremental --connection-string 'bolt://neo4j:7687' --username neo4j --password password --tables '{tables_arg}' --checkpoints-surreal-table surreal_sync_checkpoints --timeout 60 --to-namespace {namespace} --to-database {database} --surreal-endpoint 'http://surrealdb:8000' --surreal-username root --surreal-password root"
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Dependencies - wait for ALL populate containers and databases
    add_populate_dependencies(&mut service, config);
    add_surrealdb_dependency(&mut service);

    // Also wait for neo4j to be healthy
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
    depends_on.insert(Value::String("neo4j".to_string()), Value::Mapping(db_dep));

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}
