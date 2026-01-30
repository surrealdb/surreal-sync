//! Docker Compose configuration generator.
//!
//! This module orchestrates Docker Compose service generation by dispatching
//! to source-specific modules for sync service generation.
//!
//! ## Module Organization
//!
//! Source-specific generators are in submodules:
//! - `mysql`: MySQL full sync
//! - `postgresql`: PostgreSQL trigger-based full sync
//! - `postgresql_wal2json_incremental`: PostgreSQL WAL2JSON incremental sync (5-stage pipeline)
//! - `mongodb`: MongoDB full sync
//! - `neo4j`: Neo4j full sync
//! - `kafka`: Kafka streaming sync (per-topic)
//! - `csv_jsonl`: CSV and JSONL file import (per-file)
//! - `common`: Shared helper functions

mod common;
mod csv_jsonl;
mod kafka;
mod mongodb;
mod mongodb_incremental;
mod mysql;
mod mysql_incremental;
mod neo4j;
mod neo4j_incremental;
mod postgresql;
mod postgresql_trigger_incremental;
mod postgresql_wal2json_incremental;

use super::databases;
use super::ConfigGenerator;
use crate::config::{ClusterConfig, SourceType};
use anyhow::Result;
use common::normalize_memory_unit;
use serde_yaml::{Mapping, Value};

/// Generator for Docker Compose configurations.
pub struct DockerComposeGenerator;

impl ConfigGenerator for DockerComposeGenerator {
    fn generate(&self, config: &ClusterConfig) -> Result<String> {
        let mut root = Mapping::new();

        // Version
        root.insert(
            Value::String("version".to_string()),
            Value::String("3.8".to_string()),
        );

        // Services
        let mut services = Mapping::new();

        // Add source database service
        let db_service = databases::generate_docker_service(&config.database);
        services.insert(
            Value::String(common::database_service_name(config.source_type)),
            db_service,
        );

        // Add MongoDB init service if needed
        if config.source_type == SourceType::MongoDB
            || config.source_type == SourceType::MongoDBIncremental
        {
            let init_service = databases::generate_mongodb_init_service();
            services.insert(Value::String("mongodb-init".to_string()), init_service);
        }

        // Add SurrealDB service
        let surrealdb_service = generate_surrealdb_service(config);
        services.insert(Value::String("surrealdb".to_string()), surrealdb_service);

        // Incremental sync sources require a special container sequence (5-stage pipeline)
        if config.source_type == SourceType::MySQLIncremental {
            generate_mysql_incremental_pipeline(&mut services, config);
        } else if config.source_type == SourceType::PostgreSQLTriggerIncremental {
            generate_postgresql_trigger_incremental_pipeline(&mut services, config);
        } else if config.source_type == SourceType::PostgreSQLWal2JsonIncremental {
            generate_postgresql_wal2json_incremental_pipeline(&mut services, config);
        } else if config.source_type == SourceType::MongoDBIncremental {
            generate_mongodb_incremental_pipeline(&mut services, config);
        } else if config.source_type == SourceType::Neo4jIncremental {
            generate_neo4j_incremental_pipeline(&mut services, config);
        } else {
            // Standard flow for other sources (3-stage pipeline)
            generate_standard_pipeline(&mut services, config);
        }

        // Add aggregator service (HTTP-based metrics collection)
        // Expected containers = populate containers + verify containers
        let aggregator_service = generate_aggregator_service(config);
        services.insert(Value::String("aggregator".to_string()), aggregator_service);

        root.insert(
            Value::String("services".to_string()),
            Value::Mapping(services),
        );

        // Networks
        let mut networks = Mapping::new();
        let mut network_config = Mapping::new();
        network_config.insert(
            Value::String("driver".to_string()),
            Value::String("bridge".to_string()),
        );
        networks.insert(
            Value::String(config.network_name.clone()),
            Value::Mapping(network_config),
        );
        root.insert(
            Value::String("networks".to_string()),
            Value::Mapping(networks),
        );

        // Volumes
        let mut volumes = Mapping::new();
        volumes.insert(
            Value::String(config.results_volume.name.clone()),
            Value::Mapping(Mapping::new()),
        );
        // Add database volume if not using tmpfs
        if !config.database.tmpfs_storage {
            volumes.insert(
                Value::String(format!(
                    "{}_data",
                    common::database_service_name(config.source_type)
                )),
                Value::Mapping(Mapping::new()),
            );
        }
        // Add shared data volume for file-based sources (CSV/JSONL)
        if config.source_type == SourceType::Csv || config.source_type == SourceType::Jsonl {
            volumes.insert(
                Value::String("loadtest-data".to_string()),
                Value::Mapping(Mapping::new()),
            );
        }
        root.insert(
            Value::String("volumes".to_string()),
            Value::Mapping(volumes),
        );

        // Convert to YAML string
        let yaml = serde_yaml::to_string(&Value::Mapping(root))?;

        // Add header comment
        let header = format!(
            "# Generated by surreal-loadtest\n# Source: {}\n# Containers: {}\n\n",
            config.source_type,
            config.containers.len()
        );

        Ok(header + &yaml)
    }

    fn filename(&self) -> &str {
        "docker-compose.loadtest.yml"
    }
}

/// Generate MySQL Incremental 5-stage pipeline.
fn generate_mysql_incremental_pipeline(services: &mut Mapping, config: &ClusterConfig) {
    // 1. Add schema-init container (creates empty tables)
    let schema_init_service = mysql_incremental::generate_schema_init_service(config);
    services.insert(
        Value::String("schema-init".to_string()),
        schema_init_service,
    );

    // 2. Add full-sync-setup container (creates triggers/audit table, runs full sync)
    let full_sync_setup_service = mysql_incremental::generate_full_sync_setup_service(config);
    services.insert(
        Value::String("full-sync-setup".to_string()),
        full_sync_setup_service,
    );

    // 3. Add populate containers (depend on full-sync-setup, changes captured by triggers)
    for container in &config.containers {
        let mut populate_service = generate_populate_service(config, container);

        // Update dependencies to wait for full-sync-setup
        if let Some(Value::Mapping(depends_on)) =
            populate_service.get_mut(Value::String("depends_on".to_string()))
        {
            let mut setup_dep = Mapping::new();
            setup_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("full-sync-setup".to_string()),
                Value::Mapping(setup_dep),
            );
        }

        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // 4. Add incremental-sync container (reads checkpoint from SurrealDB, syncs from audit table)
    let incremental_sync_service = mysql_incremental::generate_incremental_sync_service(config);
    services.insert(
        Value::String("incremental-sync".to_string()),
        incremental_sync_service,
    );

    // 5. Add verify containers (depend on incremental-sync)
    for container in &config.containers {
        let mut verify_service = generate_verify_service(config, container);

        // Update dependencies to wait for incremental-sync instead of sync
        if let Some(Value::Mapping(depends_on)) =
            verify_service.get_mut(Value::String("depends_on".to_string()))
        {
            depends_on.remove(Value::String("sync".to_string()));

            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("incremental-sync".to_string()),
                Value::Mapping(sync_dep),
            );
        }

        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate PostgreSQL Trigger Incremental 5-stage pipeline.
fn generate_postgresql_trigger_incremental_pipeline(
    services: &mut Mapping,
    config: &ClusterConfig,
) {
    // 1. Add schema-init container (creates empty tables)
    let schema_init_service = postgresql_trigger_incremental::generate_schema_init_service(config);
    services.insert(
        Value::String("schema-init".to_string()),
        schema_init_service,
    );

    // 2. Add full-sync-setup container (creates triggers/audit table, runs full sync)
    let full_sync_setup_service =
        postgresql_trigger_incremental::generate_full_sync_setup_service(config);
    services.insert(
        Value::String("full-sync-setup".to_string()),
        full_sync_setup_service,
    );

    // 3. Add populate containers (depend on full-sync-setup, changes captured by triggers)
    for container in &config.containers {
        let mut populate_service = generate_populate_service(config, container);

        // Update dependencies to wait for full-sync-setup
        if let Some(Value::Mapping(depends_on)) =
            populate_service.get_mut(Value::String("depends_on".to_string()))
        {
            let mut setup_dep = Mapping::new();
            setup_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("full-sync-setup".to_string()),
                Value::Mapping(setup_dep),
            );
        }

        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // 4. Add incremental-sync container (reads checkpoint from SurrealDB, syncs from audit table)
    let incremental_sync_service =
        postgresql_trigger_incremental::generate_incremental_sync_service(config);
    services.insert(
        Value::String("incremental-sync".to_string()),
        incremental_sync_service,
    );

    // 5. Add verify containers (depend on incremental-sync)
    for container in &config.containers {
        let mut verify_service = generate_verify_service(config, container);

        // Update dependencies to wait for incremental-sync instead of sync
        if let Some(Value::Mapping(depends_on)) =
            verify_service.get_mut(Value::String("depends_on".to_string()))
        {
            depends_on.remove(Value::String("sync".to_string()));

            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("incremental-sync".to_string()),
                Value::Mapping(sync_dep),
            );
        }

        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate PostgreSQL WAL2JSON Incremental 5-stage pipeline.
fn generate_postgresql_wal2json_incremental_pipeline(
    services: &mut Mapping,
    config: &ClusterConfig,
) {
    // 1. Add schema-init container (creates empty tables)
    let schema_init_service = postgresql_wal2json_incremental::generate_schema_init_service(config);
    services.insert(
        Value::String("schema-init".to_string()),
        schema_init_service,
    );

    // 2. Add full-sync-setup container (creates slot, runs full sync, emits checkpoints)
    let full_sync_setup_service =
        postgresql_wal2json_incremental::generate_full_sync_setup_service(config);
    services.insert(
        Value::String("full-sync-setup".to_string()),
        full_sync_setup_service,
    );

    // 3. Add populate containers (depend on full-sync-setup)
    for container in &config.containers {
        let mut populate_service = generate_populate_service(config, container);

        // Update dependencies to wait for full-sync-setup
        if let Some(Value::Mapping(depends_on)) =
            populate_service.get_mut(Value::String("depends_on".to_string()))
        {
            let mut setup_dep = Mapping::new();
            setup_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("full-sync-setup".to_string()),
                Value::Mapping(setup_dep),
            );
        }

        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // 4. Add incremental-sync container (reads checkpoint from SurrealDB, processes WAL changes)
    let incremental_sync_service =
        postgresql_wal2json_incremental::generate_incremental_sync_service(config);
    services.insert(
        Value::String("incremental-sync".to_string()),
        incremental_sync_service,
    );

    // 5. Add verify containers (depend on incremental-sync)
    for container in &config.containers {
        let mut verify_service = generate_verify_service(config, container);

        // Update dependencies to wait for incremental-sync instead of sync
        if let Some(Value::Mapping(depends_on)) =
            verify_service.get_mut(Value::String("depends_on".to_string()))
        {
            depends_on.remove(Value::String("sync".to_string()));

            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("incremental-sync".to_string()),
                Value::Mapping(sync_dep),
            );
        }

        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate MongoDB Incremental 5-stage pipeline.
fn generate_mongodb_incremental_pipeline(services: &mut Mapping, config: &ClusterConfig) {
    // 1. Add schema-init container (creates empty collections)
    let schema_init_service = mongodb_incremental::generate_schema_init_service(config);
    services.insert(
        Value::String("schema-init".to_string()),
        schema_init_service,
    );

    // 2. Add full-sync-setup container (runs full sync, stores change stream resume token)
    let full_sync_setup_service = mongodb_incremental::generate_full_sync_setup_service(config);
    services.insert(
        Value::String("full-sync-setup".to_string()),
        full_sync_setup_service,
    );

    // 3. Add populate containers (depend on full-sync-setup, changes captured by change stream)
    for container in &config.containers {
        let mut populate_service = generate_populate_service(config, container);

        // Update dependencies to wait for full-sync-setup
        if let Some(Value::Mapping(depends_on)) =
            populate_service.get_mut(Value::String("depends_on".to_string()))
        {
            let mut setup_dep = Mapping::new();
            setup_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("full-sync-setup".to_string()),
                Value::Mapping(setup_dep),
            );
        }

        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // 4. Add incremental-sync container (reads resume token from SurrealDB, syncs from change stream)
    let incremental_sync_service = mongodb_incremental::generate_incremental_sync_service(config);
    services.insert(
        Value::String("incremental-sync".to_string()),
        incremental_sync_service,
    );

    // 5. Add verify containers (depend on incremental-sync)
    for container in &config.containers {
        let mut verify_service = generate_verify_service(config, container);

        // Update dependencies to wait for incremental-sync instead of sync
        if let Some(Value::Mapping(depends_on)) =
            verify_service.get_mut(Value::String("depends_on".to_string()))
        {
            depends_on.remove(Value::String("sync".to_string()));

            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("incremental-sync".to_string()),
                Value::Mapping(sync_dep),
            );
        }

        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate Neo4j Incremental 5-stage pipeline.
fn generate_neo4j_incremental_pipeline(services: &mut Mapping, config: &ClusterConfig) {
    // 1. Add schema-init container (creates empty nodes)
    let schema_init_service = neo4j_incremental::generate_schema_init_service(config);
    services.insert(
        Value::String("schema-init".to_string()),
        schema_init_service,
    );

    // 2. Add full-sync-setup container (runs full sync, stores timestamp checkpoint)
    let full_sync_setup_service = neo4j_incremental::generate_full_sync_setup_service(config);
    services.insert(
        Value::String("full-sync-setup".to_string()),
        full_sync_setup_service,
    );

    // 3. Add populate containers (depend on full-sync-setup, changes tracked by timestamp)
    for container in &config.containers {
        let mut populate_service = generate_populate_service(config, container);

        // Update dependencies to wait for full-sync-setup
        if let Some(Value::Mapping(depends_on)) =
            populate_service.get_mut(Value::String("depends_on".to_string()))
        {
            let mut setup_dep = Mapping::new();
            setup_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("full-sync-setup".to_string()),
                Value::Mapping(setup_dep),
            );
        }

        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // 4. Add incremental-sync container (reads checkpoint from SurrealDB, syncs newer nodes)
    let incremental_sync_service = neo4j_incremental::generate_incremental_sync_service(config);
    services.insert(
        Value::String("incremental-sync".to_string()),
        incremental_sync_service,
    );

    // 5. Add verify containers (depend on incremental-sync)
    for container in &config.containers {
        let mut verify_service = generate_verify_service(config, container);

        // Update dependencies to wait for incremental-sync instead of sync
        if let Some(Value::Mapping(depends_on)) =
            verify_service.get_mut(Value::String("depends_on".to_string()))
        {
            depends_on.remove(Value::String("sync".to_string()));

            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String("incremental-sync".to_string()),
                Value::Mapping(sync_dep),
            );
        }

        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate standard 3-stage pipeline for most sources.
fn generate_standard_pipeline(services: &mut Mapping, config: &ClusterConfig) {
    // Add populate container services
    for container in &config.containers {
        let populate_service = generate_populate_service(config, container);
        services.insert(Value::String(container.id.clone()), populate_service);
    }

    // Add sync service(s)
    // Kafka, CSV, and JSONL need one sync service per table since their CLIs handle one table at a time
    if config.source_type == SourceType::Kafka {
        for (idx, container) in config.containers.iter().enumerate() {
            for table in &container.tables {
                let sync_service =
                    kafka::generate_sync_service(config, table, container.row_count, idx + 1);
                services.insert(Value::String(format!("sync-{table}")), sync_service);
            }
        }
    } else if config.source_type == SourceType::Csv || config.source_type == SourceType::Jsonl {
        // CSV/JSONL need one sync service per file/table
        for (idx, container) in config.containers.iter().enumerate() {
            for table in &container.tables {
                let sync_service =
                    csv_jsonl::generate_sync_service(config, table, container.row_count, idx + 1);
                services.insert(Value::String(format!("sync-{table}")), sync_service);
            }
        }
    } else {
        // For other sources, a single sync service handles all tables
        let sync_service = generate_sync_service(config);
        services.insert(Value::String("sync".to_string()), sync_service);
    }

    // Add verify container services (run after sync completes)
    for container in &config.containers {
        let verify_service = generate_verify_service(config, container);
        // Use verify-N naming to match populate-N
        let verify_id = container.id.replace("populate-", "verify-");
        services.insert(Value::String(verify_id), verify_service);
    }
}

/// Generate sync service by dispatching to the appropriate source-specific module.
fn generate_sync_service(config: &ClusterConfig) -> Value {
    match config.source_type {
        SourceType::MySQL => mysql::generate_sync_service(config),
        SourceType::MySQLIncremental => {
            // MySQL incremental uses the 5-stage pipeline, this shouldn't be called directly
            unreachable!("MySQLIncremental uses 5-stage pipeline, not single sync service")
        }
        SourceType::PostgreSQL => postgresql::generate_sync_service(config),
        SourceType::PostgreSQLTriggerIncremental | SourceType::PostgreSQLWal2JsonIncremental => {
            // PostgreSQL incremental types use the 5-stage pipeline, this shouldn't be called directly
            unreachable!(
                "PostgreSQL incremental types use 5-stage pipeline, not single sync service"
            )
        }
        SourceType::MongoDB => mongodb::generate_sync_service(config),
        SourceType::MongoDBIncremental => {
            // MongoDB incremental uses the 5-stage pipeline, this shouldn't be called directly
            unreachable!("MongoDBIncremental uses 5-stage pipeline, not single sync service")
        }
        SourceType::Neo4j => neo4j::generate_sync_service(config),
        SourceType::Neo4jIncremental => {
            // Neo4j incremental uses the 5-stage pipeline, this shouldn't be called directly
            unreachable!("Neo4jIncremental uses 5-stage pipeline, not single sync service")
        }
        SourceType::Kafka => {
            unreachable!("Kafka uses per-topic sync services, not a single sync service")
        }
        SourceType::Csv | SourceType::Jsonl => {
            unreachable!("CSV/JSONL use per-file sync services, not a single sync service")
        }
    }
}

/// Generate SurrealDB service configuration.
fn generate_surrealdb_service(config: &ClusterConfig) -> Value {
    let mut service = Mapping::new();

    service.insert(
        Value::String("image".to_string()),
        Value::String(config.surrealdb.image.clone()),
    );

    // Command
    let storage = if config.surrealdb.memory_storage {
        "memory"
    } else {
        "file:/data/database.db"
    };
    service.insert(
        Value::String("command".to_string()),
        Value::String(format!(
            "start --log info --user root --pass root --bind 0.0.0.0:8000 {storage}"
        )),
    );

    // Resources
    let mut deploy = Mapping::new();
    let mut resources = Mapping::new();
    let mut limits = Mapping::new();
    limits.insert(
        Value::String("cpus".to_string()),
        Value::String(config.surrealdb.resources.cpu_limit.clone()),
    );
    limits.insert(
        Value::String("memory".to_string()),
        Value::String(normalize_memory_unit(
            &config.surrealdb.resources.memory_limit,
        )),
    );
    resources.insert(Value::String("limits".to_string()), Value::Mapping(limits));
    deploy.insert(
        Value::String("resources".to_string()),
        Value::Mapping(resources),
    );
    service.insert(Value::String("deploy".to_string()), Value::Mapping(deploy));

    // Networks
    let networks = vec![Value::String(config.network_name.clone())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    // Healthcheck using surreal is-ready (curl is not available in surrealdb image)
    let mut healthcheck = Mapping::new();
    healthcheck.insert(
        Value::String("test".to_string()),
        Value::Sequence(vec![
            Value::String("CMD".to_string()),
            Value::String("/surreal".to_string()),
            Value::String("is-ready".to_string()),
            Value::String("--endpoint".to_string()),
            Value::String("http://localhost:8000".to_string()),
        ]),
    );
    healthcheck.insert(
        Value::String("interval".to_string()),
        Value::String("5s".to_string()),
    );
    healthcheck.insert(
        Value::String("timeout".to_string()),
        Value::String("3s".to_string()),
    );
    healthcheck.insert(
        Value::String("retries".to_string()),
        Value::Number(10.into()),
    );
    healthcheck.insert(
        Value::String("start_period".to_string()),
        Value::String("5s".to_string()),
    );
    service.insert(
        Value::String("healthcheck".to_string()),
        Value::Mapping(healthcheck),
    );

    Value::Mapping(service)
}

/// Generate populate service configuration.
fn generate_populate_service(
    config: &ClusterConfig,
    container: &crate::config::ContainerConfig,
) -> Value {
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

    // Command - uses surreal-sync loadtest populate
    let tables_arg = container.tables.join(",");
    let source_cmd = match config.source_type {
        // All MySQL variants use the same populate command
        SourceType::MySQL | SourceType::MySQLIncremental => "mysql",
        // All PostgreSQL variants use the same populate command (data goes to same database)
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => "postgresql",
        SourceType::MongoDB | SourceType::MongoDBIncremental => "mongodb",
        SourceType::Neo4j | SourceType::Neo4jIncremental => "neo4j",
        SourceType::Kafka => "kafka",
        SourceType::Csv => "csv",
        SourceType::Jsonl => "jsonl",
    };

    // Build connection string args based on source type
    let connection_args = match config.source_type {
        SourceType::MySQL | SourceType::MySQLIncremental => {
            format!(
                "--mysql-connection-string '{}'",
                container.connection_string
            )
        }
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => {
            format!(
                "--postgresql-connection-string '{}'",
                container.connection_string
            )
        }
        SourceType::MongoDB | SourceType::MongoDBIncremental => {
            // MongoDB needs connection string and database
            format!(
                "--mongodb-connection-string '{}' --mongodb-database loadtest",
                container.connection_string
            )
        }
        SourceType::Neo4j | SourceType::Neo4jIncremental => {
            format!(
                "--neo4j-connection-string '{}' --neo4j-username neo4j --neo4j-password password",
                container.connection_string
            )
        }
        SourceType::Kafka => {
            format!("--kafka-brokers '{}'", container.connection_string)
        }
        SourceType::Csv | SourceType::Jsonl => {
            format!("--output-dir '{}'", container.connection_string)
        }
    };

    let dry_run_flag = if config.dry_run { " --dry-run" } else { "" };
    // For incremental types, populate runs AFTER schema-init creates tables, so use --data-only
    let data_only_flag = if config.source_type == SourceType::PostgreSQLTriggerIncremental
        || config.source_type == SourceType::PostgreSQLWal2JsonIncremental
        || config.source_type == SourceType::MySQLIncremental
        || config.source_type == SourceType::MongoDBIncremental
        || config.source_type == SourceType::Neo4jIncremental
    {
        " --data-only"
    } else {
        ""
    };
    let command = format!(
        "loadtest populate {} --schema /config/schema.yaml --tables {} --row-count {} --seed {} {} --batch-size {} --aggregator-url http://aggregator:9090{}{}",
        source_cmd,
        tables_arg,
        container.row_count,
        container.seed,
        connection_args,
        container.batch_size,
        dry_run_flag,
        data_only_flag
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Environment
    let mut environment = Vec::new();
    environment.push(Value::String(format!("CONTAINER_ID={}", container.id)));
    environment.push(Value::String("RUST_LOG=info".to_string()));
    service.insert(
        Value::String("environment".to_string()),
        Value::Sequence(environment),
    );

    // Volumes - config volume always needed, plus shared data volume for file-based sources
    let mut volumes = vec![Value::String("./config:/config:ro".to_string())];

    // For CSV/JSONL, use shared volume instead of tmpfs so sync container can read the files
    if config.source_type == SourceType::Csv || config.source_type == SourceType::Jsonl {
        volumes.push(Value::String("loadtest-data:/data".to_string()));
    }

    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volumes),
    );

    // tmpfs - only for non-file-based sources (file-based sources use shared volume)
    if config.source_type != SourceType::Csv && config.source_type != SourceType::Jsonl {
        if let Some(ref tmpfs) = container.tmpfs {
            let mut tmpfs_mounts = Vec::new();
            tmpfs_mounts.push(Value::String(format!(
                "{}:size={}",
                tmpfs.path,
                normalize_memory_unit(&tmpfs.size)
            )));
            service.insert(
                Value::String("tmpfs".to_string()),
                Value::Sequence(tmpfs_mounts),
            );
        }
    }

    // Resources
    let mut deploy = Mapping::new();
    let mut resources = Mapping::new();
    let mut limits = Mapping::new();
    limits.insert(
        Value::String("cpus".to_string()),
        Value::String(container.resources.cpu_limit.clone()),
    );
    limits.insert(
        Value::String("memory".to_string()),
        Value::String(normalize_memory_unit(&container.resources.memory_limit)),
    );
    resources.insert(Value::String("limits".to_string()), Value::Mapping(limits));
    deploy.insert(
        Value::String("resources".to_string()),
        Value::Mapping(resources),
    );
    service.insert(Value::String("deploy".to_string()), Value::Mapping(deploy));

    // Networks
    let networks = vec![Value::String(config.network_name.clone())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    // Dependencies - wait for aggregator to start (and database to be healthy)
    let mut depends_on = Mapping::new();
    let db_name = common::database_service_name(config.source_type);

    let mut db_dep = Mapping::new();
    db_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(Value::String(db_name), Value::Mapping(db_dep));

    // Wait for aggregator to start
    let mut aggregator_dep = Mapping::new();
    aggregator_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_started".to_string()),
    );
    depends_on.insert(
        Value::String("aggregator".to_string()),
        Value::Mapping(aggregator_dep),
    );

    // For MongoDB, also wait for mongodb-init to complete (replica set initialization)
    if config.source_type == SourceType::MongoDB
        || config.source_type == SourceType::MongoDBIncremental
    {
        let mut init_dep = Mapping::new();
        init_dep.insert(
            Value::String("condition".to_string()),
            Value::String("service_completed_successfully".to_string()),
        );
        depends_on.insert(
            Value::String("mongodb-init".to_string()),
            Value::Mapping(init_dep),
        );
    }

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}

/// Generate verify service configuration (runs after sync completes).
fn generate_verify_service(
    config: &ClusterConfig,
    container: &crate::config::ContainerConfig,
) -> Value {
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

    // Command - uses surreal-sync loadtest verify (new CLI structure)
    let tables_arg = container.tables.join(",");
    let dry_run_flag = if config.dry_run { " --dry-run" } else { "" };
    let command = format!(
        "loadtest verify --surreal-endpoint 'http://surrealdb:8000' --surreal-namespace {} --surreal-database {} --surreal-username root --surreal-password root --schema /config/schema.yaml --tables {tables_arg} --row-count {} --seed {} --aggregator-url http://aggregator:9090{}",
        config.surrealdb.namespace,
        config.surrealdb.database,
        container.row_count,
        container.seed,
        dry_run_flag
    );
    service.insert(Value::String("command".to_string()), Value::String(command));

    // Environment - use verify-N naming to match service name
    let verify_id = container.id.replace("populate-", "verify-");
    let mut environment = Vec::new();
    environment.push(Value::String(format!("CONTAINER_ID={verify_id}")));
    environment.push(Value::String("RUST_LOG=info".to_string()));
    service.insert(
        Value::String("environment".to_string()),
        Value::Sequence(environment),
    );

    // Volumes - mount config
    let volumes = vec![Value::String("./config:/config:ro".to_string())];
    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volumes),
    );

    // Networks
    let networks = vec![Value::String(config.network_name.clone())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    // Dependencies - wait for sync to complete
    let mut depends_on = Mapping::new();

    // For Kafka/CSV/JSONL, wait for all sync-<table> services; for other sources, wait for single "sync" service
    if config.source_type == SourceType::Kafka
        || config.source_type == SourceType::Csv
        || config.source_type == SourceType::Jsonl
    {
        // Wait for sync services for tables this container handles
        for table in &container.tables {
            let mut sync_dep = Mapping::new();
            sync_dep.insert(
                Value::String("condition".to_string()),
                Value::String("service_completed_successfully".to_string()),
            );
            depends_on.insert(
                Value::String(format!("sync-{table}")),
                Value::Mapping(sync_dep),
            );
        }
    } else {
        let mut sync_dep = Mapping::new();
        sync_dep.insert(
            Value::String("condition".to_string()),
            Value::String("service_completed_successfully".to_string()),
        );
        depends_on.insert(Value::String("sync".to_string()), Value::Mapping(sync_dep));
    }

    // Wait for aggregator to be ready (so we can POST metrics)
    let mut aggregator_dep = Mapping::new();
    aggregator_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_started".to_string()),
    );
    depends_on.insert(
        Value::String("aggregator".to_string()),
        Value::Mapping(aggregator_dep),
    );

    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    Value::Mapping(service)
}

/// Generate aggregator service configuration (HTTP-based).
fn generate_aggregator_service(config: &ClusterConfig) -> Value {
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

    // Command - uses surreal-sync loadtest aggregate-server
    // Expected containers = populate containers + verify containers (2x)
    let num_containers = config.containers.len() * 2;
    service.insert(
        Value::String("command".to_string()),
        Value::String(format!(
            "loadtest aggregate-server --listen 0.0.0.0:9090 --expected-containers {num_containers} --timeout 30m --output-format table"
        )),
    );

    // Environment
    let environment = vec![Value::String("RUST_LOG=info".to_string())];
    service.insert(
        Value::String("environment".to_string()),
        Value::Sequence(environment),
    );

    // Expose port (useful for debugging/status)
    let ports = vec![Value::String("9090:9090".to_string())];
    service.insert(Value::String("ports".to_string()), Value::Sequence(ports));

    // Networks
    let networks = vec![Value::String(config.network_name.clone())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    // Note: No healthcheck for aggregator - curl is not available in the minimal container.
    // The aggregator is treated as ready when the container starts.
    // Other containers use service_started condition for dependency.

    // No dependencies - aggregator starts first, other containers depend on it

    Value::Mapping(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn test_config() -> ClusterConfig {
        test_config_with_dry_run(false)
    }

    fn test_config_with_dry_run(dry_run: bool) -> ClusterConfig {
        ClusterConfig {
            platform: Platform::DockerCompose,
            source_type: SourceType::MySQL,
            containers: vec![ContainerConfig {
                id: "populate-1".to_string(),
                source_type: SourceType::MySQL,
                tables: vec!["users".to_string()],
                row_count: 1000,
                seed: 42,
                resources: ResourceLimits::default(),
                tmpfs: None,
                connection_string: "mysql://root:root@mysql:3306/testdb".to_string(),
                batch_size: 100,
            }],
            results_volume: VolumeConfig::default(),
            database: DatabaseConfig {
                source_type: SourceType::MySQL,
                image: "mysql:8.0".to_string(),
                resources: ResourceLimits::default(),
                tmpfs_storage: false,
                tmpfs_size: None,
                database_name: "testdb".to_string(),
                environment: vec![],
                command_args: vec![],
            },
            surrealdb: SurrealDbConfig::default(),
            schema_path: "schema.yaml".to_string(),
            schema_content: None, // Not needed for Docker Compose (mounted as volume)
            proto_contents: None, // Not needed for Docker Compose (mounted as volume)
            network_name: "loadtest".to_string(),
            dry_run,
            num_sync_containers: 1, // MySQL uses single sync container
        }
    }

    #[test]
    fn test_generate_docker_compose() {
        let config = test_config();
        let generator = DockerComposeGenerator;
        let yaml = generator.generate(&config).unwrap();

        assert!(yaml.contains("version:"));
        assert!(yaml.contains("services:"));
        assert!(yaml.contains("mysql:"));
        assert!(yaml.contains("surrealdb:"));
        assert!(yaml.contains("populate-1:"));
        assert!(yaml.contains("aggregator:"));
        // Verify HTTP-based aggregation
        assert!(yaml.contains("aggregate-server"));
        assert!(yaml.contains("0.0.0.0:9090"));
    }

    #[test]
    fn test_docker_compose_without_dry_run() {
        let config = test_config_with_dry_run(false);
        let generator = DockerComposeGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Should NOT contain --dry-run flag
        assert!(!yaml.contains("--dry-run"));
        // Should still contain populate command
        assert!(yaml.contains("loadtest populate mysql"));
    }

    #[test]
    fn test_docker_compose_with_dry_run() {
        let config = test_config_with_dry_run(true);
        let generator = DockerComposeGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Should contain --dry-run flag in populate and verify commands
        assert!(yaml.contains("--dry-run"));
        // Should contain populate command with --dry-run
        assert!(yaml.contains("loadtest populate mysql"));

        // Parse YAML to verify structure
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let services = parsed.get("services").unwrap().as_mapping().unwrap();

        // Check populate command contains --dry-run
        let populate = services.get("populate-1").unwrap().as_mapping().unwrap();
        let command = populate.get("command").unwrap().as_str().unwrap();
        assert!(
            command.contains("--dry-run"),
            "Populate command should contain --dry-run"
        );
    }

    #[test]
    fn test_num_sync_containers_matches_generated_kafka_services() {
        // Create a Kafka config with 3 tables across 2 containers
        let config = ClusterConfig {
            platform: Platform::DockerCompose,
            source_type: SourceType::Kafka,
            containers: vec![
                ContainerConfig {
                    id: "populate-1".to_string(),
                    source_type: SourceType::Kafka,
                    tables: vec!["users".to_string(), "products".to_string()],
                    row_count: 1000,
                    seed: 42,
                    resources: ResourceLimits::default(),
                    tmpfs: None,
                    connection_string: "kafka:9092".to_string(),
                    batch_size: 100,
                },
                ContainerConfig {
                    id: "populate-2".to_string(),
                    source_type: SourceType::Kafka,
                    tables: vec!["orders".to_string()],
                    row_count: 1000,
                    seed: 43,
                    resources: ResourceLimits::default(),
                    tmpfs: None,
                    connection_string: "kafka:9092".to_string(),
                    batch_size: 100,
                },
            ],
            results_volume: VolumeConfig::default(),
            database: DatabaseConfig {
                source_type: SourceType::Kafka,
                image: "bitnami/kafka:latest".to_string(),
                resources: ResourceLimits::default(),
                tmpfs_storage: false,
                tmpfs_size: None,
                database_name: String::new(),
                environment: vec![],
                command_args: vec![],
            },
            surrealdb: SurrealDbConfig::default(),
            schema_path: "schema.yaml".to_string(),
            schema_content: None,
            proto_contents: None,
            network_name: "loadtest".to_string(),
            dry_run: false,
            num_sync_containers: 3, // 3 tables = 3 sync containers for Kafka
        };

        // Generate docker-compose YAML
        let generator = DockerComposeGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Parse YAML and count sync-* services
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let services = parsed.get("services").unwrap().as_mapping().unwrap();

        let sync_service_count = services
            .keys()
            .filter(|k| k.as_str().map(|s| s.starts_with("sync-")).unwrap_or(false))
            .count();

        // Verify num_sync_containers matches actual sync services generated
        assert_eq!(
            config.num_sync_containers, sync_service_count,
            "num_sync_containers ({}) should match actual sync services in docker-compose ({})",
            config.num_sync_containers, sync_service_count
        );
    }

    #[test]
    fn test_num_sync_containers_for_non_kafka_sources() {
        // For non-Kafka sources, num_sync_containers should be 1
        let config = test_config(); // Uses MySQL

        let generator = DockerComposeGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Parse YAML and count sync services
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let services = parsed.get("services").unwrap().as_mapping().unwrap();

        // Should have exactly one "sync" service (not sync-*)
        assert!(services.contains_key(Value::String("sync".to_string())));

        let sync_service_count = services
            .keys()
            .filter(|k| {
                k.as_str()
                    .map(|s| s == "sync" || s.starts_with("sync-"))
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(
            sync_service_count, 1,
            "Non-Kafka sources should have exactly 1 sync service"
        );
    }
}
