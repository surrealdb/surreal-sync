//! Neo4j full and incremental sync handlers.
//!
//! Source crate: crates/neo4j-source/
//! CLI commands:
//! - Full sync: `from neo4j full --connection-string ... --tables ... --checkpoints-surreal-table ...`
//! - Incremental sync: `from neo4j incremental --connection-string ... --tables ... --checkpoints-surreal-table ...`

use anyhow::Context;
use checkpoint::Checkpoint;

use super::{
    extract_json_fields_from_schema, get_sdk_version, load_schema_if_provided, SdkVersion,
};
use crate::{Neo4jFullArgs, Neo4jIncrementalArgs};

/// Run Neo4j full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: Neo4jFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_full_v2(args).await,
        SdkVersion::V3 => run_full_v3(args).await,
    }
}

async fn run_full_v2(args: Neo4jFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from Neo4j to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    // Parse assumed_start_timestamp if provided
    let assumed_start_timestamp = if let Some(ts_str) = &args.assumed_start_timestamp {
        Some(
            chrono::DateTime::parse_from_rfc3339(ts_str)
                .map_err(|e| anyhow::anyhow!("Invalid --assumed-start-timestamp format: {e}"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        labels: args.tables,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
        change_tracking_property: args.change_tracking_property,
        assumed_start_timestamp,
        allow_empty_tracking_timestamp: args.allow_empty_tracking_timestamp,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_neo4j_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            // SurrealDB v2 checkpoint storage
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_neo4j_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap's conflicts_with
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: Neo4jFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from Neo4j to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    // Parse assumed_start_timestamp if provided
    let assumed_start_timestamp = if let Some(ts_str) = &args.assumed_start_timestamp {
        Some(
            chrono::DateTime::parse_from_rfc3339(ts_str)
                .map_err(|e| anyhow::anyhow!("Invalid --assumed-start-timestamp format: {e}"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        labels: args.tables,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
        change_tracking_property: args.change_tracking_property,
        assumed_start_timestamp,
        allow_empty_tracking_timestamp: args.allow_empty_tracking_timestamp,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_neo4j_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            // SurrealDB v3 checkpoint storage
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_neo4j_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap's conflicts_with
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run Neo4j incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_incremental_v2(args).await,
        SdkVersion::V3 => run_incremental_v3(args).await,
    }
}

async fn run_incremental_v2(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from Neo4j to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            // Explicit checkpoint from CLI
            surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            // Read from SurrealDB v2 checkpoint storage
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_neo4j_source::Neo4jCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    tracing::info!(
        "Starting from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let neo4j_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    // Parse assumed_start_timestamp if provided
    let assumed_start_timestamp = if let Some(ts_str) = &args.assumed_start_timestamp {
        Some(
            chrono::DateTime::parse_from_rfc3339(ts_str)
                .map_err(|e| anyhow::anyhow!("Invalid --assumed-start-timestamp format: {e}"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        labels: args.tables,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
        change_tracking_property: args.change_tracking_property,
        assumed_start_timestamp,
        allow_empty_tracking_timestamp: args.allow_empty_tracking_timestamp,
    };

    // Connect to SurrealDB using v2 SDK
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_neo4j_source::run_incremental_sync(
        &sink,
        source_opts,
        surreal_sync_neo4j_source::SyncOpts {
            batch_size: args.surreal.batch_size,
            dry_run: args.surreal.dry_run,
        },
        from_checkpoint,
        deadline,
        neo4j_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: Neo4jIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from Neo4j to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

    // If json_properties not explicitly provided but schema file is, extract JSON fields from schema
    let json_properties = if args.json_properties.is_some() {
        args.json_properties
    } else if let Some(ref s) = schema {
        let json_fields = extract_json_fields_from_schema(s);
        if !json_fields.is_empty() {
            tracing::info!(
                "Auto-detected JSON properties from schema: {:?}",
                json_fields
            );
            Some(json_fields)
        } else {
            None
        }
    } else {
        None
    };

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            // Explicit checkpoint from CLI
            surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            // Read from SurrealDB v3 checkpoint storage
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_neo4j_source::Neo4jCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    tracing::info!(
        "Starting from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let neo4j_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_neo4j_source::Neo4jCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    // Parse assumed_start_timestamp if provided
    let assumed_start_timestamp = if let Some(ts_str) = &args.assumed_start_timestamp {
        Some(
            chrono::DateTime::parse_from_rfc3339(ts_str)
                .map_err(|e| anyhow::anyhow!("Invalid --assumed-start-timestamp format: {e}"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        source_username: args.username,
        source_password: args.password,
        labels: args.tables,
        neo4j_timezone: args.timezone,
        neo4j_json_properties: json_properties,
        change_tracking_property: args.change_tracking_property,
        assumed_start_timestamp,
        allow_empty_tracking_timestamp: args.allow_empty_tracking_timestamp,
    };

    // Connect to SurrealDB using v3 SDK
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_neo4j_source::run_incremental_sync(
        &sink,
        source_opts,
        surreal_sync_neo4j_source::SyncOpts {
            batch_size: args.surreal.batch_size,
            dry_run: args.surreal.dry_run,
        },
        from_checkpoint,
        deadline,
        neo4j_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
