//! PostgreSQL WAL-based logical replication sync handlers.
//!
//! Source crate: crates/postgresql-wal2json-source/
//! CLI commands:
//! - Full sync: `from postgresql full --connection-string ... --slot ... --tables ... --to-namespace ... --to-database ...`
//! - Incremental sync: `from postgresql incremental --connection-string ... --slot ... --tables ... --checkpoints-surreal-table ...`

use anyhow::Context;
use checkpoint::Checkpoint;

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::{PostgreSQLLogicalFullArgs, PostgreSQLLogicalIncrementalArgs};

/// Run PostgreSQL WAL-based full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
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

async fn run_full_v2(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

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

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
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
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
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

    Ok(())
}

async fn run_full_v3(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

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

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            // SurrealDB v3 checkpoint storage
            let store = checkpoint_surreal3::Surreal3Store::new(surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_wal2json_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
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

    Ok(())
}

/// Run PostgreSQL WAL-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLLogicalIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: PostgreSQLLogicalIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            // Explicit checkpoint from CLI
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
        }
        (None, Some(table)) => {
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
                .read_checkpoint::<surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                &s,
            )
        })
        .transpose()?;

    // Parse timeout
    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    // Connect to SurrealDB v2 for data sink
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string.clone(),
        slot_name: args.slot.clone(),
        tables: args.tables.clone(),
        schema: args.schema.clone(),
    };

    surreal_sync_postgresql_wal2json_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        to_checkpoint,
    )
    .await?;

    Ok(())
}

async fn run_incremental_v3(args: PostgreSQLLogicalIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Get checkpoint from CLI arg or SurrealDB
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            // Explicit checkpoint from CLI
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
        }
        (None, Some(table)) => {
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
                .read_checkpoint::<surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                &s,
            )
        })
        .transpose()?;

    // Parse timeout
    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    // Connect to SurrealDB v3 for data sink
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string.clone(),
        slot_name: args.slot.clone(),
        tables: args.tables.clone(),
        schema: args.schema.clone(),
    };

    surreal_sync_postgresql_wal2json_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        to_checkpoint,
    )
    .await?;

    Ok(())
}
