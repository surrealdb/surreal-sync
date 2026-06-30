//! MySQL trigger-based CDC sync handlers.
//!
//! Source crate: crates/mysql-trigger-source/
//! CLI commands:
//! - Full sync: `from mysql full --connection-string ... --tables ... --checkpoints-surreal-table ...`
//! - Incremental sync: `from mysql incremental --connection-string ... --tables ... --checkpoints-surreal-table ...`

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync::orchestrate_snapshot_then_incremental;
use surreal_sync_mysql_trigger_source::MySQLCheckpoint;
use surreal_sync_snapshot_stream::{NoopCheckpointer, SnapshotStreamConfig};

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::{MySQLFullArgs, MySQLIncrementalArgs, MySQLSnapshotArgs, MySQLSyncArgs, SyncStrategy};

/// Run MySQL full sync, dispatching by strategy then SDK version.
pub async fn run_full(args: MySQLFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match (args.strategy, sdk_version) {
        (SyncStrategy::Bulk, SdkVersion::V2) => run_full_v2(args).await,
        (SyncStrategy::Bulk, SdkVersion::V3) => run_full_v3(args).await,
        (SyncStrategy::SnapshotStream, SdkVersion::V2) => run_full_snapshot_stream_v2(args).await,
        (SyncStrategy::SnapshotStream, SdkVersion::V3) => run_full_snapshot_stream_v3(args).await,
    }
}

async fn run_full_v2(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL to SurrealDB (SDK v2)");
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

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        tables: args.tables,
        mysql_boolean_paths: args.boolean_paths,
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
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
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
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

async fn run_full_v3(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL to SurrealDB (SDK v3)");
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

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        tables: args.tables,
        mysql_boolean_paths: args.boolean_paths,
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    // Handle checkpoint storage
    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            // Filesystem checkpoint storage
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
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
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            // No checkpoint storage
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
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

/// Run MySQL incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL to SurrealDB (SDK v2)");
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
            tracing::info!("Starting from checkpoint: {}", s);
            // Explicit checkpoint from CLI
            surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s)?
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
                .read_checkpoint::<surreal_sync_mysql_trigger_source::MySQLCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let mysql_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        tables: args.tables,
        mysql_boolean_paths: args.boolean_paths,
    };

    // Connect to SurrealDB using v2 SDK
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_mysql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        mysql_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// Watermark snapshot+stream strategy
// =============================================================================

/// Resolve the MySQL database name from the explicit option or the connection's
/// current database.
async fn resolve_mysql_database(
    pool: &mysql_async::Pool,
    explicit: &Option<String>,
) -> anyhow::Result<String> {
    if let Some(db) = explicit {
        return Ok(db.clone());
    }
    let mut conn = pool.get_conn().await?;
    let current: Option<String> = mysql_async::prelude::Queryable::query_first(
        &mut conn,
        "SELECT DATABASE()",
    )
    .await?
    .flatten();
    current.ok_or_else(|| anyhow::anyhow!("No MySQL database selected; pass --database"))
}

/// Run a MySQL watermark snapshot+stream full sync, emitting the handoff
/// position as a checkpoint (when checkpoint storage is configured) so a later
/// `incremental` run can resume from the consistent end position.
async fn mysql_snapshot_full<S, St>(
    sink: &S,
    connection_string: String,
    database: Option<String>,
    chunk_size: usize,
    manager: Option<&SyncManager<St>>,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&connection_string)?;
    let database = resolve_mysql_database(&pool, &database).await?;
    let config = SnapshotStreamConfig { chunk_size };
    let mut checkpointer = NoopCheckpointer;
    let final_seq = surreal_sync_mysql_trigger_source::run_mysql_snapshot_stream(
        pool,
        database,
        sink,
        &config,
        &mut checkpointer,
    )
    .await?;

    if let Some(manager) = manager {
        let checkpoint = MySQLCheckpoint {
            sequence_id: final_seq,
            timestamp: chrono::Utc::now(),
        };
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
    }
    tracing::info!("Watermark snapshot full sync completed (final sequence_id: {final_seq})");
    Ok(())
}

async fn run_full_snapshot_stream_v2(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot-stream full sync from MySQL to SurrealDB (SDK v2)");
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            mysql_snapshot_full(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                Some(&manager),
            )
            .await
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager =
                SyncManager::new(checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone()));
            mysql_snapshot_full(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                Some(&manager),
            )
            .await
        }
        (None, None) => {
            mysql_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_full_snapshot_stream_v3(args: MySQLFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot-stream full sync from MySQL to SurrealDB (SDK v3)");
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            mysql_snapshot_full(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                Some(&manager),
            )
            .await
        }
        (None, Some(table)) => {
            let manager =
                SyncManager::new(checkpoint_surreal3::Surreal3Store::new(surreal, table.clone()));
            mysql_snapshot_full(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                Some(&manager),
            )
            .await
        }
        (None, None) => {
            mysql_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                args.connection_string,
                args.database,
                args.chunk_size,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

/// Run the combined `from mysql sync` orchestrator: a watermark snapshot+stream
/// full sync followed by incremental from the handed-off position, in one
/// process.
pub async fn run_sync(args: MySQLSyncArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;
    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args).await,
        SdkVersion::V3 => run_sync_v3(args).await,
    }
}

async fn run_sync_v2(args: MySQLSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot+stream sync from MySQL to SurrealDB (SDK v2)");
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);
    mysql_orchestrate(&sink, args).await
}

async fn run_sync_v3(args: MySQLSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot+stream sync from MySQL to SurrealDB (SDK v3)");
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);
    mysql_orchestrate(&sink, args).await
}

async fn mysql_orchestrate<S: SurrealSink>(sink: &S, args: MySQLSyncArgs) -> anyhow::Result<()> {
    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&args.connection_string)?;
    let database = resolve_mysql_database(&pool, &args.database).await?;
    let config = SnapshotStreamConfig {
        chunk_size: args.chunk_size,
    };

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string.clone(),
        source_database: Some(database.clone()),
        tables: args.tables.clone(),
        mysql_boolean_paths: args.boolean_paths.clone(),
    };

    let snapshot_pool = pool.clone();
    let snapshot_db = database.clone();
    orchestrate_snapshot_then_incremental(
        async move {
            let mut checkpointer = NoopCheckpointer;
            surreal_sync_mysql_trigger_source::run_mysql_snapshot_stream(
                snapshot_pool,
                snapshot_db,
                sink,
                &config,
                &mut checkpointer,
            )
            .await
        },
        |sequence_id| MySQLCheckpoint {
            sequence_id,
            timestamp: chrono::Utc::now(),
        },
        |from_checkpoint| {
            surreal_sync_mysql_trigger_source::run_incremental_sync(
                sink,
                source_opts,
                from_checkpoint,
                deadline,
                None,
            )
        },
    )
    .await
}

/// Emit an ad-hoc `execute-snapshot` signal so a running `sync` snapshots the
/// requested tables.
pub async fn run_snapshot_signal(args: MySQLSnapshotArgs) -> anyhow::Result<()> {
    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&args.connection_string)?;
    let database = resolve_mysql_database(&pool, &args.database).await?;
    surreal_sync_mysql_trigger_source::request_snapshot(pool, database, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}

async fn run_incremental_v3(args: MySQLIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL to SurrealDB (SDK v3)");
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
            tracing::info!("Starting from checkpoint: {}", s);
            // Explicit checkpoint from CLI
            surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s)?
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
                .read_checkpoint::<surreal_sync_mysql_trigger_source::MySQLCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let mysql_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mysql_trigger_source::MySQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: args.database,
        tables: args.tables,
        mysql_boolean_paths: args.boolean_paths,
    };

    // Connect to SurrealDB using v3 SDK
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_mysql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        mysql_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
