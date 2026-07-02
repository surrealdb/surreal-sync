//! MySQL/MariaDB binlog CDC sync handlers.
//!
//! Source crate: crates/mysql-binlog-source/
//! CLI commands:
//! - Full sync: `from mysql-binlog full --connection-string ... --tables ...`
//! - Incremental sync: `from mysql-binlog incremental --incremental-from mysql-binlog:...`
//! - Combined snapshot+stream: `from mysql-binlog sync ...`
//! - Ad-hoc snapshot signal: `from mysql-binlog snapshot ...`

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync::orchestrate_snapshot_then_incremental;
use surreal_sync_interleaved_snapshot::{InterleavedSnapshotConfig, NoopCheckpointer};
use surreal_sync_mysql_binlog_source::{
    request_snapshot, run_full_sync, run_incremental_sync, run_interleaved_snapshot_full_sync,
    BinlogCheckpoint, SourceOpts, SyncOpts,
};

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::{
    MySQLBinlogFlavorArg, MySQLBinlogFullArgs, MySQLBinlogIncrementalArgs, MySQLBinlogSnapshotArgs,
    MySQLBinlogSyncArgs, SyncStrategy,
};

fn binlog_source_opts(args: &MySQLBinlogFullArgs) -> SourceOpts {
    binlog_source_opts_from(
        args.connection_string.clone(),
        args.database.clone(),
        args.tables.clone(),
        args.boolean_paths.clone(),
        args.server_id,
        args.flavor,
    )
}

fn binlog_source_opts_from(
    connection_string: String,
    database: Option<String>,
    tables: Vec<String>,
    boolean_paths: Option<Vec<String>>,
    server_id: Option<u32>,
    flavor: Option<MySQLBinlogFlavorArg>,
) -> SourceOpts {
    SourceOpts {
        connection_string,
        database,
        tables,
        server_id,
        flavor: flavor.map(Into::into),
        mysql_boolean_paths: boolean_paths,
    }
}

fn binlog_sync_opts(batch_size: usize, dry_run: bool) -> SyncOpts {
    SyncOpts {
        batch_size,
        dry_run,
    }
}

/// Run MySQL binlog full sync, dispatching by strategy then SDK version.
pub async fn run_full(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match (args.strategy, sdk_version) {
        (SyncStrategy::SequentialSnapshot, SdkVersion::V2) => run_full_v2(args).await,
        (SyncStrategy::SequentialSnapshot, SdkVersion::V3) => run_full_v3(args).await,
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V2) => {
            run_full_interleaved_snapshot_v2(args).await
        }
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V3) => {
            run_full_interleaved_snapshot_v3(args).await
        }
    }
}

async fn run_full_v2(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL binlog to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

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

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
        (None, None) => {
            run_full_sync::<_, checkpoint::NullStore>(&sink, &source_opts, &sync_opts, None)
                .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL binlog to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

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

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
        (None, Some(table)) => {
            let store = checkpoint_surreal3::Surreal3Store::new(surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
        (None, None) => {
            run_full_sync::<_, checkpoint::NullStore>(&sink, &source_opts, &sync_opts, None)
                .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run MySQL binlog incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: MySQLBinlogIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: MySQLBinlogIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL binlog to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            BinlogCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<BinlogCheckpoint>(checkpoint::SyncPhase::FullSyncStart)
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

    let to_checkpoint = args
        .incremental_to
        .as_ref()
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = binlog_source_opts_from(
        args.connection_string,
        args.database,
        args.tables,
        args.boolean_paths,
        args.server_id,
        args.flavor,
    );

    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    run_incremental_sync(&sink, source_opts, from_checkpoint, deadline, to_checkpoint).await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: MySQLBinlogIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL binlog to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            BinlogCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<BinlogCheckpoint>(checkpoint::SyncPhase::FullSyncStart)
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

    let to_checkpoint = args
        .incremental_to
        .as_ref()
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = binlog_source_opts_from(
        args.connection_string,
        args.database,
        args.tables,
        args.boolean_paths,
        args.server_id,
        args.flavor,
    );

    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    run_incremental_sync(&sink, source_opts, from_checkpoint, deadline, to_checkpoint).await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// Interleaved snapshot strategy
// =============================================================================

async fn resolve_mysql_database(
    pool: &mysql_async::Pool,
    explicit: &Option<String>,
) -> anyhow::Result<String> {
    if let Some(db) = explicit {
        return Ok(db.clone());
    }
    let mut conn = pool.get_conn().await?;
    let current: Option<String> =
        mysql_async::prelude::Queryable::query_first(&mut conn, "SELECT DATABASE()")
            .await?
            .flatten();
    current.ok_or_else(|| anyhow::anyhow!("No MySQL database selected; pass --database"))
}

async fn binlog_snapshot_full<S, St>(
    sink: &S,
    source_opts: &SourceOpts,
    chunk_size: usize,
    manager: Option<&SyncManager<St>>,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let config = InterleavedSnapshotConfig { chunk_size };
    let mut checkpointer = NoopCheckpointer;
    let checkpoint =
        run_interleaved_snapshot_full_sync(sink, source_opts, config, &mut checkpointer).await?;

    if let Some(manager) = manager {
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
    }
    tracing::info!(
        "Binlog watermark snapshot full sync completed (final checkpoint: {})",
        checkpoint.to_cli_string()
    );
    Ok(())
}

async fn run_full_interleaved_snapshot_v2(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from MySQL binlog to SurrealDB (SDK v2)"
    );
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
    let source_opts = binlog_source_opts(&args);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager = SyncManager::new(checkpoint::Surreal2Store::new(
                checkpoint_surreal,
                table.clone(),
            ));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, None) => {
            binlog_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
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

async fn run_full_interleaved_snapshot_v3(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from MySQL binlog to SurrealDB (SDK v3)"
    );
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
    let source_opts = binlog_source_opts(&args);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(
                surreal,
                table.clone(),
            ));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, None) => {
            binlog_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
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

/// Run the combined `from mysql-binlog sync` orchestrator.
pub async fn run_sync(args: MySQLBinlogSyncArgs) -> anyhow::Result<()> {
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

async fn run_sync_v2(args: MySQLBinlogSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting interleaved snapshot sync from MySQL binlog to SurrealDB (SDK v2)");
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
    binlog_orchestrate(&sink, args).await
}

async fn run_sync_v3(args: MySQLBinlogSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting interleaved snapshot sync from MySQL binlog to SurrealDB (SDK v3)");
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
    binlog_orchestrate(&sink, args).await
}

async fn binlog_orchestrate<S: SurrealSink>(
    sink: &S,
    args: MySQLBinlogSyncArgs,
) -> anyhow::Result<()> {
    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let flavor = args.flavor;
    let server_id = args.server_id;
    let snapshot_opts = binlog_source_opts_from(
        args.connection_string.clone(),
        args.database.clone(),
        args.tables.clone(),
        args.boolean_paths.clone(),
        server_id,
        flavor,
    );
    let incremental_opts = binlog_source_opts_from(
        args.connection_string,
        args.database,
        args.tables,
        args.boolean_paths,
        server_id,
        flavor,
    );
    let chunk_size = args.chunk_size;
    let config = InterleavedSnapshotConfig { chunk_size };

    orchestrate_snapshot_then_incremental(
        async move {
            let mut checkpointer = NoopCheckpointer;
            run_interleaved_snapshot_full_sync(sink, &snapshot_opts, config, &mut checkpointer)
                .await
        },
        |checkpoint| checkpoint,
        |from_checkpoint| {
            run_incremental_sync(sink, incremental_opts, from_checkpoint, deadline, None)
        },
    )
    .await
}

/// Emit an ad-hoc execute-snapshot signal so a running `sync` snapshots the
/// requested tables.
pub async fn run_snapshot_signal(args: MySQLBinlogSnapshotArgs) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::from_url(&args.connection_string)?;
    let database = resolve_mysql_database(&pool, &args.database).await?;
    request_snapshot(&pool, &database, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}
