//! PostgreSQL WAL-based logical replication sync handlers.
//!
//! Source crate: crates/postgresql-wal2json-source/
//! CLI commands:
//! - Full sync: `from postgresql full --connection-string ... --slot ... --tables ... --to-namespace ... --to-database ...`
//! - Incremental sync: `from postgresql incremental --connection-string ... --slot ... --tables ... --checkpoints-surreal-table ...`
//!
//! Both commands support `-c` / `--config-file` to load settings from a TOML file:
//! - Full sync: `from postgresql full -c surreal-sync.toml`
//! - Incremental sync: `from postgresql incremental -c surreal-sync.toml`
//!
//! CLI flags take precedence over config file values when both are provided.

use anyhow::Context;
use checkpoint::Checkpoint;
use std::path::PathBuf;
use surreal_sync::SurrealOpts;

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::config::load_config;
use crate::{PostgreSQLLogicalFullArgs, PostgreSQLLogicalIncrementalArgs};
use surreal_sync_postgresql_wal2json_source::toml_config::{
    Wal2jsonFullSource, Wal2jsonIncrementalSource,
};

// ---- Resolved args (all required fields are non-optional) ----

struct ResolvedWal2jsonFullArgs {
    connection_string: String,
    slot: String,
    tables: Vec<String>,
    schema: String,
    to_namespace: String,
    to_database: String,
    schema_file: Option<PathBuf>,
    checkpoint_dir: Option<String>,
    checkpoints_surreal_table: Option<String>,
    surreal: SurrealOpts,
}

struct ResolvedWal2jsonIncrementalArgs {
    connection_string: String,
    slot: String,
    tables: Vec<String>,
    schema: String,
    to_namespace: String,
    to_database: String,
    schema_file: Option<PathBuf>,
    incremental_from: Option<String>,
    checkpoints_surreal_table: Option<String>,
    incremental_to: Option<String>,
    timeout: String,
    surreal: SurrealOpts,
}

fn resolve_full_args(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<ResolvedWal2jsonFullArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<Wal2jsonFullSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedWal2jsonFullArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            slot: if args.slot != "surreal_sync_slot" { args.slot } else { pg.slot },
            tables: if args.tables.is_empty() { pg.tables } else { args.tables },
            schema: if args.schema != "public" { args.schema } else { pg.schema },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            schema_file: args.schema_file.or(pg.schema_file),
            checkpoint_dir: args.checkpoint_dir.or(pg.checkpoint_dir),
            checkpoints_surreal_table: args.checkpoints_surreal_table.or(pg.checkpoints_surreal_table),
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
            },
        })
    } else {
        Ok(ResolvedWal2jsonFullArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            slot: args.slot,
            tables: args.tables,
            schema: args.schema,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            schema_file: args.schema_file,
            checkpoint_dir: args.checkpoint_dir,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            surreal: args.surreal,
        })
    }
}

fn resolve_incremental_args(
    args: PostgreSQLLogicalIncrementalArgs,
) -> anyhow::Result<ResolvedWal2jsonIncrementalArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<Wal2jsonIncrementalSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedWal2jsonIncrementalArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            slot: if args.slot != "surreal_sync_slot" { args.slot } else { pg.slot },
            tables: if args.tables.is_empty() { pg.tables } else { args.tables },
            schema: if args.schema != "public" { args.schema } else { pg.schema },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            schema_file: args.schema_file.or(pg.schema_file),
            incremental_from: args.incremental_from.or(pg.incremental_from),
            checkpoints_surreal_table: args.checkpoints_surreal_table.or(pg.checkpoints_surreal_table),
            incremental_to: args.incremental_to.or(pg.incremental_to),
            timeout: if args.timeout != "3600" {
                args.timeout
            } else {
                pg.timeout.to_string()
            },
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
            },
        })
    } else {
        Ok(ResolvedWal2jsonIncrementalArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            slot: args.slot,
            tables: args.tables,
            schema: args.schema,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            schema_file: args.schema_file,
            incremental_from: args.incremental_from,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            incremental_to: args.incremental_to,
            timeout: args.timeout,
            surreal: args.surreal,
        })
    }
}

// ---- Public entry points ----

/// Run PostgreSQL WAL-based full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    let args = resolve_full_args(args)?;
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

async fn run_full_v2(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
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

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
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
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

async fn run_full_v3(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
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

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
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
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

/// Run PostgreSQL WAL-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLLogicalIncrementalArgs) -> anyhow::Result<()> {
    let args = resolve_incremental_args(args)?;
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

async fn run_incremental_v2(args: ResolvedWal2jsonIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
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
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
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
        .as_ref()
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(s)
        })
        .transpose()?;

    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
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

async fn run_incremental_v3(args: ResolvedWal2jsonIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
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
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(
                s,
            )?
        }
        (None, Some(table)) => {
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
        .as_ref()
        .map(|s| {
            surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint::from_cli_string(s)
        })
        .transpose()?;

    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
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
