//! PostgreSQL trigger-based CDC sync handlers.
//!
//! Source crate: crates/postgresql-trigger-source/
//! CLI commands:
//! - Full sync: `from postgresql-trigger full --connection-string ... --tables ... --checkpoints-surreal-table ...`
//! - Incremental sync: `from postgresql-trigger incremental --connection-string ... --tables ... --checkpoints-surreal-table ...`
//!
//! Both commands support `-c` / `--config-file` to load settings from a TOML file:
//! - Full sync: `from postgresql-trigger full -c surreal-sync.toml`
//! - Incremental sync: `from postgresql-trigger incremental -c surreal-sync.toml`
//!
//! CLI flags take precedence over config file values when both are provided.

use anyhow::Context;
use checkpoint::Checkpoint;
use std::path::PathBuf;
use surreal_sync::SurrealOpts;

use super::{extract_postgresql_database, get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::config::load_config;
use crate::{PostgreSQLTriggerFullArgs, PostgreSQLTriggerIncrementalArgs};
use surreal_sync_postgresql_trigger_source::toml_config::{
    TriggerFullSource, TriggerIncrementalSource,
};

// ---- Resolved args (all required fields are non-optional) ----

struct ResolvedTriggerFullArgs {
    connection_string: String,
    tables: Vec<String>,
    to_namespace: String,
    to_database: String,
    checkpoint_dir: Option<String>,
    checkpoints_surreal_table: Option<String>,
    schema_file: Option<PathBuf>,
    surreal: SurrealOpts,
}

struct ResolvedTriggerIncrementalArgs {
    connection_string: String,
    tables: Vec<String>,
    to_namespace: String,
    to_database: String,
    incremental_from: Option<String>,
    checkpoints_surreal_table: Option<String>,
    incremental_to: Option<String>,
    timeout: String,
    schema_file: Option<PathBuf>,
    surreal: SurrealOpts,
}

fn resolve_full_args(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<ResolvedTriggerFullArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<TriggerFullSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedTriggerFullArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            checkpoint_dir: args.checkpoint_dir.or(pg.checkpoint_dir),
            checkpoints_surreal_table: args
                .checkpoints_surreal_table
                .or(pg.checkpoints_surreal_table),
            schema_file: args.schema_file.or(pg.schema_file),
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
        Ok(ResolvedTriggerFullArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            tables: args.tables,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            checkpoint_dir: args.checkpoint_dir,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            schema_file: args.schema_file,
            surreal: args.surreal,
        })
    }
}

fn resolve_incremental_args(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<ResolvedTriggerIncrementalArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<TriggerIncrementalSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedTriggerIncrementalArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            incremental_from: args.incremental_from.or(pg.incremental_from),
            checkpoints_surreal_table: args
                .checkpoints_surreal_table
                .or(pg.checkpoints_surreal_table),
            incremental_to: args.incremental_to.or(pg.incremental_to),
            timeout: if args.timeout != "3600" {
                args.timeout
            } else {
                pg.timeout.to_string()
            },
            schema_file: args.schema_file.or(pg.schema_file),
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
        Ok(ResolvedTriggerIncrementalArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            tables: args.tables,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            incremental_from: args.incremental_from,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            incremental_to: args.incremental_to,
            timeout: args.timeout,
            schema_file: args.schema_file,
            surreal: args.surreal,
        })
    }
}

// ---- Public entry points ----

/// Run PostgreSQL trigger-based full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
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

async fn run_full_v2(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)");
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

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
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
            surreal_sync_postgresql_trigger_source::run_full_sync(
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
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
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

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)");
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

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
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
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
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
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
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

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run PostgreSQL trigger-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLTriggerIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: ResolvedTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)"
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
            tracing::info!("Starting from checkpoint: {}", s);
            surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s)?
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
                .read_checkpoint::<surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint>(
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

    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: ResolvedTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)"
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
            tracing::info!("Starting from checkpoint: {}", s);
            surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s)?
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
                .read_checkpoint::<surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint>(
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

    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
