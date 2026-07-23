//! Snowflake ingestion handler.
//!
//! Source crate: crates/snowflake-source/
//! CLI command (ingestion-only, no full/incremental split):
//! - `from snowflake --account ... --user ... --private-key-path ... \
//!    --warehouse ... --database ... --schema ... [--tables ...] [--id-columns ...] \
//!    [--transforms-config ...] --to-namespace ... --to-database ...`

use anyhow::Context;
use surreal_sync_snowflake_source::{
    run_full_sync_with_transforms, SnowflakeClient, SourceOpts, SyncOpts,
};

use super::transforms::load_transforms_from_args;
use super::{get_sdk_version, make_surreal2_sink, make_surreal3_sink, SdkVersion};
use crate::SnowflakeArgs;

/// Run Snowflake ingestion, dispatching to the detected SurrealDB SDK version.
pub async fn run(args: SnowflakeArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_v2(args).await,
        SdkVersion::V3 => run_v3(args).await,
    }
}

/// Read the private key PEM and assemble the source/sync options shared by both
/// SDK paths.
fn build_opts(args: &SnowflakeArgs) -> anyhow::Result<(SourceOpts, SyncOpts)> {
    let private_key_pem = std::fs::read_to_string(&args.private_key_path).with_context(|| {
        format!(
            "failed to read private key from {}",
            args.private_key_path.display()
        )
    })?;

    let source_opts = SourceOpts {
        account: args.account.clone(),
        user: args.user.clone(),
        private_key_pem,
        private_key_passphrase: args.private_key_passphrase.clone(),
        warehouse: args.warehouse.clone(),
        database: args.database.clone(),
        schema: args.schema.clone(),
        role: args.role.clone(),
        tables: args.tables.clone(),
        id_columns: args.id_columns.clone(),
    };

    let sync_opts = SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    Ok((source_opts, sync_opts))
}

async fn run_v2(args: SnowflakeArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Snowflake ingestion (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let (source_opts, sync_opts) = build_opts(&args)?;
    let client = SnowflakeClient::new(&source_opts)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);

    run_full_sync_with_transforms(
        &client,
        &sink,
        &source_opts,
        &sync_opts,
        &pipeline,
        &apply_opts,
    )
    .await?;

    tracing::info!("Snowflake ingestion completed successfully");
    Ok(())
}

async fn run_v3(args: SnowflakeArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Snowflake ingestion (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let (source_opts, sync_opts) = build_opts(&args)?;
    let client = SnowflakeClient::new(&source_opts)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = make_surreal3_sink(surreal, args.surreal.zero_temporal);

    run_full_sync_with_transforms(
        &client,
        &sink,
        &source_opts,
        &sync_opts,
        &pipeline,
        &apply_opts,
    )
    .await?;

    tracing::info!("Snowflake ingestion completed successfully");
    Ok(())
}
