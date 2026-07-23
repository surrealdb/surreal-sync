//! Snowflake → SurrealDB import — library entrypoint for the stock CLI and embedders.
//!
//! # Embedder usage
//!
//! Parse the same flags as `surreal-sync from snowflake` (without that prefix)
//! and append in-process [`InPlaceTransform`](sync_transform::InPlaceTransform)
//! stages:
//!
//! ```ignore
//! surreal_sync::snowflake::run([FlattenId::default(), /* … */]).await?;
//! ```
//!
//! See `examples/snowflake_custom_transform.rs`.

mod args;

pub use args::Args;

use anyhow::Context;
use clap::Parser;
use surreal_sync_snowflake_source::{
    run_full_sync_with_transforms, SnowflakeClient, SourceOpts, SyncOpts,
};
use sync_transform::{ApplyOpts, InPlaceTransform, Pipeline};

use crate::sync_helpers::{get_sdk_version, make_surreal2_sink, make_surreal3_sink, SdkVersion};
use crate::transforms::{load_transforms_from_args, merge_inplace_boxed};

fn build_opts(args: &Args) -> anyhow::Result<(SourceOpts, SyncOpts)> {
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

/// Run a Snowflake import with an already-built transform pipeline.
///
/// Shared by the stock binary and embedders. Call [`crate::init`] first if you
/// use this directly (not needed when calling [`run`]).
pub async fn run_sync(
    args: Args,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args, pipeline, apply_opts).await,
        SdkVersion::V3 => run_sync_v3(args, pipeline, apply_opts).await,
    }
}

/// Load optional `--transforms-config`, append Rust in-place stages, then import.
pub async fn run_with_extra_transforms(
    args: Args,
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = merge_inplace_boxed(args.transforms_config.as_deref(), extra)?;
    run_sync(args, pipeline, apply_opts).await
}

/// Run with parsed [`Args`] and only TOML transforms (stock binary path).
pub async fn run_args(args: Args) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    run_sync(args, pipeline, apply_opts).await
}

/// Top-level clap root for source-shaped argv (same flags as `from snowflake`).
#[derive(Parser)]
#[command(
    name = "surreal-sync-snowflake",
    about = "Import from Snowflake into SurrealDB (same flags as `surreal-sync from snowflake`)"
)]
struct EmbedCli {
    #[command(flatten)]
    args: Args,
}

/// Embedder entrypoint: initialize logging/TLS, parse argv, append in-place
/// stages, and run the same import as `surreal-sync from snowflake`.
///
/// Pass [`Box<dyn InPlaceTransform>`] values so stages of different concrete
/// types can be listed together (see the `snowflake_custom_transform` example).
pub async fn run(extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>) -> anyhow::Result<()> {
    crate::init();
    let cli = EmbedCli::parse();
    run_with_extra_transforms(cli.args, extra).await
}

async fn run_sync_v2(
    args: Args,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Snowflake ingestion (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

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

async fn run_sync_v3(
    args: Args,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Snowflake ingestion (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

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
