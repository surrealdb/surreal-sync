//! Snowflake → SurrealDB import — embeddable entrypoints.
//!
//! # Documented embed path
//!
//! ```ignore
//! use surreal_sync_snowflake::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Links only SurrealDB v3 — do not use CLI auto-detect here
//!     run::<Surreal3Sink>([
//!         Box::new(FlattenId::default()),
//!     ]).await
//! }
//! ```
//!
//! Use [`run`] for the usual path. Lower-level helpers stay internal to the CLI.
//! Snowflake is a one-shot import — there are no checkpoint flags.

mod args;

pub use args::Args;

use anyhow::Context;
use clap::Parser;
use surreal_sync_core::SurrealSink;
use surreal_sync_runtime::ApplyOpts;
use surreal_sync_runtime::{init, load_transforms_from_args, merge_inplace_boxed, SinkConnect};

use super::client::SnowflakeClient;
use super::full_sync::run_full_sync_with_transforms;
use super::{SourceOpts, SyncOpts};

// Re-exports that form the public embed surface (also re-exported from
// `from_snowflake` and the crate root).
pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SurrealConfig;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

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

fn surreal_config_from_args(args: &Args) -> SurrealConfig {
    args.surreal
        .to_config(args.to_namespace.clone(), args.to_database.clone())
}

/// Primary API: run with a connected sink (links only the SurrealDB version you choose).
pub async fn run_sync<S: SurrealSink>(
    args: &Args,
    sink: &S,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Snowflake ingestion");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let (source_opts, sync_opts) = build_opts(args)?;
    let client = SnowflakeClient::new(&source_opts)?;

    run_full_sync_with_transforms(
        &client,
        sink,
        &source_opts,
        &sync_opts,
        pipeline,
        apply_opts,
    )
    .await?;

    tracing::info!("Snowflake ingestion completed successfully");
    Ok(())
}

/// Load optional `--transforms-config`, append Rust in-place stages, connect
/// one sink type, then import.
pub async fn run_with_extra_transforms<S: SinkConnect>(
    args: Args,
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = merge_inplace_boxed(args.transforms_config.as_deref(), extra)?;
    let config = surreal_config_from_args(&args);
    let sink = S::connect(&config).await?;
    run_sync(&args, &sink, &pipeline, &apply_opts).await
}

/// Run with parsed [`Args`] and only TOML transforms (stock binary path when
/// the sink is already chosen by the CLI).
pub async fn run_args_with_sink<S: SurrealSink>(args: Args, sink: &S) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    run_sync(&args, sink, &pipeline, &apply_opts).await
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

/// Parses CLI args and runs with one sink type (e.g. `Surreal3Sink`). Does not
/// auto-detect SurrealDB major version.
pub async fn run<S: SinkConnect>(
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    init();
    let cli = EmbedCli::parse();
    run_with_extra_transforms::<S>(cli.args, extra).await
}
