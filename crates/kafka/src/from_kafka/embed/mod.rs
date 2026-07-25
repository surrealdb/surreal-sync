//! Kafka → SurrealDB sync — embeddable entrypoints.
//!
//! # Documented embed path
//!
//! ```ignore
//! use surreal_sync_kafka::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     run::<Surreal3Sink>([
//!         Box::new(FlattenId::default()),
//!     ]).await
//! }
//! ```
//!
//! Use [`run`] for the usual path. Lower-level helpers stay internal to the CLI.
//! Progress uses Kafka consumer-group offsets — there are no Surreal checkpoint flags.

mod args;

pub use args::Args;

use anyhow::Context;
use clap::Parser;
use std::sync::Arc;
use surreal_sync_core::{Schema, SurrealSink};
use surreal_sync_runtime::ApplyOpts;
use surreal_sync_runtime::{
    init, load_transforms_from_args, merge_inplace_boxed, parse_duration_to_secs, SinkConnect,
};

use super::run_incremental_sync_with_transforms;

// Re-exports that form the public embed surface (also re-exported from
// `from_kafka` and the crate root).
pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SurrealConfig;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

fn surreal_config_from_args(args: &Args) -> SurrealConfig {
    args.surreal
        .to_config(args.to_namespace.clone(), args.to_database.clone())
}

fn table_schema_from_args(args: &Args) -> anyhow::Result<Option<surreal_sync_core::TableDefinition>> {
    let Some(schema_path) = &args.schema_file else {
        return Ok(None);
    };
    let schema = Schema::from_file(schema_path)
        .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
    let table_name = args
        .config
        .table_name
        .as_ref()
        .unwrap_or(&args.config.topic);
    Ok(schema
        .get_table(table_name)
        .map(|t| t.to_table_definition()))
}

/// Primary API: run with a connected sink (links only the SurrealDB version you choose).
pub async fn run_sync<S: SurrealSink + Send + Sync + 'static>(
    args: Args,
    sink: Arc<S>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Timeout: {}", args.timeout);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let timeout_secs = parse_duration_to_secs(&args.timeout)
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);
    tracing::info!("Will consume until deadline: {}", deadline);

    let table_schema = table_schema_from_args(&args)?;

    run_incremental_sync_with_transforms(
        sink,
        args.config,
        deadline,
        table_schema,
        pipeline,
        apply_opts,
    )
    .await?;

    Ok(())
}

/// Load optional `--transforms-config`, append Rust in-place stages, connect
/// one sink type, then sync.
pub async fn run_with_extra_transforms<S: SinkConnect + Send + Sync + 'static>(
    args: Args,
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = merge_inplace_boxed(args.transforms_config.as_deref(), extra)?;
    let config = surreal_config_from_args(&args);
    let sink = Arc::new(S::connect(&config).await?);
    run_sync(args, sink, &pipeline, &apply_opts).await
}

/// Run with parsed [`Args`] and only TOML transforms (stock binary path when
/// the sink is already chosen by the CLI).
pub async fn run_args_with_sink<S: SurrealSink + Send + Sync + 'static>(
    args: Args,
    sink: Arc<S>,
) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    run_sync(args, sink, &pipeline, &apply_opts).await
}

/// Top-level clap root for source-shaped argv (same flags as `from kafka`).
#[derive(Parser)]
#[command(
    name = "surreal-sync-kafka",
    about = "Sync from Kafka into SurrealDB (same flags as `surreal-sync from kafka`)"
)]
struct EmbedCli {
    #[command(flatten)]
    args: Args,
}

/// Parses CLI args and runs with one sink type (e.g. `Surreal3Sink`). Does not
/// auto-detect SurrealDB major version.
pub async fn run<S: SinkConnect + Send + Sync + 'static>(
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    init();
    let cli = EmbedCli::parse();
    run_with_extra_transforms::<S>(cli.args, extra).await
}
