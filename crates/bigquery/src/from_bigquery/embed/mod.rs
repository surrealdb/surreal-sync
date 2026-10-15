//! BigQuery → SurrealDB import — embeddable entrypoints.
//!
//! # Documented embed path
//!
//! ```ignore
//! use surreal_sync_bigquery::{run, FlattenId, InPlaceTransform, Value};
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
//! BigQuery is a one-shot import — there are no checkpoint flags.

mod args;

pub use args::Args;

use anyhow::Context;
use clap::Parser;
use surreal_sync_core::SurrealSink;
use surreal_sync_runtime::ApplyOpts;
use surreal_sync_runtime::{init, load_transforms_from_args, merge_inplace_boxed, SinkConnect};

use super::client::BigQueryClient;
use super::full_sync::run_full_sync_with_transforms;
use super::{SourceOpts, SyncOpts};

// Re-exports that form the public embed surface (also re-exported from
// `from_bigquery` and the crate root).
pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SurrealConfig;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

fn build_opts(args: &Args) -> anyhow::Result<(SourceOpts, SyncOpts)> {
    let credentials_json = match &args.credentials_path {
        Some(path) => Some(std::fs::read_to_string(path).with_context(|| {
            format!("failed to read service-account key from {}", path.display())
        })?),
        None => None,
    };

    let source_opts = SourceOpts {
        project_id: args.project_id.clone(),
        dataset: args.dataset.clone(),
        job_project_id: args.job_project_id.clone(),
        credentials_json,
        location: args.location.clone(),
        api_endpoint: args.api_endpoint.clone(),
        tables: args.tables.clone(),
        id_columns: args.id_columns.clone(),
        page_size: args.page_size,
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

/// Validate the BigQuery half of the configuration without touching the network.
///
/// Called before the SurrealDB sink is connected so a missing credentials file or
/// an unreadable service-account key is reported immediately, rather than after a
/// connection attempt against an unrelated service.
pub fn preflight(args: &Args) -> anyhow::Result<()> {
    let (source_opts, _) = build_opts(args)?;
    BigQueryClient::new(&source_opts)?;
    Ok(())
}

/// Primary API: run with a connected sink (links only the SurrealDB version you choose).
pub async fn run_sync<S: SurrealSink>(
    args: &Args,
    sink: &S,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting BigQuery ingestion");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let (source_opts, sync_opts) = build_opts(args)?;
    let client = BigQueryClient::new(&source_opts)?;

    run_full_sync_with_transforms(
        &client,
        sink,
        &source_opts,
        &sync_opts,
        pipeline,
        apply_opts,
    )
    .await?;

    tracing::info!("BigQuery ingestion completed successfully");
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

/// Top-level clap root for source-shaped argv (same flags as `from bigquery`).
#[derive(Parser)]
#[command(
    name = "surreal-sync-bigquery",
    about = "Import from Google BigQuery into SurrealDB (same flags as `surreal-sync from bigquery`)"
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
