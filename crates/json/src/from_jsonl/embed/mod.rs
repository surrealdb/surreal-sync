//! JSONL → SurrealDB import — embeddable entrypoints.
//!
//! # Documented embed path
//!
//! ```ignore
//! use surreal_sync_json::{run, FlattenId, InPlaceTransform, Value};
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
//! JSONL is a one-shot import — there are no checkpoint flags.

mod args;

pub use args::Args;

use anyhow::Context;
use clap::Parser;
use surreal_sync_core::{Schema, SurrealSink};
use surreal_sync_runtime::ApplyOpts;
use surreal_sync_runtime::{init, load_transforms_from_args, merge_inplace_boxed, SinkConnect};

use super::{sync_with_transforms, Config};

// Re-exports that form the public embed surface (also re-exported from
// `from_jsonl` and the crate root).
pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SurrealConfig;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

fn build_config(args: &Args) -> anyhow::Result<Config> {
    let schema = if let Some(schema_path) = &args.schema_file {
        Some(
            Schema::from_file(schema_path)
                .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?
                .to_database_schema(),
        )
    } else {
        None
    };

    Ok(Config {
        sources: vec![],
        files: vec![args.path.clone().into()],
        s3_uris: vec![],
        http_uris: vec![],
        id_field: args.id_field.clone(),
        id_columns: args.id_columns.clone(),
        conversion_rules: args.conversion_rules.clone(),
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema,
    })
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
    tracing::info!("Starting JSONL import");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let config = build_config(args)?;
    sync_with_transforms(sink, config, pipeline, apply_opts).await?;

    tracing::info!("JSONL import completed successfully");
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

/// Top-level clap root for source-shaped argv (same flags as `from jsonl`).
#[derive(Parser)]
#[command(
    name = "surreal-sync-jsonl",
    about = "Import from JSONL into SurrealDB (same flags as `surreal-sync from jsonl`)"
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
