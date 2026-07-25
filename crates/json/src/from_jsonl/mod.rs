//! JSONL import for SurrealDB
//!
//! This module provides functionality for importing JSONL (JSON Lines) files into SurrealDB.
//! It supports conversion rules that transform JSON objects into SurrealDB Thing references.
//!
//! # Embed surface
//!
//! Public embed API is only [`run`], [`FlattenId`], [`InPlaceTransform`], and
//! [`Value`]:
//!
//! ```ignore
//! use surreal_sync_json::from_jsonl::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>]).await?;
//! ```

pub mod conversion;
pub(crate) mod embed;
mod sync;

pub use conversion::ConversionRule;
pub use sync::{sync, sync_with_transforms, Config, SourceOpts};

// Re-export file source types for convenience
pub use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};

/// Public embed surface: `run`, `FlattenId`, `InPlaceTransform`, `Value` only.
pub use embed::{run, FlattenId, InPlaceTransform, Value};

/// Stock CLI argv helpers (`Args`, `run_args_with_sink`). Not part of the embed API.
#[doc(hidden)]
pub mod cli {
    pub use super::embed::{run_args_with_sink, Args};
}
