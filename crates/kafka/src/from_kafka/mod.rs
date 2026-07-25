//! Kafka consumer and sync library for surreal-sync.
//!
//! This crate provides:
//! - Kafka consumer with protobuf decoding
//! - Incremental sync to SurrealDB
//!
//! # Embed surface
//!
//! Public embed API is only [`run`], [`FlattenId`], [`InPlaceTransform`], and
//! [`Value`]:
//!
//! ```ignore
//! use surreal_sync_kafka::from_kafka::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>]).await?;
//! ```
//!
//! Types come from [`crate::types`]; this module adds decoding and consumer logic.

/// High-level API for spawning consumer tasks
///
/// Takes the consumer config and .proto schema, to create one or more consumers
/// in the same consumer group, each running in its own async task.
pub mod client;

/// Low-level consumer with peek buffer and manual offsets
///
/// Created by the client given the consumer config and .proto schema.
pub mod consumer;
pub(crate) mod embed;
pub mod error;
pub mod proto;
pub mod sync;

// Re-export types for convenience
pub use crate::types::{
    Message, Payload, ProtoFieldDescriptor, ProtoFieldValue, ProtoMessage, ProtoMessageDescriptor,
    ProtoSchema, ProtoType,
};

// Re-export sync functions
pub use sync::{run_incremental_sync, run_incremental_sync_with_transforms, Config};

// Re-export consumer types
pub use client::Client;
pub use consumer::{Consumer, ConsumerConfig, SaslMechanism, SecurityProtocol};
pub use error::{Error, Result};
pub use proto::decoder::ProtoDecoder;
pub use proto::parser::ProtoParser;

/// Public embed surface: `run`, `FlattenId`, `InPlaceTransform`, `Value` only.
pub use embed::{run, FlattenId, InPlaceTransform, Value};

/// Stock CLI argv helpers (`Args`, `run_args_with_sink`). Not part of the embed API.
#[doc(hidden)]
pub mod cli {
    pub use super::embed::{run_args_with_sink, Args};
}
