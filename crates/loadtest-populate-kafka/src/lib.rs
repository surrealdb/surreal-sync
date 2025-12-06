//! Kafka populator for the surreal-sync load testing framework.
//!
//! This crate provides the `KafkaPopulator` which generates test data based on
//! a SyncSchema YAML definition, encodes it as protobuf messages, and publishes
//! to Kafka topics. The protobuf schema is generated dynamically from the YAML
//! schema, enabling schema-driven testing without maintaining separate .proto files.
//!
//! # Architecture
//!
//! ```text
//! SyncSchema (YAML)
//!        │
//!        ├──────────────────────────┐
//!        ▼                          ▼
//! ┌─────────────────┐     ┌─────────────────┐
//! │  DataGenerator  │     │  proto_gen.rs   │
//! │                 │     │                 │
//! │ - generates     │     │ - generates     │
//! │   InternalRow   │     │   .proto files  │
//! └────────┬────────┘     └────────┬────────┘
//!          │                       │
//!          │                       ▼
//!          │              ┌─────────────────┐
//!          │              │  Consumer uses  │
//!          │              │  .proto to      │
//!          │              │  decode msgs    │
//!          │              └─────────────────┘
//!          ▼
//!   ┌─────────────────┐
//!   │   encoder.rs    │
//!   │                 │
//!   │ - encodes rows  │
//!   │   to protobuf   │
//!   └────────┬────────┘
//!            │
//!            ▼
//!     Kafka Topic
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use loadtest_populate_kafka::KafkaPopulator;
//! use sync_core::SyncSchema;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Load schema from YAML
//!     let schema = SyncSchema::from_file("schema.yaml")?;
//!
//!     // Create populator
//!     let mut populator = KafkaPopulator::new("localhost:9092", schema, 42).await?;
//!
//!     // Prepare table (generates .proto file)
//!     let proto_path = populator.prepare_table("users")?;
//!     println!("Proto file at: {:?}", proto_path);
//!
//!     // Create topic if needed
//!     populator.create_topic("users").await?;
//!
//!     // Populate with 1000 messages
//!     let metrics = populator.populate("users", 1000).await?;
//!     println!("Published {} messages in {:?}", metrics.messages_published, metrics.total_duration);
//!
//!     Ok(())
//! }
//! ```

pub mod args;
pub mod encoder;
pub mod error;
pub mod populator;
pub mod proto_gen;

// Re-exports for convenience
pub use args::{CommonPopulateArgs, KafkaPopulateArgs};
pub use error::KafkaPopulatorError;
pub use populator::{KafkaPopulator, PopulateMetrics, DEFAULT_BATCH_SIZE};
pub use proto_gen::generate_proto_for_table;
