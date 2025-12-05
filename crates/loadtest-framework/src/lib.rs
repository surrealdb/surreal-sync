//! Load testing framework for surreal-sync.
//!
//! This crate provides a complete pipeline for load testing surreal-sync:
//! 1. Populate source databases with deterministic test data
//! 2. Run surreal-sync to sync data to SurrealDB
//! 3. Verify the synced data matches expected values
//!
//! # Example
//!
//! ```ignore
//! use loadtest_framework::{LoadTestConfig, LoadTestPipeline, SourceType};
//! use sync_core::SyncSchema;
//!
//! let schema = SyncSchema::from_file("schema.yaml")?;
//! let config = LoadTestConfig::new(schema, 42)
//!     .with_source(SourceType::PostgreSQL { connection_string: "...".into() })
//!     .with_row_count(10_000);
//!
//! let pipeline = LoadTestPipeline::new(config);
//! let report = pipeline.run().await?;
//! ```

pub mod config;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod report;
pub mod source;

pub use config::{LoadTestConfig, SourceConfig, SurrealConfig};
pub use error::LoadTestError;
pub use metrics::PipelineMetrics;
pub use pipeline::LoadTestPipeline;
pub use report::LoadTestReport;
pub use source::SourceType;
