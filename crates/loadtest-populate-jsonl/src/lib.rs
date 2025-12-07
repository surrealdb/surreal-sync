//! JSONL (JSON Lines) populator for load testing.
//!
//! This crate provides functionality to generate JSONL files with deterministic
//! test data for use in surreal-sync load testing scenarios.
//!
//! # Example
//!
//! ```ignore
//! use loadtest_jsonl::JsonlPopulator;
//! use sync_core::Schema;
//!
//! let schema = Schema::from_file("schema.yaml")?;
//! let mut populator = JsonlPopulator::new(schema, 42);
//!
//! let metrics = populator.populate("users", "output.jsonl", 1000)?;
//! println!("Generated {} rows in {:?}", metrics.rows_written, metrics.total_duration);
//! ```

pub mod args;
pub mod error;
pub mod populator;

pub use args::{CommonPopulateArgs, JSONLPopulateArgs};
pub use error::JsonlPopulatorError;
pub use populator::{JsonlPopulator, PopulateMetrics};
