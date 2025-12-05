//! CSV file populator for load testing.
//!
//! This crate provides functionality to generate CSV files with test data
//! using the loadtest-generator crate.
//!
//! # Example
//!
//! ```ignore
//! use loadtest_csv::CSVPopulator;
//! use sync_core::SyncSchema;
//!
//! let schema = SyncSchema::from_yaml("path/to/schema.yaml")?;
//! let mut populator = CSVPopulator::new(schema, 42);
//!
//! // Generate CSV file with 1000 rows
//! let metrics = populator.populate("users", "/path/to/output.csv", 1000).await?;
//! ```

pub mod args;
mod error;
mod populator;

pub use args::{CSVPopulateArgs, CommonPopulateArgs};
pub use error::CSVPopulatorError;
pub use populator::{CSVPopulator, PopulateMetrics};
