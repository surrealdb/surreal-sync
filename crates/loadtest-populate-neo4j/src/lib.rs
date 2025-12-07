//! Neo4j populator for surreal-sync loadtests.
//!
//! This crate provides a populator that can generate and insert test data into
//! Neo4j for load testing. It uses the schema-based generation system to create
//! deterministic test data.
//!
//! # Overview
//!
//! In Neo4j, "tables" become node labels. Each row generated becomes a node with
//! properties matching the schema fields.
//!
//! # Example
//!
//! ```ignore
//! use loadtest_populate_neo4j::Neo4jPopulator;
//! use sync_core::Schema;
//!
//! // Load schema from YAML
//! let schema = Schema::from_file("schema.yaml")?;
//!
//! // Create populator with deterministic seed
//! let mut populator = Neo4jPopulator::new(
//!     "bolt://localhost:7687",
//!     "neo4j",
//!     "password",
//!     "neo4j",
//!     schema,
//!     42, // seed
//! ).await?;
//!
//! // Delete existing nodes and populate
//! populator.delete_nodes("users").await?;
//! let metrics = populator.populate("users", 1000).await?;
//!
//! println!("Inserted {} nodes in {:?}", metrics.rows_inserted, metrics.total_duration);
//! ```

mod error;
mod insert;
mod populator;

pub use error::Neo4jPopulatorError;
pub use insert::DEFAULT_BATCH_SIZE;
pub use populator::{Neo4jPopulator, PopulateMetrics};
