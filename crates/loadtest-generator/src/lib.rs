//! Data generator for the surreal-sync load testing framework.
//!
//! This crate provides the `DataGenerator` which produces deterministic test data
//! based on a YAML schema. The generator uses a seeded RNG to ensure reproducibility
//! across runs with the same seed.
//!
//! # Architecture
//!
//! ```text
//! SyncSchema (YAML)
//!        │
//!        ▼
//! ┌─────────────────┐
//! │  DataGenerator  │
//! │                 │
//! │  - seed         │
//! │  - rng (StdRng) │
//! │  - index        │
//! └────────┬────────┘
//!          │
//!          ▼
//!    InternalRow { table, index, id, fields }
//! ```
//!
//! # Example
//!
//! ```rust
//! use loadtest_generator::DataGenerator;
//! use sync_core::SyncSchema;
//!
//! let schema = SyncSchema::from_yaml(r#"
//! version: 1
//! seed: 42
//! tables:
//!   - name: users
//!     id:
//!       type: uuid
//!       generator:
//!         type: uuid_v4
//!     fields:
//!       - name: email
//!         type: text
//!         generator:
//!           type: pattern
//!           pattern: "user_{index}@example.com"
//! "#).unwrap();
//!
//! let mut generator = DataGenerator::new(schema, 42);
//! let row = generator.next_internal_row("users").unwrap();
//! println!("Generated row: {:?}", row);
//! ```
//!
//! # Generators
//!
//! The following generator types are supported:
//!
//! - `uuid_v4` - Random UUID v4
//! - `sequential` - Sequential integers
//! - `pattern` - Pattern strings with placeholders (`{index}`, `{uuid}`, `{rand:N}`)
//! - `int_range` - Random integers in a range
//! - `float_range` - Random floats in a range
//! - `decimal_range` - Random decimals in a range
//! - `timestamp_range` - Random timestamps in a date range
//! - `weighted_bool` - Boolean with configurable true probability
//! - `one_of` - Random selection from a list
//! - `sample_array` - Array of random samples from a pool
//! - `static` - Static value
//! - `null` - Null value

pub mod generator;
pub mod generators;

// Re-exports for convenience
pub use generator::{DataGenerator, GeneratorError, InternalRowIterator};
