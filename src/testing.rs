//! Test data infrastructure for database type testing
//!
//! This module provides a complete testing infrastructure that can represent
//! data in its native form for each database source and validate consistency
//! after synchronization to SurrealDB.
//!

pub mod checkpoint;
pub mod cli;
pub mod field;
pub mod full;
pub mod mongodb;
pub mod mysql;
pub mod mysql_cleanup;
pub mod neo4j;
pub mod postgresql;
pub mod postgresql_cleanup;
pub mod posts;
pub mod relations;
pub mod schema;
pub mod schemas_for_tests;
pub mod surrealdb;
pub mod table;
pub mod test_helpers;
pub mod users;
pub mod value;

pub use full::create_unified_full_dataset;
pub use test_helpers::{connect_surrealdb, generate_test_id, TestConfig};
