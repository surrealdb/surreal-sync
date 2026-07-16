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
pub mod mongodb_container;
pub mod mysql;
pub mod mysql_binlog_container;
pub mod mysql_binlog_e2e;
pub mod mysql_cleanup;
pub mod mysql_e2e;
pub mod neo4j;
pub mod postgresql;
pub mod postgresql_cleanup;
pub mod postgresql_pgoutput_container;
pub mod postgresql_pgoutput_e2e;
pub mod posts;
pub mod relations;
pub mod schema;
pub mod schemas_for_tests;
pub mod shared_containers;
pub mod surreal;
pub mod surreal2;
pub mod surreal3;
pub mod surrealdb_container;
pub mod table;
pub mod test_helpers;
pub mod users;
pub mod value;

pub use full::create_unified_full_dataset;
pub use table::SourceDatabase;
pub use test_helpers::{connect_surrealdb, generate_test_id, TestConfig};
