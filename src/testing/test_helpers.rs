//! Shared test helpers for E2E and CLI integration tests
//!
//! This module contains reusable functions for test setup, cleanup, and verification
//! that can be used across different test suites.

use std::sync::atomic::{AtomicU64, Ordering};
use surrealdb::{engine::any::connect, Surreal};

// Generate unique test identifiers for parallel execution
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique test identifier for parallel test execution
pub fn generate_test_id() -> u64 {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    timestamp.wrapping_add(counter)
}

/// Test configuration for database connections
#[derive(Clone)]
pub struct TestConfig {
    pub neo4j_uri: String,
    pub neo4j_username: String,
    pub neo4j_password: String,
    pub surreal_endpoint: String,
    pub surreal_namespace: String,
    pub surreal_database: String,
}

impl TestConfig {
    /// Create a new test configuration with unique identifiers
    pub fn new(test_id: u64, neo4j_instance: &str) -> Self {
        TestConfig {
            neo4j_uri: format!("bolt://{neo4j_instance}:7687"),
            neo4j_username: "neo4j".to_string(),
            neo4j_password: "password".to_string(),
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            surreal_namespace: format!("test_ns_{test_id}"),
            surreal_database: format!("test_db_{test_id}"),
        }
    }
}

/// Connect to SurrealDB with the given configuration
pub async fn connect_surrealdb(
    config: &TestConfig,
) -> Result<Surreal<surrealdb::engine::any::Any>, Box<dyn std::error::Error>> {
    let surreal = connect(&config.surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal
        .use_ns(&config.surreal_namespace)
        .use_db(&config.surreal_database)
        .await?;
    Ok(surreal)
}

/// Clean up test data from SurrealDB tables
pub async fn cleanup_surrealdb_test_data(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for table in tables {
        let query = format!("DELETE FROM {table}");
        let _: Vec<surrealdb::sql::Value> = surreal.query(&query).await?.take(0)?;
    }
    Ok(())
}

/// Clean up all tables from a test dataset in SurrealDB
///
/// This function extracts all table names from the dataset and deletes all data
/// from those tables in SurrealDB.
pub async fn cleanup_surrealdb(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    dataset: &crate::testing::table::TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract all table names from the dataset
    let table_names: Vec<&str> = dataset
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect();

    // Clean up all tables
    cleanup_surrealdb_test_data(surreal, &table_names).await
}
