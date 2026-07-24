//! Trait definitions for SurrealDB test clients.

use anyhow::Result;

/// Trait for direct SurrealDB SDK operations in tests.
///
/// This trait abstracts over version differences allowing tests to run
/// against both SurrealDB v2 and v3 servers.
#[async_trait::async_trait]
pub trait SurrealTestClient: Send + Sync {
    /// Execute raw SurrealQL query (for test setup/teardown).
    ///
    /// Returns Ok(()) on success, error on failure.
    async fn query(&self, sql: &str) -> Result<()>;

    /// Delete all records from a table.
    async fn delete_table(&self, table: &str) -> Result<()>;

    /// Check if a table exists.
    async fn table_exists(&self, table: &str) -> Result<bool>;

    /// Get the count of records in a table.
    async fn count_records(&self, table: &str) -> Result<usize>;
}
