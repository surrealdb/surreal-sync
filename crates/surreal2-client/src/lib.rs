//! SurrealDB v2 test client implementation.
//!
//! Provides an implementation of `SurrealTestClient` for SurrealDB v2 servers.

use anyhow::Result;
use surreal_client::SurrealTestClient;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

/// SurrealDB v2 test client wrapper.
pub struct Surreal2Client {
    client: Surreal<Any>,
}

impl Surreal2Client {
    /// Create a new client wrapper from an existing Surreal connection.
    pub fn new(client: Surreal<Any>) -> Self {
        Self { client }
    }

    /// Connect to SurrealDB v2 and return a wrapped client.
    pub async fn connect(
        endpoint: &str,
        username: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        // Convert http:// to ws:// for WebSocket connection
        let ws_endpoint = endpoint
            .replace("http://", "ws://")
            .replace("https://", "wss://");

        tracing::debug!("Connecting to SurrealDB v2 at {}", ws_endpoint);

        let client = surrealdb::engine::any::connect(&ws_endpoint).await?;

        client
            .signin(surrealdb::opt::auth::Root { username, password })
            .await?;

        client.use_ns(namespace).use_db(database).await?;

        Ok(Self { client })
    }

    /// Get a reference to the underlying Surreal client.
    pub fn inner(&self) -> &Surreal<Any> {
        &self.client
    }

    /// Get a mutable reference to the underlying Surreal client.
    pub fn inner_mut(&mut self) -> &mut Surreal<Any> {
        &mut self.client
    }
}

#[async_trait::async_trait]
impl SurrealTestClient for Surreal2Client {
    async fn query(&self, sql: &str) -> Result<()> {
        self.client.query(sql).await?;
        Ok(())
    }

    async fn delete_table(&self, table: &str) -> Result<()> {
        self.client.query(format!("DELETE {table};")).await?;
        Ok(())
    }

    async fn table_exists(&self, table: &str) -> Result<bool> {
        let mut result = self.client.query("INFO FOR DB;").await?;

        // Extract the tables info from the response
        let info: Option<serde_json::Value> = result.take(0)?;

        if let Some(info) = info {
            if let Some(tables) = info.get("tables") {
                if let Some(tables_obj) = tables.as_object() {
                    return Ok(tables_obj.contains_key(table));
                }
            }
        }

        Ok(false)
    }

    async fn count_records(&self, table: &str) -> Result<usize> {
        let mut result = self
            .client
            .query(format!("SELECT count() FROM {table} GROUP ALL;"))
            .await?;

        let count: Option<i64> = result.take((0, "count"))?;
        Ok(count.unwrap_or(0) as usize)
    }
}

/// Helper function to connect to SurrealDB v2.
pub async fn connect(
    endpoint: &str,
    username: &str,
    password: &str,
    namespace: &str,
    database: &str,
) -> Result<Surreal2Client> {
    Surreal2Client::connect(endpoint, username, password, namespace, database).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        // This is a compile-time test to ensure the types work correctly
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Surreal2Client>();
    }
}
