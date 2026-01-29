#![allow(clippy::uninlined_format_args)]

//! Demo crate verifying that SurrealDB SDK v2 and v3 can coexist in the same crate.
//!
//! This crate uses Cargo's package aliasing feature to import both SDK versions:
//! - `surrealdb2` = SurrealDB SDK 2.3.7
//! - `surrealdb3` = SurrealDB SDK 3.0.0-beta.2
//!
//! ## Key Findings
//!
//! 1. **Type namespacing**: Types are properly separated:
//!    - V2: `surrealdb2::sql::Thing`, `surrealdb2::sql::Id`, `surrealdb2::sql::Value`
//!    - V3: `surrealdb3::types::RecordId`, `surrealdb3::types::RecordIdKey`
//!
//! 2. **Connection types**: Both use `Surreal<Any>` but from different namespaces
//!
//! 3. **Protocol incompatibility**: V2 SDK uses WebSocket subprotocol "revision",
//!    V3 SDK uses "flatbuffers". They cannot connect to the wrong server version.

// Note: anyhow::Result is used in tests and function signatures

// ============================================================================
// V2 SDK Types and Functions
// ============================================================================

/// V2 SDK types (from surrealdb 2.3.7)
pub mod v2 {
    pub use surrealdb2::engine::any::Any;
    pub use surrealdb2::opt::auth::Root;
    pub use surrealdb2::sql::{Id, Thing, Value};
    pub use surrealdb2::Surreal;

    /// Connect to a SurrealDB v2 server
    pub async fn connect(
        endpoint: &str,
        namespace: &str,
        database: &str,
    ) -> anyhow::Result<Surreal<Any>> {
        let client = surrealdb2::engine::any::connect(endpoint).await?;
        client
            .signin(Root {
                username: "root",
                password: "root",
            })
            .await?;
        client.use_ns(namespace).use_db(database).await?;
        Ok(client)
    }

    /// Create a Thing (record ID) using v2 SDK types
    pub fn create_thing(table: &str, id: &str) -> Thing {
        Thing::from((table.to_string(), Id::String(id.to_string())))
    }

    /// Example: Insert and query a record using v2 SDK
    pub async fn demo_insert_query(client: &Surreal<Any>) -> anyhow::Result<()> {
        // Use raw query to avoid serde_json::Value serialization issues
        client
            .query("CREATE demo_v2:test1 SET name = 'V2 Test Record', version = 2")
            .await?;

        // Query it back - count records
        let mut response = client
            .query("SELECT count() FROM demo_v2 GROUP ALL")
            .await?;

        let count: Option<i64> = response.take((0, "count"))?;
        assert!(count.unwrap_or(0) > 0, "Should find the inserted record");

        // Cleanup
        client.query("DELETE FROM demo_v2").await?;

        Ok(())
    }
}

// ============================================================================
// V3 SDK Types and Functions
// ============================================================================

/// V3 SDK types (from surrealdb 3.0.0-beta.2)
pub mod v3 {
    pub use surrealdb3::engine::any::Any;
    pub use surrealdb3::opt::auth::Root;
    pub use surrealdb3::types::{RecordId, RecordIdKey, SurrealValue, Value};
    pub use surrealdb3::Surreal;

    /// Connect to a SurrealDB v3 server
    ///
    /// Note: V3 SDK requires String for username/password, unlike V2 which accepts &str
    pub async fn connect(
        endpoint: &str,
        namespace: &str,
        database: &str,
    ) -> anyhow::Result<Surreal<Any>> {
        let client = surrealdb3::engine::any::connect(endpoint).await?;
        client
            .signin(Root {
                username: "root".to_string(),
                password: "root".to_string(),
            })
            .await?;
        client.use_ns(namespace).use_db(database).await?;
        Ok(client)
    }

    /// Create a RecordId using v3 SDK types
    ///
    /// Note: V3 uses RecordId::new() instead of from_table_key()
    pub fn create_record_id(table: &str, id: &str) -> RecordId {
        RecordId::new(table, RecordIdKey::String(id.to_string()))
    }

    /// Example: Insert and query a record using v3 SDK
    pub async fn demo_insert_query(client: &Surreal<Any>) -> anyhow::Result<()> {
        // Use raw query to keep demo simple
        client
            .query("CREATE demo_v3:test1 SET name = 'V3 Test Record', version = 3")
            .await?;

        // Query it back - count records
        let mut response = client
            .query("SELECT count() FROM demo_v3 GROUP ALL")
            .await?;

        let count: Option<i64> = response.take((0, "count"))?;
        assert!(count.unwrap_or(0) > 0, "Should find the inserted record");

        // Cleanup
        client.query("DELETE FROM demo_v3").await?;

        Ok(())
    }
}

// ============================================================================
// Compile-time verification that types don't mix
// ============================================================================

/// This module demonstrates that v2 and v3 types are distinct at compile time.
/// Uncommenting the invalid lines would cause compile errors.
#[allow(dead_code)]
mod type_safety_demo {
    use super::*;

    fn v2_thing_is_not_v3_record_id() {
        let _v2_thing: v2::Thing = v2::create_thing("table", "id");
        let _v3_record_id: v3::RecordId = v3::create_record_id("table", "id");

        // These would NOT compile - types are incompatible:
        // let _wrong: v3::RecordId = _v2_thing;  // Error: mismatched types
        // let _wrong: v2::Thing = _v3_record_id; // Error: mismatched types
    }

    fn v2_client_is_not_v3_client() {
        // v2::Surreal<Any> and v3::Surreal<Any> are distinct types
        // even though they have the same name structure.
        // You cannot pass a v2 client to a function expecting v3 client.
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logging() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .try_init();
    }

    /// Test that both SDKs compile and types are properly namespaced
    #[test]
    fn test_types_compile_and_are_distinct() {
        // V2 types
        let v2_thing = v2::create_thing("users", "alice");
        assert_eq!(v2_thing.tb, "users");
        match &v2_thing.id {
            v2::Id::String(s) => assert_eq!(s, "alice"),
            _ => panic!("Expected V2 Id::String, got {:?}", v2_thing.id),
        }

        // V3 types - note: table is a field, not a method in v3
        let v3_record_id = v3::create_record_id("users", "alice");
        assert_eq!(v3_record_id.table.to_string(), "users");
        match &v3_record_id.key {
            v3::RecordIdKey::String(s) => assert_eq!(s, "alice"),
            _ => panic!(
                "Expected V3 RecordIdKey::String, got {:?}",
                v3_record_id.key
            ),
        }

        // Verify types are distinct (this is a compile-time check, but we document it)
        // V2 Thing and V3 RecordId are different types - mixing them would be a compile error
    }

    /// Test connecting to V2 server (DevContainer default at surrealdb:8000)
    ///
    /// This test requires a V2 SurrealDB server to be running.
    /// In DevContainer, this is available at ws://surrealdb:8000.
    #[tokio::test]
    async fn test_v2_connection() -> anyhow::Result<()> {
        init_logging();

        let endpoint = std::env::var("SURREAL_V2_ENDPOINT")
            .unwrap_or_else(|_| "ws://surrealdb:8000".to_string());

        // Connect to V2 server - this should succeed
        let client = v2::connect(&endpoint, "test", "demo")
            .await
            .expect("V2 SDK should connect to V2 server");

        // Run demo operations to verify the connection works
        v2::demo_insert_query(&client)
            .await
            .expect("V2 insert/query should succeed");

        Ok(())
    }

    /// Test connecting to V3 server (typically at localhost:8001)
    ///
    /// This test is skipped if SURREAL_V3_ENDPOINT is not set or server is unavailable.
    /// Set SURREAL_V3_ENDPOINT to enable this test.
    #[tokio::test]
    async fn test_v3_connection() -> anyhow::Result<()> {
        init_logging();

        // Only run this test if V3 endpoint is explicitly configured
        let endpoint = match std::env::var("SURREAL_V3_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => {
                eprintln!("Skipping V3 connection test: SURREAL_V3_ENDPOINT not set");
                return Ok(());
            }
        };

        // Connect to V3 server - this should succeed when server is available
        let client = v3::connect(&endpoint, "test", "demo")
            .await
            .expect("V3 SDK should connect to V3 server when SURREAL_V3_ENDPOINT is set");

        // Run demo operations to verify the connection works
        v3::demo_insert_query(&client)
            .await
            .expect("V3 insert/query should succeed");

        Ok(())
    }

    /// Test that V3 SDK cannot connect to V2 server (protocol mismatch)
    ///
    /// V2 servers use WebSocket subprotocol "revision", V3 SDK expects "flatbuffers".
    /// This test verifies the protocol incompatibility.
    #[tokio::test]
    async fn test_v3_sdk_cannot_connect_to_v2_server() -> anyhow::Result<()> {
        init_logging();

        let v2_endpoint = std::env::var("SURREAL_V2_ENDPOINT")
            .unwrap_or_else(|_| "ws://surrealdb:8000".to_string());

        // V3 SDK connecting to V2 server should fail due to protocol mismatch
        let result = v3::connect(&v2_endpoint, "test", "demo").await;

        assert!(
            result.is_err(),
            "V3 SDK should NOT be able to connect to V2 server due to protocol mismatch"
        );

        Ok(())
    }

    /// Test that V2 SDK cannot connect to V3 server (protocol mismatch)
    ///
    /// V3 servers use WebSocket subprotocol "flatbuffers", V2 SDK expects "revision".
    /// This test is skipped if SURREAL_V3_ENDPOINT is not set.
    #[tokio::test]
    async fn test_v2_sdk_cannot_connect_to_v3_server() -> anyhow::Result<()> {
        init_logging();

        // Only run this test if V3 endpoint is explicitly configured
        let v3_endpoint = match std::env::var("SURREAL_V3_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => {
                eprintln!("Skipping V2-to-V3 mismatch test: SURREAL_V3_ENDPOINT not set");
                return Ok(());
            }
        };

        // V2 SDK connecting to V3 server should fail due to protocol mismatch
        let result = v2::connect(&v3_endpoint, "test", "demo").await;

        assert!(
            result.is_err(),
            "V2 SDK should NOT be able to connect to V3 server due to protocol mismatch"
        );

        Ok(())
    }

    /// Test V2 SDK deserializing query results to custom struct
    #[tokio::test]
    async fn test_v2_query_deserialize_to_struct() -> anyhow::Result<()> {
        init_logging();

        let endpoint = std::env::var("SURREAL_V2_ENDPOINT")
            .unwrap_or_else(|_| "ws://surrealdb:8000".to_string());

        let client = v2::connect(&endpoint, "test", "demo_struct_test").await?;

        // Create test data
        client
            .query("CREATE test_record:1 SET name = 'Alice', age = 30, active = true")
            .await?;

        // Define struct to deserialize into
        #[derive(serde::Deserialize, Debug)]
        struct TestRecord {
            name: String,
            age: i64,
            active: bool,
        }

        // Query and deserialize - V2 SDK supports Deserialize trait directly
        let mut response = client.query("SELECT * FROM test_record").await?;
        let records: Vec<TestRecord> = response.take(0)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "Alice");
        assert_eq!(records[0].age, 30);
        assert!(records[0].active);

        // Cleanup
        client.query("DELETE FROM test_record").await?;

        Ok(())
    }

    /// Test V3 SDK deserializing query results to custom struct
    ///
    /// V3 SDK requires SurrealValue trait for .take(). Use #[derive(SurrealValue)]
    /// on the struct to enable direct deserialization.
    #[tokio::test]
    async fn test_v3_query_deserialize_to_struct() -> anyhow::Result<()> {
        init_logging();

        // Only run this test if V3 endpoint is explicitly configured
        let endpoint = match std::env::var("SURREAL_V3_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => {
                eprintln!("Skipping V3 struct deserialization test: SURREAL_V3_ENDPOINT not set");
                return Ok(());
            }
        };

        let client = v3::connect(&endpoint, "test", "demo_struct_test").await?;

        // Create test data
        client
            .query("CREATE test_record:1 SET name = 'Bob', age = 25, active = false")
            .await?;

        // Define struct with SurrealValue derive for V3 SDK
        // This enables direct deserialization via .take()
        // Use #[surreal(crate = "...")] to specify the path to surrealdb_types
        use surrealdb3::types::SurrealValue;

        #[derive(SurrealValue, Debug)]
        #[surreal(crate = "surrealdb3::types")]
        struct TestRecord {
            name: String,
            age: i64,
            active: bool,
        }

        // Query and deserialize directly - V3 SDK uses SurrealValue trait
        let mut response = client.query("SELECT * FROM test_record").await?;
        let records: Vec<TestRecord> = response.take(0)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "Bob");
        assert_eq!(records[0].age, 25);
        assert!(!records[0].active);

        // Cleanup
        client.query("DELETE FROM test_record").await?;

        Ok(())
    }
}
