#![allow(clippy::uninlined_format_args)]

//! Demo crate verifying that SurrealDB SDK v2 and v3 can coexist in the same crate.
//!
//! This crate uses Cargo's package aliasing feature to import both SDK versions:
//! - `surrealdb2` = SurrealDB SDK 2.3.7
//! - `surrealdb3` = SurrealDB SDK 3.0.1
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
        // Cleanup any existing data first
        client.query("DELETE FROM demo_v2").await?;

        // Use raw query to avoid serde_json::Value serialization issues
        client
            .query("CREATE demo_v2:test1 SET name = 'V2 Test Record', version = 2")
            .await?;

        // Cleanup
        client.query("DELETE FROM demo_v2").await?;

        Ok(())
    }
}

// ============================================================================
// V3 SDK Types and Functions
// ============================================================================

/// V3 SDK types (from surrealdb 3.0.1)
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
        // Cleanup any existing data first
        client.query("DELETE FROM demo_v3").await?;

        // Use raw query to keep demo simple
        client
            .query("CREATE demo_v3:test1 SET name = 'V3 Test Record', version = 3")
            .await?;

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
    use std::sync::{Mutex, OnceLock};
    use surreal_version::testing::SurrealDbContainer;

    const V2_IMAGE: &str = "surrealdb/surrealdb:v2.3.7";
    const V3_IMAGE: &str = "surrealdb/surrealdb:v3.0.1";

    static CONTAINER_NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    fn register_container(name: &str) {
        let names = CONTAINER_NAMES.get_or_init(|| {
            extern "C" fn cleanup() {
                if let Some(names) = CONTAINER_NAMES.get() {
                    if let Ok(names) = names.lock() {
                        for name in names.iter() {
                            let _ = std::process::Command::new("docker")
                                .args(["rm", "-f", name])
                                .stdout(std::process::Stdio::null())
                                .stderr(std::process::Stdio::null())
                                .status();
                        }
                    }
                }
            }
            unsafe { libc::atexit(cleanup) };
            Mutex::new(Vec::new())
        });
        if let Ok(mut names) = names.lock() {
            names.push(name.to_string());
        }
    }

    fn init_logging() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .try_init();
    }

    /// Returns a shared V2 SurrealDB container, starting it on first call.
    fn shared_surrealdb_v2() -> &'static SurrealDbContainer {
        static DB: OnceLock<SurrealDbContainer> = OnceLock::new();
        DB.get_or_init(|| {
            let name = format!("multi-sdk-v2-{}", std::process::id());
            register_container(&name);
            let mut c = SurrealDbContainer::with_image(&name, V2_IMAGE);
            c.start().expect("V2 SurrealDB failed to start");
            c.wait_until_ready(30)
                .expect("V2 SurrealDB not ready in 30s");
            c
        })
    }

    /// Returns a shared V3 SurrealDB container, starting it on first call.
    fn shared_surrealdb_v3() -> &'static SurrealDbContainer {
        static DB: OnceLock<SurrealDbContainer> = OnceLock::new();
        DB.get_or_init(|| {
            let name = format!("multi-sdk-v3-{}", std::process::id());
            register_container(&name);
            let mut c = SurrealDbContainer::with_image(&name, V3_IMAGE);
            c.start().expect("V3 SurrealDB failed to start");
            c.wait_until_ready(30)
                .expect("V3 SurrealDB not ready in 30s");
            c
        })
    }

    fn v2_endpoint() -> String {
        shared_surrealdb_v2().ws_endpoint()
    }

    fn v3_endpoint() -> String {
        shared_surrealdb_v3().ws_endpoint()
    }

    /// Test that both SDKs compile and types are properly namespaced
    #[test]
    fn test_types_compile_and_are_distinct() {
        let v2_thing = v2::create_thing("users", "alice");
        assert_eq!(v2_thing.tb, "users");
        match &v2_thing.id {
            v2::Id::String(s) => assert_eq!(s, "alice"),
            _ => panic!("Expected V2 Id::String, got {:?}", v2_thing.id),
        }

        let v3_record_id = v3::create_record_id("users", "alice");
        assert_eq!(v3_record_id.table.to_string(), "users");
        match &v3_record_id.key {
            v3::RecordIdKey::String(s) => assert_eq!(s, "alice"),
            _ => panic!(
                "Expected V3 RecordIdKey::String, got {:?}",
                v3_record_id.key
            ),
        }
    }

    #[tokio::test]
    async fn test_v2_connection() -> anyhow::Result<()> {
        init_logging();

        let client = v2::connect(&v2_endpoint(), "test", "demo")
            .await
            .expect("V2 SDK should connect to V2 server");

        v2::demo_insert_query(&client)
            .await
            .expect("V2 insert/query should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_connection() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo")
            .await
            .expect("V3 SDK should connect to V3 server");

        v3::demo_insert_query(&client)
            .await
            .expect("V3 insert/query should succeed");

        Ok(())
    }

    /// V3 SDK uses "flatbuffers" subprotocol, V2 server expects "revision".
    #[tokio::test]
    async fn test_v3_sdk_cannot_connect_to_v2_server() -> anyhow::Result<()> {
        init_logging();

        let result = v3::connect(&v2_endpoint(), "test", "demo").await;

        assert!(
            result.is_err(),
            "V3 SDK should NOT be able to connect to V2 server due to protocol mismatch"
        );

        Ok(())
    }

    /// V2 SDK uses "revision" subprotocol, V3 server expects "flatbuffers".
    #[tokio::test]
    async fn test_v2_sdk_cannot_connect_to_v3_server() -> anyhow::Result<()> {
        init_logging();

        let result = v2::connect(&v3_endpoint(), "test", "demo").await;

        assert!(
            result.is_err(),
            "V2 SDK should NOT be able to connect to V3 server due to protocol mismatch"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v2_query_deserialize_to_struct() -> anyhow::Result<()> {
        init_logging();

        let client = v2::connect(&v2_endpoint(), "test", "demo_struct_test").await?;

        client.query("DELETE FROM test_record").await?;
        client
            .query("CREATE test_record:1 SET name = 'Alice', age = 30, active = true")
            .await?;

        #[derive(serde::Deserialize, Debug)]
        struct TestRecord {
            name: String,
            age: i64,
            active: bool,
        }

        let mut response = client.query("SELECT * FROM test_record").await?;
        let records: Vec<TestRecord> = response.take(0)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "Alice");
        assert_eq!(records[0].age, 30);
        assert!(records[0].active);

        client.query("DELETE FROM test_record").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_query_deserialize_to_struct() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo_struct_test").await?;

        client.query("DELETE FROM test_record").await?;
        client
            .query("CREATE test_record:1 SET name = 'Bob', age = 25, active = false")
            .await?;

        use surrealdb3::types::SurrealValue;

        #[derive(SurrealValue, Debug)]
        #[surreal(crate = "surrealdb3::types")]
        struct TestRecord {
            name: String,
            age: i64,
            active: bool,
        }

        let mut response = client.query("SELECT * FROM test_record").await?;
        let records: Vec<TestRecord> = response.take(0)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "Bob");
        assert_eq!(records[0].age, 25);
        assert!(!records[0].active);

        client.query("DELETE FROM test_record").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v2_delete_nonexistent_table() -> anyhow::Result<()> {
        init_logging();

        let client = v2::connect(&v2_endpoint(), "test", "demo_delete_test").await?;

        let result = client
            .query("DELETE FROM this_table_definitely_does_not_exist_12345")
            .await;

        assert!(
            result.is_ok(),
            "V2 SDK should handle DELETE on non-existent table gracefully, got: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_delete_nonexistent_table() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo_delete_test").await?;

        let result = client
            .query("DELETE FROM this_table_definitely_does_not_exist_12345 RETURN NONE")
            .await;

        assert!(
            result.is_ok(),
            "V3 SDK should handle DELETE on non-existent table gracefully, got: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v2_decimal_deserialization() -> anyhow::Result<()> {
        init_logging();

        let client = v2::connect(&v2_endpoint(), "test", "demo_decimal_test").await?;

        client.query("DELETE FROM decimal_test").await?;

        client
            .query("CREATE decimal_test:1 SET dec_val = <decimal> 123.45")
            .await?;

        eprintln!("\n=== V2 SDK Decimal Deserialization Tests ===");

        let mut response = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_f64: Result<Option<f64>, _> = response.take((0, "dec_val"));
        eprintln!("V2 SDK: decimal as f64        = {:?}", val_f64);

        let mut response2 = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_decimal: Result<Option<rust_decimal::Decimal>, _> = response2.take((0, "dec_val"));
        eprintln!("V2 SDK: decimal as Decimal    = {:?}", val_decimal);

        let mut response3 = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_str: Result<Option<String>, _> = response3.take((0, "dec_val"));
        eprintln!("V2 SDK: decimal as String     = {:?}", val_str);

        let mut response4 = client
            .query("SELECT <float> dec_val as casted FROM decimal_test:1")
            .await?;
        let val_casted: Result<Option<f64>, _> = response4.take((0, "casted"));
        eprintln!("V2 SDK: decimal <float> cast  = {:?}", val_casted);

        let mut response5 = client
            .query("SELECT <string> dec_val as casted FROM decimal_test:1")
            .await?;
        let val_str_cast: Result<Option<String>, _> = response5.take((0, "casted"));
        eprintln!("V2 SDK: decimal <string> cast = {:?}", val_str_cast);

        eprintln!("\n--- V2 SDK Summary ---");
        eprintln!("| Approach          | Result |");
        eprintln!("|-------------------|--------|");
        eprintln!(
            "| as f64            | {} |",
            if val_f64.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| as Decimal        | {} |",
            if val_decimal.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| as String         | {} |",
            if val_str.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| <float> cast      | {} |",
            if val_casted.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| <string> cast     | {} |",
            if val_str_cast.is_ok() { "OK" } else { "Err" }
        );

        client.query("DELETE FROM decimal_test").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_decimal_deserialization() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo_decimal_test").await?;

        client.query("DELETE FROM decimal_test").await?;

        client
            .query("CREATE decimal_test:1 SET dec_val = <decimal> 123.45")
            .await?;

        eprintln!("\n=== V3 SDK Decimal Deserialization Tests ===");

        let mut response = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_f64: Result<Option<f64>, _> = response.take((0, "dec_val"));
        eprintln!("V3 SDK: decimal as f64        = {:?}", val_f64);

        let mut response2 = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_decimal: Result<Option<rust_decimal::Decimal>, _> = response2.take((0, "dec_val"));
        eprintln!("V3 SDK: decimal as Decimal    = {:?}", val_decimal);

        let mut response3 = client.query("SELECT dec_val FROM decimal_test:1").await?;
        let val_str: Result<Option<String>, _> = response3.take((0, "dec_val"));
        eprintln!("V3 SDK: decimal as String     = {:?}", val_str);

        let mut response4 = client
            .query("SELECT <float> dec_val as casted FROM decimal_test:1")
            .await?;
        let val_casted: Result<Option<f64>, _> = response4.take((0, "casted"));
        eprintln!("V3 SDK: decimal <float> cast  = {:?}", val_casted);

        let mut response5 = client
            .query("SELECT <string> dec_val as casted FROM decimal_test:1")
            .await?;
        let val_str_cast: Result<Option<String>, _> = response5.take((0, "casted"));
        eprintln!("V3 SDK: decimal <string> cast = {:?}", val_str_cast);

        eprintln!("\n--- V3 SDK Summary ---");
        eprintln!("| Approach          | Result |");
        eprintln!("|-------------------|--------|");
        eprintln!(
            "| as f64            | {} |",
            if val_f64.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| as Decimal        | {} |",
            if val_decimal.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| as String         | {} |",
            if val_str.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| <float> cast      | {} |",
            if val_casted.is_ok() { "OK" } else { "Err" }
        );
        eprintln!(
            "| <string> cast     | {} |",
            if val_str_cast.is_ok() { "OK" } else { "Err" }
        );

        client.query("DELETE FROM decimal_test").await?;

        Ok(())
    }

    /// rust_decimal uses 96 bits for mantissa. MAX is 79228162514264337593543950335.
    /// This means not all 29-digit numbers can be parsed - only those <= MAX.
    #[tokio::test]
    async fn test_rust_decimal_parsing_limits() -> anyhow::Result<()> {
        init_logging();

        eprintln!("\n=== rust_decimal Parsing Limits ===");
        eprintln!("MAX = {}", rust_decimal::Decimal::MAX);
        eprintln!("MIN = {}", rust_decimal::Decimal::MIN);

        let test_cases = [
            ("rust_decimal MAX", "79228162514264337593543950335", true),
            (
                "rust_decimal MAX + 1",
                "79228162514264337593543950336",
                false,
            ),
            ("28 nines", "9999999999999999999999999999", true),
            ("29 nines", "99999999999999999999999999999", false),
            ("typical money", "99999999.99", true),
            ("high precision", "123.456789012345678901234567890", true),
        ];

        for (label, value, expected_ok) in &test_cases {
            let parsed: Result<rust_decimal::Decimal, _> = value.parse();
            let ok = parsed.is_ok();
            eprintln!("{}: parse={} (expected={})", label, ok, expected_ok);
            assert_eq!(ok, *expected_ok, "Unexpected result for {}", label);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_v2_decimal_storage_with_dec_suffix() -> anyhow::Result<()> {
        init_logging();

        let client = v2::connect(&v2_endpoint(), "test", "demo_decimal_limits").await?;

        client.query("DELETE FROM decimal_limits").await?;

        eprintln!("\n=== V2 SDK Decimal Storage (using dec suffix) ===");

        let test_cases = [
            ("typical_money", "99999999.99dec"),
            ("small_decimal", "123.45dec"),
            ("large_integer_as_decimal", "1234567890123456789dec"),
            ("rust_decimal_max", "79228162514264337593543950335dec"),
            ("over_rust_decimal_max", "79228162514264337593543950336dec"),
            ("28_nines", "9999999999999999999999999999dec"),
            ("29_nines", "99999999999999999999999999999dec"),
        ];

        for (label, value) in &test_cases {
            let query = format!("CREATE decimal_limits:{label} SET val = {value}");
            let result = client.query(&query).await;

            if result.is_ok() {
                let mut response = client
                    .query(format!("SELECT val FROM decimal_limits:{label}"))
                    .await?;
                let read_result: Result<Option<rust_decimal::Decimal>, _> =
                    response.take((0, "val"));
                eprintln!("V2 {}: store=OK, read_as_decimal={:?}", label, read_result);
            } else {
                eprintln!("V2 {}: store=FAIL", label);
            }
        }

        client.query("DELETE FROM decimal_limits").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_decimal_storage_with_dec_suffix() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo_decimal_limits").await?;

        client.query("DELETE FROM decimal_limits").await?;

        eprintln!("\n=== V3 SDK Decimal Storage (using dec suffix) ===");

        let test_cases = [
            ("typical_money", "99999999.99dec"),
            ("small_decimal", "123.45dec"),
            ("large_integer_as_decimal", "1234567890123456789dec"),
            ("rust_decimal_max", "79228162514264337593543950335dec"),
            ("over_rust_decimal_max", "79228162514264337593543950336dec"),
            ("28_nines", "9999999999999999999999999999dec"),
            ("29_nines", "99999999999999999999999999999dec"),
        ];

        for (label, value) in &test_cases {
            let query = format!("CREATE decimal_limits:{label} SET val = {value}");
            let result = client.query(&query).await;

            if result.is_ok() {
                let mut response = client
                    .query(format!("SELECT val FROM decimal_limits:{label}"))
                    .await?;
                let read_result: Result<Option<rust_decimal::Decimal>, _> =
                    response.take((0, "val"));
                eprintln!("V3 {}: store=OK, read_as_decimal={:?}", label, read_result);
            } else {
                eprintln!("V3 {}: store=FAIL", label);
            }
        }

        client.query("DELETE FROM decimal_limits").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v3_cleanup_pattern() -> anyhow::Result<()> {
        init_logging();

        let client = v3::connect(&v3_endpoint(), "test", "demo_cleanup_test").await?;

        let tables = [
            "nonexistent_table_1",
            "nonexistent_table_2",
            "nonexistent_table_3",
        ];

        for table in tables {
            let query = format!("DELETE FROM {table} RETURN NONE");
            match client.query(&query).await {
                Ok(_) => {
                    eprintln!("Cleaned up table '{}'", table);
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("does not exist") {
                        eprintln!("Table '{}' does not exist, skipping: {}", table, err_str);
                    } else {
                        panic!("Unexpected error during cleanup: {}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
