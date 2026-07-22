//! End-to-end ingestion test for the Snowflake source (library entry points).
//!
//! Snowflake cannot be run as a throwaway Docker container the way the other
//! sources' tests are, so this test is **gated on credentials** and cleanly
//! skips when they are absent (mirroring the `NEO4J_ENTERPRISE_*` pattern in
//! `tests/neo4j/`). The always-on correctness coverage for this source lives in
//! the `snowflake-types` unit tests.
//!
//! To run it against a real account, set:
//!   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
//!   SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA (optional
//!   SNOWFLAKE_ROLE), then:
//!     cargo nextest run -E 'test(snowflake)'

use surreal_sync::testing::surreal::{connect_auto, is_v3, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_snowflake_source::{run_full_sync, SnowflakeClient, SourceOpts, SyncOpts};

/// Read all required Snowflake env vars, or return `None` (skip) if any is unset.
fn snowflake_opts_from_env(tables: Vec<String>, id_columns: Vec<String>) -> Option<SourceOpts> {
    let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
    let user = std::env::var("SNOWFLAKE_USER").ok()?;
    let key_path = std::env::var("SNOWFLAKE_PRIVATE_KEY_PATH").ok()?;
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok()?;
    let database = std::env::var("SNOWFLAKE_DATABASE").ok()?;
    let schema = std::env::var("SNOWFLAKE_SCHEMA").unwrap_or_else(|_| "PUBLIC".to_string());
    let role = std::env::var("SNOWFLAKE_ROLE").ok();

    let private_key_pem = std::fs::read_to_string(&key_path)
        .unwrap_or_else(|e| panic!("failed to read SNOWFLAKE_PRIVATE_KEY_PATH {key_path}: {e}"));

    Some(SourceOpts {
        account,
        user,
        private_key_pem,
        private_key_passphrase: None,
        warehouse,
        database,
        schema,
        role,
        tables,
        id_columns,
    })
}

/// Count rows currently stored in a SurrealDB table (version-agnostic).
async fn count_rows(conn: &SurrealConnection, table: &str) -> usize {
    let sql = format!("SELECT * FROM {table}");
    match conn {
        SurrealConnection::V2(client) => {
            let mut resp = client.query(sql).await.expect("v2 query failed");
            let rows: Vec<serde_json::Value> = resp.take(0).expect("v2 take failed");
            rows.len()
        }
        SurrealConnection::V3(client) => {
            let mut resp = client.query(sql).await.expect("v3 query failed");
            let rows: Vec<serde_json::Value> = resp.take(0).expect("v3 take failed");
            rows.len()
        }
    }
}

#[tokio::test]
async fn snowflake_full_sync_ingests_selected_table() {
    let test_id = generate_test_id();
    let table = format!("SURREAL_SYNC_TEST_{test_id}");

    // Skip cleanly when no account is configured.
    let Some(source_opts) = snowflake_opts_from_env(vec![table.clone()], vec!["ID".to_string()])
    else {
        eprintln!(
            "skipping snowflake_full_sync_ingests_selected_table: \
             SNOWFLAKE_* credentials not set"
        );
        return;
    };

    let client = SnowflakeClient::new(&source_opts).expect("failed to build Snowflake client");

    // --- Seed a unique temp table in Snowflake ---
    let db = &source_opts.database;
    let schema = &source_opts.schema;
    let qualified = format!("\"{db}\".\"{schema}\".\"{table}\"");

    client
        .execute_query(&format!(
            "CREATE OR REPLACE TABLE {qualified} \
             (ID NUMBER, NAME STRING, AMOUNT NUMBER(10,2), ACTIVE BOOLEAN)"
        ))
        .await
        .expect("failed to create temp table");

    client
        .execute_query(&format!(
            "INSERT INTO {qualified} (ID, NAME, AMOUNT, ACTIVE) VALUES \
             (1, 'alice', 10.50, TRUE), (2, 'bob', 20.00, FALSE)"
        ))
        .await
        .expect("failed to insert seed rows");

    // --- Connect to the shared SurrealDB container ---
    let sdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &sdb.ws_endpoint());
    let conn = connect_auto(&config)
        .await
        .expect("failed to connect to SurrealDB");

    let ns = config.surreal_namespace.clone();
    let sdb_name = config.surreal_database.clone();
    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // --- Run the ingestion through the version-appropriate sink ---
    if is_v3(&conn) {
        let opts = surreal3_sink::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal3_sink::surreal_connect(&opts, &ns, &sdb_name)
            .await
            .expect("v3 sink connect failed");
        let sink = surreal3_sink::Surreal3Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v3 ingestion failed");
    } else {
        let opts = surreal2_sink::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal2_sink::surreal_connect(&opts, &ns, &sdb_name)
            .await
            .expect("v2 sink connect failed");
        let sink = surreal2_sink::Surreal2Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v2 ingestion failed");
    }

    // --- Assert the rows landed (table name is upper-cased by the source) ---
    let count = count_rows(&conn, &table).await;

    // Best-effort teardown of the temp table before the final assertion.
    let _ = client
        .execute_query(&format!("DROP TABLE IF EXISTS {qualified}"))
        .await;

    assert_eq!(count, 2, "expected 2 ingested rows in table {table}");
}
