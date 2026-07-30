//! End-to-end ingestion test for the Snowflake source (library entry points).
//!
//! Snowflake cannot be run as a throwaway Docker container the way the other
//! sources' tests are, so this test is **gated on credentials** and cleanly skips
//! when they are absent (mirroring the `NEO4J_ENTERPRISE_*` pattern in
//! `tests/neo4j/`). The always-on correctness coverage for this source lives in
//! the `surreal-sync-snowflake (types)` unit tests.
//!
//! It reads a table from the `SNOWFLAKE_SAMPLE_DATA` share, which every account
//! has and which needs no seeding. That keeps the test **read-only**: the CI role
//! needs only USAGE on a warehouse plus IMPORTED PRIVILEGES on the share, with no
//! writable scratch database and nothing to clean up afterwards.
//!
//! Defaults to `SNOWFLAKE_SAMPLE_DATA` / `TPCH_SF10` / `CUSTOMER`, which is
//! 1,500,000 rows -- a real multi-partition sync, not a smoke test. Expect it to
//! take a while; `.config/nextest.toml` carries a `binary(snowflake)` override
//! that widens the slow-timeout and disables retries accordingly.
//!
//! Required env vars:
//!   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
//!   SNOWFLAKE_WAREHOUSE
//!
//! Optional overrides:
//!   SNOWFLAKE_DATABASE        (default SNOWFLAKE_SAMPLE_DATA)
//!   SNOWFLAKE_SCHEMA          (default TPCH_SF10; use TPCH_SF1 for a fast run)
//!   SNOWFLAKE_ROLE            (default: the user's default role)
//!   SNOWFLAKE_TEST_TABLE      (default CUSTOMER)
//!   SNOWFLAKE_TEST_ID_COLUMNS (default C_CUSTKEY; empty for a sequential index)
//!
//!     cargo nextest run -E 'binary(snowflake)'
//!
//! Set `SNOWFLAKE_REQUIRED=1` to turn the skip into a failure. CI does this so a
//! misnamed secret surfaces as a red build instead of a test that silently passes
//! without ever contacting Snowflake.

use surreal_sync::testing::surreal::{connect_auto, is_v3, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_snowflake::from_snowflake::client::SnowflakeClient;
use surreal_sync_snowflake::from_snowflake::full_sync::run_full_sync;
use surreal_sync_snowflake::from_snowflake::{SourceOpts, SyncOpts};

const DEFAULT_DATABASE: &str = "SNOWFLAKE_SAMPLE_DATA";
const DEFAULT_SCHEMA: &str = "TPCH_SF10";
const DEFAULT_TABLE: &str = "CUSTOMER";
const DEFAULT_ID_COLUMNS: &str = "C_CUSTKEY";

/// Read an optional env var, treating empty as absent.
///
/// GitHub Actions expands an undefined secret or variable to the empty string
/// rather than leaving it out of the environment, so an unset repository variable
/// must fall back to the default instead of overriding it with "".
fn optional_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Read a required env var, recording its name in `missing` when unset or empty.
fn required_var(name: &'static str, missing: &mut Vec<&'static str>) -> Option<String> {
    match optional_var(name) {
        Some(value) => Some(value),
        None => {
            missing.push(name);
            None
        }
    }
}

/// Read all required Snowflake env vars, or return the names of the ones that are
/// unset so the caller can decide between skipping and failing.
fn snowflake_opts_from_env(
    tables: Vec<String>,
    id_columns: Vec<String>,
) -> Result<SourceOpts, Vec<&'static str>> {
    let mut missing = Vec::new();
    let account = required_var("SNOWFLAKE_ACCOUNT", &mut missing);
    let user = required_var("SNOWFLAKE_USER", &mut missing);
    let key_path = required_var("SNOWFLAKE_PRIVATE_KEY_PATH", &mut missing);
    let warehouse = required_var("SNOWFLAKE_WAREHOUSE", &mut missing);

    let (Some(account), Some(user), Some(key_path), Some(warehouse)) =
        (account, user, key_path, warehouse)
    else {
        return Err(missing);
    };

    let private_key_pem = std::fs::read_to_string(&key_path)
        .unwrap_or_else(|e| panic!("failed to read SNOWFLAKE_PRIVATE_KEY_PATH {key_path}: {e}"));

    Ok(SourceOpts {
        account,
        user,
        private_key_pem,
        private_key_passphrase: None,
        warehouse,
        database: optional_var("SNOWFLAKE_DATABASE")
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string()),
        schema: optional_var("SNOWFLAKE_SCHEMA").unwrap_or_else(|| DEFAULT_SCHEMA.to_string()),
        role: optional_var("SNOWFLAKE_ROLE"),
        tables,
        id_columns,
    })
}

/// Authoritative source row count, so the assertion needs no hardcoded TPC-H
/// totals and holds at any scale factor or table.
async fn snowflake_row_count(client: &SnowflakeClient, qualified: &str) -> u64 {
    let result = client
        .execute_query(&format!("SELECT COUNT(*) FROM {qualified}"))
        .await
        .unwrap_or_else(|e| panic!("failed to count rows in {qualified}: {e:#}"));

    let cell = result
        .rows
        .first()
        .and_then(|row| row.first())
        .unwrap_or_else(|| panic!("COUNT(*) on {qualified} returned no cells"));

    // The SQL API v2 renders numerics as JSON strings; tolerate both.
    match cell {
        serde_json::Value::String(s) => s.parse().expect("COUNT(*) was not an integer"),
        serde_json::Value::Number(n) => n.as_u64().expect("COUNT(*) was not a u64"),
        other => panic!("unexpected COUNT(*) cell shape: {other}"),
    }
}

/// Run a query and decode the first result set as JSON, version-agnostically.
async fn query_json(conn: &SurrealConnection, sql: String) -> Vec<serde_json::Value> {
    match conn {
        SurrealConnection::V2(client) => {
            let mut resp = client.query(sql).await.expect("v2 query failed");
            resp.take(0).expect("v2 take failed")
        }
        SurrealConnection::V3(client) => {
            let mut resp = client.query(sql).await.expect("v3 query failed");
            resp.take(0).expect("v3 take failed")
        }
    }
}

/// Count rows in a SurrealDB table.
///
/// Uses `count()` with `GROUP ALL` rather than `SELECT *`: the sample tables run
/// to millions of rows and materializing them client-side would dwarf the sync.
async fn count_rows(conn: &SurrealConnection, table: &str) -> u64 {
    let rows = query_json(conn, format!("SELECT count() FROM {table} GROUP ALL")).await;
    rows.first()
        .and_then(|row| row.get("count"))
        .and_then(|count| count.as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn snowflake_full_sync_ingests_sample_table() {
    let test_id = generate_test_id();

    let table = optional_var("SNOWFLAKE_TEST_TABLE").unwrap_or_else(|| DEFAULT_TABLE.to_string());
    // An explicitly empty SNOWFLAKE_TEST_ID_COLUMNS selects the sequential-index
    // path, so distinguish "unset" from "set to empty" here.
    let id_columns: Vec<String> = std::env::var("SNOWFLAKE_TEST_ID_COLUMNS")
        .unwrap_or_else(|_| DEFAULT_ID_COLUMNS.to_string())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();

    // Skip cleanly when no account is configured, unless the caller demanded a
    // live run via SNOWFLAKE_REQUIRED (CI does).
    let source_opts = match snowflake_opts_from_env(vec![table.clone()], id_columns.clone()) {
        Ok(opts) => opts,
        Err(missing) => {
            assert!(
                std::env::var_os("SNOWFLAKE_REQUIRED").is_none(),
                "SNOWFLAKE_REQUIRED is set, so this test must run against a real \
                 Snowflake account, but these variables are unset or empty: {missing:?}"
            );
            eprintln!(
                "skipping snowflake_full_sync_ingests_sample_table: \
                 SNOWFLAKE_* credentials not set (missing {missing:?})"
            );
            return;
        }
    };

    let client = SnowflakeClient::new(&source_opts).expect("failed to build Snowflake client");

    let db = &source_opts.database;
    let schema = &source_opts.schema;
    let qualified = format!("\"{db}\".\"{schema}\".\"{table}\"");

    // --- Establish the expected row count from the source itself ---
    let expected = snowflake_row_count(&client, &qualified).await;
    assert!(
        expected > 0,
        "{qualified} is empty, so this test would pass trivially -- check \
         SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA / SNOWFLAKE_TEST_TABLE"
    );
    eprintln!("ingesting {expected} row(s) from {qualified}");

    // --- Connect to the shared SurrealDB container ---
    let sdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &sdb.ws_endpoint());
    let conn = connect_auto(&config)
        .await
        .expect("failed to connect to SurrealDB");

    let ns = config.surreal_namespace.clone();
    let sdb_name = config.surreal_database.clone();
    let sync_opts = SyncOpts {
        batch_size: 5000,
        dry_run: false,
    };

    // --- Run the ingestion through the version-appropriate sink ---
    if is_v3(&conn) {
        let opts = surreal_sync_surreal::v3::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal_sync_surreal::v3::surreal_connect(&opts, &ns, &sdb_name)
            .await
            .expect("v3 sink connect failed");
        let sink = surreal_sync_surreal::v3::Surreal3Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v3 ingestion failed");
    } else {
        let opts = surreal_sync_surreal::v2::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal_sync_surreal::v2::surreal_connect(&opts, &ns, &sdb_name)
            .await
            .expect("v2 sink connect failed");
        let sink = surreal_sync_surreal::v2::Surreal2Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v2 ingestion failed");
    }

    // --- Every source row must be present in SurrealDB ---
    let actual = count_rows(&conn, &table).await;
    assert_eq!(
        actual, expected,
        "{qualified} has {expected} rows but SurrealDB table {table} holds {actual}"
    );

    // --- Spot-check field fidelity on the default table ---
    //
    // Deep per-type coverage lives in the crates/snowflake types unit tests; this
    // just confirms real columns survived the round trip rather than the table
    // being full of empty records.
    if table == DEFAULT_TABLE {
        let sample = query_json(&conn, format!("SELECT * FROM {table} LIMIT 1")).await;
        let record = sample.first().expect("no sample record returned");

        assert!(
            record.get("C_NAME").and_then(|v| v.as_str()).is_some(),
            "expected C_NAME to be a string, got {record}"
        );
        // C_ACCTBAL is NUMBER(12,2), i.e. the Decimal conversion path.
        assert!(
            record.get("C_ACCTBAL").is_some_and(|v| !v.is_null()),
            "expected C_ACCTBAL to be populated, got {record}"
        );
        // With explicit id columns the source keeps the ID out of the field map.
        if id_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case("C_CUSTKEY"))
        {
            assert!(
                record.get("C_CUSTKEY").is_none(),
                "C_CUSTKEY is the record ID and should not be duplicated as a field, got {record}"
            );
        }
    }
}
