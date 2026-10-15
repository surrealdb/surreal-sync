//! Type-conversion coverage against a live BigQuery emulator.
//!
//! The unit tests in `surreal-sync-bigquery`'s `types.rs` pin the wire encodings
//! from fixtures; this test proves the emulator really sends those encodings and
//! that they survive the whole path into SurrealDB.
//!
//! Not covered here, and unit-tested only:
//!
//!   - `GEOGRAPHY` — the emulator's SQL engine cannot produce geography values.
//!   - `RANGE` / `INTERVAL` — not in the emulator's supported type set.
//!
//! Those, plus every encoding the emulator spells differently from the real API
//! (notably `TIMESTAMP` in scientific notation), live in the crate's unit tests.

use crate::common::{sync_opts, unique_table, BigQueryTestEnv};

#[tokio::test]
async fn bigquery_converts_every_supported_column_type() {
    let (env, mut source_opts) = BigQueryTestEnv::new(Vec::new(), vec!["id".to_string()]).await;
    let table = unique_table("bq_types", env.test_id);
    source_opts.tables = vec![table.clone()];

    let qualified = env.qualified(&table);
    env.execute(&format!(
        "CREATE TABLE {qualified} (
            id INT64,
            s STRING,
            b BYTES,
            i INT64,
            f FLOAT64,
            n NUMERIC,
            bn BIGNUMERIC,
            ok BOOL,
            ts TIMESTAMP,
            d DATE,
            t TIME,
            dt DATETIME,
            j JSON,
            arr ARRAY<INT64>,
            st STRUCT<a INT64, b STRING>
        )"
    ))
    .await;

    env.execute(&format!(
        "INSERT INTO {qualified} VALUES (
            1,
            'hello',
            b'\\xde\\xad\\xbe\\xef',
            42,
            1.5,
            12.34,
            2.5,
            true,
            TIMESTAMP '2022-01-01 00:00:00 UTC',
            DATE '2022-02-01',
            TIME '01:01:01.5',
            DATETIME '2022-01-01 12:30:00',
            JSON '{{\"a\":1}}',
            [1, 2, 3],
            STRUCT(7 AS a, 'seven' AS b)
        )"
    ))
    .await;

    assert_eq!(env.run_full_sync(&source_opts, &sync_opts(1000)).await, 1);

    // `b` is omitted here because SurrealDB bytes have no serde_json representation;
    // it is asserted separately below.
    let rows = env.fetch_rows_omitting(&table, &["id", "b"]).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    assert_eq!(row["s"], "hello", "STRING");
    assert_eq!(row["i"], 42, "INT64");
    assert_eq!(row["f"], 1.5, "FLOAT64");
    assert_eq!(row["ok"], true, "BOOL");

    // NUMERIC / BIGNUMERIC land as SurrealDB decimals; compare numerically so the
    // assertion does not depend on how the SDK renders them.
    let numeric = row["n"].as_f64().unwrap_or_else(|| {
        row["n"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("NUMERIC came back as {:?}", row["n"]))
    });
    assert!((numeric - 12.34).abs() < 1e-9, "NUMERIC: {numeric}");

    // ARRAY<INT64> keeps element order and integer typing.
    let arr = row["arr"].as_array().expect("ARRAY should be an array");
    assert_eq!(arr.len(), 3, "ARRAY length");
    assert_eq!(arr[0], 1);
    assert_eq!(arr[2], 3);

    // STRUCT becomes a nested object keyed by the declared field names.
    let st = row["st"].as_object().expect("STRUCT should be an object");
    assert_eq!(st["a"], 7, "STRUCT.a");
    assert_eq!(st["b"], "seven", "STRUCT.b");

    // BYTES arrives base64-encoded and must decode back to the original four bytes.
    let bytes = env.fetch_bytes_column(&table, "b").await;
    assert_eq!(bytes, vec![vec![0xDEu8, 0xAD, 0xBE, 0xEF]], "BYTES");

    // The temporal columns must all be present and non-null; their exact SurrealDB
    // rendering differs between SDK majors, so the precise values are asserted in
    // the crate's unit tests instead.
    for column in ["ts", "d", "t", "dt", "j"] {
        assert!(
            !row[column].is_null(),
            "column '{column}' should not be null: {row:?}"
        );
    }
}

#[tokio::test]
async fn bigquery_nulls_and_empty_arrays_round_trip() {
    let (env, mut source_opts) = BigQueryTestEnv::new(Vec::new(), vec!["id".to_string()]).await;
    let table = unique_table("bq_nulls", env.test_id);
    source_opts.tables = vec![table.clone()];

    let qualified = env.qualified(&table);
    env.execute(&format!(
        "CREATE TABLE {qualified} (id INT64, s STRING, n NUMERIC, arr ARRAY<INT64>, \
         st STRUCT<a INT64>)"
    ))
    .await;
    env.execute(&format!("INSERT INTO {qualified} (id) VALUES (1)"))
        .await;

    assert_eq!(env.run_full_sync(&source_opts, &sync_opts(1000)).await, 1);

    let rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    assert!(row["s"].is_null(), "NULL STRING: {:?}", row["s"]);
    assert!(row["n"].is_null(), "NULL NUMERIC: {:?}", row["n"]);
    assert!(row["st"].is_null(), "NULL STRUCT: {:?}", row["st"]);

    // BigQuery has no null arrays: an unset ARRAY column comes back empty.
    let arr = row["arr"]
        .as_array()
        .expect("unset ARRAY should be an array");
    assert!(arr.is_empty(), "unset ARRAY should be empty: {arr:?}");
}
