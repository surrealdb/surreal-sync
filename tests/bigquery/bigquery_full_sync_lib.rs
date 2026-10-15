//! End-to-end ingestion test for the BigQuery source (library entry points).
//!
//! Runs against the goccy BigQuery emulator started by
//! `surreal_sync::testing::shared_containers::shared_bigquery`, so it needs no
//! credentials and runs on every PR. The emulator disables authentication, which
//! is why the real service-account path is covered separately by
//! `bigquery_real_account_lib.rs`.

use crate::common::{sync_opts, unique_table, BigQueryTestEnv};

#[tokio::test]
async fn bigquery_full_sync_ingests_selected_table() {
    let (env, mut source_opts) = BigQueryTestEnv::new(Vec::new(), vec!["id".to_string()]).await;
    let table = unique_table("bq_full_sync", env.test_id);
    source_opts.tables = vec![table.clone()];

    let qualified = env.qualified(&table);
    env.execute(&format!(
        "CREATE TABLE {qualified} (id INT64, name STRING, amount NUMERIC, active BOOL)"
    ))
    .await;
    env.execute(&format!(
        "INSERT INTO {qualified} (id, name, amount, active) \
         VALUES (1, 'alice', 10.50, true), (2, 'bob', 20.00, false)"
    ))
    .await;

    let sunk = env.run_full_sync(&source_opts, &sync_opts(1000)).await;
    assert_eq!(sunk, 2, "expected 2 ingested rows");

    let mut rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), 2, "expected 2 rows in SurrealDB table {table}");

    rows.sort_by_key(|r| r["name"].as_str().unwrap_or_default().to_string());
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[1]["name"], "bob");
    assert_eq!(rows[0]["active"], true);
    assert_eq!(rows[1]["active"], false);

    // The id column becomes the record ID, so it must not also appear as a field.
    assert!(
        rows[0]
            .get("id")
            .is_none_or(|v| v.is_null() || !v.is_number()),
        "id should not be duplicated as a plain field: {:?}",
        rows[0]
    );
}

#[tokio::test]
async fn bigquery_table_name_case_is_preserved() {
    // BigQuery identifiers are case-sensitive; unlike the Snowflake source we must
    // not fold them to upper case on the way into SurrealDB.
    let (env, mut source_opts) = BigQueryTestEnv::new(Vec::new(), Vec::new()).await;
    let table = unique_table("bq_MixedCase", env.test_id);
    source_opts.tables = vec![table.clone()];

    let qualified = env.qualified(&table);
    env.execute(&format!("CREATE TABLE {qualified} (v STRING)"))
        .await;
    env.execute(&format!("INSERT INTO {qualified} (v) VALUES ('x')"))
        .await;

    assert_eq!(env.run_full_sync(&source_opts, &sync_opts(1000)).await, 1);

    let rows = env.fetch_rows(&format!("`{table}`")).await;
    assert_eq!(rows.len(), 1, "expected the verbatim table name {table}");
    assert_eq!(rows[0]["v"], "x");
}

#[tokio::test]
async fn bigquery_discovers_every_table_in_the_dataset() {
    // With no --tables, the source discovers tables through datasets.tables.list.
    // Assert the discovery call itself rather than syncing the whole dataset: other
    // tests in this process share the dataset, and a dataset-wide sync would pick
    // up their tables too.
    use surreal_sync_bigquery::from_bigquery::autoconf::list_tables;

    let (env, source_opts) = BigQueryTestEnv::new(Vec::new(), Vec::new()).await;
    let first = unique_table("bq_discover_a", env.test_id);
    let second = unique_table("bq_discover_b", env.test_id);

    for table in [&first, &second] {
        let qualified = env.qualified(table);
        env.execute(&format!("CREATE TABLE {qualified} (k INT64, v STRING)"))
            .await;
        env.execute(&format!(
            "INSERT INTO {qualified} (k, v) VALUES (1, '{table}')"
        ))
        .await;
    }

    let discovered = list_tables(&env.client)
        .await
        .expect("table discovery failed");
    assert!(
        discovered.contains(&first),
        "discovery should list {first}, got {discovered:?}"
    );
    assert!(
        discovered.contains(&second),
        "discovery should list {second}, got {discovered:?}"
    );

    // And the discovered names really are ingestable.
    let mut opts = source_opts.clone();
    opts.tables = vec![first.clone(), second.clone()];
    assert_eq!(env.run_full_sync(&opts, &sync_opts(1000)).await, 2);

    for table in [&first, &second] {
        let rows = env.fetch_rows(table).await;
        assert_eq!(rows.len(), 1, "expected to ingest {table}");
        assert_eq!(rows[0]["v"], table.as_str());
    }
}

#[tokio::test]
async fn bigquery_composite_id_columns_build_an_array_record_id() {
    let (env, mut source_opts) = BigQueryTestEnv::new(
        Vec::new(),
        vec!["order_id".to_string(), "line_no".to_string()],
    )
    .await;
    let table = unique_table("bq_composite", env.test_id);
    source_opts.tables = vec![table.clone()];

    let qualified = env.qualified(&table);
    env.execute(&format!(
        "CREATE TABLE {qualified} (order_id INT64, line_no INT64, sku STRING)"
    ))
    .await;
    env.execute(&format!(
        "INSERT INTO {qualified} (order_id, line_no, sku) VALUES (7, 1, 'widget')"
    ))
    .await;

    assert_eq!(env.run_full_sync(&source_opts, &sync_opts(1000)).await, 1);

    let rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["sku"], "widget");
    // Both key columns move into the record ID rather than staying as fields.
    assert!(rows[0].get("order_id").is_none());
    assert!(rows[0].get("line_no").is_none());
}
