//! Result-page pagination coverage.
//!
//! The source reads through `jobs.getQueryResults` with an explicit `maxResults`
//! so large tables never materialize in memory. This test seeds more rows than one
//! page holds and asserts every row still arrives, which is the only guard on the
//! `pageToken` loop in `QueryStream::next_batch`.

use crate::common::{sync_opts, unique_table, BigQueryTestEnv};

#[tokio::test]
async fn bigquery_full_sync_follows_page_tokens() {
    let (env, mut source_opts) = BigQueryTestEnv::new(Vec::new(), vec!["id".to_string()]).await;
    let table = unique_table("bq_paged", env.test_id);
    source_opts.tables = vec![table.clone()];
    // Four rows per page over twenty-five rows: seven pages, with a partial last one.
    source_opts.page_size = 4;

    let qualified = env.qualified(&table);
    env.execute(&format!("CREATE TABLE {qualified} (id INT64, v STRING)"))
        .await;

    const ROWS: usize = 25;
    let values: Vec<String> = (0..ROWS).map(|i| format!("({i}, 'row{i}')")).collect();
    env.execute(&format!(
        "INSERT INTO {qualified} (id, v) VALUES {}",
        values.join(", ")
    ))
    .await;

    // A batch size that is not a divisor of the page size, so chunk boundaries and
    // page boundaries deliberately fall out of step.
    let sunk = env.run_full_sync(&source_opts, &sync_opts(3)).await;
    assert_eq!(sunk, ROWS, "every paged row should be sunk");

    let rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), ROWS, "every paged row should reach SurrealDB");

    let mut seen: Vec<String> = rows
        .iter()
        .map(|r| r["v"].as_str().expect("v should be a string").to_string())
        .collect();
    seen.sort();
    let mut expected: Vec<String> = (0..ROWS).map(|i| format!("row{i}")).collect();
    expected.sort();
    assert_eq!(seen, expected, "no row may be dropped or duplicated");
}
