//! CLI e2e: `surreal-sync from bigquery` against the emulator.
//!
//! Exercises the stock binary — argument parsing, SurrealDB version auto-detect,
//! and the anonymous-auth path taken when `--api-endpoint` is not Google's.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};

use crate::common::{unique_table, BigQueryTestEnv};

#[tokio::test]
async fn bigquery_full_sync_cli_ingests_a_table() -> Result<(), Box<dyn std::error::Error>> {
    let (env, source_opts) = BigQueryTestEnv::new(Vec::new(), Vec::new()).await;
    let table = unique_table("bq_cli", env.test_id);

    let qualified = env.qualified(&table);
    env.execute(&format!("CREATE TABLE {qualified} (id INT64, name STRING)"))
        .await;
    env.execute(&format!(
        "INSERT INTO {qualified} (id, name) VALUES (1, 'alice'), (2, 'bob')"
    ))
    .await;

    let args = [
        "from",
        "bigquery",
        "--project-id",
        &source_opts.project_id,
        "--dataset",
        &source_opts.dataset,
        "--api-endpoint",
        &env.api_endpoint,
        "--tables",
        &table,
        "--id-columns",
        "id",
        "--surreal-endpoint",
        &env.config.surreal_endpoint,
        "--to-namespace",
        &env.config.surreal_namespace,
        "--to-database",
        &env.config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "BigQuery full sync CLI");

    let rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), 2, "expected 2 rows via the CLI, got {rows:?}");

    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r["name"].as_str().unwrap_or_default().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["alice".to_string(), "bob".to_string()]);
    Ok(())
}

#[tokio::test]
async fn bigquery_dry_run_cli_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let (env, source_opts) = BigQueryTestEnv::new(Vec::new(), Vec::new()).await;
    let table = unique_table("bq_cli_dry", env.test_id);

    let qualified = env.qualified(&table);
    env.execute(&format!("CREATE TABLE {qualified} (id INT64)"))
        .await;
    env.execute(&format!("INSERT INTO {qualified} (id) VALUES (1)"))
        .await;

    let args = [
        "from",
        "bigquery",
        "--project-id",
        &source_opts.project_id,
        "--dataset",
        &source_opts.dataset,
        "--api-endpoint",
        &env.api_endpoint,
        "--tables",
        &table,
        "--surreal-endpoint",
        &env.config.surreal_endpoint,
        "--to-namespace",
        &env.config.surreal_namespace,
        "--to-database",
        &env.config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--dry-run",
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "BigQuery dry-run CLI");

    let rows = env.fetch_rows(&table).await;
    assert!(rows.is_empty(), "dry-run must not write rows: {rows:?}");
    Ok(())
}

#[tokio::test]
async fn bigquery_cli_refuses_the_public_api_without_credentials(
) -> Result<(), Box<dyn std::error::Error>> {
    // Reading zero rows because a key was missing would be a silent, confusing
    // success; the client must fail loudly instead.
    let args = [
        "from",
        "bigquery",
        "--project-id",
        "some-project",
        "--dataset",
        "some_dataset",
        "--to-namespace",
        "ns",
        "--to-database",
        "db",
    ];
    let output = execute_surreal_sync(&args)?;
    assert!(
        !output.status.success(),
        "expected a failure without credentials"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("credentials") || combined.contains("--credentials-path"),
        "error should name the missing credentials, got: {combined}"
    );
    Ok(())
}
