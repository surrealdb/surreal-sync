//! Ingestion against a **real** BigQuery project (credential-gated).
//!
//! The emulator that backs the rest of `tests/bigquery/` disables authentication
//! entirely, so nothing else in the suite exercises the service-account JWT →
//! OAuth2 token exchange, Google's own error envelopes, or the real API's wire
//! encodings. This test does, and skips cleanly when no account is configured —
//! the same pattern as `tests/snowflake/snowflake_full_sync_lib.rs`.
//!
//! It only **reads**, so a service account with `roles/bigquery.dataViewer` and
//! `roles/bigquery.jobUser` is enough. Point it at an existing table:
//!
//!   BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE,
//!   GOOGLE_APPLICATION_CREDENTIALS (optionally BIGQUERY_LOCATION), then:
//!     cargo nextest run -E 'test(bigquery_real_account)'

use surreal_sync::testing::shared_containers::shared_surrealdb;
use surreal_sync::testing::surreal::{connect_auto, is_v3};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_bigquery::from_bigquery::client::BigQueryClient;
use surreal_sync_bigquery::from_bigquery::full_sync::run_full_sync;
use surreal_sync_bigquery::from_bigquery::{
    SourceOpts, SyncOpts, DEFAULT_API_ENDPOINT, DEFAULT_PAGE_SIZE,
};

/// Read the required env vars, or return `None` (skip) if any is unset.
fn real_account_opts() -> Option<(SourceOpts, String)> {
    let project_id = std::env::var("BIGQUERY_PROJECT_ID").ok()?;
    let dataset = std::env::var("BIGQUERY_DATASET").ok()?;
    let table = std::env::var("BIGQUERY_TABLE").ok()?;
    let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok()?;

    let credentials_json = std::fs::read_to_string(&credentials_path).unwrap_or_else(|e| {
        panic!("failed to read GOOGLE_APPLICATION_CREDENTIALS {credentials_path}: {e}")
    });

    Some((
        SourceOpts {
            project_id,
            dataset,
            job_project_id: std::env::var("BIGQUERY_BILLING_PROJECT_ID").ok(),
            credentials_json: Some(credentials_json),
            location: std::env::var("BIGQUERY_LOCATION").ok(),
            api_endpoint: DEFAULT_API_ENDPOINT.to_string(),
            tables: vec![table.clone()],
            id_columns: Vec::new(),
            page_size: DEFAULT_PAGE_SIZE,
        },
        table,
    ))
}

#[tokio::test]
async fn bigquery_real_account_full_sync_reads_a_table() {
    let Some((source_opts, table)) = real_account_opts() else {
        eprintln!(
            "skipping bigquery_real_account_full_sync_reads_a_table: \
             BIGQUERY_PROJECT_ID / BIGQUERY_DATASET / BIGQUERY_TABLE / \
             GOOGLE_APPLICATION_CREDENTIALS not set"
        );
        return;
    };

    let client = BigQueryClient::new(&source_opts).expect("failed to build BigQuery client");

    let test_id = generate_test_id();
    let sdb = shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &sdb.ws_endpoint());
    let conn = connect_auto(&config)
        .await
        .expect("failed to connect to SurrealDB");

    let ns = config.surreal_namespace.clone();
    let db = config.surreal_database.clone();
    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let sunk = if is_v3(&conn) {
        let opts = surreal_sync_surreal::v3::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal_sync_surreal::v3::surreal_connect(&opts, &ns, &db)
            .await
            .expect("v3 sink connect failed");
        let sink = surreal_sync_surreal::v3::Surreal3Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v3 ingestion failed")
    } else {
        let opts = surreal_sync_surreal::v2::SurrealOpts {
            surreal_endpoint: sdb.ws_endpoint(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        };
        let surreal = surreal_sync_surreal::v2::surreal_connect(&opts, &ns, &db)
            .await
            .expect("v2 sink connect failed");
        let sink = surreal_sync_surreal::v2::Surreal2Sink::new(surreal);
        run_full_sync(&client, &sink, &source_opts, &sync_opts)
            .await
            .expect("v2 ingestion failed")
    };

    assert!(
        sunk > 0,
        "expected at least one row from {table}; point BIGQUERY_TABLE at a non-empty table"
    );
}
