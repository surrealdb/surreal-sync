//! End-to-end integration tests for CSV HTTP/HTTPS import
//!
//! These tests verify that CSV files can be fetched from HTTP servers and imported into SurrealDB.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use surreal2_sink::Surreal2Sink;
use surreal_sync_csv_source::{sync, Config};
use tokio::net::TcpListener;
use tower::ServiceBuilder;

/// Test CSV data
const TEST_CSV_DATA: &str = "id,name,age,active
1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true
4,Diana,28,false
5,Eve,32,true";

/// Namespace cleanup helper for SurrealDB
async fn cleanup_namespace(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    namespace: &str,
) -> anyhow::Result<()> {
    // Remove the namespace
    let query = format!("REMOVE NAMESPACE IF EXISTS {namespace}");
    surreal.query(query).await?;
    Ok(())
}

/// Setup SurrealDB connection for tests
async fn setup_surrealdb(
    namespace: &str,
    database: &str,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    let surreal = surrealdb::engine::any::connect("ws://surrealdb:8000").await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;

    surreal.use_ns(namespace).use_db(database).await?;

    Ok(surreal)
}

/// HTTP handler that serves CSV data
async fn serve_csv() -> Response {
    (
        StatusCode::OK,
        [("Content-Type", "text/csv")],
        TEST_CSV_DATA,
    )
        .into_response()
}

/// HTTP handler that serves CSV data at a dynamic path
async fn serve_csv_with_path(Path(filename): Path<String>) -> Response {
    if filename == "test.csv" {
        (
            StatusCode::OK,
            [("Content-Type", "text/csv")],
            TEST_CSV_DATA,
        )
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, "File not found").into_response()
    }
}

/// Start a test HTTP server serving CSV files
async fn start_test_server() -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
    let app = Router::new()
        .route("/test.csv", get(serve_csv))
        .route("/data/:filename", get(serve_csv_with_path))
        .layer(ServiceBuilder::new());

    // Bind to any available port
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");

    // Spawn server in background
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok((base_url, server_handle))
}

#[tokio::test]
async fn test_csv_http_import() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let namespace = "test_csv_http";
    let database = "test_db";
    let table = "users";

    // Setup SurrealDB connection for cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    // Cleanup before test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Reconnect after cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    let sink = Surreal2Sink::new(surreal.clone());

    // Start test HTTP server
    let (base_url, server_handle) = start_test_server().await.unwrap();

    let csv_url = format!("{base_url}/test.csv");
    tracing::info!("Test server started at: {}", base_url);
    tracing::info!("CSV URL: {}", csv_url);

    // Configure CSV import
    let config = Config {
        sources: vec![],
        files: vec![],
        s3_uris: vec![],
        http_uris: vec![csv_url.clone()],
        table: table.to_string(),
        batch_size: 100,
        has_headers: true,
        delimiter: b',',
        id_field: Some("id".to_string()),
        column_names: None,
        emit_metrics: None,
        dry_run: false,
        schema: None,
    };

    // Run CSV import
    let result = sync(&sink, config).await;
    assert!(result.is_ok(), "CSV import should succeed: {result:?}");

    // Verify data was imported correctly
    let query = format!("SELECT name, age, active FROM {table}");
    let mut response = surreal.query(query).await.unwrap();

    #[derive(Debug, serde::Deserialize)]
    struct User {
        name: String,
        age: i64,
        active: bool,
    }

    let users: Vec<User> = response.take(0).unwrap();

    assert_eq!(users.len(), 5, "Should have imported 5 records");
    assert_eq!(users[0].name, "Alice");
    assert_eq!(users[0].age, 30);
    assert!(users[0].active);
    assert_eq!(users[1].name, "Bob");
    assert_eq!(users[4].name, "Eve");

    // Cleanup after test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Stop test server
    server_handle.abort();
}

#[tokio::test]
async fn test_csv_http_import_with_path() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let namespace = "test_csv_http_path";
    let database = "test_db";
    let table = "people";

    // Setup SurrealDB connection for cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    // Cleanup before test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Reconnect after cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    let sink = Surreal2Sink::new(surreal.clone());

    // Start test HTTP server
    let (base_url, server_handle) = start_test_server().await.unwrap();

    let csv_url = format!("{base_url}/data/test.csv");
    tracing::info!("Test server started at: {}", base_url);
    tracing::info!("CSV URL: {}", csv_url);

    // Configure CSV import
    let config = Config {
        sources: vec![],
        files: vec![],
        s3_uris: vec![],
        http_uris: vec![csv_url],
        table: table.to_string(),
        batch_size: 100,
        has_headers: true,
        delimiter: b',',
        id_field: Some("id".to_string()), // Use id field from CSV
        column_names: None,
        emit_metrics: None,
        dry_run: false,
        schema: None,
    };

    // Run CSV import
    let result = sync(&sink, config).await;
    assert!(result.is_ok(), "CSV import should succeed: {result:?}");

    // Reconnect to verify data (ensures we're seeing fresh data)
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    // First, try getting count
    let count_query = format!("SELECT count() AS count FROM {table} GROUP ALL");
    let mut count_response = surreal.query(count_query).await.unwrap();

    #[derive(Debug, serde::Deserialize)]
    struct CountResult {
        count: i64,
    }

    let counts: Vec<CountResult> = count_response.take(0).unwrap();
    tracing::info!("Count query result: {:?}", counts);
    assert_eq!(counts.len(), 1, "Should have exactly one count result");
    assert_eq!(counts[0].count, 5, "Should have 5 records");

    // Verify data was imported
    let query = format!("SELECT name, age, active FROM {table}");
    let mut response = surreal.query(query).await.unwrap();

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Person {
        name: String,
        age: i64,
        active: bool,
    }

    let mut people: Vec<Person> = response.take(0).unwrap();
    tracing::info!("People query result: {:?}", people);

    assert_eq!(people.len(), 5, "Should have imported 5 records");

    // Sort by name for consistent comparison
    people.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify all Person records match expected data
    let expected = vec![
        Person {
            name: "Alice".to_string(),
            age: 30,
            active: true,
        },
        Person {
            name: "Bob".to_string(),
            age: 25,
            active: false,
        },
        Person {
            name: "Charlie".to_string(),
            age: 35,
            active: true,
        },
        Person {
            name: "Diana".to_string(),
            age: 28,
            active: false,
        },
        Person {
            name: "Eve".to_string(),
            age: 32,
            active: true,
        },
    ];
    assert_eq!(people, expected);

    // Cleanup after test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Stop test server
    server_handle.abort();
}

#[tokio::test]
async fn test_csv_http_404_error() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let namespace = "test_csv_http_404";
    let database = "test_db";

    // Setup SurrealDB connection for cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    // Cleanup before test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Reconnect after cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    let sink = Surreal2Sink::new(surreal.clone());

    // Start test HTTP server
    let (base_url, server_handle) = start_test_server().await.unwrap();

    let csv_url = format!("{base_url}/nonexistent.csv");

    // Configure CSV import with non-existent file
    let config = Config {
        sources: vec![],
        files: vec![],
        s3_uris: vec![],
        http_uris: vec![csv_url],
        table: "users".to_string(),
        batch_size: 100,
        has_headers: true,
        delimiter: b',',
        id_field: None,
        column_names: None,
        emit_metrics: None,
        dry_run: false,
        schema: None,
    };

    // Run CSV import - should fail with HTTP 404
    let result = sync(&sink, config).await;
    assert!(result.is_err(), "Should fail with HTTP 404");

    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(
        error_msg.contains("404") || error_msg.contains("HTTP request failed"),
        "Error should mention HTTP failure: {error_msg}"
    );

    // Cleanup after test
    cleanup_namespace(&surreal, namespace).await.unwrap();

    // Stop test server
    server_handle.abort();
}
