//! End-to-end integration tests for CSV HTTP/HTTPS import.
//!
//! Works with both SurrealDB v2 and v3 servers. The server version is
//! auto-detected from the `SURREALDB_IMAGE` env var.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use surreal_sync_csv_source::{sync, Config};
use surreal_version::testing::SurrealDbContainer;
use surreal_version::SurrealMajorVersion;
use surrealdb3::types::SurrealValue;
use tokio::net::TcpListener;
use tower::ServiceBuilder;

const TEST_CSV_DATA: &str = "id,name,age,active
1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true
4,Diana,28,false
5,Eve,32,true";

// ---------------------------------------------------------------------------
// HTTP test server
// ---------------------------------------------------------------------------

async fn serve_csv() -> Response {
    (StatusCode::OK, [("Content-Type", "text/csv")], TEST_CSV_DATA).into_response()
}

async fn serve_csv_with_path(Path(filename): Path<String>) -> Response {
    if filename == "test.csv" {
        (StatusCode::OK, [("Content-Type", "text/csv")], TEST_CSV_DATA).into_response()
    } else {
        (StatusCode::NOT_FOUND, "File not found").into_response()
    }
}

async fn start_test_server() -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
    let app = Router::new()
        .route("/test.csv", get(serve_csv))
        .route("/data/:filename", get(serve_csv_with_path))
        .layer(ServiceBuilder::new());

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Ok((base_url, server_handle))
}

// ---------------------------------------------------------------------------
// V2 helpers
// ---------------------------------------------------------------------------

async fn setup_v2(
    endpoint: &str,
    namespace: &str,
    database: &str,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    let surreal = surrealdb::engine::any::connect(endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns(namespace).use_db(database).await?;
    Ok(surreal)
}

async fn cleanup_ns_v2(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    namespace: &str,
) -> anyhow::Result<()> {
    let query = format!("REMOVE NAMESPACE IF EXISTS {namespace}");
    surreal.query(query).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// V3 helpers
// ---------------------------------------------------------------------------

async fn setup_v3(
    endpoint: &str,
    namespace: &str,
    database: &str,
) -> anyhow::Result<surrealdb3::Surreal<surrealdb3::engine::any::Any>> {
    let surreal = surrealdb3::engine::any::connect(endpoint).await?;
    surreal
        .signin(surrealdb3::opt::auth::Root {
            username: "root".to_string(),
            password: "root".to_string(),
        })
        .await?;
    surreal.use_ns(namespace).use_db(database).await?;
    Ok(surreal)
}

async fn cleanup_ns_v3(
    surreal: &surrealdb3::Surreal<surrealdb3::engine::any::Any>,
    namespace: &str,
) -> anyhow::Result<()> {
    let query = format!("REMOVE NAMESPACE IF EXISTS {namespace}");
    surreal.query(query).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_csv_http_import() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let mut db = SurrealDbContainer::new("test-csv-http-import");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();

    let endpoint = db.ws_endpoint();
    let namespace = "test_csv_http";
    let database = "test_db";
    let table = "users";

    let (base_url, server_handle) = start_test_server().await.unwrap();
    let csv_url = format!("{base_url}/test.csv");

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

    match db.detected_version {
        Some(SurrealMajorVersion::V3) => {
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v3(&surreal, namespace).await.unwrap();
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_ok(), "CSV import should succeed: {result:?}");

            #[derive(Debug, SurrealValue)]
            #[surreal(crate = "surrealdb3::types")]
            struct User {
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal.query(format!("SELECT name, age, active FROM {table}")).await.unwrap();
            let users: Vec<User> = response.take(0).unwrap();

            assert_eq!(users.len(), 5, "Should have imported 5 records");
            assert_eq!(users[0].name, "Alice");
            assert_eq!(users[0].age, 30);
            assert!(users[0].active);
            assert_eq!(users[1].name, "Bob");
            assert_eq!(users[4].name, "Eve");

            cleanup_ns_v3(&surreal, namespace).await.unwrap();
        }
        _ => {
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v2(&surreal, namespace).await.unwrap();
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_ok(), "CSV import should succeed: {result:?}");

            #[derive(Debug, serde::Deserialize)]
            struct User {
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal.query(format!("SELECT name, age, active FROM {table}")).await.unwrap();
            let users: Vec<User> = response.take(0).unwrap();

            assert_eq!(users.len(), 5, "Should have imported 5 records");
            assert_eq!(users[0].name, "Alice");
            assert_eq!(users[0].age, 30);
            assert!(users[0].active);
            assert_eq!(users[1].name, "Bob");
            assert_eq!(users[4].name, "Eve");

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_csv_http_import_with_path() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let mut db = SurrealDbContainer::new("test-csv-http-path");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();

    let endpoint = db.ws_endpoint();
    let namespace = "test_csv_http_path";
    let database = "test_db";
    let table = "people";

    let (base_url, server_handle) = start_test_server().await.unwrap();
    let csv_url = format!("{base_url}/data/test.csv");

    let config = Config {
        sources: vec![],
        files: vec![],
        s3_uris: vec![],
        http_uris: vec![csv_url],
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

    match db.detected_version {
        Some(SurrealMajorVersion::V3) => {
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v3(&surreal, namespace).await.unwrap();
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_ok(), "CSV import should succeed: {result:?}");

            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();

            #[derive(Debug, PartialEq, SurrealValue)]
            #[surreal(crate = "surrealdb3::types")]
            struct CountResult {
                count: i64,
            }

            let mut count_response = surreal
                .query(format!("SELECT count() AS count FROM {table} GROUP ALL"))
                .await
                .unwrap();
            let counts: Vec<CountResult> = count_response.take(0).unwrap();
            assert_eq!(counts.len(), 1);
            assert_eq!(counts[0].count, 5);

            #[derive(Debug, PartialEq, SurrealValue)]
            #[surreal(crate = "surrealdb3::types")]
            struct Person {
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal
                .query(format!("SELECT name, age, active FROM {table}"))
                .await
                .unwrap();
            let mut people: Vec<Person> = response.take(0).unwrap();
            assert_eq!(people.len(), 5);

            people.sort_by(|a, b| a.name.cmp(&b.name));
            assert_eq!(people[0].name, "Alice");
            assert_eq!(people[0].age, 30);
            assert!(people[0].active);
            assert_eq!(people[1].name, "Bob");
            assert_eq!(people[4].name, "Eve");

            cleanup_ns_v3(&surreal, namespace).await.unwrap();
        }
        _ => {
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v2(&surreal, namespace).await.unwrap();
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_ok(), "CSV import should succeed: {result:?}");

            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();

            #[derive(Debug, serde::Deserialize)]
            struct CountResult {
                count: i64,
            }

            let mut count_response = surreal
                .query(format!("SELECT count() AS count FROM {table} GROUP ALL"))
                .await
                .unwrap();
            let counts: Vec<CountResult> = count_response.take(0).unwrap();
            assert_eq!(counts.len(), 1);
            assert_eq!(counts[0].count, 5);

            #[derive(Debug, PartialEq, serde::Deserialize)]
            struct Person {
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal
                .query(format!("SELECT name, age, active FROM {table}"))
                .await
                .unwrap();
            let mut people: Vec<Person> = response.take(0).unwrap();
            assert_eq!(people.len(), 5);

            people.sort_by(|a, b| a.name.cmp(&b.name));
            let expected = vec![
                Person { name: "Alice".to_string(), age: 30, active: true },
                Person { name: "Bob".to_string(), age: 25, active: false },
                Person { name: "Charlie".to_string(), age: 35, active: true },
                Person { name: "Diana".to_string(), age: 28, active: false },
                Person { name: "Eve".to_string(), age: 32, active: true },
            ];
            assert_eq!(people, expected);

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_csv_http_404_error() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let mut db = SurrealDbContainer::new("test-csv-http-404");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();

    let endpoint = db.ws_endpoint();
    let namespace = "test_csv_http_404";
    let database = "test_db";

    let (base_url, server_handle) = start_test_server().await.unwrap();
    let csv_url = format!("{base_url}/nonexistent.csv");

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

    match db.detected_version {
        Some(SurrealMajorVersion::V3) => {
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v3(&surreal, namespace).await.unwrap();
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_err(), "Should fail with HTTP 404");
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("404") || error_msg.contains("HTTP request failed"),
                "Error should mention HTTP failure: {error_msg}"
            );

            cleanup_ns_v3(&surreal, namespace).await.unwrap();
        }
        _ => {
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v2(&surreal, namespace).await.unwrap();
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_err(), "Should fail with HTTP 404");
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("404") || error_msg.contains("HTTP request failed"),
                "Error should mention HTTP failure: {error_msg}"
            );

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }

    server_handle.abort();
}
