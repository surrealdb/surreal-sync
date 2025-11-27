//! Tests for column names feature when has_headers is false

use std::io::Write;
use surreal_sync_csv::{sync, Config};
use tempfile::NamedTempFile;

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

/// Namespace cleanup helper for SurrealDB
async fn cleanup_namespace(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    namespace: &str,
) -> anyhow::Result<()> {
    let query = format!("REMOVE NAMESPACE IF EXISTS {namespace}");
    surreal.query(query).await?;
    Ok(())
}

#[tokio::test]
async fn test_csv_with_custom_column_names() {
    // CSV data without headers
    let csv_data = "1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true";

    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_column_names";
    let database = "test_db";
    let table = "users";

    // Setup and cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    cleanup_namespace(&surreal, namespace).await.unwrap();
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        namespace: namespace.to_string(),
        database: database.to_string(),
        surreal_opts: surreal_sync_csv::surreal::SurrealOpts {
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        },
        has_headers: false,
        delimiter: b',',
        id_field: Some("user_id".to_string()),
        column_names: Some(vec![
            "user_id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "active".to_string(),
        ]),
        emit_metrics: None,
        dry_run: false,
    };

    let result = sync(config).await;
    assert!(result.is_ok(), "CSV import should succeed: {result:?}");

    // Verify data
    let query = format!("SELECT user_id, name, age, active FROM {table}");
    let mut response = surreal.query(query).await.unwrap();

    #[derive(Debug, serde::Deserialize)]
    struct User {
        user_id: i64,
        name: String,
        age: i64,
        active: bool,
    }

    let users: Vec<User> = response.take(0).unwrap();

    assert_eq!(users.len(), 3, "Should have imported 3 records");
    assert_eq!(users[0].user_id, 1);
    assert_eq!(users[0].name, "Alice");
    assert_eq!(users[0].age, 30);
    assert!(users[0].active);

    // Cleanup
    cleanup_namespace(&surreal, namespace).await.unwrap();
}

#[tokio::test]
async fn test_csv_column_count_mismatch_error() {
    // CSV data with 3 columns
    let csv_data = "1,Alice,30
2,Bob,25
3,Charlie,35";

    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_column_mismatch";
    let database = "test_db";
    let table = "users";

    // Setup and cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    cleanup_namespace(&surreal, namespace).await.unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        namespace: namespace.to_string(),
        database: database.to_string(),
        surreal_opts: surreal_sync_csv::surreal::SurrealOpts {
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        },
        has_headers: false,
        delimiter: b',',
        id_field: Some("user_id".to_string()),
        // Specifying 4 columns but CSV has only 3
        column_names: Some(vec![
            "user_id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "extra".to_string(),
        ]),
        emit_metrics: None,
        dry_run: false,
    };

    let result = sync(config).await;
    assert!(result.is_err(), "Should fail with column count mismatch");

    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(
        error_msg.contains("Column count mismatch"),
        "Error should mention column count mismatch: {error_msg}"
    );

    // Cleanup
    cleanup_namespace(&surreal, namespace).await.unwrap();
}

#[tokio::test]
async fn test_csv_without_headers_auto_generated_names() {
    // CSV data without headers and no column_names specified
    let csv_data = "1,Alice,30
2,Bob,25
3,Charlie,35";

    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_auto_columns";
    let database = "test_db";
    let table = "data";

    // Setup and cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    cleanup_namespace(&surreal, namespace).await.unwrap();
    let surreal = setup_surrealdb(namespace, database).await.unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        namespace: namespace.to_string(),
        database: database.to_string(),
        surreal_opts: surreal_sync_csv::surreal::SurrealOpts {
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        },
        has_headers: false,
        delimiter: b',',
        id_field: Some("column_0".to_string()),
        column_names: None, // Let it auto-generate column_0, column_1, column_2
        emit_metrics: None,
        dry_run: false,
    };

    let result = sync(config).await;
    assert!(result.is_ok(), "CSV import should succeed: {result:?}");

    // Verify data with auto-generated column names
    let query = format!("SELECT column_0, column_1, column_2 FROM {table}");
    let mut response = surreal.query(query).await.unwrap();

    #[derive(Debug, serde::Deserialize)]
    struct Data {
        column_0: i64,
        column_1: String,
        column_2: i64,
    }

    let records: Vec<Data> = response.take(0).unwrap();

    assert_eq!(records.len(), 3, "Should have imported 3 records");
    assert_eq!(records[0].column_0, 1);
    assert_eq!(records[0].column_1, "Alice");
    assert_eq!(records[0].column_2, 30);

    // Cleanup
    cleanup_namespace(&surreal, namespace).await.unwrap();
}

#[tokio::test]
async fn test_csv_column_count_mismatch_with_extra_columns_in_row() {
    // First row has 3 columns, second row has 4 columns
    let csv_data = "1,Alice,30
2,Bob,25,extra_field";

    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_extra_columns";
    let database = "test_db";
    let table = "users";

    // Setup and cleanup
    let surreal = setup_surrealdb(namespace, database).await.unwrap();
    cleanup_namespace(&surreal, namespace).await.unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        namespace: namespace.to_string(),
        database: database.to_string(),
        surreal_opts: surreal_sync_csv::surreal::SurrealOpts {
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            surreal_username: "root".to_string(),
            surreal_password: "root".to_string(),
        },
        has_headers: false,
        delimiter: b',',
        id_field: Some("id".to_string()),
        column_names: Some(vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ]),
        emit_metrics: None,
        dry_run: false,
    };

    let result = sync(config).await;
    assert!(
        result.is_err(),
        "Should fail with column count mismatch on row 2"
    );

    let error_msg = format!("{:?}", result.unwrap_err());
    // The CSV library itself may detect the mismatch, or our validation will
    // Either error is acceptable as long as it mentions the mismatch
    assert!(
        error_msg.contains("Column count mismatch")
            || error_msg.contains("found record with 4 fields")
            || error_msg.contains("previous record has 3 fields"),
        "Error should mention column count mismatch: {error_msg}"
    );

    // Cleanup
    cleanup_namespace(&surreal, namespace).await.unwrap();
}
