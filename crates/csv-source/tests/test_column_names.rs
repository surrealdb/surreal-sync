//! Tests for column names feature when has_headers is false.
//!
//! Works with both SurrealDB v2 and v3 servers.

use std::io::Write;
use surreal_sync_csv_source::{sync, Config};
use surreal_version::testing::SurrealDbContainer;
use surreal_version::SurrealMajorVersion;
use surrealdb3::types::SurrealValue;
use tempfile::NamedTempFile;

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
    surreal
        .query(format!("REMOVE NAMESPACE IF EXISTS {namespace}"))
        .await?;
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
    surreal
        .query(format!("REMOVE NAMESPACE IF EXISTS {namespace}"))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_csv_with_custom_column_names() {
    let mut db = SurrealDbContainer::new("test-csv-custom-cols");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();
    let endpoint = db.ws_endpoint();

    let csv_data = "1,Alice,30,true\n2,Bob,25,false\n3,Charlie,35,true";
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_column_names";
    let database = "test_db";
    let table = "users";

    let config = Config {
        sources: vec![],
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
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
                user_id: i64,
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal
                .query(format!("SELECT user_id, name, age, active FROM {table}"))
                .await
                .unwrap();
            let users: Vec<User> = response.take(0).unwrap();

            assert_eq!(users.len(), 3, "Should have imported 3 records");
            assert_eq!(users[0].user_id, 1);
            assert_eq!(users[0].name, "Alice");
            assert_eq!(users[0].age, 30);
            assert!(users[0].active);

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
                user_id: i64,
                name: String,
                age: i64,
                active: bool,
            }

            let mut response = surreal
                .query(format!("SELECT user_id, name, age, active FROM {table}"))
                .await
                .unwrap();
            let users: Vec<User> = response.take(0).unwrap();

            assert_eq!(users.len(), 3, "Should have imported 3 records");
            assert_eq!(users[0].user_id, 1);
            assert_eq!(users[0].name, "Alice");
            assert_eq!(users[0].age, 30);
            assert!(users[0].active);

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_csv_column_count_mismatch_error() {
    let mut db = SurrealDbContainer::new("test-csv-col-mismatch");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();
    let endpoint = db.ws_endpoint();

    let csv_data = "1,Alice,30\n2,Bob,25\n3,Charlie,35";
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_column_mismatch";
    let database = "test_db";
    let table = "users";

    let config = Config {
        sources: vec![],
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        has_headers: false,
        delimiter: b',',
        id_field: Some("user_id".to_string()),
        column_names: Some(vec![
            "user_id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "extra".to_string(),
        ]),
        emit_metrics: None,
        dry_run: false,
        schema: None,
    };

    // Error comes from column count check before any DB interaction, so SDK version doesn't matter.
    match db.detected_version {
        Some(SurrealMajorVersion::V3) => {
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v3(&surreal, namespace).await.unwrap();
            let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_err(), "Should fail with column count mismatch");
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("Column count mismatch"),
                "Error should mention column count mismatch: {error_msg}"
            );

            cleanup_ns_v3(&surreal, namespace).await.unwrap();
        }
        _ => {
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v2(&surreal, namespace).await.unwrap();
            let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(result.is_err(), "Should fail with column count mismatch");
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("Column count mismatch"),
                "Error should mention column count mismatch: {error_msg}"
            );

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_csv_without_headers_auto_generated_names() {
    let mut db = SurrealDbContainer::new("test-csv-auto-cols");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();
    let endpoint = db.ws_endpoint();

    let csv_data = "1,Alice,30\n2,Bob,25\n3,Charlie,35";
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_auto_columns";
    let database = "test_db";
    let table = "data";

    let config = Config {
        sources: vec![],
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
        has_headers: false,
        delimiter: b',',
        id_field: Some("column_0".to_string()),
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
            struct Data {
                column_0: i64,
                column_1: String,
                column_2: i64,
            }

            let mut response = surreal
                .query(format!("SELECT column_0, column_1, column_2 FROM {table}"))
                .await
                .unwrap();
            let records: Vec<Data> = response.take(0).unwrap();

            assert_eq!(records.len(), 3, "Should have imported 3 records");
            assert_eq!(records[0].column_0, 1);
            assert_eq!(records[0].column_1, "Alice");
            assert_eq!(records[0].column_2, 30);

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
            struct Data {
                column_0: i64,
                column_1: String,
                column_2: i64,
            }

            let mut response = surreal
                .query(format!("SELECT column_0, column_1, column_2 FROM {table}"))
                .await
                .unwrap();
            let records: Vec<Data> = response.take(0).unwrap();

            assert_eq!(records.len(), 3, "Should have imported 3 records");
            assert_eq!(records[0].column_0, 1);
            assert_eq!(records[0].column_1, "Alice");
            assert_eq!(records[0].column_2, 30);

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_csv_column_count_mismatch_with_extra_columns_in_row() {
    let mut db = SurrealDbContainer::new("test-csv-extra-cols");
    db.start().unwrap();
    db.wait_until_ready(30).unwrap();
    let endpoint = db.ws_endpoint();

    let csv_data = "1,Alice,30\n2,Bob,25,extra_field";
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{csv_data}").unwrap();
    temp_file.flush().unwrap();

    let namespace = "test_extra_columns";
    let database = "test_db";
    let table = "users";

    let config = Config {
        sources: vec![],
        files: vec![temp_file.path().to_path_buf()],
        s3_uris: vec![],
        http_uris: vec![],
        table: table.to_string(),
        batch_size: 100,
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
        schema: None,
    };

    match db.detected_version {
        Some(SurrealMajorVersion::V3) => {
            let surreal = setup_v3(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v3(&surreal, namespace).await.unwrap();
            let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(
                result.is_err(),
                "Should fail with column count mismatch on row 2"
            );
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("Column count mismatch")
                    || error_msg.contains("found record with 4 fields")
                    || error_msg.contains("previous record has 3 fields"),
                "Error should mention column count mismatch: {error_msg}"
            );

            cleanup_ns_v3(&surreal, namespace).await.unwrap();
        }
        _ => {
            let surreal = setup_v2(&endpoint, namespace, database).await.unwrap();
            cleanup_ns_v2(&surreal, namespace).await.unwrap();
            let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

            let result = sync(&sink, config).await;
            assert!(
                result.is_err(),
                "Should fail with column count mismatch on row 2"
            );
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(
                error_msg.contains("Column count mismatch")
                    || error_msg.contains("found record with 4 fields")
                    || error_msg.contains("previous record has 3 fields"),
                "Error should mention column count mismatch: {error_msg}"
            );

            cleanup_ns_v2(&surreal, namespace).await.unwrap();
        }
    }
}
