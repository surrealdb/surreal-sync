//! End-to-end test for array handling in incremental sync
//!
//! This test traces the actual data flow through all components to identify
//! where array values might be lost.

use std::sync::Arc;
use surreal_sync_postgresql_trigger_source::IncrementalSource;
use sync_core::UniversalValue;
use tokio::sync::Mutex;

/// Comprehensive E2E test that traces array data through the entire incremental sync flow
#[tokio::test]
async fn test_incremental_sync_array_e2e() {
    // Connect to PostgreSQL
    let connection_string = std::env::var("POSTGRESQL_TEST_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgresql:5432/testdb".to_string());

    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .expect("Failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    let client = Arc::new(Mutex::new(client));

    // Step 1: Create table with TEXT[] column
    println!("=== STEP 1: Create table with TEXT[] column ===");
    {
        let client = client.lock().await;
        client
            .execute("DROP TABLE IF EXISTS test_array_e2e CASCADE", &[])
            .await
            .unwrap();
        client
            .execute("DROP TABLE IF EXISTS surreal_sync_changes CASCADE", &[])
            .await
            .unwrap();

        let create_ddl = "CREATE TABLE test_array_e2e (
            post_id TEXT PRIMARY KEY,
            title TEXT,
            post_categories TEXT[]
        )";
        println!("DDL: {create_ddl}");
        client.execute(create_ddl, &[]).await.unwrap();
    }

    // Step 2: Set up triggers using PostgresIncrementalSource
    println!("\n=== STEP 2: Set up triggers ===");
    let mut source =
        surreal_sync_postgresql_trigger_source::PostgresIncrementalSource::new(client.clone(), 0);

    source
        .setup_tracking(vec!["test_array_e2e".to_string()])
        .await
        .unwrap();
    println!("Triggers set up successfully");

    // Verify audit table exists
    {
        let client_guard = client.lock().await;
        let rows: Vec<tokio_postgres::Row> = client_guard
            .query(
                "SELECT 1 FROM information_schema.tables WHERE table_name = 'surreal_sync_changes'",
                &[],
            )
            .await
            .unwrap();
        assert!(!rows.is_empty(), "Audit table should exist");
        println!("Audit table exists: YES");
    }

    // Step 3: Insert data with array
    println!("\n=== STEP 3: Insert data with array ===");
    {
        let client = client.lock().await;
        let tags: Vec<String> = vec!["technology".to_string(), "tutorial".to_string()];

        let insert_sql =
            "INSERT INTO test_array_e2e (post_id, title, post_categories) VALUES ($1, $2, $3)";
        println!("Insert SQL: {insert_sql}");
        println!("Array value: {tags:?}");

        client
            .execute(insert_sql, &[&"post_001", &"Test Post", &tags])
            .await
            .expect("Insert failed");
        println!("Insert successful");
    }

    // Step 4: Verify data in source table
    println!("\n=== STEP 4: Verify data in source table ===");
    {
        let client = client.lock().await;
        let row = client
            .query_one(
                "SELECT post_id, title, post_categories FROM test_array_e2e WHERE post_id = 'post_001'",
                &[],
            )
            .await
            .expect("Failed to query source table");

        let post_id: String = row.get("post_id");
        let title: String = row.get("title");
        let categories: Vec<String> = row.get("post_categories");

        println!("post_id: {post_id}");
        println!("title: {title}");
        println!("post_categories: {categories:?}");

        assert_eq!(categories.len(), 2);
        assert_eq!(categories[0], "technology");
        assert_eq!(categories[1], "tutorial");
    }

    // Step 5: Verify data in audit table (trigger capture)
    println!("\n=== STEP 5: Verify data in audit table (trigger capture) ===");
    {
        let client = client.lock().await;
        let rows = client
            .query("SELECT sequence_id, table_name, operation, row_id, new_data FROM surreal_sync_changes ORDER BY sequence_id", &[])
            .await
            .expect("Failed to query audit table");

        println!("Audit table has {} rows", rows.len());
        assert!(!rows.is_empty(), "Audit table should have at least one row");

        for row in &rows {
            let sequence_id: i64 = row.get("sequence_id");
            let table_name: String = row.get("table_name");
            let operation: String = row.get("operation");
            let row_id: serde_json::Value = row.get("row_id");
            let new_data: serde_json::Value = row.get("new_data");

            println!("\nAudit row {sequence_id}:");
            println!("  table_name: {table_name}");
            println!("  operation: {operation}");
            println!("  row_id: {row_id}");
            println!(
                "  new_data: {}",
                serde_json::to_string_pretty(&new_data).unwrap()
            );

            // Verify post_categories is in the new_data and is an array
            assert!(
                new_data.get("post_categories").is_some(),
                "new_data should contain post_categories"
            );
            assert!(
                new_data["post_categories"].is_array(),
                "post_categories should be an array, got: {}",
                new_data["post_categories"]
            );

            let categories = new_data["post_categories"].as_array().unwrap();
            assert_eq!(categories.len(), 2, "Should have 2 categories");
            assert_eq!(categories[0], "technology");
            assert_eq!(categories[1], "tutorial");
        }
    }

    // Step 6: Collect schema and verify array type detection
    println!("\n=== STEP 6: Collect schema and verify array type detection ===");
    let schema = {
        let client = client.lock().await;
        surreal_sync_postgresql_trigger_source::schema::collect_postgresql_database_schema(&client)
            .await
            .expect("Failed to collect schema")
    };

    let table_def = schema
        .get_table("test_array_e2e")
        .expect("Table not found in schema");
    let col_type = table_def.get_column_type("post_categories");
    println!("post_categories column type from schema: {col_type:?}");

    assert!(
        matches!(col_type, Some(sync_core::UniversalType::Array { .. })),
        "post_categories should be Array type in schema, got {col_type:?}"
    );

    // Step 7: Initialize source and get changes
    println!("\n=== STEP 7: Initialize source and get changes ===");
    source.initialize().await.expect("Failed to initialize");

    let mut stream = source
        .get_changes()
        .await
        .expect("Failed to get change stream");

    println!("Getting changes from stream...");
    let mut changes = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(change) => {
                println!("\nReceived change:");
                println!("  operation: {:?}", change.operation);
                println!("  table: {}", change.table);
                println!("  id: {:?}", change.id);

                if let Some(ref data) = change.data {
                    println!("  data fields:");
                    for (key, value) in data {
                        println!("    {key}: {value:?}");
                    }

                    // Verify post_categories is an array
                    if let Some(categories) = data.get("post_categories") as Option<&UniversalValue>
                    {
                        println!("\n  post_categories value: {categories:?}");

                        match categories {
                            UniversalValue::Array { elements, .. } => {
                                println!(
                                    "  ✓ post_categories is Array with {} elements",
                                    elements.len()
                                );
                                assert_eq!(elements.len(), 2);
                            }
                            UniversalValue::Null => {
                                println!("  ✗ post_categories is Null - THIS IS THE BUG!");
                                panic!("post_categories should not be Null");
                            }
                            other => {
                                println!("  ✗ post_categories has unexpected type: {other:?}");
                                panic!("post_categories has unexpected type");
                            }
                        }
                    } else {
                        println!("  ✗ post_categories not found in data!");
                        panic!("post_categories should be in data");
                    }
                }

                changes.push(change);
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }
    }

    assert!(
        !changes.is_empty(),
        "Should have received at least one change"
    );
    println!("\n=== TEST PASSED ===");

    // Cleanup
    {
        let client = client.lock().await;
        client
            .execute("DROP TABLE IF EXISTS test_array_e2e CASCADE", &[])
            .await
            .unwrap();
        client
            .execute("DROP TABLE IF EXISTS surreal_sync_changes CASCADE", &[])
            .await
            .unwrap();
    }
}
