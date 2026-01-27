//! Integration test for PostgreSQL array handling through triggers
//!
//! This test verifies that:
//! 1. TEXT[] arrays can be inserted correctly using tokio_postgres
//! 2. The to_jsonb() function correctly serializes arrays
//! 3. The trigger captures array data correctly in the audit table

use tokio_postgres::{types::ToSql, NoTls};

/// Test that Vec<String> correctly inserts as TEXT[] and serializes via to_jsonb()
#[tokio::test]
async fn test_array_insertion_and_jsonb_serialization() {
    let connection_string = std::env::var("POSTGRESQL_TEST_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgresql:5432/testdb".to_string());

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Clean up any existing test table
    let _ = client
        .execute("DROP TABLE IF EXISTS test_array_table CASCADE", &[])
        .await;

    // Create a table with a TEXT[] column
    client
        .execute(
            "CREATE TABLE test_array_table (
                id TEXT PRIMARY KEY,
                tags TEXT[]
            )",
            &[],
        )
        .await
        .expect("Failed to create test table");

    // Test 1: Insert array using Vec<String> directly
    let tags: Vec<String> = vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()];
    client
        .execute(
            "INSERT INTO test_array_table (id, tags) VALUES ($1, $2)",
            &[&"test_id_1", &tags],
        )
        .await
        .expect("Failed to insert with Vec<String>");

    // Verify the array was inserted correctly
    let row = client
        .query_one(
            "SELECT tags FROM test_array_table WHERE id = 'test_id_1'",
            &[],
        )
        .await
        .expect("Failed to query");

    let retrieved_tags: Vec<String> = row.get(0);
    assert_eq!(retrieved_tags, vec!["tag1", "tag2", "tag3"]);

    // Test 2: Verify to_jsonb() correctly serializes the array
    let row = client
        .query_one(
            "SELECT to_jsonb(tags) as tags_json FROM test_array_table WHERE id = 'test_id_1'",
            &[],
        )
        .await
        .expect("Failed to query with to_jsonb");

    let tags_json: serde_json::Value = row.get(0);
    assert!(tags_json.is_array(), "Expected array, got {tags_json:?}");
    let arr = tags_json.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], serde_json::json!("tag1"));
    assert_eq!(arr[1], serde_json::json!("tag2"));
    assert_eq!(arr[2], serde_json::json!("tag3"));

    // Test 3: Verify to_jsonb(ROW) correctly includes the array
    let row = client
        .query_one(
            "SELECT to_jsonb(t) as row_json FROM test_array_table t WHERE id = 'test_id_1'",
            &[],
        )
        .await
        .expect("Failed to query row as jsonb");

    let row_json: serde_json::Value = row.get(0);
    let tags_in_row = &row_json["tags"];
    assert!(
        tags_in_row.is_array(),
        "Expected tags to be array in row JSON, got {tags_in_row:?}"
    );

    // Clean up
    client
        .execute("DROP TABLE test_array_table CASCADE", &[])
        .await
        .expect("Failed to drop test table");

    println!("All array tests passed!");
}

/// Test that Box<dyn ToSql> with Vec<String> works correctly
#[tokio::test]
async fn test_boxed_array_parameter() {
    let connection_string = std::env::var("POSTGRESQL_TEST_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgresql:5432/testdb".to_string());

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Clean up any existing test table
    let _ = client
        .execute("DROP TABLE IF EXISTS test_boxed_array_table CASCADE", &[])
        .await;

    // Create a table with a TEXT[] column
    client
        .execute(
            "CREATE TABLE test_boxed_array_table (
                id TEXT PRIMARY KEY,
                tags TEXT[]
            )",
            &[],
        )
        .await
        .expect("Failed to create test table");

    // Test using Box<dyn ToSql + Sync> - this is how the test injection code works
    let tags: Vec<String> = vec!["boxed_tag1".to_string(), "boxed_tag2".to_string()];
    let boxed_tags: Box<dyn ToSql + Sync> = Box::new(tags);
    let id: Box<dyn ToSql + Sync> = Box::new("test_boxed_id".to_string());

    let params: Vec<&(dyn ToSql + Sync)> = vec![id.as_ref(), boxed_tags.as_ref()];

    client
        .execute(
            "INSERT INTO test_boxed_array_table (id, tags) VALUES ($1, $2)",
            &params,
        )
        .await
        .expect("Failed to insert with boxed Vec<String>");

    // Verify the array was inserted correctly
    let row = client
        .query_one(
            "SELECT to_jsonb(tags) as tags_json FROM test_boxed_array_table WHERE id = 'test_boxed_id'",
            &[],
        )
        .await
        .expect("Failed to query");

    let tags_json: serde_json::Value = row.get(0);
    assert!(
        tags_json.is_array(),
        "Expected array with boxed param, got {tags_json:?}"
    );
    let arr = tags_json.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Clean up
    client
        .execute("DROP TABLE test_boxed_array_table CASCADE", &[])
        .await
        .expect("Failed to drop test table");

    println!("Boxed array parameter test passed!");
}

/// Test the full trigger flow: insert array -> trigger captures -> audit table has correct JSON
#[tokio::test]
async fn test_array_through_trigger_audit() {
    let connection_string = std::env::var("POSTGRESQL_TEST_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgresql:5432/testdb".to_string());

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Clean up
    let _ = client
        .execute("DROP TABLE IF EXISTS test_trigger_array_audit CASCADE", &[])
        .await;
    let _ = client
        .execute("DROP TABLE IF EXISTS test_trigger_audit_table CASCADE", &[])
        .await;
    let _ = client
        .execute(
            "DROP FUNCTION IF EXISTS test_trigger_track_changes() CASCADE",
            &[],
        )
        .await;

    // Create audit table (similar to what the incremental sync creates)
    client
        .execute(
            "CREATE TABLE test_trigger_audit_table (
                sequence_id BIGSERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                row_id JSONB,
                old_data JSONB,
                new_data JSONB,
                changed_at TIMESTAMPTZ DEFAULT NOW()
            )",
            &[],
        )
        .await
        .expect("Failed to create audit table");

    // Create the main table with TEXT[] column
    client
        .execute(
            "CREATE TABLE test_trigger_array_audit (
                id TEXT PRIMARY KEY,
                name TEXT,
                tags TEXT[]
            )",
            &[],
        )
        .await
        .expect("Failed to create main table");

    // Create trigger function (similar to what the incremental sync creates)
    client
        .execute(
            "CREATE OR REPLACE FUNCTION test_trigger_track_changes() RETURNS TRIGGER AS $$
            DECLARE
                row_json jsonb;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    row_json := to_jsonb(NEW);
                    INSERT INTO test_trigger_audit_table (table_name, operation, row_id, new_data)
                    VALUES (TG_TABLE_NAME, TG_OP, jsonb_build_array(row_json->>'id'), row_json);
                    RETURN NEW;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql",
            &[],
        )
        .await
        .expect("Failed to create trigger function");

    // Create trigger
    client
        .execute(
            "CREATE TRIGGER test_array_trigger
            AFTER INSERT ON test_trigger_array_audit
            FOR EACH ROW EXECUTE FUNCTION test_trigger_track_changes()",
            &[],
        )
        .await
        .expect("Failed to create trigger");

    // Insert data with array using boxed parameter (like test injection does)
    let tags: Vec<String> = vec!["category1".to_string(), "category2".to_string()];
    let boxed_id: Box<dyn ToSql + Sync> = Box::new("test_id_audit".to_string());
    let boxed_name: Box<dyn ToSql + Sync> = Box::new("Test Name".to_string());
    let boxed_tags: Box<dyn ToSql + Sync> = Box::new(tags);

    let params: Vec<&(dyn ToSql + Sync)> =
        vec![boxed_id.as_ref(), boxed_name.as_ref(), boxed_tags.as_ref()];

    client
        .execute(
            "INSERT INTO test_trigger_array_audit (id, name, tags) VALUES ($1, $2, $3)",
            &params,
        )
        .await
        .expect("Failed to insert with array");

    // Now check what's in the audit table
    let row = client
        .query_one(
            "SELECT new_data FROM test_trigger_audit_table WHERE table_name = 'test_trigger_array_audit'",
            &[],
        )
        .await
        .expect("Failed to query audit table");

    let new_data: serde_json::Value = row.get(0);
    println!("Audit table new_data: {new_data}");

    // Verify the tags field in the audit data
    let tags_value = &new_data["tags"];
    assert!(
        !tags_value.is_null(),
        "tags should not be null in audit data, got: {new_data}"
    );
    assert!(
        tags_value.is_array(),
        "tags should be array in audit data, got: {tags_value}"
    );
    let arr = tags_value.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0], serde_json::json!("category1"));
    assert_eq!(arr[1], serde_json::json!("category2"));

    // Clean up
    let _ = client
        .execute("DROP TABLE IF EXISTS test_trigger_array_audit CASCADE", &[])
        .await;
    let _ = client
        .execute("DROP TABLE IF EXISTS test_trigger_audit_table CASCADE", &[])
        .await;

    println!("Trigger audit array test passed!");
}
