//! Targeted test for post_categories array handling
//!
//! This test verifies the complete flow for array type handling:
//! 1. Schema collection correctly identifies TEXT[] as UniversalType::Array
//! 2. JSON conversion correctly handles arrays with schema information

use sync_core::{ColumnDefinition, TableDefinition, UniversalType, UniversalValue};

/// Test that schema collection correctly identifies array types
#[tokio::test]
async fn test_schema_collection_identifies_array_types() {
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

    // Create a table with TEXT[] column
    client
        .execute("DROP TABLE IF EXISTS test_array_schema CASCADE", &[])
        .await
        .unwrap();

    client
        .execute(
            "CREATE TABLE test_array_schema (
                id TEXT PRIMARY KEY,
                tags TEXT[],
                numbers INTEGER[]
            )",
            &[],
        )
        .await
        .unwrap();

    // Collect schema
    let schema =
        surreal_sync_postgresql_trigger_source::schema::collect_postgresql_database_schema(&client)
            .await
            .expect("Failed to collect schema");

    // Get the table definition
    let table = schema
        .get_table("test_array_schema")
        .expect("Table not found");

    // Verify the tags column is identified as Array<Text>
    let tags_type = table.get_column_type("tags");
    println!("tags column type: {tags_type:?}");

    match tags_type {
        Some(UniversalType::Array { element_type }) => {
            assert!(
                matches!(**element_type, UniversalType::Text),
                "Expected Array<Text>, got Array<{element_type:?}>"
            );
        }
        other => panic!("Expected Array type for 'tags', got {other:?}"),
    }

    // Verify the numbers column is identified as Array<Int32>
    let numbers_type = table.get_column_type("numbers");
    println!("numbers column type: {numbers_type:?}");

    match numbers_type {
        Some(UniversalType::Array { element_type }) => {
            assert!(
                matches!(**element_type, UniversalType::Int32),
                "Expected Array<Int32>, got Array<{element_type:?}>"
            );
        }
        other => panic!("Expected Array type for 'numbers', got {other:?}"),
    }

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_array_schema CASCADE", &[])
        .await
        .unwrap();
}

/// Test that JSON conversion correctly handles arrays with schema
#[test]
fn test_json_array_conversion_with_schema() {
    use json_types::json_to_universal_with_table_schema;

    // Create a table schema with Array<Text> column
    let pk = ColumnDefinition::new("id", UniversalType::Text);
    let columns = vec![ColumnDefinition::new(
        "post_categories",
        UniversalType::Array {
            element_type: Box::new(UniversalType::Text),
        },
    )];
    let table_schema = TableDefinition::new("all_types_posts", pk, columns);

    // Simulate JSON from trigger (as it would come from to_jsonb(NEW))
    let json_array = serde_json::json!(["technology", "tutorial"]);

    let result = json_to_universal_with_table_schema(json_array, "post_categories", &table_schema)
        .expect("Failed to convert JSON array");

    println!("Conversion result: {result:?}");

    match result {
        UniversalValue::Array { elements, .. } => {
            assert_eq!(elements.len(), 2);
            assert!(matches!(&elements[0], UniversalValue::Text(s) if s == "technology"));
            assert!(matches!(&elements[1], UniversalValue::Text(s) if s == "tutorial"));
        }
        other => panic!("Expected Array, got {other:?}"),
    }
}

/// Test the complete flow: schema collection + JSON conversion
#[tokio::test]
async fn test_complete_array_flow() {
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

    // Create a table with TEXT[] column
    client
        .execute("DROP TABLE IF EXISTS test_complete_array CASCADE", &[])
        .await
        .unwrap();

    client
        .execute(
            "CREATE TABLE test_complete_array (
                post_id TEXT PRIMARY KEY,
                post_categories TEXT[]
            )",
            &[],
        )
        .await
        .unwrap();

    // Insert test data
    let tags: Vec<String> = vec!["technology".to_string(), "tutorial".to_string()];
    client
        .execute(
            "INSERT INTO test_complete_array (post_id, post_categories) VALUES ($1, $2)",
            &[&"post_001", &tags],
        )
        .await
        .expect("Failed to insert test data");

    // Query the data as JSONB (like trigger does)
    let row = client
        .query_one(
            "SELECT to_jsonb(t) as data FROM test_complete_array t WHERE post_id = 'post_001'",
            &[],
        )
        .await
        .expect("Failed to query");

    let json_data: serde_json::Value = row.get("data");
    println!("JSON from PostgreSQL: {json_data}");

    // Verify the JSON structure
    assert!(
        json_data["post_categories"].is_array(),
        "post_categories should be an array in JSON"
    );

    // Collect schema
    let schema =
        surreal_sync_postgresql_trigger_source::schema::collect_postgresql_database_schema(&client)
            .await
            .expect("Failed to collect schema");

    // Get the table definition
    let table = schema
        .get_table("test_complete_array")
        .expect("Table not found in schema");

    // Verify column type
    let col_type = table.get_column_type("post_categories");
    println!("Schema column type for post_categories: {col_type:?}");

    assert!(
        matches!(col_type, Some(UniversalType::Array { element_type }) if matches!(**element_type, UniversalType::Text)),
        "Expected Array<Text> in schema, got {col_type:?}"
    );

    // Convert using the schema
    let post_categories_json = json_data["post_categories"].clone();
    let result = json_types::json_to_universal_with_table_schema(
        post_categories_json,
        "post_categories",
        table,
    )
    .expect("Failed to convert with schema");

    println!("Final UniversalValue: {result:?}");

    match result {
        UniversalValue::Array { elements, .. } => {
            assert_eq!(elements.len(), 2, "Expected 2 elements");
            assert!(matches!(&elements[0], UniversalValue::Text(s) if s == "technology"));
            assert!(matches!(&elements[1], UniversalValue::Text(s) if s == "tutorial"));
        }
        other => panic!("Expected Array, got {other:?}"),
    }

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_complete_array CASCADE", &[])
        .await
        .unwrap();
}
