//! Debug test to understand SurrealDB behavior

use serde::{Deserialize, Serialize};
use surrealdb::engine::any;
use surrealdb::sql::{Id, Thing};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestRecord {
    data: String,
    value: i64,
}

#[tokio::test]
async fn debug_update_and_select() -> anyhow::Result<()> {
    // Connect to SurrealDB
    let surreal = any::connect("ws://surrealdb:8000").await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    // Create a Thing ID
    let thing = Thing::from(("debug_table", Id::String("test_record".to_string())));
    println!("Thing: {thing:?}");

    // Store a record using UPDATE CONTENT
    let record = TestRecord {
        data: "test data".to_string(),
        value: 42,
    };

    println!("\n=== STORING RECORD WITH CREATE ===");
    let mut store_result = surreal
        .query("CREATE $record_id CONTENT $content")
        .bind(("record_id", thing.clone()))
        .bind(("content", record.clone()))
        .await?;

    println!("Store result: {store_result:?}");

    // Check what CREATE returned
    let created: Vec<TestRecord> = store_result.take(0)?;
    println!("Created records: {created:?}");

    // Try to read it back
    println!("\n=== READING RECORD ===");
    let mut read_result = surreal
        .query("SELECT * FROM $record_id")
        .bind(("record_id", thing.clone()))
        .await?;

    let records: Vec<TestRecord> = read_result.take(0)?;
    println!("Read records: {records:?}");
    assert_eq!(records.len(), 1, "Should have one record");
    assert_eq!(records[0], record, "Record should match");

    // Now try using .upsert() method for create-or-replace
    println!("\n=== TESTING UPSERT METHOD ===");
    let updated_record = TestRecord {
        data: "updated data".to_string(),
        value: 99,
    };

    let upsert_result: Option<TestRecord> = surreal
        .upsert(("debug_table", "test_record"))
        .content(updated_record.clone())
        .await?;

    println!("Upsert result: {upsert_result:?}");

    // Read again to verify update
    let mut read_result2 = surreal
        .query("SELECT * FROM $record_id")
        .bind(("record_id", thing.clone()))
        .await?;

    let records2: Vec<TestRecord> = read_result2.take(0)?;
    println!("After upsert records: {records2:?}");
    assert_eq!(records2.len(), 1, "Should have one record");
    assert_eq!(records2[0], updated_record, "Record should be updated");

    // Cleanup
    surreal
        .query("DELETE $record_id")
        .bind(("record_id", thing))
        .await?;

    Ok(())
}
