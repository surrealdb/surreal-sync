//! Debug test to understand SurrealDB behavior (works with both v2 and v3 servers).

use serde::{Deserialize, Serialize};
use surreal_version::testing::SurrealDbContainer;
use surreal_version::SurrealMajorVersion;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestRecord {
    data: String,
    value: i64,
}

#[tokio::test]
async fn debug_update_and_select() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-debug-surreal");
    db.start()?;
    db.wait_until_ready(30)?;

    match db.detected_version {
        Some(SurrealMajorVersion::V3) => debug_update_and_select_v3(&db).await,
        _ => debug_update_and_select_v2(&db).await,
    }
}

async fn debug_update_and_select_v2(db: &SurrealDbContainer) -> anyhow::Result<()> {
    use surrealdb::engine::any;
    use surrealdb::sql::{Id, Thing};

    let surreal = any::connect(db.ws_endpoint()).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let thing = Thing::from(("debug_table", Id::String("test_record".to_string())));
    println!("Thing: {thing:?}");

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
    let created: Vec<TestRecord> = store_result.take(0)?;
    println!("Created records: {created:?}");

    println!("\n=== READING RECORD ===");
    let mut read_result = surreal
        .query("SELECT * FROM $record_id")
        .bind(("record_id", thing.clone()))
        .await?;

    let records: Vec<TestRecord> = read_result.take(0)?;
    println!("Read records: {records:?}");
    assert_eq!(records.len(), 1, "Should have one record");
    assert_eq!(records[0], record, "Record should match");

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

    let mut read_result2 = surreal
        .query("SELECT * FROM $record_id")
        .bind(("record_id", thing.clone()))
        .await?;

    let records2: Vec<TestRecord> = read_result2.take(0)?;
    println!("After upsert records: {records2:?}");
    assert_eq!(records2.len(), 1, "Should have one record");
    assert_eq!(records2[0], updated_record, "Record should be updated");

    surreal
        .query("DELETE $record_id")
        .bind(("record_id", thing))
        .await?;

    Ok(())
}

async fn debug_update_and_select_v3(db: &SurrealDbContainer) -> anyhow::Result<()> {
    use surrealdb3::types::{RecordId, RecordIdKey, SurrealValue};

    #[derive(Debug, Clone, PartialEq, SurrealValue)]
    #[surreal(crate = "surrealdb3::types")]
    struct TestRecordV3 {
        data: String,
        value: i64,
    }

    let surreal = surrealdb3::engine::any::connect(db.ws_endpoint()).await?;
    surreal
        .signin(surrealdb3::opt::auth::Root {
            username: "root".to_string(),
            password: "root".to_string(),
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let record_id = RecordId::new("debug_table", RecordIdKey::String("test_record".to_string()));
    println!("RecordId: {record_id:?}");

    let record = TestRecordV3 {
        data: "test data".to_string(),
        value: 42,
    };

    println!("\n=== STORING RECORD WITH CREATE (V3) ===");
    let record_json = serde_json::to_value(serde_json::json!({
        "data": record.data,
        "value": record.value,
    }))?;
    surreal
        .query("CREATE $record_id CONTENT $content")
        .bind(("record_id", record_id.clone()))
        .bind(("content", record_json))
        .await?;

    println!("\n=== READING RECORD (V3) ===");
    let mut read_result = surreal
        .query("SELECT data, value FROM $record_id")
        .bind(("record_id", record_id.clone()))
        .await?;

    let records: Vec<TestRecordV3> = read_result.take(0)?;
    println!("Read records: {records:?}");
    assert_eq!(records.len(), 1, "Should have one record");
    assert_eq!(records[0], record, "Record should match");

    println!("\n=== TESTING UPSERT (V3) ===");
    let updated_record = TestRecordV3 {
        data: "updated data".to_string(),
        value: 99,
    };

    let updated_json = serde_json::to_value(serde_json::json!({
        "data": updated_record.data,
        "value": updated_record.value,
    }))?;
    surreal
        .query("UPSERT $record_id CONTENT $content")
        .bind(("record_id", record_id.clone()))
        .bind(("content", updated_json))
        .await?;

    let mut read_result2 = surreal
        .query("SELECT data, value FROM $record_id")
        .bind(("record_id", record_id.clone()))
        .await?;

    let records2: Vec<TestRecordV3> = read_result2.take(0)?;
    println!("After upsert records: {records2:?}");
    assert_eq!(records2.len(), 1, "Should have one record");
    assert_eq!(records2[0], updated_record, "Record should be updated");

    surreal
        .query("DELETE $record_id")
        .bind(("record_id", record_id))
        .await?;

    Ok(())
}
