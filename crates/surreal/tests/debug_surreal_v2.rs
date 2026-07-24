//! Debug test for SurrealDB v2 query/upsert behavior.

use serde::{Deserialize, Serialize};
use surreal_sync_surreal::version::testing::SurrealDbContainer;
use surreal_sync_surreal::version::SurrealMajorVersion;

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

    if matches!(db.detected_version, Some(SurrealMajorVersion::V3)) {
        eprintln!("skip: v2 debug test requires a v2 server");
        return Ok(());
    }

    use surrealdb2::engine::any;
    use surrealdb2::sql::{Id, Thing};

    let surreal = any::connect(db.ws_endpoint()).await?;
    surreal
        .signin(surrealdb2::opt::auth::Root {
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
        .query("DELETE type::thing($record_tb, $record_id)")
        .bind(("record_tb", thing.tb.clone()))
        .bind((
            "record_id",
            match &thing.id {
                surrealdb2::sql::Id::String(s) => {
                    surrealdb2::sql::Value::Strand(surrealdb2::sql::Strand::from(s.as_str()))
                }
                surrealdb2::sql::Id::Number(n) => {
                    surrealdb2::sql::Value::Number(surrealdb2::sql::Number::Int(*n))
                }
                other => surrealdb2::sql::Value::Strand(surrealdb2::sql::Strand::from(format!(
                    "{other:?}"
                ))),
            },
        ))
        .await?;

    Ok(())
}
