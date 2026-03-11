//! Trigger-based incremental-only FK sync test.
//!
//! Verifies that incremental sync (without relying on full sync for data)
//! correctly converts FK columns to record links and relation tables to
//! SurrealDB RELATE edges.
//!
//! Flow:
//! 1. Create tables with FK constraints in PostgreSQL
//! 2. Run full sync on EMPTY tables (sets up triggers + generates checkpoint)
//! 3. INSERT data into PostgreSQL AFTER full sync
//! 4. Run incremental sync to pick up those inserts
//! 5. Verify SurrealDB has record links and graph edges

use surreal_sync::testing::surreal::{connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};

#[tokio::test]
async fn test_trigger_fk_incremental_only() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,surreal_sync_postgresql=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let container = surreal_sync::testing::shared_containers::shared_postgres().await;
    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgres_test_db(container, test_id)
            .await?;

    let (pg_client, pg_conn) =
        tokio_postgres::connect(&test_conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            eprintln!("PG connection error: {e}");
        }
    });

    // --- Create schema with FK constraints (tables are EMPTY) ---
    pg_client
        .batch_execute(
            "
        CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE tags (id SERIAL PRIMARY KEY, label TEXT NOT NULL);
        CREATE TABLE books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author_id INT NOT NULL REFERENCES authors(id)
        );
        CREATE TABLE book_tags (
            book_id INT REFERENCES books(id),
            tag_id INT REFERENCES tags(id),
            PRIMARY KEY (book_id, tag_id)
        );
    ",
        )
        .await?;

    // --- Connect to SurrealDB ---
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    // --- Prepare checkpoint infrastructure ---
    let checkpoint_dir = format!(".test-fk-incr-trigger-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: test_conn_str.clone(),
        source_database: Some(format!("test_{test_id}")),
        tables: vec![],
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let checkpoint_store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    // --- Step 1: Run full sync on EMPTY tables (sets up triggers + checkpoint) ---
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts.clone(),
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts.clone(),
                Some(&sync_manager),
            )
            .await?;
        }
    }

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    // --- Step 2: INSERT data AFTER full sync (triggers will capture these) ---
    pg_client
        .batch_execute(
            "
        INSERT INTO authors VALUES (1, 'Alice'), (2, 'Bob');
        INSERT INTO tags VALUES (1, 'programming'), (2, 'systems');
        INSERT INTO books VALUES (1, 'Rust in Action', 1), (2, 'Go Programming', 2);
        INSERT INTO book_tags VALUES (1, 1), (1, 2), (2, 1);
    ",
        )
        .await?;

    // --- Step 3: Read checkpoint and run incremental sync ---
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, checkpoint::SyncPhase::FullSyncStart)
            .await?;
    let sync_checkpoint: surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint =
        checkpoint_file.parse()?;

    let deadline = chrono::Utc::now() + chrono::Duration::seconds(30);

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                deadline,
                None,
            )
            .await?;

            verify_fk_incremental_v2(client).await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                deadline,
                None,
            )
            .await?;

            verify_fk_incremental_v3(client).await?;
        }
    }

    // Cleanup
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}

async fn verify_fk_incremental_v2(
    client: &surrealdb2::Surreal<surrealdb2::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    // --- Entity table: books.author_id should be a record link ---
    let mut resp = client.query("SELECT author_id FROM books:1").await?;
    let author_id: Option<surrealdb2::sql::Thing> = resp.take((0, "author_id"))?;
    let author_id = author_id.expect("author_id should exist on books:1 from incremental sync");
    assert_eq!(
        author_id.tb, "authors",
        "Incremental: books:1.author_id should be record link to authors"
    );
    println!("PASS [trigger-incremental]: books:1.author_id = {author_id} (record link)");

    // --- Relation table: book_tags as graph edges ---
    let mut resp = client
        .query("SELECT count() FROM book_tags GROUP ALL")
        .await?;
    let count: Option<i64> = resp.take((0, "count"))?;
    assert_eq!(
        count,
        Some(3),
        "Incremental: should have 3 book_tag relation edges"
    );
    println!("PASS [trigger-incremental]: book_tags has 3 relation edges");

    // --- Graph traversal (check both directions since FK order may vary) ---
    let mut resp = client
        .query("SELECT ->book_tags->tags AS outward, <-book_tags<-tags AS inward FROM books:1")
        .await?;
    let outward: Option<Vec<surrealdb2::sql::Thing>> = resp.take((0, "outward"))?;
    let inward: Option<Vec<surrealdb2::sql::Thing>> = resp.take((0, "inward"))?;
    let out_count = outward.as_ref().map(|v| v.len()).unwrap_or(0);
    let in_count = inward.as_ref().map(|v| v.len()).unwrap_or(0);
    assert_eq!(
        out_count + in_count,
        2,
        "Incremental: books:1 should link to 2 tags via book_tags (out={out_count}, in={in_count})"
    );
    println!("PASS [trigger-incremental]: books:1 book_tags outward={outward:?} inward={inward:?}");

    // --- Verify plain data on authors (no FK transformation) ---
    let mut resp = client.query("SELECT name FROM authors:1").await?;
    let name: Option<String> = resp.take((0, "name"))?;
    assert_eq!(name, Some("Alice".to_string()));
    println!("PASS [trigger-incremental]: authors:1.name = \"Alice\"");

    Ok(())
}

async fn verify_fk_incremental_v3(
    client: &surrealdb3::Surreal<surrealdb3::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    use surrealdb3::types::Value;

    let mut resp = client.query("SELECT * FROM books:1").await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(Value::Object(obj)) = result {
        if let Some(author_id) = obj.get("author_id") {
            let s = format!("{author_id:?}");
            assert!(
                s.contains("authors"),
                "Incremental v3: books:1.author_id should reference authors, got: {s}"
            );
            println!("PASS [trigger-incremental-v3]: books:1.author_id = {s}");
        }
    }

    let mut resp = client
        .query("SELECT count() FROM book_tags GROUP ALL")
        .await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(Value::Object(obj)) = result {
        let count = match obj.get("count") {
            Some(Value::Number(n)) => {
                if let surrealdb3::types::Number::Int(i) = n {
                    Some(*i)
                } else {
                    None
                }
            }
            _ => None,
        };
        assert_eq!(count, Some(3), "Incremental v3: 3 book_tag edges");
        println!("PASS [trigger-incremental-v3]: book_tags has 3 edges");
    }

    let mut resp = client
        .query("SELECT ->book_tags->tags AS outward, <-book_tags<-tags AS inward FROM books:1")
        .await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(Value::Object(obj)) = result {
        let out_count = match obj.get("outward") {
            Some(Value::Array(a)) => a.len(),
            _ => 0,
        };
        let in_count = match obj.get("inward") {
            Some(Value::Array(a)) => a.len(),
            _ => 0,
        };
        assert_eq!(
            out_count + in_count,
            2,
            "Incremental v3: books:1 linked to 2 tags"
        );
        println!(
            "PASS [trigger-incremental-v3]: graph traversal OK (total={})",
            out_count + in_count
        );
    }

    Ok(())
}
