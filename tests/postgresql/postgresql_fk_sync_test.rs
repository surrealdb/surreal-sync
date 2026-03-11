//! E2E test for PostgreSQL FK-to-record-link and relation-table sync.
//!
//! Validates that:
//! 1. FK columns in entity tables become SurrealDB record links (Thing)
//! 2. Join (relation) tables are synced as SurrealDB RELATE graph edges
//! 3. Config override forces a table to be treated as a relation
//!
//! Requires Docker (PostgresContainer) and a running SurrealDB instance.

use surreal_sync::testing::surreal::{connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};

/// Full sync: FK columns -> record links, join table -> RELATE edges
#[tokio::test]
async fn test_postgresql_fk_full_sync() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,surreal_sync_postgresql=debug")
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

    // --- Create schema with FKs ---
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

    // --- Insert test data ---
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

    // --- Collect schema and verify FK detection ---
    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(&pg_client).await?;

    let books_def = db_schema.get_table("books").expect("books table in schema");
    assert_eq!(books_def.foreign_keys.len(), 1, "books should have 1 FK");
    let book_tags_def = db_schema
        .get_table("book_tags")
        .expect("book_tags table in schema");
    assert_eq!(
        book_tags_def.foreign_keys.len(),
        2,
        "book_tags should have 2 FKs"
    );

    // --- Connect to SurrealDB ---
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    // --- Run full sync using migrate_table directly ---
    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let tables = vec!["authors", "tags", "books", "book_tags"];

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            for table_name in &tables {
                surreal_sync_postgresql::migrate_table(
                    &pg_client,
                    &sink,
                    table_name,
                    &sync_opts,
                    Some(&db_schema),
                    &[],
                )
                .await?;
            }

            verify_fk_sync_v2(client).await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            for table_name in &tables {
                surreal_sync_postgresql::migrate_table(
                    &pg_client,
                    &sink,
                    table_name,
                    &sync_opts,
                    Some(&db_schema),
                    &[],
                )
                .await?;
            }

            verify_fk_sync_v3(client).await?;
        }
    }

    Ok(())
}

async fn verify_fk_sync_v2(
    client: &surrealdb2::Surreal<surrealdb2::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    // --- Entity table: books.author_id should be a record link ---
    let mut resp = client.query("SELECT author_id FROM books:1").await?;
    let author_id: Option<surrealdb2::sql::Thing> = resp.take((0, "author_id"))?;
    let author_id = author_id.expect("author_id should be present on books:1");
    assert_eq!(
        author_id.tb, "authors",
        "books:1.author_id should be a record link to 'authors' table"
    );
    println!("PASS: books:1.author_id = {author_id} (record link to authors)");

    let mut resp = client.query("SELECT author_id FROM books:2").await?;
    let author_id: Option<surrealdb2::sql::Thing> = resp.take((0, "author_id"))?;
    let author_id = author_id.expect("author_id should be present on books:2");
    assert_eq!(author_id.tb, "authors");
    println!("PASS: books:2.author_id = {author_id} (record link to authors)");

    // --- Entity table: authors has no FK columns, just data ---
    let mut resp = client.query("SELECT name FROM authors:1").await?;
    let name: Option<String> = resp.take((0, "name"))?;
    assert_eq!(name, Some("Alice".to_string()));
    println!("PASS: authors:1.name = \"Alice\" (plain data, no FK transformation)");

    // --- Relation table: book_tags as graph edges ---
    let mut resp = client
        .query("SELECT count() FROM book_tags GROUP ALL")
        .await?;
    let count: Option<i64> = resp.take((0, "count"))?;
    assert_eq!(count, Some(3), "Should have 3 book_tag relation edges");
    println!("PASS: book_tags has 3 relation edges");

    // --- Graph traversal: books:1 -> book_tags -> tags ---
    let mut resp = client
        .query("SELECT ->book_tags->tags AS linked_tags FROM books:1")
        .await?;
    let tag_ids: Option<Vec<surrealdb2::sql::Thing>> = resp.take((0, "linked_tags"))?;
    let tag_ids = tag_ids.unwrap_or_default();
    assert_eq!(
        tag_ids.len(),
        2,
        "books:1 should be linked to 2 tags via book_tags"
    );
    println!("PASS: books:1 ->book_tags-> tags = {tag_ids:?} (graph traversal works)");

    Ok(())
}

async fn verify_fk_sync_v3(
    client: &surrealdb3::Surreal<surrealdb3::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    use surrealdb3::types::Value;

    let mut resp = client.query("SELECT * FROM books:1").await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(record) = result {
        println!("books:1 in SurrealDB v3: {record:?}");
        if let Value::Object(obj) = &record {
            if let Some(author_id) = obj.get("author_id") {
                let author_str = format!("{author_id:?}");
                assert!(
                    author_str.contains("authors"),
                    "books:1.author_id should reference authors table, got: {author_str}"
                );
                println!("PASS: books:1.author_id = {author_str} (record link to authors)");
            }
        }
    }

    let mut resp = client
        .query("SELECT count() FROM book_tags GROUP ALL")
        .await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(Value::Object(obj)) = result {
        println!("book_tags count: {obj:?}");
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
        assert_eq!(count, Some(3), "Should have 3 book_tag relation edges");
        println!("PASS: book_tags has 3 relation edges");
    }

    let mut resp = client
        .query("SELECT ->book_tags->tags AS linked_tags FROM books:1")
        .await?;
    let result: Option<Value> = resp.take(0)?;
    if let Some(Value::Object(obj)) = result {
        println!("books:1 graph traversal: {obj:?}");
        if let Some(Value::Array(tags)) = obj.get("linked_tags") {
            assert_eq!(tags.len(), 2, "books:1 should link to 2 tags");
            println!("PASS: books:1 ->book_tags-> tags has 2 entries (graph traversal works)");
        }
    }

    Ok(())
}

/// E2E test for config override: force a non-join table to be treated as relation
#[tokio::test]
async fn test_postgresql_fk_config_override() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,surreal_sync_postgresql=debug")
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

    pg_client
        .batch_execute(
            "
        CREATE TABLE people (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE mentorship (
            id SERIAL PRIMARY KEY,
            mentor_id INT NOT NULL REFERENCES people(id),
            mentee_id INT NOT NULL REFERENCES people(id),
            started_at TIMESTAMPTZ DEFAULT NOW()
        );
        INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
        INSERT INTO mentorship (id, mentor_id, mentee_id) VALUES (1, 1, 2), (2, 1, 3);
    ",
        )
        .await?;

    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(&pg_client).await?;

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let relation_overrides = vec!["mentorship".to_string()];

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            for table_name in &["people", "mentorship"] {
                surreal_sync_postgresql::migrate_table(
                    &pg_client,
                    &sink,
                    table_name,
                    &sync_opts,
                    Some(&db_schema),
                    &relation_overrides,
                )
                .await?;
            }

            // mentorship should now be graph edges
            let mut resp = client
                .query("SELECT count() FROM mentorship GROUP ALL")
                .await?;
            let count: Option<i64> = resp.take((0, "count"))?;
            assert_eq!(count, Some(2), "Should have 2 mentorship relation edges");
            println!("PASS: mentorship has 2 relation edges (forced via config override)");

            // Graph traversal: try both directions since FK order determines in/out
            let mut resp = client
                .query("SELECT ->mentorship->people AS outward, <-mentorship<-people AS inward FROM people:1")
                .await?;
            let outward: Option<Vec<surrealdb2::sql::Thing>> = resp.take((0, "outward"))?;
            let inward: Option<Vec<surrealdb2::sql::Thing>> = resp.take((0, "inward"))?;
            let outward = outward.unwrap_or_default();
            let inward = inward.unwrap_or_default();
            let total_links = outward.len() + inward.len();
            assert_eq!(
                total_links, 2,
                "people:1 should be connected to 2 others via mentorship (in either direction)"
            );
            println!("PASS: people:1 mentorship outward={outward:?} inward={inward:?} (total={total_links})");
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            for table_name in &["people", "mentorship"] {
                surreal_sync_postgresql::migrate_table(
                    &pg_client,
                    &sink,
                    table_name,
                    &sync_opts,
                    Some(&db_schema),
                    &relation_overrides,
                )
                .await?;
            }

            use surrealdb3::types::Value;

            let mut resp = client
                .query("SELECT count() FROM mentorship GROUP ALL")
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
                assert_eq!(count, Some(2), "Should have 2 mentorship edges");
                println!("PASS: mentorship has 2 relation edges (forced via config override)");
            }

            let mut resp = client
                .query("SELECT ->mentorship->people AS outward, <-mentorship<-people AS inward FROM people:1")
                .await?;
            let result: Option<Value> = resp.take(0)?;
            if let Some(Value::Object(obj)) = result {
                println!("people:1 graph traversal: {obj:?}");
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
                    "people:1 linked to 2 via mentorship"
                );
                println!(
                    "PASS: people:1 mentorship links total={}",
                    out_count + in_count
                );
            }
        }
    }

    Ok(())
}
