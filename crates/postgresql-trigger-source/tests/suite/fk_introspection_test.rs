//! Integration test for PostgreSQL FK introspection.
//!
//! Verifies that `collect_database_schema_with_fks` correctly discovers
//! foreign keys and that `classify_table` returns the right `TableKind`.
//!
//! Requires Docker to start a PostgreSQL container.

use surreal_sync_postgresql::schema::collect_database_schema_with_fks;
use sync_core::{classify_table, TableKind};

#[tokio::test]
async fn test_fk_introspection_and_classification() -> Result<(), Box<dyn std::error::Error>> {
    let container = crate::shared::postgres().await;

    let (client, connection) =
        tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Create test schema with FK constraints
    client
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
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (book_id, tag_id)
        );
    ",
        )
        .await?;

    // Collect schema with FKs
    let schema = collect_database_schema_with_fks(&client).await?;

    // --- Verify authors: 0 FKs, Entity ---
    let authors = schema.get_table("authors").expect("authors table");
    assert!(
        authors.foreign_keys.is_empty(),
        "authors should have no FKs"
    );
    assert!(
        matches!(classify_table(authors, &[]), TableKind::Entity),
        "authors should be Entity"
    );

    // --- Verify tags: 0 FKs, Entity ---
    let tags = schema.get_table("tags").expect("tags table");
    assert!(tags.foreign_keys.is_empty(), "tags should have no FKs");
    assert!(
        matches!(classify_table(tags, &[]), TableKind::Entity),
        "tags should be Entity"
    );

    // --- Verify books: 1 FK (author_id -> authors.id), Entity ---
    let books = schema.get_table("books").expect("books table");
    assert_eq!(books.foreign_keys.len(), 1, "books should have 1 FK");
    let book_fk = &books.foreign_keys[0];
    assert_eq!(book_fk.columns, vec!["author_id"]);
    assert_eq!(book_fk.referenced_table, "authors");
    assert_eq!(book_fk.referenced_columns, vec!["id"]);
    assert!(
        matches!(classify_table(books, &[]), TableKind::Entity),
        "books should be Entity (only 1 FK)"
    );

    // --- Verify book_tags: 2 FKs, composite PK, Relation ---
    let book_tags = schema.get_table("book_tags").expect("book_tags table");
    assert_eq!(
        book_tags.foreign_keys.len(),
        2,
        "book_tags should have 2 FKs"
    );

    // Verify composite PK
    assert!(
        book_tags.composite_primary_key.is_some(),
        "book_tags should have composite PK"
    );
    let cpk = book_tags.composite_primary_key.as_ref().unwrap();
    assert!(cpk.contains(&"book_id".to_string()));
    assert!(cpk.contains(&"tag_id".to_string()));

    // FK details
    let fk_tables: Vec<&str> = book_tags
        .foreign_keys
        .iter()
        .map(|fk| fk.referenced_table.as_str())
        .collect();
    assert!(fk_tables.contains(&"books"), "should reference books");
    assert!(fk_tables.contains(&"tags"), "should reference tags");

    // Should be classified as Relation
    match classify_table(book_tags, &[]) {
        TableKind::Relation { in_fk, out_fk } => {
            let ref_tables: Vec<&str> = vec![
                in_fk.referenced_table.as_str(),
                out_fk.referenced_table.as_str(),
            ];
            assert!(ref_tables.contains(&"books"));
            assert!(ref_tables.contains(&"tags"));
        }
        TableKind::Entity => panic!("book_tags should be classified as Relation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_override_forces_relation_classification() -> Result<(), Box<dyn std::error::Error>> {
    let container = crate::shared::postgres().await;

    let (client, connection) =
        tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    client
        .batch_execute(
            "
        CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE collaborations (
            id SERIAL PRIMARY KEY,
            user1_id INT REFERENCES users(id),
            user2_id INT REFERENCES users(id),
            project TEXT
        );
    ",
        )
        .await?;

    let schema = collect_database_schema_with_fks(&client).await?;
    let collab = schema
        .get_table("collaborations")
        .expect("collaborations table");

    // Without override: Entity (PK is own 'id', not composed of FKs)
    assert!(
        matches!(classify_table(collab, &[]), TableKind::Entity),
        "Without override, collaborations should be Entity"
    );

    // With override: Relation (forced)
    let overrides = vec!["collaborations".to_string()];
    assert!(
        matches!(
            classify_table(collab, &overrides),
            TableKind::Relation { .. }
        ),
        "With override, collaborations should be Relation"
    );

    Ok(())
}
