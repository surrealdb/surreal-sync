use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use surrealdb::{engine::any::connect, sql::Thing, Surreal};

#[derive(Debug, Deserialize)]
struct Page {
    id: Thing,
    title: String,
    content: String,
    parent: Thing,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct Block {
    id: Thing,
    r#type: String,
    text: Option<String>,
    level: Option<i64>,
    language: Option<String>,
    items: Option<Vec<String>>,
    parent: Thing,
    order: i64,
}

#[derive(Debug, Deserialize)]
struct Database {
    id: Thing,
    name: String,
    description: String,
    created_at: String,
    properties: HashMap<String, serde_json::Value>,
}

/// End-to-end test for JSONL to SurrealDB migration
#[tokio::test]
async fn test_jsonl_migration_e2e() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok();

    println!("🧪 Starting JSONL to SurrealDB migration end-to-end test");

    // Setup test configuration
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal_namespace = "test_jsonl_ns";
    let surreal_database = "test_jsonl_db";

    // Get the test data directory path
    let test_data_dir = PathBuf::from("/workspace/tests/test_data/jsonl");

    // Connect to SurrealDB
    println!("🗄️  Connecting to SurrealDB...");
    let surreal = connect(surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal
        .use_ns(surreal_namespace)
        .use_db(surreal_database)
        .await?;

    // Clean up any existing test data
    println!("🧹 Cleaning up existing test data...");
    cleanup_test_data(&surreal).await?;

    // Run the migration using the library functions directly
    println!("🔄 Running JSONL migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: test_data_dir.to_string_lossy().to_string(),
        source_database: None,
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    // Define conversion rules for Notion-like parent references
    let conversion_rules = vec![
        r#"type="database_id",database_id databases:database_id"#.to_string(),
        r#"type="page_id",page_id pages:page_id"#.to_string(),
        r#"type="block_id",block_id blocks:block_id"#.to_string(),
    ];

    // Execute the migration
    surreal_sync::migrate_from_jsonl(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
        "id".to_string(),
        conversion_rules,
    )
    .await?;

    // Validate the migration results
    println!("✅ Validating migration results...");
    validate_migration_results(&surreal).await?;

    // Clean up test data after successful test
    cleanup_test_data(&surreal).await?;

    println!("🎉 End-to-end test completed successfully!");
    Ok(())
}

/// Clean up test data from SurrealDB
async fn cleanup_test_data(
    surreal: &Surreal<surrealdb::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tables = ["pages", "blocks", "databases"];
    for table in tables {
        let query = format!("DELETE FROM {}", table);
        let _: Vec<Thing> = surreal.query(query).await?.take("id").unwrap_or_default();
    }
    Ok(())
}

/// Validate the migration results
async fn validate_migration_results(
    surreal: &Surreal<surrealdb::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate databases table
    println!("📊 Validating databases...");
    let query = "SELECT * FROM databases ORDER BY id";
    let mut response = surreal.query(query).await?;
    let databases: Vec<Database> = response.take(0)?;

    assert_eq!(databases.len(), 2);
    assert_eq!(databases[0].id.to_string(), "databases:db1");
    assert_eq!(databases[0].name, "Documentation");
    assert_eq!(databases[1].id.to_string(), "databases:db2");
    assert_eq!(databases[1].name, "API Docs");

    // Validate pages table
    println!("📄 Validating pages...");
    let query = "SELECT * FROM pages ORDER BY id";
    let mut response = surreal.query(query).await?;
    let pages: Vec<Page> = response.take(0)?;

    assert_eq!(pages.len(), 3);
    assert_eq!(pages[0].id.to_string(), "pages:page1");
    assert_eq!(pages[0].title, "Getting Started");
    assert_eq!(pages[0].parent.to_string(), "databases:db1");

    assert_eq!(pages[1].id.to_string(), "pages:page2");
    assert_eq!(pages[1].title, "Advanced Topics");
    assert_eq!(pages[1].parent.to_string(), "pages:page1");

    // Validate blocks table
    println!("🧱 Validating blocks...");
    let query = "SELECT * FROM blocks ORDER BY id";
    let mut response = surreal.query(query).await?;
    let blocks: Vec<Block> = response.take(0)?;

    assert_eq!(blocks.len(), 4);
    assert_eq!(blocks[0].id.to_string(), "blocks:block1");
    assert_eq!(blocks[0].r#type, "paragraph");
    assert_eq!(blocks[0].parent.to_string(), "pages:page1");

    assert_eq!(blocks[3].id.to_string(), "blocks:block4");
    assert_eq!(blocks[3].r#type, "list");
    assert_eq!(blocks[3].parent.to_string(), "blocks:block2");
    assert_eq!(
        blocks[3].items,
        Some(vec![
            "Item 1".to_string(),
            "Item 2".to_string(),
            "Item 3".to_string()
        ])
    );

    println!("✨ All validations passed!");
    Ok(())
}

#[tokio::test]
async fn test_jsonl_conversion_rules() -> Result<(), Box<dyn std::error::Error>> {
    use surreal_sync::jsonl::ConversionRule;

    // Test valid rule parsing
    let rule_str = r#"type="page_id",page_id pages:page_id"#;
    let rule = ConversionRule::parse(rule_str)?;
    assert_eq!(rule.type_field, "type");
    assert_eq!(rule.type_value, "page_id");
    assert_eq!(rule.id_field, "page_id");
    assert_eq!(rule.target_table, "pages");

    // Test invalid rule formats
    assert!(ConversionRule::parse("invalid").is_err());
    assert!(ConversionRule::parse(r#"type="page_id",page_id"#).is_err());
    assert!(ConversionRule::parse(r#"invalid="page_id",page_id pages:page_id"#).is_err());

    Ok(())
}

#[tokio::test]
async fn test_jsonl_with_custom_id_field() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test directory
    let test_dir = PathBuf::from("/tmp/jsonl_test_custom_id");
    std::fs::create_dir_all(&test_dir)?;

    // Create test file with custom ID field
    let test_file = test_dir.join("items.jsonl");
    std::fs::write(
        &test_file,
        r#"{"item_id": "item1", "name": "Test Item 1", "value": 100}
{"item_id": "item2", "name": "Test Item 2", "value": 200}"#,
    )?;

    // Setup SurrealDB connection
    // Change to ws://localhost:8000 if testing locally
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal = connect(surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test_custom_id").use_db("test_db").await?;

    // Clean up
    let _: Vec<Thing> = surreal
        .query("DELETE FROM items")
        .await?
        .take("id")
        .unwrap_or_default();

    // Run migration with custom ID field
    let from_opts = surreal_sync::SourceOpts {
        source_uri: test_dir.to_string_lossy().to_string(),
        source_database: None,
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_jsonl(
        from_opts,
        "test_custom_id".to_string(),
        "test_db".to_string(),
        to_opts,
        "item_id".to_string(), // Custom ID field
        vec![],
    )
    .await?;

    // Validate results
    #[derive(Debug, Deserialize)]
    struct Item {
        id: Thing,
        name: String,
        value: i64,
    }

    let query = "SELECT * FROM items ORDER BY id";
    let mut response = surreal.query(query).await?;
    let items: Vec<Item> = response.take(0)?;

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].id.to_string(), "items:item1");
    assert_eq!(items[0].name, "Test Item 1");
    assert_eq!(items[0].value, 100);
    assert_eq!(items[1].id.to_string(), "items:item2");
    assert_eq!(items[1].name, "Test Item 2");
    assert_eq!(items[1].value, 200);

    // Cleanup
    std::fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[tokio::test]
async fn test_jsonl_with_complex_id_field() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test directory
    let test_dir = PathBuf::from("/tmp/jsonl_test_custom_id");
    std::fs::create_dir_all(&test_dir).unwrap();

    // Create test file with custom ID field
    let test_file = test_dir.join("items.jsonl");
    std::fs::write(
        &test_file,
        r#"{"timestamp":"2025-07-22T03:18:59.349350Z","level":"INFO","target":"surreal::env"}
        {"timestamp":"2025-07-22T03:19:59.349350Z","level":"INFO","target":"surreal::env"}"#,
    )
    .unwrap();

    // Setup SurrealDB connection
    // Change to ws://localhost:8000 if testing locally
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal = connect(surreal_endpoint).await.unwrap();
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await
        .unwrap();
    surreal
        .use_ns("test_custom_id")
        .use_db("test_db")
        .await
        .unwrap();

    // Clean up
    let _: Vec<Thing> = surreal
        .query("DELETE FROM items")
        .await
        .unwrap()
        .take("id")
        .unwrap_or_default();

    // Run migration with custom ID field
    let from_opts = surreal_sync::SourceOpts {
        source_uri: test_dir.to_string_lossy().to_string(),
        source_database: None,
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_jsonl(
        from_opts,
        "test_custom_id".to_string(),
        "test_db".to_string(),
        to_opts,
        "timestamp".to_string(), // Custom ID field
        vec![],
    )
    .await
    .unwrap();

    // Validate results
    #[derive(Debug, Deserialize)]
    struct Item {
        id: Thing,
        level: String,
        target: String,
    }

    let query = "SELECT * FROM items";
    let mut response = surreal.query(query).await.unwrap();
    let items: Vec<Item> = response.take(0).unwrap();

    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0].id.to_string(),
        "items:⟨2025-07-22T03:18:59.349350Z⟩"
    );
    assert_eq!(items[0].level, "INFO");
    assert_eq!(items[0].target, "surreal::env");
    assert_eq!(
        items[1].id.to_string(),
        "items:⟨2025-07-22T03:19:59.349350Z⟩"
    );
    assert_eq!(items[1].level, "INFO");
    assert_eq!(items[1].target, "surreal::env");

    // Cleanup
    std::fs::remove_dir_all(&test_dir).unwrap();
}
