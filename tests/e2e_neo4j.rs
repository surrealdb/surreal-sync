use neo4rs::{ConfigBuilder, Graph, Query};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use surrealdb::{engine::any::connect, Surreal};

// Generate unique test identifiers for parallel execution
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

// We use this to generate unique test identifiers for parallel execution.
// The identifier is not used as the neo4j database but as the test marker,
// because creating Neo4j databases require extra permissions that
// might not be available to the user running the tests.
fn generate_test_id() -> u64 {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    timestamp.wrapping_add(counter)
}

/// End-to-end test for Neo4j to SurrealDB migration
#[tokio::test]
async fn test_neo4j_migration_e2e() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    // Generate unique test identifier for parallel execution
    let test_id = generate_test_id();
    let test_marker = format!("e2e_test_{}", test_id);

    println!(
        "🧪 Starting Neo4j to SurrealDB migration end-to-end test (ID: {})",
        test_id
    );

    // Setup test database connections with unique identifiers
    let neo4j_uri = "bolt://neo4j:7687";
    let neo4j_username = "neo4j";
    let neo4j_password = "password";
    let neo4j_database = "neo4j";
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal_namespace = format!("test_ns_{}", test_id);
    let surreal_database = format!("test_db_{}", test_id);

    // Connect to Neo4j
    println!("📊 Connecting to Neo4j...");
    let config = ConfigBuilder::default()
        .uri(neo4j_uri)
        .user(neo4j_username)
        .password(neo4j_password)
        .db(neo4j_database)
        .build()?;

    let graph = Graph::connect(config)?;

    // Global cleanup of any leftover test data from previous runs
    let global_cleanup =
        Query::new("MATCH (n) WHERE n.test_marker IS NOT NULL DETACH DELETE n".to_string());
    let mut result = graph.execute(global_cleanup).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

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
        .use_ns(&surreal_namespace)
        .use_db(&surreal_database)
        .await?;

    // Clean up any existing test data
    println!("🧹 Cleaning up existing test data...");
    cleanup_test_data(
        &graph,
        &surreal,
        &surreal_namespace,
        &surreal_database,
        &test_marker,
    )
    .await?;

    // Populate Neo4j with comprehensive test data
    println!("📝 Populating Neo4j with test data...");
    let expected_data = populate_test_data(&graph, &test_marker).await?;

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: neo4j_uri.to_string(),
        source_database: Some(neo4j_database.to_string()),
        source_username: Some(neo4j_username.to_string()),
        source_password: Some(neo4j_password.to_string()),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10, // Small batch size for testing
        dry_run: false,
    };

    // Execute the migration
    surreal_sync::migrate_from_neo4j(
        from_opts,
        surreal_namespace.clone(),
        surreal_database.clone(),
        to_opts,
    )
    .await?;

    // Validate the migration results
    println!("✅ Validating migration results...");
    validate_migration_results(&surreal, &expected_data).await?;

    // Clean up test data after successful test
    cleanup_test_data(
        &graph,
        &surreal,
        &surreal_namespace,
        &surreal_database,
        &test_marker,
    )
    .await?;

    println!("🎉 End-to-end test completed successfully!");
    Ok(())
}

/// Test migration with various Neo4j data types
#[tokio::test]
async fn test_neo4j_data_types_migration() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok();

    // Generate unique test identifier for parallel execution
    let test_id = generate_test_id();
    let test_marker = format!("data_types_test_{}", test_id);

    println!("🧪 Testing Neo4j data types migration (ID: {})", test_id);

    let neo4j_uri = "bolt://neo4j:7687";
    let neo4j_username = "neo4j";
    let neo4j_password = "password";
    let neo4j_database = "neo4j";
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal_namespace = format!("test_types_ns_{}", test_id);
    let surreal_database = format!("test_types_db_{}", test_id);

    // Connect to databases
    let config = ConfigBuilder::default()
        .uri(neo4j_uri)
        .user(neo4j_username)
        .password(neo4j_password)
        .db(neo4j_database)
        .build()?;

    let graph = Graph::connect(config)?;

    let surreal = connect(surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal
        .use_ns(&surreal_namespace)
        .use_db(&surreal_database)
        .await?;

    // Clean up
    cleanup_data_types_test(&graph, &surreal, &test_marker).await?;

    // Create nodes with various data types
    populate_data_types_test(&graph, &test_marker).await?;

    // Run migration
    let from_opts = surreal_sync::SourceOpts {
        source_uri: neo4j_uri.to_string(),
        source_database: Some(neo4j_database.to_string()),
        source_username: Some(neo4j_username.to_string()),
        source_password: Some(neo4j_password.to_string()),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 5,
        dry_run: false,
    };

    surreal_sync::migrate_from_neo4j(
        from_opts,
        surreal_namespace.clone(),
        surreal_database.clone(),
        to_opts,
    )
    .await?;

    // Validate data types
    validate_data_types_migration(&surreal).await?;

    // Clean up
    cleanup_data_types_test(&graph, &surreal, &test_marker).await?;

    println!("🎉 Data types migration test completed successfully!");
    Ok(())
}

/// Test large dataset migration performance
#[tokio::test]
async fn test_neo4j_large_dataset_migration() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    // Generate unique test identifier for parallel execution
    let test_id = generate_test_id();
    let test_marker = format!("large_dataset_test_{}", test_id);

    println!("🧪 Testing Neo4j large dataset migration (ID: {})", test_id);

    let neo4j_uri = "bolt://neo4j:7687";
    let neo4j_username = "neo4j";
    let neo4j_password = "password";
    let neo4j_database = "neo4j";
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal_namespace = format!("test_large_ns_{}", test_id);
    let surreal_database = format!("test_large_db_{}", test_id);

    // Connect to databases
    let config = ConfigBuilder::default()
        .uri(neo4j_uri)
        .user(neo4j_username)
        .password(neo4j_password)
        .db(neo4j_database)
        .build()?;

    let graph = Graph::connect(config)?;

    let surreal = connect(surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal
        .use_ns(&surreal_namespace)
        .use_db(&surreal_database)
        .await?;

    // Clean up
    cleanup_large_dataset_test(&graph, &surreal, &test_marker).await?;

    // Create large dataset
    let node_count = populate_large_dataset(&graph, &test_marker).await?;
    println!("📊 Created {} nodes and relationships", node_count);

    // Run migration with timing
    let start_time = std::time::Instant::now();

    let from_opts = surreal_sync::SourceOpts {
        source_uri: neo4j_uri.to_string(),
        source_database: Some(neo4j_database.to_string()),
        source_username: Some(neo4j_username.to_string()),
        source_password: Some(neo4j_password.to_string()),
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 100, // Larger batch size for performance
        dry_run: false,
    };

    surreal_sync::migrate_from_neo4j(
        from_opts,
        surreal_namespace.clone(),
        surreal_database.clone(),
        to_opts,
    )
    .await?;

    let elapsed = start_time.elapsed();
    println!("⏱️  Migration completed in: {:?}", elapsed);

    // Validate counts
    validate_large_dataset_migration(&surreal, node_count).await?;

    // Clean up
    cleanup_large_dataset_test(&graph, &surreal, &test_marker).await?;

    println!("🎉 Large dataset migration test completed successfully!");
    Ok(())
}

/// Clean up test data from both databases
async fn cleanup_test_data(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    _namespace: &str,
    _database: &str,
    test_marker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clean Neo4j test data - only remove data with our specific test marker
    let cleanup_query = Query::new(format!(
        "MATCH (n) WHERE n.test_marker = '{}' DETACH DELETE n",
        test_marker
    ));
    let mut result = graph.execute(cleanup_query).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    // Clean SurrealDB test data
    let tables = [
        "person",
        "product",
        "company",
        "category",
        "knows",
        "works_at",
        "bought",
        "belongs_to",
    ];
    for table in tables {
        let query = format!("DELETE FROM {}", table);
        let _result = surreal.query(query).await?;
    }

    Ok(())
}

/// Populate Neo4j with comprehensive test data
async fn populate_test_data(
    graph: &Graph,
    test_marker: &str,
) -> Result<HashMap<String, usize>, Box<dyn std::error::Error>> {
    let mut expected_counts = HashMap::new();

    // Create Person nodes
    let create_persons = Query::new(format!(
        r#"
        CREATE (alice:Person {{
            name: 'Alice Johnson',
            email: 'alice@example.com',
            age: 28,
            skills: ['Rust', 'Neo4j', 'SurrealDB'],
            active: true,
            salary: 85000.50,
            test_marker: '{}'
        }}),
        (bob:Person {{
            name: 'Bob Smith',
            email: 'bob@example.com',
            age: 35,
            skills: ['Management', 'Strategy'],
            active: false,
            salary: 95000.75,
            test_marker: '{}'
        }}),
        (carol:Person {{
            name: 'Carol Davis',
            email: 'carol@example.com',
            age: 42,
            skills: ['Python', 'Data Science'],
            active: true,
            salary: 110000.00,
            test_marker: '{}'
        }})
    "#,
        test_marker, test_marker, test_marker
    ));

    let mut result = graph.execute(create_persons).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }
    expected_counts.insert("person".to_string(), 3);

    // Create Company nodes
    let create_companies = Query::new(format!(
        r#"
        CREATE (acme:Company {{
            name: 'ACME Corp',
            industry: 'Technology',
            founded: 2010,
            revenue: 50000000.00,
            public: true,
            test_marker: '{}'
        }}),
        (globex:Company {{
            name: 'Globex Corporation',
            industry: 'Manufacturing',
            founded: 1995,
            revenue: 75000000.00,
            public: false,
            test_marker: '{}'
        }})
    "#,
        test_marker, test_marker
    ));

    let mut result = graph.execute(create_companies).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }
    expected_counts.insert("company".to_string(), 2);

    // Create Product nodes
    let create_products = Query::new(format!(
        r#"
        CREATE (laptop:Product {{
            name: 'Laptop Pro',
            price: 1299.99,
            category: 'Electronics',
            in_stock: true,
            rating: 4.5,
            reviews: 150,
            test_marker: '{}'
        }}),
        (headphones:Product {{
            name: 'Wireless Headphones',
            price: 199.99,
            category: 'Audio',
            in_stock: false,
            rating: 4.2,
            reviews: 89,
            test_marker: '{}'
        }})
    "#,
        test_marker, test_marker
    ));

    let mut result = graph.execute(create_products).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }
    expected_counts.insert("product".to_string(), 2);

    // Create Category nodes
    let create_categories = Query::new(format!(
        r#"
        CREATE (electronics:Category {{
            name: 'Electronics',
            description: 'Electronic devices and gadgets',
            active: true,
            test_marker: '{}'
        }}),
        (audio:Category {{
            name: 'Audio',
            description: 'Audio equipment and accessories',
            active: true,
            test_marker: '{}'
        }})
    "#,
        test_marker, test_marker
    ));

    let mut result = graph.execute(create_categories).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }
    expected_counts.insert("category".to_string(), 2);

    // Create relationships
    let create_relationships = Query::new(format!(
        r#"
        MATCH (alice:Person {{name: 'Alice Johnson'}}),
              (bob:Person {{name: 'Bob Smith'}}),
              (carol:Person {{name: 'Carol Davis'}}),
              (acme:Company {{name: 'ACME Corp'}}),
              (globex:Company {{name: 'Globex Corporation'}}),
              (laptop:Product {{name: 'Laptop Pro'}}),
              (headphones:Product {{name: 'Wireless Headphones'}}),
              (electronics:Category {{name: 'Electronics'}}),
              (audio:Category {{name: 'Audio'}})
        
        CREATE (alice)-[:KNOWS {{since: 2020, strength: 0.8, test_marker: '{}'}}]->(bob),
               (bob)-[:KNOWS {{since: 2019, strength: 0.6, test_marker: '{}'}}]->(carol),
               (alice)-[:WORKS_AT {{position: 'Senior Engineer', start_date: '2022-01-15', test_marker: '{}'}}]->(acme),
               (bob)-[:WORKS_AT {{position: 'Product Manager', start_date: '2021-03-01', test_marker: '{}'}}]->(acme),
               (carol)-[:WORKS_AT {{position: 'Data Scientist', start_date: '2020-06-10', test_marker: '{}'}}]->(globex),
               (alice)-[:BOUGHT {{quantity: 1, purchase_date: '2023-05-15', amount: 1299.99, test_marker: '{}'}}]->(laptop),
               (bob)-[:BOUGHT {{quantity: 2, purchase_date: '2023-04-20', amount: 399.98, test_marker: '{}'}}]->(headphones),
               (laptop)-[:BELONGS_TO {{primary: true, test_marker: '{}'}}]->(electronics),
               (headphones)-[:BELONGS_TO {{primary: true, test_marker: '{}'}}]->(audio)
        
    "#,
        test_marker,
        test_marker,
        test_marker,
        test_marker,
        test_marker,
        test_marker,
        test_marker,
        test_marker,
        test_marker
    ));

    let mut result = graph.execute(create_relationships).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    // Set expected relationship counts
    let knows_count = 2; // alice->bob, bob->carol
    let works_at_count = 3; // alice->acme, bob->acme, carol->globex
    let bought_count = 2; // alice->laptop, bob->headphones
    let belongs_to_count = 2; // laptop->electronics, headphones->audio

    expected_counts.insert("knows".to_string(), knows_count);
    expected_counts.insert("works_at".to_string(), works_at_count);
    expected_counts.insert("bought".to_string(), bought_count);
    expected_counts.insert("belongs_to".to_string(), belongs_to_count);

    println!("📊 Inserted test data into Neo4j:");
    for (label, count) in &expected_counts {
        println!("  - {} {}", count, label);
    }

    Ok(expected_counts)
}

/// Validate that the migration worked correctly
async fn validate_migration_results(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    expected_counts: &HashMap<String, usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (table_name, expected_count) in expected_counts {
        println!("🔍 Validating table: {}", table_name);

        // Query all records from the table
        let query = format!("SELECT * FROM {}", table_name);
        let mut result = surreal.query(query).await?;
        let actual_ids: Vec<surrealdb::sql::Thing> = result.take("id")?;

        // Check record count
        assert_eq!(
            actual_ids.len(),
            *expected_count,
            "Record count mismatch for table '{}': expected {}, got {}",
            table_name,
            expected_count,
            actual_ids.len()
        );

        // Validate that SurrealDB records have proper IDs
        for actual_id in &actual_ids {
            // Verify the ID format (should be table:objectid)
            let surrealdb::sql::Thing { tb, id, .. } = actual_id;
            assert_eq!(tb, table_name);
            println!("🔍 SurrealDB table: {}", tb);
            println!("🔍 SurrealDB ID: {}", id);
        }

        println!(
            "✅ Table '{}' validation passed ({} records)",
            table_name,
            actual_ids.len()
        );
    }

    Ok(())
}

/// Clean up data types test data
async fn cleanup_data_types_test(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    test_marker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clean Neo4j test data - only remove data with our specific test marker
    let cleanup_query = Query::new(format!(
        "MATCH (n) WHERE n.test_marker = '{}' DETACH DELETE n",
        test_marker
    ));
    let mut result = graph.execute(cleanup_query).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    // Clean SurrealDB
    let tables = ["datatypes"];
    for table in tables {
        let query = format!("DELETE FROM {}", table);
        let _: Vec<Value> = surreal.query(query).await?.take(0).unwrap_or_default();
    }

    Ok(())
}

/// Populate Neo4j with various data types for testing
async fn populate_data_types_test(
    graph: &Graph,
    test_marker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let create_data_types = Query::new(format!(
        r#"
        CREATE (dt:DataTypes {{
            string_prop: 'test string',
            int_prop: 42,
            float_prop: 3.14159,
            bool_prop: true,
            int_array: [1, 2, 3, 4],
            string_array: ['one', 'two', 'three'],
            bool_array: [true, false, true],
            null_prop: null,
            test_marker: '{}'
        }})
    "#,
        test_marker
    ));

    let mut result = graph.execute(create_data_types).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }
    println!("📊 Created data types test node");

    Ok(())
}

/// Validate data types migration
async fn validate_data_types_migration(
    surreal: &Surreal<surrealdb::engine::any::Any>,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = "SELECT * FROM datatypes";
    let mut result = surreal.query(query).await?;
    let ids: Vec<surrealdb::sql::Thing> = result.take("id")?;

    assert_eq!(ids.len(), 1, "Expected exactly 1 data types record");

    // Just verify we have the right number of records and proper ID format
    let actual_id = &ids[0];
    let surrealdb::sql::Thing { tb, .. } = actual_id;
    assert_eq!(tb, "datatypes");

    println!("✅ Data types validation passed");
    Ok(())
}

/// Clean up large dataset test
async fn cleanup_large_dataset_test(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    test_marker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clean Neo4j test data - only remove data with our specific test marker
    let cleanup_query = Query::new(format!(
        "MATCH (n) WHERE n.test_marker = '{}' DETACH DELETE n",
        test_marker
    ));
    let mut result = graph.execute(cleanup_query).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    // Clean SurrealDB
    let tables = ["user", "post", "follows", "likes"];
    for table in tables {
        let query = format!("DELETE FROM {}", table);
        let _: Vec<Value> = surreal.query(query).await?.take(0).unwrap_or_default();
    }

    Ok(())
}

/// Create a large dataset for performance testing
async fn populate_large_dataset(
    graph: &Graph,
    test_marker: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let user_count = 100;
    let post_count = 200;

    // Create users in batches
    for i in 0..user_count {
        let create_user = Query::new(format!(
            r#"
            CREATE (u:User {{
                name: 'User {}',
                email: 'user{}@example.com',
                age: {},
                active: {},
                test_marker: '{}'
            }})
        "#,
            i,
            i,
            20 + (i % 50),
            i % 2 == 0,
            test_marker
        ));

        if i == 0 {
            println!("🔍 Sample user creation query: {}", create_user.query());
        }

        let mut result = graph.execute(create_user).await?;
        while result.next().await?.is_some() {
            // Process all results to ensure query completes
        }
    }

    // Create posts
    for i in 0..post_count {
        let _user_id = i % user_count;
        let create_post = Query::new(format!(
            r#"
            CREATE (p:Post {{
                title: 'Post {}',
                content: 'This is the content of post number {}',
                created_at: datetime(),
                likes: {},
                test_marker: '{}'
            }})
        "#,
            i,
            i,
            i % 100,
            test_marker
        ));

        if i == 0 {
            println!("🔍 Sample post creation query: {}", create_post.query());
        }

        let mut result = graph.execute(create_post).await?;
        while result.next().await?.is_some() {
            // Process all results to ensure query completes
        }
    }

    // Create relationships
    let create_follows = Query::new(format!(
        r#"
        MATCH (u1:User), (u2:User)
        WHERE u1.test_marker = '{}' 
          AND u2.test_marker = '{}'
          AND id(u1) < id(u2)
          AND rand() < 0.1
        CREATE (u1)-[:FOLLOWS {{
            since: datetime(),
            test_marker: '{}'
        }}]->(u2)
    "#,
        test_marker, test_marker, test_marker
    ));

    let mut result = graph.execute(create_follows).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    let create_likes = Query::new(format!(
        r#"
        MATCH (u:User), (p:Post)
        WHERE u.test_marker = '{}' 
          AND p.test_marker = '{}'
          AND rand() < 0.2
        CREATE (u)-[:LIKES {{
            timestamp: datetime(),
            test_marker: '{}'
        }}]->(p)
    "#,
        test_marker, test_marker, test_marker
    ));

    let mut result = graph.execute(create_likes).await?;
    while result.next().await?.is_some() {
        // Process all results to ensure query completes
    }

    let total_nodes = user_count + post_count;
    println!("📊 Created {} users and {} posts", user_count, post_count);

    Ok(total_nodes)
}

/// Validate large dataset migration
async fn validate_large_dataset_migration(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    expected_node_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check user count
    let user_query = "SELECT * FROM user";
    let mut result = surreal.query(user_query).await?;
    let user_ids: Vec<surrealdb::sql::Thing> = result.take("id")?;

    // Check post count
    let post_query = "SELECT * FROM post";
    let mut result = surreal.query(post_query).await?;
    let post_ids: Vec<surrealdb::sql::Thing> = result.take("id")?;

    let total_migrated = user_ids.len() + post_ids.len();

    assert_eq!(
        total_migrated, expected_node_count,
        "Expected {} total nodes, got {}",
        expected_node_count, total_migrated
    );

    println!(
        "✅ Large dataset validation passed: {} nodes migrated",
        total_migrated
    );
    Ok(())
}
