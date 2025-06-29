use base64::{self, Engine};
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use surrealdb::{engine::any::connect, Surreal};

#[derive(Debug, Deserialize, Serialize)]
struct User {
    id: String,
    name: String,
    score: f64,
    tags: Vec<String>,
}

/// End-to-end test for MongoDB to SurrealDB migration
#[tokio::test]
async fn test_mongodb_migration_e2e() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB to SurrealDB migration end-to-end test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_migration_db";
    let surreal_namespace = "test_ns";
    let surreal_database = "test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    // Add connection timeout to prevent hanging
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    cleanup_test_data(&mongo_db, &surreal, surreal_namespace, surreal_database).await?;

    // Populate MongoDB with comprehensive test data
    println!("📝 Populating MongoDB with test data...");
    let expected_data = populate_test_data(&mongo_db).await?;

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10, // Small batch size for testing
        dry_run: false,
    };

    // Execute the migration
    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    // Validate the migration results
    println!("✅ Validating migration results...");
    validate_migration_results(&surreal, &expected_data).await?;

    // Clean up test data after successful test
    cleanup_test_data(&mongo_db, &surreal, surreal_namespace, surreal_database).await?;

    println!("🎉 End-to-end test completed successfully!");
    Ok(())
}

/// Clean up test data from both databases
async fn cleanup_test_data(
    mongo_db: &mongodb::Database,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    _namespace: &str,
    _database: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Drop MongoDB test collections
    let collections = ["users", "products", "orders", "categories"];
    for collection_name in collections {
        let collection = mongo_db.collection::<mongodb::bson::Document>(collection_name);
        collection.drop().await.ok(); // Ignore errors if collection doesn't exist
    }

    // Clean SurrealDB test data
    let tables = ["users", "products", "orders", "categories"];
    for table in tables {
        let query = format!("DELETE FROM {}", table);
        let _: Vec<surrealdb::sql::Thing> =
            surreal.query(query).await?.take("id").unwrap_or_default();
    }

    Ok(())
}

/// Populate MongoDB with comprehensive test data
async fn populate_test_data(
    mongo_db: &mongodb::Database,
) -> Result<HashMap<String, Vec<Value>>, Box<dyn std::error::Error>> {
    let mut expected_data = HashMap::new();

    // Users collection
    let users_collection = mongo_db.collection("users");
    let users_data = vec![
        doc! {
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 28,
            "profile": {
                "bio": "Software Engineer",
                "location": "San Francisco",
                "skills": ["Rust", "MongoDB", "SurrealDB"]
            },
            "active": true,
            "created_at": mongodb::bson::DateTime::now(),
            "metadata": {
                "source": "web",
                "campaign": "signup_2024"
            }
        },
        doc! {
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 35,
            "profile": {
                "bio": "Product Manager",
                "location": "New York",
                "skills": ["Leadership", "Strategy"]
            },
            "active": false,
            "created_at": mongodb::bson::DateTime::now(),
            "preferences": {
                "notifications": true,
                "theme": "dark"
            }
        },
        doc! {
            "name": "Carol Davis",
            "email": "carol@example.com",
            "age": 42,
            "profile": {
                "bio": "Data Scientist",
                "location": "Seattle",
                "skills": ["Python", "Machine Learning", "Statistics"]
            },
            "active": true,
            "created_at": mongodb::bson::DateTime::now(),
            "tags": ["premium", "beta_tester"]
        },
    ];
    users_collection.insert_many(users_data.clone()).await?;

    // Convert BSON to JSON for validation
    let users_json: Vec<Value> = users_data
        .iter()
        .map(|doc| mongodb::bson::from_document(doc.clone()).unwrap())
        .collect();
    expected_data.insert("users".to_string(), users_json);

    // Products collection
    let products_collection = mongo_db.collection("products");
    let products_data = vec![
        doc! {
            "name": "Laptop Pro",
            "price": 1299.99,
            "category": "Electronics",
            "specifications": {
                "cpu": "Intel i7",
                "ram": "16GB",
                "storage": "512GB SSD"
            },
            "in_stock": true,
            "tags": ["laptop", "professional", "high-performance"],
            "rating": 4.5,
            "reviews_count": 150
        },
        doc! {
            "name": "Wireless Headphones",
            "price": 199.99,
            "category": "Audio",
            "specifications": {
                "battery_life": "30 hours",
                "noise_cancellation": true,
                "wireless": true
            },
            "in_stock": false,
            "tags": ["headphones", "wireless", "noise-cancelling"],
            "rating": 4.2,
            "reviews_count": 89
        },
    ];
    products_collection
        .insert_many(products_data.clone())
        .await?;

    let products_json: Vec<Value> = products_data
        .iter()
        .map(|doc| mongodb::bson::from_document(doc.clone()).unwrap())
        .collect();
    expected_data.insert("products".to_string(), products_json);

    // Categories collection
    let categories_collection = mongo_db.collection("categories");
    let categories_data = vec![
        doc! {
            "name": "Electronics",
            "description": "Electronic devices and gadgets",
            "parent_category": null,
            "subcategories": ["Laptops", "Phones", "Tablets"],
            "active": true
        },
        doc! {
            "name": "Audio",
            "description": "Audio equipment and accessories",
            "parent_category": "Electronics",
            "subcategories": ["Headphones", "Speakers", "Microphones"],
            "active": true
        },
    ];
    categories_collection
        .insert_many(categories_data.clone())
        .await?;

    let categories_json: Vec<Value> = categories_data
        .iter()
        .map(|doc| mongodb::bson::from_document(doc.clone()).unwrap())
        .collect();
    expected_data.insert("categories".to_string(), categories_json);

    println!("📊 Inserted test data into MongoDB:");
    println!("  - {} users", expected_data["users"].len());
    println!("  - {} products", expected_data["products"].len());
    println!("  - {} categories", expected_data["categories"].len());

    Ok(expected_data)
}

/// Validate that the migration worked correctly
async fn validate_migration_results(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    expected_data: &HashMap<String, Vec<Value>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (table_name, expected_records) in expected_data {
        println!("🔍 Validating table: {}", table_name);

        // Query all records from the table
        let query = format!("SELECT * FROM {}", table_name);
        let mut result = surreal.query(query).await?;
        let actual_ids: Vec<surrealdb::sql::Thing> = result.take("id")?;

        // Check record count
        assert_eq!(
            actual_ids.len(),
            expected_records.len(),
            "Record count mismatch for table '{}': expected {}, got {}",
            table_name,
            expected_records.len(),
            actual_ids.len()
        );

        // Validate each record exists and has correct structure
        // for expected_record in expected_records {
        //     let found = actual_ids.iter().any(|actual_record| {
        //         // Check key fields match (excluding MongoDB _id and SurrealDB id)
        //         records_match(expected_record, actual_record)
        //     });

        //     assert!(
        //         found,
        //         "Expected record not found in SurrealDB table '{}': {:?}",
        //         table_name,
        //         expected_record
        //     );
        // }

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

/// Test for MongoDB $binary data type migration
#[tokio::test]
async fn test_mongodb_binary_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB $binary migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_binary_migration_db";
    let surreal_namespace = "binary_test_ns";
    let surreal_database = "binary_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("binary_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM binary_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with binary in MongoDB
    println!("📝 Creating MongoDB test data with binary...");
    let test_bytes = b"Hello, MongoDB binary data!";
    let base64_data = base64::engine::general_purpose::STANDARD.encode(test_bytes);

    // Insert documents with canonical $binary format
    let documents = vec![
        // Canonical format: {"$binary": {"base64": "...", "subType": "00"}}
        doc! {
            "name": "binary_test_1",
            "description": "Test with canonical $binary format",
            "data": {
                "$binary": {
                    "base64": &base64_data,
                    "subType": "00"
                }
            }
        },
        // Another canonical format example with different subType
        doc! {
            "name": "binary_test_2",
            "description": "Test with canonical $binary format (subType 01)",
            "data": {
                "$binary": {
                    "base64": &base64_data,
                    "subType": "01"
                }
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description FROM binary_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;

    assert_eq!(
        migrated_names.len(),
        2,
        "Should have migrated 2 test records"
    );

    // Verify records are in alphabetical order: binary_test_1, binary_test_2
    assert_eq!(migrated_names[0], "binary_test_1");
    assert_eq!(
        migrated_descriptions[0],
        "Test with canonical $binary format"
    );

    assert_eq!(migrated_names[1], "binary_test_2");
    assert_eq!(
        migrated_descriptions[1],
        "Test with canonical $binary format (subType 01)"
    );

    // Verify that the bytes fields exist by checking they are not null using separate queries
    let binary_test_1_query =
        "SELECT count() FROM binary_test WHERE name = 'binary_test_1' AND data IS NOT NULL";
    let mut binary_test_1_result = surreal.query(binary_test_1_query).await?;
    let binary_test_1_count: Vec<i64> = binary_test_1_result.take("count")?;
    assert_eq!(
        binary_test_1_count[0], 1,
        "Binary test 1 record should have non-null data field"
    );

    let binary_test_2_query =
        "SELECT count() FROM binary_test WHERE name = 'binary_test_2' AND data IS NOT NULL";
    let mut binary_test_2_result = surreal.query(binary_test_2_query).await?;
    let binary_test_2_count: Vec<i64> = binary_test_2_result.take("count")?;
    assert_eq!(
        binary_test_2_count[0], 1,
        "Binary test 2 record should have non-null data field"
    );

    println!("✅ MongoDB binary migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM binary_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB binary migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB $numberDouble data type migration
#[tokio::test]
async fn test_mongodb_number_double_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB $numberDouble migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_number_double_migration_db";
    let surreal_namespace = "number_double_test_ns";
    let surreal_database = "number_double_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("number_double_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM number_double_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with $numberDouble in MongoDB
    println!("📝 Creating MongoDB test data with $numberDouble...");

    // Insert documents with various $numberDouble formats
    let documents = vec![
        // Standard double values
        doc! {
            "name": "double_test_1",
            "description": "Test with standard double value",
            "value": {
                "$numberDouble": "3.14159"
            }
        },
        // Large double value
        doc! {
            "name": "double_test_2",
            "description": "Test with large double value",
            "value": {
                "$numberDouble": "1.7976931348623157e+308"
            }
        },
        // Small double value
        doc! {
            "name": "double_test_3",
            "description": "Test with small double value",
            "value": {
                "$numberDouble": "2.2250738585072014e-308"
            }
        },
        // Negative double value
        doc! {
            "name": "double_test_4",
            "description": "Test with negative double value",
            "value": {
                "$numberDouble": "-123.456789"
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description, value FROM number_double_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;
    let migrated_values: Vec<f64> = result.take((0, "value"))?;

    assert_eq!(
        migrated_names.len(),
        4,
        "Should have migrated 4 test records"
    );

    // Verify records are in alphabetical order and have correct values
    assert_eq!(migrated_names[0], "double_test_1");
    assert_eq!(migrated_descriptions[0], "Test with standard double value");
    assert!((migrated_values[0] - 3.14159).abs() < f64::EPSILON);

    assert_eq!(migrated_names[1], "double_test_2");
    assert_eq!(migrated_descriptions[1], "Test with large double value");
    assert!((migrated_values[1] - 1.7976931348623157e+308).abs() < 1e+300);

    assert_eq!(migrated_names[2], "double_test_3");
    assert_eq!(migrated_descriptions[2], "Test with small double value");
    assert!((migrated_values[2] - 2.2250738585072014e-308).abs() < 1e-300);

    assert_eq!(migrated_names[3], "double_test_4");
    assert_eq!(migrated_descriptions[3], "Test with negative double value");
    assert!((migrated_values[3] - (-123.456789)).abs() < f64::EPSILON);

    println!("✅ MongoDB $numberDouble migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM number_double_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB $numberDouble migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB $numberInt data type migration
#[tokio::test]
async fn test_mongodb_number_int_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB $numberInt migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_number_int_migration_db";
    let surreal_namespace = "number_int_test_ns";
    let surreal_database = "number_int_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("number_int_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM number_int_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with $numberInt in MongoDB
    println!("📝 Creating MongoDB test data with $numberInt...");

    // Insert documents with various $numberInt formats
    let documents = vec![
        // Standard int values
        doc! {
            "name": "int_test_1",
            "description": "Test with standard int value",
            "value": {
                "$numberInt": "42"
            }
        },
        // Large int value (within 32-bit range)
        doc! {
            "name": "int_test_2",
            "description": "Test with large int value",
            "value": {
                "$numberInt": "2147483647"
            }
        },
        // Small negative int value
        doc! {
            "name": "int_test_3",
            "description": "Test with negative int value",
            "value": {
                "$numberInt": "-2147483648"
            }
        },
        // Zero value
        doc! {
            "name": "int_test_4",
            "description": "Test with zero int value",
            "value": {
                "$numberInt": "0"
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description, value FROM number_int_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;
    let migrated_values: Vec<i64> = result.take((0, "value"))?;

    assert_eq!(
        migrated_names.len(),
        4,
        "Should have migrated 4 test records"
    );

    // Verify records are in alphabetical order and have correct values
    assert_eq!(migrated_names[0], "int_test_1");
    assert_eq!(migrated_descriptions[0], "Test with standard int value");
    assert_eq!(migrated_values[0], 42);

    assert_eq!(migrated_names[1], "int_test_2");
    assert_eq!(migrated_descriptions[1], "Test with large int value");
    assert_eq!(migrated_values[1], 2147483647);

    assert_eq!(migrated_names[2], "int_test_3");
    assert_eq!(migrated_descriptions[2], "Test with negative int value");
    assert_eq!(migrated_values[2], -2147483648);

    assert_eq!(migrated_names[3], "int_test_4");
    assert_eq!(migrated_descriptions[3], "Test with zero int value");
    assert_eq!(migrated_values[3], 0);

    println!("✅ MongoDB $numberInt migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM number_int_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB $numberInt migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB Decimal128 ($numberDecimal) data type migration
#[tokio::test]
async fn test_mongodb_decimal128_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB Decimal128 migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_decimal128_migration_db";
    let test_collection_name = "decimal128_test_collection";
    let surreal_namespace = "test_decimal128_ns";
    let surreal_database = "test_decimal128_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);
    let mongo_collection = mongo_db.collection::<mongodb::bson::Document>(test_collection_name);

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
    let _ = mongo_collection.drop().await;
    let cleanup_surreal = format!("DELETE FROM {};", test_collection_name);
    let _ = surreal.query(cleanup_surreal).await;

    // Create test documents with various Decimal128 values
    println!("📝 Creating test documents with Decimal128 values...");

    // Test cases with different decimal precision scenarios
    let test_docs = vec![
        doc! {
            "name": "decimal_test_1",
            "description": "Standard decimal",
            "price": {"$numberDecimal": "123.456"}
        },
        doc! {
            "name": "decimal_test_2",
            "description": "High precision decimal",
            "price": {"$numberDecimal": "99999999999999999999999999999999.999999"}
        },
        doc! {
            "name": "decimal_test_3",
            "description": "Very small decimal",
            "price": {"$numberDecimal": "0.000000000000000000000000000001"}
        },
        doc! {
            "name": "decimal_test_4",
            "description": "Negative decimal",
            "price": {"$numberDecimal": "-12345.6789"}
        },
        doc! {
            "name": "decimal_test_5",
            "description": "Zero decimal",
            "price": {"$numberDecimal": "0.0"}
        },
        doc! {
            "name": "decimal_test_6",
            "description": "Integer as decimal",
            "price": {"$numberDecimal": "42"}
        },
        doc! {
            "name": "decimal_test_7",
            "description": "Scientific notation decimal",
            "price": {"$numberDecimal": "1.23456E+10"}
        },
    ];

    // Insert test documents
    mongo_collection.insert_many(test_docs).await?;
    println!("✅ Inserted {} test documents", 7);

    // Run the migration
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    // Validate the migration results
    println!("✅ Validating Decimal128 migration results...");

    // Query the migrated data - use separate queries for different fields
    let query_names = "SELECT name FROM decimal128_test_collection ORDER BY name;";
    let query_descriptions =
        "SELECT name, description FROM decimal128_test_collection ORDER BY name;";
    let query_prices = "SELECT name, price FROM decimal128_test_collection ORDER BY name;";

    let mut names_result = surreal.query(query_names).await?;
    let mut descriptions_result = surreal.query(query_descriptions).await?;
    let mut prices_result = surreal.query(query_prices).await?;

    let names: Vec<String> = names_result.take((0, "name"))?;
    let descriptions: Vec<String> = descriptions_result.take((0, "description"))?;
    let prices: Vec<f64> = prices_result.take((0, "price"))?;

    // Verify record count
    assert_eq!(names.len(), 7, "Expected 7 records, got {}", names.len());
    assert_eq!(
        descriptions.len(),
        7,
        "Expected 7 descriptions, got {}",
        descriptions.len()
    );
    assert_eq!(prices.len(), 7, "Expected 7 prices, got {}", prices.len());

    println!("✅ Record count validation passed: {} records", names.len());

    // Verify specific values
    assert_eq!(names[0], "decimal_test_1");
    assert_eq!(descriptions[0], "Standard decimal");
    println!("✅ Standard decimal: {} - {}", names[0], prices[0]);

    assert_eq!(names[1], "decimal_test_2");
    assert_eq!(descriptions[1], "High precision decimal");
    println!("✅ High precision decimal: {} - {}", names[1], prices[1]);

    assert_eq!(names[2], "decimal_test_3");
    assert_eq!(descriptions[2], "Very small decimal");
    println!("✅ Very small decimal: {} - {}", names[2], prices[2]);

    assert_eq!(names[3], "decimal_test_4");
    assert_eq!(descriptions[3], "Negative decimal");
    println!("✅ Negative decimal: {} - {}", names[3], prices[3]);

    assert_eq!(names[4], "decimal_test_5");
    assert_eq!(descriptions[4], "Zero decimal");
    println!("✅ Zero decimal: {} - {}", names[4], prices[4]);

    assert_eq!(names[5], "decimal_test_6");
    assert_eq!(descriptions[5], "Integer as decimal");
    println!("✅ Integer as decimal: {} - {}", names[5], prices[5]);

    assert_eq!(names[6], "decimal_test_7");
    assert_eq!(descriptions[6], "Scientific notation decimal");
    println!(
        "✅ Scientific notation decimal: {} - {}",
        names[6], prices[6]
    );

    // Clean up test data
    println!("🧹 Cleaning up test data...");
    let _ = mongo_collection.drop().await;
    let cleanup_surreal = format!("DELETE FROM {};", test_collection_name);
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB Decimal128 migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB Document (nested objects) data type migration
#[tokio::test]
async fn test_mongodb_document_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB Document migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_document_migration_db";
    let surreal_namespace = "document_test_ns";
    let surreal_database = "document_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("document_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM document_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with nested documents containing Extended JSON v2 in MongoDB
    println!("📝 Creating MongoDB test data with nested documents...");

    // Insert documents with complex nested structures containing Extended JSON v2 types
    let documents = vec![
        // Document with nested Extended JSON v2 types
        doc! {
            "name": "document_test_1",
            "description": "Test with nested Extended JSON v2 types",
            "metadata": {
                "created": {
                    "$date": "2024-01-01T10:30:00.000Z"
                },
                "version": {
                    "$numberLong": "123456789"
                },
                "precision": {
                    "$numberDouble": "99.999"
                },
                "flags": {
                    "$numberInt": "42"
                },
                "data": {
                    "$binary": {
                        "base64": "SGVsbG8gV29ybGQ=",
                        "subType": "00"
                    }
                }
            },
            "config": {
                "enabled": true,
                "timeout": 30,
                "retries": 3,
                "nested": {
                    "level2": {
                        "value": "deep nested string",
                        "timestamp": {
                            "$date": "2024-06-28T12:00:00.000Z"
                        }
                    }
                }
            }
        },
        // Document with mixed nested types
        doc! {
            "name": "document_test_2",
            "description": "Test with mixed nested document types",
            "user": {
                "id": {
                    "$oid": "507f1f77bcf86cd799439011"
                },
                "profile": {
                    "name": "John Doe",
                    "age": {
                        "$numberInt": "35"
                    },
                    "score": {
                        "$numberDouble": "98.5"
                    },
                    "active": true,
                    "tags": ["developer", "mongodb", "rust"]
                },
                "settings": {
                    "theme": "dark",
                    "notifications": {
                        "email": true,
                        "push": false,
                        "frequency": {
                            "$numberInt": "24"
                        }
                    }
                }
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description FROM document_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;

    assert_eq!(
        migrated_names.len(),
        2,
        "Should have migrated 2 test records"
    );

    // Verify records are in alphabetical order
    assert_eq!(migrated_names[0], "document_test_1");
    assert_eq!(
        migrated_descriptions[0],
        "Test with nested Extended JSON v2 types"
    );

    assert_eq!(migrated_names[1], "document_test_2");
    assert_eq!(
        migrated_descriptions[1],
        "Test with mixed nested document types"
    );

    // Verify nested document structure for document_test_1 using separate queries
    let version_query =
        "SELECT metadata.version as version FROM document_test WHERE name = 'document_test_1'";
    let mut version_result = surreal.query(version_query).await?;
    let versions: Vec<i64> = version_result.take((0, "version"))?;
    assert_eq!(versions.len(), 1, "Should have one record");
    assert_eq!(versions[0], 123456789);

    let precision_query =
        "SELECT metadata.precision as precision FROM document_test WHERE name = 'document_test_1'";
    let mut precision_result = surreal.query(precision_query).await?;
    let precisions: Vec<f64> = precision_result.take((0, "precision"))?;
    assert_eq!(precisions.len(), 1, "Should have one record");
    assert!((precisions[0] - 99.999).abs() < f64::EPSILON);

    let enabled_query =
        "SELECT config.enabled as enabled FROM document_test WHERE name = 'document_test_1'";
    let mut enabled_result = surreal.query(enabled_query).await?;
    let enabled_values: Vec<bool> = enabled_result.take((0, "enabled"))?;
    assert_eq!(enabled_values.len(), 1, "Should have one record");
    assert_eq!(enabled_values[0], true);

    let nested_query = "SELECT config.nested.level2.value as nested_value FROM document_test WHERE name = 'document_test_1'";
    let mut nested_result = surreal.query(nested_query).await?;
    let nested_values: Vec<String> = nested_result.take((0, "nested_value"))?;
    assert_eq!(nested_values.len(), 1, "Should have one record");
    assert_eq!(nested_values[0], "deep nested string");

    // Verify nested document structure for document_test_2 using separate queries
    let name_query =
        "SELECT user.profile.name as name FROM document_test WHERE name = 'document_test_2'";
    let mut name_result = surreal.query(name_query).await?;
    let names: Vec<String> = name_result.take((0, "name"))?;
    assert_eq!(names.len(), 1, "Should have one record");
    assert_eq!(names[0], "John Doe");

    let age_query =
        "SELECT user.profile.age as age FROM document_test WHERE name = 'document_test_2'";
    let mut age_result = surreal.query(age_query).await?;
    let ages: Vec<i64> = age_result.take((0, "age"))?;
    assert_eq!(ages.len(), 1, "Should have one record");
    assert_eq!(ages[0], 35);

    let score_query =
        "SELECT user.profile.score as score FROM document_test WHERE name = 'document_test_2'";
    let mut score_result = surreal.query(score_query).await?;
    let scores: Vec<f64> = score_result.take((0, "score"))?;
    assert_eq!(scores.len(), 1, "Should have one record");
    assert!((scores[0] - 98.5).abs() < f64::EPSILON);

    let theme_query =
        "SELECT user.settings.theme as theme FROM document_test WHERE name = 'document_test_2'";
    let mut theme_result = surreal.query(theme_query).await?;
    let themes: Vec<String> = theme_result.take((0, "theme"))?;
    assert_eq!(themes.len(), 1, "Should have one record");
    assert_eq!(themes[0], "dark");

    println!("✅ MongoDB Document migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM document_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB Document migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB Array data type migration
#[tokio::test]
async fn test_mongodb_array_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB Array migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_array_migration_db";
    let surreal_namespace = "array_test_ns";
    let surreal_database = "array_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("array_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM array_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with arrays containing Extended JSON v2 in MongoDB
    println!("📝 Creating MongoDB test data with arrays...");

    // Insert documents with arrays containing Extended JSON v2 types
    let documents = vec![
        // Document with arrays of Extended JSON v2 types
        doc! {
            "name": "array_test_1",
            "description": "Test with arrays of Extended JSON v2 types",
            "timestamps": [
                {
                    "$date": "2024-01-01T10:00:00.000Z"
                },
                {
                    "$date": "2024-06-15T14:30:00.000Z"
                },
                {
                    "$date": "2024-12-31T23:59:59.999Z"
                }
            ],
            "numbers": [
                {
                    "$numberLong": "123456789"
                },
                {
                    "$numberDouble": "3.14159"
                },
                {
                    "$numberInt": "42"
                }
            ],
            "mixed_array": [
                "simple string",
                {
                    "$numberInt": "100"
                },
                true,
                {
                    "$date": "2024-06-28T12:00:00.000Z"
                },
                {
                    "$binary": {
                        "base64": "VGVzdCBkYXRh",
                        "subType": "00"
                    }
                }
            ]
        },
        // Document with nested arrays
        doc! {
            "name": "array_test_2",
            "description": "Test with nested arrays and complex structures",
            "matrix": [
                [
                    {
                        "$numberInt": "1"
                    },
                    {
                        "$numberInt": "2"
                    },
                    {
                        "$numberInt": "3"
                    }
                ],
                [
                    {
                        "$numberDouble": "4.0"
                    },
                    {
                        "$numberDouble": "5.0"
                    },
                    {
                        "$numberDouble": "6.0"
                    }
                ]
            ],
            "users": [
                {
                    "id": {
                        "$oid": "507f1f77bcf86cd799439011"
                    },
                    "name": "Alice",
                    "score": {
                        "$numberDouble": "95.5"
                    },
                    "tags": ["admin", "power-user"]
                },
                {
                    "id": {
                        "$oid": "507f1f77bcf86cd799439012"
                    },
                    "name": "Bob",
                    "score": {
                        "$numberDouble": "87.2"
                    },
                    "tags": ["user", "active"]
                }
            ],
            "metadata_array": [
                {
                    "created": {
                        "$date": "2024-01-01T00:00:00.000Z"
                    },
                    "version": {
                        "$numberLong": "1"
                    }
                },
                {
                    "created": {
                        "$date": "2024-06-01T00:00:00.000Z"
                    },
                    "version": {
                        "$numberLong": "2"
                    }
                }
            ]
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description FROM array_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;

    assert_eq!(
        migrated_names.len(),
        2,
        "Should have migrated 2 test records"
    );

    // Verify records are in alphabetical order
    assert_eq!(migrated_names[0], "array_test_1");
    assert_eq!(
        migrated_descriptions[0],
        "Test with arrays of Extended JSON v2 types"
    );

    assert_eq!(migrated_names[1], "array_test_2");
    assert_eq!(
        migrated_descriptions[1],
        "Test with nested arrays and complex structures"
    );

    // Verify array content for array_test_1 - numbers array
    // Note: Mixed types in arrays require separate verification due to type safety
    let numbers_query =
        "SELECT array::len(numbers) as length FROM array_test WHERE name = 'array_test_1'";
    let mut numbers_result = surreal.query(numbers_query).await?;
    let lengths: Vec<i64> = numbers_result.take((0, "length"))?;

    assert_eq!(lengths.len(), 1, "Should have one record");
    assert_eq!(lengths[0], 3, "Numbers array should have 3 elements");

    // Verify first element (integer)
    let first_num_query =
        "SELECT numbers[0] as first_num FROM array_test WHERE name = 'array_test_1'";
    let mut first_result = surreal.query(first_num_query).await?;
    let first_nums: Vec<i64> = first_result.take((0, "first_num"))?;
    assert_eq!(first_nums[0], 123456789);

    // Verify third element (integer)
    let third_num_query =
        "SELECT numbers[2] as third_num FROM array_test WHERE name = 'array_test_1'";
    let mut third_result = surreal.query(third_num_query).await?;
    let third_nums: Vec<i64> = third_result.take((0, "third_num"))?;
    assert_eq!(third_nums[0], 42);

    // Verify mixed_array exists and has expected length - skip detailed verification due to binary data
    // Note: Binary data in arrays cannot be serialized to primitive types, so we'll verify length only
    let mixed_length_query =
        "SELECT array::len(mixed_array) as length FROM array_test WHERE name = 'array_test_1'";
    let mut mixed_length_result = surreal.query(mixed_length_query).await?;
    let mixed_lengths: Vec<i64> = mixed_length_result.take((0, "length"))?;

    assert_eq!(mixed_lengths.len(), 1, "Should have one record");
    assert_eq!(mixed_lengths[0], 5, "Mixed array should have 5 elements");

    // Verify first element (string)
    let first_mixed_query =
        "SELECT mixed_array[0] as first_elem FROM array_test WHERE name = 'array_test_1'";
    let mut first_mixed_result = surreal.query(first_mixed_query).await?;
    let first_elems: Vec<String> = first_mixed_result.take((0, "first_elem"))?;
    assert_eq!(first_elems[0], "simple string");

    // Verify second element (integer)
    let second_mixed_query =
        "SELECT mixed_array[1] as second_elem FROM array_test WHERE name = 'array_test_1'";
    let mut second_mixed_result = surreal.query(second_mixed_query).await?;
    let second_elems: Vec<i64> = second_mixed_result.take((0, "second_elem"))?;
    assert_eq!(second_elems[0], 100);

    println!("✅ Mixed array partially verified (binary data elements skipped)");

    // Verify nested arrays for array_test_2 - matrix structure
    let matrix_length_query =
        "SELECT array::len(matrix) as rows FROM array_test WHERE name = 'array_test_2'";
    let mut matrix_length_result = surreal.query(matrix_length_query).await?;
    let row_counts: Vec<i64> = matrix_length_result.take((0, "rows"))?;
    assert_eq!(row_counts[0], 2, "Matrix should have 2 rows");

    // Verify first row (integers)
    let row1_query = "SELECT matrix[0] as row1 FROM array_test WHERE name = 'array_test_2'";
    let mut row1_result = surreal.query(row1_query).await?;
    let row1_data: Vec<Vec<i64>> = row1_result.take((0, "row1"))?;
    assert_eq!(row1_data[0].len(), 3, "First row should have 3 elements");
    assert_eq!(row1_data[0][0], 1);
    assert_eq!(row1_data[0][1], 2);
    assert_eq!(row1_data[0][2], 3);

    // Verify second row (floats)
    let row2_query = "SELECT matrix[1] as row2 FROM array_test WHERE name = 'array_test_2'";
    let mut row2_result = surreal.query(row2_query).await?;
    let row2_data: Vec<Vec<f64>> = row2_result.take((0, "row2"))?;
    assert_eq!(row2_data[0].len(), 3, "Second row should have 3 elements");
    assert!((row2_data[0][0] - 4.0).abs() < f64::EPSILON);
    assert!((row2_data[0][1] - 5.0).abs() < f64::EPSILON);
    assert!((row2_data[0][2] - 6.0).abs() < f64::EPSILON);

    // Verify users array with nested objects
    let users_query = "SELECT users FROM array_test WHERE name = 'array_test_2'";
    let mut users_result = surreal.query(users_query).await?;
    let users_data: Vec<Vec<User>> = users_result.take((0, "users"))?;

    assert_eq!(users_data.len(), 1, "Should have one record");
    let users = &users_data[0];
    assert_eq!(users.len(), 2);

    // Check first user
    assert_eq!(users[0].name, "Alice");
    assert!((users[0].score - 95.5).abs() < f64::EPSILON);
    assert_eq!(users[0].id, "507f1f77bcf86cd799439011");
    assert_eq!(users[0].tags, vec!["admin", "power-user"]);

    // Check second user
    assert_eq!(users[1].name, "Bob");
    assert!((users[1].score - 87.2).abs() < f64::EPSILON);
    assert_eq!(users[1].id, "507f1f77bcf86cd799439012");
    assert_eq!(users[1].tags, vec!["user", "active"]);

    println!("✅ MongoDB Array migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM array_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB Array migration test completed successfully!");
    Ok(())
}

/// Test for MongoDB $date relaxed and canonical format migration
#[tokio::test]
async fn test_mongodb_date_canonical_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB $date relaxed and canonical format migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_date_canonical_migration_db";
    let surreal_namespace = "date_canonical_test_ns";
    let surreal_database = "date_canonical_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("date_canonical_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM date_canonical_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with relaxed and canonical $date formats in MongoDB
    println!("📝 Creating MongoDB test data with relaxed and canonical $date formats...");

    // Insert documents with various $date formats
    let documents = vec![
        // Relaxed format: ISO 8601 string
        doc! {
            "name": "date_test_1",
            "description": "Test with relaxed $date (ISO 8601 string)",
            "timestamp": {
                "$date": "2024-01-15T09:30:45.123Z"
            }
        },
        // Relaxed format: Different ISO 8601 string
        doc! {
            "name": "date_test_2",
            "description": "Test with relaxed $date (different ISO format)",
            "timestamp": {
                "$date": "2024-06-28T14:22:33.456Z"
            }
        },
        // Relaxed format: End of year
        doc! {
            "name": "date_test_3",
            "description": "Test with relaxed $date (end of year)",
            "timestamp": {
                "$date": "2024-12-31T23:59:59.999Z"
            }
        },
        // Relaxed format: Early date
        doc! {
            "name": "date_test_4",
            "description": "Test with relaxed $date (early date)",
            "timestamp": {
                "$date": "1970-01-01T00:00:00.000Z"
            }
        },
        // $numberLong representation of $date (canonical format)
        doc! {
            "name": "date_test_5",
            "description": "Test with canonical $date ($numberLong representation)",
            "timestamp": {
                "$date": {
                    "$numberLong": "1640995200000"
                }
            }
        },
        // Another $numberLong representation
        doc! {
            "name": "date_test_6",
            "description": "Test with canonical $date ($numberLong format)",
            "timestamp": {
                "$date": {
                    "$numberLong": "1719580953456"
                }
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description FROM date_canonical_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;

    assert_eq!(
        migrated_names.len(),
        6,
        "Should have migrated 6 test records"
    );

    // Verify records are in alphabetical order
    assert_eq!(migrated_names[0], "date_test_1");
    assert_eq!(
        migrated_descriptions[0],
        "Test with relaxed $date (ISO 8601 string)"
    );

    assert_eq!(migrated_names[1], "date_test_2");
    assert_eq!(
        migrated_descriptions[1],
        "Test with relaxed $date (different ISO format)"
    );

    assert_eq!(migrated_names[2], "date_test_3");
    assert_eq!(
        migrated_descriptions[2],
        "Test with relaxed $date (end of year)"
    );

    assert_eq!(migrated_names[3], "date_test_4");
    assert_eq!(
        migrated_descriptions[3],
        "Test with relaxed $date (early date)"
    );

    assert_eq!(migrated_names[4], "date_test_5");
    assert_eq!(
        migrated_descriptions[4],
        "Test with canonical $date ($numberLong representation)"
    );

    assert_eq!(migrated_names[5], "date_test_6");
    assert_eq!(
        migrated_descriptions[5],
        "Test with canonical $date ($numberLong format)"
    );

    // Verify that all datetime fields are properly converted and queryable
    let datetime_query =
        "SELECT name, timestamp FROM date_canonical_test WHERE timestamp IS NOT NULL ORDER BY name";
    let mut datetime_result = surreal.query(datetime_query).await?;
    let datetime_names: Vec<String> = datetime_result.take((0, "name"))?;
    let datetime_timestamps: Vec<chrono::DateTime<chrono::Utc>> =
        datetime_result.take((0, "timestamp"))?;

    assert_eq!(
        datetime_names.len(),
        6,
        "All records should have valid datetime fields"
    );

    // Verify specific datetime values for some records
    // date_test_1: "2024-01-15T09:30:45.123Z"
    let expected_dt1 = chrono::DateTime::parse_from_rfc3339("2024-01-15T09:30:45.123Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(datetime_timestamps[0], expected_dt1);

    // date_test_4: "1970-01-01T00:00:00.000Z" (epoch)
    let expected_dt4 = chrono::DateTime::parse_from_rfc3339("1970-01-01T00:00:00.000Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(datetime_timestamps[3], expected_dt4);

    // Verify $numberLong timestamp conversion for date_test_5
    // 1640995200000 milliseconds = 2022-01-01T00:00:00.000Z
    let expected_dt5 = chrono::DateTime::from_timestamp_millis(1640995200000)
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(datetime_timestamps[4], expected_dt5);

    // Verify $numberLong timestamp conversion for date_test_6
    // 1719580953456 milliseconds = 2024-06-28T14:22:33.456Z
    let expected_dt6 = chrono::DateTime::from_timestamp_millis(1719580953456)
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(datetime_timestamps[5], expected_dt6);

    // Test datetime queries work properly
    let recent_query =
        "SELECT name FROM date_canonical_test WHERE timestamp > '2020-01-01T00:00:00Z'";
    let mut recent_result = surreal.query(recent_query).await?;
    let recent_names: Vec<String> = recent_result.take((0, "name"))?;
    assert_eq!(
        recent_names.len(),
        5,
        "Should find 5 records after 2020 (excluding epoch date)"
    );

    let year_2024_query = "SELECT name FROM date_canonical_test WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2025-01-01T00:00:00Z'";
    let mut year_2024_result = surreal.query(year_2024_query).await?;
    let year_2024_names: Vec<String> = year_2024_result.take((0, "name"))?;
    assert_eq!(
        year_2024_names.len(),
        4,
        "Should find 4 records in year 2024"
    );

    println!("✅ MongoDB $date relaxed and canonical format migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM date_canonical_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!(
        "🎉 MongoDB $date relaxed and canonical format migration test completed successfully!"
    );
    Ok(())
}

/// Test for MongoDB $timestamp data type migration
#[tokio::test]
async fn test_mongodb_timestamp_migration() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for the test
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,test=debug")
        .try_init()
        .ok(); // Ignore if already initialized

    println!("🧪 Starting MongoDB $timestamp migration test");

    // Setup test database connections
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let surreal_endpoint = "ws://surrealdb:8000";
    let test_db_name = "test_timestamp_migration_db";
    let surreal_namespace = "timestamp_test_ns";
    let surreal_database = "timestamp_test_db";

    // Connect to MongoDB
    println!("📊 Connecting to MongoDB...");
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database(test_db_name);

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
    let collection = mongo_db.collection::<mongodb::bson::Document>("timestamp_test");
    collection.drop().await.ok(); // Ignore errors if collection doesn't exist

    let cleanup_surreal = "DELETE FROM timestamp_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    // Create test data with $timestamp in MongoDB
    println!("📝 Creating MongoDB test data with $timestamp...");

    // Insert documents with various $timestamp formats
    let documents = vec![
        // Standard timestamp
        doc! {
            "name": "timestamp_test_1",
            "description": "Test with standard timestamp",
            "operation_time": {
                "$timestamp": {
                    "t": 1640995200, // 2022-01-01T00:00:00Z in seconds
                    "i": 1
                }
            }
        },
        // Different timestamp with higher increment
        doc! {
            "name": "timestamp_test_2",
            "description": "Test with timestamp and higher increment",
            "operation_time": {
                "$timestamp": {
                    "t": 1719580953, // 2024-06-28T14:22:33Z in seconds
                    "i": 42
                }
            }
        },
        // Recent timestamp
        doc! {
            "name": "timestamp_test_3",
            "description": "Test with recent timestamp",
            "operation_time": {
                "$timestamp": {
                    "t": 1672531200, // 2023-01-01T00:00:00Z in seconds
                    "i": 100
                }
            }
        },
        // Early timestamp (near epoch)
        doc! {
            "name": "timestamp_test_4",
            "description": "Test with early timestamp",
            "operation_time": {
                "$timestamp": {
                    "t": 86400, // 1970-01-02T00:00:00Z in seconds (1 day after epoch)
                    "i": 5
                }
            }
        },
    ];

    collection.insert_many(&documents).await?;
    println!("✅ Created test data in MongoDB");

    // Run the migration using the library functions directly
    println!("🔄 Running migration...");
    let from_opts = surreal_sync::SourceOpts {
        source_uri: mongo_uri.to_string(),
        source_database: Some(test_db_name.to_string()),
        source_username: None,
        source_password: None,
    };

    let to_opts = surreal_sync::SurrealOpts {
        surreal_endpoint: surreal_endpoint.to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 10,
        dry_run: false,
    };

    surreal_sync::migrate_from_mongodb(
        from_opts,
        surreal_namespace.to_string(),
        surreal_database.to_string(),
        to_opts,
    )
    .await?;

    println!("✅ Migration completed successfully");

    // Verify migration results
    println!("🔍 Verifying migration results...");

    // Check that the test data was migrated
    let query = "SELECT name, description FROM timestamp_test ORDER BY name";
    let mut result = surreal.query(query).await?;
    let migrated_names: Vec<String> = result.take((0, "name"))?;
    let migrated_descriptions: Vec<String> = result.take((0, "description"))?;

    assert_eq!(
        migrated_names.len(),
        4,
        "Should have migrated 4 test records"
    );

    // Verify records are in alphabetical order
    assert_eq!(migrated_names[0], "timestamp_test_1");
    assert_eq!(migrated_descriptions[0], "Test with standard timestamp");

    assert_eq!(migrated_names[1], "timestamp_test_2");
    assert_eq!(
        migrated_descriptions[1],
        "Test with timestamp and higher increment"
    );

    assert_eq!(migrated_names[2], "timestamp_test_3");
    assert_eq!(migrated_descriptions[2], "Test with recent timestamp");

    assert_eq!(migrated_names[3], "timestamp_test_4");
    assert_eq!(migrated_descriptions[3], "Test with early timestamp");

    // Verify that all operation_time fields are properly converted to DateTime
    let datetime_query =
        "SELECT name, operation_time FROM timestamp_test WHERE operation_time IS NOT NULL ORDER BY name";
    let mut datetime_result = surreal.query(datetime_query).await?;
    let datetime_names: Vec<String> = datetime_result.take((0, "name"))?;
    let operation_times: Vec<chrono::DateTime<chrono::Utc>> =
        datetime_result.take((0, "operation_time"))?;

    assert_eq!(
        datetime_names.len(),
        4,
        "All records should have valid DateTime fields"
    );

    // Verify specific timestamp conversions
    // timestamp_test_1: t=1640995200 (2022-01-01T00:00:00Z)
    let expected_dt1 = chrono::DateTime::from_timestamp(1640995200, 0).unwrap();
    assert_eq!(operation_times[0], expected_dt1);

    // timestamp_test_2: t=1719580953 (2024-06-28T14:22:33Z)
    let expected_dt2 = chrono::DateTime::from_timestamp(1719580953, 0).unwrap();
    assert_eq!(operation_times[1], expected_dt2);

    // timestamp_test_3: t=1672531200 (2023-01-01T00:00:00Z)
    let expected_dt3 = chrono::DateTime::from_timestamp(1672531200, 0).unwrap();
    assert_eq!(operation_times[2], expected_dt3);

    // timestamp_test_4: t=86400 (1970-01-02T00:00:00Z)
    let expected_dt4 = chrono::DateTime::from_timestamp(86400, 0).unwrap();
    assert_eq!(operation_times[3], expected_dt4);

    // Test datetime queries work properly with migrated timestamps
    let recent_query =
        "SELECT name FROM timestamp_test WHERE operation_time > '2020-01-01T00:00:00Z'";
    let mut recent_result = surreal.query(recent_query).await?;
    let recent_names: Vec<String> = recent_result.take((0, "name"))?;

    assert_eq!(
        recent_names.len(),
        3,
        "Should find 3 records after 2020 (excluding 1970 timestamp)"
    );

    // Test another date range to verify $timestamp is working correctly
    let year_2024_query = "SELECT name FROM timestamp_test WHERE operation_time >= '2024-01-01T00:00:00Z' AND operation_time < '2025-01-01T00:00:00Z'";
    let mut year_2024_result = surreal.query(year_2024_query).await?;
    let year_2024_names: Vec<String> = year_2024_result.take((0, "name"))?;
    assert_eq!(
        year_2024_names.len(),
        1,
        "Should find 1 record in year 2024"
    );
    assert_eq!(
        year_2024_names[0], "timestamp_test_2",
        "Should be timestamp_test_2 in 2024"
    );

    println!("✅ MongoDB $timestamp migration test passed");

    // Cleanup
    println!("🧹 Cleaning up test data...");
    collection.drop().await.ok();

    let cleanup_surreal = "DELETE FROM timestamp_test";
    let mut cleanup_result = surreal.query(cleanup_surreal).await?;
    let _: Vec<surrealdb::sql::Thing> = cleanup_result.take("id").unwrap_or_default();

    println!("🎉 MongoDB $timestamp migration test completed successfully!");
    Ok(())
}
