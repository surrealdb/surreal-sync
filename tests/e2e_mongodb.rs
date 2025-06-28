use base64::{self, Engine};
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use serde_json::Value;
use std::collections::HashMap;
use surrealdb::{engine::any::connect, Surreal};

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
        let _: Vec<surrealdb::sql::Thing> = surreal.query(query).await?.take("id").unwrap_or_default();
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

/// Check if two records match (ignoring ID fields)
fn records_match(expected: &Value, actual: &Value) -> bool {
    let expected_obj = expected.as_object().unwrap();
    let actual_obj = actual.as_object().unwrap();

    // Check that key fields match (excluding _id and id)
    for (key, expected_value) in expected_obj {
        if key == "_id" {
            continue; // Skip MongoDB _id
        }

        match actual_obj.get(key) {
            Some(actual_value) => {
                if !values_equal(expected_value, actual_value) {
                    println!(
                        "🔍 Field '{}' mismatch: expected {:?}, got {:?}",
                        key, expected_value, actual_value
                    );
                    return false;
                }
            }
            None => {
                println!("🔍 Field '{}' missing in actual record", key);
                return false;
            }
        }
    }

    true
}

/// Deep comparison of JSON values, handling MongoDB-specific types
fn values_equal(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            // Handle nested objects
            for (key, expected_value) in e {
                match a.get(key) {
                    Some(actual_value) => {
                        if !values_equal(expected_value, actual_value) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::Array(e), Value::Array(a)) => {
            // Handle arrays
            if e.len() != a.len() {
                return false;
            }
            e.iter()
                .zip(a.iter())
                .all(|(e_item, a_item)| values_equal(e_item, a_item))
        }
        // Handle MongoDB DateTime objects (converted to ISO strings)
        (expected_val, actual_val)
            if expected_val.is_object() && expected_val.get("$date").is_some() =>
        {
            // MongoDB DateTime should be converted to a timestamp or date string
            actual_val.is_string() || actual_val.is_number()
        }
        // Direct value comparison
        (e, a) => e == a,
    }
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
    let migrated_names: Vec<String> = result.take("name")?;
    let migrated_descriptions: Vec<String> = result.take("description")?;

    assert_eq!(
        migrated_names.len(),
        2,
        "Should have migrated 2 test records"
    );

    // Verify records are in alphabetical order: binary_test_1, binary_test_2
    assert_eq!(migrated_names[0], "binary_test_1");
    assert_eq!(migrated_descriptions[0], "Test with canonical $binary format");
    
    assert_eq!(migrated_names[1], "binary_test_2");
    assert_eq!(migrated_descriptions[1], "Test with canonical $binary format (subType 01)");

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
