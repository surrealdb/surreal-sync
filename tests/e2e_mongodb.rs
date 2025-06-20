use mongodb::{Client as MongoClient, options::ClientOptions, bson::doc};
use surrealdb::{Surreal, engine::any::connect};
use serde_json::Value;
use std::collections::HashMap;
use tokio;

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
    surreal.signin(surrealdb::opt::auth::Root {
        username: "root",
        password: "root",
    }).await?;
    surreal.use_ns(surreal_namespace).use_db(surreal_database).await?;

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
    ).await?;

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
        let _: Vec<Value> = surreal.query(query).await?.take(0).unwrap_or_default();
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
        }
    ];
    users_collection.insert_many(users_data.clone()).await?;

    // Convert BSON to JSON for validation
    let users_json: Vec<Value> = users_data.iter()
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
        }
    ];
    products_collection.insert_many(products_data.clone()).await?;

    let products_json: Vec<Value> = products_data.iter()
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
        }
    ];
    categories_collection.insert_many(categories_data.clone()).await?;

    let categories_json: Vec<Value> = categories_data.iter()
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
        let actual_records: Vec<Value> = result.take(0)?;

        // Check record count
        assert_eq!(
            actual_records.len(),
            expected_records.len(),
            "Record count mismatch for table '{}': expected {}, got {}",
            table_name,
            expected_records.len(),
            actual_records.len()
        );

        // Validate each record exists and has correct structure
        for expected_record in expected_records {
            let found = actual_records.iter().any(|actual_record| {
                // Check key fields match (excluding MongoDB _id and SurrealDB id)
                records_match(expected_record, actual_record)
            });

            assert!(
                found,
                "Expected record not found in SurrealDB table '{}': {:?}",
                table_name,
                expected_record
            );
        }

        // Validate that SurrealDB records have proper IDs
        for actual_record in &actual_records {
            assert!(
                actual_record.get("id").is_some(),
                "SurrealDB record missing 'id' field: {:?}",
                actual_record
            );

            // Verify the ID format (should be table:objectid)
            if let Some(id_value) = actual_record.get("id") {
                if let Some(id_str) = id_value.as_str() {
                    assert!(
                        id_str.starts_with(&format!("{}:", table_name)),
                        "SurrealDB record ID format incorrect: expected '{}:*', got '{}'",
                        table_name,
                        id_str
                    );
                }
            }
        }

        println!("✅ Table '{}' validation passed ({} records)", table_name, actual_records.len());
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
                    println!("🔍 Field '{}' mismatch: expected {:?}, got {:?}", key, expected_value, actual_value);
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
            e.iter().zip(a.iter()).all(|(e_item, a_item)| values_equal(e_item, a_item))
        }
        // Handle MongoDB DateTime objects (converted to ISO strings)
        (expected_val, actual_val) if expected_val.is_object() && expected_val.get("$date").is_some() => {
            // MongoDB DateTime should be converted to a timestamp or date string
            actual_val.is_string() || actual_val.is_number()
        }
        // Direct value comparison
        (e, a) => e == a,
    }
}