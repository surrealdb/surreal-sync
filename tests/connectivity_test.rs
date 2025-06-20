use mongodb::{Client as MongoClient, options::ClientOptions, bson::doc};
use surrealdb::{engine::any::connect};
use std::time::Duration;

#[tokio::test]
async fn test_mongodb_connectivity() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing MongoDB connectivity...");
    
    let mongo_uri = "mongodb://root:root@mongodb:27017";
    let mut mongo_options = ClientOptions::parse(mongo_uri).await?;
    
    // Add reasonable timeouts
    mongo_options.connect_timeout = Some(Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(Duration::from_secs(10));
    
    let mongo_client = MongoClient::with_options(mongo_options)?;
    let mongo_db = mongo_client.database("test_connectivity");
    
    // Try to ping the database
    let ping_result = mongo_db.run_command(doc! { "ping": 1 }).await;
    assert!(ping_result.is_ok(), "MongoDB ping failed: {:?}", ping_result);
    
    println!("✅ MongoDB connectivity test passed");
    Ok(())
}

#[tokio::test] 
async fn test_surrealdb_connectivity() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing SurrealDB connectivity...");
    
    let surreal_endpoint = "ws://surrealdb:8000";
    let surreal = connect(surreal_endpoint).await?;
    
    // Try to sign in
    surreal.signin(surrealdb::opt::auth::Root {
        username: "root",
        password: "root",
    }).await?;
    
    // Try to use a test namespace and database
    surreal.use_ns("test").use_db("test").await?;
    
    // Try a simple query
    let result: Vec<serde_json::Value> = surreal.query("SELECT * FROM 1").await?.take(0)?;
    assert!(!result.is_empty(), "SurrealDB query returned empty result");
    
    println!("✅ SurrealDB connectivity test passed");
    Ok(())
}