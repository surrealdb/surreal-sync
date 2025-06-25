//! SurrealSync Library
//!
//! A library for migrating data from Neo4j and MongoDB databases to SurrealDB.

use chrono::{DateTime, Utc};
use clap::Parser;
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use surrealdb::{
    engine::any::connect,
    sql::{
        Array as SurrealArray, Number as SurrealNumber, Object as SurrealObject, Strand,
        Value as SurrealValue,
    },
    Surreal,
};

#[derive(Parser)]
pub struct SourceOpts {
    /// Source database connection string/URI
    #[arg(long, env = "SOURCE_URI")]
    pub source_uri: String,

    /// Source database name
    #[arg(long, env = "SOURCE_DATABASE")]
    pub source_database: Option<String>,

    /// Source database username
    #[arg(long, env = "SOURCE_USERNAME")]
    pub source_username: Option<String>,

    /// Source database password
    #[arg(long, env = "SOURCE_PASSWORD")]
    pub source_password: Option<String>,
}

#[derive(Parser)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    #[arg(
        long,
        default_value = "http://localhost:8000",
        env = "SURREAL_ENDPOINT"
    )]
    pub surreal_endpoint: String,

    /// SurrealDB username
    #[arg(long, default_value = "root", env = "SURREAL_USERNAME")]
    pub surreal_username: String,

    /// SurrealDB password
    #[arg(long, default_value = "root", env = "SURREAL_PASSWORD")]
    pub surreal_password: String,

    /// Batch size for data migration
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Dry run mode - don't actually write data
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn migrate_from_mongodb(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting MongoDB migration");
    tracing::debug!(
        "migrate_from_mongodb function called with URI: {}",
        from_opts.source_uri
    );

    // Connect to MongoDB
    tracing::debug!(
        "About to parse MongoDB connection options from URI: {}",
        from_opts.source_uri
    );
    let mut mongo_options = match ClientOptions::parse(&from_opts.source_uri).await {
        Ok(options) => {
            tracing::debug!("MongoDB options parsed successfully");
            options
        }
        Err(e) => {
            tracing::error!("Failed to parse MongoDB connection options: {}", e);
            return Err(e.into());
        }
    };
    // Add connection timeout to prevent hanging
    mongo_options.connect_timeout = Some(Duration::from_secs(10));
    mongo_options.server_selection_timeout = Some(Duration::from_secs(10));
    tracing::debug!("Added timeouts to MongoDB options");

    tracing::debug!("Creating MongoDB client with parsed options");
    let mongo_client = MongoClient::with_options(mongo_options)?;
    tracing::debug!("MongoDB client created successfully");

    // Get the source database
    let source_db_name = from_opts
        .source_database
        .ok_or_else(|| anyhow::anyhow!("MongoDB source database name is required"))?;
    tracing::debug!("Using MongoDB database: {}", source_db_name);
    let mongo_db = mongo_client.database(&source_db_name);

    // Connect to SurrealDB
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    tracing::debug!("Connecting to SurrealDB at: {}", surreal_endpoint);
    let surreal = connect(surreal_endpoint).await?;
    tracing::debug!("SurrealDB connection established");

    tracing::debug!(
        "Signing in to SurrealDB with username: {}",
        to_opts.surreal_username
    );
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;
    tracing::debug!("SurrealDB signin successful");

    tracing::debug!(
        "Using SurrealDB namespace: {} and database: {}",
        to_namespace,
        to_database
    );
    surreal.use_ns(&to_namespace).use_db(&to_database).await?;
    tracing::debug!("SurrealDB namespace and database selected");

    tracing::info!("Connected to both MongoDB and SurrealDB");

    // Get list of collections from MongoDB
    tracing::debug!("Listing collection names from MongoDB database");
    let collection_names = mongo_db.list_collection_names().await?;
    tracing::info!("Found {} collections in MongoDB", collection_names.len());
    tracing::debug!("Collections: {:?}", collection_names);

    let mut total_migrated = 0;

    for collection_name in collection_names {
        tracing::info!("Migrating collection: {}", collection_name);

        tracing::debug!("Getting collection handle for: {}", collection_name);
        let collection = mongo_db.collection::<mongodb::bson::Document>(&collection_name);

        // Count total documents in collection
        tracing::debug!("Counting documents in collection: {}", collection_name);
        let total_docs = collection.count_documents(doc! {}).await?;
        tracing::info!(
            "Collection '{}' contains {} documents",
            collection_name,
            total_docs
        );

        if total_docs == 0 {
            continue;
        }

        // Process documents in batches
        tracing::debug!("Creating cursor for collection: {}", collection_name);
        let mut cursor = collection.find(doc! {}).await?;
        tracing::debug!(
            "Cursor created successfully for collection: {}",
            collection_name
        );
        let mut batch = Vec::new();
        let mut processed = 0;

        tracing::debug!(
            "Starting to iterate through documents in collection: {}",
            collection_name
        );
        while cursor.advance().await? {
            tracing::trace!(
                "Processing document {} in collection: {}",
                processed + 1,
                collection_name
            );
            let doc = cursor.current();
            tracing::trace!("Got current document from cursor");

            // Convert MongoDB BSON document to JSON
            tracing::trace!("Converting RawDocument to owned Document");
            let doc_owned: mongodb::bson::Document = doc.try_into()?;
            tracing::trace!("Converting Document to JSON Value");

            // Add debug logging to see the BSON document before conversion
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("BSON document before conversion: {:?}", doc_owned);
            }

            let json_value: Value = mongodb::bson::from_document(doc_owned)?;
            tracing::trace!("Document conversion completed");

            // Add debug logging to see the JSON after conversion
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("JSON value after conversion: {:?}", json_value);
            }

            // Extract MongoDB ObjectId and use it as SurrealDB record ID
            let record_id = if let Some(id_value) = json_value.get("_id") {
                if let Some(oid) = id_value.get("$oid") {
                    // Use the ObjectId string as the record ID
                    format!("{}:{}", collection_name, oid.as_str().unwrap_or("unknown"))
                } else {
                    // Handle other ID formats (string, number, etc.)
                    format!(
                        "{}:{}",
                        collection_name,
                        id_value.to_string().trim_matches('"')
                    )
                }
            } else {
                // This shouldn't happen as MongoDB always has _id, but fallback to table name only
                collection_name.to_string()
            };

            // Remove MongoDB _id and let SurrealDB use the record_id we specify
            let mut surreal_doc = json_value.clone();
            if let Some(obj) = surreal_doc.as_object_mut() {
                obj.remove("_id");
            }

            // Convert MongoDB-specific types to bindable values
            let bindable_object = convert_mongodb_object_to_bindable(surreal_doc)?;

            // Add debug logging to see the final document before adding to batch
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Final document for SurrealDB (record_id: {}): {:?}",
                    record_id,
                    bindable_object
                );
            }

            batch.push((record_id, bindable_object));

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for collection: {}",
                    batch.len(),
                    collection_name
                );
                if !to_opts.dry_run {
                    tracing::debug!("Migrating batch of {} documents to SurrealDB", batch.len());
                    migrate_batch(&surreal, &collection_name, &batch).await?;
                    tracing::debug!("Batch migration completed");
                } else {
                    tracing::debug!("Dry-run mode: skipping actual migration of batch");
                }
                processed += batch.len();
                total_migrated += batch.len();
                tracing::info!(
                    "Processed {}/{} documents from '{}'",
                    processed,
                    total_docs,
                    collection_name
                );
                batch.clear();
            }
        }

        // Process remaining documents in the last batch
        if !batch.is_empty() {
            tracing::debug!(
                "Processing final batch of {} documents for collection: {}",
                batch.len(),
                collection_name
            );
            if !to_opts.dry_run {
                tracing::debug!(
                    "Migrating final batch of {} documents to SurrealDB",
                    batch.len()
                );
                migrate_batch(&surreal, &collection_name, &batch).await?;
                tracing::debug!("Final batch migration completed");
            } else {
                tracing::debug!("Dry-run mode: skipping actual migration of final batch");
            }
            processed += batch.len();
            total_migrated += batch.len();
        }

        tracing::info!(
            "Completed migration of collection '{}': {} documents",
            collection_name,
            processed
        );
    }

    tracing::info!(
        "MongoDB migration completed: {} total documents migrated",
        total_migrated
    );
    Ok(())
}

fn extract_object_fields(value: &SurrealValue) -> Option<&BTreeMap<String, SurrealValue>> {
    match value {
        SurrealValue::Object(obj) => Some(obj),
        _ => None,
    }
}

async fn migrate_batch(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[(String, std::collections::HashMap<String, BindableValue>)],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, (record_id, document)) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}: {}", i + 1, batch.len(), record_id);

        // Extract fields from the bindable document
        let fields = document;

        // Build flattened field list for the query
        let field_bindings: Vec<String> = fields
            .keys()
            .map(|key| format!("{}: ${}", key, key))
            .collect();
        let content_fields = format!("{{{}}}", field_bindings.join(", "));
        let query = format!("CREATE {} CONTENT {}", record_id, content_fields);

        tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);

        // Add debug logging to see the document being bound
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::debug!(
                "Binding document to SurrealDB query for record {}: {:?}",
                record_id,
                document
            );
        }

        // Build query with individual field bindings
        let mut q = surreal.query(query);
        for (field_name, field_value) in fields {
            tracing::debug!("Binding field: {} to value: {:?}", field_name, field_value);
            q = match field_value {
                BindableValue::Bool(b) => q.bind((field_name.clone(), *b)),
                BindableValue::Int(i) => q.bind((field_name.clone(), *i)),
                BindableValue::Float(f) => q.bind((field_name.clone(), *f)),
                BindableValue::String(s) => q.bind((field_name.clone(), s.clone())),
                BindableValue::DateTime(dt) => q.bind((field_name.clone(), *dt)),
                BindableValue::Array(arr) => q.bind((field_name.clone(), arr.clone())),
                BindableValue::Object(obj) => q.bind((field_name.clone(), obj.clone())),
                BindableValue::Null => q.bind((field_name.clone(), Option::<String>::None)),
            };
        }

        let mut response: surrealdb::Response = q.await?;
        let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> = response.take("id");

        match result {
            Ok(res) => {
                if res.is_empty() {
                    tracing::warn!("Failed to create record: {}", record_id);
                } else {
                    tracing::trace!("Successfully created record: {}", record_id);
                }
            }
            Err(e) => {
                tracing::error!("Error creating record {}: {}", record_id, e);
                if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                    tracing::error!("Problematic document: {:?}", document);
                }
                return Err(e.into());
            }
        }
    }

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}

pub async fn migrate_from_neo4j(
    _from_opts: SourceOpts,
    _to_namespace: String,
    _to_database: String,
    _to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    tracing::info!("Neo4j migration not yet implemented");
    // TODO: Implement Neo4j migration
    Ok(())
}

/// Convert MongoDB Object to bindable HashMap with BindableValue
fn convert_mongodb_object_to_bindable(
    value: Value,
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    match value {
        Value::Object(obj) => {
            let mut bindable_obj = std::collections::HashMap::new();
            for (key, val) in obj {
                let bindable_val = convert_mongodb_types_to_bindable(val)?;
                bindable_obj.insert(key, bindable_val);
            }
            Ok(bindable_obj)
        }
        _ => Err(anyhow::anyhow!(
            "Input must be a JSON Object to convert to bindable HashMap"
        )),
    }
}

/// Enum to represent bindable values that can be bound to SurrealDB queries
#[derive(Debug, Clone)]
enum BindableValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Array(Vec<serde_json::Value>), // For arrays, we'll use JSON since SurrealDB accepts Vec<T> where T: Into<Value>
    Object(serde_json::Map<String, serde_json::Value>), // For objects, use JSON
    Null,
}

/// Convert MongoDB-specific JSON structures to bindable Rust types
fn convert_mongodb_types_to_bindable(value: Value) -> anyhow::Result<BindableValue> {
    match value {
        Value::Object(obj) => {
            // Handle MongoDB DateTime objects
            if let Some(date_str) = obj.get("$date").and_then(|v| v.as_str()) {
                // Return actual DateTime<Utc> for binding
                let utc_datetime = chrono::DateTime::parse_from_rfc3339(date_str)
                    .map_err(|e| anyhow::anyhow!("Failed to parse MongoDB datetime: {}", e))?
                    .to_utc();
                return Ok(BindableValue::DateTime(utc_datetime));
            }

            // Handle MongoDB ObjectId objects
            if let Some(oid_str) = obj.get("$oid").and_then(|v| v.as_str()) {
                // Return string for binding
                return Ok(BindableValue::String(oid_str.to_string()));
            }

            // Handle MongoDB NumberLong objects
            if let Some(number_long) = obj.get("$numberLong").and_then(|v| v.as_str()) {
                if let Ok(num) = number_long.parse::<i64>() {
                    return Ok(BindableValue::Int(num));
                }
            }

            // Handle MongoDB NumberDecimal objects
            if let Some(number_decimal) = obj.get("$numberDecimal").and_then(|v| v.as_str()) {
                if let Ok(num) = number_decimal.parse::<f64>() {
                    return Ok(BindableValue::Float(num));
                }
            }

            // Recursively process nested objects - return as JSON for binding
            let mut json_obj = serde_json::Map::new();
            for (key, val) in obj {
                let bindable_val = convert_mongodb_types_to_bindable(val)?;
                json_obj.insert(key, bindable_to_json(&bindable_val)?);
            }
            Ok(BindableValue::Object(json_obj))
        }
        Value::Array(arr) => {
            // Recursively process array elements - return as JSON array for binding
            let mut json_arr = Vec::new();
            for item in arr {
                let bindable_val = convert_mongodb_types_to_bindable(item)?;
                json_arr.push(bindable_to_json(&bindable_val)?);
            }
            Ok(BindableValue::Array(json_arr))
        }
        // Convert JSON values to bindable types
        Value::Bool(b) => Ok(BindableValue::Bool(b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(BindableValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(BindableValue::Float(f))
            } else {
                // Fallback to string representation for unusual number types
                Ok(BindableValue::String(n.to_string()))
            }
        }
        Value::String(s) => Ok(BindableValue::String(s)),
        Value::Null => Ok(BindableValue::Null),
    }
}

/// Convert BindableValue to JSON for nested objects/arrays
fn bindable_to_json(value: &BindableValue) -> anyhow::Result<serde_json::Value> {
    match value {
        BindableValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        BindableValue::Int(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        BindableValue::Float(f) => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
        )),
        BindableValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        BindableValue::DateTime(dt) => Ok(serde_json::Value::String(dt.to_rfc3339())),
        BindableValue::Array(arr) => Ok(serde_json::Value::Array(arr.clone())),
        BindableValue::Object(obj) => Ok(serde_json::Value::Object(obj.clone())),
        BindableValue::Null => Ok(serde_json::Value::Null),
    }
}
