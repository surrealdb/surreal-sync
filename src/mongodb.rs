use base64::{self, Engine};
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use serde_json::Value;
use std::{collections::HashMap, time::Duration};
use surrealdb::engine::any::connect;

use crate::{BindableValue, SourceOpts, SurrealOpts};

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
                    crate::migrate_batch(&surreal, &collection_name, &batch).await?;
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
                crate::migrate_batch(&surreal, &collection_name, &batch).await?;
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

/// Convert MongoDB Extended JSON (v2) values to our own bindables
fn convert_mongodb_types_to_bindable(value: Value) -> anyhow::Result<BindableValue> {
    match value {
        Value::Object(obj) => {
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Date
            if let Some(date_str) = obj.get("$date").and_then(|v| v.as_str()) {
                // Relaxed Format of Date is a string representation of a date and time in ISO-8601
                let utc_datetime = chrono::DateTime::parse_from_rfc3339(date_str)
                    .map_err(|e| anyhow::anyhow!("Failed to parse MongoDB datetime: {}", e))?
                    .to_utc();
                return Ok(BindableValue::DateTime(utc_datetime));
            } else if let Some(serde_json::Value::Object(obj)) = obj.get("$date") {
                // Canonical Format of Date is a JSON object with the following fields:
                // - $numberLong: The number of milliseconds since the Unix epoch (can be negative in case it's before 1970 or after 9999)
                if let Some(number_long) = obj.get("$numberLong").and_then(|v| v.as_str()) {
                    if let Ok(num) = number_long.parse::<i64>() {
                        if num < 0 {
                            // TODO Handle negative timestamps
                            // See "For dates before year 1970 or after year 9999:" in https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Date
                            return Err(anyhow::anyhow!("Negative timestamps are not supported"));
                        }
                        let datetime = chrono::DateTime::from_timestamp_millis(num);
                        match datetime {
                            Some(datetime) => return Ok(BindableValue::DateTime(datetime)),
                            None => {
                                tracing::warn!("Failed to parse MongoDB datetime: {}", num);
                                return Err(anyhow::anyhow!("Failed to parse MongoDB datetime"));
                            }
                        }
                    }
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-ObjectId
            if let Some(oid_str) = obj.get("$oid").and_then(|v| v.as_str()) {
                // Return string for binding
                return Ok(BindableValue::String(oid_str.to_string()));
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Timestamp
            if let Some(timestamp_obj) = obj.get("$timestamp") {
                if let Some(ts) = timestamp_obj.as_object() {
                    // Canonical format: {"$timestamp": {"t": <pos-integer>, "i": <pos-integer>}}
                    if let (Some(t_val), Some(i_val)) = (ts.get("t"), ts.get("i")) {
                        if let (Some(t), Some(i)) = (t_val.as_u64(), i_val.as_u64()) {
                            // Convert timestamp to DateTime using seconds since epoch
                            // The 't' field represents seconds since Unix epoch
                            // The 'i' field is an increment and doesn't affect the datetime conversion
                            if let Some(datetime) = chrono::DateTime::from_timestamp(t as i64, 0) {
                                return Ok(BindableValue::DateTime(datetime));
                            } else {
                                tracing::warn!(
                                    "Failed to convert MongoDB timestamp to datetime: t={}, i={}",
                                    t,
                                    i
                                );
                                return Err(anyhow::anyhow!(
                                    "Failed to convert MongoDB timestamp to datetime"
                                ));
                            }
                        }
                    }
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Binary
            if let Some(binary_value) = obj.get("$binary") {
                if let Some(binary_obj) = binary_value.as_object() {
                    // Canonical format: {"$binary": {"base64": "...", "subType": "..."}}
                    if let Some(base64_str) = binary_obj.get("base64").and_then(|v| v.as_str()) {
                        match base64::engine::general_purpose::STANDARD.decode(base64_str) {
                            Ok(bytes) => return Ok(BindableValue::Bytes(bytes)),
                            Err(e) => {
                                tracing::warn!("Failed to decode base64 binary data: {}", e);
                                return Ok(BindableValue::String(base64_str.to_string()));
                            }
                        }
                    }
                } else {
                    return Err(anyhow::anyhow!("Invalid binary object format"));
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Double
            if let Some(number_double) = obj.get("$numberDouble").and_then(|v| v.as_str()) {
                if let Ok(num) = number_double.parse::<f64>() {
                    return Ok(BindableValue::Float(num));
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Int32
            if let Some(number_int) = obj.get("$numberInt").and_then(|v| v.as_str()) {
                if let Ok(num) = number_int.parse::<i64>() {
                    return Ok(BindableValue::Int(num));
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Int64
            if let Some(number_long) = obj.get("$numberLong").and_then(|v| v.as_str()) {
                if let Ok(num) = number_long.parse::<i64>() {
                    return Ok(BindableValue::Int(num));
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Decimal128
            if let Some(number_decimal) = obj.get("$numberDecimal").and_then(|v| v.as_str()) {
                match surrealdb::sql::Number::try_from(number_decimal) {
                    Ok(decimal_num) => return Ok(BindableValue::Decimal(decimal_num)),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to parse MongoDB Decimal128 '{}': {:?}",
                            number_decimal,
                            e
                        );
                        // We intentionally NOT fallback to string representation for Decimal128
                        // because it's not a valid SurrealDB number type nor its hard to tell
                        // it's correct or not.
                        return Err(anyhow::anyhow!("Failed to parse MongoDB Decimal128"));
                    }
                }
            }

            // https://docs.mongoing.com/mongo-introduction/bson-types/extended-json-v2#db-reference
            if let Some(ref_collection) = obj.get("$ref").and_then(|v| v.as_str()) {
                // "The $ref field holds the name of the collection where the referenced document resides.""
                // https://www.mongodb.com/docs/manual/reference/database-references/

                // "The $id field contains the value of the _id field in the referenced document."
                // https://www.mongodb.com/docs/manual/reference/database-references/
                if let Some(ref_id) = obj.get("$id") {
                    // MongoDB DBRef format: {"$ref": "collection_name", "$id": "document_id"}
                    // Convert to SurrealDB Thing: collection_name:document_id
                    let id_string = match ref_id {
                        // Could be string if it's as documented in https://docs.mongoing.com/mongo-introduction/bson-types/extended-json-v2#db-reference
                        serde_json::Value::String(s) => s.clone(),

                        // TODO Is this possible?
                        // serde_json::Value::Number(n) => n.to_string(),

                        // $id contains ObjectID according to https://www.mongodb.com/docs/manual/reference/database-references/,
                        serde_json::Value::Object(id_obj) => {
                            // Handle ObjectId: {"$oid": "..."}
                            if let Some(oid) = id_obj.get("$oid").and_then(|v| v.as_str()) {
                                oid.to_string()
                            } else {
                                // For other extended JSON types, use the object as string
                                ref_id.to_string()
                            }
                        }
                        _ => ref_id.to_string(),
                    };

                    let thing =
                        surrealdb::sql::Thing::from((ref_collection.to_string(), id_string));
                    return Ok(BindableValue::Thing(thing));
                }
            }

            // TODO How's the `JavaScript` type in MongoDB is expressed in MongoDB Extended JSON?
            // https://www.mongodb.com/docs/manual/reference/bson-types/

            // MongoDB Document as SurrealDB Object
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Document
            //
            // This would also cover the following types:
            // - $regularExpression: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Timestamp
            // - $maxKey: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-MaxKey
            // - $minKey: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-MinKey
            let mut bindables = HashMap::new();
            for (key, val) in obj {
                let bindable_val = convert_mongodb_types_to_bindable(val)?;
                bindables.insert(key, bindable_val);
            }
            Ok(BindableValue::Object(bindables))
        }
        // Canonical and Relaxed Array
        // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Array
        Value::Array(arr) => {
            // Recursively process array elements - return as JSON array for binding
            let mut bindables = Vec::new();
            for item in arr {
                let bindable_val = convert_mongodb_types_to_bindable(item)?;
                bindables.push(bindable_val);
            }
            Ok(BindableValue::Array(bindables))
        }
        // Convert JSON values to bindable types
        Value::Bool(b) => Ok(BindableValue::Bool(b)),
        Value::Number(n) => {
            // According to https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#type-representations,
            // Number values can be any of the following types:
            // - 32-bit integer
            // - 64-bit integer
            // - double-precision floating-point number
            if let Some(i) = n.as_i64() {
                Ok(BindableValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(BindableValue::Float(f))
            } else {
                Err(anyhow::anyhow!("Unsupported number type"))
            }
        }
        Value::String(s) => Ok(BindableValue::String(s)),
        Value::Null => Ok(BindableValue::Null),
    }
}
