use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use std::{collections::HashMap, hash::Hash, time::Duration};
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

            // Convert MongoDB BSON document
            tracing::trace!("Converting RawDocument to owned Document");
            let doc_owned: mongodb::bson::Document = doc.try_into()?;

            // Convert using BSON mode
            tracing::trace!("Using BSON mode for document conversion");

            // Add debug logging to see the BSON document
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("BSON document: {:?}", doc_owned);
            }

            // Extract MongoDB ObjectId for record ID
            let record_id = if let Some(id_value) = doc_owned.get("_id") {
                match id_value {
                    mongodb::bson::Bson::ObjectId(oid) => {
                        format!("{}:{}", collection_name, oid.to_string())
                    }
                    mongodb::bson::Bson::String(s) => {
                        format!("{}:{}", collection_name, s)
                    }
                    mongodb::bson::Bson::Int32(i) => {
                        format!("{}:{}", collection_name, i)
                    }
                    mongodb::bson::Bson::Int64(i) => {
                        format!("{}:{}", collection_name, i)
                    }
                    _ => {
                        format!("{}:{}", collection_name, id_value.to_string())
                    }
                }
            } else {
                collection_name.to_string()
            };

            // Remove _id field before conversion
            let mut doc_without_id = doc_owned.clone();
            doc_without_id.remove("_id");

            // Convert BSON document to bindable
            let bindable_object = convert_bson_document_to_bindable(doc_without_id)?;

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

/// Convert BSON values directly to bindable values
fn convert_bson_to_bindable(bson_value: mongodb::bson::Bson) -> anyhow::Result<BindableValue> {
    use mongodb::bson::Bson;

    match bson_value {
        Bson::Double(f) => Ok(BindableValue::Float(f)),
        Bson::String(s) => Ok(BindableValue::String(s)),
        Bson::Array(arr) => {
            let mut bindables = Vec::new();
            for item in arr {
                let bindable_val = convert_bson_to_bindable(item)?;
                bindables.push(bindable_val);
            }
            Ok(BindableValue::Array(bindables))
        }
        Bson::Document(doc) => {
            // Check if this document is a DBRef
            if let (Some(Bson::String(ref_collection)), Some(ref_id)) =
                (doc.get("$ref"), doc.get("$id"))
            {
                // This is a DBRef - convert to SurrealDB Thing
                let id_string = match ref_id {
                    Bson::String(s) => s.clone(),
                    Bson::ObjectId(oid) => oid.to_string(),
                    Bson::Document(id_doc) => {
                        // Handle nested ObjectId: {"$oid": "..."}
                        if let Some(Bson::String(oid)) = id_doc.get("$oid") {
                            oid.clone()
                        } else {
                            ref_id.to_string()
                        }
                    }
                    _ => ref_id.to_string(),
                };
                let thing = surrealdb::sql::Thing::from((ref_collection.clone(), id_string));
                Ok(BindableValue::Thing(thing))
            } else {
                // Regular document - convert recursively
                let mut bindables = HashMap::new();
                for (key, val) in doc {
                    let bindable_val = convert_bson_to_bindable(val)?;
                    bindables.insert(key, bindable_val);
                }
                Ok(BindableValue::Object(bindables))
            }
        }
        Bson::Boolean(b) => Ok(BindableValue::Bool(b)),
        Bson::Null => Ok(BindableValue::Null),
        Bson::RegularExpression(regex) => {
            // We assume SurrealDB's regex always use Rust's regex crate under the hood,
            // so we can say (?OPTIONS)PATTERN in SurrealDB whereas it is /PATTERN/OPTIONS in MongoDB.
            // Note that teh regex crate does not support the /PATTERN/OPTIONS style.
            // See https://docs.rs/regex/latest/regex/#grouping-and-flags
            Ok(BindableValue::String(format!(
                "(?{}){}",
                regex.options, regex.pattern
            )))
        }
        Bson::JavaScriptCode(code) => Ok(BindableValue::String(code)),
        Bson::JavaScriptCodeWithScope(code_with_scope) => {
            let mut scope = HashMap::new();
            for (key, val) in code_with_scope.scope {
                let bindable_val = convert_bson_to_bindable(val)?;
                scope.insert(key, bindable_val);
            }
            let scope = BindableValue::Object(scope);
            let code = BindableValue::String(code_with_scope.code);
            let mut code_with_scope = HashMap::new();
            code_with_scope.insert("$code".to_string(), code);
            code_with_scope.insert("$scope".to_string(), scope);
            Ok(BindableValue::Object(code_with_scope))
        }
        Bson::Int32(i) => Ok(BindableValue::Int(i as i64)),
        Bson::Int64(i) => Ok(BindableValue::Int(i)),
        Bson::Timestamp(ts) => {
            // MongoDB Timestamp.time is seconds since Unix epoch
            let seconds = ts.time as i64;
            // To keep the ordering across timestamps, we exploit the increment component as the nanoseconds.
            let assumed_ns = ts.increment;
            if let Some(datetime) = chrono::DateTime::from_timestamp(seconds, assumed_ns) {
                Ok(BindableValue::DateTime(datetime))
            } else {
                Err(anyhow::anyhow!(
                    "Failed to convert MongoDB timestamp to datetime"
                ))
            }
        }
        Bson::Binary(binary) => Ok(BindableValue::Bytes(binary.bytes)),
        Bson::ObjectId(oid) => Ok(BindableValue::String(oid.to_string())),
        Bson::DateTime(dt) => Ok(BindableValue::DateTime(dt.to_chrono())),
        Bson::Symbol(s) => Ok(BindableValue::String(s)),
        Bson::Decimal128(d) => {
            let decimal_str = d.to_string();
            match surrealdb::sql::Number::try_from(decimal_str.as_str()) {
                Ok(decimal_num) => Ok(BindableValue::Decimal(decimal_num)),
                Err(e) => {
                    tracing::warn!("Failed to parse BSON Decimal128 '{}': {:?}", decimal_str, e);
                    Err(anyhow::anyhow!("Failed to parse BSON Decimal128"))
                }
            }
        }
        Bson::Undefined => Ok(BindableValue::None), // Map undefined to null
        Bson::MaxKey => {
            let mut mk = HashMap::new();
            mk.insert("$maxKey".to_string(), BindableValue::Int(1));
            Ok(BindableValue::Object(mk))
        }
        Bson::MinKey => {
            let mut mk = HashMap::new();
            mk.insert("$minKey".to_string(), BindableValue::Int(1));
            Ok(BindableValue::Object(mk))
        }
        Bson::DbPointer(_db_pointer) => {
            // DBPointer is deprecated and fields are private
            // Store as a special string to preserve the information
            Ok(BindableValue::String("$dbPointer".to_string()))
        }
    }
}

/// Convert BSON document to bindable HashMap
fn convert_bson_document_to_bindable(
    doc: mongodb::bson::Document,
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    let mut bindable_obj = std::collections::HashMap::new();
    for (key, val) in doc {
        let bindable_val = convert_bson_to_bindable(val)?;
        bindable_obj.insert(key, bindable_val);
    }
    Ok(bindable_obj)
}
