//! MongoDB full sync implementation
//!
//! This module provides full synchronization from MongoDB to SurrealDB.

use checkpoint::{Checkpoint, SyncConfig, SyncManager, SyncPhase};
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use std::collections::HashMap;
use std::time::Duration;
use surreal_sink::SurrealSink;
use sync_core::{UniversalRow, UniversalType, UniversalValue};

/// Source database connection options (MongoDB-specific, library type without clap)
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub source_uri: String,
    pub source_database: Option<String>,
}

/// Sync options (non-connection related)
#[derive(Clone, Debug)]
pub struct SyncOpts {
    pub batch_size: usize,
    pub dry_run: bool,
}

/// Parse an ISO 8601 duration string (PTxS or PTx.xxxxxxxxxS format).
fn try_parse_iso8601_duration(s: &str) -> Option<std::time::Duration> {
    let trimmed = s.trim();
    if let Some(secs_str) = trimmed.strip_prefix("PT").and_then(|s| s.strip_suffix('S')) {
        if let Some(dot_pos) = secs_str.find('.') {
            let secs: u64 = secs_str[..dot_pos].parse().ok()?;
            let nanos_str = &secs_str[dot_pos + 1..];
            let nanos: u32 = nanos_str.parse().ok()?;
            Some(std::time::Duration::new(secs, nanos))
        } else {
            let secs: u64 = secs_str.parse().ok()?;
            Some(std::time::Duration::from_secs(secs))
        }
    } else {
        None
    }
}

/// Simple migration entry point (no checkpoint support)
pub async fn migrate_from_mongodb<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
) -> anyhow::Result<()> {
    run_full_sync(surreal, from_opts, sync_opts, None).await
}

/// Enhanced version that supports checkpoint emission for incremental sync coordination
pub async fn run_full_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_config: Option<SyncConfig>,
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
        .clone()
        .ok_or_else(|| anyhow::anyhow!("MongoDB source database name is required"))?;
    tracing::debug!("Using MongoDB database: {}", source_db_name);
    let mongo_db = mongo_client.database(&source_db_name);

    // Emit checkpoint t1 (before full sync starts) if configured
    let _checkpoint_t1 = if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Get current resume token from MongoDB before creating source
        let initial_resume_token =
            crate::checkpoint::get_resume_token(&mongo_client, &source_db_name).await?;

        // Create source with initial resume token
        let source = crate::incremental_sync::MongodbIncrementalSource::new(
            &from_opts.source_uri,
            &source_db_name,
            initial_resume_token,
        )
        .await?;
        let checkpoint = source.get_checkpoint().await?;

        // Emit the checkpoint
        sync_manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;

        tracing::info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
        Some(checkpoint)
    } else {
        None
    };

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
        let mut batch: Vec<UniversalRow> = Vec::new();
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

            // Convert BSON document to UniversalRow
            let surreal_record =
                convert_bson_document_to_record(doc_owned, &collection_name, processed as u64)?;

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("Final document for SurrealDB: {surreal_record:?}",);
            }

            batch.push(surreal_record);

            if batch.len() >= sync_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for collection: {}",
                    batch.len(),
                    collection_name
                );
                if !sync_opts.dry_run {
                    tracing::debug!("Migrating batch of {} documents to SurrealDB", batch.len());
                    surreal.write_universal_rows(&batch).await?;
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
            if !sync_opts.dry_run {
                tracing::debug!(
                    "Migrating final batch of {} documents to SurrealDB",
                    batch.len()
                );
                surreal.write_universal_rows(&batch).await?;
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

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Get current checkpoint after migration
        let database_name = from_opts
            .source_database
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MongoDB database name is required"))?;
        // Get current resume token for end checkpoint
        let end_resume_token =
            crate::checkpoint::get_resume_token(&mongo_client, database_name).await?;
        let source = crate::incremental_sync::MongodbIncrementalSource::new(
            &from_opts.source_uri,
            database_name,
            end_resume_token,
        )
        .await?;
        let checkpoint = source.get_checkpoint().await?;

        // Emit the checkpoint
        sync_manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;

        tracing::info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_cli_string()
        );
    }

    tracing::info!(
        "MongoDB migration completed: {} total documents migrated",
        total_migrated
    );
    Ok(())
}

/// Convert BSON values directly to UniversalValue
pub fn convert_bson_to_universal_value(
    bson_value: mongodb::bson::Bson,
) -> anyhow::Result<UniversalValue> {
    use mongodb::bson::Bson;

    match bson_value {
        Bson::Double(f) => Ok(UniversalValue::Float64(f)),
        Bson::String(s) => {
            // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
            if let Some(duration) = try_parse_iso8601_duration(&s) {
                Ok(UniversalValue::Duration(duration))
            } else {
                Ok(UniversalValue::Text(s))
            }
        }
        Bson::Array(arr) => {
            let mut vs = Vec::new();
            for item in arr {
                let v = convert_bson_to_universal_value(item)?;
                vs.push(v);
            }
            // Use Json as element type since BSON arrays can contain mixed types
            Ok(UniversalValue::Array {
                elements: vs,
                element_type: Box::new(UniversalType::Json),
            })
        }
        Bson::Document(doc) => {
            // Check if this document is a DBRef
            if let (Some(Bson::String(ref_collection)), Some(ref_id)) =
                (doc.get("$ref"), doc.get("$id"))
            {
                // This is a DBRef - convert to Thing reference
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
                Ok(UniversalValue::Thing {
                    table: ref_collection.clone(),
                    id: Box::new(UniversalValue::Text(id_string)),
                })
            } else {
                // Regular document - convert recursively
                let mut obj = HashMap::new();
                for (key, val) in doc {
                    let v = convert_bson_to_universal_value(val)?;
                    obj.insert(key, v);
                }
                Ok(UniversalValue::Object(obj))
            }
        }
        Bson::Boolean(b) => Ok(UniversalValue::Bool(b)),
        Bson::Null => Ok(UniversalValue::Null),
        Bson::RegularExpression(regex) => {
            // We assume SurrealDB's regex always use Rust's regex crate under the hood,
            // so we can say (?OPTIONS)PATTERN in SurrealDB whereas it is /PATTERN/OPTIONS in MongoDB.
            // Note that the regex crate does not support the /PATTERN/OPTIONS style.
            // See https://docs.rs/regex/latest/regex/#grouping-and-flags
            Ok(UniversalValue::Text(format!(
                "(?{}){}",
                regex.options, regex.pattern
            )))
        }
        Bson::JavaScriptCode(code) => Ok(UniversalValue::Text(code)),
        Bson::JavaScriptCodeWithScope(code_with_scope) => {
            let mut scope_obj = HashMap::new();
            for (key, val) in code_with_scope.scope {
                let v = convert_bson_to_universal_value(val)?;
                scope_obj.insert(key, v);
            }
            let scope = UniversalValue::Object(scope_obj);
            let code = UniversalValue::Text(code_with_scope.code);
            let mut result_obj = HashMap::new();
            result_obj.insert("$code".to_string(), code);
            result_obj.insert("$scope".to_string(), scope);
            Ok(UniversalValue::Object(result_obj))
        }
        Bson::Int32(i) => Ok(UniversalValue::Int64(i as i64)),
        Bson::Int64(i) => Ok(UniversalValue::Int64(i)),
        Bson::Timestamp(ts) => {
            // MongoDB Timestamp.time is seconds since Unix epoch
            let seconds = ts.time as i64;
            // To keep the ordering across timestamps, we exploit the increment component as the nanoseconds.
            let assumed_ns = ts.increment;
            if let Some(datetime) = chrono::DateTime::from_timestamp(seconds, assumed_ns) {
                // MongoDB timestamps are in UTC, so use ZonedDateTime
                Ok(UniversalValue::ZonedDateTime(datetime))
            } else {
                Err(anyhow::anyhow!(
                    "Failed to convert MongoDB timestamp to datetime"
                ))
            }
        }
        Bson::Binary(binary) => Ok(UniversalValue::Bytes(binary.bytes)),
        Bson::ObjectId(oid) => Ok(UniversalValue::Text(oid.to_string())),
        Bson::DateTime(dt) => Ok(UniversalValue::ZonedDateTime(dt.to_chrono())),
        Bson::Symbol(s) => Ok(UniversalValue::Text(s)),
        Bson::Decimal128(d) => {
            let decimal_str = d.to_string();
            // Try to parse as float64
            match decimal_str.parse::<f64>() {
                Ok(f) => Ok(UniversalValue::Float64(f)),
                Err(e) => {
                    tracing::warn!("Failed to parse BSON Decimal128 '{}': {:?}", decimal_str, e);
                    Err(anyhow::anyhow!("Failed to parse BSON Decimal128"))
                }
            }
        }
        Bson::Undefined => Ok(UniversalValue::Null), // Map undefined to null
        Bson::MaxKey => {
            let mut mk = HashMap::new();
            mk.insert("$maxKey".to_string(), UniversalValue::Int64(1));
            Ok(UniversalValue::Object(mk))
        }
        Bson::MinKey => {
            let mut mk = HashMap::new();
            mk.insert("$minKey".to_string(), UniversalValue::Int64(1));
            Ok(UniversalValue::Object(mk))
        }
        Bson::DbPointer(_db_pointer) => {
            // DBPointer is deprecated and fields are private
            // Store as a special string to preserve the information
            Ok(UniversalValue::Text("$dbPointer".to_string()))
        }
    }
}

/// Converts a BSON document containing _id to a UniversalRow
fn convert_bson_document_to_record(
    doc: mongodb::bson::Document,
    collection_name: &str,
    row_index: u64,
) -> anyhow::Result<UniversalRow> {
    // Extract MongoDB _id and convert to UniversalValue
    let id_value = if let Some(id_bson) = doc.get("_id") {
        match id_bson {
            mongodb::bson::Bson::ObjectId(oid) => UniversalValue::Text(oid.to_string()),
            mongodb::bson::Bson::String(s) => UniversalValue::Text(s.clone()),
            mongodb::bson::Bson::Int32(i) => UniversalValue::Int64(*i as i64),
            mongodb::bson::Bson::Int64(i) => UniversalValue::Int64(*i),
            _ => anyhow::bail!("Unsupported _id type in MongoDB document: {id_bson:?}"),
        }
    } else {
        anyhow::bail!("Document is missing _id field");
    };

    // Convert remaining fields (excluding _id)
    let mut fields = HashMap::new();
    for (key, value) in doc {
        if key != "_id" {
            let v = convert_bson_to_universal_value(value)?;
            fields.insert(key, v);
        }
    }

    Ok(UniversalRow::new(
        collection_name.to_string(),
        row_index,
        id_value,
        fields,
    ))
}
