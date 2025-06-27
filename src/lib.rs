//! SurrealSync Library
//!
//! A library for migrating data from Neo4j and MongoDB databases to SurrealDB.

use chrono::{DateTime, Utc};
use clap::Parser;
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use neo4rs::{ConfigBuilder, Graph, Query};
use serde_json::Value;
use std::{collections::HashMap, time::Duration};
use surrealdb::{engine::any::connect, Surreal};

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
                BindableValue::Array(bindables) => {
                    let mut arr = Vec::new();
                    for item in bindables {
                        let v = bindable_to_surrealdb_value(&item);
                        arr.push(v);
                    }
                    let surreal_arr = surrealdb::sql::Array::from(arr);
                    q.bind((field_name.clone(), surreal_arr))
                }
                BindableValue::Object(obj) => {
                    let mut map = std::collections::BTreeMap::new();
                    for (key, value) in obj {
                        let v = bindable_to_surrealdb_value(&value);
                        map.insert(key.clone(), v);
                    }
                    let surreal_obj = surrealdb::sql::Object::from(map);
                    q.bind((field_name.clone(), surreal_obj))
                }
                BindableValue::Duration(d) => q.bind((field_name.clone(), d.clone())),
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

fn bindable_to_surrealdb_value(bindable: &BindableValue) -> surrealdb::sql::Value {
    let sql_value = match bindable {
        BindableValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
        BindableValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
        BindableValue::Float(f) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f)),
        BindableValue::String(s) => {
            surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
        }
        BindableValue::DateTime(dt) => {
            surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
        }
        BindableValue::Array(bindables) => {
            let mut arr = Vec::new();
            for item in bindables {
                let v = bindable_to_surrealdb_value(&item);
                arr.push(v);
            }
            surrealdb::sql::Value::Array(surrealdb::sql::Array::from(arr))
        }
        BindableValue::Object(obj) => {
            let mut map = std::collections::BTreeMap::new();
            for (key, value) in obj {
                let v = bindable_to_surrealdb_value(&value);
                map.insert(key.clone(), v);
            }
            surrealdb::sql::Value::Object(surrealdb::sql::Object::from(map))
        }
        BindableValue::Duration(d) => {
            surrealdb::sql::Value::Duration(surrealdb::sql::Duration::from(*d))
        }
        BindableValue::Null => surrealdb::sql::Value::Null,
    };
    sql_value
}

pub async fn migrate_from_neo4j(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Neo4j migration");
    tracing::debug!(
        "migrate_from_neo4j function called with URI: {}",
        from_opts.source_uri
    );

    // Connect to Neo4j
    tracing::debug!("Connecting to Neo4j at: {}", from_opts.source_uri);
    let config = ConfigBuilder::default()
        .uri(&from_opts.source_uri)
        .user(
            from_opts
                .source_username
                .unwrap_or_else(|| "neo4j".to_string()),
        )
        .password(
            from_opts
                .source_password
                .unwrap_or_else(|| "password".to_string()),
        )
        .db(from_opts
            .source_database
            .unwrap_or_else(|| "neo4j".to_string()))
        .build()?;

    let graph = Graph::connect(config)?;
    tracing::debug!("Neo4j connection established");

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

    tracing::info!("Connected to both Neo4j and SurrealDB");

    let mut total_migrated = 0;

    // Migrate nodes first
    total_migrated += migrate_neo4j_nodes(&graph, &surreal, &to_opts).await?;

    // Then migrate relationships
    total_migrated += migrate_neo4j_relationships(&graph, &surreal, &to_opts).await?;

    tracing::info!(
        "Neo4j migration completed: {} total items migrated",
        total_migrated
    );
    Ok(())
}

/// Migrate all nodes from Neo4j to SurrealDB
async fn migrate_neo4j_nodes(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    to_opts: &SurrealOpts,
) -> anyhow::Result<usize> {
    tracing::info!("Starting Neo4j nodes migration");

    // Get all distinct node labels first
    let label_query = Query::new("MATCH (n) RETURN DISTINCT labels(n) as labels".to_string());
    let mut result = graph.execute(label_query).await?;

    let mut all_labels = std::collections::HashSet::new();
    while let Some(row) = result.next().await? {
        let labels: Vec<String> = row.get("labels")?;
        for label in labels {
            all_labels.insert(label);
        }
    }

    tracing::info!("Found {} distinct node labels", all_labels.len());
    tracing::debug!("Node labels: {:?}", all_labels);

    let mut total_migrated = 0;

    // Process each label separately to create proper SurrealDB tables
    for label in all_labels {
        tracing::info!("Migrating nodes with label: {}", label);

        let node_query = Query::new(
            "MATCH (n) WHERE $label IN labels(n) RETURN n, id(n) as node_id".to_string(),
        )
        .param("label", label.clone());
        let mut node_result = graph.execute(node_query).await?;

        let mut batch = Vec::new();
        let mut processed = 0;

        while let Some(row) = node_result.next().await? {
            let node: neo4rs::Node = row.get("n")?;
            let node_id: i64 = row.get("node_id")?;

            // Convert Neo4j node to SurrealDB format
            let surreal_record = convert_neo4j_node_to_bindable(node, node_id, &label)?;
            let record_id = format!("{}:{}", label.to_lowercase(), node_id);

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j node to SurrealDB record (id: {}): {:?}",
                    record_id,
                    surreal_record
                );
            }

            batch.push((record_id, surreal_record));

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for label: {}",
                    batch.len(),
                    label
                );
                if !to_opts.dry_run {
                    migrate_batch(surreal, &label.to_lowercase(), &batch).await?;
                } else {
                    tracing::debug!("Dry-run mode: skipping actual migration of batch");
                }
                processed += batch.len();
                total_migrated += batch.len();
                tracing::info!("Processed {} nodes with label '{}'", processed, label);
                batch.clear();
            }
        }

        // Process remaining nodes in the last batch
        if !batch.is_empty() {
            tracing::debug!(
                "Processing final batch of {} nodes for label: {}",
                batch.len(),
                label
            );
            if !to_opts.dry_run {
                migrate_batch(surreal, &label.to_lowercase(), &batch).await?;
            } else {
                tracing::debug!("Dry-run mode: skipping actual migration of final batch");
            }
            processed += batch.len();
            total_migrated += batch.len();
        }

        tracing::info!(
            "Completed migration of label '{}': {} nodes",
            label,
            processed
        );
    }

    tracing::info!(
        "Completed Neo4j nodes migration: {} total nodes",
        total_migrated
    );
    Ok(total_migrated)
}

/// Migrate all relationships from Neo4j to SurrealDB
async fn migrate_neo4j_relationships(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    to_opts: &SurrealOpts,
) -> anyhow::Result<usize> {
    tracing::info!("Starting Neo4j relationships migration");

    // Get all distinct relationship types first
    let type_query = Query::new("MATCH ()-[r]->() RETURN DISTINCT type(r) as rel_type".to_string());
    let mut result = graph.execute(type_query).await?;

    let mut all_types = std::collections::HashSet::new();
    while let Some(row) = result.next().await? {
        let rel_type: String = row.get("rel_type")?;
        all_types.insert(rel_type);
    }

    tracing::info!("Found {} distinct relationship types", all_types.len());
    tracing::debug!("Relationship types: {:?}", all_types);

    let mut total_migrated = 0;

    // Process each relationship type separately
    for rel_type in all_types {
        tracing::info!("Migrating relationships of type: {}", rel_type);

        let rel_query = Query::new(
            "MATCH (start_node)-[r]->(end_node) WHERE type(r) = $rel_type 
             RETURN r, id(r) as rel_id, id(start_node) as start_id, id(end_node) as end_id,
             labels(start_node) as start_labels, labels(end_node) as end_labels"
                .to_string(),
        )
        .param("rel_type", rel_type.clone());
        let mut rel_result = graph.execute(rel_query).await?;

        let mut batch = Vec::new();
        let mut processed = 0;

        while let Some(row) = rel_result.next().await? {
            let relationship: neo4rs::Relation = row.get("r")?;
            let rel_id: i64 = row.get("rel_id")?;
            let start_id: i64 = row.get("start_id")?;
            let end_id: i64 = row.get("end_id")?;
            let start_labels: Vec<String> = row.get("start_labels")?;
            let end_labels: Vec<String> = row.get("end_labels")?;

            // Convert Neo4j relationship to SurrealDB format
            let surreal_record = convert_neo4j_relationship_to_bindable(
                relationship,
                rel_id,
                start_id,
                end_id,
                &start_labels,
                &end_labels,
                &rel_type,
            )?;
            let record_id = format!("{}:{}", rel_type.to_lowercase(), rel_id);

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j relationship to SurrealDB record (id: {}): {:?}",
                    record_id,
                    surreal_record
                );
            }

            batch.push((record_id, surreal_record));

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for type: {}",
                    batch.len(),
                    rel_type
                );
                if !to_opts.dry_run {
                    migrate_batch(surreal, &rel_type.to_lowercase(), &batch).await?;
                } else {
                    tracing::debug!("Dry-run mode: skipping actual migration of batch");
                }
                processed += batch.len();
                total_migrated += batch.len();
                tracing::info!(
                    "Processed {} relationships of type '{}'",
                    processed,
                    rel_type
                );
                batch.clear();
            }
        }

        // Process remaining relationships in the last batch
        if !batch.is_empty() {
            tracing::debug!(
                "Processing final batch of {} relationships for type: {}",
                batch.len(),
                rel_type
            );
            if !to_opts.dry_run {
                migrate_batch(surreal, &rel_type.to_lowercase(), &batch).await?;
            } else {
                tracing::debug!("Dry-run mode: skipping actual migration of final batch");
            }
            processed += batch.len();
            total_migrated += batch.len();
        }

        tracing::info!(
            "Completed migration of relationship type '{}': {} relationships",
            rel_type,
            processed
        );
    }

    tracing::info!(
        "Completed Neo4j relationships migration: {} total relationships",
        total_migrated
    );
    Ok(total_migrated)
}

/// Convert Neo4j node to bindable HashMap with BindableValue
fn convert_neo4j_node_to_bindable(
    node: neo4rs::Node,
    node_id: i64,
    _label: &str,
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    let mut bindable_obj = std::collections::HashMap::new();

    // Add neo4j_id as a field (preserve original Neo4j ID)
    bindable_obj.insert("neo4j_id".to_string(), BindableValue::Int(node_id));

    // Add labels as an array
    let labels: Vec<String> = node.labels().into_iter().map(|s| s.to_string()).collect();
    let labels_bindables: Vec<BindableValue> = labels
        .into_iter()
        .map(|s| BindableValue::String(s))
        .collect();
    bindable_obj.insert("labels".to_string(), BindableValue::Array(labels_bindables));

    // Convert all properties
    for key in node.keys() {
        let value = node.get::<neo4rs::BoltType>(key)?;
        let bindable_value = convert_neo4j_type_to_bindable(value)?;
        bindable_obj.insert(key.to_string(), bindable_value);
    }

    Ok(bindable_obj)
}

/// Convert Neo4j relationship to bindable HashMap with BindableValue
fn convert_neo4j_relationship_to_bindable(
    relationship: neo4rs::Relation,
    rel_id: i64,
    start_id: i64,
    end_id: i64,
    start_labels: &[String],
    end_labels: &[String],
    rel_type: &str,
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    let mut bindable_obj = std::collections::HashMap::new();

    // Add neo4j_id as a field (preserve original Neo4j ID)
    bindable_obj.insert("neo4j_id".to_string(), BindableValue::Int(rel_id));

    // Add relationship type
    bindable_obj.insert(
        "relationship_type".to_string(),
        BindableValue::String(rel_type.to_string()),
    );

    // Add references to start and end nodes with SurrealDB Thing format
    let start_table = start_labels
        .first()
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "node".to_string());
    let end_table = end_labels
        .first()
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "node".to_string());

    bindable_obj.insert(
        "from_node".to_string(),
        BindableValue::String(format!("{}:{}", start_table, start_id)),
    );
    bindable_obj.insert(
        "to_node".to_string(),
        BindableValue::String(format!("{}:{}", end_table, end_id)),
    );

    // Convert all properties
    for key in relationship.keys() {
        let value = relationship.get::<neo4rs::BoltType>(key)?;
        let bindable_value = convert_neo4j_type_to_bindable(value)?;
        bindable_obj.insert(key.to_string(), bindable_value);
    }

    Ok(bindable_obj)
}

/// Convert Neo4j BoltType to BindableValue
fn convert_neo4j_type_to_bindable(value: neo4rs::BoltType) -> anyhow::Result<BindableValue> {
    match value {
        neo4rs::BoltType::String(s) => Ok(BindableValue::String(s.value)),
        neo4rs::BoltType::Boolean(b) => Ok(BindableValue::Bool(b.value)),
        neo4rs::BoltType::Map(map) => {
            let mut bindables = HashMap::new();
            for (key, val) in map.value {
                let bindable_val = convert_neo4j_type_to_bindable(val)?;
                bindables.insert(key.to_string(), bindable_val);
            }
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Null(_) => Ok(BindableValue::Null),
        neo4rs::BoltType::Integer(i) => Ok(BindableValue::Int(i.value)),
        neo4rs::BoltType::Float(f) => Ok(BindableValue::Float(f.value)),
        neo4rs::BoltType::List(list) => {
            let mut bindables = Vec::new();
            for item in list.value {
                let bindable_val = convert_neo4j_type_to_bindable(item)?;
                bindables.push(bindable_val);
            }
            Ok(BindableValue::Array(bindables))
        }
        neo4rs::BoltType::Node(node) => {
            // TODO: Add proper support for this by respecting id, labels, and properties in the node object
            Ok(BindableValue::String(format!("{:?}", node)))
        }
        neo4rs::BoltType::Relation(relation) => {
            // TODO: Add proper support for this by respecting id, start_node_id, end_node_id, typ, and properties in the relation object
            Ok(BindableValue::String(format!("{:?}", relation)))
        }
        neo4rs::BoltType::UnboundedRelation(unbounded_relation) => {
            // TODO: Add proper support for this by rsepecting id, typ, and properties in the relation object
            Ok(BindableValue::String(format!("{:?}", unbounded_relation)))
        }
        neo4rs::BoltType::Point2D(point) => {
            // TODO: We need to turn into into https://geojson.org/
            // sr_id is the srid, which is not directly representable in SurrealDB
            let mut bindables = HashMap::new();
            bindables.insert(
                "type".to_string(),
                BindableValue::String("Point2D".to_string()),
            );
            bindables.insert("srid".to_string(), BindableValue::Int(point.sr_id.value));
            bindables.insert("x".to_string(), BindableValue::Float(point.x.value));
            bindables.insert("y".to_string(), BindableValue::Float(point.y.value));
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Point3D(point) => {
            // TODO: We need to turn into into https://geojson.org/
            // sr_id is the srid, which is not directly representable in SurrealDB
            let mut bindables = HashMap::new();
            bindables.insert(
                "type".to_string(),
                BindableValue::String("Point3D".to_string()),
            );
            bindables.insert("srid".to_string(), BindableValue::Int(point.sr_id.value));
            bindables.insert("x".to_string(), BindableValue::Float(point.x.value));
            bindables.insert("y".to_string(), BindableValue::Float(point.y.value));
            bindables.insert("z".to_string(), BindableValue::Float(point.z.value));
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Bytes(bytes) => {
            // TODO: Add proper support for this by using SurrealDB bytes type
            Ok(BindableValue::String(format!("{:?}", bytes)))
        }
        neo4rs::BoltType::Path(path) => {
            // TODO: Add proper support for this that respects nodes, rels, and indices lists in the path object
            Ok(BindableValue::String(format!("{:?}", path)))
        }
        neo4rs::BoltType::Date(date) => {
            let naive_d: chrono::NaiveDate = date.try_into()?;
            // Make this configurable?
            let now = chrono::Local::now();
            let tz = now.offset();
            // Assumes the naivedate is in the specified tz, and produce a UTC datetime
            let naive_dt = chrono::NaiveDateTime::new(
                naive_d,
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            );
            let dt_with_tz: DateTime<chrono::FixedOffset> =
                chrono::DateTime::from_naive_utc_and_offset(naive_dt, tz.to_owned());
            let utc_dt = dt_with_tz.into();
            Ok(BindableValue::DateTime(utc_dt))
        }
        neo4rs::BoltType::Time(time) => {
            let t: (chrono::NaiveTime, chrono::FixedOffset) = time.into();
            let mut obj = HashMap::new();
            // Maybe we should have options to make it internally tagged (as its externally tagged for now by a parallel `type` field)
            // and make the type field value configurable for parsing convenience?
            obj.insert(
                "type".to_string(),
                BindableValue::String("$Neo4jTime".to_string()),
            );
            obj.insert(
                "offset_seconds".to_string(),
                BindableValue::Int(t.1.local_minus_utc() as i64),
            );
            use chrono::Timelike;
            obj.insert("hour".to_string(), BindableValue::Int(t.0.hour() as i64));
            obj.insert(
                "minute".to_string(),
                BindableValue::Int(t.0.minute() as i64),
            );
            obj.insert(
                "second".to_string(),
                BindableValue::Int(t.0.second() as i64),
            );
            obj.insert(
                "nanosecond".to_string(),
                BindableValue::Int(t.0.nanosecond() as i64),
            );
            Ok(BindableValue::Object(obj))
        }
        neo4rs::BoltType::LocalTime(local_time) => {
            let cnt: chrono::NaiveTime = local_time.into();
            let mut obj = HashMap::new();
            // Maybe we should have options to make it internally tagged (as its externally tagged for now by a parallel `type` field)
            // and make the type field value configurable for parsing convenience?
            obj.insert(
                "type".to_string(),
                BindableValue::String("$Neo4jLocalTime".to_string()),
            );
            use chrono::Timelike;
            obj.insert("hour".to_string(), BindableValue::Int(cnt.hour() as i64));
            obj.insert(
                "minute".to_string(),
                BindableValue::Int(cnt.minute() as i64),
            );
            obj.insert(
                "second".to_string(),
                BindableValue::Int(cnt.second() as i64),
            );
            obj.insert(
                "nanosecond".to_string(),
                BindableValue::Int(cnt.nanosecond() as i64),
            );
            Ok(BindableValue::Object(obj))
        }
        neo4rs::BoltType::DateTime(datetime) => {
            let dt: chrono::DateTime<chrono::FixedOffset> = datetime.try_into()?;
            let utc_dt = dt.into();
            Ok(BindableValue::DateTime(utc_dt))
        }
        neo4rs::BoltType::LocalDateTime(local_datetime) => {
            let dt: chrono::NaiveDateTime = local_datetime.try_into()?;
            let utc_dt = dt.and_utc();
            Ok(BindableValue::DateTime(utc_dt))
        }
        neo4rs::BoltType::DateTimeZoneId(datetime_zone_id) => {
            // Convert Neo4j DateTimeZoneId to string representation
            Ok(BindableValue::String(format!("{:?}", datetime_zone_id)))
        }
        neo4rs::BoltType::Duration(duration) => {
            let std_duration: std::time::Duration = duration.into();
            Ok(BindableValue::Duration(std_duration))
        }
    }
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
    Array(Vec<BindableValue>), // For arrays, we'll use JSON since SurrealDB accepts Vec<T> where T: Into<Value>
    Object(HashMap<String, BindableValue>), // For objects, use JSON
    Duration(std::time::Duration),
    Null,
}

/// Convert MongoDB Extended JSON (v2) values to our own bindables
fn convert_mongodb_types_to_bindable(value: Value) -> anyhow::Result<BindableValue> {
    match value {
        Value::Object(obj) => {
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Date
            if let Some(date_str) = obj.get("$date").and_then(|v| v.as_str()) {
                // Return actual DateTime<Utc> for binding
                let utc_datetime = chrono::DateTime::parse_from_rfc3339(date_str)
                    .map_err(|e| anyhow::anyhow!("Failed to parse MongoDB datetime: {}", e))?
                    .to_utc();
                return Ok(BindableValue::DateTime(utc_datetime));
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-ObjectId
            if let Some(oid_str) = obj.get("$oid").and_then(|v| v.as_str()) {
                // Return string for binding
                return Ok(BindableValue::String(oid_str.to_string()));
            }

            // TODO $timestamp as SurrealDB datetime
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Timestamp

            // TODO array
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Array

            // TODO $binary as SurrealDB bytes
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Binary

            // TODO $numberDouble as SurrealDB float (64-bit)
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Double

            // TODO $numberInt as SurrealDB int (64-bit)
            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Int32

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Int64
            if let Some(number_long) = obj.get("$numberLong").and_then(|v| v.as_str()) {
                if let Ok(num) = number_long.parse::<i64>() {
                    return Ok(BindableValue::Int(num));
                }
            }

            // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/#mongodb-bsontype-Decimal128
            if let Some(number_decimal) = obj.get("$numberDecimal").and_then(|v| v.as_str()) {
                // TODO Should we use SurrealDB `decimal` type?
                if let Ok(num) = number_decimal.parse::<f64>() {
                    return Ok(BindableValue::Float(num));
                }
            }

            // TODO Data Reference ($ref) should be converted to SurrealDB Thing
            // https://docs.mongoing.com/mongo-introduction/bson-types/extended-json-v2#db-reference

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
            // TODO Can we just use SurrealDB `decimal` type?
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
