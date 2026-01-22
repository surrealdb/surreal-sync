//! Neo4j full sync implementation
//!
//! This module provides full synchronization from Neo4j to SurrealDB.

use neo4rs::{ConfigBuilder, Graph, Query};
use std::collections::HashSet;
use surrealdb::sql::{Array, Datetime, Number, Object, Strand, Value};
use surrealdb::{engine::any::connect, Surreal};
use surrealdb_types::{RecordWithSurrealValues as Record, Relation as SurrealRelation};

use checkpoint::{Checkpoint, SyncConfig, SyncManager, SyncPhase};

/// Source database connection options (Neo4j-specific, library type without clap)
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub source_uri: String,
    pub source_database: Option<String>,
    pub source_username: Option<String>,
    pub source_password: Option<String>,
    pub neo4j_timezone: String,
    pub neo4j_json_properties: Option<Vec<String>>,
}

/// SurrealDB connection options (library type without clap)
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    pub surreal_endpoint: String,
    pub surreal_username: String,
    pub surreal_password: String,
    pub batch_size: usize,
    pub dry_run: bool,
}

/// Parsed configuration for Neo4j JSON-to-object conversion
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Neo4jJsonProperty {
    pub label: String,
    pub property: String,
}

impl Neo4jJsonProperty {
    /// Parse "Label.property" format into Neo4jJsonProperty
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid Neo4j JSON property format: '{s}'. Expected format: 'NodeLabel.propertyName'",
            );
        }
        Ok(Neo4jJsonProperty {
            label: parts[0].to_string(),
            property: parts[1].to_string(),
        })
    }

    /// Parse multiple entries from CLI argument
    pub fn parse_vec(entries: &[String]) -> anyhow::Result<Vec<Self>> {
        entries.iter().map(|s| Self::parse(s)).collect()
    }
}

/// Context for Neo4j type conversion with JSON-to-object configuration
#[derive(Clone)]
pub struct Neo4jConversionContext {
    pub timezone: String,
    /// Set of (label, property) pairs that should be converted from JSON strings to objects
    pub json_properties: HashSet<(String, String)>,
}

impl Neo4jConversionContext {
    pub fn new(timezone: String, json_properties: Option<Vec<String>>) -> anyhow::Result<Self> {
        let json_properties = if let Some(entries) = json_properties {
            Neo4jJsonProperty::parse_vec(&entries)?
                .into_iter()
                .map(|p| (p.label, p.property))
                .collect()
        } else {
            HashSet::new()
        };

        Ok(Self {
            timezone,
            json_properties,
        })
    }

    /// Check if a property should be converted from JSON string to object
    pub fn should_parse_json(&self, label: &str, property: &str) -> bool {
        self.json_properties
            .contains(&(label.to_string(), property.to_string()))
    }
}

/// Enhanced version that supports checkpoint emission for incremental sync coordination
pub async fn run_full_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    sync_config: Option<SyncConfig>,
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
                .clone()
                .unwrap_or_else(|| "neo4j".to_string()),
        )
        .password(
            from_opts
                .source_password
                .clone()
                .unwrap_or_else(|| "password".to_string()),
        )
        .db(from_opts
            .source_database
            .clone()
            .unwrap_or_else(|| "neo4j".to_string()))
        .build()?;

    let graph = Graph::connect(config)?;
    tracing::debug!("Neo4j connection established");

    // Emit checkpoint t1 (before full sync starts) if configured
    let _checkpoint_t1 = if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Use timestamp-based checkpoint tracking
        tracing::info!("Using timestamp-based checkpoint tracking");
        let checkpoint = crate::neo4j_checkpoint::get_current_checkpoint();

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

    // Create conversion context with JSON-to-object configuration
    let ctx = Neo4jConversionContext::new(
        from_opts.neo4j_timezone.clone(),
        from_opts.neo4j_json_properties.clone(),
    )?;

    let mut total_migrated = 0;

    // Migrate nodes first
    total_migrated += migrate_neo4j_nodes(&graph, &surreal, &to_opts, &ctx).await?;

    // Then migrate relationships
    total_migrated += migrate_neo4j_relationships(&graph, &surreal, &to_opts, &ctx).await?;

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Use timestamp-based checkpoint tracking
        let checkpoint = crate::neo4j_checkpoint::get_current_checkpoint();

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
    ctx: &Neo4jConversionContext,
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

        let mut batch: Vec<Record> = Vec::new();
        let mut processed = 0;

        while let Some(row) = node_result.next().await? {
            let surreal_record = convert_neo4j_row_to_record(&row, &label, ctx)?;

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("Converted Neo4j node to SurrealDB record {surreal_record:?}",);
            }

            batch.push(surreal_record);

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for label: {}",
                    batch.len(),
                    label
                );
                if !to_opts.dry_run {
                    surreal_sync_surreal::write_records(surreal, &label.to_lowercase(), &batch)
                        .await?;
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
                surreal_sync_surreal::write_records(surreal, &label.to_lowercase(), &batch).await?;
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
    ctx: &Neo4jConversionContext,
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

        let mut batch: Vec<SurrealRelation> = Vec::new();
        let mut processed = 0;

        while let Some(row) = rel_result.next().await? {
            let r = row_to_relation(&row, Some(rel_type.clone()), None)?;

            let surreal_relation = r.to_relation(ctx)?;

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j relationship to SurrealDB record (rel_id: {}): {:?}",
                    r.id,
                    r.surreal_record_id(),
                );
            }

            batch.push(surreal_relation);

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for type: {}",
                    batch.len(),
                    rel_type
                );
                if !to_opts.dry_run {
                    surreal_sync_surreal::write_relations(
                        surreal,
                        &rel_type.to_lowercase(),
                        &batch,
                    )
                    .await?;
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
                surreal_sync_surreal::write_relations(surreal, &rel_type.to_lowercase(), &batch)
                    .await?;
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

// Convert Neo4j node row to surreal record
fn convert_neo4j_row_to_record(
    row: &neo4rs::Row,
    label: &str,
    ctx: &Neo4jConversionContext,
) -> anyhow::Result<Record> {
    let node: neo4rs::Node = row.get("n")?;
    let node_id: i64 = row.get("node_id")?;

    // TODO make this configurable
    let id_property = "id"; // Use 'id' property as the SurrealDB record ID if present

    // Convert Neo4j node to SurrealDB format
    let mut data = convert_neo4j_node_to_surreal_kvs(node, node_id, label, ctx)?;
    // Create proper SurrealDB Thing for the record
    // Neo4j stores all properties as bolt types - integers are preserved as Integer,
    // strings as String, etc. We convert each type to the appropriate SurrealDB ID type.
    let id = match data.remove(id_property) {
        Some(Value::Strand(s)) => {
            // String ID - try to parse as integer first (common case: numeric strings)
            let s_str = s.as_str();
            if let Ok(n) = s_str.parse::<i64>() {
                surrealdb::sql::Thing::from((label.to_lowercase(), surrealdb::sql::Id::Number(n)))
            } else {
                // Keep as string ID
                surrealdb::sql::Thing::from((label.to_lowercase(), s_str.to_string()))
            }
        }
        Some(Value::Number(Number::Int(n))) => {
            // Integer ID - use Number ID type directly
            surrealdb::sql::Thing::from((label.to_lowercase(), surrealdb::sql::Id::Number(n)))
        }
        Some(other) => {
            return Err(anyhow::anyhow!(
                "Node with label '{label}' and id '{node_id}' has unsupported 'id' property type: {other:?}. \
                 Expected String or Int.",
            ));
        }
        None => {
            // No 'id' property - use Neo4j's internal node_id as string
            // Note: Could be improved to use Number ID if desired
            surrealdb::sql::Thing::from((label.to_lowercase(), node_id.to_string()))
        }
    };

    anyhow::Ok(Record::new(id, data))
}

/// Convert Neo4j node to keys and surreal values
fn convert_neo4j_node_to_surreal_kvs(
    node: neo4rs::Node,
    node_id: i64,
    label: &str,
    ctx: &Neo4jConversionContext,
) -> anyhow::Result<std::collections::HashMap<String, Value>> {
    let mut kvs = std::collections::HashMap::new();

    // Add neo4j_id as a field (preserve original Neo4j ID)
    kvs.insert("neo4j_id".to_string(), Value::Number(Number::Int(node_id)));

    // Add labels as an array
    let labels: Vec<String> = node.labels().into_iter().map(|s| s.to_string()).collect();
    let labels_surreal_values: Vec<Value> = labels
        .into_iter()
        .map(|s| Value::Strand(Strand::from(s)))
        .collect();
    kvs.insert(
        "labels".to_string(),
        Value::Array(Array::from(labels_surreal_values)),
    );

    // Convert all properties
    for key in node.keys() {
        let value = node.get::<neo4rs::BoltType>(key)?;

        // Check if this property should be parsed from JSON string
        let should_parse_json = ctx.should_parse_json(label, key);
        tracing::debug!(
            "Converting property '{}' of node label '{}' with JSON parse: {}",
            key,
            label,
            should_parse_json
        );
        let val = convert_neo4j_type_to_surreal_value(value, &ctx.timezone, should_parse_json)?;

        kvs.insert(key.to_string(), val);
    }

    Ok(kvs)
}

pub struct Relation {
    pub rel_type: String,
    pub id: i64,
    pub start_labels: Vec<String>,
    pub start_node_id: i64,
    pub end_labels: Vec<String>,
    pub end_node_id: i64,
    pub relationship: neo4rs::Relation,
    pub updated_at: i64,
}

impl Relation {
    pub fn to_relation(&self, ctx: &Neo4jConversionContext) -> anyhow::Result<SurrealRelation> {
        let id = surrealdb::sql::Thing::from((
            self.rel_type.to_lowercase(),
            surrealdb::sql::Id::from(self.id),
        ));

        let input = surrealdb::sql::Thing::from((
            self.start_labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string()),
            surrealdb::sql::Id::from(self.start_node_id),
        ));

        let output = surrealdb::sql::Thing::from((
            self.end_labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string()),
            surrealdb::sql::Id::from(self.end_node_id),
        ));

        let mut data = std::collections::HashMap::new();
        for k in self.relationship.keys() {
            let value = self.relationship.get::<neo4rs::BoltType>(k)?;
            // Relationships don't have labels, so we can't use JSON-to-object for them
            let v = convert_neo4j_type_to_surreal_value(value, &ctx.timezone, false)?;
            // Wrap the Value in SurrealValue for the Relation struct
            data.insert(k.to_string(), surrealdb_types::SurrealValue(v));
        }

        Ok(SurrealRelation {
            id,
            input,
            output,
            data,
        })
    }

    pub fn surreal_record_id(&self) -> surrealdb::sql::Thing {
        let id = surrealdb::sql::Id::from(self.id).to_string();
        surrealdb::sql::Thing::from((self.rel_type.to_lowercase(), id))
    }
}

// If rel_type is provided, this function skips the type extraction from the relationship
pub fn row_to_relation(
    row: &neo4rs::Row,
    rel_type: Option<String>,
    change_tracking_property: Option<String>,
) -> anyhow::Result<Relation> {
    let relationship: neo4rs::Relation = row.get("r")?;
    let rel_id: i64 = row.get("rel_id")?;
    let rel_type: String = if let Some(t) = rel_type {
        t
    } else {
        row.get("rel_type")?
    };
    let start_id: i64 = row.get("start_id")?;
    let end_id: i64 = row.get("end_id")?;
    let start_labels: Vec<String> = row.get("start_labels")?;
    let end_labels: Vec<String> = row.get("end_labels")?;

    // Get the checkpoint value from the relationship
    let rel_checkpoint_opt = if let Some(change_tracking_property) = &change_tracking_property {
        if let Ok(ts) = relationship.get::<i64>(change_tracking_property) {
            Some(ts)
        } else if let Ok(dt) =
            relationship.get::<chrono::DateTime<chrono::Utc>>(change_tracking_property)
        {
            Some(dt.timestamp_millis())
        } else {
            anyhow::bail!(
                "Change tracking property '{change_tracking_property}' not found or of unsupported type in relationship",
            );
        }
    } else {
        None
    };

    anyhow::Ok(Relation {
        rel_type,
        id: rel_id,
        start_labels,
        start_node_id: start_id,
        end_labels,
        end_node_id: end_id,
        relationship,
        updated_at: rel_checkpoint_opt.unwrap_or(0),
    })
}

/// Convert Neo4j BoltType to surrealdb::sql::Value
///
/// The timezone parameter specifies which timezone to use when converting Neo4j local datetime and time values.
/// This should be an IANA timezone name (e.g., "America/New_York", "Europe/London", "UTC").
///
/// If should_parse_json is true and the value is a String, attempt to parse it as JSON and convert to Object.
///
/// This function uses the `neo4j-types` crate for BoltType → UniversalValue conversion,
/// then converts UniversalValue to surrealdb::sql::Value.
pub fn convert_neo4j_type_to_surreal_value(
    value: neo4rs::BoltType,
    timezone: &str,
    should_parse_json: bool,
) -> anyhow::Result<Value> {
    use neo4j_types::reverse::ConversionConfig;

    // Create conversion config
    let config = ConversionConfig {
        timezone: timezone.to_string(),
        parse_json_strings: should_parse_json,
    };

    // Convert BoltType → UniversalValue using neo4j-types
    let universal_value = neo4j_types::convert_bolt_to_universal_value(value, &config)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Convert UniversalValue → surrealdb::sql::Value
    universal_value_to_surreal_value(universal_value)
}

/// Convert UniversalValue to surrealdb::sql::Value.
///
/// This bridges from sync-core's UniversalValue to surrealdb's native Value type.
fn universal_value_to_surreal_value(value: sync_core::UniversalValue) -> anyhow::Result<Value> {
    use sync_core::UniversalValue;

    match value {
        UniversalValue::Null => Ok(Value::None),
        UniversalValue::Bool(b) => Ok(Value::Bool(b)),

        // Integer types → Number::Int
        UniversalValue::Int8 { value, .. } => Ok(Value::Number(Number::Int(value as i64))),
        UniversalValue::Int16(i) => Ok(Value::Number(Number::Int(i as i64))),
        UniversalValue::Int32(i) => Ok(Value::Number(Number::Int(i as i64))),
        UniversalValue::Int64(i) => Ok(Value::Number(Number::Int(i))),

        // Float types → Number::Float
        UniversalValue::Float32(f) => Ok(Value::Number(Number::Float(f as f64))),
        UniversalValue::Float64(f) => Ok(Value::Number(Number::Float(f))),

        // Decimal → Number::Float (best effort)
        UniversalValue::Decimal { value, .. } => match value.parse::<f64>() {
            Ok(f) => Ok(Value::Number(Number::Float(f))),
            Err(_) => Ok(Value::Strand(Strand::from(value))),
        },

        // String types → Strand
        UniversalValue::Char { value, .. } => Ok(Value::Strand(Strand::from(value))),
        UniversalValue::VarChar { value, .. } => Ok(Value::Strand(Strand::from(value))),
        UniversalValue::Text(s) => Ok(Value::Strand(Strand::from(s))),

        // Binary types → Bytes
        UniversalValue::Blob(b) => Ok(Value::Bytes(surrealdb::sql::Bytes::from(b))),
        UniversalValue::Bytes(b) => Ok(Value::Bytes(surrealdb::sql::Bytes::from(b))),

        // UUID → Strand
        UniversalValue::Uuid(u) => Ok(Value::Strand(Strand::from(u.to_string()))),

        // DateTime types → Datetime
        UniversalValue::Date(dt) => Ok(Value::Datetime(Datetime::from(dt))),
        UniversalValue::Time(dt) => Ok(Value::Datetime(Datetime::from(dt))),
        UniversalValue::LocalDateTime(dt) => Ok(Value::Datetime(Datetime::from(dt))),
        UniversalValue::LocalDateTimeNano(dt) => Ok(Value::Datetime(Datetime::from(dt))),
        UniversalValue::ZonedDateTime(dt) => Ok(Value::Datetime(Datetime::from(dt))),

        // JSON types → Object (via helper function)
        UniversalValue::Json(json_val) => json_value_to_surreal_value(&json_val),
        UniversalValue::Jsonb(json_val) => json_value_to_surreal_value(&json_val),

        // Array → Array
        UniversalValue::Array { elements, .. } => {
            let mut arr = Vec::new();
            for elem in elements {
                arr.push(universal_value_to_surreal_value(elem)?);
            }
            Ok(Value::Array(Array::from(arr)))
        }

        // Set → Array of strings
        UniversalValue::Set { elements, .. } => {
            let arr: Vec<Value> = elements
                .into_iter()
                .map(|s| Value::Strand(Strand::from(s)))
                .collect();
            Ok(Value::Array(Array::from(arr)))
        }

        // Enum → Strand
        UniversalValue::Enum { value, .. } => Ok(Value::Strand(Strand::from(value))),

        // Geometry → Object (via GeoJSON-like structure)
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(json_val) = data;
            json_value_to_surreal_value(&json_val)
        }

        // Duration → Duration
        UniversalValue::Duration(d) => Ok(Value::Duration(surrealdb::sql::Duration::from(d))),
    }
}

/// Convert a serde_json::Value to surrealdb::sql::Value.
fn json_value_to_surreal_value(value: &serde_json::Value) -> anyhow::Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::None),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Number(Number::Int(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Number(Number::Float(f)))
            } else {
                Ok(Value::None)
            }
        }
        serde_json::Value::String(s) => Ok(Value::Strand(Strand::from(s.clone()))),
        serde_json::Value::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                result.push(json_value_to_surreal_value(item)?);
            }
            Ok(Value::Array(Array::from(result)))
        }
        serde_json::Value::Object(obj) => {
            let mut result = std::collections::BTreeMap::new();
            for (key, val) in obj {
                result.insert(key.clone(), json_value_to_surreal_value(val)?);
            }
            Ok(Value::Object(Object::from(result)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use neo4rs::{BoltDate, BoltDateTimeZoneId, BoltLocalDateTime};

    #[test]
    fn test_date_conversion_with_utc_timezone() {
        // Create a Neo4j Date for 2024-01-15
        let date = BoltDate::from(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let bolt_type = neo4rs::BoltType::Date(date);

        // Convert with UTC timezone
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // Date should be at midnight UTC on 2024-01-15
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 0);
                assert_eq!(dt.minute(), 0);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_date_conversion_with_new_york_timezone() {
        // Create a Neo4j Date for 2024-01-15
        let date = BoltDate::from(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let bolt_type = neo4rs::BoltType::Date(date);

        // Convert with America/New_York timezone
        let result =
            convert_neo4j_type_to_surreal_value(bolt_type, "America/New_York", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // Date at midnight in New York (EST = UTC-5) should be 5 AM UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 00:00 EST = 05:00 UTC
                assert_eq!(dt.minute(), 0);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_local_datetime_conversion_with_utc_timezone() {
        // Create a Neo4j LocalDateTime for 2024-01-15 14:30:00
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );
        let local_dt = BoltLocalDateTime::from(naive_dt);
        let bolt_type = neo4rs::BoltType::LocalDateTime(local_dt);

        // Convert with UTC timezone
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // Should be same time in UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_local_datetime_conversion_with_tokyo_timezone() {
        // Create a Neo4j LocalDateTime for 2024-01-15 14:30:00
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );
        let local_dt = BoltLocalDateTime::from(naive_dt);
        let bolt_type = neo4rs::BoltType::LocalDateTime(local_dt);

        // Convert with Asia/Tokyo timezone
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "Asia/Tokyo", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // 14:30 in Tokyo (JST = UTC+9) should be 05:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 14:30 JST = 05:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_invalid_timezone_returns_error() {
        let date = BoltDate::from(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let bolt_type = neo4rs::BoltType::Date(date);

        // Try with invalid timezone
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "Invalid/Timezone", false);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid timezone"));
    }

    #[test]
    fn test_daylight_saving_time_transition() {
        // Test date during DST transition (March 10, 2024, when DST starts in US)
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 10).unwrap(),
            NaiveTime::from_hms_opt(2, 30, 0).unwrap(), // 2:30 AM - a time that doesn't exist due to DST
        );
        let local_dt = BoltLocalDateTime::from(naive_dt);
        let bolt_type = neo4rs::BoltType::LocalDateTime(local_dt);

        // Convert with America/New_York timezone - this should handle the DST gap
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "America/New_York", false);

        // The conversion should either succeed with an adjusted time or fail gracefully
        // depending on how chrono-tz handles DST gaps
        match result {
            Ok(Value::Datetime(_)) => {
                // If it succeeds, that's fine - chrono-tz adjusted the time
            }
            Err(e) => {
                // If it fails, it should be due to ambiguous datetime
                assert!(e.to_string().contains("Ambiguous or invalid datetime"));
            }
            _ => panic!("Unexpected result type"),
        }
    }

    #[test]
    fn test_datetime_zone_id_conversion_utc() {
        // Create a DateTimeZoneId with UTC timezone
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );

        let datetime_zone_id = BoltDateTimeZoneId::from((naive_dt, "UTC"));
        let bolt_type = neo4rs::BoltType::DateTimeZoneId(datetime_zone_id);

        // Convert to surreal value
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // Should be same time in UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_datetime_zone_id_conversion_new_york() {
        // Create a DateTimeZoneId with America/New_York timezone
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );

        let datetime_zone_id = BoltDateTimeZoneId::from((naive_dt, "America/New_York"));
        let bolt_type = neo4rs::BoltType::DateTimeZoneId(datetime_zone_id);

        // Convert to surreal value
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // 14:30 EST (UTC-5) should be 19:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 19); // 14:30 EST = 19:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_datetime_zone_id_conversion_tokyo() {
        // Create a DateTimeZoneId with Asia/Tokyo timezone
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );

        let datetime_zone_id = BoltDateTimeZoneId::from((naive_dt, "Asia/Tokyo"));
        let bolt_type = neo4rs::BoltType::DateTimeZoneId(datetime_zone_id);

        // Convert to surreal value
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // 14:30 JST (UTC+9) should be 05:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 14:30 JST = 05:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_datetime_zone_id_conversion_london_dst() {
        // Create a DateTimeZoneId with Europe/London timezone during DST
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 7, 15).unwrap(), // July - during DST
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        );

        let datetime_zone_id = BoltDateTimeZoneId::from((naive_dt, "Europe/London"));
        let bolt_type = neo4rs::BoltType::DateTimeZoneId(datetime_zone_id);

        // Convert to surreal value
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // 14:30 BST (UTC+1) should be 13:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 7);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 13); // 14:30 BST = 13:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_datetime_zone_id_conversion_negative_offset() {
        // Create a DateTimeZoneId with Pacific/Honolulu timezone (UTC-10)
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(8, 15, 30).unwrap(),
        );

        let datetime_zone_id = BoltDateTimeZoneId::from((naive_dt, "Pacific/Honolulu"));
        let bolt_type = neo4rs::BoltType::DateTimeZoneId(datetime_zone_id);

        // Convert to surreal value
        let result = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();

        match result {
            Value::Datetime(dt) => {
                // 08:15:30 HST (UTC-10) should be 18:15:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 6);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 18); // 08:15 HST = 18:15 UTC
                assert_eq!(dt.minute(), 15);
                assert_eq!(dt.second(), 30);
            }
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_json_string_conversion() {
        // Test JSON string parsed as object
        let json_string = r#"{"preferences":{"theme":"dark","language":"en"},"tags":["premium","verified"],"settings":{"notifications":true,"privacy":"strict"}}"#;
        let bolt_type = neo4rs::BoltType::String(neo4rs::BoltString::new(json_string));

        // Convert with JSON parsing enabled
        let result = convert_neo4j_type_to_surreal_value(bolt_type.clone(), "UTC", true).unwrap();

        match result {
            Value::Object(obj) => {
                // Verify the JSON was parsed correctly
                assert!(obj.contains_key("preferences"));
                assert!(obj.contains_key("tags"));
                assert!(obj.contains_key("settings"));

                // Verify nested structure
                if let Some(Value::Object(prefs)) = obj.get("preferences") {
                    assert_eq!(
                        prefs.get("theme"),
                        Some(&Value::Strand(Strand::from("dark".to_string())))
                    );
                }

                // Verify array
                if let Some(Value::Array(tags)) = obj.get("tags") {
                    assert_eq!(tags.len(), 2);
                }
            }
            _ => panic!("Expected Object, got {result:?}"),
        }

        // Test that without JSON parsing, it remains a string
        let result_no_parse = convert_neo4j_type_to_surreal_value(bolt_type, "UTC", false).unwrap();
        match result_no_parse {
            Value::Strand(s) => {
                assert_eq!(s.as_str(), json_string);
            }
            _ => panic!("Expected Strand, got {result_no_parse:?}"),
        }
    }

    #[test]
    fn test_neo4j_conversion_context() {
        // Test context creation
        let ctx = Neo4jConversionContext::new(
            "UTC".to_string(),
            Some(vec!["User.metadata".to_string(), "Post.config".to_string()]),
        )
        .unwrap();

        // Test should_parse_json
        assert!(ctx.should_parse_json("User", "metadata"));
        assert!(ctx.should_parse_json("Post", "config"));
        assert!(!ctx.should_parse_json("User", "name"));
        assert!(!ctx.should_parse_json("Post", "title"));

        // Test empty context
        let empty_ctx = Neo4jConversionContext::new("UTC".to_string(), None).unwrap();
        assert!(!empty_ctx.should_parse_json("User", "metadata"));
    }

    #[test]
    fn test_neo4j_json_property_parse() {
        // Test valid format
        let prop = Neo4jJsonProperty::parse("User.metadata").unwrap();
        assert_eq!(prop.label, "User");
        assert_eq!(prop.property, "metadata");

        // Test invalid format
        assert!(Neo4jJsonProperty::parse("Invalid").is_err());
        assert!(Neo4jJsonProperty::parse("Too.Many.Dots").is_err());

        // Test parse_vec
        let props =
            Neo4jJsonProperty::parse_vec(&["User.metadata".to_string(), "Post.config".to_string()])
                .unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].label, "User");
        assert_eq!(props[1].property, "config");
    }
}
