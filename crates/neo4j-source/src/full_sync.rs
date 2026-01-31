//! Neo4j full sync implementation
//!
//! This module provides full synchronization from Neo4j to SurrealDB.

use neo4rs::{ConfigBuilder, Graph, Query};
use std::collections::HashMap;
use std::collections::HashSet;
use surreal_sink::SurrealSink;
use sync_core::{UniversalRelation, UniversalRow, UniversalThingRef, UniversalValue};

use crate::neo4j_checkpoint::Neo4jCheckpoint;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};

/// Source database connection options (Neo4j-specific, library type without clap)
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub source_uri: String,
    pub source_database: Option<String>,
    pub source_username: Option<String>,
    pub source_password: Option<String>,
    /// Labels to sync (empty means all labels)
    pub labels: Vec<String>,
    pub neo4j_timezone: String,
    pub neo4j_json_properties: Option<Vec<String>>,
    /// Property name for change tracking (e.g., "updated_at")
    pub change_tracking_property: String,
    /// Assumed start timestamp to use when no tracking property timestamps are found
    /// Only used when allow_empty_tracking_timestamp is true
    pub assumed_start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Allow full sync on data without tracking property timestamps
    /// When enabled with assumed_start_timestamp, uses the assumed timestamp for checkpoints
    pub allow_empty_tracking_timestamp: bool,
}

/// Sync options (non-connection related)
#[derive(Clone, Debug)]
pub struct SyncOpts {
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
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `from_opts` - Source database options
/// * `sync_opts` - Sync configuration options
/// * `sync_manager` - Optional sync manager for checkpoint emission
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
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
    tracing::info!("Connected to both Neo4j and SurrealDB");

    // Create conversion context with JSON-to-object configuration
    let ctx = Neo4jConversionContext::new(
        from_opts.neo4j_timezone.clone(),
        from_opts.neo4j_json_properties.clone(),
    )?;

    let mut total_migrated = 0;

    // Migrate nodes first and track min/max timestamps
    let (nodes_migrated, nodes_min_ts, nodes_max_ts) = migrate_neo4j_nodes(
        &graph,
        surreal,
        &sync_opts,
        &ctx,
        &from_opts.labels,
        &from_opts.change_tracking_property,
    )
    .await?;
    total_migrated += nodes_migrated;

    // Then migrate relationships and track min/max timestamps
    let (rels_migrated, rels_min_ts, rels_max_ts) = migrate_neo4j_relationships(
        &graph,
        surreal,
        &sync_opts,
        &ctx,
        &from_opts.change_tracking_property,
    )
    .await?;
    total_migrated += rels_migrated;

    // Compute overall min/max timestamps from both nodes and relationships
    let overall_min = [nodes_min_ts, rels_min_ts].into_iter().flatten().min();
    let overall_max = [nodes_max_ts, rels_max_ts].into_iter().flatten().max();

    // Emit checkpoints t1 and t2 (at end of full sync) if configured
    if let Some(manager) = sync_manager {
        // Determine timestamps to use for checkpoints
        let (min_timestamp, max_timestamp) = match (overall_min, overall_max) {
            // Case 1: We have actual timestamps from the data - use them
            (Some(min), Some(max)) => (min, max),

            // Case 2: No timestamps found - check if fallback is allowed
            (None, None) => {
                if from_opts.allow_empty_tracking_timestamp {
                    // User opted in to allow empty tracking timestamps
                    if let Some(assumed_ts) = from_opts.assumed_start_timestamp {
                        tracing::warn!(
                            "No tracking property '{}' timestamps found in synced data. \
                             Using assumed start timestamp for checkpoints: {}",
                            from_opts.change_tracking_property,
                            assumed_ts.to_rfc3339()
                        );
                        (assumed_ts, assumed_ts)
                    } else {
                        anyhow::bail!(
                            "No tracking property '{}' timestamps found in synced data and \
                             --assumed-start-timestamp is not set. When using \
                             --allow-empty-tracking-timestamp, you must also provide \
                             --assumed-start-timestamp.",
                            from_opts.change_tracking_property
                        );
                    }
                } else {
                    anyhow::bail!(
                        "No tracking property '{}' timestamps found in synced data. \
                         For incremental sync to work, all Neo4j nodes and relationships must have \
                         the '{}' field. Options:\n\
                         1. Add the tracking property to your data\n\
                         2. Run full sync without checkpoint tracking (omit --checkpoint-dir and \
                            --checkpoints-surreal-table flags)\n\
                         3. Use --assumed-start-timestamp and --allow-empty-tracking-timestamp flags \
                            for testing/loadtest scenarios",
                        from_opts.change_tracking_property,
                        from_opts.change_tracking_property
                    );
                }
            }

            // Case 3: Partial timestamps (shouldn't happen, but handle it)
            _ => {
                anyhow::bail!(
                    "Inconsistent tracking property '{}' timestamps: found timestamps in some \
                     but not all synced data. This indicates data quality issues.",
                    from_opts.change_tracking_property
                );
            }
        };

        // t1 (FullSyncStart) = min timestamp from synced data (or assumed timestamp)
        // t2 (FullSyncEnd) = max timestamp from synced data (or assumed timestamp)
        //
        // These checkpoints mark the logical boundaries of the synced data:
        // - t1: Oldest tracking property timestamp in the synced data
        // - t2: Newest tracking property timestamp in the synced data
        //
        // Incremental sync will read t1 and query: `WHERE n.{tracking_property} > datetime(t1)`
        // This captures all changes that occurred after the oldest synced data timestamp.
        //
        // **Note**: Subsequent incremental sync runs read checkpoints from previous
        // incremental sync runs (via --incremental-from CLI param or checkpoint store).
        let checkpoint_t1 = Neo4jCheckpoint {
            timestamp: min_timestamp,
        };

        manager
            .emit_checkpoint(&checkpoint_t1, SyncPhase::FullSyncStart)
            .await?;

        tracing::info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint_t1.to_cli_string()
        );

        let checkpoint_t2 = Neo4jCheckpoint {
            timestamp: max_timestamp,
        };

        manager
            .emit_checkpoint(&checkpoint_t2, SyncPhase::FullSyncEnd)
            .await?;

        tracing::info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint_t2.to_cli_string()
        );
    }

    tracing::info!(
        "Neo4j migration completed: {} total items migrated",
        total_migrated
    );
    Ok(())
}

/// Migrate all nodes from Neo4j to SurrealDB
/// Returns (count, min_timestamp, max_timestamp)
async fn migrate_neo4j_nodes<S: SurrealSink>(
    graph: &Graph,
    surreal: &S,
    sync_opts: &SyncOpts,
    ctx: &Neo4jConversionContext,
    labels_filter: &[String],
    tracking_property: &str,
) -> anyhow::Result<(
    usize,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
)> {
    tracing::info!("Starting Neo4j nodes migration");

    let mut min_timestamp: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut max_timestamp: Option<chrono::DateTime<chrono::Utc>> = None;

    // Determine which labels to process
    let labels_to_process: Vec<String> = if labels_filter.is_empty() {
        // No filter - get all distinct labels from the database
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
        all_labels.into_iter().collect()
    } else {
        tracing::info!(
            "Filtering to {} specified labels: {:?}",
            labels_filter.len(),
            labels_filter
        );
        labels_filter.to_vec()
    };

    let mut total_migrated = 0;

    // Process each label separately to create proper SurrealDB tables
    for label in labels_to_process {
        tracing::info!("Migrating nodes with label: {}", label);

        let node_query = Query::new(
            "MATCH (n) WHERE $label IN labels(n) RETURN n, id(n) as node_id".to_string(),
        )
        .param("label", label.clone());
        let mut node_result = graph.execute(node_query).await?;

        let mut batch: Vec<UniversalRow> = Vec::new();
        let mut processed = 0;

        while let Some(row) = node_result.next().await? {
            let universal_row = convert_neo4j_row_to_universal_row(&row, &label, ctx)?;

            // Extract tracking property timestamp if present
            if let Some(UniversalValue::LocalDateTime(ts)) =
                universal_row.get_field(tracking_property)
            {
                min_timestamp = Some(min_timestamp.map_or(*ts, |min| min.min(*ts)));
                max_timestamp = Some(max_timestamp.map_or(*ts, |max| max.max(*ts)));
            }

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!("Converted Neo4j node to UniversalRow {universal_row:?}",);
            }

            batch.push(universal_row);

            if batch.len() >= sync_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for label: {}",
                    batch.len(),
                    label
                );
                if !sync_opts.dry_run {
                    surreal.write_universal_rows(&batch).await?;
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
            if !sync_opts.dry_run {
                surreal.write_universal_rows(&batch).await?;
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

    if let (Some(min), Some(max)) = (min_timestamp, max_timestamp) {
        tracing::info!(
            "Node timestamp range: {} to {}",
            min.to_rfc3339(),
            max.to_rfc3339()
        );
    }

    Ok((total_migrated, min_timestamp, max_timestamp))
}

/// Migrate all relationships from Neo4j to SurrealDB
/// Returns (count, min_timestamp, max_timestamp)
async fn migrate_neo4j_relationships<S: SurrealSink>(
    graph: &Graph,
    surreal: &S,
    sync_opts: &SyncOpts,
    ctx: &Neo4jConversionContext,
    tracking_property: &str,
) -> anyhow::Result<(
    usize,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
)> {
    tracing::info!("Starting Neo4j relationships migration");

    let mut min_timestamp: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut max_timestamp: Option<chrono::DateTime<chrono::Utc>> = None;

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

        let mut batch: Vec<UniversalRelation> = Vec::new();
        let mut processed = 0;

        while let Some(row) = rel_result.next().await? {
            let r = row_to_relation(&row, Some(rel_type.clone()), None)?;

            let universal_relation = r.to_universal_relation(ctx)?;

            // Extract tracking property timestamp if present
            if let Some(UniversalValue::LocalDateTime(ts)) =
                universal_relation.data.get(tracking_property)
            {
                min_timestamp = Some(min_timestamp.map_or(*ts, |min| min.min(*ts)));
                max_timestamp = Some(max_timestamp.map_or(*ts, |max| max.max(*ts)));
            }

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j relationship to UniversalRelation (rel_id: {})",
                    r.id,
                );
            }

            batch.push(universal_relation);

            if batch.len() >= sync_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for type: {}",
                    batch.len(),
                    rel_type
                );
                if !sync_opts.dry_run {
                    surreal.write_universal_relations(&batch).await?;
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
            if !sync_opts.dry_run {
                surreal.write_universal_relations(&batch).await?;
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

    if let (Some(min), Some(max)) = (min_timestamp, max_timestamp) {
        tracing::info!(
            "Relationship timestamp range: {} to {}",
            min.to_rfc3339(),
            max.to_rfc3339()
        );
    }

    Ok((total_migrated, min_timestamp, max_timestamp))
}

// Convert Neo4j node row to UniversalRow
fn convert_neo4j_row_to_universal_row(
    row: &neo4rs::Row,
    label: &str,
    ctx: &Neo4jConversionContext,
) -> anyhow::Result<UniversalRow> {
    let node: neo4rs::Node = row.get("n")?;
    let node_id: i64 = row.get("node_id")?;

    // TODO make this configurable
    let id_property = "id"; // Use 'id' property as the SurrealDB record ID if present

    // Convert Neo4j node to universal format
    let mut data = convert_neo4j_node_to_universal_kvs(node, node_id, label, ctx)?;
    // Create proper ID for the record
    // Neo4j stores all properties as bolt types - integers are preserved as Integer,
    // strings as String, etc. We convert each type to the appropriate ID type.
    let id = match data.remove(id_property) {
        Some(UniversalValue::Text(s)) => {
            // String ID - try to parse as integer first (common case: numeric strings)
            if let Ok(n) = s.parse::<i64>() {
                UniversalValue::Int64(n)
            } else {
                // Keep as string ID
                UniversalValue::Text(s)
            }
        }
        Some(UniversalValue::Int64(n)) => UniversalValue::Int64(n),
        Some(other) => {
            return Err(anyhow::anyhow!(
                "Node with label '{label}' and id '{node_id}' has unsupported 'id' property type: {other:?}. \
                 Expected String or Int.",
            ));
        }
        None => {
            // No 'id' property - use Neo4j's internal node_id
            UniversalValue::Int64(node_id)
        }
    };

    Ok(UniversalRow::new(
        label.to_lowercase(),
        node_id as u64,
        id,
        data,
    ))
}

/// Convert Neo4j node to keys and universal values
fn convert_neo4j_node_to_universal_kvs(
    node: neo4rs::Node,
    node_id: i64,
    label: &str,
    ctx: &Neo4jConversionContext,
) -> anyhow::Result<HashMap<String, UniversalValue>> {
    let mut kvs = HashMap::new();

    // Add neo4j_id as a field (preserve original Neo4j ID)
    kvs.insert("neo4j_id".to_string(), UniversalValue::Int64(node_id));

    // Add labels as an array
    let labels: Vec<String> = node.labels().into_iter().map(|s| s.to_string()).collect();
    let labels_universal_values: Vec<UniversalValue> =
        labels.into_iter().map(UniversalValue::Text).collect();
    kvs.insert(
        "labels".to_string(),
        UniversalValue::Array {
            elements: labels_universal_values,
            element_type: Box::new(sync_core::UniversalType::Text),
        },
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
        let val = convert_neo4j_type_to_universal_value(value, &ctx.timezone, should_parse_json)?;

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
    pub fn to_universal_relation(
        &self,
        ctx: &Neo4jConversionContext,
    ) -> anyhow::Result<UniversalRelation> {
        let id = UniversalValue::Int64(self.id);

        let input = UniversalThingRef::new(
            self.start_labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string()),
            UniversalValue::Int64(self.start_node_id),
        );

        let output = UniversalThingRef::new(
            self.end_labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string()),
            UniversalValue::Int64(self.end_node_id),
        );

        let mut data = HashMap::new();
        for k in self.relationship.keys() {
            let value = self.relationship.get::<neo4rs::BoltType>(k)?;
            // Relationships don't have labels, so we can't use JSON-to-object for them
            let v = convert_neo4j_type_to_universal_value(value, &ctx.timezone, false)?;
            data.insert(k.to_string(), v);
        }

        Ok(UniversalRelation::new(
            self.rel_type.to_lowercase(),
            id,
            input,
            output,
            data,
        ))
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

/// Convert Neo4j BoltType to UniversalValue
///
/// The timezone parameter specifies which timezone to use when converting Neo4j local datetime and time values.
/// This should be an IANA timezone name (e.g., "America/New_York", "Europe/London", "UTC").
///
/// If should_parse_json is true and the value is a String, attempt to parse it as JSON and convert to Object.
pub fn convert_neo4j_type_to_universal_value(
    value: neo4rs::BoltType,
    timezone: &str,
    should_parse_json: bool,
) -> anyhow::Result<UniversalValue> {
    use neo4j_types::reverse::ConversionConfig;

    // Create conversion config
    let config = ConversionConfig {
        timezone: timezone.to_string(),
        parse_json_strings: should_parse_json,
    };

    // Convert BoltType → UniversalValue using neo4j-types
    neo4j_types::convert_bolt_to_universal_value(value, &config).map_err(|e| anyhow::anyhow!("{e}"))
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
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        // Neo4j Date converts to LocalDateTime (midnight in the configured timezone)
        match result {
            UniversalValue::LocalDateTime(dt) => {
                // Date should be at midnight UTC on 2024-01-15
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 0);
                assert_eq!(dt.minute(), 0);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected LocalDateTime, got {result:?}"),
        }
    }

    #[test]
    fn test_date_conversion_with_new_york_timezone() {
        // Create a Neo4j Date for 2024-01-15
        let date = BoltDate::from(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let bolt_type = neo4rs::BoltType::Date(date);

        // Convert with America/New_York timezone
        let result =
            convert_neo4j_type_to_universal_value(bolt_type, "America/New_York", false).unwrap();

        // Neo4j Date converts to LocalDateTime (midnight in the configured timezone, then converted to UTC)
        match result {
            UniversalValue::LocalDateTime(dt) => {
                // Date at midnight in New York (EST = UTC-5) should be 5 AM UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 00:00 EST = 05:00 UTC
                assert_eq!(dt.minute(), 0);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected LocalDateTime, got {result:?}"),
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
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::LocalDateTime(dt) => {
                // Should be same time in UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected LocalDateTime, got {result:?}"),
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
        let result = convert_neo4j_type_to_universal_value(bolt_type, "Asia/Tokyo", false).unwrap();

        match result {
            UniversalValue::LocalDateTime(dt) => {
                // 14:30 in Tokyo (JST = UTC+9) should be 05:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 14:30 JST = 05:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected LocalDateTime, got {result:?}"),
        }
    }

    #[test]
    fn test_invalid_timezone_returns_error() {
        let date = BoltDate::from(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let bolt_type = neo4rs::BoltType::Date(date);

        // Try with invalid timezone
        let result = convert_neo4j_type_to_universal_value(bolt_type, "Invalid/Timezone", false);

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
        let result = convert_neo4j_type_to_universal_value(bolt_type, "America/New_York", false);

        // The conversion should either succeed with an adjusted time or fail gracefully
        // depending on how chrono-tz handles DST gaps
        match result {
            Ok(UniversalValue::LocalDateTime(_)) => {
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

        // Convert to universal value
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::ZonedDateTime(dt) => {
                // Should be same time in UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected ZonedDateTime, got {result:?}"),
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

        // Convert to universal value
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::ZonedDateTime(dt) => {
                // 14:30 EST (UTC-5) should be 19:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 19); // 14:30 EST = 19:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected ZonedDateTime, got {result:?}"),
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

        // Convert to universal value
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::ZonedDateTime(dt) => {
                // 14:30 JST (UTC+9) should be 05:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 1);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 5); // 14:30 JST = 05:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected ZonedDateTime, got {result:?}"),
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

        // Convert to universal value
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::ZonedDateTime(dt) => {
                // 14:30 BST (UTC+1) should be 13:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 7);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 13); // 14:30 BST = 13:30 UTC
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            }
            _ => panic!("Expected ZonedDateTime, got {result:?}"),
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

        // Convert to universal value
        let result = convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();

        match result {
            UniversalValue::ZonedDateTime(dt) => {
                // 08:15:30 HST (UTC-10) should be 18:15:30 UTC
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 6);
                assert_eq!(dt.day(), 15);
                assert_eq!(dt.hour(), 18); // 08:15 HST = 18:15 UTC
                assert_eq!(dt.minute(), 15);
                assert_eq!(dt.second(), 30);
            }
            _ => panic!("Expected ZonedDateTime, got {result:?}"),
        }
    }

    #[test]
    fn test_json_string_conversion() {
        // Test JSON string parsed as Json type
        let json_string = r#"{"preferences":{"theme":"dark","language":"en"},"tags":["premium","verified"],"settings":{"notifications":true,"privacy":"strict"}}"#;
        let bolt_type = neo4rs::BoltType::String(neo4rs::BoltString::new(json_string));

        // Convert with JSON parsing enabled
        let result = convert_neo4j_type_to_universal_value(bolt_type.clone(), "UTC", true).unwrap();

        // neo4j-types returns UniversalValue::Json when JSON parsing is enabled
        match result {
            UniversalValue::Json(json_val) => {
                // Verify the JSON was parsed correctly
                let obj = json_val.as_object().expect("Expected JSON object");
                assert!(obj.contains_key("preferences"));
                assert!(obj.contains_key("tags"));
                assert!(obj.contains_key("settings"));

                // Verify nested structure
                let prefs = obj.get("preferences").and_then(|v| v.as_object());
                assert!(prefs.is_some());
                assert_eq!(
                    prefs.unwrap().get("theme").and_then(|v| v.as_str()),
                    Some("dark")
                );

                // Verify array
                let tags = obj.get("tags").and_then(|v| v.as_array());
                assert!(tags.is_some());
                assert_eq!(tags.unwrap().len(), 2);
            }
            _ => panic!("Expected Json, got {result:?}"),
        }

        // Test that without JSON parsing, it remains a string
        let result_no_parse =
            convert_neo4j_type_to_universal_value(bolt_type, "UTC", false).unwrap();
        match result_no_parse {
            UniversalValue::Text(s) => {
                assert_eq!(s, json_string);
            }
            _ => panic!("Expected Text, got {result_no_parse:?}"),
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
