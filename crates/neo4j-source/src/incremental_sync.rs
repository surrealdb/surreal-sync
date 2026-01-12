//! Neo4j incremental sync implementation
//!
//! This module provides incremental synchronization capabilities for Neo4j using
//! timestamp-based change tracking.
//!
//! The approach relies on a specified property (e.g., "updated_at") on nodes and relationships
//! to track changes. Changes are detected by querying for entities with a timestamp greater than
//! the last known checkpoint.
//!
//! This timestamp-based approach cannot detect
//! when nodes or relationships are deleted from Neo4j, since deleted entities
//! no longer exist to query. Deleted data will remain in SurrealDB.
//!
//! If you need to sync deletions, run periodic clean-ups and full syncs to ensure SurrealDB
//! exactly matches Neo4j.

use crate::neo4j_checkpoint::Neo4jCheckpoint;
use crate::{Neo4jConversionContext, SourceOpts, SurrealOpts};
use async_trait::async_trait;
use chrono::Utc;
use neo4rs::{Graph, Query};
use std::collections::HashMap;
use surreal_sync_surreal::{apply_change, surreal_connect, Change, SurrealOpts as SurrealConnOpts};
use surrealdb::sql::{Array, Number, Strand, Value};
use surrealdb_types::RecordWithSurrealValues as Record;

/// Trait for a stream of changes from Neo4j
#[async_trait]
pub trait ChangeStream: Send + Sync {
    /// Get the next change event from the stream
    async fn next(&mut self) -> Option<anyhow::Result<Change>>;

    /// Get the current checkpoint of the stream
    fn checkpoint(&self) -> Option<Neo4jCheckpoint>;
}

/// Neo4j implementation of incremental sync source
pub struct Neo4jIncrementalSource {
    graph: Graph,
    /// Conversion context with timezone and JSON-to-object configuration
    ctx: Neo4jConversionContext,
    /// The property name used for tracking changes (e.g., "updated_at", "modified_at")
    change_tracking_property: String,
    /// Current timestamp position for incremental sync (as i64)
    current_timestamp: i64,
}

impl Neo4jIncrementalSource {
    /// Create a new Neo4j incremental source
    pub fn new(
        graph: Graph,
        neo4j_timezone: String,
        neo4j_json_properties: Option<Vec<String>>,
        change_tracking_property: Option<String>,
        initial_timestamp: i64,
    ) -> anyhow::Result<Self> {
        let ctx = Neo4jConversionContext::new(neo4j_timezone, neo4j_json_properties)?;
        Ok(Neo4jIncrementalSource {
            graph,
            ctx,
            change_tracking_property: change_tracking_property
                .unwrap_or_else(|| "updated_at".to_string()),
            current_timestamp: initial_timestamp,
        })
    }

    /// Initialize the incremental source
    pub async fn initialize(&mut self) -> anyhow::Result<()> {
        // Source is already initialized via constructor - nothing to do
        Ok(())
    }

    /// Get a stream of changes
    pub async fn get_changes(&mut self) -> anyhow::Result<Box<dyn ChangeStream>> {
        Ok(Box::new(Neo4jChangeStream::new(
            self.graph.clone(),
            self.current_timestamp,
            self.change_tracking_property.clone(),
            self.ctx.clone(),
        )))
    }

    /// Get the current checkpoint
    pub fn get_checkpoint(&self) -> anyhow::Result<Neo4jCheckpoint> {
        let checkpoint_datetime =
            chrono::DateTime::from_timestamp_millis(self.current_timestamp)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", self.current_timestamp))?;
        Ok(Neo4jCheckpoint {
            timestamp: checkpoint_datetime,
        })
    }

    /// Cleanup resources
    pub async fn cleanup(self) -> anyhow::Result<()> {
        // No cleanup needed for custom tracking approach
        Ok(())
    }
}

/// Implementation of change stream for Neo4j
pub struct Neo4jChangeStream {
    graph: Graph,
    current_checkpoint: i64,
    change_tracking_property: String,
    /// Conversion context with timezone and JSON-to-object configuration
    ctx: Neo4jConversionContext,
    /// Buffer for batched changes
    change_buffer: Vec<Change>,
    /// Whether we've finished reading all changes
    finished: bool,
}

impl Neo4jChangeStream {
    pub fn new(
        graph: Graph,
        from_checkpoint: i64,
        change_tracking_property: String,
        ctx: Neo4jConversionContext,
    ) -> Self {
        Neo4jChangeStream {
            graph,
            current_checkpoint: from_checkpoint,
            change_tracking_property,
            ctx,
            change_buffer: Vec::new(),
            finished: false,
        }
    }

    /// Fetch the next batch of changes from Neo4j
    async fn fetch_next_batch(&mut self) -> anyhow::Result<()> {
        if self.finished {
            return Ok(());
        }

        // Query for nodes that have been modified since the checkpoint
        // Convert checkpoint timestamp to datetime for comparison with Neo4j datetime properties
        let checkpoint_datetime = chrono::DateTime::from_timestamp_millis(self.current_checkpoint)
            .unwrap_or_else(chrono::Utc::now);

        let node_query = Query::new(format!(
            "MATCH (n)
             WHERE n.{} > datetime($checkpoint)
             RETURN n, id(n) as node_id, labels(n) as labels
             ORDER BY n.{}
             LIMIT 100",
            self.change_tracking_property, self.change_tracking_property
        ))
        .param("checkpoint", checkpoint_datetime.to_rfc3339());

        let mut result = self.graph.execute(node_query).await?;
        let mut batch_changes = Vec::new();
        let mut max_checkpoint = self.current_checkpoint;
        let mut record_id: surrealdb::sql::Id;

        while let Some(row) = result.next().await? {
            let node: neo4rs::Node = row.get("n")?;
            let node_id: i64 = row.get("node_id")?;
            let labels: Vec<String> = row.get("labels")?;
            let label = labels.first().map(|s| s.as_str()).unwrap_or("");

            // Get the checkpoint value from the node
            let node_checkpoint = if let Ok(ts) = node.get::<i64>(&self.change_tracking_property) {
                ts
            } else if let Ok(dt) =
                node.get::<chrono::DateTime<chrono::Utc>>(&self.change_tracking_property)
            {
                dt.timestamp_millis()
            } else {
                continue; // Skip nodes without valid tracking property
            };

            max_checkpoint = max_checkpoint.max(node_checkpoint);

            // Convert node to surreal data
            let mut keys_and_surreal_values: HashMap<String, Value> = HashMap::new();
            keys_and_surreal_values
                .insert("neo4j_id".to_string(), Value::Number(Number::Int(node_id)));
            record_id = surrealdb::sql::Id::from(node_id);

            let surreal_labels: Vec<Value> = labels
                .iter()
                .map(|s| Value::Strand(Strand::from(s.clone())))
                .collect();
            keys_and_surreal_values.insert(
                "labels".to_string(),
                Value::Array(Array::from(surreal_labels)),
            );

            // Add all node properties with field renaming
            for key in node.keys() {
                if let Ok(value) = node.get::<neo4rs::BoltType>(key) {
                    // Determine label for JSON-to-object check
                    let should_parse_json = self.ctx.should_parse_json(label, key);

                    if let Ok(v) = crate::convert_neo4j_type_to_surreal_value(
                        value,
                        &self.ctx.timezone,
                        should_parse_json,
                    ) {
                        // Rename 'id' field to 'neo4j_original_id' to avoid conflict with SurrealDB record ID
                        let field_name = if key == "id" {
                            // Extract ID from the Value
                            record_id = match &v {
                                Value::Number(Number::Int(n)) => surrealdb::sql::Id::Number(*n),
                                Value::Strand(s) => {
                                    // Try to parse as integer first (common case: numeric strings)
                                    if let Ok(n) = s.as_str().parse::<i64>() {
                                        surrealdb::sql::Id::Number(n)
                                    } else {
                                        surrealdb::sql::Id::String(s.as_str().to_string())
                                    }
                                }
                                _ => surrealdb::sql::Id::from(node_id),
                            };
                            "neo4j_original_id".to_string()
                        } else {
                            key.to_string()
                        };
                        keys_and_surreal_values.insert(field_name, v);
                    }
                }
            }

            let table = labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string());

            batch_changes.push(Change::UpsertRecord(Record::new(
                surrealdb::sql::Thing::from((table, record_id)),
                keys_and_surreal_values,
            )));
        }

        // Also query for relationship changes
        let rel_query = Query::new(format!(
            "MATCH (a)-[r]->(b)
             WHERE r.{} > $checkpoint
             RETURN r, id(r) as rel_id, type(r) as rel_type,
                    id(a) as start_id, id(b) as end_id,
                    labels(a) as start_labels, labels(b) as end_labels,
                    'update' as operation
             ORDER BY r.{}
             LIMIT 100",
            self.change_tracking_property, self.change_tracking_property
        ))
        .param("checkpoint", self.current_checkpoint);

        let mut rel_result = self.graph.execute(rel_query).await?;

        while let Some(row) = rel_result.next().await? {
            // Convert relationship to surreal data
            let r =
                crate::row_to_relation(&row, None, Some(self.change_tracking_property.clone()))?;

            max_checkpoint = max_checkpoint.max(r.updated_at);

            batch_changes.push(Change::UpsertRelation(r.to_relation(&self.ctx)?));
        }

        if batch_changes.is_empty() {
            self.finished = true;
        } else {
            self.current_checkpoint = max_checkpoint;
            self.change_buffer = batch_changes;
        }

        Ok(())
    }
}

#[async_trait]
impl ChangeStream for Neo4jChangeStream {
    async fn next(&mut self) -> Option<anyhow::Result<Change>> {
        // If buffer is empty, fetch next batch
        if self.change_buffer.is_empty() && !self.finished {
            if let Err(e) = self.fetch_next_batch().await {
                return Some(Err(e));
            }
        }

        // Return next change from buffer
        self.change_buffer.pop().map(Ok)
    }

    fn checkpoint(&self) -> Option<Neo4jCheckpoint> {
        Some(Neo4jCheckpoint {
            timestamp: Utc::now(),
        })
    }
}

/// Apply incremental changes to SurrealDB
pub async fn apply_incremental_changes(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    changes: Vec<Change>,
    dry_run: bool,
) -> anyhow::Result<usize> {
    let mut applied_count = 0;

    for change in changes {
        tracing::debug!("Applying {change:?}",);

        if dry_run {
            tracing::info!("DRY RUN: Would apply {change:?}",);
            applied_count += 1;
            continue;
        }

        apply_change(surreal, &change).await?;

        applied_count += 1;
    }

    Ok(applied_count)
}

/// Run incremental sync from Neo4j to SurrealDB
///
/// This function implements the incremental sync logic:
/// 1. Uses timestamp-based change tracking
/// 2. Connects to both Neo4j and SurrealDB
/// 3. Reads changes from the specified checkpoint
/// 4. Applies changes to SurrealDB
/// 5. Continues until caught up with current state
pub async fn run_incremental_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    from_checkpoint: Neo4jCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<Neo4jCheckpoint>,
) -> anyhow::Result<()> {
    use checkpoint::Checkpoint;

    tracing::info!(
        "Starting Neo4j incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    // Use timestamp-based change tracking for incremental sync
    tracing::info!("Using timestamp-based change tracking for incremental sync");
    // Extract timestamp from checkpoint and create graph
    let initial_timestamp = from_checkpoint.timestamp.timestamp_millis();
    let graph = crate::new_neo4j_client(&from_opts).await?;

    let mut source = Neo4jIncrementalSource::new(
        graph,
        from_opts.neo4j_timezone.clone(),
        from_opts.neo4j_json_properties.clone(),
        None,
        initial_timestamp,
    )?;

    let surreal_conn_opts = SurrealConnOpts {
        surreal_endpoint: to_opts.surreal_endpoint.clone(),
        surreal_username: to_opts.surreal_username.clone(),
        surreal_password: to_opts.surreal_password.clone(),
    };
    let surreal = surreal_connect(&surreal_conn_opts, &to_namespace, &to_database).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    surreal.use_ns(&to_namespace).use_db(&to_database).await?;

    // Start reading changes
    let mut stream = source.get_changes().await?;
    let mut total_applied = 0;
    let mut batch = Vec::new();

    // Process changes in batches
    while let Some(change_result) = stream.next().await {
        let change = change_result?;

        // Check if we've reached the deadline
        if chrono::Utc::now() >= deadline {
            tracing::info!("Reached deadline: {deadline}, stopping incremental sync");
            break;
        }

        // Check if we've reached the target checkpoint
        if let Some(ref target) = target_checkpoint {
            if let Some(current) = stream.checkpoint() {
                if current.timestamp >= target.timestamp {
                    tracing::info!(
                        "Reached target checkpoint: {}, stopping incremental sync",
                        target.to_cli_string()
                    );
                    break;
                }
            }
        }

        batch.push(change);

        // Apply batch when it reaches the configured size
        if batch.len() >= to_opts.batch_size {
            let applied =
                apply_incremental_changes(&surreal, batch.clone(), to_opts.dry_run).await?;
            total_applied += applied;

            tracing::info!("Applied {} changes (total: {})", applied, total_applied);

            batch.clear();
        }
    }

    // Apply remaining changes
    if !batch.is_empty() {
        let applied = apply_incremental_changes(&surreal, batch, to_opts.dry_run).await?;
        total_applied += applied;
    }

    tracing::info!(
        "Incremental sync completed: {} total changes applied",
        total_applied
    );

    // Update the transaction tracker if not in dry run mode
    // Note: Transaction tracking update is only available for the custom tracking approach,
    // not for CDC which manages its own cursor internally
    if !to_opts.dry_run {
        tracing::info!(
            "Incremental sync checkpoint tracking updated internally by the source implementation"
        );
    }

    Ok(())
}
