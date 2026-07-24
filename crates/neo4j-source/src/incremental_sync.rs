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

use crate::full_sync::with_use_clause;
use crate::neo4j_checkpoint::Neo4jCheckpoint;
use crate::{Neo4jConversionContext, SourceOpts, SyncOpts};
use async_trait::async_trait;
use chrono::Utc;
use neo4rs::{Graph, Query};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, RelationChange, Row, Type, Value};
use surreal_sync_runtime::{
    ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver, SourceRuntimeOpts,
    StopReason,
};

/// A change from Neo4j (either a node or a relationship)
#[derive(Debug, Clone)]
pub enum IncrementalChange {
    /// A node upsert
    Node(Row),
    /// A relationship upsert (boxed to reduce enum size variance)
    Relation(Box<Relation>),
}

/// Sink-ordered apply position: timestamp plus keyset tie-breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Neo4jApplyPos {
    pub timestamp_millis: i64,
    pub after_node_id: i64,
    pub after_rel_id: i64,
}

/// Trait for a stream of changes from Neo4j
#[async_trait]
pub trait ChangeStream: Send + Sync {
    /// Get the next change event from the stream
    async fn next(&mut self) -> Option<anyhow::Result<IncrementalChange>>;

    /// Sink-safe checkpoint (advanced only via [`ChangeStream::commit_sunk`]).
    fn checkpoint(&self) -> Option<Neo4jCheckpoint>;

    /// Record that events through this apply position were successfully sunk.
    fn commit_sunk(&mut self, position: Neo4jApplyPos) {
        let _ = position;
    }
}

/// Neo4j implementation of incremental sync source
pub struct Neo4jIncrementalSource {
    graph: Graph,
    /// Conversion context with timezone and JSON-to-object configuration
    ctx: Neo4jConversionContext,
    /// The property name used for tracking changes (e.g., "updated_at", "modified_at")
    change_tracking_property: String,
    /// Property name to use as SurrealDB record ID (e.g., "id", "uuid")
    id_property: String,
    /// Current timestamp position for incremental sync (as i64)
    current_timestamp: i64,
    /// Optional composite database constituent for `USE` clause
    composite_constituent: Option<String>,
}

impl Neo4jIncrementalSource {
    /// Create a new Neo4j incremental source
    pub fn new(
        graph: Graph,
        neo4j_timezone: String,
        neo4j_json_properties: Option<Vec<String>>,
        change_tracking_property: Option<String>,
        id_property: String,
        initial_timestamp: i64,
        composite_constituent: Option<String>,
    ) -> anyhow::Result<Self> {
        let ctx = Neo4jConversionContext::new(neo4j_timezone, neo4j_json_properties)?;
        Ok(Neo4jIncrementalSource {
            graph,
            ctx,
            change_tracking_property: change_tracking_property
                .unwrap_or_else(|| "updated_at".to_string()),
            id_property,
            current_timestamp: initial_timestamp,
            composite_constituent,
        })
    }

    /// Initialize the incremental source
    pub async fn initialize(&mut self) -> anyhow::Result<()> {
        // Source is already initialized via constructor - nothing to do
        Ok(())
    }

    /// Get a stream of changes
    pub async fn get_changes(&mut self) -> anyhow::Result<Box<dyn ChangeStream>> {
        let cp = Neo4jCheckpoint::at(
            chrono::DateTime::from_timestamp_millis(self.current_timestamp)
                .unwrap_or_else(Utc::now),
        );
        self.get_changes_from(&cp).await
    }

    /// Get a stream starting from a full checkpoint (timestamp + tie-breaks).
    pub async fn get_changes_from(
        &mut self,
        from_checkpoint: &Neo4jCheckpoint,
    ) -> anyhow::Result<Box<dyn ChangeStream>> {
        Ok(Box::new(Neo4jChangeStream::new(
            self.graph.clone(),
            from_checkpoint,
            self.change_tracking_property.clone(),
            self.id_property.clone(),
            self.ctx.clone(),
            self.composite_constituent.clone(),
        )))
    }

    /// Get the current checkpoint
    pub fn get_checkpoint(&self) -> anyhow::Result<Neo4jCheckpoint> {
        let checkpoint_datetime =
            chrono::DateTime::from_timestamp_millis(self.current_timestamp)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", self.current_timestamp))?;
        Ok(Neo4jCheckpoint::at(checkpoint_datetime))
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
    /// Read-head timestamp used for the next keyset query (may be ahead of sunk).
    read_checkpoint: i64,
    /// Tie-break: last neo4j node id seen at [`Self::read_checkpoint`].
    after_node_id: i64,
    /// Tie-break: last neo4j relationship id seen at [`Self::read_checkpoint`].
    after_rel_id: i64,
    /// Sink-safe watermark (authoritative for [`ChangeStream::checkpoint`]).
    sunk_checkpoint: i64,
    /// Tie-break ids at [`Self::sunk_checkpoint`] (crash-resume keyset).
    sunk_after_node_id: i64,
    sunk_after_rel_id: i64,
    change_tracking_property: String,
    /// Property name to use as SurrealDB record ID
    id_property: String,
    /// Conversion context with timezone and JSON-to-object configuration
    ctx: Neo4jConversionContext,
    /// Buffer for batched changes (FIFO: nodes were appended before relations).
    change_buffer: VecDeque<IncrementalChange>,
    /// Whether we've finished reading all changes
    finished: bool,
    /// Optional composite database constituent for `USE` clause
    composite_constituent: Option<String>,
}

impl Neo4jChangeStream {
    pub fn new(
        graph: Graph,
        from_checkpoint: &Neo4jCheckpoint,
        change_tracking_property: String,
        id_property: String,
        ctx: Neo4jConversionContext,
        composite_constituent: Option<String>,
    ) -> Self {
        let from_ts = from_checkpoint.timestamp.timestamp_millis();
        Neo4jChangeStream {
            graph,
            read_checkpoint: from_ts,
            after_node_id: from_checkpoint.after_node_id,
            after_rel_id: from_checkpoint.after_rel_id,
            sunk_checkpoint: from_ts,
            sunk_after_node_id: from_checkpoint.after_node_id,
            sunk_after_rel_id: from_checkpoint.after_rel_id,
            change_tracking_property,
            id_property,
            ctx,
            change_buffer: VecDeque::new(),
            finished: false,
            composite_constituent,
        }
    }

    /// Fetch the next batch of changes from Neo4j
    async fn fetch_next_batch(&mut self) -> anyhow::Result<()> {
        if self.finished {
            return Ok(());
        }

        // Keyset on (timestamp, element id) so same-timestamp siblings are not
        // skipped when LIMIT splits a dense timestamp bucket.
        let checkpoint_datetime = chrono::DateTime::from_timestamp_millis(self.read_checkpoint)
            .unwrap_or_else(chrono::Utc::now);

        let checkpoint_str = checkpoint_datetime.to_rfc3339();

        let node_query = Query::new(with_use_clause(
            &format!(
                "MATCH (n)
             WHERE n.{tracking} > datetime($checkpoint)
                OR (n.{tracking} = datetime($checkpoint) AND id(n) > $after_id)
             RETURN n, id(n) as node_id, labels(n) as labels
             ORDER BY n.{tracking}, id(n)
             LIMIT 100",
                tracking = self.change_tracking_property,
            ),
            &self.composite_constituent,
        ))
        .param("checkpoint", checkpoint_str.clone())
        .param("after_id", self.after_node_id);

        let mut result = self.graph.execute(node_query).await?;
        let mut batch_changes = Vec::new();
        let mut max_checkpoint = self.read_checkpoint;
        let mut last_node_id_at_max = i64::MIN;
        let mut last_rel_id_at_max = i64::MIN;
        let mut record_id: Value;

        while let Some(row) = result.next().await? {
            let node: neo4rs::Node = row.get("n")?;
            let node_id: i64 = row.get("node_id")?;
            let labels: Vec<String> = row.get("labels")?;
            let label = labels.first().map(|s| s.as_str()).unwrap_or("");

            // Get the checkpoint value from the node (as milliseconds since epoch)
            // Neo4j datetime() values are extracted as chrono::DateTime
            let node_checkpoint = node
                .get::<chrono::DateTime<chrono::Utc>>(&self.change_tracking_property)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Node {} has invalid {} property (expected datetime): {}",
                        node_id,
                        self.change_tracking_property,
                        e
                    )
                })?
                .timestamp_millis();

            if node_checkpoint > max_checkpoint {
                max_checkpoint = node_checkpoint;
                last_node_id_at_max = node_id;
                last_rel_id_at_max = i64::MIN;
            } else if node_checkpoint == max_checkpoint {
                last_node_id_at_max = last_node_id_at_max.max(node_id);
            }

            // Convert node to universal data
            let mut fields: HashMap<String, Value> = HashMap::new();
            fields.insert("neo4j_id".to_string(), Value::Int64(node_id));
            record_id = Value::Int64(node_id);

            let universal_labels: Vec<Value> =
                labels.iter().map(|s| Value::Text(s.clone())).collect();
            fields.insert(
                "labels".to_string(),
                Value::Array {
                    elements: universal_labels,
                    element_type: Box::new(Type::Text),
                },
            );

            // Add all node properties with field renaming
            for key in node.keys() {
                if let Ok(value) = node.get::<neo4rs::BoltType>(key) {
                    // Determine label for JSON-to-object check
                    let should_parse_json = self.ctx.should_parse_json(label, key);

                    if let Ok(v) = crate::convert_neo4j_type_to_universal_value(
                        value,
                        &self.ctx.timezone,
                        should_parse_json,
                    ) {
                        // Rename id property field to avoid conflict with SurrealDB record ID
                        let field_name = if key == self.id_property {
                            // Extract ID from the Value
                            record_id = match &v {
                                Value::Int64(n) => Value::Int64(*n),
                                Value::Text(s) => {
                                    // Try to parse as integer first (common case: numeric strings)
                                    if let Ok(n) = s.parse::<i64>() {
                                        Value::Int64(n)
                                    } else {
                                        Value::Text(s.clone())
                                    }
                                }
                                _ => Value::Int64(node_id),
                            };
                            "neo4j_original_id".to_string()
                        } else {
                            key.to_string()
                        };
                        fields.insert(field_name, v);
                    }
                }
            }

            let table = labels
                .first()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "node".to_string());

            batch_changes.push(IncrementalChange::Node(Row::new(
                table,
                node_id as u64,
                record_id,
                fields,
            )));
        }

        // Also query for relationship changes (same keyset shape).
        let rel_query = Query::new(with_use_clause(
            &format!(
                "MATCH (a)-[r]->(b)
             WHERE r.{tracking} > datetime($checkpoint)
                OR (r.{tracking} = datetime($checkpoint) AND id(r) > $after_id)
             RETURN r, id(r) as rel_id, type(r) as rel_type,
                    id(a) as start_id, id(b) as end_id,
                    labels(a) as start_labels, labels(b) as end_labels,
                    a.{id_prop} as start_prop_id, b.{id_prop} as end_prop_id,
                    'update' as operation
             ORDER BY r.{tracking}, id(r)
             LIMIT 100",
                tracking = self.change_tracking_property,
                id_prop = self.id_property,
            ),
            &self.composite_constituent,
        ))
        .param("checkpoint", checkpoint_str)
        .param("after_id", self.after_rel_id);

        let mut rel_result = self.graph.execute(rel_query).await?;

        while let Some(row) = rel_result.next().await? {
            // Convert relationship to universal data
            let r =
                crate::row_to_relation(&row, None, Some(self.change_tracking_property.clone()))?;

            let rel_id: i64 = row.get("rel_id")?;
            if r.updated_at > max_checkpoint {
                max_checkpoint = r.updated_at;
                last_rel_id_at_max = rel_id;
                last_node_id_at_max = i64::MIN;
            } else if r.updated_at == max_checkpoint {
                last_rel_id_at_max = last_rel_id_at_max.max(rel_id);
            }

            batch_changes.push(IncrementalChange::Relation(Box::new(
                r.to_universal_relation(&self.ctx)?,
            )));
        }

        if batch_changes.is_empty() {
            self.finished = true;
        } else {
            // Advance read-head only; sunk watermark waits for commit_sunk.
            if max_checkpoint > self.read_checkpoint {
                self.read_checkpoint = max_checkpoint;
                self.after_node_id = last_node_id_at_max;
                self.after_rel_id = last_rel_id_at_max;
            } else if max_checkpoint == self.read_checkpoint {
                self.after_node_id = self.after_node_id.max(last_node_id_at_max);
                self.after_rel_id = self.after_rel_id.max(last_rel_id_at_max);
            }
            // FIFO dequeue: nodes were pushed before relations so endpoints
            // emit before edges (matches pre-port apply_incremental_changes).
            self.change_buffer = VecDeque::from(batch_changes);
        }

        Ok(())
    }
}

#[async_trait]
impl ChangeStream for Neo4jChangeStream {
    async fn next(&mut self) -> Option<anyhow::Result<IncrementalChange>> {
        // If buffer is empty, fetch next batch
        if self.change_buffer.is_empty() && !self.finished {
            if let Err(e) = self.fetch_next_batch().await {
                return Some(Err(e));
            }
        }

        // FIFO: nodes (appended first) before relations within a fetch batch.
        self.change_buffer.pop_front().map(Ok)
    }

    fn checkpoint(&self) -> Option<Neo4jCheckpoint> {
        let checkpoint_datetime =
            chrono::DateTime::from_timestamp_millis(self.sunk_checkpoint).unwrap_or_else(Utc::now);
        Some(Neo4jCheckpoint {
            timestamp: checkpoint_datetime,
            after_node_id: self.sunk_after_node_id,
            after_rel_id: self.sunk_after_rel_id,
        })
    }

    fn commit_sunk(&mut self, position: Neo4jApplyPos) {
        if position.timestamp_millis > self.sunk_checkpoint {
            self.sunk_checkpoint = position.timestamp_millis;
            self.sunk_after_node_id = position.after_node_id;
            self.sunk_after_rel_id = position.after_rel_id;
        } else if position.timestamp_millis == self.sunk_checkpoint {
            self.sunk_after_node_id = self.sunk_after_node_id.max(position.after_node_id);
            self.sunk_after_rel_id = self.sunk_after_rel_id.max(position.after_rel_id);
        }
    }
}

/// Options for the Neo4j timestamp-tracking replication tail.
#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    /// Wall-clock stop for the stream phase.
    pub deadline: chrono::DateTime<chrono::Utc>,
    /// Optional timestamp stop bound.
    pub until: Option<Neo4jCheckpoint>,
    /// When true, drain changes without applying them.
    pub dry_run: bool,
    /// Batch hint used only for dry-run counting logs (apply uses ApplyOpts).
    pub batch_size: usize,
}

impl ReplicationTailOptions {
    /// Build options from the historical `run_incremental_sync` arguments.
    pub fn stream(
        deadline: chrono::DateTime<chrono::Utc>,
        until: Option<Neo4jCheckpoint>,
        dry_run: bool,
        batch_size: usize,
    ) -> Self {
        Self {
            deadline,
            until,
            dry_run,
            batch_size,
        }
    }
}

/// Dry-run helper that counts incremental changes without writing to the sink.
///
/// Live sync must use [`run_incremental_sync_with_transforms`].
/// Calling with `dry_run = false` returns an error — this function never
/// performs live `write_universal_*` bypasses.
pub async fn apply_incremental_changes<S: SurrealSink>(
    _surreal: &S,
    changes: Vec<IncrementalChange>,
    dry_run: bool,
) -> anyhow::Result<usize> {
    let mut nodes = 0usize;
    let mut relations = 0usize;

    for change in changes {
        match change {
            IncrementalChange::Node(_) => nodes += 1,
            IncrementalChange::Relation(_) => relations += 1,
        }
    }

    let total_count = nodes + relations;

    if !dry_run {
        anyhow::bail!(
            "apply_incremental_changes refuses live writes; use \
             run_incremental_sync_with_transforms (dry_run callers pass true)"
        );
    }

    tracing::info!(
        "DRY RUN: Would apply {} nodes and {} relations",
        nodes,
        relations
    );
    Ok(total_count)
}

fn incremental_change_to_positioned(
    change: IncrementalChange,
    tracking_property: &str,
    prev: Neo4jApplyPos,
) -> (PositionedEvent<Neo4jApplyPos>, Neo4jApplyPos) {
    match change {
        IncrementalChange::Node(row) => {
            let ts = tracking_millis_from_fields(&row.fields, tracking_property)
                .unwrap_or(prev.timestamp_millis);
            let node_id = match row.fields.get("neo4j_id") {
                Some(Value::Int64(id)) => *id,
                _ => row.index as i64,
            };
            let position = if ts > prev.timestamp_millis {
                Neo4jApplyPos {
                    timestamp_millis: ts,
                    after_node_id: node_id,
                    after_rel_id: i64::MIN,
                }
            } else {
                Neo4jApplyPos {
                    timestamp_millis: ts,
                    after_node_id: node_id,
                    after_rel_id: prev.after_rel_id,
                }
            };
            let uc = Change::update(row.table, row.id, row.fields);
            (PositionedEvent::change(uc, position), position)
        }
        IncrementalChange::Relation(rel) => {
            let ts = tracking_millis_from_fields(&rel.data, tracking_property)
                .unwrap_or(prev.timestamp_millis);
            let rel_id = match &rel.id {
                Value::Int64(id) => *id,
                _ => prev.after_rel_id,
            };
            let position = if ts > prev.timestamp_millis {
                Neo4jApplyPos {
                    timestamp_millis: ts,
                    after_node_id: i64::MIN,
                    after_rel_id: rel_id,
                }
            } else {
                Neo4jApplyPos {
                    timestamp_millis: ts,
                    after_node_id: prev.after_node_id,
                    after_rel_id: rel_id,
                }
            };
            let rc = RelationChange::update(*rel);
            (PositionedEvent::relation_change(rc, position), position)
        }
    }
}

/// Run incremental sync from Neo4j to SurrealDB (identity transforms).
///
/// Timestamp-based change tracking cannot detect deletes; deleted Neo4j
/// entities remain in SurrealDB until a full resync/cleanup.
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    from_checkpoint: Neo4jCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<Neo4jCheckpoint>,
) -> anyhow::Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        surreal,
        from_opts,
        sync_opts.clone(),
        from_checkpoint,
        ReplicationTailOptions::stream(
            deadline,
            target_checkpoint,
            sync_opts.dry_run,
            sync_opts.batch_size,
        ),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Incremental sync via `SourceDriver` + `run_source_runtime` (shared apply window).
///
/// Nodes and relationships are emitted as mixed [`PositionedEvent`]s (row
/// changes and relation changes) with the tracking-property timestamp as
/// position. Deletes are not invented — Neo4j timestamp tracking cannot see them.
pub async fn run_incremental_sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    from_checkpoint: Neo4jCheckpoint,
    options: ReplicationTailOptions,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> anyhow::Result<()> {
    use surreal_sync_core::Checkpoint;

    tracing::info!(
        "Starting Neo4j incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    if pipeline.is_identity() {
        tracing::debug!("Incremental sync using identity transform pipeline");
    } else {
        tracing::info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            batch_size = apply_opts.batch_size,
            "Incremental sync using transform pipeline"
        );
    }

    tracing::info!("Using timestamp-based change tracking for incremental sync");
    let initial_timestamp = from_checkpoint.timestamp.timestamp_millis();
    let graph = crate::new_neo4j_client(&from_opts).await?;

    let mut source = Neo4jIncrementalSource::new(
        graph,
        from_opts.neo4j_timezone.clone(),
        from_opts.neo4j_json_properties.clone(),
        Some(from_opts.change_tracking_property.clone()),
        from_opts.id_property.clone(),
        initial_timestamp,
        from_opts.composite_constituent.clone(),
    )?;

    let mut stream = source.get_changes_from(&from_checkpoint).await?;

    // Preserve dry-run: drain and count without transform/sink apply.
    if options.dry_run || sync_opts.dry_run {
        let mut total_applied = 0;
        let mut batch = Vec::new();
        let tracking = from_opts.change_tracking_property.clone();
        while let Some(change_result) = stream.next().await {
            let change = change_result?;
            if chrono::Utc::now() >= options.deadline {
                tracing::info!(
                    "Reached deadline: {}, stopping incremental sync",
                    options.deadline
                );
                break;
            }
            let position = match &change {
                IncrementalChange::Node(row) => {
                    let ts = tracking_millis_from_fields(&row.fields, &tracking).unwrap_or(0);
                    let node_id = match row.fields.get("neo4j_id") {
                        Some(Value::Int64(id)) => *id,
                        _ => row.index as i64,
                    };
                    Neo4jApplyPos {
                        timestamp_millis: ts,
                        after_node_id: node_id,
                        after_rel_id: i64::MIN,
                    }
                }
                IncrementalChange::Relation(rel) => {
                    let ts = tracking_millis_from_fields(&rel.data, &tracking).unwrap_or(0);
                    let rel_id = match &rel.id {
                        Value::Int64(id) => *id,
                        _ => i64::MIN,
                    };
                    Neo4jApplyPos {
                        timestamp_millis: ts,
                        after_node_id: i64::MIN,
                        after_rel_id: rel_id,
                    }
                }
            };
            stream.commit_sunk(position);
            if let Some(ref target) = options.until {
                if position.timestamp_millis >= target.timestamp.timestamp_millis() {
                    use surreal_sync_core::Checkpoint;
                    tracing::info!(
                        "Reached target checkpoint: {}, stopping incremental sync",
                        target.to_cli_string()
                    );
                    break;
                }
            }
            batch.push(change);
            if batch.len() >= sync_opts.batch_size.max(options.batch_size).max(1) {
                let applied =
                    apply_incremental_changes(surreal, std::mem::take(&mut batch), true).await?;
                total_applied += applied;
            }
        }
        if !batch.is_empty() {
            total_applied += apply_incremental_changes(surreal, batch, true).await?;
        }
        tracing::info!(
            "Incremental sync dry-run completed: {} total changes",
            total_applied
        );
        source.cleanup().await?;
        return Ok(());
    }

    let mut driver = Neo4jSourceDriver {
        stream,
        options: &options,
        tracking_property: from_opts.change_tracking_property.clone(),
        until_reached: false,
        finished: false,
        total_changes: 0,
        last_position: Neo4jApplyPos {
            timestamp_millis: from_checkpoint.timestamp.timestamp_millis(),
            after_node_id: from_checkpoint.after_node_id,
            after_rel_id: from_checkpoint.after_rel_id,
        },
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = surreal_sync_runtime::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Deadline) => {
            tracing::info!("Reached deadline, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Until) => {
            tracing::info!("Reached target checkpoint, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Finished) => {
            tracing::info!("Neo4j source caught up (no more timestamped changes)");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Cancelled) => {
            tracing::info!("Cancellation requested, stopping incremental sync");
        }
    }

    tracing::info!(
        "Incremental sync completed: {} total changes applied",
        driver.total_changes
    );

    drop(driver);
    source.cleanup().await?;
    Ok(())
}

/// Neo4j timestamp CDC driver emitting mixed node/relation [`PositionedEvent`]s.
struct Neo4jSourceDriver<'a> {
    stream: Box<dyn ChangeStream>,
    options: &'a ReplicationTailOptions,
    tracking_property: String,
    until_reached: bool,
    finished: bool,
    total_changes: u64,
    last_position: Neo4jApplyPos,
}

#[async_trait::async_trait]
impl SourceDriver for Neo4jSourceDriver<'_> {
    type Position = Neo4jApplyPos;

    async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() || self.finished {
            return Ok(Vec::new());
        }

        match self.stream.next().await {
            None => {
                self.finished = true;
                Ok(Vec::new())
            }
            Some(Err(e)) => Err(e),
            Some(Ok(change)) => {
                let (pe, position) = incremental_change_to_positioned(
                    change,
                    &self.tracking_property,
                    self.last_position,
                );
                self.last_position = position;

                if let Some(ref target) = self.options.until {
                    if self.last_position.timestamp_millis >= target.timestamp.timestamp_millis() {
                        use surreal_sync_core::Checkpoint;
                        tracing::info!(
                            "Reached target checkpoint: {}, stopping after this event",
                            target.to_cli_string()
                        );
                        self.until_reached = true;
                    }
                }

                Ok(vec![pe])
            }
        }
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
        self.stream.commit_sunk(position);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::AdvanceOnly
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if chrono::Utc::now() >= self.options.deadline {
            return Some(StopReason::Deadline);
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        None
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.total_changes = self.total_changes.saturating_add(count);
        if self.total_changes.is_multiple_of(100) {
            tracing::info!("Processed {} changes", self.total_changes);
        }
    }
}

fn tracking_millis_from_fields(
    fields: &HashMap<String, Value>,
    tracking_property: &str,
) -> Option<i64> {
    match fields.get(tracking_property) {
        Some(Value::LocalDateTime(ts)) | Some(Value::ZonedDateTime(ts)) => {
            Some(ts.timestamp_millis())
        }
        Some(Value::Int64(ms)) => Some(*ms),
        _ => None,
    }
}
