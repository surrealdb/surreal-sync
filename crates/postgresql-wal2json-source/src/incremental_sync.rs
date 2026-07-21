//! PostgreSQL wal2json incremental sync via SourceDriver.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoint::{CheckpointID, CheckpointStore, Surreal2Store};
use chrono::{DateTime, Utc};
use surreal_sink::SurrealSink;
use sync_core::{
    classify_table, DatabaseSchema, TableKind, UniversalChange, UniversalChangeOp,
    UniversalRelationChange,
};
use sync_transform::{
    ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver, SourceRuntimeOpts,
    StopReason,
};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::checkpoint::PostgreSQLLogicalCheckpoint;
use crate::full_sync::SourceOpts;
use crate::logical_replication::{ChangeAtLsn, Slot};
use crate::watermark_source::Lsn;

/// Default sleep when a wal2json peek returns no changes.
pub const DEFAULT_IDLE_SLEEP: Duration = Duration::from_millis(100);

/// Options for the wal2json replication tail (deadline / until only — no cancel
/// or CatchUpProgress store; those were not present on this source before).
#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    /// Wall-clock stop for the stream phase.
    pub deadline: DateTime<Utc>,
    /// Optional exact LSN stop bound.
    pub until: Option<PostgreSQLLogicalCheckpoint>,
    /// Sleep when a poll returns no events before retrying.
    pub idle_sleep: Duration,
}

impl ReplicationTailOptions {
    /// Build options from the historical `run_incremental_sync` arguments.
    pub fn stream(deadline: DateTime<Utc>, until: Option<PostgreSQLLogicalCheckpoint>) -> Self {
        Self {
            deadline,
            until,
            idle_sleep: DEFAULT_IDLE_SLEEP,
        }
    }
}

/// Read t1 checkpoint from SurrealDB
///
/// Reads the full_sync_start checkpoint from the specified table.
#[allow(dead_code)]
pub async fn read_t1_checkpoint_from_surrealdb(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
) -> Result<PostgreSQLLogicalCheckpoint> {
    let store = Surreal2Store::new(surreal.clone(), table_name.to_string());

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let stored = store.read_checkpoint(&id).await?.ok_or_else(|| {
        anyhow::anyhow!("No t1 checkpoint found in SurrealDB table '{table_name}'")
    })?;

    let checkpoint: PostgreSQLLogicalCheckpoint = serde_json::from_str(&stored.checkpoint_data)?;
    Ok(checkpoint)
}

/// Run incremental sync from PostgreSQL to SurrealDB (identity transforms).
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: PostgreSQLLogicalCheckpoint,
    deadline: DateTime<Utc>,
    to_checkpoint: Option<PostgreSQLLogicalCheckpoint>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        surreal,
        from_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(deadline, to_checkpoint),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Incremental sync through the transform apply framework.
///
/// Peek → convert (with FK enrichment / relation routing) →
/// [`sync_transform::run_source_runtime_with`] → advance slot only after sink
/// success. Peeks may continue under window capacity: `pg_logical_slot_peek_changes`
/// is non-consuming, so the driver skips an already-emitted prefix
/// (`returned_since_advance`) and only `advance`s once every emitted event is sunk.
pub async fn run_incremental_sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: PostgreSQLLogicalCheckpoint,
    options: ReplicationTailOptions,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!(
        "Starting PostgreSQL logical replication incremental sync from LSN: {}",
        from_checkpoint.lsn
    );

    if let Some(ref target) = options.until {
        info!("Target LSN: {}", target.lsn);
    }
    let duration_until_deadline = options.deadline.signed_duration_since(Utc::now());
    info!(
        "Deadline in {} seconds",
        duration_until_deadline.num_seconds()
    );

    if pipeline.is_identity() {
        debug!("Incremental sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            batch_size = apply_opts.batch_size,
            "Incremental sync using transform pipeline"
        );
    }

    let (client, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {e}");
        }
    });

    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(&client).await?;
    let relation_table_overrides = from_opts.relation_tables.clone();

    let pg_client = crate::Client::new(client, from_opts.tables.clone());
    pg_client.create_slot(&from_opts.slot_name).await?;
    let slot = pg_client
        .start_replication(Some(&from_opts.slot_name))
        .await?;

    info!(
        "Logical replication started on slot: {}",
        from_opts.slot_name
    );

    if !from_checkpoint.lsn.is_empty() && from_checkpoint.lsn != "0/0" {
        info!("Advancing slot to starting LSN: {}", from_checkpoint.lsn);
        slot.advance(&from_checkpoint.lsn).await?;
    }

    let mut driver = Wal2JsonSourceDriver {
        slot,
        db_schema,
        relation_table_overrides,
        options: &options,
        until_reached: false,
        returned_since_advance: 0,
        emitted_since_advance: 0,
        sunk_since_advance: 0,
        latest_nextlsn: None,
        total_changes: 0,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = sync_transform::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        sync_transform::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Deadline reached, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target checkpoint, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("PostgreSQL wal2json source finished");
        }
    }

    info!(
        "PostgreSQL logical replication incremental sync completed: {} changes sunk",
        driver.total_changes
    );
    Ok(())
}

/// wal2json CDC driver for [`sync_transform::run_source_runtime_with`].
///
/// Slot `advance` is deferred until every event emitted since the last advance
/// has been sunk. Peeks are non-consuming: the driver may peek again while prior
/// work is still in the apply window, skipping the stable already-returned
/// prefix via [`Self::returned_since_advance`] (same pattern as the interleaved
/// watermark source).
struct Wal2JsonSourceDriver<'a> {
    slot: Slot,
    db_schema: DatabaseSchema,
    relation_table_overrides: Vec<String>,
    options: &'a ReplicationTailOptions,
    until_reached: bool,
    /// Changes already converted/emitted from the current unadvanced peek prefix.
    returned_since_advance: usize,
    /// Positioned events emitted since last slot advance (excludes filtered-out).
    emitted_since_advance: u64,
    /// Events noted sunk since last slot advance.
    sunk_since_advance: u64,
    /// Latest commit `nextlsn` observed on peeks since last advance.
    latest_nextlsn: Option<Lsn>,
    total_changes: u64,
}

impl Wal2JsonSourceDriver<'_> {
    fn convert_actions(
        &self,
        changes: &[ChangeAtLsn],
        nextlsn: Lsn,
    ) -> Result<Vec<PositionedEvent<Lsn>>> {
        let mut out = Vec::new();
        for change in changes {
            let (row, op) = match &change.action {
                crate::Action::Insert(row) => {
                    debug!(
                        "INSERT: table={}, primary_key={:?}",
                        row.table, row.primary_key
                    );
                    (row, UniversalChangeOp::Create)
                }
                crate::Action::Update(row) => {
                    debug!(
                        "UPDATE: table={}, primary_key={:?}",
                        row.table, row.primary_key
                    );
                    (row, UniversalChangeOp::Update)
                }
                crate::Action::Delete(row) => {
                    debug!(
                        "DELETE: table={}, primary_key={:?}",
                        row.table, row.primary_key
                    );
                    (row, UniversalChangeOp::Delete)
                }
                crate::Action::Begin { .. } | crate::Action::Commit { .. } => continue,
            };
            out.push(action_to_positioned_event(
                row,
                op,
                nextlsn,
                &self.db_schema,
                &self.relation_table_overrides,
            )?);
        }
        Ok(out)
    }

    fn maybe_mark_until(&mut self, nextlsn: &Lsn) {
        if let Some(ref target) = self.options.until {
            if compare_lsn(&nextlsn.to_pg_string(), &target.lsn) >= 0 {
                info!(
                    "Reached target LSN {} (current: {})",
                    target.lsn,
                    nextlsn.to_pg_string()
                );
                self.until_reached = true;
            }
        }
    }

    async fn try_advance_if_drained(&mut self) -> Result<()> {
        if self.sunk_since_advance < self.emitted_since_advance {
            return Ok(());
        }
        let Some(nextlsn) = self.latest_nextlsn else {
            return Ok(());
        };

        // Do not advance while the current peek still has unsurfaced changes;
        // advancing would drop them (watermark_source commit_reconciled rule).
        let (changes, _) = self.slot.peek_with_positions().await?;
        if self.returned_since_advance < changes.len() {
            debug!(
                "Deferring wal2json slot advance at {}: {}/{} peeked changes surfaced",
                nextlsn,
                self.returned_since_advance,
                changes.len()
            );
            return Ok(());
        }

        self.slot.advance(&nextlsn.to_pg_string()).await?;
        self.returned_since_advance = 0;
        self.emitted_since_advance = 0;
        self.sunk_since_advance = 0;
        self.latest_nextlsn = None;
        self.maybe_mark_until(&nextlsn);
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceDriver for Wal2JsonSourceDriver<'_> {
    type Position = Lsn;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() {
            return Ok(Vec::new());
        }

        match self.slot.peek_with_positions().await {
            Ok((changes, nextlsn_str)) => {
                if changes.is_empty() {
                    // Filtered-empty peek with a commit nextlsn: advance past it
                    // when nothing is outstanding from a prior emit.
                    if !nextlsn_str.is_empty()
                        && self.emitted_since_advance == 0
                        && self.sunk_since_advance == 0
                        && self.returned_since_advance == 0
                    {
                        let nextlsn = Lsn::parse(&nextlsn_str)?;
                        self.slot.advance(&nextlsn.to_pg_string()).await?;
                        self.maybe_mark_until(&nextlsn);
                        return Ok(Vec::new());
                    }
                    if let Some(ref target) = self.options.until {
                        if !nextlsn_str.is_empty() && compare_lsn(&nextlsn_str, &target.lsn) >= 0 {
                            info!(
                                "Reached target LSN {} (current: {})",
                                target.lsn, nextlsn_str
                            );
                            self.until_reached = true;
                            return Ok(Vec::new());
                        }
                    }
                    tokio::time::sleep(self.options.idle_sleep).await;
                    return Ok(Vec::new());
                }

                let nextlsn = if nextlsn_str.is_empty() {
                    anyhow::bail!("wal2json peek returned changes without a commit nextlsn");
                } else {
                    Lsn::parse(&nextlsn_str)?
                };

                // Stable prefix skip: peek re-returns unadvanced changes.
                let skip = self.returned_since_advance.min(changes.len());
                let new_changes = &changes[skip..];
                if new_changes.is_empty() {
                    // Fully surfaced this peek; allow sink-gated advance, then idle.
                    self.latest_nextlsn = Some(nextlsn);
                    self.try_advance_if_drained().await?;
                    tokio::time::sleep(self.options.idle_sleep).await;
                    return Ok(Vec::new());
                }

                let events = self.convert_actions(new_changes, nextlsn)?;
                self.returned_since_advance = self
                    .returned_since_advance
                    .saturating_add(new_changes.len());
                self.emitted_since_advance = self
                    .emitted_since_advance
                    .saturating_add(events.len() as u64);
                self.latest_nextlsn = Some(nextlsn);

                if events.is_empty() {
                    // New peek prefix was Begin/Commit-only / fully filtered —
                    // try advancing if prior emitted work is already sunk.
                    self.try_advance_if_drained().await?;
                }

                Ok(events)
            }
            Err(e) => {
                warn!("Error peeking changes: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(Vec::new())
            }
        }
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        let Some(nextlsn) = self.latest_nextlsn else {
            return Ok(());
        };
        // Runtime commits the batch's last_position; only attempt advance when
        // it matches the current peek watermark (or we have fully sunk).
        if position != nextlsn && self.sunk_since_advance < self.emitted_since_advance {
            return Ok(());
        }
        self.try_advance_if_drained().await
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        // Incremental wal2json does not persist CatchUpProgress today; slot
        // advance in `advance_watermark` is the durability mechanism.
        CheckpointPolicy::AdvanceOnly
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if Utc::now() >= self.options.deadline {
            return Some(StopReason::Deadline);
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        if let Some(ref target) = self.options.until {
            if let Some(nextlsn) = self.latest_nextlsn {
                if self.emitted_since_advance == 0
                    && self.sunk_since_advance == 0
                    && compare_lsn(&nextlsn.to_pg_string(), &target.lsn) >= 0
                {
                    return Some(StopReason::Until);
                }
            }
        }
        None
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.sunk_since_advance = self.sunk_since_advance.saturating_add(count);
        self.total_changes = self.total_changes.saturating_add(count);
    }
}

/// Convert a Row to UniversalChange
fn row_to_universal_change(row: &crate::Row, op: UniversalChangeOp) -> UniversalChange {
    let data = if op == UniversalChangeOp::Delete {
        None
    } else {
        Some(row.columns.clone())
    };
    UniversalChange::new(op, row.table.clone(), row.primary_key.clone(), data)
}

/// FK enrichment and relation routing before the event enters the apply window.
fn action_to_positioned_event(
    row: &crate::Row,
    op: UniversalChangeOp,
    position: Lsn,
    db_schema: &DatabaseSchema,
    relation_table_overrides: &[String],
) -> Result<PositionedEvent<Lsn>> {
    let table_def = db_schema.get_table(&row.table);
    let table_kind = table_def.map(|td| classify_table(td, relation_table_overrides));

    match table_kind {
        Some(TableKind::Relation {
            ref in_fk,
            ref out_fk,
        }) => {
            let data = if op == UniversalChangeOp::Delete {
                std::collections::HashMap::new()
            } else {
                row.columns.clone()
            };
            let relation = surreal_sync_postgresql::fk_transform::build_relation_from_change(
                &row.table,
                row.primary_key.clone(),
                data,
                in_fk,
                out_fk,
            );
            let rel_change = UniversalRelationChange::new(op, relation);
            Ok(PositionedEvent::relation_change(rel_change, position))
        }
        _ => {
            let mut change = row_to_universal_change(row, op);
            if let (Some(td), Some(ref mut data)) = (table_def, change.data.as_mut()) {
                surreal_sync_postgresql::fk_transform::transform_fk_values(data, td);
            }
            Ok(PositionedEvent::change(change, position))
        }
    }
}

/// Compare two LSN strings
/// Returns: -1 if lsn1 < lsn2, 0 if equal, 1 if lsn1 > lsn2
fn compare_lsn(lsn1: &str, lsn2: &str) -> i32 {
    // LSN format: "segment/offset" (e.g., "0/1949850")
    let parse_lsn = |lsn: &str| -> Option<(u64, u64)> {
        let parts: Vec<&str> = lsn.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let segment = u64::from_str_radix(parts[0], 16).ok()?;
        let offset = u64::from_str_radix(parts[1], 16).ok()?;
        Some((segment, offset))
    };

    match (parse_lsn(lsn1), parse_lsn(lsn2)) {
        (Some((s1, o1)), Some((s2, o2))) => {
            if s1 < s2 || (s1 == s2 && o1 < o2) {
                -1
            } else if s1 == s2 && o1 == o2 {
                0
            } else {
                1
            }
        }
        _ => {
            // Fallback to string comparison
            lsn1.cmp(lsn2) as i32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_lsn_equal() {
        assert_eq!(compare_lsn("0/1949850", "0/1949850"), 0);
    }

    #[test]
    fn test_compare_lsn_less_than() {
        assert_eq!(compare_lsn("0/100", "0/200"), -1);
        assert_eq!(compare_lsn("0/FF", "1/0"), -1);
    }

    #[test]
    fn test_compare_lsn_greater_than() {
        assert_eq!(compare_lsn("0/200", "0/100"), 1);
        assert_eq!(compare_lsn("1/0", "0/FF"), 1);
    }
}
