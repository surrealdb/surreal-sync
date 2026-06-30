//! Watermark snapshot+stream backend for the PostgreSQL trigger source.
//!
//! This implements the generic [`WatermarkSource`] contract on top of the
//! trigger-based audit table (`surreal_sync_changes`). The stream position is
//! the audit table's `BIGSERIAL` `sequence_id`, which is already totally
//! ordered.
//!
//! Watermarks are written as rows into a dedicated signal table that carries
//! its own change-capture trigger, so each watermark INSERT receives a real
//! `sequence_id` and flows through the very same ordered audit stream the
//! framework consumes. Consumed audit rows are pruned as the stream is
//! applied, keeping the audit table's retention bounded.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use surreal_sink::SurrealSink;
use sync_core::{DatabaseSchema, UniversalChange, UniversalRow, UniversalValue};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use uuid::Uuid;

use surreal_sync_snapshot_stream::{
    run_snapshot_stream, PkTuple, SnapshotCheckpointer, SnapshotSignal, SnapshotStreamConfig,
    StreamEvent, TableSpec, WatermarkKind, WatermarkSource,
};

use crate::incremental_sync::{ChangeStream, IncrementalSource, PostgresIncrementalSource};
use crate::SourceOpts;

/// The trigger source's audit table that captures every tracked change.
const AUDIT_TABLE: &str = "surreal_sync_changes";

/// The dedicated signal table watermark rows are written into.
pub(crate) const SIGNAL_TABLE: &str = "surreal_sync_signal";

/// `kind` value identifying an ad-hoc `execute-snapshot` request row (as
/// opposed to a `low`/`high` watermark row).
const EXECUTE_SNAPSHOT_KIND: &str = "execute-snapshot";

/// SQL that creates the signal table. The same statement is used by the
/// watermark source and the ad-hoc `snapshot` command so the schema is
/// identical regardless of which runs first.
///
/// The table holds two unambiguous kinds of rows, distinguished by `kind`:
/// `low`/`high` watermark rows (their inserts flow through the change stream),
/// and `execute-snapshot` request rows (polled directly via `read_signals`).
pub(crate) fn create_signal_table_sql() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {SIGNAL_TABLE} (\
            id UUID PRIMARY KEY, \
            kind TEXT NOT NULL, \
            tables TEXT, \
            consumed BOOLEAN NOT NULL DEFAULT FALSE)"
    )
}

/// The current low/high watermark UUIDs, so only the active watermark pair is
/// surfaced from the change stream; all other signal-table rows (stale
/// watermarks, `execute-snapshot` requests, consumed-flag updates) are consumed
/// silently and never applied to the sink.
#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

/// A [`WatermarkSource`] backed by the PostgreSQL trigger audit table.
///
/// The position type is the audit `sequence_id` (`i64`); `Ord` over `i64`
/// already gives the stream ordering the framework needs.
pub struct PostgresTriggerWatermarkSource {
    /// Connection used for chunk reads, watermark writes, position queries, and
    /// audit pruning. The change stream owns a clone of the same underlying
    /// connection; the framework only ever touches one at a time.
    client: Arc<Mutex<Client>>,
    /// The ordered audit change stream (yields `sequence_id` + change).
    stream: Box<dyn ChangeStream>,
    /// User tables to snapshot, each with its ordered primary key columns.
    tables: Vec<TableSpec>,
    /// Schema (with foreign keys) used for keyset chunk reads.
    schema: DatabaseSchema,
    /// Highest `sequence_id` actually yielded by (and thus applied from) the
    /// stream. Pruning never advances past this, so unconsumed audit rows are
    /// never dropped.
    last_consumed: i64,
    /// The current low/high watermark UUIDs (see [`WatermarkIds`]).
    watermarks: std::sync::Mutex<WatermarkIds>,
}

impl PostgresTriggerWatermarkSource {
    /// Prepare the source: create the signal table, set up change-capture
    /// triggers on the user tables and the signal table, collect schema, and
    /// open the audit change stream.
    pub async fn setup(from_opts: &SourceOpts) -> Result<Self> {
        let client = surreal_sync_postgresql::new_postgresql_client(&from_opts.source_uri).await?;

        // Create the signal table. Its own change-capture trigger is created by
        // `setup_tracking` below (because we include it in the tracked tables),
        // so every watermark INSERT lands in the audit stream with a real
        // sequence_id.
        {
            let c = client.lock().await;
            c.simple_query(&create_signal_table_sql()).await?;
        }

        // Enumerate user tables. `get_user_tables` already excludes every
        // `surreal_sync_%` table, so both the audit table and the signal table
        // are left out of the snapshot set.
        let database = from_opts.source_database.as_deref().unwrap_or("public");
        let user_tables = {
            let c = client.lock().await;
            surreal_sync_postgresql::get_user_tables(&c, database).await?
        };

        // Resolve ordered primary key columns for each snapshot table.
        let mut tables = Vec::with_capacity(user_tables.len());
        {
            let c = client.lock().await;
            for table in &user_tables {
                let pk_columns = surreal_sync_postgresql::get_primary_key_columns(&c, table).await?;
                if pk_columns.is_empty() {
                    anyhow::bail!(
                        "Table '{table}' has no primary key; the snapshot-stream strategy requires \
                         a primary key on every table. Re-run with --strategy bulk to copy this \
                         source without watermark snapshots."
                    );
                }
                tables.push(TableSpec::new(table.clone(), pk_columns));
            }
        }

        // Collect schema (with FK info) used by the keyset chunk reader so chunk
        // rows match the bulk full-sync conversion.
        let schema = {
            let c = client.lock().await;
            surreal_sync_postgresql::collect_database_schema_with_fks(&c).await?
        };

        // Set up triggers on the user tables and the signal table. This creates
        // the audit table and records each table's PK columns, which the change
        // stream needs to convert audit rows (including watermark rows).
        let mut tracking_tables = user_tables;
        tracking_tables.push(SIGNAL_TABLE.to_string());
        let mut source = PostgresIncrementalSource::new(client.clone(), 0);
        source.setup_tracking(tracking_tables).await?;
        source.initialize().await?;
        let stream = source.get_changes().await?;

        Ok(Self {
            client,
            stream,
            tables,
            schema,
            last_consumed: 0,
            watermarks: std::sync::Mutex::new(WatermarkIds::default()),
        })
    }

    /// Read the current stream position: the highest `sequence_id` ever
    /// assigned by the audit table's `BIGSERIAL`.
    ///
    /// This reads the sequence's `last_value` rather than `MAX(sequence_id)`,
    /// because pruning consumed rows removes the row carrying the maximum id
    /// while the sequence itself keeps climbing. Using `MAX` would make the
    /// position regress (even back to 0) after a prune, which would be wrong
    /// for the handoff. Returns 0 before the sequence has ever advanced.
    async fn query_stream_position(&self) -> Result<i64> {
        let client = self.client.lock().await;
        let row = client
            .query_one(
                &format!(
                    "SELECT COALESCE(\
                        pg_sequence_last_value(pg_get_serial_sequence('{AUDIT_TABLE}', 'sequence_id')::regclass), \
                        0)"
                ),
                &[],
            )
            .await?;
        Ok(row.get(0))
    }
}

/// Build a primary key tuple from a change's record id.
///
/// Composite keys arrive as an [`UniversalValue::Array`]; single keys (and the
/// UUID-keyed watermark rows) arrive as a scalar. Either way the resulting
/// [`PkTuple`] matches the buffer keys produced from snapshot rows, and a
/// single-UUID tuple is recognized by the framework as a watermark.
fn pk_from_change(change: &UniversalChange) -> PkTuple {
    match &change.id {
        UniversalValue::Array { elements, .. } => PkTuple::new(elements.clone()),
        other => PkTuple::new(vec![other.clone()]),
    }
}

#[async_trait]
impl WatermarkSource for PostgresTriggerWatermarkSource {
    type Position = i64;

    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>> {
        Ok(self.tables.clone())
    }

    async fn read_chunk(
        &self,
        table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<UniversalRow>> {
        let after_values: Option<Vec<UniversalValue>> = after.map(|pk| pk.0.clone());
        let client = self.client.lock().await;
        let chunk = surreal_sync_postgresql::read_table_chunk(
            &client,
            &table.table,
            &table.pk_columns,
            after_values.as_deref(),
            limit,
            Some(&self.schema),
        )
        .await?;
        Ok(chunk.rows)
    }

    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()> {
        let kind_str = match kind {
            WatermarkKind::Low => "low".to_string(),
            WatermarkKind::High => "high".to_string(),
        };
        {
            let client = self.client.lock().await;
            client
                .execute(
                    &format!("INSERT INTO {SIGNAL_TABLE} (id, kind) VALUES ($1, $2)"),
                    &[&id, &kind_str],
                )
                .await?;
        }
        let mut guard = self.watermarks.lock().expect("watermark lock poisoned");
        match kind {
            WatermarkKind::Low => guard.low = Some(id),
            WatermarkKind::High => guard.high = Some(id),
        }
        Ok(())
    }

    async fn next_stream_events(&mut self) -> Result<Vec<StreamEvent<i64>>> {
        match self.stream.next_with_sequence_id().await {
            Some(Ok((sequence_id, change))) => {
                if sequence_id > self.last_consumed {
                    self.last_consumed = sequence_id;
                }
                let pk = pk_from_change(&change);

                // Signal-table rows that are not the active watermark pair
                // (stale watermarks, `execute-snapshot` requests, consumed-flag
                // updates) are consumed silently so they are pruned but never
                // applied to the sink.
                if change.table == SIGNAL_TABLE {
                    let is_active_watermark = pk.single_uuid().is_some_and(|id| {
                        let guard = self.watermarks.lock().expect("watermark lock poisoned");
                        Some(id) == guard.low || Some(id) == guard.high
                    });
                    if !is_active_watermark {
                        return Ok(Vec::new());
                    }
                }

                Ok(vec![StreamEvent {
                    position: sequence_id,
                    table: change.table.clone(),
                    pk,
                    change,
                }])
            }
            Some(Err(e)) => Err(e),
            None => Ok(Vec::new()),
        }
    }

    async fn current_position(&self) -> Result<i64> {
        self.query_stream_position().await
    }

    async fn commit_consumed(&mut self, position: i64) -> Result<()> {
        // Prune only rows the stream has actually consumed: never advance past
        // `last_consumed` (would drop unapplied changes) nor past the resumable
        // checkpoint position handed in by the loop.
        let prune_to = position.min(self.last_consumed);
        if prune_to <= 0 {
            return Ok(());
        }
        let client = self.client.lock().await;
        client
            .execute(
                &format!("DELETE FROM {AUDIT_TABLE} WHERE sequence_id <= $1"),
                &[&prune_to],
            )
            .await?;
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT id, tables FROM {SIGNAL_TABLE} \
                     WHERE kind = $1 AND consumed = FALSE ORDER BY id"
                ),
                &[&EXECUTE_SNAPSHOT_KIND],
            )
            .await?;

        let mut signals = Vec::new();
        for row in &rows {
            let id: Uuid = row.get(0);
            let tables_json: Option<String> = row.get(1);
            let tables = parse_signal_tables(tables_json.as_deref());
            client
                .execute(
                    &format!("UPDATE {SIGNAL_TABLE} SET consumed = TRUE WHERE id = $1"),
                    &[&id],
                )
                .await?;
            signals.push(SnapshotSignal {
                id: id.to_string(),
                tables,
            });
        }
        Ok(signals)
    }

    async fn resolve_tables(&self, names: &[String]) -> Result<Vec<TableSpec>> {
        let client = self.client.lock().await;
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            if name == SIGNAL_TABLE || name == AUDIT_TABLE {
                continue;
            }
            let pk_columns = surreal_sync_postgresql::get_primary_key_columns(&client, name).await?;
            if pk_columns.is_empty() {
                anyhow::bail!(
                    "Table '{name}' requested by an execute-snapshot signal has no primary key; \
                     watermark snapshots require a primary key (use --strategy bulk for such tables)"
                );
            }
            specs.push(TableSpec::new(name.clone(), pk_columns));
        }
        Ok(specs)
    }
}

/// Parse the `tables` payload stored on an `execute-snapshot` signal row. The
/// payload is a JSON array of table names; a missing or invalid payload yields
/// no tables.
fn parse_signal_tables(payload: Option<&str>) -> Vec<String> {
    payload
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

/// Insert an ad-hoc `execute-snapshot` signal row requesting `tables` be
/// snapshotted by a running watermark sync. Connects, ensures the signal table
/// exists, and inserts a single request row.
pub async fn request_snapshot(from_opts: &SourceOpts, tables: &[String]) -> Result<()> {
    let client = surreal_sync_postgresql::new_postgresql_client(&from_opts.source_uri).await?;
    let c = client.lock().await;
    c.simple_query(&create_signal_table_sql()).await?;
    let id = Uuid::new_v4();
    let tables_json = serde_json::to_string(tables)?;
    c.execute(
        &format!("INSERT INTO {SIGNAL_TABLE} (id, kind, tables, consumed) VALUES ($1, $2, $3, FALSE)"),
        &[&id, &EXECUTE_SNAPSHOT_KIND, &tables_json],
    )
    .await?;
    Ok(())
}

/// Run a watermark snapshot+stream full sync of every primary-keyed user table
/// from PostgreSQL (trigger source) into `surreal`, returning the final audit
/// `sequence_id` to hand off to incremental/live processing.
pub async fn run_snapshot_stream_full_sync<S, C>(
    surreal: &S,
    from_opts: &SourceOpts,
    config: &SnapshotStreamConfig,
    checkpointer: &mut C,
) -> Result<i64>
where
    S: SurrealSink,
    C: SnapshotCheckpointer,
{
    let mut source = PostgresTriggerWatermarkSource::setup(from_opts).await?;
    let result = run_snapshot_stream(&mut source, surreal, config, checkpointer).await?;
    Ok(result.final_position)
}
