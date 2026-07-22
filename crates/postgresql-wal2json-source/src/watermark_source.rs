//! Watermark snapshot backend for PostgreSQL wal2json.
//!
//! This module implements the generic
//! [`WatermarkSource`](surreal_sync_interleaved_snapshot::WatermarkSource) trait
//! on top of the existing logical-replication slot. It lets the DBLog-style
//! interleaved snapshot framework copy tables in primary-key-ordered chunks
//! while concurrently consuming the WAL change stream, deduplicating snapshot
//! reads against live changes via low/high watermark rows written to a dedicated
//! signal table.
//!
//! Key pieces:
//!
//! - [`Lsn`]: a numerically-ordered Log Sequence Number. PostgreSQL formats an
//!   LSN as `segment/offset` in hex; lexicographic `String` ordering of that
//!   text is wrong (e.g. `"0/9"` would sort after `"0/10"`), so [`Lsn`] parses
//!   both halves as integers and orders on the `(segment, offset)` pair.
//! - [`Wal2JsonWatermarkSource`]: the backend itself.
//! - [`run_interleaved_snapshot_full_sync`]: a small entry point that builds the
//!   backend, runs the framework loop, and returns the final stream position so
//!   a downstream orchestrator can hand it to the existing incremental runner.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{
    run_interleaved_snapshot_with_transforms, InterleavedSnapshotConfig, NoopCheckpointer, PkTuple,
    ReconciliationEvent, SnapshotSignal, SnapshotTransforms, TableSpec, WatermarkKind,
    WatermarkSource,
};
use sync_core::{UniversalChange, UniversalChangeOp, UniversalRow, UniversalValue};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;
use tracing::{debug, info};
use uuid::Uuid;

use crate::change::Action;
use crate::full_sync::SourceOpts;
use crate::logical_replication::{Client, Slot};

/// Name of the signal/watermark table created on the source.
///
/// Watermark rows are inserted here so they are captured by the replication
/// slot and reappear in the change stream, letting the framework detect when
/// its low/high watermarks pass by. The `surreal_sync_` prefix means
/// [`surreal_sync_postgresql::get_user_tables`] already excludes it from the
/// set of tables to snapshot.
pub const SIGNAL_TABLE: &str = "surreal_sync_signal";

/// `kind` value identifying an ad-hoc `execute-snapshot` request row (as
/// opposed to a `low`/`high` watermark row).
const EXECUTE_SNAPSHOT_KIND: &str = "execute-snapshot";

/// SQL that creates the signal table, shared by the watermark source and the
/// ad-hoc `snapshot` command so the schema is identical regardless of which
/// runs first.
///
/// The table holds two unambiguous kinds of rows, distinguished by `kind`:
/// `low`/`high` watermark rows (captured by the slot so they reappear in the
/// change stream) and `execute-snapshot` request rows (polled directly via
/// `read_signals`). FULL replica identity makes wal2json emit the primary key
/// for every operation.
pub(crate) fn create_signal_table_sql(signal_table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {signal_table} (\
            id UUID PRIMARY KEY, \
            kind TEXT NOT NULL, \
            tables TEXT, \
            consumed BOOLEAN NOT NULL DEFAULT FALSE);
         ALTER TABLE {signal_table} REPLICA IDENTITY FULL;"
    )
}

/// The current low/high watermark UUIDs, so only the active watermark pair is
/// surfaced from the change stream; all other signal-table rows are consumed
/// silently and never applied to the sink.
#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

/// A PostgreSQL Log Sequence Number, ordered numerically.
///
/// PostgreSQL prints an LSN as `segment/offset` in hexadecimal (e.g.
/// `"0/1949850"`). Comparing those strings lexicographically is incorrect, so
/// this type parses each half into a `u64` and derives ordering on the
/// `(segment, offset)` pair. It serializes back to the canonical PostgreSQL
/// text form for compact, human-readable checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn {
    segment: u64,
    offset: u64,
}

impl Lsn {
    /// Construct an LSN from its segment and offset components.
    pub fn new(segment: u64, offset: u64) -> Self {
        Self { segment, offset }
    }

    /// Parse an LSN from PostgreSQL's `segment/offset` hexadecimal text.
    pub fn parse(s: &str) -> Result<Self> {
        let (segment, offset) = s
            .split_once('/')
            .with_context(|| format!("invalid LSN '{s}': expected 'segment/offset'"))?;
        let segment = u64::from_str_radix(segment.trim(), 16)
            .with_context(|| format!("invalid LSN segment in '{s}'"))?;
        let offset = u64::from_str_radix(offset.trim(), 16)
            .with_context(|| format!("invalid LSN offset in '{s}'"))?;
        Ok(Self { segment, offset })
    }

    /// Render this LSN in PostgreSQL's canonical `segment/offset` hex form.
    pub fn to_pg_string(&self) -> String {
        format!("{:X}/{:X}", self.segment, self.offset)
    }

    /// The 64-bit numeric value of this LSN (`segment << 32 | offset`), matching
    /// the byte offset PostgreSQL's `pg_wal_lsn_diff` measures against.
    pub fn as_u64(&self) -> u64 {
        (self.segment << 32) | self.offset
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_pg_string())
    }
}

impl FromStr for Lsn {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_pg_string())
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Lsn::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// PostgreSQL wal2json implementation of [`WatermarkSource`].
///
/// Holds a replication-slot client used both for direct SQL (chunk reads,
/// watermark inserts, slot advancement) and for peeking the WAL change stream.
pub struct Wal2JsonWatermarkSource {
    /// Replication client (owns the SQL connection); used for queries.
    client: Client,
    /// Replication slot used to peek/advance the change stream.
    slot: Slot,
    /// Replication slot name.
    slot_name: String,
    /// Source schema (e.g. `public`).
    schema: String,
    /// Explicit list of tables to snapshot; empty means all user tables.
    explicit_tables: Vec<String>,
    /// Signal/watermark table name.
    signal_table: String,
    /// Commit position (`nextlsn`) of the most recently peeked batch.
    ///
    /// This is the consumption frontier reported as the current position, so
    /// the framework checkpoints and frees the WAL exactly up to the last
    /// committed transaction whose changes have been surfaced. Commit LSNs are
    /// monotonic in delivery order (unlike per-row LSNs), which makes them the
    /// correct resume/handoff point.
    confirmed: Lsn,
    /// Number of changes already surfaced since the last slot advance.
    ///
    /// `pg_logical_slot_peek_changes` does not consume, so each peek re-returns
    /// the same committed changes (in commit order) from the slot's restart
    /// point. The slot is only advanced on `commit_reconciled`; until then this
    /// count lets `next_reconciliation_events` skip the stable already-returned prefix
    /// rather than relying on per-row LSNs, which are not monotonic in commit
    /// order.
    returned_since_advance: usize,
    /// Highest LSN the slot has been advanced to (monotonic guard).
    last_advanced: Option<Lsn>,
    /// Cache of primary-key column type OIDs per table, used to coerce keyset
    /// cursor integer widths to the actual column types.
    pk_type_cache: Mutex<HashMap<String, HashMap<String, u32>>>,
    /// The current low/high watermark UUIDs (see [`WatermarkIds`]).
    watermarks: Mutex<WatermarkIds>,
}

/// PostgreSQL type OIDs for the integer types whose width must match the bound
/// keyset cursor parameter.
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;

impl Wal2JsonWatermarkSource {
    /// Connect to PostgreSQL, ensure the signal table and replication slot
    /// exist, and capture the starting stream position.
    pub async fn connect(from_opts: &SourceOpts) -> Result<Self> {
        Self::connect_with_signal_table(from_opts, SIGNAL_TABLE).await
    }

    /// Like [`connect`](Self::connect) but allows overriding the signal-table
    /// name (used by tests).
    pub async fn connect_with_signal_table(
        from_opts: &SourceOpts,
        signal_table: &str,
    ) -> Result<Self> {
        let (pg, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {e}");
            }
        });

        // The slot must surface both the signal-table watermarks and the user
        // tables we snapshot. An empty filter means "all tables"; otherwise we
        // include the signal table alongside the requested tables.
        let mut filter = from_opts.tables.clone();
        if !filter.is_empty() {
            filter.push(signal_table.to_string());
        }

        let client = Client::new(pg, filter);

        // Create the signal table before the slot is consumed so watermark
        // inserts are captured. A UUID primary key lets the framework key
        // watermarks by the UUID it generated; FULL replica identity makes
        // wal2json emit the primary key for every operation.
        client
            .pg_client()
            .batch_execute(&create_signal_table_sql(signal_table))
            .await
            .context("Failed to ensure signal table exists")?;

        client.create_slot(&from_opts.slot_name).await?;

        // Capture the starting position after the slot exists; every watermark
        // and concurrent change committed afterward has a strictly greater LSN.
        let start_lsn = Lsn::parse(&client.get_current_wal_lsn().await?)?;

        let slot = client.start_replication(Some(&from_opts.slot_name)).await?;

        info!(
            "wal2json watermark source ready (slot: {}, start LSN: {})",
            from_opts.slot_name, start_lsn
        );

        Ok(Self {
            client,
            slot,
            slot_name: from_opts.slot_name.clone(),
            schema: from_opts.schema.clone(),
            explicit_tables: from_opts.tables.clone(),
            signal_table: signal_table.to_string(),
            confirmed: start_lsn,
            returned_since_advance: 0,
            last_advanced: None,
            pk_type_cache: Mutex::new(HashMap::new()),
            watermarks: Mutex::new(WatermarkIds::default()),
        })
    }

    /// Look up (and cache) the type OIDs of a table's columns.
    async fn column_type_oids(&self, table: &str) -> Result<HashMap<String, u32>> {
        if let Some(types) = self.pk_type_cache.lock().await.get(table) {
            return Ok(types.clone());
        }
        let rows = self
            .client
            .pg_client()
            .query(
                "SELECT attname, atttypid::oid::int8
                 FROM pg_attribute
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
                &[&table],
            )
            .await
            .context("Failed to read column types for keyset cursor coercion")?;
        let mut types = HashMap::new();
        for row in rows {
            let name: String = row.get(0);
            let oid: i64 = row.get(1);
            types.insert(name, oid as u32);
        }
        self.pk_type_cache
            .lock()
            .await
            .insert(table.to_string(), types.clone());
        Ok(types)
    }

    /// Coerce a keyset cursor so each integer value matches the width of its
    /// primary-key column.
    ///
    /// The shared chunk reader widens small integer primary keys to `Int64`,
    /// but PostgreSQL's wire protocol rejects an `int8` parameter bound against
    /// an `int4`/`int2` column. This narrows the cursor values back to the
    /// column's actual type so the bound parameter type matches.
    async fn coerce_after(
        &self,
        table: &TableSpec,
        after: &PkTuple,
    ) -> Result<Vec<UniversalValue>> {
        let types = self.column_type_oids(&table.table).await?;
        let mut out = Vec::with_capacity(after.0.len());
        for (col, value) in table.pk_columns.iter().zip(after.0.iter()) {
            let coerced = match (value, types.get(col).copied()) {
                (UniversalValue::Int64(n), Some(OID_INT4)) => UniversalValue::Int32(*n as i32),
                (UniversalValue::Int64(n), Some(OID_INT2)) => UniversalValue::Int16(*n as i16),
                (UniversalValue::Int32(n), Some(OID_INT8)) => UniversalValue::Int64(*n as i64),
                (UniversalValue::Int32(n), Some(OID_INT2)) => UniversalValue::Int16(*n as i16),
                (UniversalValue::Int16(n), Some(OID_INT8)) => UniversalValue::Int64(*n as i64),
                (UniversalValue::Int16(n), Some(OID_INT4)) => UniversalValue::Int32(*n as i32),
                (other, _) => other.clone(),
            };
            out.push(coerced);
        }
        Ok(out)
    }

    /// The replication slot name.
    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    /// The signal/watermark table name.
    pub fn signal_table(&self) -> &str {
        &self.signal_table
    }
}

/// Normalize a primary-key value so the dedup key matches the snapshot row's.
///
/// The shared chunk reader widens small integer primary keys to `Int64`, while
/// wal2json reports them as `Int16`/`Int32`. Widening here keeps both
/// representations of the same key byte-identical when serialized as the
/// buffer's dedup key.
fn normalize_pk_value(value: &UniversalValue) -> UniversalValue {
    match value {
        UniversalValue::Int16(n) => UniversalValue::Int64(*n as i64),
        UniversalValue::Int32(n) => UniversalValue::Int64(*n as i64),
        other => other.clone(),
    }
}

/// Build a [`PkTuple`] from a change's primary key, matching how the framework
/// derives the buffer key from a snapshot row.
fn pk_tuple_from_primary_key(pk: &UniversalValue) -> PkTuple {
    match pk {
        UniversalValue::Array { elements, .. } => {
            PkTuple::new(elements.iter().map(normalize_pk_value).collect())
        }
        single => PkTuple::new(vec![normalize_pk_value(single)]),
    }
}

/// Convert a decoded WAL action into a stream event payload, or `None` for
/// transaction markers.
fn action_to_event(action: &Action) -> Option<(String, PkTuple, UniversalChange)> {
    let (row, op) = match action {
        Action::Insert(row) => (row, UniversalChangeOp::Create),
        Action::Update(row) => (row, UniversalChangeOp::Update),
        Action::Delete(row) => (row, UniversalChangeOp::Delete),
        Action::Begin { .. } | Action::Commit { .. } => return None,
    };
    let pk = pk_tuple_from_primary_key(&row.primary_key);
    let data = if op == UniversalChangeOp::Delete {
        None
    } else {
        Some(row.columns.clone())
    };
    let change = UniversalChange::new(op, row.table.clone(), row.primary_key.clone(), data);
    Some((row.table.clone(), pk, change))
}

#[async_trait::async_trait]
impl WatermarkSource for Wal2JsonWatermarkSource {
    type Position = Lsn;

    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>> {
        let tables = if self.explicit_tables.is_empty() {
            surreal_sync_postgresql::get_user_tables(self.client.pg_client(), &self.schema).await?
        } else {
            self.explicit_tables.clone()
        };

        let mut specs = Vec::new();
        for table in tables {
            if table == self.signal_table {
                continue;
            }
            let pk_columns =
                surreal_sync_postgresql::get_primary_key_columns(self.client.pg_client(), &table)
                    .await?;
            if pk_columns.is_empty() {
                anyhow::bail!(
                    "Table '{table}' has no primary key; the interleaved-snapshot strategy \
                     requires a primary key on every table. Re-run with \
                     --strategy sequential-snapshot to copy this source without watermark \
                     snapshots."
                );
            }
            specs.push(TableSpec::new(table, pk_columns));
        }
        Ok(specs)
    }

    async fn read_chunk(
        &self,
        table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<UniversalRow>> {
        let after_values: Option<Vec<UniversalValue>> = match after {
            Some(pk) => Some(self.coerce_after(table, pk).await?),
            None => None,
        };
        let chunk = surreal_sync_postgresql::read_table_chunk(
            self.client.pg_client(),
            &table.table,
            &table.pk_columns,
            after_values.as_deref(),
            limit,
            None,
        )
        .await?;
        Ok(chunk.rows)
    }

    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()> {
        let kind_str = match kind {
            WatermarkKind::Low => "low",
            WatermarkKind::High => "high",
        };
        // `kind_str` is a controlled literal, so it is safe to inline.
        let query = format!(
            "INSERT INTO {} (id, kind) VALUES ($1, '{kind_str}')",
            self.signal_table
        );
        self.client
            .pg_client()
            .execute(&query, &[&id])
            .await
            .context("Failed to insert watermark row")?;
        let mut guard = self.watermarks.lock().await;
        match kind {
            WatermarkKind::Low => guard.low = Some(id),
            WatermarkKind::High => guard.high = Some(id),
        }
        Ok(())
    }

    async fn next_reconciliation_events(&mut self) -> Result<Vec<ReconciliationEvent<Lsn>>> {
        let (changes, nextlsn) = self.slot.peek_with_positions().await?;

        let batch_commit = if nextlsn.is_empty() {
            None
        } else {
            Some(Lsn::parse(&nextlsn)?)
        };

        // Snapshot the active watermark pair once for this batch so signal-table
        // rows that are not the current low/high (stale watermarks,
        // `execute-snapshot` requests, consumed-flag updates) are consumed
        // silently and never applied to the sink.
        let (low, high) = {
            let guard = self.watermarks.lock().await;
            (guard.low, guard.high)
        };

        // The peek returns committed changes in commit order from the slot's
        // restart point; the prefix is stable across calls until the slot is
        // advanced. Skip the changes already surfaced and emit the rest, so a
        // transaction that committed late (but whose rows carry low per-row
        // LSNs) is still delivered in its correct commit position.
        let mut out = Vec::new();
        for change in changes.iter().skip(self.returned_since_advance) {
            if let Some((table, pk, event_change)) = action_to_event(&change.action) {
                let emit = if table == self.signal_table {
                    pk.single_uuid()
                        .is_some_and(|id| Some(id) == low || Some(id) == high)
                } else {
                    true
                };
                if emit {
                    let event_position = batch_commit.unwrap_or(self.confirmed);
                    out.push(ReconciliationEvent {
                        position: event_position,
                        table,
                        pk,
                        change: event_change,
                    });
                }
            }
            self.returned_since_advance += 1;
            let change_lsn = Lsn::parse(&change.lsn)?;
            if change_lsn > self.confirmed {
                self.confirmed = change_lsn;
            }
        }

        if self.returned_since_advance >= changes.len() {
            if let Some(commit_lsn) = batch_commit {
                if commit_lsn > self.confirmed {
                    self.confirmed = commit_lsn;
                }
            }
        }

        if out.is_empty() {
            // Avoid a tight busy-loop while the framework waits for a watermark
            // to become visible in the stream.
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        Ok(out)
    }

    async fn current_position(&self) -> Result<Lsn> {
        Ok(self.confirmed)
    }

    async fn commit_reconciled(&mut self, position: Lsn) -> Result<()> {
        // Do not advance the slot while a peek batch still has changes that
        // have not been surfaced to the framework; advancing would drop them.
        let (changes, _) = self.slot.peek_with_positions().await?;
        if self.returned_since_advance < changes.len() {
            debug!(
                "Deferring slot {} advance at {}: {}/{} peeked changes surfaced",
                self.slot_name,
                position,
                self.returned_since_advance,
                changes.len()
            );
            return Ok(());
        }

        // Monotonic: never move the slot backwards, and never past the
        // checkpointed (consumed) position handed in here.
        if self.last_advanced.is_none_or(|prev| position > prev) {
            self.slot.advance(&position.to_pg_string()).await?;
            self.last_advanced = Some(position);
            // After advancing, the slot's restart point moves forward and the
            // next peek returns only newer committed changes, so the
            // already-returned prefix count resets.
            self.returned_since_advance = 0;
            debug!("Advanced slot {} to LSN {}", self.slot_name, position);
        }
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        let rows = self
            .client
            .pg_client()
            .query(
                &format!(
                    "SELECT id, tables FROM {} WHERE kind = $1 AND consumed = FALSE ORDER BY id",
                    self.signal_table
                ),
                &[&EXECUTE_SNAPSHOT_KIND],
            )
            .await
            .context("Failed to read execute-snapshot signals")?;

        let mut signals = Vec::new();
        for row in &rows {
            let id: Uuid = row.get(0);
            let tables_json: Option<String> = row.get(1);
            let tables = parse_signal_tables(tables_json.as_deref());
            self.client
                .pg_client()
                .execute(
                    &format!(
                        "UPDATE {} SET consumed = TRUE WHERE id = $1",
                        self.signal_table
                    ),
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
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            if name == &self.signal_table {
                continue;
            }
            let pk_columns =
                surreal_sync_postgresql::get_primary_key_columns(self.client.pg_client(), name)
                    .await?;
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

/// Parse the `tables` payload (a JSON array of table names) stored on an
/// `execute-snapshot` signal row. A missing or invalid payload yields no
/// tables.
fn parse_signal_tables(payload: Option<&str>) -> Vec<String> {
    payload
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

/// Insert an ad-hoc `execute-snapshot` signal row requesting `tables` be
/// snapshotted by a running watermark sync. Connects, ensures the signal table
/// exists, and inserts a single request row that the running sync will pick up
/// via [`WatermarkSource::read_signals`].
pub async fn request_snapshot(from_opts: &SourceOpts, tables: &[String]) -> Result<()> {
    let (pg, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });
    pg.batch_execute(&create_signal_table_sql(SIGNAL_TABLE))
        .await
        .context("Failed to ensure signal table exists")?;
    let id = Uuid::new_v4();
    let tables_json = serde_json::to_string(tables)?;
    pg.execute(
        &format!(
            "INSERT INTO {SIGNAL_TABLE} (id, kind, tables, consumed) VALUES ($1, $2, $3, FALSE)"
        ),
        &[&id, &EXECUTE_SNAPSHOT_KIND, &tables_json],
    )
    .await
    .context("Failed to insert execute-snapshot signal row")?;
    Ok(())
}

/// Run an interleaved snapshot full sync from PostgreSQL to SurrealDB (identity transforms).
///
/// Builds the wal2json backend, runs the generic framework loop, and returns
/// the final stream position. A downstream orchestrator hands this position to
/// the existing incremental runner to continue live replication from exactly
/// where the snapshot finished.
pub async fn run_interleaved_snapshot_full_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    chunk_size: usize,
) -> Result<Lsn> {
    run_interleaved_snapshot_full_sync_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        &SnapshotTransforms::identity(),
    )
    .await
}

/// Interleaved snapshot full sync with an explicit transform pipeline.
pub async fn run_interleaved_snapshot_full_sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    chunk_size: usize,
    transforms: &SnapshotTransforms,
) -> Result<Lsn> {
    let mut source = Wal2JsonWatermarkSource::connect(&from_opts).await?;
    let config = InterleavedSnapshotConfig { chunk_size };
    let mut checkpointer = NoopCheckpointer;
    let result = run_interleaved_snapshot_with_transforms(
        &mut source,
        surreal,
        &config,
        &mut checkpointer,
        transforms,
    )
    .await?;
    info!(
        "wal2json watermark snapshot complete (final LSN: {}, peak buffered rows: {})",
        result.final_position, result.peak_buffered_rows
    );
    Ok(result.final_position)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_orders_numerically_not_lexicographically() {
        let small = Lsn::parse("0/9").unwrap();
        let large = Lsn::parse("0/10").unwrap();
        // Lexicographically "0/9" > "0/10"; numerically it must be smaller.
        assert!(small < large);

        let next_segment = Lsn::parse("1/0").unwrap();
        assert!(large < next_segment);
        assert!(Lsn::parse("0/FF").unwrap() < next_segment);
    }

    #[test]
    fn lsn_roundtrips_through_serde() {
        let lsn = Lsn::parse("2A/1949850").unwrap();
        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, "\"2A/1949850\"");
        let parsed: Lsn = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn pk_tuple_widens_small_integers() {
        let from_stream = pk_tuple_from_primary_key(&UniversalValue::Int32(42));
        let from_snapshot = PkTuple::new(vec![UniversalValue::Int64(42)]);
        assert_eq!(from_stream.key(), from_snapshot.key());
    }
}
