//! Watermark-based interleaved snapshot full sync for MySQL.
//!
//! This module implements the generic [`WatermarkSource`] backend trait for the
//! MySQL trigger source, driving a DBLog-style interleaved snapshot: user tables
//! are copied in primary-key-ordered chunks concurrently with consuming the
//! `surreal_sync_changes` audit stream, using low/high watermarks to reconcile
//! snapshot reads against live changes.
//!
//! Watermarks are written as rows into a dedicated signal table that carries its
//! own change-tracking trigger, so each watermark receives a real ordered
//! `sequence_id` in the same stream as data changes. As the stream is applied,
//! consumed audit rows are pruned (`DELETE ... WHERE sequence_id <= consumed`)
//! so the audit table's retention stays bounded rather than growing one row per
//! change.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use json_types::{convert_id_to_universal_value, JsonValueWithSchema};
use mysql_async::{prelude::*, Pool, Row, Value};
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, InterleavedSnapshotResult, PkTuple,
    SnapshotCheckpointer, SnapshotSignal, ReconciliationEvent, TableSpec, WatermarkKind, WatermarkSource,
};
use sync_core::{
    DatabaseSchema, UniversalChange, UniversalChangeOp, UniversalRow, UniversalType, UniversalValue,
};
use uuid::Uuid;

use crate::change_tracking::setup_mysql_change_tracking;
use crate::checkpoint::get_current_checkpoint;
use crate::full_sync::{get_primary_key_columns, read_table_chunk};
use crate::schema::collect_mysql_database_schema;
use mysql_types::RowConversionConfig;

/// Audit table that captures changes for both data tables and the signal table.
const AUDIT_TABLE: &str = "surreal_sync_changes";

/// Dedicated table whose inserts emit ordered watermark positions in the stream.
const SIGNAL_TABLE: &str = "surreal_sync_signal";

/// `kind` value identifying an ad-hoc `execute-snapshot` request row (as
/// opposed to a `low`/`high` watermark row).
const EXECUTE_SNAPSHOT_KIND: &str = "execute-snapshot";

/// SQL that creates the signal table, shared by the watermark source and the
/// ad-hoc `snapshot` command so the schema is identical regardless of which
/// runs first.
///
/// The table holds two unambiguous kinds of rows, distinguished by `kind`:
/// `low`/`high` watermark rows (their inserts flow through the audit stream so
/// the framework detects its watermarks) and `execute-snapshot` request rows
/// (polled directly via `read_signals`).
fn create_signal_table_sql() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {SIGNAL_TABLE} (\
            id CHAR(36) PRIMARY KEY, \
            kind VARCHAR(32) NOT NULL DEFAULT 'watermark', \
            tables TEXT NULL, \
            consumed TINYINT(1) NOT NULL DEFAULT 0)"
    )
}

/// Per-table schema-aware row-conversion settings (boolean / SET / JSON columns).
#[derive(Default, Clone)]
struct TableConversion {
    boolean_columns: Vec<String>,
    set_columns: Vec<String>,
    json_columns: Vec<String>,
}

/// A MySQL trigger-backed [`WatermarkSource`].
///
/// The stream position is the audit table's `BIGINT AUTO_INCREMENT`
/// `sequence_id`. Watermarks are inserted into [`SIGNAL_TABLE`], whose own
/// trigger records them in [`AUDIT_TABLE`] so they interleave with data changes
/// in stream order.
pub struct MySqlWatermarkSource {
    pool: Pool,
    /// Database name, used to resolve primary keys for ad-hoc snapshot tables.
    database: String,
    schema: DatabaseSchema,
    tables: Vec<TableSpec>,
    /// Primary key columns per table. Behind a mutex so ad-hoc
    /// `execute-snapshot` requests can register the primary key of a table that
    /// was not part of the initial snapshot set.
    pk_by_table: Mutex<HashMap<String, Vec<String>>>,
    conversion_by_table: HashMap<String, TableConversion>,
    /// Highest audit `sequence_id` delivered through [`Self::next_reconciliation_events`]
    /// so far. Reads are resumed strictly after this, and it is the position the
    /// loop checkpoints / frees up to.
    last_sequence_id: AtomicI64,
    /// The most recently written low / high watermark UUIDs. Only the current
    /// pair is surfaced as watermark events; stale watermark rows are consumed
    /// silently so they are pruned but never applied to the sink.
    watermarks: Mutex<WatermarkIds>,
}

#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

impl MySqlWatermarkSource {
    /// Connect, set up the signal table and change-tracking triggers, collect
    /// schema, and capture the starting stream position.
    pub async fn new(pool: Pool, database: String) -> Result<Self> {
        let mut conn = pool.get_conn().await?;

        conn.query_drop(format!("USE `{database}`")).await?;

        // Create the signal table before installing triggers so it gets its own
        // change-tracking trigger writing ordered watermark positions.
        conn.query_drop(create_signal_table_sql()).await?;

        setup_mysql_change_tracking(&mut conn, &database).await?;

        let schema = collect_mysql_database_schema(&mut conn).await?;

        let mut tables = Vec::new();
        let mut pk_by_table = HashMap::new();
        for table_name in get_snapshot_tables(&mut conn, &database).await? {
            let pk_columns = get_primary_key_columns(&mut conn, &database, &table_name).await?;
            if pk_columns.is_empty() {
                return Err(anyhow!(
                    "Table '{table_name}' has no primary key; watermark snapshot requires a primary key \
                     (use the bulk strategy for tables without one)"
                ));
            }
            pk_by_table.insert(table_name.clone(), pk_columns.clone());
            tables.push(TableSpec::new(table_name, pk_columns));
        }

        let conversion_by_table = conversions_from_schema(&schema);

        let starting = get_current_checkpoint(&mut conn).await?.sequence_id;

        Ok(Self {
            pool,
            database,
            schema,
            tables,
            pk_by_table: Mutex::new(pk_by_table),
            conversion_by_table,
            last_sequence_id: AtomicI64::new(starting),
            watermarks: Mutex::new(WatermarkIds::default()),
        })
    }

    fn conversion_config(&self, table: &str) -> RowConversionConfig {
        let conv = self
            .conversion_by_table
            .get(table)
            .cloned()
            .unwrap_or_default();
        RowConversionConfig {
            boolean_columns: conv.boolean_columns,
            set_columns: conv.set_columns,
            json_columns: conv.json_columns,
            json_config: None,
        }
    }

    /// Build the primary-key tuple and the record id for a data-table audit row
    /// from its `row_id` column, normalizing to the same canonical value kinds
    /// the snapshot chunk reads use (so window dedup matches).
    fn pk_and_id(
        &self,
        table: &str,
        pk_columns: &[String],
        row_id: &str,
    ) -> Result<(PkTuple, UniversalValue)> {
        let table_def = self.schema.get_table(table);
        if pk_columns.len() == 1 {
            let ty = table_def
                .and_then(|d| d.get_column_type(&pk_columns[0]))
                .cloned()
                .unwrap_or(UniversalType::Text);
            let value = convert_id_to_universal_value(row_id, table, &ty)?;
            Ok((PkTuple::new(vec![value.clone()]), value))
        } else {
            let parsed: serde_json::Value = serde_json::from_str(row_id).map_err(|e| {
                anyhow!("composite row_id '{row_id}' for table '{table}' is not valid JSON: {e}")
            })?;
            let elements = parsed
                .as_array()
                .ok_or_else(|| anyhow!("composite row_id '{row_id}' is not a JSON array"))?;
            if elements.len() != pk_columns.len() {
                return Err(anyhow!(
                    "composite row_id '{row_id}' has {} parts but table '{table}' has {} key columns",
                    elements.len(),
                    pk_columns.len()
                ));
            }
            let mut values = Vec::with_capacity(pk_columns.len());
            for (col, elem) in pk_columns.iter().zip(elements.iter()) {
                let ty = table_def
                    .and_then(|d| d.get_column_type(col))
                    .cloned()
                    .unwrap_or(UniversalType::Text);
                let part = convert_id_to_universal_value(&json_scalar_to_string(elem), table, &ty)?;
                values.push(part);
            }
            let id = UniversalValue::Array {
                elements: values.clone(),
                element_type: Box::new(UniversalType::Text),
            };
            Ok((PkTuple::new(values), id))
        }
    }

    /// Convert an audit row's `new_data` JSON object into the record's non-key
    /// fields, mirroring the snapshot chunk reads which exclude key columns.
    fn fields_from_new_data(
        &self,
        table: &str,
        pk_columns: &[String],
        json: serde_json::Map<String, serde_json::Value>,
    ) -> HashMap<String, UniversalValue> {
        let table_def = self.schema.get_table(table);
        let mut fields = HashMap::new();
        for (key, value) in json {
            if pk_columns.iter().any(|c| c == &key) {
                continue;
            }
            let col_type = table_def
                .and_then(|d| d.get_column_type(&key))
                .cloned()
                .unwrap_or(UniversalType::Text);
            let universal = if let UniversalType::Set { .. } = col_type {
                match &value {
                    serde_json::Value::String(s) => {
                        let elements = if s.is_empty() {
                            Vec::new()
                        } else {
                            s.split(',').map(|v| v.to_string()).collect()
                        };
                        UniversalValue::set(elements, Vec::new())
                    }
                    _ => {
                        JsonValueWithSchema::new(value, col_type)
                            .to_typed_value()
                            .value
                    }
                }
            } else {
                JsonValueWithSchema::new(value, col_type)
                    .to_typed_value()
                    .value
            };
            fields.insert(key, universal);
        }
        fields
    }
}

#[async_trait::async_trait]
impl WatermarkSource for MySqlWatermarkSource {
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
        let mut conn = self.pool.get_conn().await?;
        let config = self.conversion_config(&table.table);
        let after_values = after.map(|pk| pk.0.as_slice());

        let chunk = read_table_chunk(
            &mut conn,
            &table.table,
            &table.pk_columns,
            after_values,
            limit,
            &config,
        )
        .await?;

        let mut rows = chunk.rows;
        for row in rows.iter_mut() {
            normalize_row_pk(row, &table.pk_columns);
        }
        Ok(rows)
    }

    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()> {
        let kind_str = match kind {
            WatermarkKind::Low => "low",
            WatermarkKind::High => "high",
        };
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(
            format!("INSERT INTO {SIGNAL_TABLE} (id, kind) VALUES (?, ?)"),
            (id.to_string(), kind_str),
        )
        .await?;

        let mut guard = self.watermarks.lock().expect("watermark lock poisoned");
        match kind {
            WatermarkKind::Low => guard.low = Some(id),
            WatermarkKind::High => guard.high = Some(id),
        }
        Ok(())
    }

    async fn next_reconciliation_events(&mut self) -> Result<Vec<ReconciliationEvent<Self::Position>>> {
        let mut conn = self.pool.get_conn().await?;
        let last = self.last_sequence_id.load(Ordering::SeqCst);

        let rows: Vec<Row> = conn
            .exec(
                format!(
                    "SELECT sequence_id, table_name, operation, row_id, new_data \
                     FROM {AUDIT_TABLE} WHERE sequence_id > ? ORDER BY sequence_id LIMIT 500"
                ),
                (last,),
            )
            .await?;

        let (low, high) = {
            let guard = self.watermarks.lock().expect("watermark lock poisoned");
            (guard.low, guard.high)
        };

        let mut events = Vec::new();
        let mut max_seq = last;

        for row in rows {
            let sequence_id: i64 = row.get(0).ok_or_else(|| anyhow!("missing sequence_id"))?;
            let table_name: String = row.get(1).ok_or_else(|| anyhow!("missing table_name"))?;
            let operation: String = row.get(2).ok_or_else(|| anyhow!("missing operation"))?;
            let row_id: String = row.get(3).ok_or_else(|| anyhow!("missing row_id"))?;
            let new_data: Option<Value> = row.get(4);

            if sequence_id > max_seq {
                max_seq = sequence_id;
            }

            if table_name == SIGNAL_TABLE {
                // Only surface the current low/high watermarks. Stale watermark
                // rows are consumed (advancing the position so they get pruned)
                // but never emitted, so they are never applied to the sink.
                let id = Uuid::parse_str(&row_id)
                    .map_err(|e| anyhow!("invalid watermark id '{row_id}' in signal table: {e}"))?;
                if Some(id) == low || Some(id) == high {
                    events.push(ReconciliationEvent {
                        position: sequence_id,
                        table: SIGNAL_TABLE.to_string(),
                        pk: PkTuple::new(vec![UniversalValue::Uuid(id)]),
                        change: UniversalChange::new(
                            UniversalChangeOp::Create,
                            SIGNAL_TABLE,
                            UniversalValue::Uuid(id),
                            None,
                        ),
                    });
                }
                continue;
            }

            let pk_columns = {
                let guard = self.pk_by_table.lock().expect("pk_by_table lock poisoned");
                guard
                    .get(&table_name)
                    .ok_or_else(|| {
                        anyhow!("no primary key columns known for table '{table_name}'")
                    })?
                    .clone()
            };

            let (pk, id) = self.pk_and_id(&table_name, &pk_columns, &row_id)?;

            let (op, data) = match operation.as_str() {
                "INSERT" => (UniversalChangeOp::Create, parse_new_data(new_data)?),
                "UPDATE" => (UniversalChangeOp::Update, parse_new_data(new_data)?),
                "DELETE" => (UniversalChangeOp::Delete, None),
                other => return Err(anyhow!("unknown audit operation '{other}'")),
            };

            let data = data.map(|obj| self.fields_from_new_data(&table_name, &pk_columns, obj));

            let change = UniversalChange::new(op, table_name.clone(), id, data);
            events.push(ReconciliationEvent {
                position: sequence_id,
                table: table_name,
                pk,
                change,
            });
        }

        self.last_sequence_id.store(max_seq, Ordering::SeqCst);
        Ok(events)
    }

    async fn current_position(&self) -> Result<Self::Position> {
        Ok(self.last_sequence_id.load(Ordering::SeqCst))
    }

    async fn commit_reconciled(&mut self, position: Self::Position) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(
            format!("DELETE FROM {AUDIT_TABLE} WHERE sequence_id <= ?"),
            (position,),
        )
        .await?;
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Row> = conn
            .exec(
                format!(
                    "SELECT id, tables FROM {SIGNAL_TABLE} \
                     WHERE kind = ? AND consumed = 0 ORDER BY id"
                ),
                (EXECUTE_SNAPSHOT_KIND,),
            )
            .await?;

        let mut signals = Vec::new();
        for row in rows {
            let id: String = row.get(0).ok_or_else(|| anyhow!("missing signal id"))?;
            let tables_json: Option<String> = row.get(1);
            let tables = parse_signal_tables(tables_json.as_deref());
            conn.exec_drop(
                format!("UPDATE {SIGNAL_TABLE} SET consumed = 1 WHERE id = ?"),
                (id.clone(),),
            )
            .await?;
            signals.push(SnapshotSignal { id, tables });
        }
        Ok(signals)
    }

    async fn resolve_tables(&self, names: &[String]) -> Result<Vec<TableSpec>> {
        let mut conn = self.pool.get_conn().await?;
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            if name == SIGNAL_TABLE || name == AUDIT_TABLE {
                continue;
            }
            let pk_columns = get_primary_key_columns(&mut conn, &self.database, name).await?;
            if pk_columns.is_empty() {
                return Err(anyhow!(
                    "Table '{name}' requested by an execute-snapshot signal has no primary key; \
                     watermark snapshots require a primary key (use --strategy bulk for such tables)"
                ));
            }
            self.pk_by_table
                .lock()
                .expect("pk_by_table lock poisoned")
                .insert(name.clone(), pk_columns.clone());
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
pub async fn request_snapshot(pool: Pool, database: String, tables: &[String]) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE `{database}`")).await?;
    conn.query_drop(create_signal_table_sql()).await?;
    let id = Uuid::new_v4().to_string();
    let tables_json = serde_json::to_string(tables)?;
    conn.exec_drop(
        format!("INSERT INTO {SIGNAL_TABLE} (id, kind, tables, consumed) VALUES (?, ?, ?, 0)"),
        (id, EXECUTE_SNAPSHOT_KIND, tables_json),
    )
    .await?;
    Ok(())
}

/// Run an interleaved snapshot sync for MySQL, returning the final audit
/// `sequence_id` that downstream incremental/live processing should resume from.
pub async fn run_interleaved_snapshot_full_sync<S, C>(
    pool: Pool,
    database: String,
    sink: &S,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
) -> Result<i64>
where
    S: SurrealSink,
    C: SnapshotCheckpointer,
{
    let result =
        run_interleaved_snapshot_full_sync_result(pool, database, sink, config, checkpointer)
            .await?;
    Ok(result.final_position)
}

/// Like [`run_interleaved_snapshot_full_sync`] but returns the full
/// [`InterleavedSnapshotResult`] (including the exact peak buffered-row count).
pub async fn run_interleaved_snapshot_full_sync_result<S, C>(
    pool: Pool,
    database: String,
    sink: &S,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
) -> Result<InterleavedSnapshotResult<i64>>
where
    S: SurrealSink,
    C: SnapshotCheckpointer,
{
    let mut source = MySqlWatermarkSource::new(pool, database).await?;
    run_interleaved_snapshot(&mut source, sink, config, checkpointer).await
}

/// Enumerate the user tables to snapshot, excluding the audit and signal tables
/// and the usual MySQL system schemas.
async fn get_snapshot_tables(conn: &mut mysql_async::Conn, database: &str) -> Result<Vec<String>> {
    let rows: Vec<Row> = conn
        .exec(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_NAME NOT IN (?, ?) \
             ORDER BY TABLE_NAME",
            (database, AUDIT_TABLE, SIGNAL_TABLE),
        )
        .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("TABLE_NAME"))
        .filter(|name| {
            !(name.starts_with("mysql")
                || name.starts_with("information_schema")
                || name.starts_with("performance_schema")
                || name.starts_with("sys"))
        })
        .collect())
}

/// Derive per-table boolean / SET column lists from the collected schema.
fn conversions_from_schema(schema: &DatabaseSchema) -> HashMap<String, TableConversion> {
    let mut out = HashMap::new();
    for table in &schema.tables {
        let mut conv = TableConversion::default();
        for column in table.column_names() {
            match table.get_column_type(column) {
                Some(UniversalType::Bool) => conv.boolean_columns.push(column.to_string()),
                Some(UniversalType::Set { .. }) => conv.set_columns.push(column.to_string()),
                Some(UniversalType::Json) => conv.json_columns.push(column.to_string()),
                _ => {}
            }
        }
        out.insert(table.name.clone(), conv);
    }
    out
}

/// Parse the audit `new_data` column (JSON stored as bytes) into a JSON object.
fn parse_new_data(
    new_data: Option<Value>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
    match new_data {
        Some(Value::Bytes(bytes)) => {
            let json: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("invalid JSON in audit new_data: {e}"))?;
            match json {
                serde_json::Value::Object(map) => Ok(Some(map)),
                other => Err(anyhow!(
                    "expected JSON object in audit new_data, got {other}"
                )),
            }
        }
        _ => Ok(None),
    }
}

/// Normalize a snapshot row's primary key to canonical value kinds so that the
/// keys derived here match the ones derived from audit `row_id` strings. For a
/// composite key the canonical parts are also re-inserted into the row's fields
/// so the generic loop can recover the key via `PkTuple::from_row`.
fn normalize_row_pk(row: &mut UniversalRow, pk_columns: &[String]) {
    if pk_columns.len() == 1 {
        row.id = canonicalize_pk_value(&row.id);
        return;
    }
    if let UniversalValue::Array { elements, .. } = &row.id {
        let canonical: Vec<UniversalValue> = elements.iter().map(canonicalize_pk_value).collect();
        for (col, value) in pk_columns.iter().zip(canonical.iter()) {
            row.fields.insert(col.clone(), value.clone());
        }
        row.id = UniversalValue::Array {
            elements: canonical,
            element_type: Box::new(UniversalType::Text),
        };
    }
}

/// Reduce a primary-key value to the canonical kinds produced by
/// [`convert_id_to_universal_value`] (`Int64` / `Uuid` / `Text`), so snapshot
/// reads and stream events compare equal for the same logical key.
fn canonicalize_pk_value(value: &UniversalValue) -> UniversalValue {
    match value {
        UniversalValue::Int8 { value, .. } => UniversalValue::Int64(*value as i64),
        UniversalValue::Int16(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int32(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int64(v) => UniversalValue::Int64(*v),
        UniversalValue::Uuid(u) => UniversalValue::Uuid(*u),
        UniversalValue::Char { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::VarChar { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::Text(s) => UniversalValue::Text(s.clone()),
        other => other.clone(),
    }
}

/// Render a JSON scalar (from a composite `row_id` array) as the string form
/// expected by [`convert_id_to_universal_value`].
fn json_scalar_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}
