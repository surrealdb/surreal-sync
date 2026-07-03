//! Binlog-backed watermark source for interleaved snapshot sync.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use binlog_protocol::{BinlogClient, CdcChange, EventBody, RowChange, TableMapEvent};
use mysql_async::{prelude::*, Pool};
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, NoopCheckpointer, PkTuple, SnapshotSignal,
    StreamEvent, TableSpec, WatermarkKind, WatermarkSource,
};
use sync_core::{
    DatabaseSchema, UniversalChange, UniversalChangeOp, UniversalRow, UniversalType, UniversalValue,
};
use tracing::info;
use uuid::Uuid;

use crate::change::cdc_change_to_universal;
use crate::checkpoint::{get_current_checkpoint, BinlogCheckpoint, BinlogStreamPosition};
use crate::client::{
    connect_binlog_client, new_mysql_pool, resolve_database, start_binlog_at_end, use_database,
};
use crate::full_sync::{get_primary_key_columns, read_table_chunk};
use crate::schema::{collect_mysql_database_schema, get_table_column_names_ordinal};
use crate::signal::{create_signal_table_sql, EXECUTE_SNAPSHOT_KIND, SIGNAL_TABLE};
use crate::SourceOpts;
use mysql_types::RowConversionConfig;

#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

#[derive(Default, Clone)]
struct TableConversion {
    boolean_columns: Vec<String>,
    set_columns: Vec<String>,
    json_columns: Vec<String>,
}

pub struct BinlogWatermarkSource {
    pool: Pool,
    database: String,
    binlog: BinlogClient,
    schema: DatabaseSchema,
    json_columns: HashMap<String, Vec<String>>,
    column_names_by_table: HashMap<String, Vec<String>>,
    tables: Vec<TableSpec>,
    pk_by_table: Mutex<HashMap<String, Vec<String>>>,
    conversion_by_table: HashMap<String, TableConversion>,
    table_maps: HashMap<u64, TableMapEvent>,
    confirmed: BinlogStreamPosition,
    watermarks: Mutex<WatermarkIds>,
    cancel: tokio_util::sync::CancellationToken,
}

impl BinlogWatermarkSource {
    pub async fn connect(from_opts: &SourceOpts) -> Result<Self> {
        let pool = new_mysql_pool(&from_opts.connection_string)?;
        let database = resolve_database(&pool, from_opts).await?;
        let mut conn = pool.get_conn().await?;
        use_database(&mut conn, &database).await?;
        conn.query_drop(create_signal_table_sql()).await?;

        let schema = collect_mysql_database_schema(&mut conn).await?;
        let json_columns =
            surreal_sync_mysql_trigger_source::json_columns::get_json_columns(&mut conn, &database)
                .await?;

        let mut tables = Vec::new();
        let mut pk_by_table = HashMap::new();
        let mut column_names_by_table = HashMap::new();
        for table_name in get_snapshot_tables(&mut conn, &database, from_opts).await? {
            let pk_columns = get_primary_key_columns(&mut conn, &database, &table_name).await?;
            if pk_columns.is_empty() {
                return Err(anyhow!(
                    "Table '{table_name}' has no primary key; watermark snapshot requires a primary key"
                ));
            }
            column_names_by_table.insert(
                table_name.clone(),
                get_table_column_names_ordinal(&mut conn, &table_name).await?,
            );
            pk_by_table.insert(table_name.clone(), pk_columns.clone());
            tables.push(TableSpec::new(table_name, pk_columns));
        }

        let conversion_by_table = conversions_from_schema(&schema, &json_columns);
        let mut binlog = connect_binlog_client(from_opts).await?;
        start_binlog_at_end(&mut binlog, &pool).await?;
        let confirmed = BinlogStreamPosition::from(get_current_checkpoint(&binlog)?);

        Ok(Self {
            pool,
            database,
            binlog,
            schema,
            json_columns,
            column_names_by_table,
            tables,
            pk_by_table: Mutex::new(pk_by_table),
            conversion_by_table,
            table_maps: HashMap::new(),
            confirmed,
            watermarks: Mutex::new(WatermarkIds::default()),
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Attach a cancellation token so a running interleaved snapshot/stream can
    /// be stopped gracefully (SIGINT/SIGTERM in the CLI, or a test).
    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// The stream position captured at connect time — the streaming lower bound
    /// that must be persisted BEFORE the snapshot so a stop/restart re-snapshots
    /// and resumes streaming from here without missing changes.
    pub fn start_checkpoint(&self) -> BinlogCheckpoint {
        BinlogCheckpoint {
            flavor: self.binlog.flavor(),
            position: self.confirmed.position.clone(),
            timestamp: chrono::Utc::now(),
        }
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

    fn row_change_to_stream_event(
        &self,
        change: &CdcChange,
        table_map: &TableMapEvent,
    ) -> Result<StreamEvent<BinlogStreamPosition>> {
        let column_names = self
            .column_names_by_table
            .get(&change.table)
            .ok_or_else(|| anyhow!("missing column names for table '{}'", change.table))?;
        let universal = cdc_change_to_universal(
            change,
            table_map,
            column_names,
            &self.schema,
            &self.json_columns,
        )?;
        let pk = pk_tuple_from_primary_key(&universal.id);
        Ok(StreamEvent {
            position: BinlogStreamPosition::new(change.position.clone()),
            table: change.table.clone(),
            pk,
            change: universal,
        })
    }

    /// Track `RENAME TABLE old TO new` for tables we snapshot/stream so live
    /// events on the new name keep flowing (and per-table PK metadata follows).
    fn apply_table_renames(&mut self, renames: &[crate::ddl::TableRename]) {
        for rename in renames {
            let Some(spec) = self.tables.iter_mut().find(|t| t.table == rename.old) else {
                continue;
            };
            info!(
                "RENAME TABLE '{}' -> '{}': tracking renamed table under new name",
                rename.old, rename.new
            );
            let pk_columns = spec.pk_columns.clone();
            spec.table = rename.new.clone();
            if let Some(cols) = self.column_names_by_table.remove(&rename.old) {
                self.column_names_by_table.insert(rename.new.clone(), cols);
            }
            let mut pk_guard = self.pk_by_table.lock().expect("pk_by_table lock poisoned");
            if let Some(pk) = pk_guard.remove(&rename.old) {
                pk_guard.insert(rename.new.clone(), pk);
            } else {
                pk_guard.insert(rename.new.clone(), pk_columns);
            }
        }
    }

    async fn refresh_schema_metadata(&mut self) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        use_database(&mut conn, &self.database).await?;
        self.schema = collect_mysql_database_schema(&mut conn).await?;
        self.json_columns = surreal_sync_mysql_trigger_source::json_columns::get_json_columns(
            &mut conn,
            &self.database,
        )
        .await?;

        self.column_names_by_table.clear();
        for table in &self.tables {
            self.column_names_by_table.insert(
                table.table.clone(),
                get_table_column_names_ordinal(&mut conn, &table.table).await?,
            );
        }
        self.conversion_by_table = conversions_from_schema(&self.schema, &self.json_columns);
        self.table_maps.clear();
        Ok(())
    }
}

#[async_trait::async_trait]
impl WatermarkSource for BinlogWatermarkSource {
    type Position = BinlogStreamPosition;

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
        use_database(&mut conn, &self.database).await?;
        let config = self.conversion_config(&table.table);
        let after_values = after.map(|pk| pk.0.as_slice());
        let mut rows = read_table_chunk(
            &mut conn,
            &table.table,
            &table.pk_columns,
            after_values,
            limit,
            &config,
        )
        .await?;
        for row in &mut rows {
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
        use_database(&mut conn, &self.database).await?;
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

    async fn next_stream_events(&mut self) -> Result<Vec<StreamEvent<Self::Position>>> {
        let (low, high) = {
            let guard = self.watermarks.lock().expect("watermark lock poisoned");
            (guard.low, guard.high)
        };

        // Cooperative cancellation: interrupt the blocking binlog read so the
        // interleaved snapshot/stream can stop promptly. The lower-bound
        // checkpoint is persisted before the snapshot begins, so unwinding here
        // is safe (a restart re-snapshots and resumes from that lower bound).
        let events = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                return Err(anyhow!("interleaved snapshot cancelled"));
            }
            result = self.binlog.next_events(32) => {
                result.map_err(|e| anyhow!("{e}"))?
            }
        };
        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for event in events {
            match event.body {
                EventBody::Query(query) => {
                    if crate::ddl::is_table_affecting_ddl(&query, &self.database) {
                        info!(
                            "Refreshing MySQL binlog watermark metadata after DDL: {}",
                            query.sql
                        );
                        self.apply_table_renames(&crate::ddl::parse_table_renames(&query.sql));
                        self.refresh_schema_metadata().await?;
                    }
                }
                EventBody::TableMap(tm) => {
                    self.table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    // A row event is always preceded by its TableMap in-stream; a
                    // missing map would silently drop the change, so fail loudly.
                    let table_map =
                        self.table_maps
                            .get(&rows.table_id)
                            .cloned()
                            .ok_or_else(|| {
                                anyhow!(
                                    "binlog row event for table_id {} has no preceding TableMap; \
                                 refusing to silently drop the change",
                                    rows.table_id
                                )
                            })?;
                    if table_map.database != self.database {
                        continue;
                    }

                    for row_change in rows.rows {
                        let position = self.binlog.current_position();
                        self.confirmed = BinlogStreamPosition::new(position.clone());
                        let change = CdcChange {
                            position,
                            database: table_map.database.clone(),
                            table: table_map.table.clone(),
                            operation: row_change,
                            xid: None,
                            gtid: None,
                        };

                        if change.table == SIGNAL_TABLE {
                            if let RowChange::Insert(values) = &change.operation {
                                if let Some(event) =
                                    signal_insert_to_event(&change, values, low, high)?
                                {
                                    out.push(event);
                                }
                            }
                            continue;
                        }

                        if self.tables.iter().any(|t| t.table == change.table) {
                            out.push(self.row_change_to_stream_event(&change, &table_map)?);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(out)
    }

    async fn current_position(&self) -> Result<Self::Position> {
        Ok(self.confirmed.clone())
    }

    async fn commit_consumed(&mut self, position: Self::Position) -> Result<()> {
        if position > self.confirmed {
            self.confirmed = position.clone();
        }
        self.binlog.commit(position.position);
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        let mut conn = self.pool.get_conn().await?;
        use_database(&mut conn, &self.database).await?;
        let rows: Vec<mysql_async::Row> = conn
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
        use_database(&mut conn, &self.database).await?;
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            if name == SIGNAL_TABLE {
                continue;
            }
            let pk_columns = get_primary_key_columns(&mut conn, &self.database, name).await?;
            if pk_columns.is_empty() {
                return Err(anyhow!(
                    "Table '{name}' requested by execute-snapshot has no primary key"
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

fn signal_insert_to_event(
    change: &CdcChange,
    values: &[binlog_protocol::CellValue],
    low: Option<Uuid>,
    high: Option<Uuid>,
) -> Result<Option<StreamEvent<BinlogStreamPosition>>> {
    let id = signal_uuid_from_row(values)?;
    if Some(id) != low && Some(id) != high {
        return Ok(None);
    }
    Ok(Some(StreamEvent {
        position: BinlogStreamPosition::new(change.position.clone()),
        table: SIGNAL_TABLE.to_string(),
        pk: PkTuple::new(vec![UniversalValue::Uuid(id)]),
        change: UniversalChange::new(
            UniversalChangeOp::Create,
            SIGNAL_TABLE,
            UniversalValue::Uuid(id),
            None,
        ),
    }))
}

fn signal_uuid_from_row(values: &[binlog_protocol::CellValue]) -> Result<Uuid> {
    let id_cell = values
        .first()
        .ok_or_else(|| anyhow!("signal row missing id"))?;
    let id_str = match id_cell {
        binlog_protocol::CellValue::String(s) | binlog_protocol::CellValue::JsonText(s) => {
            s.clone()
        }
        other => return Err(anyhow!("unexpected signal id cell: {other:?}")),
    };
    Uuid::parse_str(&id_str).map_err(|e| anyhow!("invalid signal UUID '{id_str}': {e}"))
}

fn pk_tuple_from_primary_key(pk: &UniversalValue) -> PkTuple {
    match pk {
        UniversalValue::Array { elements, .. } => {
            PkTuple::new(elements.iter().map(normalize_pk_value).collect())
        }
        single => PkTuple::new(vec![normalize_pk_value(single)]),
    }
}

fn normalize_pk_value(value: &UniversalValue) -> UniversalValue {
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

/// Re-insert composite primary key columns into row fields so the interleaved
/// snapshot loop can recover keys via `PkTuple::from_row`.
fn normalize_row_pk(row: &mut UniversalRow, pk_columns: &[String]) {
    if pk_columns.len() == 1 {
        row.id = normalize_pk_value(&row.id);
        return;
    }
    if let UniversalValue::Array { elements, .. } = &row.id {
        let canonical: Vec<UniversalValue> = elements.iter().map(normalize_pk_value).collect();
        for (col, value) in pk_columns.iter().zip(canonical.iter()) {
            row.fields.insert(col.clone(), value.clone());
        }
        row.id = UniversalValue::Array {
            elements: canonical,
            element_type: Box::new(UniversalType::Text),
        };
    }
}

fn parse_signal_tables(payload: Option<&str>) -> Vec<String> {
    payload
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

async fn get_snapshot_tables(
    conn: &mut mysql_async::Conn,
    database: &str,
    from_opts: &SourceOpts,
) -> Result<Vec<String>> {
    if !from_opts.tables.is_empty() {
        return Ok(from_opts.tables.clone());
    }
    let rows: Vec<mysql_async::Row> = conn
        .exec(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_NAME NOT IN (?) \
             ORDER BY TABLE_NAME",
            (database, SIGNAL_TABLE),
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("TABLE_NAME"))
        .collect())
}

fn conversions_from_schema(
    schema: &DatabaseSchema,
    json_columns: &HashMap<String, Vec<String>>,
) -> HashMap<String, TableConversion> {
    let mut out = HashMap::new();
    for table in &schema.tables {
        let mut conv = TableConversion {
            json_columns: json_columns.get(&table.name).cloned().unwrap_or_default(),
            ..Default::default()
        };
        for column in table.column_names() {
            match table.get_column_type(column) {
                Some(UniversalType::Bool) => conv.boolean_columns.push(column.to_string()),
                Some(UniversalType::Set { .. }) => conv.set_columns.push(column.to_string()),
                Some(UniversalType::Json) if !conv.json_columns.iter().any(|c| c == column) => {
                    conv.json_columns.push(column.to_string());
                }
                _ => {}
            }
        }
        out.insert(table.name.clone(), conv);
    }
    out
}

pub async fn request_snapshot(pool: &Pool, database: &str, tables: &[String]) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, database).await?;
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

/// Outcome of an interleaved snapshot full sync.
#[derive(Debug, Clone)]
pub struct InterleavedFullSyncOutcome {
    /// Streaming lower bound captured (and persisted, if a store is configured)
    /// BEFORE the snapshot began.
    pub start: BinlogCheckpoint,
    /// Consistent position reached at snapshot completion; incremental streaming
    /// resumes from here. Equals `start` when the snapshot was cancelled.
    pub end: BinlogCheckpoint,
    /// True when the snapshot was interrupted by cancellation before completing.
    pub cancelled: bool,
}

/// Run an interleaved snapshot full sync with graceful cancellation and correct
/// resume semantics:
///
/// 1. Capture the current stream position and, if a checkpoint store is
///    configured, persist it as `FullSyncStart` **before** copying any rows.
///    This is the streaming lower bound: a stop/restart re-snapshots and resumes
///    streaming from here, so no change committed during the snapshot is missed.
/// 2. Copy every table in primary-key chunks while consuming the live stream.
/// 3. On success, persist the final consistent position as `FullSyncEnd`.
/// 4. On cancellation, return cleanly without emitting `FullSyncEnd` — the
///    persisted `FullSyncStart` remains the safe resume point.
pub async fn run_interleaved_snapshot_full_sync<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&checkpoint::SyncManager<St>>,
) -> Result<InterleavedFullSyncOutcome>
where
    S: SurrealSink,
    St: checkpoint::CheckpointStore,
{
    use checkpoint::{Checkpoint, SyncPhase};

    let mut source = BinlogWatermarkSource::connect(from_opts)
        .await?
        .with_cancel(cancel.clone());

    let start = source.start_checkpoint();
    if let Some(manager) = manager {
        manager
            .emit_checkpoint(&start, SyncPhase::FullSyncStart)
            .await?;
        info!(
            "Emitted interleaved snapshot start checkpoint (streaming lower bound): {}",
            start.to_cli_string()
        );
    }

    let config = InterleavedSnapshotConfig { chunk_size };
    match run_interleaved_snapshot(&mut source, surreal, &config, &mut NoopCheckpointer).await {
        Ok(result) => {
            let end = BinlogCheckpoint {
                flavor: source.binlog.flavor(),
                position: result.final_position.position,
                timestamp: chrono::Utc::now(),
            };
            info!(
                "binlog watermark snapshot complete (final position {:?}, peak buffered rows: {})",
                end.position, result.peak_buffered_rows
            );
            if let Some(manager) = manager {
                manager
                    .emit_checkpoint(&end, SyncPhase::FullSyncEnd)
                    .await?;
            }
            Ok(InterleavedFullSyncOutcome {
                start,
                end,
                cancelled: false,
            })
        }
        Err(e) if cancel.is_cancelled() => {
            info!(
                "Interleaved snapshot cancelled before completion ({e}); \
                 FullSyncStart remains the resume point"
            );
            Ok(InterleavedFullSyncOutcome {
                end: start.clone(),
                start,
                cancelled: true,
            })
        }
        Err(e) => Err(e),
    }
}
