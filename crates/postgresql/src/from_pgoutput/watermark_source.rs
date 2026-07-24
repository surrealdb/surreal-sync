//! PostgreSQL pgoutput WAL watermark source for interleaved snapshot sync.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::get_primary_key_columns;
use crate::pgoutput_protocol::{CdcChange, PgWalClient, RelationMeta, RowChange, StreamEvent};
use anyhow::{anyhow, Result};
use pg_walstream::ColumnValue;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, ChangeOp, DatabaseSchema, Row, Type, Value};
use surreal_sync_runtime::{
    run_adhoc_snapshot_tables_with_transforms, run_interleaved_snapshot_with_resume_and_transforms,
    InterleavedSnapshotConfig, NoopCheckpointer, PkTuple, ReconciliationEvent,
    SnapshotCheckpointer, SnapshotSignal, SnapshotTransforms, TableSpec, WatermarkKind,
    WatermarkSource,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::Client;
use tracing::info;
use uuid::Uuid;

use crate::from_pgoutput::catch_up::{
    effective_sync_tables, emit_catch_up_progress, read_catch_up_progress, CatchUpProgress,
    CoverageKind,
};
use crate::from_pgoutput::change::cdc_to_change;
use crate::from_pgoutput::checkpoint::{
    get_current_checkpoint, PgoutputCheckpoint, PgoutputReconciliationPos,
};
use crate::from_pgoutput::client::{
    connect_wal_client, ensure_publication_for_source, new_sql_client, resolve_schema,
    start_wal_at_end, start_wal_from_checkpoint,
};
use crate::from_pgoutput::ddl::{
    detect_relation_rename_from_filter, detect_relation_renames, truncate_affects_synced,
    TableRename,
};
use crate::from_pgoutput::full_sync::read_table_chunk;
use crate::from_pgoutput::schema::{
    collect_postgresql_database_schema, get_table_column_names_ordinal,
};
use crate::from_pgoutput::signal::{
    acknowledge_execute_snapshot_signal, read_pending_execute_snapshot_signals, SIGNAL_TABLE,
};
use crate::from_pgoutput::SourceOpts;
use surreal_sync_core::InterleavedSnapshotCheckpoint;

#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

pub struct PgoutputWatermarkSource {
    sql: Arc<AsyncMutex<Client>>,
    schema: String,
    wal: PgWalClient,
    db_schema: DatabaseSchema,
    column_names_by_table: HashMap<String, Vec<String>>,
    tables: Vec<TableSpec>,
    pk_by_table: Mutex<HashMap<String, Vec<String>>>,
    prev_relations: HashMap<u32, RelationMeta>,
    ddl_refresh_pending: bool,
    confirmed: PgoutputReconciliationPos,
    watermarks: Mutex<WatermarkIds>,
    cancel: tokio_util::sync::CancellationToken,
    delivered_signal_ids: Mutex<HashSet<String>>,
    signals_awaiting_ack: Mutex<HashMap<String, HashSet<String>>>,
    /// Cache of primary-key column type OIDs per table for keyset cursor coercion.
    pk_type_cache: AsyncMutex<HashMap<String, HashMap<String, u32>>>,
}

/// Options for [`PgoutputWatermarkSource::connect_with_options`].
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// When set, position the WAL client at this checkpoint instead of the current head.
    pub start_at: Option<PgoutputCheckpoint>,
    /// When set, only include these tables in the snapshot set.
    pub tables_filter: Option<Vec<String>>,
    pub cancel: tokio_util::sync::CancellationToken,
}

impl ConnectOptions {
    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            start_at: None,
            tables_filter: None,
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }
}

impl PgoutputWatermarkSource {
    pub async fn connect(from_opts: &SourceOpts) -> Result<Self> {
        Self::connect_with_options(from_opts, ConnectOptions::default()).await
    }

    pub async fn connect_with_options(
        from_opts: &SourceOpts,
        options: ConnectOptions,
    ) -> Result<Self> {
        let sql = new_sql_client(&from_opts.connection_string).await?;
        let schema = resolve_schema(from_opts).await;
        Self::build(
            sql,
            schema,
            from_opts,
            options.start_at,
            options.tables_filter,
            options.cancel,
            None,
        )
        .await
    }

    /// Resolve the table names that would be snapshotted for these source options.
    pub async fn resolve_snapshot_table_names(from_opts: &SourceOpts) -> Result<Vec<String>> {
        let sql = new_sql_client(&from_opts.connection_string).await?;
        let schema = resolve_schema(from_opts).await;
        let client = sql.lock().await;
        get_snapshot_tables(&client, &schema, from_opts).await
    }

    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.wal = self.wal.with_cancel(cancel.clone());
        self.cancel = cancel;
        self
    }

    /// Wrap an already-positioned WAL client for ad-hoc snapshot windows during
    /// steady-state incremental streaming.
    pub(crate) async fn wrap_active_wal_client(
        sql: Arc<AsyncMutex<Client>>,
        schema: String,
        from_opts: &SourceOpts,
        wal: PgWalClient,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Self> {
        Self::build(sql, schema, from_opts, None, None, cancel, Some(wal)).await
    }

    pub(crate) fn into_wal_client(self) -> PgWalClient {
        self.wal
    }

    pub(crate) fn current_checkpoint(&self) -> Result<PgoutputCheckpoint> {
        get_current_checkpoint(&self.wal)
    }

    pub fn start_checkpoint(&self) -> PgoutputCheckpoint {
        PgoutputCheckpoint {
            lsn: self.confirmed.lsn,
            timestamp: chrono::Utc::now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn build(
        sql: Arc<AsyncMutex<Client>>,
        schema: String,
        from_opts: &SourceOpts,
        start_at: Option<PgoutputCheckpoint>,
        tables_filter: Option<Vec<String>>,
        cancel: tokio_util::sync::CancellationToken,
        existing_wal: Option<PgWalClient>,
    ) -> Result<Self> {
        {
            let client = sql.lock().await;
            ensure_publication_for_source(&client, from_opts, &schema).await?;
        }

        let db_schema = {
            let client = sql.lock().await;
            collect_postgresql_database_schema(&client).await?
        };

        let mut table_names = {
            let client = sql.lock().await;
            get_snapshot_tables(&client, &schema, from_opts).await?
        };
        if let Some(filter) = &tables_filter {
            let allowed: HashSet<_> = filter.iter().cloned().collect();
            table_names.retain(|name| allowed.contains(name));
        }

        let mut tables = Vec::new();
        let mut pk_by_table = HashMap::new();
        let mut column_names_by_table = HashMap::new();
        {
            let client = sql.lock().await;
            for table_name in table_names {
                let pk_columns = get_primary_key_columns(&client, &table_name).await?;
                if pk_columns.is_empty() {
                    return Err(anyhow!(
                        "Table '{table_name}' has no primary key; watermark snapshot requires a primary key"
                    ));
                }
                column_names_by_table.insert(
                    table_name.clone(),
                    get_table_column_names_ordinal(&client, &schema, &table_name).await?,
                );
                pk_by_table.insert(table_name.clone(), pk_columns.clone());
                tables.push(TableSpec::new(table_name, pk_columns));
            }
        }

        let is_new_wal = existing_wal.is_none();
        let mut wal = match existing_wal {
            Some(client) => client.with_cancel(cancel.clone()),
            None => connect_wal_client(from_opts)
                .await?
                .with_cancel(cancel.clone()),
        };
        if is_new_wal {
            wal.ensure_replication_slot().await?;
            if let Some(checkpoint) = &start_at {
                start_wal_from_checkpoint(&mut wal, checkpoint).await?;
            } else {
                let client = sql.lock().await;
                start_wal_at_end(&mut wal, &client).await?;
            }
        }
        let confirmed = PgoutputReconciliationPos::from(get_current_checkpoint(&wal)?);

        Ok(Self {
            sql,
            schema,
            wal,
            db_schema,
            column_names_by_table,
            tables,
            pk_by_table: Mutex::new(pk_by_table),
            prev_relations: HashMap::new(),
            ddl_refresh_pending: false,
            confirmed,
            watermarks: Mutex::new(WatermarkIds::default()),
            cancel,
            delivered_signal_ids: Mutex::new(HashSet::new()),
            signals_awaiting_ack: Mutex::new(HashMap::new()),
            pk_type_cache: AsyncMutex::new(HashMap::new()),
        })
    }

    async fn try_acknowledge_signals_for_table(&self, table: &str) -> Result<()> {
        let mut to_ack = Vec::new();
        {
            let mut awaiting = self
                .signals_awaiting_ack
                .lock()
                .expect("signals_awaiting_ack lock poisoned");
            for (id, tables) in awaiting.iter_mut() {
                tables.remove(table);
                if tables.is_empty() {
                    to_ack.push(id.clone());
                }
            }
            for id in &to_ack {
                awaiting.remove(id);
            }
        }
        for id in to_ack {
            let client = self.sql.lock().await;
            acknowledge_execute_snapshot_signal(&client, &id).await?;
        }
        Ok(())
    }

    fn change_to_stream_event(
        &self,
        change: &CdcChange,
        relation: &RelationMeta,
    ) -> Result<ReconciliationEvent<PgoutputReconciliationPos>> {
        let column_names = self
            .column_names_by_table
            .get(&change.table)
            .ok_or_else(|| anyhow!("missing column names for table '{}'", change.table))?;
        let universal = cdc_to_change(change, relation, column_names, &self.db_schema)?;
        let pk = pk_tuple_from_primary_key(&universal.id);
        Ok(ReconciliationEvent {
            position: PgoutputReconciliationPos::new(change.position),
            table: change.table.clone(),
            pk,
            change: universal,
        })
    }

    fn apply_table_renames(&mut self, renames: &[TableRename]) {
        for rename in renames {
            let Some(spec) = self.tables.iter_mut().find(|t| t.table == rename.old) else {
                continue;
            };
            info!(
                "Relation rename '{}' -> '{}': tracking renamed table under new name",
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
        let client = self.sql.lock().await;
        self.db_schema = collect_postgresql_database_schema(&client).await?;
        self.column_names_by_table.clear();
        for table in &self.tables {
            self.column_names_by_table.insert(
                table.table.clone(),
                get_table_column_names_ordinal(&client, &self.schema, &table.table).await?,
            );
        }
        Ok(())
    }
}

/// PostgreSQL type OIDs for integer primary-key columns.
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;

impl PgoutputWatermarkSource {
    async fn column_type_oids(&self, table: &str) -> Result<HashMap<String, u32>> {
        if let Some(types) = self.pk_type_cache.lock().await.get(table) {
            return Ok(types.clone());
        }
        let client = self.sql.lock().await;
        let rows = client
            .query(
                "SELECT attname, atttypid::oid::int8
                 FROM pg_attribute
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
                &[&table],
            )
            .await
            .map_err(|e| anyhow!("Failed to read column types for keyset cursor coercion: {e}"))?;
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

    async fn coerce_after(&self, table: &TableSpec, after: &PkTuple) -> Result<Vec<Value>> {
        let types = self.column_type_oids(&table.table).await?;
        let mut out = Vec::with_capacity(after.0.len());
        for (col, value) in table.pk_columns.iter().zip(after.0.iter()) {
            let coerced = match (value, types.get(col).copied()) {
                (Value::Int64(n), Some(OID_INT4)) => Value::Int32(*n as i32),
                (Value::Int64(n), Some(OID_INT2)) => Value::Int16(*n as i16),
                (Value::Int32(n), Some(OID_INT8)) => Value::Int64(*n as i64),
                (Value::Int32(n), Some(OID_INT2)) => Value::Int16(*n as i16),
                (Value::Int16(n), Some(OID_INT8)) => Value::Int64(*n as i64),
                (Value::Int16(n), Some(OID_INT4)) => Value::Int32(*n as i32),
                (other, _) => other.clone(),
            };
            out.push(coerced);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl WatermarkSource for PgoutputWatermarkSource {
    type Position = PgoutputReconciliationPos;

    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>> {
        Ok(self.tables.clone())
    }

    async fn read_chunk(
        &self,
        table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let after_values: Option<Vec<Value>> = match after {
            Some(pk) => Some(self.coerce_after(table, pk).await?),
            None => None,
        };
        let client = self.sql.lock().await;
        let chunk = read_table_chunk(
            &client,
            &table.table,
            &table.pk_columns,
            after_values.as_deref(),
            limit,
            Some(&self.db_schema),
        )
        .await?;
        let mut rows = chunk.rows;
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
        let client = self.sql.lock().await;
        client
            .execute(
                &format!("INSERT INTO {SIGNAL_TABLE} (id, kind) VALUES ($1, $2)"),
                &[&id, &kind_str],
            )
            .await?;

        let mut guard = self.watermarks.lock().expect("watermark lock poisoned");
        match kind {
            WatermarkKind::Low => guard.low = Some(id),
            WatermarkKind::High => guard.high = Some(id),
        }
        Ok(())
    }

    async fn next_reconciliation_events(
        &mut self,
    ) -> Result<Vec<ReconciliationEvent<Self::Position>>> {
        let (low, high) = {
            let guard = self.watermarks.lock().expect("watermark lock poisoned");
            (guard.low, guard.high)
        };

        let events = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                return Err(anyhow!("interleaved snapshot cancelled"));
            }
            result = self.wal.next_events(32) => {
                result.map_err(|e| anyhow!("{e}"))?
            }
        };
        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for event in events {
            match event {
                StreamEvent::Relation(meta) => {
                    let mut renames = detect_relation_renames(&self.prev_relations, &meta);
                    if renames.is_empty() {
                        let filter: Option<Vec<String>> = if self.tables.is_empty() {
                            None
                        } else {
                            Some(self.tables.iter().map(|t| t.table.clone()).collect())
                        };
                        if let Some(rename) = detect_relation_rename_from_filter(&filter, &meta) {
                            renames.push(rename);
                        }
                    }
                    self.prev_relations.insert(meta.relation_oid, meta.clone());
                    if !renames.is_empty() {
                        self.apply_table_renames(&renames);
                        info!(
                            "Refreshing PostgreSQL watermark metadata after Relation rename for '{}'",
                            meta.table
                        );
                        self.refresh_schema_metadata().await?;
                    } else {
                        self.ddl_refresh_pending = true;
                    }
                }
                StreamEvent::Type {
                    schema: type_schema,
                    name,
                    ..
                } => {
                    if type_schema == self.schema {
                        self.ddl_refresh_pending = true;
                        info!(
                            "PostgreSQL Type event for '{}.{}' during watermark snapshot; refresh deferred to commit",
                            type_schema, name
                        );
                    }
                }
                StreamEvent::Truncate { tables } => {
                    let snapshotted: Vec<String> =
                        self.tables.iter().map(|t| t.table.clone()).collect();
                    if truncate_affects_synced(&tables, &snapshotted) {
                        info!(
                            "TRUNCATE during watermark snapshot affects tables {:?}",
                            tables
                        );
                    }
                }
                StreamEvent::Change(change) => {
                    let relation = self
                        .wal
                        .relation_cache()
                        .get(&change.relation_oid)
                        .cloned()
                        .ok_or_else(|| {
                            anyhow!(
                                "pgoutput change for relation_oid {} has no preceding Relation; \
                                 refusing to silently drop the change",
                                change.relation_oid
                            )
                        })?;
                    if relation.schema != self.schema {
                        continue;
                    }

                    self.confirmed = PgoutputReconciliationPos::new(change.position);

                    if change.table == SIGNAL_TABLE {
                        if let RowChange::Insert { new } = &change.operation {
                            if let Some(event) = signal_insert_to_event(&change, new, low, high)? {
                                out.push(event);
                            }
                        }
                        continue;
                    }

                    if self.tables.iter().any(|t| t.table == change.table) {
                        out.push(self.change_to_stream_event(&change, &relation)?);
                    }
                }
                StreamEvent::Control => {}
                StreamEvent::Commit => {
                    if self.ddl_refresh_pending {
                        info!("Refreshing PostgreSQL watermark metadata after DDL commit");
                        self.refresh_schema_metadata().await?;
                        self.ddl_refresh_pending = false;
                    }
                }
            }
        }

        Ok(out)
    }

    async fn current_position(&self) -> Result<Self::Position> {
        Ok(self.confirmed.clone())
    }

    async fn commit_reconciled(&mut self, position: Self::Position) -> Result<()> {
        if position > self.confirmed {
            self.confirmed = position.clone();
        }
        self.wal.commit(position.lsn);
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        let pending = {
            let client = self.sql.lock().await;
            read_pending_execute_snapshot_signals(&client).await?
        };
        let mut out = Vec::new();
        let mut delivered = self
            .delivered_signal_ids
            .lock()
            .expect("delivered_signal_ids lock poisoned");
        let mut awaiting = self
            .signals_awaiting_ack
            .lock()
            .expect("signals_awaiting_ack lock poisoned");
        for signal in pending {
            if delivered.contains(&signal.id) {
                continue;
            }
            delivered.insert(signal.id.clone());
            awaiting.insert(signal.id.clone(), signal.tables.iter().cloned().collect());
            out.push(signal);
        }
        Ok(out)
    }

    async fn on_table_snapshot_complete(&mut self, table: &str) -> Result<()> {
        self.try_acknowledge_signals_for_table(table).await
    }

    async fn resolve_tables(&self, names: &[String]) -> Result<Vec<TableSpec>> {
        let client = self.sql.lock().await;
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            if name == SIGNAL_TABLE {
                continue;
            }
            let pk_columns = get_primary_key_columns(&client, name).await?;
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
    values: &pg_walstream::RowData,
    low: Option<Uuid>,
    high: Option<Uuid>,
) -> Result<Option<ReconciliationEvent<PgoutputReconciliationPos>>> {
    let id = signal_uuid_from_row(values)?;
    if Some(id) != low && Some(id) != high {
        return Ok(None);
    }
    Ok(Some(ReconciliationEvent {
        position: PgoutputReconciliationPos::new(change.position),
        table: SIGNAL_TABLE.to_string(),
        pk: PkTuple::new(vec![Value::Uuid(id)]),
        change: Change::new(ChangeOp::Create, SIGNAL_TABLE, Value::Uuid(id), None),
    }))
}

fn signal_uuid_from_row(values: &pg_walstream::RowData) -> Result<Uuid> {
    let id_cell = values
        .get("id")
        .ok_or_else(|| anyhow!("signal row missing id"))?;
    let id_str = match id_cell {
        ColumnValue::Text(bytes) => std::str::from_utf8(bytes)
            .map_err(|e| anyhow!("invalid UTF-8 in signal id: {e}"))?
            .to_string(),
        ColumnValue::Binary(bytes) => {
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                return Ok(Uuid::from_bytes(arr));
            }
            String::from_utf8_lossy(bytes).into_owned()
        }
        ColumnValue::Null => return Err(anyhow!("signal row id is null")),
    };
    Uuid::parse_str(&id_str).map_err(|e| anyhow!("invalid signal UUID '{id_str}': {e}"))
}

fn pk_tuple_from_primary_key(pk: &Value) -> PkTuple {
    match pk {
        Value::Array { elements, .. } => {
            PkTuple::new(elements.iter().map(normalize_pk_value).collect())
        }
        single => PkTuple::new(vec![normalize_pk_value(single)]),
    }
}

fn normalize_pk_value(value: &Value) -> Value {
    match value {
        Value::Int8 { value, .. } => Value::Int64(*value as i64),
        Value::Int16(v) => Value::Int64(*v as i64),
        Value::Int32(v) => Value::Int64(*v as i64),
        Value::Int64(v) => Value::Int64(*v),
        Value::Uuid(u) => Value::Uuid(*u),
        Value::Char { value, .. } => Value::Text(value.clone()),
        Value::VarChar { value, .. } => Value::Text(value.clone()),
        Value::Text(s) => Value::Text(s.clone()),
        other => other.clone(),
    }
}

fn normalize_row_pk(row: &mut Row, pk_columns: &[String]) {
    if pk_columns.len() == 1 {
        row.id = normalize_pk_value(&row.id);
        return;
    }
    if let Value::Array { elements, .. } = &row.id {
        let canonical: Vec<Value> = elements.iter().map(normalize_pk_value).collect();
        for (col, value) in pk_columns.iter().zip(canonical.iter()) {
            row.fields.insert(col.clone(), value.clone());
        }
        row.id = Value::Array {
            elements: canonical,
            element_type: Box::new(Type::Text),
        };
    }
}

async fn get_snapshot_tables(
    client: &Client,
    schema: &str,
    from_opts: &SourceOpts,
) -> Result<Vec<String>> {
    if !from_opts.tables.is_empty() {
        return Ok(from_opts.tables.clone());
    }
    if schema == "public" {
        return crate::get_user_tables(client, schema).await;
    }
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables \
             WHERE schemaname = $1 AND tablename NOT LIKE 'surreal_sync_%' \
             ORDER BY tablename",
            &[&schema],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

pub async fn request_snapshot(from_opts: &SourceOpts, tables: &[String]) -> Result<()> {
    let sql = new_sql_client(&from_opts.connection_string).await?;
    let client = sql.lock().await;
    crate::from_pgoutput::signal::request_snapshot(&client, tables).await
}

/// Options controlling interleaved snapshot restart and table selection.
#[derive(Clone, Debug)]
pub struct InterleavedFullSyncOptions {
    pub resume_progress: Option<InterleavedSnapshotCheckpoint>,
    pub tables_filter: Option<Vec<String>>,
    pub start_at: Option<PgoutputCheckpoint>,
    pub emit_full_sync_start: bool,
    pub coverage_kind: CoverageKind,
}

impl Default for InterleavedFullSyncOptions {
    fn default() -> Self {
        Self {
            resume_progress: None,
            tables_filter: None,
            start_at: None,
            emit_full_sync_start: true,
            coverage_kind: CoverageKind::Initial,
        }
    }
}

/// Outcome of an interleaved snapshot full sync.
#[derive(Debug, Clone)]
pub struct InterleavedFullSyncOutcome {
    pub start: PgoutputCheckpoint,
    pub end: PgoutputCheckpoint,
    pub cancelled: bool,
}

pub async fn run_interleaved_snapshot_full_sync<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&surreal_sync_core::SyncManager<St>>,
    options: InterleavedFullSyncOptions,
) -> Result<InterleavedFullSyncOutcome>
where
    S: SurrealSink,
    St: surreal_sync_core::CheckpointStore,
{
    let transforms = SnapshotTransforms::identity();
    run_interleaved_snapshot_full_sync_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        options,
        &transforms,
    )
    .await
}

/// Interleaved snapshot full sync with an explicit transform pipeline.
pub async fn run_interleaved_snapshot_full_sync_with_transforms<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&surreal_sync_core::SyncManager<St>>,
    options: InterleavedFullSyncOptions,
    transforms: &SnapshotTransforms,
) -> Result<InterleavedFullSyncOutcome>
where
    S: SurrealSink,
    St: surreal_sync_core::CheckpointStore,
{
    use surreal_sync_core::{Checkpoint, SyncPhase};

    let connect_opts = ConnectOptions {
        start_at: options.start_at.clone(),
        tables_filter: options.tables_filter.clone(),
        cancel: cancel.clone(),
    };
    let mut source = PgoutputWatermarkSource::connect_with_options(from_opts, connect_opts).await?;

    let start = if options.emit_full_sync_start {
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
        start
    } else {
        source.start_checkpoint()
    };

    let config = InterleavedSnapshotConfig { chunk_size };
    let resume_ref = options.resume_progress.as_ref();
    let snapshot_result = if let Some(manager) = manager {
        let mut checkpointer = ManagerRefCheckpointer { manager };
        run_interleaved_snapshot_with_resume_and_transforms(
            &mut source,
            surreal,
            &config,
            &mut checkpointer,
            resume_ref,
            transforms,
        )
        .await
    } else {
        run_interleaved_snapshot_with_resume_and_transforms(
            &mut source,
            surreal,
            &config,
            &mut NoopCheckpointer,
            resume_ref,
            transforms,
        )
        .await
    };

    match snapshot_result {
        Ok(result) => {
            let end = PgoutputCheckpoint {
                lsn: result.final_position.lsn,
                timestamp: chrono::Utc::now(),
            };
            info!(
                "pgoutput watermark snapshot complete (final LSN {}, peak buffered rows: {})",
                end.lsn, result.peak_buffered_rows
            );
            if let Some(manager) = manager {
                manager
                    .emit_checkpoint(&end, SyncPhase::FullSyncEnd)
                    .await?;
                let existing = read_catch_up_progress(manager).await?;
                write_catch_up_for_tables(
                    manager,
                    source.tables.iter().map(|t| t.table.clone()).collect(),
                    options.coverage_kind,
                    &end,
                    existing,
                )
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

pub(crate) async fn write_catch_up_for_tables<St: surreal_sync_core::CheckpointStore>(
    manager: &surreal_sync_core::SyncManager<St>,
    table_names: Vec<String>,
    kind: CoverageKind,
    checkpoint: &PgoutputCheckpoint,
    existing: Option<CatchUpProgress>,
) -> Result<()> {
    let mut progress = existing.unwrap_or_else(|| CatchUpProgress::new(checkpoint.clone()));
    progress.merge_tables(&table_names, kind, checkpoint);
    emit_catch_up_progress(manager, &progress).await
}

pub(crate) async fn run_adhoc_snapshots_for_tables<
    S: SurrealSink,
    St: surreal_sync_core::CheckpointStore,
>(
    source: &mut PgoutputWatermarkSource,
    surreal: &S,
    table_names: &[String],
    chunk_size: usize,
    checkpoint_manager: Option<&surreal_sync_core::SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> Result<()> {
    if table_names.is_empty() {
        return Ok(());
    }
    let specs = source.resolve_tables(table_names).await?;
    let config = InterleavedSnapshotConfig { chunk_size };
    match checkpoint_manager {
        Some(manager) => {
            let mut checkpointer = ManagerRefCheckpointer { manager };
            run_adhoc_snapshot_tables_with_transforms(
                source,
                surreal,
                specs,
                &config,
                &mut checkpointer,
                transforms,
            )
            .await
        }
        None => {
            let mut checkpointer = NoopCheckpointer;
            run_adhoc_snapshot_tables_with_transforms(
                source,
                surreal,
                specs,
                &config,
                &mut checkpointer,
                transforms,
            )
            .await
        }
    }
}

struct ManagerRefCheckpointer<'a, St: surreal_sync_core::CheckpointStore> {
    manager: &'a surreal_sync_core::SyncManager<St>,
}

#[async_trait::async_trait]
impl<St: surreal_sync_core::CheckpointStore> SnapshotCheckpointer
    for ManagerRefCheckpointer<'_, St>
{
    async fn save_progress(&mut self, checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        self.manager
            .emit_checkpoint(checkpoint, surreal_sync_core::SyncPhase::SnapshotProgress)
            .await?;
        Ok(())
    }
}

/// Result of the initial interleaved snapshot planning step.
#[derive(Debug, Clone)]
pub struct InitialInterleavedOutcome {
    pub snapshot_skipped: bool,
    pub sync_outcome: Option<InterleavedFullSyncOutcome>,
}

pub async fn run_initial_interleaved_snapshot<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&surreal_sync_core::SyncManager<St>>,
) -> Result<InitialInterleavedOutcome>
where
    S: SurrealSink,
    St: surreal_sync_core::CheckpointStore,
{
    let transforms = SnapshotTransforms::identity();
    run_initial_interleaved_snapshot_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        &transforms,
    )
    .await
}

/// Initial interleaved snapshot planning with an explicit transform pipeline.
pub async fn run_initial_interleaved_snapshot_with_transforms<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&surreal_sync_core::SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> Result<InitialInterleavedOutcome>
where
    S: SurrealSink,
    St: surreal_sync_core::CheckpointStore,
{
    use crate::from_pgoutput::catch_up::{emit_catch_up_progress, tables_pending_snapshot};
    use surreal_sync_core::{Checkpoint, SyncPhase};

    let base_tables = PgoutputWatermarkSource::resolve_snapshot_table_names(from_opts).await?;

    if let Some(manager) = manager {
        if let Ok(progress) = manager
            .read_checkpoint::<InterleavedSnapshotCheckpoint>(SyncPhase::SnapshotProgress)
            .await
        {
            if !progress.all_done() {
                info!("Resuming interleaved snapshot from saved per-chunk progress");
                let start_at = reconciliation_checkpoint_from_snapshot_progress(&progress).await?;
                let outcome = run_interleaved_snapshot_full_sync_with_transforms(
                    surreal,
                    from_opts,
                    chunk_size,
                    cancel,
                    Some(manager),
                    InterleavedFullSyncOptions {
                        resume_progress: Some(progress),
                        start_at: Some(start_at),
                        emit_full_sync_start: false,
                        coverage_kind: CoverageKind::Initial,
                        ..InterleavedFullSyncOptions::default()
                    },
                    transforms,
                )
                .await?;
                return Ok(InitialInterleavedOutcome {
                    snapshot_skipped: false,
                    sync_outcome: Some(outcome),
                });
            }
        }

        let mut progress = read_catch_up_progress(manager).await?;
        let requested_tables = effective_sync_tables(&base_tables, progress.as_ref())
            .unwrap_or_else(|| base_tables.clone());
        if let Some(ref mut catch_up) = progress {
            let before = catch_up.covered_tables.len();
            catch_up.prune_to_requested(&requested_tables);
            if catch_up.covered_tables.len() != before {
                emit_catch_up_progress(manager, catch_up).await?;
            }
        }
        if let Ok(end) = manager
            .read_checkpoint::<PgoutputCheckpoint>(SyncPhase::FullSyncEnd)
            .await
        {
            if progress
                .as_ref()
                .is_some_and(|p| !p.covered_tables.is_empty())
            {
                let catch_up = progress.unwrap();
                let pending = tables_pending_snapshot(&requested_tables, &catch_up);
                if pending.is_empty() {
                    info!(
                        "All requested tables already snapshotted; skipping snapshot and streaming from {}",
                        end.to_cli_string()
                    );
                    return Ok(InitialInterleavedOutcome {
                        snapshot_skipped: true,
                        sync_outcome: Some(InterleavedFullSyncOutcome {
                            start: end.clone(),
                            end,
                            cancelled: false,
                        }),
                    });
                }
                info!(
                    "Snapshotted tables {:?}; running delta snapshot for {:?}",
                    catch_up.covered_names(),
                    pending
                );
                let outcome = run_interleaved_snapshot_full_sync_with_transforms(
                    surreal,
                    from_opts,
                    chunk_size,
                    cancel,
                    Some(manager),
                    InterleavedFullSyncOptions {
                        tables_filter: Some(pending),
                        start_at: Some(end),
                        emit_full_sync_start: false,
                        coverage_kind: CoverageKind::Initial,
                        ..InterleavedFullSyncOptions::default()
                    },
                    transforms,
                )
                .await?;
                return Ok(InitialInterleavedOutcome {
                    snapshot_skipped: false,
                    sync_outcome: Some(outcome),
                });
            }
        }
    }

    let outcome = run_interleaved_snapshot_full_sync_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        InterleavedFullSyncOptions {
            coverage_kind: CoverageKind::Initial,
            ..InterleavedFullSyncOptions::default()
        },
        transforms,
    )
    .await?;
    Ok(InitialInterleavedOutcome {
        snapshot_skipped: false,
        sync_outcome: Some(outcome),
    })
}

async fn reconciliation_checkpoint_from_snapshot_progress(
    progress: &InterleavedSnapshotCheckpoint,
) -> Result<PgoutputCheckpoint> {
    let stream: PgoutputReconciliationPos =
        serde_json::from_value(progress.reconciliation_pos.clone())?;
    Ok(PgoutputCheckpoint {
        lsn: stream.lsn,
        timestamp: chrono::Utc::now(),
    })
}
