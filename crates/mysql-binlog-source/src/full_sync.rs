//! MySQL binlog full sync implementation.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use checkpoint::Checkpoint;
use checkpoint::{CheckpointStore, SyncManager, SyncPhase};
use mysql_async::{prelude::*, Pool, Row as MysqlRow};
use mysql_types::{row_to_typed_values_with_config, RowConversionConfig};
use surreal_sink::SurrealSink;
use sync_core::{Row, Value};
use sync_transform::{
    run_source_runtime_with, ApplyOpts, Pipeline, RowChunkDriver, RowChunkSource, SourceRuntimeOpts,
};
use tracing::{debug, info};

use crate::catch_up::{
    emit_catch_up_progress, read_catch_up_progress, CatchUpProgress, CoverageKind,
};
use crate::checkpoint::BinlogCheckpoint;
use crate::client::{
    connect_binlog_client, get_pool_conn, new_mysql_pool_with_ssl, resolve_database,
    show_master_status, use_database,
};
use crate::schema::collect_mysql_database_schema;
use crate::signal::SIGNAL_TABLE;
use crate::{SourceOpts, SyncOpts};

#[derive(Clone, Debug, Default)]
struct TableSchemaInfo {
    boolean_columns: Vec<String>,
    set_columns: Vec<String>,
    json_columns: Vec<String>,
}

pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_cancellable_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        &tokio_util::sync::CancellationToken::new(),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// [`run_full_sync`] with cooperative cancellation. On cancel, the snapshot
/// stops between tables/batches and returns cleanly **without** emitting a
/// `FullSyncEnd` checkpoint: the `FullSyncStart` position (the streaming lower
/// bound captured before the dump) remains the safe resume point, so a
/// subsequent run re-dumps and streams from there with no missed changes.
///
/// Identity-pipeline overload (no transforms).
pub async fn run_full_sync_cancellable<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_cancellable_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        cancel,
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Full sync with an explicit transform pipeline (`write_rows` before sink).
pub async fn run_full_sync_cancellable_with_transforms<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting MySQL binlog full sync to SurrealDB");
    if pipeline.is_identity() {
        debug!("Full sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            "Full sync using transform pipeline"
        );
    }

    let pool = new_mysql_pool_with_ssl(&from_opts.connection_string, &from_opts.ssl).await?;
    let database = resolve_database(&pool, from_opts).await?;
    let mut conn = get_pool_conn(&pool, &from_opts.connection_string).await?;
    use_database(&mut conn, &database).await?;

    // Detect the server flavor via a short-lived binlog handshake so the captured
    // checkpoints prefer GTID when the server runs in GTID mode.
    let flavor = connect_binlog_client(from_opts).await?.flavor();

    if let Some(manager) = sync_manager {
        // The sequential strategy replays the binlog from the position captured
        // BEFORE the dump, so the start checkpoint must be the real master
        // position (SHOW MASTER STATUS / GTID), not the placeholder default.
        let checkpoint = capture_binlog_checkpoint(&pool, flavor).await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
    }

    let schema_info = collect_schema_info(&mut conn, &database).await?;
    let tables = get_user_tables(&mut conn, &database, from_opts).await?;
    info!("Found {} tables to migrate", tables.len());

    let mut total_migrated = 0;
    for table_name in &tables {
        if cancel.is_cancelled() {
            info!(
                "Cancellation requested during full sync; stopping before table '{table_name}'. \
                 Resume point remains the FullSyncStart position."
            );
            drop(conn);
            pool.disconnect().await?;
            return Ok(());
        }
        info!("Migrating table: {table_name}");
        let count = migrate_table(
            &mut conn,
            surreal,
            table_name,
            sync_opts,
            schema_info.get(table_name),
            cancel,
            pipeline,
            apply_opts,
        )
        .await?;
        total_migrated += count;
        info!("Migrated {count} records from table {table_name}");
        if cancel.is_cancelled() {
            info!(
                "Cancellation requested after table '{table_name}'; stopping full sync. \
                 Resume point remains the FullSyncStart position."
            );
            drop(conn);
            pool.disconnect().await?;
            return Ok(());
        }
    }

    if let Some(manager) = sync_manager {
        // Capture the real master position after the dump so `incremental` resumes
        // exactly where the snapshot ended.
        let checkpoint = capture_binlog_checkpoint(&pool, flavor).await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_cli_string()
        );

        let table_names = tables.to_vec();
        let existing = read_catch_up_progress(manager).await?;
        let mut progress = existing.unwrap_or_else(|| CatchUpProgress::new(checkpoint.clone()));
        progress.merge_tables(&table_names, CoverageKind::Initial, &checkpoint);
        emit_catch_up_progress(manager, &progress).await?;
    }

    drop(conn);
    pool.disconnect().await?;
    info!("MySQL binlog full sync completed: {total_migrated} total records migrated");
    Ok(())
}

async fn collect_schema_info(
    conn: &mut mysql_async::Conn,
    database: &str,
) -> Result<HashMap<String, TableSchemaInfo>> {
    let mut schema_info: HashMap<String, TableSchemaInfo> = HashMap::new();

    let boolean_rows: Vec<MysqlRow> = conn
        .query(
            r#"
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND COLUMN_TYPE = 'tinyint(1)'
            ORDER BY TABLE_NAME, COLUMN_NAME
            "#,
        )
        .await?;
    for row in boolean_rows {
        let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();
        schema_info
            .entry(table_name)
            .or_default()
            .boolean_columns
            .push(column_name);
    }

    let set_rows: Vec<MysqlRow> = conn
        .query(
            r#"
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND DATA_TYPE = 'set'
            ORDER BY TABLE_NAME, COLUMN_NAME
            "#,
        )
        .await?;
    for row in set_rows {
        let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();
        schema_info
            .entry(table_name)
            .or_default()
            .set_columns
            .push(column_name);
    }

    let json_columns_by_table =
        surreal_sync_mysql_trigger_source::json_columns::get_json_columns(conn, database).await?;
    for (table_name, columns) in json_columns_by_table {
        schema_info.entry(table_name).or_default().json_columns = columns;
    }

    Ok(schema_info)
}

async fn get_user_tables(
    conn: &mut mysql_async::Conn,
    database: &str,
    from_opts: &SourceOpts,
) -> Result<Vec<String>> {
    if !from_opts.tables.is_empty() {
        return Ok(from_opts.tables.clone());
    }
    let rows: Vec<MysqlRow> = conn
        .query(format!(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{database}' \
             AND TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_NAME NOT IN ('{SIGNAL_TABLE}')"
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("TABLE_NAME"))
        .collect())
}

pub async fn get_primary_key_columns(
    conn: &mut mysql_async::Conn,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows: Vec<MysqlRow> = conn
        .query(format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}' \
             AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION"
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("COLUMN_NAME"))
        .collect())
}

#[allow(clippy::too_many_arguments)]
async fn migrate_table<S: SurrealSink>(
    conn: &mut mysql_async::Conn,
    surreal: &S,
    table_name: &str,
    sync_opts: &SyncOpts,
    schema_info: Option<&TableSchemaInfo>,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    let database: Option<String> = conn.query_first("SELECT DATABASE()").await?;
    let database = database.ok_or_else(|| anyhow::anyhow!("no database selected"))?;
    let pk_columns = get_primary_key_columns(conn, &database, table_name).await?;

    let boolean_columns = schema_info
        .map(|s| s.boolean_columns.clone())
        .unwrap_or_default();
    let set_columns = schema_info
        .map(|s| s.set_columns.clone())
        .unwrap_or_default();
    let json_columns = schema_info
        .map(|s| s.json_columns.clone())
        .unwrap_or_default();

    let config = RowConversionConfig {
        boolean_columns,
        set_columns,
        json_columns,
        json_config: None,
    };

    let batch_size = sync_opts.batch_size.max(1);

    // Prefer keyset pagination when a primary key exists (avoids loading the
    // whole table into memory). Tables without a PK stream via LIMIT/OFFSET chunks.
    // A long-lived RowChunkDriver lets the next keyset read overlap prior-chunk
    // transform/sink when max_in_flight > 1 (CSV-like streaming pattern).
    if !pk_columns.is_empty() {
        if sync_opts.dry_run {
            let mut total_processed = 0usize;
            let mut after: Option<Vec<Value>> = None;
            loop {
                if cancel.is_cancelled() {
                    return Ok(total_processed);
                }
                let chunk = surreal_sync_mysql_trigger_source::read_table_chunk(
                    conn,
                    table_name,
                    &pk_columns,
                    after.as_deref(),
                    batch_size,
                    &config,
                )
                .await?;
                if chunk.rows.is_empty() {
                    break;
                }
                let n = chunk.rows.len();
                total_processed += n;
                after = chunk.last_pk;
                if n < batch_size {
                    break;
                }
            }
            debug!("Dry-run keyset scan of {table_name}: {total_processed} rows");
            return Ok(total_processed);
        }

        struct MysqlKeysetChunks<'a> {
            conn: &'a mut mysql_async::Conn,
            table_name: &'a str,
            pk_columns: &'a [String],
            after: Option<Vec<Value>>,
            batch_size: usize,
            config: &'a RowConversionConfig,
            cancel: &'a tokio_util::sync::CancellationToken,
            exhausted: bool,
        }

        #[async_trait]
        impl RowChunkSource for MysqlKeysetChunks<'_> {
            async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
                if self.exhausted || self.cancel.is_cancelled() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let chunk = surreal_sync_mysql_trigger_source::read_table_chunk(
                    self.conn,
                    self.table_name,
                    self.pk_columns,
                    self.after.as_deref(),
                    self.batch_size,
                    self.config,
                )
                .await?;
                if chunk.rows.is_empty() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let n = chunk.rows.len();
                self.after = chunk.last_pk;
                if n < self.batch_size {
                    self.exhausted = true;
                }
                Ok(Some(chunk.rows))
            }
        }

        let chunks = MysqlKeysetChunks {
            conn,
            table_name,
            pk_columns: &pk_columns,
            after: None,
            batch_size,
            config: &config,
            cancel,
            exhausted: false,
        };
        let mut driver = RowChunkDriver::new(chunks);
        let transformer = Arc::new(pipeline.clone());
        let runtime_opts = SourceRuntimeOpts::new();
        run_source_runtime_with(&mut driver, surreal, transformer, apply_opts, &runtime_opts)
            .await?;
        let total_processed = driver.sunk_count() as usize;
        debug!("Processed {total_processed} rows from {table_name} (keyset stream)");
        return Ok(total_processed);
    }

    tracing::warn!("Table '{table_name}' has no primary key; streaming via LIMIT/OFFSET chunks");

    if sync_opts.dry_run {
        let mut total_processed = 0usize;
        let mut offset = 0usize;
        loop {
            if cancel.is_cancelled() {
                return Ok(total_processed);
            }
            let rows: Vec<MysqlRow> = conn
                .query(format!(
                    "SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                ))
                .await
                .map_err(|e| anyhow::anyhow!("failed to query table {table_name}: {e}"))?;
            if rows.is_empty() {
                break;
            }
            let n = rows.len();
            total_processed += n;
            offset += n;
            if n < batch_size {
                break;
            }
        }
        return Ok(total_processed);
    }

    struct MysqlOffsetChunks<'a> {
        conn: &'a mut mysql_async::Conn,
        table_name: &'a str,
        batch_size: usize,
        offset: usize,
        config: &'a RowConversionConfig,
        cancel: &'a tokio_util::sync::CancellationToken,
        exhausted: bool,
    }

    #[async_trait]
    impl RowChunkSource for MysqlOffsetChunks<'_> {
        async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
            if self.exhausted || self.cancel.is_cancelled() {
                self.exhausted = true;
                return Ok(None);
            }
            let rows: Vec<MysqlRow> = self
                .conn
                .query(format!(
                    "SELECT * FROM `{}` LIMIT {} OFFSET {}",
                    self.table_name, self.batch_size, self.offset
                ))
                .await
                .map_err(|e| anyhow::anyhow!("failed to query table {}: {e}", self.table_name))?;
            if rows.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            let mut batch = Vec::with_capacity(rows.len());
            for (i, row) in rows.iter().enumerate() {
                let row_index = (self.offset + i) as u64;
                let typed_values = row_to_typed_values_with_config(row, self.config)?;
                let values: HashMap<String, Value> = typed_values
                    .into_iter()
                    .map(|(k, tv)| (k, tv.value))
                    .collect();
                let id = Value::Int64(row_index as i64);
                let fields = values;
                batch.push(Row::new(self.table_name.to_string(), row_index, id, fields));
            }
            let n = batch.len();
            self.offset += n;
            if n < self.batch_size {
                self.exhausted = true;
            }
            Ok(Some(batch))
        }
    }

    let chunks = MysqlOffsetChunks {
        conn,
        table_name,
        batch_size,
        offset: 0,
        config: &config,
        cancel,
        exhausted: false,
    };
    let mut driver = RowChunkDriver::new(chunks);
    let transformer = Arc::new(pipeline.clone());
    let runtime_opts = SourceRuntimeOpts::new();
    run_source_runtime_with(&mut driver, surreal, transformer, apply_opts, &runtime_opts).await?;
    let total_processed = driver.sunk_count() as usize;
    debug!("Processed {total_processed} rows from {table_name} (offset stream)");
    Ok(total_processed)
}

pub async fn read_table_chunk(
    conn: &mut mysql_async::Conn,
    table_name: &str,
    pk_columns: &[String],
    after: Option<&[Value]>,
    limit: usize,
    config: &RowConversionConfig,
) -> Result<Vec<Row>> {
    surreal_sync_mysql_trigger_source::read_table_chunk(
        conn, table_name, pk_columns, after, limit, config,
    )
    .await
    .map(|chunk| chunk.rows)
}

/// Resolve a "start at head" checkpoint for incremental sync: the server's
/// current master position (GTID when in GTID mode, else file+offset). Resuming
/// from it streams only changes committed after this instant — the explicit,
/// resumable equivalent of "start fresh from the current position".
pub async fn capture_head_checkpoint(from_opts: &SourceOpts) -> Result<BinlogCheckpoint> {
    let pool = new_mysql_pool_with_ssl(&from_opts.connection_string, &from_opts.ssl).await?;
    let flavor = connect_binlog_client(from_opts).await?.flavor();
    let checkpoint = capture_binlog_checkpoint(&pool, flavor).await?;
    pool.disconnect().await?;
    Ok(checkpoint)
}

/// Capture the current master position as a checkpoint, preferring a
/// rotation/failover-safe GTID position when the server runs in GTID mode and
/// falling back to file+offset (`SHOW MASTER STATUS`) otherwise.
pub async fn capture_binlog_checkpoint(
    pool: &Pool,
    flavor: binlog_protocol::Flavor,
) -> Result<BinlogCheckpoint> {
    use binlog_protocol::{BinlogPosition, Flavor};

    let mut conn = pool.get_conn().await?;

    let gtid_position: Option<BinlogPosition> = match flavor {
        Flavor::MySql => {
            let raw: Option<String> = conn.query_first("SELECT @@global.gtid_executed").await?;
            parse_mysql_gtid_executed(raw.as_deref())?
        }
        Flavor::MariaDb => {
            let raw: Option<String> = conn.query_first("SELECT @@global.gtid_binlog_pos").await?;
            parse_mariadb_gtid_binlog_pos(raw.as_deref())?
        }
    };

    let position = match gtid_position {
        Some(position) => position,
        None => {
            let (file, pos) = show_master_status(&mut conn).await?;
            BinlogPosition::file_pos(file, pos)
        }
    };

    Ok(BinlogCheckpoint {
        flavor,
        position,
        timestamp: chrono::Utc::now(),
    })
}

/// Parse a MySQL `@@global.gtid_executed` value into a checkpoint position.
///
/// - `None` / empty (whitespace only) → `Ok(None)`: no GTID history yet, so the
///   caller falls back to `SHOW MASTER STATUS` file+offset.
/// - a valid non-empty set → `Ok(Some(..))`.
/// - a non-empty but unparseable value → `Err`. We must never silently downgrade
///   a GTID position to file+offset, which could skip or replay transactions
///   (mirrors `client::resume_gtid_list_from_pos`).
fn parse_mysql_gtid_executed(raw: Option<&str>) -> Result<Option<binlog_protocol::BinlogPosition>> {
    use binlog_protocol::{BinlogPosition, MySqlGtidSet};
    let Some(raw) = raw else {
        return Ok(None);
    };
    // gtid_executed may span multiple lines for multi-interval sets.
    let cleaned = raw.replace(['\n', '\r'], "");
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let executed = MySqlGtidSet::parse(trimmed)
        .map_err(|e| anyhow::anyhow!("failed to parse @@global.gtid_executed '{trimmed}': {e}"))?;
    Ok(Some(BinlogPosition::MySqlGtid { executed }))
}

/// Parse a MariaDB `@@global.gtid_binlog_pos` value into a checkpoint position.
/// Same hard-error semantics as [`parse_mysql_gtid_executed`].
fn parse_mariadb_gtid_binlog_pos(
    raw: Option<&str>,
) -> Result<Option<binlog_protocol::BinlogPosition>> {
    use binlog_protocol::{BinlogPosition, MariaDbGtidList};
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let executed = MariaDbGtidList::parse(trimmed).map_err(|e| {
        anyhow::anyhow!("failed to parse @@global.gtid_binlog_pos '{trimmed}': {e}")
    })?;
    Ok(Some(BinlogPosition::MariaDbGtid { executed }))
}

#[allow(dead_code)]
pub async fn load_schema(pool: &Pool, database: &str) -> Result<sync_core::DatabaseSchema> {
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, database).await?;
    collect_mysql_database_schema(&mut conn).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use binlog_protocol::BinlogPosition;

    #[test]
    fn mysql_gtid_executed_empty_falls_back() {
        assert!(parse_mysql_gtid_executed(None).unwrap().is_none());
        assert!(parse_mysql_gtid_executed(Some("")).unwrap().is_none());
        assert!(parse_mysql_gtid_executed(Some("  \n ")).unwrap().is_none());
    }

    #[test]
    fn mysql_gtid_executed_valid_is_gtid() {
        let pos = parse_mysql_gtid_executed(Some("d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107"))
            .unwrap()
            .expect("valid gtid set");
        assert!(matches!(pos, BinlogPosition::MySqlGtid { .. }));
    }

    #[test]
    fn mysql_gtid_executed_malformed_is_hard_error() {
        let err = parse_mysql_gtid_executed(Some("not-a-gtid"))
            .expect_err("malformed non-empty gtid_executed must error");
        assert!(
            format!("{err}").contains("gtid_executed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mariadb_gtid_pos_empty_falls_back() {
        assert!(parse_mariadb_gtid_binlog_pos(None).unwrap().is_none());
        assert!(parse_mariadb_gtid_binlog_pos(Some(" ")).unwrap().is_none());
    }

    #[test]
    fn mariadb_gtid_pos_valid_is_gtid() {
        let pos = parse_mariadb_gtid_binlog_pos(Some("0-1-270,1-7-42"))
            .unwrap()
            .expect("valid gtid list");
        assert!(matches!(pos, BinlogPosition::MariaDbGtid { .. }));
    }

    #[test]
    fn mariadb_gtid_pos_malformed_is_hard_error() {
        let err = parse_mariadb_gtid_binlog_pos(Some("not-a-gtid"))
            .expect_err("malformed non-empty gtid_binlog_pos must error");
        assert!(
            format!("{err}").contains("gtid_binlog_pos"),
            "unexpected error: {err}"
        );
    }
}
