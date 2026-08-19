//! SQL Server CDC enablement and LSN polling.

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use tracing::warn;

use crate::from_mssql::catalog::MssqlTableMeta;
use crate::from_mssql::checkpoint::MssqlLsn;
use crate::from_mssql::client::{MssqlClient, SqlArg};
use crate::from_mssql::naming::QualifiedName;
use crate::types::tiberius_to_value;
use surreal_sync_core::{Type, Value};

/// CDC `__$operation` values from `fn_cdc_get_all_changes_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// 1 — delete
    Delete,
    /// 2 — insert
    Insert,
    /// 3 — update before-image
    UpdateBefore,
    /// 4 — update after-image
    UpdateAfter,
}

impl CdcOperation {
    fn from_i32(v: i32) -> Result<Self> {
        match v {
            1 => Ok(Self::Delete),
            2 => Ok(Self::Insert),
            3 => Ok(Self::UpdateBefore),
            4 => Ok(Self::UpdateAfter),
            other => anyhow::bail!("unknown CDC __$operation {other}"),
        }
    }
}

/// One row from `cdc.fn_cdc_get_all_changes_<capture>`.
#[derive(Debug, Clone)]
pub struct CdcChange {
    pub start_lsn: MssqlLsn,
    pub operation: CdcOperation,
    #[allow(dead_code)]
    pub source: QualifiedName,
    pub fields: HashMap<String, Value>,
}

fn agent_hint() -> &'static str {
    "SQL Server Agent must be running for CDC capture jobs \
     (Linux containers: MSSQL_AGENT_ENABLED=true). To enable Agent XPs:\n\
     EXEC sp_configure 'show advanced options', 1; RECONFIGURE;\n\
     EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"
}

fn map_cdc_error(err: anyhow::Error) -> anyhow::Error {
    let msg = err.to_string();
    if msg.contains("Agent") || msg.contains("cdc.") || msg.to_ascii_lowercase().contains("capture")
    {
        anyhow!("{msg}\n{}", agent_hint())
    } else {
        err
    }
}

/// SQL Server error 313: the LSN is not in the capture instance's range yet (Agent lag).
fn is_lsn_range_not_ready(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("insufficient number of arguments") || msg.contains("(code: 313")
}

/// Fail with copy-paste T-SQL when CDC is off for the database.
pub async fn ensure_cdc_enabled(client: &MssqlClient) -> Result<()> {
    let rows = client
        .query(
            "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()",
            &[],
        )
        .await?;
    let enabled: bool = rows
        .first()
        .and_then(|r| r.try_get::<bool, _>(0).ok().flatten())
        .or_else(|| {
            rows.first()
                .and_then(|r| r.try_get::<u8, _>(0).ok().flatten())
                .map(|v| v != 0)
        })
        .unwrap_or(false);
    if enabled {
        return Ok(());
    }
    match client.execute("EXEC sys.sp_cdc_enable_db;", &[]).await {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(
            "CDC is not enabled for this database ({e}). A DBA must run:\n\
             EXEC sys.sp_cdc_enable_db;"
        )),
    }
}

/// Enable CDC on one table, or return the T-SQL a DBA must run.
pub async fn ensure_table_cdc(client: &MssqlClient, table: &QualifiedName) -> Result<()> {
    let rows = client
        .query(
            "SELECT 1 FROM cdc.change_tables ct \
             INNER JOIN sys.tables t ON t.object_id = ct.source_object_id \
             INNER JOIN sys.schemas s ON s.schema_id = t.schema_id \
             WHERE s.name = @P1 AND t.name = @P2",
            &[
                SqlArg::String(table.schema.clone()),
                SqlArg::String(table.table.clone()),
            ],
        )
        .await;
    if let Ok(rows) = &rows {
        if !rows.is_empty() {
            return Ok(());
        }
    }
    let sql = format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'{}', @source_name=N'{}', @role_name=NULL;",
        table.schema.replace('\'', "''"),
        table.table.replace('\'', "''"),
    );
    match client.execute(&sql, &[]).await {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(
            "CDC is not enabled for table `{}` ({e}). A DBA must run:\n{sql}",
            table.dotted()
        )),
    }
}

/// Current maximum captured LSN, or `None` if CDC has not captured anything yet.
pub async fn max_lsn(client: &MssqlClient) -> Result<Option<MssqlLsn>> {
    let rows = client
        .query("SELECT sys.fn_cdc_get_max_lsn()", &[])
        .await
        .map_err(map_cdc_error)?;
    lsn_from_first(&rows)
}

/// Exclusive start LSN (`sys.fn_cdc_increment_lsn`).
pub async fn increment_lsn(client: &MssqlClient, from: &MssqlLsn) -> Result<MssqlLsn> {
    let rows = client
        .query(
            "SELECT sys.fn_cdc_increment_lsn(@P1)",
            &[SqlArg::Bytes(from.0.clone())],
        )
        .await
        .map_err(map_cdc_error)?;
    lsn_from_first(&rows)?.ok_or_else(|| anyhow!("fn_cdc_increment_lsn returned NULL"))
}

fn lsn_from_first(rows: &[tiberius::Row]) -> Result<Option<MssqlLsn>> {
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let bytes: Option<&[u8]> = row.try_get(0).ok().flatten();
    match bytes {
        Some(b) if !b.is_empty() => Ok(Some(MssqlLsn::from_bytes(b.to_vec())?)),
        _ => Ok(None),
    }
}

/// Capture instance name from `cdc.change_tables` (usually `schema_table`).
pub async fn capture_instance(client: &MssqlClient, table: &QualifiedName) -> Result<String> {
    let rows = client
        .query(
            "SELECT ct.capture_instance FROM cdc.change_tables ct \
             INNER JOIN sys.tables t ON t.object_id = ct.source_object_id \
             INNER JOIN sys.schemas s ON s.schema_id = t.schema_id \
             WHERE s.name = @P1 AND t.name = @P2",
            &[
                SqlArg::String(table.schema.clone()),
                SqlArg::String(table.table.clone()),
            ],
        )
        .await
        .map_err(map_cdc_error)?;
    let name: Option<&str> = rows.first().and_then(|r| r.try_get(0).ok().flatten());
    name.map(str::to_string).ok_or_else(|| {
        anyhow!(
            "no CDC capture instance for `{}`. Enable CDC with:\n\
             EXEC sys.sp_cdc_enable_table @source_schema=N'{}', @source_name=N'{}', @role_name=NULL;",
            table.dotted(),
            table.schema,
            table.table
        )
    })
}

/// Poll `cdc.fn_cdc_get_all_changes_<capture>` for `(from_lsn, to_lsn]`.
///
/// `from_lsn` is exclusive (incremented). If `max_lsn <= from`, returns no rows.
/// Update before-images (`__$operation` = 3) are skipped; after-images (4) are kept.
pub async fn poll_changes(
    client: &MssqlClient,
    table: &MssqlTableMeta,
    from_lsn: Option<&MssqlLsn>,
    to_lsn: &MssqlLsn,
) -> Result<Vec<CdcChange>> {
    if let Some(from) = from_lsn {
        if to_lsn <= from {
            return Ok(Vec::new());
        }
    }
    let start = match from_lsn {
        Some(from) => increment_lsn(client, from).await?,
        None => to_lsn.clone(),
    };
    if from_lsn.is_none() || start > *to_lsn {
        return Ok(Vec::new());
    }

    let instance = capture_instance(client, &table.source).await?;
    let fn_name = format!("cdc.fn_cdc_get_all_changes_{}", instance.replace(']', "]]"));
    // `all update old` includes update before-images (`__$operation` 3) so temporal
    // CDC can clear `is_current` on the previous version.
    let sql = format!("SELECT * FROM {fn_name}(@P1, @P2, N'all update old')");
    let rows = match client
        .query(
            &sql,
            &[
                SqlArg::Bytes(start.0.clone()),
                SqlArg::Bytes(to_lsn.0.clone()),
            ],
        )
        .await
    {
        Ok(r) => r,
        Err(e) if is_lsn_range_not_ready(&e) => return Ok(Vec::new()),
        Err(e) => {
            return Err(map_cdc_error(e))
                .with_context(|| format!("polling CDC for {}", table.source))
        }
    };

    let mut out = Vec::new();
    for row in rows {
        let start_lsn = row
            .try_get::<&[u8], _>(0)
            .ok()
            .flatten()
            .ok_or_else(|| anyhow!("CDC row missing __$start_lsn"))?;
        let op: i32 = row.try_get(2).ok().flatten().unwrap_or(0);
        let operation = CdcOperation::from_i32(op)?;
        let mut fields = HashMap::new();
        for col in &table.columns {
            let idx = row
                .columns()
                .iter()
                .position(|c| c.name() == col.name)
                .ok_or_else(|| {
                    anyhow!(
                        "CDC capture for `{}` is missing column `{}`",
                        table.source,
                        col.name
                    )
                })?;
            let value = tiberius_to_value(&row, idx, &col.universal_type)?;
            fields.insert(col.name.clone(), value);
        }
        out.push(CdcChange {
            start_lsn: MssqlLsn::from_bytes(start_lsn.to_vec())?,
            operation,
            source: table.source.clone(),
            fields,
        });
    }
    Ok(out)
}

/// Poll the signal table capture instance (columns: id, kind, tables, consumed).
pub async fn poll_signal_changes(
    client: &MssqlClient,
    signal: &QualifiedName,
    from_lsn: Option<&MssqlLsn>,
    to_lsn: &MssqlLsn,
) -> Result<Vec<CdcChange>> {
    if let Some(from) = from_lsn {
        if to_lsn <= from {
            return Ok(Vec::new());
        }
    }
    let start = match from_lsn {
        Some(from) => increment_lsn(client, from).await?,
        None => return Ok(Vec::new()),
    };
    if start > *to_lsn {
        return Ok(Vec::new());
    }
    let instance = match capture_instance(client, signal).await {
        Ok(i) => i,
        Err(e) => {
            warn!("signal table CDC not ready: {e}");
            return Ok(Vec::new());
        }
    };
    let fn_name = format!("cdc.fn_cdc_get_all_changes_{instance}");
    let sql = format!("SELECT * FROM {fn_name}(@P1, @P2, N'all')");
    let rows = match client
        .query(
            &sql,
            &[
                SqlArg::Bytes(start.0.clone()),
                SqlArg::Bytes(to_lsn.0.clone()),
            ],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("signal CDC poll failed: {e}");
            return Ok(Vec::new());
        }
    };

    let mut out = Vec::new();
    for row in rows {
        let start_lsn = row
            .try_get::<&[u8], _>(0)
            .ok()
            .flatten()
            .ok_or_else(|| anyhow!("signal CDC row missing __$start_lsn"))?;
        let op: i32 = row.try_get(2).ok().flatten().unwrap_or(0);
        let operation = CdcOperation::from_i32(op)?;
        if operation == CdcOperation::UpdateBefore {
            continue;
        }
        let cols = row.columns();
        let id_idx = cols
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case("id"));
        let kind_idx = cols
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case("kind"));
        let tables_idx = cols
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case("tables"));
        let mut fields = HashMap::new();
        if let Some(idx) = id_idx {
            fields.insert("id".into(), tiberius_to_value(&row, idx, &Type::Uuid)?);
        }
        if let Some(idx) = kind_idx {
            fields.insert(
                "kind".into(),
                tiberius_to_value(&row, idx, &Type::VarChar { length: 32 })?,
            );
        }
        if let Some(idx) = tables_idx {
            fields.insert("tables".into(), tiberius_to_value(&row, idx, &Type::Text)?);
        }
        out.push(CdcChange {
            start_lsn: MssqlLsn::from_bytes(start_lsn.to_vec())?,
            operation,
            source: signal.clone(),
            fields,
        });
    }
    Ok(out)
}
