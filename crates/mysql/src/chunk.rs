//! Shared MySQL table-read helpers used by trigger and binlog sources.

use crate::{row_to_typed_values_with_config, RowConversionConfig};
use anyhow::Result;
use mysql_async::{prelude::*, Params, Row as MysqlRow, Value as MysqlValue};
use std::collections::HashMap;
use surreal_sync_core::{Row, Type, Value};
use tracing::debug;

/// Get primary key column names for a table, in key ordinal order (returns an
/// empty vec if the table has no primary key).
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

    let pk_columns: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("COLUMN_NAME"))
        .collect();

    Ok(pk_columns)
}

pub struct TableChunk {
    /// The converted rows, in primary-key order.
    pub rows: Vec<Row>,
    /// Primary-key values of the last row in `rows`, in primary-key column
    /// order. `None` when no rows were returned. Pass this back as `after` to
    /// read the following chunk.
    pub last_pk: Option<Vec<Value>>,
}

/// Read a single primary-key-ordered chunk of a table using keyset pagination.
///
/// Rows are ordered by the table's primary key column(s). When `after` is
/// `None`, reading starts from the beginning; otherwise only rows strictly
/// greater than `after` (by row-value comparison over the primary-key columns)
/// are returned. A single primary-key column degenerates to `pk > ?`; a
/// composite primary key uses `(a, b, ...) > (?, ?, ...)`. At most `limit` rows
/// are returned.
///
/// This is an additive, chunked alternative to the monolithic `SELECT *` full
/// sync; it does not write to a sink. `config` supplies the schema-aware
/// boolean/SET column handling used elsewhere in this module.
pub async fn read_table_chunk(
    conn: &mut mysql_async::Conn,
    table_name: &str,
    pk_columns: &[String],
    after: Option<&[Value]>,
    limit: usize,
    config: &RowConversionConfig,
) -> Result<TableChunk> {
    if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table '{table_name}' has no primary key columns; keyset chunk reads require a primary key"
        ));
    }

    let order_by = pk_columns.join(", ");

    let (where_clause, bind_values): (String, Vec<MysqlValue>) = match after {
        Some(cursor) => {
            if cursor.len() != pk_columns.len() {
                return Err(anyhow::anyhow!(
                    "Keyset cursor length ({}) does not match primary key column count ({}) for table '{table_name}'",
                    cursor.len(),
                    pk_columns.len()
                ));
            }
            let placeholders = vec!["?"; pk_columns.len()];
            let clause = if pk_columns.len() == 1 {
                format!("WHERE {} > ?", pk_columns[0])
            } else {
                format!(
                    "WHERE ({}) > ({})",
                    pk_columns.join(", "),
                    placeholders.join(", ")
                )
            };
            let values = cursor
                .iter()
                .map(pk_value_to_mysql_value)
                .collect::<Result<Vec<_>>>()?;
            (clause, values)
        }
        None => (String::new(), Vec::new()),
    };

    let query =
        format!("SELECT * FROM {table_name} {where_clause} ORDER BY {order_by} LIMIT {limit}");
    debug!("Chunk-reading table {table_name} with: {query}");

    let rows: Vec<MysqlRow> = conn
        .exec(query, Params::Positional(bind_values))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read chunk from table {table_name}: {e}"))?;

    let mut out = Vec::with_capacity(rows.len());
    let mut last_pk: Option<Vec<Value>> = None;

    for (row_index, row) in rows.iter().enumerate() {
        let typed_values = row_to_typed_values_with_config(row, config)?;
        let values: HashMap<String, Value> = typed_values
            .into_iter()
            .map(|(k, tv)| (k, tv.value))
            .collect();

        let id = extract_primary_key_value(&values, pk_columns)?;

        let mut cursor_values = Vec::with_capacity(pk_columns.len());
        for col in pk_columns {
            let v = values.get(col).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Primary key column '{col}' not found in row for table '{table_name}'"
                )
            })?;
            cursor_values.push(v);
        }
        last_pk = Some(cursor_values);

        let fields: HashMap<String, Value> = values
            .into_iter()
            .filter(|(k, _)| !pk_columns.contains(k))
            .collect();

        out.push(Row::new(
            table_name.to_string(),
            row_index as u64,
            id,
            fields,
        ));
    }

    Ok(TableChunk { rows: out, last_pk })
}

/// Convert a primary-key `Value` into a MySQL bind value for keyset
/// pagination.
fn pk_value_to_mysql_value(value: &Value) -> Result<MysqlValue> {
    Ok(match value {
        Value::Int8 { value, .. } => MysqlValue::Int(*value as i64),
        Value::Int16(v) => MysqlValue::Int(*v as i64),
        Value::Int32(v) => MysqlValue::Int(*v as i64),
        Value::Int64(v) => MysqlValue::Int(*v),
        Value::Text(v) => MysqlValue::Bytes(v.clone().into_bytes()),
        Value::VarChar { value, .. } => MysqlValue::Bytes(value.clone().into_bytes()),
        Value::Char { value, .. } => MysqlValue::Bytes(value.clone().into_bytes()),
        Value::Uuid(v) => MysqlValue::Bytes(v.to_string().into_bytes()),
        other => {
            return Err(anyhow::anyhow!(
                "Unsupported primary key value type for keyset pagination: {other:?}"
            ))
        }
    })
}

/// Extract primary key value from values map
fn extract_primary_key_value(
    values: &HashMap<String, Value>,
    pk_columns: &[String],
) -> Result<Value> {
    if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table has no primary key defined - primary key is required for sync"
        ));
    }

    if pk_columns.len() == 1 {
        // Single primary key column
        let pk_col = &pk_columns[0];
        values
            .get(pk_col)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Primary key column '{pk_col}' not found in row"))
    } else {
        // Composite primary key - create an array
        let mut pk_values = Vec::new();
        for col in pk_columns {
            let v = values
                .get(col)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Primary key column '{col}' not found in row"))?;
            pk_values.push(v);
        }
        Ok(Value::Array {
            elements: pk_values,
            element_type: Box::new(Type::Text),
        })
    }
}
