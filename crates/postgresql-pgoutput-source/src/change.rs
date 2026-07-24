//! Convert pgoutput CDC changes into sync-core universal changes.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pg_walstream::RowData;
use pgoutput_protocol::{CdcChange, RelationMeta, RowChange};
use postgres_types::Type as PgType;
use sync_core::{Change, ChangeOp, DatabaseSchema, Type, Value};

pub fn cdc_to_change(
    change: &CdcChange,
    relation: &RelationMeta,
    column_names: &[String],
    schema: &DatabaseSchema,
) -> Result<Change> {
    let table_def = schema
        .get_table(&change.table)
        .ok_or_else(|| anyhow!("unknown table '{}' in schema", change.table))?;

    let pk_columns = table_def
        .primary_key_column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

    let (op, row_data): (ChangeOp, &RowData) = match &change.operation {
        RowChange::Insert { new } => (ChangeOp::Create, new),
        RowChange::Update { new, .. } => (ChangeOp::Update, new.as_ref()),
        RowChange::Delete { old } => (ChangeOp::Delete, old),
    };

    let mut values = HashMap::new();
    if relation.columns.is_empty() {
        for col_name in column_names {
            let cell = row_data
                .get(col_name.as_str())
                .ok_or_else(|| anyhow!("missing column '{col_name}' in row data"))?;
            let universal = column_value_to_universal(cell, &PgType::TEXT, table_def, col_name)?;
            values.insert(col_name.clone(), universal);
        }
    } else {
        if column_names.len() != relation.columns.len() {
            return Err(anyhow!(
                "column count mismatch for table '{}'",
                change.table
            ));
        }
        for (col_name, col_meta) in column_names.iter().zip(relation.columns.iter()) {
            let cell = row_data
                .get(col_name.as_str())
                .ok_or_else(|| anyhow!("missing column '{col_name}' in row data"))?;
            let pg_type = PgType::from_oid(col_meta.type_id).unwrap_or(PgType::TEXT);
            let universal = column_value_to_universal(cell, &pg_type, table_def, col_name)?;
            values.insert(col_name.clone(), universal);
        }
    }

    let id = extract_primary_key(&values, &pk_columns)?;
    let data = if op == ChangeOp::Delete {
        None
    } else {
        let fields: HashMap<String, Value> = values
            .into_iter()
            .filter(|(name, _)| !pk_columns.contains(name))
            .collect();
        Some(fields)
    };

    Ok(Change::new(op, change.table.clone(), id, data))
}

fn column_value_to_universal(
    cell: &pg_walstream::ColumnValue,
    pg_type: &PgType,
    table_def: &sync_core::TableDefinition,
    column_name: &str,
) -> Result<Value> {
    if cell.is_null() {
        return Ok(Value::Null);
    }

    let universal_type = table_def
        .get_column_type(column_name)
        .cloned()
        .unwrap_or(Type::Text);

    match cell {
        pg_walstream::ColumnValue::Text(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|e| anyhow!("invalid UTF-8 in column '{column_name}': {e}"))?;
            parse_text_value(text, &universal_type, pg_type)
        }
        pg_walstream::ColumnValue::Binary(bytes) => Ok(Value::Bytes(bytes.to_vec())),
        pg_walstream::ColumnValue::Null => Ok(Value::Null),
    }
}

fn parse_text_value(text: &str, universal_type: &Type, pg_type: &PgType) -> Result<Value> {
    if text == "null" {
        return Ok(Value::Null);
    }
    match universal_type {
        Type::Bool => Ok(Value::Bool(text == "t" || text == "true")),
        Type::Int16 => text
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|e| anyhow!("{e}")),
        Type::Int32 => text
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|e| anyhow!("{e}")),
        Type::Int64 => text
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| anyhow!("{e}")),
        Type::Float32 => text
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|e| anyhow!("{e}")),
        Type::Float64 => text
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| anyhow!("{e}")),
        Type::Uuid => {
            let u = uuid::Uuid::parse_str(text).map_err(|e| anyhow!("{e}"))?;
            Ok(Value::Uuid(u))
        }
        Type::Json | Type::Jsonb => {
            let parsed: serde_json::Value = serde_json::from_str(text)?;
            Ok(Value::Json(Box::new(parsed)))
        }
        Type::Enum { values, .. } => {
            if values.iter().any(|v| v == text) {
                Ok(Value::Enum {
                    value: text.to_string(),
                    allowed_values: values.clone(),
                })
            } else {
                tracing::warn!("enum value '{text}' not in allowed set for type {pg_type:?}");
                Ok(Value::Text(text.to_string()))
            }
        }
        Type::ZonedDateTime => Ok(Value::ZonedDateTime(parse_postgres_timestamptz(text)?)),
        Type::LocalDateTime => {
            let naive = parse_postgres_timestamp(text)?;
            Ok(Value::LocalDateTime(
                DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc),
            ))
        }
        Type::Date => {
            let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_err(|e| anyhow!("invalid date '{text}': {e}"))?;
            let dt = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow!("invalid date '{text}'"))?;
            Ok(Value::Date(DateTime::<Utc>::from_naive_utc_and_offset(
                dt, Utc,
            )))
        }
        Type::Array { element_type, .. } => parse_postgres_text_array(text, element_type),
        Type::Decimal { precision, scale } => Ok(Value::Decimal {
            value: text.to_string(),
            precision: *precision,
            scale: *scale,
        }),
        Type::Time => {
            let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
                .map_err(|e| anyhow!("invalid time '{text}': {e}"))?;
            let dt = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(time);
            Ok(Value::Time(DateTime::<Utc>::from_naive_utc_and_offset(
                dt, Utc,
            )))
        }
        _ => Ok(Value::Text(text.to_string())),
    }
}

fn parse_postgres_timestamptz(text: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Ok(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f %z",
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(text, fmt) {
            return Ok(dt.with_timezone(&Utc));
        }
    }
    let naive = parse_postgres_timestamp(text)?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn parse_postgres_text_array(text: &str, element_type: &Type) -> Result<Value> {
    let inner = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| anyhow!("invalid PostgreSQL array literal: {text}"))?;
    if inner.is_empty() {
        return Ok(Value::Array {
            elements: Vec::new(),
            element_type: Box::new(element_type.clone()),
        });
    }
    let elements = inner
        .split(',')
        .map(|item| {
            let item = item.trim().trim_matches('"');
            parse_text_value(item, element_type, &PgType::TEXT)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Array {
        elements,
        element_type: Box::new(element_type.clone()),
    })
}

fn parse_postgres_timestamp(text: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| anyhow!("invalid timestamp '{text}': {e}"))
}

fn extract_primary_key(values: &HashMap<String, Value>, pk_columns: &[String]) -> Result<Value> {
    if pk_columns.len() == 1 {
        return values
            .get(&pk_columns[0])
            .cloned()
            .ok_or_else(|| anyhow!("missing primary key column '{}'", pk_columns[0]));
    }
    let elements: Vec<Value> = pk_columns
        .iter()
        .map(|col| {
            values
                .get(col)
                .cloned()
                .ok_or_else(|| anyhow!("missing PK column '{col}'"))
        })
        .collect::<Result<_>>()?;
    Ok(Value::Array {
        elements,
        element_type: Box::new(Type::Text),
    })
}
