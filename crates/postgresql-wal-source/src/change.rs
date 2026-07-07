//! Convert pgoutput CDC changes into sync-core universal changes.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pg_walstream::RowData;
use pgoutput_protocol::{CdcChange, RelationMeta, RowChange};
use postgres_types::Type;
use sync_core::{
    DatabaseSchema, UniversalChange, UniversalChangeOp, UniversalType, UniversalValue,
};

pub fn cdc_change_to_universal(
    change: &CdcChange,
    relation: &RelationMeta,
    column_names: &[String],
    schema: &DatabaseSchema,
) -> Result<UniversalChange> {
    let table_def = schema
        .get_table(&change.table)
        .ok_or_else(|| anyhow!("unknown table '{}' in schema", change.table))?;

    let pk_columns = table_def
        .primary_key_column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

    let (op, row_data): (UniversalChangeOp, &RowData) = match &change.operation {
        RowChange::Insert { new } => (UniversalChangeOp::Create, new),
        RowChange::Update { new, .. } => (UniversalChangeOp::Update, new.as_ref()),
        RowChange::Delete { old } => (UniversalChangeOp::Delete, old),
    };

    let mut values = HashMap::new();
    if relation.columns.is_empty() {
        for col_name in column_names {
            let cell = row_data
                .get(col_name.as_str())
                .ok_or_else(|| anyhow!("missing column '{col_name}' in row data"))?;
            let universal = column_value_to_universal(cell, &Type::TEXT, table_def, col_name)?;
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
            let pg_type = Type::from_oid(col_meta.type_id).unwrap_or(Type::TEXT);
            let universal = column_value_to_universal(cell, &pg_type, table_def, col_name)?;
            values.insert(col_name.clone(), universal);
        }
    }

    let id = extract_primary_key(&values, &pk_columns)?;
    let data = if op == UniversalChangeOp::Delete {
        None
    } else {
        let fields: HashMap<String, UniversalValue> = values
            .into_iter()
            .filter(|(name, _)| !pk_columns.contains(name))
            .collect();
        Some(fields)
    };

    Ok(UniversalChange::new(op, change.table.clone(), id, data))
}

fn column_value_to_universal(
    cell: &pg_walstream::ColumnValue,
    pg_type: &Type,
    table_def: &sync_core::TableDefinition,
    column_name: &str,
) -> Result<UniversalValue> {
    if cell.is_null() {
        return Ok(UniversalValue::Null);
    }

    let universal_type = table_def
        .get_column_type(column_name)
        .cloned()
        .unwrap_or(UniversalType::Text);

    match cell {
        pg_walstream::ColumnValue::Text(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|e| anyhow!("invalid UTF-8 in column '{column_name}': {e}"))?;
            parse_text_value(text, &universal_type, pg_type)
        }
        pg_walstream::ColumnValue::Binary(bytes) => Ok(UniversalValue::Bytes(bytes.to_vec())),
        pg_walstream::ColumnValue::Null => Ok(UniversalValue::Null),
    }
}

fn parse_text_value(
    text: &str,
    universal_type: &UniversalType,
    pg_type: &Type,
) -> Result<UniversalValue> {
    if text == "null" {
        return Ok(UniversalValue::Null);
    }
    match universal_type {
        UniversalType::Bool => Ok(UniversalValue::Bool(text == "t" || text == "true")),
        UniversalType::Int16 => text
            .parse::<i16>()
            .map(UniversalValue::Int16)
            .map_err(|e| anyhow!("{e}")),
        UniversalType::Int32 => text
            .parse::<i32>()
            .map(UniversalValue::Int32)
            .map_err(|e| anyhow!("{e}")),
        UniversalType::Int64 => text
            .parse::<i64>()
            .map(UniversalValue::Int64)
            .map_err(|e| anyhow!("{e}")),
        UniversalType::Float32 => text
            .parse::<f32>()
            .map(UniversalValue::Float32)
            .map_err(|e| anyhow!("{e}")),
        UniversalType::Float64 => text
            .parse::<f64>()
            .map(UniversalValue::Float64)
            .map_err(|e| anyhow!("{e}")),
        UniversalType::Uuid => {
            let u = uuid::Uuid::parse_str(text).map_err(|e| anyhow!("{e}"))?;
            Ok(UniversalValue::Uuid(u))
        }
        UniversalType::Json | UniversalType::Jsonb => {
            let parsed: serde_json::Value = serde_json::from_str(text)?;
            Ok(UniversalValue::Json(Box::new(parsed)))
        }
        UniversalType::Enum { values, .. } => {
            if values.iter().any(|v| v == text) {
                Ok(UniversalValue::Enum {
                    value: text.to_string(),
                    allowed_values: values.clone(),
                })
            } else {
                tracing::warn!("enum value '{text}' not in allowed set for type {pg_type:?}");
                Ok(UniversalValue::Text(text.to_string()))
            }
        }
        UniversalType::ZonedDateTime => Ok(UniversalValue::ZonedDateTime(
            parse_postgres_timestamptz(text)?,
        )),
        UniversalType::LocalDateTime => {
            let naive = parse_postgres_timestamp(text)?;
            Ok(UniversalValue::LocalDateTime(
                DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc),
            ))
        }
        UniversalType::Date => {
            let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_err(|e| anyhow!("invalid date '{text}': {e}"))?;
            let dt = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow!("invalid date '{text}'"))?;
            Ok(UniversalValue::Date(
                DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc),
            ))
        }
        UniversalType::Array { element_type, .. } => parse_postgres_text_array(text, element_type),
        UniversalType::Decimal { precision, scale } => Ok(UniversalValue::Decimal {
            value: text.to_string(),
            precision: *precision,
            scale: *scale,
        }),
        UniversalType::Time => {
            let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
                .map_err(|e| anyhow!("invalid time '{text}': {e}"))?;
            let dt = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(time);
            Ok(UniversalValue::Time(
                DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc),
            ))
        }
        _ => Ok(UniversalValue::Text(text.to_string())),
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

fn parse_postgres_text_array(text: &str, element_type: &UniversalType) -> Result<UniversalValue> {
    let inner = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| anyhow!("invalid PostgreSQL array literal: {text}"))?;
    if inner.is_empty() {
        return Ok(UniversalValue::Array {
            elements: Vec::new(),
            element_type: Box::new(element_type.clone()),
        });
    }
    let elements = inner
        .split(',')
        .map(|item| {
            let item = item.trim().trim_matches('"');
            parse_text_value(item, element_type, &Type::TEXT)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(UniversalValue::Array {
        elements,
        element_type: Box::new(element_type.clone()),
    })
}

fn parse_postgres_timestamp(text: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| anyhow!("invalid timestamp '{text}': {e}"))
}

fn extract_primary_key(
    values: &HashMap<String, UniversalValue>,
    pk_columns: &[String],
) -> Result<UniversalValue> {
    if pk_columns.len() == 1 {
        return values
            .get(&pk_columns[0])
            .cloned()
            .ok_or_else(|| anyhow!("missing primary key column '{}'", pk_columns[0]));
    }
    let elements: Vec<UniversalValue> = pk_columns
        .iter()
        .map(|col| {
            values
                .get(col)
                .cloned()
                .ok_or_else(|| anyhow!("missing PK column '{col}'"))
        })
        .collect::<Result<_>>()?;
    Ok(UniversalValue::Array {
        elements,
        element_type: Box::new(UniversalType::Text),
    })
}
