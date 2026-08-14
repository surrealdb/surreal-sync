//! System-versioned temporal tables: version ids, UNION ALL snapshot, is_current.

use anyhow::Result;
use chrono::Datelike;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use surreal_sync_core::{
    build_composite_record_id, Change, ChangeOp, Row, SchemaFieldExtra, SchemaIndex, Value,
};

use crate::from_mssql::catalog::{MssqlColumnMeta, MssqlTableMeta};
use crate::from_mssql::cdc::{CdcChange, CdcOperation};
use crate::from_mssql::client::{MssqlClient, SqlArg};
use crate::from_mssql::naming::bracket;
use crate::types::tiberius_to_value;

/// Hash remaining field values (sorted keys) for the version id.
pub fn hash_version_content(fields: &HashMap<String, Value>) -> String {
    let mut keys: Vec<&String> = fields.keys().collect();
    keys.sort();
    let mut hasher = Sha256::new();
    for k in keys {
        hasher.update(k.as_bytes());
        hasher.update(b"=");
        let json = serde_json::to_string(fields.get(k).unwrap()).unwrap_or_default();
        hasher.update(json.as_bytes());
        hasher.update(b";");
    }
    hex::encode(hasher.finalize())
}

fn iso8601(value: &Value) -> String {
    match value {
        Value::LocalDateTime(dt)
        | Value::LocalDateTimeNano(dt)
        | Value::ZonedDateTime(dt)
        | Value::Date(dt)
        | Value::Time(dt) => dt.to_rfc3339(),
        Value::Text(s) | Value::VarChar { value: s, .. } | Value::Char { value: s, .. } => {
            s.clone()
        }
        other => format!("{other:?}"),
    }
}

/// Array record id: PK parts + period start + period end + content hash + duplicate ordinal.
pub fn version_id(
    pk_parts: Vec<Value>,
    period_start: &str,
    period_end: &str,
    remaining_fields: &HashMap<String, Value>,
    duplicate_ordinal: i64,
) -> Value {
    let hash = hash_version_content(remaining_fields);
    let mut parts = pk_parts;
    parts.push(Value::Text(period_start.to_string()));
    parts.push(Value::Text(period_end.to_string()));
    parts.push(Value::Text(hash));
    parts.push(Value::Int64(duplicate_ordinal));
    build_composite_record_id(parts)
}

/// True when ValidTo is the open sentinel (year 9999).
pub fn is_open_period_end(value: &Value) -> bool {
    match value {
        Value::LocalDateTime(dt)
        | Value::LocalDateTimeNano(dt)
        | Value::ZonedDateTime(dt)
        | Value::Date(dt) => dt.year() >= 9999,
        _ => false,
    }
}

fn pk_parts(fields: &HashMap<String, Value>, pk_columns: &[String]) -> Vec<Value> {
    pk_columns
        .iter()
        .map(|c| fields.get(c).cloned().unwrap_or(Value::Null))
        .collect()
}

fn remaining_for_hash(
    fields: &HashMap<String, Value>,
    pk_columns: &[String],
    period_start: &str,
    period_end: &str,
) -> HashMap<String, Value> {
    fields
        .iter()
        .filter(|(k, _)| {
            *k != period_start
                && *k != period_end
                && *k != "is_current"
                && !pk_columns.iter().any(|p| p == *k)
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Assign version ids and `is_current` for a snapshot chunk (duplicate ordinals are local).
pub fn rows_from_maps(
    meta: &MssqlTableMeta,
    maps: Vec<HashMap<String, Value>>,
    start_index: u64,
) -> Vec<Row> {
    let start = meta.period_start.as_deref().unwrap_or("ValidFrom");
    let end = meta.period_end.as_deref().unwrap_or("ValidTo");
    let mut seen: HashMap<String, i64> = HashMap::new();
    let mut rows = Vec::new();
    for (i, mut fields) in maps.into_iter().enumerate() {
        let from = iso8601(fields.get(start).unwrap_or(&Value::Null));
        let to = iso8601(fields.get(end).unwrap_or(&Value::Null));
        let is_current = fields
            .get("is_current")
            .and_then(|v| match v {
                Value::Bool(b) => Some(*b),
                Value::Int16(n) => Some(*n != 0),
                Value::Int32(n) => Some(*n != 0),
                Value::Int64(n) => Some(*n != 0),
                _ => None,
            })
            .unwrap_or_else(|| is_open_period_end(fields.get(end).unwrap_or(&Value::Null)));
        fields.insert("is_current".into(), Value::Bool(is_current));
        let remaining = remaining_for_hash(&fields, &meta.pk_columns, start, end);
        let hash = hash_version_content(&remaining);
        let pk = pk_parts(&fields, &meta.pk_columns);
        let key = format!(
            "{}|{from}|{to}|{hash}",
            serde_json::to_string(&pk).unwrap_or_default()
        );
        let ordinal = seen.entry(key).or_insert(-1);
        *ordinal += 1;
        let id = version_id(pk, &from, &to, &remaining, *ordinal);
        rows.push(Row::new(
            meta.target.clone(),
            start_index + i as u64,
            id,
            fields,
        ));
    }
    rows
}

fn union_sql(meta: &MssqlTableMeta) -> Result<String> {
    let history = meta.history.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "temporal table `{}` has no history table in the catalog",
            meta.source
        )
    })?;
    let start = meta.period_start.as_deref().unwrap_or("ValidFrom");
    let end = meta.period_end.as_deref().unwrap_or("ValidTo");
    let mut names: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();
    for extra in [start, end] {
        if !names.iter().any(|n| n.eq_ignore_ascii_case(extra)) {
            names.push(extra.to_string());
        }
    }
    let cols: Vec<String> = names.iter().map(|c| bracket(c)).collect();
    // Period columns may be HIDDEN; name them explicitly. Do not use FOR SYSTEM_TIME ALL
    // (it drops same-transaction zero-duration versions).
    let current_select = format!(
        "SELECT {}, CAST(1 AS bit) AS {} FROM {}",
        cols.join(", "),
        bracket("is_current"),
        meta.source.quoted()
    );
    let history_select = format!(
        "SELECT {}, CAST(0 AS bit) AS {} FROM {}",
        cols.join(", "),
        bracket("is_current"),
        history.quoted()
    );
    Ok(format!("{current_select} UNION ALL {history_select}"))
}

fn keyset_order(meta: &MssqlTableMeta) -> Vec<String> {
    let start = meta
        .period_start
        .clone()
        .unwrap_or_else(|| "ValidFrom".into());
    let end = meta.period_end.clone().unwrap_or_else(|| "ValidTo".into());
    let mut cols = meta.pk_columns.clone();
    cols.push(start);
    cols.push(end);
    cols
}

fn keyset_predicate(cols: &[String], start_param: usize) -> String {
    let mut clauses = Vec::new();
    for depth in 0..cols.len() {
        let mut parts = Vec::new();
        for (i, col) in cols.iter().enumerate().take(depth) {
            parts.push(format!("{} = @P{}", bracket(col), start_param + i));
        }
        parts.push(format!(
            "{} > @P{}",
            bracket(&cols[depth]),
            start_param + depth
        ));
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    clauses.join(" OR ")
}

/// UNION ALL current + history, keyset-ordered by business PK + period start + period end.
pub async fn read_chunk(
    client: &MssqlClient,
    meta: &MssqlTableMeta,
    after: Option<&[Value]>,
    limit: usize,
) -> Result<Vec<HashMap<String, Value>>> {
    let inner = union_sql(meta)?;
    let order_cols = keyset_order(meta);
    let order = order_cols
        .iter()
        .map(|c| bracket(c))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT * FROM ({inner}) AS versions");
    let mut args = Vec::new();
    if let Some(after) = after {
        sql.push_str(" WHERE ");
        sql.push_str(&keyset_predicate(&order_cols, 1));
        for v in after {
            args.push(SqlArg::from_value(v)?);
        }
    }
    sql.push_str(&format!(
        " ORDER BY {order} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    ));
    let rows = client.query(&sql, &args).await?;
    // UNION ALL adds is_current as the last column.
    let mut columns: Vec<MssqlColumnMeta> = meta.columns.clone();
    columns.push(MssqlColumnMeta {
        name: "is_current".into(),
        type_name: "bit".into(),
        nullable: false,
        ordinal: i32::MAX,
        universal_type: surreal_sync_core::Type::Bool,
    });
    let mut out = Vec::new();
    for row in rows {
        let mut fields = HashMap::new();
        for (idx, col) in columns.iter().enumerate() {
            fields.insert(
                col.name.clone(),
                tiberius_to_value(&row, idx, &col.universal_type)?,
            );
        }
        out.push(fields);
    }
    Ok(out)
}

/// Cookbook indexes for the unified temporal table (never copy source UNIQUE/PK).
pub fn cookbook_indexes(meta: &MssqlTableMeta) -> Vec<SchemaIndex> {
    let start = meta
        .period_start
        .clone()
        .unwrap_or_else(|| "ValidFrom".into());
    let end = meta.period_end.clone().unwrap_or_else(|| "ValidTo".into());
    let t = &meta.target;
    let mut pk_and_current = meta.pk_columns.clone();
    pk_and_current.push("is_current".into());
    vec![
        SchemaIndex {
            table: t.clone(),
            name: format!("{t}_is_current"),
            fields: vec!["is_current".into()],
            unique: false,
        },
        SchemaIndex {
            table: t.clone(),
            name: format!("{t}_pk"),
            fields: meta.pk_columns.clone(),
            unique: false,
        },
        SchemaIndex {
            table: t.clone(),
            name: format!("{t}_pk_is_current"),
            fields: pk_and_current,
            unique: false,
        },
        SchemaIndex {
            table: t.clone(),
            name: format!("{t}_period"),
            fields: vec![start, end],
            unique: false,
        },
    ]
}

/// Extra `is_current` field for SCHEMAFULL.
pub fn extra_is_current_field(meta: &MssqlTableMeta) -> SchemaFieldExtra {
    SchemaFieldExtra {
        table: meta.target.clone(),
        name: "is_current".into(),
        type_name: "bool".into(),
        nullable: false,
    }
}

fn version_change(
    meta: &MssqlTableMeta,
    mut fields: HashMap<String, Value>,
    is_current: bool,
    op: ChangeOp,
) -> Change {
    let start = meta.period_start.as_deref().unwrap_or("ValidFrom");
    let end = meta.period_end.as_deref().unwrap_or("ValidTo");
    fields.insert("is_current".into(), Value::Bool(is_current));
    let from = iso8601(fields.get(start).unwrap_or(&Value::Null));
    let to = iso8601(fields.get(end).unwrap_or(&Value::Null));
    let remaining = remaining_for_hash(&fields, &meta.pk_columns, start, end);
    let id = version_id(
        pk_parts(&fields, &meta.pk_columns),
        &from,
        &to,
        &remaining,
        0,
    );
    Change::new(op, &meta.target, id, Some(fields))
}

/// CDC apply: INSERT/UPDATE write a new current version; before-image/DELETE clear `is_current`.
pub fn apply_change(meta: &MssqlTableMeta, change: &CdcChange) -> Result<TemporalCdc> {
    match change.operation {
        CdcOperation::Insert | CdcOperation::UpdateAfter => {
            let new_version = version_change(meta, change.fields.clone(), true, ChangeOp::Create);
            Ok(TemporalCdc {
                new_version: Some(new_version),
            })
        }
        CdcOperation::Delete | CdcOperation::UpdateBefore => {
            let prior = version_change(meta, change.fields.clone(), false, ChangeOp::Update);
            Ok(TemporalCdc {
                new_version: Some(prior),
            })
        }
    }
}

/// CDC translation for a temporal current-table change.
pub struct TemporalCdc {
    pub new_version: Option<Change>,
}

/// Keyset cursor values for the next page (business PK + period start + period end).
pub fn chunk_after_from_row(meta: &MssqlTableMeta, row: &Row) -> Vec<Value> {
    let start = meta.period_start.as_deref().unwrap_or("ValidFrom");
    let end = meta.period_end.as_deref().unwrap_or("ValidTo");
    let mut vals = Vec::new();
    for c in &meta.pk_columns {
        vals.push(row.fields.get(c).cloned().unwrap_or(Value::Null));
    }
    vals.push(row.fields.get(start).cloned().unwrap_or(Value::Null));
    vals.push(row.fields.get(end).cloned().unwrap_or(Value::Null));
    vals
}
