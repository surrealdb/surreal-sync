//! Primary-key / record-ID column helpers shared across sources.
//!
//! - [`apply_id_column_overrides`] — optional per-table PK overrides after schema discovery
//! - [`flatten_composite_id`] — Array IDs → Text joined with a separator (for transforms / relations)

use crate::schema::{ColumnDefinition, DatabaseSchema, TableDefinition};
use crate::types::Type;
use crate::values::Value;
use std::collections::HashMap;
use thiserror::Error;

/// Per-table ordered ID / primary-key column names.
///
/// Keys are table names; values are ordered column lists. A single-column list
/// clears `composite_primary_key`; two or more set it.
pub type IdColumnOverrides = HashMap<String, Vec<String>>;

/// Errors from ID-column override parsing / application.
#[derive(Debug, Error)]
pub enum IdColumnsError {
    #[error("{0}")]
    Message(String),
}

impl IdColumnsError {
    fn msg(s: impl Into<String>) -> Self {
        Self::Message(s.into())
    }
}

/// Parse CLI-style overrides: `table=col1,col2` (repeatable) or a bare
/// `col1,col2` list applied to `default_table` when provided.
///
/// Empty entries are ignored. Column names are trimmed; empty column names error.
pub fn parse_id_column_overrides(
    entries: &[String],
    default_table: Option<&str>,
) -> Result<IdColumnOverrides, IdColumnsError> {
    let mut out = IdColumnOverrides::new();
    for entry in entries {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (table, cols_str) = if let Some((table, cols)) = entry.split_once('=') {
            (table.trim().to_string(), cols)
        } else if let Some(table) = default_table {
            (table.to_string(), entry)
        } else {
            return Err(IdColumnsError::msg(format!(
                "id-columns entry '{entry}' must be table=col1,col2 \
                 (or pass a default table for bare col1,col2 lists)"
            )));
        };
        if table.is_empty() {
            return Err(IdColumnsError::msg(format!(
                "id-columns entry '{entry}' has an empty table name"
            )));
        }
        let cols: Vec<String> = cols_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if cols.is_empty() {
            return Err(IdColumnsError::msg(format!(
                "id-columns entry '{entry}' has no column names"
            )));
        }
        out.insert(table, cols);
    }
    Ok(out)
}

/// Apply per-table ID column overrides onto a discovered [`DatabaseSchema`].
///
/// For each overridden table:
/// - Sets `primary_key` to the first column (type looked up from existing columns,
///   or [`Type::Text`] if missing).
/// - Moves former PK / other columns so all non-first PK columns remain in `columns`.
/// - Sets `composite_primary_key` when `cols.len() > 1`, otherwise clears it.
pub fn apply_id_column_overrides(
    schema: &mut DatabaseSchema,
    overrides: &IdColumnOverrides,
) -> Result<(), IdColumnsError> {
    for (table_name, cols) in overrides {
        let table = schema.get_table_mut(table_name).ok_or_else(|| {
            IdColumnsError::msg(format!(
                "id-columns override for unknown table '{table_name}'"
            ))
        })?;
        apply_override_to_table(table, cols)?;
    }
    Ok(())
}

fn apply_override_to_table(
    table: &mut TableDefinition,
    cols: &[String],
) -> Result<(), IdColumnsError> {
    if cols.is_empty() {
        return Err(IdColumnsError::msg(format!(
            "id-columns override for table '{}' must list at least one column",
            table.name
        )));
    }

    // Gather all existing columns including current PK.
    let mut by_name: HashMap<String, ColumnDefinition> = HashMap::new();
    by_name.insert(table.primary_key.name.clone(), table.primary_key.clone());
    for c in &table.columns {
        by_name.insert(c.name.clone(), c.clone());
    }

    let mut pk_defs = Vec::with_capacity(cols.len());
    for name in cols {
        let def = by_name
            .remove(name)
            .unwrap_or_else(|| ColumnDefinition::new(name.clone(), Type::Text));
        pk_defs.push(def);
    }

    table.primary_key = pk_defs[0].clone();
    // Remaining columns: leftover non-PK columns plus extra PK parts (so fields
    // stay available on the row when sources exclude only the synthetic id).
    let mut other: Vec<ColumnDefinition> = by_name.into_values().collect();
    other.extend(pk_defs.into_iter().skip(1));
    other.sort_by(|a, b| a.name.cmp(&b.name));
    table.columns = other;

    if cols.len() > 1 {
        table.composite_primary_key = Some(cols.to_vec());
    } else {
        table.composite_primary_key = None;
    }
    Ok(())
}

/// Flatten a composite (Array) ID into a Text ID joined with `separator`.
///
/// Scalar IDs are returned unchanged. Used by the `flatten_id` transform and
/// by relation-edge ID flattening.
pub fn flatten_composite_id(id: Value, separator: &str) -> Value {
    match id {
        Value::Array { elements, .. } => {
            let parts: Vec<String> = elements.into_iter().map(stringify_id_part).collect();
            Value::Text(parts.join(separator))
        }
        other => other,
    }
}

/// Stringify one composite-key part for colon/separator joining.
pub fn stringify_id_part(v: Value) -> String {
    match v {
        Value::Int8 { value, .. } => value.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Text(s) => s,
        Value::VarChar { value, .. } | Value::Char { value, .. } => value,
        Value::Uuid(u) => u.to_string(),
        Value::Ulid(u) => u.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => format!("{other:?}"),
    }
}

/// Build a record ID from ordered field values (single → scalar, multi → Array).
pub fn build_composite_record_id(parts: Vec<Value>) -> Value {
    match parts.len() {
        0 => Value::Null,
        1 => parts.into_iter().next().unwrap(),
        _ => Value::Array {
            elements: parts,
            element_type: Box::new(Type::Text),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> DatabaseSchema {
        DatabaseSchema::new(vec![TableDefinition::new(
            "ledger",
            ColumnDefinition::new("a", Type::Int32),
            vec![
                ColumnDefinition::new("b", Type::Int32),
                ColumnDefinition::new("amount", Type::Int64),
            ],
        )])
    }

    #[test]
    fn parse_table_eq_form() {
        let o =
            parse_id_column_overrides(&["ledger=a,b".into(), "tags=tag_id".into()], None).unwrap();
        assert_eq!(o["ledger"], vec!["a", "b"]);
        assert_eq!(o["tags"], vec!["tag_id"]);
    }

    #[test]
    fn parse_bare_with_default_table() {
        let o = parse_id_column_overrides(&["a,b".into()], Some("ledger")).unwrap();
        assert_eq!(o["ledger"], vec!["a", "b"]);
    }

    #[test]
    fn apply_sets_composite() {
        let mut schema = sample_schema();
        let overrides = parse_id_column_overrides(&["ledger=a,b".into()], None).unwrap();
        apply_id_column_overrides(&mut schema, &overrides).unwrap();
        let t = schema.get_table("ledger").unwrap();
        assert_eq!(t.primary_key.name, "a");
        assert_eq!(
            t.composite_primary_key.as_deref(),
            Some(["a".to_string(), "b".to_string()].as_slice())
        );
        assert_eq!(t.primary_key_column_names(), vec!["a", "b"]);
    }

    #[test]
    fn flatten_joins_with_separator() {
        let id = Value::Array {
            elements: vec![Value::Int32(4), Value::Text("x".into())],
            element_type: Box::new(Type::Text),
        };
        assert_eq!(flatten_composite_id(id, ":"), Value::Text("4:x".into()));
    }

    #[test]
    fn build_composite_array() {
        let id = build_composite_record_id(vec![Value::Int32(1), Value::Int32(2)]);
        assert!(matches!(id, Value::Array { elements, .. } if elements.len() == 2));
    }
}
