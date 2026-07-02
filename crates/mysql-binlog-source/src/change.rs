//! Convert binlog CDC changes into sync-core universal changes.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use binlog_protocol::{CdcChange, CellValue, ColumnDef, RowChange, TableMapEvent};
use mysql_types::{binlog_cell_to_universal_value, BinlogColumnMeta, RowConversionConfig};
use sync_core::{
    DatabaseSchema, UniversalChange, UniversalChangeOp, UniversalType, UniversalValue,
};

pub fn cdc_change_to_universal(
    change: &CdcChange,
    table_map: &TableMapEvent,
    column_names: &[String],
    schema: &DatabaseSchema,
    json_columns: &HashMap<String, Vec<String>>,
) -> Result<UniversalChange> {
    let table_def = schema
        .get_table(&change.table)
        .ok_or_else(|| anyhow!("unknown table '{}' in schema", change.table))?;

    let pk_columns = table_pk_columns(table_def);
    let config = row_conversion_config(schema, &change.table, json_columns);

    let update_merged;
    let (op, cells): (UniversalChangeOp, &[CellValue]) = match &change.operation {
        RowChange::Insert(values) => (UniversalChangeOp::Create, values.as_slice()),
        RowChange::Update { before, after } => {
            let pk_indices = pk_column_indices(column_names, &pk_columns);
            update_merged = merge_update_cells(before, after, &pk_indices);
            (UniversalChangeOp::Update, update_merged.as_slice())
        }
        RowChange::Delete(values) => (UniversalChangeOp::Delete, values.as_slice()),
    };

    if cells.len() != table_map.columns.len() {
        return Err(anyhow!(
            "column count mismatch for table '{}': got {} cells, expected {}",
            change.table,
            cells.len(),
            table_map.columns.len()
        ));
    }

    if column_names.len() != table_map.columns.len() {
        return Err(anyhow!(
            "schema/binlog column count mismatch for table '{}'",
            change.table
        ));
    }

    let mut values = HashMap::new();
    for ((col_def, cell), column_name) in table_map
        .columns
        .iter()
        .zip(cells.iter())
        .zip(column_names.iter())
    {
        let meta = binlog_column_meta(col_def, column_name, table_def, &config);
        let mut universal = binlog_cell_to_universal_value(cell, &meta, &config)
            .map_err(|e| anyhow!("convert column '{column_name}': {e}"))?;
        universal = fix_set_value_from_bitmask(column_name, table_def, universal);
        values.insert(column_name.clone(), universal);
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

fn table_pk_columns(table_def: &sync_core::TableDefinition) -> Vec<String> {
    table_def
        .primary_key_column_names()
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn row_conversion_config(
    schema: &DatabaseSchema,
    table: &str,
    json_columns: &HashMap<String, Vec<String>>,
) -> RowConversionConfig {
    let mut boolean_columns = Vec::new();
    let mut set_columns = Vec::new();
    if let Some(table_def) = schema.get_table(table) {
        for name in table_def.column_names() {
            match table_def.get_column_type(name) {
                Some(UniversalType::Bool) => boolean_columns.push(name.to_string()),
                Some(UniversalType::Set { .. }) => set_columns.push(name.to_string()),
                _ => {}
            }
        }
    }
    RowConversionConfig {
        boolean_columns,
        set_columns,
        json_columns: json_columns.get(table).cloned().unwrap_or_default(),
        json_config: None,
    }
}

fn binlog_column_meta(
    col_def: &ColumnDef,
    column_name: &str,
    table_def: &sync_core::TableDefinition,
    config: &RowConversionConfig,
) -> BinlogColumnMeta {
    let is_binary = matches!(
        table_def.get_column_type(column_name),
        Some(UniversalType::Blob | UniversalType::Bytes)
    );
    BinlogColumnMeta::new(column_name, col_def.column_type, col_def.metadata.clone())
        .with_unsigned(col_def.unsigned)
        .with_boolean_hint(config.boolean_columns.iter().any(|c| c == column_name))
        .with_binary(is_binary)
}

fn fix_set_value_from_bitmask(
    column_name: &str,
    table_def: &sync_core::TableDefinition,
    value: UniversalValue,
) -> UniversalValue {
    let Some(UniversalType::Set { values: allowed }) = table_def.get_column_type(column_name)
    else {
        return value;
    };
    if allowed.is_empty() {
        return value;
    }

    let mask = match &value {
        UniversalValue::Set { elements, .. } if elements.len() == 1 => {
            elements[0].parse::<u64>().ok()
        }
        UniversalValue::Text(s) => s.parse::<u64>().ok(),
        UniversalValue::VarChar { value: s, .. } => s.parse::<u64>().ok(),
        _ => None,
    };
    let Some(mask) = mask else {
        return value;
    };

    let mut selected = Vec::new();
    for (idx, label) in allowed.iter().enumerate() {
        if mask & (1u64 << idx) != 0 {
            selected.push(label.clone());
        }
    }
    UniversalValue::Set {
        elements: selected,
        allowed_values: allowed.clone(),
    }
}

fn pk_column_indices(column_names: &[String], pk_columns: &[String]) -> Vec<usize> {
    pk_columns
        .iter()
        .filter_map(|pk| column_names.iter().position(|name| name == pk))
        .collect()
}

fn merge_update_cells(
    before: &[CellValue],
    after: &[CellValue],
    pk_indices: &[usize],
) -> Vec<CellValue> {
    debug_assert_eq!(before.len(), after.len());
    before
        .iter()
        .zip(after.iter())
        .enumerate()
        .map(|(idx, (prev, next))| {
            if pk_indices.contains(&idx) {
                match prev {
                    CellValue::Null => next.clone(),
                    other => other.clone(),
                }
            } else {
                match next {
                    CellValue::Null => prev.clone(),
                    other => other.clone(),
                }
            }
        })
        .collect()
}

fn extract_primary_key(
    values: &HashMap<String, UniversalValue>,
    pk_columns: &[String],
) -> Result<UniversalValue> {
    if pk_columns.is_empty() {
        return Err(anyhow!("table has no primary key"));
    }
    if pk_columns.len() == 1 {
        return values
            .get(&pk_columns[0])
            .cloned()
            .ok_or_else(|| anyhow!("primary key column '{}' missing", pk_columns[0]));
    }
    let mut parts = Vec::new();
    for col in pk_columns {
        parts.push(
            values
                .get(col)
                .cloned()
                .ok_or_else(|| anyhow!("primary key column '{col}' missing"))?,
        );
    }
    Ok(UniversalValue::Array {
        elements: parts,
        element_type: Box::new(UniversalType::Text),
    })
}
