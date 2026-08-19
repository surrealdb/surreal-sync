//! Ordinary (non-temporal) tables: PK record ids, keyset reads, Thing FKs.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use surreal_sync_core::{
    build_composite_record_id, build_relation_from_change, build_relation_from_row, classify_table,
    flatten_composite_id, Change, ChangeOp, ForeignKeyDefinition, Relation, RelationChange, Row,
    TableDefinition, TableKind, Value,
};

use crate::from_mssql::catalog::{MssqlColumnMeta, MssqlTableMeta};
use crate::from_mssql::cdc::{CdcChange, CdcOperation};
use crate::from_mssql::client::{MssqlClient, SqlArg};
use crate::from_mssql::naming::bracket;
use crate::types::tiberius_to_value;

/// Record id from ordered PK values (scalar or array).
pub fn record_id(pk_parts: Vec<Value>) -> Value {
    build_composite_record_id(pk_parts)
}

/// Extract PK field values in catalog order.
pub fn pk_parts(fields: &HashMap<String, Value>, pk_columns: &[String]) -> Vec<Value> {
    pk_columns
        .iter()
        .map(|c| fields.get(c).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Wrap FKs as `record<Target>` except FKs whose referenced table is temporal.
pub fn transform_fks(
    fields: &mut HashMap<String, Value>,
    table_def: &TableDefinition,
    temporal_targets: &HashSet<String>,
) {
    for fk in &table_def.foreign_keys {
        if fk.columns.len() != 1 {
            continue;
        }
        if temporal_targets.contains(&fk.referenced_table) {
            continue;
        }
        let col = &fk.columns[0];
        if let Some(value) = fields.remove(col) {
            let transformed = match value {
                Value::Null => Value::Null,
                other => Value::Thing {
                    table: fk.referenced_table.clone(),
                    id: Box::new(other),
                },
            };
            fields.insert(col.clone(), transformed);
        }
    }
}

pub fn classify(
    table_def: &TableDefinition,
    relation_tables: &[String],
    temporal_targets: &HashSet<String>,
) -> TableKind {
    match classify_table(table_def, relation_tables) {
        TableKind::Relation { in_fk, out_fk }
            if temporal_targets.contains(&in_fk.referenced_table)
                || temporal_targets.contains(&out_fk.referenced_table) =>
        {
            TableKind::Entity
        }
        other => other,
    }
}

/// Convert a SQL Server row into fields keyed by column name.
pub fn fields_from_row(
    row: &tiberius::Row,
    columns: &[MssqlColumnMeta],
) -> Result<HashMap<String, Value>> {
    let mut fields = HashMap::new();
    for (idx, col) in columns.iter().enumerate() {
        let value = tiberius_to_value(row, idx, &col.universal_type)?;
        fields.insert(col.name.clone(), value);
    }
    Ok(fields)
}

/// Build a snapshot [`Row`] (entity) from field map.
pub fn entity_row(
    target: &str,
    index: u64,
    fields: HashMap<String, Value>,
    pk_columns: &[String],
    table_def: Option<&TableDefinition>,
    temporal_targets: &HashSet<String>,
) -> Row {
    let mut fields = fields;
    let id = record_id(pk_parts(&fields, pk_columns));
    if let Some(td) = table_def {
        transform_fks(&mut fields, td, temporal_targets);
    }
    Row::new(target, index, id, fields)
}

/// Build a relation from a join-table field map.
pub fn relation_from_fields(
    target: &str,
    fields: HashMap<String, Value>,
    pk_columns: &[String],
    in_fk: &surreal_sync_core::ForeignKeyDefinition,
    out_fk: &surreal_sync_core::ForeignKeyDefinition,
) -> Relation {
    let id = record_id(pk_parts(&fields, pk_columns));
    build_relation_from_row(target, id, fields, in_fk, out_fk)
}

fn keyset_predicate(pk_columns: &[String], start_param: usize) -> (String, usize) {
    // (c1 > @p) OR (c1 = @p AND c2 > @q) OR …
    let mut clauses = Vec::new();
    let n = pk_columns.len();
    for depth in 0..n {
        let mut parts = Vec::new();
        for (i, col) in pk_columns.iter().enumerate().take(depth) {
            parts.push(format!("{} = @P{}", bracket(col), start_param + i));
        }
        parts.push(format!(
            "{} > @P{}",
            bracket(&pk_columns[depth]),
            start_param + depth
        ));
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    (clauses.join(" OR "), start_param + n)
}

fn order_by(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|c| bracket(c))
        .collect::<Vec<_>>()
        .join(", ")
}

fn select_list(columns: &[MssqlColumnMeta]) -> String {
    columns
        .iter()
        .map(|c| bracket(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Keyset page of an ordinary table: `WHERE (pk) > @after ORDER BY pk FETCH NEXT n`.
pub async fn read_chunk(
    client: &MssqlClient,
    meta: &MssqlTableMeta,
    after: Option<&[Value]>,
    limit: usize,
) -> Result<Vec<HashMap<String, Value>>> {
    let cols = select_list(&meta.columns);
    let order = order_by(&meta.pk_columns);
    let mut sql = format!("SELECT {cols} FROM {}", meta.source.quoted());
    let mut args = Vec::new();
    if let Some(after) = after {
        let (pred, _) = keyset_predicate(&meta.pk_columns, 1);
        sql.push_str(" WHERE ");
        sql.push_str(&pred);
        for v in after {
            args.push(SqlArg::from_value(v)?);
        }
    }
    sql.push_str(&format!(
        " ORDER BY {order} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    ));
    let rows = client.query(&sql, &args).await?;
    let mut out = Vec::new();
    for row in rows {
        out.push(fields_from_row(&row, &meta.columns)?);
    }
    Ok(out)
}

/// CDC row → Change or RelationChange for an ordinary table.
pub fn apply_change(
    meta: &MssqlTableMeta,
    change: &CdcChange,
    table_def: Option<&TableDefinition>,
    relation_tables: &[String],
    temporal_targets: &HashSet<String>,
) -> Result<RegularCdc> {
    let mut fields = change.fields.clone();
    let id = record_id(pk_parts(&fields, &meta.pk_columns));
    if let Some(td) = table_def {
        transform_fks(&mut fields, td, temporal_targets);
        match classify(td, relation_tables, temporal_targets) {
            TableKind::Relation { in_fk, out_fk } => {
                let op = match change.operation {
                    CdcOperation::Delete => ChangeOp::Delete,
                    CdcOperation::Insert => ChangeOp::Create,
                    CdcOperation::UpdateAfter | CdcOperation::UpdateBefore => ChangeOp::Update,
                };
                let relation =
                    build_relation_from_change(&meta.target, id, fields, &in_fk, &out_fk);
                return Ok(RegularCdc::Relation(RelationChange::new(op, relation)));
            }
            TableKind::Entity => {}
        }
    }
    let op = match change.operation {
        CdcOperation::Delete => ChangeOp::Delete,
        CdcOperation::Insert => ChangeOp::Create,
        CdcOperation::UpdateAfter | CdcOperation::UpdateBefore => ChangeOp::Update,
    };
    let fields = if op == ChangeOp::Delete {
        None
    } else {
        Some(fields)
    };
    Ok(RegularCdc::Row(Change::new(op, &meta.target, id, fields)))
}

/// CDC apply result for an ordinary table.
#[allow(clippy::large_enum_variant)]
pub enum RegularCdc {
    Row(Change),
    Relation(RelationChange),
}

/// Flatten snapshot items so interleaved `read_chunk` can sink join tables as rows.
pub fn snapshot_rows(
    meta: &MssqlTableMeta,
    maps: Vec<HashMap<String, Value>>,
    start_index: u64,
    table_def: Option<&TableDefinition>,
    relation_tables: &[String],
    temporal_targets: &HashSet<String>,
) -> Vec<Row> {
    if let Some(td) = table_def {
        if let TableKind::Relation { in_fk, out_fk } =
            classify(td, relation_tables, temporal_targets)
        {
            return maps
                .into_iter()
                .enumerate()
                .map(|(i, fields)| {
                    relation_row_keeping_pk(
                        &meta.target,
                        start_index + i as u64,
                        fields,
                        &meta.pk_columns,
                        &in_fk,
                        &out_fk,
                    )
                })
                .collect();
        }
    }
    let (rows, _) = snapshot_items(
        meta,
        maps,
        start_index,
        table_def,
        relation_tables,
        temporal_targets,
    );
    rows
}

/// Join-table row for interleaved snapshot: keep source PK columns for keyset
/// cursors and add `in` / `out` Things for graph queries.
fn relation_row_keeping_pk(
    target: &str,
    index: u64,
    mut fields: HashMap<String, Value>,
    pk_columns: &[String],
    in_fk: &ForeignKeyDefinition,
    out_fk: &ForeignKeyDefinition,
) -> Row {
    let id = flatten_composite_id(record_id(pk_parts(&fields, pk_columns)), ":");
    add_endpoint_thing(&mut fields, in_fk, "in");
    add_endpoint_thing(&mut fields, out_fk, "out");
    Row::new(target, index, id, fields)
}

fn add_endpoint_thing(fields: &mut HashMap<String, Value>, fk: &ForeignKeyDefinition, name: &str) {
    if let Some(col) = fk.columns.first() {
        if let Some(value) = fields.get(col).cloned() {
            let thing = match value {
                Value::Null => Value::Null,
                other => Value::Thing {
                    table: fk.referenced_table.clone(),
                    id: Box::new(other),
                },
            };
            fields.insert(name.to_string(), thing);
        }
    }
}

/// Relation properties plus `in` / `out` record links.
pub fn relation_fields(rel: &Relation) -> HashMap<String, Value> {
    let mut fields = rel.data.clone();
    fields.insert(
        "in".into(),
        Value::Thing {
            table: rel.input.table.clone(),
            id: Box::new(rel.input.id.clone()),
        },
    );
    fields.insert(
        "out".into(),
        Value::Thing {
            table: rel.output.table.clone(),
            id: Box::new(rel.output.id.clone()),
        },
    );
    fields
}

/// Snapshot items from field maps (entity rows or relations).
pub fn snapshot_items(
    meta: &MssqlTableMeta,
    maps: Vec<HashMap<String, Value>>,
    start_index: u64,
    table_def: Option<&TableDefinition>,
    relation_tables: &[String],
    temporal_targets: &HashSet<String>,
) -> (Vec<Row>, Vec<Relation>) {
    let mut rows = Vec::new();
    let mut rels = Vec::new();
    if let Some(td) = table_def {
        if let TableKind::Relation { in_fk, out_fk } =
            classify(td, relation_tables, temporal_targets)
        {
            for fields in maps {
                rels.push(relation_from_fields(
                    &meta.target,
                    fields,
                    &meta.pk_columns,
                    &in_fk,
                    &out_fk,
                ));
            }
            return (rows, rels);
        }
    }
    for (i, fields) in maps.into_iter().enumerate() {
        rows.push(entity_row(
            &meta.target,
            start_index + i as u64,
            fields,
            &meta.pk_columns,
            table_def,
            temporal_targets,
        ));
    }
    (rows, rels)
}
