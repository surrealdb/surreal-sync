//! SQL Server catalog: tables, PKs, FKs, indexes, temporal kind.

use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use surreal_sync_core::{ColumnDefinition, ForeignKeyDefinition, Type};
use tracing::warn;

use crate::from_mssql::client::MssqlClient;
use crate::from_mssql::naming::{
    detect_collisions, parse_table_ref, target_table_name, QualifiedName,
};
use crate::from_mssql::signal::SIGNAL_TABLE;
use crate::types::mssql_column_to_universal_type;

/// How a selected table is copied (from `sys.tables.temporal_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSyncKind {
    /// Ordinary table: PK record ids and record-link FKs.
    Regular,
    /// System-versioned current table: one Surreal table for all versions.
    Temporal,
}

/// One SQL Server column after type mapping.
#[derive(Debug, Clone)]
pub struct MssqlColumnMeta {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub ordinal: i32,
    pub universal_type: Type,
}

/// A translatable btree index (or a skipped one, which is not stored).
#[derive(Debug, Clone)]
pub struct MssqlIndexMeta {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub is_primary: bool,
}

/// Catalog row for one selected source table (history is nested, not a second target).
#[derive(Debug, Clone)]
pub struct MssqlTableMeta {
    pub source: QualifiedName,
    pub target: String,
    pub kind: TableSyncKind,
    pub object_id: i32,
    pub pk_columns: Vec<String>,
    pub columns: Vec<MssqlColumnMeta>,
    pub indexes: Vec<MssqlIndexMeta>,
    pub foreign_keys: Vec<ForeignKeyDefinition>,
    /// Period start column (temporal tables only).
    pub period_start: Option<String>,
    /// Period end column (temporal tables only).
    pub period_end: Option<String>,
    pub history: Option<QualifiedName>,
    /// `sys.tables.temporal_type`: 0 none, 1 history, 2 current.
    pub temporal_type: u8,
}

impl MssqlTableMeta {
    pub fn column(&self, name: &str) -> Option<&MssqlColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn pk_types(&self) -> Vec<Type> {
        self.pk_columns
            .iter()
            .filter_map(|n| self.column(n).map(|c| c.universal_type.clone()))
            .collect()
    }
}

#[derive(Clone)]
struct RawTable {
    schema: String,
    name: String,
    object_id: i32,
    temporal_type: u8,
    history_table_id: Option<i32>,
}

/// User tables in the current database (excludes CDC, history, and the signal table).
pub async fn list_user_tables(client: &MssqlClient) -> Result<Vec<QualifiedName>> {
    let catalog = load_raw(client).await?;
    let mut out = Vec::new();
    for t in &catalog.tables {
        if t.temporal_type == 1 {
            continue;
        }
        if t.name.eq_ignore_ascii_case(SIGNAL_TABLE) {
            continue;
        }
        if t.schema.eq_ignore_ascii_case("cdc") {
            continue;
        }
        out.push(QualifiedName::new(&t.schema, &t.name));
    }
    Ok(out)
}

/// Load selected tables (or all user tables). History-only selection is an error.
pub async fn collect_database_schema(
    client: &MssqlClient,
    selected: &[String],
) -> Result<Vec<MssqlTableMeta>> {
    let raw = load_raw(client).await?;
    let requested: Vec<QualifiedName> = if selected.is_empty() {
        list_user_tables(client).await?
    } else {
        selected
            .iter()
            .map(|s| parse_table_ref(s))
            .collect::<Result<Vec<_>>>()?
    };

    detect_collisions(&requested)?;

    let mut by_id: HashMap<i32, &RawTable> = HashMap::new();
    let mut by_name: HashMap<(String, String), &RawTable> = HashMap::new();
    for t in &raw.tables {
        by_id.insert(t.object_id, t);
        by_name.insert(
            (t.schema.to_ascii_lowercase(), t.name.to_ascii_lowercase()),
            t,
        );
    }

    let mut metas = Vec::new();
    for name in &requested {
        let key = (
            name.schema.to_ascii_lowercase(),
            name.table.to_ascii_lowercase(),
        );
        let table = by_name.get(&key).ok_or_else(|| {
            anyhow!(
                "table `{}` was not found in the current database",
                name.dotted()
            )
        })?;
        if table.temporal_type == 1 {
            let current = raw
                .tables
                .iter()
                .find(|t| t.history_table_id == Some(table.object_id))
                .map(|t| QualifiedName::new(&t.schema, &t.name).dotted())
                .unwrap_or_else(|| "dbo.<current>".into());
            anyhow::bail!(
                "sync the current table `{current}`, not the history table `{}`",
                name.dotted()
            );
        }
        let kind = if table.temporal_type == 2 {
            TableSyncKind::Temporal
        } else {
            TableSyncKind::Regular
        };
        let history = table.history_table_id.and_then(|id| {
            by_id
                .get(&id)
                .map(|h| QualifiedName::new(&h.schema, &h.name))
        });
        let pk_columns = raw.pks.get(&table.object_id).cloned().unwrap_or_default();
        if pk_columns.is_empty() {
            anyhow::bail!(
                "table `{}` has no primary key; every synced table needs a primary key",
                name.dotted()
            );
        }
        let columns = raw
            .columns
            .get(&table.object_id)
            .cloned()
            .unwrap_or_default();
        let period = raw.periods.get(&table.object_id).cloned();
        let indexes = filter_indexes(
            name,
            raw.indexes.get(&table.object_id).unwrap_or(&Vec::new()),
            &columns,
        );
        let fks = raw
            .fks
            .get(&table.object_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut fk| {
                fk.referenced_table = target_for_source(&fk.referenced_table, &by_name);
                fk
            })
            .collect();

        metas.push(MssqlTableMeta {
            source: name.clone(),
            target: target_table_name(name),
            kind,
            object_id: table.object_id,
            pk_columns,
            columns,
            indexes,
            foreign_keys: fks,
            period_start: period.as_ref().map(|p| p.0.clone()),
            period_end: period.as_ref().map(|p| p.1.clone()),
            history,
            temporal_type: table.temporal_type,
        });
    }
    Ok(metas)
}

fn target_for_source(
    dotted_or_name: &str,
    by_name: &HashMap<(String, String), &RawTable>,
) -> String {
    if let Ok(q) = parse_table_ref(dotted_or_name) {
        if by_name.contains_key(&(q.schema.to_ascii_lowercase(), q.table.to_ascii_lowercase())) {
            return target_table_name(&q);
        }
    }
    dotted_or_name.to_string()
}

struct RawCatalog {
    tables: Vec<RawTable>,
    columns: HashMap<i32, Vec<MssqlColumnMeta>>,
    pks: HashMap<i32, Vec<String>>,
    indexes: HashMap<i32, Vec<RawIndex>>,
    fks: HashMap<i32, Vec<ForeignKeyDefinition>>,
    periods: HashMap<i32, (String, String)>,
}

#[derive(Clone)]
struct RawIndex {
    name: String,
    unique: bool,
    is_primary: bool,
    has_filter: bool,
    is_disabled: bool,
    is_hypothetical: bool,
    type_desc: String,
    columns: Vec<String>,
    include_columns: Vec<String>,
}

async fn load_raw(client: &MssqlClient) -> Result<RawCatalog> {
    let table_rows = client
        .query(
            "SELECT SCHEMA_NAME(t.schema_id), t.name, t.object_id, t.temporal_type, t.history_table_id \
             FROM sys.tables t \
             WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) NOT IN (N'sys', N'cdc')",
            &[],
        )
        .await?;
    let mut tables = Vec::new();
    for row in table_rows {
        let schema: &str = row.try_get(0)?.unwrap_or("dbo");
        let name: &str = row.try_get(1)?.unwrap_or("");
        let object_id: i32 = row.try_get(2)?.unwrap_or(0);
        let temporal_type: u8 = row
            .try_get::<u8, _>(3)
            .ok()
            .flatten()
            .or_else(|| row.try_get::<i32, _>(3).ok().flatten().map(|v| v as u8))
            .unwrap_or(0);
        let history_table_id: Option<i32> = row.try_get(4).ok().flatten();
        tables.push(RawTable {
            schema: schema.to_string(),
            name: name.to_string(),
            object_id,
            temporal_type,
            history_table_id,
        });
    }

    let col_rows = client
        .query(
            "SELECT c.object_id, c.name, ty.name, c.max_length, c.precision, c.scale, \
                    c.is_nullable, c.column_id, \
                    CASE WHEN ty.name IN (N'nchar', N'nvarchar') AND c.max_length > 0 \
                         THEN c.max_length / 2 ELSE CASE WHEN c.max_length < 0 THEN NULL ELSE c.max_length END END \
             FROM sys.columns c \
             INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id \
             INNER JOIN sys.tables t ON t.object_id = c.object_id \
             WHERE t.is_ms_shipped = 0 \
             ORDER BY c.object_id, c.column_id",
            &[],
        )
        .await?;
    let mut columns: HashMap<i32, Vec<MssqlColumnMeta>> = HashMap::new();
    for row in col_rows {
        let object_id: i32 = row.try_get(0)?.unwrap_or(0);
        let name: &str = row.try_get(1)?.unwrap_or("");
        let type_name: &str = row.try_get(2)?.unwrap_or("");
        let max_length: i16 = row.try_get(3)?.unwrap_or(0);
        let precision: u8 = row.try_get(4)?.unwrap_or(0);
        let scale: u8 = row.try_get(5)?.unwrap_or(0);
        let nullable: bool = row.try_get(6)?.unwrap_or(false);
        let ordinal: i32 = row.try_get(7)?.unwrap_or(0);
        let char_len: Option<i32> = row.try_get(8).ok().flatten();
        let max_i32 = max_length as i32;
        let length = char_len.and_then(|n| u16::try_from(n).ok());
        let universal_type = mssql_column_to_universal_type(
            type_name,
            Some(precision),
            Some(scale),
            length,
            Some(max_i32),
        )
        .map_err(|e| anyhow!("table object_id {object_id} column `{name}`: {e}"))?;
        columns.entry(object_id).or_default().push(MssqlColumnMeta {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            ordinal,
            universal_type,
        });
    }

    let pk_rows = client
        .query(
            "SELECT ic.object_id, COL_NAME(ic.object_id, ic.column_id), ic.key_ordinal \
             FROM sys.indexes i \
             INNER JOIN sys.index_columns ic \
               ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
             WHERE i.is_primary_key = 1 AND ic.is_included_column = 0 \
             ORDER BY ic.object_id, ic.key_ordinal",
            &[],
        )
        .await?;
    let mut pks: HashMap<i32, Vec<String>> = HashMap::new();
    for row in pk_rows {
        let object_id: i32 = row.try_get(0)?.unwrap_or(0);
        let col: &str = row.try_get(1)?.unwrap_or("");
        pks.entry(object_id).or_default().push(col.to_string());
    }

    let idx_rows = client
        .query(
            "SELECT i.object_id, i.name, i.is_unique, i.is_primary_key, i.has_filter, \
                    i.is_disabled, i.is_hypothetical, i.type_desc, i.index_id \
             FROM sys.indexes i \
             INNER JOIN sys.tables t ON t.object_id = i.object_id \
             WHERE i.type > 0 AND t.is_ms_shipped = 0",
            &[],
        )
        .await?;
    let ic_rows = client
        .query(
            "SELECT ic.object_id, ic.index_id, COL_NAME(ic.object_id, ic.column_id), \
                    ic.is_included_column, ic.key_ordinal \
             FROM sys.index_columns ic \
             INNER JOIN sys.tables t ON t.object_id = ic.object_id \
             WHERE t.is_ms_shipped = 0 \
             ORDER BY ic.object_id, ic.index_id, ic.key_ordinal",
            &[],
        )
        .await?;
    let mut ic_map: HashMap<(i32, i32), (Vec<String>, Vec<String>)> = HashMap::new();
    for row in ic_rows {
        let object_id: i32 = row.try_get(0)?.unwrap_or(0);
        let index_id: i32 = row.try_get(1)?.unwrap_or(0);
        let col: &str = row.try_get(2)?.unwrap_or("");
        let included: bool = row.try_get(3)?.unwrap_or(false);
        let entry = ic_map.entry((object_id, index_id)).or_default();
        if included {
            entry.1.push(col.to_string());
        } else {
            entry.0.push(col.to_string());
        }
    }
    let mut indexes: HashMap<i32, Vec<RawIndex>> = HashMap::new();
    for row in idx_rows {
        let object_id: i32 = row.try_get(0)?.unwrap_or(0);
        let name: &str = row.try_get(1)?.unwrap_or("");
        let unique: bool = row.try_get(2)?.unwrap_or(false);
        let is_primary: bool = row.try_get(3)?.unwrap_or(false);
        let has_filter: bool = row.try_get(4)?.unwrap_or(false);
        let is_disabled: bool = row.try_get(5)?.unwrap_or(false);
        let is_hypothetical: bool = row.try_get(6)?.unwrap_or(false);
        let type_desc: &str = row.try_get(7)?.unwrap_or("");
        let index_id: i32 = row.try_get(8)?.unwrap_or(0);
        let (cols, includes) = ic_map
            .get(&(object_id, index_id))
            .cloned()
            .unwrap_or_default();
        indexes.entry(object_id).or_default().push(RawIndex {
            name: name.to_string(),
            unique,
            is_primary,
            has_filter,
            is_disabled,
            is_hypothetical,
            type_desc: type_desc.to_string(),
            columns: cols,
            include_columns: includes,
        });
    }

    let fk_rows = client
        .query(
            "SELECT fk.parent_object_id, fk.name, \
                    SCHEMA_NAME(parent_t.schema_id), OBJECT_NAME(fk.parent_object_id), \
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id), \
                    SCHEMA_NAME(ref_t.schema_id), OBJECT_NAME(fk.referenced_object_id), \
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id), \
                    fkc.constraint_column_id \
             FROM sys.foreign_keys fk \
             INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id \
             INNER JOIN sys.tables parent_t ON parent_t.object_id = fk.parent_object_id \
             INNER JOIN sys.tables ref_t ON ref_t.object_id = fk.referenced_object_id \
             ORDER BY fk.parent_object_id, fkc.parent_column_id, fk.object_id, fkc.constraint_column_id",
            &[],
        )
        .await?;
    let mut fk_order: Vec<(i32, String)> = Vec::new();
    let mut fk_acc: HashMap<(i32, String), ForeignKeyDefinition> = HashMap::new();
    for row in fk_rows {
        let parent_id: i32 = row.try_get(0)?.unwrap_or(0);
        let cname: &str = row.try_get(1)?.unwrap_or("");
        let ref_schema: &str = row.try_get(5)?.unwrap_or("dbo");
        let ref_table: &str = row.try_get(6)?.unwrap_or("");
        let parent_col: &str = row.try_get(4)?.unwrap_or("");
        let ref_col: &str = row.try_get(7)?.unwrap_or("");
        let key = (parent_id, cname.to_string());
        if let std::collections::hash_map::Entry::Vacant(e) = fk_acc.entry(key.clone()) {
            fk_order.push(key.clone());
            e.insert(ForeignKeyDefinition {
                constraint_name: cname.to_string(),
                columns: Vec::new(),
                referenced_table: QualifiedName::new(ref_schema, ref_table).dotted(),
                referenced_columns: Vec::new(),
            });
        }
        let entry = fk_acc.get_mut(&key).expect("FK just inserted");
        entry.columns.push(parent_col.to_string());
        entry.referenced_columns.push(ref_col.to_string());
    }
    let mut fks: HashMap<i32, Vec<ForeignKeyDefinition>> = HashMap::new();
    for key in fk_order {
        if let Some(fk) = fk_acc.remove(&key) {
            fks.entry(key.0).or_default().push(fk);
        }
    }

    let period_rows = client
        .query(
            "SELECT p.object_id, COL_NAME(p.object_id, p.start_column_id), \
                    COL_NAME(p.object_id, p.end_column_id) \
             FROM sys.periods p",
            &[],
        )
        .await
        .unwrap_or_default();
    let mut periods = HashMap::new();
    for row in period_rows {
        let object_id: i32 = row.try_get(0)?.unwrap_or(0);
        let start: &str = row.try_get(1)?.unwrap_or("");
        let end: &str = row.try_get(2)?.unwrap_or("");
        periods.insert(object_id, (start.to_string(), end.to_string()));
    }

    Ok(RawCatalog {
        tables,
        columns,
        pks,
        indexes,
        fks,
        periods,
    })
}

fn filter_indexes(
    table: &QualifiedName,
    indexes: &[RawIndex],
    columns: &[MssqlColumnMeta],
) -> Vec<MssqlIndexMeta> {
    let known: HashSet<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    let mut out = Vec::new();
    for idx in indexes {
        let reason = if idx.is_disabled {
            Some("disabled")
        } else if idx.is_hypothetical {
            Some("hypothetical")
        } else if idx.has_filter {
            Some("filtered (WHERE)")
        } else if !idx.include_columns.is_empty() {
            Some("INCLUDE columns")
        } else if idx_type_untranslatable(&idx.type_desc) {
            Some(idx.type_desc.as_str())
        } else if idx.columns.iter().any(|c| !known.contains(c.as_str())) {
            Some("column was not copied")
        } else {
            None
        };
        if let Some(reason) = reason {
            warn!(
                "Skipping index `{}` on `{}`: {reason}",
                idx.name,
                table.dotted()
            );
            continue;
        }
        out.push(MssqlIndexMeta {
            name: idx.name.clone(),
            columns: idx.columns.clone(),
            unique: idx.unique,
            is_primary: idx.is_primary,
        });
    }
    out
}

fn idx_type_untranslatable(type_desc: &str) -> bool {
    let u = type_desc.to_ascii_uppercase();
    u.contains("XML")
        || u.contains("SPATIAL")
        || u.contains("COLUMNSTORE")
        || u.contains("FULLTEXT")
        || u.contains("HASH")
}

/// Column definitions for SCHEMAFULL / FK mapping (PK first, then the rest).
pub fn column_definitions(meta: &MssqlTableMeta) -> (ColumnDefinition, Vec<ColumnDefinition>) {
    let pk_name = meta
        .pk_columns
        .first()
        .cloned()
        .unwrap_or_else(|| "id".into());
    let pk_col = meta.column(&pk_name);
    let primary_key = match pk_col {
        Some(c) => {
            let mut d = ColumnDefinition::new(&c.name, c.universal_type.clone());
            d.nullable = c.nullable;
            d
        }
        None => ColumnDefinition::new(pk_name, Type::Text),
    };
    let rest = meta
        .columns
        .iter()
        .filter(|c| Some(c.name.as_str()) != meta.pk_columns.first().map(|s| s.as_str()))
        .map(|c| {
            let mut d = ColumnDefinition::new(&c.name, c.universal_type.clone());
            d.nullable = c.nullable;
            d
        })
        .collect();
    (primary_key, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_sync_kind_is_regular_or_temporal() {
        assert_ne!(TableSyncKind::Regular, TableSyncKind::Temporal);
    }
}
