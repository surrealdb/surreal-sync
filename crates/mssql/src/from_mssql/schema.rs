//! Map the SQL Server catalog to `DatabaseSchema` and SCHEMAFULL extras.

use std::collections::HashSet;
use surreal_sync_core::{
    ColumnDefinition, DatabaseSchema, PeriodBound, SchemaIndex, SchemafullExtras, TableDefinition,
};

use crate::from_mssql::catalog::{column_definitions, MssqlTableMeta, TableSyncKind};
use crate::from_mssql::temporal::{cookbook_indexes, extra_is_current_field};

/// Surreal target names of system-versioned tables (FKs to these stay scalars).
pub fn temporal_targets(metas: &[MssqlTableMeta]) -> HashSet<String> {
    metas
        .iter()
        .filter(|m| m.kind == TableSyncKind::Temporal)
        .map(|m| m.target.clone())
        .collect()
}

/// Build a [`DatabaseSchema`] using Surreal target table names.
pub fn database_schema(metas: &[MssqlTableMeta]) -> DatabaseSchema {
    let tables = metas
        .iter()
        .map(|meta| {
            let (primary_key, columns) = column_definitions(meta);
            let mut extra_cols = columns;
            if meta.kind == TableSyncKind::Temporal {
                extra_cols.push(ColumnDefinition::new(
                    "is_current",
                    surreal_sync_core::Type::Bool,
                ));
            }
            let mut td = TableDefinition::new(&meta.target, primary_key, extra_cols);
            td.foreign_keys = meta.foreign_keys.clone();
            if meta.pk_columns.len() > 1 {
                td.composite_primary_key = Some(meta.pk_columns.clone());
            }
            td
        })
        .collect();
    DatabaseSchema::new(tables)
}

/// SCHEMAFULL extras: ordinary btree indexes, temporal cookbook indexes, period ASSERT, `is_current`.
pub fn schemafull_extras(
    metas: &[MssqlTableMeta],
    relation_tables: Vec<String>,
) -> SchemafullExtras {
    let scalar_fk_targets = temporal_targets(metas);
    let mut extras = SchemafullExtras {
        scalar_fk_targets,
        relation_tables,
        ..SchemafullExtras::default()
    };
    for meta in metas {
        match meta.kind {
            TableSyncKind::Regular => {
                for idx in &meta.indexes {
                    if idx.is_primary {
                        continue;
                    }
                    extras.indexes.push(SchemaIndex {
                        table: meta.target.clone(),
                        name: idx.name.clone(),
                        fields: idx.columns.clone(),
                        unique: idx.unique,
                    });
                }
            }
            TableSyncKind::Temporal => {
                extras.indexes.extend(cookbook_indexes(meta));
                extras.extra_fields.push(extra_is_current_field(meta));
                if let (Some(start), Some(end)) = (&meta.period_start, &meta.period_end) {
                    extras.period_bounds.push(PeriodBound {
                        table: meta.target.clone(),
                        start: start.clone(),
                        end: end.clone(),
                    });
                }
            }
        }
    }
    extras
}
