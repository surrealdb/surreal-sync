//! PostgreSQL schema collection, foreign key introspection, and enrichment.
//!
//! Provides functions for collecting table/column metadata and foreign key
//! constraints from PostgreSQL, producing a `DatabaseSchema` with FK info
//! ready for record-link and relation-table classification.

use anyhow::Result;
use postgresql_types::postgresql_column_to_universal_type;
use std::collections::HashMap;
use sync_core::{
    ColumnDefinition, DatabaseSchema, ForeignKeyDefinition, TableDefinition, UniversalType,
};
use tokio_postgres::Client;

/// (source_columns, referenced_table, referenced_columns) grouped per constraint.
type FkConstraintEntry = (Vec<String>, String, Vec<String>);

/// Collect base database schema (tables, columns, primary keys) from PostgreSQL.
///
/// This mirrors the logic in `postgresql-trigger-source/src/schema.rs` but
/// lives in the shared crate so both source crates can use it.
pub async fn collect_database_schema(client: &Client) -> Result<DatabaseSchema> {
    let columns_query = "
        SELECT table_name, column_name, data_type, udt_name, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position";

    let column_rows = client.query(columns_query, &[]).await?;

    let pk_query = "
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        ORDER BY tc.table_name, kcu.ordinal_position";

    let pk_rows = client.query(pk_query, &[]).await?;

    let mut pk_columns: HashMap<String, String> = HashMap::new();
    for row in pk_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        pk_columns.entry(table_name).or_insert(column_name);
    }

    let mut table_columns: HashMap<String, Vec<(String, UniversalType)>> = HashMap::new();

    for row in column_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let data_type: String = row.get(2);
        let udt_name: String = row.get(3);
        let precision: Option<i32> = row.get(4);
        let scale: Option<i32> = row.get(5);

        let precision = precision.map(|p| p as u32);
        let scale = scale.map(|s| s as u32);

        let effective_type = if data_type.to_lowercase() == "array" {
            &udt_name
        } else {
            &data_type
        };

        let universal_type = postgresql_column_to_universal_type(effective_type, precision, scale);

        table_columns
            .entry(table_name)
            .or_default()
            .push((column_name, universal_type));
    }

    let mut tables = Vec::new();

    for (table_name, columns) in table_columns {
        let pk_col_name = pk_columns
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| "id".to_string());

        let mut primary_key: Option<ColumnDefinition> = None;
        let mut other_columns = Vec::new();

        for (col_name, col_type) in columns {
            if col_name == pk_col_name {
                primary_key = Some(ColumnDefinition::new(col_name, col_type));
            } else {
                other_columns.push(ColumnDefinition::new(col_name, col_type));
            }
        }

        let pk =
            primary_key.unwrap_or_else(|| ColumnDefinition::new(pk_col_name, UniversalType::Int64));

        tables.push(TableDefinition::new(table_name, pk, other_columns));
    }

    Ok(DatabaseSchema::new(tables))
}

/// Collect a complete `DatabaseSchema` with FK and composite PK info.
///
/// Combines base schema collection with FK enrichment in a single call.
pub async fn collect_database_schema_with_fks(client: &Client) -> Result<DatabaseSchema> {
    let mut schema = collect_database_schema(client).await?;
    enrich_schema_with_fks(client, &mut schema).await?;
    Ok(schema)
}

/// Collect all foreign key constraints from the `public` schema.
///
/// Returns a map from table name to the list of `ForeignKeyDefinition`s
/// for that table.
pub async fn collect_foreign_keys(
    client: &Client,
) -> Result<HashMap<String, Vec<ForeignKeyDefinition>>> {
    let fk_query = "
        SELECT
            tc.constraint_name,
            kcu.table_name,
            kcu.column_name,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position";

    let rows = client.query(fk_query, &[]).await?;

    // Group by (table_name, constraint_name)
    let mut constraint_map: HashMap<(String, String), FkConstraintEntry> = HashMap::new();

    for row in rows {
        let constraint_name: String = row.get(0);
        let table_name: String = row.get(1);
        let column_name: String = row.get(2);
        let referenced_table: String = row.get(3);
        let referenced_column: String = row.get(4);

        let key = (table_name, constraint_name);
        let entry = constraint_map
            .entry(key)
            .or_insert_with(|| (Vec::new(), referenced_table, Vec::new()));
        entry.0.push(column_name);
        entry.2.push(referenced_column);
    }

    // Convert to table_name -> Vec<ForeignKeyDefinition>, sorted by constraint name
    // for deterministic in_fk / out_fk assignment in relation classification.
    let mut result: HashMap<String, Vec<ForeignKeyDefinition>> = HashMap::new();
    let mut sorted_entries: Vec<_> = constraint_map.into_iter().collect();
    sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

    for ((table_name, constraint_name), (columns, referenced_table, referenced_columns)) in
        sorted_entries
    {
        result
            .entry(table_name)
            .or_default()
            .push(ForeignKeyDefinition {
                constraint_name,
                columns,
                referenced_table,
                referenced_columns,
            });
    }

    Ok(result)
}

/// Collect composite primary key info from PostgreSQL for all public tables.
///
/// Returns a map from table name to ordered list of PK column names.
/// Only includes tables with composite (multi-column) primary keys.
pub async fn collect_composite_primary_keys(
    client: &Client,
) -> Result<HashMap<String, Vec<String>>> {
    let pk_query = "
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        ORDER BY tc.table_name, kcu.ordinal_position";

    let rows = client.query(pk_query, &[]).await?;

    let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        pk_map.entry(table_name).or_default().push(column_name);
    }

    // Only keep composite keys (2+ columns)
    pk_map.retain(|_, cols| cols.len() > 1);
    Ok(pk_map)
}

/// Enrich a `DatabaseSchema` with foreign key and composite PK information.
///
/// Attaches `ForeignKeyDefinition`s and `composite_primary_key` data
/// to each `TableDefinition` in the schema.
pub async fn enrich_schema_with_fks(
    client: &Client,
    schema: &mut sync_core::DatabaseSchema,
) -> Result<()> {
    let fk_map = collect_foreign_keys(client).await?;
    let cpk_map = collect_composite_primary_keys(client).await?;

    for table in &mut schema.tables {
        if let Some(fks) = fk_map.get(&table.name) {
            table.foreign_keys = fks.clone();
        }
        if let Some(cpk) = cpk_map.get(&table.name) {
            table.composite_primary_key = Some(cpk.clone());
        }
    }

    Ok(())
}
