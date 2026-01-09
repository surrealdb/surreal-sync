//! PostgreSQL schema collection and type mapping
//!
//! This module provides functions for collecting PostgreSQL database schema information
//! and mapping PostgreSQL column types to generic data types for schema-aware conversion.

use postgresql_types::postgresql_column_to_universal_type;
use std::collections::HashMap;
use sync_core::{ColumnDefinition, DatabaseSchema, TableDefinition, UniversalType};

/// Collect schema information for all tables in a PostgreSQL database.
///
/// Returns a `DatabaseSchema` with proper `UniversalType` mapping.
/// This function also queries primary key information for each table.
#[allow(dead_code)]
pub async fn collect_postgresql_database_schema(
    client: &tokio_postgres::Client,
) -> anyhow::Result<DatabaseSchema> {
    // First, collect all columns with their types
    let columns_query = "
        SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position";

    let column_rows = client.query(columns_query, &[]).await?;

    // Also collect primary key information
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

    // Build primary key lookup: table_name -> first PK column name
    let mut pk_columns: HashMap<String, String> = HashMap::new();
    for row in pk_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        // Only store first PK column (for composite PKs, we use the first one)
        pk_columns.entry(table_name).or_insert(column_name);
    }

    // Build tables with columns
    let mut table_columns: HashMap<String, Vec<(String, UniversalType)>> = HashMap::new();

    for row in column_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let data_type: String = row.get(2);
        let precision: Option<i32> = row.get(3);
        let scale: Option<i32> = row.get(4);

        // Convert i32 to u32 for precision/scale
        let precision = precision.map(|p| p as u32);
        let scale = scale.map(|s| s as u32);

        let universal_type = postgresql_column_to_universal_type(&data_type, precision, scale);

        table_columns
            .entry(table_name)
            .or_default()
            .push((column_name, universal_type));
    }

    // Build TableDefinition for each table
    let mut tables = Vec::new();

    for (table_name, columns) in table_columns {
        // Find primary key column or use "id" as default
        let pk_col_name = pk_columns
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| "id".to_string());

        // Find the PK column type and remaining columns
        let mut primary_key: Option<ColumnDefinition> = None;
        let mut other_columns = Vec::new();

        for (col_name, col_type) in columns {
            if col_name == pk_col_name {
                primary_key = Some(ColumnDefinition::new(col_name, col_type));
            } else {
                other_columns.push(ColumnDefinition::new(col_name, col_type));
            }
        }

        // Use the found PK or create a default one
        let pk = primary_key.unwrap_or_else(|| {
            // If no PK column found, create a synthetic one
            ColumnDefinition::new(pk_col_name, UniversalType::Int64)
        });

        tables.push(TableDefinition::new(table_name, pk, other_columns));
    }

    Ok(DatabaseSchema::new(tables))
}
