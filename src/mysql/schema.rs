//! MySQL schema collection and type mapping
//!
//! This module provides functions for collecting MySQL database schema information
//! and mapping MySQL column types to generic data types for schema-aware conversion.

use mysql_types::mysql_column_to_universal_type;
use std::collections::HashMap;
use sync_core::{ColumnDefinition, DatabaseSchema, TableDefinition, UniversalType};

/// Collect schema information for all tables in a MySQL database.
///
/// Returns a `DatabaseSchema` with proper `UniversalType` mapping.
/// This function also queries primary key information for each table.
#[allow(dead_code)]
pub async fn collect_mysql_database_schema(
    conn: &mut mysql_async::Conn,
) -> anyhow::Result<DatabaseSchema> {
    use mysql_async::prelude::*;

    // First, collect all columns with their types
    let columns_query = "
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let column_rows: Vec<mysql_async::Row> = conn.query(columns_query).await?;

    // Also collect primary key information
    let pk_query = "
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_NAME = 'PRIMARY'
            AND TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let pk_rows: Vec<mysql_async::Row> = conn.query(pk_query).await?;

    // Build primary key lookup: table_name -> first PK column name
    let mut pk_columns: HashMap<String, String> = HashMap::new();
    for row in pk_rows {
        let table_name: String = row
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Missing table name in PK query"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("Missing column name in PK query"))?;
        // Only store first PK column (for composite PKs, we use the first one)
        pk_columns.entry(table_name).or_insert(column_name);
    }

    // Build tables with columns
    let mut table_columns: HashMap<String, Vec<(String, UniversalType)>> = HashMap::new();

    for row in column_rows {
        let table_name: String = row
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Missing table name"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("Missing column name"))?;
        let data_type: String = row
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("Missing data type"))?;
        let column_type: String = row
            .get(3)
            .ok_or_else(|| anyhow::anyhow!("Missing column type"))?;
        let precision: Option<u32> = row.get::<Option<u32>, _>(4).unwrap_or(None);
        let scale: Option<u32> = row.get::<Option<u32>, _>(5).unwrap_or(None);

        let universal_type =
            mysql_column_to_universal_type(&data_type, &column_type, precision, scale);

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
