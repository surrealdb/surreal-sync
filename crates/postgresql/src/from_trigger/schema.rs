//! PostgreSQL schema collection and type mapping
//!
//! This module provides functions for collecting PostgreSQL database schema information
//! and mapping PostgreSQL column types to generic data types for schema-aware conversion.

use crate::types::postgresql_column_to_universal_type;
use std::collections::HashMap;
use surreal_sync_core::{ColumnDefinition, DatabaseSchema, TableDefinition, Type};

/// Collect schema information for all tables in a PostgreSQL database.
///
/// Returns a `DatabaseSchema` with proper `Type` mapping.
/// This function also queries primary key information for each table.
pub async fn collect_postgresql_database_schema(
    client: &tokio_postgres::Client,
) -> anyhow::Result<DatabaseSchema> {
    // First, collect all columns with their types
    // Note: For array types, data_type returns 'ARRAY' but udt_name contains the actual type
    // like '_text' for text[], '_int4' for integer[]. We use udt_name for proper array handling.
    let columns_query = "
        SELECT table_name, column_name, data_type, udt_name, numeric_precision, numeric_scale
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

    // Build primary key lookup: table_name -> ordered PK column names
    let mut pk_columns: HashMap<String, Vec<String>> = HashMap::new();
    for row in pk_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        pk_columns.entry(table_name).or_default().push(column_name);
    }

    // Build tables with columns
    let mut table_columns: HashMap<String, Vec<(String, Type)>> = HashMap::new();

    for row in column_rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let data_type: String = row.get(2);
        let udt_name: String = row.get(3);
        let precision: Option<i32> = row.get(4);
        let scale: Option<i32> = row.get(5);

        // Convert i32 to u32 for precision/scale
        let precision = precision.map(|p| p as u32);
        let scale = scale.map(|s| s as u32);

        // For array types, data_type is 'ARRAY' but udt_name contains the internal type
        // like '_text' for text[], '_int4' for integer[]. Use udt_name for proper handling.
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

    // Build TableDefinition for each table
    let mut tables = Vec::new();

    for (table_name, columns) in table_columns {
        // Find primary key columns or use "id" as default
        let pk_list = pk_columns
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| vec!["id".to_string()]);
        let pk_col_name = pk_list[0].clone();

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
            ColumnDefinition::new(pk_col_name, Type::Int64)
        });

        let mut table_def = TableDefinition::new(table_name, pk, other_columns);
        if pk_list.len() > 1 {
            table_def.composite_primary_key = Some(pk_list);
        }
        tables.push(table_def);
    }

    Ok(DatabaseSchema::new(tables))
}

/// Determine the effective type string to use for type mapping.
/// For array types, PostgreSQL's information_schema returns 'ARRAY' as data_type
/// but the actual type (like '_text' for text[]) is in udt_name.
#[inline]
pub fn get_effective_type<'a>(data_type: &'a str, udt_name: &'a str) -> &'a str {
    if data_type.to_lowercase() == "array" {
        udt_name
    } else {
        data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_effective_type_for_array() {
        // PostgreSQL returns data_type='ARRAY' and udt_name='_text' for TEXT[]
        assert_eq!(get_effective_type("ARRAY", "_text"), "_text");
        assert_eq!(get_effective_type("array", "_text"), "_text");
        assert_eq!(get_effective_type("ARRAY", "_int4"), "_int4");
    }

    #[test]
    fn test_get_effective_type_for_non_array() {
        // Non-array types should use data_type directly
        assert_eq!(get_effective_type("integer", "int4"), "integer");
        assert_eq!(get_effective_type("text", "text"), "text");
        assert_eq!(get_effective_type("boolean", "bool"), "boolean");
    }

    #[test]
    fn test_array_type_maps_to_universal_array() {
        // Verify the full flow: ARRAY + _text -> Type::Array<Text>
        let effective_type = get_effective_type("ARRAY", "_text");
        let universal_type = postgresql_column_to_universal_type(effective_type, None, None);

        match universal_type {
            Type::Array { element_type } => {
                assert_eq!(*element_type, Type::Text);
            }
            other => panic!("Expected Array<Text>, got {other:?}"),
        }
    }
}
