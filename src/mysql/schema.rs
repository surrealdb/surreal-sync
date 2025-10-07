//! MySQL schema collection and type mapping
//!
//! This module provides functions for collecting MySQL database schema information
//! and mapping MySQL column types to generic data types for schema-aware conversion.

use crate::schema::{DatabaseSchema, GenericDataType, TableSchema};
use mysql_async::prelude::*;
use std::collections::HashMap;

/// Convert MySQL column type information to GenericDataType
pub fn column_type_to_generic_data_type(
    data_type: &str,
    column_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> GenericDataType {
    match data_type.to_uppercase().as_str() {
        "DECIMAL" | "NUMERIC" => GenericDataType::Decimal { precision, scale },
        "TIMESTAMP" | "DATETIME" => GenericDataType::Timestamp,
        "JSON" => GenericDataType::Json,
        "BOOLEAN" | "BOOL" => GenericDataType::Boolean,
        "TINYINT" => {
            // TINYINT(1) is commonly used for boolean in MySQL
            if column_type.to_lowercase().starts_with("tinyint(1)") {
                GenericDataType::Boolean
            } else {
                GenericDataType::Integer
            }
        }
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "MEDIUMINT" => GenericDataType::Integer,
        "FLOAT" | "DOUBLE" | "REAL" => GenericDataType::Float,
        "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
            GenericDataType::String
        }
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            GenericDataType::Bytes
        }
        "DATE" => GenericDataType::Date,
        "TIME" => GenericDataType::Time,
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" => GenericDataType::Geometry,
        "SET" => GenericDataType::Array(Box::new(GenericDataType::String)),
        _ => GenericDataType::SourceSpecific(data_type.to_string()),
    }
}

/// Collect schema information for all tables in a MySQL database
pub async fn collect_mysql_schema(
    conn: &mut mysql_async::Conn,
    _database: &str,
) -> anyhow::Result<DatabaseSchema> {
    use mysql_async::prelude::*;

    let query = "
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let rows: Vec<mysql_async::Row> = conn.query(query).await?;
    let mut tables = HashMap::new();

    for row in rows {
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

        let generic_type =
            column_type_to_generic_data_type(&data_type, &column_type, precision, scale);

        let table_schema = tables
            .entry(table_name.clone())
            .or_insert_with(|| TableSchema {
                table_name: table_name.clone(),
                columns: HashMap::new(),
            });

        table_schema.columns.insert(column_name, generic_type);
    }

    Ok(DatabaseSchema { tables })
}

/// Get primary key columns for a table
pub async fn get_primary_key_columns(
    conn: &mut mysql_async::Conn,
    table_name: &str,
) -> anyhow::Result<Vec<String>> {
    let query = "
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    ";

    let columns: Vec<String> = conn.exec(query, (table_name,)).await?;

    if columns.is_empty() {
        // If no primary key, use 'id' column if it exists
        Ok(vec!["id".to_string()])
    } else {
        Ok(columns)
    }
}
