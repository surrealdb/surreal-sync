//! MySQL schema collection and type mapping
//!
//! This module provides functions for collecting MySQL database schema information
//! and mapping MySQL column types to generic data types for schema-aware conversion.

use crate::surreal::{LegacySchema, LegacyTableDefinition, LegacyType};
use std::collections::HashMap;

/// Convert MySQL column type information to LegacyType
pub fn mysql_column_type_to_legacy_type(
    data_type: &str,
    column_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> LegacyType {
    match data_type.to_uppercase().as_str() {
        "DECIMAL" | "NUMERIC" => LegacyType::Decimal { precision, scale },
        "TIMESTAMP" | "DATETIME" => LegacyType::DateTime,
        "JSON" => LegacyType::Json,
        "BOOLEAN" | "BOOL" => LegacyType::Bool,
        "TINYINT" => {
            // TINYINT(1) is commonly used for boolean in MySQL
            if column_type.to_lowercase().starts_with("tinyint(1)") {
                LegacyType::Bool
            } else {
                LegacyType::Int
            }
        }
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "MEDIUMINT" => LegacyType::Int,
        "FLOAT" | "DOUBLE" | "REAL" => LegacyType::Float,
        "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => LegacyType::String,
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            LegacyType::Bytes
        }
        "DATE" => LegacyType::Date,
        "TIME" => LegacyType::Time,
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" => LegacyType::Geometry,
        "SET" => LegacyType::Array(Box::new(LegacyType::String)),
        _ => LegacyType::SourceSpecific(data_type.to_string()),
    }
}

/// Collect schema information for all tables in a MySQL database
pub async fn collect_mysql_schema(
    conn: &mut mysql_async::Conn,
    _database: &str,
) -> anyhow::Result<LegacySchema> {
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
            mysql_column_type_to_legacy_type(&data_type, &column_type, precision, scale);

        let table_schema =
            tables
                .entry(table_name.clone())
                .or_insert_with(|| LegacyTableDefinition {
                    table_name: table_name.clone(),
                    columns: HashMap::new(),
                });

        table_schema.columns.insert(column_name, generic_type);
    }

    Ok(LegacySchema { tables })
}
