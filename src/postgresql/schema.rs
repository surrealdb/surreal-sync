//! PostgreSQL schema collection and type mapping
//!
//! This module provides functions for collecting PostgreSQL database schema information
//! and mapping PostgreSQL column types to generic data types for schema-aware conversion.

use crate::schema::{DatabaseSchema, GenericDataType, TableSchema};
use std::collections::HashMap;

/// Convert PostgreSQL column type information to GenericDataType
pub fn column_type_to_generic_data_type(
    data_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> GenericDataType {
    match data_type.to_lowercase().as_str() {
        "numeric" | "decimal" => GenericDataType::Decimal { precision, scale },
        "timestamp" | "timestamp without time zone" => GenericDataType::Timestamp,
        "timestamptz" | "timestamp with time zone" => GenericDataType::TimestampWithTimezone,
        "uuid" => GenericDataType::Uuid,
        "json" | "jsonb" => GenericDataType::Json,
        "boolean" | "bool" => GenericDataType::Boolean,
        "integer" | "int4" | "bigint" | "int8" | "smallint" | "int2" => GenericDataType::Integer,
        "real" | "float4" | "double precision" | "float8" => GenericDataType::Float,
        "text" | "varchar" | "char" | "character varying" | "character" => GenericDataType::String,
        "bytea" => GenericDataType::Bytes,
        "date" => GenericDataType::Date,
        "time" | "time without time zone" => GenericDataType::Time,
        "interval" => GenericDataType::Duration,
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            GenericDataType::Geometry
        }
        _ => GenericDataType::SourceSpecific(data_type.to_string()),
    }
}

/// Collect schema information for all tables in a PostgreSQL database
pub async fn collect_postgresql_schema(
    client: &tokio_postgres::Client,
    _database: &str,
) -> anyhow::Result<DatabaseSchema> {
    let query = "
        SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position";

    let rows = client.query(query, &[]).await?;
    let mut tables = HashMap::new();

    for row in rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let data_type: String = row.get(2);
        let precision: Option<i32> = row.get(3);
        let scale: Option<i32> = row.get(4);

        // Convert i32 to u32 for precision/scale
        let precision = precision.map(|p| p as u32);
        let scale = scale.map(|s| s as u32);

        let generic_type = column_type_to_generic_data_type(&data_type, precision, scale);

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
