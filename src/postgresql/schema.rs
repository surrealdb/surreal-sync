//! PostgreSQL schema collection and type mapping
//!
//! This module provides functions for collecting PostgreSQL database schema information
//! and mapping PostgreSQL column types to generic data types for schema-aware conversion.

use crate::surreal::{LegacySchema, LegacyTableDefinition, LegacyType};
use std::collections::HashMap;

/// Convert PostgreSQL column type information to LegacyType
pub fn postgresql_column_type_to_legacy_type(
    data_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> LegacyType {
    match data_type.to_lowercase().as_str() {
        "numeric" | "decimal" => LegacyType::Decimal { precision, scale },
        "timestamp" | "timestamp without time zone" => LegacyType::DateTime,
        "timestamptz" | "timestamp with time zone" => LegacyType::TimestampTz,
        "uuid" => LegacyType::Uuid,
        "json" | "jsonb" => LegacyType::Json,
        "boolean" | "bool" => LegacyType::Bool,
        "integer" | "int4" | "bigint" | "int8" | "smallint" | "int2" => LegacyType::Int,
        "real" | "float4" | "double precision" | "float8" => LegacyType::Float,
        "text" | "varchar" | "char" | "character varying" | "character" => LegacyType::String,
        "bytea" => LegacyType::Bytes,
        "date" => LegacyType::Date,
        "time" | "time without time zone" => LegacyType::Time,
        "interval" => LegacyType::Duration,
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => LegacyType::Geometry,
        _ => LegacyType::SourceSpecific(data_type.to_string()),
    }
}

/// Collect schema information for all tables in a PostgreSQL database
pub async fn collect_postgresql_schema(
    client: &tokio_postgres::Client,
    _database: &str,
) -> anyhow::Result<LegacySchema> {
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

        let generic_type = postgresql_column_type_to_legacy_type(&data_type, precision, scale);

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
