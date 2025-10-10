//! PostgreSQL schema collection and type mapping
//!
//! This module provides functions for collecting PostgreSQL database schema information
//! and mapping PostgreSQL column types to generic data types for schema-aware conversion.

use crate::surreal::{SurrealDatabaseSchema, SurrealTableSchema, SurrealType};
use std::collections::HashMap;

/// Convert PostgreSQL column type information to SurrealType
pub fn column_type_to_surreal_type(
    data_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> SurrealType {
    match data_type.to_lowercase().as_str() {
        "numeric" | "decimal" => SurrealType::Decimal { precision, scale },
        "timestamp" | "timestamp without time zone" => SurrealType::Timestamp,
        "timestamptz" | "timestamp with time zone" => SurrealType::TimestampWithTimezone,
        "uuid" => SurrealType::Uuid,
        "json" | "jsonb" => SurrealType::Json,
        "boolean" | "bool" => SurrealType::Boolean,
        "integer" | "int4" | "bigint" | "int8" | "smallint" | "int2" => SurrealType::Integer,
        "real" | "float4" | "double precision" | "float8" => SurrealType::Float,
        "text" | "varchar" | "char" | "character varying" | "character" => SurrealType::String,
        "bytea" => SurrealType::Bytes,
        "date" => SurrealType::Date,
        "time" | "time without time zone" => SurrealType::Time,
        "interval" => SurrealType::Duration,
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => SurrealType::Geometry,
        _ => SurrealType::SourceSpecific(data_type.to_string()),
    }
}

/// Collect schema information for all tables in a PostgreSQL database
pub async fn collect_postgresql_schema(
    client: &tokio_postgres::Client,
    _database: &str,
) -> anyhow::Result<SurrealDatabaseSchema> {
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

        let generic_type = column_type_to_surreal_type(&data_type, precision, scale);

        let table_schema = tables
            .entry(table_name.clone())
            .or_insert_with(|| SurrealTableSchema {
                table_name: table_name.clone(),
                columns: HashMap::new(),
            });

        table_schema.columns.insert(column_name, generic_type);
    }

    Ok(SurrealDatabaseSchema { tables })
}
