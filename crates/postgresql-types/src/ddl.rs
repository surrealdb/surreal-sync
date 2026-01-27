//! PostgreSQL DDL generation from UniversalType.
//!
//! This module provides DDL (Data Definition Language) generation for PostgreSQL,
//! converting sync-core's `UniversalType` to PostgreSQL column type definitions.

use sync_core::{GeometryType, UniversalType};

/// Trait for generating DDL type strings.
pub trait ToDdl {
    /// Convert a UniversalType to a DDL type string.
    fn to_ddl(&self, sync_type: &UniversalType) -> String;

    /// Generate a complete CREATE TABLE statement.
    fn to_create_table(
        &self,
        table_name: &str,
        columns: &[(String, UniversalType, bool)],
    ) -> String;
}

/// PostgreSQL DDL generator.
pub struct PostgreSQLDdl;

impl ToDdl for PostgreSQLDdl {
    fn to_ddl(&self, sync_type: &UniversalType) -> String {
        match sync_type {
            // Boolean
            UniversalType::Bool => "BOOLEAN".to_string(),

            // Integer types
            UniversalType::Int8 { .. } => "SMALLINT".to_string(), // PostgreSQL doesn't have TINYINT
            UniversalType::Int16 => "SMALLINT".to_string(),
            UniversalType::Int32 => "INTEGER".to_string(),
            UniversalType::Int64 => "BIGINT".to_string(),

            // Floating point
            UniversalType::Float32 => "REAL".to_string(),
            UniversalType::Float64 => "DOUBLE PRECISION".to_string(),

            // Exact numeric
            UniversalType::Decimal { precision, scale } => {
                format!("NUMERIC({precision},{scale})")
            }

            // String types
            UniversalType::Char { length } => format!("CHAR({length})"),
            UniversalType::VarChar { length } => format!("VARCHAR({length})"),
            UniversalType::Text => "TEXT".to_string(),

            // Binary types
            UniversalType::Blob => "BYTEA".to_string(),
            UniversalType::Bytes => "BYTEA".to_string(),

            // Date/time types
            UniversalType::Date => "DATE".to_string(),
            UniversalType::Time => "TIME".to_string(),
            UniversalType::LocalDateTime => "TIMESTAMP".to_string(),
            UniversalType::LocalDateTimeNano => "TIMESTAMP".to_string(), // PostgreSQL TIMESTAMP has microsecond precision
            UniversalType::ZonedDateTime => "TIMESTAMPTZ".to_string(),

            // Special types
            UniversalType::Uuid => "UUID".to_string(),
            UniversalType::Ulid => "TEXT".to_string(), // ULID stored as string in PostgreSQL
            UniversalType::Json => "JSON".to_string(),
            UniversalType::Jsonb => "JSONB".to_string(),

            // Array types
            UniversalType::Array { element_type } => {
                format!("{}[]", self.to_ddl(element_type))
            }

            // Set - stored as TEXT[]
            UniversalType::Set { .. } => "TEXT[]".to_string(),

            // Enumeration - stored as TEXT (PostgreSQL ENUM would need separate CREATE TYPE)
            UniversalType::Enum { .. } => "TEXT".to_string(),

            // Spatial types
            UniversalType::Geometry { geometry_type } => match geometry_type {
                GeometryType::Point => "POINT".to_string(),
                GeometryType::LineString => "PATH".to_string(), // PostgreSQL native path
                GeometryType::Polygon => "POLYGON".to_string(),
                GeometryType::MultiPoint => "POINT[]".to_string(),
                GeometryType::MultiLineString => "PATH[]".to_string(),
                GeometryType::MultiPolygon => "POLYGON[]".to_string(),
                GeometryType::GeometryCollection => "JSONB".to_string(), // Store complex geometries as JSONB
            },

            // Duration - stored as TEXT with ISO 8601 duration format
            // (PostgreSQL INTERVAL requires native type support which adds complexity)
            UniversalType::Duration => "TEXT".to_string(),

            // Thing - record reference stored as TEXT (table:id format)
            UniversalType::Thing => "TEXT".to_string(),

            // Object - nested document stored as JSONB
            UniversalType::Object => "JSONB".to_string(),

            // TimeTz - time with timezone
            UniversalType::TimeTz => "TIMETZ".to_string(),
        }
    }

    fn to_create_table(
        &self,
        table_name: &str,
        columns: &[(String, UniversalType, bool)],
    ) -> String {
        let column_defs: Vec<String> = columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let null_clause = if *nullable { "NULL" } else { "NOT NULL" };
                format!("  \"{}\" {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE \"{}\" (\n{}\n);",
            table_name,
            column_defs.join(",\n")
        )
    }
}

impl PostgreSQLDdl {
    /// Generate a CREATE TABLE statement with a primary key.
    pub fn to_create_table_with_pk(
        &self,
        table_name: &str,
        pk_column: &str,
        pk_type: &UniversalType,
        columns: &[(String, UniversalType, bool)],
    ) -> String {
        let mut all_columns = vec![(pk_column.to_string(), pk_type.clone(), false)];
        all_columns.extend(columns.iter().cloned());

        let column_defs: Vec<String> = all_columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let null_clause = if *nullable { "NULL" } else { "NOT NULL" };
                format!("  \"{}\" {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE \"{}\" (\n{},\n  PRIMARY KEY (\"{}\")\n);",
            table_name,
            column_defs.join(",\n"),
            pk_column
        )
    }

    /// Generate a CREATE TABLE statement with SERIAL primary key.
    pub fn to_create_table_with_serial_pk(
        &self,
        table_name: &str,
        pk_column: &str,
        columns: &[(String, UniversalType, bool)],
    ) -> String {
        let column_defs: Vec<String> = columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let null_clause = if *nullable { "NULL" } else { "NOT NULL" };
                format!("  \"{}\" {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE \"{}\" (\n  \"{}\" BIGSERIAL NOT NULL,\n{},\n  PRIMARY KEY (\"{}\")\n);",
            table_name,
            pk_column,
            column_defs.join(",\n"),
            pk_column
        )
    }

    /// Generate an INSERT statement template.
    pub fn to_insert(&self, table_name: &str, columns: &[String]) -> String {
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
        format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table_name,
            columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        )
    }

    /// Generate a batch INSERT statement using UNNEST.
    ///
    /// PostgreSQL's UNNEST allows efficient bulk inserts:
    /// INSERT INTO table (col1, col2) SELECT * FROM UNNEST($1::type[], $2::type[])
    pub fn to_batch_insert_unnest(
        &self,
        table_name: &str,
        columns: &[(String, UniversalType)],
    ) -> String {
        let col_names: Vec<String> = columns
            .iter()
            .map(|(name, _)| format!("\"{name}\""))
            .collect();
        let unnest_params: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, (_, dtype))| {
                let idx = i + 1;
                let ddl = self.to_ddl(dtype);
                format!("${idx}::{ddl}[]")
            })
            .collect();

        format!(
            "INSERT INTO \"{}\" ({}) SELECT * FROM UNNEST({})",
            table_name,
            col_names.join(", "),
            unnest_params.join(", ")
        )
    }

    /// Generate a CREATE ENUM type statement.
    pub fn to_create_enum(&self, type_name: &str, values: &[String]) -> String {
        let escaped: Vec<String> = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect();
        format!(
            "CREATE TYPE \"{}\" AS ENUM ({});",
            type_name,
            escaped.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Bool), "BOOLEAN");
    }

    #[test]
    fn test_integer_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Int8 { width: 4 }), "SMALLINT");
        assert_eq!(ddl.to_ddl(&UniversalType::Int16), "SMALLINT");
        assert_eq!(ddl.to_ddl(&UniversalType::Int32), "INTEGER");
        assert_eq!(ddl.to_ddl(&UniversalType::Int64), "BIGINT");
    }

    #[test]
    fn test_float_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Float32), "REAL");
        assert_eq!(ddl.to_ddl(&UniversalType::Float64), "DOUBLE PRECISION");
    }

    #[test]
    fn test_decimal_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Decimal {
                precision: 10,
                scale: 2
            }),
            "NUMERIC(10,2)"
        );
    }

    #[test]
    fn test_string_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Char { length: 10 }), "CHAR(10)");
        assert_eq!(
            ddl.to_ddl(&UniversalType::VarChar { length: 255 }),
            "VARCHAR(255)"
        );
        assert_eq!(ddl.to_ddl(&UniversalType::Text), "TEXT");
    }

    #[test]
    fn test_binary_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Blob), "BYTEA");
        assert_eq!(ddl.to_ddl(&UniversalType::Bytes), "BYTEA");
    }

    #[test]
    fn test_datetime_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Date), "DATE");
        assert_eq!(ddl.to_ddl(&UniversalType::Time), "TIME");
        assert_eq!(ddl.to_ddl(&UniversalType::LocalDateTime), "TIMESTAMP");
        assert_eq!(ddl.to_ddl(&UniversalType::ZonedDateTime), "TIMESTAMPTZ");
    }

    #[test]
    fn test_uuid_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Uuid), "UUID");
    }

    #[test]
    fn test_json_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Json), "JSON");
        assert_eq!(ddl.to_ddl(&UniversalType::Jsonb), "JSONB");
    }

    #[test]
    fn test_array_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Array {
                element_type: Box::new(UniversalType::Int32)
            }),
            "INTEGER[]"
        );
        assert_eq!(
            ddl.to_ddl(&UniversalType::Array {
                element_type: Box::new(UniversalType::Text)
            }),
            "TEXT[]"
        );
    }

    #[test]
    fn test_nested_array_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Array {
                element_type: Box::new(UniversalType::Array {
                    element_type: Box::new(UniversalType::Int32)
                })
            }),
            "INTEGER[][]"
        );
    }

    #[test]
    fn test_set_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string()]
            }),
            "TEXT[]"
        );
    }

    #[test]
    fn test_enum_ddl() {
        let ddl = PostgreSQLDdl;
        // Enums in PostgreSQL need CREATE TYPE, so we store as TEXT
        assert_eq!(
            ddl.to_ddl(&UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()]
            }),
            "TEXT"
        );
    }

    #[test]
    fn test_geometry_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Geometry {
                geometry_type: GeometryType::Point
            }),
            "POINT"
        );
        assert_eq!(
            ddl.to_ddl(&UniversalType::Geometry {
                geometry_type: GeometryType::Polygon
            }),
            "POLYGON"
        );
    }

    #[test]
    fn test_create_table() {
        let ddl = PostgreSQLDdl;
        let columns = vec![
            (
                "name".to_string(),
                UniversalType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                UniversalType::VarChar { length: 255 },
                false,
            ),
            ("age".to_string(), UniversalType::Int32, true),
        ];

        let sql = ddl.to_create_table("users", &columns);
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"name\" VARCHAR(100) NOT NULL"));
        assert!(sql.contains("\"email\" VARCHAR(255) NOT NULL"));
        assert!(sql.contains("\"age\" INTEGER NULL"));
    }

    #[test]
    fn test_create_table_with_pk() {
        let ddl = PostgreSQLDdl;
        let columns = vec![
            (
                "name".to_string(),
                UniversalType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                UniversalType::VarChar { length: 255 },
                false,
            ),
        ];

        let sql = ddl.to_create_table_with_pk("users", "id", &UniversalType::Uuid, &columns);
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" UUID NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_create_table_with_serial_pk() {
        let ddl = PostgreSQLDdl;
        let columns = vec![
            (
                "name".to_string(),
                UniversalType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                UniversalType::VarChar { length: 255 },
                false,
            ),
        ];

        let sql = ddl.to_create_table_with_serial_pk("users", "id", &columns);
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" BIGSERIAL NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_insert_statement() {
        let ddl = PostgreSQLDdl;
        let columns = vec!["name".to_string(), "email".to_string(), "age".to_string()];

        let sql = ddl.to_insert("users", &columns);
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"name\", \"email\", \"age\") VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn test_batch_insert_unnest() {
        let ddl = PostgreSQLDdl;
        let columns = vec![
            ("name".to_string(), UniversalType::Text),
            ("age".to_string(), UniversalType::Int32),
        ];

        let sql = ddl.to_batch_insert_unnest("users", &columns);
        assert!(sql.contains("INSERT INTO \"users\""));
        assert!(sql.contains("UNNEST"));
        assert!(sql.contains("$1::TEXT[]"));
        assert!(sql.contains("$2::INTEGER[]"));
    }

    #[test]
    fn test_create_enum() {
        let ddl = PostgreSQLDdl;
        let values = vec![
            "active".to_string(),
            "inactive".to_string(),
            "pending".to_string(),
        ];

        let sql = ddl.to_create_enum("user_status", &values);
        assert_eq!(
            sql,
            "CREATE TYPE \"user_status\" AS ENUM ('active', 'inactive', 'pending');"
        );
    }
}
