//! PostgreSQL DDL generation from SyncDataType.
//!
//! This module provides DDL (Data Definition Language) generation for PostgreSQL,
//! converting sync-core's `SyncDataType` to PostgreSQL column type definitions.

use sync_core::{GeometryType, SyncDataType};

/// Trait for generating DDL type strings.
pub trait ToDdl {
    /// Convert a SyncDataType to a DDL type string.
    fn to_ddl(&self, sync_type: &SyncDataType) -> String;

    /// Generate a complete CREATE TABLE statement.
    fn to_create_table(&self, table_name: &str, columns: &[(String, SyncDataType, bool)])
        -> String;
}

/// PostgreSQL DDL generator.
pub struct PostgreSQLDdl;

impl ToDdl for PostgreSQLDdl {
    fn to_ddl(&self, sync_type: &SyncDataType) -> String {
        match sync_type {
            // Boolean
            SyncDataType::Bool => "BOOLEAN".to_string(),

            // Integer types
            SyncDataType::TinyInt { .. } => "SMALLINT".to_string(), // PostgreSQL doesn't have TINYINT
            SyncDataType::SmallInt => "SMALLINT".to_string(),
            SyncDataType::Int => "INTEGER".to_string(),
            SyncDataType::BigInt => "BIGINT".to_string(),

            // Floating point
            SyncDataType::Float => "REAL".to_string(),
            SyncDataType::Double => "DOUBLE PRECISION".to_string(),

            // Exact numeric
            SyncDataType::Decimal { precision, scale } => {
                format!("NUMERIC({precision},{scale})")
            }

            // String types
            SyncDataType::Char { length } => format!("CHAR({length})"),
            SyncDataType::VarChar { length } => format!("VARCHAR({length})"),
            SyncDataType::Text => "TEXT".to_string(),

            // Binary types
            SyncDataType::Blob => "BYTEA".to_string(),
            SyncDataType::Bytes => "BYTEA".to_string(),

            // Date/time types
            SyncDataType::Date => "DATE".to_string(),
            SyncDataType::Time => "TIME".to_string(),
            SyncDataType::DateTime => "TIMESTAMP".to_string(),
            SyncDataType::DateTimeNano => "TIMESTAMP".to_string(), // PostgreSQL TIMESTAMP has microsecond precision
            SyncDataType::TimestampTz => "TIMESTAMPTZ".to_string(),

            // Special types
            SyncDataType::Uuid => "UUID".to_string(),
            SyncDataType::Json => "JSON".to_string(),
            SyncDataType::Jsonb => "JSONB".to_string(),

            // Array types
            SyncDataType::Array { element_type } => {
                format!("{}[]", self.to_ddl(element_type))
            }

            // Set - stored as TEXT[]
            SyncDataType::Set { .. } => "TEXT[]".to_string(),

            // Enumeration - stored as TEXT (PostgreSQL ENUM would need separate CREATE TYPE)
            SyncDataType::Enum { .. } => "TEXT".to_string(),

            // Spatial types
            SyncDataType::Geometry { geometry_type } => match geometry_type {
                GeometryType::Point => "POINT".to_string(),
                GeometryType::LineString => "PATH".to_string(), // PostgreSQL native path
                GeometryType::Polygon => "POLYGON".to_string(),
                GeometryType::MultiPoint => "POINT[]".to_string(),
                GeometryType::MultiLineString => "PATH[]".to_string(),
                GeometryType::MultiPolygon => "POLYGON[]".to_string(),
                GeometryType::GeometryCollection => "JSONB".to_string(), // Store complex geometries as JSONB
            },
        }
    }

    fn to_create_table(
        &self,
        table_name: &str,
        columns: &[(String, SyncDataType, bool)],
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
        pk_type: &SyncDataType,
        columns: &[(String, SyncDataType, bool)],
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
        columns: &[(String, SyncDataType, bool)],
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
        columns: &[(String, SyncDataType)],
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
        assert_eq!(ddl.to_ddl(&SyncDataType::Bool), "BOOLEAN");
    }

    #[test]
    fn test_integer_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::TinyInt { width: 4 }), "SMALLINT");
        assert_eq!(ddl.to_ddl(&SyncDataType::SmallInt), "SMALLINT");
        assert_eq!(ddl.to_ddl(&SyncDataType::Int), "INTEGER");
        assert_eq!(ddl.to_ddl(&SyncDataType::BigInt), "BIGINT");
    }

    #[test]
    fn test_float_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Float), "REAL");
        assert_eq!(ddl.to_ddl(&SyncDataType::Double), "DOUBLE PRECISION");
    }

    #[test]
    fn test_decimal_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Decimal {
                precision: 10,
                scale: 2
            }),
            "NUMERIC(10,2)"
        );
    }

    #[test]
    fn test_string_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Char { length: 10 }), "CHAR(10)");
        assert_eq!(
            ddl.to_ddl(&SyncDataType::VarChar { length: 255 }),
            "VARCHAR(255)"
        );
        assert_eq!(ddl.to_ddl(&SyncDataType::Text), "TEXT");
    }

    #[test]
    fn test_binary_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Blob), "BYTEA");
        assert_eq!(ddl.to_ddl(&SyncDataType::Bytes), "BYTEA");
    }

    #[test]
    fn test_datetime_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Date), "DATE");
        assert_eq!(ddl.to_ddl(&SyncDataType::Time), "TIME");
        assert_eq!(ddl.to_ddl(&SyncDataType::DateTime), "TIMESTAMP");
        assert_eq!(ddl.to_ddl(&SyncDataType::TimestampTz), "TIMESTAMPTZ");
    }

    #[test]
    fn test_uuid_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Uuid), "UUID");
    }

    #[test]
    fn test_json_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Json), "JSON");
        assert_eq!(ddl.to_ddl(&SyncDataType::Jsonb), "JSONB");
    }

    #[test]
    fn test_array_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Array {
                element_type: Box::new(SyncDataType::Int)
            }),
            "INTEGER[]"
        );
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Array {
                element_type: Box::new(SyncDataType::Text)
            }),
            "TEXT[]"
        );
    }

    #[test]
    fn test_nested_array_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Array {
                element_type: Box::new(SyncDataType::Array {
                    element_type: Box::new(SyncDataType::Int)
                })
            }),
            "INTEGER[][]"
        );
    }

    #[test]
    fn test_set_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Set {
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
            ddl.to_ddl(&SyncDataType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()]
            }),
            "TEXT"
        );
    }

    #[test]
    fn test_geometry_ddl() {
        let ddl = PostgreSQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Geometry {
                geometry_type: GeometryType::Point
            }),
            "POINT"
        );
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Geometry {
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
                SyncDataType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                SyncDataType::VarChar { length: 255 },
                false,
            ),
            ("age".to_string(), SyncDataType::Int, true),
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
                SyncDataType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                SyncDataType::VarChar { length: 255 },
                false,
            ),
        ];

        let sql = ddl.to_create_table_with_pk("users", "id", &SyncDataType::Uuid, &columns);
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
                SyncDataType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                SyncDataType::VarChar { length: 255 },
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
            ("name".to_string(), SyncDataType::Text),
            ("age".to_string(), SyncDataType::Int),
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
