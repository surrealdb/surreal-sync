//! MySQL DDL generation from UniversalType.
//!
//! This module provides DDL (Data Definition Language) generation for MySQL,
//! converting sync-core's `UniversalType` to MySQL column type definitions.

use sync_core::{GeometryType, UniversalType};

/// Trait for generating DDL type strings.
pub trait ToDdl {
    /// Convert a UniversalType to a DDL type string.
    fn to_ddl(&self, ext_type: &UniversalType) -> String;

    /// Generate a complete CREATE TABLE statement.
    fn to_create_table(
        &self,
        table_name: &str,
        columns: &[(String, UniversalType, bool)],
    ) -> String;
}

/// MySQL DDL generator.
pub struct MySQLDdl;

impl ToDdl for MySQLDdl {
    fn to_ddl(&self, ext_type: &UniversalType) -> String {
        match ext_type {
            // Boolean - MySQL uses TINYINT(1)
            UniversalType::Bool => "TINYINT(1)".to_string(),

            // Integer types
            UniversalType::Int8 { width } => format!("TINYINT({width})"),
            UniversalType::Int16 => "SMALLINT".to_string(),
            UniversalType::Int32 => "INT".to_string(),
            UniversalType::Int64 => "BIGINT".to_string(),

            // Floating point
            UniversalType::Float32 => "FLOAT".to_string(),
            UniversalType::Float64 => "DOUBLE".to_string(),

            // Exact numeric
            UniversalType::Decimal { precision, scale } => {
                format!("DECIMAL({precision},{scale})")
            }

            // String types
            UniversalType::Char { length } => format!("CHAR({length})"),
            UniversalType::VarChar { length } => format!("VARCHAR({length})"),
            UniversalType::Text => "TEXT".to_string(),

            // Binary types
            UniversalType::Blob => "BLOB".to_string(),
            UniversalType::Bytes => "VARBINARY(65535)".to_string(),

            // Date/time types
            UniversalType::Date => "DATE".to_string(),
            UniversalType::Time => "TIME".to_string(),
            UniversalType::LocalDateTime => "DATETIME(6)".to_string(),
            UniversalType::LocalDateTimeNano => "DATETIME(6)".to_string(), // MySQL max precision is 6
            UniversalType::ZonedDateTime => "TIMESTAMP(6)".to_string(),

            // Special types
            UniversalType::Uuid => "CHAR(36)".to_string(),
            UniversalType::Ulid => "CHAR(26)".to_string(), // ULID is 26 chars in string form
            UniversalType::Json => "JSON".to_string(),
            UniversalType::Jsonb => "JSON".to_string(), // MySQL doesn't have binary JSON

            // Collection types - stored as JSON in MySQL
            UniversalType::Array { .. } => "JSON".to_string(),
            UniversalType::Set { values } => {
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                format!("SET({})", escaped.join(", "))
            }

            // Enumeration
            UniversalType::Enum { values } => {
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                format!("ENUM({})", escaped.join(", "))
            }

            // Spatial types
            UniversalType::Geometry { geometry_type } => match geometry_type {
                GeometryType::Point => "POINT".to_string(),
                GeometryType::LineString => "LINESTRING".to_string(),
                GeometryType::Polygon => "POLYGON".to_string(),
                GeometryType::MultiPoint => "MULTIPOINT".to_string(),
                GeometryType::MultiLineString => "MULTILINESTRING".to_string(),
                GeometryType::MultiPolygon => "MULTIPOLYGON".to_string(),
                GeometryType::GeometryCollection => "GEOMETRYCOLLECTION".to_string(),
            },

            // Duration - store as VARCHAR for ISO 8601 duration string
            UniversalType::Duration => "VARCHAR(64)".to_string(),

            // Thing - record reference stored as VARCHAR (table:id format)
            UniversalType::Thing => "VARCHAR(255)".to_string(),

            // Object - nested document stored as JSON
            UniversalType::Object => "JSON".to_string(),

            // TimeTz - MySQL doesn't have native TIMETZ, store as VARCHAR
            // Note: We intentionally use VARCHAR instead of TIME because MySQL TIME
            // doesn't support timezone offsets. Storing as string preserves the original format.
            UniversalType::TimeTz => "VARCHAR(32)".to_string(),
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
                format!("  `{}` {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE `{}` (\n{}\n);",
            table_name,
            column_defs.join(",\n")
        )
    }
}

impl MySQLDdl {
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
                format!("  `{}` {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE `{}` (\n{},\n  PRIMARY KEY (`{}`)\n);",
            table_name,
            column_defs.join(",\n"),
            pk_column
        )
    }

    /// Generate a CREATE TABLE statement with auto-increment primary key.
    pub fn to_create_table_with_auto_pk(
        &self,
        table_name: &str,
        pk_column: &str,
        columns: &[(String, UniversalType, bool)],
    ) -> String {
        let column_defs: Vec<String> = columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let null_clause = if *nullable { "NULL" } else { "NOT NULL" };
                format!("  `{}` {} {}", name, self.to_ddl(dtype), null_clause)
            })
            .collect();

        format!(
            "CREATE TABLE `{}` (\n  `{}` BIGINT NOT NULL AUTO_INCREMENT,\n{},\n  PRIMARY KEY (`{}`)\n);",
            table_name,
            pk_column,
            column_defs.join(",\n"),
            pk_column
        )
    }

    /// Generate an INSERT statement template.
    pub fn to_insert(&self, table_name: &str, columns: &[String]) -> String {
        let placeholders: Vec<&str> = columns.iter().map(|_| "?").collect();
        format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            table_name,
            columns
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        )
    }

    /// Generate a batch INSERT statement.
    pub fn to_batch_insert(
        &self,
        table_name: &str,
        columns: &[String],
        row_count: usize,
    ) -> String {
        let col_placeholders: Vec<&str> = columns.iter().map(|_| "?").collect();
        let row_template = format!("({})", col_placeholders.join(", "));
        let rows: Vec<&str> = (0..row_count).map(|_| row_template.as_str()).collect();

        format!(
            "INSERT INTO `{}` ({}) VALUES {}",
            table_name,
            columns
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", "),
            rows.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Bool), "TINYINT(1)");
    }

    #[test]
    fn test_integer_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Int8 { width: 4 }), "TINYINT(4)");
        assert_eq!(ddl.to_ddl(&UniversalType::Int16), "SMALLINT");
        assert_eq!(ddl.to_ddl(&UniversalType::Int32), "INT");
        assert_eq!(ddl.to_ddl(&UniversalType::Int64), "BIGINT");
    }

    #[test]
    fn test_float_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Float32), "FLOAT");
        assert_eq!(ddl.to_ddl(&UniversalType::Float64), "DOUBLE");
    }

    #[test]
    fn test_decimal_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Decimal {
                precision: 10,
                scale: 2
            }),
            "DECIMAL(10,2)"
        );
    }

    #[test]
    fn test_string_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Char { length: 10 }), "CHAR(10)");
        assert_eq!(
            ddl.to_ddl(&UniversalType::VarChar { length: 255 }),
            "VARCHAR(255)"
        );
        assert_eq!(ddl.to_ddl(&UniversalType::Text), "TEXT");
    }

    #[test]
    fn test_binary_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Blob), "BLOB");
        assert_eq!(ddl.to_ddl(&UniversalType::Bytes), "VARBINARY(65535)");
    }

    #[test]
    fn test_datetime_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Date), "DATE");
        assert_eq!(ddl.to_ddl(&UniversalType::Time), "TIME");
        assert_eq!(ddl.to_ddl(&UniversalType::LocalDateTime), "DATETIME(6)");
        assert_eq!(ddl.to_ddl(&UniversalType::ZonedDateTime), "TIMESTAMP(6)");
    }

    #[test]
    fn test_uuid_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Uuid), "CHAR(36)");
    }

    #[test]
    fn test_json_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&UniversalType::Json), "JSON");
        assert_eq!(ddl.to_ddl(&UniversalType::Jsonb), "JSON");
    }

    #[test]
    fn test_array_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Array {
                element_type: Box::new(UniversalType::Int32)
            }),
            "JSON"
        );
    }

    #[test]
    fn test_enum_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Enum {
                values: vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "pending".to_string()
                ]
            }),
            "ENUM('active', 'inactive', 'pending')"
        );
    }

    #[test]
    fn test_set_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&UniversalType::Set {
                values: vec![
                    "read".to_string(),
                    "write".to_string(),
                    "delete".to_string()
                ]
            }),
            "SET('read', 'write', 'delete')"
        );
    }

    #[test]
    fn test_geometry_ddl() {
        let ddl = MySQLDdl;
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
        let ddl = MySQLDdl;
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
        assert!(sql.contains("CREATE TABLE `users`"));
        assert!(sql.contains("`name` VARCHAR(100) NOT NULL"));
        assert!(sql.contains("`email` VARCHAR(255) NOT NULL"));
        assert!(sql.contains("`age` INT NULL"));
    }

    #[test]
    fn test_create_table_with_pk() {
        let ddl = MySQLDdl;
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
        assert!(sql.contains("CREATE TABLE `users`"));
        assert!(sql.contains("`id` CHAR(36) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (`id`)"));
    }

    #[test]
    fn test_create_table_with_auto_pk() {
        let ddl = MySQLDdl;
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

        let sql = ddl.to_create_table_with_auto_pk("users", "id", &columns);
        assert!(sql.contains("CREATE TABLE `users`"));
        assert!(sql.contains("`id` BIGINT NOT NULL AUTO_INCREMENT"));
        assert!(sql.contains("PRIMARY KEY (`id`)"));
    }

    #[test]
    fn test_insert_statement() {
        let ddl = MySQLDdl;
        let columns = vec!["name".to_string(), "email".to_string(), "age".to_string()];

        let sql = ddl.to_insert("users", &columns);
        assert_eq!(
            sql,
            "INSERT INTO `users` (`name`, `email`, `age`) VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn test_batch_insert_statement() {
        let ddl = MySQLDdl;
        let columns = vec!["name".to_string(), "email".to_string()];

        let sql = ddl.to_batch_insert("users", &columns, 3);
        assert_eq!(
            sql,
            "INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?), (?, ?)"
        );
    }
}
