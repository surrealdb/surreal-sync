//! MySQL DDL generation from SyncDataType.
//!
//! This module provides DDL (Data Definition Language) generation for MySQL,
//! converting sync-core's `SyncDataType` to MySQL column type definitions.

use sync_core::{GeometryType, SyncDataType};

/// Trait for generating DDL type strings.
pub trait ToDdl {
    /// Convert a SyncDataType to a DDL type string.
    fn to_ddl(&self, ext_type: &SyncDataType) -> String;

    /// Generate a complete CREATE TABLE statement.
    fn to_create_table(&self, table_name: &str, columns: &[(String, SyncDataType, bool)])
        -> String;
}

/// MySQL DDL generator.
pub struct MySQLDdl;

impl ToDdl for MySQLDdl {
    fn to_ddl(&self, ext_type: &SyncDataType) -> String {
        match ext_type {
            // Boolean - MySQL uses TINYINT(1)
            SyncDataType::Bool => "TINYINT(1)".to_string(),

            // Integer types
            SyncDataType::TinyInt { width } => format!("TINYINT({width})"),
            SyncDataType::SmallInt => "SMALLINT".to_string(),
            SyncDataType::Int => "INT".to_string(),
            SyncDataType::BigInt => "BIGINT".to_string(),

            // Floating point
            SyncDataType::Float => "FLOAT".to_string(),
            SyncDataType::Double => "DOUBLE".to_string(),

            // Exact numeric
            SyncDataType::Decimal { precision, scale } => {
                format!("DECIMAL({precision},{scale})")
            }

            // String types
            SyncDataType::Char { length } => format!("CHAR({length})"),
            SyncDataType::VarChar { length } => format!("VARCHAR({length})"),
            SyncDataType::Text => "TEXT".to_string(),

            // Binary types
            SyncDataType::Blob => "BLOB".to_string(),
            SyncDataType::Bytes => "VARBINARY(65535)".to_string(),

            // Date/time types
            SyncDataType::Date => "DATE".to_string(),
            SyncDataType::Time => "TIME".to_string(),
            SyncDataType::DateTime => "DATETIME(6)".to_string(),
            SyncDataType::DateTimeNano => "DATETIME(6)".to_string(), // MySQL max precision is 6
            SyncDataType::TimestampTz => "TIMESTAMP(6)".to_string(),

            // Special types
            SyncDataType::Uuid => "CHAR(36)".to_string(),
            SyncDataType::Json => "JSON".to_string(),
            SyncDataType::Jsonb => "JSON".to_string(), // MySQL doesn't have binary JSON

            // Collection types - stored as JSON in MySQL
            SyncDataType::Array { .. } => "JSON".to_string(),
            SyncDataType::Set { values } => {
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                format!("SET({})", escaped.join(", "))
            }

            // Enumeration
            SyncDataType::Enum { values } => {
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                format!("ENUM({})", escaped.join(", "))
            }

            // Spatial types
            SyncDataType::Geometry { geometry_type } => match geometry_type {
                GeometryType::Point => "POINT".to_string(),
                GeometryType::LineString => "LINESTRING".to_string(),
                GeometryType::Polygon => "POLYGON".to_string(),
                GeometryType::MultiPoint => "MULTIPOINT".to_string(),
                GeometryType::MultiLineString => "MULTILINESTRING".to_string(),
                GeometryType::MultiPolygon => "MULTIPOLYGON".to_string(),
                GeometryType::GeometryCollection => "GEOMETRYCOLLECTION".to_string(),
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
        pk_type: &SyncDataType,
        columns: &[(String, SyncDataType, bool)],
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
        columns: &[(String, SyncDataType, bool)],
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
        assert_eq!(ddl.to_ddl(&SyncDataType::Bool), "TINYINT(1)");
    }

    #[test]
    fn test_integer_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::TinyInt { width: 4 }),
            "TINYINT(4)"
        );
        assert_eq!(ddl.to_ddl(&SyncDataType::SmallInt), "SMALLINT");
        assert_eq!(ddl.to_ddl(&SyncDataType::Int), "INT");
        assert_eq!(ddl.to_ddl(&SyncDataType::BigInt), "BIGINT");
    }

    #[test]
    fn test_float_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Float), "FLOAT");
        assert_eq!(ddl.to_ddl(&SyncDataType::Double), "DOUBLE");
    }

    #[test]
    fn test_decimal_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Decimal {
                precision: 10,
                scale: 2
            }),
            "DECIMAL(10,2)"
        );
    }

    #[test]
    fn test_string_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Char { length: 10 }), "CHAR(10)");
        assert_eq!(
            ddl.to_ddl(&SyncDataType::VarChar { length: 255 }),
            "VARCHAR(255)"
        );
        assert_eq!(ddl.to_ddl(&SyncDataType::Text), "TEXT");
    }

    #[test]
    fn test_binary_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Blob), "BLOB");
        assert_eq!(ddl.to_ddl(&SyncDataType::Bytes), "VARBINARY(65535)");
    }

    #[test]
    fn test_datetime_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Date), "DATE");
        assert_eq!(ddl.to_ddl(&SyncDataType::Time), "TIME");
        assert_eq!(ddl.to_ddl(&SyncDataType::DateTime), "DATETIME(6)");
        assert_eq!(ddl.to_ddl(&SyncDataType::TimestampTz), "TIMESTAMP(6)");
    }

    #[test]
    fn test_uuid_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Uuid), "CHAR(36)");
    }

    #[test]
    fn test_json_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(ddl.to_ddl(&SyncDataType::Json), "JSON");
        assert_eq!(ddl.to_ddl(&SyncDataType::Jsonb), "JSON");
    }

    #[test]
    fn test_array_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Array {
                element_type: Box::new(SyncDataType::Int)
            }),
            "JSON"
        );
    }

    #[test]
    fn test_enum_ddl() {
        let ddl = MySQLDdl;
        assert_eq!(
            ddl.to_ddl(&SyncDataType::Enum {
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
            ddl.to_ddl(&SyncDataType::Set {
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
        let ddl = MySQLDdl;
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
                SyncDataType::VarChar { length: 100 },
                false,
            ),
            (
                "email".to_string(),
                SyncDataType::VarChar { length: 255 },
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
