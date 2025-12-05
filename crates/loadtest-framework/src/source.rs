//! Source database types for load testing.

use serde::{Deserialize, Serialize};

/// Source database type for load testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceType {
    /// MySQL database.
    MySQL {
        /// Connection string.
        connection_string: String,
    },
    /// PostgreSQL database.
    PostgreSQL {
        /// Connection string.
        connection_string: String,
    },
    /// MongoDB database.
    MongoDB {
        /// Connection string.
        connection_string: String,
        /// Database name.
        database: String,
    },
    /// CSV file.
    Csv {
        /// Output directory for CSV files.
        output_dir: String,
    },
    /// JSONL file.
    Jsonl {
        /// Output directory for JSONL files.
        output_dir: String,
    },
}

impl SourceType {
    /// Create a MySQL source.
    pub fn mysql(connection_string: impl Into<String>) -> Self {
        SourceType::MySQL {
            connection_string: connection_string.into(),
        }
    }

    /// Create a PostgreSQL source.
    pub fn postgresql(connection_string: impl Into<String>) -> Self {
        SourceType::PostgreSQL {
            connection_string: connection_string.into(),
        }
    }

    /// Create a MongoDB source.
    pub fn mongodb(connection_string: impl Into<String>, database: impl Into<String>) -> Self {
        SourceType::MongoDB {
            connection_string: connection_string.into(),
            database: database.into(),
        }
    }

    /// Create a CSV source.
    pub fn csv(output_dir: impl Into<String>) -> Self {
        SourceType::Csv {
            output_dir: output_dir.into(),
        }
    }

    /// Create a JSONL source.
    pub fn jsonl(output_dir: impl Into<String>) -> Self {
        SourceType::Jsonl {
            output_dir: output_dir.into(),
        }
    }

    /// Get the source type name.
    pub fn name(&self) -> &'static str {
        match self {
            SourceType::MySQL { .. } => "mysql",
            SourceType::PostgreSQL { .. } => "postgresql",
            SourceType::MongoDB { .. } => "mongodb",
            SourceType::Csv { .. } => "csv",
            SourceType::Jsonl { .. } => "jsonl",
        }
    }

    /// Check if this is a database source (vs file-based).
    pub fn is_database(&self) -> bool {
        matches!(
            self,
            SourceType::MySQL { .. } | SourceType::PostgreSQL { .. } | SourceType::MongoDB { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_type_mysql() {
        let source = SourceType::mysql("mysql://root:root@localhost/testdb");
        assert_eq!(source.name(), "mysql");
        assert!(source.is_database());
    }

    #[test]
    fn test_source_type_postgresql() {
        let source = SourceType::postgresql("postgres://postgres:postgres@localhost/testdb");
        assert_eq!(source.name(), "postgresql");
        assert!(source.is_database());
    }

    #[test]
    fn test_source_type_mongodb() {
        let source = SourceType::mongodb("mongodb://localhost:27017", "testdb");
        assert_eq!(source.name(), "mongodb");
        assert!(source.is_database());
    }

    #[test]
    fn test_source_type_csv() {
        let source = SourceType::csv("/tmp/output");
        assert_eq!(source.name(), "csv");
        assert!(!source.is_database());
    }

    #[test]
    fn test_source_type_jsonl() {
        let source = SourceType::jsonl("/tmp/output");
        assert_eq!(source.name(), "jsonl");
        assert!(!source.is_database());
    }
}
