//! Configuration types for load testing.

use crate::source::SourceType;
use serde::{Deserialize, Serialize};
use sync_core::SyncSchema;

/// Configuration for a load test.
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    /// Load test schema defining tables and generators.
    pub schema: SyncSchema,
    /// Random seed for deterministic data generation.
    pub seed: u64,
    /// Source database configuration.
    pub source: SourceConfig,
    /// SurrealDB configuration.
    pub surreal: SurrealConfig,
    /// Number of rows to generate per table.
    pub row_count: u64,
    /// Batch size for database inserts.
    pub batch_size: usize,
    /// Tables to include (empty = all tables).
    pub tables: Vec<String>,
    /// Whether to verify data after sync.
    pub verify: bool,
    /// Whether to clean up source data after test.
    pub cleanup: bool,
}

impl LoadTestConfig {
    /// Create a new load test configuration.
    pub fn new(schema: SyncSchema, seed: u64) -> Self {
        Self {
            schema,
            seed,
            source: SourceConfig::default(),
            surreal: SurrealConfig::default(),
            row_count: 1000,
            batch_size: 100,
            tables: Vec::new(),
            verify: true,
            cleanup: true,
        }
    }

    /// Set the source configuration.
    pub fn with_source(mut self, source_type: SourceType) -> Self {
        self.source = SourceConfig {
            source_type,
            ..self.source
        };
        self
    }

    /// Set the SurrealDB connection string.
    pub fn with_surreal(mut self, connection_string: impl Into<String>) -> Self {
        self.surreal.connection_string = connection_string.into();
        self
    }

    /// Set the SurrealDB namespace and database.
    pub fn with_surreal_ns_db(
        mut self,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        self.surreal.namespace = namespace.into();
        self.surreal.database = database.into();
        self
    }

    /// Set the number of rows to generate.
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = count;
        self
    }

    /// Set the batch size for inserts.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set specific tables to test.
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Enable or disable verification.
    pub fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Enable or disable cleanup.
    pub fn with_cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Get the tables to test (all tables if none specified).
    pub fn tables_to_test(&self) -> Vec<String> {
        if self.tables.is_empty() {
            self.schema
                .table_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            self.tables.clone()
        }
    }
}

/// Source database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Source type and connection details.
    pub source_type: SourceType,
    /// Whether to create tables before populating.
    pub create_tables: bool,
    /// Whether to drop tables before creating.
    pub drop_existing: bool,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            source_type: SourceType::csv("/tmp/loadtest"),
            create_tables: true,
            drop_existing: true,
        }
    }
}

/// SurrealDB configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurrealConfig {
    /// Connection string (e.g., "ws://localhost:8000").
    pub connection_string: String,
    /// Namespace.
    pub namespace: String,
    /// Database.
    pub database: String,
    /// Username (optional).
    pub username: Option<String>,
    /// Password (optional).
    pub password: Option<String>,
}

impl Default for SurrealConfig {
    fn default() -> Self {
        Self {
            connection_string: "ws://localhost:8000".to_string(),
            namespace: "test".to_string(),
            database: "test".to_string(),
            username: Some("root".to_string()),
            password: Some("root".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> SyncSchema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
"#;
        SyncSchema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_config_builder() {
        let schema = test_schema();
        let config = LoadTestConfig::new(schema, 42)
            .with_source(SourceType::postgresql("postgres://localhost/testdb"))
            .with_surreal("ws://localhost:8000")
            .with_surreal_ns_db("loadtest", "test")
            .with_row_count(10000)
            .with_batch_size(500)
            .with_verify(true)
            .with_cleanup(false);

        assert_eq!(config.seed, 42);
        assert_eq!(config.row_count, 10000);
        assert_eq!(config.batch_size, 500);
        assert!(config.verify);
        assert!(!config.cleanup);
        assert_eq!(config.surreal.namespace, "loadtest");
        assert_eq!(config.surreal.database, "test");
    }

    #[test]
    fn test_tables_to_test_default() {
        let schema = test_schema();
        let config = LoadTestConfig::new(schema, 42);

        let tables = config.tables_to_test();
        assert_eq!(tables, vec!["users".to_string()]);
    }

    #[test]
    fn test_tables_to_test_specific() {
        let schema = test_schema();
        let config = LoadTestConfig::new(schema, 42).with_tables(vec!["orders".to_string()]);

        let tables = config.tables_to_test();
        assert_eq!(tables, vec!["orders".to_string()]);
    }
}
