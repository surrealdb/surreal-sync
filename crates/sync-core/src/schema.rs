//! Schema definitions for the surreal-sync framework.
//!
//! This module defines the YAML schema structure used to specify
//! data generation parameters and table definitions for sync operations.

use crate::types::SyncDataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Error type for schema operations.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    /// Error reading schema file
    #[error("Failed to read schema file: {0}")]
    IoError(#[from] std::io::Error),

    /// Error parsing YAML
    #[error("Failed to parse YAML: {0}")]
    YamlError(#[from] serde_yaml::Error),

    /// Table not found in schema
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Field not found in table schema
    #[error("Field '{field}' not found in table '{table}'")]
    FieldNotFound { table: String, field: String },
}

/// Sync schema definition.
///
/// The schema defines the structure and generation rules for data.
/// It is loaded from a YAML file and provides the source of truth for
/// both data generation and verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSchema {
    /// Schema version
    #[serde(default = "default_version")]
    pub version: u32,

    /// Default seed for random generation
    #[serde(default)]
    pub seed: Option<u64>,

    /// Table definitions
    pub tables: Vec<TableSchema>,

    /// Cached table lookup (not serialized)
    #[serde(skip)]
    table_map: HashMap<String, usize>,
}

fn default_version() -> u32 {
    1
}

impl SyncSchema {
    /// Load schema from a YAML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, SchemaError> {
        let content = fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }

    /// Parse schema from YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, SchemaError> {
        let mut schema: SyncSchema = serde_yaml::from_str(yaml)?;
        schema.build_table_map();
        Ok(schema)
    }

    /// Build the internal table lookup map.
    fn build_table_map(&mut self) {
        self.table_map = self
            .tables
            .iter()
            .enumerate()
            .map(|(idx, table)| (table.name.clone(), idx))
            .collect();
    }

    /// Get a table schema by name.
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.table_map
            .get(name)
            .and_then(|&idx| self.tables.get(idx))
    }

    /// Get the type of a field in a specific table.
    pub fn get_field_type(&self, table: &str, field: &str) -> Result<&SyncDataType, SchemaError> {
        let table_schema = self
            .get_table(table)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;

        table_schema
            .get_field(field)
            .map(|f| &f.field_type)
            .ok_or_else(|| SchemaError::FieldNotFound {
                table: table.to_string(),
                field: field.to_string(),
            })
    }

    /// Get all table names in the schema.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.iter().map(|t| t.name.as_str()).collect()
    }
}

/// Schema definition for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,

    /// Primary key definition
    pub id: IdField,

    /// Field definitions
    pub fields: Vec<FieldSchema>,
}

impl TableSchema {
    /// Get a field schema by name.
    pub fn get_field(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get the type of a field by name.
    pub fn get_field_type(&self, name: &str) -> Option<&SyncDataType> {
        self.get_field(name).map(|f| &f.field_type)
    }

    /// Get all field names.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
}

/// Primary key field definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdField {
    /// Type of the primary key
    #[serde(rename = "type")]
    pub id_type: SyncDataType,

    /// Generator configuration for the primary key
    pub generator: GeneratorConfig,
}

/// Field schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field name
    pub name: String,

    /// Field type
    #[serde(rename = "type")]
    pub field_type: SyncDataType,

    /// Generator configuration for this field
    pub generator: GeneratorConfig,

    /// Whether this field is nullable
    #[serde(default)]
    pub nullable: bool,
}

/// Generator configuration for a field.
///
/// This enum defines the different types of value generators available
/// for producing test data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GeneratorConfig {
    /// Generate UUIDs (v4)
    UuidV4,

    /// Generate sequential integers
    Sequential {
        /// Starting value
        #[serde(default)]
        start: i64,
    },

    /// Generate values using a pattern with placeholders
    Pattern {
        /// Pattern string (supports {index}, {uuid}, etc.)
        pattern: String,
    },

    /// Generate random integers in a range
    IntRange {
        /// Minimum value (inclusive)
        min: i64,
        /// Maximum value (inclusive)
        max: i64,
    },

    /// Generate random floats in a range
    FloatRange {
        /// Minimum value (inclusive)
        min: f64,
        /// Maximum value (inclusive)
        max: f64,
    },

    /// Generate random decimals in a range
    DecimalRange {
        /// Minimum value (inclusive)
        min: f64,
        /// Maximum value (inclusive)
        max: f64,
    },

    /// Generate timestamps in a range
    TimestampRange {
        /// Start timestamp (ISO 8601)
        start: String,
        /// End timestamp (ISO 8601)
        end: String,
    },

    /// Generate weighted boolean values
    WeightedBool {
        /// Weight for true value (0.0 to 1.0)
        true_weight: f64,
    },

    /// Generate random selection from a pool of values
    OneOf {
        /// Pool of values to select from
        values: Vec<serde_yaml::Value>,
    },

    /// Generate arrays by sampling from a pool
    SampleArray {
        /// Pool of values to sample from
        pool: Vec<String>,
        /// Minimum array length
        #[serde(default)]
        min_length: usize,
        /// Maximum array length
        max_length: usize,
    },

    /// Generate a static value
    Static {
        /// The static value to use
        value: serde_yaml::Value,
    },

    /// Generate null values (for nullable fields)
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SCHEMA: &str = r#"
version: 1
seed: 12345

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

      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80

      - name: is_active
        type:
          type: tiny_int
          width: 1
        generator:
          type: weighted_bool
          true_weight: 0.8
"#;

    #[test]
    fn test_parse_schema() {
        let schema = SyncSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        assert_eq!(schema.version, 1);
        assert_eq!(schema.seed, Some(12345));
        assert_eq!(schema.tables.len(), 1);

        let users = schema.get_table("users").unwrap();
        assert_eq!(users.name, "users");
        assert_eq!(users.id.id_type, SyncDataType::Uuid);
        assert_eq!(users.fields.len(), 3);
    }

    #[test]
    fn test_get_field_type() {
        let schema = SyncSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        let email_type = schema.get_field_type("users", "email").unwrap();
        assert_eq!(email_type, &SyncDataType::VarChar { length: 255 });

        let age_type = schema.get_field_type("users", "age").unwrap();
        assert_eq!(age_type, &SyncDataType::Int);
    }

    #[test]
    fn test_table_not_found() {
        let schema = SyncSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        let result = schema.get_field_type("nonexistent", "field");
        assert!(matches!(result, Err(SchemaError::TableNotFound(_))));
    }

    #[test]
    fn test_field_not_found() {
        let schema = SyncSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        let result = schema.get_field_type("users", "nonexistent");
        assert!(matches!(result, Err(SchemaError::FieldNotFound { .. })));
    }

    #[test]
    fn test_generator_configs() {
        let yaml = r#"
version: 1
tables:
  - name: test
    id:
      type: int
      generator:
        type: sequential
        start: 1
    fields:
      - name: score
        type: double
        generator:
          type: float_range
          min: 0.0
          max: 100.0
      - name: status
        type: text
        generator:
          type: one_of
          values: ["active", "inactive", "pending"]
      - name: metadata
        type: json
        generator:
          type: static
          value: { "version": 1 }
"#;

        let schema = SyncSchema::from_yaml(yaml).unwrap();
        let table = schema.get_table("test").unwrap();

        // Check sequential generator
        assert!(matches!(
            table.id.generator,
            GeneratorConfig::Sequential { start: 1 }
        ));

        // Check float range generator
        let score_field = table.get_field("score").unwrap();
        assert!(matches!(
            score_field.generator,
            GeneratorConfig::FloatRange {
                min: 0.0,
                max: 100.0
            }
        ));
    }
}
