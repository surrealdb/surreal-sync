//! Schema definitions for the surreal-sync framework.
//!
//! This module defines schema types for both database introspection and data generation.
//!
//! ## Type Hierarchy
//!
//! **Base types** (context-neutral, no generators):
//! - `ColumnDefinition` - Single column metadata
//! - `TableDefinition` - Table with columns (no generators)
//! - `DatabaseSchema` - Collection of tables (no generators)
//!
//! **Generator types** (embed base types, include generators):
//! - `GeneratorFieldDefinition` - Field with generator config
//! - `GeneratorIDDefinition` - Primary key with generator config
//! - `GeneratorTableDefinition` - Table with generators
//! - `GeneratorSchema` - Full schema with generators
//!
//! ## Usage
//!
//! - Use base types for schema introspection during sync operations
//! - Use generator types for test data generation (YAML schema files)

use crate::types::UniversalType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

// ============================================================================
// Error Types
// ============================================================================

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

    /// Column not found in table schema
    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },
}

// ============================================================================
// Base Types (Context-Neutral, No Generators)
// ============================================================================

/// Column definition - shared by both introspection and generation.
///
/// This type represents a single column in a database table,
/// including its name, type, and nullability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,

    /// Column type
    #[serde(rename = "type")]
    pub column_type: UniversalType,

    /// Whether this column is nullable
    #[serde(default)]
    pub nullable: bool,
}

impl ColumnDefinition {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, column_type: UniversalType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
        }
    }

    /// Create a new nullable column definition.
    pub fn nullable(name: impl Into<String>, column_type: UniversalType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: true,
        }
    }
}

/// Table schema definition (no generators).
///
/// Used for schema introspection during incremental sync operations.
/// Contains the table name, primary key column, and all columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Table name
    pub name: String,

    /// Primary key column definition
    pub primary_key: ColumnDefinition,

    /// Column definitions (excluding primary key)
    pub columns: Vec<ColumnDefinition>,
}

impl TableDefinition {
    /// Create a new table definition.
    pub fn new(
        name: impl Into<String>,
        primary_key: ColumnDefinition,
        columns: Vec<ColumnDefinition>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key,
            columns,
        }
    }

    /// Get a column by name (searches both primary key and columns).
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        if self.primary_key.name == name {
            return Some(&self.primary_key);
        }
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get the type of a column by name.
    pub fn get_column_type(&self, name: &str) -> Option<&UniversalType> {
        self.get_column(name).map(|c| &c.column_type)
    }

    /// Get all column names (including primary key).
    pub fn column_names(&self) -> Vec<&str> {
        let mut names = vec![self.primary_key.name.as_str()];
        names.extend(self.columns.iter().map(|c| c.name.as_str()));
        names
    }
}

/// Database schema (collection of tables, no generators).
///
/// Used for schema introspection during incremental sync operations.
/// Contains all table definitions collected from the source database.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseSchema {
    /// Table definitions
    pub tables: Vec<TableDefinition>,

    /// Cached table lookup (not serialized)
    #[serde(skip)]
    table_map: HashMap<String, usize>,
}

impl DatabaseSchema {
    /// Create a new database schema from a list of table definitions.
    pub fn new(tables: Vec<TableDefinition>) -> Self {
        let mut schema = Self {
            tables,
            table_map: HashMap::new(),
        };
        schema.build_table_map();
        schema
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
    pub fn get_table(&self, name: &str) -> Option<&TableDefinition> {
        self.table_map
            .get(name)
            .and_then(|&idx| self.tables.get(idx))
    }

    /// Get the type of a column in a specific table.
    pub fn get_column_type(
        &self,
        table: &str,
        column: &str,
    ) -> Result<&UniversalType, SchemaError> {
        let table_schema = self
            .get_table(table)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;

        table_schema
            .get_column_type(column)
            .ok_or_else(|| SchemaError::ColumnNotFound {
                table: table.to_string(),
                column: column.to_string(),
            })
    }

    /// Get all table names in the schema.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.iter().map(|t| t.name.as_str()).collect()
    }

    /// Add a table to the schema.
    pub fn add_table(&mut self, table: TableDefinition) {
        let idx = self.tables.len();
        self.table_map.insert(table.name.clone(), idx);
        self.tables.push(table);
    }
}

// ============================================================================
// Generator Types (With Generators)
// ============================================================================

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

    /// Generate current timestamp at generation time
    ///
    /// This generator produces the current UTC timestamp when the row is generated.
    /// Useful for `updated_at` or `created_at` fields where the actual insert time
    /// is needed (e.g., for Neo4j incremental sync which filters by timestamp).
    ///
    /// Note: This is NOT deterministic - each generation produces a different value.
    TimestampNow,

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

    /// Generate random durations in a range (in seconds)
    DurationRange {
        /// Minimum duration in seconds (inclusive)
        min_secs: u64,
        /// Maximum duration in seconds (inclusive)
        max_secs: u64,
    },
}

/// Field with generator config. Embeds ColumnDefinition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorFieldDefinition {
    /// Field name (flattened from ColumnDefinition-like structure)
    pub name: String,

    /// Field type (flattened from ColumnDefinition-like structure)
    #[serde(rename = "type")]
    pub field_type: UniversalType,

    /// Generator configuration for this field
    pub generator: GeneratorConfig,

    /// Whether this field is nullable
    #[serde(default)]
    pub nullable: bool,
}

impl GeneratorFieldDefinition {
    /// Convert to base ColumnDefinition (discarding generator info).
    pub fn to_column_definition(&self) -> ColumnDefinition {
        ColumnDefinition {
            name: self.name.clone(),
            column_type: self.field_type.clone(),
            nullable: self.nullable,
        }
    }
}

/// Primary key with generator config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorIDDefinition {
    /// Type of the primary key
    #[serde(rename = "type")]
    pub id_type: UniversalType,

    /// Generator configuration for the primary key
    pub generator: GeneratorConfig,
}

impl GeneratorIDDefinition {
    /// Convert to base ColumnDefinition (discarding generator info).
    /// Uses "id" as the column name for primary keys.
    pub fn to_column_definition(&self) -> ColumnDefinition {
        ColumnDefinition {
            name: "id".to_string(),
            column_type: self.id_type.clone(),
            nullable: false, // Primary keys are never nullable
        }
    }
}

/// Table with generators. For test data generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorTableDefinition {
    /// Table name
    pub name: String,

    /// Primary key definition with generator
    pub id: GeneratorIDDefinition,

    /// Field definitions with generators
    pub fields: Vec<GeneratorFieldDefinition>,
}

impl GeneratorTableDefinition {
    /// Get a field schema by name.
    pub fn get_field(&self, name: &str) -> Option<&GeneratorFieldDefinition> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get the type of a field by name.
    pub fn get_field_type(&self, name: &str) -> Option<&UniversalType> {
        self.get_field(name).map(|f| &f.field_type)
    }

    /// Get all field names.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Convert to base TableDefinition (discarding generator info).
    pub fn to_table_definition(&self) -> TableDefinition {
        TableDefinition {
            name: self.name.clone(),
            primary_key: self.id.to_column_definition(),
            columns: self
                .fields
                .iter()
                .map(|f| f.to_column_definition())
                .collect(),
        }
    }
}

fn default_version() -> u32 {
    1
}

/// Full schema with generators.
///
/// The schema defines the structure and generation rules for data.
/// It is loaded from a YAML file and provides the source of truth for
/// both data generation and verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorSchema {
    /// Schema version
    #[serde(default = "default_version")]
    pub version: u32,

    /// Table definitions with generators
    pub tables: Vec<GeneratorTableDefinition>,

    /// Cached table lookup (not serialized)
    #[serde(skip)]
    table_map: HashMap<String, usize>,
}

impl GeneratorSchema {
    /// Load schema from a YAML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, SchemaError> {
        let content = fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }

    /// Parse schema from YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, SchemaError> {
        let mut schema: GeneratorSchema = serde_yaml::from_str(yaml)?;
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
    pub fn get_table(&self, name: &str) -> Option<&GeneratorTableDefinition> {
        self.table_map
            .get(name)
            .and_then(|&idx| self.tables.get(idx))
    }

    /// Get the type of a field in a specific table.
    pub fn get_field_type(&self, table: &str, field: &str) -> Result<&UniversalType, SchemaError> {
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

    /// Convert to base DatabaseSchema (discarding generator info).
    pub fn to_database_schema(&self) -> DatabaseSchema {
        DatabaseSchema::new(
            self.tables
                .iter()
                .map(|t| t.to_table_definition())
                .collect(),
        )
    }
}

// ============================================================================
// Type Aliases for Backwards Compatibility
// ============================================================================

/// Type alias for backwards compatibility.
/// `Schema` is now `GeneratorSchema`.
pub type Schema = GeneratorSchema;

/// Type alias for backwards compatibility.
/// Old `TableDefinition` (with generators) is now `GeneratorTableDefinition`.
pub type TableDefinitionWithGenerators = GeneratorTableDefinition;

/// Type alias for backwards compatibility.
/// Old `FieldDefinition` is now `GeneratorFieldDefinition`.
pub type FieldDefinition = GeneratorFieldDefinition;

/// Type alias for backwards compatibility.
/// Old `IDDefinition` is now `GeneratorIDDefinition`.
pub type IDDefinition = GeneratorIDDefinition;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Base Type Tests
    // ========================================================================

    #[test]
    fn test_column_definition_serde() {
        let col = ColumnDefinition::new("email", UniversalType::VarChar { length: 255 });

        let yaml = serde_yaml::to_string(&col).unwrap();
        let parsed: ColumnDefinition = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(col.name, parsed.name);
        assert_eq!(col.column_type, parsed.column_type);
        assert_eq!(col.nullable, parsed.nullable);
    }

    #[test]
    fn test_table_definition_get_column() {
        let table = TableDefinition::new(
            "users",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![
                ColumnDefinition::new("email", UniversalType::VarChar { length: 255 }),
                ColumnDefinition::nullable("bio", UniversalType::Text),
            ],
        );

        // Can find primary key
        let id_col = table.get_column("id").expect("id should exist");
        assert_eq!(id_col.column_type, UniversalType::Int64);

        // Can find regular column
        let email_col = table.get_column("email").expect("email should exist");
        assert_eq!(
            email_col.column_type,
            UniversalType::VarChar { length: 255 }
        );

        // Returns None for missing column
        assert!(table.get_column("nonexistent").is_none());

        // Column names includes primary key
        let names = table.column_names();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"bio"));
    }

    #[test]
    fn test_database_schema_get_table() {
        let schema = DatabaseSchema::new(vec![
            TableDefinition::new(
                "users",
                ColumnDefinition::new("id", UniversalType::Uuid),
                vec![ColumnDefinition::new("name", UniversalType::Text)],
            ),
            TableDefinition::new(
                "posts",
                ColumnDefinition::new("id", UniversalType::Int64),
                vec![ColumnDefinition::new("title", UniversalType::Text)],
            ),
        ]);

        // Can find tables
        let users = schema.get_table("users").expect("users should exist");
        assert_eq!(users.primary_key.column_type, UniversalType::Uuid);

        let posts = schema.get_table("posts").expect("posts should exist");
        assert_eq!(posts.primary_key.column_type, UniversalType::Int64);

        // Returns None for missing table
        assert!(schema.get_table("nonexistent").is_none());

        // Can get column type
        let col_type = schema.get_column_type("users", "name").unwrap();
        assert_eq!(col_type, &UniversalType::Text);
    }

    // ========================================================================
    // Generator Type Tests
    // ========================================================================

    const SAMPLE_SCHEMA: &str = r#"
version: 1

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
    fn test_parse_generator_schema() {
        let schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        assert_eq!(schema.version, 1);
        assert_eq!(schema.tables.len(), 1);

        let users = schema.get_table("users").unwrap();
        assert_eq!(users.name, "users");
        assert_eq!(users.id.id_type, UniversalType::Uuid);
        assert_eq!(users.fields.len(), 3);
    }

    #[test]
    fn test_get_field_type() {
        let schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        let email_type = schema.get_field_type("users", "email").unwrap();
        assert_eq!(email_type, &UniversalType::VarChar { length: 255 });

        let age_type = schema.get_field_type("users", "age").unwrap();
        assert_eq!(age_type, &UniversalType::Int32);
    }

    #[test]
    fn test_table_not_found() {
        let schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        let result = schema.get_field_type("nonexistent", "field");
        assert!(matches!(result, Err(SchemaError::TableNotFound(_))));
    }

    #[test]
    fn test_field_not_found() {
        let schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

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

        let schema = GeneratorSchema::from_yaml(yaml).unwrap();
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

    #[test]
    fn test_generator_field_definition_flatten_serde() {
        // Test that GeneratorFieldDefinition serializes correctly
        let field = GeneratorFieldDefinition {
            name: "email".to_string(),
            field_type: UniversalType::VarChar { length: 255 },
            generator: GeneratorConfig::Pattern {
                pattern: "user_{index}@test.com".to_string(),
            },
            nullable: false,
        };

        let yaml = serde_yaml::to_string(&field).unwrap();
        let parsed: GeneratorFieldDefinition = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(field.name, parsed.name);
        assert_eq!(field.field_type, parsed.field_type);
        assert_eq!(field.nullable, parsed.nullable);

        // Test conversion to base ColumnDefinition
        let col = field.to_column_definition();
        assert_eq!(col.name, "email");
        assert_eq!(col.column_type, UniversalType::VarChar { length: 255 });
        assert!(!col.nullable);
    }

    #[test]
    fn test_generator_schema_yaml_compatibility() {
        // Verify existing YAML schemas still parse correctly
        let schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();

        // Verify conversion to DatabaseSchema works
        let db_schema = schema.to_database_schema();

        assert_eq!(db_schema.tables.len(), 1);
        let users = db_schema.get_table("users").expect("users should exist");
        assert_eq!(users.primary_key.column_type, UniversalType::Uuid);
        assert_eq!(users.columns.len(), 3);

        // Verify column lookup works
        let email_type = db_schema.get_column_type("users", "email").unwrap();
        assert_eq!(email_type, &UniversalType::VarChar { length: 255 });
    }

    #[test]
    fn test_type_alias_compatibility() {
        // Ensure type aliases work for backwards compatibility
        let schema: Schema = GeneratorSchema::from_yaml(SAMPLE_SCHEMA).unwrap();
        let _table: &TableDefinitionWithGenerators = schema.get_table("users").unwrap();
        let _field: &FieldDefinition = schema
            .get_table("users")
            .unwrap()
            .get_field("email")
            .unwrap();
        let _id: &IDDefinition = &schema.get_table("users").unwrap().id;
    }
}
