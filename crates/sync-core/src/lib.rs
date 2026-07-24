//! Core types for the surreal-sync framework.
//!
//! This crate provides the foundational types used across the sync
//! framework, including:
//!
//! - [`Type`] - Universal type representation for all supported databases
//! - [`Value`] - Raw generated values before type conversion
//! - [`TypedValue`] - Values with type information for conversion
//! - [`Row`] - Intermediate row representation
//! - [`Schema`] - Schema definitions loaded from YAML
//!
//! # Architecture
//!
//! The sync-core crate sits at the foundation of the sync framework:
//!
//! ```text
//! sync-core (this crate)
//!    │
//!    ├─── loadtest-generator  (depends on sync-core for types)
//!    │
//!    ├─── mysql-types         (implements From/Into for MySQL)
//!    ├─── postgresql-types    (implements From/Into for PostgreSQL)
//!    ├─── mongodb-types       (implements From/Into for MongoDB)
//!    ├─── surrealdb-types     (implements From/Into for SurrealDB)
//!    └─── json-types          (implements From/Into for JSON/CSV)
//! ```
//!
//! # Example
//!
//! ```rust
//! use sync_core::types::Type;
//! use sync_core::values::{Value, TypedValue};
//!
//! // Create a typed value using factory methods
//! let value = TypedValue::int32(42);
//!
//! // For dynamic types (e.g., from schema), use try_with_type for validation:
//! let dynamic_value = TypedValue::try_with_type(
//!     Type::Int32,
//!     Value::Int32(42)
//! ).expect("valid type-value combination");
//!
//! // Type-specific crates implement From<TypedValue> for their native types:
//! // let mysql_value: MySQLValue = value.into();
//! ```

pub mod foreign_keys;
pub mod id_columns;
pub mod relation_change;
pub mod schema;
pub mod types;
pub mod values;

// Re-exports for convenience
// Foreign key types
pub use foreign_keys::{classify_table, ForeignKeyDefinition, TableKind};

// ID / primary-key column helpers
pub use id_columns::{
    apply_id_column_overrides, build_composite_record_id, flatten_composite_id,
    parse_id_column_overrides, stringify_id_part, IdColumnOverrides, IdColumnsError,
};

// Base types (context-neutral, no generators)
pub use schema::{ColumnDefinition, DatabaseSchema, TableDefinition};

// Generator types (with generators)
pub use schema::{
    FieldDefinition, GeneratorConfig, GeneratorFieldDefinition, GeneratorIDDefinition,
    GeneratorSchema, GeneratorTableDefinition, IDDefinition, Schema, SchemaError,
    TableDefinitionWithGenerators,
};

// Legacy alias for backwards compatibility
#[deprecated(since = "1.0.0", note = "Use GeneratorSchema instead")]
pub type LoadTestSchema = Schema;
pub use relation_change::RelationChange;
pub use types::{GeometryType, ToDdl, Type};
pub use values::{
    Change, ChangeOp, GeometryData, Relation, Row, RowBuilder, RowConverter, ThingRef, TypedValue,
    TypedValueError, Value, ZeroTemporalPolicy,
};
