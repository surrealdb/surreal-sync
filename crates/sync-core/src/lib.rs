//! Core types for the surreal-sync framework.
//!
//! This crate provides the foundational types used across the sync
//! framework, including:
//!
//! - [`UniversalType`] - Universal type representation for all supported databases
//! - [`UniversalValue`] - Raw generated values before type conversion
//! - [`TypedValue`] - Values with type information for conversion
//! - [`UniversalRow`] - Intermediate row representation
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
//! use sync_core::types::UniversalType;
//! use sync_core::values::{UniversalValue, TypedValue};
//!
//! // Create a typed value using factory methods
//! let value = TypedValue::int32(42);
//!
//! // For dynamic types (e.g., from schema), use try_with_type for validation:
//! let dynamic_value = TypedValue::try_with_type(
//!     UniversalType::Int32,
//!     UniversalValue::Int32(42)
//! ).expect("valid type-value combination");
//!
//! // Type-specific crates implement From<TypedValue> for their native types:
//! // let mysql_value: MySQLValue = value.into();
//! ```

pub mod schema;
pub mod types;
pub mod values;

// Re-exports for convenience
pub use schema::{
    FieldDefinition, GeneratorConfig, IDDefinition, Schema, SchemaError, TableDefinition,
};
// Legacy alias for backwards compatibility
#[deprecated(since = "1.0.0", note = "Use Schema instead")]
pub type LoadTestSchema = Schema;
pub use types::{GeometryType, ToDdl, UniversalType};
pub use values::{
    GeometryData, RowConverter, TypedValue, TypedValueError, UniversalRow, UniversalRowBuilder,
    UniversalValue,
};
