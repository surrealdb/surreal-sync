//! Core types for the surreal-sync framework.
//!
//! This crate provides the foundational types used across the sync
//! framework, including:
//!
//! - [`SyncDataType`] - Universal type representation for all supported databases
//! - [`GeneratedValue`] - Raw generated values before type conversion
//! - [`TypedValue`] - Values with type information for conversion
//! - [`InternalRow`] - Intermediate row representation
//! - [`SyncSchema`] - Schema definitions loaded from YAML
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
//! use sync_core::types::SyncDataType;
//! use sync_core::values::{GeneratedValue, TypedValue};
//!
//! // Create a typed value
//! let value = TypedValue::new(
//!     SyncDataType::Int,
//!     GeneratedValue::Int32(42)
//! );
//!
//! // Type-specific crates implement From<TypedValue> for their native types:
//! // let mysql_value: MySQLValue = value.into();
//! ```

pub mod schema;
pub mod types;
pub mod values;

// Re-exports for convenience
pub use schema::{FieldSchema, GeneratorConfig, IdField, SchemaError, SyncSchema, TableSchema};
// Legacy alias for backwards compatibility
#[deprecated(since = "1.0.0", note = "Use SyncSchema instead")]
pub type LoadTestSchema = SyncSchema;
pub use types::{GeometryType, SyncDataType, ToDdl};
pub use values::{GeneratedValue, InternalRow, InternalRowBuilder, RowConverter, TypedValue};
