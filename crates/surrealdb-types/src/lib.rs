//! SurrealDB type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and SurrealDB's native types.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → SurrealDB value conversion
//! - [`reverse`] - SurrealDB value → TypedValue conversion
//!
//! # Example
//!
//! ```ignore
//! use surrealdb_types::SurrealValue;
//! use sync_core::TypedValue;
//!
//! // Convert TypedValue to SurrealDB value
//! let tv = TypedValue::text("hello");
//! let surreal_val: SurrealValue = tv.into();
//! ```

pub mod forward;
pub mod record;
pub mod relation;
pub mod reverse;
pub mod schema;

pub use forward::typed_values_to_surreal_map;
pub use forward::RecordWithSurrealValues;
pub use forward::SurrealValue;
pub use record::typed_values_to_record_using_field_as_id;
pub use record::typed_values_to_record_with_bytes_id;
pub use relation::Relation;
pub use reverse::SurrealValueWithSchema;
pub use schema::convert_id_with_database_schema;
pub use schema::json_to_surreal_with_table_schema;
pub use schema::json_to_surreal_with_universal_type;
