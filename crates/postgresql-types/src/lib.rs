//! PostgreSQL type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and PostgreSQL's native types via `tokio-postgres`.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → PostgreSQL value conversion
//! - [`reverse`] - PostgreSQL value → TypedValue conversion
//! - [`ddl`] - PostgreSQL DDL generation from Type
//! - [`schema`] - PostgreSQL column type to Type conversion
//!
//! # Example
//!
//! ```ignore
//! use postgresql_types::{PostgreSQLValue, PostgreSQLDdl, ToDdl, postgresql_column_to_universal_type};
//! use sync_core::{TypedValue, Type};
//!
//! // Convert TypedValue to PostgreSQL value
//! let tv = TypedValue::text("hello");
//! let pg_val: PostgreSQLValue = tv.into();
//!
//! // Generate DDL
//! let ddl = PostgreSQLDdl;
//! let type_str = ddl.to_ddl(&Type::Text);
//! assert_eq!(type_str, "TEXT");
//!
//! // Convert PostgreSQL column type to Type
//! let ut = postgresql_column_to_universal_type("integer", None, None);
//! assert_eq!(ut, Type::Int32);
//! ```

pub mod ddl;
pub mod forward;
pub mod reverse;
pub mod schema;

pub use ddl::{PostgreSQLDdl, ToDdl};
pub use forward::PostgreSQLValue;
pub use reverse::PostgreSQLValueWithSchema;
pub use schema::postgresql_column_to_universal_type;
