//! PostgreSQL type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and PostgreSQL's native types via `tokio-postgres`.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → PostgreSQL value conversion
//! - [`reverse`] - PostgreSQL value → TypedValue conversion
//! - [`ddl`] - PostgreSQL DDL generation from SyncDataType
//!
//! # Example
//!
//! ```ignore
//! use postgresql_types::{PostgreSQLValue, PostgreSQLDdl, ToDdl};
//! use sync_core::{TypedValue, SyncDataType};
//!
//! // Convert TypedValue to PostgreSQL value
//! let tv = TypedValue::text("hello");
//! let pg_val: PostgreSQLValue = tv.into();
//!
//! // Generate DDL
//! let ddl = PostgreSQLDdl;
//! let type_str = ddl.to_ddl(&SyncDataType::Text);
//! assert_eq!(type_str, "TEXT");
//! ```

pub mod ddl;
pub mod forward;
pub mod reverse;

pub use ddl::{PostgreSQLDdl, ToDdl};
pub use forward::PostgreSQLValue;
pub use reverse::PostgreSQLValueWithSchema;
