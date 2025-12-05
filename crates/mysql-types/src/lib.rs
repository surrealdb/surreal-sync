//! MySQL type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and MySQL's native types, as well as DDL generation.
//!
//! # Structure
//!
//! - `forward`: Convert `TypedValue` → `MySQLValue` (for INSERT operations)
//! - `reverse`: Convert MySQL values → `TypedValue` (for reading data)
//! - `ddl`: Generate MySQL DDL from `SyncDataType`
//!
//! # Example
//!
//! ```rust,ignore
//! use mysql_types::{MySQLValue, MySQLDdl, ToDdl};
//! use sync_core::{TypedValue, SyncDataType, GeneratedValue};
//!
//! // Forward conversion
//! let typed_value = TypedValue::bool(true);
//! let mysql_value: MySQLValue = typed_value.into();
//!
//! // DDL generation
//! let ddl = MySQLDdl;
//! assert_eq!(ddl.to_ddl(&SyncDataType::Bool), "TINYINT(1)");
//! ```

pub mod ddl;
pub mod forward;
pub mod reverse;

pub use ddl::{MySQLDdl, ToDdl};
pub use forward::MySQLValue;
pub use reverse::MySQLValueWithSchema;
