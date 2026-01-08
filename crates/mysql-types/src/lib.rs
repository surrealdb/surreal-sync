//! MySQL type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and MySQL's native types, as well as DDL generation.
//!
//! # Structure
//!
//! - `forward`: Convert `TypedValue` → `MySQLValue` (for INSERT operations)
//! - `reverse`: Convert MySQL values → `TypedValue` (for reading data)
//! - `ddl`: Generate MySQL DDL from `UniversalType`
//! - `schema`: MySQL column type to UniversalType conversion
//!
//! # Example
//!
//! ```rust,ignore
//! use mysql_types::{MySQLValue, MySQLDdl, ToDdl, mysql_column_to_universal_type};
//! use sync_core::{TypedValue, UniversalType, UniversalValue};
//!
//! // Forward conversion
//! let typed_value = TypedValue::bool(true);
//! let mysql_value: MySQLValue = typed_value.into();
//!
//! // DDL generation
//! let ddl = MySQLDdl;
//! assert_eq!(ddl.to_ddl(&UniversalType::Bool), "TINYINT(1)");
//!
//! // Column type conversion
//! let ut = mysql_column_to_universal_type("INT", "int(11)", None, None);
//! assert_eq!(ut, UniversalType::Int32);
//! ```

pub mod ddl;
pub mod forward;
pub mod reverse;
pub mod schema;

pub use ddl::{MySQLDdl, ToDdl};
pub use forward::MySQLValue;
pub use reverse::{
    json_to_generated_value_with_config, json_to_typed_value_with_config, row_to_typed_values,
    row_to_typed_values_with_config, ConversionError, JsonConversionConfig, MySQLValueWithSchema,
    RowConversionConfig,
};
pub use schema::mysql_column_to_universal_type;
