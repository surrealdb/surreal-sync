//! MySQL type conversions and shared helpers for surreal-sync.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and MySQL's native types, DDL generation, and shared read
//! helpers (chunked table reads, JSON column detection) used by MySQL origins.
//!
//! # Features
//!
//! All origin/protocol units are **on by default** (full MySQL stack). Opt out
//! with `default-features = false` and enable only what you need:
//!
//! | Feature | Default | What it enables |
//! |---------|---------|-----------------|
//! | `binlog_protocol` | yes | Wire protocol + `binlog` cell conversion helpers |
//! | `from_binlog` | yes | Binlog CDC origin (`from_binlog` module; implies `binlog_protocol`) |
//! | `from_trigger` | yes | Trigger/audit-table CDC origin |
//!
//! # Structure
//!
//! - `forward`: Convert `TypedValue` → `MySQLValue` (for INSERT operations)
//! - `reverse`: Convert MySQL values → `TypedValue` (for reading data)
//! - `ddl`: Generate MySQL DDL from `Type`
//! - `schema`: MySQL column type to Type conversion
//! - `chunk`: Primary-key keyset pagination reads
//! - `json_columns`: Detect JSON columns on MySQL and MariaDB
//! - `ssl`: Shared TLS mode types and `mysql_async` pool helpers
//! - `binlog_protocol` (feature): MySQL/MariaDB ROW-format binlog replication protocol
//! - `from_binlog` (feature): MySQL/MariaDB binlog CDC origin
//! - `from_trigger` (feature): MySQL trigger/audit-table CDC origin
//!
//! # Embedders (binlog)
//!
//! Defaults already include `from_binlog`. Import the embed surface from the
//! origin module (not the crate root):
//!
//! ```rust,ignore
//! use surreal_sync_mysql::from_binlog::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([/* transforms */]).await?;
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use surreal_sync_mysql::{MySQLValue, MySQLDdl, ToDdl, mysql_column_to_universal_type};
//! use surreal_sync_core::{TypedValue, Type, Value};
//!
//! // Forward conversion
//! let typed_value = TypedValue::bool(true);
//! let mysql_value: MySQLValue = typed_value.into();
//!
//! // DDL generation
//! let ddl = MySQLDdl;
//! assert_eq!(ddl.to_ddl(&Type::Bool), "TINYINT(1)");
//!
//! // Column type conversion
//! let ut = mysql_column_to_universal_type("INT", "int(11)", None, None);
//! assert_eq!(ut, Type::Int32);
//! ```

#[cfg(feature = "binlog_protocol")]
pub mod binlog;
#[cfg(feature = "binlog_protocol")]
pub mod binlog_protocol;
pub mod chunk;
pub mod ddl;
pub mod forward;
pub mod json_columns;
pub mod reverse;
pub mod schema;
pub mod ssl;

#[cfg(feature = "from_binlog")]
pub mod from_binlog;

#[cfg(feature = "from_trigger")]
pub mod from_trigger;

#[cfg(feature = "binlog_protocol")]
pub use binlog::{
    apply_mysql_json_diffs_to_cell, binlog_cell_to_universal_value, BinlogColumnMeta,
};
pub use chunk::{get_primary_key_columns, read_table_chunk, TableChunk};
pub use ddl::{MySQLDdl, ToDdl};
pub use forward::MySQLValue;
pub use json_columns::{get_json_columns, json_object_value_expr};
pub use reverse::{
    json_to_generated_value_with_config, json_to_typed_value_with_config, row_to_typed_values,
    row_to_typed_values_with_config, ConversionError, JsonConversionConfig, MySQLValueWithSchema,
    RowConversionConfig,
};
pub use schema::mysql_column_to_universal_type;
pub use ssl::{new_mysql_pool_with_ssl, new_mysql_pool_with_ssl_sync, SslMode, SslOptions};
