//! JSON and CSV type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and JSON/CSV formats.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → JSON value conversion
//! - [`reverse`] - JSON value → TypedValue conversion
//! - [`csv`] - TypedValue → CSV string conversion
//!
//! # Example
//!
//! ```ignore
//! use json_types::{JsonValue, CsvValue};
//! use sync_core::TypedValue;
//!
//! // Convert TypedValue to JSON value
//! let tv = TypedValue::text("hello");
//! let json_val: JsonValue = tv.into();
//!
//! // Convert TypedValue to CSV string
//! let csv_val: CsvValue = tv.into();
//! ```

pub mod csv;
pub mod forward;
pub mod reverse;

pub use csv::CsvValue;
pub use forward::JsonValue;
pub use reverse::JsonValueWithSchema;
