//! CSV type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and CSV string formats.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → CSV string conversion
//! - [`reverse`] - CSV string → TypedValue conversion
//!
//! # Example
//!
//! ```ignore
//! use csv_types::{CsvValue, CsvStringWithSchema};
//! use sync_core::{TypedValue, UniversalType};
//!
//! // Forward: TypedValue → CSV string
//! let tv = TypedValue::text("hello");
//! let csv_val: CsvValue = tv.into();
//!
//! // Reverse: CSV string → TypedValue
//! let csv_str = CsvStringWithSchema::new("42", &UniversalType::Int32);
//! let tv = csv_str.to_typed_value().unwrap();
//! ```

pub mod forward;
pub mod reverse;

pub use forward::CsvValue;
pub use forward::{escape_csv, typed_values_to_csv_line, typed_values_to_csv_line_ordered};
pub use reverse::CsvStringWithSchema;
pub use reverse::{csv_string_to_typed_value, csv_string_to_typed_value_inferred, CsvParseError};
