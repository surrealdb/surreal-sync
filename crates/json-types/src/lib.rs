//! JSON type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and JSON formats.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → JSON value conversion
//! - [`reverse`] - JSON value → TypedValue conversion
//!
//! # Example
//!
//! ```ignore
//! use json_types::{JsonValue, JsonValueWithSchema};
//! use sync_core::{TypedValue, SyncDataType};
//!
//! // Forward: TypedValue → JSON value
//! let tv = TypedValue::text("hello");
//! let json_val: JsonValue = tv.into();
//!
//! // Reverse: JSON value → TypedValue
//! let json = serde_json::json!(42);
//! let json_with_schema = JsonValueWithSchema::new(json, &SyncDataType::Int);
//! let tv = json_with_schema.to_typed_value().unwrap();
//! ```

pub mod forward;
pub mod reverse;

pub use forward::JsonValue;
pub use reverse::{
    json_to_generated_value_with_config, json_to_typed_value_with_config, JsonConversionConfig,
    JsonValueWithSchema,
};
