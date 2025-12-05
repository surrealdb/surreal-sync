//! MongoDB/BSON type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue` and MongoDB's BSON types.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue → BSON value conversion
//! - [`reverse`] - BSON value → TypedValue conversion
//!
//! # Example
//!
//! ```ignore
//! use mongodb_types::BsonValue;
//! use sync_core::TypedValue;
//!
//! // Convert TypedValue to BSON value
//! let tv = TypedValue::text("hello");
//! let bson_val: BsonValue = tv.into();
//! ```

pub mod forward;
pub mod reverse;

pub use forward::BsonValue;
pub use reverse::BsonValueWithSchema;
