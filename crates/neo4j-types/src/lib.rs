//! Neo4j/Bolt type conversions for sync-core types.
//!
//! This crate provides bidirectional type conversions between sync-core's
//! `TypedValue`/`UniversalValue` and Neo4j's Bolt types.
//!
//! # Modules
//!
//! - [`forward`] - TypedValue/UniversalValue → Neo4j Cypher literals
//! - [`reverse`] - Neo4j BoltType → TypedValue/UniversalValue
//! - [`error`] - Error types for conversion failures
//!
//! # Key Design Principles
//!
//! 1. **No silent fallbacks** - All unexpected cases return explicit errors
//! 2. **Timezone-aware** - Date/time conversions require IANA timezone strings
//! 3. **Type-safe** - Uses Rust's type system to prevent invalid conversions
//!
//! # Example
//!
//! ```ignore
//! use neo4j_types::{forward::typed_to_cypher_literal, reverse::convert_bolt_to_typed_value};
//! use sync_core::TypedValue;
//!
//! // Convert TypedValue to Cypher literal
//! let tv = TypedValue::text("hello");
//! let cypher = typed_to_cypher_literal(&tv)?;
//! assert_eq!(cypher, "'hello'");
//!
//! // Convert BoltType to TypedValue
//! let bolt_value = neo4rs::BoltType::String("hello".to_string());
//! let tv = convert_bolt_to_typed_value(bolt_value, &UniversalType::Text, "UTC")?;
//! ```

pub mod error;
pub mod forward;
pub mod reverse;

pub use error::{Neo4jTypesError, Result};
pub use forward::{typed_to_cypher_literal, universal_to_cypher_literal};
pub use reverse::{convert_bolt_to_typed_value, convert_bolt_to_universal_value};
