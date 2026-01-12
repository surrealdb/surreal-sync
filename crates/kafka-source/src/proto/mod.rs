//! Protobuf parsing and decoding.
//!
//! This module provides:
//! - Schema parsing from .proto files
//! - Runtime decoding of protobuf messages
//!
//! The type definitions (ProtoMessage, ProtoFieldValue, etc.) are in kafka-types.
//! This module provides the parsing and decoding logic.

pub mod decoder;
pub mod parser;

pub use decoder::ProtoDecoder;
pub use parser::ProtoParser;
