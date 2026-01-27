//! Value representations for the surreal-sync load testing framework.
//!
//! This module defines the intermediate value types used for data generation
//! and type conversion between different database systems.

use crate::schema::Schema;
use crate::types::UniversalType;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Error when creating a TypedValue with mismatched type and value.
#[derive(Debug, Error, Clone)]
#[error(
    "Type-value mismatch: expected {expected_value} for type {sync_type:?}, got {actual_value}"
)]
pub struct TypedValueError {
    /// The UniversalType that was specified
    pub sync_type: UniversalType,
    /// Description of the expected value kind
    pub expected_value: String,
    /// Description of the actual value kind
    pub actual_value: String,
}

/// Universal value representation with 1:1 correspondence to `UniversalType`.
///
/// Each variant of `UniversalValue` corresponds exactly to one variant of `UniversalType`,
/// enabling deterministic conversion via `to_typed_value()` without inference or fallback.
///
/// # Design Principles
///
/// 1. **Exact correspondence**: Every `UniversalType` variant has exactly one matching `UniversalValue` variant
/// 2. **No inference**: `to_typed_value()` is deterministic - no guessing or fallback
/// 3. **Type metadata included**: Variants like `Char`, `VarChar`, `Decimal` include their type parameters
/// 4. **Self-describing**: Each value knows its exact type without external context
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum UniversalValue {
    // === Boolean ===
    /// Boolean value → `UniversalType::Bool`
    Bool(bool),

    // === Integer types ===
    /// Tiny integer with display width → `UniversalType::Int8 { width }`
    Int8 {
        /// The integer value
        value: i8,
        /// Display width
        width: u8,
    },

    /// 16-bit signed integer → `UniversalType::Int16`
    Int16(i16),

    /// 32-bit signed integer → `UniversalType::Int32`
    Int32(i32),

    /// 64-bit signed integer → `UniversalType::Int64`
    Int64(i64),

    // === Floating point types ===
    /// 32-bit IEEE 754 floating point → `UniversalType::Float32`
    Float32(f32),

    /// 64-bit IEEE 754 floating point → `UniversalType::Float64`
    Float64(f64),

    // === Exact numeric ===
    /// Decimal value with precision/scale → `UniversalType::Decimal { precision, scale }`
    Decimal {
        /// String representation of the decimal value
        value: String,
        /// Total number of digits
        precision: u8,
        /// Number of digits after decimal point
        scale: u8,
    },

    // === String types ===
    /// Fixed-length character string → `UniversalType::Char { length }`
    Char {
        /// The string value
        value: String,
        /// Maximum length
        length: u16,
    },

    /// Variable-length character string → `UniversalType::VarChar { length }`
    VarChar {
        /// The string value
        value: String,
        /// Maximum length
        length: u16,
    },

    /// Unlimited text → `UniversalType::Text`
    Text(String),

    // === Binary types ===
    /// Binary large object → `UniversalType::Blob`
    Blob(Vec<u8>),

    /// Binary data → `UniversalType::Bytes`
    Bytes(Vec<u8>),

    // === Temporal types ===
    /// Date only (YYYY-MM-DD) → `UniversalType::Date`
    Date(DateTime<Utc>),

    /// Time only (HH:MM:SS) → `UniversalType::Time`
    Time(DateTime<Utc>),

    /// Timestamp without timezone (microsecond precision) → `UniversalType::LocalDateTime`
    LocalDateTime(DateTime<Utc>),

    /// Timestamp with nanosecond precision → `UniversalType::LocalDateTimeNano`
    LocalDateTimeNano(DateTime<Utc>),

    /// Timestamp with timezone → `UniversalType::ZonedDateTime`
    ZonedDateTime(DateTime<Utc>),

    // === Special types ===
    /// UUID (128-bit) → `UniversalType::Uuid`
    Uuid(Uuid),

    /// ULID (128-bit sortable identifier) → `UniversalType::Ulid`
    Ulid(ulid::Ulid),

    /// JSON document → `UniversalType::Json`
    Json(Box<serde_json::Value>),

    /// Binary JSON (PostgreSQL JSONB) → `UniversalType::Jsonb`
    Jsonb(Box<serde_json::Value>),

    // === Collection types ===
    /// Array of a specific type → `UniversalType::Array { element_type }`
    Array {
        /// The array elements
        elements: Vec<UniversalValue>,
        /// Element type
        element_type: Box<UniversalType>,
    },

    /// MySQL SET type → `UniversalType::Set { values }`
    Set {
        /// Selected values from the set
        elements: Vec<String>,
        /// Allowed values in the set definition
        allowed_values: Vec<String>,
    },

    // === Enumeration ===
    /// Enumeration type → `UniversalType::Enum { values }`
    Enum {
        /// The selected enum value
        value: String,
        /// Allowed enum values
        allowed_values: Vec<String>,
    },

    // === Spatial ===
    /// Spatial/geometry type → `UniversalType::Geometry { geometry_type }`
    Geometry {
        /// Geometry data (WKB bytes or GeoJSON object)
        data: GeometryData,
        /// Specific geometry variant
        geometry_type: crate::types::GeometryType,
    },

    /// Duration type → `UniversalType::Duration`
    Duration(std::time::Duration),

    /// Record reference/link (e.g., SurrealDB Thing) → `UniversalType::Thing`
    Thing {
        /// The target table/collection name
        table: String,
        /// The record ID (can be string, int, uuid, etc.)
        id: Box<UniversalValue>,
    },

    /// Null value (can be any nullable type)
    Null,
}

/// Geometry data representation.
///
/// Currently only supports GeoJSON format. Native geometry types
/// (Point, LineString, Polygon, etc.) may be added in the future.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeometryData(pub serde_json::Value);

impl UniversalValue {
    // === Factory methods ===

    /// Create a TinyInt value.
    pub fn tinyint(value: i8, width: u8) -> Self {
        Self::Int8 { value, width }
    }

    /// Create a Decimal value.
    pub fn decimal(value: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::Decimal {
            value: value.into(),
            precision,
            scale,
        }
    }

    /// Create a Char value.
    pub fn char(value: impl Into<String>, length: u16) -> Self {
        Self::Char {
            value: value.into(),
            length,
        }
    }

    /// Create a VarChar value.
    pub fn varchar(value: impl Into<String>, length: u16) -> Self {
        Self::VarChar {
            value: value.into(),
            length,
        }
    }

    /// Create an Array value.
    pub fn array(elements: Vec<UniversalValue>, element_type: UniversalType) -> Self {
        Self::Array {
            elements,
            element_type: Box::new(element_type),
        }
    }

    /// Create a Set value.
    pub fn set(elements: Vec<String>, allowed_values: Vec<String>) -> Self {
        Self::Set {
            elements,
            allowed_values,
        }
    }

    /// Create an Enum value.
    pub fn enum_value(value: impl Into<String>, allowed_values: Vec<String>) -> Self {
        Self::Enum {
            value: value.into(),
            allowed_values,
        }
    }

    /// Create a Geometry value from GeoJSON.
    pub fn geometry_geojson(
        data: serde_json::Value,
        geometry_type: crate::types::GeometryType,
    ) -> Self {
        Self::Geometry {
            data: GeometryData(data),
            geometry_type,
        }
    }

    /// Create a JSON value.
    pub fn json(value: serde_json::Value) -> Self {
        Self::Json(Box::new(value))
    }

    /// Create a JSONB value.
    pub fn jsonb(value: serde_json::Value) -> Self {
        Self::Jsonb(Box::new(value))
    }

    // === Predicates ===

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    // === Accessors ===

    /// Try to get this value as a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get this value as an i32.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Int32(i) => Some(*i),
            Self::Int8 { value, .. } => Some(*value as i32),
            Self::Int16(i) => Some(*i as i32),
            _ => None,
        }
    }

    /// Try to get this value as an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(i) => Some(*i),
            Self::Int32(i) => Some(*i as i64),
            Self::Int16(i) => Some(*i as i64),
            Self::Int8 { value, .. } => Some(*value as i64),
            _ => None,
        }
    }

    /// Try to get this value as an f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(f) => Some(*f),
            Self::Float32(f) => Some(*f as f64),
            _ => None,
        }
    }

    /// Try to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            Self::Char { value, .. } => Some(value),
            Self::VarChar { value, .. } => Some(value),
            Self::Enum { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Try to get this value as a byte slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b),
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get this value as a UUID.
    pub fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            Self::Uuid(u) => Some(u),
            _ => None,
        }
    }

    /// Try to get this value as a ULID.
    pub fn as_ulid(&self) -> Option<&ulid::Ulid> {
        match self {
            Self::Ulid(u) => Some(u),
            _ => None,
        }
    }

    /// Try to get this value as a DateTime.
    pub fn as_datetime(&self) -> Option<&DateTime<Utc>> {
        match self {
            Self::LocalDateTime(dt) => Some(dt),
            Self::Date(dt) => Some(dt),
            Self::Time(dt) => Some(dt),
            Self::LocalDateTimeNano(dt) => Some(dt),
            Self::ZonedDateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to get this value as an array of elements.
    pub fn as_array(&self) -> Option<&Vec<UniversalValue>> {
        match self {
            Self::Array { elements, .. } => Some(elements),
            _ => None,
        }
    }

    /// Get a human-readable description of this value's variant.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "Bool",
            Self::Int8 { .. } => "Int8",
            Self::Int16(_) => "Int16",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Float32(_) => "Float32",
            Self::Float64(_) => "Float64",
            Self::Decimal { .. } => "Decimal",
            Self::Char { .. } => "Char",
            Self::VarChar { .. } => "VarChar",
            Self::Text(_) => "Text",
            Self::Blob(_) => "Blob",
            Self::Bytes(_) => "Bytes",
            Self::Date(_) => "Date",
            Self::Time(_) => "Time",
            Self::LocalDateTime(_) => "LocalDateTime",
            Self::LocalDateTimeNano(_) => "LocalDateTimeNano",
            Self::ZonedDateTime(_) => "ZonedDateTime",
            Self::Uuid(_) => "Uuid",
            Self::Ulid(_) => "Ulid",
            Self::Json(_) => "Json",
            Self::Jsonb(_) => "Jsonb",
            Self::Array { .. } => "Array",
            Self::Set { .. } => "Set",
            Self::Enum { .. } => "Enum",
            Self::Geometry { .. } => "Geometry",
            Self::Duration(_) => "Duration",
            Self::Thing { .. } => "Thing",
            Self::Null => "Null",
        }
    }

    /// Convert this value to a TypedValue deterministically.
    ///
    /// Each `UniversalValue` variant maps to exactly one `UniversalType` variant,
    /// so there is no inference or fallback - the mapping is deterministic.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_core::{UniversalValue, UniversalType};
    ///
    /// let value = UniversalValue::Int32(42);
    /// let typed = value.to_typed_value();
    /// assert!(matches!(typed.sync_type, UniversalType::Int32));
    ///
    /// let value = UniversalValue::Text("hello".to_string());
    /// let typed = value.to_typed_value();
    /// assert!(matches!(typed.sync_type, UniversalType::Text));
    /// ```
    pub fn to_typed_value(self) -> TypedValue {
        let sync_type = self.to_type();
        // Safe to use new() since we're deriving type from the value itself
        TypedValue::new(sync_type, self)
    }

    /// Get the corresponding UniversalType for this value.
    ///
    /// This is a deterministic 1:1 mapping - no inference or fallback.
    pub fn to_type(&self) -> UniversalType {
        match self {
            Self::Bool(_) => UniversalType::Bool,
            Self::Int8 { width, .. } => UniversalType::Int8 { width: *width },
            Self::Int16(_) => UniversalType::Int16,
            Self::Int32(_) => UniversalType::Int32,
            Self::Int64(_) => UniversalType::Int64,
            Self::Float32(_) => UniversalType::Float32,
            Self::Float64(_) => UniversalType::Float64,
            Self::Decimal {
                precision, scale, ..
            } => UniversalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Self::Char { length, .. } => UniversalType::Char { length: *length },
            Self::VarChar { length, .. } => UniversalType::VarChar { length: *length },
            Self::Text(_) => UniversalType::Text,
            Self::Blob(_) => UniversalType::Blob,
            Self::Bytes(_) => UniversalType::Bytes,
            Self::Date(_) => UniversalType::Date,
            Self::Time(_) => UniversalType::Time,
            Self::LocalDateTime(_) => UniversalType::LocalDateTime,
            Self::LocalDateTimeNano(_) => UniversalType::LocalDateTimeNano,
            Self::ZonedDateTime(_) => UniversalType::ZonedDateTime,
            Self::Uuid(_) => UniversalType::Uuid,
            Self::Ulid(_) => UniversalType::Ulid,
            Self::Json(_) => UniversalType::Json,
            Self::Jsonb(_) => UniversalType::Jsonb,
            Self::Array { element_type, .. } => UniversalType::Array {
                element_type: element_type.clone(),
            },
            Self::Set { allowed_values, .. } => UniversalType::Set {
                values: allowed_values.clone(),
            },
            Self::Enum { allowed_values, .. } => UniversalType::Enum {
                values: allowed_values.clone(),
            },
            Self::Geometry { geometry_type, .. } => UniversalType::Geometry {
                geometry_type: geometry_type.clone(),
            },
            Self::Duration(_) => UniversalType::Duration,
            Self::Thing { .. } => UniversalType::Thing,
            // Null doesn't have a single type - this is a special case
            // We use Text as a placeholder, but callers should handle Null explicitly
            Self::Null => UniversalType::Text,
        }
    }
}

/// Typed value with its UniversalType for conversion.
///
/// `TypedValue` combines a `UniversalValue` with its corresponding `UniversalType`,
/// providing the type context needed for `From`/`Into` trait implementations
/// in the database-specific type crates.
#[derive(Debug, Clone)]
pub struct TypedValue {
    /// The extended type information
    pub sync_type: UniversalType,

    /// The raw generated value
    pub value: UniversalValue,
}

impl TypedValue {
    /// Create a new typed value (internal use - prefer factory methods).
    fn new(sync_type: UniversalType, value: UniversalValue) -> Self {
        Self { sync_type, value }
    }

    /// Create a typed value with a dynamically specified type, validating the combination.
    ///
    /// This validates that the type and value are compatible using strict 1:1 matching.
    /// Returns an error if the combination is invalid (e.g., passing a Text value for a Bool type).
    ///
    /// This is useful when the type is determined at runtime (e.g., from schema).
    /// For known types, prefer the specific factory methods like `bool()`, `int()`, etc.
    ///
    /// # Strict 1:1 Valid Combinations
    ///
    /// - `Null` is valid for any type
    /// - `Bool` type requires `Bool` value
    /// - `TinyInt` type requires `TinyInt` value
    /// - `SmallInt` type requires `SmallInt` value
    /// - `Int` type requires `Int` value
    /// - `BigInt` type requires `BigInt` value
    /// - `Float` type requires `Float` value
    /// - `Double` type requires `Double` value
    /// - `Decimal` type requires `Decimal` value
    /// - `Char` type requires `Char` value
    /// - `VarChar` type requires `VarChar` value
    /// - `Text` type requires `Text` value
    /// - `Blob` type requires `Blob` value
    /// - `Bytes` type requires `Bytes` value
    /// - `Date` type requires `Date` value
    /// - `Time` type requires `Time` value
    /// - `DateTime` type requires `DateTime` value
    /// - `DateTimeNano` type requires `DateTimeNano` value
    /// - `TimestampTz` type requires `TimestampTz` value
    /// - `Uuid` type requires `Uuid` value
    /// - `Json` type requires `Json` value
    /// - `Jsonb` type requires `Jsonb` value
    /// - `Array` type requires `Array` value
    /// - `Set` type requires `Set` value
    /// - `Enum` type requires `Enum` value
    /// - `Geometry` type requires `Geometry` value
    pub fn try_with_type(
        sync_type: UniversalType,
        value: UniversalValue,
    ) -> Result<Self, TypedValueError> {
        // Null is always valid for any type
        if matches!(value, UniversalValue::Null) {
            return Ok(Self::new(sync_type, value));
        }

        // Strict 1:1 validation - each type requires its exact corresponding value variant
        let is_valid = match (&sync_type, &value) {
            // Boolean
            (UniversalType::Bool, UniversalValue::Bool(_)) => true,

            // Integer types - strict matching
            (UniversalType::Int8 { .. }, UniversalValue::Int8 { .. }) => true,
            (UniversalType::Int16, UniversalValue::Int16(_)) => true,
            (UniversalType::Int32, UniversalValue::Int32(_)) => true,
            (UniversalType::Int64, UniversalValue::Int64(_)) => true,

            // Floating point types - strict matching
            (UniversalType::Float32, UniversalValue::Float32(_)) => true,
            (UniversalType::Float64, UniversalValue::Float64(_)) => true,

            // Decimal
            (UniversalType::Decimal { .. }, UniversalValue::Decimal { .. }) => true,

            // String types - strict matching
            (UniversalType::Char { .. }, UniversalValue::Char { .. }) => true,
            (UniversalType::VarChar { .. }, UniversalValue::VarChar { .. }) => true,
            (UniversalType::Text, UniversalValue::Text(_)) => true,

            // Binary types - strict matching
            (UniversalType::Blob, UniversalValue::Blob(_)) => true,
            (UniversalType::Bytes, UniversalValue::Bytes(_)) => true,

            // Temporal types - strict matching
            (UniversalType::Date, UniversalValue::Date(_)) => true,
            (UniversalType::Time, UniversalValue::Time(_)) => true,
            (UniversalType::LocalDateTime, UniversalValue::LocalDateTime(_)) => true,
            (UniversalType::LocalDateTimeNano, UniversalValue::LocalDateTimeNano(_)) => true,
            (UniversalType::ZonedDateTime, UniversalValue::ZonedDateTime(_)) => true,

            // UUID
            (UniversalType::Uuid, UniversalValue::Uuid(_)) => true,

            // JSON types - strict matching
            (UniversalType::Json, UniversalValue::Json(_)) => true,
            (UniversalType::Jsonb, UniversalValue::Jsonb(_)) => true,

            // Collection types - strict matching
            (UniversalType::Array { .. }, UniversalValue::Array { .. }) => true,
            (UniversalType::Set { .. }, UniversalValue::Set { .. }) => true,

            // Enumeration
            (UniversalType::Enum { .. }, UniversalValue::Enum { .. }) => true,

            // Geometry
            (UniversalType::Geometry { .. }, UniversalValue::Geometry { .. }) => true,

            // Duration
            (UniversalType::Duration, UniversalValue::Duration(_)) => true,

            // All other combinations are invalid
            _ => false,
        };

        if is_valid {
            Ok(Self::new(sync_type, value))
        } else {
            Err(TypedValueError {
                expected_value: Self::expected_value_description(&sync_type),
                actual_value: value.variant_name().to_string(),
                sync_type,
            })
        }
    }

    /// Get the expected value description for a given type.
    fn expected_value_description(sync_type: &UniversalType) -> String {
        match sync_type {
            UniversalType::Bool => "Bool".to_string(),
            UniversalType::Int8 { .. } => "Int8".to_string(),
            UniversalType::Int16 => "Int16".to_string(),
            UniversalType::Int32 => "Int32".to_string(),
            UniversalType::Int64 => "Int64".to_string(),
            UniversalType::Float32 => "Float32".to_string(),
            UniversalType::Float64 => "Float64".to_string(),
            UniversalType::Decimal { .. } => "Decimal".to_string(),
            UniversalType::Char { .. } => "Char".to_string(),
            UniversalType::VarChar { .. } => "VarChar".to_string(),
            UniversalType::Text => "Text".to_string(),
            UniversalType::Blob => "Blob".to_string(),
            UniversalType::Bytes => "Bytes".to_string(),
            UniversalType::Date => "Date".to_string(),
            UniversalType::Time => "Time".to_string(),
            UniversalType::LocalDateTime => "LocalDateTime".to_string(),
            UniversalType::LocalDateTimeNano => "LocalDateTimeNano".to_string(),
            UniversalType::ZonedDateTime => "ZonedDateTime".to_string(),
            UniversalType::Uuid => "Uuid".to_string(),
            UniversalType::Ulid => "Ulid".to_string(),
            UniversalType::Json => "Json".to_string(),
            UniversalType::Jsonb => "Jsonb".to_string(),
            UniversalType::Array { .. } => "Array".to_string(),
            UniversalType::Set { .. } => "Set".to_string(),
            UniversalType::Enum { .. } => "Enum".to_string(),
            UniversalType::Geometry { .. } => "Geometry".to_string(),
            UniversalType::Duration => "Duration".to_string(),
            UniversalType::Thing => "Thing".to_string(),
        }
    }

    /// Create a typed value with a dynamically specified type (unchecked).
    ///
    /// **Warning**: This does not validate the type-value combination.
    /// Prefer `try_with_type` when possible to catch mismatches early.
    ///
    /// This is useful when you're certain the combination is valid or when
    /// performance is critical and validation overhead is unacceptable.
    #[inline]
    pub fn with_type_unchecked(sync_type: UniversalType, value: UniversalValue) -> Self {
        Self::new(sync_type, value)
    }

    /// Create a boolean typed value.
    pub fn bool(value: bool) -> Self {
        Self::new(UniversalType::Bool, UniversalValue::Bool(value))
    }

    /// Create a smallint typed value.
    pub fn int16(value: i16) -> Self {
        Self::new(UniversalType::Int16, UniversalValue::Int16(value))
    }

    /// Create an integer typed value.
    pub fn int32(value: i32) -> Self {
        Self::new(UniversalType::Int32, UniversalValue::Int32(value))
    }

    /// Create a bigint typed value.
    pub fn int64(value: i64) -> Self {
        Self::new(UniversalType::Int64, UniversalValue::Int64(value))
    }

    /// Create a double typed value.
    pub fn float64(value: f64) -> Self {
        Self::new(UniversalType::Float64, UniversalValue::Float64(value))
    }

    /// Create a text typed value.
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(UniversalType::Text, UniversalValue::Text(value.into()))
    }

    /// Create a bytes typed value.
    pub fn bytes(value: Vec<u8>) -> Self {
        Self::new(UniversalType::Bytes, UniversalValue::Bytes(value))
    }

    /// Create a UUID typed value.
    pub fn uuid(value: Uuid) -> Self {
        Self::new(UniversalType::Uuid, UniversalValue::Uuid(value))
    }

    /// Create a ULID typed value.
    pub fn ulid(value: ulid::Ulid) -> Self {
        Self::new(UniversalType::Ulid, UniversalValue::Ulid(value))
    }

    /// Create a datetime typed value.
    pub fn datetime(value: DateTime<Utc>) -> Self {
        Self::new(
            UniversalType::LocalDateTime,
            UniversalValue::LocalDateTime(value),
        )
    }

    /// Create a float typed value.
    pub fn float32(value: f32) -> Self {
        Self::new(UniversalType::Float32, UniversalValue::Float32(value))
    }

    /// Create a null typed value with a specified type.
    pub fn null(sync_type: UniversalType) -> Self {
        Self::new(sync_type, UniversalValue::Null)
    }

    /// Create a decimal typed value.
    pub fn decimal(value: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::new(
            UniversalType::Decimal { precision, scale },
            UniversalValue::Decimal {
                value: value.into(),
                precision,
                scale,
            },
        )
    }

    /// Create an array typed value.
    pub fn array(elements: Vec<UniversalValue>, element_type: UniversalType) -> Self {
        Self::new(
            UniversalType::Array {
                element_type: Box::new(element_type.clone()),
            },
            UniversalValue::Array {
                elements,
                element_type: Box::new(element_type),
            },
        )
    }

    /// Create a JSON typed value from a serde_json::Value.
    pub fn json(value: serde_json::Value) -> Self {
        Self::new(UniversalType::Json, UniversalValue::Json(Box::new(value)))
    }

    /// Create a JSONB typed value from a serde_json::Value.
    pub fn jsonb(value: serde_json::Value) -> Self {
        Self::new(UniversalType::Jsonb, UniversalValue::Jsonb(Box::new(value)))
    }

    /// Create a TINYINT typed value with optional width.
    pub fn int8(value: i8, width: u8) -> Self {
        Self::new(
            UniversalType::Int8 { width },
            UniversalValue::Int8 { value, width },
        )
    }

    /// Create a CHAR typed value with specified length.
    pub fn char_type(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            UniversalType::Char { length },
            UniversalValue::Char {
                value: value.into(),
                length,
            },
        )
    }

    /// Create a VARCHAR typed value with specified length.
    pub fn varchar(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            UniversalType::VarChar { length },
            UniversalValue::VarChar {
                value: value.into(),
                length,
            },
        )
    }

    /// Create a BLOB typed value.
    pub fn blob(value: Vec<u8>) -> Self {
        Self::new(UniversalType::Blob, UniversalValue::Blob(value))
    }

    /// Create a DATE typed value from a DateTime.
    pub fn date(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::Date, UniversalValue::Date(value))
    }

    /// Create a TIME typed value from a DateTime.
    pub fn time(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::Time, UniversalValue::Time(value))
    }

    /// Create a TIMESTAMPTZ typed value.
    pub fn timestamptz(value: DateTime<Utc>) -> Self {
        Self::new(
            UniversalType::ZonedDateTime,
            UniversalValue::ZonedDateTime(value),
        )
    }

    /// Create a DATETIME with nanosecond precision typed value.
    pub fn datetime_nano(value: DateTime<Utc>) -> Self {
        Self::new(
            UniversalType::LocalDateTimeNano,
            UniversalValue::LocalDateTimeNano(value),
        )
    }

    /// Create an ENUM typed value.
    pub fn enum_type(value: impl Into<String>, variants: Vec<String>) -> Self {
        Self::new(
            UniversalType::Enum {
                values: variants.clone(),
            },
            UniversalValue::Enum {
                value: value.into(),
                allowed_values: variants,
            },
        )
    }

    /// Create a SET typed value.
    pub fn set(elements: Vec<String>, variants: Vec<String>) -> Self {
        Self::new(
            UniversalType::Set {
                values: variants.clone(),
            },
            UniversalValue::Set {
                elements,
                allowed_values: variants,
            },
        )
    }

    /// Create a GEOMETRY typed value from a GeoJSON object.
    pub fn geometry_geojson(
        value: serde_json::Value,
        geometry_type: crate::types::GeometryType,
    ) -> Self {
        Self::new(
            UniversalType::Geometry {
                geometry_type: geometry_type.clone(),
            },
            UniversalValue::Geometry {
                data: GeometryData(value),
                geometry_type,
            },
        )
    }

    /// Check if this typed value is null.
    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }

    /// Create a DURATION typed value.
    pub fn duration(value: std::time::Duration) -> Self {
        Self::new(UniversalType::Duration, UniversalValue::Duration(value))
    }
}

/// Internal row representation - the intermediate format.
///
/// `UniversalRow` represents a single row of data in the intermediate format,
/// produced by the data generator and consumed by both source populators
/// and the streaming verifier.
#[derive(Debug, Clone)]
pub struct UniversalRow {
    /// Table name
    pub table: String,

    /// Row index (for incremental support and reproducibility)
    pub index: u64,

    /// Primary key value
    pub id: UniversalValue,

    /// Field values (column name -> value)
    pub fields: HashMap<String, UniversalValue>,
}

impl UniversalRow {
    /// Create a new internal row.
    pub fn new(
        table: impl Into<String>,
        index: u64,
        id: UniversalValue,
        fields: HashMap<String, UniversalValue>,
    ) -> Self {
        Self {
            table: table.into(),
            index,
            id,
            fields,
        }
    }

    /// Create a new internal row with a builder pattern.
    pub fn builder(
        table: impl Into<String>,
        index: u64,
        id: UniversalValue,
    ) -> UniversalRowBuilder {
        UniversalRowBuilder {
            table: table.into(),
            index,
            id,
            fields: HashMap::new(),
        }
    }

    /// Get a field value by name.
    pub fn get_field(&self, name: &str) -> Option<&UniversalValue> {
        self.fields.get(name)
    }

    /// Get the number of fields (excluding the id).
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

/// Builder for `UniversalRow`.
pub struct UniversalRowBuilder {
    table: String,
    index: u64,
    id: UniversalValue,
    fields: HashMap<String, UniversalValue>,
}

impl UniversalRowBuilder {
    /// Add a field to the row.
    pub fn field(mut self, name: impl Into<String>, value: UniversalValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Build the internal row.
    pub fn build(self) -> UniversalRow {
        UniversalRow {
            table: self.table,
            index: self.index,
            id: self.id,
            fields: self.fields,
        }
    }
}

/// Converter that holds schema context for From implementations.
///
/// `RowConverter` wraps an `UniversalRow` along with the schema context,
/// enabling database-specific `From` implementations to look up type
/// information for each field.
pub struct RowConverter<'a> {
    /// The internal row to convert
    pub row: UniversalRow,

    /// Schema providing type information for fields
    pub schema: &'a Schema,
}

impl<'a> RowConverter<'a> {
    /// Create a new row converter.
    pub fn new(row: UniversalRow, schema: &'a Schema) -> Self {
        Self { row, schema }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generated_value_accessors() {
        assert_eq!(UniversalValue::Bool(true).as_bool(), Some(true));
        assert_eq!(UniversalValue::Int32(42).as_i32(), Some(42));
        assert_eq!(UniversalValue::Int64(100).as_i64(), Some(100));
        assert_eq!(UniversalValue::Float64(3.15).as_f64(), Some(3.15));
        assert_eq!(
            UniversalValue::Text("test".to_string()).as_str(),
            Some("test")
        );

        // Cross-type conversions
        assert_eq!(UniversalValue::Int32(42).as_i64(), Some(42));
        assert_eq!(UniversalValue::Bool(true).as_i32(), None);
    }

    #[test]
    fn test_typed_value_constructors() {
        let tv = TypedValue::bool(true);
        assert_eq!(tv.sync_type, UniversalType::Bool);
        assert_eq!(tv.value, UniversalValue::Bool(true));

        let tv = TypedValue::int32(42);
        assert_eq!(tv.sync_type, UniversalType::Int32);
        assert_eq!(tv.value, UniversalValue::Int32(42));
    }

    #[test]
    fn test_internal_row_builder() {
        let row = UniversalRow::builder("users", 0, UniversalValue::Int64(1))
            .field("name", UniversalValue::Text("Alice".to_string()))
            .field("age", UniversalValue::Int32(30))
            .build();

        assert_eq!(row.table, "users");
        assert_eq!(row.index, 0);
        assert_eq!(row.id, UniversalValue::Int64(1));
        assert_eq!(row.field_count(), 2);
        assert_eq!(
            row.get_field("name"),
            Some(&UniversalValue::Text("Alice".to_string()))
        );
        assert_eq!(row.get_field("age"), Some(&UniversalValue::Int32(30)));
    }

    #[test]
    fn test_try_with_type_valid_combinations() {
        // Bool type with Bool value
        assert!(TypedValue::try_with_type(UniversalType::Bool, UniversalValue::Bool(true)).is_ok());

        // Int type with Int value (strict 1:1)
        assert!(TypedValue::try_with_type(UniversalType::Int32, UniversalValue::Int32(42)).is_ok());

        // BigInt type with BigInt value (strict 1:1)
        assert!(
            TypedValue::try_with_type(UniversalType::Int64, UniversalValue::Int64(100)).is_ok()
        );

        // Text type with Text value (strict 1:1)
        assert!(TypedValue::try_with_type(
            UniversalType::Text,
            UniversalValue::Text("hello".to_string())
        )
        .is_ok());

        // DateTime type with DateTime value
        assert!(TypedValue::try_with_type(
            UniversalType::LocalDateTime,
            UniversalValue::LocalDateTime(chrono::Utc::now())
        )
        .is_ok());

        // Date type with Date value (strict 1:1)
        assert!(TypedValue::try_with_type(
            UniversalType::Date,
            UniversalValue::Date(chrono::Utc::now())
        )
        .is_ok());

        // Null is always valid
        assert!(TypedValue::try_with_type(UniversalType::Bool, UniversalValue::Null).is_ok());
        assert!(TypedValue::try_with_type(UniversalType::Int32, UniversalValue::Null).is_ok());
        assert!(TypedValue::try_with_type(UniversalType::Text, UniversalValue::Null).is_ok());

        // JSON type with Json value (strict 1:1)
        assert!(TypedValue::try_with_type(
            UniversalType::Json,
            UniversalValue::Json(Box::new(serde_json::Value::Bool(true)))
        )
        .is_ok());
    }

    #[test]
    fn test_try_with_type_invalid_combinations() {
        // Bool type with wrong value types
        let err =
            TypedValue::try_with_type(UniversalType::Bool, UniversalValue::Int32(1)).unwrap_err();
        assert_eq!(err.expected_value, "Bool");
        assert_eq!(err.actual_value, "Int32");

        // Int type with wrong value types (strict 1:1 now)
        let err =
            TypedValue::try_with_type(UniversalType::Int32, UniversalValue::Text("42".to_string()))
                .unwrap_err();
        assert_eq!(err.expected_value, "Int32");
        assert_eq!(err.actual_value, "Text");

        // Int type no longer accepts BigInt (strict 1:1)
        let err =
            TypedValue::try_with_type(UniversalType::Int32, UniversalValue::Int64(42)).unwrap_err();
        assert_eq!(err.expected_value, "Int32");
        assert_eq!(err.actual_value, "Int64");

        // BigInt type no longer accepts Int (strict 1:1)
        let err =
            TypedValue::try_with_type(UniversalType::Int64, UniversalValue::Int32(42)).unwrap_err();
        assert_eq!(err.expected_value, "Int64");
        assert_eq!(err.actual_value, "Int32");

        // Text type with wrong value types
        let err =
            TypedValue::try_with_type(UniversalType::Text, UniversalValue::Int32(42)).unwrap_err();
        assert_eq!(err.expected_value, "Text");
        assert_eq!(err.actual_value, "Int32");

        // Uuid type with Text value (strict 1:1)
        let err = TypedValue::try_with_type(
            UniversalType::Uuid,
            UniversalValue::Text("not-a-uuid".to_string()),
        )
        .unwrap_err();
        assert_eq!(err.expected_value, "Uuid");
        assert_eq!(err.actual_value, "Text");
    }

    #[test]
    fn test_try_with_type_error_message() {
        let err = TypedValue::try_with_type(
            UniversalType::Bool,
            UniversalValue::Text("true".to_string()),
        )
        .unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("Type-value mismatch"));
        assert!(msg.contains("Bool"));
        assert!(msg.contains("Text"));
    }

    #[test]
    fn test_variant_name() {
        assert_eq!(UniversalValue::Bool(true).variant_name(), "Bool");
        assert_eq!(UniversalValue::Int32(42).variant_name(), "Int32");
        assert_eq!(UniversalValue::Int64(100).variant_name(), "Int64");
        assert_eq!(UniversalValue::Float64(1.5).variant_name(), "Float64");
        assert_eq!(
            UniversalValue::Text("test".to_string()).variant_name(),
            "Text"
        );
        assert_eq!(UniversalValue::Bytes(vec![1, 2, 3]).variant_name(), "Bytes");
        assert_eq!(UniversalValue::Null.variant_name(), "Null");
    }

    #[test]
    fn test_to_typed_value_deterministic() {
        // Each UniversalValue variant should map to exactly one UniversalType
        let value = UniversalValue::Int32(42);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Int32);

        let value = UniversalValue::Text("hello".to_string());
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Text);

        let value = UniversalValue::Int64(100);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Int64);

        let value = UniversalValue::Float64(3.15);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Float64);

        let value = UniversalValue::Float32(1.5);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Float32);

        let value = UniversalValue::Int16(100);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, UniversalType::Int16);

        let value = UniversalValue::Int8 { value: 1, width: 1 };
        let typed = value.to_typed_value();
        assert!(matches!(typed.sync_type, UniversalType::Int8 { width: 1 }));
    }
}
