//! Value representations for the surreal-sync load testing framework.
//!
//! This module defines the intermediate value types used for data generation
//! and type conversion between different database systems.

use crate::schema::Schema;
use crate::types::Type;
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
    /// The Type that was specified
    pub sync_type: Type,
    /// Description of the expected value kind
    pub expected_value: String,
    /// Description of the actual value kind
    pub actual_value: String,
}

/// Universal value representation with 1:1 correspondence to `Type`.
///
/// Each variant of `Value` corresponds exactly to one variant of `Type`,
/// enabling deterministic conversion via `to_typed_value()` without inference or fallback.
///
/// # Design Principles
///
/// 1. **Exact correspondence**: Every `Type` variant has exactly one matching `Value` variant
/// 2. **No inference**: `to_typed_value()` is deterministic - no guessing or fallback
/// 3. **Type metadata included**: Variants like `Char`, `VarChar`, `Decimal` include their type parameters
/// 4. **Self-describing**: Each value knows its exact type without external context
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Value {
    // === Boolean ===
    /// Boolean value → `Type::Bool`
    Bool(bool),

    // === Integer types ===
    /// Tiny integer with display width → `Type::Int8 { width }`
    Int8 {
        /// The integer value
        value: i8,
        /// Display width
        width: u8,
    },

    /// 16-bit signed integer → `Type::Int16`
    Int16(i16),

    /// 32-bit signed integer → `Type::Int32`
    Int32(i32),

    /// 64-bit signed integer → `Type::Int64`
    Int64(i64),

    // === Floating point types ===
    /// 32-bit IEEE 754 floating point → `Type::Float32`
    Float32(f32),

    /// 64-bit IEEE 754 floating point → `Type::Float64`
    Float64(f64),

    // === Exact numeric ===
    /// Decimal value with precision/scale → `Type::Decimal { precision, scale }`
    Decimal {
        /// String representation of the decimal value
        value: String,
        /// Total number of digits
        precision: u8,
        /// Number of digits after decimal point
        scale: u8,
    },

    // === String types ===
    /// Fixed-length character string → `Type::Char { length }`
    Char {
        /// The string value
        value: String,
        /// Maximum length
        length: u16,
    },

    /// Variable-length character string → `Type::VarChar { length }`
    VarChar {
        /// The string value
        value: String,
        /// Maximum length
        length: u16,
    },

    /// Unlimited text → `Type::Text`
    Text(String),

    // === Binary types ===
    /// Binary large object → `Type::Blob`
    Blob(Vec<u8>),

    /// Binary data → `Type::Bytes`
    Bytes(Vec<u8>),

    // === Temporal types ===
    /// Date only (YYYY-MM-DD) → `Type::Date`
    Date(DateTime<Utc>),

    /// Time only (HH:MM:SS) → `Type::Time`
    Time(DateTime<Utc>),

    /// Timestamp without timezone (microsecond precision) → `Type::LocalDateTime`
    LocalDateTime(DateTime<Utc>),

    /// Timestamp with nanosecond precision → `Type::LocalDateTimeNano`
    LocalDateTimeNano(DateTime<Utc>),

    /// Timestamp with timezone → `Type::ZonedDateTime`
    ZonedDateTime(DateTime<Utc>),

    /// Time with timezone (stored as string to preserve original format)
    /// → `Type::TimeTz`
    ///
    /// Note: We intentionally use String instead of DateTime because time and datetime
    /// are fundamentally different types. DateTime implies a specific point in time,
    /// while time with timezone represents a daily recurring time in a specific timezone.
    /// Using DateTime to represent time would misrepresent the semantics.
    TimeTz(String),

    // === Special types ===
    /// UUID (128-bit) → `Type::Uuid`
    Uuid(Uuid),

    /// ULID (128-bit sortable identifier) → `Type::Ulid`
    Ulid(ulid::Ulid),

    /// JSON document → `Type::Json`
    Json(Box<serde_json::Value>),

    /// Binary JSON (PostgreSQL JSONB) → `Type::Jsonb`
    Jsonb(Box<serde_json::Value>),

    // === Collection types ===
    /// Array of a specific type → `Type::Array { element_type }`
    Array {
        /// The array elements
        elements: Vec<Value>,
        /// Element type
        element_type: Box<Type>,
    },

    /// MySQL SET type → `Type::Set { values }`
    Set {
        /// Selected values from the set
        elements: Vec<String>,
        /// Allowed values in the set definition
        allowed_values: Vec<String>,
    },

    // === Enumeration ===
    /// Enumeration type → `Type::Enum { values }`
    Enum {
        /// The selected enum value
        value: String,
        /// Allowed enum values
        allowed_values: Vec<String>,
    },

    // === Spatial ===
    /// Spatial/geometry type → `Type::Geometry { geometry_type }`
    Geometry {
        /// Geometry data (WKB bytes or GeoJSON object)
        data: GeometryData,
        /// Specific geometry variant
        geometry_type: crate::types::GeometryType,
    },

    /// Duration type → `Type::Duration`
    Duration(std::time::Duration),

    /// Record reference/link (e.g., SurrealDB Thing) → `Type::Thing`
    Thing {
        /// The target table/collection name
        table: String,
        /// The record ID (can be string, int, uuid, etc.)
        id: Box<Value>,
    },

    /// Nested object/document (e.g., MongoDB embedded documents, SurrealDB objects)
    /// → `Type::Object`
    ///
    /// This differs from Json/Jsonb which are serialized JSON storage types.
    /// Object represents a structured nested document with typed fields.
    Object(HashMap<String, Value>),

    /// Null value (can be any nullable type)
    Null,

    /// Source emitted a temporal that is not a valid calendar/chrono value
    /// (e.g. MySQL/MariaDB zero date `0000-00-00`).
    ///
    /// Distinct from [`Value::Null`]: SQL NULL stays `Null`; MySQL-style
    /// zero dates/timestamps use this sentinel so transforms retain the intended
    /// column type before the SurrealDB sink maps it.
    ZeroTemporal {
        /// Intended column type (`Date`, `Time`, `LocalDateTime`, `ZonedDateTime`, …)
        intended_type: Type,
        /// Optional source literal for transforms/debugging, e.g. `"0000-00-00 00:00:00"`
        source: Option<String>,
    },
}

/// How the SurrealDB sink represents [`Value::ZeroTemporal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZeroTemporalPolicy {
    /// SurrealDB `NONE` (undefined). Default — safe for typed datetime fields.
    #[default]
    None,
    /// SurrealDB `NULL`.
    Null,
    /// Canonical MySQL-style zero literal string (e.g. `"0000-00-00"`).
    String,
}

/// Geometry data representation.
///
/// Currently only supports GeoJSON format. Native geometry types
/// (Point, LineString, Polygon, etc.) may be added in the future.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeometryData(pub serde_json::Value);

impl Value {
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
    pub fn array(elements: Vec<Value>, element_type: Type) -> Self {
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

    /// Create a zero-temporal sentinel with the intended column type.
    pub fn zero_temporal(intended_type: Type, source: Option<String>) -> Self {
        Self::ZeroTemporal {
            intended_type,
            source,
        }
    }

    /// Canonical MySQL-style zero literal for an intended temporal type.
    pub fn canonical_zero_literal(intended_type: &Type) -> &'static str {
        match intended_type {
            Type::Date => "0000-00-00",
            Type::Time | Type::TimeTz => "00:00:00",
            Type::LocalDateTime | Type::LocalDateTimeNano | Type::ZonedDateTime => {
                "0000-00-00 00:00:00"
            }
            _ => "0000-00-00 00:00:00",
        }
    }

    /// Whether `s` is a MySQL/MariaDB zero date or zero datetime literal.
    ///
    /// Matches `0000-00-00` and `0000-00-00 00:00:00` with optional fractional seconds.
    pub fn is_mysql_zero_temporal_literal(s: &str) -> bool {
        let s = s.trim();
        if s == "0000-00-00" {
            return true;
        }
        s.starts_with("0000-00-00 00:00:00")
    }

    /// Whether year/month/day are the MySQL zero-date triple `(0, 0, 0)`.
    pub fn is_mysql_zero_date_ymd(year: u16, month: u8, day: u8) -> bool {
        year == 0 && month == 0 && day == 0
    }

    /// Whether `intended_type` may be used with [`Value::ZeroTemporal`].
    pub fn is_zero_temporal_type(intended_type: &Type) -> bool {
        matches!(
            intended_type,
            Type::Date
                | Type::Time
                | Type::LocalDateTime
                | Type::LocalDateTimeNano
                | Type::ZonedDateTime
                | Type::TimeTz
        )
    }

    // === Predicates ===

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Check if this value is a zero-temporal sentinel.
    pub fn is_zero_temporal(&self) -> bool {
        matches!(self, Self::ZeroTemporal { .. })
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
    pub fn as_array(&self) -> Option<&Vec<Value>> {
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
            Self::TimeTz(_) => "TimeTz",
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
            Self::Object(_) => "Object",
            Self::Null => "Null",
            Self::ZeroTemporal { .. } => "ZeroTemporal",
        }
    }

    /// Convert this value to a TypedValue deterministically.
    ///
    /// Each `Value` variant maps to exactly one `Type` variant,
    /// so there is no inference or fallback - the mapping is deterministic.
    ///
    /// # Example
    ///
    /// ```rust
    /// use surreal_sync_core::{Value, Type};
    ///
    /// let value = Value::Int32(42);
    /// let typed = value.to_typed_value();
    /// assert!(matches!(typed.sync_type, Type::Int32));
    ///
    /// let value = Value::Text("hello".to_string());
    /// let typed = value.to_typed_value();
    /// assert!(matches!(typed.sync_type, Type::Text));
    /// ```
    pub fn to_typed_value(self) -> TypedValue {
        let sync_type = self.to_type();
        // Safe to use new() since we're deriving type from the value itself
        TypedValue::new(sync_type, self)
    }

    /// Get the corresponding Type for this value.
    ///
    /// This is a deterministic 1:1 mapping - no inference or fallback.
    pub fn to_type(&self) -> Type {
        match self {
            Self::Bool(_) => Type::Bool,
            Self::Int8 { width, .. } => Type::Int8 { width: *width },
            Self::Int16(_) => Type::Int16,
            Self::Int32(_) => Type::Int32,
            Self::Int64(_) => Type::Int64,
            Self::Float32(_) => Type::Float32,
            Self::Float64(_) => Type::Float64,
            Self::Decimal {
                precision, scale, ..
            } => Type::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Self::Char { length, .. } => Type::Char { length: *length },
            Self::VarChar { length, .. } => Type::VarChar { length: *length },
            Self::Text(_) => Type::Text,
            Self::Blob(_) => Type::Blob,
            Self::Bytes(_) => Type::Bytes,
            Self::Date(_) => Type::Date,
            Self::Time(_) => Type::Time,
            Self::LocalDateTime(_) => Type::LocalDateTime,
            Self::LocalDateTimeNano(_) => Type::LocalDateTimeNano,
            Self::ZonedDateTime(_) => Type::ZonedDateTime,
            Self::TimeTz(_) => Type::TimeTz,
            Self::Uuid(_) => Type::Uuid,
            Self::Ulid(_) => Type::Ulid,
            Self::Json(_) => Type::Json,
            Self::Jsonb(_) => Type::Jsonb,
            Self::Array { element_type, .. } => Type::Array {
                element_type: element_type.clone(),
            },
            Self::Set { allowed_values, .. } => Type::Set {
                values: allowed_values.clone(),
            },
            Self::Enum { allowed_values, .. } => Type::Enum {
                values: allowed_values.clone(),
            },
            Self::Geometry { geometry_type, .. } => Type::Geometry {
                geometry_type: geometry_type.clone(),
            },
            Self::Duration(_) => Type::Duration,
            Self::Thing { .. } => Type::Thing,
            Self::Object(_) => Type::Object,
            // Null doesn't have a single type - this is a special case
            // We use Text as a placeholder, but callers should handle Null explicitly
            Self::Null => Type::Text,
            Self::ZeroTemporal { intended_type, .. } => intended_type.clone(),
        }
    }
}

/// Typed value with its Type for conversion.
///
/// `TypedValue` combines a `Value` with its corresponding `Type`,
/// providing the type context needed for `From`/`Into` trait implementations
/// in the database-specific type crates.
#[derive(Debug, Clone)]
pub struct TypedValue {
    /// The extended type information
    pub sync_type: Type,

    /// The raw generated value
    pub value: Value,
}

impl TypedValue {
    /// Create a new typed value (internal use - prefer factory methods).
    fn new(sync_type: Type, value: Value) -> Self {
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
    /// - `ZeroTemporal` is valid when `sync_type` equals `intended_type` and is temporal
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
    pub fn try_with_type(sync_type: Type, value: Value) -> Result<Self, TypedValueError> {
        // Null is always valid for any type
        if matches!(value, Value::Null) {
            return Ok(Self::new(sync_type, value));
        }

        // ZeroTemporal is valid when sync_type matches intended_type and is temporal
        if let Value::ZeroTemporal { intended_type, .. } = &value {
            if sync_type == *intended_type && Value::is_zero_temporal_type(&sync_type) {
                return Ok(Self::new(sync_type, value));
            }
            return Err(TypedValueError {
                expected_value: Self::expected_value_description(&sync_type),
                actual_value: value.variant_name().to_string(),
                sync_type,
            });
        }

        // Strict 1:1 validation - each type requires its exact corresponding value variant
        let is_valid = match (&sync_type, &value) {
            // Boolean
            (Type::Bool, Value::Bool(_)) => true,

            // Integer types - strict matching
            (Type::Int8 { .. }, Value::Int8 { .. }) => true,
            (Type::Int16, Value::Int16(_)) => true,
            (Type::Int32, Value::Int32(_)) => true,
            (Type::Int64, Value::Int64(_)) => true,

            // Floating point types - strict matching
            (Type::Float32, Value::Float32(_)) => true,
            (Type::Float64, Value::Float64(_)) => true,

            // Decimal
            (Type::Decimal { .. }, Value::Decimal { .. }) => true,

            // String types - strict matching
            (Type::Char { .. }, Value::Char { .. }) => true,
            (Type::VarChar { .. }, Value::VarChar { .. }) => true,
            (Type::Text, Value::Text(_)) => true,

            // Binary types - strict matching
            (Type::Blob, Value::Blob(_)) => true,
            (Type::Bytes, Value::Bytes(_)) => true,

            // Temporal types - strict matching
            (Type::Date, Value::Date(_)) => true,
            (Type::Time, Value::Time(_)) => true,
            (Type::LocalDateTime, Value::LocalDateTime(_)) => true,
            (Type::LocalDateTimeNano, Value::LocalDateTimeNano(_)) => true,
            (Type::ZonedDateTime, Value::ZonedDateTime(_)) => true,
            (Type::TimeTz, Value::TimeTz(_)) => true,

            // UUID
            (Type::Uuid, Value::Uuid(_)) => true,

            // JSON types - strict matching
            (Type::Json, Value::Json(_)) => true,
            (Type::Jsonb, Value::Jsonb(_)) => true,

            // Collection types - strict matching
            (Type::Array { .. }, Value::Array { .. }) => true,
            (Type::Set { .. }, Value::Set { .. }) => true,

            // Enumeration
            (Type::Enum { .. }, Value::Enum { .. }) => true,

            // Geometry
            (Type::Geometry { .. }, Value::Geometry { .. }) => true,

            // Duration
            (Type::Duration, Value::Duration(_)) => true,

            // Object
            (Type::Object, Value::Object(_)) => true,

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
    fn expected_value_description(sync_type: &Type) -> String {
        match sync_type {
            Type::Bool => "Bool".to_string(),
            Type::Int8 { .. } => "Int8".to_string(),
            Type::Int16 => "Int16".to_string(),
            Type::Int32 => "Int32".to_string(),
            Type::Int64 => "Int64".to_string(),
            Type::Float32 => "Float32".to_string(),
            Type::Float64 => "Float64".to_string(),
            Type::Decimal { .. } => "Decimal".to_string(),
            Type::Char { .. } => "Char".to_string(),
            Type::VarChar { .. } => "VarChar".to_string(),
            Type::Text => "Text".to_string(),
            Type::Blob => "Blob".to_string(),
            Type::Bytes => "Bytes".to_string(),
            Type::Date => "Date".to_string(),
            Type::Time => "Time".to_string(),
            Type::LocalDateTime => "LocalDateTime".to_string(),
            Type::LocalDateTimeNano => "LocalDateTimeNano".to_string(),
            Type::ZonedDateTime => "ZonedDateTime".to_string(),
            Type::TimeTz => "TimeTz".to_string(),
            Type::Uuid => "Uuid".to_string(),
            Type::Ulid => "Ulid".to_string(),
            Type::Json => "Json".to_string(),
            Type::Jsonb => "Jsonb".to_string(),
            Type::Array { .. } => "Array".to_string(),
            Type::Set { .. } => "Set".to_string(),
            Type::Enum { .. } => "Enum".to_string(),
            Type::Geometry { .. } => "Geometry".to_string(),
            Type::Duration => "Duration".to_string(),
            Type::Thing => "Thing".to_string(),
            Type::Object => "Object".to_string(),
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
    pub fn with_type_unchecked(sync_type: Type, value: Value) -> Self {
        Self::new(sync_type, value)
    }

    /// Create a boolean typed value.
    pub fn bool(value: bool) -> Self {
        Self::new(Type::Bool, Value::Bool(value))
    }

    /// Create a smallint typed value.
    pub fn int16(value: i16) -> Self {
        Self::new(Type::Int16, Value::Int16(value))
    }

    /// Create an integer typed value.
    pub fn int32(value: i32) -> Self {
        Self::new(Type::Int32, Value::Int32(value))
    }

    /// Create a bigint typed value.
    pub fn int64(value: i64) -> Self {
        Self::new(Type::Int64, Value::Int64(value))
    }

    /// Create a double typed value.
    pub fn float64(value: f64) -> Self {
        Self::new(Type::Float64, Value::Float64(value))
    }

    /// Create a text typed value.
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(Type::Text, Value::Text(value.into()))
    }

    /// Create a bytes typed value.
    pub fn bytes(value: Vec<u8>) -> Self {
        Self::new(Type::Bytes, Value::Bytes(value))
    }

    /// Create a UUID typed value.
    pub fn uuid(value: Uuid) -> Self {
        Self::new(Type::Uuid, Value::Uuid(value))
    }

    /// Create a ULID typed value.
    pub fn ulid(value: ulid::Ulid) -> Self {
        Self::new(Type::Ulid, Value::Ulid(value))
    }

    /// Create a datetime typed value.
    pub fn datetime(value: DateTime<Utc>) -> Self {
        Self::new(Type::LocalDateTime, Value::LocalDateTime(value))
    }

    /// Create a float typed value.
    pub fn float32(value: f32) -> Self {
        Self::new(Type::Float32, Value::Float32(value))
    }

    /// Create a null typed value with a specified type.
    pub fn null(sync_type: Type) -> Self {
        Self::new(sync_type, Value::Null)
    }

    /// Create a zero-temporal typed value with the intended column type.
    ///
    /// # Panics
    ///
    /// Panics if `intended_type` is not a temporal type allowed for zero temporals.
    pub fn zero_temporal(intended_type: Type, source: Option<String>) -> Self {
        assert!(
            Value::is_zero_temporal_type(&intended_type),
            "zero_temporal requires a temporal Type, got {intended_type:?}"
        );
        Self::new(
            intended_type.clone(),
            Value::ZeroTemporal {
                intended_type,
                source,
            },
        )
    }

    /// Create a decimal typed value.
    pub fn decimal(value: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::new(
            Type::Decimal { precision, scale },
            Value::Decimal {
                value: value.into(),
                precision,
                scale,
            },
        )
    }

    /// Create an array typed value.
    pub fn array(elements: Vec<Value>, element_type: Type) -> Self {
        Self::new(
            Type::Array {
                element_type: Box::new(element_type.clone()),
            },
            Value::Array {
                elements,
                element_type: Box::new(element_type),
            },
        )
    }

    /// Create a JSON typed value from a serde_json::Value.
    pub fn json(value: serde_json::Value) -> Self {
        Self::new(Type::Json, Value::Json(Box::new(value)))
    }

    /// Create a JSONB typed value from a serde_json::Value.
    pub fn jsonb(value: serde_json::Value) -> Self {
        Self::new(Type::Jsonb, Value::Jsonb(Box::new(value)))
    }

    /// Create a TINYINT typed value with optional width.
    pub fn int8(value: i8, width: u8) -> Self {
        Self::new(Type::Int8 { width }, Value::Int8 { value, width })
    }

    /// Create a CHAR typed value with specified length.
    pub fn char_type(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            Type::Char { length },
            Value::Char {
                value: value.into(),
                length,
            },
        )
    }

    /// Create a VARCHAR typed value with specified length.
    pub fn varchar(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            Type::VarChar { length },
            Value::VarChar {
                value: value.into(),
                length,
            },
        )
    }

    /// Create a BLOB typed value.
    pub fn blob(value: Vec<u8>) -> Self {
        Self::new(Type::Blob, Value::Blob(value))
    }

    /// Create a DATE typed value from a DateTime.
    pub fn date(value: DateTime<Utc>) -> Self {
        Self::new(Type::Date, Value::Date(value))
    }

    /// Create a TIME typed value from a DateTime.
    pub fn time(value: DateTime<Utc>) -> Self {
        Self::new(Type::Time, Value::Time(value))
    }

    /// Create a TIMESTAMPTZ typed value.
    pub fn timestamptz(value: DateTime<Utc>) -> Self {
        Self::new(Type::ZonedDateTime, Value::ZonedDateTime(value))
    }

    /// Create a DATETIME with nanosecond precision typed value.
    pub fn datetime_nano(value: DateTime<Utc>) -> Self {
        Self::new(Type::LocalDateTimeNano, Value::LocalDateTimeNano(value))
    }

    /// Create an ENUM typed value.
    pub fn enum_type(value: impl Into<String>, variants: Vec<String>) -> Self {
        Self::new(
            Type::Enum {
                values: variants.clone(),
            },
            Value::Enum {
                value: value.into(),
                allowed_values: variants,
            },
        )
    }

    /// Create a SET typed value.
    pub fn set(elements: Vec<String>, variants: Vec<String>) -> Self {
        Self::new(
            Type::Set {
                values: variants.clone(),
            },
            Value::Set {
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
            Type::Geometry {
                geometry_type: geometry_type.clone(),
            },
            Value::Geometry {
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
        Self::new(Type::Duration, Value::Duration(value))
    }
}

/// Internal row representation - the intermediate format.
///
/// `Row` represents a single row of data in the intermediate format,
/// produced by the data generator and consumed by both source populators
/// and the streaming verifier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// Table name
    pub table: String,

    /// Row index (for incremental support and reproducibility)
    pub index: u64,

    /// Primary key value
    pub id: Value,

    /// Field values (column name -> value)
    pub fields: HashMap<String, Value>,
}

impl Row {
    /// Create a new internal row.
    pub fn new(
        table: impl Into<String>,
        index: u64,
        id: Value,
        fields: HashMap<String, Value>,
    ) -> Self {
        Self {
            table: table.into(),
            index,
            id,
            fields,
        }
    }

    /// Create a new internal row with a builder pattern.
    pub fn builder(table: impl Into<String>, index: u64, id: Value) -> RowBuilder {
        RowBuilder {
            table: table.into(),
            index,
            id,
            fields: HashMap::new(),
        }
    }

    /// Get a field value by name.
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    /// Get the number of fields (excluding the id).
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

/// Builder for `Row`.
pub struct RowBuilder {
    table: String,
    index: u64,
    id: Value,
    fields: HashMap<String, Value>,
}

impl RowBuilder {
    /// Add a field to the row.
    pub fn field(mut self, name: impl Into<String>, value: Value) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Build the internal row.
    pub fn build(self) -> Row {
        Row {
            table: self.table,
            index: self.index,
            id: self.id,
            fields: self.fields,
        }
    }
}

/// Converter that holds schema context for From implementations.
///
/// `RowConverter` wraps an `Row` along with the schema context,
/// enabling database-specific `From` implementations to look up type
/// information for each field.
pub struct RowConverter<'a> {
    /// The internal row to convert
    pub row: Row,

    /// Schema providing type information for fields
    pub schema: &'a Schema,
}

impl<'a> RowConverter<'a> {
    /// Create a new row converter.
    pub fn new(row: Row, schema: &'a Schema) -> Self {
        Self { row, schema }
    }
}

// ============================================================================
// Universal Change Type (for incremental sync)
// ============================================================================

/// Operation type for incremental sync changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChangeOp {
    /// Create a new record
    Create,
    /// Update an existing record
    Update,
    /// Delete a record
    Delete,
}

/// A database-agnostic change event for incremental sync.
///
/// This type represents a change (INSERT/UPDATE/DELETE) from a source database
/// in a shared IR that can be converted to any target database format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    /// The operation type (Create/Update/Delete)
    pub operation: ChangeOp,
    /// The table name
    pub table: String,
    /// The record ID
    pub id: Value,
    /// Field values (None for Delete operations)
    pub fields: Option<HashMap<String, Value>>,
}

impl Change {
    /// Create a new change.
    pub fn new(
        operation: ChangeOp,
        table: impl Into<String>,
        id: Value,
        fields: Option<HashMap<String, Value>>,
    ) -> Self {
        Self {
            operation,
            table: table.into(),
            id,
            fields,
        }
    }

    /// Create a CREATE change.
    pub fn create(table: impl Into<String>, id: Value, fields: HashMap<String, Value>) -> Self {
        Self::new(ChangeOp::Create, table, id, Some(fields))
    }

    /// Create an UPDATE change.
    pub fn update(table: impl Into<String>, id: Value, fields: HashMap<String, Value>) -> Self {
        Self::new(ChangeOp::Update, table, id, Some(fields))
    }

    /// Create a DELETE change.
    pub fn delete(table: impl Into<String>, id: Value) -> Self {
        Self::new(ChangeOp::Delete, table, id, None)
    }
}

// ============================================================================
// Universal Relation Type (for graph database relationships)
// ============================================================================

/// A database-agnostic graph relation/edge.
///
/// This type represents a relationship between two nodes (records) in graph databases
/// like Neo4j and SurrealDB. It contains the relation type, IDs for both endpoints,
/// and optional properties on the relation itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    /// The relation type (table name in SurrealDB terms)
    pub relation_type: String,
    /// The relation's own ID
    pub id: Value,
    /// The source node reference (table name + id)
    pub input: ThingRef,
    /// The target node reference (table name + id)
    pub output: ThingRef,
    /// Properties on the relation itself
    pub data: HashMap<String, Value>,
}

/// A reference to a record/node in a specific table.
///
/// This is used to identify the endpoints of a relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThingRef {
    /// The table/collection name
    pub table: String,
    /// The record ID
    pub id: Value,
}

impl ThingRef {
    /// Create a new thing reference.
    pub fn new(table: impl Into<String>, id: Value) -> Self {
        Self {
            table: table.into(),
            id,
        }
    }
}

impl Relation {
    /// Create a new relation.
    pub fn new(
        relation_type: impl Into<String>,
        id: Value,
        input: ThingRef,
        output: ThingRef,
        data: HashMap<String, Value>,
    ) -> Self {
        Self {
            relation_type: relation_type.into(),
            id,
            input,
            output,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generated_value_accessors() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Int32(42).as_i32(), Some(42));
        assert_eq!(Value::Int64(100).as_i64(), Some(100));
        assert_eq!(Value::Float64(3.15).as_f64(), Some(3.15));
        assert_eq!(Value::Text("test".to_string()).as_str(), Some("test"));

        // Cross-type conversions
        assert_eq!(Value::Int32(42).as_i64(), Some(42));
        assert_eq!(Value::Bool(true).as_i32(), None);
    }

    #[test]
    fn test_typed_value_constructors() {
        let tv = TypedValue::bool(true);
        assert_eq!(tv.sync_type, Type::Bool);
        assert_eq!(tv.value, Value::Bool(true));

        let tv = TypedValue::int32(42);
        assert_eq!(tv.sync_type, Type::Int32);
        assert_eq!(tv.value, Value::Int32(42));
    }

    #[test]
    fn test_internal_row_builder() {
        let row = Row::builder("users", 0, Value::Int64(1))
            .field("name", Value::Text("Alice".to_string()))
            .field("age", Value::Int32(30))
            .build();

        assert_eq!(row.table, "users");
        assert_eq!(row.index, 0);
        assert_eq!(row.id, Value::Int64(1));
        assert_eq!(row.field_count(), 2);
        assert_eq!(
            row.get_field("name"),
            Some(&Value::Text("Alice".to_string()))
        );
        assert_eq!(row.get_field("age"), Some(&Value::Int32(30)));
    }

    #[test]
    fn test_try_with_type_valid_combinations() {
        // Bool type with Bool value
        assert!(TypedValue::try_with_type(Type::Bool, Value::Bool(true)).is_ok());

        // Int type with Int value (strict 1:1)
        assert!(TypedValue::try_with_type(Type::Int32, Value::Int32(42)).is_ok());

        // BigInt type with BigInt value (strict 1:1)
        assert!(TypedValue::try_with_type(Type::Int64, Value::Int64(100)).is_ok());

        // Text type with Text value (strict 1:1)
        assert!(TypedValue::try_with_type(Type::Text, Value::Text("hello".to_string())).is_ok());

        // DateTime type with DateTime value
        assert!(TypedValue::try_with_type(
            Type::LocalDateTime,
            Value::LocalDateTime(chrono::Utc::now())
        )
        .is_ok());

        // Date type with Date value (strict 1:1)
        assert!(TypedValue::try_with_type(Type::Date, Value::Date(chrono::Utc::now())).is_ok());

        // Null is always valid
        assert!(TypedValue::try_with_type(Type::Bool, Value::Null).is_ok());
        assert!(TypedValue::try_with_type(Type::Int32, Value::Null).is_ok());
        assert!(TypedValue::try_with_type(Type::Text, Value::Null).is_ok());

        // ZeroTemporal is valid when sync_type matches intended_type
        assert!(TypedValue::try_with_type(
            Type::Date,
            Value::zero_temporal(Type::Date, Some("0000-00-00".into()))
        )
        .is_ok());
        assert!(TypedValue::try_with_type(
            Type::LocalDateTime,
            Value::zero_temporal(Type::LocalDateTime, Some("0000-00-00 00:00:00".into()))
        )
        .is_ok());
        // Mismatched intended_type is invalid
        assert!(TypedValue::try_with_type(
            Type::Date,
            Value::zero_temporal(Type::LocalDateTime, None)
        )
        .is_err());
        // Non-temporal intended_type is invalid
        assert!(
            TypedValue::try_with_type(Type::Text, Value::zero_temporal(Type::Text, None)).is_err()
        );

        // JSON type with Json value (strict 1:1)
        assert!(TypedValue::try_with_type(
            Type::Json,
            Value::Json(Box::new(serde_json::Value::Bool(true)))
        )
        .is_ok());
    }

    #[test]
    fn test_try_with_type_invalid_combinations() {
        // Bool type with wrong value types
        let err = TypedValue::try_with_type(Type::Bool, Value::Int32(1)).unwrap_err();
        assert_eq!(err.expected_value, "Bool");
        assert_eq!(err.actual_value, "Int32");

        // Int type with wrong value types (strict 1:1 now)
        let err =
            TypedValue::try_with_type(Type::Int32, Value::Text("42".to_string())).unwrap_err();
        assert_eq!(err.expected_value, "Int32");
        assert_eq!(err.actual_value, "Text");

        // Int type no longer accepts BigInt (strict 1:1)
        let err = TypedValue::try_with_type(Type::Int32, Value::Int64(42)).unwrap_err();
        assert_eq!(err.expected_value, "Int32");
        assert_eq!(err.actual_value, "Int64");

        // BigInt type no longer accepts Int (strict 1:1)
        let err = TypedValue::try_with_type(Type::Int64, Value::Int32(42)).unwrap_err();
        assert_eq!(err.expected_value, "Int64");
        assert_eq!(err.actual_value, "Int32");

        // Text type with wrong value types
        let err = TypedValue::try_with_type(Type::Text, Value::Int32(42)).unwrap_err();
        assert_eq!(err.expected_value, "Text");
        assert_eq!(err.actual_value, "Int32");

        // Uuid type with Text value (strict 1:1)
        let err = TypedValue::try_with_type(Type::Uuid, Value::Text("not-a-uuid".to_string()))
            .unwrap_err();
        assert_eq!(err.expected_value, "Uuid");
        assert_eq!(err.actual_value, "Text");
    }

    #[test]
    fn test_try_with_type_error_message() {
        let err =
            TypedValue::try_with_type(Type::Bool, Value::Text("true".to_string())).unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("Type-value mismatch"));
        assert!(msg.contains("Bool"));
        assert!(msg.contains("Text"));
    }

    #[test]
    fn test_variant_name() {
        assert_eq!(Value::Bool(true).variant_name(), "Bool");
        assert_eq!(Value::Int32(42).variant_name(), "Int32");
        assert_eq!(Value::Int64(100).variant_name(), "Int64");
        assert_eq!(Value::Float64(1.5).variant_name(), "Float64");
        assert_eq!(Value::Text("test".to_string()).variant_name(), "Text");
        assert_eq!(Value::Bytes(vec![1, 2, 3]).variant_name(), "Bytes");
        assert_eq!(Value::Null.variant_name(), "Null");
    }

    #[test]
    fn test_to_typed_value_deterministic() {
        // Each Value variant should map to exactly one Type
        let value = Value::Int32(42);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Int32);

        let value = Value::Text("hello".to_string());
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Text);

        let value = Value::Int64(100);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Int64);

        let value = Value::Float64(3.15);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Float64);

        let value = Value::Float32(1.5);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Float32);

        let value = Value::Int16(100);
        let typed = value.to_typed_value();
        assert_eq!(typed.sync_type, Type::Int16);

        let value = Value::Int8 { value: 1, width: 1 };
        let typed = value.to_typed_value();
        assert!(matches!(typed.sync_type, Type::Int8 { width: 1 }));
    }

    #[test]
    fn universal_row_serde_roundtrip() {
        let row = Row::builder("users", 7, Value::Int64(42))
            .field("name", Value::Text("Alice".to_string()))
            .field("active", Value::Bool(true))
            .build();
        let json = serde_json::to_string(&row).expect("serialize row");
        let back: Row = serde_json::from_str(&json).expect("deserialize row");
        assert_eq!(back, row);
    }

    #[test]
    fn universal_change_serde_roundtrip_and_golden() {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::Text("Bob".to_string()));
        let change = Change::create("users", Value::Int64(9), data);

        let json = serde_json::to_string(&change).expect("serialize change");
        let back: Change = serde_json::from_str(&json).expect("deserialize change");
        assert_eq!(back, change);

        // Golden: fixed JSON (single field so HashMap order is irrelevant).
        let golden = r#"{"operation":"Create","table":"users","id":{"type":"Int64","value":9},"fields":{"name":{"type":"Text","value":"Bob"}}}"#;
        let from_golden: Change = serde_json::from_str(golden).expect("deserialize golden");
        assert_eq!(from_golden, change);

        let delete = Change::delete("users", Value::Int64(9));
        let delete_json = serde_json::to_string(&delete).unwrap();
        let delete_back: Change = serde_json::from_str(&delete_json).unwrap();
        assert_eq!(delete_back, delete);
        assert!(delete_back.fields.is_none());
    }
}
