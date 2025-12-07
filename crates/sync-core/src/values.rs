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

/// Raw generated value before type conversion.
///
/// `UniversalValue` represents the raw, type-agnostic value produced by
/// the data generator. It holds the actual data that will be converted
/// to database-specific formats via the `TypedValue` wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UniversalValue {
    /// Boolean value
    Bool(bool),

    /// 32-bit signed integer
    Int32(i32),

    /// 64-bit signed integer
    Int64(i64),

    /// 64-bit floating point
    Float64(f64),

    /// String value
    String(String),

    /// Binary data
    Bytes(Vec<u8>),

    /// UUID value
    Uuid(Uuid),

    /// Date/time with timezone
    DateTime(DateTime<Utc>),

    /// Decimal value stored as string with precision info
    Decimal {
        /// String representation of the decimal value
        value: String,
        /// Total number of digits
        precision: u8,
        /// Number of digits after decimal point
        scale: u8,
    },

    /// Array of values
    Array(Vec<UniversalValue>),

    /// Object/map of values
    Object(HashMap<String, UniversalValue>),

    /// Null value
    Null,
}

impl UniversalValue {
    /// Create a new decimal value.
    pub fn decimal(value: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::Decimal {
            value: value.into(),
            precision,
            scale,
        }
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

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
            _ => None,
        }
    }

    /// Try to get this value as an i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(i) => Some(*i),
            Self::Int32(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Try to get this value as an f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get this value as a byte slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b),
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

    /// Try to get this value as a DateTime.
    pub fn as_datetime(&self) -> Option<&DateTime<Utc>> {
        match self {
            Self::DateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to get this value as an array.
    pub fn as_array(&self) -> Option<&Vec<UniversalValue>> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to get this value as an object.
    pub fn as_object(&self) -> Option<&HashMap<String, UniversalValue>> {
        match self {
            Self::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Get a human-readable description of this value's variant.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "Bool",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Float64(_) => "Float64",
            Self::String(_) => "String",
            Self::Bytes(_) => "Bytes",
            Self::Uuid(_) => "Uuid",
            Self::DateTime(_) => "DateTime",
            Self::Decimal { .. } => "Decimal",
            Self::Array(_) => "Array",
            Self::Object(_) => "Object",
            Self::Null => "Null",
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
    /// This validates that the type and value are compatible. Returns an error if the
    /// combination is invalid (e.g., passing a String value for a Bool type).
    ///
    /// This is useful when the type is determined at runtime (e.g., from schema).
    /// For known types, prefer the specific factory methods like `bool()`, `int()`, etc.
    ///
    /// # Valid combinations
    ///
    /// - `Null` is valid for any type
    /// - `Bool` type requires `Bool` value
    /// - Integer types (`TinyInt`, `SmallInt`, `Int`) require `Int32` value
    /// - `BigInt` type requires `Int64` value
    /// - Floating point types (`Float`, `Double`) require `Float64` value
    /// - String types (`Char`, `VarChar`, `Text`, `Enum`) require `String` value
    /// - Binary types (`Bytes`, `Blob`) require `Bytes` value
    /// - `Uuid` type requires `Uuid` value
    /// - Date/time types require `DateTime` or `String` value
    /// - `Decimal` type requires `Decimal` or `String` value
    /// - `Array`/`Set` types require `Array` value
    /// - `Json`/`Jsonb` types accept any value (they're flexible containers)
    /// - `Geometry` type requires `Bytes` or `Object` value
    pub fn try_with_type(
        sync_type: UniversalType,
        value: UniversalValue,
    ) -> Result<Self, TypedValueError> {
        // Null is always valid for any type
        if matches!(value, UniversalValue::Null) {
            return Ok(Self::new(sync_type, value));
        }

        let is_valid = match &sync_type {
            // Boolean type
            UniversalType::Bool => matches!(value, UniversalValue::Bool(_)),

            // Integer types - accept both Int32 and Int64 for flexibility
            // Generators may produce Int64 even for smaller integer types
            UniversalType::TinyInt { .. }
            | UniversalType::SmallInt
            | UniversalType::Int
            | UniversalType::BigInt => {
                matches!(value, UniversalValue::Int32(_) | UniversalValue::Int64(_))
            }

            // Floating point types
            UniversalType::Float | UniversalType::Double => {
                matches!(value, UniversalValue::Float64(_))
            }

            // String types
            UniversalType::Char { .. }
            | UniversalType::VarChar { .. }
            | UniversalType::Text
            | UniversalType::Enum { .. } => matches!(value, UniversalValue::String(_)),

            // Binary types
            UniversalType::Bytes | UniversalType::Blob => matches!(value, UniversalValue::Bytes(_)),

            // UUID type
            UniversalType::Uuid => matches!(value, UniversalValue::Uuid(_)),

            // Date/time types - accept DateTime or String (for formatted dates)
            UniversalType::DateTime
            | UniversalType::DateTimeNano
            | UniversalType::TimestampTz
            | UniversalType::Date
            | UniversalType::Time => {
                matches!(
                    value,
                    UniversalValue::DateTime(_) | UniversalValue::String(_)
                )
            }

            // Decimal type - accept Decimal or String
            UniversalType::Decimal { .. } => {
                matches!(
                    value,
                    UniversalValue::Decimal { .. } | UniversalValue::String(_)
                )
            }

            // Array and Set types
            UniversalType::Array { .. } | UniversalType::Set { .. } => {
                matches!(value, UniversalValue::Array(_))
            }

            // JSON types are flexible - accept any value
            UniversalType::Json | UniversalType::Jsonb => true,

            // Geometry can be bytes (WKB) or object (GeoJSON)
            UniversalType::Geometry { .. } => {
                matches!(value, UniversalValue::Bytes(_) | UniversalValue::Object(_))
            }
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
            UniversalType::TinyInt { .. }
            | UniversalType::SmallInt
            | UniversalType::Int
            | UniversalType::BigInt => "Int32 or Int64".to_string(),
            UniversalType::Float | UniversalType::Double => "Float64".to_string(),
            UniversalType::Char { .. }
            | UniversalType::VarChar { .. }
            | UniversalType::Text
            | UniversalType::Enum { .. } => "String".to_string(),
            UniversalType::Bytes | UniversalType::Blob => "Bytes".to_string(),
            UniversalType::Uuid => "Uuid".to_string(),
            UniversalType::DateTime
            | UniversalType::DateTimeNano
            | UniversalType::TimestampTz
            | UniversalType::Date
            | UniversalType::Time => "DateTime or String".to_string(),
            UniversalType::Decimal { .. } => "Decimal or String".to_string(),
            UniversalType::Array { .. } | UniversalType::Set { .. } => "Array".to_string(),
            UniversalType::Json | UniversalType::Jsonb => "any value".to_string(),
            UniversalType::Geometry { .. } => "Bytes or Object".to_string(),
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
    pub fn smallint(value: i32) -> Self {
        Self::new(UniversalType::SmallInt, UniversalValue::Int32(value))
    }

    /// Create an integer typed value.
    pub fn int(value: i32) -> Self {
        Self::new(UniversalType::Int, UniversalValue::Int32(value))
    }

    /// Create a bigint typed value.
    pub fn bigint(value: i64) -> Self {
        Self::new(UniversalType::BigInt, UniversalValue::Int64(value))
    }

    /// Create a double typed value.
    pub fn double(value: f64) -> Self {
        Self::new(UniversalType::Double, UniversalValue::Float64(value))
    }

    /// Create a text typed value.
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(UniversalType::Text, UniversalValue::String(value.into()))
    }

    /// Create a bytes typed value.
    pub fn bytes(value: Vec<u8>) -> Self {
        Self::new(UniversalType::Bytes, UniversalValue::Bytes(value))
    }

    /// Create a UUID typed value.
    pub fn uuid(value: Uuid) -> Self {
        Self::new(UniversalType::Uuid, UniversalValue::Uuid(value))
    }

    /// Create a datetime typed value.
    pub fn datetime(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::DateTime, UniversalValue::DateTime(value))
    }

    /// Create a float typed value.
    pub fn float(value: f64) -> Self {
        Self::new(UniversalType::Float, UniversalValue::Float64(value))
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
    pub fn array(values: Vec<UniversalValue>, element_type: UniversalType) -> Self {
        Self::new(
            UniversalType::Array {
                element_type: Box::new(element_type),
            },
            UniversalValue::Array(values),
        )
    }

    /// Create a JSON object typed value.
    pub fn json_object(obj: std::collections::HashMap<String, UniversalValue>) -> Self {
        Self::new(UniversalType::Json, UniversalValue::Object(obj))
    }

    /// Create a JSON typed value with any UniversalValue.
    pub fn json(value: UniversalValue) -> Self {
        Self::new(UniversalType::Json, value)
    }

    /// Create a JSONB typed value with any UniversalValue.
    pub fn jsonb(value: UniversalValue) -> Self {
        Self::new(UniversalType::Jsonb, value)
    }

    /// Create a TINYINT typed value with optional width.
    pub fn tinyint(value: i32, width: u8) -> Self {
        Self::new(
            UniversalType::TinyInt { width },
            UniversalValue::Int32(value),
        )
    }

    /// Create a CHAR typed value with specified length.
    pub fn char_type(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            UniversalType::Char { length },
            UniversalValue::String(value.into()),
        )
    }

    /// Create a VARCHAR typed value with specified length.
    pub fn varchar(value: impl Into<String>, length: u16) -> Self {
        Self::new(
            UniversalType::VarChar { length },
            UniversalValue::String(value.into()),
        )
    }

    /// Create a BLOB typed value.
    pub fn blob(value: Vec<u8>) -> Self {
        Self::new(UniversalType::Blob, UniversalValue::Bytes(value))
    }

    /// Create a DATE typed value from a DateTime.
    pub fn date(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::Date, UniversalValue::DateTime(value))
    }

    /// Create a DATE typed value from a string.
    pub fn date_string(value: impl Into<String>) -> Self {
        Self::new(UniversalType::Date, UniversalValue::String(value.into()))
    }

    /// Create a TIME typed value from a DateTime.
    pub fn time(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::Time, UniversalValue::DateTime(value))
    }

    /// Create a TIME typed value from a string.
    pub fn time_string(value: impl Into<String>) -> Self {
        Self::new(UniversalType::Time, UniversalValue::String(value.into()))
    }

    /// Create a TIMESTAMPTZ typed value.
    pub fn timestamptz(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::TimestampTz, UniversalValue::DateTime(value))
    }

    /// Create a DATETIME with nanosecond precision typed value.
    pub fn datetime_nano(value: DateTime<Utc>) -> Self {
        Self::new(UniversalType::DateTimeNano, UniversalValue::DateTime(value))
    }

    /// Create an ENUM typed value.
    pub fn enum_type(value: impl Into<String>, variants: Vec<String>) -> Self {
        Self::new(
            UniversalType::Enum { values: variants },
            UniversalValue::String(value.into()),
        )
    }

    /// Create a SET typed value.
    pub fn set(values: Vec<UniversalValue>, variants: Vec<String>) -> Self {
        Self::new(
            UniversalType::Set { values: variants },
            UniversalValue::Array(values),
        )
    }

    /// Create a GEOMETRY typed value from bytes.
    pub fn geometry_bytes(value: Vec<u8>, geometry_type: crate::types::GeometryType) -> Self {
        Self::new(
            UniversalType::Geometry { geometry_type },
            UniversalValue::Bytes(value),
        )
    }

    /// Create a GEOMETRY typed value from an object (GeoJSON).
    pub fn geometry_object(
        value: std::collections::HashMap<String, UniversalValue>,
        geometry_type: crate::types::GeometryType,
    ) -> Self {
        Self::new(
            UniversalType::Geometry { geometry_type },
            UniversalValue::Object(value),
        )
    }

    /// Check if this typed value is null.
    pub fn is_null(&self) -> bool {
        self.value.is_null()
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
            UniversalValue::String("test".to_string()).as_str(),
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

        let tv = TypedValue::int(42);
        assert_eq!(tv.sync_type, UniversalType::Int);
        assert_eq!(tv.value, UniversalValue::Int32(42));
    }

    #[test]
    fn test_internal_row_builder() {
        let row = UniversalRow::builder("users", 0, UniversalValue::Int64(1))
            .field("name", UniversalValue::String("Alice".to_string()))
            .field("age", UniversalValue::Int32(30))
            .build();

        assert_eq!(row.table, "users");
        assert_eq!(row.index, 0);
        assert_eq!(row.id, UniversalValue::Int64(1));
        assert_eq!(row.field_count(), 2);
        assert_eq!(
            row.get_field("name"),
            Some(&UniversalValue::String("Alice".to_string()))
        );
        assert_eq!(row.get_field("age"), Some(&UniversalValue::Int32(30)));
    }

    #[test]
    fn test_try_with_type_valid_combinations() {
        // Bool type with Bool value
        assert!(TypedValue::try_with_type(UniversalType::Bool, UniversalValue::Bool(true)).is_ok());

        // Int type with Int32 value
        assert!(TypedValue::try_with_type(UniversalType::Int, UniversalValue::Int32(42)).is_ok());

        // Int type with Int64 value (flexible for generators)
        assert!(TypedValue::try_with_type(UniversalType::Int, UniversalValue::Int64(42)).is_ok());

        // BigInt type with Int64 value
        assert!(
            TypedValue::try_with_type(UniversalType::BigInt, UniversalValue::Int64(100)).is_ok()
        );

        // BigInt type with Int32 value (also allowed)
        assert!(
            TypedValue::try_with_type(UniversalType::BigInt, UniversalValue::Int32(100)).is_ok()
        );

        // Text type with String value
        assert!(TypedValue::try_with_type(
            UniversalType::Text,
            UniversalValue::String("hello".to_string())
        )
        .is_ok());

        // DateTime type with DateTime value
        assert!(TypedValue::try_with_type(
            UniversalType::DateTime,
            UniversalValue::DateTime(chrono::Utc::now())
        )
        .is_ok());

        // DateTime type with String value (formatted date)
        assert!(TypedValue::try_with_type(
            UniversalType::Date,
            UniversalValue::String("2024-01-01".to_string())
        )
        .is_ok());

        // Null is always valid
        assert!(TypedValue::try_with_type(UniversalType::Bool, UniversalValue::Null).is_ok());
        assert!(TypedValue::try_with_type(UniversalType::Int, UniversalValue::Null).is_ok());
        assert!(TypedValue::try_with_type(UniversalType::Text, UniversalValue::Null).is_ok());

        // JSON accepts any value
        assert!(TypedValue::try_with_type(UniversalType::Json, UniversalValue::Bool(true)).is_ok());
        assert!(TypedValue::try_with_type(UniversalType::Json, UniversalValue::Int32(42)).is_ok());
        assert!(TypedValue::try_with_type(
            UniversalType::Json,
            UniversalValue::String("test".to_string())
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

        // Int type with wrong value types
        let err =
            TypedValue::try_with_type(UniversalType::Int, UniversalValue::String("42".to_string()))
                .unwrap_err();
        assert_eq!(err.expected_value, "Int32 or Int64");
        assert_eq!(err.actual_value, "String");

        // BigInt type with wrong value types (but Int32 is allowed now)
        assert!(
            TypedValue::try_with_type(UniversalType::BigInt, UniversalValue::Int32(42)).is_ok()
        );
        // Test with wrong type
        let err = TypedValue::try_with_type(
            UniversalType::BigInt,
            UniversalValue::String("42".to_string()),
        )
        .unwrap_err();
        assert_eq!(err.expected_value, "Int32 or Int64");
        assert_eq!(err.actual_value, "String");

        // Text type with wrong value types
        let err =
            TypedValue::try_with_type(UniversalType::Text, UniversalValue::Int32(42)).unwrap_err();
        assert_eq!(err.expected_value, "String");
        assert_eq!(err.actual_value, "Int32");

        // Uuid type with String value
        let err = TypedValue::try_with_type(
            UniversalType::Uuid,
            UniversalValue::String("not-a-uuid".to_string()),
        )
        .unwrap_err();
        assert_eq!(err.expected_value, "Uuid");
        assert_eq!(err.actual_value, "String");
    }

    #[test]
    fn test_try_with_type_error_message() {
        let err = TypedValue::try_with_type(
            UniversalType::Bool,
            UniversalValue::String("true".to_string()),
        )
        .unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("Type-value mismatch"));
        assert!(msg.contains("Bool"));
        assert!(msg.contains("String"));
    }

    #[test]
    fn test_variant_name() {
        assert_eq!(UniversalValue::Bool(true).variant_name(), "Bool");
        assert_eq!(UniversalValue::Int32(42).variant_name(), "Int32");
        assert_eq!(UniversalValue::Int64(100).variant_name(), "Int64");
        assert_eq!(UniversalValue::Float64(1.5).variant_name(), "Float64");
        assert_eq!(
            UniversalValue::String("test".to_string()).variant_name(),
            "String"
        );
        assert_eq!(UniversalValue::Bytes(vec![1, 2, 3]).variant_name(), "Bytes");
        assert_eq!(UniversalValue::Null.variant_name(), "Null");
    }
}
