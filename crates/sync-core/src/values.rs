//! Value representations for the surreal-sync load testing framework.
//!
//! This module defines the intermediate value types used for data generation
//! and type conversion between different database systems.

use crate::schema::SyncSchema;
use crate::types::SyncDataType;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Raw generated value before type conversion.
///
/// `GeneratedValue` represents the raw, type-agnostic value produced by
/// the data generator. It holds the actual data that will be converted
/// to database-specific formats via the `TypedValue` wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GeneratedValue {
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
    Array(Vec<GeneratedValue>),

    /// Object/map of values
    Object(HashMap<String, GeneratedValue>),

    /// Null value
    Null,
}

impl GeneratedValue {
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
    pub fn as_array(&self) -> Option<&Vec<GeneratedValue>> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to get this value as an object.
    pub fn as_object(&self) -> Option<&HashMap<String, GeneratedValue>> {
        match self {
            Self::Object(obj) => Some(obj),
            _ => None,
        }
    }
}

/// Typed value with its SyncDataType for conversion.
///
/// `TypedValue` combines a `GeneratedValue` with its corresponding `SyncDataType`,
/// providing the type context needed for `From`/`Into` trait implementations
/// in the database-specific type crates.
#[derive(Debug, Clone)]
pub struct TypedValue {
    /// The extended type information
    pub sync_type: SyncDataType,

    /// The raw generated value
    pub value: GeneratedValue,
}

impl TypedValue {
    /// Create a new typed value.
    pub fn new(sync_type: SyncDataType, value: GeneratedValue) -> Self {
        Self { sync_type, value }
    }

    /// Create a boolean typed value.
    pub fn bool(value: bool) -> Self {
        Self::new(SyncDataType::Bool, GeneratedValue::Bool(value))
    }

    /// Create a smallint typed value.
    pub fn smallint(value: i32) -> Self {
        Self::new(SyncDataType::SmallInt, GeneratedValue::Int32(value))
    }

    /// Create an integer typed value.
    pub fn int(value: i32) -> Self {
        Self::new(SyncDataType::Int, GeneratedValue::Int32(value))
    }

    /// Create a bigint typed value.
    pub fn bigint(value: i64) -> Self {
        Self::new(SyncDataType::BigInt, GeneratedValue::Int64(value))
    }

    /// Create a double typed value.
    pub fn double(value: f64) -> Self {
        Self::new(SyncDataType::Double, GeneratedValue::Float64(value))
    }

    /// Create a text typed value.
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(SyncDataType::Text, GeneratedValue::String(value.into()))
    }

    /// Create a bytes typed value.
    pub fn bytes(value: Vec<u8>) -> Self {
        Self::new(SyncDataType::Bytes, GeneratedValue::Bytes(value))
    }

    /// Create a UUID typed value.
    pub fn uuid(value: Uuid) -> Self {
        Self::new(SyncDataType::Uuid, GeneratedValue::Uuid(value))
    }

    /// Create a datetime typed value.
    pub fn datetime(value: DateTime<Utc>) -> Self {
        Self::new(SyncDataType::DateTime, GeneratedValue::DateTime(value))
    }

    /// Create a float typed value.
    pub fn float(value: f64) -> Self {
        Self::new(SyncDataType::Float, GeneratedValue::Float64(value))
    }

    /// Create a null typed value with a specified type.
    pub fn null(sync_type: SyncDataType) -> Self {
        Self::new(sync_type, GeneratedValue::Null)
    }

    /// Create a decimal typed value.
    pub fn decimal(value: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::new(
            SyncDataType::Decimal { precision, scale },
            GeneratedValue::Decimal {
                value: value.into(),
                precision,
                scale,
            },
        )
    }

    /// Create an array typed value.
    pub fn array(values: Vec<GeneratedValue>, element_type: SyncDataType) -> Self {
        Self::new(
            SyncDataType::Array {
                element_type: Box::new(element_type),
            },
            GeneratedValue::Array(values),
        )
    }

    /// Create a JSON object typed value.
    pub fn json_object(obj: std::collections::HashMap<String, GeneratedValue>) -> Self {
        Self::new(SyncDataType::Json, GeneratedValue::Object(obj))
    }

    /// Check if this typed value is null.
    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }
}

/// Internal row representation - the intermediate format.
///
/// `InternalRow` represents a single row of data in the intermediate format,
/// produced by the data generator and consumed by both source populators
/// and the streaming verifier.
#[derive(Debug, Clone)]
pub struct InternalRow {
    /// Table name
    pub table: String,

    /// Row index (for incremental support and reproducibility)
    pub index: u64,

    /// Primary key value
    pub id: GeneratedValue,

    /// Field values (column name -> value)
    pub fields: HashMap<String, GeneratedValue>,
}

impl InternalRow {
    /// Create a new internal row.
    pub fn new(
        table: impl Into<String>,
        index: u64,
        id: GeneratedValue,
        fields: HashMap<String, GeneratedValue>,
    ) -> Self {
        Self {
            table: table.into(),
            index,
            id,
            fields,
        }
    }

    /// Create a new internal row with a builder pattern.
    pub fn builder(table: impl Into<String>, index: u64, id: GeneratedValue) -> InternalRowBuilder {
        InternalRowBuilder {
            table: table.into(),
            index,
            id,
            fields: HashMap::new(),
        }
    }

    /// Get a field value by name.
    pub fn get_field(&self, name: &str) -> Option<&GeneratedValue> {
        self.fields.get(name)
    }

    /// Get the number of fields (excluding the id).
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

/// Builder for `InternalRow`.
pub struct InternalRowBuilder {
    table: String,
    index: u64,
    id: GeneratedValue,
    fields: HashMap<String, GeneratedValue>,
}

impl InternalRowBuilder {
    /// Add a field to the row.
    pub fn field(mut self, name: impl Into<String>, value: GeneratedValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Build the internal row.
    pub fn build(self) -> InternalRow {
        InternalRow {
            table: self.table,
            index: self.index,
            id: self.id,
            fields: self.fields,
        }
    }
}

/// Converter that holds schema context for From implementations.
///
/// `RowConverter` wraps an `InternalRow` along with the schema context,
/// enabling database-specific `From` implementations to look up type
/// information for each field.
pub struct RowConverter<'a> {
    /// The internal row to convert
    pub row: InternalRow,

    /// Schema providing type information for fields
    pub schema: &'a SyncSchema,
}

impl<'a> RowConverter<'a> {
    /// Create a new row converter.
    pub fn new(row: InternalRow, schema: &'a SyncSchema) -> Self {
        Self { row, schema }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generated_value_accessors() {
        assert_eq!(GeneratedValue::Bool(true).as_bool(), Some(true));
        assert_eq!(GeneratedValue::Int32(42).as_i32(), Some(42));
        assert_eq!(GeneratedValue::Int64(100).as_i64(), Some(100));
        assert_eq!(GeneratedValue::Float64(3.15).as_f64(), Some(3.15));
        assert_eq!(
            GeneratedValue::String("test".to_string()).as_str(),
            Some("test")
        );

        // Cross-type conversions
        assert_eq!(GeneratedValue::Int32(42).as_i64(), Some(42));
        assert_eq!(GeneratedValue::Bool(true).as_i32(), None);
    }

    #[test]
    fn test_typed_value_constructors() {
        let tv = TypedValue::bool(true);
        assert_eq!(tv.sync_type, SyncDataType::Bool);
        assert_eq!(tv.value, GeneratedValue::Bool(true));

        let tv = TypedValue::int(42);
        assert_eq!(tv.sync_type, SyncDataType::Int);
        assert_eq!(tv.value, GeneratedValue::Int32(42));
    }

    #[test]
    fn test_internal_row_builder() {
        let row = InternalRow::builder("users", 0, GeneratedValue::Int64(1))
            .field("name", GeneratedValue::String("Alice".to_string()))
            .field("age", GeneratedValue::Int32(30))
            .build();

        assert_eq!(row.table, "users");
        assert_eq!(row.index, 0);
        assert_eq!(row.id, GeneratedValue::Int64(1));
        assert_eq!(row.field_count(), 2);
        assert_eq!(
            row.get_field("name"),
            Some(&GeneratedValue::String("Alice".to_string()))
        );
        assert_eq!(row.get_field("age"), Some(&GeneratedValue::Int32(30)));
    }
}
