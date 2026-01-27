//! Core data types for the surreal-sync load testing framework.
//!
//! This module defines `UniversalType`, the universal type universe that represents
//! all supported data types across different database sources and SurrealDB.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

/// Universal data type representation for surreal-sync.
///
/// `UniversalType` represents the complete type universe across ALL supported data sources.
/// Each database (including SurrealDB) defines its own mapping FROM UniversalType TO its
/// native type via `From`/`Into` trait implementations in the respective type crates.
///
/// # Design Principles
///
/// 1. **Source-agnostic**: Represents the conceptual type, not any database's specific type
/// 2. **Type-safe conversions**: All databases implement mapping via `From`/`Into` traits
/// 3. **Precision preservation**: High-precision decimals exceeding SurrealDB's 128-bit are stored as strings
/// 4. **DDL derivation**: Each database derives its DDL from UniversalType via the `ToDdl` trait
///
/// # YAML Format
///
/// Simple types can be specified as strings:
/// ```yaml
/// type: uuid
/// type: int
/// type: text
/// ```
///
/// Complex types use object format:
/// ```yaml
/// type:
///   type: var_char
///   length: 255
/// type:
///   type: decimal
///   precision: 10
///   scale: 2
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum UniversalType {
    // Boolean
    /// Boolean value
    Bool,

    // Integer types (sized)
    /// 8-bit signed integer
    /// Used for e.g. MySQL TINYINT with display width (e.g., `Int8(1)` for boolean-like)
    Int8 {
        /// Display width
        width: u8,
    },

    /// 16-bit signed integer
    Int16,

    /// 32-bit signed integer
    Int32,

    /// 64-bit signed integer
    Int64,

    // Floating point
    /// 32-bit IEEE 754 floating point
    Float32,

    /// 64-bit IEEE 754 floating point
    Float64,

    // Exact numeric
    /// Exact decimal with specified precision and scale
    Decimal {
        /// Total number of digits
        precision: u8,
        /// Number of digits after the decimal point
        scale: u8,
    },

    // String types
    /// Fixed-length character string
    Char {
        /// Maximum length
        length: u16,
    },

    /// Variable-length character string with max length
    VarChar {
        /// Maximum length
        length: u16,
    },

    /// Unlimited text
    Text,

    // Binary types
    /// Binary large object
    Blob,

    /// Binary data
    Bytes,

    // Temporal types
    /// Date only (YYYY-MM-DD)
    Date,

    /// Time only (HH:MM:SS)
    Time,

    /// Timestamp without timezone (microsecond precision)
    LocalDateTime,

    /// Timestamp with nanosecond precision
    LocalDateTimeNano,

    /// Timestamp with timezone
    ZonedDateTime,

    // Special types
    /// UUID (128-bit)
    Uuid,

    /// ULID (Universally Unique Lexicographically Sortable Identifier)
    Ulid,

    /// JSON document
    Json,

    /// Binary JSON (PostgreSQL JSONB)
    Jsonb,

    // Collection types
    /// Array of a specific type
    Array {
        /// Element type
        element_type: Box<UniversalType>,
    },

    /// MySQL SET type
    Set {
        /// Allowed values
        values: Vec<String>,
    },

    // Enumeration
    /// Enumeration type
    Enum {
        /// Allowed values
        values: Vec<String>,
    },

    // Spatial
    /// Spatial/geometry type
    Geometry {
        /// Specific geometry variant
        geometry_type: GeometryType,
    },

    // Duration
    /// Time duration (seconds + nanoseconds)
    Duration,
}

/// Geometry type variants for spatial data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeometryType {
    /// Point geometry
    Point,
    /// Line string geometry
    LineString,
    /// Polygon geometry
    Polygon,
    /// Multi-point geometry
    MultiPoint,
    /// Multi-line string geometry
    MultiLineString,
    /// Multi-polygon geometry
    MultiPolygon,
    /// Collection of geometries
    GeometryCollection,
}

// Custom serialization/deserialization for UniversalType
// Supports both simple string format ("uuid", "int") and object format ({"type": "var_char", "length": 255})

impl Serialize for UniversalType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            // Simple types - serialize as string
            Self::Bool => serializer.serialize_str("bool"),
            Self::Int16 => serializer.serialize_str("small_int"),
            Self::Int32 => serializer.serialize_str("int"),
            Self::Int64 => serializer.serialize_str("big_int"),
            Self::Float32 => serializer.serialize_str("float"),
            Self::Float64 => serializer.serialize_str("double"),
            Self::Text => serializer.serialize_str("text"),
            Self::Blob => serializer.serialize_str("blob"),
            Self::Bytes => serializer.serialize_str("bytes"),
            Self::Date => serializer.serialize_str("date"),
            Self::Time => serializer.serialize_str("time"),
            Self::LocalDateTime => serializer.serialize_str("date_time"),
            Self::LocalDateTimeNano => serializer.serialize_str("date_time_nano"),
            Self::ZonedDateTime => serializer.serialize_str("timestamp_tz"),
            Self::Uuid => serializer.serialize_str("uuid"),
            Self::Ulid => serializer.serialize_str("ulid"),
            Self::Json => serializer.serialize_str("json"),
            Self::Jsonb => serializer.serialize_str("jsonb"),

            // Complex types - serialize as map
            Self::Int8 { width } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "tiny_int")?;
                map.serialize_entry("width", width)?;
                map.end()
            }
            Self::Decimal { precision, scale } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "decimal")?;
                map.serialize_entry("precision", precision)?;
                map.serialize_entry("scale", scale)?;
                map.end()
            }
            Self::Char { length } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "char")?;
                map.serialize_entry("length", length)?;
                map.end()
            }
            Self::VarChar { length } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "var_char")?;
                map.serialize_entry("length", length)?;
                map.end()
            }
            Self::Array { element_type } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("element_type", element_type)?;
                map.end()
            }
            Self::Set { values } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "set")?;
                map.serialize_entry("values", values)?;
                map.end()
            }
            Self::Enum { values } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("values", values)?;
                map.end()
            }
            Self::Geometry { geometry_type } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "geometry")?;
                map.serialize_entry("geometry_type", geometry_type)?;
                map.end()
            }
            Self::Duration => serializer.serialize_str("duration"),
        }
    }
}

impl<'de> Deserialize<'de> for UniversalType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, MapAccess, Visitor};

        struct UniversalTypeVisitor;

        impl<'de> Visitor<'de> for UniversalTypeVisitor {
            type Value = UniversalType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or map representing a UniversalType")
            }

            // Handle string format: "uuid", "int", etc.
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                match value {
                    "bool" => Ok(UniversalType::Bool),
                    "small_int" | "smallint" => Ok(UniversalType::Int16),
                    "int" => Ok(UniversalType::Int32),
                    "big_int" | "bigint" => Ok(UniversalType::Int64),
                    "float" => Ok(UniversalType::Float32),
                    "double" => Ok(UniversalType::Float64),
                    "text" => Ok(UniversalType::Text),
                    "blob" => Ok(UniversalType::Blob),
                    "bytes" => Ok(UniversalType::Bytes),
                    "date" => Ok(UniversalType::Date),
                    "time" => Ok(UniversalType::Time),
                    "date_time" | "datetime" => Ok(UniversalType::LocalDateTime),
                    "date_time_nano" | "datetime_nano" => Ok(UniversalType::LocalDateTimeNano),
                    "timestamp_tz" | "timestamptz" => Ok(UniversalType::ZonedDateTime),
                    "uuid" => Ok(UniversalType::Uuid),
                    "ulid" => Ok(UniversalType::Ulid),
                    "json" => Ok(UniversalType::Json),
                    "jsonb" => Ok(UniversalType::Jsonb),
                    "duration" => Ok(UniversalType::Duration),
                    _ => Err(E::custom(format!("unknown simple type: {value}"))),
                }
            }

            // Handle map format: {"type": "var_char", "length": 255}
            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut type_name: Option<String> = None;
                let mut fields: HashMap<String, serde_yaml::Value> = HashMap::new();

                while let Some(key) = map.next_key::<String>()? {
                    if key == "type" {
                        type_name = Some(map.next_value()?);
                    } else {
                        fields.insert(key, map.next_value()?);
                    }
                }

                let type_name = type_name.ok_or_else(|| M::Error::missing_field("type"))?;

                match type_name.as_str() {
                    // Simple types that might appear in map format
                    "bool" => Ok(UniversalType::Bool),
                    "small_int" | "smallint" => Ok(UniversalType::Int16),
                    "int" => Ok(UniversalType::Int32),
                    "big_int" | "bigint" => Ok(UniversalType::Int64),
                    "float" => Ok(UniversalType::Float32),
                    "double" => Ok(UniversalType::Float64),
                    "text" => Ok(UniversalType::Text),
                    "blob" => Ok(UniversalType::Blob),
                    "bytes" => Ok(UniversalType::Bytes),
                    "date" => Ok(UniversalType::Date),
                    "time" => Ok(UniversalType::Time),
                    "date_time" | "datetime" => Ok(UniversalType::LocalDateTime),
                    "date_time_nano" | "datetime_nano" => Ok(UniversalType::LocalDateTimeNano),
                    "timestamp_tz" | "timestamptz" => Ok(UniversalType::ZonedDateTime),
                    "uuid" => Ok(UniversalType::Uuid),
                    "ulid" => Ok(UniversalType::Ulid),
                    "json" => Ok(UniversalType::Json),
                    "jsonb" => Ok(UniversalType::Jsonb),
                    "duration" => Ok(UniversalType::Duration),

                    // Complex types
                    "tiny_int" | "tinyint" => {
                        let width = get_field(&fields, "width").unwrap_or(1);
                        Ok(UniversalType::Int8 { width })
                    }
                    "decimal" => {
                        let precision = get_field_required(&fields, "precision")?;
                        let scale = get_field_required(&fields, "scale")?;
                        Ok(UniversalType::Decimal { precision, scale })
                    }
                    "char" => {
                        let length = get_field_required(&fields, "length")?;
                        Ok(UniversalType::Char { length })
                    }
                    "var_char" | "varchar" => {
                        let length = get_field_required(&fields, "length")?;
                        Ok(UniversalType::VarChar { length })
                    }
                    "array" => {
                        let element_type: UniversalType =
                            get_field_required(&fields, "element_type")?;
                        Ok(UniversalType::Array {
                            element_type: Box::new(element_type),
                        })
                    }
                    "set" => {
                        let values = get_field_required(&fields, "values")?;
                        Ok(UniversalType::Set { values })
                    }
                    "enum" => {
                        let values = get_field_required(&fields, "values")?;
                        Ok(UniversalType::Enum { values })
                    }
                    "geometry" => {
                        let geometry_type = get_field_required(&fields, "geometry_type")?;
                        Ok(UniversalType::Geometry { geometry_type })
                    }
                    _ => Err(M::Error::custom(format!("unknown type: {type_name}"))),
                }
            }
        }

        deserializer.deserialize_any(UniversalTypeVisitor)
    }
}

// Helper functions for deserialization
fn get_field<T: for<'de> Deserialize<'de>>(
    fields: &HashMap<String, serde_yaml::Value>,
    key: &str,
) -> Option<T> {
    fields
        .get(key)
        .and_then(|v| serde_yaml::from_value(v.clone()).ok())
}

fn get_field_required<T: for<'de> Deserialize<'de>, E: serde::de::Error>(
    fields: &HashMap<String, serde_yaml::Value>,
    key: &'static str,
) -> Result<T, E> {
    let value = fields.get(key).ok_or_else(|| E::missing_field(key))?;
    serde_yaml::from_value(value.clone())
        .map_err(|e| E::custom(format!("invalid field '{key}': {e}")))
}

/// Trait for generating DDL statements from `UniversalType`.
///
/// Each database-specific type crate implements this trait to generate
/// appropriate DDL for creating tables with the correct column types.
pub trait ToDdl {
    /// Generate DDL type definition for the given `UniversalType`.
    fn to_ddl(&self, sync_type: &UniversalType) -> String;
}

impl UniversalType {
    /// Create a new TinyInt type with the given display width.
    pub fn tiny_int(width: u8) -> Self {
        Self::Int8 { width }
    }

    /// Create a new Decimal type with the given precision and scale.
    pub fn decimal(precision: u8, scale: u8) -> Self {
        Self::Decimal { precision, scale }
    }

    /// Create a new Char type with the given length.
    pub fn char(length: u16) -> Self {
        Self::Char { length }
    }

    /// Create a new VarChar type with the given length.
    pub fn varchar(length: u16) -> Self {
        Self::VarChar { length }
    }

    /// Create a new Array type with the given element type.
    pub fn array(element_type: UniversalType) -> Self {
        Self::Array {
            element_type: Box::new(element_type),
        }
    }

    /// Create a new Set type with the given values.
    pub fn set(values: Vec<String>) -> Self {
        Self::Set { values }
    }

    /// Create a new Enum type with the given values.
    pub fn enumeration(values: Vec<String>) -> Self {
        Self::Enum { values }
    }

    /// Create a new Geometry type with the given geometry variant.
    pub fn geometry(geometry_type: GeometryType) -> Self {
        Self::Geometry { geometry_type }
    }

    /// Check if this type represents a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int8 { .. }
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Float32
                | Self::Float64
                | Self::Decimal { .. }
        )
    }

    /// Check if this type represents a string type.
    pub fn is_string(&self) -> bool {
        matches!(self, Self::Char { .. } | Self::VarChar { .. } | Self::Text)
    }

    /// Check if this type represents a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time
                | Self::LocalDateTime
                | Self::LocalDateTimeNano
                | Self::ZonedDateTime
        )
    }

    /// Check if this type represents a binary type.
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Blob | Self::Bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_data_type_constructors() {
        assert_eq!(UniversalType::tiny_int(1), UniversalType::Int8 { width: 1 });
        assert_eq!(
            UniversalType::decimal(10, 2),
            UniversalType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            UniversalType::varchar(255),
            UniversalType::VarChar { length: 255 }
        );
    }

    #[test]
    fn test_type_categories() {
        assert!(UniversalType::Int32.is_numeric());
        assert!(UniversalType::decimal(10, 2).is_numeric());
        assert!(!UniversalType::Text.is_numeric());

        assert!(UniversalType::Text.is_string());
        assert!(UniversalType::varchar(255).is_string());
        assert!(!UniversalType::Int32.is_string());

        assert!(UniversalType::LocalDateTime.is_temporal());
        assert!(UniversalType::Date.is_temporal());
        assert!(!UniversalType::Int32.is_temporal());

        assert!(UniversalType::Blob.is_binary());
        assert!(UniversalType::Bytes.is_binary());
        assert!(!UniversalType::Text.is_binary());
    }

    #[test]
    fn test_deserialize_simple_string() {
        let yaml = "uuid";
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, UniversalType::Uuid);

        let yaml = "int";
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, UniversalType::Int32);

        let yaml = "text";
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, UniversalType::Text);
    }

    #[test]
    fn test_deserialize_complex_types() {
        let yaml = r#"
type: var_char
length: 255
"#;
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, UniversalType::VarChar { length: 255 });

        let yaml = r#"
type: decimal
precision: 10
scale: 2
"#;
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            parsed,
            UniversalType::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let yaml = r#"
type: tiny_int
width: 1
"#;
        let parsed: UniversalType = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, UniversalType::Int8 { width: 1 });
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let types = vec![
            UniversalType::Bool,
            UniversalType::tiny_int(1),
            UniversalType::Int32,
            UniversalType::decimal(10, 2),
            UniversalType::varchar(255),
            UniversalType::array(UniversalType::Int32),
            UniversalType::enumeration(vec!["a".to_string(), "b".to_string()]),
        ];

        for ty in types {
            let yaml = serde_yaml::to_string(&ty).unwrap();
            let parsed: UniversalType = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(ty, parsed);
        }
    }
}
