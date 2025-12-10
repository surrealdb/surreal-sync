//! Reverse conversion: PostgreSQL value → TypedValue
//!
//! This module implements conversion from PostgreSQL values (as read from the database)
//! back to sync-core's `TypedValue` format.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_types::Type;
use rust_decimal::Decimal;
use sync_core::{GeometryType, TypedValue, UniversalType, UniversalValue};
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during PostgreSQL to TypedValue conversion.
#[derive(Debug, Error)]
pub enum ConversionError {
    /// The column type is not supported
    #[error("Unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),

    /// A type mismatch occurred
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Invalid UTF-8 in string data
    #[error("Invalid UTF-8: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    /// Invalid UUID format
    #[error("Invalid UUID: {0}")]
    UuidError(#[from] uuid::Error),

    /// Invalid JSON
    #[error("Invalid JSON: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Invalid datetime
    #[error("Invalid datetime")]
    InvalidDateTime,

    /// Invalid decimal
    #[error("Invalid decimal: {0}")]
    DecimalError(String),
}

/// PostgreSQL value with schema information for reverse conversion.
///
/// This struct wraps a PostgreSQL value along with its type information,
/// enabling proper conversion to TypedValue.
#[derive(Debug, Clone)]
pub struct PostgreSQLValueWithSchema {
    /// The PostgreSQL type OID
    pub pg_type: Type,
    /// The raw value (as bytes or parsed)
    pub value: PostgreSQLRawValue,
}

/// Raw value from PostgreSQL.
#[derive(Debug, Clone)]
pub enum PostgreSQLRawValue {
    /// Null value
    Null,
    /// Boolean
    Bool(bool),
    /// 16-bit integer
    Int16(i16),
    /// 32-bit integer
    Int32(i32),
    /// 64-bit integer
    Int64(i64),
    /// 32-bit float
    Float32(f32),
    /// 64-bit float
    Float64(f64),
    /// Decimal
    Decimal(Decimal),
    /// Text/string
    Text(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// UUID
    Uuid(Uuid),
    /// Date
    Date(NaiveDate),
    /// Time
    Time(NaiveTime),
    /// Timestamp (no timezone)
    Timestamp(NaiveDateTime),
    /// Timestamp with timezone
    TimestampTz(DateTime<Utc>),
    /// JSON value
    Json(serde_json::Value),
    /// Array of text
    TextArray(Vec<String>),
    /// Array of i32
    Int32Array(Vec<i32>),
    /// Array of i64
    Int64Array(Vec<i64>),
    /// Array of f64
    Float64Array(Vec<f64>),
    /// Array of bool
    BoolArray(Vec<bool>),
    /// Point (x, y)
    Point(f64, f64),
}

impl PostgreSQLValueWithSchema {
    /// Create a new PostgreSQLValueWithSchema.
    pub fn new(pg_type: Type, value: PostgreSQLRawValue) -> Self {
        Self { pg_type, value }
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(&self) -> Result<TypedValue, ConversionError> {
        // Handle NULL first
        if matches!(self.value, PostgreSQLRawValue::Null) {
            let sync_type = pg_type_to_sync_type(&self.pg_type);
            return Ok(TypedValue::null(sync_type));
        }

        match &self.pg_type {
            // Boolean
            t if *t == Type::BOOL => {
                if let PostgreSQLRawValue::Bool(b) = self.value {
                    Ok(TypedValue::bool(b))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "bool".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Integer types
            t if *t == Type::INT2 => {
                if let PostgreSQLRawValue::Int16(i) = self.value {
                    Ok(TypedValue::int16(i))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int16".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::INT4 => {
                if let PostgreSQLRawValue::Int32(i) = self.value {
                    Ok(TypedValue::int32(i))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int32".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::INT8 => {
                if let PostgreSQLRawValue::Int64(i) = self.value {
                    Ok(TypedValue::int64(i))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int64".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Floating point
            t if *t == Type::FLOAT4 => {
                if let PostgreSQLRawValue::Float32(f) = self.value {
                    Ok(TypedValue::float32(f))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "float32".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::FLOAT8 => {
                if let PostgreSQLRawValue::Float64(f) = self.value {
                    Ok(TypedValue::float64(f))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "float64".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Numeric/Decimal
            t if *t == Type::NUMERIC => {
                if let PostgreSQLRawValue::Decimal(d) = &self.value {
                    // Estimate precision and scale from the decimal
                    let s = d.to_string();
                    let precision = s.replace(['-', '.'], "").len() as u8;
                    let scale = d.scale() as u8;
                    Ok(TypedValue::decimal(s, precision, scale))
                } else if let PostgreSQLRawValue::Text(s) = &self.value {
                    Ok(TypedValue::decimal(s.clone(), 38, 10))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "decimal".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Text types
            t if *t == Type::TEXT || *t == Type::VARCHAR || *t == Type::BPCHAR => {
                if let PostgreSQLRawValue::Text(s) = &self.value {
                    Ok(TypedValue::text(s.clone()))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "text".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Binary
            t if *t == Type::BYTEA => {
                if let PostgreSQLRawValue::Bytes(b) = &self.value {
                    Ok(TypedValue::bytes(b.clone()))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "bytes".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // UUID
            t if *t == Type::UUID => {
                if let PostgreSQLRawValue::Uuid(u) = &self.value {
                    Ok(TypedValue::uuid(*u))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "uuid".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Date
            t if *t == Type::DATE => {
                if let PostgreSQLRawValue::Date(d) = &self.value {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap().and_utc();
                    Ok(TypedValue::date(dt))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "date".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Time
            t if *t == Type::TIME => {
                if let PostgreSQLRawValue::Time(t) = &self.value {
                    let today = chrono::Utc::now().date_naive();
                    let dt = NaiveDateTime::new(today, *t).and_utc();
                    Ok(TypedValue::time(dt))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "time".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Timestamp
            t if *t == Type::TIMESTAMP => {
                if let PostgreSQLRawValue::Timestamp(ts) = &self.value {
                    Ok(TypedValue::datetime(ts.and_utc()))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "timestamp".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Timestamp with timezone
            t if *t == Type::TIMESTAMPTZ => {
                if let PostgreSQLRawValue::TimestampTz(ts) = &self.value {
                    Ok(TypedValue::timestamptz(*ts))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "timestamptz".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // JSON/JSONB
            t if *t == Type::JSON || *t == Type::JSONB => {
                if let PostgreSQLRawValue::Json(j) = &self.value {
                    if *t == Type::JSONB {
                        Ok(TypedValue::jsonb(j.clone()))
                    } else {
                        Ok(TypedValue::json(j.clone()))
                    }
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "json".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Arrays
            t if *t == Type::TEXT_ARRAY => {
                if let PostgreSQLRawValue::TextArray(arr) = &self.value {
                    let values: Vec<UniversalValue> = arr
                        .iter()
                        .map(|s| UniversalValue::Text(s.clone()))
                        .collect();
                    Ok(TypedValue::array(values, UniversalType::Text))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "text[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::INT4_ARRAY => {
                if let PostgreSQLRawValue::Int32Array(arr) = &self.value {
                    let values: Vec<UniversalValue> =
                        arr.iter().map(|i| UniversalValue::Int32(*i)).collect();
                    Ok(TypedValue::array(values, UniversalType::Int32))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int4[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::INT8_ARRAY => {
                if let PostgreSQLRawValue::Int64Array(arr) = &self.value {
                    let values: Vec<UniversalValue> =
                        arr.iter().map(|i| UniversalValue::Int64(*i)).collect();
                    Ok(TypedValue::array(values, UniversalType::Int64))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int8[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::FLOAT8_ARRAY => {
                if let PostgreSQLRawValue::Float64Array(arr) = &self.value {
                    let values: Vec<UniversalValue> =
                        arr.iter().map(|f| UniversalValue::Float64(*f)).collect();
                    Ok(TypedValue::array(values, UniversalType::Float64))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "float8[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == Type::BOOL_ARRAY => {
                if let PostgreSQLRawValue::BoolArray(arr) = &self.value {
                    let values: Vec<UniversalValue> =
                        arr.iter().map(|b| UniversalValue::Bool(*b)).collect();
                    Ok(TypedValue::array(values, UniversalType::Bool))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "bool[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Point
            t if *t == Type::POINT => {
                if let PostgreSQLRawValue::Point(x, y) = &self.value {
                    // Store as GeoJSON Point
                    let geojson = serde_json::json!({
                        "type": "Point",
                        "coordinates": [x, y]
                    });
                    Ok(TypedValue::geometry_geojson(geojson, GeometryType::Point))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "point".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Unknown type - return as text if possible
            other => {
                if let PostgreSQLRawValue::Text(s) = &self.value {
                    Ok(TypedValue::text(s.clone()))
                } else {
                    Err(ConversionError::UnsupportedType(other.to_string()))
                }
            }
        }
    }
}

/// Convert PostgreSQL type to UniversalType.
fn pg_type_to_sync_type(pg_type: &Type) -> UniversalType {
    match pg_type {
        t if *t == Type::BOOL => UniversalType::Bool,
        t if *t == Type::INT2 => UniversalType::Int16,
        t if *t == Type::INT4 => UniversalType::Int32,
        t if *t == Type::INT8 => UniversalType::Int64,
        t if *t == Type::FLOAT4 => UniversalType::Float32,
        t if *t == Type::FLOAT8 => UniversalType::Float64,
        t if *t == Type::NUMERIC => UniversalType::Decimal {
            precision: 38,
            scale: 10,
        },
        t if *t == Type::TEXT => UniversalType::Text,
        t if *t == Type::VARCHAR => UniversalType::VarChar { length: 255 },
        t if *t == Type::BPCHAR => UniversalType::Char { length: 1 },
        t if *t == Type::BYTEA => UniversalType::Bytes,
        t if *t == Type::UUID => UniversalType::Uuid,
        t if *t == Type::DATE => UniversalType::Date,
        t if *t == Type::TIME => UniversalType::Time,
        t if *t == Type::TIMESTAMP => UniversalType::LocalDateTime,
        t if *t == Type::TIMESTAMPTZ => UniversalType::ZonedDateTime,
        t if *t == Type::JSON => UniversalType::Json,
        t if *t == Type::JSONB => UniversalType::Jsonb,
        t if *t == Type::TEXT_ARRAY => UniversalType::Array {
            element_type: Box::new(UniversalType::Text),
        },
        t if *t == Type::INT4_ARRAY => UniversalType::Array {
            element_type: Box::new(UniversalType::Int32),
        },
        t if *t == Type::INT8_ARRAY => UniversalType::Array {
            element_type: Box::new(UniversalType::Int64),
        },
        t if *t == Type::FLOAT8_ARRAY => UniversalType::Array {
            element_type: Box::new(UniversalType::Float64),
        },
        t if *t == Type::BOOL_ARRAY => UniversalType::Array {
            element_type: Box::new(UniversalType::Bool),
        },
        t if *t == Type::POINT => UniversalType::Geometry {
            geometry_type: GeometryType::Point,
        },
        _ => UniversalType::Text,
    }
}

/// Convert serde_json::Value to UniversalValue.
#[allow(dead_code)]
fn json_to_generated_value(json: serde_json::Value) -> UniversalValue {
    match json {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => UniversalValue::Text(s),
        serde_json::Value::Array(arr) => {
            let elements: Vec<UniversalValue> =
                arr.into_iter().map(json_to_generated_value).collect();
            UniversalValue::Array {
                elements,
                element_type: Box::new(UniversalType::Json),
            }
        }
        serde_json::Value::Object(obj) => {
            UniversalValue::Json(Box::new(serde_json::Value::Object(obj)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_bool_conversion() {
        let pv = PostgreSQLValueWithSchema::new(Type::BOOL, PostgreSQLRawValue::Bool(true));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let pv = PostgreSQLValueWithSchema::new(Type::INT4, PostgreSQLRawValue::Int32(42));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Int32));
        assert!(matches!(tv.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let pv = PostgreSQLValueWithSchema::new(
            Type::INT8,
            PostgreSQLRawValue::Int64(9_223_372_036_854_775_807),
        );
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Int64));
        assert!(matches!(
            tv.value,
            UniversalValue::Int64(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_text_conversion() {
        let pv = PostgreSQLValueWithSchema::new(
            Type::TEXT,
            PostgreSQLRawValue::Text("hello world".to_string()),
        );
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Text));
        if let UniversalValue::Text(s) = tv.value {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected String value");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let pv = PostgreSQLValueWithSchema::new(Type::UUID, PostgreSQLRawValue::Uuid(uuid));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Uuid));
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid value");
        }
    }

    #[test]
    fn test_timestamp_conversion() {
        let ts = NaiveDateTime::parse_from_str("2024-06-15 10:30:45", "%Y-%m-%d %H:%M:%S").unwrap();
        let pv = PostgreSQLValueWithSchema::new(Type::TIMESTAMP, PostgreSQLRawValue::Timestamp(ts));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::LocalDateTime));
        if let UniversalValue::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.year(), 2024);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_null_conversion() {
        let pv = PostgreSQLValueWithSchema::new(Type::INT4, PostgreSQLRawValue::Null);
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Int32));
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_json_conversion() {
        let json = serde_json::json!({"name": "Alice", "age": 30});
        let pv = PostgreSQLValueWithSchema::new(Type::JSON, PostgreSQLRawValue::Json(json));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Json));
        if let UniversalValue::Json(json_val) = tv.value {
            if let serde_json::Value::Object(obj) = json_val.as_ref() {
                assert!(obj.contains_key("name"));
                assert!(obj.contains_key("age"));
            } else {
                panic!("Expected JSON Object");
            }
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let decimal = Decimal::from_str_exact("123.456").unwrap();
        let pv =
            PostgreSQLValueWithSchema::new(Type::NUMERIC, PostgreSQLRawValue::Decimal(decimal));
        let tv = pv.to_typed_value().unwrap();
        if let UniversalType::Decimal { scale, .. } = tv.sync_type {
            assert_eq!(scale, 3);
        } else {
            panic!("Expected Decimal type");
        }
        if let UniversalValue::Decimal { value, .. } = tv.value {
            assert_eq!(value, "123.456");
        } else {
            panic!("Expected Decimal value");
        }
    }

    #[test]
    fn test_text_array_conversion() {
        let arr = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let pv =
            PostgreSQLValueWithSchema::new(Type::TEXT_ARRAY, PostgreSQLRawValue::TextArray(arr));
        let tv = pv.to_typed_value().unwrap();
        if let UniversalType::Array { element_type } = &tv.sync_type {
            assert!(matches!(**element_type, UniversalType::Text));
        } else {
            panic!("Expected Array type");
        }
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_int_array_conversion() {
        let arr = vec![1, 2, 3];
        let pv =
            PostgreSQLValueWithSchema::new(Type::INT4_ARRAY, PostgreSQLRawValue::Int32Array(arr));
        let tv = pv.to_typed_value().unwrap();
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], UniversalValue::Int32(1)));
        } else {
            panic!("Expected Array value");
        }
    }
}
