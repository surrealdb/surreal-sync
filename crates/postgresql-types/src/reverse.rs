//! Reverse conversion: PostgreSQL value → TypedValue
//!
//! This module implements conversion from PostgreSQL values (as read from the database)
//! back to sync-core's `TypedValue` format.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use postgres_types::Type as PgType;
use rust_decimal::Decimal;
use sync_core::{GeometryType, Type, TypedValue, Value};
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
    pub pg_type: PgType,
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
    pub fn new(pg_type: PgType, value: PostgreSQLRawValue) -> Self {
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
            t if *t == PgType::BOOL => {
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
            t if *t == PgType::INT2 => {
                if let PostgreSQLRawValue::Int16(i) = self.value {
                    Ok(TypedValue::int16(i))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int16".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::INT4 => {
                if let PostgreSQLRawValue::Int32(i) = self.value {
                    Ok(TypedValue::int32(i))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int32".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::INT8 => {
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
            t if *t == PgType::FLOAT4 => {
                if let PostgreSQLRawValue::Float32(f) = self.value {
                    Ok(TypedValue::float32(f))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "float32".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::FLOAT8 => {
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
            t if *t == PgType::NUMERIC => {
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
            t if *t == PgType::TEXT || *t == PgType::VARCHAR || *t == PgType::BPCHAR => {
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
            t if *t == PgType::BYTEA => {
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
            t if *t == PgType::UUID => {
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
            t if *t == PgType::DATE => {
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
            t if *t == PgType::TIME => {
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
            t if *t == PgType::TIMESTAMP => {
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
            t if *t == PgType::TIMESTAMPTZ => {
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
            t if *t == PgType::JSON || *t == PgType::JSONB => {
                if let PostgreSQLRawValue::Json(j) = &self.value {
                    if *t == PgType::JSONB {
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
            t if *t == PgType::TEXT_ARRAY => {
                if let PostgreSQLRawValue::TextArray(arr) = &self.value {
                    let values: Vec<Value> = arr.iter().map(|s| Value::Text(s.clone())).collect();
                    Ok(TypedValue::array(values, Type::Text))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "text[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::INT4_ARRAY => {
                if let PostgreSQLRawValue::Int32Array(arr) = &self.value {
                    let values: Vec<Value> = arr.iter().map(|i| Value::Int32(*i)).collect();
                    Ok(TypedValue::array(values, Type::Int32))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int4[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::INT8_ARRAY => {
                if let PostgreSQLRawValue::Int64Array(arr) = &self.value {
                    let values: Vec<Value> = arr.iter().map(|i| Value::Int64(*i)).collect();
                    Ok(TypedValue::array(values, Type::Int64))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "int8[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::FLOAT8_ARRAY => {
                if let PostgreSQLRawValue::Float64Array(arr) = &self.value {
                    let values: Vec<Value> = arr.iter().map(|f| Value::Float64(*f)).collect();
                    Ok(TypedValue::array(values, Type::Float64))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "float8[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            t if *t == PgType::BOOL_ARRAY => {
                if let PostgreSQLRawValue::BoolArray(arr) = &self.value {
                    let values: Vec<Value> = arr.iter().map(|b| Value::Bool(*b)).collect();
                    Ok(TypedValue::array(values, Type::Bool))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "bool[]".to_string(),
                        actual: format!("{:?}", self.value),
                    })
                }
            }

            // Point
            t if *t == PgType::POINT => {
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

/// Convert PostgreSQL type to Type.
fn pg_type_to_sync_type(pg_type: &PgType) -> Type {
    match pg_type {
        t if *t == PgType::BOOL => Type::Bool,
        t if *t == PgType::INT2 => Type::Int16,
        t if *t == PgType::INT4 => Type::Int32,
        t if *t == PgType::INT8 => Type::Int64,
        t if *t == PgType::FLOAT4 => Type::Float32,
        t if *t == PgType::FLOAT8 => Type::Float64,
        t if *t == PgType::NUMERIC => Type::Decimal {
            precision: 38,
            scale: 10,
        },
        t if *t == PgType::TEXT => Type::Text,
        t if *t == PgType::VARCHAR => Type::VarChar { length: 255 },
        t if *t == PgType::BPCHAR => Type::Char { length: 1 },
        t if *t == PgType::BYTEA => Type::Bytes,
        t if *t == PgType::UUID => Type::Uuid,
        t if *t == PgType::DATE => Type::Date,
        t if *t == PgType::TIME => Type::Time,
        t if *t == PgType::TIMESTAMP => Type::LocalDateTime,
        t if *t == PgType::TIMESTAMPTZ => Type::ZonedDateTime,
        t if *t == PgType::JSON => Type::Json,
        t if *t == PgType::JSONB => Type::Jsonb,
        t if *t == PgType::TEXT_ARRAY => Type::Array {
            element_type: Box::new(Type::Text),
        },
        t if *t == PgType::INT4_ARRAY => Type::Array {
            element_type: Box::new(Type::Int32),
        },
        t if *t == PgType::INT8_ARRAY => Type::Array {
            element_type: Box::new(Type::Int64),
        },
        t if *t == PgType::FLOAT8_ARRAY => Type::Array {
            element_type: Box::new(Type::Float64),
        },
        t if *t == PgType::BOOL_ARRAY => Type::Array {
            element_type: Box::new(Type::Bool),
        },
        t if *t == PgType::POINT => Type::Geometry {
            geometry_type: GeometryType::Point,
        },
        _ => Type::Text,
    }
}

/// Convert serde_json::Value to Value.
#[allow(dead_code)]
fn json_to_generated_value(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s),
        serde_json::Value::Array(arr) => {
            let elements: Vec<Value> = arr.into_iter().map(json_to_generated_value).collect();
            Value::Array {
                elements,
                element_type: Box::new(Type::Json),
            }
        }
        serde_json::Value::Object(obj) => Value::Json(Box::new(serde_json::Value::Object(obj))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_bool_conversion() {
        let pv = PostgreSQLValueWithSchema::new(PgType::BOOL, PostgreSQLRawValue::Bool(true));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Bool));
        assert!(matches!(tv.value, Value::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let pv = PostgreSQLValueWithSchema::new(PgType::INT4, PostgreSQLRawValue::Int32(42));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Int32));
        assert!(matches!(tv.value, Value::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let pv = PostgreSQLValueWithSchema::new(
            PgType::INT8,
            PostgreSQLRawValue::Int64(9_223_372_036_854_775_807),
        );
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Int64));
        assert!(matches!(tv.value, Value::Int64(9_223_372_036_854_775_807)));
    }

    #[test]
    fn test_text_conversion() {
        let pv = PostgreSQLValueWithSchema::new(
            PgType::TEXT,
            PostgreSQLRawValue::Text("hello world".to_string()),
        );
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Text));
        if let Value::Text(s) = tv.value {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected String value");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let pv = PostgreSQLValueWithSchema::new(PgType::UUID, PostgreSQLRawValue::Uuid(uuid));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Uuid));
        if let Value::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid value");
        }
    }

    #[test]
    fn test_timestamp_conversion() {
        let ts = NaiveDateTime::parse_from_str("2024-06-15 10:30:45", "%Y-%m-%d %H:%M:%S").unwrap();
        let pv =
            PostgreSQLValueWithSchema::new(PgType::TIMESTAMP, PostgreSQLRawValue::Timestamp(ts));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::LocalDateTime));
        if let Value::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.year(), 2024);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_null_conversion() {
        let pv = PostgreSQLValueWithSchema::new(PgType::INT4, PostgreSQLRawValue::Null);
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Int32));
        assert!(matches!(tv.value, Value::Null));
    }

    #[test]
    fn test_json_conversion() {
        let json = serde_json::json!({"name": "Alice", "age": 30});
        let pv = PostgreSQLValueWithSchema::new(PgType::JSON, PostgreSQLRawValue::Json(json));
        let tv = pv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, Type::Json));
        if let Value::Json(json_val) = tv.value {
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
            PostgreSQLValueWithSchema::new(PgType::NUMERIC, PostgreSQLRawValue::Decimal(decimal));
        let tv = pv.to_typed_value().unwrap();
        if let Type::Decimal { scale, .. } = tv.sync_type {
            assert_eq!(scale, 3);
        } else {
            panic!("Expected Decimal type");
        }
        if let Value::Decimal { value, .. } = tv.value {
            assert_eq!(value, "123.456");
        } else {
            panic!("Expected Decimal value");
        }
    }

    #[test]
    fn test_text_array_conversion() {
        let arr = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let pv =
            PostgreSQLValueWithSchema::new(PgType::TEXT_ARRAY, PostgreSQLRawValue::TextArray(arr));
        let tv = pv.to_typed_value().unwrap();
        if let Type::Array { element_type } = &tv.sync_type {
            assert!(matches!(**element_type, Type::Text));
        } else {
            panic!("Expected Array type");
        }
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_int_array_conversion() {
        let arr = vec![1, 2, 3];
        let pv =
            PostgreSQLValueWithSchema::new(PgType::INT4_ARRAY, PostgreSQLRawValue::Int32Array(arr));
        let tv = pv.to_typed_value().unwrap();
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], Value::Int32(1)));
        } else {
            panic!("Expected Array value");
        }
    }
}
