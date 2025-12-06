//! Forward conversion: TypedValue → CSV string.
//!
//! This module provides conversion from sync-core's `TypedValue` to CSV string values.

use base64::Engine;
use sync_core::{GeneratedValue, SyncDataType, TypedValue};

/// Wrapper for CSV string values.
#[derive(Debug, Clone)]
pub struct CsvValue(pub String);

impl CsvValue {
    /// Get the inner CSV string.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Get a reference to the inner CSV string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<TypedValue> for CsvValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, &tv.value) {
            // Null - empty string
            (_, GeneratedValue::Null) => CsvValue(String::new()),

            // Boolean
            (SyncDataType::Bool, GeneratedValue::Bool(b)) => CsvValue(if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }),

            // Integer types - handle both Int32 and Int64 since generators may produce either
            (SyncDataType::TinyInt { .. }, GeneratedValue::Int32(i)) => CsvValue(i.to_string()),
            (SyncDataType::TinyInt { .. }, GeneratedValue::Int64(i)) => CsvValue(i.to_string()),
            (SyncDataType::SmallInt, GeneratedValue::Int32(i)) => CsvValue(i.to_string()),
            (SyncDataType::SmallInt, GeneratedValue::Int64(i)) => CsvValue(i.to_string()),
            (SyncDataType::Int, GeneratedValue::Int32(i)) => CsvValue(i.to_string()),
            (SyncDataType::Int, GeneratedValue::Int64(i)) => CsvValue(i.to_string()),
            (SyncDataType::BigInt, GeneratedValue::Int64(i)) => CsvValue(i.to_string()),
            (SyncDataType::BigInt, GeneratedValue::Int32(i)) => CsvValue(i.to_string()),

            // Floating point
            (SyncDataType::Float, GeneratedValue::Float64(f)) => CsvValue(f.to_string()),
            (SyncDataType::Double, GeneratedValue::Float64(f)) => CsvValue(f.to_string()),

            // Decimal - preserve as-is
            (SyncDataType::Decimal { .. }, GeneratedValue::Decimal { value, .. }) => {
                CsvValue(value.clone())
            }

            // String types - may need escaping for CSV
            (SyncDataType::Char { .. }, GeneratedValue::String(s)) => CsvValue(s.clone()),
            (SyncDataType::VarChar { .. }, GeneratedValue::String(s)) => CsvValue(s.clone()),
            (SyncDataType::Text, GeneratedValue::String(s)) => CsvValue(s.clone()),

            // Binary types - base64 encode
            (SyncDataType::Blob, GeneratedValue::Bytes(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                CsvValue(encoded)
            }
            (SyncDataType::Bytes, GeneratedValue::Bytes(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                CsvValue(encoded)
            }

            // Date/time types - ISO 8601 format
            (SyncDataType::Date, GeneratedValue::DateTime(dt)) => {
                CsvValue(dt.format("%Y-%m-%d").to_string())
            }
            (SyncDataType::Time, GeneratedValue::DateTime(dt)) => {
                CsvValue(dt.format("%H:%M:%S").to_string())
            }
            (SyncDataType::DateTime, GeneratedValue::DateTime(dt)) => CsvValue(dt.to_rfc3339()),
            (SyncDataType::DateTimeNano, GeneratedValue::DateTime(dt)) => CsvValue(dt.to_rfc3339()),
            (SyncDataType::TimestampTz, GeneratedValue::DateTime(dt)) => CsvValue(dt.to_rfc3339()),

            // UUID
            (SyncDataType::Uuid, GeneratedValue::Uuid(u)) => CsvValue(u.to_string()),

            // JSON types - serialize as JSON string
            (SyncDataType::Json, GeneratedValue::Object(map)) => {
                let json = hashmap_to_json(map);
                CsvValue(serde_json::to_string(&json).unwrap_or_default())
            }
            (SyncDataType::Jsonb, GeneratedValue::Object(map)) => {
                let json = hashmap_to_json(map);
                CsvValue(serde_json::to_string(&json).unwrap_or_default())
            }
            (SyncDataType::Json, GeneratedValue::String(s)) => CsvValue(s.clone()),
            (SyncDataType::Jsonb, GeneratedValue::String(s)) => CsvValue(s.clone()),

            // Array types - serialize as JSON array
            (SyncDataType::Array { .. }, GeneratedValue::Array(arr)) => {
                let json_arr: Vec<serde_json::Value> =
                    arr.iter().map(generated_value_to_json).collect();
                CsvValue(serde_json::to_string(&json_arr).unwrap_or_default())
            }

            // Set - comma-separated values
            (SyncDataType::Set { .. }, GeneratedValue::Array(arr)) => {
                let values: Vec<String> = arr
                    .iter()
                    .filter_map(|v| match v {
                        GeneratedValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                CsvValue(values.join(","))
            }

            // Enum - string value
            (SyncDataType::Enum { .. }, GeneratedValue::String(s)) => CsvValue(s.clone()),

            // Geometry types - serialize as GeoJSON
            (SyncDataType::Geometry { .. }, GeneratedValue::Object(map)) => {
                let json = hashmap_to_json(map);
                CsvValue(serde_json::to_string(&json).unwrap_or_default())
            }

            // Fallback - panic instead of silently returning empty string
            (sync_type, value) => panic!(
                "Unsupported type/value combination for CSV conversion: sync_type={sync_type:?}, value={value:?}"
            ),
        }
    }
}

/// Convert a HashMap of GeneratedValue to JSON.
fn hashmap_to_json(map: &std::collections::HashMap<String, GeneratedValue>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (key, value) in map {
        obj.insert(key.clone(), generated_value_to_json(value));
    }
    serde_json::Value::Object(obj)
}

/// Convert a GeneratedValue to JSON.
fn generated_value_to_json(value: &GeneratedValue) -> serde_json::Value {
    match value {
        GeneratedValue::Null => serde_json::Value::Null,
        GeneratedValue::Bool(b) => serde_json::json!(*b),
        GeneratedValue::Int32(i) => serde_json::json!(*i),
        GeneratedValue::Int64(i) => serde_json::json!(*i),
        GeneratedValue::Float64(f) => serde_json::json!(*f),
        GeneratedValue::String(s) => serde_json::json!(s),
        GeneratedValue::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::json!(encoded)
        }
        GeneratedValue::Uuid(u) => serde_json::json!(u.to_string()),
        GeneratedValue::DateTime(dt) => serde_json::json!(dt.to_rfc3339()),
        GeneratedValue::Decimal { value, .. } => serde_json::json!(value),
        GeneratedValue::Array(arr) => {
            serde_json::json!(arr.iter().map(generated_value_to_json).collect::<Vec<_>>())
        }
        GeneratedValue::Object(map) => hashmap_to_json(map),
    }
}

/// Escape a value for CSV (double quotes and add quotes if needed).
pub fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Convert a row of TypedValues to a CSV line.
pub fn typed_values_to_csv_line<I>(fields: I) -> String
where
    I: IntoIterator<Item = TypedValue>,
{
    let values: Vec<String> = fields
        .into_iter()
        .map(|tv| {
            let csv_val = CsvValue::from(tv);
            escape_csv(&csv_val.0)
        })
        .collect();
    values.join(",")
}

/// Convert a row of named TypedValues to a CSV line (in field order).
pub fn typed_values_to_csv_line_ordered<I>(fields: I, column_order: &[&str]) -> String
where
    I: IntoIterator<Item = (String, TypedValue)>,
{
    let map: std::collections::HashMap<String, TypedValue> = fields.into_iter().collect();
    let values: Vec<String> = column_order
        .iter()
        .map(|col| {
            let csv_val = map
                .get(*col)
                .map(|tv| CsvValue::from(tv.clone()))
                .unwrap_or_else(|| CsvValue(String::new()));
            escape_csv(&csv_val.0)
        })
        .collect();
    values.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(SyncDataType::Text);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "");
    }

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "true");

        let tv = TypedValue::bool(false);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "false");
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(12345);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "12345");
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9876543210i64);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "9876543210");
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.23);
        let csv_val: CsvValue = tv.into();
        assert!(csv_val.0.starts_with("1.23"));
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.45", 10, 2);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "123.45");
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("hello world");
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "hello world");
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let csv_val: CsvValue = tv.into();
        let expected =
            base64::engine::general_purpose::STANDARD.encode(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(csv_val.0, expected);
    }

    #[test]
    fn test_uuid_conversion() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(u);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let tv = TypedValue::datetime(dt);
        let csv_val: CsvValue = tv.into();
        assert!(csv_val.0.starts_with("2024-06-15T10:30:00"));
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue {
            sync_type: SyncDataType::Date,
            value: GeneratedValue::DateTime(dt),
        };
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "2024-06-15");
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue {
            sync_type: SyncDataType::Time,
            value: GeneratedValue::DateTime(dt),
        };
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "14:30:45");
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            value: GeneratedValue::Array(vec![
                GeneratedValue::String("a".to_string()),
                GeneratedValue::String("b".to_string()),
            ]),
        };
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "a,b");
    }

    #[test]
    fn test_enum_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            value: GeneratedValue::String("active".to_string()),
        };
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "active");
    }

    #[test]
    fn test_escape_csv_no_escape() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv("hello, world"), "\"hello, world\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn test_typed_values_to_csv_line() {
        let fields = vec![
            TypedValue::text("Alice"),
            TypedValue::int(30),
            TypedValue::bool(true),
        ];
        let line = typed_values_to_csv_line(fields);
        assert_eq!(line, "Alice,30,true");
    }

    #[test]
    fn test_typed_values_to_csv_line_with_escape() {
        let fields = vec![TypedValue::text("Hello, World"), TypedValue::int(42)];
        let line = typed_values_to_csv_line(fields);
        assert_eq!(line, "\"Hello, World\",42");
    }

    #[test]
    fn test_typed_values_to_csv_line_ordered() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Bob")),
            ("age".to_string(), TypedValue::int(25)),
            ("city".to_string(), TypedValue::text("NYC")),
        ];
        let order = ["age", "name", "city"];
        let line = typed_values_to_csv_line_ordered(fields, &order);
        assert_eq!(line, "25,Bob,NYC");
    }
}
