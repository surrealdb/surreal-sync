//! Forward conversion: TypedValue → CSV string.
//!
//! This module provides conversion from sync-core's `TypedValue` to CSV string values.

use base64::Engine;
use sync_core::{TypedValue, UniversalValue};

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
        CsvValue::from(tv.value)
    }
}

impl From<UniversalValue> for CsvValue {
    fn from(value: UniversalValue) -> Self {
        match value {
            // Null - empty string
            UniversalValue::Null => CsvValue(String::new()),

            // Boolean
            UniversalValue::Bool(b) => CsvValue(if b {
                "true".to_string()
            } else {
                "false".to_string()
            }),

            // Integer types
            UniversalValue::Int8 { value, .. } => CsvValue(value.to_string()),
            UniversalValue::Int16(i) => CsvValue(i.to_string()),
            UniversalValue::Int32(i) => CsvValue(i.to_string()),
            UniversalValue::Int64(i) => CsvValue(i.to_string()),

            // Floating point
            UniversalValue::Float32(f) => CsvValue(f.to_string()),
            UniversalValue::Float64(f) => CsvValue(f.to_string()),

            // Decimal - preserve as-is (no precision check needed for CSV)
            UniversalValue::Decimal { value, .. } => CsvValue(value),

            // String types
            UniversalValue::Char { value, .. } => CsvValue(value),
            UniversalValue::VarChar { value, .. } => CsvValue(value),
            UniversalValue::Text(s) => CsvValue(s),

            // Binary types - base64 encode
            UniversalValue::Blob(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                CsvValue(encoded)
            }
            UniversalValue::Bytes(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                CsvValue(encoded)
            }

            // Date/time types - ISO 8601 format
            UniversalValue::Date(dt) => CsvValue(dt.format("%Y-%m-%d").to_string()),
            UniversalValue::Time(dt) => CsvValue(dt.format("%H:%M:%S").to_string()),
            UniversalValue::LocalDateTime(dt) => CsvValue(dt.to_rfc3339()),
            UniversalValue::LocalDateTimeNano(dt) => CsvValue(dt.to_rfc3339()),
            UniversalValue::ZonedDateTime(dt) => CsvValue(dt.to_rfc3339()),

            // UUID
            UniversalValue::Uuid(u) => CsvValue(u.to_string()),

            // ULID
            UniversalValue::Ulid(u) => CsvValue(u.to_string()),

            // JSON types - serialize as JSON string
            UniversalValue::Json(payload) => {
                CsvValue(serde_json::to_string(&*payload).unwrap_or_default())
            }
            UniversalValue::Jsonb(payload) => {
                CsvValue(serde_json::to_string(&*payload).unwrap_or_default())
            }

            // Array types - serialize as JSON array
            UniversalValue::Array { elements, .. } => {
                let json_arr: Vec<serde_json::Value> =
                    elements.iter().map(generated_value_to_json).collect();
                CsvValue(serde_json::to_string(&json_arr).unwrap_or_default())
            }

            // Set - comma-separated values
            UniversalValue::Set { elements, .. } => CsvValue(elements.join(",")),

            // Enum - string value
            UniversalValue::Enum { value, .. } => CsvValue(value),

            // Geometry types - serialize as GeoJSON (includes geometry_type from UniversalValue)
            UniversalValue::Geometry { data, .. } => {
                use sync_core::values::GeometryData;
                let GeometryData(value) = data;
                CsvValue(serde_json::to_string(&value).unwrap_or_default())
            }

            // Duration - format as ISO 8601 duration string
            UniversalValue::Duration(d) => {
                let secs = d.as_secs();
                let nanos = d.subsec_nanos();
                if nanos == 0 {
                    CsvValue(format!("PT{secs}S"))
                } else {
                    CsvValue(format!("PT{secs}.{nanos:09}S"))
                }
            }

            // Thing - record reference as "table:id" format
            UniversalValue::Thing { table, id } => {
                let id_str = match id.as_ref() {
                    UniversalValue::Text(s) => s.clone(),
                    UniversalValue::Int32(i) => i.to_string(),
                    UniversalValue::Int64(i) => i.to_string(),
                    UniversalValue::Uuid(u) => u.to_string(),
                    other => panic!(
                        "Unsupported Thing ID type: {other:?}. \
                         Supported types: Text, Int32, Int64, Uuid"
                    ),
                };
                CsvValue(format!("{table}:{id_str}"))
            }
        }
    }
}

/// Convert a UniversalValue to JSON.
fn generated_value_to_json(value: &UniversalValue) -> serde_json::Value {
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::json!(*b),
        UniversalValue::Int8 { value, .. } => serde_json::json!(*value),
        UniversalValue::Int16(i) => serde_json::json!(*i),
        UniversalValue::Int32(i) => serde_json::json!(*i),
        UniversalValue::Int64(i) => serde_json::json!(*i),
        UniversalValue::Float32(f) => serde_json::json!(*f),
        UniversalValue::Float64(f) => serde_json::json!(*f),
        UniversalValue::Char { value, .. } => serde_json::json!(value),
        UniversalValue::VarChar { value, .. } => serde_json::json!(value),
        UniversalValue::Text(s) => serde_json::json!(s),
        UniversalValue::Blob(b) | UniversalValue::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::json!(encoded)
        }
        UniversalValue::Uuid(u) => serde_json::json!(u.to_string()),
        UniversalValue::Ulid(u) => serde_json::json!(u.to_string()),
        UniversalValue::Date(dt) => serde_json::json!(dt.format("%Y-%m-%d").to_string()),
        UniversalValue::Time(dt) => serde_json::json!(dt.format("%H:%M:%S").to_string()),
        UniversalValue::LocalDateTime(dt)
        | UniversalValue::LocalDateTimeNano(dt)
        | UniversalValue::ZonedDateTime(dt) => serde_json::json!(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => serde_json::json!(value),
        UniversalValue::Array { elements, .. } => {
            serde_json::json!(elements
                .iter()
                .map(generated_value_to_json)
                .collect::<Vec<_>>())
        }
        UniversalValue::Set { elements, .. } => serde_json::json!(elements),
        UniversalValue::Enum { value, .. } => serde_json::json!(value),
        UniversalValue::Json(payload) | UniversalValue::Jsonb(payload) => (**payload).clone(),
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(value) = data;
            value.clone()
        }
        UniversalValue::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                serde_json::json!(format!("PT{secs}S"))
            } else {
                serde_json::json!(format!("PT{secs}.{nanos:09}S"))
            }
        }
        UniversalValue::Thing { table, id } => {
            let id_str = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
                UniversalValue::Int32(i) => i.to_string(),
                UniversalValue::Int64(i) => i.to_string(),
                UniversalValue::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            serde_json::json!(format!("{table}:{id_str}"))
        }
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
    use sync_core::UniversalType;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
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
        let tv = TypedValue::int32(12345);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "12345");
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9876543210i64);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "9876543210");
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.23);
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
        let tv = TypedValue::date(dt);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "2024-06-15");
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue::time(dt);
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "14:30:45");
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue::set(
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        let csv_val: CsvValue = tv.into();
        assert_eq!(csv_val.0, "a,b");
    }

    #[test]
    fn test_enum_conversion() {
        let tv =
            TypedValue::enum_type("active", vec!["active".to_string(), "inactive".to_string()]);
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
            TypedValue::int32(30),
            TypedValue::bool(true),
        ];
        let line = typed_values_to_csv_line(fields);
        assert_eq!(line, "Alice,30,true");
    }

    #[test]
    fn test_typed_values_to_csv_line_with_escape() {
        let fields = vec![TypedValue::text("Hello, World"), TypedValue::int32(42)];
        let line = typed_values_to_csv_line(fields);
        assert_eq!(line, "\"Hello, World\",42");
    }

    #[test]
    fn test_typed_values_to_csv_line_ordered() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Bob")),
            ("age".to_string(), TypedValue::int32(25)),
            ("city".to_string(), TypedValue::text("NYC")),
        ];
        let order = ["age", "name", "city"];
        let line = typed_values_to_csv_line_ordered(fields, &order);
        assert_eq!(line, "25,Bob,NYC");
    }
}
