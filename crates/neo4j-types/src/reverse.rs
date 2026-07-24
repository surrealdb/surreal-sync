//! Reverse conversion: Neo4j BoltType → TypedValue/Value
//!
//! This module implements conversion from Neo4j's BoltType to sync-core's TypedValue
//! and Value types. This is used when reading data from Neo4j.
//!
//! ## Timezone Handling
//!
//! Neo4j's `Date` and `LocalDateTime` types are timezone-naive. To convert them to
//! UTC-based DateTime values, a timezone string must be provided (e.g., "America/New_York").
//!
//! ## Error Handling
//!
//! This module returns `Result` types for all conversions. Unexpected cases (like
//! Node, Relation, Path types) return errors instead of silently falling back to
//! default values.

use crate::error::{Neo4jTypesError, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use neo4rs::BoltType;
use std::str::FromStr;
use sync_core::types::GeometryType;
use sync_core::{Type, TypedValue, Value};

/// Configuration options for reverse conversion.
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    /// Timezone for interpreting timezone-naive types (Date, LocalDateTime).
    /// Must be an IANA timezone name like "America/New_York", "UTC", "Europe/London".
    pub timezone: String,

    /// Whether to attempt JSON parsing of string values at the top level.
    /// Nested strings in arrays/maps are never parsed as JSON.
    pub parse_json_strings: bool,
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            timezone: "UTC".to_string(),
            parse_json_strings: false,
        }
    }
}

impl ConversionConfig {
    /// Create a new configuration with the specified timezone.
    pub fn with_timezone(timezone: impl Into<String>) -> Self {
        Self {
            timezone: timezone.into(),
            ..Default::default()
        }
    }

    /// Enable JSON string parsing.
    pub fn with_json_parsing(mut self) -> Self {
        self.parse_json_strings = true;
        self
    }

    /// Parse the configured timezone into a chrono-tz Tz.
    fn parse_timezone(&self) -> Result<Tz> {
        Tz::from_str(&self.timezone)
            .map_err(|_| Neo4jTypesError::InvalidTimezone(self.timezone.clone()))
    }
}

/// Convert a Neo4j BoltType to a TypedValue.
///
/// This function handles all Neo4j property types and converts them to
/// appropriate sync-core TypedValue representations.
///
/// # Arguments
///
/// * `bolt` - The Neo4j BoltType value to convert
/// * `config` - Configuration options for the conversion
///
/// # Errors
///
/// Returns an error for:
/// - Node, Relation, UnboundedRelation, Path types (not property values)
/// - Invalid timezone configuration
/// - Ambiguous datetime values during DST transitions
/// - NaN or Infinity float values
pub fn convert_bolt_to_typed_value(
    bolt: BoltType,
    config: &ConversionConfig,
) -> Result<TypedValue> {
    let (value, sync_type) = convert_bolt_to_universal_value_with_type(bolt, config, true)?;
    Ok(TypedValue { value, sync_type })
}

/// Convert a Neo4j BoltType to an Value.
///
/// This is a convenience wrapper that discards type information.
pub fn convert_bolt_to_universal_value(bolt: BoltType, config: &ConversionConfig) -> Result<Value> {
    let (value, _) = convert_bolt_to_universal_value_with_type(bolt, config, true)?;
    Ok(value)
}

/// Internal conversion function that returns both value and type.
///
/// The `is_top_level` parameter controls whether JSON parsing is attempted
/// on string values. Only top-level strings may be parsed as JSON.
fn convert_bolt_to_universal_value_with_type(
    bolt: BoltType,
    config: &ConversionConfig,
    is_top_level: bool,
) -> Result<(Value, Type)> {
    match bolt {
        BoltType::Null(_) => Ok((Value::Null, Type::Text)),

        BoltType::Boolean(b) => Ok((Value::Bool(b.value), Type::Bool)),

        BoltType::Integer(i) => {
            // Neo4j integers are i64
            Ok((Value::Int64(i.value), Type::Int64))
        }

        BoltType::Float(f) => {
            let f_val = f.value;
            // Check for special float values
            if f_val.is_nan() {
                return Err(Neo4jTypesError::NanFloat);
            }
            if f_val.is_infinite() {
                return Err(Neo4jTypesError::InfinityFloat);
            }
            Ok((Value::Float64(f_val), Type::Float64))
        }

        BoltType::String(s) => {
            let s_val = s.value.clone();
            // Optionally try to parse as JSON at top level
            if is_top_level && config.parse_json_strings {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&s_val) {
                    // Only convert if it's actually JSON (object or array)
                    if json_val.is_object() || json_val.is_array() {
                        return Ok((Value::Json(Box::new(json_val)), Type::Json));
                    }
                }
            }
            Ok((Value::Text(s_val), Type::Text))
        }

        BoltType::Bytes(b) => {
            let bytes = b.value.to_vec();
            Ok((Value::Bytes(bytes), Type::Bytes))
        }

        BoltType::List(list) => {
            let elements: Result<Vec<Value>> = list
                .value
                .into_iter()
                .map(|elem| {
                    let (val, _) = convert_bolt_to_universal_value_with_type(elem, config, false)?;
                    Ok(val)
                })
                .collect();
            let elements = elements?;

            // Determine element type from first element, default to Text
            let element_type = if elements.is_empty() {
                Type::Text
            } else {
                infer_element_type(&elements[0])
            };

            Ok((
                Value::Array {
                    elements,
                    element_type: Box::new(element_type.clone()),
                },
                Type::Array {
                    element_type: Box::new(element_type),
                },
            ))
        }

        BoltType::Map(map) => {
            // Convert to JSON object
            let mut json_obj = serde_json::Map::new();
            for (key, value) in map.value.into_iter() {
                let (uval, _) = convert_bolt_to_universal_value_with_type(value, config, false)?;
                json_obj.insert(key.to_string(), universal_value_to_json(&uval));
            }
            Ok((
                Value::Json(Box::new(serde_json::Value::Object(json_obj))),
                Type::Json,
            ))
        }

        BoltType::Date(date) => {
            // Neo4j Date converts to NaiveDate via TryInto
            let naive_date: NaiveDate =
                date.try_into().map_err(|e| Neo4jTypesError::InvalidDate {
                    reason: format!("Failed to convert BoltDate: {e}"),
                })?;

            // Convert to DateTime at midnight in the configured timezone
            let tz = config.parse_timezone()?;
            let naive_datetime =
                NaiveDateTime::new(naive_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());

            let datetime = tz
                .from_local_datetime(&naive_datetime)
                .single()
                .ok_or_else(|| Neo4jTypesError::AmbiguousDateTime {
                    timezone: config.timezone.clone(),
                    datetime: naive_datetime.to_string(),
                })?;

            Ok((
                Value::LocalDateTime(datetime.with_timezone(&Utc)),
                Type::LocalDateTime,
            ))
        }

        BoltType::Time(time) => {
            // Neo4j Time has nanoseconds since midnight and offset in seconds
            // Converts to (NaiveTime, FixedOffset) via Into
            let (naive_time, offset): (NaiveTime, FixedOffset) = time.into();

            // Create a structured object for Time (no direct TypedValue equivalent)
            use chrono::Timelike;
            let json_obj = serde_json::json!({
                "type": "$Neo4jTime",
                "hour": naive_time.hour(),
                "minute": naive_time.minute(),
                "second": naive_time.second(),
                "nanosecond": naive_time.nanosecond(),
                "offset_seconds": offset.local_minus_utc()
            });

            Ok((Value::Json(Box::new(json_obj)), Type::Json))
        }

        BoltType::LocalTime(local_time) => {
            // Neo4j LocalTime converts to NaiveTime via Into
            let naive_time: NaiveTime = local_time.into();

            use chrono::Timelike;
            let json_obj = serde_json::json!({
                "type": "$Neo4jLocalTime",
                "hour": naive_time.hour(),
                "minute": naive_time.minute(),
                "second": naive_time.second(),
                "nanosecond": naive_time.nanosecond()
            });

            Ok((Value::Json(Box::new(json_obj)), Type::Json))
        }

        BoltType::DateTime(dt) => {
            // Neo4j DateTime with fixed offset - converts to DateTime<FixedOffset>
            let dt_with_offset: DateTime<FixedOffset> =
                dt.try_into()
                    .map_err(|e| Neo4jTypesError::InvalidDateTime {
                        reason: format!("Failed to convert BoltDateTime: {e}"),
                    })?;

            Ok((
                Value::ZonedDateTime(dt_with_offset.with_timezone(&Utc)),
                Type::ZonedDateTime,
            ))
        }

        BoltType::LocalDateTime(local_dt) => {
            // Neo4j LocalDateTime is timezone-naive - converts to NaiveDateTime
            let naive_dt: NaiveDateTime =
                local_dt
                    .try_into()
                    .map_err(|e| Neo4jTypesError::InvalidDateTime {
                        reason: format!("Failed to convert BoltLocalDateTime: {e}"),
                    })?;

            // Interpret as local time in configured timezone
            let tz = config.parse_timezone()?;
            let datetime = tz.from_local_datetime(&naive_dt).single().ok_or_else(|| {
                Neo4jTypesError::AmbiguousDateTime {
                    timezone: config.timezone.clone(),
                    datetime: naive_dt.to_string(),
                }
            })?;

            Ok((
                Value::LocalDateTime(datetime.with_timezone(&Utc)),
                Type::LocalDateTime,
            ))
        }

        BoltType::DateTimeZoneId(dt_zone) => {
            // Neo4j DateTime with named timezone (IANA zone ID)
            // Converts to DateTime<FixedOffset> via TryInto (from reference)
            let dt_with_offset: DateTime<FixedOffset> =
                (&dt_zone)
                    .try_into()
                    .map_err(|e| Neo4jTypesError::InvalidDateTime {
                        reason: format!("Failed to convert BoltDateTimeZoneId: {e}"),
                    })?;

            Ok((
                Value::ZonedDateTime(dt_with_offset.with_timezone(&Utc)),
                Type::ZonedDateTime,
            ))
        }

        BoltType::Duration(duration) => {
            // Neo4j Duration converts to std::time::Duration via Into
            let std_duration: std::time::Duration = duration.into();

            Ok((Value::Duration(std_duration), Type::Duration))
        }

        BoltType::Point2D(point) => {
            // Convert to GeoJSON Point
            let geojson = serde_json::json!({
                "type": "Point",
                "coordinates": [point.x.value, point.y.value],
                "srid": point.sr_id.value
            });

            Ok((
                Value::Geometry {
                    geometry_type: GeometryType::Point,
                    data: sync_core::values::GeometryData(geojson),
                },
                Type::Geometry {
                    geometry_type: GeometryType::Point,
                },
            ))
        }

        BoltType::Point3D(point) => {
            // Convert to GeoJSON Point with elevation
            let geojson = serde_json::json!({
                "type": "Point",
                "coordinates": [point.x.value, point.y.value, point.z.value],
                "srid": point.sr_id.value
            });

            Ok((
                Value::Geometry {
                    geometry_type: GeometryType::Point,
                    data: sync_core::values::GeometryData(geojson),
                },
                Type::Geometry {
                    geometry_type: GeometryType::Point,
                },
            ))
        }

        // These types cannot be converted to property values
        BoltType::Node(_) => Err(Neo4jTypesError::UnsupportedBoltType {
            bolt_type: "Node".to_string(),
        }),

        BoltType::Relation(_) => Err(Neo4jTypesError::UnsupportedBoltType {
            bolt_type: "Relation".to_string(),
        }),

        BoltType::UnboundedRelation(_) => Err(Neo4jTypesError::UnsupportedBoltType {
            bolt_type: "UnboundedRelation".to_string(),
        }),

        BoltType::Path(_) => Err(Neo4jTypesError::UnsupportedBoltType {
            bolt_type: "Path".to_string(),
        }),
    }
}

/// Infer the Type from an Value.
fn infer_element_type(value: &Value) -> Type {
    match value {
        Value::Null => Type::Text,
        Value::Bool(_) => Type::Bool,
        Value::Int8 { width, .. } => Type::Int8 { width: *width },
        Value::Int16(_) => Type::Int16,
        Value::Int32(_) => Type::Int32,
        Value::Int64(_) => Type::Int64,
        Value::Float32(_) => Type::Float32,
        Value::Float64(_) => Type::Float64,
        Value::Decimal {
            precision, scale, ..
        } => Type::Decimal {
            precision: *precision,
            scale: *scale,
        },
        Value::Char { length, .. } => Type::Char { length: *length },
        Value::VarChar { length, .. } => Type::VarChar { length: *length },
        Value::Text(_) => Type::Text,
        Value::Blob(_) => Type::Blob,
        Value::Bytes(_) => Type::Bytes,
        Value::Uuid(_) => Type::Uuid,
        Value::Ulid(_) => Type::Ulid,
        Value::Date(_) => Type::Date,
        Value::Time(_) => Type::Time,
        Value::LocalDateTime(_) => Type::LocalDateTime,
        Value::LocalDateTimeNano(_) => Type::LocalDateTimeNano,
        Value::ZonedDateTime(_) => Type::ZonedDateTime,
        Value::Json(_) => Type::Json,
        Value::Jsonb(_) => Type::Jsonb,
        Value::Array { element_type, .. } => Type::Array {
            element_type: element_type.clone(),
        },
        Value::Set { allowed_values, .. } => Type::Set {
            values: allowed_values.clone(),
        },
        Value::Enum { allowed_values, .. } => Type::Enum {
            values: allowed_values.clone(),
        },
        Value::Geometry { geometry_type, .. } => Type::Geometry {
            geometry_type: geometry_type.clone(),
        },
        Value::Duration(_) => Type::Duration,
        Value::Thing { .. } => Type::Thing,
        Value::Object(_) => Type::Object,
        Value::TimeTz(_) => Type::TimeTz,
        Value::ZeroTemporal { intended_type, .. } => intended_type.clone(),
    }
}

/// Convert an Value to serde_json::Value.
fn universal_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int8 { value, .. } => serde_json::json!(*value),
        Value::Int16(i) => serde_json::json!(*i),
        Value::Int32(i) => serde_json::json!(*i),
        Value::Int64(i) => serde_json::json!(*i),
        Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Decimal { value, .. } => serde_json::Value::String(value.clone()),
        Value::Char { value, .. } => serde_json::Value::String(value.clone()),
        Value::VarChar { value, .. } => serde_json::Value::String(value.clone()),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Blob(b) | Value::Bytes(b) => serde_json::Value::String(hex::encode(b)),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Ulid(u) => serde_json::Value::String(u.to_string()),
        Value::Date(dt)
        | Value::Time(dt)
        | Value::LocalDateTime(dt)
        | Value::LocalDateTimeNano(dt)
        | Value::ZonedDateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Json(j) | Value::Jsonb(j) => (**j).clone(),
        Value::Array { elements, .. } => {
            serde_json::Value::Array(elements.iter().map(universal_value_to_json).collect())
        }
        Value::Set { elements, .. } => serde_json::Value::Array(
            elements
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
        Value::Enum { value, .. } => serde_json::Value::String(value.clone()),
        Value::Geometry { data, .. } => {
            let sync_core::values::GeometryData(json) = data;
            json.clone()
        }
        Value::Duration(d) => serde_json::json!({
            "secs": d.as_secs(),
            "nanos": d.subsec_nanos()
        }),
        Value::Thing { table, id } => {
            let id_str = match id.as_ref() {
                Value::Text(s) => s.clone(),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for Neo4j: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            serde_json::json!(format!("{table}:{id_str}"))
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        // TimeTz - stored as string to preserve timezone format
        Value::TimeTz(s) => serde_json::Value::String(s.clone()),
        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s = source
                .as_deref()
                .unwrap_or_else(|| Value::canonical_zero_literal(intended_type));
            serde_json::Value::String(s.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4rs::{BoltBoolean, BoltBytes, BoltFloat, BoltInteger, BoltNull, BoltString};

    #[test]
    fn test_null_conversion() {
        let config = ConversionConfig::default();
        let result = convert_bolt_to_typed_value(BoltType::Null(BoltNull), &config).unwrap();
        assert!(matches!(result.value, Value::Null));
    }

    #[test]
    fn test_boolean_conversion() {
        let config = ConversionConfig::default();

        let bolt_true = BoltType::Boolean(BoltBoolean::new(true));
        let result = convert_bolt_to_typed_value(bolt_true, &config).unwrap();
        assert!(matches!(result.value, Value::Bool(true)));

        let bolt_false = BoltType::Boolean(BoltBoolean::new(false));
        let result = convert_bolt_to_typed_value(bolt_false, &config).unwrap();
        assert!(matches!(result.value, Value::Bool(false)));
    }

    #[test]
    fn test_integer_conversion() {
        let config = ConversionConfig::default();

        let bolt_int = BoltType::Integer(BoltInteger::new(42));
        let result = convert_bolt_to_typed_value(bolt_int, &config).unwrap();
        assert!(matches!(result.value, Value::Int64(42)));

        let bolt_big = BoltType::Integer(BoltInteger::new(i64::MAX));
        let result = convert_bolt_to_typed_value(bolt_big, &config).unwrap();
        assert!(matches!(result.value, Value::Int64(i64::MAX)));

        let bolt_neg = BoltType::Integer(BoltInteger::new(-100));
        let result = convert_bolt_to_typed_value(bolt_neg, &config).unwrap();
        assert!(matches!(result.value, Value::Int64(-100)));
    }

    #[test]
    fn test_float_conversion() {
        let config = ConversionConfig::default();

        let bolt_float = BoltType::Float(BoltFloat::new(1.23456789));
        let result = convert_bolt_to_typed_value(bolt_float, &config).unwrap();
        if let Value::Float64(f) = result.value {
            assert!((f - 1.23456789).abs() < 0.0001);
        } else {
            panic!("Expected Double value");
        }
    }

    #[test]
    fn test_float_nan_error() {
        let config = ConversionConfig::default();

        let bolt_nan = BoltType::Float(BoltFloat::new(f64::NAN));
        let result = convert_bolt_to_typed_value(bolt_nan, &config);
        assert!(matches!(result, Err(Neo4jTypesError::NanFloat)));
    }

    #[test]
    fn test_float_infinity_error() {
        let config = ConversionConfig::default();

        let bolt_inf = BoltType::Float(BoltFloat::new(f64::INFINITY));
        let result = convert_bolt_to_typed_value(bolt_inf, &config);
        assert!(matches!(result, Err(Neo4jTypesError::InfinityFloat)));

        let bolt_neg_inf = BoltType::Float(BoltFloat::new(f64::NEG_INFINITY));
        let result = convert_bolt_to_typed_value(bolt_neg_inf, &config);
        assert!(matches!(result, Err(Neo4jTypesError::InfinityFloat)));
    }

    #[test]
    fn test_string_conversion() {
        let config = ConversionConfig::default();

        let bolt_str = BoltType::String(BoltString::new("hello world"));
        let result = convert_bolt_to_typed_value(bolt_str, &config).unwrap();
        if let Value::Text(s) = result.value {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_string_json_parsing_disabled() {
        let config = ConversionConfig::default(); // JSON parsing disabled by default

        let bolt_str = BoltType::String(BoltString::new("{\"key\": \"value\"}"));
        let result = convert_bolt_to_typed_value(bolt_str, &config).unwrap();
        // Should remain as text, not parsed as JSON
        if let Value::Text(s) = result.value {
            assert_eq!(s, "{\"key\": \"value\"}");
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_string_json_parsing_enabled() {
        let config = ConversionConfig::default().with_json_parsing();

        let bolt_str = BoltType::String(BoltString::new("{\"key\": \"value\"}"));
        let result = convert_bolt_to_typed_value(bolt_str, &config).unwrap();
        // Should be parsed as JSON
        if let Value::Json(json) = result.value {
            assert_eq!(json["key"], "value");
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_bytes_conversion() {
        let config = ConversionConfig::default();

        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let bolt_bytes = BoltType::Bytes(BoltBytes::new(bytes.clone().into()));
        let result = convert_bolt_to_typed_value(bolt_bytes, &config).unwrap();
        if let Value::Bytes(b) = result.value {
            assert_eq!(b, bytes);
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_invalid_timezone_error() {
        let config = ConversionConfig::with_timezone("Invalid/Timezone");

        // Try to use the timezone - Date requires timezone interpretation
        // We can test by trying to parse the timezone directly
        let result = config.parse_timezone();
        assert!(matches!(result, Err(Neo4jTypesError::InvalidTimezone(_))));
    }

    #[test]
    fn test_date_conversion() {
        use chrono::{Datelike, NaiveDate};
        use neo4rs::BoltDate;

        let config = ConversionConfig::with_timezone("UTC");
        let naive_date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let bolt_date = BoltType::Date(BoltDate::from(naive_date));
        let result = convert_bolt_to_typed_value(bolt_date, &config).unwrap();

        if let Value::LocalDateTime(dt) = result.value {
            assert_eq!(dt.year(), 2024);
            assert_eq!(dt.month(), 6);
            assert_eq!(dt.day(), 15);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_date_with_timezone_conversion() {
        use chrono::{Datelike, NaiveDate, Timelike};
        use neo4rs::BoltDate;

        // America/New_York is UTC-5 in winter, UTC-4 in summer
        let config = ConversionConfig::with_timezone("America/New_York");
        let naive_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let bolt_date = BoltType::Date(BoltDate::from(naive_date));
        let result = convert_bolt_to_typed_value(bolt_date, &config).unwrap();

        if let Value::LocalDateTime(dt) = result.value {
            // Midnight in EST (UTC-5) is 05:00 UTC
            assert_eq!(dt.year(), 2024);
            assert_eq!(dt.month(), 1);
            assert_eq!(dt.day(), 15);
            assert_eq!(dt.hour(), 5); // 00:00 EST = 05:00 UTC
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_list_integer_conversion() {
        use neo4rs::BoltList;

        let config = ConversionConfig::default();
        let mut bolt_list = BoltList::new();
        bolt_list.push(BoltType::Integer(BoltInteger::new(1)));
        bolt_list.push(BoltType::Integer(BoltInteger::new(2)));
        bolt_list.push(BoltType::Integer(BoltInteger::new(3)));

        let result = convert_bolt_to_typed_value(BoltType::List(bolt_list), &config).unwrap();

        if let Value::Array { elements, .. } = result.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], Value::Int64(1)));
            assert!(matches!(elements[1], Value::Int64(2)));
            assert!(matches!(elements[2], Value::Int64(3)));
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_list_string_conversion() {
        use neo4rs::BoltList;

        let config = ConversionConfig::default();
        let mut bolt_list = BoltList::new();
        bolt_list.push(BoltType::String(BoltString::new("a")));
        bolt_list.push(BoltType::String(BoltString::new("b")));

        let result = convert_bolt_to_typed_value(BoltType::List(bolt_list), &config).unwrap();

        if let Value::Array { elements, .. } = result.value {
            assert_eq!(elements.len(), 2);
            if let Value::Text(s) = &elements[0] {
                assert_eq!(s, "a");
            } else {
                panic!("Expected Text value");
            }
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_empty_list_conversion() {
        use neo4rs::BoltList;

        let config = ConversionConfig::default();
        let bolt_list = BoltList::new();

        let result = convert_bolt_to_typed_value(BoltType::List(bolt_list), &config).unwrap();

        if let Value::Array { elements, .. } = result.value {
            assert!(elements.is_empty());
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_map_conversion() {
        use neo4rs::BoltMap;

        let config = ConversionConfig::default();
        let mut bolt_map = BoltMap::new();
        bolt_map.put("name".into(), BoltType::String(BoltString::new("Alice")));
        bolt_map.put("age".into(), BoltType::Integer(BoltInteger::new(30)));

        let result = convert_bolt_to_typed_value(BoltType::Map(bolt_map), &config).unwrap();

        if let Value::Json(json) = result.value {
            assert_eq!(json["name"], "Alice");
            assert_eq!(json["age"], 30);
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_nested_list_no_json_parsing() {
        use neo4rs::BoltList;

        // Even with JSON parsing enabled, nested strings should NOT be parsed
        let config = ConversionConfig::default().with_json_parsing();
        let mut bolt_list = BoltList::new();
        bolt_list.push(BoltType::String(BoltString::new("{\"nested\": true}")));

        let result = convert_bolt_to_typed_value(BoltType::List(bolt_list), &config).unwrap();

        if let Value::Array { elements, .. } = result.value {
            // The nested string should NOT be parsed as JSON
            if let Value::Text(s) = &elements[0] {
                assert_eq!(s, "{\"nested\": true}");
            } else {
                panic!("Expected nested string to remain as Text");
            }
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_local_datetime_conversion() {
        use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
        use neo4rs::BoltLocalDateTime;

        let config = ConversionConfig::with_timezone("UTC");
        let naive_dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 45).unwrap(),
        );
        let bolt_local_dt = BoltType::LocalDateTime(BoltLocalDateTime::from(naive_dt));
        let result = convert_bolt_to_typed_value(bolt_local_dt, &config).unwrap();

        if let Value::LocalDateTime(dt) = result.value {
            assert_eq!(dt.year(), 2024);
            assert_eq!(dt.month(), 6);
            assert_eq!(dt.day(), 15);
            assert_eq!(dt.hour(), 10);
            assert_eq!(dt.minute(), 30);
            assert_eq!(dt.second(), 45);
        } else {
            panic!("Expected DateTime value");
        }
    }

    // Point2D and Point3D tests are skipped here because BoltPoint2D/3D don't have
    // public constructors for testing. These types are tested in E2E integration tests
    // with actual Neo4j data in tests/neo4j/.
}
