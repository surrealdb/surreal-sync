//! Snowflake SQL API v2 type conversions for surreal-sync.
//!
//! The Snowflake SQL REST API returns every result cell as a JSON string (or
//! JSON `null`), together with per-column metadata (`resultSetMetaData.rowType`)
//! describing the logical Snowflake type. This crate turns a `(cell, column)`
//! pair into a [`sync_core::Value`], the intermediate representation the
//! rest of surreal-sync writes to SurrealDB.
//!
//! It is intentionally free of any network or HTTP concerns so the conversion
//! logic can be unit-tested with plain JSON fixtures (see the tests at the bottom
//! of this file).
//!
//! # References
//! - Snowflake SQL API data types are described at
//!   <https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses>.
//! - The scalar encodings this crate depends on:
//!   - `fixed` (NUMBER): integer/decimal digits as a string, e.g. `"42"`, `"3.14"`.
//!   - `real`/`float`: floating point as a string, e.g. `"1.5"`.
//!   - `text`: the string itself.
//!   - `boolean`: `"true"` / `"false"` (also tolerating `"1"` / `"0"`).
//!   - `date`: integer number of days since the Unix epoch, e.g. `"19024"`.
//!   - `time`: seconds since midnight with a fractional part, e.g. `"3661.000000000"`.
//!   - `timestamp_ntz` / `timestamp_ltz`: seconds since the Unix epoch with a
//!     fractional part, e.g. `"1623456789.123456789"`.
//!   - `timestamp_tz`: the same epoch encoding followed by a space and a timezone
//!     offset token, e.g. `"1623456789.123456789 1440"`. The instant is already
//!     absolute, so the offset token is not needed to recover the UTC time.
//!   - `variant` / `object` / `array`: a JSON document serialized as a string.
//!   - `binary`: hex-encoded bytes, e.g. `"DEADBEEF"`.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use sync_core::{Type, Value};

/// Metadata for a single result column, deserialized directly from the Snowflake
/// SQL API `resultSetMetaData.rowType[]` entries.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ColumnType {
    /// Column name as reported by Snowflake (typically upper-cased).
    pub name: String,
    /// Logical Snowflake type, e.g. `fixed`, `text`, `timestamp_ntz`.
    #[serde(rename = "type")]
    pub snowflake_type: String,
    /// Total number of digits for `fixed` (NUMBER) columns.
    #[serde(default)]
    pub precision: Option<i64>,
    /// Number of fractional digits for `fixed`/`time`/`timestamp` columns.
    #[serde(default)]
    pub scale: Option<i64>,
    /// Whether the column is nullable (unused by conversion, kept for fidelity).
    #[serde(default)]
    pub nullable: bool,
    /// Declared length for `text`/`binary` columns (unused by conversion).
    #[serde(default)]
    pub length: Option<i64>,
}

impl ColumnType {
    /// Construct a column descriptor (handy in tests and callers that build
    /// metadata by hand).
    pub fn new(name: impl Into<String>, snowflake_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            snowflake_type: snowflake_type.into(),
            precision: None,
            scale: None,
            nullable: true,
            length: None,
        }
    }

    /// Set the precision (builder-style).
    pub fn with_precision(mut self, precision: i64) -> Self {
        self.precision = Some(precision);
        self
    }

    /// Set the scale (builder-style).
    pub fn with_scale(mut self, scale: i64) -> Self {
        self.scale = Some(scale);
        self
    }
}

/// Convert a single SQL API result cell into a [`Value`], using the
/// column's Snowflake type metadata to interpret the stringified value.
///
/// `raw` is the JSON value straight out of the `data` array: a
/// [`JsonValue::String`] for present scalars, [`JsonValue::Null`] for SQL NULL,
/// and (defensively) a JSON number/bool if a future server ever emits one.
pub fn convert_cell(raw: &JsonValue, col: &ColumnType) -> Result<Value> {
    // SQL NULL is a genuine JSON null (not the string "null").
    if raw.is_null() {
        return Ok(Value::Null);
    }

    let text = cell_as_string(raw);
    let ty = col.snowflake_type.to_ascii_lowercase();

    let value = match ty.as_str() {
        "fixed" | "number" | "decimal" | "numeric" | "int" | "integer" | "bigint" | "smallint"
        | "tinyint" | "byteint" => {
            convert_fixed(&text, col).with_context(|| format!("column '{}' (fixed)", col.name))?
        }

        "real" | "float" | "float4" | "float8" | "double" | "double precision" => {
            let f: f64 = text
                .trim()
                .parse()
                .with_context(|| format!("column '{}': invalid float '{text}'", col.name))?;
            Value::Float64(f)
        }

        "text" | "string" | "varchar" | "char" | "character" => Value::Text(text),

        "boolean" | "bool" => Value::Bool(parse_bool(&text)?),

        "date" => convert_date(&text).with_context(|| format!("column '{}' (date)", col.name))?,

        "time" => convert_time(&text).with_context(|| format!("column '{}' (time)", col.name))?,

        "timestamp_ntz" | "timestampntz" | "datetime" => {
            let dt = convert_epoch_timestamp(&text)
                .with_context(|| format!("column '{}' (timestamp_ntz)", col.name))?;
            Value::LocalDateTime(dt)
        }

        "timestamp_ltz" | "timestampltz" | "timestamp_tz" | "timestamptz" | "timestamp" => {
            let dt = convert_epoch_timestamp(&text)
                .with_context(|| format!("column '{}' (timestamp)", col.name))?;
            Value::ZonedDateTime(dt)
        }

        "variant" | "object" => {
            let json: JsonValue = serde_json::from_str(&text)
                .with_context(|| format!("column '{}': invalid VARIANT/OBJECT JSON", col.name))?;
            json_to_universal_value(json)
        }

        "array" => {
            let json: JsonValue = serde_json::from_str(&text)
                .with_context(|| format!("column '{}': invalid ARRAY JSON", col.name))?;
            match json_to_universal_value(json) {
                arr @ Value::Array { .. } => arr,
                // A non-array VARIANT stored in an ARRAY column: wrap defensively.
                other => Value::Array {
                    elements: vec![other],
                    element_type: Box::new(Type::Json),
                },
            }
        }

        "binary" | "varbinary" => Value::Bytes(
            decode_hex(text.trim())
                .with_context(|| format!("column '{}': invalid hex BINARY", col.name))?,
        ),

        // Unknown/unsupported logical type: preserve the raw text rather than
        // failing the whole sync.
        _ => Value::Text(text),
    };

    Ok(value)
}

/// Extract the string payload of a cell. Present scalars arrive as JSON strings;
/// we also tolerate raw JSON numbers/booleans in case the server emits them.
fn cell_as_string(raw: &JsonValue) -> String {
    match raw {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn convert_fixed(text: &str, col: &ColumnType) -> Result<Value> {
    let scale = col.scale.unwrap_or(0);
    let trimmed = text.trim();

    if scale <= 0 {
        // Integer NUMBER. Prefer i64; fall back to Decimal when it does not fit
        // (Snowflake NUMBER supports up to 38 digits of precision).
        if let Ok(n) = trimmed.parse::<i64>() {
            return Ok(Value::Int64(n));
        }
    }

    let precision = col.precision.unwrap_or(38).clamp(0, u8::MAX as i64) as u8;
    let scale_u8 = scale.clamp(0, u8::MAX as i64) as u8;

    Ok(Value::Decimal {
        value: trimmed.to_string(),
        precision,
        scale: scale_u8,
    })
}

fn parse_bool(text: &str) -> Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        other => Err(anyhow!("invalid boolean value '{other}'")),
    }
}

/// DATE: integer number of days since the Unix epoch.
fn convert_date(text: &str) -> Result<Value> {
    let days: i64 = text
        .trim()
        .parse()
        .with_context(|| format!("invalid date days value '{text}'"))?;
    let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    );
    let dt = epoch + chrono::Duration::days(days);
    Ok(Value::Date(dt))
}

/// TIME: seconds since midnight with a fractional part.
fn convert_time(text: &str) -> Result<Value> {
    let (secs, nanos) = parse_epoch_fraction(text)?;
    let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    );
    let dt = epoch + chrono::Duration::seconds(secs) + chrono::Duration::nanoseconds(nanos as i64);
    Ok(Value::Time(dt))
}

/// TIMESTAMP_*: seconds since the Unix epoch with a fractional part, optionally
/// followed by a timezone offset token (which we ignore because the instant is
/// already absolute).
fn convert_epoch_timestamp(text: &str) -> Result<DateTime<Utc>> {
    let epoch_token = text.split_whitespace().next().unwrap_or(text.trim());
    let (secs, nanos) = parse_epoch_fraction(epoch_token)?;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("timestamp out of range: {text}"))
}

/// Parse a `"<seconds>.<fraction>"` string into `(seconds, nanoseconds)`.
/// Handles negative values (pre-epoch) and a missing fractional part.
fn parse_epoch_fraction(text: &str) -> Result<(i64, u32)> {
    let trimmed = text.trim();
    let (whole, frac) = match trimmed.split_once('.') {
        Some((w, f)) => (w, f),
        None => (trimmed, ""),
    };

    let secs: i64 = whole
        .parse()
        .with_context(|| format!("invalid epoch seconds '{trimmed}'"))?;

    if frac.is_empty() {
        return Ok((secs, 0));
    }

    // Normalize the fractional digits to exactly 9 (nanosecond) places.
    let mut nanos_str = frac.to_string();
    if nanos_str.len() > 9 {
        nanos_str.truncate(9);
    } else {
        while nanos_str.len() < 9 {
            nanos_str.push('0');
        }
    }
    let mut nanos: u32 = nanos_str
        .parse()
        .with_context(|| format!("invalid epoch fraction '{frac}'"))?;

    // For negative instants the fraction subtracts from the whole second, so map
    // it onto chrono's convention of a non-negative nanosecond offset.
    let secs = if secs < 0 && nanos > 0 {
        nanos = 1_000_000_000 - nanos;
        secs - 1
    } else {
        secs
    };

    Ok((secs, nanos))
}

/// Recursively convert a decoded JSON document (VARIANT/OBJECT/ARRAY) into a
/// [`Value`], mirroring the PostgreSQL source's JSON handling.
fn json_to_universal_value(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => Value::Text(s),
        JsonValue::Array(arr) => {
            let elements: Vec<Value> = arr.into_iter().map(json_to_universal_value).collect();
            Value::Array {
                elements,
                element_type: Box::new(Type::Json),
            }
        }
        JsonValue::Object(map) => {
            let obj: HashMap<String, Value> = map
                .into_iter()
                .map(|(k, v)| (k, json_to_universal_value(v)))
                .collect();
            Value::Object(obj)
        }
    }
}

/// Decode an even-length hex string into bytes. Accepts upper- or lower-case.
fn decode_hex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(anyhow!("hex string has odd length: {} chars", s.len()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(anyhow!("invalid hex digit: {:?}", other as char)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(ty: &str) -> ColumnType {
        ColumnType::new("C", ty)
    }

    #[test]
    fn null_cell_maps_to_null_regardless_of_type() {
        assert_eq!(
            convert_cell(&JsonValue::Null, &col("fixed")).unwrap(),
            Value::Null
        );
        assert_eq!(
            convert_cell(&JsonValue::Null, &col("text")).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn string_literal_null_is_text_not_null() {
        // The JSON string "null" is real data, not SQL NULL.
        assert_eq!(
            convert_cell(&json!("null"), &col("text")).unwrap(),
            Value::Text("null".to_string())
        );
    }

    #[test]
    fn fixed_scale_zero_is_int64() {
        let c = col("fixed").with_precision(10).with_scale(0);
        assert_eq!(convert_cell(&json!("42"), &c).unwrap(), Value::Int64(42));
        assert_eq!(convert_cell(&json!("-7"), &c).unwrap(), Value::Int64(-7));
    }

    #[test]
    fn fixed_with_scale_is_decimal() {
        let c = col("fixed").with_precision(10).with_scale(2);
        assert_eq!(
            convert_cell(&json!("3.14"), &c).unwrap(),
            Value::Decimal {
                value: "3.14".to_string(),
                precision: 10,
                scale: 2,
            }
        );
    }

    #[test]
    fn fixed_overflowing_i64_falls_back_to_decimal() {
        // NUMBER(38,0) larger than i64::MAX must not be truncated.
        let c = col("fixed").with_precision(38).with_scale(0);
        let big = "123456789012345678901234567890";
        assert_eq!(
            convert_cell(&json!(big), &c).unwrap(),
            Value::Decimal {
                value: big.to_string(),
                precision: 38,
                scale: 0,
            }
        );
    }

    #[test]
    fn real_is_float64() {
        assert_eq!(
            convert_cell(&json!("1.5"), &col("real")).unwrap(),
            Value::Float64(1.5)
        );
    }

    #[test]
    fn text_passthrough() {
        assert_eq!(
            convert_cell(&json!("hello"), &col("text")).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn boolean_variants() {
        assert_eq!(
            convert_cell(&json!("true"), &col("boolean")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            convert_cell(&json!("false"), &col("boolean")).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            convert_cell(&json!("1"), &col("boolean")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            convert_cell(&json!("0"), &col("boolean")).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn date_days_since_epoch() {
        // 0 days => 1970-01-01, 1 day => 1970-01-02.
        let Value::Date(dt) = convert_cell(&json!("0"), &col("date")).unwrap() else {
            panic!("expected Date");
        };
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "1970-01-01");

        let Value::Date(dt) = convert_cell(&json!("19024"), &col("date")).unwrap() else {
            panic!("expected Date");
        };
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2022-02-01");
    }

    #[test]
    fn time_seconds_since_midnight() {
        // 3661.5s => 01:01:01.5
        let Value::Time(dt) = convert_cell(&json!("3661.500000000"), &col("time")).unwrap() else {
            panic!("expected Time");
        };
        assert_eq!(dt.format("%H:%M:%S").to_string(), "01:01:01");
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
    }

    #[test]
    fn timestamp_ntz_is_local_datetime() {
        let Value::LocalDateTime(dt) =
            convert_cell(&json!("1640995200.000000000"), &col("timestamp_ntz")).unwrap()
        else {
            panic!("expected LocalDateTime");
        };
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2022-01-01T00:00:00"
        );
    }

    #[test]
    fn timestamp_ltz_is_zoned_datetime() {
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1623456789.123456789"), &col("timestamp_ltz")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1623456789);
        assert_eq!(dt.timestamp_subsec_nanos(), 123456789);
    }

    #[test]
    fn timestamp_tz_ignores_offset_token() {
        // "<epoch> <offset>" — the offset token must not break parsing.
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1623456789.000000000 1440"), &col("timestamp_tz")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1623456789);
    }

    #[test]
    fn variant_object_becomes_object() {
        let Value::Object(map) =
            convert_cell(&json!("{\"a\":1,\"b\":\"x\"}"), &col("object")).unwrap()
        else {
            panic!("expected Object");
        };
        assert_eq!(map.get("a"), Some(&Value::Int64(1)));
        assert_eq!(map.get("b"), Some(&Value::Text("x".to_string())));
    }

    #[test]
    fn array_becomes_array() {
        let Value::Array { elements, .. } = convert_cell(&json!("[1,2,3]"), &col("array")).unwrap()
        else {
            panic!("expected Array");
        };
        assert_eq!(
            elements,
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3),]
        );
    }

    #[test]
    fn binary_hex_decodes() {
        assert_eq!(
            convert_cell(&json!("DEADBEEF"), &col("binary")).unwrap(),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn unknown_type_falls_back_to_text() {
        assert_eq!(
            convert_cell(&json!("geospatial-thing"), &col("geography")).unwrap(),
            Value::Text("geospatial-thing".to_string())
        );
    }

    #[test]
    fn column_type_deserializes_from_api_metadata() {
        let meta = json!({
            "name": "AMOUNT",
            "type": "fixed",
            "precision": 38,
            "scale": 2,
            "nullable": false,
            "length": null
        });
        let col: ColumnType = serde_json::from_value(meta).unwrap();
        assert_eq!(col.name, "AMOUNT");
        assert_eq!(col.snowflake_type, "fixed");
        assert_eq!(col.precision, Some(38));
        assert_eq!(col.scale, Some(2));
        assert!(!col.nullable);
    }
}
