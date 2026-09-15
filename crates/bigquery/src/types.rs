//! BigQuery REST API v2 type conversions for surreal-sync.
//!
//! The BigQuery REST API returns every result cell as a JSON string (or JSON
//! `null`), together with per-column metadata (`schema.fields`) describing the
//! logical BigQuery type. This module turns a `(cell, field)` pair into a
//! [`surreal_sync_core::Value`], the intermediate representation the rest of
//! surreal-sync writes to SurrealDB.
//!
//! It is intentionally free of any network or HTTP concerns so the conversion
//! logic can be unit-tested with plain JSON fixtures (see the tests at the bottom
//! of this file). Those tests are the always-on correctness coverage for this
//! source: the emulator-backed integration tests in `tests/bigquery/` prove the
//! wiring, but only these pin the wire encodings of the real API.
//!
//! # References
//! - Response shape: <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query>
//! - Field metadata: <https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema>
//!
//! # Wire encodings this module depends on
//!
//! Rows arrive as `{"f": [{"v": …}, …]}`. After the caller unwraps `"v"`, the
//! payload is one of:
//!   - a JSON string for every present scalar (yes, including numbers and booleans),
//!   - JSON `null` for SQL NULL,
//!   - a JSON array of `{"v": …}` wrappers for a `REPEATED` field,
//!   - a nested `{"f": [{"v": …}, …]}` object for a `RECORD`/`STRUCT` field.
//!
//! Scalar encodings:
//!   - `INTEGER`/`INT64`: digits as a string, e.g. `"42"`.
//!   - `FLOAT`/`FLOAT64`: e.g. `"1.5"`, and the non-finite tokens `"NaN"`,
//!     `"Infinity"`, `"-Infinity"`.
//!   - `NUMERIC`/`BIGNUMERIC`: exact decimal digits as a string, e.g. `"3.14"`.
//!   - `BOOLEAN`/`BOOL`: `"true"` / `"false"`.
//!   - `STRING`: the string itself.
//!   - `BYTES`: standard base64.
//!   - `DATE`: `"2022-02-01"`.
//!   - `TIME`: `"01:01:01.500000"`.
//!   - `DATETIME`: `"2022-01-01T00:00:00"` (civil time, no offset).
//!   - `TIMESTAMP`: epoch **seconds** in float representation. Both plain
//!     (`"1640995200.0"`) and scientific (`"1.6409952E9"`) notation occur, so the
//!     parser below accepts either.
//!   - `JSON`: a JSON document serialized as a string.
//!   - `GEOGRAPHY`: WKT, e.g. `"POINT(1 2)"`.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use surreal_sync_core::{Type, Value};

/// Metadata for a single result column, deserialized directly from the BigQuery
/// REST API `schema.fields[]` entries.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FieldSchema {
    /// Column name as reported by BigQuery.
    pub name: String,
    /// Logical BigQuery type, e.g. `STRING`, `INTEGER`, `TIMESTAMP`, `RECORD`.
    #[serde(rename = "type")]
    pub field_type: String,
    /// `NULLABLE` (default), `REQUIRED`, or `REPEATED`.
    #[serde(default)]
    pub mode: Option<String>,
    /// Child fields for `RECORD`/`STRUCT` columns.
    #[serde(default)]
    pub fields: Vec<FieldSchema>,
    /// Total digits for `NUMERIC`/`BIGNUMERIC`. The API sends this as a string.
    #[serde(default)]
    pub precision: Option<String>,
    /// Fractional digits for `NUMERIC`/`BIGNUMERIC`. Also string-encoded.
    #[serde(default)]
    pub scale: Option<String>,
}

impl FieldSchema {
    /// Construct a field descriptor (handy in tests and callers that build
    /// metadata by hand).
    pub fn new(name: impl Into<String>, field_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            field_type: field_type.into(),
            mode: None,
            fields: Vec::new(),
            precision: None,
            scale: None,
        }
    }

    /// Set the mode (builder-style), e.g. `"REPEATED"`.
    pub fn with_mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = Some(mode.into());
        self
    }

    /// Set the child fields of a `RECORD` (builder-style).
    pub fn with_fields(mut self, fields: Vec<FieldSchema>) -> Self {
        self.fields = fields;
        self
    }

    /// Set precision and scale (builder-style).
    pub fn with_precision_scale(mut self, precision: i64, scale: i64) -> Self {
        self.precision = Some(precision.to_string());
        self.scale = Some(scale.to_string());
        self
    }

    /// Whether this column is an array column.
    pub fn is_repeated(&self) -> bool {
        self.mode
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case("REPEATED"))
    }

    fn is_record(&self) -> bool {
        let ty = self.field_type.to_ascii_uppercase();
        ty == "RECORD" || ty == "STRUCT"
    }

    /// A copy of this field as a single (non-repeated) element, used when
    /// recursing into the elements of a `REPEATED` column.
    fn as_element(&self) -> Self {
        let mut element = self.clone();
        element.mode = None;
        element
    }
}

/// Unwrap the `{"v": …}` envelope the REST API wraps every cell in.
///
/// Values that are not wrapped are returned as-is, so this is safe to apply to
/// already-unwrapped payloads.
fn unwrap_v(raw: &JsonValue) -> &JsonValue {
    match raw {
        JsonValue::Object(map) if map.len() == 1 => map.get("v").unwrap_or(raw),
        other => other,
    }
}

/// Convert a single REST API result cell into a [`Value`], using the column's
/// BigQuery type metadata to interpret the stringified value.
///
/// `raw` is the cell payload with the outer `"v"` envelope already removed: a
/// [`JsonValue::String`] for present scalars, [`JsonValue::Null`] for SQL NULL, a
/// [`JsonValue::Array`] of `{"v": …}` wrappers for `REPEATED` fields, and a
/// `{"f": [...]}` object for `RECORD` fields.
pub fn convert_cell(raw: &JsonValue, field: &FieldSchema) -> Result<Value> {
    // BigQuery has no null arrays: a REPEATED column is either populated or empty.
    // Normalise a null cell to an empty array so a schemafull SurrealDB table that
    // declares an array field does not receive NULL instead.
    if field.is_repeated() {
        if raw.is_null() {
            return Ok(Value::Array {
                elements: Vec::new(),
                element_type: Box::new(element_type_for(&field.as_element())),
            });
        }
        return convert_repeated(raw, field);
    }

    // SQL NULL is a genuine JSON null (not the string "null").
    if raw.is_null() {
        return Ok(Value::Null);
    }

    if field.is_record() {
        return convert_record(raw, field);
    }

    convert_scalar(raw, field)
}

/// A `REPEATED` column: a JSON array whose elements are `{"v": …}` wrappers.
fn convert_repeated(raw: &JsonValue, field: &FieldSchema) -> Result<Value> {
    let array = raw.as_array().ok_or_else(|| {
        anyhow!(
            "column '{}' is REPEATED but the cell is not a JSON array",
            field.name
        )
    })?;

    let element_field = field.as_element();
    let mut elements = Vec::with_capacity(array.len());
    for item in array {
        elements.push(convert_cell(unwrap_v(item), &element_field)?);
    }

    Ok(Value::Array {
        elements,
        element_type: Box::new(element_type_for(&element_field)),
    })
}

/// A `RECORD`/`STRUCT` column: `{"f": [{"v": …}, …]}` aligned positionally with
/// `field.fields`.
fn convert_record(raw: &JsonValue, field: &FieldSchema) -> Result<Value> {
    let cells = raw
        .get("f")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("column '{}' is a RECORD but has no 'f' array", field.name))?;

    if cells.len() != field.fields.len() {
        return Err(anyhow!(
            "column '{}' has {} nested cells but {} nested fields were declared",
            field.name,
            cells.len(),
            field.fields.len()
        ));
    }

    let mut object: HashMap<String, Value> = HashMap::with_capacity(cells.len());
    for (cell, child) in cells.iter().zip(&field.fields) {
        object.insert(child.name.clone(), convert_cell(unwrap_v(cell), child)?);
    }
    Ok(Value::Object(object))
}

fn convert_scalar(raw: &JsonValue, field: &FieldSchema) -> Result<Value> {
    let text = cell_as_string(raw);
    let ty = field.field_type.to_ascii_uppercase();

    let value = match ty.as_str() {
        "STRING" => Value::Text(text),

        "INTEGER" | "INT64" => {
            let n: i64 = text
                .trim()
                .parse()
                .with_context(|| format!("column '{}': invalid integer '{text}'", field.name))?;
            Value::Int64(n)
        }

        "FLOAT" | "FLOAT64" => Value::Float64(
            parse_float(&text)
                .with_context(|| format!("column '{}': invalid float '{text}'", field.name))?,
        ),

        "NUMERIC" | "DECIMAL" => decimal(&text, field, 38, 9),
        "BIGNUMERIC" | "BIGDECIMAL" => decimal(&text, field, 76, 38),

        "BOOLEAN" | "BOOL" => Value::Bool(parse_bool(&text)?),

        "BYTES" => Value::Bytes(
            decode_base64(text.trim())
                .with_context(|| format!("column '{}': invalid base64 BYTES", field.name))?,
        ),

        "DATE" => convert_date(&text).with_context(|| format!("column '{}' (DATE)", field.name))?,

        "TIME" => convert_time(&text).with_context(|| format!("column '{}' (TIME)", field.name))?,

        "DATETIME" => Value::LocalDateTime(
            convert_datetime(&text)
                .with_context(|| format!("column '{}' (DATETIME)", field.name))?,
        ),

        "TIMESTAMP" => Value::ZonedDateTime(
            convert_epoch_timestamp(&text)
                .with_context(|| format!("column '{}' (TIMESTAMP)", field.name))?,
        ),

        "JSON" => {
            let json: JsonValue = serde_json::from_str(&text)
                .with_context(|| format!("column '{}': invalid JSON payload", field.name))?;
            Value::Json(Box::new(json))
        }

        // GEOGRAPHY arrives as WKT. surreal-sync has no WKT parser, so the text is
        // preserved verbatim rather than dropped or half-converted.
        "GEOGRAPHY" => Value::Text(text),

        // INTERVAL, RANGE, and anything a future API version introduces: preserve
        // the raw text rather than failing the whole sync.
        _ => Value::Text(text),
    };

    Ok(value)
}

/// The declared element type of an array column, used for [`Value::Array`].
fn element_type_for(field: &FieldSchema) -> Type {
    match field.field_type.to_ascii_uppercase().as_str() {
        "STRING" | "GEOGRAPHY" => Type::Text,
        "INTEGER" | "INT64" => Type::Int64,
        "FLOAT" | "FLOAT64" => Type::Float64,
        "BOOLEAN" | "BOOL" => Type::Bool,
        "BYTES" => Type::Bytes,
        "DATE" => Type::Date,
        "TIME" => Type::Time,
        "DATETIME" => Type::LocalDateTime,
        "TIMESTAMP" => Type::ZonedDateTime,
        "RECORD" | "STRUCT" => Type::Object,
        // NUMERIC/BIGNUMERIC carry precision/scale that Type cannot express here,
        // and unknown types fall back to Text like their scalar conversion does.
        _ => Type::Json,
    }
}

/// Extract the string payload of a cell. Present scalars arrive as JSON strings;
/// we also tolerate raw JSON numbers/booleans in case the server emits them.
fn cell_as_string(raw: &JsonValue) -> String {
    match raw {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn decimal(text: &str, field: &FieldSchema, default_precision: u8, default_scale: u8) -> Value {
    let precision = field
        .precision
        .as_deref()
        .and_then(|p| p.trim().parse::<i64>().ok())
        .map(|p| p.clamp(0, u8::MAX as i64) as u8)
        .unwrap_or(default_precision);
    let scale = field
        .scale
        .as_deref()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .map(|s| s.clamp(0, u8::MAX as i64) as u8)
        .unwrap_or(default_scale);

    Value::Decimal {
        value: text.trim().to_string(),
        precision,
        scale,
    }
}

/// BigQuery encodes non-finite floats as bare tokens rather than JSON numbers.
fn parse_float(text: &str) -> Result<f64> {
    let trimmed = text.trim();
    match trimmed {
        "NaN" => return Ok(f64::NAN),
        "Infinity" | "inf" => return Ok(f64::INFINITY),
        "-Infinity" | "-inf" => return Ok(f64::NEG_INFINITY),
        _ => {}
    }
    trimmed
        .parse::<f64>()
        .map_err(|e| anyhow!("invalid float '{trimmed}': {e}"))
}

fn parse_bool(text: &str) -> Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        other => Err(anyhow!("invalid boolean value '{other}'")),
    }
}

/// DATE: `"YYYY-MM-DD"`.
fn convert_date(text: &str) -> Result<Value> {
    let date = NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
        .with_context(|| format!("invalid date '{text}'"))?;
    let naive = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("date out of range: {text}"))?;
    Ok(Value::Date(DateTime::<Utc>::from_naive_utc_and_offset(
        naive, Utc,
    )))
}

/// TIME: `"HH:MM:SS"` with an optional fractional part.
fn convert_time(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    let time = NaiveTime::parse_from_str(trimmed, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(trimmed, "%H:%M:%S"))
        .with_context(|| format!("invalid time '{text}'"))?;
    let naive = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(time);
    Ok(Value::Time(DateTime::<Utc>::from_naive_utc_and_offset(
        naive, Utc,
    )))
}

/// DATETIME: civil `"YYYY-MM-DDTHH:MM:SS"` with an optional fractional part. The
/// API has also been seen using a space separator instead of `T`.
fn convert_datetime(text: &str) -> Result<DateTime<Utc>> {
    let trimmed = text.trim();
    let naive = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f"))
        .with_context(|| format!("invalid datetime '{text}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

/// Values at or beyond this magnitude are microseconds, below it seconds.
///
/// 1e12 microseconds is 1970-01-12; 1e12 seconds is the year 33658. Any timestamp
/// a real workload carries falls unambiguously on one side. The cost is that an
/// instant in the first eleven days of 1970, sent as integer microseconds, would be
/// read as seconds — an acceptable trade for not having to trust a single encoding.
const MICROS_THRESHOLD: i128 = 1_000_000_000_000;

/// TIMESTAMP: epoch microseconds (integer) or epoch seconds (float).
///
/// The client requests `formatOptions.useInt64Timestamp`, so both real BigQuery and
/// the emulator should send integer microseconds. The float-seconds forms are still
/// accepted because that is what either server sends without the flag, and they
/// disagree on spelling: real BigQuery emits scientific notation (`"1.6409952E9"`)
/// while the emulator emits plain decimal (`"1640995200.000000"`). Being tolerant
/// here is what keeps an emulator-green CI honest about the real API.
fn convert_epoch_timestamp(text: &str) -> Result<DateTime<Utc>> {
    let trimmed = text.trim();

    let (secs, nanos) = if trimmed.contains(['.', 'e', 'E']) {
        parse_float_seconds(trimmed)?
    } else {
        let micros: i128 = trimmed
            .parse()
            .with_context(|| format!("invalid epoch timestamp '{trimmed}'"))?;
        if micros.abs() >= MICROS_THRESHOLD {
            split_micros(micros)
        } else {
            (
                i64::try_from(micros).map_err(|_| anyhow!("timestamp out of range: {text}"))?,
                0,
            )
        }
    };

    DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("timestamp out of range: {text}"))
}

/// Split signed epoch microseconds into `(seconds, nanoseconds)` with chrono's
/// convention of a non-negative nanosecond offset.
fn split_micros(micros: i128) -> (i64, u32) {
    let secs = micros.div_euclid(1_000_000);
    let rem = micros.rem_euclid(1_000_000);
    (secs as i64, (rem as u32) * 1_000)
}

/// Parse epoch seconds carrying a fractional part, in plain or exponent notation.
fn parse_float_seconds(text: &str) -> Result<(i64, u32)> {
    if text.contains(['e', 'E']) {
        // Exponent form loses no meaningful precision at microsecond resolution:
        // f64 has ~15-16 significant digits and epoch-seconds-with-micros needs 16.
        let as_float: f64 = text
            .parse()
            .with_context(|| format!("invalid epoch timestamp '{text}'"))?;
        let micros = (as_float * 1_000_000.0).round() as i128;
        return Ok(split_micros(micros));
    }
    parse_epoch_fraction(text)
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

/// Decode standard base64 (the encoding the REST API uses for `BYTES`).
fn decode_base64(s: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| anyhow!("invalid base64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn f(ty: &str) -> FieldSchema {
        FieldSchema::new("c", ty)
    }

    #[test]
    fn null_cell_maps_to_null_regardless_of_type() {
        assert_eq!(
            convert_cell(&JsonValue::Null, &f("INTEGER")).unwrap(),
            Value::Null
        );
        assert_eq!(
            convert_cell(&JsonValue::Null, &f("STRING")).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn null_repeated_cell_is_an_empty_array() {
        // BigQuery has no null arrays, so a null cell on a REPEATED column means
        // "no elements" rather than NULL.
        let field = f("STRING").with_mode("REPEATED");
        let Value::Array { elements, .. } = convert_cell(&JsonValue::Null, &field).unwrap() else {
            panic!("expected Array");
        };
        assert!(elements.is_empty());
    }

    #[test]
    fn string_literal_null_is_text_not_null() {
        // The JSON string "null" is real data, not SQL NULL.
        assert_eq!(
            convert_cell(&json!("null"), &f("STRING")).unwrap(),
            Value::Text("null".to_string())
        );
    }

    #[test]
    fn string_passthrough() {
        assert_eq!(
            convert_cell(&json!("hello"), &f("STRING")).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn integer_is_int64() {
        assert_eq!(
            convert_cell(&json!("42"), &f("INTEGER")).unwrap(),
            Value::Int64(42)
        );
        assert_eq!(
            convert_cell(&json!("-7"), &f("INT64")).unwrap(),
            Value::Int64(-7)
        );
    }

    #[test]
    fn float_is_float64() {
        assert_eq!(
            convert_cell(&json!("1.5"), &f("FLOAT")).unwrap(),
            Value::Float64(1.5)
        );
        assert_eq!(
            convert_cell(&json!("-2.25"), &f("FLOAT64")).unwrap(),
            Value::Float64(-2.25)
        );
    }

    #[test]
    fn float_non_finite_tokens() {
        let Value::Float64(nan) = convert_cell(&json!("NaN"), &f("FLOAT")).unwrap() else {
            panic!("expected Float64");
        };
        assert!(nan.is_nan());
        assert_eq!(
            convert_cell(&json!("Infinity"), &f("FLOAT")).unwrap(),
            Value::Float64(f64::INFINITY)
        );
        assert_eq!(
            convert_cell(&json!("-Infinity"), &f("FLOAT")).unwrap(),
            Value::Float64(f64::NEG_INFINITY)
        );
    }

    #[test]
    fn numeric_defaults_to_38_9() {
        assert_eq!(
            convert_cell(&json!("3.14"), &f("NUMERIC")).unwrap(),
            Value::Decimal {
                value: "3.14".to_string(),
                precision: 38,
                scale: 9,
            }
        );
    }

    #[test]
    fn bignumeric_defaults_to_76_38() {
        assert_eq!(
            convert_cell(&json!("1.5"), &f("BIGNUMERIC")).unwrap(),
            Value::Decimal {
                value: "1.5".to_string(),
                precision: 76,
                scale: 38,
            }
        );
    }

    #[test]
    fn numeric_honours_declared_precision_and_scale() {
        let field = f("NUMERIC").with_precision_scale(10, 2);
        assert_eq!(
            convert_cell(&json!("3.14"), &field).unwrap(),
            Value::Decimal {
                value: "3.14".to_string(),
                precision: 10,
                scale: 2,
            }
        );
    }

    #[test]
    fn numeric_beyond_i64_is_preserved_exactly() {
        let big = "123456789012345678901234567890.5";
        let Value::Decimal { value, .. } = convert_cell(&json!(big), &f("NUMERIC")).unwrap() else {
            panic!("expected Decimal");
        };
        assert_eq!(value, big);
    }

    #[test]
    fn boolean_variants() {
        assert_eq!(
            convert_cell(&json!("true"), &f("BOOLEAN")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            convert_cell(&json!("false"), &f("BOOL")).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn bytes_base64_decodes() {
        // "DEADBEEF" as bytes, base64-encoded.
        assert_eq!(
            convert_cell(&json!("3q2+7w=="), &f("BYTES")).unwrap(),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn date_iso_string() {
        let Value::Date(dt) = convert_cell(&json!("2022-02-01"), &f("DATE")).unwrap() else {
            panic!("expected Date");
        };
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2022-02-01");
    }

    #[test]
    fn time_with_and_without_fraction() {
        let Value::Time(dt) = convert_cell(&json!("01:01:01.500000"), &f("TIME")).unwrap() else {
            panic!("expected Time");
        };
        assert_eq!(dt.format("%H:%M:%S").to_string(), "01:01:01");
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);

        let Value::Time(dt) = convert_cell(&json!("23:59:59"), &f("TIME")).unwrap() else {
            panic!("expected Time");
        };
        assert_eq!(dt.format("%H:%M:%S").to_string(), "23:59:59");
    }

    #[test]
    fn datetime_is_local_datetime() {
        let Value::LocalDateTime(dt) =
            convert_cell(&json!("2022-01-01T00:00:00"), &f("DATETIME")).unwrap()
        else {
            panic!("expected LocalDateTime");
        };
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2022-01-01T00:00:00"
        );
    }

    #[test]
    fn datetime_accepts_space_separator() {
        let Value::LocalDateTime(dt) =
            convert_cell(&json!("2022-01-01 12:30:00"), &f("DATETIME")).unwrap()
        else {
            panic!("expected LocalDateTime");
        };
        assert_eq!(dt.format("%H:%M:%S").to_string(), "12:30:00");
    }

    #[test]
    fn timestamp_int64_microseconds() {
        // What the API sends when formatOptions.useInt64Timestamp is requested.
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1640995200000000"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1_640_995_200);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn timestamp_int64_microseconds_keeps_sub_second_precision() {
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1623456789123456"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1_623_456_789);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_000);
    }

    #[test]
    fn timestamp_negative_int64_microseconds() {
        // 1969-12-31T23:59:58.5Z as microseconds.
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("-1500000000000"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), -1_500_000);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn small_integer_timestamp_is_read_as_seconds() {
        // Below the microsecond threshold, a bare integer is epoch seconds.
        let Value::ZonedDateTime(dt) = convert_cell(&json!("0"), &f("TIMESTAMP")).unwrap() else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 0);
    }

    #[test]
    fn timestamp_plain_decimal_notation() {
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1640995200.0"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1_640_995_200);
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2022-01-01T00:00:00"
        );
    }

    #[test]
    fn timestamp_keeps_microsecond_precision() {
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1623456789.123456"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1_623_456_789);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_000);
    }

    #[test]
    fn timestamp_scientific_notation() {
        // The API also emits floats in exponent form.
        let Value::ZonedDateTime(dt) =
            convert_cell(&json!("1.6409952E9"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), 1_640_995_200);
    }

    #[test]
    fn timestamp_before_epoch() {
        let Value::ZonedDateTime(dt) = convert_cell(&json!("-1.5"), &f("TIMESTAMP")).unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
    }

    #[test]
    fn json_column_parses_into_json_value() {
        let Value::Json(doc) = convert_cell(&json!("{\"a\":1}"), &f("JSON")).unwrap() else {
            panic!("expected Json");
        };
        assert_eq!(*doc, json!({"a": 1}));
    }

    #[test]
    fn geography_is_preserved_as_wkt_text() {
        assert_eq!(
            convert_cell(&json!("POINT(1 2)"), &f("GEOGRAPHY")).unwrap(),
            Value::Text("POINT(1 2)".to_string())
        );
    }

    #[test]
    fn unknown_type_falls_back_to_text() {
        assert_eq!(
            convert_cell(&json!("0-3 22 10:30:15"), &f("INTERVAL")).unwrap(),
            Value::Text("0-3 22 10:30:15".to_string())
        );
    }

    #[test]
    fn repeated_unwraps_each_element_envelope() {
        let field = f("INTEGER").with_mode("REPEATED");
        let cell = json!([{"v": "1"}, {"v": "2"}, {"v": "3"}]);
        let Value::Array {
            elements,
            element_type,
        } = convert_cell(&cell, &field).unwrap()
        else {
            panic!("expected Array");
        };
        assert_eq!(
            elements,
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]
        );
        assert_eq!(*element_type, Type::Int64);
    }

    #[test]
    fn empty_repeated_is_empty_array() {
        let field = f("STRING").with_mode("REPEATED");
        let Value::Array { elements, .. } = convert_cell(&json!([]), &field).unwrap() else {
            panic!("expected Array");
        };
        assert!(elements.is_empty());
    }

    #[test]
    fn record_becomes_object_keyed_by_child_names() {
        let field = f("RECORD").with_fields(vec![
            FieldSchema::new("id", "INTEGER"),
            FieldSchema::new("label", "STRING"),
        ]);
        let cell = json!({"f": [{"v": "7"}, {"v": "seven"}]});
        let Value::Object(map) = convert_cell(&cell, &field).unwrap() else {
            panic!("expected Object");
        };
        assert_eq!(map.get("id"), Some(&Value::Int64(7)));
        assert_eq!(map.get("label"), Some(&Value::Text("seven".to_string())));
    }

    #[test]
    fn repeated_record_nests_both_shapes() {
        let field = f("RECORD")
            .with_mode("REPEATED")
            .with_fields(vec![FieldSchema::new("n", "INTEGER")]);
        let cell = json!([
            {"v": {"f": [{"v": "1"}]}},
            {"v": {"f": [{"v": "2"}]}}
        ]);
        let Value::Array { elements, .. } = convert_cell(&cell, &field).unwrap() else {
            panic!("expected Array");
        };
        assert_eq!(elements.len(), 2);
        let Value::Object(first) = &elements[0] else {
            panic!("expected Object");
        };
        assert_eq!(first.get("n"), Some(&Value::Int64(1)));
    }

    #[test]
    fn record_arity_mismatch_is_an_error() {
        let field = f("RECORD").with_fields(vec![FieldSchema::new("a", "STRING")]);
        let cell = json!({"f": [{"v": "x"}, {"v": "y"}]});
        assert!(convert_cell(&cell, &field).is_err());
    }

    #[test]
    fn field_schema_deserializes_from_api_metadata() {
        let meta = json!({
            "name": "amount",
            "type": "NUMERIC",
            "mode": "NULLABLE",
            "precision": "38",
            "scale": "9"
        });
        let field: FieldSchema = serde_json::from_value(meta).unwrap();
        assert_eq!(field.name, "amount");
        assert_eq!(field.field_type, "NUMERIC");
        assert_eq!(field.mode.as_deref(), Some("NULLABLE"));
        assert!(!field.is_repeated());
        assert_eq!(field.precision.as_deref(), Some("38"));
    }

    #[test]
    fn field_schema_deserializes_nested_record_metadata() {
        let meta = json!({
            "name": "person",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "tags", "type": "STRING", "mode": "REPEATED"}
            ]
        });
        let field: FieldSchema = serde_json::from_value(meta).unwrap();
        assert!(field.is_repeated());
        assert_eq!(field.fields.len(), 2);
        assert!(field.fields[1].is_repeated());
    }
}
