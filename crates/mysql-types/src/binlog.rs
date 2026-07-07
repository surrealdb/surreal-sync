//! Binlog cell → `UniversalValue` conversion.

use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, ColumnMetadata, JsonDiff, JsonDiffOperation};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use sync_core::{TypedValue, UniversalType, UniversalValue};

use crate::reverse::{
    json_to_typed_value_with_config, ConversionError, JsonConversionConfig, RowConversionConfig,
};

/// Schema/metadata for one binlog column when converting a [`CellValue`].
#[derive(Debug, Clone)]
pub struct BinlogColumnMeta {
    pub column_name: String,
    pub column_type: u8,
    pub metadata: ColumnMetadata,
    pub unsigned: bool,
    pub boolean_hint: bool,
    pub is_binary: bool,
}

impl BinlogColumnMeta {
    pub fn new(column_name: impl Into<String>, column_type: u8, metadata: ColumnMetadata) -> Self {
        Self {
            column_name: column_name.into(),
            column_type,
            metadata,
            unsigned: false,
            boolean_hint: false,
            is_binary: false,
        }
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
    }

    pub fn with_boolean_hint(mut self, hint: bool) -> Self {
        self.boolean_hint = hint;
        self
    }

    pub fn with_binary(mut self, is_binary: bool) -> Self {
        self.is_binary = is_binary;
        self
    }

    fn is_boolean_column(&self) -> bool {
        self.column_type == MYSQL_TYPE_TINY && self.boolean_hint
    }

    fn max_string_length(&self) -> u16 {
        match &self.metadata {
            ColumnMetadata::String { max_length } | ColumnMetadata::EnumSet { max_length } => {
                *max_length
            }
            _ => 255,
        }
    }

    fn decimal_precision_scale(&self) -> (u8, u8) {
        match &self.metadata {
            ColumnMetadata::Numeric { precision, scale } => (*precision, *scale),
            _ => (10, 0),
        }
    }
}

/// Convert a decoded binlog cell into a [`UniversalValue`].
pub fn binlog_cell_to_universal_value(
    cell: &CellValue,
    column: &BinlogColumnMeta,
    config: &RowConversionConfig,
) -> Result<UniversalValue, ConversionError> {
    Ok(binlog_cell_to_typed_value(cell, column, config)?.value)
}

fn binlog_cell_to_typed_value(
    cell: &CellValue,
    column: &BinlogColumnMeta,
    config: &RowConversionConfig,
) -> Result<TypedValue, ConversionError> {
    if matches!(cell, CellValue::Null) {
        return Ok(TypedValue::null(column_type_to_sync_type(column)));
    }

    if config.set_columns.contains(&column.column_name) {
        if let CellValue::String(s) = cell {
            let values: Vec<String> = if s.is_empty() {
                Vec::new()
            } else {
                s.split(',').map(str::to_string).collect()
            };
            return Ok(TypedValue::set(values, vec![]));
        }
    }

    let is_json =
        column.column_type == MYSQL_TYPE_JSON || config.json_columns.contains(&column.column_name);
    if is_json {
        if let Some(tv) = convert_json_cell(cell, config.json_config.as_ref()) {
            return Ok(tv);
        }
    }

    match column.column_type {
        MYSQL_TYPE_TINY => {
            let i = extract_signed_int(cell)?;
            if column.is_boolean_column() {
                if i == 0 {
                    return Ok(TypedValue::bool(false));
                }
                if i == 1 {
                    return Ok(TypedValue::bool(true));
                }
            }
            Ok(TypedValue::int8(i as i8, 4))
        }
        MYSQL_TYPE_SHORT => Ok(TypedValue::int16(extract_signed_int(cell)? as i16)),
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
            Ok(TypedValue::int32(extract_signed_int(cell)? as i32))
        }
        MYSQL_TYPE_LONGLONG => Ok(TypedValue::int64(extract_signed_int(cell)?)),
        MYSQL_TYPE_FLOAT => Ok(TypedValue::float32(extract_float(cell)? as f32)),
        MYSQL_TYPE_DOUBLE => Ok(TypedValue::float64(extract_float(cell)?)),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
            let (precision, scale) = column.decimal_precision_scale();
            let s = match cell {
                CellValue::Decimal(d) => d.to_string(),
                CellValue::String(s) => s.clone(),
                other => return Err(type_mismatch("decimal", other)),
            };
            Ok(TypedValue::decimal(s, precision, scale))
        }
        MYSQL_TYPE_STRING => {
            let s = extract_string(cell)?;
            Ok(TypedValue::char_type(s, column.max_string_length()))
        }
        MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => {
            let s = extract_string(cell)?;
            let length = column.max_string_length();
            if length == 36 && is_uuid_format(&s) {
                if let Ok(uuid) = uuid::Uuid::parse_str(&s) {
                    return Ok(TypedValue::uuid(uuid));
                }
            }
            Ok(TypedValue::varchar(s, length))
        }
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB => {
            if column.is_binary {
                Ok(TypedValue::blob(extract_bytes(cell)?))
            } else {
                Ok(TypedValue::text(extract_string(cell)?))
            }
        }
        MYSQL_TYPE_DATE => {
            let (year, month, day) = extract_date_parts(cell)?;
            let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).ok_or_else(
                || {
                    ConversionError::InvalidDateTime(format!(
                        "invalid date components: year={year}, month={month}, day={day}"
                    ))
                },
            )?;
            Ok(TypedValue::date(
                date.and_hms_opt(0, 0, 0).unwrap().and_utc(),
            ))
        }
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => {
            let (hour, minute, second, micros) = extract_time_parts(cell)?;
            let today = Utc::now().date_naive();
            let time = NaiveTime::from_hms_micro_opt(
                hour.unsigned_abs(),
                u32::from(minute),
                u32::from(second),
                micros,
            )
            .ok_or_else(|| {
                ConversionError::InvalidDateTime(format!(
                    "invalid time components: hour={hour}, minute={minute}, second={second}, micro={micros}"
                ))
            })?;
            Ok(TypedValue::time(
                chrono::NaiveDateTime::new(today, time).and_utc(),
            ))
        }
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => {
            Ok(TypedValue::datetime(extract_datetime(cell)?))
        }
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => {
            Ok(TypedValue::timestamptz(extract_timestamp(cell)?))
        }
        MYSQL_TYPE_YEAR => {
            let year = match cell {
                CellValue::Year(y) => *y,
                CellValue::Int(i) => *i as u16,
                CellValue::UInt(u) => *u as u16,
                other => return Err(type_mismatch("year", other)),
            };
            Ok(TypedValue::int16(year as i16))
        }
        MYSQL_TYPE_JSON => match cell {
            CellValue::JsonText(s) => parse_json_text(s, config.json_config.as_ref()),
            CellValue::JsonBytes(bytes) => parse_json_bytes(bytes, config.json_config.as_ref()),
            CellValue::String(s) => parse_json_text(s, config.json_config.as_ref()),
            other => Err(type_mismatch("json", other)),
        },
        MYSQL_TYPE_ENUM => Ok(TypedValue::enum_type(extract_string(cell)?, vec![])),
        MYSQL_TYPE_SET => {
            let s = extract_string(cell)?;
            let values: Vec<String> = s.split(',').map(str::to_string).collect();
            Ok(TypedValue::set(values, vec![]))
        }
        MYSQL_TYPE_GEOMETRY => {
            let bytes = extract_bytes(cell)?;
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let geojson = serde_json::json!({
                "type": "Point",
                "wkb_base64": encoded
            });
            Ok(TypedValue::geometry_geojson(
                geojson,
                sync_core::GeometryType::Point,
            ))
        }
        MYSQL_TYPE_BIT => {
            let bytes = extract_bytes(cell)?;
            if bytes.len() == 1 && bytes[0] <= 1 {
                Ok(TypedValue::bool(bytes[0] == 1))
            } else {
                Ok(TypedValue::bytes(bytes))
            }
        }
        other => Err(ConversionError::UnsupportedType(
            mysql_async::consts::ColumnType::try_from(other)
                .unwrap_or(mysql_async::consts::ColumnType::MYSQL_TYPE_NULL),
        )),
    }
}

fn column_type_to_sync_type(column: &BinlogColumnMeta) -> UniversalType {
    if column.is_boolean_column() {
        return UniversalType::Bool;
    }
    match column.column_type {
        MYSQL_TYPE_TINY => UniversalType::Int8 { width: 4 },
        MYSQL_TYPE_SHORT => UniversalType::Int16,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => UniversalType::Int32,
        MYSQL_TYPE_LONGLONG => UniversalType::Int64,
        MYSQL_TYPE_FLOAT => UniversalType::Float32,
        MYSQL_TYPE_DOUBLE => UniversalType::Float64,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
            let (precision, scale) = column.decimal_precision_scale();
            UniversalType::Decimal { precision, scale }
        }
        MYSQL_TYPE_STRING => UniversalType::Char {
            length: column.max_string_length(),
        },
        MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => UniversalType::VarChar {
            length: column.max_string_length(),
        },
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB => {
            if column.is_binary {
                UniversalType::Blob
            } else {
                UniversalType::Text
            }
        }
        MYSQL_TYPE_DATE => UniversalType::Date,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => UniversalType::Time,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => UniversalType::LocalDateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => UniversalType::ZonedDateTime,
        MYSQL_TYPE_YEAR => UniversalType::Int16,
        MYSQL_TYPE_JSON => UniversalType::Json,
        MYSQL_TYPE_ENUM => UniversalType::Enum { values: vec![] },
        MYSQL_TYPE_SET => UniversalType::Set { values: vec![] },
        MYSQL_TYPE_GEOMETRY => UniversalType::Geometry {
            geometry_type: sync_core::GeometryType::Point,
        },
        MYSQL_TYPE_BIT => UniversalType::Bytes,
        _ => UniversalType::Text,
    }
}

fn convert_json_cell(
    cell: &CellValue,
    json_config: Option<&JsonConversionConfig>,
) -> Option<TypedValue> {
    match cell {
        CellValue::JsonText(s) => Some(parse_json_text(s, json_config).ok()?),
        CellValue::JsonBytes(bytes) => Some(parse_json_bytes(bytes, json_config).ok()?),
        CellValue::String(s) => Some(parse_json_text(s, json_config).ok()?),
        _ => None,
    }
}

fn parse_json_text(
    s: &str,
    json_config: Option<&JsonConversionConfig>,
) -> Result<TypedValue, ConversionError> {
    let json_value = serde_json::from_str::<serde_json::Value>(s)
        .map_err(|e| ConversionError::InvalidJson(e.to_string()))?;
    let default_config;
    let config = match json_config {
        Some(cfg) => cfg,
        None => {
            default_config = JsonConversionConfig::default();
            &default_config
        }
    };
    Ok(json_to_typed_value_with_config(json_value, "", config))
}

fn parse_json_bytes(
    bytes: &[u8],
    json_config: Option<&JsonConversionConfig>,
) -> Result<TypedValue, ConversionError> {
    let json_value =
        parse_mysql_json_binary(bytes).map_err(|e| ConversionError::InvalidJson(e.to_string()))?;
    let default_config;
    let config = match json_config {
        Some(cfg) => cfg,
        None => {
            default_config = JsonConversionConfig::default();
            &default_config
        }
    };
    Ok(json_to_typed_value_with_config(json_value, "", config))
}

pub fn apply_mysql_json_diffs_to_cell(
    base: &CellValue,
    diffs: &[JsonDiff],
) -> Result<CellValue, ConversionError> {
    let mut value = match base {
        CellValue::JsonBytes(bytes) => {
            parse_mysql_json_binary(bytes).map_err(ConversionError::InvalidJson)?
        }
        CellValue::JsonText(s) | CellValue::String(s) => {
            serde_json::from_str::<serde_json::Value>(s)
                .map_err(|e| ConversionError::InvalidJson(e.to_string()))?
        }
        CellValue::Null => serde_json::Value::Null,
        other => return Err(type_mismatch("json base", other)),
    };

    for diff in diffs {
        let data = match (&diff.operation, &diff.data) {
            (JsonDiffOperation::Remove, None) => None,
            (JsonDiffOperation::Remove, Some(_)) => {
                return Err(ConversionError::InvalidJson(
                    "REMOVE JSON diff unexpectedly carried data".into(),
                ))
            }
            (_, Some(bytes)) => {
                Some(parse_mysql_json_binary(bytes).map_err(ConversionError::InvalidJson)?)
            }
            (_, None) => {
                return Err(ConversionError::InvalidJson(format!(
                    "{:?} JSON diff missing data",
                    diff.operation
                )))
            }
        };
        apply_json_diff(&mut value, &diff.operation, &diff.path, data)?;
    }

    serde_json::to_string(&value)
        .map(CellValue::JsonText)
        .map_err(|e| ConversionError::InvalidJson(e.to_string()))
}

pub fn parse_mysql_json_binary(bytes: &[u8]) -> Result<serde_json::Value, String> {
    let mut input = bytes;
    parse_jsonb_value(&mut input)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum JsonPathLeg {
    Key(String),
    Index(usize),
}

fn apply_json_diff(
    root: &mut serde_json::Value,
    operation: &JsonDiffOperation,
    path: &str,
    data: Option<serde_json::Value>,
) -> Result<(), ConversionError> {
    let legs = parse_json_path(path)?;
    if legs.is_empty() {
        match operation {
            JsonDiffOperation::Replace | JsonDiffOperation::Insert => {
                *root = data.expect("checked above");
            }
            JsonDiffOperation::Remove => {
                *root = serde_json::Value::Null;
            }
        }
        return Ok(());
    }

    let (parent_legs, last) = legs.split_at(legs.len() - 1);
    let Some(parent) = navigate_json_path_mut(root, parent_legs) else {
        if *operation == JsonDiffOperation::Insert {
            return Ok(());
        }
        return Err(ConversionError::InvalidJson(format!(
            "JSON diff path parent not found: {path}"
        )));
    };

    match (&last[0], operation) {
        (JsonPathLeg::Key(key), JsonDiffOperation::Replace) => {
            let Some(obj) = parent.as_object_mut() else {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff path is not an object: {path}"
                )));
            };
            if !obj.contains_key(key) {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff replace key not found: {path}"
                )));
            }
            obj.insert(key.clone(), data.expect("checked above"));
        }
        (JsonPathLeg::Key(key), JsonDiffOperation::Insert) => {
            let Some(obj) = parent.as_object_mut() else {
                return Ok(());
            };
            obj.entry(key.clone())
                .or_insert_with(|| data.expect("checked above"));
        }
        (JsonPathLeg::Key(key), JsonDiffOperation::Remove) => {
            let Some(obj) = parent.as_object_mut() else {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff path is not an object: {path}"
                )));
            };
            obj.remove(key);
        }
        (JsonPathLeg::Index(index), JsonDiffOperation::Replace) => {
            let Some(arr) = parent.as_array_mut() else {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff path is not an array: {path}"
                )));
            };
            let Some(slot) = arr.get_mut(*index) else {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff replace index not found: {path}"
                )));
            };
            *slot = data.expect("checked above");
        }
        (JsonPathLeg::Index(index), JsonDiffOperation::Insert) => {
            let Some(arr) = parent.as_array_mut() else {
                return Ok(());
            };
            if *index <= arr.len() {
                arr.insert(*index, data.expect("checked above"));
            }
        }
        (JsonPathLeg::Index(index), JsonDiffOperation::Remove) => {
            let Some(arr) = parent.as_array_mut() else {
                return Err(ConversionError::InvalidJson(format!(
                    "JSON diff path is not an array: {path}"
                )));
            };
            if *index < arr.len() {
                arr.remove(*index);
            }
        }
    }
    Ok(())
}

fn navigate_json_path_mut<'a>(
    mut value: &'a mut serde_json::Value,
    legs: &[JsonPathLeg],
) -> Option<&'a mut serde_json::Value> {
    for leg in legs {
        value = match leg {
            JsonPathLeg::Key(key) => value.as_object_mut()?.get_mut(key)?,
            JsonPathLeg::Index(index) => value.as_array_mut()?.get_mut(*index)?,
        };
    }
    Some(value)
}

fn parse_json_path(path: &str) -> Result<Vec<JsonPathLeg>, ConversionError> {
    let mut chars = path.chars().peekable();
    if chars.next() != Some('$') {
        return Err(ConversionError::InvalidJson(format!(
            "JSON diff path must start with '$': {path}"
        )));
    }
    let mut legs = Vec::new();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    legs.push(JsonPathLeg::Key(read_quoted_path_key(&mut chars, path)?));
                } else {
                    let mut key = String::new();
                    while let Some(&next) = chars.peek() {
                        if next == '.' || next == '[' {
                            break;
                        }
                        key.push(next);
                        chars.next();
                    }
                    if key.is_empty() {
                        return Err(ConversionError::InvalidJson(format!(
                            "empty JSON path key: {path}"
                        )));
                    }
                    legs.push(JsonPathLeg::Key(key));
                }
            }
            '[' => {
                let mut index = String::new();
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next == ']' {
                        break;
                    }
                    index.push(next);
                }
                let index = index.parse::<usize>().map_err(|_| {
                    ConversionError::InvalidJson(format!("unsupported JSON path index: {path}"))
                })?;
                legs.push(JsonPathLeg::Index(index));
            }
            _ => {
                return Err(ConversionError::InvalidJson(format!(
                    "unsupported JSON path syntax: {path}"
                )))
            }
        }
    }
    Ok(legs)
}

fn read_quoted_path_key<I>(
    chars: &mut std::iter::Peekable<I>,
    path: &str,
) -> Result<String, ConversionError>
where
    I: Iterator<Item = char>,
{
    let mut key = String::new();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => return Ok(key),
            '\\' => {
                let Some(escaped) = chars.next() else {
                    return Err(ConversionError::InvalidJson(format!(
                        "unterminated escape in JSON path: {path}"
                    )));
                };
                key.push(escaped);
            }
            other => key.push(other),
        }
    }
    Err(ConversionError::InvalidJson(format!(
        "unterminated quoted JSON path key: {path}"
    )))
}

const JSONB_SMALL_OBJECT: u8 = 0x00;
const JSONB_LARGE_OBJECT: u8 = 0x01;
const JSONB_SMALL_ARRAY: u8 = 0x02;
const JSONB_LARGE_ARRAY: u8 = 0x03;
const JSONB_LITERAL: u8 = 0x04;
const JSONB_INT16: u8 = 0x05;
const JSONB_UINT16: u8 = 0x06;
const JSONB_INT32: u8 = 0x07;
const JSONB_UINT32: u8 = 0x08;
const JSONB_INT64: u8 = 0x09;
const JSONB_UINT64: u8 = 0x0a;
const JSONB_DOUBLE: u8 = 0x0b;
const JSONB_UTF8: u8 = 0x0c;

const JSON_LITERAL_NULL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

fn parse_jsonb_value(input: &mut &[u8]) -> Result<serde_json::Value, String> {
    let kind = read_u8(input)?;
    parse_jsonb_value_typed(input, kind)
}

fn parse_jsonb_value_typed(input: &mut &[u8], kind: u8) -> Result<serde_json::Value, String> {
    match kind {
        JSONB_SMALL_OBJECT | JSONB_LARGE_OBJECT => parse_jsonb_object(input, kind),
        JSONB_SMALL_ARRAY | JSONB_LARGE_ARRAY => parse_jsonb_array(input, kind),
        JSONB_LITERAL => parse_jsonb_literal(input),
        JSONB_INT16 => Ok(serde_json::Value::Number(read_i16_le(input)?.into())),
        JSONB_UINT16 => Ok(serde_json::Value::Number(read_u16_le(input)?.into())),
        JSONB_INT32 => Ok(serde_json::Value::Number(read_i32_le(input)?.into())),
        JSONB_UINT32 => Ok(serde_json::Value::Number(read_u32_le(input)?.into())),
        JSONB_INT64 => Ok(serde_json::Value::Number(read_i64_le(input)?.into())),
        JSONB_UINT64 => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(read_u64_le(input)? as f64)
                .ok_or_else(|| "uint64 out of JSON range".to_string())?,
        )),
        JSONB_DOUBLE => {
            let v = read_f64_le(input)?;
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .ok_or_else(|| format!("invalid JSON double {v}"))
        }
        JSONB_UTF8 => Ok(serde_json::Value::String(read_jsonb_string(input)?)),
        kind => Err(format!("unknown MySQL JSON binary type 0x{kind:02x}")),
    }
}

fn jsonb_offset_size(is_small: bool) -> usize {
    if is_small {
        2
    } else {
        4
    }
}

fn jsonb_key_entry_size(is_small: bool) -> usize {
    2 + jsonb_offset_size(is_small)
}

fn jsonb_value_entry_size(is_small: bool) -> usize {
    1 + jsonb_offset_size(is_small)
}

fn is_inline_json_value(kind: u8, is_small: bool) -> bool {
    matches!(kind, JSONB_INT16 | JSONB_UINT16 | JSONB_LITERAL)
        || (!is_small && matches!(kind, JSONB_INT32 | JSONB_UINT32))
}

fn read_json_count_at(data: &[u8], offset: usize, is_small: bool) -> Result<usize, String> {
    if is_small {
        if data.len() < offset + 2 {
            return Err("unexpected EOF".into());
        }
        Ok(u16::from_le_bytes([data[offset], data[offset + 1]]) as usize)
    } else {
        if data.len() < offset + 4 {
            Err("unexpected EOF".into())
        } else {
            Ok(u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize)
        }
    }
}

fn read_u16_at(data: &[u8], offset: usize) -> Result<u16, String> {
    if data.len() < offset + 2 {
        return Err("unexpected EOF".into());
    }
    Ok(u16::from_le_bytes([data[offset], data[offset + 1]]))
}

fn parse_jsonb_object(input: &mut &[u8], kind: u8) -> Result<serde_json::Value, String> {
    let is_small = kind == JSONB_SMALL_OBJECT;
    let offset_size = jsonb_offset_size(is_small);
    if input.len() < offset_size * 2 {
        return Err("unexpected EOF".into());
    }
    let count = read_json_count_at(input, 0, is_small)?;
    let size = read_json_count_at(input, offset_size, is_small)?;
    if input.len() < size {
        return Err("unexpected EOF".into());
    }
    let body = &input[..size];
    *input = &input[size..];

    let key_entry_size = jsonb_key_entry_size(is_small);
    let value_entry_size = jsonb_value_entry_size(is_small);
    let header_size = offset_size * 2 + count * value_entry_size + count * key_entry_size;
    if header_size > size {
        return Err(format!(
            "JSON object header size {header_size} exceeds body size {size}"
        ));
    }

    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let entry_offset = offset_size * 2 + key_entry_size * i;
        let key_offset = read_json_count_at(body, entry_offset, is_small)?;
        let key_length = read_u16_at(body, entry_offset + offset_size)? as usize;
        if key_offset < header_size || key_offset + key_length > size {
            return Err("invalid JSON object key offset".into());
        }
        keys.push(
            String::from_utf8(body[key_offset..key_offset + key_length].to_vec())
                .map_err(|e| e.to_string())?,
        );
    }

    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let entry_offset = offset_size * 2 + value_entry_size * i + key_entry_size * count;
        let value_type = body[entry_offset];
        if is_inline_json_value(value_type, is_small) {
            let mut slot = &body[entry_offset + 1..entry_offset + value_entry_size];
            values.push(parse_jsonb_value_typed(&mut slot, value_type)?);
        } else {
            let value_offset = read_json_count_at(body, entry_offset + 1, is_small)?;
            if value_offset >= size {
                return Err("invalid JSON object value offset".into());
            }
            let mut value_input = &body[value_offset..size];
            values.push(parse_jsonb_value_typed(&mut value_input, value_type)?);
        }
    }

    let mut map = serde_json::Map::new();
    for (key, value) in keys.into_iter().zip(values) {
        map.insert(key, value);
    }
    Ok(serde_json::Value::Object(map))
}

fn parse_jsonb_array(input: &mut &[u8], kind: u8) -> Result<serde_json::Value, String> {
    let is_small = kind == JSONB_SMALL_ARRAY;
    let offset_size = jsonb_offset_size(is_small);
    if input.len() < offset_size * 2 {
        return Err("unexpected EOF".into());
    }
    let count = read_json_count_at(input, 0, is_small)?;
    let size = read_json_count_at(input, offset_size, is_small)?;
    if input.len() < size {
        return Err("unexpected EOF".into());
    }
    let body = &input[..size];
    *input = &input[size..];

    let value_entry_size = jsonb_value_entry_size(is_small);
    let header_size = offset_size * 2 + count * value_entry_size;
    if header_size > size {
        return Err(format!(
            "JSON array header size {header_size} exceeds body size {size}"
        ));
    }

    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let entry_offset = offset_size * 2 + value_entry_size * i;
        let value_type = body[entry_offset];
        if is_inline_json_value(value_type, is_small) {
            let mut slot = &body[entry_offset + 1..entry_offset + value_entry_size];
            values.push(parse_jsonb_value_typed(&mut slot, value_type)?);
        } else {
            let value_offset = read_json_count_at(body, entry_offset + 1, is_small)?;
            if value_offset >= size {
                return Err("invalid JSON array value offset".into());
            }
            let mut value_input = &body[value_offset..size];
            values.push(parse_jsonb_value_typed(&mut value_input, value_type)?);
        }
    }
    Ok(serde_json::Value::Array(values))
}

fn parse_jsonb_literal(input: &mut &[u8]) -> Result<serde_json::Value, String> {
    match read_u8(input)? {
        JSON_LITERAL_NULL => Ok(serde_json::Value::Null),
        JSON_LITERAL_TRUE => Ok(serde_json::Value::Bool(true)),
        JSON_LITERAL_FALSE => Ok(serde_json::Value::Bool(false)),
        other => Err(format!("unknown MySQL JSON literal type 0x{other:02x}")),
    }
}

fn read_jsonb_string(input: &mut &[u8]) -> Result<String, String> {
    let len = read_varint(input)? as usize;
    let bytes = read_bytes(input, len)?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

fn read_varint(input: &mut &[u8]) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(input)?;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 63 {
            return Err("JSON varint overflow".into());
        }
    }
}

fn read_u8(input: &mut &[u8]) -> Result<u8, String> {
    if input.is_empty() {
        return Err("unexpected EOF".into());
    }
    let v = input[0];
    *input = &input[1..];
    Ok(v)
}

fn read_bytes(input: &mut &[u8], len: usize) -> Result<Vec<u8>, String> {
    if input.len() < len {
        return Err("unexpected EOF".into());
    }
    let out = input[..len].to_vec();
    *input = &input[len..];
    Ok(out)
}

fn read_i16_le(input: &mut &[u8]) -> Result<i16, String> {
    let b = read_bytes(input, 2)?;
    Ok(i16::from_le_bytes([b[0], b[1]]))
}

fn read_u16_le(input: &mut &[u8]) -> Result<u16, String> {
    let b = read_bytes(input, 2)?;
    Ok(u16::from_le_bytes([b[0], b[1]]))
}

fn read_i32_le(input: &mut &[u8]) -> Result<i32, String> {
    let b = read_bytes(input, 4)?;
    Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u32_le(input: &mut &[u8]) -> Result<u32, String> {
    let b = read_bytes(input, 4)?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_i64_le(input: &mut &[u8]) -> Result<i64, String> {
    let b = read_bytes(input, 8)?;
    Ok(i64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_u64_le(input: &mut &[u8]) -> Result<u64, String> {
    let b = read_bytes(input, 8)?;
    Ok(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_f64_le(input: &mut &[u8]) -> Result<f64, String> {
    let b = read_bytes(input, 8)?;
    Ok(f64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn extract_signed_int(cell: &CellValue) -> Result<i64, ConversionError> {
    match cell {
        CellValue::Int(i) => Ok(*i),
        CellValue::UInt(u) => Ok(*u as i64),
        CellValue::String(s) => s.parse().map_err(|_| type_mismatch("integer", cell)),
        other => Err(type_mismatch("integer", other)),
    }
}

fn extract_float(cell: &CellValue) -> Result<f64, ConversionError> {
    match cell {
        CellValue::Float(f) => Ok(f64::from(*f)),
        CellValue::Double(d) => Ok(*d),
        CellValue::Int(i) => Ok(*i as f64),
        CellValue::UInt(u) => Ok(*u as f64),
        CellValue::String(s) => s.parse().map_err(|_| type_mismatch("float", cell)),
        other => Err(type_mismatch("float", other)),
    }
}

fn extract_string(cell: &CellValue) -> Result<String, ConversionError> {
    match cell {
        CellValue::String(s) => Ok(s.clone()),
        CellValue::JsonText(s) => Ok(s.clone()),
        CellValue::Int(i) => Ok(i.to_string()),
        CellValue::UInt(u) => Ok(u.to_string()),
        CellValue::Float(f) => Ok(f.to_string()),
        CellValue::Double(d) => Ok(d.to_string()),
        CellValue::Decimal(d) => Ok(d.to_string()),
        other => Err(type_mismatch("string", other)),
    }
}

fn extract_bytes(cell: &CellValue) -> Result<Vec<u8>, ConversionError> {
    match cell {
        CellValue::Bytes(b) | CellValue::Bit(b) => Ok(b.clone()),
        CellValue::String(s) => Ok(s.as_bytes().to_vec()),
        other => Err(type_mismatch("bytes", other)),
    }
}

fn extract_date_parts(cell: &CellValue) -> Result<(u16, u8, u8), ConversionError> {
    match cell {
        CellValue::Date { year, month, day } => Ok((*year, *month, *day)),
        other => Err(type_mismatch("date", other)),
    }
}

fn extract_time_parts(cell: &CellValue) -> Result<(i32, u8, u8, u32), ConversionError> {
    match cell {
        CellValue::Time {
            hour,
            minute,
            second,
            micros,
        } => Ok((*hour, *minute, *second, *micros)),
        other => Err(type_mismatch("time", other)),
    }
}

fn extract_datetime(cell: &CellValue) -> Result<chrono::DateTime<Utc>, ConversionError> {
    match cell {
        CellValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => {
            let date = NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                .ok_or_else(|| {
                    ConversionError::InvalidDateTime(format!(
                        "invalid date components: year={year}, month={month}, day={day}"
                    ))
                })?;
            let time = NaiveTime::from_hms_micro_opt(
                *hour as u32,
                u32::from(*minute),
                u32::from(*second),
                *micros,
            )
                .ok_or_else(|| {
                    ConversionError::InvalidDateTime(format!(
                        "invalid time components: hour={hour}, minute={minute}, second={second}, micro={micros}"
                    ))
                })?;
            Ok(Utc.from_utc_datetime(&chrono::NaiveDateTime::new(date, time)))
        }
        other => Err(type_mismatch("datetime", other)),
    }
}

fn extract_timestamp(cell: &CellValue) -> Result<chrono::DateTime<Utc>, ConversionError> {
    match cell {
        CellValue::TimestampMillis(ms) => {
            let secs = (*ms / 1000) as i64;
            let nanos = ((*ms % 1000) * 1_000_000) as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| ConversionError::InvalidDateTime(format!("invalid timestamp {ms}")))
        }
        other => Err(type_mismatch("timestamp", other)),
    }
}

fn type_mismatch(expected: &str, actual: &CellValue) -> ConversionError {
    ConversionError::TypeMismatch {
        expected: expected.to_string(),
        actual: mysql_async::Value::Bytes(format!("{actual:?}").into_bytes()),
    }
}

fn is_uuid_format(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let chars: Vec<char> = s.chars().collect();
    chars[8] == '-' && chars[13] == '-' && chars[18] == '-' && chars[23] == '-'
}

#[cfg(test)]
mod tests {
    use super::*;
    use binlog_protocol::{JsonDiff, JsonDiffOperation};
    use chrono::Datelike;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use sync_core::UniversalValue;

    fn col(name: &str, column_type: u8, metadata: ColumnMetadata) -> BinlogColumnMeta {
        BinlogColumnMeta::new(name, column_type, metadata)
    }

    fn convert(cell: &CellValue, column: &BinlogColumnMeta) -> UniversalValue {
        binlog_cell_to_universal_value(cell, column, &RowConversionConfig::default()).unwrap()
    }

    #[test]
    fn applies_mysql_partial_json_replace_diff() {
        let cell = apply_mysql_json_diffs_to_cell(
            &CellValue::JsonText(r#"{"a":false,"b":[1,2]}"#.into()),
            &[JsonDiff {
                operation: JsonDiffOperation::Replace,
                path: "$.a".into(),
                data: Some(vec![0x04, 0x01]),
            }],
        )
        .expect("apply diff");

        assert_eq!(cell, CellValue::JsonText(r#"{"a":true,"b":[1,2]}"#.into()));
    }

    #[test]
    fn test_int_conversion() {
        let column = col("n", MYSQL_TYPE_LONG, ColumnMetadata::None);
        assert!(matches!(
            convert(&CellValue::Int(42), &column),
            UniversalValue::Int32(42)
        ));
    }

    #[test]
    fn test_bigint_conversion() {
        let column = col("n", MYSQL_TYPE_LONGLONG, ColumnMetadata::None);
        assert!(matches!(
            convert(&CellValue::Int(9_223_372_036_854_775_807), &column),
            UniversalValue::Int64(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_string_conversion() {
        let column = col(
            "s",
            MYSQL_TYPE_VAR_STRING,
            ColumnMetadata::String { max_length: 255 },
        );
        let value = convert(&CellValue::String("hello world".into()), &column);
        if let UniversalValue::VarChar { value, .. } = value {
            assert_eq!(value, "hello world");
        } else {
            panic!("expected VarChar");
        }
    }

    #[test]
    fn test_uuid_detection() {
        let column = col(
            "id",
            MYSQL_TYPE_VAR_STRING,
            ColumnMetadata::String { max_length: 36 },
        );
        let value = convert(
            &CellValue::String("550e8400-e29b-41d4-a716-446655440000".into()),
            &column,
        );
        if let UniversalValue::Uuid(u) = value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let column = col("ts", MYSQL_TYPE_DATETIME, ColumnMetadata::None);
        let value = convert(
            &CellValue::DateTime {
                year: 2024,
                month: 6,
                day: 15,
                hour: 10,
                minute: 30,
                second: 45,
                micros: 0,
            },
            &column,
        );
        if let UniversalValue::LocalDateTime(dt) = value {
            assert_eq!(dt.year(), 2024);
            assert_eq!(dt.month(), 6);
            assert_eq!(dt.day(), 15);
        } else {
            panic!("expected LocalDateTime");
        }
    }

    #[test]
    fn test_null_conversion() {
        let column = col("n", MYSQL_TYPE_LONG, ColumnMetadata::None);
        assert!(matches!(
            convert(&CellValue::Null, &column),
            UniversalValue::Null
        ));
    }

    #[test]
    fn test_json_text_conversion() {
        let column = col(
            "j",
            MYSQL_TYPE_JSON,
            ColumnMetadata::Blob { length_bytes: 2 },
        );
        let value = convert(
            &CellValue::JsonText(r#"{"name":"Alice","age":30}"#.into()),
            &column,
        );
        if let UniversalValue::Json(obj) = value {
            assert!(obj.get("name").is_some());
            assert!(obj.get("age").is_some());
        } else {
            panic!("expected Json");
        }
    }

    #[test]
    fn test_mysql_json_binary_true_literal() {
        let column = col(
            "j",
            MYSQL_TYPE_JSON,
            ColumnMetadata::Blob { length_bytes: 1 },
        );
        let value = convert(&CellValue::JsonBytes(vec![0x04, 0x01]), &column);
        assert_eq!(
            value,
            UniversalValue::Json(Box::new(serde_json::Value::Bool(true)))
        );
    }

    #[test]
    fn test_mysql_json_binary_nested_object_from_binlog() {
        let bytes = hex::decode(
            "0001002b000b00080000130073657474696e6773010018000b000d000501006e6f74696669636174696f6e73",
        )
        .expect("valid hex");
        let value = parse_mysql_json_binary(&bytes).expect("parse nested json");
        let settings = value
            .get("settings")
            .and_then(|v| v.as_object())
            .expect("settings object");
        assert_eq!(
            settings.get("notifications").and_then(|v| v.as_i64()),
            Some(1)
        );
    }

    #[test]
    fn test_decimal_conversion() {
        let column = col(
            "d",
            MYSQL_TYPE_NEWDECIMAL,
            ColumnMetadata::Numeric {
                precision: 10,
                scale: 3,
            },
        );
        let value = convert(
            &CellValue::Decimal(Decimal::from_str("123.456").unwrap()),
            &column,
        );
        if let UniversalValue::Decimal { value, .. } = value {
            assert_eq!(value, "123.456");
        } else {
            panic!("expected Decimal");
        }
    }

    #[test]
    fn test_blob_conversion() {
        let column = col(
            "b",
            MYSQL_TYPE_BLOB,
            ColumnMetadata::Blob { length_bytes: 2 },
        )
        .with_binary(true);
        let data = vec![0x00, 0x01, 0x02, 0xFF];
        if let UniversalValue::Blob(b) = convert(&CellValue::Bytes(data.clone()), &column) {
            assert_eq!(b, data);
        } else {
            panic!("expected Blob");
        }
    }

    #[test]
    fn test_text_conversion() {
        let column = col(
            "t",
            MYSQL_TYPE_BLOB,
            ColumnMetadata::Blob { length_bytes: 2 },
        );
        if let UniversalValue::Text(s) =
            convert(&CellValue::String("long text content".into()), &column)
        {
            assert_eq!(s, "long text content");
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn test_tinyint1_boolean_true() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Int(1), &column),
            UniversalValue::Bool(true)
        ));
    }

    #[test]
    fn test_tinyint1_boolean_false() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Int(0), &column),
            UniversalValue::Bool(false)
        ));
    }

    #[test]
    fn test_tinyint_without_boolean_hint() {
        let column = col("n", MYSQL_TYPE_TINY, ColumnMetadata::None);
        assert!(matches!(
            convert(&CellValue::Int(1), &column),
            UniversalValue::Int8 { value: 1, .. }
        ));
    }

    #[test]
    fn test_tinyint_with_boolean_hint_true() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Int(1), &column),
            UniversalValue::Bool(true)
        ));
    }

    #[test]
    fn test_tinyint_with_boolean_hint_false() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Int(0), &column),
            UniversalValue::Bool(false)
        ));
    }

    #[test]
    fn test_tinyint_non_boolean_value() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Int(5), &column),
            UniversalValue::Int8 { value: 5, .. }
        ));
    }

    #[test]
    fn test_null_boolean_column() {
        let column = col("flag", MYSQL_TYPE_TINY, ColumnMetadata::None).with_boolean_hint(true);
        assert!(matches!(
            convert(&CellValue::Null, &column),
            UniversalValue::Null
        ));
    }

    #[test]
    fn test_json_boolean_path_conversion() {
        let config = RowConversionConfig {
            json_config: Some(
                JsonConversionConfig::new()
                    .with_boolean_path("settings.enabled")
                    .with_boolean_path("flags.is_active"),
            ),
            ..Default::default()
        };
        let column = col(
            "j",
            MYSQL_TYPE_JSON,
            ColumnMetadata::Blob { length_bytes: 2 },
        );
        let value = binlog_cell_to_universal_value(
            &CellValue::JsonText(
                r#"{"settings":{"enabled":1,"count":5},"flags":{"is_active":0}}"#.into(),
            ),
            &column,
            &config,
        )
        .unwrap();
        if let UniversalValue::Json(root) = value {
            let settings = root.get("settings").unwrap();
            assert_eq!(
                settings.get("enabled"),
                Some(&serde_json::Value::Bool(true))
            );
            let flags = root.get("flags").unwrap();
            assert_eq!(
                flags.get("is_active"),
                Some(&serde_json::Value::Bool(false))
            );
        } else {
            panic!("expected Json");
        }
    }

    #[test]
    fn test_json_set_path_conversion() {
        let config = RowConversionConfig {
            json_config: Some(JsonConversionConfig::new().with_set_path("permissions")),
            ..Default::default()
        };
        let column = col(
            "j",
            MYSQL_TYPE_JSON,
            ColumnMetadata::Blob { length_bytes: 2 },
        );
        let value = binlog_cell_to_universal_value(
            &CellValue::JsonText(r#"{"permissions":"read,write,execute","name":"admin"}"#.into()),
            &column,
            &config,
        )
        .unwrap();
        if let UniversalValue::Json(root) = value {
            let perms = root.get("permissions").unwrap().as_array().unwrap();
            assert_eq!(perms.len(), 3);
        } else {
            panic!("expected Json");
        }
    }

    #[test]
    fn test_json_empty_config() {
        let column = col(
            "j",
            MYSQL_TYPE_JSON,
            ColumnMetadata::Blob { length_bytes: 2 },
        );
        if let UniversalValue::Json(root) = convert(
            &CellValue::JsonText(r#"{"enabled":1,"disabled":0}"#.into()),
            &column,
        ) {
            assert_eq!(root.get("enabled").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(root.get("disabled").and_then(|v| v.as_i64()), Some(0));
        } else {
            panic!("expected Json");
        }
    }

    #[test]
    fn test_unsupported_type_error() {
        let column = col("x", MYSQL_TYPE_NULL, ColumnMetadata::None);
        let err = binlog_cell_to_universal_value(
            &CellValue::Int(42),
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::UnsupportedType(_)));
    }

    #[test]
    fn test_type_mismatch_int_expects_int() {
        let column = col("n", MYSQL_TYPE_LONG, ColumnMetadata::None);
        let err = binlog_cell_to_universal_value(
            &CellValue::String("not a number".into()),
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
    }

    #[test]
    fn test_type_mismatch_float_expects_float() {
        let column = col("n", MYSQL_TYPE_DOUBLE, ColumnMetadata::None);
        let err = binlog_cell_to_universal_value(
            &CellValue::String("not_a_float".into()),
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
    }

    #[test]
    fn test_invalid_date_error() {
        let column = col("d", MYSQL_TYPE_DATE, ColumnMetadata::None);
        let err = binlog_cell_to_universal_value(
            &CellValue::Date {
                year: 2024,
                month: 13,
                day: 1,
            },
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::InvalidDateTime(_)));
    }

    #[test]
    fn test_bytes_type_mismatch() {
        let column = col(
            "b",
            MYSQL_TYPE_BLOB,
            ColumnMetadata::Blob { length_bytes: 2 },
        )
        .with_binary(true);
        let err = binlog_cell_to_universal_value(
            &CellValue::Int(42),
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
    }

    #[test]
    fn test_date_type_mismatch() {
        let column = col("d", MYSQL_TYPE_DATE, ColumnMetadata::None);
        let err = binlog_cell_to_universal_value(
            &CellValue::Time {
                hour: 10,
                minute: 30,
                second: 0,
                micros: 0,
            },
            &column,
            &RowConversionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
    }

    #[test]
    fn test_set_column_empty_string() {
        let config = RowConversionConfig {
            set_columns: vec!["tags".into()],
            ..Default::default()
        };
        let column = col(
            "tags",
            MYSQL_TYPE_SET,
            ColumnMetadata::EnumSet { max_length: 0 },
        );
        if let UniversalValue::Set { elements, .. } =
            binlog_cell_to_universal_value(&CellValue::String(String::new()), &column, &config)
                .unwrap()
        {
            assert!(elements.is_empty());
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn test_set_column_multiple_values() {
        let column = col(
            "tags",
            MYSQL_TYPE_SET,
            ColumnMetadata::EnumSet { max_length: 0 },
        );
        if let UniversalValue::Set { elements, .. } =
            convert(&CellValue::String("read,write,execute".into()), &column)
        {
            assert_eq!(elements, vec!["read", "write", "execute"]);
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn test_enum_column() {
        let column = col(
            "status",
            MYSQL_TYPE_ENUM,
            ColumnMetadata::EnumSet { max_length: 0 },
        );
        if let UniversalValue::Enum { value, .. } =
            convert(&CellValue::String("active".into()), &column)
        {
            assert_eq!(value, "active");
        } else {
            panic!("expected Enum");
        }
    }

    #[test]
    fn test_bit_as_boolean() {
        let column = col(
            "flag",
            MYSQL_TYPE_BIT,
            ColumnMetadata::Bit { length_bits: 1 },
        );
        assert!(matches!(
            convert(&CellValue::Bit(vec![0]), &column),
            UniversalValue::Bool(false)
        ));
        assert!(matches!(
            convert(&CellValue::Bit(vec![1]), &column),
            UniversalValue::Bool(true)
        ));
    }

    #[test]
    fn test_bit_as_bytes() {
        let column = col(
            "bits",
            MYSQL_TYPE_BIT,
            ColumnMetadata::Bit { length_bits: 8 },
        );
        if let UniversalValue::Bytes(b) = convert(&CellValue::Bit(vec![0xff]), &column) {
            assert_eq!(b, vec![0xff]);
        } else {
            panic!("expected Bytes");
        }
    }

    #[test]
    fn test_geometry_column() {
        use base64::Engine;
        use sync_core::values::GeometryData;
        let wkb_point = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        let column = col(
            "geo",
            MYSQL_TYPE_GEOMETRY,
            ColumnMetadata::Blob { length_bytes: 4 },
        );
        if let UniversalValue::Geometry { data, .. } =
            convert(&CellValue::Bytes(wkb_point.clone()), &column)
        {
            let GeometryData(json) = data;
            let expected_b64 = base64::engine::general_purpose::STANDARD.encode(&wkb_point);
            assert_eq!(json["type"], "Point");
            assert_eq!(json["wkb_base64"], expected_b64);
        } else {
            panic!("expected Geometry");
        }
    }

    #[test]
    fn test_year_column() {
        let column = col("y", MYSQL_TYPE_YEAR, ColumnMetadata::None);
        assert!(matches!(
            convert(&CellValue::Year(2024), &column),
            UniversalValue::Int16(2024)
        ));
    }
}
