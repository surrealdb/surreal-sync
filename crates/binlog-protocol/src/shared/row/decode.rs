use crate::error::Error;
use crate::flavor::Flavor;
use crate::shared::buf::{
    read_bitmap, read_bytes, read_f32_le, read_f64_le, read_i16_le, read_i24_le, read_i32_le,
    read_i64_le, read_i8, read_u16_le, read_u24_le, read_u32_le, read_u64_le, read_u8,
    read_uint_be,
};
use crate::shared::column_type::*;
use crate::types::{CellValue, ColumnDef, ColumnMetadata};
use rust_decimal::Decimal;

pub fn decode_cell(
    column: &ColumnDef,
    flavor: Flavor,
    payload: &mut &[u8],
) -> Result<CellValue, Error> {
    let column_type = resolve_string_type(column.column_type, &column.metadata);
    match column_type {
        MYSQL_TYPE_TINY => {
            if column.unsigned {
                Ok(CellValue::UInt(u64::from(read_u8(payload)?)))
            } else {
                Ok(CellValue::Int(i64::from(read_i8(payload)?)))
            }
        }
        MYSQL_TYPE_SHORT => {
            if column.unsigned {
                Ok(CellValue::UInt(u64::from(read_u16_le(payload)?)))
            } else {
                Ok(CellValue::Int(i64::from(read_i16_le(payload)?)))
            }
        }
        MYSQL_TYPE_INT24 => {
            if column.unsigned {
                Ok(CellValue::UInt(u64::from(read_u24_le(payload)?)))
            } else {
                Ok(CellValue::Int(i64::from(read_i24_le(payload)?)))
            }
        }
        MYSQL_TYPE_LONG => {
            if column.unsigned {
                Ok(CellValue::UInt(u64::from(read_u32_le(payload)?)))
            } else {
                Ok(CellValue::Int(i64::from(read_i32_le(payload)?)))
            }
        }
        MYSQL_TYPE_LONGLONG => {
            if column.unsigned {
                Ok(CellValue::UInt(read_u64_le(payload)?))
            } else {
                Ok(CellValue::Int(read_i64_le(payload)?))
            }
        }
        MYSQL_TYPE_FLOAT => {
            let pack_length = float_pack_length(&column.metadata);
            if pack_length == 8 {
                Ok(CellValue::Float(read_f64_le(payload)? as f32))
            } else {
                Ok(CellValue::Float(read_f32_le(payload)?))
            }
        }
        MYSQL_TYPE_DOUBLE => {
            let pack_length = float_pack_length(&column.metadata);
            if pack_length == 4 {
                Ok(CellValue::Double(f64::from(read_f32_le(payload)?)))
            } else {
                Ok(CellValue::Double(read_f64_le(payload)?))
            }
        }
        MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL => decode_decimal(payload, &column.metadata),
        MYSQL_TYPE_YEAR => Ok(CellValue::Year(1900 + read_u8(payload)? as u16)),
        MYSQL_TYPE_DATE => decode_date(payload),
        MYSQL_TYPE_TIME => decode_time_v1(payload),
        MYSQL_TYPE_TIME2 => decode_time2(payload, fsp(&column.metadata)),
        MYSQL_TYPE_DATETIME => decode_datetime_v1(payload),
        MYSQL_TYPE_DATETIME2 => decode_datetime2(payload, fsp(&column.metadata)),
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => {
            decode_timestamp(payload, column_type, fsp(&column.metadata))
        }
        MYSQL_TYPE_BIT => decode_bit(payload, &column.metadata),
        MYSQL_TYPE_JSON if flavor == Flavor::MySql => Ok(CellValue::JsonBytes(read_blob(
            payload,
            blob_len(&column.metadata),
        )?)),
        MYSQL_TYPE_JSON if flavor == Flavor::MariaDb => {
            let bytes = read_blob(payload, blob_len(&column.metadata))?;
            Ok(CellValue::JsonText(
                String::from_utf8_lossy(&bytes).into_owned(),
            ))
        }
        MYSQL_TYPE_ENUM => {
            let pack_len = enum_set_pack_len(&column.metadata);
            let idx = match pack_len {
                1 => u64::from(read_u8(payload)?),
                2 => u64::from(read_u16_le(payload)?),
                n => {
                    return Err(Error::CellDecode(format!(
                        "unsupported ENUM pack length {n}"
                    )))
                }
            };
            Ok(CellValue::String(idx.to_string()))
        }
        MYSQL_TYPE_SET => {
            let pack_len = enum_set_pack_len(&column.metadata);
            let v = match pack_len {
                1 => u64::from(read_u8(payload)?),
                2 => u64::from(read_u16_le(payload)?),
                4 => read_u32_le(payload)? as u64,
                8 => read_u64_le(payload)?,
                n => {
                    return Err(Error::CellDecode(format!(
                        "unsupported SET pack length {n}"
                    )))
                }
            };
            Ok(CellValue::String(v.to_string()))
        }
        MYSQL_TYPE_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            decode_string(payload, &column.metadata, column.column_type)
        }
        MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_GEOMETRY => {
            let bytes = read_blob(payload, blob_len(&column.metadata))?;
            if column_type == MYSQL_TYPE_GEOMETRY {
                Ok(CellValue::Bytes(bytes))
            } else {
                Ok(CellValue::String(
                    String::from_utf8_lossy(&bytes).into_owned(),
                ))
            }
        }
        other => Err(Error::CellDecode(format!(
            "unsupported column type {other}"
        ))),
    }
}

fn float_pack_length(metadata: &ColumnMetadata) -> u8 {
    match metadata {
        ColumnMetadata::FloatPackLength { bytes } => *bytes,
        _ => 8,
    }
}

fn fsp(metadata: &ColumnMetadata) -> u8 {
    match metadata {
        ColumnMetadata::Temporal { fsp } => *fsp,
        _ => 0,
    }
}

fn blob_len(metadata: &ColumnMetadata) -> u8 {
    match metadata {
        ColumnMetadata::Blob { length_bytes } => *length_bytes,
        _ => 2,
    }
}

fn enum_set_pack_len(metadata: &ColumnMetadata) -> usize {
    match metadata {
        ColumnMetadata::EnumSet { max_length } | ColumnMetadata::String { max_length } => {
            let len = (max_length & 0xFF) as usize;
            if len == 0 {
                2
            } else {
                len
            }
        }
        _ => 2,
    }
}

fn resolve_string_type(column_type: u8, metadata: &ColumnMetadata) -> u8 {
    if column_type == MYSQL_TYPE_STRING {
        if let ColumnMetadata::EnumSet { max_length } = metadata {
            let real = (*max_length >> 8) as u8;
            if real == MYSQL_TYPE_ENUM_REAL || real == MYSQL_TYPE_SET_REAL {
                return real;
            }
        }
    }
    column_type
}

fn decode_string(
    payload: &mut &[u8],
    metadata: &ColumnMetadata,
    column_type: u8,
) -> Result<CellValue, Error> {
    let max_len = match metadata {
        ColumnMetadata::String { max_length } | ColumnMetadata::EnumSet { max_length } => {
            *max_length
        }
        _ => 256,
    };
    // Fixed-width BINARY/VARBINARY fields may omit the length prefix.
    if matches!(column_type, MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING)
        && max_len > 0
        && payload.len() == max_len as usize
    {
        let bytes = read_bytes(payload, max_len as usize)?;
        return Ok(CellValue::Bytes(bytes));
    }
    let len = if max_len < 256 {
        read_u8(payload)? as usize
    } else {
        read_u16_le(payload)? as usize
    };
    let bytes = read_bytes(payload, len)?;
    Ok(CellValue::String(
        String::from_utf8_lossy(&bytes).into_owned(),
    ))
}

fn read_blob_length(payload: &mut &[u8], length_bytes: u8) -> Result<usize, Error> {
    match length_bytes {
        1 => Ok(read_u8(payload)? as usize),
        2 => Ok(read_u16_le(payload)? as usize),
        3 => Ok(read_u24_le(payload)? as usize),
        4 => Ok(read_u32_le(payload)? as usize),
        n => Err(Error::Protocol(format!("invalid blob length metadata {n}"))),
    }
}

fn read_blob(payload: &mut &[u8], length_bytes: u8) -> Result<Vec<u8>, Error> {
    let len = read_blob_length(payload, length_bytes)?;
    read_bytes(payload, len).map_err(|e| {
        Error::Protocol(format!(
            "blob read len={len} length_bytes={length_bytes} have={}: {e}",
            payload.len()
        ))
    })
}

fn decode_bit(payload: &mut &[u8], metadata: &ColumnMetadata) -> Result<CellValue, Error> {
    let nbits = match metadata {
        ColumnMetadata::Bit { length_bits } => *length_bits as usize,
        _ => 1,
    };
    let mut bitmap = read_bitmap(payload, nbits)?;
    bitmap.reverse();
    Ok(CellValue::Bit(bitmap))
}

fn decode_date(payload: &mut &[u8]) -> Result<CellValue, Error> {
    let value = read_u24_le(payload)?;
    let day = value % (1 << 5);
    let month = (value >> 5) % (1 << 4);
    let year = value >> 9;
    Ok(CellValue::Date {
        year: year as u16,
        month: month as u8,
        day: day as u8,
    })
}

fn decode_time_v1(payload: &mut &[u8]) -> Result<CellValue, Error> {
    let mut value = read_i24_le(payload)?;
    if value < 0 {
        return Err(Error::CellDecode("negative TIME not supported".into()));
    }
    let second = value % 100;
    value /= 100;
    let minute = value % 100;
    value /= 100;
    let hour = value;
    Ok(CellValue::Time {
        hour,
        minute: minute as u8,
        second: second as u8,
        micros: 0,
    })
}

fn decode_time2(payload: &mut &[u8], fsp: u8) -> Result<CellValue, Error> {
    const TIMEF_INT_OFS: i32 = 0x800000;
    let raw = read_u24_le(payload)? as i32;
    let micros = read_fraction(payload, fsp)? / 1000;
    let mut hms = raw - TIMEF_INT_OFS;
    let negative = hms < 0;
    if negative {
        hms = -hms;
    }
    let hour = (hms >> 12) % (1 << 10);
    let minute = (hms >> 6) % (1 << 6);
    let second = hms % (1 << 6);
    Ok(CellValue::Time {
        hour: if negative { -hour } else { hour },
        minute: minute as u8,
        second: second as u8,
        micros,
    })
}

fn decode_datetime_v1(payload: &mut &[u8]) -> Result<CellValue, Error> {
    let mut value = read_u64_le(payload)?;
    let second = value % 100;
    value /= 100;
    let minute = value % 100;
    value /= 100;
    let hour = value % 100;
    value /= 100;
    let day = value % 100;
    value /= 100;
    let month = value % 100;
    value /= 100;
    let year = value;
    Ok(CellValue::DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        micros: 0,
    })
}

fn decode_datetime2(payload: &mut &[u8], fsp: u8) -> Result<CellValue, Error> {
    let value = read_uint_be(payload, 5)?;
    let micros = read_fraction(payload, fsp)? / 1000;
    let year_month = (value >> 22) % (1 << 17);
    let year = year_month / 13;
    let month = year_month % 13;
    let day = (value >> 17) % (1 << 5);
    let hour = (value >> 12) % (1 << 5);
    let minute = (value >> 6) % (1 << 6);
    let second = value % (1 << 6);
    Ok(CellValue::DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        micros,
    })
}

fn decode_timestamp(payload: &mut &[u8], column_type: u8, fsp: u8) -> Result<CellValue, Error> {
    let seconds = read_u32_le(payload)? as u64;
    let micros = if column_type == MYSQL_TYPE_TIMESTAMP2 {
        read_fraction(payload, fsp)? / 1000
    } else {
        0
    };
    Ok(CellValue::TimestampMillis(
        seconds * 1000 + u64::from(micros),
    ))
}

fn read_fraction(payload: &mut &[u8], fsp: u8) -> Result<u32, Error> {
    let length = fsp.div_ceil(2);
    if length == 0 {
        return Ok(0);
    }
    let fraction = read_uint_be(payload, length as usize)?;
    let scale = 100u64.pow(3 - length as u32);
    Ok((fraction * scale) as u32)
}

const DIGITS_PER_INTEGER: usize = 9;
const COMPRESSED_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

fn decimal_bin_size(precision: u8, scale: u8) -> usize {
    let integral = usize::from(precision.saturating_sub(scale));
    let decimals = usize::from(scale);
    let uncomp_integral = integral / DIGITS_PER_INTEGER;
    let uncomp_fractional = decimals / DIGITS_PER_INTEGER;
    let comp_integral = integral - uncomp_integral * DIGITS_PER_INTEGER;
    let comp_fractional = decimals - uncomp_fractional * DIGITS_PER_INTEGER;
    uncomp_integral * 4
        + COMPRESSED_BYTES[comp_integral]
        + uncomp_fractional * 4
        + COMPRESSED_BYTES[comp_fractional]
}

fn decode_decimal_decompress(comp_idx: usize, data: &[u8], mask: u8) -> (usize, u32) {
    let size = COMPRESSED_BYTES[comp_idx];
    let value = match size {
        0 => 0,
        1 => u32::from(data[0] ^ mask),
        2 => u32::from(data[1] ^ mask) | (u32::from(data[0] ^ mask) << 8),
        3 => {
            u32::from(data[2] ^ mask)
                | (u32::from(data[1] ^ mask) << 8)
                | (u32::from(data[0] ^ mask) << 16)
        }
        4 => {
            u32::from(data[3] ^ mask)
                | (u32::from(data[2] ^ mask) << 8)
                | (u32::from(data[1] ^ mask) << 16)
                | (u32::from(data[0] ^ mask) << 24)
        }
        _ => 0,
    };
    (size, value)
}

fn decode_decimal(payload: &mut &[u8], metadata: &ColumnMetadata) -> Result<CellValue, Error> {
    let (precision, scale) = match metadata {
        ColumnMetadata::Numeric { precision, scale } => (*precision, *scale),
        _ => (10, 0),
    };
    let bin_size = decimal_bin_size(precision, scale);
    if payload.len() < bin_size {
        return Err(Error::UnexpectedEof);
    }
    let mut data = payload[..bin_size].to_vec();
    *payload = &payload[bin_size..];

    let integral = usize::from(precision.saturating_sub(scale));
    let decimals = usize::from(scale);
    let uncomp_integral = integral / DIGITS_PER_INTEGER;
    let uncomp_fractional = decimals / DIGITS_PER_INTEGER;
    let comp_integral = integral - uncomp_integral * DIGITS_PER_INTEGER;
    let comp_fractional = decimals - uncomp_fractional * DIGITS_PER_INTEGER;

    let mut mask = 0u8;
    let mut negative = false;
    if data[0] & 0x80 == 0 {
        mask = 0xFF;
        negative = true;
    }
    data[0] ^= 0x80;

    let mut res = String::new();
    if negative {
        res.push('-');
    }

    let mut pos = 0usize;
    let mut zero_leading = true;

    let (size, value) = decode_decimal_decompress(comp_integral, &data, mask);
    pos += size;
    if value != 0 {
        zero_leading = false;
        res.push_str(&value.to_string());
    }

    for _ in 0..uncomp_integral {
        let word = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            ^ u32::from(mask);
        pos += 4;
        if zero_leading {
            if word != 0 {
                zero_leading = false;
                res.push_str(&word.to_string());
            }
        } else {
            res.push_str(&format!("{:09}", word));
        }
    }

    if zero_leading {
        res.push('0');
    }

    if pos < data.len() {
        res.push('.');
        for _ in 0..uncomp_fractional {
            let word = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                ^ u32::from(mask);
            pos += 4;
            res.push_str(&format!("{:09}", word));
        }
        let (size, value) = decode_decimal_decompress(comp_fractional, &data[pos..], mask);
        if size > 0 {
            let to_write = value.to_string();
            let padding = comp_fractional.saturating_sub(to_write.len());
            res.extend(std::iter::repeat_n('0', padding));
            res.push_str(&to_write);
        }
    }

    Decimal::from_str_exact(&res)
        .map(CellValue::Decimal)
        .map_err(|e| Error::CellDecode(e.to_string()))
}

mod null_bitmap {
    pub fn is_null(bitmap: &[u8], index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        bitmap
            .get(byte)
            .map(|b| (b >> bit) & 1 == 1)
            .unwrap_or(false)
    }
}

pub use null_bitmap::is_null;

fn column_present(columns_bitmap: &[u8], index: usize) -> bool {
    let byte = index / 8;
    let bit = index % 8;
    columns_bitmap
        .get(byte)
        .map(|b| (b >> bit) & 1 == 1)
        .unwrap_or(false)
}

fn present_column_count(columns_bitmap: &[u8], column_count: usize) -> usize {
    (0..column_count)
        .filter(|&idx| column_present(columns_bitmap, idx))
        .count()
}

pub fn decode_row(
    payload: &mut &[u8],
    columns: &[ColumnDef],
    flavor: Flavor,
) -> Result<Vec<CellValue>, Error> {
    let all_present = vec![0xFF; columns.len().div_ceil(8)];
    decode_row_with_bitmap(payload, columns, &all_present, flavor)
}

pub fn decode_row_with_bitmap(
    payload: &mut &[u8],
    columns: &[ColumnDef],
    columns_bitmap: &[u8],
    flavor: Flavor,
) -> Result<Vec<CellValue>, Error> {
    let present_count = present_column_count(columns_bitmap, columns.len());
    let null_bitmap = read_bitmap(payload, present_count)?;
    let mut values = Vec::with_capacity(columns.len());
    let mut null_idx = 0usize;
    for (idx, column) in columns.iter().enumerate() {
        if !column_present(columns_bitmap, idx) {
            values.push(CellValue::Null);
        } else if is_null(&null_bitmap, null_idx) {
            values.push(CellValue::Null);
            null_idx += 1;
        } else {
            values.push(decode_cell(column, flavor, payload).map_err(|e| {
                Error::Protocol(format!(
                    "column {idx} type {}: {e} ({} bytes left)",
                    column.column_type,
                    payload.len()
                ))
            })?);
            null_idx += 1;
        }
    }
    Ok(values)
}
