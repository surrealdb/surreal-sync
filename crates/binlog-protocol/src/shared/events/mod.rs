pub mod rows;

use crate::error::Error;
use crate::flavor::Flavor;
use crate::shared::buf::{
    read_bitmap, read_cstring, read_lenenc_int, read_u16_le, read_u32_le, read_u64_le, read_u6_le,
    read_u8,
};
use crate::types::{ColumnDef, ColumnMetadata};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatDescriptionEvent {
    pub binlog_version: u16,
    pub server_version: String,
    pub checksum_alg: u8,
    pub post_header_lengths: Vec<u8>,
}

impl FormatDescriptionEvent {
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        let mut payload = body;
        if payload.len() < 84 {
            return Err(Error::UnexpectedEof);
        }
        let binlog_version = read_u16_le(&mut payload)?;
        let server_version = read_cstring(&mut payload)?;
        let _timestamp = read_u32_le_skip(&mut payload)?;
        let _header_len = read_u8(&mut payload)?;
        let post_header_len = read_u8(&mut payload)? as usize;
        let post_header_lengths = crate::shared::buf::read_bytes(&mut payload, post_header_len)?;
        let checksum_alg = if payload.is_empty() {
            0
        } else {
            read_u8(&mut payload)?
        };
        Ok(Self {
            binlog_version,
            server_version,
            checksum_alg,
            post_header_lengths,
        })
    }
}

fn read_u32_le_skip(payload: &mut &[u8]) -> Result<u32, Error> {
    crate::shared::buf::read_u32_le(payload)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RotateEvent {
    pub position: u64,
    pub next_file: String,
}

impl RotateEvent {
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        let mut payload = body;
        let position = read_u64_le(&mut payload)?;
        while payload.first() == Some(&0) {
            payload = &payload[1..];
        }
        let next_file = parse_rotate_filename(payload);
        Ok(Self {
            position,
            next_file,
        })
    }
}

pub(crate) fn sanitize_binlog_file(name: &str) -> String {
    if let Some(rest) = name.strip_prefix("mysql-bin.") {
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            return format!("mysql-bin.{digits}");
        }
    }
    name.chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect()
}

fn parse_rotate_filename(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    sanitize_binlog_file(&String::from_utf8_lossy(&payload[..end]))
}

fn read_lenenc_name(payload: &mut &[u8]) -> Result<String, Error> {
    let len = read_lenenc_int(payload)? as usize;
    let name = String::from_utf8_lossy(&crate::shared::buf::read_bytes(payload, len)?).into_owned();
    if payload.first() == Some(&0) {
        *payload = &payload[1..];
    }
    Ok(name)
}

fn read_legacy_name(payload: &mut &[u8]) -> Result<String, Error> {
    let len = read_u8(payload)? as usize;
    let name = String::from_utf8_lossy(&crate::shared::buf::read_bytes(payload, len)?).into_owned();
    if payload.first() == Some(&0) {
        *payload = &payload[1..];
    }
    Ok(name)
}

pub fn table_id_from_post_header(
    body: &[u8],
    post_header_len: usize,
    flavor: Flavor,
    raw_type_code: u8,
) -> Result<u64, Error> {
    let mut payload = body;
    let six_byte = rows_event_six_byte_table_id(raw_type_code, flavor);
    if six_byte {
        Ok(read_u6_le(&mut payload)?)
    } else if post_header_len == 6 {
        Ok(u64::from(read_u32_le(&mut payload)?))
    } else if body.len() >= 6 {
        Ok(read_u6_le(&mut payload)?)
    } else {
        Err(Error::UnexpectedEof)
    }
}

pub(crate) fn rows_event_six_byte_table_id(raw_type_code: u8, flavor: Flavor) -> bool {
    match flavor {
        Flavor::MariaDb => matches!(raw_type_code, 23..=25 | 30..=32),
        Flavor::MySql => matches!(raw_type_code, 30..=32),
    }
}

/// MySQL/MariaDB `ROWS_HEADER_LEN_V2` post-header size (table_id + flags + var_header_len).
pub const ROWS_HEADER_LEN_V2: usize = 10;

#[derive(Clone, Debug, PartialEq)]
pub struct TableMapEvent {
    pub table_id: u64,
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

impl TableMapEvent {
    pub fn parse(body: &[u8], post_header_len: usize, flavor: Flavor) -> Result<Self, Error> {
        let mut payload = body;
        let table_id = match flavor {
            Flavor::MariaDb => {
                let id = read_u6_le(&mut payload)?;
                if post_header_len >= 8 {
                    let _flags = read_u16_le(&mut payload)?;
                }
                id
            }
            Flavor::MySql => {
                let id = if post_header_len == 6 {
                    u64::from(read_u32_le(&mut payload)?)
                } else {
                    read_u6_le(&mut payload)?
                };
                if post_header_len >= 8 {
                    let _flags = read_u16_le(&mut payload)?;
                }
                id
            }
        };
        let database = match flavor {
            Flavor::MariaDb => read_legacy_name(&mut payload)?,
            Flavor::MySql => read_lenenc_name(&mut payload)?,
        };
        let table = match flavor {
            Flavor::MariaDb => read_legacy_name(&mut payload)?,
            Flavor::MySql => read_lenenc_name(&mut payload)?,
        };
        let column_count = read_lenenc_int(&mut payload)? as usize;
        let mut column_types = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_types.push(read_u8(&mut payload)?);
        }
        let metadata_len = read_lenenc_int(&mut payload)? as usize;
        let metadata_bytes = crate::shared::buf::read_bytes(&mut payload, metadata_len)?;
        let null_bitmap = read_bitmap(&mut payload, column_count)?;

        let mut meta_offset = 0usize;
        let mut columns = Vec::with_capacity(column_count);
        for &column_type in &column_types {
            let (real_type, meta) =
                read_column_meta(column_type, &metadata_bytes, &mut meta_offset)?;
            let metadata = ColumnMetadata::from_table_map(real_type, meta);
            let _ = null_bitmap;
            columns.push(ColumnDef::new(real_type, metadata));
        }

        if flavor == Flavor::MySql && !payload.is_empty() {
            apply_optional_metadata(&mut columns, payload);
        }

        Ok(Self {
            table_id,
            database,
            table,
            columns,
        })
    }
}

fn read_column_meta(
    column_type: u8,
    metadata: &[u8],
    offset: &mut usize,
) -> Result<(u8, u16), Error> {
    use crate::shared::column_type::*;
    match column_type {
        MYSQL_TYPE_STRING => {
            if *offset + 2 > metadata.len() {
                return Err(Error::UnexpectedEof);
            }
            let byte0 = metadata[*offset];
            let byte1 = metadata[*offset + 1];
            *offset += 2;
            if byte0 == MYSQL_TYPE_ENUM || byte0 == MYSQL_TYPE_SET {
                return Ok((byte0, u16::from(byte1)));
            }
            let length = if (byte0 & 0x30) != 0x30 {
                u16::from(byte1) | (u16::from(byte0 & 0x30) << 4)
            } else {
                u16::from(byte1)
            };
            let real = if byte0 & 0x30 != 0x30 {
                byte0 | 0x30
            } else {
                byte1
            };
            if real == MYSQL_TYPE_ENUM_REAL || real == MYSQL_TYPE_SET_REAL {
                Ok((real, length))
            } else {
                Ok((MYSQL_TYPE_STRING, length))
            }
        }
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_BIT
        | MYSQL_TYPE_NEWDECIMAL
        | MYSQL_TYPE_DECIMAL => read_u16_meta(column_type, metadata, offset),
        MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_JSON
        | MYSQL_TYPE_GEOMETRY => {
            let m = read_u8_meta(metadata, offset)?;
            Ok((column_type, u16::from(m)))
        }
        MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIME2 => {
            let m = read_u8_meta(metadata, offset)?;
            Ok((column_type, u16::from(m)))
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            let m = read_u8_meta(metadata, offset)?;
            Ok((column_type, u16::from(m)))
        }
        MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => read_u16_meta(column_type, metadata, offset),
        _ => Ok((column_type, 0)),
    }
}

const OPTIONAL_METADATA_SIGNEDNESS: u8 = 1;

fn apply_optional_metadata(columns: &mut [ColumnDef], optional: &[u8]) {
    let mut pos = 0usize;
    while pos < optional.len() {
        let field_type = optional[pos];
        pos += 1;
        let Some((len, consumed)) = read_lenenc_int_slice(&optional[pos..]) else {
            break;
        };
        pos += consumed;
        if pos + len > optional.len() {
            break;
        }
        let value = &optional[pos..pos + len];
        pos += len;
        if field_type == OPTIONAL_METADATA_SIGNEDNESS {
            apply_signedness(columns, value);
        }
    }
}

fn read_lenenc_int_slice(data: &[u8]) -> Option<(usize, usize)> {
    if data.is_empty() {
        return None;
    }
    let first = data[0];
    match first {
        0xfb => Some((0, 1)),
        0xfc if data.len() >= 3 => Some((u16::from_le_bytes([data[1], data[2]]) as usize, 3)),
        0xfd if data.len() >= 4 => {
            let v = u32::from_le_bytes([data[1], data[2], data[3], 0]) as usize;
            Some((v, 4))
        }
        0xfe if data.len() >= 9 => Some((
            u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]) as usize,
            9,
        )),
        0xfc..=0xfe => None,
        n => Some((n as usize, 1)),
    }
}

fn apply_signedness(columns: &mut [ColumnDef], data: &[u8]) {
    let mut bit_idx = 0usize;
    for &byte in data {
        for mask in [0x80u8, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01] {
            if bit_idx >= columns.len() {
                return;
            }
            if is_numeric_column(columns[bit_idx].column_type) && (byte & mask) != 0 {
                columns[bit_idx].unsigned = true;
            }
            bit_idx += 1;
        }
    }
}

fn is_numeric_column(column_type: u8) -> bool {
    use crate::shared::column_type::*;
    matches!(
        column_type,
        MYSQL_TYPE_TINY
            | MYSQL_TYPE_SHORT
            | MYSQL_TYPE_INT24
            | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG
            | MYSQL_TYPE_FLOAT
            | MYSQL_TYPE_DOUBLE
            | MYSQL_TYPE_NEWDECIMAL
    )
}

fn read_u8_meta(metadata: &[u8], offset: &mut usize) -> Result<u8, Error> {
    if *offset >= metadata.len() {
        return Err(Error::UnexpectedEof);
    }
    let v = metadata[*offset];
    *offset += 1;
    Ok(v)
}

fn read_u16_meta(column_type: u8, metadata: &[u8], offset: &mut usize) -> Result<(u8, u16), Error> {
    if *offset + 2 > metadata.len() {
        return Err(Error::UnexpectedEof);
    }
    let v = u16::from_le_bytes([metadata[*offset], metadata[*offset + 1]]);
    *offset += 2;
    Ok((column_type, v))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XidEvent {
    pub xid: u64,
}

impl XidEvent {
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        let mut payload = body;
        Ok(Self {
            xid: read_u64_le(&mut payload)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryEvent {
    pub thread_id: u32,
    pub database: String,
    pub sql: String,
}

impl QueryEvent {
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        let mut payload = body;
        let thread_id = crate::shared::buf::read_u32_le(&mut payload)?;
        let _exec_time = crate::shared::buf::read_u32_le(&mut payload)?;
        let db_len = read_u8(&mut payload)? as usize;
        let _error_code = read_u16_le(&mut payload)?;
        let _status_vars_len = read_u16_le(&mut payload)?;
        let database = if db_len > 0 {
            let db = String::from_utf8_lossy(&payload[..db_len]).into_owned();
            payload = &payload[db_len..];
            db
        } else {
            String::new()
        };
        let sql = String::from_utf8_lossy(payload).into_owned();
        Ok(Self {
            thread_id,
            database,
            sql,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatEvent {
    pub log_filename: String,
}

impl HeartbeatEvent {
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            log_filename: String::from_utf8_lossy(body).into_owned(),
        })
    }
}
