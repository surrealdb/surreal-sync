use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::flavor::mariadb::gtid_list::MariaDbGtidList;
use crate::binlog_protocol::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::binlog_protocol::flavor::Flavor;
use crate::binlog_protocol::shared::column_type::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use crate::binlog_protocol::shared::events::rows::RowsEvent;
pub use crate::binlog_protocol::shared::events::{
    FormatDescriptionEvent, HeartbeatEvent, QueryEvent, RotateEvent, TableMapEvent, XidEvent,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BinlogPosition {
    FilePos { file: String, pos: u64 },
    MySqlGtid { executed: MySqlGtidSet },
    MariaDbGtid { executed: MariaDbGtidList },
}

impl BinlogPosition {
    pub fn flavor(&self) -> Flavor {
        match self {
            BinlogPosition::FilePos { .. } => Flavor::MySql,
            BinlogPosition::MySqlGtid { .. } => Flavor::MySql,
            BinlogPosition::MariaDbGtid { .. } => Flavor::MariaDb,
        }
    }

    pub fn file_pos(file: impl Into<String>, pos: u64) -> Self {
        Self::FilePos {
            file: file.into(),
            pos,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnMetadata {
    None,
    Numeric { precision: u8, scale: u8 },
    String { max_length: u16 },
    Blob { length_bytes: u8 },
    Bit { length_bits: u16 },
    FloatPackLength { bytes: u8 },
    Temporal { fsp: u8 },
    EnumSet { max_length: u16 },
}

impl ColumnMetadata {
    pub fn from_table_map(column_type: u8, meta: u16) -> Self {
        match column_type {
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => ColumnMetadata::Numeric {
                precision: (meta & 0xFF) as u8,
                scale: (meta >> 8) as u8,
            },
            MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => ColumnMetadata::FloatPackLength {
                bytes: (meta & 0xFF) as u8,
            },
            MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIME2 => {
                ColumnMetadata::Temporal {
                    fsp: (meta & 0xFF) as u8,
                }
            }
            MYSQL_TYPE_BIT => ColumnMetadata::Bit {
                length_bits: (meta >> 8) * 8 + (meta & 0xFF),
            },
            MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
                ColumnMetadata::String { max_length: meta }
            }
            MYSQL_TYPE_STRING => ColumnMetadata::EnumSet { max_length: meta },
            MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => ColumnMetadata::EnumSet { max_length: meta },
            MYSQL_TYPE_BLOB
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB
            | MYSQL_TYPE_JSON
            | MYSQL_TYPE_GEOMETRY => ColumnMetadata::Blob {
                length_bytes: meta as u8,
            },
            _ => ColumnMetadata::None,
        }
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(self, ColumnMetadata::Numeric { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: Option<String>,
    pub column_type: u8,
    pub metadata: ColumnMetadata,
    pub unsigned: bool,
}

impl ColumnDef {
    pub fn new(column_type: u8, metadata: ColumnMetadata) -> Self {
        Self {
            name: None,
            column_type,
            metadata,
            unsigned: false,
        }
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CellValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Bit(Vec<u8>),
    JsonBytes(Vec<u8>),
    JsonText(String),
    JsonDiff(Vec<JsonDiff>),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    Time {
        hour: i32,
        minute: u8,
        second: u8,
        micros: u32,
    },
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micros: u32,
    },
    TimestampMillis(u64),
    Year(u16),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonDiffOperation {
    Replace,
    Insert,
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDiff {
    pub operation: JsonDiffOperation,
    pub path: String,
    pub data: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellValueKind {
    Null,
    Int,
    UInt,
    Float,
    Double,
    Decimal,
    String,
    Bytes,
    Bit,
    JsonBytes,
    JsonText,
    Date,
    Time,
    DateTime,
    TimestampMillis,
    Year,
}

pub fn expected_cell_kind(column_type: u8, flavor: Flavor) -> CellValueKind {
    use CellValueKind::*;
    match column_type {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
        | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_YEAR => Int,
        MYSQL_TYPE_FLOAT => Float,
        MYSQL_TYPE_DOUBLE => Double,
        MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL => Decimal,
        MYSQL_TYPE_DATE => Date,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => Time,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => DateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => TimestampMillis,
        MYSQL_TYPE_BIT => Bit,
        MYSQL_TYPE_JSON if flavor == Flavor::MySql => JsonBytes,
        MYSQL_TYPE_JSON if flavor == Flavor::MariaDb => JsonText,
        MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET => String,
        MYSQL_TYPE_GEOMETRY => Bytes,
        _ => Bytes,
    }
}

pub fn decode_cell(
    column: &ColumnDef,
    flavor: Flavor,
    payload: &mut &[u8],
) -> Result<CellValue, Error> {
    crate::binlog_protocol::shared::row::decode::decode_cell(column, flavor, payload)
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowChange {
    Insert(Vec<CellValue>),
    Update {
        before: Vec<CellValue>,
        after: Vec<CellValue>,
    },
    Delete(Vec<CellValue>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GtidMarker {
    MySql {
        source_id: [u8; 16],
        gno: u64,
    },
    MariaDb {
        domain_id: u32,
        server_id: u32,
        sequence: u64,
    },
}
