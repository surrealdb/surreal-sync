//! MySQL/MariaDB binlog replication protocol for CDC from `binlog_format=ROW` sources
//! (not STATEMENT/MIXED). Handles row opcodes 23–25 (MariaDB typical) and MySQL
//! `WRITE/UPDATE/DELETE_ROWS_EVENT_V1` opcodes 30–32, plus MySQL-only opcode 39
//! (`PARTIAL_UPDATE_ROWS`). Post-header lengths follow the format-description event;
//! `ROWS_HEADER_LEN_V2` is the V1 wire-layout name, not `binlog_format`.

mod bytes_reader;
mod cdc_stream;
mod client;
mod detect;
mod error;
mod event;
mod file_reader;
mod flavor;
mod options;
mod shared;
mod types;

pub mod test_images;

pub use crate::ssl::{SslMode, SslOptions};
pub use bytes_reader::BinlogBytesReader;
pub use cdc_stream::{CdcChange, CdcStream};
pub use client::BinlogClient;
pub use error::Error;
pub use event::{EventBody, RawEvent};
pub use file_reader::BinlogFileReader;
pub use flavor::mariadb::{MariaDbGtid, MariaDbGtidList, MariaDbPositionTracker};
pub use flavor::mysql::{MySqlGtidSet, MySqlPositionTracker};
pub use flavor::Flavor;
pub use options::{MariaDbDumpFlags, MariaDbGtidStrictMode, ReplicaOptions, ResumePosition};
pub use shared::event_header::EventHeader;
pub use shared::event_type::EventType;
pub use types::{
    decode_cell, expected_cell_kind, BinlogPosition, CellValue, CellValueKind, ColumnDef,
    ColumnMetadata, FormatDescriptionEvent, GtidMarker, HeartbeatEvent, JsonDiff,
    JsonDiffOperation, QueryEvent, RotateEvent, RowChange, RowsEvent, TableMapEvent, XidEvent,
};

/// MYSQL_TYPE_* column type byte constants.
pub mod column_types {
    pub use crate::binlog_protocol::shared::column_type::*;
}
