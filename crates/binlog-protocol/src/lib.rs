//! MySQL/MariaDB binlog replication protocol.

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

pub use bytes_reader::BinlogBytesReader;
pub use cdc_stream::{CdcChange, CdcStream};
pub use client::BinlogClient;
pub use error::Error;
pub use event::{EventBody, RawEvent};
pub use file_reader::BinlogFileReader;
pub use flavor::mariadb::{MariaDbGtid, MariaDbGtidList, MariaDbPositionTracker};
pub use flavor::mysql::{MySqlGtidSet, MySqlPositionTracker};
pub use flavor::Flavor;
pub use options::{MariaDbDumpFlags, ReplicaOptions, ResumePosition, SslMode};
pub use shared::event_header::EventHeader;
pub use shared::event_type::EventType;
pub use types::{
    decode_cell, expected_cell_kind, BinlogPosition, CellValue, CellValueKind, ColumnDef,
    ColumnMetadata, FormatDescriptionEvent, GtidMarker, HeartbeatEvent, QueryEvent, RotateEvent,
    RowChange, RowsEvent, TableMapEvent, XidEvent,
};

/// MYSQL_TYPE_* column type byte constants.
pub mod column_types {
    pub use crate::shared::column_type::*;
}
