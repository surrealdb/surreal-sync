//! Shared helpers for synthetic cell-decode fixtures.

use binlog_protocol::{
    decode_cell, expected_cell_kind, CellValue, CellValueKind, ColumnDef, ColumnMetadata, Flavor,
};

pub fn col(column_type: u8, metadata: ColumnMetadata) -> ColumnDef {
    ColumnDef::new(column_type, metadata).with_unsigned(false)
}

pub fn col_unsigned(column_type: u8, metadata: ColumnMetadata) -> ColumnDef {
    ColumnDef::new(column_type, metadata).with_unsigned(true)
}

pub fn decode_hex(column: &ColumnDef, flavor: Flavor, hex: &str) -> CellValue {
    let payload = hex::decode(hex.replace(' ', "")).expect("valid hex fixture");
    decode_cell(column, flavor, &mut payload.as_slice()).expect("decode_cell")
}

pub fn assert_kind(column_type: u8, flavor: Flavor, expected: CellValueKind) {
    assert_eq!(expected_cell_kind(column_type, flavor), expected);
}

pub mod binary;
pub mod bit_set;
pub mod float_decimal;
pub mod geometry;
pub mod integers;
pub mod json;
pub mod strings;
pub mod temporal;
