use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::flavor::Flavor;
use crate::binlog_protocol::shared::buf::{
    read_bitmap, read_bytes, read_lenenc_int, read_u16_le, read_u32_le, read_u6_le,
};
use crate::binlog_protocol::shared::column_type::MYSQL_TYPE_JSON;
use crate::binlog_protocol::shared::events::ROWS_HEADER_LEN_V2;
use crate::binlog_protocol::shared::row::{
    decode_row_with_bitmap, decode_row_with_bitmap_and_json_partials,
};
use crate::binlog_protocol::types::{ColumnDef, RowChange};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowsKind {
    Write,
    Update,
    PartialUpdate,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowsEvent {
    pub table_id: u64,
    pub flags: u16,
    pub rows: Vec<RowChange>,
}

impl RowsEvent {
    pub fn parse_write(
        body: &[u8],
        columns: &[ColumnDef],
        flavor: Flavor,
        six_byte_table_id: bool,
        v2_var_header: bool,
        post_header_len: usize,
    ) -> Result<Self, Error> {
        parse_rows(
            body,
            columns,
            flavor,
            six_byte_table_id,
            v2_var_header,
            post_header_len,
            RowsKind::Write,
        )
    }

    pub fn parse_update(
        body: &[u8],
        columns: &[ColumnDef],
        flavor: Flavor,
        six_byte_table_id: bool,
        v2_var_header: bool,
        post_header_len: usize,
    ) -> Result<Self, Error> {
        parse_rows(
            body,
            columns,
            flavor,
            six_byte_table_id,
            v2_var_header,
            post_header_len,
            RowsKind::Update,
        )
    }

    pub fn parse_delete(
        body: &[u8],
        columns: &[ColumnDef],
        flavor: Flavor,
        six_byte_table_id: bool,
        v2_var_header: bool,
        post_header_len: usize,
    ) -> Result<Self, Error> {
        parse_rows(
            body,
            columns,
            flavor,
            six_byte_table_id,
            v2_var_header,
            post_header_len,
            RowsKind::Delete,
        )
        .map(|mut e| {
            e.rows = e
                .rows
                .into_iter()
                .map(|r| match r {
                    RowChange::Insert(cells) => RowChange::Delete(cells),
                    other => other,
                })
                .collect();
            e
        })
    }

    pub fn parse_partial_update(
        body: &[u8],
        columns: &[ColumnDef],
        flavor: Flavor,
        six_byte_table_id: bool,
        v2_var_header: bool,
        post_header_len: usize,
    ) -> Result<Self, Error> {
        parse_rows(
            body,
            columns,
            flavor,
            six_byte_table_id,
            v2_var_header,
            post_header_len,
            RowsKind::PartialUpdate,
        )
    }
}

fn parse_rows(
    body: &[u8],
    columns: &[ColumnDef],
    flavor: Flavor,
    six_byte_table_id: bool,
    v2_var_header: bool,
    post_header_len: usize,
    kind: RowsKind,
) -> Result<RowsEvent, Error> {
    let mut payload = body;
    let table_id = if six_byte_table_id {
        read_u6_le(&mut payload)?
    } else {
        u64::from(read_u32_le(&mut payload)?)
    };
    let flags = read_u16_le(&mut payload)?;
    if v2_var_header && post_header_len >= ROWS_HEADER_LEN_V2 {
        skip_v2_var_header(&mut payload)?;
    }

    let width = read_lenenc_int(&mut payload)? as usize;
    if width != columns.len() {
        return Err(Error::Protocol(format!(
            "row event width {width} does not match table map column count {} (body={:02x?})",
            columns.len(),
            &body[..body.len().min(32)]
        )));
    }

    let bitmap_len = width.div_ceil(8);
    let columns_before = read_bytes(&mut payload, bitmap_len)?;
    let columns_after = if matches!(kind, RowsKind::Update | RowsKind::PartialUpdate) {
        read_bytes(&mut payload, bitmap_len)?
    } else {
        columns_before.clone()
    };

    let mut rows = Vec::new();
    while !payload.is_empty() {
        let remaining = payload.len();
        match kind {
            RowsKind::Update => {
                let before =
                    decode_row_with_bitmap(&mut payload, columns, &columns_before, flavor)?;
                let after = decode_row_with_bitmap(&mut payload, columns, &columns_after, flavor)?;
                rows.push(RowChange::Update { before, after });
            }
            RowsKind::PartialUpdate => {
                let before =
                    decode_row_with_bitmap(&mut payload, columns, &columns_before, flavor)?;
                let partial_json_columns = read_partial_json_columns(&mut payload, columns)?;
                let after = decode_row_with_bitmap_and_json_partials(
                    &mut payload,
                    columns,
                    &columns_after,
                    flavor,
                    |idx| partial_json_columns.get(idx).copied().unwrap_or(false),
                )?;
                rows.push(RowChange::Update { before, after });
            }
            RowsKind::Write | RowsKind::Delete => {
                let cells = decode_row_with_bitmap(&mut payload, columns, &columns_after, flavor)?;
                rows.push(RowChange::Insert(cells));
            }
        }
        if payload.len() >= remaining {
            return Err(Error::Protocol(
                "row decode made no progress; check table map metadata".into(),
            ));
        }
    }

    Ok(RowsEvent {
        table_id,
        flags,
        rows,
    })
}

fn read_partial_json_columns(
    payload: &mut &[u8],
    columns: &[ColumnDef],
) -> Result<Vec<bool>, Error> {
    let value_options = read_lenenc_int(payload)?;
    let mut partial_columns = vec![false; columns.len()];
    if value_options & 1 == 0 {
        return Ok(partial_columns);
    }
    if value_options & !1 != 0 {
        return Err(Error::Protocol(format!(
            "unsupported PARTIAL_UPDATE_ROWS value_options=0x{value_options:x}"
        )));
    }

    let json_column_count = columns
        .iter()
        .filter(|col| col.column_type == MYSQL_TYPE_JSON)
        .count();
    let bits = read_bitmap(payload, json_column_count)?;
    let mut json_idx = 0usize;
    for (idx, col) in columns.iter().enumerate() {
        if col.column_type != MYSQL_TYPE_JSON {
            continue;
        }
        partial_columns[idx] = bit_is_set(&bits, json_idx);
        json_idx += 1;
    }
    Ok(partial_columns)
}

fn bit_is_set(bitmap: &[u8], index: usize) -> bool {
    let byte = index / 8;
    let bit = index % 8;
    bitmap
        .get(byte)
        .map(|b| (b >> bit) & 1 == 1)
        .unwrap_or(false)
}

/// Skip the v2 variable-length post-header (NDB/partition extra row info).
fn skip_v2_var_header(payload: &mut &[u8]) -> Result<(), Error> {
    let var_header_len = read_u16_le(payload)? as usize;
    if var_header_len < 2 {
        return Err(Error::Protocol(format!(
            "invalid rows v2 var_header_len {var_header_len}"
        )));
    }
    let data_len = var_header_len - 2;
    if payload.len() < data_len {
        return Err(Error::UnexpectedEof);
    }
    *payload = &payload[data_len..];
    Ok(())
}
