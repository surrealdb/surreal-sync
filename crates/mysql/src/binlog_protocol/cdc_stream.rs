use std::collections::HashMap;

use crate::binlog_protocol::client::BinlogClient;
use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::event::EventBody;
use crate::binlog_protocol::types::{BinlogPosition, RowChange, TableMapEvent};

pub struct CdcStream<'a> {
    client: &'a mut BinlogClient,
    table_maps: HashMap<u64, TableMapEvent>,
    current_xid: Option<u64>,
    current_gtid: Option<crate::binlog_protocol::types::GtidMarker>,
}

impl<'a> CdcStream<'a> {
    pub fn new(client: &'a mut BinlogClient) -> Self {
        Self {
            client,
            table_maps: HashMap::new(),
            current_xid: None,
            current_gtid: None,
        }
    }

    pub async fn next_change(&mut self) -> Result<Option<CdcChange>, Error> {
        loop {
            let events = self.client.next_events(16).await?;
            if events.is_empty() {
                return Ok(None);
            }
            for event in events {
                match event.body {
                    EventBody::TableMap(tm) => {
                        self.table_maps.insert(tm.table_id, tm);
                    }
                    EventBody::Rows(rows) => {
                        let table = self
                            .table_maps
                            .get(&rows.table_id)
                            .ok_or_else(|| Error::Protocol("missing table map".into()))?;
                        if let Some(row) = rows.rows.into_iter().next() {
                            let position = self.client.current_position();
                            return Ok(Some(CdcChange {
                                position,
                                database: table.database.clone(),
                                table: table.table.clone(),
                                operation: row,
                                xid: self.current_xid,
                                gtid: self.current_gtid.clone(),
                            }));
                        }
                    }
                    EventBody::Xid(xid) => {
                        self.current_xid = Some(xid.xid);
                    }
                    EventBody::Gtid(gtid) => {
                        self.current_gtid = Some(gtid);
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CdcChange {
    pub position: BinlogPosition,
    pub database: String,
    pub table: String,
    pub operation: RowChange,
    pub xid: Option<u64>,
    pub gtid: Option<crate::binlog_protocol::types::GtidMarker>,
}
