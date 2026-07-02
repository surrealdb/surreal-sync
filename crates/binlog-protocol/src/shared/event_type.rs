use crate::flavor::Flavor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    FormatDescription,
    Query,
    Rotate,
    TableMap,
    WriteRows,
    UpdateRows,
    DeleteRows,
    Xid,
    Heartbeat,
    MySqlGtid,
    TransactionPayload,
    PartialUpdateRows,
    MariaDbGtid,
    GtidList,
    AnnotateRows,
    BinlogCheckpoint,
    StartEncryption,
    Unknown(u8),
}

impl EventType {
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => EventType::Unknown(0),
            2 => EventType::Query,
            4 => EventType::Rotate,
            15 => EventType::FormatDescription,
            16 => EventType::Xid,
            19 => EventType::TableMap,
            23 => EventType::WriteRows,
            24 => EventType::UpdateRows,
            25 => EventType::DeleteRows,
            27 => EventType::Heartbeat,
            30 => EventType::WriteRows, // WRITE_ROWS_EVENT_V1
            31 => EventType::UpdateRows,
            32 => EventType::DeleteRows,
            33 => EventType::MySqlGtid,
            34 => EventType::TransactionPayload,
            35 => EventType::PartialUpdateRows,
            160 => EventType::AnnotateRows,
            161 => EventType::BinlogCheckpoint,
            162 => EventType::MariaDbGtid,
            163 => EventType::GtidList,
            164 => EventType::StartEncryption,
            _ => EventType::Unknown(code),
        }
    }

    pub fn code(self) -> u8 {
        match self {
            EventType::FormatDescription => 15,
            EventType::Query => 2,
            EventType::Rotate => 4,
            EventType::TableMap => 19,
            EventType::WriteRows => 30,
            EventType::UpdateRows => 31,
            EventType::DeleteRows => 32,
            EventType::Xid => 16,
            EventType::Heartbeat => 27,
            EventType::MySqlGtid => 33,
            EventType::TransactionPayload => 34,
            EventType::PartialUpdateRows => 35,
            EventType::MariaDbGtid => 162,
            EventType::GtidList => 163,
            EventType::AnnotateRows => 160,
            EventType::BinlogCheckpoint => 161,
            EventType::StartEncryption => 164,
            EventType::Unknown(c) => c,
        }
    }

    pub fn for_flavor(self, flavor: Flavor) -> Self {
        match (flavor, self) {
            (
                Flavor::MySql,
                EventType::MariaDbGtid | EventType::GtidList | EventType::AnnotateRows,
            ) => EventType::Unknown(self.code()),
            (Flavor::MariaDb, EventType::MySqlGtid | EventType::TransactionPayload) => {
                EventType::Unknown(self.code())
            }
            _ => self,
        }
    }
}
