use std::time::Duration;

use crate::flavor::mariadb::gtid_list::MariaDbGtidList;
use crate::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::flavor::Flavor;

#[derive(Clone, Debug)]
pub enum SslMode {
    Disabled,
}

#[derive(Clone, Debug, Default)]
pub struct MariaDbDumpFlags {
    pub send_annotate_rows: bool,
}

#[derive(Clone, Debug)]
pub struct ReplicaOptions {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub server_id: u32,
    pub ssl: SslMode,
    pub blocking_poll: Duration,
    pub flavor: Option<Flavor>,
    pub mariadb_flags: MariaDbDumpFlags,
}

impl ReplicaOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            server_id: 1,
            ssl: SslMode::Disabled,
            blocking_poll: Duration::from_millis(100),
            flavor: None,
            mariadb_flags: MariaDbDumpFlags::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ResumePosition {
    Start,
    End,
    FilePos { file: String, pos: u32 },
    MySqlGtid(MySqlGtidSet),
    MariaDbGtid(MariaDbGtidList),
}
