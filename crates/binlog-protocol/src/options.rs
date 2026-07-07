use std::time::Duration;

use crate::flavor::mariadb::gtid_list::MariaDbGtidList;
use crate::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::flavor::Flavor;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SslOptions {
    pub ca: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SslMode {
    Disabled,
    Preferred(SslOptions),
    Required(SslOptions),
}

impl SslMode {
    pub fn preferred() -> Self {
        Self::Preferred(SslOptions::default())
    }

    pub fn required() -> Self {
        Self::Required(SslOptions::default())
    }

    pub fn options(&self) -> Option<&SslOptions> {
        match self {
            Self::Disabled => None,
            Self::Preferred(options) | Self::Required(options) => Some(options),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MariaDbDumpFlags {
    pub send_annotate_rows: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MariaDbGtidStrictMode {
    #[default]
    ServerDefault,
    On,
    Off,
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
    pub mariadb_gtid_strict_mode: MariaDbGtidStrictMode,
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
            mariadb_gtid_strict_mode: MariaDbGtidStrictMode::default(),
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
