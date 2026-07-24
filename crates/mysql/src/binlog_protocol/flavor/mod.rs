pub mod mariadb;
pub mod mysql;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Flavor {
    MySql,
    MariaDb,
}

impl Flavor {
    pub fn detect(version_string: &str) -> Self {
        crate::binlog_protocol::detect::detect_flavor(version_string)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Flavor::MySql => "mysql",
            Flavor::MariaDb => "mariadb",
        }
    }
}
