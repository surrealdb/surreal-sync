//! Testing helpers for from-mysql-binlog integration tests (stubbed for e2e phase).

#![allow(dead_code)]

#[derive(Debug)]
pub struct MySQLBinlogContainer;

impl MySQLBinlogContainer {
    pub fn new(_name: &str) -> Self {
        Self
    }
}

#[derive(Debug)]
pub struct MariaDBBinlogContainer;

impl MariaDBBinlogContainer {
    pub fn mariadb(_name: &str) -> Self {
        Self
    }
}
