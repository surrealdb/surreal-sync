//! Interleaved snapshot e2e tests for the binlog watermark backend.

use anyhow::Result;

use crate::common::Engine;

#[tokio::test]
async fn mysql_binlog_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    crate::common::run_parity_under_concurrent_writes(Engine::MySql, "binlog-ss-parity-mysql").await
}

#[tokio::test]
async fn mysql_binlog_interleaved_snapshot_bounded_retention() -> Result<()> {
    crate::common::run_bounded_retention(Engine::MySql, "binlog-ss-retention-mysql").await
}

#[tokio::test]
async fn mysql_binlog_adhoc_snapshot_adds_table_mid_stream() -> Result<()> {
    crate::common::run_adhoc_snapshot(Engine::MySql, "binlog-ss-adhoc-mysql").await
}

#[tokio::test]
async fn mariadb_binlog_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    crate::common::run_parity_under_concurrent_writes(Engine::MariaDb, "binlog-ss-parity-mdb").await
}

#[tokio::test]
async fn mariadb_binlog_interleaved_snapshot_bounded_retention() -> Result<()> {
    crate::common::run_bounded_retention(Engine::MariaDb, "binlog-ss-retention-mdb").await
}

#[tokio::test]
async fn mariadb_binlog_adhoc_snapshot_adds_table_mid_stream() -> Result<()> {
    crate::common::run_adhoc_snapshot(Engine::MariaDb, "binlog-ss-adhoc-mdb").await
}
