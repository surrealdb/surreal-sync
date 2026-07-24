//! End-to-end tests for the interleaved snapshot backend against MariaDB.
//!
//! MariaDB speaks the MySQL wire protocol and runs the identical
//! interleaved-snapshot code path, so these tests reuse the shared bodies in
//! `common` (parity under concurrent writes with non-`id` and composite primary
//! keys, bounded audit-table retention, and ad-hoc mid-stream snapshots) against
//! a `mariadb:11` container.

mod common;

use anyhow::Result;
use common::Engine;

#[tokio::test]
async fn test_mariadb_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    common::run_parity_under_concurrent_writes(Engine::MariaDb, "test-ss-parity-mariadb").await
}

#[tokio::test]
async fn test_mariadb_interleaved_snapshot_bounded_retention() -> Result<()> {
    common::run_bounded_retention(Engine::MariaDb, "test-ss-retention-mariadb").await
}

#[tokio::test]
async fn test_mariadb_adhoc_snapshot_adds_table_mid_stream() -> Result<()> {
    common::run_adhoc_snapshot(Engine::MariaDb, "test-ss-adhoc-mariadb").await
}
