//! End-to-end tests for the MySQL interleaved snapshot backend.
//!
//! These tests exercise the DBLog-style snapshot under concurrent writes
//! (including a non-`id` single-column primary key and a composite primary key,
//! to cover the trigger primary-key fix), the bounded audit-table retention
//! (consumed rows pruned per chunk so the audit table never grows one row per
//! change), and the exact bounded-memory peak.
//!
//! The shared bodies live in `common` and are reused by the MariaDB variant in
//! `mariadb_interleaved_snapshot.rs`; these wrappers just pick the MySQL engine.

mod common;

use anyhow::Result;
use common::Engine;

#[tokio::test]
async fn test_mysql_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    common::run_parity_under_concurrent_writes(Engine::MySql, "test-ss-parity").await
}

#[tokio::test]
async fn test_mysql_interleaved_snapshot_bounded_retention() -> Result<()> {
    common::run_bounded_retention(Engine::MySql, "test-ss-retention").await
}

#[tokio::test]
async fn test_mysql_adhoc_snapshot_adds_table_mid_stream() -> Result<()> {
    common::run_adhoc_snapshot(Engine::MySql, "test-ss-adhoc").await
}
