//! MySQL client utilities
//!
//! This module provides utilities for creating and managing MySQL connection pools.

use anyhow::Result;
use binlog_protocol::SslMode;
use mysql_async::Pool;

/// Create a new MySQL connection pool (plaintext).
pub fn new_mysql_pool(connection_string: &str) -> Result<Pool> {
    new_mysql_pool_sync(connection_string, &SslMode::Disabled)
}

/// Create a MySQL pool honouring TLS mode (sync; Preferred does not probe-fallback).
pub fn new_mysql_pool_sync(connection_string: &str, ssl: &SslMode) -> Result<Pool> {
    mysql_types::ssl::new_mysql_pool_with_ssl_sync(connection_string, ssl)
}

/// Create a MySQL pool honouring TLS mode, with Preferred plaintext fallback.
pub async fn new_mysql_pool_with_ssl(connection_string: &str, ssl: &SslMode) -> Result<Pool> {
    mysql_types::new_mysql_pool_with_ssl(connection_string, ssl).await
}
