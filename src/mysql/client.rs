//! MySQL client utilities
//!
//! This module provides utilities for creating and managing MySQL connection pools.

use anyhow::Result;
use mysql_async::Pool;

/// Create a new MySQL connection pool
pub fn new_mysql_pool(connection_string: &str) -> Result<Pool> {
    let pool = Pool::from_url(connection_string)?;
    Ok(pool)
}
