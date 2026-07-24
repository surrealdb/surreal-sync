//! Testing utilities for MySQL trigger source
//!
//! This module provides Docker container management for MySQL testing.

pub mod container;

pub use container::MySQLContainer;

/// MariaDB test container. MariaDB reuses [`MySQLContainer`] verbatim (same wire
/// protocol, env vars, and `mysql://` scheme); construct it via
/// [`MySQLContainer::mariadb`].
pub type MariaDBContainer = MySQLContainer;
