//! Unified file abstraction for reading from local filesystem or S3
//!
//! This module provides `std::io::Read` implementations that work with both
//! local files and S3 objects. Both use buffered async reading under the hood,
//! bridged to sync interfaces via tokio_util::io::SyncIoBridge.

pub mod local;
pub mod s3;

/// Default buffer size for reading operations (1MB)
pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
