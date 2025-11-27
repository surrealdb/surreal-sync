//! Unified file abstraction for reading from local filesystem, S3, or HTTP/HTTPS
//!
//! This module provides `std::io::Read` implementations that work with
//! local files, S3 objects, and HTTP/HTTPS URLs. All use buffered async reading
//! under the hood, bridged to sync interfaces via tokio_util::io::SyncIoBridge.

pub mod http;
pub mod local;
pub mod s3;

/// Default buffer size for reading operations (1MB)
pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
