//! CSV file import module for SurrealDB
//!
//! This module provides functionality to stream CSV files from various sources
//! (local files, S3, HTTP/HTTPS) and import them into SurrealDB tables.

mod file;
mod metrics;
pub mod surreal;
mod sync;

pub use sync::{sync, Config};
