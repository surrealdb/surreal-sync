//! CSV file import module for SurrealDB
//!
//! This module provides functionality to stream CSV files
//! and import them into SurrealDB tables.

mod file;
mod s3;
mod sync;

pub use sync::{sync, Config};
