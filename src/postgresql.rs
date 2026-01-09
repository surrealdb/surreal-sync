//! PostgreSQL logical decoding sync (WAL-based)
//!
//! This module provides PostgreSQL logical decoding-based synchronization.
//! For trigger-based sync, use the `surreal_sync_postgresql_trigger` crate.

mod logical_decoding;
mod state;

pub use logical_decoding::*;
