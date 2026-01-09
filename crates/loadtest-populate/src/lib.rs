//! Common types and utilities for loadtest populators.
//!
//! This crate provides shared argument types and utilities used across
//! all loadtest-populate-* crates (MySQL, PostgreSQL, MongoDB, CSV, JSONL, Kafka).

pub mod args;

pub use args::CommonPopulateArgs;
