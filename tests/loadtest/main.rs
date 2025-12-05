//! Load testing integration tests.
//!
//! These tests verify the populate -> sync -> verify workflow using
//! deterministic data generation. Each test:
//! 1. Cleans up any existing data
//! 2. Populates source database with deterministic test data (fixed seed)
//! 3. Runs sync to transfer data to SurrealDB
//! 4. Verifies synced data matches expected values (same seed)
//! 5. Cleans up all test data

mod csv_loadtest;
mod jsonl_loadtest;
mod mongodb_loadtest;
mod mysql_loadtest;
mod postgresql_loadtest;
