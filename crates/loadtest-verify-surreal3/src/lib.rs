//! Streaming verifier for load testing surreal-sync (SurrealDB v3).
//!
//! This crate provides functionality to verify that data synced to SurrealDB v3
//! matches expected values generated deterministically from the same schema and seed.
//!
//! # Example
//!
//! ```ignore
//! use loadtest_verify_surreal3::StreamingVerifier3;
//! use sync_core::Schema;
//!
//! let schema = Schema::from_file("schema.yaml")?;
//! let mut verifier = StreamingVerifier3::new(&surreal, schema, 42).await?;
//!
//! let report = verifier.verify_streaming("users", 1000).await?;
//! assert!(report.mismatched.is_empty());
//! ```

pub mod args;
pub mod compare;
pub mod error;
pub mod report;
pub mod verifier;

pub use args::VerifyArgs;
pub use compare::{compare_values, CompareResult};
pub use error::VerifyError;
pub use report::{MismatchInfo, VerificationReport};
pub use verifier::StreamingVerifier3;
