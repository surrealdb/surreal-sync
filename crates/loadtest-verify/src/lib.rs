//! Streaming verifier for load testing surreal-sync.
//!
//! This crate provides functionality to verify that data synced to SurrealDB
//! matches expected values generated deterministically from the same schema and seed.
//!
//! # Example
//!
//! ```ignore
//! use loadtest_verify::StreamingVerifier;
//! use sync_core::SyncSchema;
//!
//! let schema = SyncSchema::from_file("schema.yaml")?;
//! let mut verifier = StreamingVerifier::new(&surreal, schema, 42).await?;
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
pub use compare::{compare_values, compare_values_with_options, CompareOptions, CompareResult};
pub use error::VerifyError;
pub use report::{MismatchInfo, VerificationReport};
pub use verifier::StreamingVerifier;
