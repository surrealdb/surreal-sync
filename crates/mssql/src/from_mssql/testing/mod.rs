//! Docker helper for SQL Server CDC tests.
//!
//! Running the container accepts Microsoft's EULA
//! (https://go.microsoft.com/fwlink/?linkid=2143497). Developer edition is for
//! test only — do not imply production licensing.

mod container;

pub use super::client::{connect, MssqlClient};
pub use container::MssqlContainer;
