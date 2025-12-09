// Re-export RecordWithSurrealValues from surrealdb-types
pub use surrealdb_types::RecordWithSurrealValues;

// Type alias for backwards compatibility - Record is now RecordWithSurrealValues
pub type Record = RecordWithSurrealValues;
