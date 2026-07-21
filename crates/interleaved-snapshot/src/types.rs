//! Core types shared by the watermark snapshot framework.

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use uuid::Uuid;

/// A position in a source's change stream.
///
/// Positions must be totally ordered so the framework can reason about
/// "before"/"after" relationships, and serializable so they can be persisted
/// in resumable checkpoints. The blanket implementation below means common
/// position representations work out of the box:
///
/// - PostgreSQL wal2json: an LSN `String` (lexicographically comparable).
/// - PostgreSQL/MySQL trigger sources: an `i64` sequence id.
pub trait ReconciliationPos:
    Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<T> ReconciliationPos for T where
    T: Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// Identifies a table to snapshot together with the ordered primary key
/// columns used for keyset-paginated chunk reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSpec {
    /// Table name.
    pub table: String,
    /// Ordered primary key column names (the order defines the keyset sort).
    pub pk_columns: Vec<String>,
}

impl TableSpec {
    /// Create a new table spec.
    pub fn new(table: impl Into<String>, pk_columns: Vec<String>) -> Self {
        Self {
            table: table.into(),
            pk_columns,
        }
    }
}

/// An ordered tuple of primary key values.
///
/// The values are ordered to match the corresponding [`TableSpec::pk_columns`]
/// so single and composite primary keys are handled uniformly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PkTuple(pub Vec<UniversalValue>);

impl PkTuple {
    /// Create a primary key tuple from ordered values.
    pub fn new(values: Vec<UniversalValue>) -> Self {
        Self(values)
    }

    /// A stable string key for this primary key, suitable for use as a hash
    /// map key (works for composite keys and value kinds that are not
    /// themselves `Hash`/`Eq`).
    ///
    /// Values are canonicalized before serialization so snapshot reads and CDC
    /// events that differ only by integer width (`Int32` vs `Int64`) or string
    /// kind (`VarChar` vs `Text`) still collide for window dedup.
    pub fn key(&self) -> String {
        let canonical: Vec<UniversalValue> = self.0.iter().map(canonicalize_pk_value).collect();
        serde_json::to_string(&canonical).unwrap_or_default()
    }

    /// If this is a single-column UUID key, return the UUID.
    ///
    /// The framework writes watermarks as rows keyed by a freshly generated
    /// UUID and recognizes those rows in the change stream via this helper.
    pub fn single_uuid(&self) -> Option<Uuid> {
        match self.0.as_slice() {
            [UniversalValue::Uuid(u)] => Some(*u),
            _ => None,
        }
    }

    /// Extract the primary key tuple from a row given the table's primary key
    /// columns.
    ///
    /// Each column is looked up in the row's fields. As a convenience for
    /// single-column primary keys, a missing field falls back to the row's
    /// dedicated `id` value. Extracted values are canonicalized so they match
    /// [`Self::key`] used for window dedup.
    pub fn from_row(row: &UniversalRow, pk_columns: &[String]) -> Result<Self> {
        let mut values = Vec::with_capacity(pk_columns.len());
        for col in pk_columns {
            if let Some(v) = row.fields.get(col) {
                values.push(canonicalize_pk_value(v));
            } else if pk_columns.len() == 1 {
                values.push(canonicalize_pk_value(&row.id));
            } else {
                anyhow::bail!(
                    "primary key column '{col}' not found in row for table '{}'",
                    row.table
                );
            }
        }
        Ok(Self(values))
    }
}

/// Reduce a primary-key scalar to the canonical kinds used for buffer dedup
/// (`Int64` / `Uuid` / `Text`), matching how most CDC backends normalize
/// stream keys relative to snapshot reads.
fn canonicalize_pk_value(value: &UniversalValue) -> UniversalValue {
    match value {
        UniversalValue::Int8 { value, .. } => UniversalValue::Int64(*value as i64),
        UniversalValue::Int16(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int32(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int64(v) => UniversalValue::Int64(*v),
        UniversalValue::Uuid(u) => UniversalValue::Uuid(*u),
        UniversalValue::Char { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::VarChar { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::Text(s) => UniversalValue::Text(s.clone()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod pk_key_tests {
    use super::*;

    #[test]
    fn key_equates_int_widths() {
        let i32 = PkTuple::new(vec![UniversalValue::Int32(42)]);
        let i64 = PkTuple::new(vec![UniversalValue::Int64(42)]);
        assert_eq!(i32.key(), i64.key());
    }

    #[test]
    fn key_equates_string_kinds() {
        let text = PkTuple::new(vec![UniversalValue::Text("acct-001".into())]);
        let varchar = PkTuple::new(vec![UniversalValue::VarChar {
            value: "acct-001".into(),
            length: 32,
        }]);
        let char_v = PkTuple::new(vec![UniversalValue::Char {
            value: "acct-001".into(),
            length: 32,
        }]);
        assert_eq!(text.key(), varchar.key());
        assert_eq!(text.key(), char_v.key());
    }

    #[test]
    fn from_row_canonicalizes_field_pk() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("id".to_string(), UniversalValue::Int32(7));
        let row = UniversalRow::new("users", 0, UniversalValue::Int32(7), fields);
        let pk = PkTuple::from_row(&row, &["id".to_string()]).unwrap();
        assert_eq!(pk.0, vec![UniversalValue::Int64(7)]);
        assert_eq!(pk.key(), PkTuple::new(vec![UniversalValue::Int64(7)]).key());
    }
}

/// Which watermark is being written around a chunk read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatermarkKind {
    /// Written immediately before a chunk is read; opens the dedup window.
    Low,
    /// Written immediately after a chunk is read; closes the dedup window.
    High,
}

/// A single change observed on the source's change stream.
///
/// Carries the stream position, the originating table and primary key, and the
/// universal change to apply. Watermark rows surface here too: a watermark
/// event is one whose primary key is the single watermark UUID
/// (see [`PkTuple::single_uuid`]).
#[derive(Debug, Clone)]
pub struct ReconciliationEvent<P: ReconciliationPos> {
    /// Reconciliation stream position of this event.
    pub position: P,
    /// Table the change applies to.
    pub table: String,
    /// Primary key of the affected row.
    pub pk: PkTuple,
    /// The change to apply to the sink.
    pub change: UniversalChange,
}

/// An inbound ad-hoc snapshot signal (e.g. a Debezium-style `execute-snapshot`
/// request) asking the framework to snapshot additional tables while streaming
/// continues.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSignal {
    /// Opaque signal identifier (for de-duplication / acknowledgement).
    pub id: String,
    /// Tables (data collections) the signal requests be snapshotted.
    pub tables: Vec<String>,
}
