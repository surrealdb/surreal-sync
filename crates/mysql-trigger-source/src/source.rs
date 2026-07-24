//! MySQL incremental sync implementation using audit table-based change tracking
//!
//! This module provides reliable incremental synchronization from MySQL to SurrealDB
//! using a trigger-based approach that works across all MySQL versions and deployment environments.
//!
//! ## Conversion Flow
//!
//! This implementation uses the unified TypedValue conversion path:
//! ```text
//! JSON (from audit table) → TypedValue (json-types) → SurrealDB values
//! ```
//!
//! ## Implementation Approach
//!
//! The implementation uses database triggers and audit tables to capture data changes.
//! The implementation works with all MySQL versions (5.6+)
//! and requires only standard SQL operations, avoiding binlog parsing.
//!
//! Changes are captured by creating audit tables that track INSERT, UPDATE, and DELETE operations.
//! Database triggers automatically populate these tables when data changes occur.

use std::collections::HashMap;

use super::checkpoint::MySQLCheckpoint;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use json_types::{convert_id_to_value, convert_id_with_database_schema, JsonValueWithSchema};
use log::info;
use mysql_async::{prelude::*, Conn, Pool, Row as MysqlRow, Value as MysqlValue};
use sync_core::{Change, ChangeOp, DatabaseSchema, Type, TypedValue, Value};

// ============================================================================
// Traits for MySQL incremental sync
//
// These traits are duplicated here (rather than shared via a common crate)
// because they use database-specific checkpoint types and each database's
// implementation is self-contained. The duplication is minimal (~20 lines)
// and simplifies the dependency graph.
// ============================================================================

/// Trait for MySQL incremental sync source
///
/// Provides the interface for initializing the source, getting a change stream,
/// and managing checkpoints for reliable resumption.
#[async_trait]
pub trait IncrementalSource: Send + Sync {
    /// Get the source database type identifier
    fn source_type(&self) -> &'static str;

    /// Initialize the incremental source (setup tasks like schema collection)
    async fn initialize(&mut self) -> Result<()>;

    /// Get a stream of changes from the source
    async fn get_changes(&mut self) -> Result<Box<dyn ChangeStream>>;

    /// Get the current checkpoint position
    async fn get_checkpoint(&self) -> Result<MySQLCheckpoint>;

    /// Cleanup resources
    async fn cleanup(self) -> Result<()>;
}

/// Trait for a stream of changes from MySQL
#[async_trait]
pub trait ChangeStream: Send + Sync {
    /// Get the next change event from the stream
    /// Returns None when no more changes are available
    async fn next(&mut self) -> Option<Result<Change>>;

    /// Like [`ChangeStream::next`], but also yields the source `sequence_id`
    /// (stream position) of the change, so callers can track per-change
    /// positions for watermark-based reconciliation.
    /// Returns None when no more changes are available.
    async fn next_with_sequence_id(&mut self) -> Option<Result<(i64, Change)>>;

    /// Get the current sink-safe checkpoint of the stream.
    ///
    /// This advances only via [`ChangeStream::commit_sunk`], never on fetch —
    /// so it never reports past events that are still transforming or unsunk.
    fn checkpoint(&self) -> Option<MySQLCheckpoint>;

    /// Record that events through `sequence_id` were successfully sunk.
    ///
    /// The read-head used for audit-table pagination may already be ahead;
    /// only this watermark is authoritative for "how far we've safely progressed."
    fn commit_sunk(&mut self, sequence_id: i64) {
        let _ = sequence_id;
    }
}

/// MySQL incremental sync implementation using audit table-based change tracking
pub struct MySQLIncrementalSource {
    pool: Pool,
    server_id: u32,
    sequence_id: i64,
    database_schema: Option<DatabaseSchema>,
    id_column_overrides: sync_core::IdColumnOverrides,
}

impl MySQLIncrementalSource {
    pub fn new(pool: Pool, initial_sequence_id: i64) -> Self {
        Self::with_id_column_overrides(pool, initial_sequence_id, Default::default())
    }

    pub fn with_id_column_overrides(
        pool: Pool,
        initial_sequence_id: i64,
        id_column_overrides: sync_core::IdColumnOverrides,
    ) -> Self {
        let server_id = rand::random::<u32>() % 1000000 + 1000000; // Random ID between 1M-2M

        Self {
            pool,
            server_id,
            sequence_id: initial_sequence_id,
            database_schema: None,
            id_column_overrides,
        }
    }
}

#[async_trait]
impl IncrementalSource for MySQLIncrementalSource {
    fn source_type(&self) -> &'static str {
        "mysql"
    }

    async fn initialize(&mut self) -> Result<()> {
        // Check MySQL connection
        let _conn = self.pool.get_conn().await?;
        info!("MySQL connection established for trigger-based incremental sync");

        // Collect database schema for type-aware conversion
        let mut conn = self.pool.get_conn().await?;
        let mut schema = super::schema::collect_mysql_database_schema(&mut conn).await?;
        sync_core::apply_id_column_overrides(&mut schema, &self.id_column_overrides)
            .map_err(|e| anyhow!("{e}"))?;
        self.database_schema = Some(schema);
        info!("Collected MySQL database schema for type-aware conversion");

        Ok(())
    }

    async fn get_changes(&mut self) -> Result<Box<dyn ChangeStream>> {
        let starting_sequence_id = self.sequence_id;

        let stream = MySQLChangeStream::new(
            self.pool.clone(),
            self.server_id,
            starting_sequence_id,
            self.database_schema.clone(),
        )
        .await?;

        Ok(Box::new(stream))
    }

    async fn get_checkpoint(&self) -> Result<MySQLCheckpoint> {
        Ok(MySQLCheckpoint {
            sequence_id: self.sequence_id,
            timestamp: Utc::now(),
        })
    }

    async fn cleanup(self) -> Result<()> {
        // No cleanup needed for trigger-based sync
        Ok(())
    }
}

pub struct MySQLChangeStream {
    #[allow(dead_code)]
    pool: Pool,
    #[allow(dead_code)]
    connection: Option<Conn>,
    #[allow(dead_code)]
    server_id: u32,
    /// Buffered changes, each paired with its source `sequence_id`.
    buffer: Vec<(i64, Change)>,
    /// Highest sequence_id fetched into the buffer (audit pagination cursor).
    read_sequence_id: i64,
    /// Highest sequence_id successfully sunk (authoritative resume watermark).
    sunk_sequence_id: i64,
    database_schema: Option<DatabaseSchema>,
}

impl MySQLChangeStream {
    async fn new(
        pool: Pool,
        server_id: u32,
        starting_sequence_id: i64,
        database_schema: Option<DatabaseSchema>,
    ) -> Result<Self> {
        let connection = pool.get_conn().await?;

        Ok(Self {
            pool,
            connection: Some(connection),
            server_id,
            buffer: Vec::new(),
            read_sequence_id: starting_sequence_id,
            sunk_sequence_id: starting_sequence_id,
            database_schema,
        })
    }

    /// Convert JSON value to TypedValue using schema information
    fn json_to_typed_value(
        &self,
        value: serde_json::Value,
        field_name: &str,
        table_name: &str,
    ) -> Result<TypedValue> {
        // Get the Type from schema to check for SET columns
        let column_type = self
            .database_schema
            .as_ref()
            .and_then(|s| s.get_table(table_name))
            .and_then(|ts| ts.get_column_type(field_name));

        // Handle SET columns specially - MySQL JSON_OBJECT stores SET as comma-separated string
        if let Some(Type::Set { .. }) = column_type {
            if let serde_json::Value::String(s) = &value {
                let values: Vec<String> = if s.is_empty() {
                    Vec::new()
                } else {
                    s.split(',').map(|v| v.to_string()).collect()
                };
                return Ok(sync_core::TypedValue::set(values, vec![]));
            }
        }

        // Get the sync type from schema for standard conversion
        let sync_type = column_type.cloned().unwrap_or(Type::Text); // Default to text if not found

        // Use json-types for conversion
        let jvs = JsonValueWithSchema::new(value, sync_type);
        Ok(jvs.to_typed_value())
    }

    /// Convert JSON object to HashMap of TypedValue
    fn json_object_to_typed_values(
        &self,
        obj: serde_json::Map<String, serde_json::Value>,
        table_name: &str,
        pk_columns: &[String],
        row_id: &str,
    ) -> Result<HashMap<String, TypedValue>> {
        let mut result = HashMap::new();

        for (key, val) in obj {
            // Skip primary-key columns — they form the SurrealDB record ID.
            // SurrealDB doesn't allow 'id' in content when using UPSERT $record_id.
            if pk_columns.iter().any(|c| c == &key) {
                continue;
            }
            // Also skip a literal "id" field when it duplicates a single-column
            // PK that was recorded under a different name in row_id (legacy).
            if key == "id" && pk_columns.len() == 1 {
                let json_id_str = match &val {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    other => format!("{other}"),
                };
                if row_id == json_id_str || pk_columns[0] == "id" {
                    continue;
                }
            }

            let tv = self.json_to_typed_value(val, &key, table_name)?;
            result.insert(key, tv);
        }

        Ok(result)
    }

    /// Resolve ordered PK column names for a table from schema (fallback: `["id"]`).
    fn pk_columns_for(&self, table_name: &str) -> Vec<String> {
        self.database_schema
            .as_ref()
            .and_then(|s| s.get_table(table_name))
            .map(|t| {
                t.primary_key_column_names()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_else(|| vec!["id".to_string()])
    }

    /// Convert audit `row_id` (scalar string or JSON array) to a Value ID.
    fn row_id_to_universal(
        &self,
        row_id: &str,
        table_name: &str,
        pk_columns: &[String],
    ) -> Result<Value> {
        let schema = self.database_schema.as_ref();
        if pk_columns.len() <= 1 {
            let col = pk_columns.first().map(|s| s.as_str()).unwrap_or("id");
            if let Some(schema) = schema {
                return convert_id_with_database_schema(row_id, table_name, col, schema);
            }
            return Ok(Value::Text(row_id.to_string()));
        }

        // Composite: triggers store JSON_ARRAY(...) in row_id.
        let parsed: serde_json::Value = serde_json::from_str(row_id).map_err(|e| {
            anyhow!("composite row_id '{row_id}' for table '{table_name}' is not valid JSON: {e}")
        })?;
        let elements = parsed
            .as_array()
            .ok_or_else(|| anyhow!("composite row_id '{row_id}' is not a JSON array"))?;
        if elements.len() != pk_columns.len() {
            return Err(anyhow!(
                "composite row_id '{row_id}' has {} parts but table '{table_name}' has {} key columns",
                elements.len(),
                pk_columns.len()
            ));
        }
        let mut values = Vec::with_capacity(pk_columns.len());
        for (col, elem) in pk_columns.iter().zip(elements.iter()) {
            let id_str = match elem {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                other => other.to_string(),
            };
            let part = if let Some(schema) = schema {
                convert_id_with_database_schema(&id_str, table_name, col, schema)?
            } else {
                let ty = Type::Text;
                convert_id_to_value(&id_str, table_name, &ty)?
            };
            values.push(part);
        }
        Ok(Value::Array {
            elements: values,
            element_type: Box::new(Type::Text),
        })
    }

    async fn fetch_changes(&mut self) -> Result<Vec<(i64, Change)>> {
        let conn = self
            .connection
            .as_mut()
            .ok_or_else(|| anyhow!("No connection available"))?;

        // Check if audit table exists before querying
        let table_exists: Vec<MysqlRow> = conn
            .query("SELECT 1 FROM information_schema.tables WHERE table_name = 'surreal_sync_changes' AND table_schema = DATABASE()")
            .await?;

        if table_exists.is_empty() {
            // Audit table doesn't exist yet, return empty changes
            return Ok(Vec::new());
        }

        // Use trigger-based change capture via audit table
        // This is more reliable than trying to parse binlog directly
        let query =
            "SELECT sequence_id, table_name, operation, row_id, old_data, new_data, changed_at
             FROM surreal_sync_changes
             WHERE sequence_id > ?
             ORDER BY sequence_id
             LIMIT 100"
                .to_string();

        let rows: Vec<MysqlRow> = conn.exec(query, (self.read_sequence_id,)).await?;
        let mut changes = Vec::new();

        for row in rows {
            let sequence_id: i64 = row.get(0).ok_or_else(|| anyhow!("Missing sequence_id"))?;
            let table_name: String = row.get(1).ok_or_else(|| anyhow!("Missing table_name"))?;
            let operation: String = row.get(2).ok_or_else(|| anyhow!("Missing operation"))?;
            let row_id: String = row.get(3).ok_or_else(|| anyhow!("Missing row_id"))?;
            let _old_data: Option<MysqlValue> = row.get(4);
            let new_data: Option<MysqlValue> = row.get(5);

            let op = match operation.as_str() {
                "INSERT" => ChangeOp::Create,
                "UPDATE" => ChangeOp::Update,
                "DELETE" => ChangeOp::Delete,
                _ => {
                    return Err(anyhow!("Unknown operation type: {operation}"));
                }
            };

            // Convert JSON data to Value map using TypedValue conversion flow
            let pk_columns = self.pk_columns_for(&table_name);
            let universal_data: Option<HashMap<String, Value>> = match operation.as_str() {
                "INSERT" | "UPDATE" => {
                    if let Some(MysqlValue::Bytes(json_data)) = new_data {
                        if let Ok(json_value) =
                            serde_json::from_slice::<serde_json::Value>(&json_data)
                        {
                            // Ensure we have schema
                            if self.database_schema.is_none()
                                || self
                                    .database_schema
                                    .as_ref()
                                    .and_then(|s| s.get_table(&table_name))
                                    .is_none()
                            {
                                anyhow::bail!(
                                    "No schema information available for table '{table_name}'. Cannot convert data.",
                                );
                            }

                            match json_value {
                                serde_json::Value::Object(map) => {
                                    // Step 1: JSON → HashMap<String, TypedValue>
                                    let typed_values = self.json_object_to_typed_values(
                                        map,
                                        &table_name,
                                        &pk_columns,
                                        &row_id,
                                    )?;

                                    // Step 2: HashMap<String, TypedValue> → HashMap<String, Value>
                                    let universal_map: HashMap<String, Value> = typed_values
                                        .into_iter()
                                        .map(|(k, tv)| (k, tv.value))
                                        .collect();
                                    Some(universal_map)
                                }
                                _ => {
                                    anyhow::bail!(
                                        "Expected JSON object for row data in table '{table_name}'",
                                    );
                                }
                            }
                        } else {
                            anyhow::bail!("Invalid JSON data in new_data for table '{table_name}'");
                        }
                    } else {
                        anyhow::bail!("Missing new_data for INSERT/UPDATE in table '{table_name}'");
                    }
                }
                "DELETE" => None, // No data for deletes
                _ => anyhow::bail!("Unknown operation type: {operation}"),
            };

            // Convert the string row_id to Value using schema information
            // (scalar or JSON-array composite keys).
            let record_id: Value = self.row_id_to_universal(&row_id, &table_name, &pk_columns)?;

            // Create change record using universal types
            changes.push((
                sequence_id,
                Change::new(op, table_name.clone(), record_id, universal_data),
            ));

            // Advance read-head only; sunk watermark waits for commit_sunk.
            self.read_sequence_id = sequence_id;
        }

        Ok(changes)
    }
}

#[async_trait]
impl ChangeStream for MySQLChangeStream {
    async fn next(&mut self) -> Option<Result<Change>> {
        match self.next_with_sequence_id().await {
            Some(Ok((_seq, change))) => Some(Ok(change)),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    async fn next_with_sequence_id(&mut self) -> Option<Result<(i64, Change)>> {
        // Return buffered changes first
        if !self.buffer.is_empty() {
            return Some(Ok(self.buffer.remove(0)));
        }

        // Fetch new changes
        match self.fetch_changes().await {
            Ok(mut changes) => {
                if changes.is_empty() {
                    None
                } else {
                    self.buffer.append(&mut changes);
                    Some(Ok(self.buffer.remove(0)))
                }
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn checkpoint(&self) -> Option<MySQLCheckpoint> {
        Some(MySQLCheckpoint {
            sequence_id: self.sunk_sequence_id,
            timestamp: Utc::now(),
        })
    }

    fn commit_sunk(&mut self, sequence_id: i64) {
        if sequence_id > self.sunk_sequence_id {
            self.sunk_sequence_id = sequence_id;
        }
    }
}

/// Fallback implementation using trigger-based change tracking for MySQL
#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::MySQLCheckpoint;
    use checkpoint::Checkpoint;

    #[tokio::test]
    async fn test_sequence_checkpoint() {
        let sequence_id = 1234i64;
        let checkpoint = MySQLCheckpoint {
            sequence_id,
            timestamp: Utc::now(),
        };

        assert_eq!(checkpoint.sequence_id, sequence_id);

        // Test CLI string roundtrip
        let cli_str = checkpoint.to_cli_string();
        let parsed = MySQLCheckpoint::from_cli_string(&cli_str).unwrap();
        assert_eq!(parsed.sequence_id, sequence_id);
    }

    #[tokio::test]
    async fn test_initial_checkpoint() {
        let checkpoint = MySQLCheckpoint {
            sequence_id: 0,
            timestamp: Utc::now(),
        };

        assert_eq!(checkpoint.sequence_id, 0);
    }

    #[tokio::test]
    async fn test_checkpoint_json_serialization() {
        use checkpoint::{CheckpointFile, SyncPhase};

        let checkpoint = MySQLCheckpoint {
            sequence_id: 156,
            timestamp: Utc::now(),
        };

        // Test direct JSON serialization
        let json = serde_json::to_string(&checkpoint).unwrap();
        println!("Direct JSON: {json}");

        // Test deserialization
        let deserialized: MySQLCheckpoint = serde_json::from_str(&json).unwrap();
        println!("Direct deserialization: {deserialized:?}");
        assert_eq!(deserialized.sequence_id, 156);

        // Test CheckpointFile format (new approach)
        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart).unwrap();
        let file_json = serde_json::to_string_pretty(&file).unwrap();
        println!("File format JSON: {file_json}");

        // Test parsing from CheckpointFile
        let parsed_file: CheckpointFile = serde_json::from_str(&file_json).unwrap();
        let parsed_checkpoint: MySQLCheckpoint = parsed_file.parse().unwrap();
        println!("From file deserialization: {parsed_checkpoint:?}");
        assert_eq!(parsed_checkpoint.sequence_id, 156);
    }
}
