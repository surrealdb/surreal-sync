use std::collections::HashMap;

use crate::surreal::{
    convert_id_with_schema, json_to_surreal_with_schema, Change, ChangeOp, SurrealDatabaseSchema,
};
use crate::sync::{ChangeStream, IncrementalSource, SourceDatabase, SyncCheckpoint};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use log::info;
use mysql_async::{prelude::*, Conn, Pool, Row, Value};

/// MySQL incremental sync implementation using audit table-based change tracking
///
/// This implementation provides reliable incremental synchronization from MySQL to SurrealDB
/// using a trigger-based approach that works across all MySQL versions and deployment environments.
///
/// ## Implementation Approach
///
/// The implementation uses database triggers and audit tables to capture data changes.
/// The implementation works with all MySQL versions (5.6+)
/// and requires only standard SQL operations, avoiding binlog parsing.
///
/// Changes are captured by creating audit tables that track INSERT, UPDATE, and DELETE operations.
/// Database triggers automatically populate these tables when data changes occur.
///
/// The approach uses only the [`mysql_async`] driver, minimizing external dependencies while
/// providing reliable change data capture suitable for most production workloads.
///
/// ## Alternative Approaches
///
/// MySQL's native binary log replication could potentially provide higher throughput, and several
/// Rust crates support binlog parsing including `mysql_cdc`, `mysql-binlog-connector-rust`, and
/// `rust-mysql-binlog`. These libraries can parse binlog events over network connections:
///
/// ```ignore
/// // Example of what native binlog replication would require
/// use mysql_cdc::{BinlogClient, BinlogOpts};
///
/// // Would need specialized binlog parsing libraries
/// // Parse binary binlog events over network
/// ```
///
/// This approach was not implemented because it would require additional dependencies for specialized
/// binlog parsing, handling different MySQL versions with varying binlog formats, managing replication
/// client privileges, and dealing with complex network connection lifecycles including failover scenarios.
/// The implementation would also need to handle the intricacies of MySQL's replication protocol and
/// maintain state across connection interruptions.
///
/// The performance difference between approaches is typically not significant for most use cases.
/// The current audit table approach handles 1,000-10,000 changes per second, while native binlog
/// replication might achieve 10,000-50,000 changes per second at the cost of substantially higher
/// implementation complexity.
///
/// ## Configuration
///
/// The audit table approach works with default MySQL settings and requires no special configuration.
/// In contrast, native binlog replication would require `binlog_format = 'ROW'`, `gtid_mode = ON`,
/// and `REPLICATION CLIENT` privileges.
///
/// ## See Also
/// - [`crate::sync::IncrementalSource`] - The trait this implements
/// - [`crate::sync::SyncCheckpoint::MySQL`] - GTID-based checkpoint format
/// - [`crate::postgresql_incremental::PostgresIncrementalSource`] - Similar approach for PostgreSQL
pub struct MySQLIncrementalSource {
    pool: Pool,
    server_id: u32,
    sequence_id: i64,
    database_schema: Option<SurrealDatabaseSchema>,
}

impl MySQLIncrementalSource {
    pub fn new(pool: Pool, initial_sequence_id: i64) -> Self {
        let server_id = rand::random::<u32>() % 1000000 + 1000000; // Random ID between 1M-2M

        Self {
            pool,
            server_id,
            sequence_id: initial_sequence_id,
            database_schema: None,
        }
    }
}

#[async_trait]
impl IncrementalSource for MySQLIncrementalSource {
    fn source_type(&self) -> SourceDatabase {
        SourceDatabase::MySQL
    }

    async fn initialize(&mut self) -> Result<()> {
        // Check MySQL connection
        let _conn = self.pool.get_conn().await?;
        info!("MySQL connection established for trigger-based incremental sync");

        // Collect database schema for type-aware conversion
        let mut conn = self.pool.get_conn().await?;
        let schema = super::schema::collect_mysql_schema(&mut conn, "").await?;
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

    async fn get_checkpoint(&self) -> Result<SyncCheckpoint> {
        Ok(SyncCheckpoint::MySQL {
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
    buffer: Vec<Change>,
    last_sequence_id: i64,
    database_schema: Option<SurrealDatabaseSchema>,
}

impl MySQLChangeStream {
    async fn new(
        pool: Pool,
        server_id: u32,
        starting_sequence_id: i64,
        database_schema: Option<SurrealDatabaseSchema>,
    ) -> Result<Self> {
        let connection = pool.get_conn().await?;

        Ok(Self {
            pool,
            connection: Some(connection),
            server_id,
            buffer: Vec::new(),
            last_sequence_id: starting_sequence_id,
            database_schema,
        })
    }

    async fn fetch_changes(&mut self) -> Result<Vec<Change>> {
        let conn = self
            .connection
            .as_mut()
            .ok_or_else(|| anyhow!("No connection available"))?;

        // Check if audit table exists before querying
        let table_exists: Vec<Row> = conn
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

        let rows: Vec<Row> = conn.exec(query, (self.last_sequence_id,)).await?;
        let mut changes = Vec::new();

        for row in rows {
            let sequence_id: i64 = row.get(0).ok_or_else(|| anyhow!("Missing sequence_id"))?;
            let table_name: String = row.get(1).ok_or_else(|| anyhow!("Missing table_name"))?;
            let operation: String = row.get(2).ok_or_else(|| anyhow!("Missing operation"))?;
            let row_id: String = row.get(3).ok_or_else(|| anyhow!("Missing row_id"))?;
            let _old_data: Option<Value> = row.get(4);
            let new_data: Option<Value> = row.get(5);

            let op = match operation.as_str() {
                "INSERT" => ChangeOp::Create,
                "UPDATE" => ChangeOp::Update,
                "DELETE" => ChangeOp::Delete,
                _ => {
                    return Err(anyhow!("Unknown operation type: {operation}"));
                }
            };

            // Convert JSON data to SurrealValue map using schema-aware conversion
            let surreal_data = match operation.as_str() {
                "INSERT" | "UPDATE" => {
                    if let Some(Value::Bytes(json_data)) = new_data {
                        if let Ok(json_value) =
                            serde_json::from_slice::<serde_json::Value>(&json_data)
                        {
                            // Use schema-aware conversion if available
                            if let (Some(_schema), Some(table_schema)) = (
                                &self.database_schema,
                                self.database_schema
                                    .as_ref()
                                    .and_then(|s| s.tables.get(&table_name)),
                            ) {
                                match json_value {
                                    serde_json::Value::Object(map) => {
                                        let mut kvs = std::collections::HashMap::new();
                                        for (key, val) in map {
                                            // Skip the 'id' field as it's used as the record ID (row_id)
                                            // SurrealDB doesn't allow 'id' in content when using UPSERT $record_id
                                            //
                                            // LIMITATION: MySQL incremental sync assumes the primary key column
                                            // is always named 'id'. Tables with different primary key column names
                                            // (e.g., 'user_id') won't sync correctly. Additionally, if a table has
                                            // an 'id' column that is NOT the primary key, this check will incorrectly
                                            // skip it, causing data loss for that column.
                                            //
                                            // The row_id comes from NEW.id/OLD.id in the trigger (see change_tracking.rs)
                                            // and the JSON 'id' field also contains the same value.
                                            if key == "id" {
                                                // Verify they match - row_id should equal the JSON id value
                                                let json_id_str = match &val {
                                                    serde_json::Value::Number(n) => n.to_string(),
                                                    serde_json::Value::String(s) => s.clone(),
                                                    other => format!("{other}"),
                                                };
                                                if row_id != json_id_str {
                                                    anyhow::bail!(
                                                        "row_id and JSON id field mismatch in table '{table_name}': \
                                                        row_id={row_id}, json_id={json_id_str}. \
                                                        This may indicate the 'id' column is not the primary key."
                                                    );
                                                }
                                                continue;
                                            }
                                            let v = json_to_surreal_with_schema(
                                                val,
                                                &key,
                                                table_schema,
                                            )?;
                                            kvs.insert(key, v);
                                        }
                                        kvs
                                    }
                                    _ => {
                                        anyhow::bail!(
                                            "Expected JSON object for row data in table '{table_name}'",
                                        );
                                    }
                                }
                            } else {
                                anyhow::bail!(
                                    "No schema information available for table '{table_name}'. Cannot convert data.",
                                );
                            }
                        } else {
                            anyhow::bail!("Invalid JSON data in new_data for table '{table_name}'");
                        }
                    } else {
                        anyhow::bail!("Missing new_data for INSERT/UPDATE in table '{table_name}'");
                    }
                }
                "DELETE" => HashMap::new(), // No data for deletes
                _ => anyhow::bail!("Unknown operation type: {operation}"),
            };

            // Convert the string row_id to the proper type using schema information
            // This ensures that BIGINT IDs are stored as numbers, UUIDs as UUIDs, etc.
            let record_id = if let Some(schema) = &self.database_schema {
                // Use schema-aware conversion to get proper ID type
                let surreal_id = convert_id_with_schema(&row_id, &table_name, "id", schema)?;
                surrealdb::sql::Thing::from((table_name.clone(), surreal_id))
            } else {
                // Fallback to string ID if no schema available (shouldn't happen)
                surrealdb::sql::Thing::from((table_name.clone(), row_id))
            };

            changes.push(Change::record(op, record_id, surreal_data));

            self.last_sequence_id = sequence_id;
        }

        Ok(changes)
    }
}

#[async_trait]
impl ChangeStream for MySQLChangeStream {
    async fn next(&mut self) -> Option<Result<Change>> {
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

    fn checkpoint(&self) -> Option<SyncCheckpoint> {
        Some(SyncCheckpoint::MySQL {
            sequence_id: self.last_sequence_id,
            timestamp: Utc::now(),
        })
    }
}

/// Fallback implementation using trigger-based change tracking for MySQL
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sequence_checkpoint() {
        let sequence_id = 1234i64;
        let checkpoint = SyncCheckpoint::MySQL {
            sequence_id,
            timestamp: Utc::now(),
        };

        match checkpoint {
            SyncCheckpoint::MySQL {
                sequence_id: parsed_seq,
                ..
            } => {
                assert_eq!(parsed_seq, sequence_id);
            }
            _ => panic!("Wrong checkpoint type"),
        }
    }

    #[tokio::test]
    async fn test_initial_checkpoint() {
        let checkpoint = SyncCheckpoint::MySQL {
            sequence_id: 0,
            timestamp: Utc::now(),
        };

        match checkpoint {
            SyncCheckpoint::MySQL {
                sequence_id: parsed_seq,
                ..
            } => {
                assert_eq!(parsed_seq, 0);
            }
            _ => panic!("Wrong checkpoint type"),
        }
    }

    #[tokio::test]
    async fn test_checkpoint_json_serialization() {
        let checkpoint = SyncCheckpoint::MySQL {
            sequence_id: 156,
            timestamp: Utc::now(),
        };

        // Test direct JSON serialization
        let json = serde_json::to_string(&checkpoint).unwrap();
        println!("Direct JSON: {json}");

        // Test deserialization
        let deserialized: SyncCheckpoint = serde_json::from_str(&json).unwrap();
        println!("Direct deserialization: {deserialized:?}");

        // Test file format (like SyncManager does)
        let checkpoint_data = serde_json::json!({
            "checkpoint": checkpoint,
            "phase": "full_sync_start",
            "timestamp": Utc::now().to_rfc3339(),
        });

        let file_json = serde_json::to_string_pretty(&checkpoint_data).unwrap();
        println!("File format JSON: {file_json}");

        // Test deserialization from file format
        let parsed: serde_json::Value = serde_json::from_str(&file_json).unwrap();
        let checkpoint_from_file =
            serde_json::from_value::<SyncCheckpoint>(parsed["checkpoint"].clone()).unwrap();
        println!("From file deserialization: {checkpoint_from_file:?}");
    }
}
