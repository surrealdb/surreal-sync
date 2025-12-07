//! PostgreSQL populator for load testing.

use crate::error::PostgreSQLPopulatorError;
use crate::insert::{generate_create_table, generate_drop_table, insert_batch, DEFAULT_BATCH_SIZE};
use loadtest_generator::DataGenerator;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sync_core::{UniversalRow, Schema};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info};

/// Metrics from a populate operation.
#[derive(Debug, Clone, Default)]
pub struct PopulateMetrics {
    /// Number of rows inserted.
    pub rows_inserted: u64,
    /// Total time taken.
    pub total_duration: Duration,
    /// Time spent generating data.
    pub generation_duration: Duration,
    /// Time spent inserting data.
    pub insert_duration: Duration,
    /// Number of batches executed.
    pub batch_count: u64,
}

impl PopulateMetrics {
    /// Calculate rows per second.
    pub fn rows_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.rows_inserted as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// PostgreSQL populator that generates and inserts test data.
pub struct PostgreSQLPopulator {
    client: Arc<Mutex<Client>>,
    schema: Schema,
    generator: DataGenerator,
    batch_size: usize,
}

impl PostgreSQLPopulator {
    /// Create a new PostgreSQL populator.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - PostgreSQL connection string (e.g., "host=localhost user=postgres password=postgres dbname=testdb")
    /// * `schema` - Load test schema defining tables and field generators
    /// * `seed` - Random seed for deterministic generation
    ///
    /// # Example
    ///
    /// ```ignore
    /// let populator = PostgreSQLPopulator::new(
    ///     "host=localhost user=postgres password=postgres dbname=testdb",
    ///     schema,
    ///     42,
    /// ).await?;
    /// ```
    pub async fn new(
        connection_string: &str,
        schema: Schema,
        seed: u64,
    ) -> Result<Self, PostgreSQLPopulatorError> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        // Test connection
        client.simple_query("SELECT 1").await?;

        let generator = DataGenerator::new(schema.clone(), seed);

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            schema,
            generator,
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Create a new PostgreSQL populator with an existing client.
    pub fn with_client(client: Arc<Mutex<Client>>, schema: Schema, seed: u64) -> Self {
        let generator = DataGenerator::new(schema.clone(), seed);
        Self {
            client,
            schema,
            generator,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the batch size for INSERT operations.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the starting index for generation (for incremental population).
    pub fn with_start_index(mut self, index: u64) -> Self {
        self.generator = std::mem::replace(
            &mut self.generator,
            DataGenerator::new(self.schema.clone(), 0),
        )
        .with_start_index(index);
        self
    }

    /// Get the current generation index.
    pub fn current_index(&self) -> u64 {
        self.generator.current_index()
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Drop the table if it exists.
    pub async fn drop_table(&self, table_name: &str) -> Result<(), PostgreSQLPopulatorError> {
        let client = self.client.lock().await;
        let sql = generate_drop_table(table_name);
        info!("Dropping table: {}", table_name);
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    /// Create the table for the given table name.
    pub async fn create_table(&self, table_name: &str) -> Result<(), PostgreSQLPopulatorError> {
        let sql = generate_create_table(&self.schema, table_name)
            .ok_or_else(|| PostgreSQLPopulatorError::TableNotFound(table_name.to_string()))?;

        let client = self.client.lock().await;
        info!("Creating table: {}", table_name);
        debug!("DDL: {}", sql);
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    /// Drop and recreate the table.
    pub async fn recreate_table(&self, table_name: &str) -> Result<(), PostgreSQLPopulatorError> {
        self.drop_table(table_name).await?;
        self.create_table(table_name).await?;
        Ok(())
    }

    /// Populate a table with the specified number of rows.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to populate
    /// * `count` - Number of rows to insert
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub async fn populate(
        &mut self,
        table_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, PostgreSQLPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| PostgreSQLPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        info!(
            "Populating table '{}' with {} rows (batch size: {})",
            table_name, count, self.batch_size
        );

        let mut remaining = count;
        let mut generation_time = Duration::ZERO;
        let mut insert_time = Duration::ZERO;

        while remaining > 0 {
            let batch_count = std::cmp::min(remaining, self.batch_size as u64);

            // Generate rows
            let gen_start = Instant::now();
            let rows: Vec<UniversalRow> = self
                .generator
                .internal_rows(table_name, batch_count)
                .map_err(|e| PostgreSQLPopulatorError::Generator(e.to_string()))?
                .collect();
            generation_time += gen_start.elapsed();

            // Insert rows
            let insert_start = Instant::now();
            let client = self.client.lock().await;
            let inserted = insert_batch(&client, &table_schema, &rows).await?;
            drop(client); // Release lock
            insert_time += insert_start.elapsed();

            metrics.rows_inserted += inserted;
            metrics.batch_count += 1;
            remaining -= batch_count;

            debug!(
                "Batch {} complete: {} rows inserted, {} remaining",
                metrics.batch_count, inserted, remaining
            );
        }

        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.insert_duration = insert_time;

        info!(
            "Population complete: {} rows in {:?} ({:.2} rows/sec)",
            metrics.rows_inserted,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }

    /// Populate incrementally (append rows to existing data).
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental sync scenarios.
    pub async fn populate_incremental(
        &mut self,
        table_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, PostgreSQLPopulatorError> {
        info!(
            "Incremental population starting at index {}",
            self.generator.current_index()
        );
        self.populate(table_name, count).await
    }

    /// Truncate the table (delete all rows).
    pub async fn truncate_table(&self, table_name: &str) -> Result<(), PostgreSQLPopulatorError> {
        let client = self.client.lock().await;
        let sql = format!("TRUNCATE TABLE \"{table_name}\"");
        info!("Truncating table: {}", table_name);
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    /// Get the row count for a table.
    pub async fn row_count(&self, table_name: &str) -> Result<u64, PostgreSQLPopulatorError> {
        let client = self.client.lock().await;
        let sql = format!("SELECT COUNT(*) FROM \"{table_name}\"");
        let row = client.query_one(&sql, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_metrics() {
        let metrics = PopulateMetrics {
            rows_inserted: 1000,
            total_duration: Duration::from_secs(10),
            generation_duration: Duration::from_secs(2),
            insert_duration: Duration::from_secs(8),
            batch_count: 10,
        };

        assert_eq!(metrics.rows_per_second(), 100.0);
    }

    #[test]
    fn test_schema_validation() {
        let schema = test_schema();
        assert!(schema.get_table("users").is_some());
        assert!(schema.get_table("nonexistent").is_none());
    }
}
