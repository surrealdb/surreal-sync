//! MongoDB populator for load testing.

use crate::error::MongoDBPopulatorError;
use crate::insert::{count_documents, drop_collection, insert_batch, DEFAULT_BATCH_SIZE};
use bson::Document;
use loadtest_generator::DataGenerator;
use mongodb::{Client, Collection, Database};
use std::time::{Duration, Instant};
use sync_core::{InternalRow, SyncSchema};
use tracing::{debug, info};

/// Metrics from a populate operation.
#[derive(Debug, Clone, Default)]
pub struct PopulateMetrics {
    /// Number of documents inserted.
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

/// MongoDB populator that generates and inserts test data.
pub struct MongoDBPopulator {
    database: Database,
    schema: SyncSchema,
    generator: DataGenerator,
    batch_size: usize,
}

impl MongoDBPopulator {
    /// Create a new MongoDB populator.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - MongoDB connection string (e.g., "mongodb://root:root@localhost:27017")
    /// * `database_name` - Name of the database to use
    /// * `schema` - Load test schema defining collections and field generators
    /// * `seed` - Random seed for deterministic generation
    ///
    /// # Example
    ///
    /// ```ignore
    /// let populator = MongoDBPopulator::new(
    ///     "mongodb://root:root@localhost:27017",
    ///     "testdb",
    ///     schema,
    ///     42,
    /// ).await?;
    /// ```
    pub async fn new(
        connection_string: &str,
        database_name: &str,
        schema: SyncSchema,
        seed: u64,
    ) -> Result<Self, MongoDBPopulatorError> {
        let client = Client::with_uri_str(connection_string).await?;
        let database = client.database(database_name);

        // Test connection
        database.list_collection_names().await?;

        let generator = DataGenerator::new(schema.clone(), seed);

        Ok(Self {
            database,
            schema,
            generator,
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Create a new MongoDB populator with an existing database handle.
    pub fn with_database(database: Database, schema: SyncSchema, seed: u64) -> Self {
        let generator = DataGenerator::new(schema.clone(), seed);
        Self {
            database,
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
    pub fn schema(&self) -> &SyncSchema {
        &self.schema
    }

    /// Get the collection for a given table name.
    fn get_collection(&self, table_name: &str) -> Collection<Document> {
        self.database.collection(table_name)
    }

    /// Drop the collection if it exists.
    pub async fn drop_collection(
        &self,
        collection_name: &str,
    ) -> Result<(), MongoDBPopulatorError> {
        let collection = self.get_collection(collection_name);
        info!("Dropping collection: {}", collection_name);
        drop_collection(&collection).await
    }

    /// Populate a collection with the specified number of documents.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to populate (must match a table name in schema)
    /// * `count` - Number of documents to insert
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub async fn populate(
        &mut self,
        collection_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, MongoDBPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(collection_name)
            .ok_or_else(|| MongoDBPopulatorError::CollectionNotFound(collection_name.to_string()))?
            .clone();

        let collection = self.get_collection(collection_name);

        info!(
            "Populating collection '{}' with {} documents (batch size: {})",
            collection_name, count, self.batch_size
        );

        let mut remaining = count;
        let mut generation_time = Duration::ZERO;
        let mut insert_time = Duration::ZERO;

        while remaining > 0 {
            let batch_count = std::cmp::min(remaining, self.batch_size as u64);

            // Generate rows
            let gen_start = Instant::now();
            let rows: Vec<InternalRow> = self
                .generator
                .internal_rows(collection_name, batch_count)
                .map_err(|e| MongoDBPopulatorError::Generator(e.to_string()))?
                .collect();
            generation_time += gen_start.elapsed();

            // Insert rows
            let insert_start = Instant::now();
            let inserted = insert_batch(&collection, &table_schema, &rows).await?;
            insert_time += insert_start.elapsed();

            metrics.rows_inserted += inserted;
            metrics.batch_count += 1;
            remaining -= batch_count;

            debug!(
                "Batch {} complete: {} documents inserted, {} remaining",
                metrics.batch_count, inserted, remaining
            );
        }

        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.insert_duration = insert_time;

        info!(
            "Population complete: {} documents in {:?} ({:.2} docs/sec)",
            metrics.rows_inserted,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }

    /// Populate incrementally (append documents to existing data).
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental sync scenarios.
    pub async fn populate_incremental(
        &mut self,
        collection_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, MongoDBPopulatorError> {
        info!(
            "Incremental population starting at index {}",
            self.generator.current_index()
        );
        self.populate(collection_name, count).await
    }

    /// Get the document count for a collection.
    pub async fn document_count(
        &self,
        collection_name: &str,
    ) -> Result<u64, MongoDBPopulatorError> {
        let collection = self.get_collection(collection_name);
        count_documents(&collection).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> SyncSchema {
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
        SyncSchema::from_yaml(yaml).unwrap()
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
