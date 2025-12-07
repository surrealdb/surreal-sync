//! Neo4j populator for load testing.

use crate::error::Neo4jPopulatorError;
use crate::insert::{count_nodes, delete_all_nodes, insert_batch, DEFAULT_BATCH_SIZE};
use loadtest_generator::DataGenerator;
use neo4rs::{ConfigBuilder, Graph};
use std::time::{Duration, Instant};
use sync_core::{Schema, UniversalRow};
use tracing::{debug, info};

/// Metrics from a populate operation.
#[derive(Debug, Clone, Default)]
pub struct PopulateMetrics {
    /// Number of nodes inserted.
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

/// Neo4j populator that generates and inserts test data.
///
/// In Neo4j, "tables" become node labels. Each row is a node with properties.
pub struct Neo4jPopulator {
    graph: Graph,
    schema: Schema,
    generator: DataGenerator,
    batch_size: usize,
}

impl Neo4jPopulator {
    /// Create a new Neo4j populator.
    ///
    /// # Arguments
    ///
    /// * `uri` - Neo4j Bolt URI (e.g., "bolt://localhost:7687")
    /// * `username` - Neo4j username
    /// * `password` - Neo4j password
    /// * `database` - Neo4j database name (typically "neo4j")
    /// * `schema` - Load test schema defining labels and field generators
    /// * `seed` - Random seed for deterministic generation
    ///
    /// # Example
    ///
    /// ```ignore
    /// let populator = Neo4jPopulator::new(
    ///     "bolt://localhost:7687",
    ///     "neo4j",
    ///     "password",
    ///     "neo4j",
    ///     schema,
    ///     42,
    /// ).await?;
    /// ```
    pub async fn new(
        uri: &str,
        username: &str,
        password: &str,
        database: &str,
        schema: Schema,
        seed: u64,
    ) -> Result<Self, Neo4jPopulatorError> {
        let config = ConfigBuilder::default()
            .uri(uri)
            .user(username)
            .password(password)
            .db(database)
            .build()
            .map_err(|e| Neo4jPopulatorError::Config(e.to_string()))?;

        let graph =
            Graph::connect(config).map_err(|e| Neo4jPopulatorError::Connection(e.to_string()))?;

        let generator = DataGenerator::new(schema.clone(), seed);

        Ok(Self {
            graph,
            schema,
            generator,
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Create a new Neo4j populator with an existing Graph connection.
    pub fn with_graph(graph: Graph, schema: Schema, seed: u64) -> Self {
        let generator = DataGenerator::new(schema.clone(), seed);
        Self {
            graph,
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

    /// Delete all nodes with the specified label.
    pub async fn delete_nodes(&self, label: &str) -> Result<(), Neo4jPopulatorError> {
        info!("Deleting all nodes with label: {}", label);
        delete_all_nodes(&self.graph, label).await
    }

    /// Populate a label (table) with the specified number of nodes.
    ///
    /// # Arguments
    ///
    /// * `label` - Label name for the nodes (must match a table name in schema)
    /// * `count` - Number of nodes to insert
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub async fn populate(
        &mut self,
        label: &str,
        count: u64,
    ) -> Result<PopulateMetrics, Neo4jPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(label)
            .ok_or_else(|| Neo4jPopulatorError::LabelNotFound(label.to_string()))?
            .clone();

        info!(
            "Populating label '{}' with {} nodes (batch size: {})",
            label, count, self.batch_size
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
                .internal_rows(label, batch_count)
                .map_err(|e| Neo4jPopulatorError::Generator(e.to_string()))?
                .collect();
            generation_time += gen_start.elapsed();

            // Insert rows
            let insert_start = Instant::now();
            let inserted = insert_batch(&self.graph, &table_schema, &rows).await?;
            insert_time += insert_start.elapsed();

            metrics.rows_inserted += inserted;
            metrics.batch_count += 1;
            remaining -= batch_count;

            debug!(
                "Batch {} complete: {} nodes inserted, {} remaining",
                metrics.batch_count, inserted, remaining
            );
        }

        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.insert_duration = insert_time;

        info!(
            "Population complete: {} nodes in {:?} ({:.2} nodes/sec)",
            metrics.rows_inserted,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }

    /// Populate incrementally (append nodes to existing data).
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental sync scenarios.
    pub async fn populate_incremental(
        &mut self,
        label: &str,
        count: u64,
    ) -> Result<PopulateMetrics, Neo4jPopulatorError> {
        info!(
            "Incremental population starting at index {}",
            self.generator.current_index()
        );
        self.populate(label, count).await
    }

    /// Get the node count for a label.
    pub async fn node_count(&self, label: &str) -> Result<u64, Neo4jPopulatorError> {
        count_nodes(&self.graph, label).await
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
      type: int
      generator:
        type: sequential
        start: 1
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
